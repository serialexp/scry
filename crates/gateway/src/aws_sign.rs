//! AWS SigV4 request signing for the OpenSearch sink.
//!
//! Amazon OpenSearch Service (managed domains, signing name `es`) and OpenSearch
//! Serverless (`aoss`) authorize requests with **AWS SigV4** rather than
//! basic/cookie auth, so the sink must sign *every* request it makes — the bulk
//! writes **and** the management calls (ISM policy, index template, data
//! streams). Credentials and region come from the standard AWS chain
//! (`aws-config`): env vars, the shared profile, EKS IRSA, or EC2/ECS IMDS.
//!
//! Signing happens just before send: we build the `reqwest::Request`, sign a
//! [`SignableRequest`] view of it (method / url / headers / body), then copy the
//! resulting `Authorization` + `X-Amz-*` headers back onto the request. The
//! mandatory `host` header is derived by aws-sigv4 from the URL (default ports
//! stripped exactly as reqwest/hyper do), so the signed `host` matches what goes
//! on the wire without us pre-populating it.

use std::time::{Duration, SystemTime};

use anyhow::{Context, Result};
use aws_credential_types::{
    provider::{ProvideCredentials, SharedCredentialsProvider},
    Credentials,
};
use aws_sigv4::{
    http_request::{sign, PayloadChecksumKind, SignableBody, SignableRequest, SigningSettings},
    sign::v4,
};
use reqwest::header::{HeaderName, HeaderValue};
use tokio::sync::Mutex;

/// Refresh cached credentials this long before they expire (covers clock skew
/// and the round-trip of the signed request).
const REFRESH_SKEW: Duration = Duration::from_secs(300);

/// Signs `reqwest` requests with AWS SigV4 for an OpenSearch endpoint.
pub struct SigV4Signer {
    provider: SharedCredentialsProvider,
    region: String,
    /// Signing name: `es` for managed OpenSearch Service domains, `aoss` for
    /// OpenSearch Serverless.
    service: String,
    /// Last resolved credentials, reused until near expiry so we don't hit the
    /// provider (IMDS/STS) on every request. The OpenSearch sink worker is
    /// single-threaded, so this `Mutex` only exists to satisfy `&self`.
    cached: Mutex<Option<Credentials>>,
}

impl SigV4Signer {
    pub fn new(provider: SharedCredentialsProvider, region: String, service: String) -> Self {
        Self {
            provider,
            region,
            service,
            cached: Mutex::new(None),
        }
    }

    /// Resolve credentials, reusing the cached set until it's within
    /// [`REFRESH_SKEW`] of expiry (static credentials never expire).
    async fn credentials(&self) -> Result<Credentials> {
        let mut slot = self.cached.lock().await;
        let still_fresh = slot.as_ref().is_some_and(|c| match c.expiry() {
            Some(exp) => exp > SystemTime::now() + REFRESH_SKEW,
            None => true,
        });
        if still_fresh {
            return Ok(slot.clone().expect("checked Some above"));
        }
        let creds = self
            .provider
            .provide_credentials()
            .await
            .context("resolving AWS credentials for OpenSearch SigV4")?;
        *slot = Some(creds.clone());
        Ok(creds)
    }

    /// Sign `req` in place, adding the SigV4 `Authorization` + `X-Amz-*` headers.
    pub async fn sign(&self, req: &mut reqwest::Request) -> Result<()> {
        let creds = self.credentials().await?;
        let identity = creds.into();

        let mut settings = SigningSettings::default();
        // Emit `x-amz-content-sha256` (required by OpenSearch Serverless,
        // accepted by managed domains) so the body is covered by the signature.
        settings.payload_checksum_kind = PayloadChecksumKind::XAmzSha256;

        let params = v4::SigningParams::builder()
            .identity(&identity)
            .region(&self.region)
            .name(&self.service)
            .time(SystemTime::now())
            .settings(settings)
            .build()
            .context("building SigV4 signing params")?
            .into();

        // Snapshot the request's headers + body for the signable view. The body
        // is always in-memory here (serialized JSON / NDJSON, or empty), so
        // `Body::as_bytes` returns `Some`.
        let body = req
            .body()
            .and_then(|b| b.as_bytes())
            .unwrap_or(&[])
            .to_vec();
        let headers: Vec<(String, String)> = req
            .headers()
            .iter()
            .filter_map(|(k, v)| {
                v.to_str()
                    .ok()
                    .map(|v| (k.as_str().to_string(), v.to_string()))
            })
            .collect();
        let signable = SignableRequest::new(
            req.method().as_str(),
            req.url().as_str(),
            headers.iter().map(|(k, v)| (k.as_str(), v.as_str())),
            SignableBody::Bytes(&body),
        )
        .context("building signable request")?;

        let (instructions, _signature) = sign(signable, &params)
            .context("computing SigV4 signature")?
            .into_parts();
        let (signed_headers, _params) = instructions.into_parts();
        for h in signed_headers {
            let name = HeaderName::from_bytes(h.name().as_bytes())
                .context("SigV4 produced an invalid header name")?;
            let value = HeaderValue::from_str(h.value())
                .context("SigV4 produced an invalid header value")?;
            req.headers_mut().insert(name, value);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn signs_request_with_sigv4_headers() {
        let creds = Credentials::new("AKIDEXAMPLE", "wJalrXUtnFEMIsecretKEY", None, None, "test");
        let signer = SigV4Signer::new(
            SharedCredentialsProvider::new(creds),
            "us-east-1".to_string(),
            "es".to_string(),
        );

        let client = reqwest::Client::new();
        let mut req = client
            .post("https://search-foo.us-east-1.es.amazonaws.com/_bulk")
            .header("content-type", "application/x-ndjson")
            .body("{\"create\":{}}\n{\"body\":\"hi\"}\n")
            .build()
            .unwrap();

        signer.sign(&mut req).await.unwrap();

        let auth = req
            .headers()
            .get("authorization")
            .unwrap()
            .to_str()
            .unwrap();
        assert!(auth.starts_with("AWS4-HMAC-SHA256 "), "scheme: {auth}");
        assert!(
            auth.contains("Credential=AKIDEXAMPLE/"),
            "credential: {auth}"
        );
        assert!(auth.contains("/us-east-1/es/aws4_request"), "scope: {auth}");
        // host must be in the signed set, and the body checksum header present.
        assert!(
            auth.contains("SignedHeaders=") && auth.contains("host"),
            "signed headers: {auth}"
        );
        assert!(auth.contains("Signature="), "signature: {auth}");
        assert!(req.headers().contains_key("x-amz-date"));
        assert!(req.headers().contains_key("x-amz-content-sha256"));
    }

    #[tokio::test]
    async fn empty_body_get_is_signed() {
        let creds = Credentials::new("AKIDEXAMPLE", "secret", None, None, "test");
        let signer = SigV4Signer::new(
            SharedCredentialsProvider::new(creds),
            "eu-west-1".to_string(),
            "aoss".to_string(),
        );
        let mut req = reqwest::Client::new()
            .get("https://abc.eu-west-1.aoss.amazonaws.com/_data_stream/scry-logs-api")
            .build()
            .unwrap();
        signer.sign(&mut req).await.unwrap();
        let auth = req
            .headers()
            .get("authorization")
            .unwrap()
            .to_str()
            .unwrap();
        assert!(
            auth.contains("/eu-west-1/aoss/aws4_request"),
            "scope: {auth}"
        );
    }
}
