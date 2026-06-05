//! Shared HTTP client construction for the gateway's HTTP sinks
//! (Loki / OpenSearch / Mimir).
//!
//! All three reach their endpoints with a single `reqwest::Client`. By default
//! that client trusts only the built-in webpki roots; endpoints fronted by a
//! private or internal CA then fail TLS verification. [`build_http_client`]
//! optionally loads a custom CA certificate (PEM, possibly a multi-cert bundle)
//! and **adds** it to the trust store — public-CA endpoints keep working, the
//! private CA is trusted on top.

use std::{path::Path, time::Duration};

use anyhow::{Context, Result};

/// Build the shared HTTP client for the Loki/OpenSearch/Mimir sinks.
///
/// When `ca_cert` is set, the file is read and parsed as a PEM bundle (one or
/// more `-----BEGIN CERTIFICATE-----` blocks) and every certificate is added as
/// an extra trusted root, augmenting the built-in roots.
pub fn build_http_client(timeout: Duration, ca_cert: Option<&Path>) -> Result<reqwest::Client> {
    let mut builder = reqwest::Client::builder().timeout(timeout);

    if let Some(path) = ca_cert {
        let pem = std::fs::read(path)
            .with_context(|| format!("reading CA certificate {}", path.display()))?;
        let certs = reqwest::Certificate::from_pem_bundle(&pem)
            .with_context(|| format!("parsing CA certificate bundle {}", path.display()))?;
        if certs.is_empty() {
            anyhow::bail!(
                "no certificates found in CA bundle {} (expected PEM)",
                path.display()
            );
        }
        for cert in certs {
            builder = builder.add_root_certificate(cert);
        }
    }

    builder
        .build()
        .context("building HTTP client for Loki/OpenSearch/Mimir sinks")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_without_ca() {
        assert!(build_http_client(Duration::from_secs(30), None).is_ok());
    }

    #[test]
    fn errors_on_missing_ca_file() {
        let res = build_http_client(
            Duration::from_secs(30),
            Some(Path::new("/nonexistent/ca-does-not-exist.pem")),
        );
        assert!(res.is_err());
    }
}
