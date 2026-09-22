//! Authenticated, bounded HTTP proxy from the browser alerts surface to alertd.
//! Browser credentials are never forwarded: request headers are allowlisted and
//! the process-wide service token is installed as `Authorization: Bearer ...`.

use axum::body::{Body, Bytes};
use axum::extract::{OriginalUri, Path, State};
use axum::http::{header, HeaderMap, Method, StatusCode};
use axum::response::Response;
use axum_extra::extract::cookie::SignedCookieJar;
use futures::StreamExt;
use tracing::warn;

use crate::auth::{mutation_request_valid, session_valid};
use crate::AppState;

const TARGET_HEADER: &str = "x-scry-target";
const MAX_RESPONSE_BYTES: usize = 8 * 1024 * 1024;

pub async fn proxy(
    State(state): State<AppState>,
    jar: SignedCookieJar,
    Path(path): Path<String>,
    OriginalUri(original_uri): OriginalUri,
    method: Method,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if !session_valid(&jar) {
        return text(StatusCode::UNAUTHORIZED, "authentication required");
    }
    let mutation = method != Method::GET;
    if mutation && state.insecure_cookie() {
        return text(
            StatusCode::FORBIDDEN,
            "alert mutations are disabled while --insecure-cookie is active",
        );
    }
    if mutation && !mutation_request_valid(&headers, &jar) {
        return text(StatusCode::FORBIDDEN, "CSRF validation failed");
    }

    let requested = headers
        .get(TARGET_HEADER)
        .and_then(|value| value.to_str().ok());
    let Some(target) = state.find_target(requested) else {
        return text(StatusCode::BAD_REQUEST, "unknown target");
    };
    let Some(base) = target.alertd_addr.as_deref() else {
        return text(
            StatusCode::CONFLICT,
            "alerts are not configured for this target",
        );
    };
    let Some(token) = state.alertd_token() else {
        warn!("alertd target exists without a service token");
        return text(
            StatusCode::SERVICE_UNAVAILABLE,
            "alert service is unavailable",
        );
    };
    if path.is_empty() || path.split('/').any(|segment| segment == "..") {
        return text(StatusCode::BAD_REQUEST, "invalid alert path");
    }

    let permit = match state.alertd_permits().clone().try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => return text(StatusCode::SERVICE_UNAVAILABLE, "alert proxy is busy"),
    };
    let mut url = format!("{base}/v1/{path}");
    if let Some(query) = original_uri.query() {
        url.push('?');
        url.push_str(query);
    }
    let mut request = state
        .alertd_client()
        .request(method, url)
        .bearer_auth(token)
        .body(body);
    // Explicit allowlist: browser Authorization, Cookie, forwarding, and all
    // hop-by-hop headers are intentionally discarded.
    for name in [header::ACCEPT, header::CONTENT_TYPE, header::IF_MATCH] {
        if let Some(value) = headers.get(&name) {
            request = request.header(name, value);
        }
    }
    if let Some(value) = headers.get("idempotency-key") {
        request = request.header("idempotency-key", value);
    }

    let result = tokio::time::timeout(state.alertd_timeout(), async {
        let upstream = request.send().await.map_err(ProxyError::Transport)?;
        let status = upstream.status();
        let content_type = upstream.headers().get(header::CONTENT_TYPE).cloned();
        let etag = upstream.headers().get(header::ETAG).cloned();
        let location = upstream.headers().get(header::LOCATION).cloned();
        if upstream
            .content_length()
            .is_some_and(|length| length > MAX_RESPONSE_BYTES as u64)
        {
            return Err(ProxyError::TooLarge);
        }
        let mut bytes = Vec::new();
        let mut stream = upstream.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(ProxyError::Transport)?;
            if bytes.len().saturating_add(chunk.len()) > MAX_RESPONSE_BYTES {
                return Err(ProxyError::TooLarge);
            }
            bytes.extend_from_slice(&chunk);
        }
        Ok::<_, ProxyError>((status, content_type, etag, location, bytes))
    })
    .await;
    drop(permit);

    match result {
        Err(_) => text(StatusCode::GATEWAY_TIMEOUT, "alert service timed out"),
        Ok(Err(ProxyError::Transport(error))) => {
            warn!(%error, "alertd proxy request failed");
            text(StatusCode::BAD_GATEWAY, "alert service request failed")
        }
        Ok(Err(ProxyError::TooLarge)) => text(
            StatusCode::BAD_GATEWAY,
            "alert service response exceeded the size limit",
        ),
        Ok(Ok((status, content_type, etag, location, bytes))) => {
            let mut response = Response::builder().status(status);
            if let Some(value) = content_type {
                response = response.header(header::CONTENT_TYPE, value);
            }
            if let Some(value) = etag {
                response = response.header(header::ETAG, value);
            }
            if let Some(value) = location {
                response = response.header(header::LOCATION, value);
            }
            response
                .body(Body::from(bytes))
                .expect("upstream status and allowlisted headers are valid")
        }
    }
}

enum ProxyError {
    Transport(reqwest::Error),
    TooLarge,
}

fn text(status: StatusCode, message: &'static str) -> Response {
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "text/plain; charset=utf-8")
        .body(Body::from(message))
        .expect("static response is valid")
}
