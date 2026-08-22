//! OpenSearch read client: point-in-time (PIT) + `search_after` paging.
//!
//! We page the whole corpus oldest→newest with a **PIT** snapshot so the view is
//! stable even while the cluster keeps ingesting, and `search_after` (not the
//! deprecated `scroll`) for deep pagination. The sort is
//! `[{<ts_field>: asc}, {_shard_doc: asc}]` — the timestamp gives the
//! oldest-first order the replay wants, and `_shard_doc` (a PIT-only, total
//! per-shard doc order) is the tiebreaker that makes the cursor unambiguous.
//!
//! Auth is one of: none, HTTP basic, bearer token, or AWS SigV4 (managed
//! OpenSearch Service / Serverless) — mutually exclusive, applied per request.

use anyhow::{bail, Context, Result};
use scry_httpsig::SigV4Signer;
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::Arc;

#[derive(Deserialize)]
struct SearchResponse {
    hits: SearchHits,
}

#[derive(Deserialize)]
struct SearchHits {
    hits: Vec<SearchHit>,
}

#[derive(Deserialize)]
struct SearchHit {
    #[serde(rename = "_source", default = "empty_object")]
    source: Value,
    #[serde(default)]
    sort: Option<Vec<Value>>,
}

fn empty_object() -> Value {
    json!({})
}

/// How to authenticate each request.
pub enum Auth {
    None,
    Basic {
        username: String,
        password: String,
    },
    Bearer(String),
    /// Sign every request with AWS SigV4.
    SigV4(Arc<SigV4Signer>),
}

/// A configured OpenSearch reader.
pub struct OsClient {
    http: reqwest::Client,
    /// Base cluster URL, no trailing slash (e.g. `https://os.internal:9200`).
    base_url: String,
    /// Index or index pattern to read.
    index: String,
    auth: Auth,
    /// PIT keep-alive window (e.g. `5m`), refreshed on each search.
    keep_alive: String,
    /// The filter query (`match_all` by default).
    query: Value,
}

/// One page of results from [`OsClient::search_after`].
pub struct Page {
    /// The `_source` object of every hit, in sort order.
    pub sources: Vec<Value>,
    /// The `sort` array of the last hit — feed it back as the next
    /// `search_after`. `None` when the page was empty (end of stream).
    pub next_after: Option<Vec<Value>>,
}

impl OsClient {
    pub fn new(
        http: reqwest::Client,
        base_url: String,
        index: String,
        auth: Auth,
        keep_alive: String,
        query: Value,
    ) -> Self {
        Self {
            http,
            base_url: base_url.trim_end_matches('/').to_string(),
            index,
            auth,
            keep_alive,
            query,
        }
    }

    /// Apply auth (header or SigV4 signature) and execute the request.
    async fn send(&self, mut req: reqwest::Request) -> Result<reqwest::Response> {
        match &self.auth {
            Auth::None => {}
            Auth::Basic { username, password } => {
                use reqwest::header::{HeaderValue, AUTHORIZATION};
                let raw = format!("{username}:{password}");
                let enc = base64_encode(raw.as_bytes());
                let val = HeaderValue::from_str(&format!("Basic {enc}"))
                    .context("building basic-auth header")?;
                req.headers_mut().insert(AUTHORIZATION, val);
            }
            Auth::Bearer(token) => {
                use reqwest::header::{HeaderValue, AUTHORIZATION};
                let val = HeaderValue::from_str(&format!("Bearer {token}"))
                    .context("building bearer header")?;
                req.headers_mut().insert(AUTHORIZATION, val);
            }
            Auth::SigV4(signer) => signer.sign(&mut req).await?,
        }
        let resp = self.http.execute(req).await.context("OpenSearch request")?;
        Ok(resp)
    }

    /// Build a JSON POST request to `path` (relative to the base URL).
    fn post(&self, path: &str, body: &Value) -> Result<reqwest::Request> {
        self.http
            .post(format!("{}{}", self.base_url, path))
            .json(body)
            .build()
            .context("building request")
    }

    /// Total documents matching the query — the progress-bar length.
    pub async fn count(&self) -> Result<u64> {
        let body = json!({ "query": self.query });
        let path = format!("/{}/_count", self.index);
        let resp = self.send(self.post(&path, &body)?).await?;
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            bail!("_count failed: HTTP {status}: {text}");
        }
        let v: Value = serde_json::from_str(&text).context("parsing _count response")?;
        v.get("count")
            .and_then(|c| c.as_u64())
            .context("_count response missing numeric `count`")
    }

    /// Open a PIT over the index; returns the PIT id.
    pub async fn open_pit(&self) -> Result<String> {
        let path = format!("/{}/_pit?keep_alive={}", self.index, self.keep_alive);
        // _pit takes no body.
        let req = self
            .http
            .post(format!("{}{}", self.base_url, path))
            .build()
            .context("building _pit request")?;
        let resp = self.send(req).await?;
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            bail!("_pit open failed: HTTP {status}: {text}");
        }
        let v: Value = serde_json::from_str(&text).context("parsing _pit response")?;
        // OpenSearch returns {"pit_id": "..."}; Elasticsearch used {"id": "..."}.
        v.get("pit_id")
            .or_else(|| v.get("id"))
            .and_then(|s| s.as_str())
            .map(|s| s.to_string())
            .context("_pit response missing `pit_id`")
    }

    /// Fetch one page after `after` (or the first page when `after` is `None`).
    pub async fn search_after(
        &self,
        pit_id: &str,
        size: usize,
        ts_field: &str,
        after: Option<&[Value]>,
    ) -> Result<Page> {
        let mut body = json!({
            "size": size,
            "track_total_hits": false,
            "query": self.query,
            "sort": [
                { ts_field: { "order": "asc" } },
                { "_shard_doc": "asc" }
            ],
            "pit": { "id": pit_id, "keep_alive": self.keep_alive },
        });
        if let Some(a) = after {
            body["search_after"] = Value::Array(a.to_vec());
        }
        // With a PIT the index is carried by the PIT, so search hits `/_search`.
        let resp = self.send(self.post("/_search", &body)?).await?;
        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            bail!("_search failed: HTTP {status}: {text}");
        }

        // Deserialize directly into owned hits. The previous generic-Value path
        // retained the complete response tree and then cloned every `_source`,
        // roughly doubling a page's peak JSON memory.
        let parsed: SearchResponse = resp.json().await.context("parsing _search response")?;
        let mut hits = parsed.hits.hits;
        let next_after = hits.last_mut().and_then(|hit| hit.sort.take());
        let sources = hits.into_iter().map(|hit| hit.source).collect();
        Ok(Page {
            sources,
            next_after,
        })
    }

    /// Close the PIT. Best-effort — a failure here just lets the PIT expire on
    /// its keep-alive.
    pub async fn close_pit(&self, pit_id: &str) -> Result<()> {
        let body = json!({ "pit_id": pit_id, "id": pit_id });
        let req = self
            .http
            .request(reqwest::Method::DELETE, format!("{}/_pit", self.base_url))
            .json(&body)
            .build()
            .context("building _pit delete request")?;
        let resp = self.send(req).await?;
        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            bail!("_pit close failed: HTTP {status}: {text}");
        }
        Ok(())
    }
}

/// Minimal standard base64 (for the HTTP basic-auth header). Avoids pulling a
/// base64 crate for one 3-line function.
fn base64_encode(input: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = *chunk.get(1).unwrap_or(&0) as u32;
        let b2 = *chunk.get(2).unwrap_or(&0) as u32;
        let n = (b0 << 16) | (b1 << 8) | b2;
        out.push(TABLE[((n >> 18) & 63) as usize] as char);
        out.push(TABLE[((n >> 12) & 63) as usize] as char);
        if chunk.len() > 1 {
            out.push(TABLE[((n >> 6) & 63) as usize] as char);
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(TABLE[(n & 63) as usize] as char);
        } else {
            out.push('=');
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base64_matches_known_vectors() {
        assert_eq!(base64_encode(b""), "");
        assert_eq!(base64_encode(b"f"), "Zg==");
        assert_eq!(base64_encode(b"fo"), "Zm8=");
        assert_eq!(base64_encode(b"foo"), "Zm9v");
        assert_eq!(base64_encode(b"foob"), "Zm9vYg==");
        assert_eq!(base64_encode(b"fooba"), "Zm9vYmE=");
        assert_eq!(base64_encode(b"foobar"), "Zm9vYmFy");
        assert_eq!(base64_encode(b"user:pass"), "dXNlcjpwYXNz");
    }
}
