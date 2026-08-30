//! Shared operator-status envelope and tiny local fleet HTTP server.

use std::{
    borrow::Cow,
    future::Future,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use tracing::{info, warn};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct StatusSnapshot {
    pub role: String,
    pub instance_id: String,
    pub addr: String,
    #[serde(default)]
    pub version: String,
    pub now_unix_ms: u64,
    pub uptime_secs: f64,
    pub rss_kib: Option<u64>,
    pub data: serde_json::Value,
}

pub trait LocalStatus: Send + Sync + 'static {
    fn snapshot(&self) -> StatusSnapshot;
}

#[async_trait::async_trait]
pub trait FleetSource: Send + Sync + 'static {
    fn source(&self) -> &'static str {
        "valkey"
    }
    async fn blobs(&self) -> Vec<String>;
}

struct StatusState {
    local: Arc<dyn LocalStatus>,
    fleet: Option<Arc<dyn FleetSource>>,
    self_id: String,
}

impl StatusState {
    async fn stats_json(&self) -> String {
        let (source, mut instances): (&str, Vec<StatusSnapshot>) = match &self.fleet {
            Some(fleet) => {
                let instances = fleet
                    .blobs()
                    .await
                    .into_iter()
                    .filter_map(|blob| serde_json::from_str(&blob).ok())
                    .collect();
                (fleet.source(), instances)
            }
            None => ("local", Vec::new()),
        };
        if !instances
            .iter()
            .any(|snapshot| snapshot.instance_id == self.self_id)
        {
            instances.push(self.local.snapshot());
        }
        serde_json::json!({"source": source, "self_id": self.self_id, "instances": instances})
            .to_string()
    }
}

pub async fn serve_status<F>(
    listen_addr: String,
    local: Arc<dyn LocalStatus>,
    fleet: Option<Arc<dyn FleetSource>>,
    self_id: String,
    shutdown: F,
) -> Result<()>
where
    F: Future<Output = ()>,
{
    let listener = TcpListener::bind(&listen_addr)
        .await
        .with_context(|| format!("binding status endpoint {listen_addr}"))?;
    info!(addr = %listen_addr, "status HTTP endpoint listening (GET / and /stats.json)");
    let state = Arc::new(StatusState {
        local,
        fleet,
        self_id,
    });
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok((socket, _)) => {
                    let state = state.clone();
                    tokio::spawn(async move {
                        if let Err(error) = handle_http(socket, state).await {
                            tracing::debug!(%error, "status connection ended with error");
                        }
                    });
                }
                Err(error) => warn!(%error, "status accept failed"),
            },
            _ = &mut shutdown => break,
        }
    }
    Ok(())
}

async fn handle_http(mut socket: TcpStream, state: Arc<StatusState>) -> Result<()> {
    const MAX_REQUEST_BYTES: usize = 8 * 1024;
    let mut request = Vec::with_capacity(256);
    let mut chunk = [0_u8; 1024];
    while request.len() < MAX_REQUEST_BYTES && find_subslice(&request, b"\r\n\r\n").is_none() {
        let read = socket
            .read(&mut chunk)
            .await
            .context("reading HTTP request")?;
        if read == 0 {
            break;
        }
        request.extend_from_slice(&chunk[..read]);
    }
    let (method, path) = parse_request_line(&request);
    let (status, content_type, body): (&str, &str, Cow<'static, str>) = match (method, path) {
        (Some("GET"), Some("/")) => (
            "200 OK",
            "text/html; charset=utf-8",
            Cow::Borrowed(FLEET_HTML),
        ),
        (Some("GET"), Some("/stats.json")) => (
            "200 OK",
            "application/json",
            Cow::Owned(state.stats_json().await),
        ),
        (Some("GET"), Some(_)) => (
            "404 Not Found",
            "text/plain; charset=utf-8",
            Cow::Borrowed("not found\n"),
        ),
        (Some(_), _) => (
            "405 Method Not Allowed",
            "text/plain; charset=utf-8",
            Cow::Borrowed("method not allowed\n"),
        ),
        _ => (
            "400 Bad Request",
            "text/plain; charset=utf-8",
            Cow::Borrowed("bad request\n"),
        ),
    };
    let response = format!("HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nAccess-Control-Allow-Origin: *\r\nConnection: close\r\n\r\n{body}", body.len());
    socket
        .write_all(response.as_bytes())
        .await
        .context("writing HTTP response")?;
    let _ = socket.shutdown().await;
    Ok(())
}

fn parse_request_line(request: &[u8]) -> (Option<&str>, Option<&str>) {
    let end = find_subslice(request, b"\r\n").unwrap_or(request.len());
    let Ok(line) = std::str::from_utf8(&request[..end]) else {
        return (None, None);
    };
    let mut parts = line.split(' ');
    (
        parts.next().filter(|part| !part.is_empty()),
        parts.next().filter(|part| !part.is_empty()),
    )
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    (!needle.is_empty() && haystack.len() >= needle.len())
        .then(|| {
            haystack
                .windows(needle.len())
                .position(|window| window == needle)
        })
        .flatten()
}

pub fn rss_kib() -> Option<u64> {
    std::fs::read_to_string("/proc/self/status")
        .ok()?
        .lines()
        .find_map(|line| line.strip_prefix("VmRSS:"))?
        .split_whitespace()
        .next()?
        .parse()
        .ok()
}

pub fn unix_ms_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

const FLEET_HTML: &str = r#"<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>scry status</title><style>body{font:13px ui-monospace,monospace;background:#0d1117;color:#c9d1d9;padding:24px}h1,h2{color:#58a6ff}.cards{display:grid;grid-template-columns:repeat(auto-fill,minmax(320px,1fr));gap:12px}.card{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:14px}.self{border-color:#58a6ff}.meta{color:#8b949e}pre{white-space:pre-wrap;overflow-wrap:anywhere}</style></head><body><h1>scry fleet status</h1><div id="meta">connecting…</div><div id="fleet"></div><script>const esc=s=>String(s).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));function render(p){const groups={};for(const s of p.instances||[])(groups[s.role]??=[]).push(s);document.querySelector('#meta').textContent=`${(p.instances||[]).length} instance(s) · source=${p.source}`;document.querySelector('#fleet').innerHTML=Object.entries(groups).sort().map(([role,list])=>`<h2>${esc(role)}</h2><div class="cards">${list.map(s=>`<article class="card ${s.instance_id===p.self_id?'self':''}"><strong>${esc(s.instance_id)}</strong><div class="meta">${esc(s.addr||'?')} · v${esc(s.version||'?')} · up ${Math.round(s.uptime_secs||0)}s</div><pre>${esc(JSON.stringify(s.data,null,2))}</pre></article>`).join('')}</div>`).join('')}async function poll(){try{render(await(await fetch('/stats.json',{cache:'no-store'})).json())}catch(e){document.querySelector('#meta').textContent=String(e)}}poll();setInterval(poll,1000)</script></body></html>"#;

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::{io::AsyncReadExt, sync::Notify};

    struct Local(StatusSnapshot);
    impl LocalStatus for Local {
        fn snapshot(&self) -> StatusSnapshot {
            self.0.clone()
        }
    }
    fn snapshot(role: &str, id: &str) -> StatusSnapshot {
        StatusSnapshot {
            role: role.into(),
            instance_id: id.into(),
            addr: "127.0.0.1:1".into(),
            version: "1.2.3".into(),
            now_unix_ms: 1,
            uptime_secs: 2.0,
            rss_kib: None,
            data: serde_json::json!({"ok": true}),
        }
    }

    #[test]
    fn old_snapshot_defaults_version() {
        let value = serde_json::json!({"role":"query","instance_id":"q","addr":"x","now_unix_ms":1,"uptime_secs":1.0,"rss_kib":null,"data":{}});
        assert_eq!(
            serde_json::from_value::<StatusSnapshot>(value)
                .unwrap()
                .version,
            ""
        );
    }

    #[tokio::test]
    async fn serves_local_snapshot() {
        let probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = probe.local_addr().unwrap();
        drop(probe);
        let stop = Arc::new(Notify::new());
        let waiter = stop.clone();
        let task = tokio::spawn(serve_status(
            addr.to_string(),
            Arc::new(Local(snapshot("gateway", "g"))),
            None,
            "g".into(),
            async move { waiter.notified().await },
        ));
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        let mut socket = TcpStream::connect(addr).await.unwrap();
        socket
            .write_all(b"GET /stats.json HTTP/1.1\r\nHost: x\r\n\r\n")
            .await
            .unwrap();
        let mut response = String::new();
        socket.read_to_string(&mut response).await.unwrap();
        assert!(response.contains("\"role\":\"gateway\""));
        assert!(response.contains("\"source\":\"local\""));
        stop.notify_waiters();
        task.await.unwrap().unwrap();
    }
}
