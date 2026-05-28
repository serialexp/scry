//! CRI container-log format: parsing, partial-line reassembly, and a
//! polling tailer.
//!
//! kubelet writes one file per container under
//! `/var/log/pods/<ns>_<pod>_<uid>/<container>/<restart>.log`, with each
//! line shaped as:
//!
//! ```text
//! 2024-10-01T12:00:00.123456789Z stdout F a complete log line
//! 2024-10-01T12:00:00.123456789Z stderr P a line split by kubelet at 16KiB…
//! ```
//!
//! The 4th field tag is `F` (full) or `P` (partial). A run of `P` lines for
//! the same stream is concatenated with the terminating `F` line into one
//! logical entry.

use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::sync::{mpsc, watch};
use tracing::{debug, warn};

/// Largest slice of a log file we read in a single poll wake. Bounds the
/// per-iteration allocation; if more is pending we loop again immediately.
const READ_CHUNK: u64 = 4 * 1024 * 1024;

/// Which CRI output stream a line came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stream {
    Stdout,
    Stderr,
}

impl Stream {
    pub fn name(self) -> &'static str {
        match self {
            Stream::Stdout => "stdout",
            Stream::Stderr => "stderr",
        }
    }

    /// Map to the syslog-ish severity scale the rest of scry uses
    /// (5 debug, 9 info, 13 warn, 17 error). We have no real level from CRI,
    /// so stdout→info and stderr→error is the conventional approximation.
    pub fn severity(self) -> u8 {
        match self {
            Stream::Stdout => 9,
            Stream::Stderr => 17,
        }
    }

    fn parse(s: &str) -> Option<Stream> {
        match s {
            "stdout" => Some(Stream::Stdout),
            "stderr" => Some(Stream::Stderr),
            _ => None,
        }
    }
}

/// A container's identity derived from its log directory path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PodPath {
    pub namespace: String,
    pub pod: String,
    pub uid: String,
    pub container: String,
}

impl PodPath {
    /// Parse `<logs_root>/<ns>_<pod>_<uid>/<container>/<restart>.log`.
    /// Kubernetes object names are DNS-1123 (no `_`), so the pod directory
    /// splits cleanly into exactly three `_`-separated fields.
    pub fn from_log_file(path: &Path) -> Option<PodPath> {
        let container = path.parent()?.file_name()?.to_str()?.to_string();
        let pod_dir = path.parent()?.parent()?.file_name()?.to_str()?;
        let mut parts = pod_dir.splitn(3, '_');
        let namespace = parts.next()?.to_string();
        let pod = parts.next()?.to_string();
        let uid = parts.next()?.to_string();
        if namespace.is_empty() || pod.is_empty() || uid.is_empty() || container.is_empty() {
            return None;
        }
        Some(PodPath { namespace, pod, uid, container })
    }
}

/// One reassembled log line ready to ship.
#[derive(Debug, Clone)]
pub struct RawLog {
    pub pod: Arc<PodPath>,
    pub stream: Stream,
    pub ts_unix_nano: u64,
    pub body: String,
}

/// Parse a single CRI line into `(ts_unix_nano, stream, is_full, message)`.
/// Returns `None` if the line doesn't match the 4-field CRI shape.
fn parse_line(line: &str) -> Option<(u64, Stream, bool, &str)> {
    let mut it = line.splitn(4, ' ');
    let ts_str = it.next()?;
    let stream = Stream::parse(it.next()?)?;
    let tag = it.next()?;
    let msg = it.next().unwrap_or("");
    let is_full = match tag {
        "F" => true,
        "P" => false,
        _ => return None,
    };
    let ts = chrono::DateTime::parse_from_rfc3339(ts_str).ok()?;
    let nanos = ts.timestamp_nanos_opt()? as u64;
    Some((nanos, stream, is_full, msg))
}

/// Per-stream accumulator for partial (`P`) lines.
#[derive(Default)]
struct PartialBufs {
    stdout: String,
    stderr: String,
}

impl PartialBufs {
    fn clear(&mut self) {
        self.stdout.clear();
        self.stderr.clear();
    }

    fn buf(&mut self, s: Stream) -> &mut String {
        match s {
            Stream::Stdout => &mut self.stdout,
            Stream::Stderr => &mut self.stderr,
        }
    }
}

/// Spawn a polling tailer for one container log file. Emits [`RawLog`] into
/// `tx` until `shutdown` flips true or the channel closes. `from_start`
/// controls whether we replay the existing file (tests) or begin at EOF
/// (production — we don't want history on every (re)start).
pub fn spawn_tailer(
    pod: Arc<PodPath>,
    path: PathBuf,
    from_start: bool,
    poll: Duration,
    tx: mpsc::Sender<RawLog>,
    mut shutdown: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut offset: u64 = 0;
        if !from_start {
            offset = current_len(&path).await.unwrap_or(0);
        }
        let mut leftover: Vec<u8> = Vec::new();
        let mut partial = PartialBufs::default();

        loop {
            // Drain everything currently available, in bounded chunks.
            loop {
                match read_new(&path, &mut offset, &mut leftover, &mut partial).await {
                    Ok(Some(bytes)) => {
                        if !bytes.is_empty() {
                            process_chunk(&pod, &bytes, &mut partial, &tx).await;
                        }
                        // A full chunk likely means more is pending; keep going.
                        if bytes.len() < READ_CHUNK as usize {
                            break;
                        }
                    }
                    Ok(None) => break, // truncated/rotated handled inside; nothing to emit
                    Err(e) => {
                        debug!(path = %path.display(), error = %e, "tailer read error");
                        break;
                    }
                }
            }

            tokio::select! {
                _ = tokio::time::sleep(poll) => {}
                res = shutdown.changed() => {
                    if res.is_err() || *shutdown.borrow() {
                        break;
                    }
                }
            }
            if tx.is_closed() {
                break;
            }
        }
        debug!(path = %path.display(), "tailer stopped");
    })
}

async fn current_len(path: &Path) -> Result<u64> {
    Ok(tokio::fs::metadata(path).await?.len())
}

/// Read newly-appended bytes from `path` starting at `*offset`, returning the
/// completed-line bytes (everything up to the last `\n`). Updates `*offset`
/// and stashes any trailing partial line into `leftover`. Detects truncation
/// (file shorter than offset → rotated) and resets to the file start.
async fn read_new(
    path: &Path,
    offset: &mut u64,
    leftover: &mut Vec<u8>,
    partial: &mut PartialBufs,
) -> Result<Option<Vec<u8>>> {
    let mut f = match File::open(path).await {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e).context("open log file"),
    };
    let len = f.metadata().await?.len();
    if len < *offset {
        // Rotated or truncated: drop any cross-file partial state and start over.
        *offset = 0;
        leftover.clear();
        partial.clear();
    }
    if len == *offset {
        return Ok(None);
    }
    let to_read = (len - *offset).min(READ_CHUNK);
    f.seek(SeekFrom::Start(*offset)).await?;
    let mut buf = vec![0u8; to_read as usize];
    f.read_exact(&mut buf).await?;
    *offset += to_read;

    // Combine with any previously-stashed partial bytes.
    let mut combined = std::mem::take(leftover);
    combined.extend_from_slice(&buf);

    // Everything up to the last newline is complete; the tail is the new leftover.
    match combined.iter().rposition(|&b| b == b'\n') {
        Some(idx) => {
            let rest = combined.split_off(idx + 1);
            *leftover = rest;
            Ok(Some(combined))
        }
        None => {
            *leftover = combined;
            Ok(Some(Vec::new()))
        }
    }
}

/// Parse complete lines from `bytes`, reassemble partials, and emit.
async fn process_chunk(
    pod: &Arc<PodPath>,
    bytes: &[u8],
    partial: &mut PartialBufs,
    tx: &mpsc::Sender<RawLog>,
) {
    for raw in bytes.split(|&b| b == b'\n') {
        if raw.is_empty() {
            continue;
        }
        let line = match std::str::from_utf8(raw) {
            Ok(s) => s,
            Err(_) => {
                warn!("skipping non-utf8 log line");
                continue;
            }
        };
        let (ts, stream, is_full, msg) = match parse_line(line) {
            Some(p) => p,
            None => continue,
        };

        let buf = partial.buf(stream);
        let body = if buf.is_empty() {
            msg.to_string()
        } else {
            buf.push_str(msg);
            std::mem::take(buf)
        };

        if !is_full {
            // Partial: stash and wait for the terminating F line.
            let buf = partial.buf(stream);
            *buf = body;
            continue;
        }

        let rec = RawLog {
            pod: Arc::clone(pod),
            stream,
            ts_unix_nano: ts,
            body,
        };
        if tx.send(rec).await.is_err() {
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_full_line() {
        let (ts, stream, full, msg) =
            parse_line("2024-10-01T12:00:00.000000001Z stdout F hello world").unwrap();
        assert_eq!(stream, Stream::Stdout);
        assert!(full);
        assert_eq!(msg, "hello world");
        assert_eq!(ts % 1_000_000_000, 1); // 1 ns past the second
    }

    #[test]
    fn rejects_garbage() {
        assert!(parse_line("not a cri line").is_none());
        assert!(parse_line("2024-10-01T12:00:00Z stdout X msg").is_none());
    }

    #[test]
    fn pod_path_from_file() {
        let p = Path::new("/var/log/pods/team-a_api-7d9_abc-123/server/0.log");
        let pp = PodPath::from_log_file(p).unwrap();
        assert_eq!(pp.namespace, "team-a");
        assert_eq!(pp.pod, "api-7d9");
        assert_eq!(pp.uid, "abc-123");
        assert_eq!(pp.container, "server");
    }

    #[tokio::test]
    async fn reassembles_partial_lines() {
        let pod = Arc::new(PodPath {
            namespace: "ns".into(),
            pod: "pod".into(),
            uid: "uid".into(),
            container: "c".into(),
        });
        let (tx, mut rx) = mpsc::channel(8);
        let mut partial = PartialBufs::default();
        let chunk = b"2024-10-01T12:00:00.000000000Z stdout P part1 \n2024-10-01T12:00:00.000000000Z stdout F part2\n";
        process_chunk(&pod, chunk, &mut partial, &tx).await;
        let rec = rx.try_recv().unwrap();
        assert_eq!(rec.body, "part1 part2");
        assert_eq!(rec.stream, Stream::Stdout);
        assert!(rx.try_recv().is_err());
    }
}
