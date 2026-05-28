//! Per-writer, per-signal write-ahead log.
//!
//! Sits between ingest and the block builder per
//! `ARCHITECTURE.md § The WAL`:
//!
//! 1. Every batch that's accepted by ingest is appended to the WAL
//!    before being added to the in-memory builder, so a process crash
//!    doesn't lose acknowledged data.
//! 2. Segments are sealed (fsynced) only on rotation. Per-record fsync
//!    is explicitly out of scope; the durability boundary is "the last
//!    few ms of records may be lost on crash."
//!
//! ## On-disk layout
//!
//! ```text
//! <dir>/<signal>/wal-<u64-seq>.log
//! ```
//!
//! `seq` is zero-padded to 20 digits so lexicographic and numeric
//! orderings agree. Sequence numbers are dense and monotonic; gaps
//! after [`Wal::mark_uploaded`] are expected.
//!
//! ## Frame format
//!
//! ```text
//! [ len: u32 BE | crc32: u32 BE | payload: len bytes ]
//! ```
//!
//! CRC is computed over the payload only — the length field is
//! self-validating against the file size. Big-endian for consistency
//! with the wire protocol; the local-vs-network distinction isn't
//! worth the 1–2 ns/frame a swap would cost.
//!
//! ## Lifecycle
//!
//! ```text
//!   open(dir, signal)
//!     │  → scan existing segments, pick next seq, create empty active
//!     │
//!   append(payload)   ←─── caller's ingest path
//!     │  → may rotate internally if max_segment_bytes is reached
//!     │
//!   rotate()          ←─── caller's "block is about to upload" hook
//!     │  → returns SegmentId of the just-sealed segment
//!     │
//!   < upload block to object storage >
//!     │
//!   mark_uploaded(SegmentId)
//!     │  → deletes every segment whose seq ≤ given seq
//! ```
//!
//! On the next process start, [`Wal::replay`] yields every record from
//! every undeleted segment in append order. The first frame whose CRC
//! fails or whose payload is short ends replay for that segment (torn
//! tail).

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use tokio::{
    fs::{self, File, OpenOptions},
    io::{AsyncWriteExt, BufWriter},
};

/// Size of the per-frame header (len: u32 + crc32: u32).
const FRAME_HEADER_SIZE: usize = 8;

/// Default segment size cap. Picked to match the figure in
/// `ARCHITECTURE.md § The WAL` (256 MiB).
pub const DEFAULT_MAX_SEGMENT_BYTES: u64 = 256 * 1024 * 1024;

/// Identifies a WAL segment by its monotonically increasing seq.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SegmentId(pub u64);

/// Opening parameters for [`Wal`].
#[derive(Debug, Clone)]
pub struct WalConfig {
    /// Root WAL directory. The WAL's own subdir is `<dir>/<signal>/`.
    pub dir: PathBuf,
    /// Signal name. Subdirectory under `dir`. For v0.1 this is always
    /// `"dummy"`; the per-signal split lives in the API anyway because
    /// it's free here and required later.
    pub signal: String,
    /// Roll the active segment once it grows beyond this many bytes.
    pub max_segment_bytes: u64,
}

impl WalConfig {
    pub fn new(dir: impl Into<PathBuf>, signal: impl Into<String>) -> Self {
        Self {
            dir: dir.into(),
            signal: signal.into(),
            max_segment_bytes: DEFAULT_MAX_SEGMENT_BYTES,
        }
    }
}

/// Append-only WAL, one per (writer, signal).
///
/// Not internally synchronized. Wrap in `Arc<tokio::sync::Mutex<Wal>>`
/// (or whatever fits your scheduling) if more than one task needs to
/// append concurrently.
pub struct Wal {
    signal_dir: PathBuf,
    /// seq of the currently active (open, writable) segment.
    current_seq: u64,
    /// `BufWriter<File>` around the active segment. Bytes are not yet
    /// fsynced; rotation is the only place we sync.
    current: BufWriter<File>,
    /// Bytes written into the active segment so far (header + payload).
    /// Used by [`Wal::append`] to decide whether to auto-rotate.
    current_bytes: u64,
    max_segment_bytes: u64,
}

impl Wal {
    /// Open (or create) a WAL under `<dir>/<signal>/`. Picks the next
    /// available seq number based on what's already on disk. Does not
    /// replay anything — call [`Wal::replay`] for that.
    pub async fn open(cfg: WalConfig) -> Result<Self> {
        let signal_dir = cfg.dir.join(&cfg.signal);
        fs::create_dir_all(&signal_dir)
            .await
            .with_context(|| format!("creating WAL dir {}", signal_dir.display()))?;

        let mut max_seq: Option<u64> = None;
        let mut rd = fs::read_dir(&signal_dir).await?;
        while let Some(entry) = rd.next_entry().await? {
            let name = entry.file_name();
            if let Some(seq) = parse_segment_filename(&name.to_string_lossy()) {
                max_seq = Some(max_seq.map_or(seq, |m| m.max(seq)));
            }
        }
        let next_seq = max_seq.map_or(0, |m| m + 1);
        let path = segment_path(&signal_dir, next_seq);
        let file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path)
            .await
            .with_context(|| format!("creating WAL segment {}", path.display()))?;

        Ok(Self {
            signal_dir,
            current_seq: next_seq,
            current: BufWriter::with_capacity(64 * 1024, file),
            current_bytes: 0,
            max_segment_bytes: cfg.max_segment_bytes,
        })
    }

    /// Append one opaque payload. The caller chooses the granularity —
    /// scry's noise-sink writes one whole `DummyBatch` payload per
    /// frame, which is also the natural durability unit for an ack.
    ///
    /// Triggers an internal rotation (with fsync) if the active
    /// segment crosses `max_segment_bytes`. The rotation happens after
    /// the frame is written so the per-segment cap is a soft ceiling,
    /// not a hard one.
    pub async fn append(&mut self, payload: &[u8]) -> Result<()> {
        let len: u32 = payload
            .len()
            .try_into()
            .context("WAL payload exceeds u32::MAX")?;
        let crc = crc32fast::hash(payload);
        let header = encode_header(len, crc);
        self.current
            .write_all(&header)
            .await
            .context("WAL: write header")?;
        self.current
            .write_all(payload)
            .await
            .context("WAL: write payload")?;
        self.current_bytes += FRAME_HEADER_SIZE as u64 + payload.len() as u64;

        if self.current_bytes >= self.max_segment_bytes {
            self.rotate().await?;
        }
        Ok(())
    }

    /// Seal the current segment (flush buffer + fsync + close) and
    /// start a fresh active one. Returns the [`SegmentId`] of the
    /// segment that was just sealed.
    ///
    /// After a block is built and successfully uploaded, the caller
    /// passes this id to [`Wal::mark_uploaded`] to release the
    /// segments that fed the block.
    pub async fn rotate(&mut self) -> Result<SegmentId> {
        // Flush the BufWriter so its in-memory bytes hit the OS.
        self.current.flush().await.context("WAL: flush on rotate")?;

        let sealed_seq = self.current_seq;
        let next_seq = sealed_seq + 1;
        let new_path = segment_path(&self.signal_dir, next_seq);
        let new_file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&new_path)
            .await
            .with_context(|| format!("creating WAL segment {}", new_path.display()))?;
        let new_writer = BufWriter::with_capacity(64 * 1024, new_file);

        // Swap, then fsync the sealed file's contents to disk and
        // close it by dropping. fsync after flush is what makes the
        // sealed segment durable — the BufWriter held bytes in
        // userspace, then flush pushed them into the kernel, and
        // sync_all now pushes them onto the platter.
        let old = std::mem::replace(&mut self.current, new_writer);
        let old_file = old.into_inner();
        old_file
            .sync_all()
            .await
            .context("WAL: fsync sealed segment")?;
        drop(old_file);

        self.current_seq = next_seq;
        self.current_bytes = 0;
        tracing::debug!(sealed = sealed_seq, next = next_seq, "WAL rotated");
        Ok(SegmentId(sealed_seq))
    }

    /// Delete every segment whose seq is ≤ `up_to`. Refuses to delete
    /// the active segment (which would lose all subsequent appends
    /// silently); rotate first.
    ///
    /// Convenience wrapper around [`Wal::prepare_release`] +
    /// [`Wal::release_segments`] that does both steps while holding
    /// `&mut self`. The hot ingest path instead calls those two
    /// separately so the slow filesystem work (a `read_dir` plus N
    /// `unlink`s) runs *without* the WAL lock held — see the pipeline's
    /// `run_upload`.
    pub async fn mark_uploaded(&mut self, up_to: SegmentId) -> Result<()> {
        let signal_dir = self.prepare_release(up_to)?;
        Self::release_segments(&signal_dir, up_to).await
    }

    /// Validate that `up_to` is a *sealed* (non-active) segment and
    /// return the signal directory, so the caller can release the
    /// eligible segments with [`Wal::release_segments`] *without*
    /// holding the WAL mutex. Cheap: a single comparison plus a
    /// `PathBuf` clone, so the WAL lock is held only for microseconds
    /// even on the block-close path.
    ///
    /// Refuses the active segment (deleting it would silently drop every
    /// subsequent append); rotate first.
    pub fn prepare_release(&self, up_to: SegmentId) -> Result<PathBuf> {
        if up_to.0 >= self.current_seq {
            anyhow::bail!(
                "WAL: release({}) refers to active segment ({}); rotate first",
                up_to.0,
                self.current_seq
            );
        }
        Ok(self.signal_dir.clone())
    }

    /// Delete every sealed segment in `signal_dir` whose seq is ≤
    /// `up_to`. **No WAL lock required**: sealed segments are immutable
    /// and never reopened by `append`/`rotate` (which only ever touch
    /// the active segment, whose seq is strictly greater than any
    /// releasable one — guaranteed by [`Wal::prepare_release`]). Pair it
    /// with `prepare_release` so a slow directory scan + unlinks never
    /// blocks the foreground append path, which takes the WAL lock under
    /// the pipeline mutex.
    ///
    /// Idempotent: an already-deleted segment is ignored, so two uploads
    /// finishing out of order (each releasing through its own sealed id,
    /// with overlapping ≤-ranges) don't race-fail on the overlap.
    pub async fn release_segments(signal_dir: &Path, up_to: SegmentId) -> Result<()> {
        let mut deleted = 0u64;
        let mut rd = fs::read_dir(signal_dir).await?;
        while let Some(entry) = rd.next_entry().await? {
            let name = entry.file_name();
            if let Some(seq) = parse_segment_filename(&name.to_string_lossy()) {
                if seq <= up_to.0 {
                    let path = entry.path();
                    match fs::remove_file(&path).await {
                        Ok(()) => deleted += 1,
                        // A concurrent out-of-order release already
                        // unlinked it — fine, the goal state is reached.
                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                        Err(e) => {
                            return Err(anyhow::Error::from(e)
                                .context(format!("WAL: removing {}", path.display())));
                        }
                    }
                }
            }
        }
        tracing::debug!(up_to = up_to.0, deleted, "WAL segments deleted");
        Ok(())
    }

    /// Synchronous iterator over every record currently on disk, in
    /// append (per-segment, then per-frame) order.
    ///
    /// Intended for crash recovery on startup: call before any
    /// [`Wal::append`]. The active segment (created by [`Wal::open`])
    /// is empty and is skipped; everything older is replayed.
    ///
    /// Replay stops on the first truncated or CRC-mismatched frame
    /// inside any one segment (the rest of that segment is treated as
    /// a torn tail) and moves on to the next segment.
    pub fn replay(&self) -> Result<ReplayIter> {
        use std::fs as stdfs;
        let mut seqs: Vec<u64> = Vec::new();
        for entry in stdfs::read_dir(&self.signal_dir)? {
            let entry = entry?;
            let name = entry.file_name();
            if let Some(seq) = parse_segment_filename(&name.to_string_lossy()) {
                // The current segment is freshly created and empty at
                // open(); skip it. (If a later append crossed the
                // size cap and rotated, current_seq has advanced and
                // the previously-active segment is now < current_seq
                // and is replay-eligible — but callers should replay
                // before they ever append, so this case shouldn't
                // arise in practice.)
                if seq < self.current_seq {
                    seqs.push(seq);
                }
            }
        }
        seqs.sort_unstable();
        Ok(ReplayIter {
            signal_dir: self.signal_dir.clone(),
            seqs,
            cur_file: None,
            cur_seq: 0,
        })
    }

    /// Current active segment's seq. Mostly useful for tests and
    /// logging — production code rarely needs it.
    pub fn current_segment(&self) -> SegmentId {
        SegmentId(self.current_seq)
    }

    /// Bytes written into the active segment so far.
    pub fn current_bytes(&self) -> u64 {
        self.current_bytes
    }
}

/// Iterator returned by [`Wal::replay`]. See its docs for semantics.
pub struct ReplayIter {
    signal_dir: PathBuf,
    seqs: Vec<u64>,
    cur_file: Option<std::io::BufReader<std::fs::File>>,
    cur_seq: u64,
}

impl Iterator for ReplayIter {
    type Item = Result<Vec<u8>>;

    fn next(&mut self) -> Option<Self::Item> {
        use std::io::Read;
        loop {
            if self.cur_file.is_none() {
                if self.seqs.is_empty() {
                    return None;
                }
                let seq = self.seqs.remove(0);
                self.cur_seq = seq;
                let path = segment_path(&self.signal_dir, seq);
                let f = match std::fs::File::open(&path) {
                    Ok(f) => f,
                    Err(e) => {
                        return Some(Err(anyhow::Error::from(e)
                            .context(format!("WAL replay: opening {}", path.display()))));
                    }
                };
                self.cur_file = Some(std::io::BufReader::with_capacity(64 * 1024, f));
            }
            let f = self.cur_file.as_mut().unwrap();

            let mut hdr = [0u8; FRAME_HEADER_SIZE];
            match f.read_exact(&mut hdr) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    // Clean end of segment.
                    self.cur_file = None;
                    continue;
                }
                Err(e) => return Some(Err(anyhow::Error::from(e))),
            }
            let (len, crc_expected) = decode_header(&hdr);
            let mut buf = vec![0u8; len as usize];
            match f.read_exact(&mut buf) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    tracing::warn!(
                        seq = self.cur_seq,
                        "WAL replay: torn tail (truncated payload), skipping rest of segment"
                    );
                    self.cur_file = None;
                    continue;
                }
                Err(e) => return Some(Err(anyhow::Error::from(e))),
            }
            let crc_actual = crc32fast::hash(&buf);
            if crc_actual != crc_expected {
                tracing::warn!(
                    seq = self.cur_seq,
                    expected = crc_expected,
                    actual = crc_actual,
                    "WAL replay: CRC mismatch, skipping rest of segment"
                );
                self.cur_file = None;
                continue;
            }
            return Some(Ok(buf));
        }
    }
}

fn encode_header(len: u32, crc: u32) -> [u8; FRAME_HEADER_SIZE] {
    let l = len.to_be_bytes();
    let c = crc.to_be_bytes();
    [l[0], l[1], l[2], l[3], c[0], c[1], c[2], c[3]]
}

fn decode_header(hdr: &[u8; FRAME_HEADER_SIZE]) -> (u32, u32) {
    let len = u32::from_be_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]);
    let crc = u32::from_be_bytes([hdr[4], hdr[5], hdr[6], hdr[7]]);
    (len, crc)
}

fn segment_path(dir: &Path, seq: u64) -> PathBuf {
    dir.join(format!("wal-{:020}.log", seq))
}

fn parse_segment_filename(s: &str) -> Option<u64> {
    let s = s.strip_prefix("wal-")?.strip_suffix(".log")?;
    s.parse().ok()
}
