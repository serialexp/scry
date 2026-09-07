//! End-to-end exercise for [`scry_wal::Wal`]: write, rotate, replay,
//! cross-restart durability, mark_uploaded.

use scry_wal::{SegmentId, Wal, WalConfig, MAX_REPLAY_FRAME_BYTES};
use tempfile::TempDir;

fn cfg(dir: &TempDir, max_bytes: u64) -> WalConfig {
    WalConfig {
        dir: dir.path().to_path_buf(),
        signal: "dummy".into(),
        max_segment_bytes: max_bytes,
    }
}

#[tokio::test]
async fn append_then_replay_within_same_process() {
    let tmp = TempDir::new().unwrap();
    let mut w = Wal::open(cfg(&tmp, 1024 * 1024)).await.unwrap();
    w.append(b"hello").await.unwrap();
    w.append(b"world").await.unwrap();
    let _sealed = w.rotate().await.unwrap();
    w.append(b"trailing-after-rotate").await.unwrap();
    // Replay only sees sealed segments; the still-active one is
    // skipped (caller would also be issuing fresh appends in real
    // usage).
    let records: Vec<Vec<u8>> = w.replay().unwrap().collect::<Result<_, _>>().unwrap();
    assert_eq!(
        records,
        vec![b"hello".to_vec(), b"world".to_vec()],
        "in-process replay sees sealed-segment records only"
    );
}

#[tokio::test]
async fn replay_after_reopen_recovers_everything() {
    let tmp = TempDir::new().unwrap();
    {
        let mut w = Wal::open(cfg(&tmp, 1024 * 1024)).await.unwrap();
        w.append(b"a").await.unwrap();
        w.append(b"b").await.unwrap();
        w.rotate().await.unwrap();
        w.append(b"c").await.unwrap();
        w.rotate().await.unwrap();
        // Dropping w closes the file. The trailing active segment
        // exists but is empty — it should not affect replay.
    }
    let w2 = Wal::open(cfg(&tmp, 1024 * 1024)).await.unwrap();
    let records: Vec<Vec<u8>> = w2.replay().unwrap().collect::<Result<_, _>>().unwrap();
    assert_eq!(records, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
}

#[tokio::test]
async fn mark_uploaded_deletes_eligible_segments() {
    let tmp = TempDir::new().unwrap();
    let mut w = Wal::open(cfg(&tmp, 1024 * 1024)).await.unwrap();
    w.append(b"seg0-record").await.unwrap();
    let s0 = w.rotate().await.unwrap();
    w.append(b"seg1-record").await.unwrap();
    let s1 = w.rotate().await.unwrap();
    w.append(b"seg2-record").await.unwrap();
    let _s2 = w.rotate().await.unwrap();
    assert_eq!(s0, SegmentId(0));
    assert_eq!(s1, SegmentId(1));

    // Release [0, 1] — segments 0 and 1 should be gone, 2 stays.
    w.mark_uploaded(s0, s1).await.unwrap();
    let records: Vec<Vec<u8>> = w.replay().unwrap().collect::<Result<_, _>>().unwrap();
    assert_eq!(records, vec![b"seg2-record".to_vec()]);
}

/// The whole point of range-based release: a block that owns segments
/// `[2, 2]` must not take out segments 0–1, which belong to an earlier
/// block whose upload failed. A cumulative `≤`-release would delete all
/// three and destroy records that exist nowhere else.
#[tokio::test]
async fn release_leaves_segments_below_the_range_intact() {
    let tmp = TempDir::new().unwrap();
    let mut w = Wal::open(cfg(&tmp, 1024 * 1024)).await.unwrap();
    w.append(b"failed-block-seg0").await.unwrap();
    let _s0 = w.rotate().await.unwrap();
    w.append(b"failed-block-seg1").await.unwrap();
    let _s1 = w.rotate().await.unwrap();
    w.append(b"uploaded-block-seg2").await.unwrap();
    let s2 = w.rotate().await.unwrap();
    assert_eq!(s2, SegmentId(2));

    // Only the successfully-uploaded block's own range goes.
    w.mark_uploaded(s2, s2).await.unwrap();

    let records: Vec<Vec<u8>> = w.replay().unwrap().collect::<Result<_, _>>().unwrap();
    assert_eq!(
        records,
        vec![b"failed-block-seg0".to_vec(), b"failed-block-seg1".to_vec()],
        "the failed block's segments must survive for replay"
    );
}

/// Releasing the same range twice (a retried release, or a duplicate
/// call) is a no-op rather than an error.
#[tokio::test]
async fn release_is_idempotent() {
    let tmp = TempDir::new().unwrap();
    let mut w = Wal::open(cfg(&tmp, 1024 * 1024)).await.unwrap();
    w.append(b"seg0").await.unwrap();
    let s0 = w.rotate().await.unwrap();
    w.append(b"seg1").await.unwrap();
    let _s1 = w.rotate().await.unwrap();

    w.mark_uploaded(s0, s0).await.unwrap();
    w.mark_uploaded(s0, s0).await.unwrap();

    let records: Vec<Vec<u8>> = w.replay().unwrap().collect::<Result<_, _>>().unwrap();
    assert_eq!(records, vec![b"seg1".to_vec()]);
}

#[tokio::test]
async fn mark_uploaded_refuses_active_segment() {
    let tmp = TempDir::new().unwrap();
    let mut w = Wal::open(cfg(&tmp, 1024 * 1024)).await.unwrap();
    w.append(b"x").await.unwrap();
    let active = w.current_segment();
    let err = w.mark_uploaded(active, active).await.unwrap_err();
    assert!(
        err.to_string().contains("active segment"),
        "expected refusal mentioning the active segment, got: {err}"
    );
}

#[tokio::test]
async fn auto_rotates_when_segment_exceeds_cap() {
    let tmp = TempDir::new().unwrap();
    // Tiny cap so even small frames trip the rotation.
    let mut w = Wal::open(cfg(&tmp, 32)).await.unwrap();
    // Each frame is 8 (header) + 16 (payload) = 24 bytes. Two of
    // them blow past 32 and force a rotation.
    let payload = vec![0xABu8; 16];
    w.append(&payload).await.unwrap();
    assert_eq!(w.current_segment(), SegmentId(0));
    w.append(&payload).await.unwrap();
    assert_eq!(
        w.current_segment(),
        SegmentId(1),
        "second append should have crossed the cap and rotated"
    );
}

#[tokio::test]
async fn replay_rejects_oversized_header_before_allocating() {
    let tmp = TempDir::new().unwrap();
    {
        let mut w = Wal::open(cfg(&tmp, 1024 * 1024)).await.unwrap();
        w.append(b"before-bad-header").await.unwrap();
        w.rotate().await.unwrap();
        w.append(b"later-segment").await.unwrap();
        w.rotate().await.unwrap();
    }
    let seg0 = tmp
        .path()
        .join("dummy")
        .join("wal-00000000000000000000.log");
    let bytes = std::fs::read(&seg0).unwrap();
    let first_frame_len = 8 + b"before-bad-header".len();
    let mut rewritten = bytes[..first_frame_len].to_vec();
    rewritten.extend_from_slice(&(MAX_REPLAY_FRAME_BYTES + 1).to_be_bytes());
    rewritten.extend_from_slice(&0u32.to_be_bytes());
    std::fs::write(&seg0, rewritten).unwrap();

    let w2 = Wal::open(cfg(&tmp, 1024 * 1024)).await.unwrap();
    let mut replay = w2.replay().unwrap();
    assert_eq!(replay.next().unwrap().unwrap(), b"before-bad-header");
    let error = replay.next().unwrap().unwrap_err();
    assert!(error.to_string().contains("oversized frame"));
    assert!(
        seg0.exists(),
        "corrupt source segment must remain for recovery"
    );
}

#[tokio::test]
async fn append_rejects_payload_above_replay_limit_without_writing() {
    let tmp = TempDir::new().unwrap();
    let mut w = Wal::open(cfg(&tmp, u64::MAX)).await.unwrap();
    let before = w.current_bytes();
    let oversized = vec![0u8; MAX_REPLAY_FRAME_BYTES as usize + 1];
    assert!(w.append(&oversized).await.is_err());
    assert_eq!(w.current_bytes(), before);
}

#[tokio::test]
async fn replay_fails_closed_on_torn_tail() {
    let tmp = TempDir::new().unwrap();
    {
        let mut w = Wal::open(cfg(&tmp, 1024 * 1024)).await.unwrap();
        w.append(b"good-record").await.unwrap();
        w.rotate().await.unwrap();
    }
    // Hand-corrupt the trailing bytes of seg 0 to simulate a torn
    // tail. Append a partial header (4 bytes — looks like a len, no
    // crc or payload to follow).
    let seg0 = tmp
        .path()
        .join("dummy")
        .join("wal-00000000000000000000.log");
    {
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&seg0)
            .unwrap();
        f.write_all(&[0, 0, 0, 8]).unwrap(); // claims an 8-byte payload, nothing follows
    }
    let w2 = Wal::open(cfg(&tmp, 1024 * 1024)).await.unwrap();
    let mut replay = w2.replay().unwrap();
    assert_eq!(replay.next().unwrap().unwrap(), b"good-record");
    let error = replay.next().unwrap().unwrap_err();
    assert!(error.to_string().contains("truncated frame header"));
    assert!(
        seg0.exists(),
        "torn source segment must remain for recovery"
    );
}
