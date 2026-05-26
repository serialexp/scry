//! End-to-end exercise for [`scry_wal::Wal`]: write, rotate, replay,
//! cross-restart durability, mark_uploaded.

use scry_wal::{SegmentId, Wal, WalConfig};
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

    // Delete up to seg 1 — segments 0 and 1 should be gone, 2 stays.
    w.mark_uploaded(s1).await.unwrap();
    let records: Vec<Vec<u8>> = w.replay().unwrap().collect::<Result<_, _>>().unwrap();
    assert_eq!(records, vec![b"seg2-record".to_vec()]);
}

#[tokio::test]
async fn mark_uploaded_refuses_active_segment() {
    let tmp = TempDir::new().unwrap();
    let mut w = Wal::open(cfg(&tmp, 1024 * 1024)).await.unwrap();
    w.append(b"x").await.unwrap();
    let active = w.current_segment();
    let err = w.mark_uploaded(active).await.unwrap_err();
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
async fn replay_skips_torn_tail() {
    let tmp = TempDir::new().unwrap();
    {
        let mut w = Wal::open(cfg(&tmp, 1024 * 1024)).await.unwrap();
        w.append(b"good-record").await.unwrap();
        w.rotate().await.unwrap();
    }
    // Hand-corrupt the trailing bytes of seg 0 to simulate a torn
    // tail. Append a partial header (4 bytes — looks like a len, no
    // crc or payload to follow).
    let seg0 = tmp.path().join("dummy").join("wal-00000000000000000000.log");
    {
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new().append(true).open(&seg0).unwrap();
        f.write_all(&[0, 0, 0, 8]).unwrap(); // claims an 8-byte payload, nothing follows
    }
    let w2 = Wal::open(cfg(&tmp, 1024 * 1024)).await.unwrap();
    let records: Vec<Vec<u8>> = w2.replay().unwrap().collect::<Result<_, _>>().unwrap();
    assert_eq!(
        records,
        vec![b"good-record".to_vec()],
        "torn-tail bytes should not fabricate a record"
    );
}
