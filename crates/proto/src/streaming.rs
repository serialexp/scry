//! Streaming decoders that bypass the per-record heap allocations the
//! generated `*::decode_with_decoder` paths perform.
//!
//! The generated `DummyBatch::decode_with_decoder` produces a fully
//! materialised `DummyBatch { records: Vec<DummyRecord> }` where every
//! `DummyRecord` carries an owned `String` (key) and `Vec<u8>` (value).
//! At ingest rates of a few thousand batches per second that's
//! millions of tiny mallocs per second, which glibc's arena keeps
//! around as high-water-mark slack. See `crates/block/src/dummy.rs`
//! and CLAUDE.md § Performance for the broader picture.
//!
//! This module gives callers a way to walk the same wire format
//! without ever materialising the intermediate Vec/String/Vec<u8>:
//! they implement [`DummyAppender`] (typically backed by CSR buffers
//! that absorb each record with `extend_from_slice`), and
//! [`decode_dummy_batch_into`] reads the encoded payload, handing the
//! appender borrowed slices straight out of the source buffer.
//!
//! The wire format is mirrored from the generated `encode_into`; if
//! the schema's representation of `DummyBatch` ever changes, both this
//! file and the generated decoder must be updated in lockstep. The
//! [`tests::streaming_matches_generated`] integration test guards that
//! agreement: it round-trips through both paths and asserts identical
//! results.

use binschema_runtime::{BinSchemaError, BitOrder, BitStreamDecoder};

/// Consumer for streaming `DummyBatch` decode. Each call hands the
/// implementation borrowed slices of the wire payload; if it wants
/// to retain them past the call, it must copy them (this is the
/// entire point — let the implementation copy directly into its
/// destination buffer, with no intermediate `String` / `Vec<u8>`).
pub trait DummyAppender {
    fn append_raw(&mut self, ts_unix_nano: u64, key: &[u8], value: &[u8]);
}

/// Decode a `DummyBatch` payload into `appender`. Returns the record
/// count on success.
///
/// Performance: every per-record allocation that the generated
/// `DummyBatch::decode_with_decoder` performed is gone. The only
/// allocation cost left is the byte-level memcpys the appender does
/// internally (e.g. `extend_from_slice` into a CSR buffer), which we
/// can't avoid because parquet eventually needs the bytes in one
/// contiguous place anyway.
pub fn decode_dummy_batch_into<A: DummyAppender>(
    payload: &[u8],
    appender: &mut A,
) -> Result<usize, BinSchemaError> {
    let mut dec = BitStreamDecoder::new(payload, BitOrder::MsbFirst);
    let count = dec.read_u32_be()? as usize;
    for _ in 0..count {
        let ts = dec.read_u64_be()?;

        let key_len = dec.read_u16_be()? as usize;
        let key_start = dec.position();
        let key_end = key_start
            .checked_add(key_len)
            .ok_or(BinSchemaError::UnexpectedEof)?;
        if key_end > payload.len() {
            return Err(BinSchemaError::UnexpectedEof);
        }
        let key = &payload[key_start..key_end];
        dec.seek(key_end)?;

        let value_len = dec.read_u32_be()? as usize;
        let value_start = dec.position();
        let value_end = value_start
            .checked_add(value_len)
            .ok_or(BinSchemaError::UnexpectedEof)?;
        if value_end > payload.len() {
            return Err(BinSchemaError::UnexpectedEof);
        }
        let value = &payload[value_start..value_end];
        dec.seek(value_end)?;

        appender.append_raw(ts, key, value);
    }
    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generated::{DummyBatch, DummyRecord};

    #[derive(Default, Debug, PartialEq)]
    struct Collected {
        records: Vec<(u64, Vec<u8>, Vec<u8>)>,
    }

    impl DummyAppender for Collected {
        fn append_raw(&mut self, ts: u64, key: &[u8], value: &[u8]) {
            self.records.push((ts, key.to_vec(), value.to_vec()));
        }
    }

    /// Encode a hand-built `DummyBatch` through the generated path,
    /// then decode it through the streaming path, and assert the
    /// streaming consumer sees exactly the same records.
    #[test]
    fn streaming_matches_generated() {
        let batch = DummyBatch {
            records: vec![
                DummyRecord {
                    ts_unix_nano: 100,
                    key: "first".into(),
                    value: vec![1, 2, 3],
                },
                DummyRecord {
                    ts_unix_nano: 200,
                    key: "second-with-a-longer-key".into(),
                    value: (0..255u16).map(|b| b as u8).collect(),
                },
                DummyRecord {
                    // Empty key + empty value — make sure the zero-length
                    // path doesn't trip the bounds check.
                    ts_unix_nano: 300,
                    key: "".into(),
                    value: vec![],
                },
            ],
        };
        let payload = batch.encode().expect("encode");

        let mut collected = Collected::default();
        let n = decode_dummy_batch_into(&payload, &mut collected).expect("decode");
        assert_eq!(n, batch.records.len());
        assert_eq!(collected.records.len(), batch.records.len());
        for (got, want) in collected.records.iter().zip(batch.records.iter()) {
            assert_eq!(got.0, want.ts_unix_nano);
            assert_eq!(&got.1, want.key.as_bytes());
            assert_eq!(&got.2, &want.value);
        }
    }

    /// Empty batch — record_count = 0, no record bytes. Should
    /// produce zero appends and not error.
    #[test]
    fn streaming_handles_empty_batch() {
        let batch = DummyBatch { records: vec![] };
        let payload = batch.encode().expect("encode");
        let mut collected = Collected::default();
        let n = decode_dummy_batch_into(&payload, &mut collected).expect("decode");
        assert_eq!(n, 0);
        assert!(collected.records.is_empty());
    }

    /// Truncated payload — claim N records but cut the bytes short.
    /// Should error, not panic.
    #[test]
    fn streaming_rejects_truncated_payload() {
        let batch = DummyBatch {
            records: vec![DummyRecord {
                ts_unix_nano: 42,
                key: "k".into(),
                value: vec![7, 7, 7],
            }],
        };
        let mut payload = batch.encode().expect("encode");
        let truncated_len = payload.len() - 2;
        payload.truncate(truncated_len);

        let mut collected = Collected::default();
        let result = decode_dummy_batch_into(&payload, &mut collected);
        assert!(result.is_err(), "expected error, got {:?}", result);
    }
}
