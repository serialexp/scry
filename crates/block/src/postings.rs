//! Shared postings sidecar encode/decode.
//!
//! Metrics and logs blocks both carry a `<uuid>.postings.parquet`
//! inverted index with an **identical** schema — `(label_name Utf8,
//! label_value Utf8, stream_fingerprints List<u64>)`, one row per
//! `(name, value)` pair, sorted by `(name, value)` and each fingerprint
//! list sorted+deduped. The two builders historically each carried a
//! verbatim copy of the encode; this module is the single source of
//! truth they (and the compactor) share. The list column is named
//! `fingerprints` (neutral across metrics' series and logs' streams);
//! readers key on column position, not name.
//!
//! The in-memory shape passed around is
//! `Vec<((String, String), Vec<u64>)>` — the postings entries in output
//! order. [`encode_postings`] serialises it to parquet bytes;
//! [`decode_postings`] reads parquet bytes back to the same shape (used
//! by the compactor to union the postings of the blocks it merges).

use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::{Array, ListArray, StringArray, UInt64Array, UInt64Builder};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

/// One postings entry: a `(label_name, label_value)` key and the sorted,
/// deduped fingerprints that carry it.
pub type PostingsEntry = ((String, String), Vec<u64>);

/// The canonical postings sidecar schema, shared by metrics and logs.
/// The postings cache and the query-side resolver read these columns by
/// position, so the shape must not drift.
pub fn postings_schema() -> SchemaRef {
    let inner = Field::new("item", DataType::UInt64, false);
    Arc::new(Schema::new(vec![
        Field::new("label_name", DataType::Utf8, false),
        Field::new("label_value", DataType::Utf8, false),
        // Neutral column name shared by metrics (series) and logs
        // (streams). The query-side reader and the compactor's decoder
        // both read this column by **position** (column index 2), never
        // by name, so the unified name is invisible to them.
        Field::new("fingerprints", DataType::List(Arc::new(inner)), false),
    ]))
}

/// Serialise postings entries to parquet bytes using `props`.
///
/// `entries` must already be in output order (`(name, value)`
/// lexicographic) with each fingerprint list sorted+deduped — callers
/// build them that way. An empty input still writes a valid empty
/// parquet so the sidecar object is always present when
/// `has_postings = true`; the query path detects empty by row count.
pub fn encode_postings(entries: &[PostingsEntry], props: &WriterProperties) -> Result<Bytes> {
    let schema = postings_schema();
    if entries.is_empty() {
        let empty = RecordBatch::new_empty(schema.clone());
        let mut buf: Vec<u8> = Vec::new();
        let mut w = ArrowWriter::try_new(&mut buf, schema, Some(props.clone()))
            .context("ArrowWriter::try_new (empty postings)")?;
        w.write(&empty)
            .context("ArrowWriter::write (empty postings)")?;
        w.close().context("ArrowWriter::close (empty postings)")?;
        return Ok(Bytes::from(buf));
    }

    let names: StringArray = entries.iter().map(|((k, _), _)| Some(k.as_str())).collect();
    let values: StringArray = entries.iter().map(|((_, v), _)| Some(v.as_str())).collect();

    // The ListArray uses i32 offsets. At the 60M-series architecture
    // ceiling each fingerprint list maxes at ~thousands of u64s and the
    // cumulative offset stays well under i32::MAX. The checked_add below
    // turns any future overflow into a hard panic rather than silent
    // corruption (see the LargeListArray TODO carried from metrics.rs).
    let total_fps: usize = entries.iter().map(|(_, fps)| fps.len()).sum();
    let mut values_builder = UInt64Builder::with_capacity(total_fps);
    let mut offsets: Vec<i32> = Vec::with_capacity(entries.len() + 1);
    let mut running: i32 = 0;
    offsets.push(running);
    for (_, fps) in entries.iter() {
        for &fp in fps {
            values_builder.append_value(fp);
        }
        running = running
            .checked_add(fps.len() as i32)
            .expect("postings offset overflow (i32); see LargeListArray TODO");
        offsets.push(running);
    }
    debug_assert!(running >= 0);
    let values_array = Arc::new(values_builder.finish());
    let offset_buf = OffsetBuffer::new(offsets.into());
    let field = match postings_schema().field(2).data_type() {
        DataType::List(f) => f.clone(),
        other => anyhow::bail!("postings schema column 2 should be List, found {other:?}"),
    };
    let list = ListArray::new(field, offset_buf, values_array, None);
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(names), Arc::new(values), Arc::new(list)],
    )
    .context("constructing postings RecordBatch")?;

    let mut buf: Vec<u8> = Vec::with_capacity(64 * 1024);
    let mut w = ArrowWriter::try_new(&mut buf, schema, Some(props.clone()))
        .context("ArrowWriter::try_new (postings)")?;
    w.write(&batch).context("ArrowWriter::write (postings)")?;
    w.close().context("ArrowWriter::close (postings)")?;
    Ok(Bytes::from(buf))
}

/// Read postings parquet bytes back into `Vec<PostingsEntry>`. Used by
/// the compactor to union the postings of the blocks it merges. The
/// returned entries preserve the file's row order (which is the sorted
/// output order written by [`encode_postings`]); callers that merge
/// across files re-sort the union anyway.
pub fn decode_postings(bytes: Bytes) -> Result<Vec<PostingsEntry>> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
        .context("postings ParquetRecordBatchReaderBuilder")?
        .build()
        .context("postings reader build")?;
    let mut out: Vec<PostingsEntry> = Vec::new();
    for batch in reader {
        let batch = batch.context("reading postings batch")?;
        if batch.num_rows() == 0 {
            continue;
        }
        let names = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .context("postings col 0 not Utf8")?;
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .context("postings col 1 not Utf8")?;
        let lists = batch
            .column(2)
            .as_any()
            .downcast_ref::<ListArray>()
            .context("postings col 2 not List")?;
        for row in 0..batch.num_rows() {
            let name = names.value(row).to_string();
            let value = values.value(row).to_string();
            let fps_arr = lists.value(row);
            let fps = fps_arr
                .as_any()
                .downcast_ref::<UInt64Array>()
                .context("postings fingerprint list not UInt64")?;
            let fingerprints: Vec<u64> = fps.iter().flatten().collect();
            out.push(((name, value), fingerprints));
        }
    }
    Ok(out)
}

/// Union a set of decoded postings (e.g. one per input block in a
/// compaction merge) into a single set of output-ordered entries, each
/// with sorted+deduped fingerprints. This is exactly the shape
/// [`encode_postings`] expects.
pub fn merge_postings(sets: Vec<Vec<PostingsEntry>>) -> Vec<PostingsEntry> {
    use std::collections::HashMap;
    let mut inv: HashMap<(String, String), Vec<u64>> = HashMap::new();
    for set in sets {
        for (key, fps) in set {
            inv.entry(key).or_default().extend(fps);
        }
    }
    let mut entries: Vec<PostingsEntry> = inv.into_iter().collect();
    entries.sort_by(|a, b| a.0.cmp(&b.0));
    for (_, fps) in entries.iter_mut() {
        fps.sort_unstable();
        fps.dedup();
    }
    entries
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::BlockBuilderConfig;

    fn props() -> WriterProperties {
        BlockBuilderConfig::default()
            .postings_writer_props()
            .unwrap()
    }

    #[test]
    fn encode_decode_roundtrip() {
        let entries: Vec<PostingsEntry> = vec![
            (("env".into(), "prod".into()), vec![1, 2, 3]),
            (("service".into(), "api".into()), vec![1, 3]),
            (("service".into(), "web".into()), vec![2]),
        ];
        let bytes = encode_postings(&entries, &props()).unwrap();
        let back = decode_postings(bytes).unwrap();
        assert_eq!(entries, back);
    }

    #[test]
    fn empty_roundtrips_to_empty() {
        let bytes = encode_postings(&[], &props()).unwrap();
        let back = decode_postings(bytes).unwrap();
        assert!(back.is_empty());
    }

    #[test]
    fn merge_unions_and_dedups() {
        // Two blocks sharing a label key with overlapping fingerprints;
        // the merge unions and dedups per key, sorted on output.
        let a: Vec<PostingsEntry> = vec![
            (("env".into(), "prod".into()), vec![1, 5]),
            (("service".into(), "api".into()), vec![5]),
        ];
        let b: Vec<PostingsEntry> = vec![
            (("env".into(), "prod".into()), vec![5, 9]),
            (("service".into(), "web".into()), vec![2]),
        ];
        let merged = merge_postings(vec![a, b]);
        assert_eq!(
            merged,
            vec![
                (("env".to_string(), "prod".to_string()), vec![1, 5, 9]),
                (("service".to_string(), "api".to_string()), vec![5]),
                (("service".to_string(), "web".to_string()), vec![2]),
            ]
        );
    }
}
