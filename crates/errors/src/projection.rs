//! Frozen Arrow contract and deterministic object names for occurrence projections.
//!
//! Projection data is written before its commit marker. Object-store code is
//! intentionally outside this module; callers must use conditional create for both
//! objects and treat the commit marker as the visibility boundary.

use std::sync::Arc;

use arrow::array::{
    Array, BinaryArray, BinaryBuilder, FixedSizeBinaryArray, FixedSizeBinaryBuilder, UInt16Array,
    UInt32Array, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use sha2::{Digest, Sha256};

use crate::{Occurrence, CANONICAL_VERSION, SCRUB_POLICY_VERSION};
use uuid::Uuid;

pub const OCCURRENCE_PROJECTION_SCHEMA_VERSION: u16 = 1;
pub const OCCURRENCE_PROJECTION_PREFIX: &str = "_scry/errors/v1/occurrences";

/// The contractual, ordered schema of occurrence projection Parquet files.
///
/// IDs are stored as raw bytes rather than formatted strings. `canonical` is the
/// exact OCC1 value and is deliberately retained next to its SHA-256 accelerator.
pub fn occurrence_schema() -> SchemaRef {
    Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("deployment_id", DataType::FixedSizeBinary(16), false),
            Field::new("app_id", DataType::FixedSizeBinary(16), false),
            Field::new("app_identity_sha256", DataType::FixedSizeBinary(32), false),
            Field::new("event_id", DataType::FixedSizeBinary(16), false),
            Field::new("occurred_at_unix_nano", DataType::UInt64, false),
            Field::new("observed_at_unix_nano", DataType::UInt64, true),
            Field::new("received_at_unix_nano", DataType::UInt64, false),
            Field::new("trace_id", DataType::FixedSizeBinary(16), true),
            Field::new("span_id", DataType::FixedSizeBinary(8), true),
            Field::new("trace_flags", DataType::UInt32, false),
            Field::new("canonical_version", DataType::UInt16, false),
            Field::new("scrub_policy_version", DataType::UInt16, false),
            Field::new("canonical_sha256", DataType::FixedSizeBinary(32), false),
            Field::new("canonical", DataType::Binary, false),
            Field::new(
                "source_log_block_uuid",
                DataType::FixedSizeBinary(16),
                false,
            ),
            Field::new("source_row_ordinal", DataType::UInt64, false),
        ],
        [(
            "scry.occurrence_projection.version".to_owned(),
            "1".to_owned(),
        )]
        .into_iter()
        .collect(),
    ))
}

/// Hard bounds applied before allocating Arrow arrays or reading Parquet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProjectionLimits {
    pub max_rows: usize,
    pub max_bytes: usize,
    pub max_canonical_bytes: usize,
    /// Maximum memory retained by decoded owned rows, including the row vector
    /// allocation and every canonical byte vector allocation.
    pub max_decoded_bytes: usize,
}

impl Default for ProjectionLimits {
    fn default() -> Self {
        Self {
            max_rows: 4_096,
            max_bytes: 64 * 1024 * 1024,
            max_canonical_bytes: 1024 * 1024,
            max_decoded_bytes: 64 * 1024 * 1024,
        }
    }
}

/// Borrowed projection row. Its field shape intentionally matches SQLite's
/// `OccurrenceRow`; [`ProjectionRow::as_sqlite_row`] is allocation-free.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProjectionRow<'a> {
    pub deployment_id: &'a [u8],
    pub app_id: &'a [u8],
    pub app_identity_sha256: &'a [u8],
    pub event_id: &'a [u8],
    pub occurred_at_unix_nano: u64,
    pub observed_at_unix_nano: Option<u64>,
    pub received_at_unix_nano: u64,
    pub trace_id: Option<&'a [u8]>,
    pub span_id: Option<&'a [u8]>,
    pub trace_flags: u32,
    pub canonical_version: u16,
    pub scrub_policy_version: u16,
    pub canonical_sha256: &'a [u8],
    pub canonical: &'a [u8],
    pub source_log_block_uuid: &'a [u8],
    pub source_row_ordinal: u64,
}

impl<'a> ProjectionRow<'a> {
    pub fn from_occurrence(
        occurrence: &'a Occurrence,
        occurred_at_unix_nano: u64,
        observed_at_unix_nano: Option<u64>,
        received_at_unix_nano: u64,
        source_log_block_uuid: &'a [u8; 16],
        source_row_ordinal: u64,
    ) -> Self {
        Self {
            deployment_id: occurrence.deployment_id.as_bytes(),
            app_id: &occurrence.app.app_id,
            app_identity_sha256: &occurrence.app.digest,
            event_id: occurrence.event_id.as_bytes(),
            occurred_at_unix_nano,
            observed_at_unix_nano,
            received_at_unix_nano,
            trace_id: occurrence.trace_id.as_ref().map(<[u8; 16]>::as_slice),
            span_id: occurrence.span_id.as_ref().map(<[u8; 8]>::as_slice),
            trace_flags: occurrence.trace_flags,
            canonical_version: CANONICAL_VERSION,
            scrub_policy_version: SCRUB_POLICY_VERSION,
            canonical_sha256: &occurrence.canonical_sha256,
            canonical: &occurrence.canonical,
            source_log_block_uuid,
            source_row_ordinal,
        }
    }

    pub fn as_sqlite_row(self) -> crate::sqlite::OccurrenceRow<'a> {
        crate::sqlite::OccurrenceRow {
            deployment_id: self.deployment_id,
            app_id: self.app_id,
            app_identity_sha256: self.app_identity_sha256,
            event_id: self.event_id,
            occurred_at_unix_nano: self.occurred_at_unix_nano,
            observed_at_unix_nano: self.observed_at_unix_nano,
            received_at_unix_nano: self.received_at_unix_nano,
            trace_id: self.trace_id,
            span_id: self.span_id,
            trace_flags: self.trace_flags,
            canonical_version: self.canonical_version,
            scrub_policy_version: self.scrub_policy_version,
            canonical_sha256: self.canonical_sha256,
            canonical: self.canonical,
            source_log_block_uuid: self.source_log_block_uuid,
            source_row_ordinal: self.source_row_ordinal,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnedProjectionRow {
    pub deployment_id: [u8; 16],
    pub app_id: [u8; 16],
    pub app_identity_sha256: [u8; 32],
    pub event_id: [u8; 16],
    pub occurred_at_unix_nano: u64,
    pub observed_at_unix_nano: Option<u64>,
    pub received_at_unix_nano: u64,
    pub trace_id: Option<[u8; 16]>,
    pub span_id: Option<[u8; 8]>,
    pub trace_flags: u32,
    pub canonical_version: u16,
    pub scrub_policy_version: u16,
    pub canonical_sha256: [u8; 32],
    pub canonical: Vec<u8>,
    pub source_log_block_uuid: [u8; 16],
    pub source_row_ordinal: u64,
}

impl OwnedProjectionRow {
    pub fn as_row(&self) -> ProjectionRow<'_> {
        ProjectionRow {
            deployment_id: &self.deployment_id,
            app_id: &self.app_id,
            app_identity_sha256: &self.app_identity_sha256,
            event_id: &self.event_id,
            occurred_at_unix_nano: self.occurred_at_unix_nano,
            observed_at_unix_nano: self.observed_at_unix_nano,
            received_at_unix_nano: self.received_at_unix_nano,
            trace_id: self.trace_id.as_ref().map(<[u8; 16]>::as_slice),
            span_id: self.span_id.as_ref().map(<[u8; 8]>::as_slice),
            trace_flags: self.trace_flags,
            canonical_version: self.canonical_version,
            scrub_policy_version: self.scrub_policy_version,
            canonical_sha256: &self.canonical_sha256,
            canonical: &self.canonical,
            source_log_block_uuid: &self.source_log_block_uuid,
            source_row_ordinal: self.source_row_ordinal,
        }
    }
}

fn compare_rows(a: &ProjectionRow<'_>, b: &ProjectionRow<'_>) -> std::cmp::Ordering {
    a.source_log_block_uuid
        .cmp(b.source_log_block_uuid)
        .then_with(|| a.source_row_ordinal.cmp(&b.source_row_ordinal))
        .then_with(|| a.event_id.cmp(b.event_id))
        .then_with(|| a.deployment_id.cmp(b.deployment_id))
        .then_with(|| a.app_id.cmp(b.app_id))
        .then_with(|| a.app_identity_sha256.cmp(b.app_identity_sha256))
        .then_with(|| a.occurred_at_unix_nano.cmp(&b.occurred_at_unix_nano))
        .then_with(|| a.observed_at_unix_nano.cmp(&b.observed_at_unix_nano))
        .then_with(|| a.received_at_unix_nano.cmp(&b.received_at_unix_nano))
        .then_with(|| a.trace_id.cmp(&b.trace_id))
        .then_with(|| a.span_id.cmp(&b.span_id))
        .then_with(|| a.trace_flags.cmp(&b.trace_flags))
        .then_with(|| a.canonical_version.cmp(&b.canonical_version))
        .then_with(|| a.scrub_policy_version.cmp(&b.scrub_policy_version))
        .then_with(|| a.canonical_sha256.cmp(b.canonical_sha256))
        .then_with(|| a.canonical.cmp(b.canonical))
}

fn validate_row(row: &ProjectionRow<'_>, limits: ProjectionLimits) -> Result<(), ProjectionError> {
    for (field, actual, expected) in [
        ("deployment_id", row.deployment_id.len(), 16),
        ("app_id", row.app_id.len(), 16),
        ("app_identity_sha256", row.app_identity_sha256.len(), 32),
        ("event_id", row.event_id.len(), 16),
        ("canonical_sha256", row.canonical_sha256.len(), 32),
        ("source_log_block_uuid", row.source_log_block_uuid.len(), 16),
    ] {
        if actual != expected {
            return Err(ProjectionError::InvalidFieldLength {
                field,
                expected,
                actual,
            });
        }
    }
    if let Some(trace_id) = row.trace_id {
        if trace_id.len() != 16 {
            return Err(ProjectionError::InvalidFieldLength {
                field: "trace_id",
                expected: 16,
                actual: trace_id.len(),
            });
        }
    }
    if let Some(span_id) = row.span_id {
        if span_id.len() != 8 {
            return Err(ProjectionError::InvalidFieldLength {
                field: "span_id",
                expected: 8,
                actual: span_id.len(),
            });
        }
    }
    if row.canonical.len() > limits.max_canonical_bytes {
        return Err(ProjectionError::CanonicalLimit {
            actual: row.canonical.len(),
            limit: limits.max_canonical_bytes,
        });
    }
    Ok(())
}

/// Encodes rows in source-location order. Input order therefore cannot affect the
/// projection, and one bounded batch prevents unbounded Arrow writer state.
pub fn encode_occurrences(
    rows: &[ProjectionRow<'_>],
    limits: ProjectionLimits,
) -> Result<Bytes, ProjectionError> {
    if rows.len() > limits.max_rows {
        return Err(ProjectionError::RowLimit {
            actual: rows.len(),
            limit: limits.max_rows,
        });
    }
    let mut order: Vec<usize> = (0..rows.len()).collect();
    order.sort_unstable_by(|&a, &b| compare_rows(&rows[a], &rows[b]));
    let sorted: Vec<_> = order.into_iter().map(|index| rows[index]).collect();
    let batch = occurrence_batch(&sorted, limits)?;
    let mut output = Vec::new();
    {
        let properties = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(&mut output, occurrence_schema(), Some(properties))
            .map_err(|error| ProjectionError::Parquet(error.to_string()))?;
        writer
            .write(&batch)
            .map_err(|error| ProjectionError::Parquet(error.to_string()))?;
        writer
            .close()
            .map_err(|error| ProjectionError::Parquet(error.to_string()))?;
    }
    if output.len() > limits.max_bytes {
        return Err(ProjectionError::ByteLimit {
            actual: output.len(),
            limit: limits.max_bytes,
        });
    }
    Ok(output.into())
}

fn occurrence_batch(
    rows: &[ProjectionRow<'_>],
    limits: ProjectionLimits,
) -> Result<RecordBatch, ProjectionError> {
    let canonical_bytes = rows.iter().try_fold(0usize, |total, row| {
        validate_row(row, limits)?;
        total
            .checked_add(row.canonical.len())
            .ok_or(ProjectionError::ByteLimit {
                actual: usize::MAX,
                limit: limits.max_bytes,
            })
    })?;
    if canonical_bytes > limits.max_bytes {
        return Err(ProjectionError::ByteLimit {
            actual: canonical_bytes,
            limit: limits.max_bytes,
        });
    }

    macro_rules! fixed {
        ($width:expr, $field:ident) => {{
            let mut builder = FixedSizeBinaryBuilder::with_capacity(rows.len(), $width);
            for row in rows {
                builder
                    .append_value(row.$field)
                    .map_err(|error| ProjectionError::Arrow(error.to_string()))?;
            }
            Arc::new(builder.finish()) as Arc<dyn Array>
        }};
    }
    let mut trace_ids = FixedSizeBinaryBuilder::with_capacity(rows.len(), 16);
    let mut span_ids = FixedSizeBinaryBuilder::with_capacity(rows.len(), 8);
    let mut canonical = BinaryBuilder::with_capacity(rows.len(), canonical_bytes);
    for row in rows {
        if let Some(value) = row.trace_id {
            trace_ids
                .append_value(value)
                .map_err(|error| ProjectionError::Arrow(error.to_string()))?;
        } else {
            trace_ids.append_null();
        }
        if let Some(value) = row.span_id {
            span_ids
                .append_value(value)
                .map_err(|error| ProjectionError::Arrow(error.to_string()))?;
        } else {
            span_ids.append_null();
        }
        canonical.append_value(row.canonical);
    }
    RecordBatch::try_new(
        occurrence_schema(),
        vec![
            fixed!(16, deployment_id),
            fixed!(16, app_id),
            fixed!(32, app_identity_sha256),
            fixed!(16, event_id),
            Arc::new(UInt64Array::from_iter_values(
                rows.iter().map(|row| row.occurred_at_unix_nano),
            )),
            Arc::new(UInt64Array::from_iter(
                rows.iter().map(|row| row.observed_at_unix_nano),
            )),
            Arc::new(UInt64Array::from_iter_values(
                rows.iter().map(|row| row.received_at_unix_nano),
            )),
            Arc::new(trace_ids.finish()),
            Arc::new(span_ids.finish()),
            Arc::new(UInt32Array::from_iter_values(
                rows.iter().map(|row| row.trace_flags),
            )),
            Arc::new(UInt16Array::from_iter_values(
                rows.iter().map(|row| row.canonical_version),
            )),
            Arc::new(UInt16Array::from_iter_values(
                rows.iter().map(|row| row.scrub_policy_version),
            )),
            fixed!(32, canonical_sha256),
            Arc::new(canonical.finish()),
            fixed!(16, source_log_block_uuid),
            Arc::new(UInt64Array::from_iter_values(
                rows.iter().map(|row| row.source_row_ordinal),
            )),
        ],
    )
    .map_err(|error| ProjectionError::Arrow(error.to_string()))
}

fn fields_match(actual: &Schema) -> bool {
    actual.fields() == occurrence_schema().fields()
}

fn schema_matches(actual: &Schema) -> bool {
    fields_match(actual)
        && actual
            .metadata()
            .get("scry.occurrence_projection.version")
            .map(String::as_str)
            == Some("1")
}

/// Decodes an already materialized Parquet object after enforcing byte and row
/// limits. The exact frozen Arrow fields and projection-version metadata are required.
pub fn decode_occurrences(
    bytes: Bytes,
    limits: ProjectionLimits,
) -> Result<Vec<OwnedProjectionRow>, ProjectionError> {
    if bytes.len() > limits.max_bytes {
        return Err(ProjectionError::ByteLimit {
            actual: bytes.len(),
            limit: limits.max_bytes,
        });
    }
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)
        .map_err(|error| ProjectionError::Parquet(error.to_string()))?;
    if !schema_matches(builder.schema().as_ref()) {
        return Err(ProjectionError::WrongSchema);
    }
    let metadata_rows =
        usize::try_from(builder.metadata().file_metadata().num_rows()).unwrap_or(usize::MAX);
    if metadata_rows > limits.max_rows {
        return Err(ProjectionError::RowLimit {
            actual: metadata_rows,
            limit: limits.max_rows,
        });
    }
    let reader = builder
        .with_batch_size(limits.max_rows.max(1))
        .build()
        .map_err(|error| ProjectionError::Parquet(error.to_string()))?;
    // Capacity is acquired batch by batch only after accounting for both the
    // fixed-width owned rows and their canonical allocations.
    let mut output = Vec::new();
    for batch in reader {
        let batch = batch.map_err(|error| ProjectionError::Parquet(error.to_string()))?;
        // Parquet preserves and validates file-level Arrow metadata on the builder,
        // but does not retain that metadata on yielded batches.
        decode_occurrence_batch_inner(&batch, limits, &mut output, false)?;
    }
    Ok(output)
}

/// Validates and appends one batch carrying the exact frozen Arrow schema.
pub fn decode_occurrence_batch(
    batch: &RecordBatch,
    limits: ProjectionLimits,
    output: &mut Vec<OwnedProjectionRow>,
) -> Result<(), ProjectionError> {
    decode_occurrence_batch_inner(batch, limits, output, true)
}

fn decode_occurrence_batch_inner(
    batch: &RecordBatch,
    limits: ProjectionLimits,
    output: &mut Vec<OwnedProjectionRow>,
    require_metadata: bool,
) -> Result<(), ProjectionError> {
    if if require_metadata {
        !schema_matches(batch.schema().as_ref())
    } else {
        !fields_match(batch.schema().as_ref())
    } {
        return Err(ProjectionError::WrongSchema);
    }
    let total = output.len().saturating_add(batch.num_rows());
    if total > limits.max_rows {
        return Err(ProjectionError::RowLimit {
            actual: total,
            limit: limits.max_rows,
        });
    }
    let batch_bytes = batch.get_array_memory_size();
    if batch_bytes > limits.max_bytes {
        return Err(ProjectionError::ByteLimit {
            actual: batch_bytes,
            limit: limits.max_bytes,
        });
    }
    macro_rules! column {
        ($index:expr, $type:ty) => {
            batch
                .column($index)
                .as_any()
                .downcast_ref::<$type>()
                .ok_or(ProjectionError::WrongSchema)?
        };
    }
    let deployment = column!(0, FixedSizeBinaryArray);
    let app = column!(1, FixedSizeBinaryArray);
    let app_digest = column!(2, FixedSizeBinaryArray);
    let event = column!(3, FixedSizeBinaryArray);
    let occurred = column!(4, UInt64Array);
    let observed = column!(5, UInt64Array);
    let received = column!(6, UInt64Array);
    let trace = column!(7, FixedSizeBinaryArray);
    let span = column!(8, FixedSizeBinaryArray);
    let flags = column!(9, UInt32Array);
    let canonical_version = column!(10, UInt16Array);
    let scrub_version = column!(11, UInt16Array);
    let canonical_digest = column!(12, FixedSizeBinaryArray);
    let canonical = column!(13, BinaryArray);
    let source = column!(14, FixedSizeBinaryArray);
    let ordinal = column!(15, UInt64Array);
    let mut new_canonical_bytes = 0usize;
    for index in 0..batch.num_rows() {
        let actual = canonical.value_length(index) as usize;
        if actual > limits.max_canonical_bytes {
            return Err(ProjectionError::CanonicalLimit {
                actual,
                limit: limits.max_canonical_bytes,
            });
        }
        new_canonical_bytes =
            new_canonical_bytes
                .checked_add(actual)
                .ok_or(ProjectionError::DecodedByteLimit {
                    actual: usize::MAX,
                    limit: limits.max_decoded_bytes,
                })?;
    }

    // Validate the aggregate retained allocation before copying canonical data.
    // The output Vec's capacity, rather than its length, is memory that remains
    // live, as is each canonical Vec's capacity.
    let existing_canonical_bytes = output.iter().try_fold(0usize, |total, row| {
        total
            .checked_add(row.canonical.capacity())
            .ok_or(ProjectionError::DecodedByteLimit {
                actual: usize::MAX,
                limit: limits.max_decoded_bytes,
            })
    })?;
    let projected_row_capacity = output.capacity().max(total);
    let projected_bytes = projected_row_capacity
        .checked_mul(std::mem::size_of::<OwnedProjectionRow>())
        .and_then(|bytes| bytes.checked_add(existing_canonical_bytes))
        .and_then(|bytes| bytes.checked_add(new_canonical_bytes))
        .ok_or(ProjectionError::DecodedByteLimit {
            actual: usize::MAX,
            limit: limits.max_decoded_bytes,
        })?;
    if projected_bytes > limits.max_decoded_bytes {
        return Err(ProjectionError::DecodedByteLimit {
            actual: projected_bytes,
            limit: limits.max_decoded_bytes,
        });
    }

    // Stage the whole batch so a validation or allocation failure never appends
    // a prefix to the caller's output.
    let mut pending = Vec::with_capacity(batch.num_rows());
    for index in 0..batch.num_rows() {
        pending.push(OwnedProjectionRow {
            deployment_id: copy_array(deployment.value(index)),
            app_id: copy_array(app.value(index)),
            app_identity_sha256: copy_array(app_digest.value(index)),
            event_id: copy_array(event.value(index)),
            occurred_at_unix_nano: occurred.value(index),
            observed_at_unix_nano: (!observed.is_null(index)).then(|| observed.value(index)),
            received_at_unix_nano: received.value(index),
            trace_id: (!trace.is_null(index)).then(|| copy_array(trace.value(index))),
            span_id: (!span.is_null(index)).then(|| copy_array(span.value(index))),
            trace_flags: flags.value(index),
            canonical_version: canonical_version.value(index),
            scrub_policy_version: scrub_version.value(index),
            canonical_sha256: copy_array(canonical_digest.value(index)),
            canonical: canonical.value(index).to_vec(),
            source_log_block_uuid: copy_array(source.value(index)),
            source_row_ordinal: ordinal.value(index),
        });
    }
    output
        .try_reserve_exact(batch.num_rows())
        .map_err(|_| ProjectionError::DecodedByteLimit {
            actual: usize::MAX,
            limit: limits.max_decoded_bytes,
        })?;
    let actual_bytes = output
        .capacity()
        .checked_mul(std::mem::size_of::<OwnedProjectionRow>())
        .and_then(|bytes| bytes.checked_add(existing_canonical_bytes))
        .and_then(|bytes| {
            pending.iter().try_fold(bytes, |total, row| {
                total.checked_add(row.canonical.capacity())
            })
        })
        .unwrap_or(usize::MAX);
    if actual_bytes > limits.max_decoded_bytes {
        return Err(ProjectionError::DecodedByteLimit {
            actual: actual_bytes,
            limit: limits.max_decoded_bytes,
        });
    }
    output.extend(pending);
    Ok(())
}

fn copy_array<const N: usize>(value: &[u8]) -> [u8; N] {
    value.try_into().expect("exact schema fixes binary width")
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectionKeys {
    pub data: String,
    pub commit: String,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ProjectionError {
    #[error("date must be a canonical YYYY-MM-DD UTC date")]
    InvalidDate,
    #[error("extractor generation must contain only lowercase ASCII letters, digits, `-`, or `_`")]
    InvalidGeneration,
    #[error("object key must be a relative canonical key below `_scry/errors/`")]
    InvalidObjectKey,
    #[error("{field} has length {actual}; expected {expected}")]
    InvalidFieldLength {
        field: &'static str,
        expected: usize,
        actual: usize,
    },
    #[error("projection has {actual} rows, exceeding limit {limit}")]
    RowLimit { actual: usize, limit: usize },
    #[error("projection has {actual} bytes, exceeding limit {limit}")]
    ByteLimit { actual: usize, limit: usize },
    #[error("decoded projection retains {actual} bytes, exceeding limit {limit}")]
    DecodedByteLimit { actual: usize, limit: usize },
    #[error("canonical value has {actual} bytes, exceeding limit {limit}")]
    CanonicalLimit { actual: usize, limit: usize },
    #[error("projection does not have the frozen occurrence schema")]
    WrongSchema,
    #[error("Arrow error: {0}")]
    Arrow(String),
    #[error("Parquet error: {0}")]
    Parquet(String),
}

/// Builds the stable metadata-last object names for one source block.
pub fn occurrence_keys(
    date: &str,
    source_log_block_uuid: Uuid,
    extractor_generation: &str,
) -> Result<ProjectionKeys, ProjectionError> {
    validate_date(date)?;
    if extractor_generation.is_empty()
        || extractor_generation.len() > 128
        || !extractor_generation
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'-' | b'_'))
    {
        return Err(ProjectionError::InvalidGeneration);
    }
    let stem = format!(
        "{OCCURRENCE_PROJECTION_PREFIX}/{date}/{source_log_block_uuid}/{extractor_generation}"
    );
    Ok(ProjectionKeys {
        data: format!("{stem}.parquet"),
        commit: format!("{stem}.commit.json"),
    })
}

fn validate_date(date: &str) -> Result<(), ProjectionError> {
    let bytes = date.as_bytes();
    if bytes.len() != 10
        || bytes[4] != b'-'
        || bytes[7] != b'-'
        || bytes
            .iter()
            .enumerate()
            .any(|(i, b)| !matches!(i, 4 | 7) && !b.is_ascii_digit())
    {
        return Err(ProjectionError::InvalidDate);
    }
    let year = date[0..4]
        .parse::<u16>()
        .map_err(|_| ProjectionError::InvalidDate)?;
    let month = date[5..7]
        .parse::<u8>()
        .map_err(|_| ProjectionError::InvalidDate)?;
    let day = date[8..10]
        .parse::<u8>()
        .map_err(|_| ProjectionError::InvalidDate)?;
    let leap = year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400));
    let days = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if leap => 29,
        2 => 28,
        _ => return Err(ProjectionError::InvalidDate),
    };
    if day == 0 || day > days {
        return Err(ProjectionError::InvalidDate);
    }
    Ok(())
}

/// Small authoritative marker written after the Parquet object.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct OccurrenceCommit {
    pub schema_version: u16,
    pub source_log_block_uuid: Uuid,
    pub extractor_generation: String,
    pub data_key: String,
    pub data_sha256: String,
    pub data_size_bytes: u64,
    pub row_count: u64,
    pub committed_at_unix_nano: u64,
}

impl OccurrenceCommit {
    pub fn new(
        source_log_block_uuid: Uuid,
        extractor_generation: impl Into<String>,
        data_key: impl Into<String>,
        data_bytes: &[u8],
        row_count: u64,
        committed_at_unix_nano: u64,
    ) -> Result<Self, ProjectionError> {
        let extractor_generation = extractor_generation.into();
        // Reuse key validation rather than maintaining a second generation grammar.
        occurrence_keys("2000-01-01", source_log_block_uuid, &extractor_generation)?;
        let data_key = data_key.into();
        validate_data_key(&data_key)?;
        Ok(Self {
            schema_version: OCCURRENCE_PROJECTION_SCHEMA_VERSION,
            source_log_block_uuid,
            extractor_generation,
            data_key,
            data_sha256: hex_sha256(data_bytes),
            data_size_bytes: data_bytes.len() as u64,
            row_count,
            committed_at_unix_nano,
        })
    }

    /// Stable JSON bytes. Struct field order is contractual and no maps/floats occur.
    pub fn canonical_json(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }

    pub fn sha256(&self) -> Result<[u8; 32], serde_json::Error> {
        Ok(Sha256::digest(self.canonical_json()?).into())
    }
}

fn validate_data_key(key: &str) -> Result<(), ProjectionError> {
    if !key.starts_with(&format!("{OCCURRENCE_PROJECTION_PREFIX}/"))
        || !key.ends_with(".parquet")
        || key.starts_with('/')
        || key
            .split('/')
            .any(|part| part.is_empty() || matches!(part, "." | ".."))
        || key.bytes().any(|b| b.is_ascii_control() || b == b'\\')
    {
        return Err(ProjectionError::InvalidObjectKey);
    }
    Ok(())
}

fn hex_sha256(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut out = String::with_capacity(64);
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in digest {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_is_frozen_in_name_type_and_nullability() {
        let schema = occurrence_schema();
        let actual: Vec<_> = schema
            .fields()
            .iter()
            .map(|field| {
                (
                    field.name().as_str(),
                    field.data_type().clone(),
                    field.is_nullable(),
                )
            })
            .collect();
        assert_eq!(actual.len(), 16);
        assert_eq!(
            actual[0],
            ("deployment_id", DataType::FixedSizeBinary(16), false)
        );
        assert_eq!(
            actual[12],
            ("canonical_sha256", DataType::FixedSizeBinary(32), false)
        );
        assert_eq!(actual[13], ("canonical", DataType::Binary, false));
        assert_eq!(actual[15], ("source_row_ordinal", DataType::UInt64, false));
        assert_eq!(schema.metadata()["scry.occurrence_projection.version"], "1");
    }

    #[test]
    fn keys_and_commit_bytes_are_deterministic() {
        let source = Uuid::parse_str("01234567-89ab-cdef-8123-456789abcdef").unwrap();
        let keys = occurrence_keys("2026-09-10", source, "extractor-v1").unwrap();
        assert_eq!(
            keys.data,
            "_scry/errors/v1/occurrences/2026-09-10/01234567-89ab-cdef-8123-456789abcdef/extractor-v1.parquet"
        );
        let a =
            OccurrenceCommit::new(source, "extractor-v1", &keys.data, b"parquet", 2, 9).unwrap();
        let b = OccurrenceCommit::new(source, "extractor-v1", keys.data, b"parquet", 2, 9).unwrap();
        assert_eq!(a.canonical_json().unwrap(), b.canonical_json().unwrap());
        assert_eq!(a.sha256().unwrap(), b.sha256().unwrap());
    }

    fn fixture(ordinal: u64) -> OwnedProjectionRow {
        OwnedProjectionRow {
            deployment_id: [1; 16],
            app_id: [2; 16],
            app_identity_sha256: [3; 32],
            event_id: [ordinal as u8; 16],
            occurred_at_unix_nano: 10 + ordinal,
            observed_at_unix_nano: ordinal.is_multiple_of(2).then_some(20 + ordinal),
            received_at_unix_nano: 30 + ordinal,
            trace_id: ordinal.is_multiple_of(2).then_some([4; 16]),
            span_id: None,
            trace_flags: 1,
            canonical_version: 1,
            scrub_policy_version: 1,
            canonical_sha256: [5; 32],
            canonical: vec![6, ordinal as u8],
            source_log_block_uuid: [7; 16],
            source_row_ordinal: ordinal,
        }
    }

    #[test]
    fn parquet_roundtrip_is_sorted_and_input_order_deterministic() {
        let a = fixture(2);
        let b = fixture(1);
        let limits = ProjectionLimits::default();
        let forward = encode_occurrences(&[a.as_row(), b.as_row()], limits).unwrap();
        let reverse = encode_occurrences(&[b.as_row(), a.as_row()], limits).unwrap();
        assert_eq!(forward, reverse);
        assert_eq!(decode_occurrences(forward, limits).unwrap(), vec![b, a]);
    }

    #[test]
    fn rejects_malformed_wrong_schema_and_limits() {
        let limits = ProjectionLimits::default();
        assert!(matches!(
            decode_occurrences(Bytes::from_static(b"not parquet"), limits),
            Err(ProjectionError::Parquet(_))
        ));

        let wrong = RecordBatch::try_from_iter([(
            "value",
            Arc::new(UInt64Array::from(vec![1])) as Arc<dyn Array>,
        )])
        .unwrap();
        assert_eq!(
            decode_occurrence_batch(&wrong, limits, &mut Vec::new()),
            Err(ProjectionError::WrongSchema)
        );

        let row = fixture(1);
        assert_eq!(
            encode_occurrences(
                &[row.as_row()],
                ProjectionLimits {
                    max_rows: 0,
                    ..limits
                }
            ),
            Err(ProjectionError::RowLimit {
                actual: 1,
                limit: 0
            })
        );
        let bytes = encode_occurrences(&[row.as_row()], limits).unwrap();
        assert!(matches!(
            decode_occurrences(
                bytes,
                ProjectionLimits {
                    max_rows: 0,
                    ..limits
                }
            ),
            Err(ProjectionError::RowLimit { .. })
        ));
        assert!(matches!(
            encode_occurrences(
                &[row.as_row()],
                ProjectionLimits {
                    max_canonical_bytes: 1,
                    ..limits
                }
            ),
            Err(ProjectionError::CanonicalLimit { .. })
        ));
        assert!(matches!(
            decode_occurrences(
                encode_occurrences(&[row.as_row()], limits).unwrap(),
                ProjectionLimits {
                    max_bytes: 1,
                    ..limits
                }
            ),
            Err(ProjectionError::ByteLimit { .. })
        ));
    }

    #[test]
    fn rejects_aggregate_decoded_memory_across_compressed_row_groups() {
        let mut rows = [fixture(1), fixture(2), fixture(3)];
        for row in &mut rows {
            row.canonical = vec![b'x'; 256 * 1024];
        }
        let borrowed: Vec<_> = rows.iter().map(OwnedProjectionRow::as_row).collect();
        let batch = occurrence_batch(&borrowed, ProjectionLimits::default()).unwrap();
        let mut parquet = Vec::new();
        {
            let properties = WriterProperties::builder()
                .set_max_row_group_row_count(Some(1))
                .build();
            let mut writer =
                ArrowWriter::try_new(&mut parquet, occurrence_schema(), Some(properties)).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
        }

        let limit = std::mem::size_of::<OwnedProjectionRow>() * rows.len() + 32 * 1024;
        let result = decode_occurrences(
            parquet.into(),
            ProjectionLimits {
                max_rows: rows.len(),
                max_decoded_bytes: limit,
                ..ProjectionLimits::default()
            },
        );
        assert!(matches!(
            result,
            Err(ProjectionError::DecodedByteLimit {
                actual,
                limit: actual_limit
            }) if actual > actual_limit && actual_limit == limit
        ));
    }

    #[test]
    fn rejects_noncanonical_segments_and_impossible_dates() {
        let source = Uuid::nil();
        assert_eq!(
            occurrence_keys("2025-02-29", source, "v1"),
            Err(ProjectionError::InvalidDate)
        );
        assert_eq!(
            occurrence_keys("2024-02-29", source, "../v1"),
            Err(ProjectionError::InvalidGeneration)
        );
    }
}
