//! Block builder for trace spans — the v0.5 third real signal.
//!
//! Per the v0.5/v0.6 storage plan locked with Bart, a traces block is
//! **one parquet row per span**, with full fidelity: scalar span fields
//! as typed columns, span/resource/scope ids denormalised onto each row
//! at decode time, attributes as `Map<Utf8,Utf8>`, and the per-span
//! `events[]`/`links[]` as *native nested* `List<Struct<…>>` columns
//! (each carrying its own nested attribute `Map`). Storing the nested
//! shape natively means the query phase (trace-by-id lookup, v0.5; and
//! whatever span-analytics follow) never has to re-ingest or reshape the
//! block — the format is laid down once, here.
//!
//! In addition to the full `resource_labels` Map, three OTel discovery
//! axes — `service.name`, `service.namespace`, `deployment.environment`
//! — are **promoted** into dedicated nullable `Utf8` columns
//! (`service_name`/`service_namespace`/`deployment_environment`). They
//! are denormalised *copies* (the originals stay in the Map), there to
//! give the query phase a plain-column predicate for service-scoped
//! discovery ("root spans for service X") instead of a Map dig. They do
//! not enable row-group pruning — the block is sorted by `trace_id`, so
//! service values are scattered across every row group — but they are
//! the clean key a future service index/clustered variant would build
//! on, and baking them in now avoids a block rewrite later. Reaching
//! spans by a non-id axis (service, error, duration) still ultimately
//! wants a secondary index (postings-style or catalog-level), which is
//! query-phase work and fully backfillable from these columns.
//!
//! - `<block>.parquet` — sorted by `(trace_id, start_unix_nano)`. The
//!   intra-block sort makes parquet row-group min/max stats on the
//!   `trace_id` column the trace-by-id pruning lever the query phase
//!   will lean on — the same row-group-skipping trick metrics/logs use
//!   on their fingerprint column, no inverted index needed (per D-025 +
//!   the v0.6 storage plan).
//! - `<block>.meta.json` — `has_postings:false`, no postings file.
//!
//! Wire input is `TracesBatch { resources[], scopes[], spans[] }` (see
//! `scry_proto::generated`); the streaming decoder resolves each span's
//! `resource_idx`/`scope_idx` against the per-batch dictionaries and
//! hands the appender self-contained [`DecodedSpan`]s, so `merge` never
//! has to carry a cross-batch dictionary. Spans are mid-volume; the
//! per-span owned `Vec`s for attributes/events/links are fine on this
//! cold-ish path (≈0–1 events and ≈1 link per span typically) — same
//! owned-on-cold-path tradeoff as metrics/logs labels.

use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::{
    ArrayRef, FixedSizeBinaryBuilder, ListArray, MapBuilder, StringArray, StringBuilder,
    StructArray, UInt64Array, UInt8Array,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use object_store::{path::Path, ObjectStore, ObjectStoreExt};
use parquet::arrow::ArrowWriter;
use scry_proto::streaming::{DecodedSpan, TracesAppender};
use uuid::Uuid;

use crate::{block_path, BlockBuilder, BlockBuilderConfig, BlockMeta, EncodedBlock};

const SIGNAL: &str = "traces";
const SCHEMA_VERSION: u32 = 1;

/// Resource-attribute keys promoted to dedicated top-level nullable
/// `Utf8` columns at encode time, so the query phase can scope discovery
/// queries ("root spans for service X") by a plain column predicate
/// instead of digging into the `resource_labels` Map. Each key list is
/// tried in order and the first hit wins — `deployment.environment` was
/// renamed to `deployment.environment.name` in newer OTel semantic
/// conventions, so we accept either. The promoted values are *copies*;
/// the originals stay in `resource_labels` for full fidelity.
const PROMOTED_SERVICE_NAME_KEYS: &[&str] = &["service.name"];
const PROMOTED_SERVICE_NAMESPACE_KEYS: &[&str] = &["service.namespace"];
const PROMOTED_DEPLOYMENT_ENV_KEYS: &[&str] =
    &["deployment.environment", "deployment.environment.name"];

/// First-match lookup of a promoted resource attribute. Resource label
/// sets are tiny (a handful of pairs), so the linear scan is cheaper than
/// building a map on this cold path.
fn lookup_label(labels: &[(String, String)], keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|key| {
        labels
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.clone())
    })
}

/// A span event held in column-shaped owned form between decode and
/// encode. Cold path (~0–1 per span) so an owned `Vec` of these per span
/// is fine.
struct OwnedEvent {
    ts_unix_nano: u64,
    name: String,
    attrs: Vec<(String, String)>,
}

/// A span link held in owned form between decode and encode.
struct OwnedLink {
    trace_id: [u8; 16],
    span_id: [u8; 8],
    attrs: Vec<(String, String)>,
}

/// In-memory traces block under construction. One parallel `Vec` per
/// physical parquet column (the nested `events`/`links` columns keep a
/// `Vec` of owned child rows per span).
pub struct TracesBlockBuilder {
    writer_id: Uuid,
    cfg: BlockBuilderConfig,
    trace_ids: Vec<[u8; 16]>,
    span_ids: Vec<[u8; 8]>,
    parent_span_ids: Vec<Option<[u8; 8]>>,
    resource_labels: Vec<Vec<(String, String)>>,
    // Denormalised copies of the three OTel discovery axes, promoted out
    // of `resource_labels` into dedicated nullable columns (the values
    // stay in the Map too — these are a query convenience, not a
    // replacement). See `PROMOTED_*_KEYS` for the source keys.
    service_names: Vec<Option<String>>,
    service_namespaces: Vec<Option<String>>,
    deployment_envs: Vec<Option<String>>,
    scope_names: Vec<String>,
    scope_versions: Vec<String>,
    names: Vec<String>,
    kinds: Vec<u8>,
    starts: Vec<u64>,
    ends: Vec<u64>,
    status_codes: Vec<u8>,
    status_messages: Vec<String>,
    attributes: Vec<Vec<(String, String)>>,
    events: Vec<Vec<OwnedEvent>>,
    links: Vec<Vec<OwnedLink>>,
    bytes_est: u64,
    ts_min: u64,
    ts_max: u64,
}

/// `Map<Utf8,Utf8>` in the canonical `MapBuilder` layout (field names
/// "entries"/"keys"/"values"; `values` nullable to match the builder's
/// output type exactly). Shared by every attribute map in the schema —
/// the top-level `resource_labels`/`attributes` columns and the nested
/// attribute maps inside `events`/`links` — so the constructed arrays
/// and the declared schema agree byte-for-byte.
fn map_datatype() -> DataType {
    let entries_field = Arc::new(Field::new(
        "entries",
        DataType::Struct(Fields::from(vec![
            Field::new("keys", DataType::Utf8, false),
            Field::new("values", DataType::Utf8, true),
        ])),
        false,
    ));
    DataType::Map(entries_field, /*keys_sorted=*/ false)
}

/// Struct fields for one `events[]` element.
fn event_struct_fields() -> Fields {
    Fields::from(vec![
        Field::new("ts_unix_nano", DataType::UInt64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("attributes", map_datatype(), false),
    ])
}

/// `List` item field for the `events` column.
fn event_item_field() -> Arc<Field> {
    Arc::new(Field::new(
        "item",
        DataType::Struct(event_struct_fields()),
        true,
    ))
}

/// Struct fields for one `links[]` element.
fn link_struct_fields() -> Fields {
    Fields::from(vec![
        Field::new("trace_id", DataType::FixedSizeBinary(16), false),
        Field::new("span_id", DataType::FixedSizeBinary(8), false),
        Field::new("attributes", map_datatype(), false),
    ])
}

/// `List` item field for the `links` column.
fn link_item_field() -> Arc<Field> {
    Arc::new(Field::new(
        "item",
        DataType::Struct(link_struct_fields()),
        true,
    ))
}

impl TracesBlockBuilder {
    pub fn main_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("trace_id", DataType::FixedSizeBinary(16), false),
            Field::new("span_id", DataType::FixedSizeBinary(8), false),
            Field::new("parent_span_id", DataType::FixedSizeBinary(8), true),
            Field::new("resource_labels", map_datatype(), false),
            // Promoted resource attributes (nullable copies of the
            // matching `resource_labels` entries; null when absent).
            Field::new("service_name", DataType::Utf8, true),
            Field::new("service_namespace", DataType::Utf8, true),
            Field::new("deployment_environment", DataType::Utf8, true),
            Field::new("scope_name", DataType::Utf8, false),
            Field::new("scope_version", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("kind", DataType::UInt8, false),
            Field::new("start_unix_nano", DataType::UInt64, false),
            Field::new("end_unix_nano", DataType::UInt64, false),
            Field::new("status_code", DataType::UInt8, false),
            Field::new("status_message", DataType::Utf8, false),
            Field::new("attributes", map_datatype(), false),
            Field::new("events", DataType::List(event_item_field()), false),
            Field::new("links", DataType::List(link_item_field()), false),
        ]))
    }

    pub fn row_count(&self) -> u64 {
        self.trace_ids.len() as u64
    }
}

/// Coerce wire label bytes (`Vec<(Vec<u8>,Vec<u8>)>`) to owned UTF-8
/// pairs, lossy on bad input — a misbehaving agent shouldn't poison the
/// block. Same policy as logs/metrics/profiles.
fn coerce_labels(labels: &[(Vec<u8>, Vec<u8>)]) -> Vec<(String, String)> {
    labels
        .iter()
        .map(|(k, v)| {
            (
                String::from_utf8_lossy(k).into_owned(),
                String::from_utf8_lossy(v).into_owned(),
            )
        })
        .collect()
}

fn labels_bytes(labels: &[(String, String)]) -> usize {
    labels.iter().map(|(k, v)| k.len() + v.len()).sum()
}

impl TracesAppender for TracesBlockBuilder {
    fn append_span(&mut self, span: &DecodedSpan<'_>) {
        self.ts_min = self.ts_min.min(span.start_unix_nano);
        self.ts_max = self.ts_max.max(span.start_unix_nano);

        // The decoder guarantees fixed widths (16/8/8) via `read_fixed`.
        let trace_id: [u8; 16] = span
            .trace_id
            .try_into()
            .expect("decoder guarantees trace_id is 16 bytes");
        let span_id: [u8; 8] = span
            .span_id
            .try_into()
            .expect("decoder guarantees span_id is 8 bytes");
        let parent_span_id: Option<[u8; 8]> = span.parent_span_id.map(|p| {
            p.try_into()
                .expect("decoder guarantees parent_span_id is 8 bytes")
        });

        let resource_labels = coerce_labels(span.resource_labels);
        let service_name = lookup_label(&resource_labels, PROMOTED_SERVICE_NAME_KEYS);
        let service_namespace = lookup_label(&resource_labels, PROMOTED_SERVICE_NAMESPACE_KEYS);
        let deployment_env = lookup_label(&resource_labels, PROMOTED_DEPLOYMENT_ENV_KEYS);
        let scope_name = String::from_utf8_lossy(span.scope_name).into_owned();
        let scope_version = String::from_utf8_lossy(span.scope_version).into_owned();
        let name = String::from_utf8_lossy(span.name).into_owned();
        let status_message = String::from_utf8_lossy(span.status_message).into_owned();
        let attributes = coerce_labels(span.attributes);

        let events: Vec<OwnedEvent> = span
            .events
            .iter()
            .map(|e| OwnedEvent {
                ts_unix_nano: e.ts_unix_nano,
                name: String::from_utf8_lossy(&e.name).into_owned(),
                attrs: coerce_labels(&e.attributes),
            })
            .collect();
        let links: Vec<OwnedLink> = span
            .links
            .iter()
            .map(|l| OwnedLink {
                trace_id: l
                    .trace_id
                    .as_slice()
                    .try_into()
                    .expect("decoder guarantees link trace_id is 16 bytes"),
                span_id: l
                    .span_id
                    .as_slice()
                    .try_into()
                    .expect("decoder guarantees link span_id is 8 bytes"),
                attrs: coerce_labels(&l.attributes),
            })
            .collect();

        // Byte estimate: fixed ids + scalars + the variable-length text
        // and nested children.
        let mut est: u64 = 16 + 8 + 8 + 1 + 8 + 8 + 1; // ids + kind + start + end + status_code
        est += (scope_name.len()
            + scope_version.len()
            + name.len()
            + status_message.len()
            + labels_bytes(&resource_labels)
            + labels_bytes(&attributes)) as u64;
        for e in &events {
            est += 8 + e.name.len() as u64 + labels_bytes(&e.attrs) as u64;
        }
        for l in &links {
            est += 24 + labels_bytes(&l.attrs) as u64;
        }
        self.bytes_est += est;

        self.trace_ids.push(trace_id);
        self.span_ids.push(span_id);
        self.parent_span_ids.push(parent_span_id);
        self.resource_labels.push(resource_labels);
        self.service_names.push(service_name);
        self.service_namespaces.push(service_namespace);
        self.deployment_envs.push(deployment_env);
        self.scope_names.push(scope_name);
        self.scope_versions.push(scope_version);
        self.names.push(name);
        self.kinds.push(span.kind);
        self.starts.push(span.start_unix_nano);
        self.ends.push(span.end_unix_nano);
        self.status_codes.push(span.status_code);
        self.status_messages.push(status_message);
        self.attributes.push(attributes);
        self.events.push(events);
        self.links.push(links);
    }
}

impl BlockBuilder for TracesBlockBuilder {
    const SIGNAL: &'static str = SIGNAL;

    fn new(writer_id: Uuid, cfg: BlockBuilderConfig) -> Self {
        Self {
            writer_id,
            cfg,
            trace_ids: Vec::with_capacity(256),
            span_ids: Vec::with_capacity(256),
            parent_span_ids: Vec::with_capacity(256),
            resource_labels: Vec::with_capacity(256),
            service_names: Vec::with_capacity(256),
            service_namespaces: Vec::with_capacity(256),
            deployment_envs: Vec::with_capacity(256),
            scope_names: Vec::with_capacity(256),
            scope_versions: Vec::with_capacity(256),
            names: Vec::with_capacity(256),
            kinds: Vec::with_capacity(256),
            starts: Vec::with_capacity(256),
            ends: Vec::with_capacity(256),
            status_codes: Vec::with_capacity(256),
            status_messages: Vec::with_capacity(256),
            attributes: Vec::with_capacity(256),
            events: Vec::with_capacity(256),
            links: Vec::with_capacity(256),
            bytes_est: 0,
            ts_min: u64::MAX,
            ts_max: 0,
        }
    }

    fn is_empty(&self) -> bool {
        self.trace_ids.is_empty()
    }

    fn should_close(&self) -> bool {
        self.row_count() >= self.cfg.max_rows || self.bytes_est >= self.cfg.target_bytes
    }

    fn merge(&mut self, other: &mut Self) {
        self.trace_ids.append(&mut other.trace_ids);
        self.span_ids.append(&mut other.span_ids);
        self.parent_span_ids.append(&mut other.parent_span_ids);
        self.resource_labels.append(&mut other.resource_labels);
        self.service_names.append(&mut other.service_names);
        self.service_namespaces
            .append(&mut other.service_namespaces);
        self.deployment_envs.append(&mut other.deployment_envs);
        self.scope_names.append(&mut other.scope_names);
        self.scope_versions.append(&mut other.scope_versions);
        self.names.append(&mut other.names);
        self.kinds.append(&mut other.kinds);
        self.starts.append(&mut other.starts);
        self.ends.append(&mut other.ends);
        self.status_codes.append(&mut other.status_codes);
        self.status_messages.append(&mut other.status_messages);
        self.attributes.append(&mut other.attributes);
        self.events.append(&mut other.events);
        self.links.append(&mut other.links);

        self.bytes_est += other.bytes_est;
        self.ts_min = self.ts_min.min(other.ts_min);
        self.ts_max = self.ts_max.max(other.ts_max);

        other.bytes_est = 0;
        other.ts_min = u64::MAX;
        other.ts_max = 0;
    }

    fn reset(&mut self) {
        self.trace_ids.clear();
        self.span_ids.clear();
        self.parent_span_ids.clear();
        self.resource_labels.clear();
        self.service_names.clear();
        self.service_namespaces.clear();
        self.deployment_envs.clear();
        self.scope_names.clear();
        self.scope_versions.clear();
        self.names.clear();
        self.kinds.clear();
        self.starts.clear();
        self.ends.clear();
        self.status_codes.clear();
        self.status_messages.clear();
        self.attributes.clear();
        self.events.clear();
        self.links.clear();
        self.bytes_est = 0;
        self.ts_min = u64::MAX;
        self.ts_max = 0;
    }

    fn set_compression_level(&mut self, level: i32) {
        self.cfg.compression_level = level;
    }

    fn set_wal_seg_max(&mut self, seg: u64) {
        self.cfg.wal_seg_max = Some(seg);
    }

    fn set_wal_shard(&mut self, shard: u32) {
        self.cfg.wal_shard = Some(shard);
    }

    fn finish_and_upload(
        self,
        store: &dyn ObjectStore,
    ) -> impl std::future::Future<Output = Result<Option<BlockMeta>>> + Send {
        self.finish_and_upload_impl(store)
    }
}

impl TracesBlockBuilder {
    async fn finish_and_upload_impl(self, store: &dyn ObjectStore) -> Result<Option<BlockMeta>> {
        if self.is_empty() {
            return Ok(None);
        }
        let enc = tokio::task::spawn_blocking(move || self.encode())
            .await
            .context("join traces encode task")??;
        for (path, bytes) in enc.puts {
            store
                .put(&path, bytes.into())
                .await
                .with_context(|| format!("upload {path}"))?;
        }
        let meta = enc.meta;
        tracing::info!(
            block_uuid = %meta.uuid,
            row_count = meta.row_count,
            byte_size = meta.byte_size,
            ts_min = meta.ts_min_unix_nano,
            ts_max = meta.ts_max_unix_nano,
            "traces block uploaded"
        );
        Ok(Some(meta))
    }

    fn encode(mut self) -> Result<EncodedBlock> {
        let n = self.trace_ids.len();

        // Sort permutation by (trace_id, start) ascending, so parquet
        // row-group min/max stats on the trace_id column prune
        // trace-by-id lookups.
        let mut order: Vec<u32> = (0..n as u32).collect();
        order.sort_by(|&a, &b| {
            let a = a as usize;
            let b = b as usize;
            self.trace_ids[a]
                .cmp(&self.trace_ids[b])
                .then_with(|| self.starts[a].cmp(&self.starts[b]))
        });

        let schema = Self::main_schema();

        // ── Fixed-width id columns ─────────────────────────────────
        let mut tid_builder = FixedSizeBinaryBuilder::with_capacity(n, 16);
        let mut sid_builder = FixedSizeBinaryBuilder::with_capacity(n, 8);
        let mut parent_builder = FixedSizeBinaryBuilder::with_capacity(n, 8);
        for &i in order.iter() {
            let i = i as usize;
            tid_builder
                .append_value(self.trace_ids[i])
                .context("FixedSizeBinaryBuilder::append_value (trace_id)")?;
            sid_builder
                .append_value(self.span_ids[i])
                .context("FixedSizeBinaryBuilder::append_value (span_id)")?;
            match self.parent_span_ids[i] {
                Some(p) => parent_builder
                    .append_value(p)
                    .context("FixedSizeBinaryBuilder::append_value (parent_span_id)")?,
                None => parent_builder.append_null(),
            }
        }
        let trace_id_arr: ArrayRef = Arc::new(tid_builder.finish());
        let span_id_arr: ArrayRef = Arc::new(sid_builder.finish());
        let parent_span_id_arr: ArrayRef = Arc::new(parent_builder.finish());

        // ── Scalar columns ─────────────────────────────────────────
        let scope_name_arr: ArrayRef = Arc::new(StringArray::from_iter_values(
            order.iter().map(|&i| self.scope_names[i as usize].as_str()),
        ));
        let scope_version_arr: ArrayRef = Arc::new(StringArray::from_iter_values(
            order
                .iter()
                .map(|&i| self.scope_versions[i as usize].as_str()),
        ));
        let name_arr: ArrayRef = Arc::new(StringArray::from_iter_values(
            order.iter().map(|&i| self.names[i as usize].as_str()),
        ));
        let kind_arr: ArrayRef = Arc::new(UInt8Array::from_iter_values(
            order.iter().map(|&i| self.kinds[i as usize]),
        ));
        let start_arr: ArrayRef = Arc::new(UInt64Array::from_iter_values(
            order.iter().map(|&i| self.starts[i as usize]),
        ));
        let end_arr: ArrayRef = Arc::new(UInt64Array::from_iter_values(
            order.iter().map(|&i| self.ends[i as usize]),
        ));
        let status_code_arr: ArrayRef = Arc::new(UInt8Array::from_iter_values(
            order.iter().map(|&i| self.status_codes[i as usize]),
        ));
        let status_message_arr: ArrayRef = Arc::new(StringArray::from_iter_values(
            order
                .iter()
                .map(|&i| self.status_messages[i as usize].as_str()),
        ));

        // ── Promoted resource attributes (nullable Utf8) ───────────
        let service_name_arr: ArrayRef = Arc::new(StringArray::from_iter(
            order
                .iter()
                .map(|&i| self.service_names[i as usize].as_deref()),
        ));
        let service_namespace_arr: ArrayRef = Arc::new(StringArray::from_iter(
            order
                .iter()
                .map(|&i| self.service_namespaces[i as usize].as_deref()),
        ));
        let deployment_environment_arr: ArrayRef = Arc::new(StringArray::from_iter(
            order
                .iter()
                .map(|&i| self.deployment_envs[i as usize].as_deref()),
        ));

        // ── Top-level attribute maps ───────────────────────────────
        let resource_labels_arr = build_map(&order, &self.resource_labels)
            .context("building resource_labels MapArray")?;
        let attributes_arr =
            build_map(&order, &self.attributes).context("building attributes MapArray")?;

        // ── Nested events column: List<Struct<ts, name, attrs>> ────
        let total_events: usize = self.events.iter().map(|e| e.len()).sum();
        let mut ev_offsets: Vec<i32> = Vec::with_capacity(n + 1);
        ev_offsets.push(0);
        let mut ev_running: i32 = 0;
        let mut ev_ts: Vec<u64> = Vec::with_capacity(total_events);
        let mut ev_name_builder = StringBuilder::new();
        let mut ev_attr_builder = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
        for &i in order.iter() {
            let evs = &self.events[i as usize];
            for e in evs {
                ev_ts.push(e.ts_unix_nano);
                ev_name_builder.append_value(&e.name);
                for (k, v) in &e.attrs {
                    ev_attr_builder.keys().append_value(k);
                    ev_attr_builder.values().append_value(v);
                }
                ev_attr_builder
                    .append(true)
                    .context("MapBuilder::append (event attributes)")?;
            }
            ev_running = ev_running
                .checked_add(evs.len() as i32)
                .expect("traces events offset overflow (i32)");
            ev_offsets.push(ev_running);
        }
        let ev_ts_arr: ArrayRef = Arc::new(UInt64Array::from(ev_ts));
        let ev_name_arr: ArrayRef = Arc::new(ev_name_builder.finish());
        let ev_attr_arr: ArrayRef = Arc::new(ev_attr_builder.finish());
        let ev_struct = StructArray::try_new(
            event_struct_fields(),
            vec![ev_ts_arr, ev_name_arr, ev_attr_arr],
            None,
        )
        .context("constructing events StructArray")?;
        let events_arr: ArrayRef = Arc::new(
            ListArray::try_new(
                event_item_field(),
                OffsetBuffer::new(ev_offsets.into()),
                Arc::new(ev_struct),
                None,
            )
            .context("constructing events ListArray")?,
        );

        // ── Nested links column: List<Struct<trace_id, span_id, attrs>> ─
        let total_links: usize = self.links.iter().map(|l| l.len()).sum();
        let mut ln_offsets: Vec<i32> = Vec::with_capacity(n + 1);
        ln_offsets.push(0);
        let mut ln_running: i32 = 0;
        let mut ln_tid_builder = FixedSizeBinaryBuilder::with_capacity(total_links, 16);
        let mut ln_sid_builder = FixedSizeBinaryBuilder::with_capacity(total_links, 8);
        let mut ln_attr_builder = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
        for &i in order.iter() {
            let lns = &self.links[i as usize];
            for l in lns {
                ln_tid_builder
                    .append_value(l.trace_id)
                    .context("FixedSizeBinaryBuilder::append_value (link trace_id)")?;
                ln_sid_builder
                    .append_value(l.span_id)
                    .context("FixedSizeBinaryBuilder::append_value (link span_id)")?;
                for (k, v) in &l.attrs {
                    ln_attr_builder.keys().append_value(k);
                    ln_attr_builder.values().append_value(v);
                }
                ln_attr_builder
                    .append(true)
                    .context("MapBuilder::append (link attributes)")?;
            }
            ln_running = ln_running
                .checked_add(lns.len() as i32)
                .expect("traces links offset overflow (i32)");
            ln_offsets.push(ln_running);
        }
        let ln_tid_arr: ArrayRef = Arc::new(ln_tid_builder.finish());
        let ln_sid_arr: ArrayRef = Arc::new(ln_sid_builder.finish());
        let ln_attr_arr: ArrayRef = Arc::new(ln_attr_builder.finish());
        let ln_struct = StructArray::try_new(
            link_struct_fields(),
            vec![ln_tid_arr, ln_sid_arr, ln_attr_arr],
            None,
        )
        .context("constructing links StructArray")?;
        let links_arr: ArrayRef = Arc::new(
            ListArray::try_new(
                link_item_field(),
                OffsetBuffer::new(ln_offsets.into()),
                Arc::new(ln_struct),
                None,
            )
            .context("constructing links ListArray")?,
        );

        drop(order);
        // Release source buffers — Arrow now owns column copies.
        self.trace_ids = Vec::new();
        self.span_ids = Vec::new();
        self.parent_span_ids = Vec::new();
        self.resource_labels = Vec::new();
        self.service_names = Vec::new();
        self.service_namespaces = Vec::new();
        self.deployment_envs = Vec::new();
        self.scope_names = Vec::new();
        self.scope_versions = Vec::new();
        self.names = Vec::new();
        self.kinds = Vec::new();
        self.starts = Vec::new();
        self.ends = Vec::new();
        self.status_codes = Vec::new();
        self.status_messages = Vec::new();
        self.attributes = Vec::new();
        self.events = Vec::new();
        self.links = Vec::new();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                trace_id_arr,
                span_id_arr,
                parent_span_id_arr,
                resource_labels_arr,
                service_name_arr,
                service_namespace_arr,
                deployment_environment_arr,
                scope_name_arr,
                scope_version_arr,
                name_arr,
                kind_arr,
                start_arr,
                end_arr,
                status_code_arr,
                status_message_arr,
                attributes_arr,
                events_arr,
                links_arr,
            ],
        )
        .context("constructing traces main RecordBatch")?;

        let props = self.cfg.main_writer_props()?;
        let mut buf: Vec<u8> = Vec::with_capacity(self.bytes_est as usize);
        {
            let mut w = ArrowWriter::try_new(&mut buf, schema, Some(props))
                .context("ArrowWriter::try_new (traces main)")?;
            w.write(&batch)
                .context("ArrowWriter::write (traces main)")?;
            w.close().context("ArrowWriter::close (traces main)")?;
        }
        let parquet_bytes = Bytes::from(buf);
        let byte_size = parquet_bytes.len() as u64;

        let block_uuid = Uuid::now_v7();
        let parquet_path = Path::from(block_path(
            SIGNAL,
            self.ts_min,
            self.writer_id,
            block_uuid,
            "parquet",
        ));
        let meta_path = Path::from(block_path(
            SIGNAL,
            self.ts_min,
            self.writer_id,
            block_uuid,
            "meta.json",
        ));

        let meta = BlockMeta {
            uuid: block_uuid,
            signal: SIGNAL.to_string(),
            writer_id: self.writer_id,
            ts_min_unix_nano: self.ts_min,
            ts_max_unix_nano: self.ts_max,
            row_count: n as u64,
            byte_size,
            schema_version: SCHEMA_VERSION,
            level: 0,
            producer_version: env!("CARGO_PKG_VERSION").to_string(),
            label_fingerprint_bloom: None,
            // Trace-by-id pruning rides parquet row-group `trace_id`
            // stats (block sorted by trace_id) — no inverted index (per
            // D-025 + the v0.6 storage plan).
            has_postings: false,
            postings_size_bytes: None,
            series_types: None,
            all_fingerprints: None,
            has_body_bloom: false,
            body_bloom_size_bytes: None,
            wal_seg_max: self.cfg.wal_seg_max,
            wal_shard: self.cfg.wal_shard,
        };
        let meta_bytes =
            Bytes::from(serde_json::to_vec_pretty(&meta).context("serialising traces BlockMeta")?);

        // Upload order: parquet first, meta.json last (the sidecar is
        // the catalog's "block exists" signal).
        Ok(EncodedBlock {
            meta,
            puts: vec![(parquet_path, parquet_bytes), (meta_path, meta_bytes)],
        })
    }
}

/// Build a top-level `Map<Utf8,Utf8>` column by walking the sort
/// permutation and emitting one map per row.
fn build_map(order: &[u32], maps: &[Vec<(String, String)>]) -> Result<ArrayRef> {
    let mut builder = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
    for &i in order.iter() {
        for (k, v) in &maps[i as usize] {
            builder.keys().append_value(k);
            builder.values().append_value(v);
        }
        builder.append(true).context("MapBuilder::append")?;
    }
    Ok(Arc::new(builder.finish()))
}
