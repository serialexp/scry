//! Build a scry stream label set (and its fingerprint) for a container.
//!
//! The label set is the *identity* of a `LogStream`: a fixed core derived
//! from the CRI path (namespace / pod / container / node) plus any pod labels
//! the Kubernetes watcher discovered, namespaced under a `k8s_` prefix so
//! they can never collide with the core keys. The fingerprint must be
//! computed with the same function the server decodes with, or postings
//! won't line up.

use std::collections::BTreeMap;

use serde_json::{Map, Value};

use scry_proto::{fingerprint::fingerprint, LabelPair};

use crate::config::LogPipeline;
use crate::cri::PodPath;

/// Core stream-label keys derived from the CRI path. These are the stream's
/// identity and always win precedence over enrichment (`label_map` / static /
/// JSON-extracted labels can never clobber them).
const CORE_KEYS: [&str; 4] = ["namespace", "pod", "container", "node"];

/// Build the sorted label set for a container's log stream and its
/// fingerprint. `pod_labels` is the Kubernetes pod's `.metadata.labels`
/// (empty/None when discovery is off or the pod isn't known yet).
pub fn stream_labels(
    pod: &PodPath,
    node: &str,
    pod_labels: Option<&BTreeMap<String, String>>,
) -> (u64, Vec<LabelPair>) {
    let mut labels = Vec::with_capacity(4 + pod_labels.map_or(0, |m| m.len()));
    labels.push(LabelPair {
        key: "namespace".into(),
        value: pod.namespace.clone(),
    });
    labels.push(LabelPair {
        key: "pod".into(),
        value: pod.pod.clone(),
    });
    labels.push(LabelPair {
        key: "container".into(),
        value: pod.container.clone(),
    });
    labels.push(LabelPair {
        key: "node".into(),
        value: node.to_string(),
    });

    if let Some(m) = pod_labels {
        for (k, v) in m {
            labels.push(LabelPair {
                key: format!("k8s_{k}"),
                value: v.clone(),
            });
        }
    }

    let fp = fingerprint(&labels);
    (fp, labels)
}

/// Apply the log pipeline's label transforms to a stream's base labels (the
/// [`stream_labels`] output), returning the final `(fingerprint, label set)`.
///
/// Precedence, low→high (later wins): raw `k8s_<k>` pod labels < `label_map`
/// surfaced names < extracted `json.labels` < `static_labels` < the core
/// `namespace/pod/container/node` (always authoritative). De-dup goes through a
/// `BTreeMap` so the label vector never carries two pairs with the same key; the
/// fingerprint is order-independent (xxh3 over sorted pairs), so an empty
/// pipeline yields a fingerprint identical to `stream_labels`.
pub fn enrich_labels(
    base_labels: &[LabelPair],
    pipeline: &LogPipeline,
    json: Option<&Map<String, Value>>,
) -> (u64, Vec<LabelPair>) {
    let mut map: BTreeMap<String, String> = BTreeMap::new();

    // 1. Non-core base labels (k8s_<k> + any others) — lowest precedence.
    for l in base_labels {
        if !CORE_KEYS.contains(&l.key.as_str()) {
            map.insert(l.key.clone(), l.value.clone());
        }
    }
    // 2. label_map: surface a pod label `k8s_<k>` under the chosen name,
    //    suppressing the `k8s_` twin so the value isn't double-indexed.
    for (k, name) in &pipeline.label_map {
        let src = format!("k8s_{k}");
        if let Some(v) = map.remove(&src) {
            map.insert(name.clone(), v);
        }
    }
    // 3. json.labels (scalar values only; nested/null skipped).
    if let (Some(obj), Some(j)) = (json, pipeline.json.as_ref()) {
        for field in &j.labels {
            if let Some(v) = obj.get(field).and_then(json_scalar) {
                map.insert(field.clone(), v);
            }
        }
    }
    // 4. Static labels.
    for s in &pipeline.static_labels {
        map.insert(s.key.clone(), s.value.clone());
    }
    // 5. Core labels — always win.
    for l in base_labels {
        if CORE_KEYS.contains(&l.key.as_str()) {
            map.insert(l.key.clone(), l.value.clone());
        }
    }

    let labels: Vec<LabelPair> = map
        .into_iter()
        .map(|(key, value)| LabelPair { key, value })
        .collect();
    let fp = fingerprint(&labels);
    (fp, labels)
}

/// Build a log entry's attributes (the `stream=stdout|stderr` tag plus any
/// extracted `json.metadata` fields) and apply `message_field`. Returns
/// `(attributes, body)`. Metadata fields land in the per-entry attributes Map
/// (high-cardinality-tolerant, queryable via SQL on the Map column).
pub fn enrich_entry(
    stream_name: &str,
    mut body: String,
    pipeline: &LogPipeline,
    json: Option<&Map<String, Value>>,
) -> (Vec<LabelPair>, String) {
    let mut attrs = vec![LabelPair {
        key: "stream".into(),
        value: stream_name.into(),
    }];
    if let (Some(obj), Some(j)) = (json, pipeline.json.as_ref()) {
        for field in &j.metadata {
            if let Some(v) = obj.get(field).and_then(json_attr) {
                attrs.push(LabelPair {
                    key: field.clone(),
                    value: v,
                });
            }
        }
        if let Some(mf) = &j.message_field {
            if let Some(Value::String(s)) = obj.get(mf) {
                body = s.clone();
            }
        }
    }
    (attrs, body)
}

/// Stringify a JSON value for use as a stream *label* value. Only scalars are
/// accepted (a label value must be low-cardinality and atomic); null and nested
/// arrays/objects are skipped.
fn json_scalar(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        Value::Null | Value::Array(_) | Value::Object(_) => None,
    }
}

/// Stringify a JSON value for use as a per-entry *attribute* value. Scalars pass
/// through; nested arrays/objects are serialized (attributes tolerate high
/// cardinality); null is skipped.
fn json_attr(v: &Value) -> Option<String> {
    match v {
        Value::Null => None,
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        other => serde_json::to_string(other).ok(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pp() -> PodPath {
        PodPath {
            namespace: "ns".into(),
            pod: "pod".into(),
            uid: "uid".into(),
            container: "c".into(),
        }
    }

    #[test]
    fn core_labels_present() {
        let (_fp, labels) = stream_labels(&pp(), "node-1", None);
        assert!(labels
            .iter()
            .any(|l| l.key == "namespace" && l.value == "ns"));
        assert!(labels
            .iter()
            .any(|l| l.key == "node" && l.value == "node-1"));
        assert_eq!(labels.len(), 4);
    }

    #[test]
    fn pod_labels_prefixed_and_change_fingerprint() {
        let (fp_bare, _) = stream_labels(&pp(), "n", None);
        let mut m = BTreeMap::new();
        m.insert("app".to_string(), "api".to_string());
        let (fp_enriched, labels) = stream_labels(&pp(), "n", Some(&m));
        assert!(labels
            .iter()
            .any(|l| l.key == "k8s_app" && l.value == "api"));
        assert_ne!(fp_bare, fp_enriched);
    }

    use crate::config::{JsonPipeline, LogPipeline};

    fn obj(s: &str) -> Map<String, Value> {
        match serde_json::from_str(s).unwrap() {
            Value::Object(m) => m,
            _ => panic!("not an object"),
        }
    }

    #[test]
    fn enrich_empty_pipeline_matches_base() {
        // The regression guard: an empty pipeline must not change identity.
        let (base_fp, base) = stream_labels(&pp(), "node-1", None);
        let (fp, labels) = enrich_labels(&base, &LogPipeline::default(), None);
        assert_eq!(fp, base_fp);
        assert_eq!(labels.len(), base.len());
    }

    #[test]
    fn label_map_surfaces_and_suppresses_k8s() {
        let mut m = BTreeMap::new();
        m.insert("app".to_string(), "api".to_string());
        let (base_fp, base) = stream_labels(&pp(), "n", Some(&m));
        let mut pipe = LogPipeline::default();
        pipe.label_map.insert("app".into(), "app".into());
        let (fp, labels) = enrich_labels(&base, &pipe, None);
        assert!(labels.iter().any(|l| l.key == "app" && l.value == "api"));
        assert!(!labels.iter().any(|l| l.key == "k8s_app"));
        assert_ne!(fp, base_fp);
    }

    #[test]
    fn static_labels_added_core_always_wins() {
        let (_fp, base) = stream_labels(&pp(), "n", None);
        let mut pipe = LogPipeline::default();
        pipe.static_labels.push(LabelPair {
            key: "cluster".into(),
            value: "prod".into(),
        });
        // A static label that tries to override a core key must lose.
        pipe.static_labels.push(LabelPair {
            key: "namespace".into(),
            value: "evil".into(),
        });
        let (_fp, labels) = enrich_labels(&base, &pipe, None);
        assert!(labels
            .iter()
            .any(|l| l.key == "cluster" && l.value == "prod"));
        assert!(labels
            .iter()
            .any(|l| l.key == "namespace" && l.value == "ns"));
        assert!(!labels.iter().any(|l| l.value == "evil"));
    }

    #[test]
    fn json_labels_promoted_metadata_is_not_a_label() {
        let (_fp, base) = stream_labels(&pp(), "n", None);
        let mut pipe = LogPipeline::default();
        pipe.json = Some(JsonPipeline {
            labels: vec!["level".into()],
            metadata: vec!["request_id".into()],
            message_field: None,
        });
        let o = obj(r#"{"level":"warn","request_id":"r1"}"#);
        let (fp_with, labels) = enrich_labels(&base, &pipe, Some(&o));
        assert!(labels.iter().any(|l| l.key == "level" && l.value == "warn"));
        assert!(!labels.iter().any(|l| l.key == "request_id"));
        let (fp_without, _) = enrich_labels(&base, &pipe, None);
        assert_ne!(fp_with, fp_without);
    }

    #[test]
    fn json_scalars_stringified_null_and_nested_skipped_for_labels() {
        let (_fp, base) = stream_labels(&pp(), "n", None);
        let mut pipe = LogPipeline::default();
        pipe.json = Some(JsonPipeline {
            labels: vec!["num".into(), "flag".into(), "nul".into(), "nested".into()],
            metadata: vec![],
            message_field: None,
        });
        let o = obj(r#"{"num":42,"flag":true,"nul":null,"nested":{"a":1}}"#);
        let (_fp, labels) = enrich_labels(&base, &pipe, Some(&o));
        assert!(labels.iter().any(|l| l.key == "num" && l.value == "42"));
        assert!(labels.iter().any(|l| l.key == "flag" && l.value == "true"));
        assert!(!labels.iter().any(|l| l.key == "nul"));
        assert!(!labels.iter().any(|l| l.key == "nested"));
    }

    #[test]
    fn entry_metadata_and_message_field_applied() {
        let mut pipe = LogPipeline::default();
        pipe.json = Some(JsonPipeline {
            labels: vec![],
            metadata: vec!["request_id".into()],
            message_field: Some("msg".into()),
        });
        let o = obj(r#"{"msg":"hello","request_id":"r1"}"#);
        let (attrs, body) = enrich_entry("stdout", "{raw}".into(), &pipe, Some(&o));
        assert_eq!(body, "hello");
        assert!(attrs
            .iter()
            .any(|a| a.key == "stream" && a.value == "stdout"));
        assert!(attrs
            .iter()
            .any(|a| a.key == "request_id" && a.value == "r1"));
    }

    #[test]
    fn entry_missing_message_field_keeps_body() {
        let mut pipe = LogPipeline::default();
        pipe.json = Some(JsonPipeline {
            labels: vec![],
            metadata: vec![],
            message_field: Some("msg".into()),
        });
        let o = obj(r#"{"other":1}"#);
        let (attrs, body) = enrich_entry("stderr", "original".into(), &pipe, Some(&o));
        assert_eq!(body, "original");
        // The stream tag is always present even with no metadata.
        assert!(attrs
            .iter()
            .any(|a| a.key == "stream" && a.value == "stderr"));
    }

    #[test]
    fn entry_without_json_is_just_stream_tag() {
        let pipe = LogPipeline::default();
        let (attrs, body) = enrich_entry("stdout", "line".into(), &pipe, None);
        assert_eq!(body, "line");
        assert_eq!(attrs.len(), 1);
        assert_eq!(attrs[0].key, "stream");
    }
}
