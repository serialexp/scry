//! Build a scry stream label set (and its fingerprint) for a container.
//!
//! The label set is the *identity* of a `LogStream`: a fixed core derived
//! from the CRI path (namespace / pod / container / node) plus any pod labels
//! the Kubernetes watcher discovered, namespaced under a `k8s_` prefix so
//! they can never collide with the core keys. The fingerprint must be
//! computed with the same function the server decodes with, or postings
//! won't line up.

use std::collections::BTreeMap;

use scry_proto::{fingerprint::fingerprint, LabelPair};

use crate::cri::PodPath;

/// Build the sorted label set for a container's log stream and its
/// fingerprint. `pod_labels` is the Kubernetes pod's `.metadata.labels`
/// (empty/None when discovery is off or the pod isn't known yet).
pub fn stream_labels(
    pod: &PodPath,
    node: &str,
    pod_labels: Option<&BTreeMap<String, String>>,
) -> (u64, Vec<LabelPair>) {
    let mut labels = Vec::with_capacity(4 + pod_labels.map_or(0, |m| m.len()));
    labels.push(LabelPair { key: "namespace".into(), value: pod.namespace.clone() });
    labels.push(LabelPair { key: "pod".into(), value: pod.pod.clone() });
    labels.push(LabelPair { key: "container".into(), value: pod.container.clone() });
    labels.push(LabelPair { key: "node".into(), value: node.to_string() });

    if let Some(m) = pod_labels {
        for (k, v) in m {
            labels.push(LabelPair { key: format!("k8s_{k}"), value: v.clone() });
        }
    }

    let fp = fingerprint(&labels);
    (fp, labels)
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
        assert!(labels.iter().any(|l| l.key == "namespace" && l.value == "ns"));
        assert!(labels.iter().any(|l| l.key == "node" && l.value == "node-1"));
        assert_eq!(labels.len(), 4);
    }

    #[test]
    fn pod_labels_prefixed_and_change_fingerprint() {
        let (fp_bare, _) = stream_labels(&pp(), "n", None);
        let mut m = BTreeMap::new();
        m.insert("app".to_string(), "api".to_string());
        let (fp_enriched, labels) = stream_labels(&pp(), "n", Some(&m));
        assert!(labels.iter().any(|l| l.key == "k8s_app" && l.value == "api"));
        assert_ne!(fp_bare, fp_enriched);
    }
}
