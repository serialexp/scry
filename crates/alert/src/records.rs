use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{AlertState, Monitor, MonitorId, Observation};

pub const ALERT_RECORD_SCHEMA_VERSION: u32 = 1;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuleHead {
    pub schema_version: u32,
    pub monitor_id: MonitorId,
    pub revision: u64,
    pub revision_key: String,
    pub updated_at_unix_nano: u64,
    #[serde(default)]
    pub deleted: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tombstone_key: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuleTombstone {
    pub schema_version: u32,
    pub monitor_id: MonitorId,
    pub revision: u64,
    pub command_id: String,
    pub deleted_at_unix_nano: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuleMutationKind {
    Create,
    Update,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuleMutationReceipt {
    pub schema_version: u32,
    pub command_id: String,
    pub kind: RuleMutationKind,
    pub monitor_id: MonitorId,
    pub revision: u64,
    pub request_sha256: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransitionRecord {
    pub schema_version: u32,
    pub deployment_id: String,
    pub monitor_id: MonitorId,
    pub monitor_revision: u64,
    pub slot_id: u64,
    pub evaluated_at_unix_nano: u64,
    pub observation: DurableObservation,
    pub state: AlertState,
    pub previous_transition_key: Option<String>,
    /// Reserved atomic commit surface for the next delivery slice.
    pub notification_intents: Vec<NotificationIntentPlaceholder>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DurableObservation {
    Value { value: f64 },
    NoData,
    Error { class: String },
}

impl From<Observation> for DurableObservation {
    fn from(value: Observation) -> Self {
        match value {
            Observation::Value(value) => Self::Value { value },
            Observation::NoData => Self::NoData,
            Observation::Error { class } => Self::Error { class },
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NotificationIntentPlaceholder {
    pub schema_version: u32,
    pub event_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StateHead {
    pub schema_version: u32,
    pub deployment_id: String,
    pub monitor_id: MonitorId,
    pub transition_sequence: u64,
    pub transition_key: String,
    pub transition_sha256: String,
    pub updated_at_unix_nano: u64,
}

pub fn rule_revision_key(id: MonitorId, revision: u64) -> String {
    format!("_scry/alerts/v1/rules/{id}/{revision:020}.json")
}

pub fn rule_head_key(id: MonitorId) -> String {
    format!("_scry/alerts/v1/rules/{id}/head.json")
}

pub fn rule_tombstone_key(id: MonitorId, command_id: &str) -> String {
    format!(
        "_scry/alerts/v1/rules/{id}/tombstones/{}.json",
        sha256_hex(command_id.as_bytes())
    )
}

pub fn rule_mutation_receipt_key(command_id: &str) -> String {
    format!(
        "_scry/alerts/v1/commands/{}.json",
        sha256_hex(command_id.as_bytes())
    )
}

pub fn transition_key(id: MonitorId, sequence: u64, slot_id: u64) -> String {
    format!("_scry/alerts/v1/transitions/{id}/{sequence:020}-{slot_id:020}.json")
}

pub fn state_head_key(id: MonitorId) -> String {
    format!("_scry/alerts/v1/state/{id}/head.json")
}

pub fn canonical_json<T: Serialize>(value: &T) -> Result<Vec<u8>, serde_json::Error> {
    serde_json::to_vec(value)
}

pub fn sha256_hex(bytes: &[u8]) -> String {
    let digest: [u8; 32] = Sha256::digest(bytes).into();
    let mut output = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write;
        write!(&mut output, "{byte:02x}").expect("writing to String cannot fail");
    }
    output
}

pub fn rule_head(monitor: &Monitor) -> RuleHead {
    RuleHead {
        schema_version: ALERT_RECORD_SCHEMA_VERSION,
        monitor_id: monitor.id,
        revision: monitor.revision,
        revision_key: rule_revision_key(monitor.id, monitor.revision),
        updated_at_unix_nano: monitor.updated_at_unix_nano,
        deleted: false,
        tombstone_key: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_are_deterministic_and_sort_revisions() {
        let id = MonitorId(uuid::Uuid::from_u128(1));
        assert!(rule_revision_key(id, 2) < rule_revision_key(id, 10));
        assert_eq!(rule_head_key(id), rule_head_key(id));
        assert!(transition_key(id, 2, 9) < transition_key(id, 10, 1));
    }

    #[test]
    fn digest_is_stable() {
        assert_eq!(
            sha256_hex(b"scry"),
            "73c46de9117a51af2a6def9a7dbc1a5413cba8e5fb520474ed1029d50c774616"
        );
    }

    #[test]
    fn command_ids_cannot_change_object_key_structure() {
        let id = MonitorId::new();
        let key = rule_tombstone_key(id, "../nested/%00/control");
        assert!(key.starts_with(&format!("_scry/alerts/v1/rules/{id}/tombstones/")));
        assert_eq!(key.matches('/').count(), 6);
        assert!(key.ends_with(".json"));
    }
}
