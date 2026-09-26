use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    AlertState, AlertStatus, EncryptedSecretEnvelope, LogicalSecretId, Monitor, MonitorId,
    NotificationTarget, NotificationTargetId, Observation,
};

/// Schema of rule/target/secret control records and their heads.
pub const ALERT_RECORD_SCHEMA_VERSION: u32 = 1;

/// Schema of the alert-state head and its transition records.
///
/// Version 2 embeds the complete current [`AlertState`] in the CAS-advanced
/// head and writes an immutable [`TransitionRecord`] only when the status
/// changes. Version-1 heads/transitions live under different keys
/// ([`state_head_key`], [`transition_key`]) and are ignored.
pub const STATE_RECORD_SCHEMA_VERSION: u32 = 2;

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

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TargetHead {
    pub schema_version: u32,
    pub target_id: NotificationTargetId,
    pub revision: u64,
    pub revision_key: String,
    pub updated_at_unix_nano: u64,
    pub deleted: bool,
    pub tombstone_key: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TargetTombstone {
    pub schema_version: u32,
    pub target_id: NotificationTargetId,
    pub revision: u64,
    pub command_id: String,
    pub deleted_at_unix_nano: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TargetMutationKind {
    Create,
    Update,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TargetMutationReceipt {
    pub schema_version: u32,
    pub command_id: String,
    pub kind: TargetMutationKind,
    pub target_id: NotificationTargetId,
    pub revision: u64,
    /// Digest of the request with any secret value replaced by its action only.
    pub request_sha256: String,
    /// Secret-free candidate staged before any target publication. A retry resumes this exact
    /// candidate, including its deterministic credential identity.
    pub candidate: NotificationTarget,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SecretGenerationRecord {
    pub schema_version: u32,
    pub deployment_id: String,
    pub target_id: NotificationTargetId,
    pub logical_secret_id: LogicalSecretId,
    pub generation: u64,
    pub envelope: EncryptedSecretEnvelope,
    /// Present only for key rotation. Credential replacement always receives a new logical ID.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rotation_of_generation: Option<u64>,
    pub created_at_unix_nano: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SecretHead {
    pub schema_version: u32,
    pub deployment_id: String,
    pub target_id: NotificationTargetId,
    pub logical_secret_id: LogicalSecretId,
    pub generation: u64,
    pub generation_key: String,
    pub updated_at_unix_nano: u64,
}

/// Immutable record of one status change of a monitor's alert state.
///
/// Written with create-if-absent at its deterministic [`TransitionRef::key`]
/// before the state head that references it is advanced. It is written only
/// when the status changes, so its sequence equals the state's
/// `transition_sequence` and increments exactly once per record.
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
    /// Status before this transition; `None` for a monitor's first state.
    pub previous_status: Option<AlertStatus>,
    /// The transition leaves a NoData/Error outage back into the Firing or
    /// Recovering status it interrupted (see `StateChange::resumed`). Delivery
    /// treats it as a continuation: resuming Firing is not a new Firing.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub resumed: bool,
    pub state: AlertState,
    pub previous_transition_key: Option<String>,
    /// Reserved atomic commit surface for the next delivery slice.
    pub notification_intents: Vec<NotificationIntentPlaceholder>,
}

impl TransitionRecord {
    /// The reference a state head stores for this record.
    pub fn reference(&self) -> Result<TransitionRef, serde_json::Error> {
        Ok(TransitionRef {
            sequence: self.state.transition_sequence,
            monitor_revision: self.monitor_revision,
            slot_id: self.slot_id,
            status: self.state.status,
            sha256: sha256_hex(&canonical_json(self)?),
        })
    }
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

/// Identity and digest of the latest [`TransitionRecord`] reachable from a
/// state head. The object key is derived, never stored, so it cannot disagree
/// with the fields it is built from.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransitionRef {
    pub sequence: u64,
    pub monitor_revision: u64,
    pub slot_id: u64,
    pub status: AlertStatus,
    pub sha256: String,
}

impl TransitionRef {
    pub fn key(&self, monitor_id: MonitorId) -> String {
        transition_key(
            monitor_id,
            self.sequence,
            self.monitor_revision,
            self.slot_id,
            self.status,
        )
    }
}

/// The CAS-advanced visibility authority for one monitor's alert state.
///
/// Every evaluated slot advances this head and it embeds the complete current
/// [`AlertState`]; readers fold the head alone. `latest_transition` names the
/// immutable record of the most recent status change, which is only fetched
/// (and digest-verified) by writers chaining a new transition to it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StateHead {
    pub schema_version: u32,
    pub deployment_id: String,
    pub monitor_id: MonitorId,
    pub state: AlertState,
    pub latest_transition: TransitionRef,
    pub updated_at_unix_nano: u64,
}

impl StateHead {
    /// Structural invariants that hold without reading the transition object.
    ///
    /// The status only changes through a transition, so the current status and
    /// sequence must equal the latest transition's, and that transition cannot
    /// be newer than the state that embeds it.
    pub fn validate(&self, deployment_id: &str, monitor_id: MonitorId) -> Result<(), &'static str> {
        let transition = &self.latest_transition;
        if self.schema_version != STATE_RECORD_SCHEMA_VERSION {
            return Err("unsupported state head schema version");
        }
        if self.deployment_id != deployment_id || self.monitor_id != monitor_id {
            return Err("state head ownership does not match its key");
        }
        if transition.sequence == 0
            || transition.sequence != self.state.transition_sequence
            || transition.status != self.state.status
        {
            return Err("state head does not match its latest transition");
        }
        if (transition.monitor_revision, transition.slot_id)
            > (self.state.monitor_revision, self.state.last_slot_id)
        {
            return Err("state head is older than its latest transition");
        }
        Ok(())
    }

    /// Verify that `record` is exactly the transition this head references.
    pub fn verify_transition(&self, record: &TransitionRecord) -> Result<(), &'static str> {
        let reference = record
            .reference()
            .map_err(|_| "transition record cannot be serialized")?;
        if record.schema_version != STATE_RECORD_SCHEMA_VERSION
            || record.deployment_id != self.deployment_id
            || record.monitor_id != self.monitor_id
            || record.monitor_revision != record.state.monitor_revision
            || record.slot_id != record.state.last_slot_id
            || reference != self.latest_transition
        {
            return Err("state head and transition record do not match");
        }
        Ok(())
    }
}

/// Immutable rule revision object for one mutation command.
///
/// The command digest makes the key unique per command: a revision object
/// written by a command whose head CAS then lost (or crashed) is merely
/// unreachable, and cannot collide with a later command's different body for
/// the same revision number. The rule head's `revision_key` is the only
/// visibility authority.
pub fn rule_revision_key(id: MonitorId, revision: u64, command_id: &str) -> String {
    format!(
        "_scry/alerts/v1/rules/{id}/revisions/{revision:020}-{}.json",
        sha256_hex(command_id.as_bytes())
    )
}

/// Whether `key` may name a revision object of monitor `id`.
///
/// Heads are authoritative for the exact key; this only rejects a head that
/// points outside its own monitor's revision namespace.
pub fn is_rule_revision_key(id: MonitorId, key: &str) -> bool {
    is_revision_key(&format!("_scry/alerts/v1/rules/{id}/"), key)
}

fn is_revision_key(prefix: &str, key: &str) -> bool {
    key.strip_prefix(prefix).is_some_and(|tail| {
        tail.ends_with(".json")
            && tail != "head.json"
            && !tail.starts_with("tombstones/")
            && !tail.contains("..")
    })
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

/// Deterministic key of a state-schema-v2 transition: monitor, sequence,
/// effective revision, evaluation slot, and resulting status (kind). The
/// deployment is implied by the bucket. Keys sort by sequence.
pub fn transition_key(
    id: MonitorId,
    sequence: u64,
    monitor_revision: u64,
    slot_id: u64,
    status: AlertStatus,
) -> String {
    format!(
        "_scry/alerts/v1/transitions/v2/{id}/{sequence:020}-{monitor_revision:020}-{slot_id:020}-{}.json",
        status.as_str()
    )
}

/// State-schema-v2 head. Version-1 heads at `_scry/alerts/v1/state/<id>/head.json`
/// are not read.
pub fn state_head_key(id: MonitorId) -> String {
    format!("_scry/alerts/v1/state/v2/{id}/head.json")
}

/// Immutable target revision object for one mutation command; see
/// [`rule_revision_key`] for why the command digest is part of the key.
pub fn target_revision_key(id: NotificationTargetId, revision: u64, command_id: &str) -> String {
    format!(
        "_scry/alerts/v1/targets/{id}/revisions/{revision:020}-{}.json",
        sha256_hex(command_id.as_bytes())
    )
}

/// Whether `key` may name a revision object of target `id`.
pub fn is_target_revision_key(id: NotificationTargetId, key: &str) -> bool {
    is_revision_key(&format!("_scry/alerts/v1/targets/{id}/"), key)
}
pub fn target_head_key(id: NotificationTargetId) -> String {
    format!("_scry/alerts/v1/targets/{id}/head.json")
}
pub fn target_tombstone_key(id: NotificationTargetId, command_id: &str) -> String {
    format!(
        "_scry/alerts/v1/targets/{id}/tombstones/{}.json",
        sha256_hex(command_id.as_bytes())
    )
}
pub fn target_mutation_receipt_key(command_id: &str) -> String {
    format!(
        "_scry/alerts/v1/target-commands/{}.json",
        sha256_hex(command_id.as_bytes())
    )
}
pub fn secret_generation_key(
    target_id: NotificationTargetId,
    logical_id: LogicalSecretId,
    generation: u64,
) -> String {
    format!("_scry/alerts/v1/target-secrets/{target_id}/{logical_id}/{generation:020}.json")
}
pub fn secret_head_key(target_id: NotificationTargetId, logical_id: LogicalSecretId) -> String {
    format!("_scry/alerts/v1/target-secrets/{target_id}/{logical_id}/head.json")
}
pub fn target_head(target: &NotificationTarget, command_id: &str) -> TargetHead {
    TargetHead {
        schema_version: ALERT_RECORD_SCHEMA_VERSION,
        target_id: target.id,
        revision: target.revision,
        revision_key: target_revision_key(target.id, target.revision, command_id),
        updated_at_unix_nano: target.updated_at_unix_nano,
        deleted: false,
        tombstone_key: None,
    }
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

pub fn rule_head(monitor: &Monitor, command_id: &str) -> RuleHead {
    RuleHead {
        schema_version: ALERT_RECORD_SCHEMA_VERSION,
        monitor_id: monitor.id,
        revision: monitor.revision,
        revision_key: rule_revision_key(monitor.id, monitor.revision, command_id),
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
        assert!(rule_revision_key(id, 2, "b") < rule_revision_key(id, 10, "a"));
        assert_eq!(rule_revision_key(id, 2, "a"), rule_revision_key(id, 2, "a"));
        assert_ne!(rule_revision_key(id, 2, "a"), rule_revision_key(id, 2, "b"));
        assert_eq!(rule_head_key(id), rule_head_key(id));
        assert!(
            transition_key(id, 2, 9, 9, AlertStatus::Firing)
                < transition_key(id, 10, 1, 1, AlertStatus::Inactive)
        );
        assert_ne!(
            transition_key(id, 2, 1, 9, AlertStatus::Firing),
            transition_key(id, 2, 2, 9, AlertStatus::Firing),
            "the effective revision is part of the transition identity"
        );
    }

    #[test]
    fn revision_keys_are_scoped_to_their_owner() {
        let id = MonitorId::new();
        let other = MonitorId::new();
        assert!(is_rule_revision_key(id, &rule_revision_key(id, 3, "c")));
        assert!(!is_rule_revision_key(other, &rule_revision_key(id, 3, "c")));
        assert!(!is_rule_revision_key(id, &rule_head_key(id)));
        assert!(!is_rule_revision_key(id, &rule_tombstone_key(id, "c")));
        let target = NotificationTargetId::new();
        assert!(is_target_revision_key(
            target,
            &target_revision_key(target, 1, "c")
        ));
        assert!(!is_target_revision_key(target, &target_head_key(target)));
    }

    fn state(sequence: u64, status: AlertStatus, revision: u64, slot: u64) -> AlertState {
        AlertState {
            monitor_revision: revision,
            status,
            since_unix_nano: 1,
            last_evaluated_at_unix_nano: slot,
            last_slot_id: slot,
            last_value: None,
            last_error_class: None,
            transition_sequence: sequence,
            stale: false,
            interrupted: None,
        }
    }

    fn record(id: MonitorId, state: AlertState) -> TransitionRecord {
        TransitionRecord {
            schema_version: STATE_RECORD_SCHEMA_VERSION,
            deployment_id: "deployment".into(),
            monitor_id: id,
            monitor_revision: state.monitor_revision,
            slot_id: state.last_slot_id,
            evaluated_at_unix_nano: state.last_evaluated_at_unix_nano,
            observation: DurableObservation::NoData,
            previous_status: None,
            resumed: false,
            state,
            previous_transition_key: None,
            notification_intents: vec![],
        }
    }

    #[test]
    fn state_head_embeds_state_and_verifies_its_latest_transition() {
        let id = MonitorId::new();
        let transition = record(id, state(1, AlertStatus::Firing, 1, 5));
        let head = StateHead {
            schema_version: STATE_RECORD_SCHEMA_VERSION,
            deployment_id: "deployment".into(),
            monitor_id: id,
            // Later slots advance the head without a new transition.
            state: state(1, AlertStatus::Firing, 1, 9),
            latest_transition: transition.reference().unwrap(),
            updated_at_unix_nano: 9,
        };
        head.validate("deployment", id).unwrap();
        head.verify_transition(&transition).unwrap();
        assert!(head.validate("other", id).is_err());
        assert!(head.validate("deployment", MonitorId::new()).is_err());

        let mut tampered = transition.clone();
        tampered.observation = DurableObservation::Value { value: 1.0 };
        assert!(head.verify_transition(&tampered).is_err());

        let mut changed_status = head.clone();
        changed_status.state.status = AlertStatus::Inactive;
        assert!(
            changed_status.validate("deployment", id).is_err(),
            "a status change without a transition is corrupt"
        );
        let mut from_future = head.clone();
        from_future.state.last_slot_id = 4;
        assert!(from_future.validate("deployment", id).is_err());
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
