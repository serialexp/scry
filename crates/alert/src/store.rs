use bytes::Bytes;
use futures::TryStreamExt;
use object_store::{
    path::Path, Error as ObjectError, GetOptions, ObjectMeta, ObjectStore, PutPayload,
    UpdateVersion,
};
use serde::{de::DeserializeOwned, Serialize};
use thiserror::Error;

use crate::{
    canonical_json, is_rule_revision_key, is_target_revision_key, rule_head, rule_head_key,
    rule_mutation_receipt_key, rule_tombstone_key, secret_generation_key, secret_head_key,
    state_head_key, target_head, target_head_key, target_mutation_receipt_key,
    target_tombstone_key, LogicalSecretId, Monitor, MonitorId, NotificationTarget,
    NotificationTargetId, RuleHead, RuleMutationReceipt, RuleTombstone, SecretGenerationRecord,
    SecretHead, StateHead, TargetHead, TargetMutationReceipt, TargetTombstone, TransitionRecord,
};

pub const MAX_CONTROL_OBJECT_BYTES: u64 = 1024 * 1024;

#[derive(Debug, Error)]
pub enum AlertStoreError {
    #[error("serializing alert record: {0}")]
    Serialize(#[from] serde_json::Error),
    #[error("alert object `{path}` is not a valid record: {source}")]
    Decode {
        path: String,
        #[source]
        source: serde_json::Error,
    },
    #[error("object-store operation for `{path}` failed: {source}")]
    Object {
        path: String,
        #[source]
        source: ObjectError,
    },
    #[error("alert object `{path}` exceeds {MAX_CONTROL_OBJECT_BYTES} bytes")]
    Oversized { path: String },
    #[error("immutable alert object collision at `{path}`")]
    Collision { path: String },
    #[error("expected alert object `{path}` does not exist")]
    Missing { path: String },
    #[error("alert head conflict at `{path}`")]
    Conflict { path: String },
    #[error("alert object `{path}` failed integrity validation: {message}")]
    Corrupt { path: String, message: &'static str },
}

impl AlertStoreError {
    /// Transient backend failures that a later retry can resolve. Everything
    /// else is a definite answer about the durable records (missing, conflict,
    /// or an invalid/corrupt record that retrying cannot repair).
    pub fn is_transient(&self) -> bool {
        matches!(self, Self::Object { .. })
    }
}

#[derive(Clone, Debug)]
pub struct Versioned<T> {
    pub value: T,
    pub version: UpdateVersion,
}

pub struct AlertStore<'a> {
    store: &'a dyn ObjectStore,
}

impl<'a> AlertStore<'a> {
    pub fn new(store: &'a dyn ObjectStore) -> Self {
        Self { store }
    }

    /// Publish rule revision `monitor.revision` for mutation `command_id`.
    ///
    /// The immutable revision object is keyed by the command, so a revision
    /// written by a command whose head CAS lost or never happened is simply
    /// unreachable and never blocks a later command. The head advances only
    /// from exactly `monitor.revision - 1` (or is created for revision one);
    /// replaying the command that produced the current head succeeds.
    pub async fn create_rule_revision(
        &self,
        monitor: &Monitor,
        command_id: &str,
    ) -> Result<(), AlertStoreError> {
        let head = rule_head(monitor, command_id);
        self.create_identical(&head.revision_key, monitor).await?;
        let head_path = rule_head_key(monitor.id);
        match self.read_versioned::<RuleHead>(&head_path).await {
            Ok(current) if current.value == head => Ok(()),
            Ok(current)
                if !current.value.deleted
                    && current.value.revision.checked_add(1) == Some(monitor.revision) =>
            {
                self.update(&head_path, &head, current.version).await
            }
            Ok(_) => Err(AlertStoreError::Conflict { path: head_path }),
            Err(AlertStoreError::Missing { .. }) if monitor.revision == 1 => self
                .create_identical(&head_path, &head)
                .await
                .map_err(collision_is_conflict),
            Err(AlertStoreError::Missing { .. }) => {
                Err(AlertStoreError::Conflict { path: head_path })
            }
            Err(error) => Err(error),
        }
    }

    pub async fn read_rule(&self, id: MonitorId) -> Result<Versioned<Monitor>, AlertStoreError> {
        let head = self.read_rule_head(id).await?;
        if head.value.deleted {
            return Err(AlertStoreError::Missing {
                path: rule_head_key(id),
            });
        }
        self.read_rule_revision(&head.value).await
    }

    /// Read exactly the revision a (live) head names, without re-reading the
    /// head, and check that it is the revision the head claims.
    pub async fn read_rule_revision(
        &self,
        head: &RuleHead,
    ) -> Result<Versioned<Monitor>, AlertStoreError> {
        if head.deleted || !is_rule_revision_key(head.monitor_id, &head.revision_key) {
            return Err(AlertStoreError::Corrupt {
                path: rule_head_key(head.monitor_id),
                message: "rule head does not name one of its revisions",
            });
        }
        let monitor = self.read_versioned::<Monitor>(&head.revision_key).await?;
        if monitor.value.id != head.monitor_id || monitor.value.revision != head.revision {
            return Err(AlertStoreError::Corrupt {
                path: head.revision_key.clone(),
                message: "rule revision does not match its head",
            });
        }
        Ok(monitor)
    }

    pub async fn tombstone_rule(
        &self,
        tombstone: &RuleTombstone,
        expected_head: UpdateVersion,
    ) -> Result<(), AlertStoreError> {
        let tombstone_key = rule_tombstone_key(tombstone.monitor_id, &tombstone.command_id);
        self.create_identical(&tombstone_key, tombstone).await?;
        let head = RuleHead {
            schema_version: crate::ALERT_RECORD_SCHEMA_VERSION,
            monitor_id: tombstone.monitor_id,
            revision: tombstone.revision,
            revision_key: String::new(),
            updated_at_unix_nano: tombstone.deleted_at_unix_nano,
            deleted: true,
            tombstone_key: Some(tombstone_key),
        };
        self.update(&rule_head_key(tombstone.monitor_id), &head, expected_head)
            .await
    }

    pub async fn read_rule_head(
        &self,
        id: MonitorId,
    ) -> Result<Versioned<RuleHead>, AlertStoreError> {
        let path = rule_head_key(id);
        let head = self.read_versioned::<RuleHead>(&path).await?;
        if head.value.schema_version != crate::ALERT_RECORD_SCHEMA_VERSION
            || head.value.monitor_id != id
        {
            return Err(AlertStoreError::Corrupt {
                path,
                message: "rule head does not match its key",
            });
        }
        Ok(head)
    }

    pub async fn record_rule_mutation(
        &self,
        receipt: &RuleMutationReceipt,
    ) -> Result<(), AlertStoreError> {
        self.create_identical(&rule_mutation_receipt_key(&receipt.command_id), receipt)
            .await
    }

    pub async fn read_rule_mutation(
        &self,
        command_id: &str,
    ) -> Result<Option<RuleMutationReceipt>, AlertStoreError> {
        self.read_record(&rule_mutation_receipt_key(command_id))
            .await
    }

    pub async fn read_tombstone(
        &self,
        id: MonitorId,
        command_id: &str,
    ) -> Result<Option<RuleTombstone>, AlertStoreError> {
        self.read_record(&rule_tombstone_key(id, command_id)).await
    }

    /// Publish target revision `target.revision` for mutation `command_id`.
    ///
    /// Like rule revisions, the revision object is keyed by the command so an
    /// unpublished candidate never blocks another command. `expected_head` is
    /// the head version the command observed for an update (`None` creates).
    pub async fn create_target_revision(
        &self,
        target: &NotificationTarget,
        command_id: &str,
        expected_head: Option<UpdateVersion>,
    ) -> Result<(), AlertStoreError> {
        let next = target_head(target, command_id);
        self.create_identical(&next.revision_key, target).await?;
        let path = target_head_key(target.id);
        match self.read_versioned::<TargetHead>(&path).await {
            Ok(current) if current.value == next => Ok(()),
            Ok(current)
                if !current.value.deleted
                    && current.value.revision.checked_add(1) == Some(target.revision) =>
            {
                match expected_head {
                    Some(version) => self.update(&path, &next, version).await,
                    None => Err(AlertStoreError::Conflict { path }),
                }
            }
            Ok(_) => Err(AlertStoreError::Conflict { path }),
            Err(AlertStoreError::Missing { .. })
                if expected_head.is_none() && target.revision == 1 =>
            {
                self.create_identical(&path, &next)
                    .await
                    .map_err(collision_is_conflict)
            }
            Err(AlertStoreError::Missing { .. }) => Err(AlertStoreError::Conflict { path }),
            Err(error) => Err(error),
        }
    }

    pub async fn read_target(
        &self,
        id: NotificationTargetId,
    ) -> Result<Versioned<NotificationTarget>, AlertStoreError> {
        let head = self.read_target_head(id).await?;
        if head.value.deleted {
            return Err(AlertStoreError::Missing {
                path: target_head_key(id),
            });
        }
        self.read_target_revision(&head.value).await
    }

    /// Read exactly the revision a (live) target head names.
    pub async fn read_target_revision(
        &self,
        head: &TargetHead,
    ) -> Result<Versioned<NotificationTarget>, AlertStoreError> {
        if head.deleted || !is_target_revision_key(head.target_id, &head.revision_key) {
            return Err(AlertStoreError::Corrupt {
                path: target_head_key(head.target_id),
                message: "target head does not name one of its revisions",
            });
        }
        let target = self
            .read_versioned::<NotificationTarget>(&head.revision_key)
            .await?;
        if target.value.id != head.target_id || target.value.revision != head.revision {
            return Err(AlertStoreError::Corrupt {
                path: head.revision_key.clone(),
                message: "target revision does not match its head",
            });
        }
        Ok(target)
    }

    pub async fn read_target_head(
        &self,
        id: NotificationTargetId,
    ) -> Result<Versioned<TargetHead>, AlertStoreError> {
        let path = target_head_key(id);
        let head = self.read_versioned::<TargetHead>(&path).await?;
        if head.value.schema_version != crate::ALERT_RECORD_SCHEMA_VERSION
            || head.value.target_id != id
        {
            return Err(AlertStoreError::Corrupt {
                path,
                message: "target head does not match its key",
            });
        }
        Ok(head)
    }

    pub async fn tombstone_target(
        &self,
        tombstone: &TargetTombstone,
        expected_head: UpdateVersion,
    ) -> Result<(), AlertStoreError> {
        let tombstone_key = target_tombstone_key(tombstone.target_id, &tombstone.command_id);
        self.create_identical(&tombstone_key, tombstone).await?;
        let head = TargetHead {
            schema_version: crate::ALERT_RECORD_SCHEMA_VERSION,
            target_id: tombstone.target_id,
            revision: tombstone.revision,
            revision_key: String::new(),
            updated_at_unix_nano: tombstone.deleted_at_unix_nano,
            deleted: true,
            tombstone_key: Some(tombstone_key),
        };
        self.update(&target_head_key(tombstone.target_id), &head, expected_head)
            .await
    }

    pub async fn read_target_tombstone(
        &self,
        id: NotificationTargetId,
        command_id: &str,
    ) -> Result<Option<TargetTombstone>, AlertStoreError> {
        self.read_record(&target_tombstone_key(id, command_id))
            .await
    }
    pub async fn record_target_mutation(
        &self,
        receipt: &TargetMutationReceipt,
    ) -> Result<(), AlertStoreError> {
        self.create_identical(&target_mutation_receipt_key(&receipt.command_id), receipt)
            .await
    }
    pub async fn read_target_mutation(
        &self,
        command_id: &str,
    ) -> Result<Option<TargetMutationReceipt>, AlertStoreError> {
        self.read_record(&target_mutation_receipt_key(command_id))
            .await
    }

    /// Persist an immutable encrypted generation, then CAS its independent secret head.
    pub async fn commit_secret_generation(
        &self,
        record: &SecretGenerationRecord,
        head: &SecretHead,
        expected_head: Option<UpdateVersion>,
    ) -> Result<(), AlertStoreError> {
        let key = secret_generation_key(
            record.target_id,
            record.logical_secret_id,
            record.generation,
        );
        if record.deployment_id != head.deployment_id
            || record.target_id != head.target_id
            || record.logical_secret_id != head.logical_secret_id
            || record.generation != head.generation
            || key != head.generation_key
        {
            return Err(AlertStoreError::Corrupt {
                path: key,
                message: "secret generation and head do not match",
            });
        }
        self.create_identical(&key, record).await?;
        let path = secret_head_key(record.target_id, record.logical_secret_id);
        match expected_head {
            Some(version) => self.update(&path, head, version).await,
            None => self.create_identical(&path, head).await,
        }
    }
    pub async fn read_secret_head(
        &self,
        target: NotificationTargetId,
        logical: LogicalSecretId,
    ) -> Result<Option<Versioned<SecretHead>>, AlertStoreError> {
        match self.read_versioned(&secret_head_key(target, logical)).await {
            Ok(v) => Ok(Some(v)),
            Err(AlertStoreError::Missing { .. }) => Ok(None),
            Err(e) => Err(e),
        }
    }
    pub async fn read_secret_generation(
        &self,
        target: NotificationTargetId,
        logical: LogicalSecretId,
        generation: u64,
    ) -> Result<Versioned<SecretGenerationRecord>, AlertStoreError> {
        self.read_versioned(&secret_generation_key(target, logical, generation))
            .await
    }

    /// Read and structurally validate a monitor's state head. The embedded
    /// state is authoritative; the referenced transition is not fetched.
    pub async fn read_state_head(
        &self,
        id: MonitorId,
        deployment_id: &str,
    ) -> Result<Option<Versioned<StateHead>>, AlertStoreError> {
        let path = state_head_key(id);
        let head = match self.read_versioned::<StateHead>(&path).await {
            Ok(head) => head,
            Err(AlertStoreError::Missing { .. }) => return Ok(None),
            Err(error) => return Err(error),
        };
        head.value
            .validate(deployment_id, id)
            .map_err(|message| AlertStoreError::Corrupt { path, message })?;
        Ok(Some(head))
    }

    /// Advance a monitor's state head, first publishing `transition` when the
    /// status changed (metadata-last: the head is the single commit point).
    ///
    /// `expected_head` is the version of the head the new state was computed
    /// from (`None` when there was none); a concurrent writer makes this a
    /// [`AlertStoreError::Conflict`], leaving any published transition
    /// unreachable.
    pub async fn commit_state(
        &self,
        head: &StateHead,
        transition: Option<&TransitionRecord>,
        expected_head: Option<UpdateVersion>,
    ) -> Result<(), AlertStoreError> {
        let path = state_head_key(head.monitor_id);
        let corrupt = |message| AlertStoreError::Corrupt {
            path: path.clone(),
            message,
        };
        head.validate(&head.deployment_id, head.monitor_id)
            .map_err(corrupt)?;
        if let Some(transition) = transition {
            head.verify_transition(transition).map_err(corrupt)?;
            self.create_identical(&head.latest_transition.key(head.monitor_id), transition)
                .await?;
        }
        match expected_head {
            Some(version) => self.update(&path, head, version).await,
            None => self
                .create_identical(&path, head)
                .await
                .map_err(collision_is_conflict),
        }
    }

    /// Fetch and digest-verify the transition a state head references.
    pub async fn read_latest_transition(
        &self,
        head: &StateHead,
    ) -> Result<Versioned<TransitionRecord>, AlertStoreError> {
        let key = head.latest_transition.key(head.monitor_id);
        let transition = self.read_versioned::<TransitionRecord>(&key).await?;
        head.verify_transition(&transition.value)
            .map_err(|message| AlertStoreError::Corrupt { path: key, message })?;
        Ok(transition)
    }

    /// Create-if-absent an auxiliary immutable record. Identical bytes already
    /// present are success; different bytes are a [`AlertStoreError::Collision`].
    pub async fn create_record<T: Serialize>(
        &self,
        key: &str,
        value: &T,
    ) -> Result<(), AlertStoreError> {
        self.create_identical(key, value).await
    }

    /// Bounded single-GET read of an auxiliary record; `None` when absent.
    pub async fn read_record<T: DeserializeOwned>(
        &self,
        key: &str,
    ) -> Result<Option<T>, AlertStoreError> {
        match self.read_versioned(key).await {
            Ok(record) => Ok(Some(record.value)),
            Err(AlertStoreError::Missing { .. }) => Ok(None),
            Err(error) => Err(error),
        }
    }

    async fn create_identical<T: Serialize>(
        &self,
        key: &str,
        value: &T,
    ) -> Result<(), AlertStoreError> {
        let bytes = bounded_json(key, value)?;
        let path = Path::from(key);
        match scry_objstore::put_create(self.store, &path, PutPayload::from(bytes.clone())).await {
            Ok(_) => Ok(()),
            // `Precondition` is included for backends that report a failed
            // `If-None-Match` as 412 rather than `AlreadyExists`. A retried
            // create that had actually committed also lands here and is
            // recognised by its identical bytes.
            Err(ObjectError::AlreadyExists { .. } | ObjectError::Precondition { .. }) => {
                match self.get_bounded(key).await {
                    Ok((existing, _)) if existing == bytes => Ok(()),
                    Ok(_) => Err(AlertStoreError::Collision {
                        path: key.to_owned(),
                    }),
                    Err(error) => Err(error),
                }
            }
            Err(source) => Err(AlertStoreError::Object {
                path: key.to_owned(),
                source,
            }),
        }
    }

    /// Compare-and-swap `key` from `version` to `value`.
    ///
    /// `object_store` retries a conditional PUT after a 5xx; if the first
    /// attempt had committed, the retry reports a precondition failure (412),
    /// or `AlreadyExists` once 409 retries are exhausted, even though our bytes
    /// are durable. Every such outcome is therefore resolved by reading the
    /// object back: our exact bytes mean the write committed, anything else is
    /// a genuine [`AlertStoreError::Conflict`].
    async fn update<T: Serialize>(
        &self,
        key: &str,
        value: &T,
        version: UpdateVersion,
    ) -> Result<(), AlertStoreError> {
        let bytes = bounded_json(key, value)?;
        let path = Path::from(key);
        match scry_objstore::put_update(self.store, &path, PutPayload::from(bytes.clone()), version)
            .await
        {
            Ok(_) => Ok(()),
            Err(
                ObjectError::Precondition { .. }
                | ObjectError::NotModified { .. }
                | ObjectError::AlreadyExists { .. },
            ) => match self.get_bounded(key).await {
                Ok((existing, _)) if existing == bytes => Ok(()),
                Ok(_) | Err(AlertStoreError::Missing { .. }) => Err(AlertStoreError::Conflict {
                    path: key.to_owned(),
                }),
                Err(error) => Err(error),
            },
            Err(source) => Err(AlertStoreError::Object {
                path: key.to_owned(),
                source,
            }),
        }
    }

    /// One GET: the returned bytes and the version they were read at come
    /// from the same response, so a concurrent update can never pair new
    /// bytes with an old ETag (or fail a HEAD-then-GET precondition).
    async fn read_versioned<T: DeserializeOwned>(
        &self,
        key: &str,
    ) -> Result<Versioned<T>, AlertStoreError> {
        let (bytes, meta) = self.get_bounded(key).await?;
        let version = update_version(&meta).ok_or_else(|| AlertStoreError::Object {
            path: key.to_owned(),
            source: ObjectError::Generic {
                store: "alert-store",
                source: "object has neither ETag nor version".into(),
            },
        })?;
        let value = serde_json::from_slice(&bytes).map_err(|source| AlertStoreError::Decode {
            path: key.to_owned(),
            source,
        })?;
        Ok(Versioned { value, version })
    }

    /// Single GET bounded by [`MAX_CONTROL_OBJECT_BYTES`]: the size reported
    /// by the response is checked before any body is read, and the body
    /// stream is cut off if it exceeds the bound regardless.
    async fn get_bounded(&self, key: &str) -> Result<(Bytes, ObjectMeta), AlertStoreError> {
        let path = Path::from(key);
        let object_error = |source| match source {
            ObjectError::NotFound { .. } => AlertStoreError::Missing {
                path: key.to_owned(),
            },
            source => AlertStoreError::Object {
                path: key.to_owned(),
                source,
            },
        };
        let result = self
            .store
            .get_opts(&path, GetOptions::default())
            .await
            .map_err(object_error)?;
        let oversized = || AlertStoreError::Oversized {
            path: key.to_owned(),
        };
        if result.meta.size > MAX_CONTROL_OBJECT_BYTES {
            return Err(oversized());
        }
        let meta = result.meta.clone();
        let mut body = Vec::with_capacity(meta.size as usize);
        let mut stream = result.into_stream();
        while let Some(chunk) = stream.try_next().await.map_err(object_error)? {
            if (body.len() + chunk.len()) as u64 > MAX_CONTROL_OBJECT_BYTES {
                return Err(oversized());
            }
            body.extend_from_slice(&chunk);
        }
        Ok((Bytes::from(body), meta))
    }
}

fn bounded_json<T: Serialize>(key: &str, value: &T) -> Result<Vec<u8>, AlertStoreError> {
    let bytes = canonical_json(value)?;
    if bytes.len() as u64 > MAX_CONTROL_OBJECT_BYTES {
        return Err(AlertStoreError::Oversized {
            path: key.to_owned(),
        });
    }
    Ok(bytes)
}

/// A create-if-absent of a *head* that finds different bytes lost a race to
/// another writer: that is a head conflict, not an immutable-record collision.
fn collision_is_conflict(error: AlertStoreError) -> AlertStoreError {
    match error {
        AlertStoreError::Collision { path } => AlertStoreError::Conflict { path },
        other => other,
    }
}

fn update_version(meta: &ObjectMeta) -> Option<UpdateVersion> {
    if meta.e_tag.is_none() && meta.version.is_none() {
        return None;
    }
    Some(UpdateVersion {
        e_tag: meta.e_tag.clone(),
        version: meta.version.clone(),
    })
}

#[cfg(test)]
mod tests {
    use std::sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    };

    use async_trait::async_trait;
    use futures::stream::BoxStream;
    use object_store::{
        memory::InMemory, CopyOptions, GetResult, ListResult, MultipartUpload, ObjectStoreExt,
        PutMultipartOptions, PutOptions, PutResult,
    };

    use crate::{
        AlertState, AlertStatus, Comparator, DurableObservation, ExecutionErrorPolicy,
        NoDataPolicy, ScalarCondition, ScalarQuery, Signal, ALERT_RECORD_SCHEMA_VERSION,
        MONITOR_SCHEMA_VERSION, STATE_RECORD_SCHEMA_VERSION,
    };

    use super::*;

    const DEPLOYMENT: &str = "018f47a2-8b6c-7def-8123-456789abcdef";

    fn monitor(id: MonitorId, revision: u64) -> Monitor {
        Monitor {
            schema_version: MONITOR_SCHEMA_VERSION,
            id,
            revision,
            name: format!("rule {revision}"),
            enabled: true,
            query: ScalarQuery {
                target_id: "local".into(),
                signal: Signal::Metrics,
                matchers: vec![],
                lookback_seconds: 60,
                sql: "SELECT count(*) FROM metrics".into(),
            },
            condition: ScalarCondition {
                comparator: Comparator::Gt,
                threshold: 1.0,
            },
            every_seconds: 60,
            jitter_seconds: 0,
            for_seconds: 0,
            recover_for_seconds: 0,
            no_data: NoDataPolicy::NoData,
            execution_error: ExecutionErrorPolicy::Error,
            labels: vec![],
            annotations: vec![],
            created_at_unix_nano: 1,
            updated_at_unix_nano: revision,
        }
    }

    fn target(id: NotificationTargetId, revision: u64) -> NotificationTarget {
        NotificationTarget {
            schema_version: ALERT_RECORD_SCHEMA_VERSION,
            id,
            revision,
            name: format!("target {revision}"),
            enabled: true,
            kind: crate::NotificationTargetKind::SlackWebhook,
            format: crate::TargetFormat::BuiltIn {
                format: crate::BuiltInTargetFormat::Slack,
            },
            timeout_millis: 5_000,
            logical_secret_id: LogicalSecretId::new(),
            secret_generation: 1,
            created_at_unix_nano: 1,
            updated_at_unix_nano: revision,
        }
    }

    fn alert_state(status: AlertStatus, sequence: u64, slot: u64) -> AlertState {
        AlertState {
            monitor_revision: 1,
            status,
            since_unix_nano: slot,
            last_evaluated_at_unix_nano: slot,
            last_slot_id: slot,
            last_value: Some(2.0),
            last_error_class: None,
            transition_sequence: sequence,
            stale: false,
            interrupted: None,
        }
    }

    fn transition(rule: &Monitor, state: AlertState) -> (TransitionRecord, StateHead) {
        let record = TransitionRecord {
            schema_version: STATE_RECORD_SCHEMA_VERSION,
            deployment_id: DEPLOYMENT.into(),
            monitor_id: rule.id,
            monitor_revision: rule.revision,
            slot_id: state.last_slot_id,
            evaluated_at_unix_nano: state.last_slot_id,
            observation: DurableObservation::Value { value: 2.0 },
            previous_status: None,
            resumed: false,
            state: state.clone(),
            previous_transition_key: None,
            notification_intents: vec![],
        };
        let head = StateHead {
            schema_version: STATE_RECORD_SCHEMA_VERSION,
            deployment_id: DEPLOYMENT.into(),
            monitor_id: rule.id,
            latest_transition: record.reference().unwrap(),
            updated_at_unix_nano: state.last_slot_id,
            state,
        };
        (record, head)
    }

    #[tokio::test]
    async fn rule_revisions_are_immutable_and_head_advances() {
        let backend = InMemory::new();
        let store = AlertStore::new(&backend);
        let id = MonitorId::new();
        let first = monitor(id, 1);
        store.create_rule_revision(&first, "c1").await.unwrap();
        store.create_rule_revision(&first, "c1").await.unwrap();
        assert_eq!(store.read_rule(id).await.unwrap().value, first);

        let second = monitor(id, 2);
        store.create_rule_revision(&second, "c2").await.unwrap();
        assert_eq!(store.read_rule(id).await.unwrap().value, second);
        assert!(matches!(
            store.create_rule_revision(&first, "c1").await,
            Err(AlertStoreError::Conflict { .. })
        ));
        assert!(
            matches!(
                store.create_rule_revision(&monitor(id, 4), "c4").await,
                Err(AlertStoreError::Conflict { .. })
            ),
            "revisions cannot skip"
        );
    }

    #[tokio::test]
    async fn an_unpublished_revision_never_blocks_a_later_save() {
        let backend = InMemory::new();
        let store = AlertStore::new(&backend);
        let id = MonitorId::new();
        store
            .create_rule_revision(&monitor(id, 1), "create")
            .await
            .unwrap();
        // A save wrote its immutable revision two, then crashed (or lost its
        // head CAS) before advancing the head.
        let mut stranded = monitor(id, 2);
        stranded.name = "never published".into();
        store
            .create_identical(&crate::rule_revision_key(id, 2, "stranded"), &stranded)
            .await
            .unwrap();
        assert_eq!(store.read_rule(id).await.unwrap().value.revision, 1);

        // A later save with a different body for the same revision succeeds.
        let mut next = monitor(id, 2);
        next.name = "saved".into();
        next.updated_at_unix_nano = 99;
        store.create_rule_revision(&next, "later").await.unwrap();
        assert_eq!(store.read_rule(id).await.unwrap().value, next);

        // Same for targets.
        let target_id = NotificationTargetId::new();
        store
            .create_target_revision(&target(target_id, 1), "create", None)
            .await
            .unwrap();
        let mut orphan = target(target_id, 2);
        orphan.name = "orphan".into();
        store
            .create_identical(&crate::target_revision_key(target_id, 2, "orphan"), &orphan)
            .await
            .unwrap();
        let head = store.read_target_head(target_id).await.unwrap();
        let second = target(target_id, 2);
        store
            .create_target_revision(&second, "later", Some(head.version))
            .await
            .unwrap();
        assert_eq!(store.read_target(target_id).await.unwrap().value, second);
    }

    #[tokio::test]
    async fn target_revisions_tombstones_and_receipts_are_idempotent() {
        let backend = InMemory::new();
        let store = AlertStore::new(&backend);
        let id = NotificationTargetId::new();
        let first = target(id, 1);
        store
            .create_target_revision(&first, "c1", None)
            .await
            .unwrap();
        store
            .create_target_revision(&first, "c1", None)
            .await
            .unwrap();
        assert_eq!(store.read_target(id).await.unwrap().value, first);
        let current = store.read_target_head(id).await.unwrap();
        let second = target(id, 2);
        store
            .create_target_revision(&second, "c2", Some(current.version))
            .await
            .unwrap();
        let receipt = TargetMutationReceipt {
            schema_version: ALERT_RECORD_SCHEMA_VERSION,
            command_id: "command".into(),
            kind: crate::TargetMutationKind::Update,
            target_id: id,
            revision: 2,
            request_sha256: "hash".into(),
            candidate: second.clone(),
        };
        store.record_target_mutation(&receipt).await.unwrap();
        store.record_target_mutation(&receipt).await.unwrap();
        assert_eq!(
            store.read_target_mutation("command").await.unwrap(),
            Some(receipt)
        );
        let head = store.read_target_head(id).await.unwrap();
        let tombstone = TargetTombstone {
            schema_version: ALERT_RECORD_SCHEMA_VERSION,
            target_id: id,
            revision: 2,
            command_id: "delete".into(),
            deleted_at_unix_nano: 3,
        };
        store
            .tombstone_target(&tombstone, head.version)
            .await
            .unwrap();
        assert!(matches!(
            store.read_target(id).await,
            Err(AlertStoreError::Missing { .. })
        ));
        assert!(matches!(
            store
                .create_target_revision(&target(id, 3), "c3", None)
                .await,
            Err(AlertStoreError::Conflict { .. })
        ));
    }

    #[tokio::test]
    async fn secret_head_uses_cas_and_generation_is_immutable() {
        let backend = InMemory::new();
        let store = AlertStore::new(&backend);
        let target = NotificationTargetId::new();
        let logical = LogicalSecretId::new();
        let record = SecretGenerationRecord {
            schema_version: ALERT_RECORD_SCHEMA_VERSION,
            deployment_id: "deployment".into(),
            target_id: target,
            logical_secret_id: logical,
            generation: 1,
            envelope: crate::EncryptedSecretEnvelope {
                version: 1,
                key_id: "key".into(),
                nonce_base64url: "nonce".into(),
                ciphertext_base64url: "ciphertext".into(),
            },
            rotation_of_generation: None,
            created_at_unix_nano: 1,
        };
        let head = SecretHead {
            schema_version: ALERT_RECORD_SCHEMA_VERSION,
            deployment_id: "deployment".into(),
            target_id: target,
            logical_secret_id: logical,
            generation: 1,
            generation_key: secret_generation_key(target, logical, 1),
            updated_at_unix_nano: 1,
        };
        store
            .commit_secret_generation(&record, &head, None)
            .await
            .unwrap();
        let current = store
            .read_secret_head(target, logical)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            store
                .read_secret_generation(target, logical, 1)
                .await
                .unwrap()
                .value,
            record
        );
        let mut next_record = record.clone();
        next_record.generation = 2;
        next_record.created_at_unix_nano = 2;
        let mut next_head = head.clone();
        next_head.generation = 2;
        next_head.generation_key = secret_generation_key(target, logical, 2);
        next_head.updated_at_unix_nano = 2;
        store
            .commit_secret_generation(&next_record, &next_head, Some(current.version.clone()))
            .await
            .unwrap();
        // Replaying the committed CAS is recognised by its identical bytes.
        store
            .commit_secret_generation(&next_record, &next_head, Some(current.version.clone()))
            .await
            .unwrap();
        let mut competing = next_head.clone();
        competing.updated_at_unix_nano = 3;
        assert!(matches!(
            store
                .commit_secret_generation(&next_record, &competing, Some(current.version))
                .await,
            Err(AlertStoreError::Conflict { .. })
        ));
    }

    #[tokio::test]
    async fn tombstone_prevents_rule_resurrection() {
        let backend = InMemory::new();
        let store = AlertStore::new(&backend);
        let rule = monitor(MonitorId::new(), 1);
        store.create_rule_revision(&rule, "create").await.unwrap();
        let head = store.read_rule_head(rule.id).await.unwrap();
        store
            .tombstone_rule(
                &RuleTombstone {
                    schema_version: crate::ALERT_RECORD_SCHEMA_VERSION,
                    monitor_id: rule.id,
                    revision: 1,
                    command_id: "delete-1".into(),
                    deleted_at_unix_nano: 2,
                },
                head.version,
            )
            .await
            .unwrap();
        assert!(matches!(
            store.read_rule(rule.id).await,
            Err(AlertStoreError::Missing { .. })
        ));
        assert!(matches!(
            store
                .create_rule_revision(&monitor(rule.id, 2), "update")
                .await,
            Err(AlertStoreError::Conflict { .. })
        ));
    }

    #[tokio::test]
    async fn state_head_cas_selects_one_successor_and_writes_transitions_only_on_change() {
        let backend = InMemory::new();
        let store = AlertStore::new(&backend);
        let rule = monitor(MonitorId::new(), 1);
        let (first_record, first_head) = transition(&rule, alert_state(AlertStatus::Firing, 1, 1));
        store
            .commit_state(&first_head, Some(&first_record), None)
            .await
            .unwrap();
        let current = store
            .read_state_head(rule.id, DEPLOYMENT)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(current.value, first_head);

        // Same status in a later slot: only the head advances.
        let mut advanced = first_head.clone();
        advanced.state.last_slot_id = 2;
        advanced.state.last_evaluated_at_unix_nano = 2;
        advanced.updated_at_unix_nano = 2;
        store
            .commit_state(&advanced, None, Some(current.version.clone()))
            .await
            .unwrap();
        let listed: Vec<_> = backend
            .list(Some(&Path::from("_scry/alerts/v1/transitions")))
            .try_collect()
            .await
            .unwrap();
        assert_eq!(listed.len(), 1, "no transition for an unchanged status");

        let (loser_record, loser_head) =
            transition(&rule, alert_state(AlertStatus::Inactive, 2, 3));
        assert!(matches!(
            store
                .commit_state(&loser_head, Some(&loser_record), Some(current.version))
                .await,
            Err(AlertStoreError::Conflict { .. })
        ));
        let winner = store
            .read_state_head(rule.id, DEPLOYMENT)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(winner.value, advanced);
        assert_eq!(
            store
                .read_latest_transition(&winner.value)
                .await
                .unwrap()
                .value,
            first_record
        );
        // A competing creator of the first head is a conflict, not a collision.
        let (_, other_head) = transition(&rule, alert_state(AlertStatus::Pending, 1, 9));
        assert!(matches!(
            store.commit_state(&other_head, None, None).await,
            Err(AlertStoreError::Conflict { .. })
        ));
    }

    #[tokio::test]
    async fn state_head_from_another_deployment_is_corrupt() {
        let backend = InMemory::new();
        let store = AlertStore::new(&backend);
        let rule = monitor(MonitorId::new(), 1);
        let (record, head) = transition(&rule, alert_state(AlertStatus::Firing, 1, 1));
        store
            .commit_state(&head, Some(&record), None)
            .await
            .unwrap();
        assert!(matches!(
            store.read_state_head(rule.id, "another-deployment").await,
            Err(AlertStoreError::Corrupt { .. })
        ));
    }

    /// Wraps `InMemory`, counting HEAD requests and optionally reporting a
    /// committed conditional PUT as a precondition failure — what a 5xx
    /// followed by `object_store`'s automatic retry looks like.
    #[derive(Debug)]
    struct FlakyStore {
        inner: InMemory,
        heads: AtomicUsize,
        commit_then_412: AtomicBool,
    }

    impl FlakyStore {
        fn new() -> Self {
            Self {
                inner: InMemory::new(),
                heads: AtomicUsize::new(0),
                commit_then_412: AtomicBool::new(false),
            }
        }
    }

    impl std::fmt::Display for FlakyStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "FlakyStore")
        }
    }

    #[async_trait]
    impl ObjectStore for FlakyStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            options: PutOptions,
        ) -> object_store::Result<PutResult> {
            let update = matches!(options.mode, object_store::PutMode::Update(_));
            let result = self.inner.put_opts(location, payload, options).await?;
            if update && self.commit_then_412.swap(false, Ordering::SeqCst) {
                return Err(ObjectError::Precondition {
                    path: location.to_string(),
                    source: "retry after committed attempt".into(),
                });
            }
            Ok(result)
        }
        async fn put_multipart_opts(
            &self,
            location: &Path,
            options: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, options).await
        }
        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            if options.head {
                self.heads.fetch_add(1, Ordering::SeqCst);
            }
            self.inner.get_opts(location, options).await
        }
        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<Path>>,
        ) -> BoxStream<'static, object_store::Result<Path>> {
            self.inner.delete_stream(locations)
        }
        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }
        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }
        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    #[tokio::test]
    async fn reads_use_one_get_and_a_committed_retry_is_success() {
        let backend = Arc::new(FlakyStore::new());
        let store = AlertStore::new(backend.as_ref());
        let id = MonitorId::new();
        store
            .create_rule_revision(&monitor(id, 1), "c1")
            .await
            .unwrap();
        backend.commit_then_412.store(true, Ordering::SeqCst);
        store
            .create_rule_revision(&monitor(id, 2), "c2")
            .await
            .expect("a CAS that committed before its retried 412 is success");
        assert_eq!(store.read_rule(id).await.unwrap().value.revision, 2);
        assert_eq!(backend.heads.load(Ordering::SeqCst), 0, "no HEAD-then-GET");

        // A genuine competing write is still a conflict.
        let head = store.read_rule_head(id).await.unwrap();
        store
            .create_rule_revision(&monitor(id, 3), "c3")
            .await
            .unwrap();
        let mut stale = head.value.clone();
        stale.updated_at_unix_nano = 42;
        assert!(matches!(
            store.update(&rule_head_key(id), &stale, head.version).await,
            Err(AlertStoreError::Conflict { .. })
        ));
    }

    #[tokio::test]
    async fn oversized_and_undecodable_objects_are_rejected() {
        let backend = InMemory::new();
        let store = AlertStore::new(&backend);
        let path = Path::from("_scry/alerts/v1/commands/big.json");
        backend
            .put(
                &path,
                PutPayload::from(vec![b' '; MAX_CONTROL_OBJECT_BYTES as usize + 1]),
            )
            .await
            .unwrap();
        assert!(matches!(
            store.read_record::<RuleHead>(path.as_ref()).await,
            Err(AlertStoreError::Oversized { .. })
        ));
        let garbage = Path::from("_scry/alerts/v1/commands/garbage.json");
        backend
            .put(&garbage, PutPayload::from_static(b"{not json"))
            .await
            .unwrap();
        let error = store
            .read_record::<RuleHead>(garbage.as_ref())
            .await
            .unwrap_err();
        assert!(matches!(error, AlertStoreError::Decode { .. }));
        assert!(!error.is_transient());
    }
}
