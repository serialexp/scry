use bytes::Bytes;
use object_store::{
    path::Path, Error as ObjectError, GetOptions, ObjectMeta, ObjectStore, ObjectStoreExt,
    PutPayload, UpdateVersion,
};
use serde::{de::DeserializeOwned, Serialize};
use thiserror::Error;

use crate::{
    canonical_json, rule_head, rule_head_key, rule_mutation_receipt_key, rule_revision_key,
    rule_tombstone_key, state_head_key, Monitor, MonitorId, RuleHead, RuleMutationReceipt,
    RuleTombstone, StateHead, TransitionRecord,
};

const MAX_CONTROL_OBJECT_BYTES: u64 = 1024 * 1024;

#[derive(Debug, Error)]
pub enum AlertStoreError {
    #[error("serializing alert record: {0}")]
    Serialize(#[from] serde_json::Error),
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

    pub async fn create_rule_revision(&self, monitor: &Monitor) -> Result<(), AlertStoreError> {
        let revision_path = rule_revision_key(monitor.id, monitor.revision);
        self.create_identical(&revision_path, monitor).await?;
        let head = rule_head(monitor);
        let head_path = rule_head_key(monitor.id);
        match self.read_versioned::<RuleHead>(&head_path).await {
            Ok(current) => {
                if current.value.deleted || current.value.revision >= monitor.revision {
                    if current.value == head {
                        return Ok(());
                    }
                    return Err(AlertStoreError::Conflict { path: head_path });
                }
                self.update(&head_path, &head, current.version).await?;
            }
            Err(AlertStoreError::Missing { .. }) => {
                self.create_identical(&head_path, &head).await?;
            }
            Err(error) => return Err(error),
        }
        Ok(())
    }

    pub async fn read_rule(&self, id: MonitorId) -> Result<Versioned<Monitor>, AlertStoreError> {
        let head = self.read_versioned::<RuleHead>(&rule_head_key(id)).await?;
        if head.value.deleted {
            return Err(AlertStoreError::Missing {
                path: rule_head_key(id),
            });
        }
        self.read_versioned(&head.value.revision_key).await
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
        self.read_versioned(&rule_head_key(id)).await
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
        match self
            .read_versioned(&rule_mutation_receipt_key(command_id))
            .await
        {
            Ok(receipt) => Ok(Some(receipt.value)),
            Err(AlertStoreError::Missing { .. }) => Ok(None),
            Err(error) => Err(error),
        }
    }

    pub async fn read_tombstone(
        &self,
        id: MonitorId,
        command_id: &str,
    ) -> Result<Option<RuleTombstone>, AlertStoreError> {
        match self
            .read_versioned(&rule_tombstone_key(id, command_id))
            .await
        {
            Ok(tombstone) => Ok(Some(tombstone.value)),
            Err(AlertStoreError::Missing { .. }) => Ok(None),
            Err(error) => Err(error),
        }
    }

    pub async fn read_state_head(
        &self,
        id: MonitorId,
    ) -> Result<Option<Versioned<StateHead>>, AlertStoreError> {
        match self.read_versioned(&state_head_key(id)).await {
            Ok(head) => Ok(Some(head)),
            Err(AlertStoreError::Missing { .. }) => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// Publish an immutable transition before advancing its visibility head.
    pub async fn commit_transition(
        &self,
        key: &str,
        transition: &TransitionRecord,
        head: &StateHead,
        expected_head: Option<UpdateVersion>,
    ) -> Result<(), AlertStoreError> {
        self.create_identical(key, transition).await?;
        let head_path = state_head_key(transition.monitor_id);
        match expected_head {
            Some(version) => self.update(&head_path, head, version).await,
            None => self.create_identical(&head_path, head).await,
        }
    }

    pub async fn read_transition(
        &self,
        key: &str,
    ) -> Result<Versioned<TransitionRecord>, AlertStoreError> {
        self.read_versioned(key).await
    }

    pub async fn read_current_transition(
        &self,
        head: &StateHead,
    ) -> Result<Versioned<TransitionRecord>, AlertStoreError> {
        let transition = self.read_transition(&head.transition_key).await?;
        let bytes = canonical_json(&transition.value)?;
        if transition.value.schema_version != crate::ALERT_RECORD_SCHEMA_VERSION
            || head.schema_version != crate::ALERT_RECORD_SCHEMA_VERSION
            || transition.value.deployment_id != head.deployment_id
            || transition.value.monitor_id != head.monitor_id
            || transition.value.monitor_revision != transition.value.state.monitor_revision
            || transition.value.slot_id != transition.value.state.last_slot_id
            || transition.value.state.transition_sequence != head.transition_sequence
            || crate::transition_key(
                transition.value.monitor_id,
                transition.value.state.transition_sequence,
                transition.value.slot_id,
            ) != head.transition_key
            || crate::sha256_hex(&bytes) != head.transition_sha256
        {
            return Err(AlertStoreError::Corrupt {
                path: head.transition_key.clone(),
                message: "state head and transition record do not match",
            });
        }
        Ok(transition)
    }

    async fn create_identical<T: Serialize>(
        &self,
        key: &str,
        value: &T,
    ) -> Result<(), AlertStoreError> {
        let bytes = canonical_json(value)?;
        if bytes.len() as u64 > MAX_CONTROL_OBJECT_BYTES {
            return Err(AlertStoreError::Oversized {
                path: key.to_owned(),
            });
        }
        let path = Path::from(key);
        match scry_objstore::put_create(self.store, &path, PutPayload::from(bytes.clone())).await {
            Ok(_) => Ok(()),
            Err(ObjectError::AlreadyExists { .. }) => {
                let existing = self.read_bytes(&path).await?;
                if existing.as_ref() == bytes.as_slice() {
                    Ok(())
                } else {
                    Err(AlertStoreError::Collision {
                        path: key.to_owned(),
                    })
                }
            }
            Err(source) => Err(AlertStoreError::Object {
                path: key.to_owned(),
                source,
            }),
        }
    }

    async fn update<T: Serialize>(
        &self,
        key: &str,
        value: &T,
        version: UpdateVersion,
    ) -> Result<(), AlertStoreError> {
        let bytes = canonical_json(value)?;
        if bytes.len() as u64 > MAX_CONTROL_OBJECT_BYTES {
            return Err(AlertStoreError::Oversized {
                path: key.to_owned(),
            });
        }
        let path = Path::from(key);
        scry_objstore::put_update(self.store, &path, PutPayload::from(bytes), version)
            .await
            .map(|_| ())
            .map_err(|source| match source {
                ObjectError::Precondition { .. } | ObjectError::NotModified { .. } => {
                    AlertStoreError::Conflict {
                        path: key.to_owned(),
                    }
                }
                source => AlertStoreError::Object {
                    path: key.to_owned(),
                    source,
                },
            })
    }

    async fn read_versioned<T: DeserializeOwned>(
        &self,
        key: &str,
    ) -> Result<Versioned<T>, AlertStoreError> {
        let path = Path::from(key);
        let meta = self
            .store
            .head(&path)
            .await
            .map_err(|source| match source {
                ObjectError::NotFound { .. } => AlertStoreError::Missing {
                    path: key.to_owned(),
                },
                source => AlertStoreError::Object {
                    path: key.to_owned(),
                    source,
                },
            })?;
        let version = update_version(&meta).ok_or_else(|| AlertStoreError::Object {
            path: key.to_owned(),
            source: ObjectError::Generic {
                store: "alert-store",
                source: "object has neither ETag nor version".into(),
            },
        })?;
        let bytes = self.read_versioned_bytes(&path, &meta).await?;
        let value = serde_json::from_slice(&bytes)?;
        Ok(Versioned { value, version })
    }

    async fn read_versioned_bytes(
        &self,
        path: &Path,
        meta: &ObjectMeta,
    ) -> Result<Bytes, AlertStoreError> {
        if meta.size > MAX_CONTROL_OBJECT_BYTES {
            return Err(AlertStoreError::Oversized {
                path: path.to_string(),
            });
        }
        self.store
            .get_opts(
                path,
                GetOptions {
                    if_match: meta.e_tag.clone(),
                    version: meta.version.clone(),
                    range: Some((0..meta.size).into()),
                    ..Default::default()
                },
            )
            .await
            .map_err(|source| AlertStoreError::Object {
                path: path.to_string(),
                source,
            })?
            .bytes()
            .await
            .map_err(|source| AlertStoreError::Object {
                path: path.to_string(),
                source,
            })
    }

    async fn read_bytes(&self, path: &Path) -> Result<Bytes, AlertStoreError> {
        let meta = self
            .store
            .head(path)
            .await
            .map_err(|source| AlertStoreError::Object {
                path: path.to_string(),
                source,
            })?;
        if meta.size > MAX_CONTROL_OBJECT_BYTES {
            return Err(AlertStoreError::Oversized {
                path: path.to_string(),
            });
        }
        self.store
            .get_opts(
                path,
                GetOptions {
                    range: Some((0..meta.size).into()),
                    ..Default::default()
                },
            )
            .await
            .map_err(|source| AlertStoreError::Object {
                path: path.to_string(),
                source,
            })?
            .bytes()
            .await
            .map_err(|source| AlertStoreError::Object {
                path: path.to_string(),
                source,
            })
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
    use object_store::memory::InMemory;

    use crate::{
        AlertState, AlertStatus, Comparator, ExecutionErrorPolicy, NoDataPolicy, ScalarCondition,
        ScalarQuery, Signal, ALERT_RECORD_SCHEMA_VERSION, MONITOR_SCHEMA_VERSION,
    };

    use super::*;

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

    fn transition(
        rule: &Monitor,
        sequence: u64,
        slot: u64,
    ) -> (String, TransitionRecord, StateHead) {
        let key = crate::transition_key(rule.id, sequence, slot);
        let record = TransitionRecord {
            schema_version: ALERT_RECORD_SCHEMA_VERSION,
            deployment_id: "018f47a2-8b6c-7def-8123-456789abcdef".into(),
            monitor_id: rule.id,
            monitor_revision: rule.revision,
            slot_id: slot,
            evaluated_at_unix_nano: slot,
            observation: crate::DurableObservation::Value { value: 2.0 },
            state: AlertState {
                monitor_revision: rule.revision,
                status: AlertStatus::Firing,
                since_unix_nano: slot,
                last_evaluated_at_unix_nano: slot,
                last_slot_id: slot,
                last_value: Some(2.0),
                last_error_class: None,
                transition_sequence: sequence,
                stale: false,
            },
            previous_transition_key: None,
            notification_intents: vec![],
        };
        let bytes = canonical_json(&record).unwrap();
        let head = StateHead {
            schema_version: ALERT_RECORD_SCHEMA_VERSION,
            deployment_id: record.deployment_id.clone(),
            monitor_id: rule.id,
            transition_sequence: sequence,
            transition_key: key.clone(),
            transition_sha256: crate::sha256_hex(&bytes),
            updated_at_unix_nano: slot,
        };
        (key, record, head)
    }

    #[tokio::test]
    async fn rule_revisions_are_immutable_and_head_advances() {
        let backend = InMemory::new();
        let store = AlertStore::new(&backend);
        let id = MonitorId::new();
        let first = monitor(id, 1);
        store.create_rule_revision(&first).await.unwrap();
        store.create_rule_revision(&first).await.unwrap();
        assert_eq!(store.read_rule(id).await.unwrap().value, first);

        let second = monitor(id, 2);
        store.create_rule_revision(&second).await.unwrap();
        assert_eq!(store.read_rule(id).await.unwrap().value, second);
        assert!(matches!(
            store.create_rule_revision(&first).await,
            Err(AlertStoreError::Conflict { .. })
        ));
    }

    #[tokio::test]
    async fn tombstone_prevents_rule_resurrection() {
        let backend = InMemory::new();
        let store = AlertStore::new(&backend);
        let rule = monitor(MonitorId::new(), 1);
        store.create_rule_revision(&rule).await.unwrap();
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
            store.create_rule_revision(&monitor(rule.id, 2)).await,
            Err(AlertStoreError::Conflict { .. })
        ));
    }

    #[tokio::test]
    async fn state_head_cas_selects_one_successor() {
        let backend = InMemory::new();
        let store = AlertStore::new(&backend);
        let rule = monitor(MonitorId::new(), 1);
        let (first_key, first_record, first_head) = transition(&rule, 1, 1);
        store
            .commit_transition(&first_key, &first_record, &first_head, None)
            .await
            .unwrap();
        let current = store.read_state_head(rule.id).await.unwrap().unwrap();

        let (second_key, second_record, second_head) = transition(&rule, 2, 2);
        store
            .commit_transition(
                &second_key,
                &second_record,
                &second_head,
                Some(current.version.clone()),
            )
            .await
            .unwrap();
        let (loser_key, loser_record, loser_head) = transition(&rule, 2, 3);
        assert!(matches!(
            store
                .commit_transition(
                    &loser_key,
                    &loser_record,
                    &loser_head,
                    Some(current.version),
                )
                .await,
            Err(AlertStoreError::Conflict { .. })
        ));
        let winner = store.read_state_head(rule.id).await.unwrap().unwrap();
        assert_eq!(winner.value.transition_key, second_key);
        assert_eq!(
            store
                .read_transition(&loser_key)
                .await
                .unwrap()
                .value
                .slot_id,
            3,
            "losing candidate remains immutable but unreachable"
        );
    }
}
