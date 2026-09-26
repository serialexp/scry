//! Fold durable rule, state, and target records into `alerts.sqlite`.
//!
//! One bad or briefly unreadable record never aborts a pass. Per record:
//!
//! - a transient object-store error is retried a bounded number of times and
//!   then leaves the existing local row untouched;
//! - an invalid record (undecodable, failing integrity or domain validation)
//!   is quarantined: logged and counted, with the existing local row kept;
//! - a durable tombstone becomes a local tombstone;
//! - a monitor or target that no longer has a durable head is removed.
//!
//! Only a failure to *list* (or of the local database) fails the pass, and so
//! startup. Removal is epoch-guarded: a row folded after the pass captured
//! [`AlertsDb::projection_epoch`] was not visible to its listing and is kept.

use std::{collections::HashSet, time::Duration};

use anyhow::{Context, Result};
use futures::StreamExt;
use object_store::{path::Path as ObjectPath, ObjectStore};
use scry_alert::{
    validate_monitor, AlertState, AlertStore, AlertStoreError, AlertsDb, LocalRevision, Monitor,
    MonitorId, NotificationTarget, NotificationTargetId,
};
use tokio::sync::Mutex;
use uuid::Uuid;

const RULES_PREFIX: &str = "_scry/alerts/v1/rules/";
const TARGETS_PREFIX: &str = "_scry/alerts/v1/targets/";
/// Hard bound on projected monitors; beyond it the listing itself fails.
pub const MAX_MONITORS: usize = 100_000;
/// Hard bound on listed targets (creates are separately limited lower).
pub const MAX_LISTED_TARGETS: usize = 100_000;
const CHUNK: usize = 256;
const CONCURRENT_READS: usize = 16;
const TRANSIENT_ATTEMPTS: u32 = 3;
const RETRY_BACKOFF: Duration = Duration::from_millis(100);

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ProjectionReport {
    pub listed: usize,
    pub live: usize,
    pub deleted: usize,
    pub quarantined: usize,
    pub unavailable: usize,
    pub removed: usize,
}

impl ProjectionReport {
    pub fn log(&self, kind: &'static str) {
        if self.quarantined > 0 || self.unavailable > 0 {
            tracing::warn!(
                kind,
                listed = self.listed,
                live = self.live,
                deleted = self.deleted,
                quarantined = self.quarantined,
                unavailable = self.unavailable,
                removed = self.removed,
                "alert projection reconciled with skipped records"
            );
        } else {
            tracing::debug!(
                kind,
                listed = self.listed,
                live = self.live,
                deleted = self.deleted,
                removed = self.removed,
                "alert projection reconciled"
            );
        }
    }
}

enum Loaded<T> {
    /// `record` is `None` when the local projection already has this revision.
    Live {
        record: Option<T>,
        state: Option<AlertState>,
    },
    Deleted {
        revision: u64,
        deleted_at_unix_nano: u64,
    },
    /// No durable head (never published or lost): not part of the listing.
    Absent,
    Quarantined(String),
    Unavailable(String),
}

/// IDs with a directory directly under `prefix` (`<prefix><uuid>/...`).
async fn list_ids(store: &dyn ObjectStore, prefix: &str, bound: usize) -> Result<Vec<Uuid>> {
    let listing = store
        .list_with_delimiter(Some(&ObjectPath::from(prefix)))
        .await
        .with_context(|| format!("listing {prefix}"))?;
    let mut ids = Vec::with_capacity(listing.common_prefixes.len().min(bound));
    for path in listing.common_prefixes {
        let Some(id) = path.filename().and_then(|name| Uuid::parse_str(name).ok()) else {
            continue;
        };
        ids.push(id);
        if ids.len() > bound {
            anyhow::bail!("{prefix} lists more than {bound} records");
        }
    }
    ids.sort_unstable();
    Ok(ids)
}

/// Every target ID with a durable directory (live, deleted, or unpublished).
pub async fn list_target_ids(store: &dyn ObjectStore) -> Result<Vec<NotificationTargetId>> {
    Ok(list_ids(store, TARGETS_PREFIX, MAX_LISTED_TARGETS)
        .await?
        .into_iter()
        .map(NotificationTargetId)
        .collect())
}

/// Retry transient failures of `load`, then classify the final error.
async fn with_retries<T, F, Fut>(mut load: F) -> Loaded<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<Loaded<T>, AlertStoreError>>,
{
    let mut attempt = 1;
    loop {
        match load().await {
            Ok(loaded) => return loaded,
            Err(error) if error.is_transient() && attempt < TRANSIENT_ATTEMPTS => {
                tokio::time::sleep(RETRY_BACKOFF * attempt).await;
                attempt += 1;
            }
            Err(error) if error.is_transient() => return Loaded::Unavailable(error.to_string()),
            Err(error) => return Loaded::Quarantined(error.to_string()),
        }
    }
}

pub async fn reconcile_monitors(
    store: &dyn ObjectStore,
    db: &Mutex<AlertsDb>,
    deployment_id: &str,
) -> Result<ProjectionReport> {
    let epoch = db.lock().await.projection_epoch();
    let ids = list_ids(store, RULES_PREFIX, MAX_MONITORS).await?;
    let mut report = ProjectionReport::default();
    let mut listed = HashSet::with_capacity(ids.len());
    for chunk in ids.chunks(CHUNK) {
        let local: Vec<Option<LocalRevision>> = {
            let db = db.lock().await;
            chunk
                .iter()
                .map(|id| db.local_monitor_revision(MonitorId(*id)))
                .collect::<Result<_, _>>()?
        };
        let loads: Vec<_> = chunk
            .iter()
            .zip(local)
            .map(|(id, local)| load_monitor_retrying(store, MonitorId(*id), local, deployment_id))
            .collect();
        let loaded: Vec<(MonitorId, Loaded<Monitor>)> = futures::stream::iter(loads)
            .buffered(CONCURRENT_READS)
            .collect()
            .await;
        let mut db = db.lock().await;
        for (id, loaded) in loaded {
            match loaded {
                Loaded::Absent => continue,
                Loaded::Live { record, state } => {
                    report.live += 1;
                    if let Some(monitor) = record {
                        db.fold_rule(&monitor)?;
                    }
                    if let Some(state) = state {
                        db.fold_state(id, &state)?;
                    }
                }
                Loaded::Deleted {
                    revision,
                    deleted_at_unix_nano,
                } => {
                    report.deleted += 1;
                    db.tombstone_monitor(id, revision, deleted_at_unix_nano)?;
                }
                Loaded::Quarantined(reason) => {
                    report.quarantined += 1;
                    tracing::warn!(monitor_id = %id, reason, "quarantined invalid alert records; keeping the existing projection row");
                }
                Loaded::Unavailable(reason) => {
                    report.unavailable += 1;
                    tracing::warn!(monitor_id = %id, reason, "alert records unavailable; keeping the existing projection row");
                }
            }
            listed.insert(id);
        }
    }
    report.listed = listed.len();
    report.removed = db
        .lock()
        .await
        .prune_monitors(epoch, &listed)
        .context("removing unlisted monitors")?;
    Ok(report)
}

async fn load_monitor_retrying(
    store: &dyn ObjectStore,
    id: MonitorId,
    local: Option<LocalRevision>,
    deployment_id: &str,
) -> (MonitorId, Loaded<Monitor>) {
    let alert_store = AlertStore::new(store);
    let loaded = with_retries(|| load_monitor(&alert_store, id, local, deployment_id)).await;
    (id, loaded)
}

async fn load_monitor(
    alert_store: &AlertStore<'_>,
    id: MonitorId,
    local: Option<LocalRevision>,
    deployment_id: &str,
) -> Result<Loaded<Monitor>, AlertStoreError> {
    let head = match alert_store.read_rule_head(id).await {
        Ok(head) => head.value,
        Err(AlertStoreError::Missing { .. }) => return Ok(Loaded::Absent),
        Err(error) => return Err(error),
    };
    if head.deleted {
        return Ok(Loaded::Deleted {
            revision: head.revision,
            deleted_at_unix_nano: head.updated_at_unix_nano,
        });
    }
    let have_revision =
        local.is_some_and(|local| !local.deleted && local.revision >= head.revision);
    let record = if have_revision {
        None
    } else {
        let monitor = alert_store.read_rule_revision(&head).await?.value;
        if let Err(error) = validate_monitor(&monitor) {
            return Ok(Loaded::Quarantined(format!("invalid monitor: {error}")));
        }
        Some(monitor)
    };
    let state = alert_store
        .read_state_head(id, deployment_id)
        .await?
        .map(|head| head.value.state);
    Ok(Loaded::Live { record, state })
}

pub async fn reconcile_targets(
    store: &dyn ObjectStore,
    db: &Mutex<AlertsDb>,
) -> Result<ProjectionReport> {
    let epoch = db.lock().await.projection_epoch();
    let ids = list_ids(store, TARGETS_PREFIX, MAX_LISTED_TARGETS).await?;
    let mut report = ProjectionReport::default();
    let mut listed = HashSet::with_capacity(ids.len());
    for chunk in ids.chunks(CHUNK) {
        let local: Vec<Option<LocalRevision>> = {
            let db = db.lock().await;
            chunk
                .iter()
                .map(|id| db.local_notification_target_revision(NotificationTargetId(*id)))
                .collect::<Result<_, _>>()?
        };
        let loads: Vec<_> = chunk
            .iter()
            .zip(local)
            .map(|(id, local)| load_target_retrying(store, NotificationTargetId(*id), local))
            .collect();
        let loaded: Vec<(NotificationTargetId, Loaded<NotificationTarget>)> =
            futures::stream::iter(loads)
                .buffered(CONCURRENT_READS)
                .collect()
                .await;
        let mut db = db.lock().await;
        for (id, loaded) in loaded {
            match loaded {
                Loaded::Absent => continue,
                Loaded::Live { record, .. } => {
                    report.live += 1;
                    if let Some(target) = record {
                        db.fold_notification_target(&target)?;
                    }
                }
                Loaded::Deleted {
                    revision,
                    deleted_at_unix_nano,
                } => {
                    report.deleted += 1;
                    db.tombstone_notification_target(id, revision, deleted_at_unix_nano)?;
                }
                Loaded::Quarantined(reason) => {
                    report.quarantined += 1;
                    tracing::warn!(target_id = %id, reason, "quarantined invalid notification target; keeping the existing projection row");
                }
                Loaded::Unavailable(reason) => {
                    report.unavailable += 1;
                    tracing::warn!(target_id = %id, reason, "notification target unavailable; keeping the existing projection row");
                }
            }
            listed.insert(id);
        }
    }
    report.listed = listed.len();
    report.removed = db
        .lock()
        .await
        .prune_notification_targets(epoch, &listed)
        .context("removing unlisted notification targets")?;
    Ok(report)
}

async fn load_target_retrying(
    store: &dyn ObjectStore,
    id: NotificationTargetId,
    local: Option<LocalRevision>,
) -> (NotificationTargetId, Loaded<NotificationTarget>) {
    let alert_store = AlertStore::new(store);
    let loaded = with_retries(|| load_target(&alert_store, id, local)).await;
    (id, loaded)
}

async fn load_target(
    alert_store: &AlertStore<'_>,
    id: NotificationTargetId,
    local: Option<LocalRevision>,
) -> Result<Loaded<NotificationTarget>, AlertStoreError> {
    let head = match alert_store.read_target_head(id).await {
        Ok(head) => head.value,
        Err(AlertStoreError::Missing { .. }) => return Ok(Loaded::Absent),
        Err(error) => return Err(error),
    };
    if head.deleted {
        return Ok(Loaded::Deleted {
            revision: head.revision,
            deleted_at_unix_nano: head.updated_at_unix_nano,
        });
    }
    if local.is_some_and(|local| !local.deleted && local.revision >= head.revision) {
        return Ok(Loaded::Live {
            record: None,
            state: None,
        });
    }
    let target = alert_store.read_target_revision(&head).await?.value;
    if let Err(error) = target.validate() {
        return Ok(Loaded::Quarantined(format!(
            "invalid notification target: {error}"
        )));
    }
    Ok(Loaded::Live {
        record: Some(target),
        state: None,
    })
}

/// A full startup/periodic pass over monitors and targets. Each half is
/// independent: one failing to list does not skip the other.
pub async fn reconcile_all(
    store: &dyn ObjectStore,
    db: &Mutex<AlertsDb>,
    deployment_id: &str,
) -> Result<()> {
    let monitors = reconcile_monitors(store, db, deployment_id).await;
    let targets = reconcile_targets(store, db).await;
    match &monitors {
        Ok(report) => report.log("monitors"),
        Err(error) => tracing::warn!(error = %error, "alert monitor projection pass failed"),
    }
    match &targets {
        Ok(report) => report.log("notification_targets"),
        Err(error) => tracing::warn!(error = %error, "notification-target projection pass failed"),
    }
    monitors?;
    targets?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use object_store::{memory::InMemory, ObjectStoreExt, PutPayload};
    use scry_alert::{
        rule_head_key, Comparator, ExecutionErrorPolicy, NoDataPolicy, RuleTombstone,
        ScalarCondition, ScalarQuery, Signal, ALERT_RECORD_SCHEMA_VERSION, MONITOR_SCHEMA_VERSION,
    };

    use super::*;

    fn monitor() -> Monitor {
        Monitor {
            schema_version: MONITOR_SCHEMA_VERSION,
            id: MonitorId::new(),
            revision: 1,
            name: "m".into(),
            enabled: true,
            query: ScalarQuery {
                target_id: "local".into(),
                signal: Signal::Metrics,
                matchers: vec![],
                lookback_seconds: 60,
                sql: "SELECT count(*) AS value FROM metrics".into(),
            },
            condition: ScalarCondition {
                comparator: Comparator::Gt,
                threshold: 10.0,
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
            updated_at_unix_nano: 1,
        }
    }

    fn command() -> String {
        Uuid::new_v4().to_string()
    }

    #[tokio::test]
    async fn one_corrupt_record_is_quarantined_without_failing_the_pass() {
        let store = Arc::new(InMemory::new());
        let deployment = Uuid::new_v4();
        let db = Mutex::new(AlertsDb::open_in_memory(deployment).unwrap());
        let alerts = AlertStore::new(store.as_ref());
        let good = monitor();
        alerts
            .create_rule_revision(&good, &command())
            .await
            .unwrap();
        let corrupt = monitor();
        alerts
            .create_rule_revision(&corrupt, &command())
            .await
            .unwrap();
        // Previously projected, then its head is corrupted.
        db.lock().await.fold_rule(&corrupt).unwrap();
        store
            .put(
                &ObjectPath::from(rule_head_key(corrupt.id)),
                PutPayload::from_static(b"{not json"),
            )
            .await
            .unwrap();

        let report = reconcile_monitors(store.as_ref(), &db, &deployment.to_string())
            .await
            .unwrap();
        assert_eq!(report.live, 1);
        assert_eq!(report.quarantined, 1);
        assert_eq!(report.removed, 0);
        let db = db.lock().await;
        assert!(db.get_monitor(good.id).unwrap().is_some());
        assert!(
            db.get_monitor(corrupt.id).unwrap().is_some(),
            "the existing row is kept"
        );
    }

    #[tokio::test]
    async fn deleted_and_vanished_monitors_leave_the_projection() {
        let store = Arc::new(InMemory::new());
        let deployment = Uuid::new_v4();
        let db = Mutex::new(AlertsDb::open_in_memory(deployment).unwrap());
        let alerts = AlertStore::new(store.as_ref());
        let deleted = monitor();
        let delete_command = command();
        alerts
            .create_rule_revision(&deleted, &command())
            .await
            .unwrap();
        let head = alerts.read_rule_head(deleted.id).await.unwrap();
        alerts
            .tombstone_rule(
                &RuleTombstone {
                    schema_version: ALERT_RECORD_SCHEMA_VERSION,
                    monitor_id: deleted.id,
                    revision: 1,
                    command_id: delete_command,
                    deleted_at_unix_nano: 9,
                },
                head.version,
            )
            .await
            .unwrap();
        // Known locally but never durably published (e.g. a lost bucket).
        let vanished = monitor();
        {
            let mut db = db.lock().await;
            db.fold_rule(&deleted).unwrap();
            db.fold_rule(&vanished).unwrap();
        }
        let report = reconcile_monitors(store.as_ref(), &db, &deployment.to_string())
            .await
            .unwrap();
        assert_eq!(report.deleted, 1);
        assert_eq!(report.removed, 1);
        let db = db.lock().await;
        assert!(db.get_monitor(deleted.id).unwrap().is_none());
        assert!(
            db.local_monitor_revision(deleted.id)
                .unwrap()
                .unwrap()
                .deleted
        );
        assert!(db.get_monitor(vanished.id).unwrap().is_none());
        assert!(db.local_monitor_revision(vanished.id).unwrap().is_none());
    }

    #[tokio::test]
    async fn invalid_target_is_quarantined() {
        let store = Arc::new(InMemory::new());
        let db = Mutex::new(AlertsDb::open_in_memory(Uuid::new_v4()).unwrap());
        let target = NotificationTarget {
            schema_version: ALERT_RECORD_SCHEMA_VERSION,
            id: NotificationTargetId::new(),
            revision: 1,
            name: "t".into(),
            enabled: true,
            kind: scry_alert::NotificationTargetKind::GenericWebhook {
                url: "https://example.com/hook".into(),
                headers: vec![scry_alert::TargetHeader {
                    name: "Content-Type".into(),
                    value: "text/plain".into(),
                }],
            },
            format: scry_alert::TargetFormat::BuiltIn {
                format: scry_alert::BuiltInTargetFormat::GenericJson,
            },
            timeout_millis: 1_000,
            logical_secret_id: scry_alert::LogicalSecretId::new(),
            secret_generation: 1,
            created_at_unix_nano: 1,
            updated_at_unix_nano: 1,
        };
        AlertStore::new(store.as_ref())
            .create_target_revision(&target, &command(), None)
            .await
            .unwrap();
        let report = reconcile_targets(store.as_ref(), &db).await.unwrap();
        assert_eq!(report.quarantined, 1);
        assert!(db
            .lock()
            .await
            .get_notification_target(target.id)
            .unwrap()
            .is_none());
    }
}
