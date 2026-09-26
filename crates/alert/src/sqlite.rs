use std::{collections::HashSet, path::Path};

use rusqlite::{params, Connection, OptionalExtension};
use thiserror::Error;
use uuid::Uuid;

use crate::{
    AlertState, AlertStatus, Monitor, MonitorId, NotificationTarget, NotificationTargetId,
    NotificationTargetProjection,
};

/// Version 3 drops the never-read `transitions` table (state heads embed the
/// current state, so the projection keeps only `current_state`), keeps local
/// deletion tombstones for monitors and targets, orders state by
/// `(monitor_revision, last_slot_id)`, and stores the scheduling fields as
/// columns. Older projections are dropped and rebuilt from object storage.
pub const ALERTS_SCHEMA_VERSION: u32 = 3;

#[derive(Debug, Error)]
pub enum AlertsDbError {
    #[error("SQLite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("alerts database belongs to deployment {found}, expected {expected}")]
    DeploymentMismatch { expected: String, found: String },
    #[error("invalid stored alert record: {0}")]
    Decode(#[from] serde_json::Error),
}

/// Local, rebuildable fold of the durable alert records.
///
/// Every fold is monotonic, so a reconciliation pass that loaded durable
/// records before a concurrent local mutation cannot move a row backwards:
/// rules and targets only advance to higher revisions, state only advances in
/// `(monitor_revision, last_slot_id)` order, and a deletion leaves a local
/// tombstone row that a stale live record cannot resurrect.
pub struct AlertsDb {
    conn: Connection,
    deployment_id: Uuid,
    /// Last stamped `local_seq`; see [`AlertsDb::projection_epoch`].
    local_seq: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct MonitorSummary {
    pub monitor: Monitor,
    pub state: Option<AlertState>,
}

/// The columns the scheduler needs to decide whether a monitor is due,
/// without decoding its JSON record.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ScheduleEntry {
    pub monitor_id: MonitorId,
    pub revision: u64,
    pub enabled: bool,
    pub every_seconds: u64,
    pub jitter_seconds: u64,
    pub state: Option<ScheduledState>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ScheduledState {
    pub monitor_revision: u64,
    pub last_slot_id: u64,
    pub status: AlertStatus,
}

impl ScheduledState {
    pub fn covers(&self, revision: u64, slot_id: u64) -> bool {
        (self.monitor_revision, self.last_slot_id) >= (revision, slot_id)
    }
}

/// Local projection row version, used to skip re-reading unchanged immutable
/// revisions during reconciliation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LocalRevision {
    pub revision: u64,
    pub deleted: bool,
}

impl AlertsDb {
    pub fn open(path: &Path, deployment_id: Uuid) -> Result<Self, AlertsDbError> {
        Self::from_connection(Connection::open(path)?, deployment_id)
    }

    pub fn open_in_memory(deployment_id: Uuid) -> Result<Self, AlertsDbError> {
        Self::from_connection(Connection::open_in_memory()?, deployment_id)
    }

    fn from_connection(mut conn: Connection, deployment_id: Uuid) -> Result<Self, AlertsDbError> {
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        migrate(&conn)?;
        bind_deployment(&mut conn, deployment_id)?;
        let local_seq: u64 = conn.query_row(
            "SELECT max(
               (SELECT coalesce(max(local_seq), 0) FROM monitors),
               (SELECT coalesce(max(local_seq), 0) FROM notification_targets))",
            [],
            |row| row.get(0),
        )?;
        Ok(Self {
            conn,
            deployment_id,
            local_seq,
        })
    }

    fn next_seq(&mut self) -> u64 {
        self.local_seq += 1;
        self.local_seq
    }

    /// Every row folded after this call carries a larger `local_seq`. A
    /// reconciliation pass captures the epoch before listing object storage
    /// so [`Self::prune_monitors`] never removes a row it could not have seen.
    pub fn projection_epoch(&self) -> u64 {
        self.local_seq
    }

    /// Fold a live rule revision. Returns whether the row changed; older or
    /// equal revisions and locally tombstoned monitors are left untouched.
    pub fn fold_rule(&mut self, monitor: &Monitor) -> Result<bool, AlertsDbError> {
        let json = serde_json::to_vec(monitor)?;
        let seq = self.next_seq();
        let changed = self.conn.execute(
            "INSERT INTO monitors(monitor_id, revision, deleted, enabled, every_seconds,
               jitter_seconds, name, updated_at_unix_nano, record_json, local_seq)
             VALUES (?1, ?2, 0, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
             ON CONFLICT(monitor_id) DO UPDATE SET
               revision = excluded.revision,
               enabled = excluded.enabled,
               every_seconds = excluded.every_seconds,
               jitter_seconds = excluded.jitter_seconds,
               name = excluded.name,
               updated_at_unix_nano = excluded.updated_at_unix_nano,
               record_json = excluded.record_json,
               local_seq = excluded.local_seq
             WHERE monitors.deleted = 0 AND excluded.revision > monitors.revision",
            params![
                monitor.id.0.as_bytes().as_slice(),
                monitor.revision,
                monitor.enabled,
                monitor.every_seconds,
                monitor.jitter_seconds,
                monitor.name,
                monitor.updated_at_unix_nano,
                json,
                seq,
            ],
        )?;
        Ok(changed > 0)
    }

    /// Record a durable deletion. The tombstone row keeps the deleted revision
    /// so a reconciliation pass that loaded the live record earlier cannot
    /// re-insert it.
    pub fn tombstone_monitor(
        &mut self,
        id: MonitorId,
        revision: u64,
        deleted_at_unix_nano: u64,
    ) -> Result<(), AlertsDbError> {
        let seq = self.next_seq();
        let tx = self.conn.transaction()?;
        tx.execute(
            "INSERT INTO monitors(monitor_id, revision, deleted, enabled, every_seconds,
               jitter_seconds, name, updated_at_unix_nano, record_json, local_seq)
             VALUES (?1, ?2, 1, 0, 0, 0, '', ?3, NULL, ?4)
             ON CONFLICT(monitor_id) DO UPDATE SET
               revision = max(monitors.revision, excluded.revision),
               deleted = 1,
               enabled = 0,
               updated_at_unix_nano = excluded.updated_at_unix_nano,
               record_json = NULL,
               local_seq = excluded.local_seq
             WHERE monitors.deleted = 0",
            params![
                id.0.as_bytes().as_slice(),
                revision,
                deleted_at_unix_nano,
                seq
            ],
        )?;
        tx.execute(
            "DELETE FROM current_state WHERE monitor_id = ?1",
            [id.0.as_bytes().as_slice()],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Remove every local trace of a monitor (projection loss, or a record
    /// that no longer exists durably). Unlike a tombstone, a later fold may
    /// recreate it.
    pub fn forget_monitor(&mut self, id: MonitorId) -> Result<(), AlertsDbError> {
        let tx = self.conn.transaction()?;
        tx.execute(
            "DELETE FROM current_state WHERE monitor_id = ?1",
            [id.0.as_bytes().as_slice()],
        )?;
        tx.execute(
            "DELETE FROM monitors WHERE monitor_id = ?1",
            [id.0.as_bytes().as_slice()],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Fold a monitor's current state from its durable state head. Only a
    /// state newer in `(monitor_revision, last_slot_id)` order replaces the
    /// row, and a locally tombstoned monitor never regains state.
    pub fn fold_state(&mut self, id: MonitorId, state: &AlertState) -> Result<bool, AlertsDbError> {
        let changed = self.conn.execute(
            "INSERT INTO current_state(
               monitor_id, monitor_revision, last_slot_id, sequence, status, stale,
               since_unix_nano, last_evaluated_at_unix_nano, state_json
             )
             SELECT ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9
             WHERE NOT EXISTS (SELECT 1 FROM monitors WHERE monitor_id = ?1 AND deleted = 1)
             ON CONFLICT(monitor_id) DO UPDATE SET
               monitor_revision = excluded.monitor_revision,
               last_slot_id = excluded.last_slot_id,
               sequence = excluded.sequence,
               status = excluded.status,
               stale = excluded.stale,
               since_unix_nano = excluded.since_unix_nano,
               last_evaluated_at_unix_nano = excluded.last_evaluated_at_unix_nano,
               state_json = excluded.state_json
             WHERE (excluded.monitor_revision, excluded.last_slot_id)
                 > (current_state.monitor_revision, current_state.last_slot_id)",
            params![
                id.0.as_bytes().as_slice(),
                state.monitor_revision,
                state.last_slot_id,
                state.transition_sequence,
                status_code(state.status),
                state.stale,
                state.since_unix_nano,
                state.last_evaluated_at_unix_nano,
                serde_json::to_vec(state)?,
            ],
        )?;
        Ok(changed > 0)
    }

    pub fn local_monitor_revision(
        &self,
        id: MonitorId,
    ) -> Result<Option<LocalRevision>, AlertsDbError> {
        Ok(self
            .conn
            .query_row(
                "SELECT revision, deleted FROM monitors WHERE monitor_id = ?1",
                [id.0.as_bytes().as_slice()],
                |row| {
                    Ok(LocalRevision {
                        revision: row.get(0)?,
                        deleted: row.get(1)?,
                    })
                },
            )
            .optional()?)
    }

    /// Hard-delete monitor rows stamped at or before `epoch` whose IDs are not
    /// in `listed` (a complete durable listing started after `epoch`).
    /// Returns the number of monitors removed.
    pub fn prune_monitors(
        &mut self,
        epoch: u64,
        listed: &HashSet<MonitorId>,
    ) -> Result<usize, AlertsDbError> {
        let stale = self.unlisted_ids("monitors", "monitor_id", epoch, |id| {
            listed.contains(&MonitorId(id))
        })?;
        for id in &stale {
            self.forget_monitor(MonitorId(*id))?;
        }
        Ok(stale.len())
    }

    fn unlisted_ids(
        &self,
        table: &'static str,
        id_column: &'static str,
        epoch: u64,
        listed: impl Fn(Uuid) -> bool,
    ) -> Result<Vec<Uuid>, AlertsDbError> {
        let mut statement = self.conn.prepare(&format!(
            "SELECT {id_column} FROM {table} WHERE local_seq <= ?1"
        ))?;
        let rows = statement.query_map([epoch], |row| row.get::<_, Vec<u8>>(0))?;
        let mut stale = Vec::new();
        for row in rows {
            let bytes = row?;
            let id = Uuid::from_slice(&bytes).map_err(|_| rusqlite::Error::InvalidQuery)?;
            if !listed(id) {
                stale.push(id);
            }
        }
        Ok(stale)
    }

    pub fn fold_notification_target(
        &mut self,
        target: &NotificationTarget,
    ) -> Result<bool, AlertsDbError> {
        let projection = NotificationTargetProjection::from(target);
        let json = serde_json::to_vec(&projection)?;
        let seq = self.next_seq();
        let changed = self.conn.execute(
            "INSERT INTO notification_targets(target_id, revision, deleted, enabled, name,
               updated_at_unix_nano, projection_json, local_seq)
             VALUES (?1, ?2, 0, ?3, ?4, ?5, ?6, ?7)
             ON CONFLICT(target_id) DO UPDATE SET revision=excluded.revision,
               enabled=excluded.enabled, name=excluded.name,
               updated_at_unix_nano=excluded.updated_at_unix_nano,
               projection_json=excluded.projection_json, local_seq=excluded.local_seq
             WHERE notification_targets.deleted = 0
               AND excluded.revision > notification_targets.revision",
            params![
                target.id.0.as_bytes().as_slice(),
                target.revision,
                target.enabled,
                target.name,
                target.updated_at_unix_nano,
                json,
                seq
            ],
        )?;
        Ok(changed > 0)
    }

    pub fn tombstone_notification_target(
        &mut self,
        id: NotificationTargetId,
        revision: u64,
        deleted_at_unix_nano: u64,
    ) -> Result<(), AlertsDbError> {
        let seq = self.next_seq();
        self.conn.execute(
            "INSERT INTO notification_targets(target_id, revision, deleted, enabled, name,
               updated_at_unix_nano, projection_json, local_seq)
             VALUES (?1, ?2, 1, 0, '', ?3, NULL, ?4)
             ON CONFLICT(target_id) DO UPDATE SET
               revision = max(notification_targets.revision, excluded.revision),
               deleted = 1, enabled = 0, updated_at_unix_nano = excluded.updated_at_unix_nano,
               projection_json = NULL, local_seq = excluded.local_seq
             WHERE notification_targets.deleted = 0",
            params![
                id.0.as_bytes().as_slice(),
                revision,
                deleted_at_unix_nano,
                seq
            ],
        )?;
        Ok(())
    }

    pub fn forget_notification_target(
        &mut self,
        id: NotificationTargetId,
    ) -> Result<(), AlertsDbError> {
        self.conn.execute(
            "DELETE FROM notification_targets WHERE target_id=?1",
            [id.0.as_bytes().as_slice()],
        )?;
        Ok(())
    }

    pub fn local_notification_target_revision(
        &self,
        id: NotificationTargetId,
    ) -> Result<Option<LocalRevision>, AlertsDbError> {
        Ok(self
            .conn
            .query_row(
                "SELECT revision, deleted FROM notification_targets WHERE target_id = ?1",
                [id.0.as_bytes().as_slice()],
                |row| {
                    Ok(LocalRevision {
                        revision: row.get(0)?,
                        deleted: row.get(1)?,
                    })
                },
            )
            .optional()?)
    }

    /// Target counterpart of [`Self::prune_monitors`].
    pub fn prune_notification_targets(
        &mut self,
        epoch: u64,
        listed: &HashSet<NotificationTargetId>,
    ) -> Result<usize, AlertsDbError> {
        let stale = self.unlisted_ids("notification_targets", "target_id", epoch, |id| {
            listed.contains(&NotificationTargetId(id))
        })?;
        for id in &stale {
            self.forget_notification_target(NotificationTargetId(*id))?;
        }
        Ok(stale.len())
    }

    pub fn get_notification_target(
        &self,
        id: NotificationTargetId,
    ) -> Result<Option<NotificationTargetProjection>, AlertsDbError> {
        let json: Option<Vec<u8>> = self
            .conn
            .query_row(
                "SELECT projection_json FROM notification_targets WHERE target_id=?1 AND deleted=0",
                [id.0.as_bytes().as_slice()],
                |row| row.get(0),
            )
            .optional()?;
        json.as_deref()
            .map(serde_json::from_slice)
            .transpose()
            .map_err(Into::into)
    }

    pub fn notification_target_count(&self) -> Result<usize, AlertsDbError> {
        let count: u64 = self.conn.query_row(
            "SELECT COUNT(*) FROM notification_targets WHERE deleted=0",
            [],
            |row| row.get(0),
        )?;
        Ok(count as usize)
    }

    pub fn list_notification_targets(
        &self,
        after: Option<NotificationTargetId>,
        limit: usize,
    ) -> Result<Vec<NotificationTargetProjection>, AlertsDbError> {
        let limit = limit.clamp(1, 10_000) as u64;
        let after = after.map(|id| id.0.as_bytes().to_vec()).unwrap_or_default();
        // `after` is X'' for the first page: an empty BLOB sorts before every
        // ID, so one range predicate serves all pages and keeps the primary-key
        // seek (an `?1 = X'' OR` form would rescan from the start every page).
        let mut statement = self.conn.prepare_cached(
            "SELECT projection_json FROM notification_targets
             WHERE target_id > ?1 AND deleted = 0 ORDER BY target_id LIMIT ?2",
        )?;
        let rows = statement.query_map(params![after, limit], |row| row.get::<_, Vec<u8>>(0))?;
        let mut output = Vec::with_capacity(limit.min(128) as usize);
        for row in rows {
            output.push(serde_json::from_slice(&row?)?);
        }
        Ok(output)
    }

    pub fn get_monitor(&self, id: MonitorId) -> Result<Option<MonitorSummary>, AlertsDbError> {
        self.conn
            .query_row(
                "SELECT m.record_json, s.state_json
                 FROM monitors m LEFT JOIN current_state s USING(monitor_id)
                 WHERE m.monitor_id = ?1 AND m.deleted = 0",
                [id.0.as_bytes().as_slice()],
                |row| {
                    let monitor: Vec<u8> = row.get(0)?;
                    let state: Option<Vec<u8>> = row.get(1)?;
                    Ok((monitor, state))
                },
            )
            .optional()?
            .map(|(monitor, state)| {
                Ok(MonitorSummary {
                    monitor: serde_json::from_slice(&monitor)?,
                    state: state.as_deref().map(serde_json::from_slice).transpose()?,
                })
            })
            .transpose()
    }

    pub fn list_monitors(
        &self,
        after: Option<MonitorId>,
        limit: usize,
    ) -> Result<Vec<MonitorSummary>, AlertsDbError> {
        let limit = limit.clamp(1, 1_000) as u64;
        let after = after.map(|id| id.0.as_bytes().to_vec()).unwrap_or_default();
        // See `list_notification_targets` for the X'' first-page bound.
        let mut statement = self.conn.prepare_cached(
            "SELECT m.record_json, s.state_json
             FROM monitors m LEFT JOIN current_state s USING(monitor_id)
             WHERE m.monitor_id > ?1 AND m.deleted = 0
             ORDER BY m.monitor_id ASC LIMIT ?2",
        )?;
        let rows = statement.query_map(params![after, limit], |row| {
            Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Option<Vec<u8>>>(1)?))
        })?;
        let mut output = Vec::with_capacity(limit.min(128) as usize);
        for row in rows {
            let (monitor, state) = row?;
            output.push(MonitorSummary {
                monitor: serde_json::from_slice(&monitor)?,
                state: state.as_deref().map(serde_json::from_slice).transpose()?,
            });
        }
        Ok(output)
    }

    /// Scheduling columns of live monitors in ID order after `after`.
    pub fn list_schedule(
        &self,
        after: Option<MonitorId>,
        limit: usize,
    ) -> Result<Vec<ScheduleEntry>, AlertsDbError> {
        let limit = limit.clamp(1, 10_000) as u64;
        let after = after.map(|id| id.0.as_bytes().to_vec()).unwrap_or_default();
        let mut statement = self.conn.prepare_cached(
            "SELECT m.monitor_id, m.revision, m.enabled, m.every_seconds, m.jitter_seconds,
                    s.monitor_revision, s.last_slot_id, s.status
             FROM monitors m LEFT JOIN current_state s USING(monitor_id)
             WHERE m.monitor_id > ?1 AND m.deleted = 0
             ORDER BY m.monitor_id ASC LIMIT ?2",
        )?;
        let rows = statement.query_map(params![after, limit], |row| {
            let id: [u8; 16] = row.get(0)?;
            let state_revision: Option<u64> = row.get(5)?;
            let state = match state_revision {
                Some(monitor_revision) => Some(ScheduledState {
                    monitor_revision,
                    last_slot_id: row.get(6)?,
                    status: status_from_code(row.get(7)?)
                        .ok_or(rusqlite::Error::IntegralValueOutOfRange(7, 0))?,
                }),
                None => None,
            };
            Ok(ScheduleEntry {
                monitor_id: MonitorId(Uuid::from_bytes(id)),
                revision: row.get(1)?,
                enabled: row.get(2)?,
                every_seconds: row.get(3)?,
                jitter_seconds: row.get(4)?,
                state,
            })
        })?;
        let mut output = Vec::with_capacity(limit.min(1_024) as usize);
        for row in rows {
            output.push(row?);
        }
        Ok(output)
    }

    pub fn deployment_id(&self) -> Uuid {
        self.deployment_id
    }
}

const SCHEMA_V3: &str = "
     CREATE TABLE monitors (
       monitor_id BLOB PRIMARY KEY CHECK(length(monitor_id) = 16), revision INTEGER NOT NULL,
       deleted INTEGER NOT NULL, enabled INTEGER NOT NULL, every_seconds INTEGER NOT NULL,
       jitter_seconds INTEGER NOT NULL, name TEXT NOT NULL,
       updated_at_unix_nano INTEGER NOT NULL, record_json BLOB, local_seq INTEGER NOT NULL,
       CHECK(deleted = 1 OR record_json IS NOT NULL)
     ) STRICT, WITHOUT ROWID;
     CREATE TABLE current_state (
       monitor_id BLOB PRIMARY KEY CHECK(length(monitor_id) = 16),
       monitor_revision INTEGER NOT NULL, last_slot_id INTEGER NOT NULL,
       sequence INTEGER NOT NULL, status INTEGER NOT NULL, stale INTEGER NOT NULL,
       since_unix_nano INTEGER NOT NULL, last_evaluated_at_unix_nano INTEGER NOT NULL,
       state_json BLOB NOT NULL
     ) STRICT, WITHOUT ROWID;
     CREATE TABLE notification_targets (
       target_id BLOB PRIMARY KEY CHECK(length(target_id) = 16), revision INTEGER NOT NULL,
       deleted INTEGER NOT NULL, enabled INTEGER NOT NULL, name TEXT NOT NULL,
       updated_at_unix_nano INTEGER NOT NULL, projection_json BLOB, local_seq INTEGER NOT NULL,
       CHECK(deleted = 1 OR projection_json IS NOT NULL)
     ) STRICT, WITHOUT ROWID;";

fn migrate(conn: &Connection) -> Result<(), rusqlite::Error> {
    let version: u32 = conn.pragma_query_value(None, "user_version", |row| row.get(0))?;
    if version > ALERTS_SCHEMA_VERSION {
        return Err(rusqlite::Error::InvalidQuery);
    }
    if version == ALERTS_SCHEMA_VERSION {
        return Ok(());
    }
    // The projection is a rebuildable fold of object storage, so versions 1
    // and 2 are replaced wholesale rather than migrated row by row. Only the
    // deployment binding is carried forward.
    let reset = if version == 0 {
        "CREATE TABLE projection_meta (
           singleton INTEGER PRIMARY KEY CHECK(singleton = 1), deployment_id TEXT NOT NULL
         ) STRICT;"
    } else {
        "DROP TABLE IF EXISTS transitions;
         DROP TABLE IF EXISTS current_state;
         DROP TABLE IF EXISTS monitors;
         DROP TABLE IF EXISTS notification_targets;"
    };
    conn.execute_batch(&format!(
        "BEGIN IMMEDIATE;
         {reset}
         {SCHEMA_V3}
         PRAGMA user_version = {ALERTS_SCHEMA_VERSION};
         COMMIT;"
    ))
}

fn bind_deployment(conn: &mut Connection, expected: Uuid) -> Result<(), AlertsDbError> {
    let tx = conn.transaction()?;
    let found: Option<String> = tx
        .query_row(
            "SELECT deployment_id FROM projection_meta WHERE singleton = 1",
            [],
            |row| row.get(0),
        )
        .optional()?;
    match found {
        Some(found) if found != expected.to_string() => {
            return Err(AlertsDbError::DeploymentMismatch {
                expected: expected.to_string(),
                found,
            });
        }
        Some(_) => {}
        None => {
            tx.execute(
                "INSERT INTO projection_meta(singleton, deployment_id) VALUES (1, ?1)",
                [expected.to_string()],
            )?;
        }
    }
    tx.commit()?;
    Ok(())
}

fn status_code(status: AlertStatus) -> u8 {
    match status {
        AlertStatus::Inactive => 0,
        AlertStatus::Pending => 1,
        AlertStatus::Firing => 2,
        AlertStatus::Recovering => 3,
        AlertStatus::NoData => 4,
        AlertStatus::Error => 5,
        AlertStatus::Disabled => 6,
    }
}

fn status_from_code(code: u8) -> Option<AlertStatus> {
    Some(match code {
        0 => AlertStatus::Inactive,
        1 => AlertStatus::Pending,
        2 => AlertStatus::Firing,
        3 => AlertStatus::Recovering,
        4 => AlertStatus::NoData,
        5 => AlertStatus::Error,
        6 => AlertStatus::Disabled,
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use crate::{
        Comparator, ExecutionErrorPolicy, NoDataPolicy, ScalarCondition, ScalarQuery, Signal,
        ALERT_RECORD_SCHEMA_VERSION, MONITOR_SCHEMA_VERSION,
    };

    use super::*;

    fn monitor(id: MonitorId) -> Monitor {
        Monitor {
            schema_version: MONITOR_SCHEMA_VERSION,
            id,
            revision: 1,
            name: "test".into(),
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
            updated_at_unix_nano: 1,
        }
    }

    fn state(revision: u64, slot: u64) -> AlertState {
        AlertState {
            monitor_revision: revision,
            status: AlertStatus::Firing,
            since_unix_nano: 2,
            last_evaluated_at_unix_nano: slot,
            last_slot_id: slot,
            last_value: Some(3.0),
            last_error_class: None,
            transition_sequence: 1,
            stale: false,
            interrupted: None,
        }
    }

    #[test]
    fn folds_rule_and_state_ordered_by_revision_then_slot() {
        let mut db = AlertsDb::open_in_memory(Uuid::new_v4()).unwrap();
        let rule = monitor(MonitorId::new());
        assert!(db.fold_rule(&rule).unwrap());
        assert!(!db.fold_rule(&rule).unwrap(), "same revision is a no-op");
        assert!(db.fold_state(rule.id, &state(1, 2)).unwrap());
        assert!(db.fold_state(rule.id, &state(1, 3)).unwrap());
        assert!(!db.fold_state(rule.id, &state(1, 3)).unwrap());
        assert!(!db.fold_state(rule.id, &state(1, 1)).unwrap());
        // A newer revision with a much smaller slot (longer interval) wins.
        assert!(db.fold_state(rule.id, &state(2, 1)).unwrap());
        // An older revision can never overwrite it, whatever its slot.
        assert!(!db.fold_state(rule.id, &state(1, 1_000)).unwrap());
        assert_eq!(
            db.get_monitor(rule.id).unwrap().unwrap(),
            MonitorSummary {
                monitor: rule.clone(),
                state: Some(state(2, 1)),
            }
        );
        let schedule = db.list_schedule(None, 10).unwrap();
        assert_eq!(
            schedule,
            vec![ScheduleEntry {
                monitor_id: rule.id,
                revision: 1,
                enabled: true,
                every_seconds: 60,
                jitter_seconds: 0,
                state: Some(ScheduledState {
                    monitor_revision: 2,
                    last_slot_id: 1,
                    status: AlertStatus::Firing,
                }),
            }]
        );
    }

    #[test]
    fn tombstones_block_stale_resurrection_until_forgotten() {
        let mut db = AlertsDb::open_in_memory(Uuid::new_v4()).unwrap();
        let rule = monitor(MonitorId::new());
        db.fold_rule(&rule).unwrap();
        db.fold_state(rule.id, &state(1, 1)).unwrap();
        db.tombstone_monitor(rule.id, 1, 5).unwrap();
        assert!(db.get_monitor(rule.id).unwrap().is_none());
        assert!(db.list_monitors(None, 10).unwrap().is_empty());
        assert!(db.list_schedule(None, 10).unwrap().is_empty());
        // A reconciliation that loaded the live record before the delete.
        assert!(!db.fold_rule(&rule).unwrap());
        assert!(!db.fold_state(rule.id, &state(1, 2)).unwrap());
        assert!(db.get_monitor(rule.id).unwrap().is_none());
        assert_eq!(
            db.local_monitor_revision(rule.id).unwrap(),
            Some(LocalRevision {
                revision: 1,
                deleted: true
            })
        );
        db.forget_monitor(rule.id).unwrap();
        assert!(db.fold_rule(&rule).unwrap());
    }

    #[test]
    fn prune_removes_only_unlisted_rows_folded_before_the_epoch() {
        let mut db = AlertsDb::open_in_memory(Uuid::new_v4()).unwrap();
        let listed = monitor(MonitorId::new());
        let vanished = monitor(MonitorId::new());
        db.fold_rule(&listed).unwrap();
        db.fold_rule(&vanished).unwrap();
        let epoch = db.projection_epoch();
        // Created locally while the durable listing was in flight.
        let concurrent = monitor(MonitorId::new());
        db.fold_rule(&concurrent).unwrap();
        let removed = db
            .prune_monitors(epoch, &HashSet::from([listed.id]))
            .unwrap();
        assert_eq!(removed, 1);
        assert!(db.get_monitor(vanished.id).unwrap().is_none());
        assert!(db.get_monitor(listed.id).unwrap().is_some());
        assert!(db.get_monitor(concurrent.id).unwrap().is_some());
    }

    #[test]
    fn pagination_visits_every_live_row_once_in_id_order() {
        let mut db = AlertsDb::open_in_memory(Uuid::new_v4()).unwrap();
        let mut ids: Vec<MonitorId> = (0..7).map(|_| MonitorId::new()).collect();
        for id in &ids {
            db.fold_rule(&monitor(*id)).unwrap();
        }
        db.tombstone_monitor(ids[3], 1, 1).unwrap();
        let deleted = ids.remove(3);
        ids.sort_unstable_by_key(|id| id.0);
        for page_size in [1, 2, 3, 100] {
            let (mut monitors, mut after) = (Vec::new(), None);
            loop {
                let page = db.list_monitors(after, page_size).unwrap();
                after = page.last().map(|row| row.monitor.id);
                monitors.extend(page.iter().map(|row| row.monitor.id));
                if page.len() < page_size {
                    break;
                }
            }
            assert_eq!(monitors, ids, "list_monitors, page size {page_size}");
            let (mut scheduled, mut after) = (Vec::new(), None);
            loop {
                let page = db.list_schedule(after, page_size).unwrap();
                after = page.last().map(|row| row.monitor_id);
                scheduled.extend(page.iter().map(|row| row.monitor_id));
                if page.len() < page_size {
                    break;
                }
            }
            assert_eq!(scheduled, ids, "list_schedule, page size {page_size}");
            assert!(!scheduled.contains(&deleted));
        }
    }

    #[test]
    fn epoch_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("alerts.sqlite");
        let deployment = Uuid::new_v4();
        let epoch = {
            let mut db = AlertsDb::open(&path, deployment).unwrap();
            db.fold_rule(&monitor(MonitorId::new())).unwrap();
            db.projection_epoch()
        };
        let db = AlertsDb::open(&path, deployment).unwrap();
        assert_eq!(db.projection_epoch(), epoch);
    }

    fn target(id: NotificationTargetId, revision: u64) -> NotificationTarget {
        NotificationTarget {
            schema_version: ALERT_RECORD_SCHEMA_VERSION,
            id,
            revision,
            name: format!("target-{revision}"),
            enabled: true,
            kind: crate::NotificationTargetKind::SlackWebhook,
            format: crate::TargetFormat::BuiltIn {
                format: crate::BuiltInTargetFormat::Slack,
            },
            timeout_millis: 1_000,
            logical_secret_id: crate::LogicalSecretId::new(),
            secret_generation: 9,
            created_at_unix_nano: 1,
            updated_at_unix_nano: revision,
        }
    }

    #[test]
    fn target_projection_is_redacted_revision_ordered_and_deletable() {
        let mut db = AlertsDb::open_in_memory(Uuid::new_v4()).unwrap();
        let id = NotificationTargetId::new();
        let second = target(id, 2);
        db.fold_notification_target(&second).unwrap();
        db.fold_notification_target(&target(id, 1)).unwrap();
        let projection = db.get_notification_target(id).unwrap().unwrap();
        assert_eq!(projection.name, "target-2");
        assert!(projection.has_secret);
        let bytes = serde_json::to_vec(&projection).unwrap();
        assert!(!String::from_utf8_lossy(&bytes).contains("logical_secret"));
        assert_eq!(
            db.list_notification_targets(None, 10).unwrap(),
            vec![projection]
        );
        assert_eq!(db.notification_target_count().unwrap(), 1);
        db.tombstone_notification_target(id, 2, 3).unwrap();
        assert!(db.get_notification_target(id).unwrap().is_none());
        assert_eq!(db.notification_target_count().unwrap(), 0);
        assert!(!db.fold_notification_target(&second).unwrap());
        assert!(db.get_notification_target(id).unwrap().is_none());
    }

    #[test]
    fn migrates_schema_two_to_three_dropping_the_transition_projection() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("alerts.sqlite");
        let deployment = Uuid::new_v4();
        {
            let connection = Connection::open(&path).unwrap();
            connection
                .execute_batch(&format!(
                    "CREATE TABLE projection_meta (
                       singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                       deployment_id TEXT NOT NULL) STRICT;
                     INSERT INTO projection_meta VALUES (1, '{deployment}');
                     CREATE TABLE monitors (monitor_id BLOB PRIMARY KEY) STRICT, WITHOUT ROWID;
                     CREATE TABLE transitions (transition_key TEXT PRIMARY KEY) STRICT, WITHOUT ROWID;
                     CREATE TABLE current_state (monitor_id BLOB PRIMARY KEY) STRICT, WITHOUT ROWID;
                     CREATE TABLE notification_targets (target_id BLOB PRIMARY KEY) STRICT, WITHOUT ROWID;
                     PRAGMA user_version = 2;"
                ))
                .unwrap();
        }
        let mut db = AlertsDb::open(&path, deployment).unwrap();
        let version: u32 = db
            .conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, 3);
        let transitions: u32 = db
            .conn
            .query_row(
                "SELECT count(*) FROM sqlite_master WHERE name = 'transitions'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(transitions, 0);
        db.fold_rule(&monitor(MonitorId::new())).unwrap();
        drop(db);
        assert!(matches!(
            AlertsDb::open(&path, Uuid::new_v4()),
            Err(AlertsDbError::DeploymentMismatch { .. })
        ));
    }

    #[test]
    fn deployment_binding_is_enforced() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("alerts.sqlite");
        AlertsDb::open(&path, Uuid::new_v4()).unwrap();
        assert!(matches!(
            AlertsDb::open(&path, Uuid::new_v4()),
            Err(AlertsDbError::DeploymentMismatch { .. })
        ));
    }
}
