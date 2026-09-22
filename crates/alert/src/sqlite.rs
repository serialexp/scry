use std::path::Path;

use rusqlite::{params, Connection, OptionalExtension};
use thiserror::Error;
use uuid::Uuid;

use crate::{
    AlertState, AlertStatus, Monitor, MonitorId, NotificationTarget, NotificationTargetId,
    NotificationTargetProjection, StateHead, TransitionRecord,
};

pub const ALERTS_SCHEMA_VERSION: u32 = 2;

#[derive(Debug, Error)]
pub enum AlertsDbError {
    #[error("SQLite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("alerts database belongs to deployment {found}, expected {expected}")]
    DeploymentMismatch { expected: String, found: String },
    #[error("invalid stored alert record: {0}")]
    Decode(#[from] serde_json::Error),
}

pub struct AlertsDb {
    conn: Connection,
    deployment_id: Uuid,
}

#[derive(Clone, Debug, PartialEq)]
pub struct MonitorSummary {
    pub monitor: Monitor,
    pub state: Option<AlertState>,
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
        Ok(Self {
            conn,
            deployment_id,
        })
    }

    pub fn fold_rule(&mut self, monitor: &Monitor) -> Result<(), AlertsDbError> {
        let json = serde_json::to_vec(monitor)?;
        self.conn.execute(
            "INSERT INTO monitors(monitor_id, revision, enabled, name, updated_at_unix_nano, record_json)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(monitor_id) DO UPDATE SET
               revision = excluded.revision,
               enabled = excluded.enabled,
               name = excluded.name,
               updated_at_unix_nano = excluded.updated_at_unix_nano,
               record_json = excluded.record_json
             WHERE excluded.revision > monitors.revision",
            params![
                monitor.id.0.as_bytes().as_slice(),
                monitor.revision,
                monitor.enabled,
                monitor.name,
                monitor.updated_at_unix_nano,
                json,
            ],
        )?;
        Ok(())
    }

    pub fn fold_transition(
        &mut self,
        key: &str,
        record: &TransitionRecord,
        head: &StateHead,
    ) -> Result<(), AlertsDbError> {
        let tx = self.conn.transaction()?;
        tx.execute(
            "INSERT OR IGNORE INTO transitions(
               transition_key, monitor_id, sequence, slot_id, evaluated_at_unix_nano, record_json
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                key,
                record.monitor_id.0.as_bytes().as_slice(),
                record.state.transition_sequence,
                record.slot_id,
                record.evaluated_at_unix_nano,
                serde_json::to_vec(record)?,
            ],
        )?;
        tx.execute(
            "INSERT INTO current_state(
               monitor_id, sequence, status, stale, since_unix_nano,
               last_evaluated_at_unix_nano, last_slot_id, transition_key, state_json
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
             ON CONFLICT(monitor_id) DO UPDATE SET
               sequence = excluded.sequence,
               status = excluded.status,
               stale = excluded.stale,
               since_unix_nano = excluded.since_unix_nano,
               last_evaluated_at_unix_nano = excluded.last_evaluated_at_unix_nano,
               last_slot_id = excluded.last_slot_id,
               transition_key = excluded.transition_key,
               state_json = excluded.state_json
             WHERE excluded.last_slot_id > current_state.last_slot_id",
            params![
                record.monitor_id.0.as_bytes().as_slice(),
                head.transition_sequence,
                status_code(record.state.status),
                record.state.stale,
                record.state.since_unix_nano,
                record.state.last_evaluated_at_unix_nano,
                record.state.last_slot_id,
                key,
                serde_json::to_vec(&record.state)?,
            ],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn fold_notification_target(
        &mut self,
        target: &NotificationTarget,
    ) -> Result<(), AlertsDbError> {
        let projection = NotificationTargetProjection::from(target);
        let json = serde_json::to_vec(&projection)?;
        self.conn.execute(
            "INSERT INTO notification_targets(target_id, revision, enabled, name, updated_at_unix_nano, projection_json)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(target_id) DO UPDATE SET revision=excluded.revision, enabled=excluded.enabled,
               name=excluded.name, updated_at_unix_nano=excluded.updated_at_unix_nano,
               projection_json=excluded.projection_json
             WHERE excluded.revision > notification_targets.revision",
            params![target.id.0.as_bytes().as_slice(), target.revision, target.enabled, target.name, target.updated_at_unix_nano, json],
        )?;
        Ok(())
    }

    pub fn get_notification_target(
        &self,
        id: NotificationTargetId,
    ) -> Result<Option<NotificationTargetProjection>, AlertsDbError> {
        let json: Option<Vec<u8>> = self
            .conn
            .query_row(
                "SELECT projection_json FROM notification_targets WHERE target_id=?1",
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
        let count: u64 =
            self.conn
                .query_row("SELECT COUNT(*) FROM notification_targets", [], |row| {
                    row.get(0)
                })?;
        Ok(count as usize)
    }

    pub fn list_notification_targets(
        &self,
        after: Option<NotificationTargetId>,
        limit: usize,
    ) -> Result<Vec<NotificationTargetProjection>, AlertsDbError> {
        let limit = limit.clamp(1, 10_000) as u64;
        let after = after.map(|id| id.0.as_bytes().to_vec()).unwrap_or_default();
        let mut statement = self.conn.prepare("SELECT projection_json FROM notification_targets WHERE (?1=X'' OR target_id>?1) ORDER BY target_id LIMIT ?2")?;
        let rows = statement.query_map(params![after, limit], |row| row.get::<_, Vec<u8>>(0))?;
        let mut output = Vec::with_capacity(limit.min(128) as usize);
        for row in rows {
            output.push(serde_json::from_slice(&row?)?);
        }
        Ok(output)
    }

    pub fn delete_notification_target(
        &mut self,
        id: NotificationTargetId,
    ) -> Result<(), AlertsDbError> {
        self.conn.execute(
            "DELETE FROM notification_targets WHERE target_id=?1",
            [id.0.as_bytes().as_slice()],
        )?;
        Ok(())
    }

    pub fn delete_monitor(&mut self, id: MonitorId) -> Result<(), AlertsDbError> {
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

    pub fn get_monitor(&self, id: MonitorId) -> Result<Option<MonitorSummary>, AlertsDbError> {
        self.conn
            .query_row(
                "SELECT m.record_json, s.state_json
                 FROM monitors m LEFT JOIN current_state s USING(monitor_id)
                 WHERE m.monitor_id = ?1",
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
        let mut statement = self.conn.prepare(
            "SELECT m.record_json, s.state_json
             FROM monitors m LEFT JOIN current_state s USING(monitor_id)
             WHERE (?1 = X'' OR m.monitor_id > ?1)
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

    pub fn deployment_id(&self) -> Uuid {
        self.deployment_id
    }
}

fn migrate(conn: &Connection) -> Result<(), rusqlite::Error> {
    let mut version: u32 = conn.pragma_query_value(None, "user_version", |row| row.get(0))?;
    if version > ALERTS_SCHEMA_VERSION {
        return Err(rusqlite::Error::InvalidQuery);
    }
    if version == 0 {
        conn.execute_batch(
            "BEGIN IMMEDIATE;
             CREATE TABLE projection_meta (
               singleton INTEGER PRIMARY KEY CHECK(singleton = 1), deployment_id TEXT NOT NULL
             ) STRICT;
             CREATE TABLE monitors (
               monitor_id BLOB PRIMARY KEY CHECK(length(monitor_id) = 16), revision INTEGER NOT NULL,
               enabled INTEGER NOT NULL, name TEXT NOT NULL, updated_at_unix_nano INTEGER NOT NULL,
               record_json BLOB NOT NULL
             ) STRICT, WITHOUT ROWID;
             CREATE TABLE transitions (
               transition_key TEXT PRIMARY KEY, monitor_id BLOB NOT NULL CHECK(length(monitor_id) = 16),
               sequence INTEGER NOT NULL, slot_id INTEGER NOT NULL, evaluated_at_unix_nano INTEGER NOT NULL,
               record_json BLOB NOT NULL, UNIQUE(monitor_id, slot_id)
             ) STRICT, WITHOUT ROWID;
             CREATE TABLE current_state (
               monitor_id BLOB PRIMARY KEY CHECK(length(monitor_id) = 16), sequence INTEGER NOT NULL,
               status INTEGER NOT NULL, stale INTEGER NOT NULL, since_unix_nano INTEGER NOT NULL,
               last_evaluated_at_unix_nano INTEGER NOT NULL, last_slot_id INTEGER NOT NULL,
               transition_key TEXT NOT NULL, state_json BLOB NOT NULL
             ) STRICT, WITHOUT ROWID;
             PRAGMA user_version = 1;
             COMMIT;",
        )?;
        version = 1;
    }
    if version == 1 {
        conn.execute_batch(
            "BEGIN IMMEDIATE;
             CREATE TABLE notification_targets (
               target_id BLOB PRIMARY KEY CHECK(length(target_id) = 16), revision INTEGER NOT NULL,
               enabled INTEGER NOT NULL, name TEXT NOT NULL, updated_at_unix_nano INTEGER NOT NULL,
               projection_json BLOB NOT NULL
             ) STRICT, WITHOUT ROWID;
             PRAGMA user_version = 2;
             COMMIT;",
        )?;
    }
    Ok(())
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

#[cfg(test)]
mod tests {
    use crate::{
        Comparator, DurableObservation, ExecutionErrorPolicy, NoDataPolicy, ScalarCondition,
        ScalarQuery, Signal, ALERT_RECORD_SCHEMA_VERSION, MONITOR_SCHEMA_VERSION,
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

    #[test]
    fn folds_rule_and_winning_state() {
        let deployment = Uuid::new_v4();
        let mut db = AlertsDb::open_in_memory(deployment).unwrap();
        let rule = monitor(MonitorId::new());
        db.fold_rule(&rule).unwrap();
        let state = AlertState {
            monitor_revision: 1,
            status: AlertStatus::Firing,
            since_unix_nano: 2,
            last_evaluated_at_unix_nano: 2,
            last_slot_id: 2,
            last_value: Some(3.0),
            last_error_class: None,
            transition_sequence: 1,
            stale: false,
        };
        let record = TransitionRecord {
            schema_version: ALERT_RECORD_SCHEMA_VERSION,
            deployment_id: deployment.to_string(),
            monitor_id: rule.id,
            monitor_revision: 1,
            slot_id: 2,
            evaluated_at_unix_nano: 2,
            observation: DurableObservation::Value { value: 3.0 },
            state: state.clone(),
            previous_transition_key: None,
            notification_intents: vec![],
        };
        let head = StateHead {
            schema_version: ALERT_RECORD_SCHEMA_VERSION,
            deployment_id: deployment.to_string(),
            monitor_id: rule.id,
            transition_sequence: 1,
            transition_key: "key".into(),
            transition_sha256: "digest".into(),
            updated_at_unix_nano: 2,
        };
        db.fold_transition("key", &record, &head).unwrap();
        let mut next_record = record.clone();
        next_record.slot_id = 3;
        next_record.evaluated_at_unix_nano = 3;
        next_record.state.last_slot_id = 3;
        next_record.state.last_evaluated_at_unix_nano = 3;
        let mut next_head = head.clone();
        next_head.transition_key = "key-2".into();
        next_head.updated_at_unix_nano = 3;
        db.fold_transition("key-2", &next_record, &next_head)
            .unwrap();
        assert_eq!(
            db.get_monitor(rule.id).unwrap().unwrap(),
            MonitorSummary {
                monitor: rule,
                state: Some(next_record.state)
            }
        );
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
        db.delete_notification_target(id).unwrap();
        assert!(db.get_notification_target(id).unwrap().is_none());
    }

    #[test]
    fn migrates_schema_one_to_two_without_losing_monitors() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("alerts.sqlite");
        let deployment = Uuid::new_v4();
        {
            let connection = Connection::open(&path).unwrap();
            migrate(&connection).unwrap();
            connection
                .execute("DROP TABLE notification_targets", [])
                .unwrap();
            connection.pragma_update(None, "user_version", 1).unwrap();
        }
        let db = AlertsDb::open(&path, deployment).unwrap();
        let version: u32 = db
            .conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, 2);
        assert_eq!(db.list_notification_targets(None, 1).unwrap(), vec![]);
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
