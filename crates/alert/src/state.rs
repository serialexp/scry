use serde::{Deserialize, Serialize};

use crate::{ExecutionErrorPolicy, Monitor, NoDataPolicy};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AlertStatus {
    Inactive,
    Pending,
    Firing,
    Recovering,
    NoData,
    Error,
    Disabled,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AlertState {
    pub monitor_revision: u64,
    pub status: AlertStatus,
    #[serde(with = "crate::serde_u64")]
    pub since_unix_nano: u64,
    #[serde(with = "crate::serde_u64")]
    pub last_evaluated_at_unix_nano: u64,
    #[serde(with = "crate::serde_u64")]
    pub last_slot_id: u64,
    pub last_value: Option<f64>,
    pub last_error_class: Option<String>,
    #[serde(with = "crate::serde_u64")]
    pub transition_sequence: u64,
    pub stale: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Observation {
    Value(f64),
    NoData,
    Error { class: String },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StateChange {
    pub previous: Option<AlertStatus>,
    pub current: AlertStatus,
    pub transitioned: bool,
}

pub fn evaluate(
    monitor: &Monitor,
    prior: Option<&AlertState>,
    observation: Observation,
    slot_id: u64,
    now_unix_nano: u64,
) -> (AlertState, StateChange) {
    if let Some(prior) = prior {
        if slot_id <= prior.last_slot_id {
            return (
                prior.clone(),
                StateChange {
                    previous: Some(prior.status),
                    current: prior.status,
                    transitioned: false,
                },
            );
        }
    }

    // A rule revision is treated as semantically significant in this first slice:
    // Pending/Recovering hold time never carries across an edit. This is
    // conservative for cosmetic edits but prevents an edited condition from
    // firing immediately on time accumulated under different semantics. The
    // durable causal sequence remains monotonic across revisions, however.
    let durable_prior = prior;
    let prior = durable_prior.filter(|state| state.monitor_revision == monitor.revision);
    let previous = prior.map(|state| state.status);
    let (desired, value, error, stale) = classify(monitor, prior, observation);
    let since = prior
        .filter(|state| state.status == desired)
        .map_or(now_unix_nano, |state| state.since_unix_nano);
    let status = apply_holds(monitor, prior, desired, now_unix_nano, since);
    let status_since = if prior.is_some_and(|state| state.status == status) {
        prior.expect("checked above").since_unix_nano
    } else {
        now_unix_nano
    };
    let transitioned = previous != Some(status);
    let sequence =
        durable_prior.map_or(0, |state| state.transition_sequence) + u64::from(transitioned);
    let state = AlertState {
        monitor_revision: monitor.revision,
        status,
        since_unix_nano: status_since,
        last_evaluated_at_unix_nano: now_unix_nano,
        last_slot_id: slot_id,
        last_value: value.or_else(|| prior.and_then(|state| state.last_value)),
        last_error_class: error,
        transition_sequence: sequence,
        stale,
    };
    (
        state,
        StateChange {
            previous,
            current: status,
            transitioned,
        },
    )
}

fn classify(
    monitor: &Monitor,
    prior: Option<&AlertState>,
    observation: Observation,
) -> (AlertStatus, Option<f64>, Option<String>, bool) {
    if !monitor.enabled {
        return (AlertStatus::Disabled, None, None, false);
    }
    match observation {
        Observation::Value(value) => {
            let firing = value.is_finite()
                && monitor
                    .condition
                    .comparator
                    .compare(value, monitor.condition.threshold);
            (
                if firing {
                    AlertStatus::Firing
                } else {
                    AlertStatus::Inactive
                },
                Some(value),
                None,
                false,
            )
        }
        Observation::NoData => (
            match monitor.no_data {
                NoDataPolicy::NoData => AlertStatus::NoData,
                NoDataPolicy::Firing => AlertStatus::Firing,
            },
            None,
            None,
            true,
        ),
        Observation::Error { class } => {
            let status = match monitor.execution_error {
                ExecutionErrorPolicy::KeepLast => prior
                    .map(|state| state.status)
                    .unwrap_or(AlertStatus::Error),
                ExecutionErrorPolicy::Error => AlertStatus::Error,
                ExecutionErrorPolicy::Firing => AlertStatus::Firing,
            };
            (status, None, Some(class), true)
        }
    }
}

fn apply_holds(
    monitor: &Monitor,
    prior: Option<&AlertState>,
    desired: AlertStatus,
    now: u64,
    desired_since: u64,
) -> AlertStatus {
    let previous = prior.map(|state| state.status);
    if desired == AlertStatus::Firing
        && !matches!(previous, Some(AlertStatus::Firing))
        && monitor.for_seconds > 0
    {
        let pending_since = prior
            .filter(|state| state.status == AlertStatus::Pending)
            .map_or(desired_since, |state| state.since_unix_nano);
        if now.saturating_sub(pending_since) < monitor.for_seconds.saturating_mul(1_000_000_000) {
            return AlertStatus::Pending;
        }
    }
    if desired == AlertStatus::Inactive
        && matches!(
            previous,
            Some(AlertStatus::Firing | AlertStatus::Recovering)
        )
        && monitor.recover_for_seconds > 0
    {
        let recovering_since = prior
            .filter(|state| state.status == AlertStatus::Recovering)
            .map_or(desired_since, |state| state.since_unix_nano);
        if now.saturating_sub(recovering_since)
            < monitor.recover_for_seconds.saturating_mul(1_000_000_000)
        {
            return AlertStatus::Recovering;
        }
    }
    desired
}

#[cfg(test)]
mod tests {
    use crate::{
        Comparator, MonitorId, ScalarCondition, ScalarQuery, Signal, MONITOR_SCHEMA_VERSION,
    };

    use super::*;

    fn monitor() -> Monitor {
        Monitor {
            schema_version: MONITOR_SCHEMA_VERSION,
            id: MonitorId::new(),
            revision: 1,
            name: "high load".into(),
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
                threshold: 10.0,
            },
            every_seconds: 60,
            jitter_seconds: 0,
            for_seconds: 10,
            recover_for_seconds: 5,
            no_data: NoDataPolicy::NoData,
            execution_error: ExecutionErrorPolicy::KeepLast,
            labels: vec![],
            annotations: vec![],
            created_at_unix_nano: 1,
            updated_at_unix_nano: 1,
        }
    }

    #[test]
    fn pending_firing_recovering_inactive() {
        let rule = monitor();
        let (pending, _) = evaluate(&rule, None, Observation::Value(11.0), 1, 1_000_000_000);
        assert_eq!(pending.status, AlertStatus::Pending);
        let (firing, _) = evaluate(
            &rule,
            Some(&pending),
            Observation::Value(11.0),
            2,
            12_000_000_000,
        );
        assert_eq!(firing.status, AlertStatus::Firing);
        let (recovering, _) = evaluate(
            &rule,
            Some(&firing),
            Observation::Value(1.0),
            3,
            13_000_000_000,
        );
        assert_eq!(recovering.status, AlertStatus::Recovering);
        let (inactive, _) = evaluate(
            &rule,
            Some(&recovering),
            Observation::Value(1.0),
            4,
            19_000_000_000,
        );
        assert_eq!(inactive.status, AlertStatus::Inactive);
    }

    #[test]
    fn error_keep_last_marks_state_stale_without_recovery() {
        let mut rule = monitor();
        rule.for_seconds = 0;
        let (firing, _) = evaluate(&rule, None, Observation::Value(11.0), 1, 1);
        let (stale, change) = evaluate(
            &rule,
            Some(&firing),
            Observation::Error {
                class: "timeout".into(),
            },
            2,
            2,
        );
        assert_eq!(stale.status, AlertStatus::Firing);
        assert!(stale.stale);
        assert!(!change.transitioned);
    }

    #[test]
    fn revision_change_resets_pending_hold_but_preserves_causal_sequence() {
        let mut rule = monitor();
        let (mut pending, _) = evaluate(&rule, None, Observation::Value(11.0), 1, 1_000_000_000);
        pending.transition_sequence = 7;
        rule.revision = 2;
        let (reset, _) = evaluate(
            &rule,
            Some(&pending),
            Observation::Value(11.0),
            2,
            20_000_000_000,
        );
        assert_eq!(reset.status, AlertStatus::Pending);
        assert_eq!(reset.since_unix_nano, 20_000_000_000);
        assert_eq!(reset.transition_sequence, 8);
    }

    #[test]
    fn old_or_same_slot_cannot_rewrite_state() {
        let rule = monitor();
        let (current, _) = evaluate(&rule, None, Observation::NoData, 10, 10);
        let (old, _) = evaluate(&rule, Some(&current), Observation::Value(99.0), 9, 11);
        assert_eq!(old, current);
        let (same, _) = evaluate(&rule, Some(&current), Observation::Value(99.0), 10, 12);
        assert_eq!(same, current);
    }
}
