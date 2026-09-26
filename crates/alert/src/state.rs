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

impl AlertStatus {
    /// Stable snake_case name, identical to the serialized form.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Inactive => "inactive",
            Self::Pending => "pending",
            Self::Firing => "firing",
            Self::Recovering => "recovering",
            Self::NoData => "no_data",
            Self::Error => "error",
            Self::Disabled => "disabled",
        }
    }

    /// Statuses whose `since` is a hold timer (`for` / `recover_for`) rather
    /// than only a display timestamp.
    fn is_hold(self) -> bool {
        matches!(self, Self::Pending | Self::Recovering)
    }
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
    /// While in NoData or Error: the active status (Firing or Recovering) that
    /// the outage interrupted, with its `since`. The alert was never observed
    /// to resolve, so a returning condition resumes it instead of starting a
    /// new Pending hold (true) or skipping recovery (false).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interrupted: Option<InterruptedStatus>,
}

/// An active status suspended by a NoData or Error outage.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InterruptedStatus {
    pub status: AlertStatus,
    #[serde(with = "crate::serde_u64")]
    pub since_unix_nano: u64,
}

impl AlertState {
    /// The active status this state carries: its own if Firing/Recovering, or
    /// the one a NoData/Error outage interrupted.
    fn active(&self) -> Option<InterruptedStatus> {
        match self.status {
            AlertStatus::Firing | AlertStatus::Recovering => Some(InterruptedStatus {
                status: self.status,
                since_unix_nano: self.since_unix_nano,
            }),
            AlertStatus::NoData | AlertStatus::Error => self.interrupted,
            _ => None,
        }
    }

    /// Whether this durable state already reflects evaluating `slot_id` under
    /// monitor `revision` — or something newer.
    ///
    /// Staleness is ordered lexicographically by `(monitor_revision, slot_id)`.
    /// Slot IDs are only comparable within one revision: a revision can change
    /// the interval, and a longer interval yields numerically *smaller* slot
    /// IDs (`floor(t / every)`). A newer revision is therefore never stale
    /// relative to an older revision's slot, and an older revision's result
    /// never overwrites a newer revision's state.
    pub fn covers(&self, revision: u64, slot_id: u64) -> bool {
        (self.monitor_revision, self.last_slot_id) >= (revision, slot_id)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum Observation {
    Value(f64),
    NoData,
    Error { class: String },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StateChange {
    /// The durable status before this evaluation, regardless of revision.
    pub previous: Option<AlertStatus>,
    pub current: AlertStatus,
    /// The status changed. Exactly these evaluations allocate a new transition
    /// sequence and write an immutable transition record.
    pub transitioned: bool,
    /// This transition leaves a NoData/Error outage back into the Firing or
    /// Recovering status the outage interrupted. The alert never resolved, so
    /// delivery must treat it as a continuation, not a new Firing.
    pub resumed: bool,
}

/// Fold one observation for `slot_id` into the prior durable state.
///
/// A prior state that already [covers](AlertState::covers) `(monitor.revision,
/// slot_id)` is returned unchanged: a slot is evaluated once, and an older
/// revision cannot rewrite a newer one.
///
/// State machine (`for` = `for_seconds`, `recover` = `recover_for_seconds`):
///
/// * condition true: Firing and Recovering stay/return to Firing without a new
///   hold (a breach during recovery never re-enters Pending, so it cannot emit
///   a second Firing); anything else enters Pending and fires once the Pending
///   hold reaches `for` (immediately when `for` is zero);
/// * condition false: Firing and Recovering enter/stay Recovering until the
///   hold reaches `recover`, then Inactive; anything else is Inactive;
/// * a NoData or Error status entered from Firing/Recovering remembers that
///   active status ([`AlertState::interrupted`]). The alert was never seen to
///   resolve, so both rules above treat it as still active: a true condition
///   resumes Firing (keeping the original firing `since`) and a false one
///   goes through Recovering, never straight to Inactive;
/// * no data: `NoData` policy reports NoData; `Firing` policy treats it as a
///   true condition;
/// * execution error: `Error` reports Error; `Firing` treats it as a true
///   condition; `KeepLast` keeps the prior status (and its hold timer) marked
///   stale, so it never reports recovery and a condition that is true again
///   continues the prior Firing/Pending rather than restarting the `for` hold.
///   With no prior health to keep (no state, or Disabled) it reports Error.
///
/// A rule revision is treated as semantically significant for *hold timers*:
/// Pending and Recovering time accumulated under another revision never counts
/// toward the edited rule, so an edit restarts the hold. The status itself
/// carries across revisions (a Firing alert whose edited condition is still
/// true stays Firing), and the transition sequence is monotonic across
/// revisions.
pub fn evaluate(
    monitor: &Monitor,
    prior: Option<&AlertState>,
    observation: Observation,
    slot_id: u64,
    now_unix_nano: u64,
) -> (AlertState, StateChange) {
    if let Some(prior) = prior.filter(|state| state.covers(monitor.revision, slot_id)) {
        return (
            prior.clone(),
            StateChange {
                previous: Some(prior.status),
                current: prior.status,
                transitioned: false,
                resumed: false,
            },
        );
    }

    let same_revision = prior.filter(|state| state.monitor_revision == monitor.revision);
    let previous = prior.map(|state| state.status);
    let (status, value, error, stale) =
        next_status(monitor, prior, same_revision, observation, now_unix_nano);
    // An outage (NoData/Error) suspends the active status it interrupted.
    let suspended = prior
        .filter(|state| matches!(state.status, AlertStatus::NoData | AlertStatus::Error))
        .and_then(|state| state.interrupted);
    let resumed = suspended.filter(|active| active.status == status);
    let since = match (prior, resumed) {
        // Resuming keeps the original `since`; like any hold, a Recovering
        // hold only continues under the same revision.
        (_, Some(active)) if same_revision.is_some() || !status.is_hold() => active.since_unix_nano,
        (Some(state), _)
            if state.status == status && (same_revision.is_some() || !status.is_hold()) =>
        {
            state.since_unix_nano
        }
        _ => now_unix_nano,
    };
    let interrupted = match status {
        AlertStatus::NoData | AlertStatus::Error => prior.and_then(AlertState::active),
        _ => None,
    };
    let transitioned = previous != Some(status);
    let sequence = prior.map_or(0, |state| state.transition_sequence) + u64::from(transitioned);
    let state = AlertState {
        monitor_revision: monitor.revision,
        status,
        since_unix_nano: since,
        last_evaluated_at_unix_nano: now_unix_nano,
        last_slot_id: slot_id,
        last_value: value.or_else(|| same_revision.and_then(|state| state.last_value)),
        last_error_class: error,
        transition_sequence: sequence,
        stale,
        interrupted,
    };
    (
        state,
        StateChange {
            previous,
            current: status,
            transitioned,
            resumed: transitioned && resumed.is_some(),
        },
    )
}

fn next_status(
    monitor: &Monitor,
    prior: Option<&AlertState>,
    same_revision: Option<&AlertState>,
    observation: Observation,
    now: u64,
) -> (AlertStatus, Option<f64>, Option<String>, bool) {
    if !monitor.enabled {
        return (AlertStatus::Disabled, None, None, false);
    }
    match observation {
        Observation::Value(value) => {
            let breaching = value.is_finite()
                && monitor
                    .condition
                    .comparator
                    .compare(value, monitor.condition.threshold);
            let status = if breaching {
                breaching_status(monitor, prior, same_revision, now)
            } else {
                clear_status(monitor, prior, same_revision, now)
            };
            (status, Some(value), None, false)
        }
        Observation::NoData => {
            let status = match monitor.no_data {
                NoDataPolicy::NoData => AlertStatus::NoData,
                NoDataPolicy::Firing => breaching_status(monitor, prior, same_revision, now),
            };
            (status, None, None, true)
        }
        Observation::Error { class } => {
            let status = match monitor.execution_error {
                ExecutionErrorPolicy::KeepLast => match prior.map(|state| state.status) {
                    None | Some(AlertStatus::Disabled) => AlertStatus::Error,
                    Some(status) => status,
                },
                ExecutionErrorPolicy::Error => AlertStatus::Error,
                ExecutionErrorPolicy::Firing => {
                    breaching_status(monitor, prior, same_revision, now)
                }
            };
            (status, None, Some(class), true)
        }
    }
}

/// The condition is true (or a policy treats the observation as true).
fn breaching_status(
    monitor: &Monitor,
    prior: Option<&AlertState>,
    same_revision: Option<&AlertState>,
    now: u64,
) -> AlertStatus {
    if prior.and_then(AlertState::active).is_some() || monitor.for_seconds == 0 {
        return AlertStatus::Firing;
    }
    let pending_since = same_revision
        .filter(|state| state.status == AlertStatus::Pending)
        .map_or(now, |state| state.since_unix_nano);
    if held_for(pending_since, now, monitor.for_seconds) {
        AlertStatus::Firing
    } else {
        AlertStatus::Pending
    }
}

/// The condition is false.
fn clear_status(
    monitor: &Monitor,
    prior: Option<&AlertState>,
    same_revision: Option<&AlertState>,
    now: u64,
) -> AlertStatus {
    let Some(active) = prior.and_then(AlertState::active) else {
        return AlertStatus::Inactive;
    };
    if monitor.recover_for_seconds == 0 {
        return AlertStatus::Inactive;
    }
    // A Recovering hold (possibly suspended by an outage) continues only under
    // the same revision.
    let recovering_since = same_revision
        .and(Some(active))
        .filter(|active| active.status == AlertStatus::Recovering)
        .map_or(now, |active| active.since_unix_nano);
    if held_for(recovering_since, now, monitor.recover_for_seconds) {
        AlertStatus::Inactive
    } else {
        AlertStatus::Recovering
    }
}

fn held_for(since: u64, now: u64, seconds: u64) -> bool {
    now.saturating_sub(since) >= seconds.saturating_mul(1_000_000_000)
}

#[cfg(test)]
mod tests {
    use crate::{
        Comparator, MonitorId, ScalarCondition, ScalarQuery, Signal, MONITOR_SCHEMA_VERSION,
    };

    use super::*;

    const S: u64 = 1_000_000_000;

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

    fn error() -> Observation {
        Observation::Error {
            class: "timeout".into(),
        }
    }

    /// Drive a sequence of `(observation, slot, now)` steps, returning every state.
    fn run(rule: &Monitor, steps: &[(Observation, u64, u64)]) -> Vec<AlertState> {
        let mut states: Vec<AlertState> = Vec::new();
        for (observation, slot, now) in steps {
            let (next, _) = evaluate(rule, states.last(), observation.clone(), *slot, *now);
            states.push(next);
        }
        states
    }

    #[test]
    fn pending_firing_recovering_inactive() {
        let rule = monitor();
        let states = run(
            &rule,
            &[
                (Observation::Value(11.0), 1, S),
                (Observation::Value(11.0), 2, 12 * S),
                (Observation::Value(1.0), 3, 13 * S),
                (Observation::Value(1.0), 4, 19 * S),
            ],
        );
        let statuses: Vec<_> = states.iter().map(|state| state.status).collect();
        assert_eq!(
            statuses,
            [
                AlertStatus::Pending,
                AlertStatus::Firing,
                AlertStatus::Recovering,
                AlertStatus::Inactive
            ]
        );
        let sequences: Vec<_> = states.iter().map(|s| s.transition_sequence).collect();
        assert_eq!(sequences, [1, 2, 3, 4]);
    }

    #[test]
    fn unchanged_status_keeps_sequence_and_since() {
        let mut rule = monitor();
        rule.for_seconds = 0;
        let states = run(
            &rule,
            &[
                (Observation::Value(11.0), 1, S),
                (Observation::Value(12.0), 2, 2 * S),
                (Observation::Value(13.0), 3, 3 * S),
            ],
        );
        assert!(states.iter().all(|s| s.status == AlertStatus::Firing));
        assert!(states.iter().all(|s| s.transition_sequence == 1));
        assert!(states.iter().all(|s| s.since_unix_nano == S));
        assert_eq!(states[2].last_value, Some(13.0));
        let (_, change) = evaluate(&rule, Some(&states[1]), Observation::Value(9.0), 4, 4 * S);
        assert!(change.transitioned);
    }

    #[test]
    fn breach_during_recovery_returns_to_firing_without_a_new_hold() {
        let rule = monitor();
        let states = run(
            &rule,
            &[
                (Observation::Value(11.0), 1, S),
                (Observation::Value(11.0), 2, 12 * S),
                (Observation::Value(1.0), 3, 13 * S),
                // Condition true again while Recovering, well inside `for`.
                (Observation::Value(11.0), 4, 14 * S),
            ],
        );
        assert_eq!(states[2].status, AlertStatus::Recovering);
        assert_eq!(states[3].status, AlertStatus::Firing);
        assert_eq!(states[3].since_unix_nano, 14 * S);
        assert!(
            !states
                .iter()
                .skip(2)
                .any(|s| s.status == AlertStatus::Pending),
            "Recovering -> Pending would later emit a duplicate Firing"
        );
    }

    /// Pending at 1s, Firing at 12s, then the given outage at 13s and 14s.
    fn firing_then_outage(rule: &Monitor, outage: Observation) -> Vec<AlertState> {
        run(
            rule,
            &[
                (Observation::Value(11.0), 1, S),
                (Observation::Value(11.0), 2, 12 * S),
                (outage.clone(), 3, 13 * S),
                (outage, 4, 14 * S),
            ],
        )
    }

    #[test]
    fn breach_after_no_data_resumes_firing_without_a_new_hold() {
        let rule = monitor();
        let mut states = firing_then_outage(&rule, Observation::NoData);
        assert_eq!(states[2].status, AlertStatus::NoData);
        assert_eq!(
            states[3].interrupted,
            Some(InterruptedStatus {
                status: AlertStatus::Firing,
                since_unix_nano: 12 * S,
            }),
            "the outage keeps remembering what it interrupted"
        );
        // Condition true again, well inside `for`.
        let (resumed, change) = evaluate(&rule, states.last(), Observation::Value(11.0), 5, 15 * S);
        assert_eq!(resumed.status, AlertStatus::Firing);
        assert_eq!(resumed.since_unix_nano, 12 * S, "original firing time");
        assert_eq!(resumed.interrupted, None);
        assert!(change.transitioned && change.resumed);
        states.push(resumed);
        assert!(!states
            .iter()
            .skip(2)
            .any(|s| s.status == AlertStatus::Pending));
    }

    #[test]
    fn breach_after_error_policy_error_resumes_firing() {
        let mut rule = monitor();
        rule.execution_error = ExecutionErrorPolicy::Error;
        let states = firing_then_outage(&rule, error());
        assert_eq!(states[3].status, AlertStatus::Error);
        let (resumed, change) = evaluate(&rule, states.last(), Observation::Value(11.0), 5, 15 * S);
        assert_eq!(resumed.status, AlertStatus::Firing);
        assert!(change.resumed);
    }

    #[test]
    fn outage_chain_keeps_the_interrupted_status() {
        let mut rule = monitor();
        rule.execution_error = ExecutionErrorPolicy::Error;
        let states = run(
            &rule,
            &[
                (Observation::Value(11.0), 1, S),
                (Observation::Value(11.0), 2, 12 * S),
                (Observation::NoData, 3, 13 * S),
                (error(), 4, 14 * S),
                (Observation::Value(11.0), 5, 15 * S),
            ],
        );
        assert_eq!(states[3].status, AlertStatus::Error);
        assert_eq!(
            states[3].interrupted.map(|active| active.status),
            Some(AlertStatus::Firing)
        );
        assert_eq!(states[4].status, AlertStatus::Firing);
        assert_eq!(states[4].since_unix_nano, 12 * S);
    }

    #[test]
    fn clear_after_no_data_on_firing_goes_through_recovery() {
        let rule = monitor();
        let mut states = firing_then_outage(&rule, Observation::NoData);
        let (recovering, change) =
            evaluate(&rule, states.last(), Observation::Value(1.0), 5, 15 * S);
        assert_eq!(
            recovering.status,
            AlertStatus::Recovering,
            "an alert never seen to resolve must not jump to Inactive"
        );
        assert_eq!(recovering.since_unix_nano, 15 * S);
        assert!(!change.resumed, "Firing -> Recovering is a real change");
        states.push(recovering);
        let (done, _) = evaluate(&rule, states.last(), Observation::Value(1.0), 6, 20 * S);
        assert_eq!(done.status, AlertStatus::Inactive);
    }

    #[test]
    fn outage_during_recovery_resumes_the_recovery_hold() {
        let rule = monitor();
        let states = run(
            &rule,
            &[
                (Observation::Value(11.0), 1, S),
                (Observation::Value(11.0), 2, 12 * S),
                (Observation::Value(1.0), 3, 13 * S),
                (Observation::NoData, 4, 14 * S),
                (Observation::Value(1.0), 5, 15 * S),
                (Observation::Value(1.0), 6, 18 * S),
            ],
        );
        assert_eq!(states[2].status, AlertStatus::Recovering);
        assert_eq!(states[4].status, AlertStatus::Recovering);
        assert_eq!(states[4].since_unix_nano, 13 * S, "recovery hold continues");
        assert_eq!(states[5].status, AlertStatus::Inactive, "13s + 5s hold");
    }

    #[test]
    fn outage_without_an_active_alert_restarts_normally() {
        let rule = monitor();
        let states = run(
            &rule,
            &[
                (Observation::NoData, 1, S),
                (Observation::Value(11.0), 2, 2 * S),
            ],
        );
        assert_eq!(states[0].interrupted, None);
        assert_eq!(states[1].status, AlertStatus::Pending);
    }

    #[test]
    fn keep_last_error_on_firing_is_stale_and_does_not_restart_the_hold() {
        let rule = monitor();
        let states = run(
            &rule,
            &[
                (Observation::Value(11.0), 1, S),
                (Observation::Value(11.0), 2, 12 * S),
                (error(), 3, 13 * S),
                (error(), 4, 14 * S),
                (Observation::Value(11.0), 5, 15 * S),
            ],
        );
        assert_eq!(states[1].status, AlertStatus::Firing);
        for state in &states[2..4] {
            assert_eq!(state.status, AlertStatus::Firing);
            assert!(state.stale);
            assert_eq!(state.last_error_class.as_deref(), Some("timeout"));
        }
        assert_eq!(states[4].status, AlertStatus::Firing);
        assert!(!states[4].stale);
        assert_eq!(states[4].since_unix_nano, 12 * S, "Firing never restarted");
        assert_eq!(states[4].transition_sequence, 2);
    }

    #[test]
    fn keep_last_error_during_pending_continues_the_original_hold() {
        let rule = monitor();
        let states = run(
            &rule,
            &[
                (Observation::Value(11.0), 1, S),
                (error(), 2, 5 * S),
                (Observation::Value(11.0), 3, 11 * S),
            ],
        );
        assert_eq!(states[1].status, AlertStatus::Pending);
        assert_eq!(states[1].since_unix_nano, S);
        assert_eq!(
            states[2].status,
            AlertStatus::Firing,
            "hold measured from the first breach"
        );
    }

    #[test]
    fn keep_last_error_during_recovery_never_infers_recovery() {
        let rule = monitor();
        let states = run(
            &rule,
            &[
                (Observation::Value(11.0), 1, S),
                (Observation::Value(11.0), 2, 12 * S),
                (Observation::Value(1.0), 3, 13 * S),
                (error(), 4, 100 * S),
            ],
        );
        assert_eq!(states[3].status, AlertStatus::Recovering);
        assert!(states[3].stale);
    }

    #[test]
    fn keep_last_without_prior_health_reports_error() {
        let rule = monitor();
        let (first, _) = evaluate(&rule, None, error(), 1, S);
        assert_eq!(first.status, AlertStatus::Error);

        let mut disabled = monitor();
        disabled.enabled = false;
        let (off, _) = evaluate(&disabled, None, Observation::NoData, 1, S);
        assert_eq!(off.status, AlertStatus::Disabled);
        let mut enabled = monitor();
        enabled.id = disabled.id;
        enabled.revision = 2;
        let (after, _) = evaluate(&enabled, Some(&off), error(), 2, 2 * S);
        assert_eq!(after.status, AlertStatus::Error);
    }

    #[test]
    fn error_policy_firing_uses_the_pending_hold() {
        let mut rule = monitor();
        rule.execution_error = ExecutionErrorPolicy::Firing;
        let states = run(&rule, &[(error(), 1, S), (error(), 2, 11 * S)]);
        assert_eq!(states[0].status, AlertStatus::Pending);
        assert_eq!(states[1].status, AlertStatus::Firing);
    }

    #[test]
    fn revision_change_resets_pending_hold_but_preserves_causal_sequence() {
        let mut rule = monitor();
        let (mut pending, _) = evaluate(&rule, None, Observation::Value(11.0), 1, S);
        pending.transition_sequence = 7;
        rule.revision = 2;
        let (reset, change) = evaluate(&rule, Some(&pending), Observation::Value(11.0), 2, 20 * S);
        assert_eq!(reset.status, AlertStatus::Pending);
        assert_eq!(reset.since_unix_nano, 20 * S);
        assert_eq!(reset.monitor_revision, 2);
        assert!(!change.transitioned, "status is unchanged");
        assert_eq!(reset.transition_sequence, 7);
    }

    #[test]
    fn revision_change_keeps_firing_status_and_resets_recovery_hold() {
        let mut rule = monitor();
        let states = run(
            &rule,
            &[
                (Observation::Value(11.0), 1, S),
                (Observation::Value(11.0), 2, 12 * S),
            ],
        );
        rule.revision = 2;
        let (still_firing, change) =
            evaluate(&rule, states.last(), Observation::Value(11.0), 3, 13 * S);
        assert_eq!(still_firing.status, AlertStatus::Firing);
        assert_eq!(still_firing.since_unix_nano, 12 * S);
        assert!(!change.transitioned);

        let (recovering, _) = evaluate(
            &rule,
            Some(&still_firing),
            Observation::Value(1.0),
            4,
            14 * S,
        );
        assert_eq!(recovering.status, AlertStatus::Recovering);
        rule.revision = 3;
        let (reset, _) = evaluate(
            &rule,
            Some(&recovering),
            Observation::Value(1.0),
            5,
            100 * S,
        );
        assert_eq!(reset.status, AlertStatus::Recovering);
        assert_eq!(reset.since_unix_nano, 100 * S, "recovery hold restarts");
    }

    #[test]
    fn old_or_same_slot_cannot_rewrite_state() {
        let rule = monitor();
        let (current, _) = evaluate(&rule, None, Observation::NoData, 10, 10);
        let (old, _) = evaluate(&rule, Some(&current), Observation::Value(99.0), 9, 11);
        assert_eq!(old, current);
        let (same, change) = evaluate(&rule, Some(&current), Observation::Value(99.0), 10, 12);
        assert_eq!(same, current);
        assert!(!change.transitioned);
    }

    #[test]
    fn interval_increase_yields_smaller_slot_ids_but_still_evaluates() {
        // Revision 1 runs every 60s; revision 2 every hour. At the same wall
        // clock the hourly slot ID is ~60x smaller.
        let mut rule = monitor();
        rule.for_seconds = 0;
        rule.recover_for_seconds = 0;
        let (minutely, _) = evaluate(&rule, None, Observation::Value(1.0), 29_000_000, S);
        rule.revision = 2;
        rule.every_seconds = 3_600;
        let (hourly, change) = evaluate(
            &rule,
            Some(&minutely),
            Observation::Value(11.0),
            483_333,
            2 * S,
        );
        assert_eq!(hourly.status, AlertStatus::Firing);
        assert_eq!(hourly.last_slot_id, 483_333);
        assert_eq!(hourly.monitor_revision, 2);
        assert!(change.transitioned);
        // The next hourly slot is still newer, even though it is far smaller
        // than revision 1's last slot.
        let (next, _) = evaluate(
            &rule,
            Some(&hourly),
            Observation::Value(1.0),
            483_334,
            3 * S,
        );
        assert_eq!(next.last_slot_id, 483_334);
        assert_eq!(next.status, AlertStatus::Inactive);
    }

    #[test]
    fn interval_decrease_and_older_revision_results_are_ordered() {
        let mut rule = monitor();
        rule.for_seconds = 0;
        rule.recover_for_seconds = 0;
        rule.every_seconds = 3_600;
        let (hourly, _) = evaluate(&rule, None, Observation::Value(11.0), 483_333, S);
        let old_revision = rule.clone();
        rule.revision = 2;
        rule.every_seconds = 60;
        let (minutely, _) = evaluate(
            &rule,
            Some(&hourly),
            Observation::Value(1.0),
            29_000_000,
            2 * S,
        );
        assert_eq!(minutely.monitor_revision, 2);
        assert_eq!(minutely.status, AlertStatus::Inactive);
        // A late result for the older revision must not overwrite revision 2,
        // even with a larger slot ID.
        let (late, change) = evaluate(
            &old_revision,
            Some(&minutely),
            Observation::Value(11.0),
            483_334,
            3 * S,
        );
        assert_eq!(late, minutely);
        assert!(!change.transitioned);
        assert!(minutely.covers(1, u64::MAX));
        assert!(!minutely.covers(3, 0));
    }
}
