use sha2::{Digest, Sha256};

use crate::MonitorId;

const NANOS_PER_SECOND: u64 = 1_000_000_000;

/// One wall-clock-aligned logical evaluation slot.
///
/// The query window is `[end - lookback, end)` and the evaluation time is
/// `end`. `execute_at` adds the deployment's evaluation delay (lateness
/// allowance) and the monitor's deterministic jitter; neither changes the
/// slot identity or window.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EvaluationSlot {
    pub id: u64,
    pub start_unix_nano: u64,
    pub end_unix_nano: u64,
    pub execute_at_unix_nano: u64,
}

/// Slot `slot_id` of a monitor evaluated every `every_seconds`.
pub fn slot(
    monitor_id: MonitorId,
    slot_id: u64,
    every_seconds: u64,
    jitter_seconds: u64,
    evaluation_delay_nanos: u64,
) -> Option<EvaluationSlot> {
    let every = every_seconds.checked_mul(NANOS_PER_SECOND)?;
    if every == 0 {
        return None;
    }
    let start = slot_id.checked_mul(every)?;
    let end = start.checked_add(every)?;
    let jitter_cap = jitter_seconds
        .checked_mul(NANOS_PER_SECOND)?
        .min(every.saturating_sub(1));
    let jitter = deterministic_jitter(monitor_id, slot_id, jitter_cap);
    Some(EvaluationSlot {
        id: slot_id,
        start_unix_nano: start,
        end_unix_nano: end,
        execute_at_unix_nano: end
            .checked_add(evaluation_delay_nanos)?
            .checked_add(jitter)?,
    })
}

/// The newest slot whose `execute_at` is at or before `now`, if any.
///
/// A slot is eligible once it has ended *and* the evaluation delay plus its
/// jitter have elapsed. The newest ended-and-delayed slot may still be inside
/// its jitter; the slot before it is then due (its jitter is shorter than one
/// interval, so it is always past). The scheduler evaluates only this slot and
/// never replays older missed slots.
pub fn latest_due_slot(
    monitor_id: MonitorId,
    now_unix_nano: u64,
    every_seconds: u64,
    jitter_seconds: u64,
    evaluation_delay_nanos: u64,
) -> Option<EvaluationSlot> {
    let every = every_seconds.checked_mul(NANOS_PER_SECOND)?;
    if every == 0 {
        return None;
    }
    let horizon = now_unix_nano.checked_sub(evaluation_delay_nanos)?;
    // Slots `< ended` have end <= horizon.
    let ended = horizon / every;
    let newest = ended.checked_sub(1)?;
    let candidate = slot(
        monitor_id,
        newest,
        every_seconds,
        jitter_seconds,
        evaluation_delay_nanos,
    )?;
    if candidate.execute_at_unix_nano <= now_unix_nano {
        return Some(candidate);
    }
    let previous = slot(
        monitor_id,
        newest.checked_sub(1)?,
        every_seconds,
        jitter_seconds,
        evaluation_delay_nanos,
    )?;
    (previous.execute_at_unix_nano <= now_unix_nano).then_some(previous)
}

fn deterministic_jitter(monitor_id: MonitorId, slot_id: u64, cap: u64) -> u64 {
    if cap == 0 {
        return 0;
    }
    let mut hasher = Sha256::new();
    hasher.update(b"scry.alert.jitter.v1\0");
    hasher.update(monitor_id.0.as_bytes());
    hasher.update(slot_id.to_be_bytes());
    let digest: [u8; 32] = hasher.finalize().into();
    u64::from_be_bytes(digest[..8].try_into().expect("eight-byte prefix")) % (cap + 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    const S: u64 = NANOS_PER_SECOND;

    #[test]
    fn slots_are_aligned_and_jitter_is_stable() {
        let id = MonitorId(uuid::Uuid::from_u128(1));
        let first = slot(id, 2, 60, 5, 0).unwrap();
        assert_eq!(first, slot(id, 2, 60, 5, 0).unwrap());
        assert_eq!(first.start_unix_nano, 120 * S);
        assert_eq!(first.end_unix_nano, 180 * S);
        assert!(first.execute_at_unix_nano >= first.end_unix_nano);
        assert!(first.execute_at_unix_nano <= first.end_unix_nano + 5 * S);
        let delayed = slot(id, 2, 60, 5, 90 * S).unwrap();
        assert_eq!(
            delayed.execute_at_unix_nano,
            first.execute_at_unix_nano + 90 * S,
            "the delay moves execution, not identity or window"
        );
        assert_eq!(delayed.end_unix_nano, first.end_unix_nano);
    }

    #[test]
    fn zero_interval_is_rejected() {
        assert!(slot(MonitorId::new(), 1, 0, 0, 0).is_none());
        assert!(latest_due_slot(MonitorId::new(), 1, 0, 0, 0).is_none());
    }

    #[test]
    fn evaluation_delay_holds_back_a_just_ended_slot() {
        let id = MonitorId::new();
        // Slot 2 is [120s, 180s). Without jitter it becomes due at 180s + delay.
        assert_eq!(latest_due_slot(id, 180 * S, 60, 0, 0).unwrap().id, 2);
        assert_eq!(latest_due_slot(id, 269 * S, 60, 0, 90 * S).unwrap().id, 1);
        assert_eq!(latest_due_slot(id, 270 * S, 60, 0, 90 * S).unwrap().id, 2);
        assert_eq!(latest_due_slot(id, 299 * S, 60, 0, 90 * S).unwrap().id, 2);
    }

    #[test]
    fn newest_due_slot_falls_back_while_the_newest_is_inside_its_jitter() {
        let id = MonitorId(uuid::Uuid::from_u128(7));
        let delay = 90 * S;
        for newest in 3..200u64 {
            let candidate = slot(id, newest, 60, 59, delay).unwrap();
            let before = candidate.execute_at_unix_nano - 1;
            let due = latest_due_slot(id, before, 60, 59, delay).unwrap();
            assert!(due.execute_at_unix_nano <= before);
            if before >= candidate.end_unix_nano + delay {
                assert_eq!(due.id, newest - 1, "inside the jitter of {newest}");
            }
            assert_eq!(
                latest_due_slot(id, candidate.execute_at_unix_nano, 60, 59, delay)
                    .unwrap()
                    .id,
                newest
            );
        }
    }

    #[test]
    fn nothing_is_due_before_the_first_delayed_slot() {
        let id = MonitorId::new();
        assert!(latest_due_slot(id, 10 * S, 60, 0, 90 * S).is_none());
        assert!(latest_due_slot(id, 149 * S, 60, 0, 90 * S).is_none());
        assert_eq!(latest_due_slot(id, 150 * S, 60, 0, 90 * S).unwrap().id, 0);
    }
}
