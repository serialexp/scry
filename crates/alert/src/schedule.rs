use sha2::{Digest, Sha256};

use crate::MonitorId;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EvaluationSlot {
    pub id: u64,
    pub start_unix_nano: u64,
    pub end_unix_nano: u64,
    pub execute_at_unix_nano: u64,
}

pub fn slot_at(
    monitor_id: MonitorId,
    now_unix_nano: u64,
    every_seconds: u64,
    jitter_seconds: u64,
) -> Option<EvaluationSlot> {
    let every = every_seconds.checked_mul(1_000_000_000)?;
    if every == 0 {
        return None;
    }
    let id = now_unix_nano / every;
    let start = id.checked_mul(every)?;
    let end = start.checked_add(every)?;
    let jitter_cap = jitter_seconds
        .checked_mul(1_000_000_000)?
        .min(every.saturating_sub(1));
    let jitter = deterministic_jitter(monitor_id, id, jitter_cap);
    Some(EvaluationSlot {
        id,
        start_unix_nano: start,
        end_unix_nano: end,
        execute_at_unix_nano: end.checked_add(jitter)?,
    })
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

    #[test]
    fn slots_are_aligned_and_jitter_is_stable() {
        let id = MonitorId(uuid::Uuid::from_u128(1));
        let first = slot_at(id, 125_000_000_000, 60, 5).unwrap();
        let second = slot_at(id, 125_999_999_999, 60, 5).unwrap();
        assert_eq!(first, second);
        assert_eq!(first.id, 2);
        assert_eq!(first.start_unix_nano, 120_000_000_000);
        assert_eq!(first.end_unix_nano, 180_000_000_000);
        assert!(first.execute_at_unix_nano >= first.end_unix_nano);
        assert!(first.execute_at_unix_nano <= first.end_unix_nano + 5_000_000_000);
    }

    #[test]
    fn zero_interval_is_rejected() {
        assert!(slot_at(MonitorId::new(), 1, 0, 0).is_none());
    }
}
