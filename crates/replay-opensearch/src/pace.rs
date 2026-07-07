//! Rate pacing and the auto-ramp-hold-at-knee controller.
//!
//! Two pieces:
//!
//! - [`RampController`] — the pure, unit-tested rate policy. It climbs the target
//!   record-rate on a fixed schedule until the ingest server pushes back
//!   (`ACK_THROTTLED` or near-100% inflight saturation), then **freezes** at the
//!   last rate: the knee. Hands-off ceiling discovery.
//! - [`Pacer`] — a monotonic record-rate limiter over `tokio::time`. It meters
//!   *records* (not batches), advancing a deadline by `records / rate`, so a
//!   burst of large batches still averages the target rate.

use tokio::time::{sleep_until, Duration, Instant};

/// The pure rate policy. Call [`RampController::tick`] once per step interval
/// with whether the last interval saw ingest back-pressure.
#[derive(Debug, Clone)]
pub struct RampController {
    current: u32,
    step: u32,
    /// Ceiling; `0` means unbounded.
    max: u32,
    frozen: bool,
}

impl RampController {
    pub fn new(start: u32, step: u32, max: u32) -> Self {
        Self {
            current: start.max(1),
            step,
            max,
            frozen: false,
        }
    }

    /// The current target rate (records/sec).
    pub fn rate(&self) -> u32 {
        self.current
    }

    /// Whether the controller has frozen at the knee.
    pub fn frozen(&self) -> bool {
        self.frozen
    }

    /// Advance one step interval. `pressure` = the last interval sustained
    /// throttling or inflight saturation. Once pressure is seen the rate freezes
    /// permanently (hold at the knee); otherwise it climbs by `step` up to `max`.
    pub fn tick(&mut self, pressure: bool) {
        if self.frozen {
            return;
        }
        if pressure {
            self.frozen = true;
            return;
        }
        if self.max != 0 && self.current >= self.max {
            return;
        }
        let next = self.current.saturating_add(self.step);
        self.current = if self.max != 0 {
            next.min(self.max)
        } else {
            next
        };
    }
}

/// A record-rate limiter. Advance `deadline` by `n / rate` before each batch and
/// sleep to it. Rate is read fresh each call so a mid-run ramp takes effect.
pub struct Pacer {
    deadline: Instant,
}

impl Pacer {
    pub fn new() -> Self {
        Self {
            deadline: Instant::now(),
        }
    }

    /// Reserve capacity for `n` records at `rate` rec/s and sleep until the slot
    /// opens. A rate of 0 is treated as "unbounded" (no sleep).
    pub async fn pace(&mut self, n: usize, rate: u32) {
        if rate == 0 || n == 0 {
            return;
        }
        let micros = (n as u64 * 1_000_000) / rate as u64;
        self.deadline += Duration::from_micros(micros);
        let now = Instant::now();
        if self.deadline > now {
            sleep_until(self.deadline).await;
        } else {
            // We've fallen behind (the sender can't keep up): reset the deadline
            // to now so we don't accumulate an ever-growing sleep debt.
            self.deadline = now;
        }
    }
}

impl Default for Pacer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn climbs_by_step_up_to_max() {
        let mut c = RampController::new(500, 500, 2000);
        assert_eq!(c.rate(), 500);
        c.tick(false);
        assert_eq!(c.rate(), 1000);
        c.tick(false);
        assert_eq!(c.rate(), 1500);
        c.tick(false);
        assert_eq!(c.rate(), 2000);
        c.tick(false); // clamped at max
        assert_eq!(c.rate(), 2000);
        assert!(!c.frozen());
    }

    #[test]
    fn unbounded_when_max_zero() {
        let mut c = RampController::new(500, 500, 0);
        for _ in 0..10 {
            c.tick(false);
        }
        assert_eq!(c.rate(), 5500);
    }

    #[test]
    fn freezes_at_knee_on_pressure() {
        let mut c = RampController::new(500, 500, 0);
        c.tick(false);
        assert_eq!(c.rate(), 1000);
        c.tick(true); // back-pressure → freeze here
        assert_eq!(c.rate(), 1000);
        assert!(c.frozen());
        // Stays frozen even if pressure clears.
        c.tick(false);
        assert_eq!(c.rate(), 1000);
        assert!(c.frozen());
    }
}
