//! Progress bar + rolling stats for the replay.
//!
//! An `indicatif` progress bar (length = the corpus `_count`) whose message
//! carries a periodically-refreshed stats line: target vs achieved rec/s,
//! inflight saturation, throttled/rejected counts, ack p50 latency, and the
//! carry-forward/empty-body tallies. One widget, no interleaving with the bar.

use std::time::{Duration, Instant};

use indicatif::{ProgressBar, ProgressStyle};

use crate::map::MapCounters;
use crate::wire::Ack;
use scry_proto::constants::{ACK_ACCEPTED, ACK_REJECTED, ACK_THROTTLED};

/// Keep at most this many recent ack latencies for the p50 estimate.
const LATENCY_WINDOW: usize = 1024;

pub struct Stats {
    bar: ProgressBar,
    sent_records: u64,
    accepted: u64,
    throttled: u64,
    rejected: u64,
    /// Throttles observed since the last [`Stats::interval_pressure`] check —
    /// drives the ramp controller's knee detection.
    throttled_since_check: u64,
    latencies: Vec<Duration>,
    // Achieved-rate window.
    last_tick: Instant,
    last_records: u64,
    target_rate: u32,
}

impl Stats {
    pub fn new(total: u64) -> Self {
        let bar = ProgressBar::new(total);
        bar.set_style(
            ProgressStyle::with_template(
                "{spinner} [{elapsed_precise}] [{bar:32}] {pos}/{len} ({eta}) {msg}",
            )
            .unwrap()
            .progress_chars("=>-"),
        );
        Self {
            bar,
            sent_records: 0,
            accepted: 0,
            throttled: 0,
            rejected: 0,
            throttled_since_check: 0,
            latencies: Vec::with_capacity(LATENCY_WINDOW),
            last_tick: Instant::now(),
            last_records: 0,
            target_rate: 0,
        }
    }

    /// Account a batch of `n` records handed to the wire (advances the bar).
    pub fn on_records_sent(&mut self, n: u64) {
        self.sent_records += n;
        self.bar.inc(n);
    }

    /// Account one observed ack.
    pub fn on_ack(&mut self, ack: &Ack) {
        match ack.status {
            ACK_ACCEPTED => self.accepted += 1,
            ACK_THROTTLED => {
                self.throttled += 1;
                self.throttled_since_check += 1;
            }
            ACK_REJECTED => self.rejected += 1,
            _ => {}
        }
        if self.latencies.len() == LATENCY_WINDOW {
            self.latencies.remove(0);
        }
        self.latencies.push(ack.latency);
    }

    pub fn set_target_rate(&mut self, rate: u32) {
        self.target_rate = rate;
    }

    /// Whether the last window saw ingest back-pressure (sustained throttling or
    /// near-saturated inflight). Resets the throttle-since-check counter.
    pub fn interval_pressure(&mut self, inflight: usize, max_inflight: usize) -> bool {
        let throttled = self.throttled_since_check > 0;
        self.throttled_since_check = 0;
        let saturated = max_inflight > 0 && inflight * 100 >= max_inflight * 90;
        throttled || saturated
    }

    /// Refresh the stats line attached to the bar. Call on a fixed cadence.
    pub fn refresh_line(&mut self, inflight: usize, max_inflight: usize, counters: &MapCounters) {
        let now = Instant::now();
        let dt = now.duration_since(self.last_tick).as_secs_f64();
        let achieved = if dt > 0.0 {
            ((self.sent_records - self.last_records) as f64 / dt) as u64
        } else {
            0
        };
        self.last_tick = now;
        self.last_records = self.sent_records;

        let p50 = self.latency_p50();
        let frozen = ""; // caller may prepend ramp state via set_target
        self.bar.set_message(format!(
            "{frozen}target={} achieved={}/s | inflight={}/{} | ok={} throttled={} rejected={} | ack_p50={}ms | ts_inherited={} body_missing={}",
            self.target_rate,
            achieved,
            inflight,
            max_inflight,
            self.accepted,
            self.throttled,
            self.rejected,
            p50.as_millis(),
            counters.ts_inherited,
            counters.body_missing,
        ));
    }

    fn latency_p50(&self) -> Duration {
        if self.latencies.is_empty() {
            return Duration::ZERO;
        }
        let mut v = self.latencies.clone();
        v.sort_unstable();
        v[v.len() / 2]
    }

    pub fn throttled(&self) -> u64 {
        self.throttled
    }

    pub fn rejected(&self) -> u64 {
        self.rejected
    }

    /// Print a line above the bar (persists in scrollback).
    pub fn println(&self, msg: impl AsRef<str>) {
        self.bar.println(msg.as_ref());
    }

    /// Finish the bar with a final summary line.
    pub fn finish(&self, msg: impl Into<String>) {
        self.bar.finish_with_message(msg.into());
    }
}
