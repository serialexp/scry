use std::time::{Instant, SystemTime, UNIX_EPOCH};

use serde_json::json;

const LOG_QUEUE_CAPACITY: usize = 8192;

#[derive(Debug)]
pub struct AgentRuntimeStats {
    started: Instant,
    pub log_batches: u64,
    pub metric_batches: u64,
    pub log_records: u64,
    pub metric_samples: u64,
    pub log_uncompressed_bytes: u64,
    pub metric_uncompressed_bytes: u64,
    pub log_compressed_bytes: u64,
    pub metric_compressed_bytes: u64,
    pub reconnect_attempts: u64,
    pub reconnect_successes: u64,
    pub status_send_failures: u64,
    pub last_send_unix_ms: Option<u64>,
}

impl Default for AgentRuntimeStats {
    fn default() -> Self {
        Self {
            started: Instant::now(),
            log_batches: 0,
            metric_batches: 0,
            log_records: 0,
            metric_samples: 0,
            log_uncompressed_bytes: 0,
            metric_uncompressed_bytes: 0,
            log_compressed_bytes: 0,
            metric_compressed_bytes: 0,
            reconnect_attempts: 0,
            reconnect_successes: 0,
            status_send_failures: 0,
            last_send_unix_ms: None,
        }
    }
}

pub struct SnapshotInputs<'a> {
    pub node: &'a str,
    pub server_addr: &'a str,
    pub log_pending_records: u32,
    pub log_pending_bytes: usize,
    pub metric_pending_samples: u32,
    pub metric_pending_bytes: usize,
    pub log_queue_remaining: usize,
    pub metric_queue_remaining: usize,
    pub log_dropped: u64,
    pub metric_dropped: u64,
}

impl AgentRuntimeStats {
    pub fn record_log_batch(&mut self, records: u32, uncompressed: u32, compressed: usize) {
        self.log_batches += 1;
        self.log_records += u64::from(records);
        self.log_uncompressed_bytes += u64::from(uncompressed);
        self.log_compressed_bytes += compressed as u64;
        self.last_send_unix_ms = Some(unix_ms());
    }

    pub fn record_metric_batch(&mut self, samples: u32, uncompressed: u32, compressed: usize) {
        self.metric_batches += 1;
        self.metric_samples += u64::from(samples);
        self.metric_uncompressed_bytes += u64::from(uncompressed);
        self.metric_compressed_bytes += compressed as u64;
        self.last_send_unix_ms = Some(unix_ms());
    }

    pub fn snapshot(&self, inputs: SnapshotInputs<'_>) -> serde_json::Value {
        let log_depth = LOG_QUEUE_CAPACITY.saturating_sub(inputs.log_queue_remaining);
        let metrics_depth =
            super::METRICS_QUEUE_CAPACITY.saturating_sub(inputs.metric_queue_remaining);
        json!({
            "role": "agent",
            "instance_id": format!("agent/{}", inputs.node),
            "addr": inputs.node,
            "now_unix_ms": unix_ms(),
            "uptime_secs": self.started.elapsed().as_secs_f64(),
            "rss_kib": rss_kib(),
            "data": {
                "version": env!("CARGO_PKG_VERSION"),
                "server_addr": inputs.server_addr,
                "log_batches": self.log_batches,
                "metric_batches": self.metric_batches,
                "log_records": self.log_records,
                "metric_samples": self.metric_samples,
                "log_uncompressed_bytes": self.log_uncompressed_bytes,
                "metric_uncompressed_bytes": self.metric_uncompressed_bytes,
                "log_compressed_bytes": self.log_compressed_bytes,
                "metric_compressed_bytes": self.metric_compressed_bytes,
                "log_dropped": inputs.log_dropped,
                "metric_dropped": inputs.metric_dropped,
                "log_pending_records": inputs.log_pending_records,
                "log_pending_bytes": inputs.log_pending_bytes,
                "metric_pending_samples": inputs.metric_pending_samples,
                "metric_pending_bytes": inputs.metric_pending_bytes,
                "log_queue_depth": log_depth,
                "log_queue_capacity": LOG_QUEUE_CAPACITY,
                "metric_queue_depth": metrics_depth,
                "metric_queue_capacity": super::METRICS_QUEUE_CAPACITY,
                "reconnect_attempts": self.reconnect_attempts,
                "reconnect_successes": self.reconnect_successes,
                "status_send_failures": self.status_send_failures,
                "last_send_unix_ms": self.last_send_unix_ms,
            }
        })
    }
}

fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn rss_kib() -> Option<u64> {
    std::fs::read_to_string("/proc/self/status")
        .ok()?
        .lines()
        .find_map(|line| line.strip_prefix("VmRSS:"))?
        .split_whitespace()
        .next()?
        .parse()
        .ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_has_stable_host_identity_and_runtime_counts() {
        let mut stats = AgentRuntimeStats::default();
        stats.record_metric_batch(12, 300, 100);
        stats.reconnect_attempts = 2;
        stats.reconnect_successes = 1;
        let snapshot = stats.snapshot(SnapshotInputs {
            node: "worker-1",
            server_addr: "ingest:4000",
            log_pending_records: 3,
            log_pending_bytes: 90,
            metric_pending_samples: 4,
            metric_pending_bytes: 120,
            log_queue_remaining: LOG_QUEUE_CAPACITY - 2,
            metric_queue_remaining: super::super::METRICS_QUEUE_CAPACITY - 1,
            log_dropped: 5,
            metric_dropped: 6,
        });
        assert_eq!(snapshot["instance_id"], "agent/worker-1");
        assert_eq!(snapshot["data"]["metric_samples"], 12);
        assert_eq!(snapshot["data"]["metric_queue_depth"], 1);
        assert_eq!(snapshot["data"]["reconnect_attempts"], 2);
    }
}
