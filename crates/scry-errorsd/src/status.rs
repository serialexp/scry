use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Instant,
};

use scry_status::{rss_kib, unix_ms_now, LocalStatus, StatusSnapshot};

use crate::{engine::EngineReport, Mode};

/// Fixed-size status: no queues, identities, payloads, or per-occurrence state.
pub(crate) struct ErrorsStatus {
    instance_id: String,
    deployment_id: String,
    mode: Mode,
    started: Instant,
    reconcile_successes: AtomicU64,
    reconcile_failures: AtomicU64,
    last_success_unix_ms: AtomicU64,
    last_failure_unix_ms: AtomicU64,
    candidate_blocks: AtomicU64,
    processed_blocks: AtomicU64,
    occurrence_rows: AtomicU64,
    skipped_records: AtomicU64,
    raw_meta_quarantined: AtomicU64,
    source_blocks_quarantined: AtomicU64,
    occurrence_commits_quarantined: AtomicU64,
    publications_created: AtomicU64,
    publications_existing: AtomicU64,
    fold_inserted: AtomicU64,
    fold_exact_duplicates: AtomicU64,
    fold_collisions: AtomicU64,
    issues_created: AtomicU64,
    issues_updated: AtomicU64,
    occurrences_grouped: AtomicU64,
    sources_already_covered: AtomicU64,
    commits_already_folded: AtomicU64,
    grouping_scanned: AtomicU64,
    grouping_failures: AtomicU64,
    grouping_drained: AtomicU64,
}

impl ErrorsStatus {
    pub(crate) fn new(instance_id: String, deployment_id: String, mode: Mode) -> Self {
        Self {
            instance_id,
            deployment_id,
            mode,
            started: Instant::now(),
            reconcile_successes: AtomicU64::new(0),
            reconcile_failures: AtomicU64::new(0),
            last_success_unix_ms: AtomicU64::new(0),
            last_failure_unix_ms: AtomicU64::new(0),
            candidate_blocks: AtomicU64::new(0),
            processed_blocks: AtomicU64::new(0),
            occurrence_rows: AtomicU64::new(0),
            skipped_records: AtomicU64::new(0),
            raw_meta_quarantined: AtomicU64::new(0),
            source_blocks_quarantined: AtomicU64::new(0),
            occurrence_commits_quarantined: AtomicU64::new(0),
            publications_created: AtomicU64::new(0),
            publications_existing: AtomicU64::new(0),
            fold_inserted: AtomicU64::new(0),
            fold_exact_duplicates: AtomicU64::new(0),
            fold_collisions: AtomicU64::new(0),
            issues_created: AtomicU64::new(0),
            issues_updated: AtomicU64::new(0),
            occurrences_grouped: AtomicU64::new(0),
            sources_already_covered: AtomicU64::new(0),
            commits_already_folded: AtomicU64::new(0),
            grouping_scanned: AtomicU64::new(0),
            grouping_failures: AtomicU64::new(0),
            grouping_drained: AtomicU64::new(0),
        }
    }

    pub(crate) fn record_success(&self, report: EngineReport) {
        self.candidate_blocks
            .store(report.candidate_blocks as u64, Ordering::Relaxed);
        self.processed_blocks
            .store(report.processed_blocks as u64, Ordering::Relaxed);
        self.occurrence_rows
            .store(report.occurrence_rows as u64, Ordering::Relaxed);
        self.skipped_records
            .store(report.skipped_records as u64, Ordering::Relaxed);
        self.raw_meta_quarantined
            .store(report.raw_meta_quarantined as u64, Ordering::Relaxed);
        self.source_blocks_quarantined
            .store(report.source_blocks_quarantined as u64, Ordering::Relaxed);
        self.occurrence_commits_quarantined.store(
            report.occurrence_commits_quarantined as u64,
            Ordering::Relaxed,
        );
        self.publications_created
            .store(report.publications_created as u64, Ordering::Relaxed);
        self.publications_existing
            .store(report.publications_existing as u64, Ordering::Relaxed);
        self.fold_inserted
            .store(report.fold.inserted as u64, Ordering::Relaxed);
        self.fold_exact_duplicates
            .store(report.fold.exact_duplicates as u64, Ordering::Relaxed);
        self.fold_collisions
            .store(report.fold.collisions as u64, Ordering::Relaxed);
        self.issues_created
            .store(report.grouping.issues_created as u64, Ordering::Relaxed);
        self.issues_updated
            .store(report.grouping.issues_updated as u64, Ordering::Relaxed);
        self.occurrences_grouped.store(
            report.grouping.occurrences_grouped as u64,
            Ordering::Relaxed,
        );
        self.sources_already_covered
            .store(report.sources_already_covered as u64, Ordering::Relaxed);
        self.commits_already_folded
            .store(report.commits_already_folded as u64, Ordering::Relaxed);
        self.grouping_scanned
            .store(report.grouping.scanned as u64, Ordering::Relaxed);
        self.grouping_failures
            .store(report.grouping.failures as u64, Ordering::Relaxed);
        self.grouping_drained
            .store(u64::from(report.grouping.drained), Ordering::Relaxed);
        self.last_success_unix_ms
            .store(unix_ms_now(), Ordering::Release);
        self.reconcile_successes.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_failure(&self) {
        self.last_failure_unix_ms
            .store(unix_ms_now(), Ordering::Release);
        self.reconcile_failures.fetch_add(1, Ordering::Relaxed);
    }
}

impl LocalStatus for ErrorsStatus {
    fn snapshot(&self) -> StatusSnapshot {
        let now_unix_ms = unix_ms_now();
        let last_success_unix_ms = self.last_success_unix_ms.load(Ordering::Acquire);
        StatusSnapshot {
            role: "errors".to_owned(),
            instance_id: self.instance_id.clone(),
            addr: String::new(),
            version: env!("CARGO_PKG_VERSION").to_owned(),
            now_unix_ms,
            uptime_secs: self.started.elapsed().as_secs_f64(),
            rss_kib: rss_kib(),
            data: serde_json::json!({
                "deployment_id": self.deployment_id,
                "mode": match self.mode {
                    Mode::SingleWriter => "single_writer",
                    Mode::ReadOnly => "read_only",
                    Mode::Clustered => "clustered",
                },
                "object_store": "ready",
                "manifest": "verified",
                "occurrence_processing": "bounded_completion_relative_loop",
                "reconcile_successes": self.reconcile_successes.load(Ordering::Relaxed),
                "reconcile_failures": self.reconcile_failures.load(Ordering::Relaxed),
                "last_success_unix_ms": last_success_unix_ms,
                "last_failure_unix_ms": self.last_failure_unix_ms.load(Ordering::Acquire),
                "reconcile_lag_ms": (last_success_unix_ms != 0)
                    .then(|| now_unix_ms.saturating_sub(last_success_unix_ms)),
                "last_report": {
                    "candidate_blocks": self.candidate_blocks.load(Ordering::Relaxed),
                    "processed_blocks": self.processed_blocks.load(Ordering::Relaxed),
                    "pending_blocks": self.candidate_blocks.load(Ordering::Relaxed)
                        .saturating_sub(self.processed_blocks.load(Ordering::Relaxed)),
                    "occurrence_rows": self.occurrence_rows.load(Ordering::Relaxed),
                    "skipped_records": self.skipped_records.load(Ordering::Relaxed),
                    "raw_meta_quarantined": self.raw_meta_quarantined.load(Ordering::Relaxed),
                    "source_blocks_quarantined": self.source_blocks_quarantined.load(Ordering::Relaxed),
                    "occurrence_commits_quarantined": self.occurrence_commits_quarantined.load(Ordering::Relaxed),
                    "publications_created": self.publications_created.load(Ordering::Relaxed),
                    "publications_existing": self.publications_existing.load(Ordering::Relaxed),
                    "fold_inserted": self.fold_inserted.load(Ordering::Relaxed),
                    "fold_exact_duplicates": self.fold_exact_duplicates.load(Ordering::Relaxed),
                    "fold_collisions": self.fold_collisions.load(Ordering::Relaxed),
                    "issues_created": self.issues_created.load(Ordering::Relaxed),
                    "issues_updated": self.issues_updated.load(Ordering::Relaxed),
                    "occurrences_grouped": self.occurrences_grouped.load(Ordering::Relaxed),
                    "sources_already_covered": self.sources_already_covered.load(Ordering::Relaxed),
                    "commits_already_folded": self.commits_already_folded.load(Ordering::Relaxed),
                    "grouping_scanned": self.grouping_scanned.load(Ordering::Relaxed),
                    "grouping_failures": self.grouping_failures.load(Ordering::Relaxed),
                    "grouping_drained": self.grouping_drained.load(Ordering::Relaxed) == 1,
                }
            }),
        }
    }
}
