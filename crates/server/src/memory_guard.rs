//! Process-level memory safety for query work.
//!
//! DataFusion's pool does not account for every process allocation. This guard
//! therefore makes admission decisions from coherent cgroup committed-memory
//! snapshots. New-query admission may run one serialized, rate-limited global
//! reclaim; runtime checks only probe and never reclaim.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use async_trait::async_trait;
use scry_resources::{CgroupMemoryDescriptor, CgroupMemorySnapshot};

pub const PROCESS_MEMORY_PRESSURE_MESSAGE: &str =
    "Process memory safety pressure remains after reclamation.";
pub const PROCESS_MEMORY_RUNTIME_PRESSURE_MESSAGE: &str = "Process memory safety pressure.";
pub const PROCESS_MEMORY_PROBE_UNAVAILABLE_MESSAGE: &str =
    "Process memory safety probe unavailable.";
/// Legacy symbol retained for existing server call sites; its text now names
/// process pressure truthfully rather than assigning it to the query.
pub const QUERY_TOO_LARGE_MESSAGE: &str = PROCESS_MEMORY_RUNTIME_PRESSURE_MESSAGE;
pub const RECLAIM_INTERVAL: Duration = Duration::from_secs(30);

/// Owner-estimated application memory released during a global reclaim.
///
/// These values are operational estimates, not a promise that the allocator or
/// kernel has already reduced the cgroup charge. Admission trusts only the
/// subsequent cgroup reprobe.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MemoryReclaimReport {
    pub released_entries: u64,
    pub released_bytes: u64,
    /// Allocator purge failures are diagnostic: admission still performs its
    /// mandatory post-reclaim probe and decides from that fresh snapshot.
    pub allocator_error: Option<String>,
}

/// Synchronous process-wide reclamation invoked only on new-query admission.
pub trait MemoryReclaimer: Send + Sync {
    fn reclaim(&self) -> Result<MemoryReclaimReport>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MemoryAdmissionDecision {
    Admitted,
    AdmittedAfterConcurrentReclaim,
    AdmittedAfterReclaim,
    RejectedRateLimited,
    RejectedAfterReclaim,
    ProbeUnavailable,
}

/// Bounded diagnostic record for the most recent new-query admission decision.
#[derive(Clone, Debug)]
pub struct MemoryAdmissionOutcome {
    pub decision: MemoryAdmissionDecision,
    pub initial: Option<CgroupMemorySnapshot>,
    pub pre_reclaim: Option<CgroupMemorySnapshot>,
    pub post_reclaim: Option<CgroupMemorySnapshot>,
    pub reclaim_report: Option<MemoryReclaimReport>,
    pub reclaim_error: Option<String>,
}

impl MemoryAdmissionOutcome {
    fn admitted(snapshot: CgroupMemorySnapshot) -> Self {
        Self {
            decision: MemoryAdmissionDecision::Admitted,
            initial: Some(snapshot),
            pre_reclaim: None,
            post_reclaim: None,
            reclaim_report: None,
            reclaim_error: None,
        }
    }
}

/// Detail captured by the runtime watcher when it cancels in-flight work.
#[derive(Debug)]
pub struct RuntimeMemoryFailure {
    pub error: anyhow::Error,
    pub snapshot: Option<CgroupMemorySnapshot>,
}

impl std::fmt::Display for RuntimeMemoryFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.error.fmt(formatter)
    }
}

#[async_trait]
pub trait QueryMemoryGuard: Send + Sync {
    /// Runtime safety check. This method must never trigger global reclamation.
    fn check(&self) -> Result<()>;

    /// Runtime check paired with the coherent snapshot that caused the result.
    /// Check-only fakes retain their existing behavior without diagnostics.
    fn check_with_snapshot(&self) -> (Result<()>, Option<CgroupMemorySnapshot>) {
        (self.check(), None)
    }

    /// New-query admission hook. Existing test guards only implementing
    /// [`Self::check`] retain their prior behavior through this default.
    fn admit_new_query(&self) -> Result<()> {
        self.check()
    }

    /// Perform admission and return diagnostics belonging to this exact call.
    ///
    /// The default keeps check-only fakes source-compatible. Implementations
    /// that produce diagnostics override this method so callers never have to
    /// pair an admission result with racy process-global "last" state.
    fn admit_new_query_with_outcome(&self) -> (Result<()>, Option<MemoryAdmissionOutcome>) {
        (self.admit_new_query(), None)
    }

    /// Async admission boundary used by the network service. The default keeps
    /// lightweight test guards simple; production guards with O(cache-size)
    /// reclamation override this to use the blocking pool.
    async fn admit_new_query_with_outcome_async(
        self: Arc<Self>,
    ) -> (Result<()>, Option<MemoryAdmissionOutcome>) {
        self.admit_new_query_with_outcome()
    }

    /// Most recent admission outcome, when the implementation records one.
    /// This is process-wide diagnostic state and must not be used to attribute
    /// telemetry to an individual request. The default keeps existing test
    /// guards source-compatible.
    fn last_admission_outcome(&self) -> Option<MemoryAdmissionOutcome> {
        None
    }

    /// Wait until the process enters the unsafe region. Used around planning
    /// and streaming so in-flight work is cancelled without reclaiming caches.
    /// Returns the failing probe/pressure detail that triggered cancellation.
    async fn wait_until_exhausted(&self) -> RuntimeMemoryFailure {
        loop {
            let (result, snapshot) = self.check_with_snapshot();
            if let Err(error) = result {
                return RuntimeMemoryFailure { error, snapshot };
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

trait MemoryProbe: Send + Sync {
    fn snapshot(&self) -> std::io::Result<CgroupMemorySnapshot>;
}

impl MemoryProbe for CgroupMemoryDescriptor {
    fn snapshot(&self) -> std::io::Result<CgroupMemorySnapshot> {
        CgroupMemoryDescriptor::snapshot(self)
    }
}

#[derive(Default)]
struct NoopReclaimer;

impl MemoryReclaimer for NoopReclaimer {
    fn reclaim(&self) -> Result<MemoryReclaimReport> {
        Ok(MemoryReclaimReport::default())
    }
}

#[derive(Default)]
struct ReclaimState {
    last_attempt: Option<Instant>,
}

/// Mount-aware cgroup memory guard using committed rather than raw charge.
pub struct CgroupMemoryGuard {
    probe: Arc<dyn MemoryProbe>,
    reserve_bytes: u64,
    reclaimer: Arc<dyn MemoryReclaimer>,
    reclaim_state: Mutex<ReclaimState>,
    last_outcome: Mutex<Option<MemoryAdmissionOutcome>>,
    reclaim_interval: Duration,
}

impl CgroupMemoryGuard {
    /// Detect a finite cgroup constraint. This compatibility constructor has a
    /// no-op reclaimer; production composition should use
    /// [`Self::detect_with_reclaimer`] once application release owners exist.
    pub fn detect(reserve_bytes: u64) -> Result<Option<Self>> {
        Self::detect_with_reclaimer(reserve_bytes, Arc::new(NoopReclaimer))
    }

    pub fn detect_with_reclaimer(
        reserve_bytes: u64,
        reclaimer: Arc<dyn MemoryReclaimer>,
    ) -> Result<Option<Self>> {
        Ok(scry_resources::try_detect_cgroup_memory_descriptor()
            .context("discovering cgroup memory controls")?
            .map(|descriptor| Self::new(descriptor, reserve_bytes, reclaimer)))
    }

    pub fn new(
        descriptor: CgroupMemoryDescriptor,
        reserve_bytes: u64,
        reclaimer: Arc<dyn MemoryReclaimer>,
    ) -> Self {
        Self::from_probe(
            Arc::new(descriptor),
            reserve_bytes,
            reclaimer,
            RECLAIM_INTERVAL,
        )
    }

    fn from_probe(
        probe: Arc<dyn MemoryProbe>,
        reserve_bytes: u64,
        reclaimer: Arc<dyn MemoryReclaimer>,
        reclaim_interval: Duration,
    ) -> Self {
        Self {
            probe,
            reserve_bytes,
            reclaimer,
            reclaim_state: Mutex::new(ReclaimState::default()),
            last_outcome: Mutex::new(None),
            reclaim_interval,
        }
    }

    /// Most recent admission outcome. A poisoned diagnostics lock is recovered;
    /// it is not part of the safety decision.
    pub fn last_admission_outcome(&self) -> Option<MemoryAdmissionOutcome> {
        self.last_outcome
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    pub fn reserve_bytes(&self) -> u64 {
        self.reserve_bytes
    }

    /// Compatibility accessor based on a fresh coherent snapshot.
    pub fn limit_bytes(&self) -> u64 {
        self.probe
            .snapshot()
            .map_or(0, |snapshot| snapshot.limit_bytes)
    }

    /// Compatibility accessor based on a fresh coherent snapshot.
    pub fn reject_at_bytes(&self) -> u64 {
        self.probe
            .snapshot()
            .map_or(0, |snapshot| self.threshold(snapshot))
    }

    fn threshold(&self, snapshot: CgroupMemorySnapshot) -> u64 {
        snapshot
            .limit_bytes
            .saturating_sub(self.reserve_bytes.min(snapshot.limit_bytes))
    }

    fn pressured(&self, snapshot: CgroupMemorySnapshot) -> bool {
        snapshot.committed_bytes >= self.threshold(snapshot)
    }

    fn probe(&self) -> Result<CgroupMemorySnapshot> {
        self.probe
            .snapshot()
            .context(PROCESS_MEMORY_PROBE_UNAVAILABLE_MESSAGE)
    }

    fn record(&self, outcome: MemoryAdmissionOutcome) {
        *self
            .last_outcome
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(outcome);
    }

    fn finish_admission(
        &self,
        result: Result<()>,
        outcome: MemoryAdmissionOutcome,
    ) -> (Result<()>, MemoryAdmissionOutcome) {
        self.record(outcome.clone());
        (result, outcome)
    }

    fn pressure_error(&self, snapshot: CgroupMemorySnapshot, message: &str) -> anyhow::Error {
        anyhow::anyhow!(
            "{message} committed={} bytes, reclaimable_clean_file={} bytes, current={} bytes, safety threshold={} bytes, limit={} bytes, reserve={} bytes",
            snapshot.committed_bytes,
            snapshot.reclaimable_clean_file_bytes,
            snapshot.current_bytes,
            self.threshold(snapshot),
            snapshot.limit_bytes,
            self.reserve_bytes,
        )
    }

    fn admit(&self) -> (Result<()>, MemoryAdmissionOutcome) {
        let initial = match self.probe() {
            Ok(snapshot) => snapshot,
            Err(error) => {
                return self.finish_admission(
                    Err(error),
                    MemoryAdmissionOutcome {
                        decision: MemoryAdmissionDecision::ProbeUnavailable,
                        initial: None,
                        pre_reclaim: None,
                        post_reclaim: None,
                        reclaim_report: None,
                        reclaim_error: None,
                    },
                );
            }
        };
        if !self.pressured(initial) {
            return self.finish_admission(Ok(()), MemoryAdmissionOutcome::admitted(initial));
        }

        let mut state = match self.reclaim_state.lock() {
            Ok(state) => state,
            Err(_) => {
                return self.finish_admission(
                    Err(anyhow::anyhow!(
                        "{PROCESS_MEMORY_PROBE_UNAVAILABLE_MESSAGE} reclamation coordinator lock poisoned"
                    )),
                    MemoryAdmissionOutcome {
                        decision: MemoryAdmissionDecision::ProbeUnavailable,
                        initial: Some(initial),
                        pre_reclaim: None,
                        post_reclaim: None,
                        reclaim_report: None,
                        reclaim_error: None,
                    },
                );
            }
        };
        let pre_reclaim = match self.probe() {
            Ok(snapshot) => snapshot,
            Err(error) => {
                return self.finish_admission(
                    Err(error),
                    MemoryAdmissionOutcome {
                        decision: MemoryAdmissionDecision::ProbeUnavailable,
                        initial: Some(initial),
                        pre_reclaim: None,
                        post_reclaim: None,
                        reclaim_report: None,
                        reclaim_error: None,
                    },
                );
            }
        };
        if !self.pressured(pre_reclaim) {
            return self.finish_admission(
                Ok(()),
                MemoryAdmissionOutcome {
                    decision: MemoryAdmissionDecision::AdmittedAfterConcurrentReclaim,
                    initial: Some(initial),
                    pre_reclaim: Some(pre_reclaim),
                    post_reclaim: None,
                    reclaim_report: None,
                    reclaim_error: None,
                },
            );
        }

        let now = Instant::now();
        if state
            .last_attempt
            .is_some_and(|last| now.saturating_duration_since(last) < self.reclaim_interval)
        {
            return self.finish_admission(
                Err(self.pressure_error(
                    pre_reclaim,
                    "Process memory safety pressure; global reclamation is rate-limited.",
                )),
                MemoryAdmissionOutcome {
                    decision: MemoryAdmissionDecision::RejectedRateLimited,
                    initial: Some(initial),
                    pre_reclaim: Some(pre_reclaim),
                    post_reclaim: None,
                    reclaim_report: None,
                    reclaim_error: None,
                },
            );
        }
        state.last_attempt = Some(now);

        let (reclaim_report, reclaim_error) = match self.reclaimer.reclaim() {
            Ok(report) => (Some(report), None),
            Err(error) => (None, Some(format!("{error:#}"))),
        };
        // This reprobe is mandatory even when reclamation itself reports failure.
        let post_reclaim = match self.probe() {
            Ok(snapshot) => snapshot,
            Err(error) => {
                return self.finish_admission(
                    Err(error),
                    MemoryAdmissionOutcome {
                        decision: MemoryAdmissionDecision::ProbeUnavailable,
                        initial: Some(initial),
                        pre_reclaim: Some(pre_reclaim),
                        post_reclaim: None,
                        reclaim_report,
                        reclaim_error,
                    },
                );
            }
        };
        let admitted = !self.pressured(post_reclaim);
        let result = if admitted {
            Ok(())
        } else {
            Err(self.pressure_error(post_reclaim, PROCESS_MEMORY_PRESSURE_MESSAGE))
        };
        self.finish_admission(
            result,
            MemoryAdmissionOutcome {
                decision: if admitted {
                    MemoryAdmissionDecision::AdmittedAfterReclaim
                } else {
                    MemoryAdmissionDecision::RejectedAfterReclaim
                },
                initial: Some(initial),
                pre_reclaim: Some(pre_reclaim),
                post_reclaim: Some(post_reclaim),
                reclaim_report,
                reclaim_error,
            },
        )
    }
}

#[async_trait]
impl QueryMemoryGuard for CgroupMemoryGuard {
    fn check(&self) -> Result<()> {
        self.check_with_snapshot().0
    }

    fn check_with_snapshot(&self) -> (Result<()>, Option<CgroupMemorySnapshot>) {
        let snapshot = match self.probe() {
            Ok(snapshot) => snapshot,
            Err(error) => return (Err(error), None),
        };
        if self.pressured(snapshot) {
            return (
                Err(self.pressure_error(snapshot, PROCESS_MEMORY_RUNTIME_PRESSURE_MESSAGE)),
                Some(snapshot),
            );
        }
        (Ok(()), Some(snapshot))
    }

    fn admit_new_query(&self) -> Result<()> {
        self.admit().0
    }

    fn admit_new_query_with_outcome(&self) -> (Result<()>, Option<MemoryAdmissionOutcome>) {
        let (result, outcome) = self.admit();
        (result, Some(outcome))
    }

    async fn admit_new_query_with_outcome_async(
        self: Arc<Self>,
    ) -> (Result<()>, Option<MemoryAdmissionOutcome>) {
        match tokio::task::spawn_blocking(move || self.admit_new_query_with_outcome()).await {
            Ok(outcome) => outcome,
            Err(error) => (
                Err(anyhow::anyhow!(
                    "{PROCESS_MEMORY_PROBE_UNAVAILABLE_MESSAGE} admission worker failed: {error}"
                )),
                None,
            ),
        }
    }

    fn last_admission_outcome(&self) -> Option<MemoryAdmissionOutcome> {
        CgroupMemoryGuard::last_admission_outcome(self)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::io;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::thread;

    use scry_resources::CgroupVersion;

    use super::*;

    fn snapshot(current: u64, reclaimable: u64, committed: u64) -> CgroupMemorySnapshot {
        CgroupMemorySnapshot {
            limit_bytes: 1_000,
            current_bytes: current,
            reclaimable_clean_file_bytes: reclaimable,
            committed_bytes: committed,
            version: CgroupVersion::V2,
        }
    }

    struct ScriptedProbe(Mutex<VecDeque<io::Result<CgroupMemorySnapshot>>>);

    impl ScriptedProbe {
        fn new(values: impl IntoIterator<Item = io::Result<CgroupMemorySnapshot>>) -> Self {
            Self(Mutex::new(values.into_iter().collect()))
        }
    }

    impl MemoryProbe for ScriptedProbe {
        fn snapshot(&self) -> io::Result<CgroupMemorySnapshot> {
            self.0
                .lock()
                .unwrap()
                .pop_front()
                .expect("unexpected probe")
        }
    }

    struct FakeReclaimer {
        calls: AtomicUsize,
        fail: bool,
    }

    impl FakeReclaimer {
        fn new(fail: bool) -> Self {
            Self {
                calls: AtomicUsize::new(0),
                fail,
            }
        }
    }

    impl MemoryReclaimer for FakeReclaimer {
        fn reclaim(&self) -> Result<MemoryReclaimReport> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.fail {
                anyhow::bail!("allocator purge failed")
            }
            Ok(MemoryReclaimReport {
                released_entries: 7,
                released_bytes: 4096,
                allocator_error: None,
            })
        }
    }

    fn guard(
        snapshots: impl IntoIterator<Item = io::Result<CgroupMemorySnapshot>>,
        reclaimer: Arc<FakeReclaimer>,
    ) -> CgroupMemoryGuard {
        CgroupMemoryGuard::from_probe(
            Arc::new(ScriptedProbe::new(snapshots)),
            100,
            reclaimer,
            RECLAIM_INTERVAL,
        )
    }

    struct CheckOnlyGuard(AtomicBool);

    #[async_trait]
    impl QueryMemoryGuard for CheckOnlyGuard {
        fn check(&self) -> Result<()> {
            if self.0.load(Ordering::Relaxed) {
                anyhow::bail!("fake pressure")
            }
            Ok(())
        }
    }

    #[test]
    fn default_new_query_admission_preserves_check_only_fakes() {
        let guard = CheckOnlyGuard(AtomicBool::new(false));
        let (result, outcome) = guard.admit_new_query_with_outcome();
        result.unwrap();
        assert!(outcome.is_none());
        guard.0.store(true, Ordering::Relaxed);
        let (result, outcome) = guard.admit_new_query_with_outcome();
        assert!(result.is_err());
        assert!(outcome.is_none());
    }

    #[test]
    fn clean_file_cache_does_not_create_committed_pressure() {
        let reclaimer = Arc::new(FakeReclaimer::new(false));
        let guard = guard([Ok(snapshot(980, 880, 100))], reclaimer.clone());
        guard.admit_new_query().unwrap();
        assert_eq!(reclaimer.calls.load(Ordering::SeqCst), 0);
        let outcome = guard.last_admission_outcome().unwrap();
        assert_eq!(outcome.decision, MemoryAdmissionDecision::Admitted);
        assert_eq!(outcome.initial.unwrap().current_bytes, 980);
        assert_eq!(outcome.initial.unwrap().reclaimable_clean_file_bytes, 880);
    }

    #[test]
    fn committed_charge_equal_to_threshold_is_pressure() {
        let reclaimer = Arc::new(FakeReclaimer::new(false));
        let guard = guard([Ok(snapshot(900, 0, 900))], reclaimer.clone());
        assert!(guard.check().is_err());
        assert_eq!(reclaimer.calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn initial_probe_failure_is_precise_and_does_not_reclaim() {
        let reclaimer = Arc::new(FakeReclaimer::new(false));
        let guard = guard(
            [Err(io::Error::new(io::ErrorKind::InvalidData, "bad limit"))],
            reclaimer.clone(),
        );
        let error = guard.admit_new_query().unwrap_err().to_string();
        assert!(error.contains(PROCESS_MEMORY_PROBE_UNAVAILABLE_MESSAGE));
        assert_eq!(reclaimer.calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            guard.last_admission_outcome().unwrap().decision,
            MemoryAdmissionDecision::ProbeUnavailable
        );
    }

    #[test]
    fn true_pressure_reclaims_and_rejects_from_fresh_snapshot() {
        let reclaimer = Arc::new(FakeReclaimer::new(false));
        let pressure = snapshot(950, 10, 940);
        let guard = guard(
            [Ok(pressure), Ok(pressure), Ok(pressure)],
            reclaimer.clone(),
        );
        let error = guard.admit_new_query().unwrap_err().to_string();
        assert!(error.contains(PROCESS_MEMORY_PRESSURE_MESSAGE));
        assert!(!error.contains("Query too large"));
        assert_eq!(reclaimer.calls.load(Ordering::SeqCst), 1);
        let outcome = guard.last_admission_outcome().unwrap();
        assert_eq!(
            outcome.decision,
            MemoryAdmissionDecision::RejectedAfterReclaim
        );
        assert_eq!(outcome.reclaim_report.unwrap().released_bytes, 4096);
    }

    #[test]
    fn successful_reclaim_admits_only_after_reprobe() {
        let reclaimer = Arc::new(FakeReclaimer::new(false));
        let guard = guard(
            [
                Ok(snapshot(950, 0, 950)),
                Ok(snapshot(940, 0, 940)),
                Ok(snapshot(700, 0, 700)),
            ],
            reclaimer,
        );
        guard.admit_new_query().unwrap();
        let outcome = guard.last_admission_outcome().unwrap();
        assert_eq!(
            outcome.decision,
            MemoryAdmissionDecision::AdmittedAfterReclaim
        );
        assert_eq!(outcome.post_reclaim.unwrap().committed_bytes, 700);
    }

    #[test]
    fn failed_reclaimer_still_reprobes_and_can_admit() {
        let reclaimer = Arc::new(FakeReclaimer::new(true));
        let guard = guard(
            [
                Ok(snapshot(950, 0, 950)),
                Ok(snapshot(950, 0, 950)),
                Ok(snapshot(700, 0, 700)),
            ],
            reclaimer,
        );
        guard.admit_new_query().unwrap();
        let outcome = guard.last_admission_outcome().unwrap();
        assert!(outcome
            .reclaim_error
            .unwrap()
            .contains("allocator purge failed"));
        assert!(outcome.post_reclaim.is_some());
    }

    #[test]
    fn failed_reclaimer_and_persistent_pressure_reject() {
        let reclaimer = Arc::new(FakeReclaimer::new(true));
        let pressure = snapshot(950, 0, 950);
        let guard = guard([Ok(pressure), Ok(pressure), Ok(pressure)], reclaimer);
        assert!(guard
            .admit_new_query()
            .unwrap_err()
            .to_string()
            .contains(PROCESS_MEMORY_PRESSURE_MESSAGE));
        assert!(guard
            .last_admission_outcome()
            .unwrap()
            .post_reclaim
            .is_some());
    }

    #[test]
    fn rechecks_before_reclaim_and_observes_concurrent_relief() {
        let reclaimer = Arc::new(FakeReclaimer::new(false));
        let guard = guard(
            [Ok(snapshot(950, 0, 950)), Ok(snapshot(700, 0, 700))],
            reclaimer.clone(),
        );
        guard.admit_new_query().unwrap();
        assert_eq!(reclaimer.calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            guard.last_admission_outcome().unwrap().decision,
            MemoryAdmissionDecision::AdmittedAfterConcurrentReclaim
        );
    }

    #[test]
    fn reclaim_is_rate_limited_process_wide() {
        let reclaimer = Arc::new(FakeReclaimer::new(false));
        let pressure = snapshot(950, 0, 950);
        let guard = guard(
            [
                Ok(pressure),
                Ok(pressure),
                Ok(pressure),
                Ok(pressure),
                Ok(pressure),
            ],
            reclaimer.clone(),
        );
        assert!(guard.admit_new_query().is_err());
        let error = guard.admit_new_query().unwrap_err().to_string();
        assert!(error.contains("rate-limited"));
        assert_eq!(reclaimer.calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            guard.last_admission_outcome().unwrap().decision,
            MemoryAdmissionDecision::RejectedRateLimited
        );
    }

    #[test]
    fn post_reclaim_probe_failure_fails_closed() {
        let reclaimer = Arc::new(FakeReclaimer::new(false));
        let guard = guard(
            [
                Ok(snapshot(950, 0, 950)),
                Ok(snapshot(950, 0, 950)),
                Err(io::Error::new(io::ErrorKind::InvalidData, "bad current")),
            ],
            reclaimer,
        );
        let error = guard.admit_new_query().unwrap_err().to_string();
        assert!(error.contains(PROCESS_MEMORY_PROBE_UNAVAILABLE_MESSAGE));
        assert_eq!(
            guard.last_admission_outcome().unwrap().decision,
            MemoryAdmissionDecision::ProbeUnavailable
        );
    }

    #[test]
    fn runtime_check_never_reclaims() {
        let reclaimer = Arc::new(FakeReclaimer::new(false));
        let guard = guard([Ok(snapshot(950, 0, 950))], reclaimer.clone());
        let error = guard.check().unwrap_err().to_string();
        assert!(error.contains(PROCESS_MEMORY_RUNTIME_PRESSURE_MESSAGE));
        assert!(!error.contains("after reclamation"));
        assert_eq!(reclaimer.calls.load(Ordering::SeqCst), 0);
    }

    struct ConcurrentProbe {
        relieved: Arc<AtomicBool>,
    }

    impl MemoryProbe for ConcurrentProbe {
        fn snapshot(&self) -> io::Result<CgroupMemorySnapshot> {
            Ok(if self.relieved.load(Ordering::SeqCst) {
                snapshot(700, 0, 700)
            } else {
                snapshot(950, 0, 950)
            })
        }
    }

    struct RelievingReclaimer {
        calls: AtomicUsize,
        relieved: Arc<AtomicBool>,
    }

    impl MemoryReclaimer for RelievingReclaimer {
        fn reclaim(&self) -> Result<MemoryReclaimReport> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.relieved.store(true, Ordering::SeqCst);
            Ok(MemoryReclaimReport::default())
        }
    }

    #[test]
    fn concurrent_admissions_serialize_to_one_reclaim() {
        let relieved = Arc::new(AtomicBool::new(false));
        let reclaimer = Arc::new(RelievingReclaimer {
            calls: AtomicUsize::new(0),
            relieved: relieved.clone(),
        });
        let guard = Arc::new(CgroupMemoryGuard::from_probe(
            Arc::new(ConcurrentProbe { relieved }),
            100,
            reclaimer.clone(),
            RECLAIM_INTERVAL,
        ));
        let start = Arc::new(std::sync::Barrier::new(9));
        let mut threads = Vec::new();
        for _ in 0..8 {
            let guard = guard.clone();
            let start = start.clone();
            threads.push(thread::spawn(move || {
                start.wait();
                guard.admit_new_query_with_outcome()
            }));
        }
        start.wait();
        let mut decisions = Vec::new();
        for handle in threads {
            let (result, outcome) = handle.join().unwrap();
            result.unwrap();
            decisions.push(outcome.unwrap().decision);
        }
        assert_eq!(reclaimer.calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            decisions
                .iter()
                .filter(|&&decision| decision == MemoryAdmissionDecision::AdmittedAfterReclaim)
                .count(),
            1,
            "the reclaiming call must receive its own outcome"
        );
        assert!(decisions.iter().all(|decision| matches!(
            decision,
            MemoryAdmissionDecision::Admitted
                | MemoryAdmissionDecision::AdmittedAfterConcurrentReclaim
                | MemoryAdmissionDecision::AdmittedAfterReclaim
        )));
    }
}
