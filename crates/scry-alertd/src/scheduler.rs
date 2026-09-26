//! Due-slot dispatch.
//!
//! The tick loop never awaits an evaluation: it scans the local projection's
//! schedule columns, and spawns each due monitor's newest due slot as a task
//! holding one of [`MAX_CONCURRENT_EVALUATIONS`] permits. A per-monitor
//! in-flight set prevents a second dispatch of a monitor whose previous
//! evaluation is still running, however slow. When permits run out the scan
//! stops and the next tick resumes after the last dispatched monitor, so a
//! large fleet is served round-robin rather than always from the lowest ID.

use std::{
    collections::HashSet,
    sync::{Arc, Mutex as StdMutex},
    time::Duration,
};

use scry_alert::{latest_due_slot, AlertStatus, MonitorId, ScheduleEntry};
use tokio::{
    sync::{watch, OwnedSemaphorePermit, Semaphore},
    task::JoinSet,
    time::Instant,
};

use crate::{
    evaluator::{evaluate_monitor_slot, EvaluationContext, EvaluationError, SlotOutcome},
    leases::Leases,
};

/// Global bound on concurrently running evaluations (and so on concurrent
/// alert queries against queryd).
pub const MAX_CONCURRENT_EVALUATIONS: usize = 8;
const TICK: Duration = Duration::from_secs(1);
const SCHEDULE_PAGE: usize = 512;
/// Pause after the lease backend is unreachable, instead of retrying it for
/// every due monitor on every tick.
const LEASE_BACKOFF: Duration = Duration::from_secs(5);
/// Default time shutdown waits for running evaluations before aborting them.
/// An aborted evaluation commits nothing; its lease expires via TTL.
pub const SHUTDOWN_GRACE: Duration = Duration::from_secs(10);

#[derive(Clone, Copy, Debug)]
pub struct SchedulerConfig {
    pub evaluation_delay: Duration,
    pub max_concurrent: usize,
    pub shutdown_grace: Duration,
}

/// A due slot for one monitor revision.
#[derive(Clone, Copy, Debug)]
struct DueSlot {
    monitor_id: MonitorId,
    revision: u64,
    slot_id: u64,
    slot_end_unix_nano: u64,
}

struct JobReport {
    due: DueSlot,
    result: Result<SlotOutcome, EvaluationError>,
}

/// Removes a monitor from the in-flight set when its job ends, including on
/// panic or abort.
struct InFlight {
    set: Arc<StdMutex<HashSet<MonitorId>>>,
    id: MonitorId,
}

impl Drop for InFlight {
    fn drop(&mut self) {
        self.set
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&self.id);
    }
}

pub struct Scheduler {
    context: Arc<EvaluationContext>,
    leases: Leases,
    config: SchedulerConfig,
    permits: Arc<Semaphore>,
    in_flight: Arc<StdMutex<HashSet<MonitorId>>>,
    jobs: JoinSet<JobReport>,
    /// Round-robin resume point: the next scan starts after this monitor.
    resume_after: Option<MonitorId>,
    backoff_until: Option<Instant>,
}

impl Scheduler {
    pub fn new(context: Arc<EvaluationContext>, leases: Leases, config: SchedulerConfig) -> Self {
        let max = config.max_concurrent.max(1);
        Self {
            context,
            leases,
            config,
            permits: Arc::new(Semaphore::new(max)),
            in_flight: Arc::default(),
            jobs: JoinSet::new(),
            resume_after: None,
            backoff_until: None,
        }
    }

    /// Run until `shutdown` becomes true, then drain running evaluations for
    /// at most [`SchedulerConfig::shutdown_grace`] and abort the rest.
    pub async fn run(mut self, mut shutdown: watch::Receiver<bool>) {
        let mut interval = tokio::time::interval(TICK);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            if *shutdown.borrow() {
                break;
            }
            tokio::select! {
                _ = interval.tick() => {}
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        break;
                    }
                    continue;
                }
            }
            self.reap();
            if let Err(error) = self.tick(unix_nanos_now()).await {
                tracing::warn!(error = %error, "alert scheduler pass failed");
            }
        }
        self.drain().await;
    }

    /// One scheduling pass at `now`: dispatch every due monitor until permits
    /// run out. Returns the number of evaluations spawned.
    pub(crate) async fn tick(&mut self, now: u64) -> anyhow::Result<usize> {
        if self
            .backoff_until
            .is_some_and(|until| Instant::now() < until)
        {
            return Ok(0);
        }
        self.backoff_until = None;
        let delay = u64::try_from(self.config.evaluation_delay.as_nanos()).unwrap_or(u64::MAX);
        let wrap_at = self.resume_after;
        let mut after = wrap_at;
        let mut wrapped = false;
        let mut spawned = 0;
        let mut last_seen = None;
        loop {
            let page = self
                .context
                .db
                .lock()
                .await
                .list_schedule(after, SCHEDULE_PAGE)?;
            let page_len = page.len();
            for entry in page {
                if wrapped && wrap_at.is_some_and(|stop| entry.monitor_id.0 > stop.0) {
                    // Completed a full circle.
                    self.resume_after = None;
                    return Ok(spawned);
                }
                after = Some(entry.monitor_id);
                let Some(due) = due_slot(&entry, now, delay) else {
                    last_seen = Some(entry.monitor_id);
                    continue;
                };
                if self.in_flight_contains(entry.monitor_id) {
                    last_seen = Some(entry.monitor_id);
                    continue;
                }
                let Ok(permit) = self.permits.clone().try_acquire_owned() else {
                    // Out of capacity: resume with this monitor next tick
                    // (keeping the old resume point if it was the first one
                    // examined).
                    self.resume_after = last_seen.or(wrap_at);
                    return Ok(spawned);
                };
                self.spawn(due, permit);
                spawned += 1;
                last_seen = Some(entry.monitor_id);
            }
            if page_len < SCHEDULE_PAGE {
                if wrapped || wrap_at.is_none() {
                    self.resume_after = None;
                    return Ok(spawned);
                }
                wrapped = true;
                after = None;
            }
        }
    }

    fn in_flight_contains(&self, id: MonitorId) -> bool {
        self.in_flight
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .contains(&id)
    }

    fn spawn(&mut self, due: DueSlot, permit: OwnedSemaphorePermit) {
        self.in_flight
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(due.monitor_id);
        let in_flight = InFlight {
            set: self.in_flight.clone(),
            id: due.monitor_id,
        };
        let context = self.context.clone();
        let leases = self.leases.clone();
        self.jobs.spawn(async move {
            let _permit = permit;
            let _in_flight = in_flight;
            let result = run_job(&leases, &context, due).await;
            JobReport { due, result }
        });
    }

    fn reap(&mut self) {
        while let Some(joined) = self.jobs.try_join_next() {
            self.record(joined);
        }
    }

    fn record(&mut self, joined: Result<JobReport, tokio::task::JoinError>) {
        match joined {
            Ok(JobReport { due, result }) => match result {
                Ok(SlotOutcome::Committed(state)) => tracing::debug!(
                    monitor_id = %due.monitor_id,
                    slot = due.slot_id,
                    status = state.status.as_str(),
                    "alert slot committed"
                ),
                Ok(outcome) => tracing::debug!(
                    monitor_id = %due.monitor_id,
                    slot = due.slot_id,
                    ?outcome,
                    "alert slot not committed"
                ),
                Err(EvaluationError::LeaseUnavailable(error)) => {
                    if self.backoff_until.is_none() {
                        tracing::warn!(error = %error, "alert lease backend unavailable; pausing evaluation");
                    }
                    self.backoff_until = Some(Instant::now() + LEASE_BACKOFF);
                }
                Err(EvaluationError::Failed(error)) => tracing::warn!(
                    monitor_id = %due.monitor_id,
                    slot = due.slot_id,
                    error = %error,
                    "alert evaluation failed"
                ),
            },
            Err(error) if error.is_cancelled() => {}
            Err(error) => tracing::error!(error = %error, "alert evaluation task panicked"),
        }
    }

    async fn drain(mut self) {
        let deadline = Instant::now() + self.config.shutdown_grace;
        loop {
            match tokio::time::timeout_at(deadline, self.jobs.join_next()).await {
                Ok(Some(joined)) => self.record(joined),
                Ok(None) => return,
                Err(_) => {
                    tracing::warn!(
                        running = self.jobs.len(),
                        "aborting alert evaluations still running at shutdown"
                    );
                    self.jobs.abort_all();
                    while self.jobs.join_next().await.is_some() {}
                    return;
                }
            }
        }
    }

    #[cfg(test)]
    pub(crate) async fn wait_idle(&mut self) {
        while let Some(joined) = self.jobs.join_next().await {
            self.record(joined);
        }
    }
}

/// The newest due slot of `entry` at `now`, unless durable state already
/// covers it or a disabled monitor is already recorded as Disabled for its
/// current revision (re-committing that every interval would only cost writes).
fn due_slot(entry: &ScheduleEntry, now: u64, delay_nanos: u64) -> Option<DueSlot> {
    let slot = latest_due_slot(
        entry.monitor_id,
        now,
        entry.every_seconds,
        entry.jitter_seconds,
        delay_nanos,
    )?;
    if let Some(state) = entry.state {
        if state.covers(entry.revision, slot.id) {
            return None;
        }
        if !entry.enabled
            && state.monitor_revision == entry.revision
            && state.status == AlertStatus::Disabled
        {
            return None;
        }
    }
    Some(DueSlot {
        monitor_id: entry.monitor_id,
        revision: entry.revision,
        slot_id: slot.id,
        slot_end_unix_nano: slot.end_unix_nano,
    })
}

async fn run_job(
    leases: &Leases,
    context: &EvaluationContext,
    due: DueSlot,
) -> Result<SlotOutcome, EvaluationError> {
    let summary = context.db.lock().await.get_monitor(due.monitor_id)?;
    let Some(summary) = summary else {
        return Ok(SlotOutcome::Superseded);
    };
    if summary.monitor.revision != due.revision {
        return Ok(SlotOutcome::Superseded);
    }
    evaluate_monitor_slot(
        leases,
        context,
        &summary.monitor,
        due.slot_id,
        due.slot_end_unix_nano,
    )
    .await
}

pub(crate) fn unix_nanos_now() -> u64 {
    chrono::Utc::now()
        .timestamp_nanos_opt()
        .unwrap_or_default()
        .max(0) as u64
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use object_store::{memory::InMemory, ObjectStore};
    use scry_alert::{
        AlertStore, AlertsDb, Comparator, ExecutionErrorPolicy, Monitor, NoDataPolicy, Observation,
        ScalarCondition, ScalarQuery, Signal, MONITOR_SCHEMA_VERSION,
    };
    use tokio::sync::{Mutex, Notify};
    use uuid::Uuid;

    use super::*;
    use crate::{evaluator::ScalarExecutor, QueryTarget};

    const S: u64 = 1_000_000_000;

    /// Blocks every query until released, counting calls.
    struct GatedExecutor {
        calls: AtomicUsize,
        gate: Notify,
        open: std::sync::atomic::AtomicBool,
    }

    #[async_trait]
    impl ScalarExecutor for GatedExecutor {
        async fn execute(&self, _: &QueryTarget, _: &Monitor, _: u64, _: u64) -> Observation {
            self.calls.fetch_add(1, Ordering::SeqCst);
            while !self.open.load(Ordering::SeqCst) {
                self.gate.notified().await;
            }
            Observation::Value(1.0)
        }
    }

    impl GatedExecutor {
        fn new(open: bool) -> Arc<Self> {
            Arc::new(Self {
                calls: AtomicUsize::new(0),
                gate: Notify::new(),
                open: std::sync::atomic::AtomicBool::new(open),
            })
        }
        fn release(&self) {
            self.open.store(true, Ordering::SeqCst);
            self.gate.notify_waiters();
        }
    }

    fn monitor(every_seconds: u64) -> Monitor {
        Monitor {
            schema_version: MONITOR_SCHEMA_VERSION,
            id: scry_alert::MonitorId::new(),
            revision: 1,
            name: "m".into(),
            enabled: true,
            query: ScalarQuery {
                target_id: "local".into(),
                signal: Signal::Metrics,
                matchers: vec![],
                lookback_seconds: 60,
                sql: "SELECT count(*) AS value FROM metrics".into(),
            },
            condition: ScalarCondition {
                comparator: Comparator::Gt,
                threshold: 10.0,
            },
            every_seconds,
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

    async fn scheduler(
        monitors: &[Monitor],
        executor: Arc<GatedExecutor>,
        max_concurrent: usize,
    ) -> Scheduler {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let deployment = Uuid::new_v4();
        let mut db = AlertsDb::open_in_memory(deployment).unwrap();
        for monitor in monitors {
            AlertStore::new(store.as_ref())
                .create_rule_revision(monitor, &Uuid::new_v4().to_string())
                .await
                .unwrap();
            db.fold_rule(monitor).unwrap();
        }
        let context = Arc::new(EvaluationContext {
            store,
            db: Arc::new(Mutex::new(db)),
            deployment_id: deployment.to_string(),
            query_targets: Arc::new(vec![QueryTarget {
                id: "local".into(),
                address: "127.0.0.1:9".into(),
            }]),
            executor,
            lease_ttl: Duration::from_secs(5),
        });
        Scheduler::new(
            context,
            Leases::local(),
            SchedulerConfig {
                evaluation_delay: Duration::from_secs(90),
                max_concurrent,
                shutdown_grace: Duration::from_millis(100),
            },
        )
    }

    #[tokio::test]
    async fn slow_evaluations_never_block_the_tick_or_double_dispatch() {
        let rule = monitor(60);
        let executor = GatedExecutor::new(false);
        let mut scheduler = scheduler(std::slice::from_ref(&rule), executor.clone(), 4).await;
        let now = 10_000 * S;
        // The tick returns while the query is still blocked.
        let spawned = tokio::time::timeout(Duration::from_secs(1), scheduler.tick(now))
            .await
            .expect("tick must not wait for evaluations")
            .unwrap();
        assert_eq!(spawned, 1);
        tokio::task::yield_now().await;
        // Later ticks (even at a later slot) do not dispatch the same monitor
        // again while its evaluation is in flight.
        for later in [now + S, now + 61 * S, now + 200 * S] {
            assert_eq!(scheduler.tick(later).await.unwrap(), 0);
        }
        executor.release();
        scheduler.wait_idle().await;
        assert_eq!(executor.calls.load(Ordering::SeqCst), 1);
        // Committed: the same slot is covered and not dispatched again.
        assert_eq!(scheduler.tick(now).await.unwrap(), 0);
        // The next slot is.
        assert_eq!(scheduler.tick(now + 60 * S).await.unwrap(), 1);
        scheduler.wait_idle().await;
        assert_eq!(executor.calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn concurrency_is_bounded_and_dispatch_is_round_robin() {
        let rules: Vec<_> = (0..5).map(|_| monitor(60)).collect();
        let executor = GatedExecutor::new(false);
        let mut scheduler = scheduler(&rules, executor.clone(), 2).await;
        let now = 10_000 * S;
        assert_eq!(scheduler.tick(now).await.unwrap(), 2);
        assert_eq!(scheduler.tick(now).await.unwrap(), 0, "no free permits");
        executor.release();
        scheduler.wait_idle().await;
        // Resumes after the monitors already served instead of restarting.
        assert_eq!(scheduler.tick(now).await.unwrap(), 2);
        scheduler.wait_idle().await;
        assert_eq!(scheduler.tick(now).await.unwrap(), 1);
        scheduler.wait_idle().await;
        assert_eq!(scheduler.tick(now).await.unwrap(), 0, "all covered");
        assert_eq!(executor.calls.load(Ordering::SeqCst), 5);
    }

    #[tokio::test]
    async fn evaluation_waits_for_the_lateness_allowance() {
        let rule = monitor(60);
        let executor = GatedExecutor::new(true);
        let mut scheduler = scheduler(std::slice::from_ref(&rule), executor.clone(), 4).await;
        // Slot 99 ends at 6000s and is due at 6090s; before that slot 98 is.
        assert_eq!(scheduler.tick(6_089 * S).await.unwrap(), 1);
        scheduler.wait_idle().await;
        let state = scheduler
            .context
            .db
            .lock()
            .await
            .get_monitor(rule.id)
            .unwrap()
            .unwrap()
            .state
            .unwrap();
        assert_eq!(state.last_slot_id, 98);
        assert_eq!(state.last_evaluated_at_unix_nano, 5_940 * S);
        assert_eq!(scheduler.tick(6_089 * S).await.unwrap(), 0);
        assert_eq!(scheduler.tick(6_090 * S).await.unwrap(), 1);
        scheduler.wait_idle().await;
    }

    #[tokio::test]
    async fn shutdown_aborts_stuck_evaluations_without_committing() {
        let rule = monitor(60);
        let executor = GatedExecutor::new(false);
        let mut scheduler = scheduler(std::slice::from_ref(&rule), executor.clone(), 4).await;
        assert_eq!(scheduler.tick(10_000 * S).await.unwrap(), 1);
        let context = scheduler.context.clone();
        let (tx, rx) = watch::channel(false);
        let run = tokio::spawn(scheduler.run(rx));
        tx.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(5), run)
            .await
            .expect("shutdown must not wait for a stuck evaluation")
            .unwrap();
        assert_eq!(executor.calls.load(Ordering::SeqCst), 1);
        assert!(context
            .db
            .lock()
            .await
            .get_monitor(rule.id)
            .unwrap()
            .unwrap()
            .state
            .is_none());
    }

    /// Budget: a tick over the projection's monitor bound, with nothing due,
    /// must stay well under the one-second tick period. Run with
    /// `cargo test --release -p scry-alertd -- --ignored --nocapture tick_scales`.
    #[tokio::test]
    #[ignore = "scale check; run in release"]
    async fn tick_scales_to_the_projection_bound() {
        const MONITORS: usize = crate::projection::MAX_MONITORS;
        const BUDGET: Duration = Duration::from_millis(250);
        let executor = GatedExecutor::new(true);
        let mut scheduler = scheduler(&[], executor.clone(), MAX_CONCURRENT_EVALUATIONS).await;
        let now = 10_000 * S;
        {
            let mut db = scheduler.context.db.lock().await;
            for _ in 0..MONITORS {
                let rule = monitor(60);
                db.fold_rule(&rule).unwrap();
                // Already covers every slot due at `now`.
                db.fold_state(
                    rule.id,
                    &scry_alert::AlertState {
                        monitor_revision: 1,
                        status: AlertStatus::Inactive,
                        since_unix_nano: 1,
                        last_evaluated_at_unix_nano: now,
                        last_slot_id: u64::MAX / 2,
                        last_value: None,
                        last_error_class: None,
                        transition_sequence: 1,
                        stale: false,
                        interrupted: None,
                    },
                )
                .unwrap();
            }
        }
        let mut worst = Duration::ZERO;
        for _ in 0..5 {
            let started = std::time::Instant::now();
            assert_eq!(scheduler.tick(now).await.unwrap(), 0);
            worst = worst.max(started.elapsed());
        }
        eprintln!("tick over {MONITORS} monitors: worst {worst:?}");
        assert!(worst < BUDGET, "tick took {worst:?}, budget {BUDGET:?}");
        assert_eq!(executor.calls.load(Ordering::SeqCst), 0);
    }
}
