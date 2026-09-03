//! Process-wide resource envelope for compaction work.
//!
//! A [`CompactResources`] is intentionally shared by every merge in a process:
//! DataFusion reservations then compete in one [`FairSpillPool`], spill files
//! share one bounded [`DiskManager`], and Arrow/parquet/sidecar memory that
//! DataFusion cannot account for is admitted by a weighted semaphore.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use datafusion::execution::disk_manager::{DiskManager, DiskManagerMode};
use datafusion::execution::memory_pool::{
    FairSpillPool, MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation,
};
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

const MIB: u64 = 1024 * 1024;

/// Resolved limits used to construct a process-wide compaction envelope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceConfig {
    /// Total operator-facing compaction memory envelope. The runtime splits it
    /// across DataFusion, sidecar/output admission, and uncommitted headroom.
    pub envelope_bytes: u64,
    /// DataFusion's shared spill-aware memory pool.
    pub datafusion_memory_bytes: u64,
    /// Weighted admission budget for memory DataFusion does not track.
    pub non_datafusion_memory_bytes: u64,
    /// Maximum aggregate bytes in DataFusion spill files.
    pub spill_bytes: u64,
    /// Memory-envelope headroom reserved for cgroup-charged dirty spill pages
    /// and writeback. This is intentionally not the full historical spill cap.
    pub spill_page_cache_headroom_bytes: u64,
    /// Spill directory. `None` asks the OS for a private temporary directory.
    pub spill_dir: Option<PathBuf>,
    /// Allow spill on tmpfs/ramfs. This is unsafe under a memory cgroup because
    /// spill then competes directly with the DataFusion memory pool.
    pub allow_memory_backed_spill: bool,
    /// Bounded multipart chunk/buffer used while streaming each output object.
    pub output_buffer_bytes: usize,
    /// Flush an in-progress parquet row group after its estimated memory reaches
    /// this threshold. This bounds ArrowWriter state outside DataFusion's pool.
    pub parquet_writer_memory_bytes: usize,
    /// Maximum number of merges queued for weighted admission.
    pub max_waiters: usize,
    /// Maximum admission wait before an ordinary resource failure is returned.
    pub admission_timeout: Duration,
}

impl Default for ResourceConfig {
    fn default() -> Self {
        Self::detect()
    }
}

impl ResourceConfig {
    /// Resolve the same mount-aware cgroup budget policy used by compactd.
    /// Invalid finite limits remain authoritative and therefore produce a
    /// sub-minimum config that validation refuses rather than an unsafe fallback.
    pub fn detect() -> Self {
        let detected = scry_resources::detect_cgroup_memory_limit();
        let envelope = scry_resources::resolve_memory_budget(None, detected)
            .map(|budget| budget.bytes)
            .unwrap_or(0);
        Self::from_envelope(envelope)
    }

    pub fn from_envelope(envelope: u64) -> Self {
        // DF gets half, non-DF work a quarter, and the remaining quarter stays
        // uncommitted allocator/runtime headroom.
        Self {
            envelope_bytes: envelope,
            datafusion_memory_bytes: (envelope / 2).max(64 * MIB),
            non_datafusion_memory_bytes: (envelope / 4).max(32 * MIB),
            spill_bytes: envelope.saturating_mul(4).max(1024 * MIB),
            spill_page_cache_headroom_bytes: envelope / 8,
            spill_dir: None,
            allow_memory_backed_spill: false,
            output_buffer_bytes: 8 * MIB as usize,
            parquet_writer_memory_bytes: 16 * MIB as usize,
            max_waiters: 32,
            admission_timeout: Duration::from_secs(30),
        }
    }

    pub fn validate(&self) -> Result<(), ResourceError> {
        if self.envelope_bytes < 128 * MIB {
            return Err(ResourceError::InvalidConfig(
                "compaction memory envelope must be at least 128 MiB",
            ));
        }
        if self.datafusion_memory_bytes == 0 || self.datafusion_memory_bytes > usize::MAX as u64 {
            return Err(ResourceError::InvalidConfig(
                "datafusion memory must fit usize",
            ));
        }
        if self.non_datafusion_memory_bytes < MIB {
            return Err(ResourceError::InvalidConfig(
                "non-DataFusion memory must be at least 1 MiB",
            ));
        }
        if bytes_to_capacity_units(self.non_datafusion_memory_bytes) > u32::MAX as u64 {
            return Err(ResourceError::InvalidConfig(
                "non-DataFusion memory exceeds weighted admission capacity",
            ));
        }
        if self
            .datafusion_memory_bytes
            .checked_add(self.non_datafusion_memory_bytes)
            .and_then(|committed| committed.checked_add(self.spill_page_cache_headroom_bytes))
            .is_none_or(|committed| committed > self.envelope_bytes)
        {
            return Err(ResourceError::InvalidConfig(
                "DataFusion, non-DataFusion, and spill page-cache budgets exceed the compaction envelope",
            ));
        }
        if self.spill_bytes == 0 {
            return Err(ResourceError::InvalidConfig("spill limit must be non-zero"));
        }
        if self.output_buffer_bytes < 5 * MIB as usize {
            return Err(ResourceError::InvalidConfig(
                "output buffer must be at least 5 MiB for S3 multipart uploads",
            ));
        }
        if self.parquet_writer_memory_bytes == 0 {
            return Err(ResourceError::InvalidConfig(
                "parquet writer memory must be non-zero",
            ));
        }
        let writer_buffers = u64::try_from(self.output_buffer_bytes)
            .ok()
            .and_then(|output| {
                u64::try_from(self.parquet_writer_memory_bytes)
                    .ok()
                    .and_then(|parquet| output.checked_add(parquet))
            });
        if writer_buffers.is_none_or(|bytes| bytes > self.non_datafusion_memory_bytes) {
            return Err(ResourceError::InvalidConfig(
                "output and parquet writer buffers exceed the non-DataFusion budget",
            ));
        }
        if self.max_waiters == 0 {
            return Err(ResourceError::InvalidConfig("max_waiters must be non-zero"));
        }
        Ok(())
    }
}

/// Resource admission failures. Inputs remain live on every variant.
///
/// [`ResourceError::RequestTooLarge`] is permanent for the current envelope;
/// queue saturation and timeouts are transient and may be retried.
#[derive(Debug, thiserror::Error)]
pub enum ResourceError {
    #[error("invalid compaction resource config: {0}")]
    InvalidConfig(&'static str),
    #[error("compaction spill directory {path:?} is memory-backed; choose persistent storage or explicitly allow unsafe memory-backed spill")]
    MemoryBackedSpill { path: PathBuf },
    #[error("constructing compaction runtime: {0}")]
    Runtime(#[source] datafusion::error::DataFusionError),
    #[error("compaction exhausted its DataFusion memory or spill budget: {0}")]
    DataFusionExhausted(#[source] datafusion::error::DataFusionError),
    #[error(
        "compaction request needs {requested_bytes} non-DataFusion bytes, budget is {budget_bytes}"
    )]
    RequestTooLarge {
        requested_bytes: u64,
        budget_bytes: u64,
    },
    #[error("compaction sidecar {component} exceeded its {budget_bytes}-byte memory budget")]
    SidecarLimit {
        component: &'static str,
        budget_bytes: u64,
    },
    #[error("compaction resource queue is full ({max_waiters} waiters)")]
    QueueFull { max_waiters: usize },
    #[error("timed out after {waited:?} waiting for compaction resources")]
    AdmissionTimeout { waited: Duration },
    #[error("compaction resource admission closed")]
    Closed,
}

/// Cheap point-in-time resource telemetry (relaxed atomics plus DF counters).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ResourceTelemetry {
    pub datafusion_reserved_bytes: usize,
    /// Exact process-lifetime peak, recorded synchronously on every successful
    /// DataFusion reservation increase.
    pub datafusion_peak_bytes: usize,
    pub spill_used_bytes: u64,
    /// Process-lifetime peak of the spill usage observed by calls to
    /// [`CompactResources::telemetry`]. DiskManager does not expose spill
    /// lifecycle events, so unlike the memory peaks this remains sampled.
    pub sampled_spill_peak_bytes: u64,
    pub spill_active_files: usize,
    pub weighted_running_bytes: u64,
    /// Exact process-lifetime peak, recorded at each successful admission.
    pub weighted_peak_bytes: u64,
    pub weighted_waiters: usize,
    pub admissions: u64,
    pub rejected: u64,
    pub cumulative_wait_micros: u64,
}

/// A forwarding spill pool that observes every successful reservation increase.
///
/// The gate keeps an increase and its peak update indivisible with respect to a
/// concurrent shrink; sampling `reserved()` after the fact would otherwise miss
/// short-lived peaks.
#[derive(Debug)]
struct PeakTrackingMemoryPool {
    inner: FairSpillPool,
    event_gate: Mutex<()>,
    peak_bytes: AtomicUsize,
}

impl PeakTrackingMemoryPool {
    fn new(limit: usize) -> Self {
        Self {
            inner: FairSpillPool::new(limit),
            event_gate: Mutex::new(()),
            peak_bytes: AtomicUsize::new(0),
        }
    }

    fn record_peak(&self) {
        self.peak_bytes
            .fetch_max(self.inner.reserved(), Ordering::Relaxed);
    }

    fn peak(&self) -> usize {
        self.peak_bytes.load(Ordering::Relaxed)
    }
}

impl MemoryPool for PeakTrackingMemoryPool {
    fn register(&self, consumer: &MemoryConsumer) {
        self.inner.register(consumer);
    }

    fn unregister(&self, consumer: &MemoryConsumer) {
        self.inner.unregister(consumer);
    }

    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        let _event = self.event_gate.lock().expect("memory pool gate poisoned");
        self.inner.grow(reservation, additional);
        self.record_peak();
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        let _event = self.event_gate.lock().expect("memory pool gate poisoned");
        self.inner.shrink(reservation, shrink);
    }

    fn try_grow(
        &self,
        reservation: &MemoryReservation,
        additional: usize,
    ) -> datafusion::error::Result<()> {
        let _event = self.event_gate.lock().expect("memory pool gate poisoned");
        self.inner.try_grow(reservation, additional)?;
        self.record_peak();
        Ok(())
    }

    fn reserved(&self) -> usize {
        self.inner.reserved()
    }

    fn memory_limit(&self) -> MemoryLimit {
        self.inner.memory_limit()
    }
}

#[derive(Debug)]
pub struct CompactResources {
    runtime: Arc<RuntimeEnv>,
    pool: Arc<PeakTrackingMemoryPool>,
    disk: Arc<DiskManager>,
    admission: Arc<Semaphore>,
    config: ResourceConfig,
    waiters: AtomicUsize,
    running_units: Arc<AtomicU64>,
    peak_running_units: AtomicU64,
    sampled_spill_peak_bytes: AtomicU64,
    admissions: AtomicU64,
    rejected: AtomicU64,
    wait_micros: AtomicU64,
}

impl CompactResources {
    pub fn new(config: ResourceConfig) -> Result<Arc<Self>, ResourceError> {
        config.validate()?;
        let configured_spill_base = config.spill_dir.clone().unwrap_or_else(std::env::temp_dir);
        let spill_base = canonicalize_for_classification(&configured_spill_base).map_err(|error| {
            tracing::warn!(path = %configured_spill_base.display(), %error, "could not resolve compaction spill path");
            ResourceError::InvalidConfig("could not resolve compaction spill path")
        })?;
        let spill_filesystem = scry_resources::classify_filesystem(&spill_base).map_err(|error| {
            tracing::warn!(path = %spill_base.display(), %error, "could not classify compaction spill filesystem");
            ResourceError::InvalidConfig("could not classify compaction spill filesystem")
        })?;
        if !config.allow_memory_backed_spill
            && matches!(spill_filesystem, scry_resources::FilesystemClass::Memory)
        {
            return Err(ResourceError::MemoryBackedSpill { path: spill_base });
        }
        if matches!(spill_filesystem, scry_resources::FilesystemClass::Unknown) {
            tracing::warn!(path = %spill_base.display(), "unknown compaction spill filesystem; verify it is persistent storage");
        }
        let pool = Arc::new(PeakTrackingMemoryPool::new(
            config.datafusion_memory_bytes as usize,
        ));
        let mode = match &config.spill_dir {
            Some(path) => DiskManagerMode::Directories(vec![path.clone()]),
            None => DiskManagerMode::OsTmpDirectory,
        };
        let disk = Arc::new(
            DiskManager::builder()
                .with_mode(mode)
                .with_max_temp_directory_size(config.spill_bytes)
                .build()
                .map_err(ResourceError::Runtime)?,
        );
        // RuntimeEnvBuilder cannot accept an existing manager through its new
        // builder API. Build once with the deprecated existing-manager seam so
        // telemetry and all sessions refer to exactly the same manager.
        #[allow(deprecated)]
        let runtime = Arc::new(
            RuntimeEnvBuilder::new()
                .with_memory_pool(pool.clone())
                .with_disk_manager(
                    datafusion::execution::disk_manager::DiskManagerConfig::Existing(disk.clone()),
                )
                .build()
                .map_err(ResourceError::Runtime)?,
        );
        // Requests round up to MiB, so capacity must round down. Rounding both
        // sides up would allow reservations beyond the advertised byte budget.
        let units = bytes_to_capacity_units(config.non_datafusion_memory_bytes) as usize;
        tracing::info!(
            datafusion_memory_bytes = config.datafusion_memory_bytes,
            non_datafusion_memory_bytes = config.non_datafusion_memory_bytes,
            spill_bytes = config.spill_bytes,
            spill_page_cache_headroom_bytes = config.spill_page_cache_headroom_bytes,
            spill_dir = ?config.spill_dir,
            max_waiters = config.max_waiters,
            "resolved compaction resource envelope"
        );
        Ok(Arc::new(Self {
            runtime,
            pool,
            disk,
            admission: Arc::new(Semaphore::new(units)),
            config,
            waiters: AtomicUsize::new(0),
            running_units: Arc::new(AtomicU64::new(0)),
            peak_running_units: AtomicU64::new(0),
            sampled_spill_peak_bytes: AtomicU64::new(0),
            admissions: AtomicU64::new(0),
            rejected: AtomicU64::new(0),
            wait_micros: AtomicU64::new(0),
        }))
    }

    pub fn runtime_env(&self) -> Arc<RuntimeEnv> {
        self.runtime.clone()
    }

    pub fn config(&self) -> &ResourceConfig {
        &self.config
    }

    /// Convert DataFusion memory/spill exhaustion into a retryable compaction
    /// resource error while preserving all other execution errors unchanged.
    pub fn classify_datafusion(error: datafusion::error::DataFusionError) -> anyhow::Error {
        use datafusion::error::DataFusionError;
        if matches!(error.find_root(), DataFusionError::ResourcesExhausted(_)) {
            ResourceError::DataFusionExhausted(error).into()
        } else {
            error.into()
        }
    }

    pub async fn admit(
        self: &Arc<Self>,
        estimated_bytes: u64,
    ) -> Result<ResourcePermit, ResourceError> {
        let units = bytes_to_units(estimated_bytes).max(1);
        let capacity = bytes_to_capacity_units(self.config.non_datafusion_memory_bytes);
        if units > capacity || units > u32::MAX as u64 {
            self.rejected.fetch_add(1, Ordering::Relaxed);
            return Err(ResourceError::RequestTooLarge {
                requested_bytes: estimated_bytes,
                budget_bytes: self.config.non_datafusion_memory_bytes,
            });
        }
        let previous = self.waiters.fetch_add(1, Ordering::AcqRel);
        let waiter = WaiterGuard::new(&self.waiters);
        if previous >= self.config.max_waiters {
            self.rejected.fetch_add(1, Ordering::Relaxed);
            return Err(ResourceError::QueueFull {
                max_waiters: self.config.max_waiters,
            });
        }
        let started = Instant::now();
        let acquired = tokio::time::timeout(
            self.config.admission_timeout,
            self.admission.clone().acquire_many_owned(units as u32),
        )
        .await;
        drop(waiter);
        let waited = started.elapsed();
        self.wait_micros.fetch_add(
            waited.as_micros().min(u64::MAX as u128) as u64,
            Ordering::Relaxed,
        );
        let permit = match acquired {
            Err(_) => {
                self.rejected.fetch_add(1, Ordering::Relaxed);
                return Err(ResourceError::AdmissionTimeout { waited });
            }
            Ok(Err(_)) => return Err(ResourceError::Closed),
            Ok(Ok(permit)) => permit,
        };
        self.admissions.fetch_add(1, Ordering::Relaxed);
        let running_units = self.running_units.fetch_add(units, Ordering::Relaxed) + units;
        self.peak_running_units
            .fetch_max(running_units, Ordering::Relaxed);
        Ok(ResourcePermit {
            permit,
            units,
            running_units: self.running_units.clone(),
        })
    }

    pub fn telemetry(&self) -> ResourceTelemetry {
        let spill = self.disk.spilling_progress();
        self.sampled_spill_peak_bytes
            .fetch_max(spill.current_bytes, Ordering::Relaxed);
        ResourceTelemetry {
            datafusion_reserved_bytes: self.pool.reserved(),
            datafusion_peak_bytes: self.pool.peak(),
            spill_used_bytes: spill.current_bytes,
            sampled_spill_peak_bytes: self.sampled_spill_peak_bytes.load(Ordering::Relaxed),
            spill_active_files: spill.active_files_count,
            weighted_running_bytes: self
                .running_units
                .load(Ordering::Relaxed)
                .saturating_mul(MIB),
            weighted_peak_bytes: self
                .peak_running_units
                .load(Ordering::Relaxed)
                .saturating_mul(MIB),
            weighted_waiters: self.waiters.load(Ordering::Relaxed),
            admissions: self.admissions.load(Ordering::Relaxed),
            rejected: self.rejected.load(Ordering::Relaxed),
            cumulative_wait_micros: self.wait_micros.load(Ordering::Relaxed),
        }
    }
}

fn canonicalize_for_classification(path: &std::path::Path) -> std::io::Result<PathBuf> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };
    let mut existing = absolute.as_path();
    while !existing.exists() {
        existing = existing.parent().ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "spill path has no existing parent",
            )
        })?;
    }
    let canonical = existing.canonicalize()?;
    let remainder = absolute
        .strip_prefix(existing)
        .unwrap_or_else(|_| std::path::Path::new(""));
    Ok(canonical.join(remainder))
}

fn bytes_to_units(bytes: u64) -> u64 {
    bytes.saturating_add(MIB - 1) / MIB
}

fn bytes_to_capacity_units(bytes: u64) -> u64 {
    bytes / MIB
}

struct WaiterGuard<'a> {
    waiters: &'a AtomicUsize,
}

impl<'a> WaiterGuard<'a> {
    fn new(waiters: &'a AtomicUsize) -> Self {
        Self { waiters }
    }
}

impl Drop for WaiterGuard<'_> {
    fn drop(&mut self) {
        self.waiters.fetch_sub(1, Ordering::AcqRel);
    }
}

pub struct ResourcePermit {
    #[allow(dead_code)]
    permit: OwnedSemaphorePermit,
    units: u64,
    running_units: Arc<AtomicU64>,
}

impl ResourcePermit {
    /// Bytes reserved from the process-wide non-DataFusion envelope. Allocation
    /// limits inside a merge must be derived from this value, not from the
    /// process-global capacity, so concurrent merges cannot each spend it.
    pub fn reserved_bytes(&self) -> u64 {
        self.units.saturating_mul(MIB)
    }
}

impl Drop for ResourcePermit {
    fn drop(&mut self) {
        self.running_units.fetch_sub(self.units, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> ResourceConfig {
        ResourceConfig {
            envelope_bytes: 128 * MIB,
            datafusion_memory_bytes: 64 * MIB,
            non_datafusion_memory_bytes: 8 * MIB,
            spill_bytes: 16 * MIB,
            spill_page_cache_headroom_bytes: 8 * MIB,
            spill_dir: None,
            allow_memory_backed_spill: true,
            output_buffer_bytes: 5 * MIB as usize,
            parquet_writer_memory_bytes: MIB as usize,
            max_waiters: 1,
            admission_timeout: Duration::from_millis(20),
        }
    }

    #[tokio::test]
    async fn weighted_admission_releases_and_reports() {
        let resources = CompactResources::new(config()).unwrap();
        let permit = resources.admit(MIB + 1).await.unwrap();
        assert_eq!(resources.telemetry().weighted_running_bytes, 2 * MIB);
        drop(permit);
        let telemetry = resources.telemetry();
        assert_eq!(telemetry.weighted_running_bytes, 0);
        assert_eq!(telemetry.weighted_peak_bytes, 2 * MIB);
        assert_eq!(telemetry.admissions, 1);
    }

    #[test]
    fn datafusion_peak_survives_release() {
        let resources = CompactResources::new(config()).unwrap();
        let consumer = MemoryConsumer::new("peak-test");
        let pool: Arc<dyn MemoryPool> = resources.pool.clone();
        let reservation = consumer.register(&pool);
        reservation.grow(1234);
        reservation.free();

        let telemetry = resources.telemetry();
        assert_eq!(telemetry.datafusion_reserved_bytes, 0);
        assert_eq!(telemetry.datafusion_peak_bytes, 1234);
    }

    #[tokio::test]
    async fn oversized_request_is_retryable_error() {
        let resources = CompactResources::new(config()).unwrap();
        assert!(matches!(
            resources.admit(9 * MIB).await,
            Err(ResourceError::RequestTooLarge { .. })
        ));
        assert_eq!(resources.telemetry().rejected, 1);
    }

    #[tokio::test]
    async fn admission_times_out_without_leaking_waiter() {
        let resources = CompactResources::new(config()).unwrap();
        let _all = resources.admit(8 * MIB).await.unwrap();
        assert!(matches!(
            resources.admit(MIB).await,
            Err(ResourceError::AdmissionTimeout { .. })
        ));
        assert_eq!(resources.telemetry().weighted_waiters, 0);
    }

    #[tokio::test]
    async fn cancelled_admission_does_not_leak_waiter() {
        let resources = CompactResources::new(config()).unwrap();
        let _all = resources.admit(8 * MIB).await.unwrap();
        let resources_for_waiter = resources.clone();
        let waiter = tokio::spawn(async move { resources_for_waiter.admit(MIB).await });
        tokio::task::yield_now().await;
        assert_eq!(resources.telemetry().weighted_waiters, 1);
        waiter.abort();
        let _ = waiter.await;
        assert_eq!(resources.telemetry().weighted_waiters, 0);
    }

    #[test]
    fn rejects_sub_budgets_outside_envelope() {
        let mut cfg = config();
        cfg.datafusion_memory_bytes = 124 * MIB;
        assert!(matches!(
            CompactResources::new(cfg),
            Err(ResourceError::InvalidConfig(_))
        ));
    }

    #[test]
    fn rejects_writer_buffers_outside_non_datafusion_budget() {
        let mut cfg = config();
        cfg.parquet_writer_memory_bytes = 4 * MIB as usize;
        assert!(matches!(
            CompactResources::new(cfg),
            Err(ResourceError::InvalidConfig(_))
        ));
    }

    #[test]
    fn refuses_memory_backed_spill_without_explicit_override() {
        let Some(path) = ["/dev/shm", "/run"]
            .into_iter()
            .map(PathBuf::from)
            .find(|path| {
                matches!(
                    scry_resources::classify_filesystem(path),
                    Ok(scry_resources::FilesystemClass::Memory)
                )
            })
        else {
            return;
        };
        let mut cfg = config();
        cfg.spill_dir = Some(path);
        cfg.allow_memory_backed_spill = false;
        assert!(matches!(
            CompactResources::new(cfg),
            Err(ResourceError::MemoryBackedSpill { .. })
        ));
    }

    #[cfg(unix)]
    #[test]
    fn resolves_relative_symlinked_spill_before_classification() {
        let Some(memory_path) = ["/dev/shm", "/run"]
            .into_iter()
            .map(PathBuf::from)
            .find(|path| {
                matches!(
                    scry_resources::classify_filesystem(path),
                    Ok(scry_resources::FilesystemClass::Memory)
                )
            })
        else {
            return;
        };
        let tmp = tempfile::tempdir().unwrap();
        std::os::unix::fs::symlink(&memory_path, tmp.path().join("spill-link")).unwrap();
        let relative = tmp
            .path()
            .strip_prefix(std::env::current_dir().unwrap())
            .map(PathBuf::from)
            .unwrap_or_else(|_| tmp.path().to_path_buf())
            .join("spill-link/child");
        let mut cfg = config();
        cfg.spill_dir = Some(relative);
        cfg.allow_memory_backed_spill = false;
        assert!(matches!(
            CompactResources::new(cfg),
            Err(ResourceError::MemoryBackedSpill { .. })
        ));
    }

    #[test]
    fn envelope_split_is_conservative() {
        let cfg = ResourceConfig::from_envelope(1024 * MIB);
        assert_eq!(cfg.datafusion_memory_bytes, 512 * MIB);
        assert_eq!(cfg.non_datafusion_memory_bytes, 256 * MIB);
    }
}
