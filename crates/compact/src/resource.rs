//! Process-wide resource envelope for compaction work.
//!
//! A [`CompactResources`] is intentionally shared by every merge in a process:
//! DataFusion reservations then compete in one [`FairSpillPool`], spill files
//! share one bounded [`DiskManager`], and Arrow/parquet/sidecar memory that
//! DataFusion cannot account for is admitted by a weighted semaphore.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use datafusion::execution::disk_manager::{DiskManager, DiskManagerMode};
use datafusion::execution::memory_pool::{FairSpillPool, MemoryPool};
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

const MIB: u64 = 1024 * 1024;
const FALLBACK_ENVELOPE: u64 = 512 * MIB;
const SYSTEM_HEADROOM: u64 = 256 * MIB;
const QUERY_INGEST_RESERVE: u64 = 256 * MIB;

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
    /// Spill directory. `None` asks the OS for a private temporary directory.
    pub spill_dir: Option<PathBuf>,
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
    /// Resolve a conservative envelope from cgroup-v2. Unlimited, malformed, or
    /// unavailable limits deliberately fall back to a small fixed envelope.
    pub fn detect() -> Self {
        let envelope = detect_cgroup_v2_limit()
            // A finite cgroup is authoritative even when too small: retaining
            // the resulting sub-minimum envelope makes validation refuse
            // startup instead of silently selecting a larger fallback.
            .map(|limit| limit.saturating_sub(SYSTEM_HEADROOM + QUERY_INGEST_RESERVE))
            .unwrap_or(FALLBACK_ENVELOPE);
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
            spill_dir: None,
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
            .is_none_or(|committed| committed > self.envelope_bytes)
        {
            return Err(ResourceError::InvalidConfig(
                "DataFusion and non-DataFusion budgets exceed the compaction envelope",
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

fn read_cgroup_limit(path: &Path) -> Option<u64> {
    let raw = std::fs::read_to_string(path).ok()?;
    let raw = raw.trim();
    (raw != "max").then(|| raw.parse().ok()).flatten()
}

fn detect_cgroup_v2_limit() -> Option<u64> {
    let cgroups = std::fs::read_to_string("/proc/self/cgroup").ok()?;
    let relative = cgroups
        .lines()
        .find_map(|line| {
            let mut fields = line.splitn(3, ':');
            let _ = fields.next()?;
            fields.next()?.is_empty().then(|| fields.next()).flatten()
        })?
        .trim_start_matches('/');
    let dir = Path::new("/sys/fs/cgroup").join(relative);
    let max = read_cgroup_limit(&dir.join("memory.max"));
    let high = read_cgroup_limit(&dir.join("memory.high"));
    match (max, high) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (Some(limit), None) | (None, Some(limit)) => Some(limit),
        (None, None) => None,
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
    pub spill_used_bytes: u64,
    pub spill_active_files: usize,
    pub weighted_running_bytes: u64,
    pub weighted_waiters: usize,
    pub admissions: u64,
    pub rejected: u64,
    pub cumulative_wait_micros: u64,
}

#[derive(Debug)]
pub struct CompactResources {
    runtime: Arc<RuntimeEnv>,
    pool: Arc<FairSpillPool>,
    disk: Arc<DiskManager>,
    admission: Arc<Semaphore>,
    config: ResourceConfig,
    waiters: AtomicUsize,
    running_units: Arc<AtomicU64>,
    admissions: AtomicU64,
    rejected: AtomicU64,
    wait_micros: AtomicU64,
}

impl CompactResources {
    pub fn new(config: ResourceConfig) -> Result<Arc<Self>, ResourceError> {
        config.validate()?;
        let pool = Arc::new(FairSpillPool::new(config.datafusion_memory_bytes as usize));
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
        self.running_units.fetch_add(units, Ordering::Relaxed);
        Ok(ResourcePermit {
            permit,
            units,
            running_units: self.running_units.clone(),
        })
    }

    pub fn telemetry(&self) -> ResourceTelemetry {
        let spill = self.disk.spilling_progress();
        ResourceTelemetry {
            datafusion_reserved_bytes: self.pool.reserved(),
            spill_used_bytes: spill.current_bytes,
            spill_active_files: spill.active_files_count,
            weighted_running_bytes: self
                .running_units
                .load(Ordering::Relaxed)
                .saturating_mul(MIB),
            weighted_waiters: self.waiters.load(Ordering::Relaxed),
            admissions: self.admissions.load(Ordering::Relaxed),
            rejected: self.rejected.load(Ordering::Relaxed),
            cumulative_wait_micros: self.wait_micros.load(Ordering::Relaxed),
        }
    }
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
            spill_dir: None,
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
        assert_eq!(resources.telemetry().weighted_running_bytes, 0);
        assert_eq!(resources.telemetry().admissions, 1);
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
    fn envelope_split_is_conservative() {
        let cfg = ResourceConfig::from_envelope(1024 * MIB);
        assert_eq!(cfg.datafusion_memory_bytes, 512 * MIB);
        assert_eq!(cfg.non_datafusion_memory_bytes, 256 * MIB);
    }
}
