//! Process-level memory safety for query work.
//!
//! DataFusion's memory pool only accounts for allocations made through its
//! reservation API. Parquet decode buffers, Arrow arrays, caches, SQLite and
//! allocator-retained pages sit outside it, so queryd also watches the Linux
//! cgroup's `memory.current` against `memory.max`. The guard is deliberately a
//! small injectable trait: production uses [`CgroupMemoryGuard`], while tests
//! can force exhaustion without allocating real memory.

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result};
use async_trait::async_trait;

pub const QUERY_TOO_LARGE_MESSAGE: &str =
    "Query too large, reduce range, increase memory or add extra queriers.";

#[async_trait]
pub trait QueryMemoryGuard: Send + Sync {
    /// Return `Err` when starting or continuing query work is unsafe.
    fn check(&self) -> Result<()>;

    /// Wait until the process enters the unsafe region. Used in `select!`
    /// around planning and streaming so a query is cancelled even while an
    /// individual async operation is in progress.
    async fn wait_until_exhausted(&self) {
        loop {
            if self.check().is_err() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

/// Linux cgroup-v2 memory guard. `reject_at = memory.max - reserve`; the
/// reserve is headroom for an in-progress allocation, runtime/allocator
/// overhead, and writing the terminal StreamError itself.
pub struct CgroupMemoryGuard {
    current_path: PathBuf,
    reject_at: u64,
    limit: u64,
}

impl CgroupMemoryGuard {
    pub fn detect(reserve_bytes: u64) -> Result<Option<Self>> {
        Self::from_files(
            Path::new("/sys/fs/cgroup/memory.current"),
            Path::new("/sys/fs/cgroup/memory.max"),
            reserve_bytes,
        )
    }

    fn from_files(
        current_path: &Path,
        max_path: &Path,
        reserve_bytes: u64,
    ) -> Result<Option<Self>> {
        let max_raw = match std::fs::read_to_string(max_path) {
            Ok(v) => v,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e).with_context(|| format!("reading {}", max_path.display())),
        };
        let max_raw = max_raw.trim();
        if max_raw == "max" {
            return Ok(None);
        }
        let limit: u64 = max_raw
            .parse()
            .with_context(|| format!("parsing {} value {max_raw:?}", max_path.display()))?;
        let reject_at = limit.saturating_sub(reserve_bytes.min(limit));
        Ok(Some(Self {
            current_path: current_path.to_owned(),
            reject_at,
            limit,
        }))
    }

    pub fn limit_bytes(&self) -> u64 {
        self.limit
    }

    pub fn reject_at_bytes(&self) -> u64 {
        self.reject_at
    }

    fn current_bytes(&self) -> Result<u64> {
        let raw = std::fs::read_to_string(&self.current_path)
            .with_context(|| format!("reading {}", self.current_path.display()))?;
        raw.trim()
            .parse()
            .with_context(|| format!("parsing {} value {raw:?}", self.current_path.display()))
    }
}

#[async_trait]
impl QueryMemoryGuard for CgroupMemoryGuard {
    fn check(&self) -> Result<()> {
        let current = self.current_bytes()?;
        if current >= self.reject_at {
            anyhow::bail!(
                "{QUERY_TOO_LARGE_MESSAGE} cgroup usage={current} bytes, safety threshold={} bytes, limit={} bytes",
                self.reject_at,
                self.limit
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn finite_cgroup_limit_reserves_headroom() {
        let dir = tempdir().unwrap();
        let current = dir.path().join("current");
        let max = dir.path().join("max");
        std::fs::write(&current, "800\n").unwrap();
        std::fs::write(&max, "1000\n").unwrap();
        let guard = CgroupMemoryGuard::from_files(&current, &max, 100)
            .unwrap()
            .unwrap();
        assert_eq!(guard.reject_at_bytes(), 900);
        guard.check().unwrap();
        std::fs::write(&current, "900\n").unwrap();
        assert!(guard
            .check()
            .unwrap_err()
            .to_string()
            .contains(QUERY_TOO_LARGE_MESSAGE));
    }

    #[test]
    fn unlimited_cgroup_disables_guard() {
        let dir = tempdir().unwrap();
        let current = dir.path().join("current");
        let max = dir.path().join("max");
        std::fs::write(&current, "800\n").unwrap();
        std::fs::write(&max, "max\n").unwrap();
        assert!(CgroupMemoryGuard::from_files(&current, &max, 100)
            .unwrap()
            .is_none());
    }
}
