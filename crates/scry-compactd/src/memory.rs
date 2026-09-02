//! Linux cgroup memory-limit detection and compaction budget resolution.
//!
//! Detection intentionally reads files through an injectable path set.  This
//! keeps the production code small while allowing tests to exercise the exact
//! cgroup file formats without depending on the machine running the tests.

use std::fmt;
use std::path::{Path, PathBuf};

use anyhow::{bail, Result};

const MIB: u64 = 1024 * 1024;

/// Fixed budget used when Linux does not expose a finite, usable cgroup limit.
pub const FALLBACK_MEMORY_BUDGET_MIB: u64 = 512;

/// Compaction receives at most half of a detected process cgroup limit. The
/// other half is explicit headroom for the allocator, runtime, catalog,
/// sidecars and output buffers which are not necessarily charged to
/// DataFusion's pool.
pub const CGROUP_MEMORY_BUDGET_PERCENT: u64 = 50;

/// Fixed headroom retained even when half of a large cgroup would otherwise
/// leave less room for process overhead.
pub const MIN_CGROUP_HEADROOM_MIB: u64 = 512;

/// Smallest useful resolved compaction envelope. DataFusion needs room for its
/// sort spill reservation in addition to ordinary writer/object-store state.
pub const MIN_MEMORY_BUDGET_MIB: u64 = 128;

/// cgroup v1 uses very large page-aligned values to mean "unlimited".  Linux's
/// common value is `0x7fff_ffff_ffff_f000`; accepting anything at least this
/// large also covers architecture-specific variants without treating host RAM
/// as a compaction allowance.
const CGROUP_V1_UNLIMITED_THRESHOLD: u64 = 0x7fff_ffff_ffff_f000;

#[derive(Clone, Debug)]
pub struct CgroupMemoryPaths {
    pub v2_memory_max: PathBuf,
    pub v1_memory_limit_in_bytes: PathBuf,
}

impl Default for CgroupMemoryPaths {
    fn default() -> Self {
        Self {
            v2_memory_max: PathBuf::from("/sys/fs/cgroup/memory.max"),
            v1_memory_limit_in_bytes: PathBuf::from("/sys/fs/cgroup/memory/memory.limit_in_bytes"),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CgroupVersion {
    V2,
    V1,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CgroupMemoryLimit {
    pub bytes: u64,
    pub version: CgroupVersion,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MemoryBudgetSource {
    Explicit,
    CgroupV2,
    CgroupV1,
    ConservativeFallback,
}

impl fmt::Display for MemoryBudgetSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Explicit => "explicit --memory-budget-mib",
            Self::CgroupV2 => "cgroup v2 memory.max",
            Self::CgroupV1 => "cgroup v1 memory.limit_in_bytes",
            Self::ConservativeFallback => "conservative fixed fallback",
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResolvedMemoryBudget {
    pub bytes: u64,
    pub source: MemoryBudgetSource,
    pub cgroup_limit_bytes: Option<u64>,
}

/// Detect the process memory ceiling from the standard Linux cgroup files.
///
/// cgroup v2 takes precedence. Missing, unreadable, malformed, zero and
/// unlimited values are treated as unavailable, allowing v1 fallback and then
/// the conservative fixed budget. Detection must not accidentally turn a
/// broken cgroup mount into an unlimited compaction pool.
pub fn detect_cgroup_memory_limit() -> Option<CgroupMemoryLimit> {
    detect_cgroup_memory_limit_from(&CgroupMemoryPaths::default())
}

/// File-path-injectable form of [`detect_cgroup_memory_limit`].
pub fn detect_cgroup_memory_limit_from(paths: &CgroupMemoryPaths) -> Option<CgroupMemoryLimit> {
    read_finite_limit(&paths.v2_memory_max, false)
        .map(|bytes| CgroupMemoryLimit {
            bytes,
            version: CgroupVersion::V2,
        })
        .or_else(|| {
            read_finite_limit(&paths.v1_memory_limit_in_bytes, true).map(|bytes| {
                CgroupMemoryLimit {
                    bytes,
                    version: CgroupVersion::V1,
                }
            })
        })
}

fn read_finite_limit(path: &Path, v1: bool) -> Option<u64> {
    let raw = std::fs::read_to_string(path).ok()?;
    let value = raw.trim();
    if value.is_empty() || value == "max" {
        return None;
    }
    let bytes = value.parse::<u64>().ok()?;
    if bytes == 0 || (v1 && bytes >= CGROUP_V1_UNLIMITED_THRESHOLD) {
        return None;
    }
    Some(bytes)
}

/// Resolve the pool budget from an operator override and an optional cgroup
/// limit. Explicit configuration always wins. Without it, half of a finite
/// cgroup limit is used; absent/unlimited/invalid detection selects 512 MiB.
pub fn resolve_memory_budget(
    explicit_mib: Option<u64>,
    cgroup: Option<CgroupMemoryLimit>,
) -> Result<ResolvedMemoryBudget> {
    if let Some(mib) = explicit_mib {
        if mib < MIN_MEMORY_BUDGET_MIB {
            bail!("--memory-budget-mib must be at least {MIN_MEMORY_BUDGET_MIB} MiB");
        }
        let bytes = mib
            .checked_mul(MIB)
            .ok_or_else(|| anyhow::anyhow!("--memory-budget-mib is too large"))?;
        return Ok(ResolvedMemoryBudget {
            bytes,
            source: MemoryBudgetSource::Explicit,
            cgroup_limit_bytes: cgroup.map(|limit| limit.bytes),
        });
    }

    if let Some(limit) = cgroup {
        let percentage = limit.bytes.saturating_mul(CGROUP_MEMORY_BUDGET_PERCENT) / 100;
        let after_headroom = limit.bytes.saturating_sub(MIN_CGROUP_HEADROOM_MIB * MIB);
        let bytes = percentage.min(after_headroom);
        if bytes >= MIN_MEMORY_BUDGET_MIB * MIB {
            return Ok(ResolvedMemoryBudget {
                bytes,
                source: match limit.version {
                    CgroupVersion::V2 => MemoryBudgetSource::CgroupV2,
                    CgroupVersion::V1 => MemoryBudgetSource::CgroupV1,
                },
                cgroup_limit_bytes: Some(limit.bytes),
            });
        }
    }

    Ok(ResolvedMemoryBudget {
        bytes: FALLBACK_MEMORY_BUDGET_MIB * MIB,
        source: MemoryBudgetSource::ConservativeFallback,
        cgroup_limit_bytes: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    struct TestDir(PathBuf);

    impl TestDir {
        fn new() -> Self {
            let nonce = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let path = std::env::temp_dir().join(format!(
                "scry-compactd-memory-{}-{nonce}",
                std::process::id()
            ));
            fs::create_dir(&path).unwrap();
            Self(path)
        }

        fn paths(&self) -> CgroupMemoryPaths {
            CgroupMemoryPaths {
                v2_memory_max: self.0.join("memory.max"),
                v1_memory_limit_in_bytes: self.0.join("memory.limit_in_bytes"),
            }
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn finite_v2_limit_takes_precedence() {
        let dir = TestDir::new();
        let paths = dir.paths();
        fs::write(&paths.v2_memory_max, "2147483648\n").unwrap();
        fs::write(&paths.v1_memory_limit_in_bytes, "1073741824\n").unwrap();

        assert_eq!(
            detect_cgroup_memory_limit_from(&paths),
            Some(CgroupMemoryLimit {
                bytes: 2 * 1024 * 1024 * 1024,
                version: CgroupVersion::V2,
            })
        );
    }

    #[test]
    fn unlimited_v2_falls_back_to_finite_v1() {
        let dir = TestDir::new();
        let paths = dir.paths();
        fs::write(&paths.v2_memory_max, "max\n").unwrap();
        fs::write(&paths.v1_memory_limit_in_bytes, "1073741824\n").unwrap();

        assert_eq!(
            detect_cgroup_memory_limit_from(&paths),
            Some(CgroupMemoryLimit {
                bytes: 1024 * 1024 * 1024,
                version: CgroupVersion::V1,
            })
        );
    }

    #[test]
    fn malformed_and_v1_unlimited_values_are_unavailable() {
        let dir = TestDir::new();
        let paths = dir.paths();
        fs::write(&paths.v2_memory_max, "not-a-number\n").unwrap();
        fs::write(
            &paths.v1_memory_limit_in_bytes,
            CGROUP_V1_UNLIMITED_THRESHOLD.to_string(),
        )
        .unwrap();

        assert_eq!(detect_cgroup_memory_limit_from(&paths), None);
    }

    #[test]
    fn missing_files_are_unavailable() {
        let dir = TestDir::new();
        assert_eq!(detect_cgroup_memory_limit_from(&dir.paths()), None);
    }

    #[test]
    fn explicit_budget_wins_and_checks_units() {
        let cgroup = CgroupMemoryLimit {
            bytes: 4 * 1024 * 1024 * 1024,
            version: CgroupVersion::V2,
        };
        let budget = resolve_memory_budget(Some(768), Some(cgroup)).unwrap();
        assert_eq!(budget.bytes, 768 * MIB);
        assert_eq!(budget.source, MemoryBudgetSource::Explicit);
        assert_eq!(budget.cgroup_limit_bytes, Some(cgroup.bytes));
        assert!(resolve_memory_budget(Some(127), None).is_err());
        assert!(resolve_memory_budget(Some(u64::MAX), None).is_err());
    }

    #[test]
    fn cgroup_and_fallback_budgets_are_conservative() {
        let cgroup = CgroupMemoryLimit {
            bytes: 2 * 1024 * 1024 * 1024,
            version: CgroupVersion::V1,
        };
        let budget = resolve_memory_budget(None, Some(cgroup)).unwrap();
        assert_eq!(budget.bytes, 1024 * MIB);
        assert_eq!(budget.source, MemoryBudgetSource::CgroupV1);

        let small = CgroupMemoryLimit {
            bytes: 768 * MIB,
            version: CgroupVersion::V2,
        };
        let budget = resolve_memory_budget(None, Some(small)).unwrap();
        assert_eq!(budget.bytes, 256 * MIB, "fixed headroom is retained");

        let too_small = CgroupMemoryLimit {
            bytes: 600 * MIB,
            version: CgroupVersion::V2,
        };
        let budget = resolve_memory_budget(None, Some(too_small)).unwrap();
        assert_eq!(budget.source, MemoryBudgetSource::ConservativeFallback);

        let fallback = resolve_memory_budget(None, None).unwrap();
        assert_eq!(fallback.bytes, FALLBACK_MEMORY_BUDGET_MIB * MIB);
        assert_eq!(fallback.source, MemoryBudgetSource::ConservativeFallback);
    }
}
