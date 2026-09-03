//! Linux cgroup memory-limit detection and compaction budget resolution.
//!
//! Detection intentionally reads files through an injectable path set.  This
//! keeps the production code small while allowing tests to exercise the exact
//! cgroup file formats without depending on the machine running the tests.

use std::fmt;
use std::path::{Component, Path, PathBuf};

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

/// Injectable locations used to resolve the current process's nested cgroup.
///
/// `v2_mount` and `v1_memory_mount` are the conventional cgroup filesystem
/// mount points, not paths to individual limit files.
#[derive(Clone, Debug)]
pub struct CgroupDetectionPaths {
    pub proc_self_cgroup: PathBuf,
    pub v2_mount: PathBuf,
    pub v1_memory_mount: PathBuf,
}

impl Default for CgroupDetectionPaths {
    fn default() -> Self {
        Self {
            proc_self_cgroup: PathBuf::from("/proc/self/cgroup"),
            v2_mount: PathBuf::from("/sys/fs/cgroup"),
            v1_memory_mount: PathBuf::from("/sys/fs/cgroup/memory"),
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
/// The process's path is read from `/proc/self/cgroup`; limits are therefore
/// read from its leaf cgroup rather than accidentally from the mount root.
/// cgroup v2 takes precedence, and its effective ceiling is the smaller finite
/// value of `memory.max` and `memory.high`.
pub fn detect_cgroup_memory_limit() -> Option<CgroupMemoryLimit> {
    detect_cgroup_memory_limit_with(&CgroupDetectionPaths::default())
}

/// Injectable form of [`detect_cgroup_memory_limit`] for alternate proc and
/// cgroup mount locations.
pub fn detect_cgroup_memory_limit_with(paths: &CgroupDetectionPaths) -> Option<CgroupMemoryLimit> {
    let cgroups = std::fs::read_to_string(&paths.proc_self_cgroup).ok()?;
    let mut v2_path = None;
    let mut v1_memory_path = None;

    for line in cgroups.lines() {
        let mut fields = line.splitn(3, ':');
        let Some(_hierarchy) = fields.next() else {
            continue;
        };
        let Some(controllers) = fields.next() else {
            continue;
        };
        let Some(path) = fields.next().and_then(safe_cgroup_relative_path) else {
            continue;
        };
        if controllers.is_empty() {
            v2_path = Some(path);
        } else if controllers
            .split(',')
            .any(|controller| controller == "memory")
        {
            v1_memory_path = Some(path);
        }
    }

    v2_path
        .and_then(|path| read_v2_hierarchy_limit(&paths.v2_mount, &path))
        .map(|bytes| CgroupMemoryLimit {
            bytes,
            version: CgroupVersion::V2,
        })
        .or_else(|| {
            v1_memory_path
                .and_then(|path| read_v1_hierarchy_limit(&paths.v1_memory_mount, &path))
                .map(|bytes| CgroupMemoryLimit {
                    bytes,
                    version: CgroupVersion::V1,
                })
        })
}

/// Direct-file injectable form retained for callers that already resolve the
/// cgroup directory. `memory.high` is read beside `v2_memory_max`.
pub fn detect_cgroup_memory_limit_from(paths: &CgroupMemoryPaths) -> Option<CgroupMemoryLimit> {
    let v2_high = paths.v2_memory_max.with_file_name("memory.high");
    read_v2_effective_limit(&paths.v2_memory_max, &v2_high)
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

fn safe_cgroup_relative_path(path: &str) -> Option<PathBuf> {
    let mut relative = PathBuf::new();
    for component in Path::new(path).components() {
        match component {
            Component::RootDir | Component::CurDir => {}
            Component::Normal(part) => relative.push(part),
            Component::ParentDir | Component::Prefix(_) => return None,
        }
    }
    Some(relative)
}

fn read_v2_effective_limit(max_path: &Path, high_path: &Path) -> Option<u64> {
    match (
        read_finite_limit(max_path, false),
        read_finite_limit(high_path, false),
    ) {
        (Some(max), Some(high)) => Some(max.min(high)),
        (Some(limit), None) | (None, Some(limit)) => Some(limit),
        (None, None) => None,
    }
}

// Ancestor constraints also apply to a leaf. Walking to the mount root avoids
// overlooking a finite parent when the leaf itself says `max`.
fn read_v2_hierarchy_limit(mount: &Path, relative: &Path) -> Option<u64> {
    hierarchy_directories(mount, relative)
        .into_iter()
        .filter_map(|directory| {
            read_v2_effective_limit(
                &directory.join("memory.max"),
                &directory.join("memory.high"),
            )
        })
        .min()
}

fn read_v1_hierarchy_limit(mount: &Path, relative: &Path) -> Option<u64> {
    hierarchy_directories(mount, relative)
        .into_iter()
        .filter_map(|directory| read_finite_limit(&directory.join("memory.limit_in_bytes"), true))
        .min()
}

fn hierarchy_directories(mount: &Path, relative: &Path) -> Vec<PathBuf> {
    let mut directory = mount.join(relative);
    let mut directories = Vec::new();
    loop {
        directories.push(directory.clone());
        if directory == mount || !directory.pop() {
            break;
        }
    }
    directories
}

fn read_finite_limit(path: &Path, v1: bool) -> Option<u64> {
    let raw = std::fs::read_to_string(path).ok()?;
    let value = raw.trim();
    if value.is_empty() || value == "max" {
        return None;
    }
    let bytes = value.parse::<u64>().ok()?;
    if v1 && (bytes == 0 || bytes >= CGROUP_V1_UNLIMITED_THRESHOLD) {
        return None;
    }
    Some(bytes)
}

/// Resolve the pool budget from an operator override and an optional cgroup
/// limit. A finite cgroup is always authoritative: both automatic and explicit
/// budgets must retain the percentage and fixed-headroom safety margins. A
/// finite cgroup that cannot provide the minimum safe envelope is an error,
/// never an excuse to select the (possibly larger) fixed fallback.
pub fn resolve_memory_budget(
    explicit_mib: Option<u64>,
    cgroup: Option<CgroupMemoryLimit>,
) -> Result<ResolvedMemoryBudget> {
    let safe_cgroup_budget = cgroup.map(|limit| {
        let percentage = limit.bytes.saturating_mul(CGROUP_MEMORY_BUDGET_PERCENT) / 100;
        let after_headroom = limit.bytes.saturating_sub(MIN_CGROUP_HEADROOM_MIB * MIB);
        percentage.min(after_headroom)
    });

    if let Some(safe_bytes) = safe_cgroup_budget {
        if safe_bytes < MIN_MEMORY_BUDGET_MIB * MIB {
            bail!(
                "detected finite cgroup memory ceiling cannot support the {MIN_MEMORY_BUDGET_MIB} MiB minimum while retaining required headroom"
            );
        }
    }

    if let Some(mib) = explicit_mib {
        if mib < MIN_MEMORY_BUDGET_MIB {
            bail!("--memory-budget-mib must be at least {MIN_MEMORY_BUDGET_MIB} MiB");
        }
        let bytes = mib
            .checked_mul(MIB)
            .ok_or_else(|| anyhow::anyhow!("--memory-budget-mib is too large"))?;
        if let Some(safe_bytes) = safe_cgroup_budget {
            if bytes > safe_bytes {
                bail!(
                    "--memory-budget-mib ({mib} MiB) exceeds the safe budget under the detected finite cgroup ceiling ({} MiB)",
                    safe_bytes / MIB
                );
            }
        }
        return Ok(ResolvedMemoryBudget {
            bytes,
            source: MemoryBudgetSource::Explicit,
            cgroup_limit_bytes: cgroup.map(|limit| limit.bytes),
        });
    }

    if let (Some(limit), Some(bytes)) = (cgroup, safe_cgroup_budget) {
        return Ok(ResolvedMemoryBudget {
            bytes,
            source: match limit.version {
                CgroupVersion::V2 => MemoryBudgetSource::CgroupV2,
                CgroupVersion::V1 => MemoryBudgetSource::CgroupV1,
            },
            cgroup_limit_bytes: Some(limit.bytes),
        });
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
    fn v2_high_reduces_the_effective_limit() {
        let dir = TestDir::new();
        let paths = dir.paths();
        fs::write(&paths.v2_memory_max, "2147483648\n").unwrap();
        fs::write(
            paths.v2_memory_max.with_file_name("memory.high"),
            "1073741824\n",
        )
        .unwrap();

        assert_eq!(
            detect_cgroup_memory_limit_from(&paths),
            Some(CgroupMemoryLimit {
                bytes: 1024 * 1024 * 1024,
                version: CgroupVersion::V2,
            })
        );
    }

    #[test]
    fn nested_process_cgroup_paths_are_resolved_under_mounts() {
        let dir = TestDir::new();
        let v2_mount = dir.0.join("unified");
        let leaf = v2_mount.join("services/compactd");
        fs::create_dir_all(&leaf).unwrap();
        fs::write(dir.0.join("self.cgroup"), "0::/services/compactd\n").unwrap();
        fs::write(leaf.join("memory.max"), "2147483648\n").unwrap();
        fs::write(leaf.join("memory.high"), "1610612736\n").unwrap();

        let paths = CgroupDetectionPaths {
            proc_self_cgroup: dir.0.join("self.cgroup"),
            v2_mount,
            v1_memory_mount: dir.0.join("memory"),
        };
        assert_eq!(
            detect_cgroup_memory_limit_with(&paths),
            Some(CgroupMemoryLimit {
                bytes: 1536 * MIB,
                version: CgroupVersion::V2,
            })
        );
    }

    #[test]
    fn v1_process_cgroup_path_and_controller_list_are_resolved() {
        let dir = TestDir::new();
        let v1_mount = dir.0.join("memory");
        let leaf = v1_mount.join("docker/container");
        fs::create_dir_all(&leaf).unwrap();
        fs::write(
            dir.0.join("self.cgroup"),
            "5:cpu,memory:/docker/container\n",
        )
        .unwrap();
        fs::write(leaf.join("memory.limit_in_bytes"), (1024 * MIB).to_string()).unwrap();

        assert_eq!(
            detect_cgroup_memory_limit_with(&CgroupDetectionPaths {
                proc_self_cgroup: dir.0.join("self.cgroup"),
                v2_mount: dir.0.join("unified"),
                v1_memory_mount: v1_mount,
            }),
            Some(CgroupMemoryLimit {
                bytes: 1024 * MIB,
                version: CgroupVersion::V1,
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
    fn zero_v2_limit_is_finite_and_refuses_a_fallback() {
        let dir = TestDir::new();
        let paths = dir.paths();
        fs::write(&paths.v2_memory_max, "0\n").unwrap();
        let limit = detect_cgroup_memory_limit_from(&paths).expect("zero is a finite v2 limit");
        assert_eq!(limit.bytes, 0);
        assert!(resolve_memory_budget(None, Some(limit)).is_err());
    }

    #[test]
    fn missing_files_are_unavailable() {
        let dir = TestDir::new();
        assert_eq!(detect_cgroup_memory_limit_from(&dir.paths()), None);
    }

    #[test]
    fn explicit_budget_wins_and_checks_units_and_cgroup_safety() {
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

        let constrained = CgroupMemoryLimit {
            bytes: 1024 * MIB,
            version: CgroupVersion::V2,
        };
        assert!(resolve_memory_budget(Some(513), Some(constrained)).is_err());
        assert!(resolve_memory_budget(Some(512), Some(constrained)).is_ok());
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
        assert!(resolve_memory_budget(None, Some(too_small)).is_err());
        assert!(resolve_memory_budget(Some(128), Some(too_small)).is_err());

        let fallback = resolve_memory_budget(None, None).unwrap();
        assert_eq!(fallback.bytes, FALLBACK_MEMORY_BUDGET_MIB * MIB);
        assert_eq!(fallback.source, MemoryBudgetSource::ConservativeFallback);
    }
}
