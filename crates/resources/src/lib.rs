//! Process resource discovery and conservative memory-budget resolution.
//!
//! The crate intentionally depends only on `std` and `anyhow`. Linux procfs
//! inputs are injectable so cgroup namespaces, non-default mount roots, and
//! filesystem classification can be tested without relying on the test host.

use std::fmt;
use std::io;
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

/// Legacy injectable locations for callers that already know the cgroup mount
/// points. New code should use [`CgroupDiscoveryPaths`], which honors mount
/// roots reported by mountinfo.
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

/// Injectable procfs files used for mount-aware cgroup discovery.
#[derive(Clone, Debug)]
pub struct CgroupDiscoveryPaths {
    pub proc_self_cgroup: PathBuf,
    pub proc_self_mountinfo: PathBuf,
}

impl Default for CgroupDiscoveryPaths {
    fn default() -> Self {
        Self {
            proc_self_cgroup: PathBuf::from("/proc/self/cgroup"),
            proc_self_mountinfo: PathBuf::from("/proc/self/mountinfo"),
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

/// One mount-aware cgroup directory whose finite memory limit constrains this
/// process. Current charge, limit, and memory statistics must all be read from
/// this directory to avoid combining unrelated cgroups.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CgroupMemoryDescriptor {
    hierarchy: Vec<PathBuf>,
    discovered_limiting_directory: PathBuf,
    version: CgroupVersion,
}

impl CgroupMemoryDescriptor {
    /// Construct a descriptor for an already-resolved effective limiting
    /// directory. Mount-aware callers normally use
    /// [`detect_cgroup_memory_descriptor`].
    pub fn from_directory(directory: PathBuf, version: CgroupVersion) -> Self {
        Self {
            hierarchy: vec![directory.clone()],
            discovered_limiting_directory: directory,
            version,
        }
    }

    /// Directory which supplied the effective limit during discovery. A fresh
    /// snapshot rechecks the full visible hierarchy and may select another
    /// directory after a limit change.
    pub fn directory(&self) -> &Path {
        &self.discovered_limiting_directory
    }

    pub fn version(&self) -> CgroupVersion {
        self.version
    }

    pub fn current_path(&self) -> PathBuf {
        self.discovered_limiting_directory.join(match self.version {
            CgroupVersion::V2 => "memory.current",
            CgroupVersion::V1 => "memory.usage_in_bytes",
        })
    }

    pub fn stat_path(&self) -> PathBuf {
        self.discovered_limiting_directory.join("memory.stat")
    }

    /// Read a fresh snapshot from this descriptor. Required current and limit
    /// values fail closed; an unavailable or malformed `memory.stat` merely
    /// disables the clean ordinary-file discount.
    pub fn snapshot(&self) -> io::Result<CgroupMemorySnapshot> {
        read_cgroup_memory_snapshot(self)
    }
}

/// A coherent reading from one cgroup directory.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CgroupMemorySnapshot {
    pub limit_bytes: u64,
    pub current_bytes: u64,
    pub reclaimable_clean_file_bytes: u64,
    pub committed_bytes: u64,
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
    detect_cgroup_memory_limit_discovered_with(&CgroupDiscoveryPaths::default())
}

/// Discover the effective finite limiting cgroup directory for this process.
/// The descriptor keeps all subsequent readings on that same cgroup.
pub fn detect_cgroup_memory_descriptor() -> Option<CgroupMemoryDescriptor> {
    detect_cgroup_memory_descriptor_with(&CgroupDiscoveryPaths::default())
}

/// Discover a descriptor while preserving probe failures. Callers using the
/// descriptor as a safety boundary should use this form so malformed or
/// unreadable cgroup controls cannot be mistaken for an unlimited host.
pub fn try_detect_cgroup_memory_descriptor() -> io::Result<Option<CgroupMemoryDescriptor>> {
    try_detect_cgroup_memory_descriptor_with(&CgroupDiscoveryPaths::default())
}

/// Injectable, error-preserving mount-aware descriptor discovery.
pub fn try_detect_cgroup_memory_descriptor_with(
    paths: &CgroupDiscoveryPaths,
) -> io::Result<Option<CgroupMemoryDescriptor>> {
    let cgroups = std::fs::read_to_string(&paths.proc_self_cgroup)?;
    let mountinfo = std::fs::read_to_string(&paths.proc_self_mountinfo)?;
    try_cgroup_memory_descriptor_from_contents(&cgroups, &mountinfo)
}

/// Injectable compatibility form. This intentionally maps discovery failures
/// to `None`; safety-sensitive callers should use
/// [`try_detect_cgroup_memory_descriptor_with`].
pub fn detect_cgroup_memory_descriptor_with(
    paths: &CgroupDiscoveryPaths,
) -> Option<CgroupMemoryDescriptor> {
    try_detect_cgroup_memory_descriptor_with(paths)
        .ok()
        .flatten()
}

/// Resolve the effective finite limiting directory from injectable procfs
/// contents. Cgroup v2 takes precedence when it has any finite constraint.
pub fn cgroup_memory_descriptor_from_contents(
    cgroups: &str,
    mountinfo: &str,
) -> Option<CgroupMemoryDescriptor> {
    let (v2_path, v1_memory_path) = parse_process_cgroups(cgroups);
    let mounts = parse_mountinfo(mountinfo);
    limiting_descriptor(
        v2_path.as_deref(),
        mounts.iter().filter(|mount| mount.fs_type == "cgroup2"),
        CgroupVersion::V2,
    )
    .or_else(|| {
        limiting_descriptor(
            v1_memory_path.as_deref(),
            mounts
                .iter()
                .filter(|mount| mount.fs_type == "cgroup" && mount.has_memory_controller()),
            CgroupVersion::V1,
        )
    })
}

fn try_cgroup_memory_descriptor_from_contents(
    cgroups: &str,
    mountinfo: &str,
) -> io::Result<Option<CgroupMemoryDescriptor>> {
    let (v2_path, v1_memory_path) = parse_process_cgroups(cgroups);
    let mounts = parse_mountinfo(mountinfo);

    if let Some(descriptor) = limiting_descriptor_strict(
        v2_path.as_deref(),
        mounts.iter().filter(|mount| mount.fs_type == "cgroup2"),
        CgroupVersion::V2,
    )? {
        return Ok(Some(descriptor));
    }
    limiting_descriptor_strict(
        v1_memory_path.as_deref(),
        mounts
            .iter()
            .filter(|mount| mount.fs_type == "cgroup" && mount.has_memory_controller()),
        CgroupVersion::V1,
    )
}

/// Locate the memory-usage file for the current process's cgroup. This uses the
/// same mount-root mapping as limit discovery, avoiding assumptions that the
/// process belongs to the cgroup mount root.
pub fn detect_cgroup_memory_usage_path() -> Option<PathBuf> {
    let paths = CgroupDiscoveryPaths::default();
    let cgroups = std::fs::read_to_string(&paths.proc_self_cgroup).ok()?;
    let mountinfo = std::fs::read_to_string(&paths.proc_self_mountinfo).ok()?;
    cgroup_memory_usage_path_from_contents(&cgroups, &mountinfo)
}

/// Injectable counterpart to [`detect_cgroup_memory_usage_path`].
pub fn cgroup_memory_usage_path_from_contents(cgroups: &str, mountinfo: &str) -> Option<PathBuf> {
    cgroup_memory_descriptor_from_contents(cgroups, mountinfo)
        .map(|descriptor| descriptor.current_path())
}

/// Mount-aware injectable detector. Mountinfo's mount root is applied to the
/// process cgroup path, which is required when a cgroup namespace exposes only
/// a subtree of the host hierarchy.
pub fn detect_cgroup_memory_limit_discovered_with(
    paths: &CgroupDiscoveryPaths,
) -> Option<CgroupMemoryLimit> {
    let cgroups = std::fs::read_to_string(&paths.proc_self_cgroup).ok()?;
    let mountinfo = std::fs::read_to_string(&paths.proc_self_mountinfo).ok()?;
    detect_cgroup_memory_limit_from_contents(&cgroups, &mountinfo)
}

/// Resolve a cgroup limit from injectable procfs contents.
pub fn detect_cgroup_memory_limit_from_contents(
    cgroups: &str,
    mountinfo: &str,
) -> Option<CgroupMemoryLimit> {
    let (v2_path, v1_memory_path) = parse_process_cgroups(cgroups);
    let mounts = parse_mountinfo(mountinfo);

    v2_path
        .as_deref()
        .and_then(|path| {
            mounts
                .iter()
                .filter(|mount| mount.fs_type == "cgroup2")
                .filter_map(|mount| {
                    let directory = map_cgroup_path(&mount.mount_point, &mount.root, path)?;
                    read_v2_hierarchy_limit_to(&directory, &mount.mount_point)
                })
                .min()
        })
        .map(|bytes| CgroupMemoryLimit {
            bytes,
            version: CgroupVersion::V2,
        })
        .or_else(|| {
            v1_memory_path
                .as_deref()
                .and_then(|path| {
                    mounts
                        .iter()
                        .filter(|mount| mount.fs_type == "cgroup" && mount.has_memory_controller())
                        .filter_map(|mount| {
                            let directory = map_cgroup_path(&mount.mount_point, &mount.root, path)?;
                            read_v1_hierarchy_limit_to(&directory, &mount.mount_point)
                        })
                        .min()
                })
                .map(|bytes| CgroupMemoryLimit {
                    bytes,
                    version: CgroupVersion::V1,
                })
        })
}

/// Injectable legacy form for alternate proc and pre-resolved cgroup mounts.
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

#[derive(Clone, Debug, Eq, PartialEq)]
struct MountInfo {
    root: PathBuf,
    mount_point: PathBuf,
    fs_type: String,
    super_options: String,
}

impl MountInfo {
    fn has_memory_controller(&self) -> bool {
        self.super_options
            .split(',')
            .any(|option| option == "memory")
    }
}

fn limiting_descriptor<'a>(
    cgroup_path: Option<&Path>,
    mounts: impl Iterator<Item = &'a MountInfo>,
    version: CgroupVersion,
) -> Option<CgroupMemoryDescriptor> {
    let path = cgroup_path?;
    mounts
        .filter_map(|mount| {
            let leaf = map_cgroup_path(&mount.mount_point, &mount.root, path)?;
            let hierarchy = hierarchy_directories_from(&leaf, &mount.mount_point);
            let (limit, directory) = effective_limiting_directory(&hierarchy, version)?;
            Some((limit, directory, hierarchy))
        })
        .min_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)))
        .map(
            |(_, discovered_limiting_directory, hierarchy)| CgroupMemoryDescriptor {
                hierarchy,
                discovered_limiting_directory,
                version,
            },
        )
}

fn effective_limiting_directory(
    hierarchy: &[PathBuf],
    version: CgroupVersion,
) -> Option<(u64, PathBuf)> {
    hierarchy
        .iter()
        .filter_map(|directory| {
            let limit = match version {
                CgroupVersion::V2 => read_v2_effective_limit(
                    &directory.join("memory.max"),
                    &directory.join("memory.high"),
                ),
                CgroupVersion::V1 => {
                    read_finite_limit(&directory.join("memory.limit_in_bytes"), true)
                }
            }?;
            Some((limit, directory.clone()))
        })
        .min_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)))
}

fn limiting_descriptor_strict<'a>(
    cgroup_path: Option<&Path>,
    mounts: impl Iterator<Item = &'a MountInfo>,
    version: CgroupVersion,
) -> io::Result<Option<CgroupMemoryDescriptor>> {
    let Some(path) = cgroup_path else {
        return Ok(None);
    };
    let mut best: Option<(u64, PathBuf, Vec<PathBuf>)> = None;
    for mount in mounts {
        let Some(leaf) = map_cgroup_path(&mount.mount_point, &mount.root, path) else {
            continue;
        };
        let hierarchy = hierarchy_directories_from(&leaf, &mount.mount_point);
        let Some((limit, directory)) = effective_limiting_directory_strict(&hierarchy, version)?
        else {
            continue;
        };
        let candidate = (limit, directory, hierarchy);
        if best
            .as_ref()
            .is_none_or(|current| (candidate.0, &candidate.1) < (current.0, &current.1))
        {
            best = Some(candidate);
        }
    }
    Ok(best.map(
        |(_, discovered_limiting_directory, hierarchy)| CgroupMemoryDescriptor {
            hierarchy,
            discovered_limiting_directory,
            version,
        },
    ))
}

fn effective_limiting_directory_strict(
    hierarchy: &[PathBuf],
    version: CgroupVersion,
) -> io::Result<Option<(u64, PathBuf)>> {
    let mut limiting: Option<(u64, PathBuf)> = None;
    for directory in hierarchy {
        let limit = match version {
            CgroupVersion::V2 => {
                let max = read_limit_for_snapshot(&directory.join("memory.max"), false)?;
                let high = read_limit_for_snapshot(&directory.join("memory.high"), false)?;
                match (max, high) {
                    (Some(max), Some(high)) => Some(max.min(high)),
                    (Some(limit), None) | (None, Some(limit)) => Some(limit),
                    (None, None) => None,
                }
            }
            CgroupVersion::V1 => {
                read_limit_for_snapshot(&directory.join("memory.limit_in_bytes"), true)?
            }
        };
        let Some(limit) = limit else { continue };
        if limiting
            .as_ref()
            .is_none_or(|(best, path)| (limit, directory) < (*best, path))
        {
            limiting = Some((limit, directory.clone()));
        }
    }
    Ok(limiting)
}

fn read_limit_for_snapshot(path: &Path, v1: bool) -> io::Result<Option<u64>> {
    let raw = std::fs::read_to_string(path)?;
    let value = raw.trim();
    if value == "max" || (v1 && value.parse::<u64>().ok() == Some(0)) {
        return Ok(None);
    }
    let bytes = value.parse::<u64>().map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("parsing {} value {raw:?}: {error}", path.display()),
        )
    })?;
    if v1 && bytes >= CGROUP_V1_UNLIMITED_THRESHOLD {
        Ok(None)
    } else {
        Ok(Some(bytes))
    }
}

fn parse_process_cgroups(cgroups: &str) -> (Option<PathBuf>, Option<PathBuf>) {
    let mut v2 = None;
    let mut v1 = None;
    for line in cgroups.lines() {
        let mut fields = line.splitn(3, ':');
        let (Some(_), Some(controllers), Some(path)) =
            (fields.next(), fields.next(), fields.next())
        else {
            continue;
        };
        let Some(path) = safe_cgroup_relative_path(path) else {
            continue;
        };
        if controllers.is_empty() {
            v2 = Some(path);
        } else if controllers
            .split(',')
            .any(|controller| controller == "memory")
        {
            v1 = Some(path);
        }
    }
    (v2, v1)
}

fn unescape_mountinfo(value: &str) -> PathBuf {
    PathBuf::from(
        value
            .replace("\\040", " ")
            .replace("\\011", "\t")
            .replace("\\012", "\n")
            .replace("\\134", "\\"),
    )
}

fn parse_mountinfo(contents: &str) -> Vec<MountInfo> {
    contents
        .lines()
        .filter_map(|line| {
            let (left, right) = line.split_once(" - ")?;
            let mut left = left.split_whitespace();
            let root = unescape_mountinfo(left.nth(3)?);
            let mount_point = unescape_mountinfo(left.next()?);
            let mut right = right.split_whitespace();
            Some(MountInfo {
                root,
                mount_point,
                fs_type: right.next()?.to_owned(),
                super_options: right.nth(1).unwrap_or_default().to_owned(),
            })
        })
        .collect()
}

/// Map a process cgroup path through a mount's root into its visible mountpoint.
pub fn map_cgroup_path(
    mount_point: &Path,
    mount_root: &Path,
    cgroup_path: &Path,
) -> Option<PathBuf> {
    let relative = safe_cgroup_relative_path(cgroup_path.to_str()?)?;
    let root = safe_cgroup_relative_path(mount_root.to_str()?)?;
    let beneath_root = relative.strip_prefix(&root).ok()?;
    Some(mount_point.join(beneath_root))
}

/// Broad storage behavior useful for validating spill and other local paths.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FilesystemClass {
    Memory,
    Local,
    Network,
    Unknown,
}

/// Classify a path using injectable Linux mountinfo contents. The most-specific
/// containing mount wins.
pub fn classify_filesystem_from_mountinfo(path: &Path, mountinfo: &str) -> FilesystemClass {
    let Some(mount) = parse_mountinfo(mountinfo)
        .into_iter()
        .filter(|mount| path.starts_with(&mount.mount_point))
        .max_by_key(|mount| mount.mount_point.components().count())
    else {
        return FilesystemClass::Unknown;
    };
    match mount.fs_type.as_str() {
        "tmpfs" | "ramfs" => FilesystemClass::Memory,
        "nfs" | "nfs4" | "cifs" | "smb3" | "ceph" | "fuse.sshfs" => FilesystemClass::Network,
        "ext2" | "ext3" | "ext4" | "xfs" | "btrfs" | "f2fs" | "zfs" => FilesystemClass::Local,
        _ => FilesystemClass::Unknown,
    }
}

/// Read mountinfo and classify `path` on the current process's mount namespace.
pub fn classify_filesystem(path: &Path) -> io::Result<FilesystemClass> {
    let mountinfo = std::fs::read_to_string("/proc/self/mountinfo")?;
    Ok(classify_filesystem_from_mountinfo(path, &mountinfo))
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
    read_v2_hierarchy_limit_to(&mount.join(relative), mount)
}

fn read_v2_hierarchy_limit_to(directory: &Path, boundary: &Path) -> Option<u64> {
    hierarchy_directories_from(directory, boundary)
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
    read_v1_hierarchy_limit_to(&mount.join(relative), mount)
}

fn read_v1_hierarchy_limit_to(directory: &Path, boundary: &Path) -> Option<u64> {
    hierarchy_directories_from(directory, boundary)
        .into_iter()
        .filter_map(|directory| read_finite_limit(&directory.join("memory.limit_in_bytes"), true))
        .min()
}

fn hierarchy_directories_from(directory: &Path, boundary: &Path) -> Vec<PathBuf> {
    let mut directory = directory.to_path_buf();
    let mut directories = Vec::new();
    loop {
        directories.push(directory.clone());
        if directory == boundary || !directory.pop() {
            break;
        }
    }
    directories
}

fn read_finite_limit(path: &Path, v1: bool) -> Option<u64> {
    let raw = std::fs::read_to_string(path).ok()?;
    parse_finite_limit(&raw, v1)
}

fn parse_finite_limit(raw: &str, v1: bool) -> Option<u64> {
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

/// Read a coherent memory snapshot, refreshing the effective limiting
/// directory first so ancestor or `memory.high` shrink is observed.
pub fn read_cgroup_memory_snapshot(
    descriptor: &CgroupMemoryDescriptor,
) -> io::Result<CgroupMemorySnapshot> {
    let Some((limit_bytes, directory)) =
        effective_limiting_directory_strict(&descriptor.hierarchy, descriptor.version)?
    else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "cgroup has no finite memory limit",
        ));
    };
    let current_path = directory.join(match descriptor.version {
        CgroupVersion::V2 => "memory.current",
        CgroupVersion::V1 => "memory.usage_in_bytes",
    });
    let current_bytes = read_required_u64(&current_path)?;
    let stat = std::fs::read_to_string(directory.join("memory.stat"));
    let reclaimable_clean_file_bytes = stat
        .ok()
        .and_then(|contents| reclaimable_clean_file_bytes(&contents, descriptor.version))
        .unwrap_or(0);
    let committed_bytes =
        current_bytes.saturating_sub(current_bytes.min(reclaimable_clean_file_bytes));
    Ok(CgroupMemorySnapshot {
        limit_bytes,
        current_bytes,
        reclaimable_clean_file_bytes,
        committed_bytes,
        version: descriptor.version,
    })
}

fn read_required_u64(path: &Path) -> io::Result<u64> {
    let raw = std::fs::read_to_string(path)?;
    raw.trim().parse::<u64>().map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("parsing {} value {raw:?}: {error}", path.display()),
        )
    })
}

#[derive(Default)]
struct MemoryStat {
    file: Option<u64>,
    shmem: Option<u64>,
    active_file: Option<u64>,
    inactive_file: Option<u64>,
    dirty: Option<u64>,
    writeback: Option<u64>,
}

/// Compute the conservative clean ordinary-file cache discount from
/// `memory.stat`. Any malformed line, duplicate relevant field, or incomplete
/// required field set yields no discount.
pub fn reclaimable_clean_file_bytes(contents: &str, version: CgroupVersion) -> Option<u64> {
    let mut local = MemoryStat::default();
    let mut total = MemoryStat::default();
    for line in contents.lines() {
        let mut fields = line.split_whitespace();
        let (Some(key), Some(raw), None) = (fields.next(), fields.next(), fields.next()) else {
            return None;
        };
        let target = if version == CgroupVersion::V1 {
            key.strip_prefix("total_").map(|key| (&mut total, key))
        } else {
            None
        }
        .unwrap_or((&mut local, key));
        let slot = match (version, target.1) {
            (CgroupVersion::V2, "file") | (CgroupVersion::V1, "cache") => &mut target.0.file,
            (_, "shmem") => &mut target.0.shmem,
            (_, "active_file") => &mut target.0.active_file,
            (_, "inactive_file") => &mut target.0.inactive_file,
            (CgroupVersion::V2, "file_dirty") | (CgroupVersion::V1, "dirty") => &mut target.0.dirty,
            (CgroupVersion::V2, "file_writeback") | (CgroupVersion::V1, "writeback") => {
                &mut target.0.writeback
            }
            _ => continue,
        };
        if slot.is_some() {
            return None;
        }
        *slot = Some(raw.parse::<u64>().ok()?);
    }
    if version == CgroupVersion::V1 && memory_stat_complete(&total) {
        calculate_reclaimable_clean_file(&total)
    } else {
        calculate_reclaimable_clean_file(&local)
    }
}

fn memory_stat_complete(stat: &MemoryStat) -> bool {
    stat.file.is_some()
        && stat.shmem.is_some()
        && stat.active_file.is_some()
        && stat.inactive_file.is_some()
        && stat.dirty.is_some()
        && stat.writeback.is_some()
}

fn calculate_reclaimable_clean_file(stat: &MemoryStat) -> Option<u64> {
    let ordinary_file = stat.file?.saturating_sub(stat.shmem?);
    let file_lru = stat
        .active_file?
        .saturating_add(stat.inactive_file?)
        .saturating_sub(stat.shmem?);
    Some(
        ordinary_file
            .min(file_lru)
            .saturating_sub(stat.dirty?)
            .saturating_sub(stat.writeback?),
    )
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

    #[test]
    fn mountinfo_root_maps_namespaced_cgroup_and_parent_limit() {
        let dir = TestDir::new();
        let mount = dir.0.join("cgroup mount");
        fs::create_dir_all(mount.join("worker")).unwrap();
        fs::write(mount.join("memory.max"), "2147483648\n").unwrap();
        fs::write(mount.join("memory.current"), "1024\n").unwrap();
        fs::write(mount.join("worker/memory.max"), "max\n").unwrap();
        let escaped = mount.to_string_lossy().replace(' ', "\\040");
        let mountinfo = format!("30 20 0:27 /tenant {escaped} rw - cgroup2 cgroup rw\n");

        assert_eq!(
            detect_cgroup_memory_limit_from_contents("0::/tenant/worker\n", &mountinfo),
            Some(CgroupMemoryLimit {
                bytes: 2 * 1024 * 1024 * 1024,
                version: CgroupVersion::V2,
            })
        );
        assert_eq!(
            cgroup_memory_usage_path_from_contents("0::/tenant/worker\n", &mountinfo),
            Some(mount.join("memory.current"))
        );
    }

    #[test]
    fn filesystem_classification_uses_most_specific_mount() {
        let mounts = concat!(
            "20 1 8:1 / / rw - ext4 /dev/root rw\n",
            "21 20 0:9 / /run rw - tmpfs tmpfs rw\n",
            "22 20 0:10 / /srv/share rw - nfs4 host:/share rw\n",
        );
        assert_eq!(
            classify_filesystem_from_mountinfo(Path::new("/run/spill"), mounts),
            FilesystemClass::Memory
        );
        assert_eq!(
            classify_filesystem_from_mountinfo(Path::new("/srv/share/spill"), mounts),
            FilesystemClass::Network
        );
        assert_eq!(
            classify_filesystem_from_mountinfo(Path::new("/var/spill"), mounts),
            FilesystemClass::Local
        );
    }

    fn stat(
        file: u64,
        shmem: u64,
        active: u64,
        inactive: u64,
        dirty: u64,
        writeback: u64,
    ) -> String {
        format!(
            "file {file}\nshmem {shmem}\nactive_file {active}\ninactive_file {inactive}\nfile_dirty {dirty}\nfile_writeback {writeback}\n"
        )
    }

    #[test]
    fn clean_file_accounting_uses_the_conservative_formula() {
        assert_eq!(
            reclaimable_clean_file_bytes(&stat(1_000, 100, 400, 500, 20, 30), CgroupVersion::V2),
            Some(750)
        );
        assert_eq!(
            reclaimable_clean_file_bytes(&stat(2_000, 100, 200, 300, 20, 30), CgroupVersion::V2),
            Some(350),
            "the file LRU is the tighter bound"
        );
    }

    #[test]
    fn clean_file_accounting_saturates_every_operation() {
        assert_eq!(
            reclaimable_clean_file_bytes(&stat(5, 10, 2, 3, 7, 11), CgroupVersion::V2),
            Some(0)
        );
        assert_eq!(
            reclaimable_clean_file_bytes(
                &stat(u64::MAX, 0, u64::MAX, u64::MAX, 0, 0),
                CgroupVersion::V2
            ),
            Some(u64::MAX)
        );
        assert_eq!(
            reclaimable_clean_file_bytes(&stat(100, 0, 50, 50, 100, 100), CgroupVersion::V2),
            Some(0)
        );
    }

    #[test]
    fn malformed_missing_and_duplicate_stats_disable_the_discount() {
        for contents in [
            "file 100\nshmem 0\n",
            "file nope\nshmem 0\nactive_file 50\ninactive_file 50\nfile_dirty 0\nfile_writeback 0\n",
            "file 100\nfile 100\nshmem 0\nactive_file 50\ninactive_file 50\nfile_dirty 0\nfile_writeback 0\n",
            "file 100 extra\nshmem 0\nactive_file 50\ninactive_file 50\nfile_dirty 0\nfile_writeback 0\n",
            "file 100\nshmem 0\nactive_file 50\ninactive_file 50\nfile_dirty 0\nfile_writeback 0\nbroken\n",
        ] {
            assert_eq!(
                reclaimable_clean_file_bytes(contents, CgroupVersion::V2),
                None,
                "unexpected discount for {contents:?}"
            );
        }
    }

    #[test]
    fn v1_prefers_only_a_complete_hierarchical_stat_set() {
        let local = "cache 100\nshmem 0\nactive_file 50\ninactive_file 50\ndirty 0\nwriteback 0\n";
        let totals = "total_cache 1000\ntotal_shmem 100\ntotal_active_file 400\ntotal_inactive_file 500\ntotal_dirty 20\ntotal_writeback 30\n";
        assert_eq!(
            reclaimable_clean_file_bytes(&format!("{local}{totals}"), CgroupVersion::V1),
            Some(750)
        );
        assert_eq!(
            reclaimable_clean_file_bytes(
                &format!("{local}total_cache 1000\ntotal_shmem 100\n"),
                CgroupVersion::V1
            ),
            Some(100),
            "an incomplete total set must not mix with local fields"
        );
    }

    #[test]
    fn descriptor_snapshot_is_coherent_and_observes_limit_shrink() {
        let dir = TestDir::new();
        let mount = dir.0.join("cgroup");
        let leaf = mount.join("service");
        fs::create_dir_all(&leaf).unwrap();
        fs::write(mount.join("memory.max"), "1000\n").unwrap();
        fs::write(mount.join("memory.high"), "max\n").unwrap();
        fs::write(leaf.join("memory.max"), "max\n").unwrap();
        fs::write(leaf.join("memory.high"), "900\n").unwrap();
        fs::write(leaf.join("memory.current"), "800\n").unwrap();
        fs::write(leaf.join("memory.stat"), stat(700, 100, 300, 400, 20, 30)).unwrap();
        let mountinfo = format!("30 20 0:27 / {} rw - cgroup2 cgroup rw\n", mount.display());
        let descriptor =
            cgroup_memory_descriptor_from_contents("0::/service\n", &mountinfo).unwrap();
        assert_eq!(descriptor.directory(), leaf);
        assert_eq!(descriptor.current_path(), leaf.join("memory.current"));
        assert_eq!(
            descriptor.snapshot().unwrap(),
            CgroupMemorySnapshot {
                limit_bytes: 900,
                current_bytes: 800,
                reclaimable_clean_file_bytes: 550,
                committed_bytes: 250,
                version: CgroupVersion::V2,
            }
        );

        fs::write(mount.join("memory.high"), "700\n").unwrap();
        fs::write(mount.join("memory.current"), "650\n").unwrap();
        fs::write(mount.join("memory.stat"), stat(600, 0, 300, 300, 0, 0)).unwrap();
        assert_eq!(descriptor.snapshot().unwrap().limit_bytes, 700);
        assert_eq!(descriptor.snapshot().unwrap().committed_bytes, 50);
    }

    #[test]
    fn snapshot_clamps_discount_and_missing_stats_to_safe_values() {
        let dir = TestDir::new();
        fs::write(dir.0.join("memory.max"), "1000\n").unwrap();
        fs::write(dir.0.join("memory.high"), "max\n").unwrap();
        fs::write(dir.0.join("memory.current"), "50\n").unwrap();
        fs::write(dir.0.join("memory.stat"), stat(500, 0, 250, 250, 0, 0)).unwrap();
        let descriptor = CgroupMemoryDescriptor::from_directory(dir.0.clone(), CgroupVersion::V2);
        let snapshot = descriptor.snapshot().unwrap();
        assert_eq!(snapshot.reclaimable_clean_file_bytes, 500);
        assert_eq!(snapshot.committed_bytes, 0);

        fs::write(dir.0.join("memory.stat"), "malformed\n").unwrap();
        let snapshot = descriptor.snapshot().unwrap();
        assert_eq!(snapshot.reclaimable_clean_file_bytes, 0);
        assert_eq!(snapshot.committed_bytes, 50);
        fs::remove_file(dir.0.join("memory.stat")).unwrap();
        assert_eq!(descriptor.snapshot().unwrap().committed_bytes, 50);
    }

    #[test]
    fn strict_descriptor_discovery_distinguishes_unlimited_from_broken_controls() {
        let dir = TestDir::new();
        let mount = dir.0.join("cgroup");
        let leaf = mount.join("service");
        fs::create_dir_all(&leaf).unwrap();
        let cgroup_file = dir.0.join("self.cgroup");
        let mountinfo_file = dir.0.join("self.mountinfo");
        fs::write(&cgroup_file, "0::/service\n").unwrap();
        fs::write(
            &mountinfo_file,
            format!("30 20 0:27 / {} rw - cgroup2 cgroup rw\n", mount.display()),
        )
        .unwrap();
        let paths = CgroupDiscoveryPaths {
            proc_self_cgroup: cgroup_file,
            proc_self_mountinfo: mountinfo_file,
        };

        fs::write(mount.join("memory.max"), "max\n").unwrap();
        fs::write(mount.join("memory.high"), "max\n").unwrap();
        fs::write(leaf.join("memory.max"), "max\n").unwrap();
        fs::write(leaf.join("memory.high"), "max\n").unwrap();
        assert_eq!(
            try_detect_cgroup_memory_descriptor_with(&paths).unwrap(),
            None,
            "a readable unlimited hierarchy has no finite descriptor"
        );

        fs::write(leaf.join("memory.max"), "broken\n").unwrap();
        assert_eq!(
            try_detect_cgroup_memory_descriptor_with(&paths)
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidData,
            "a malformed required control must not look unlimited"
        );
        fs::remove_file(leaf.join("memory.max")).unwrap();
        assert_eq!(
            try_detect_cgroup_memory_descriptor_with(&paths)
                .unwrap_err()
                .kind(),
            io::ErrorKind::NotFound,
            "an unreadable required control must not disable the guard"
        );
    }

    #[test]
    fn required_snapshot_readings_fail_closed() {
        let dir = TestDir::new();
        fs::write(dir.0.join("memory.max"), "1000\n").unwrap();
        fs::write(dir.0.join("memory.high"), "max\n").unwrap();
        let descriptor = CgroupMemoryDescriptor::from_directory(dir.0.clone(), CgroupVersion::V2);
        assert_eq!(
            descriptor.snapshot().unwrap_err().kind(),
            io::ErrorKind::NotFound
        );
        fs::write(dir.0.join("memory.current"), "bad\n").unwrap();
        assert_eq!(
            descriptor.snapshot().unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
        fs::write(dir.0.join("memory.current"), "1\n").unwrap();
        fs::write(dir.0.join("memory.max"), "bad\n").unwrap();
        assert_eq!(
            descriptor.snapshot().unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
    }

    #[test]
    fn descriptor_discovery_is_deterministic_across_equivalent_mounts() {
        let dir = TestDir::new();
        let mount_a = dir.0.join("a");
        let mount_b = dir.0.join("b");
        fs::create_dir_all(mount_a.join("service")).unwrap();
        fs::create_dir_all(mount_b.join("service")).unwrap();
        for mount in [&mount_a, &mount_b] {
            fs::write(mount.join("service/memory.max"), "1000\n").unwrap();
            fs::write(mount.join("service/memory.high"), "max\n").unwrap();
        }
        let mountinfo = format!(
            "31 20 0:27 / {} rw - cgroup2 cgroup rw\n30 20 0:27 / {} rw - cgroup2 cgroup rw\n",
            mount_b.display(),
            mount_a.display()
        );
        let descriptor =
            cgroup_memory_descriptor_from_contents("0::/service\n", &mountinfo).unwrap();
        assert_eq!(descriptor.directory(), mount_a.join("service"));
    }

    #[test]
    fn v1_descriptor_uses_hierarchical_totals_and_exact_unlimited_sentinel() {
        let dir = TestDir::new();
        fs::write(dir.0.join("memory.limit_in_bytes"), "1000\n").unwrap();
        fs::write(dir.0.join("memory.usage_in_bytes"), "900\n").unwrap();
        fs::write(
            dir.0.join("memory.stat"),
            concat!(
                "cache 100\nshmem 0\nactive_file 50\ninactive_file 50\ndirty 0\nwriteback 0\n",
                "total_cache 800\ntotal_shmem 100\ntotal_active_file 350\ntotal_inactive_file 450\ntotal_dirty 20\ntotal_writeback 30\n"
            ),
        )
        .unwrap();
        let descriptor = CgroupMemoryDescriptor::from_directory(dir.0.clone(), CgroupVersion::V1);
        assert_eq!(descriptor.snapshot().unwrap().committed_bytes, 250);
        fs::write(
            dir.0.join("memory.limit_in_bytes"),
            CGROUP_V1_UNLIMITED_THRESHOLD.to_string(),
        )
        .unwrap();
        assert_eq!(
            descriptor.snapshot().unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
    }
}
