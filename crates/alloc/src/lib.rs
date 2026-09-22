//! Process-wide allocator support for the `scry` multicall binary.
//!
//! The allocator remains owned by the final binary, while this crate provides
//! the allocator type and safe, typed access to jemalloc's pressure controls.

use std::error::Error;
use std::fmt;

use tikv_jemalloc_ctl::{Access, AsName};

pub use tikv_jemallocator::Jemalloc;

const ALL_ARENAS: usize = 4096;
const DIRTY_DECAY: &[u8] = b"arena.4096.dirty_decay_ms\0";
const MUZZY_DECAY: &[u8] = b"arena.4096.muzzy_decay_ms\0";

/// The phase in which one jemalloc decay control failed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PurgePhase {
    /// Reading the decay setting before triggering a purge.
    Read,
    /// Writing the setting back, or temporarily enabling immediate decay.
    Trigger,
    /// Restoring a disabled (`-1`) decay setting after a temporary purge.
    Restore,
}

/// Failure from one jemalloc decay control.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct DecayControlError {
    /// The jemalloc control name, without its terminating NUL.
    pub control: &'static str,
    /// The operation which failed.
    pub phase: PurgePhase,
    /// The error returned by jemalloc's typed control API.
    pub source: tikv_jemalloc_ctl::Error,
}

impl fmt::Display for DecayControlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "jemalloc {} {:?} failed: {}",
            self.control, self.phase, self.source
        )
    }
}

impl Error for DecayControlError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}

/// Failures observed while purging dirty and muzzy pages.
///
/// Both controls are attempted even if the first fails, so callers receive the
/// complete diagnostic from one pressure-purge attempt.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PurgeError {
    /// Dirty-page control failure, if any.
    pub dirty: Option<DecayControlError>,
    /// Muzzy-page control failure, if any.
    pub muzzy: Option<DecayControlError>,
}

impl fmt::Display for PurgeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match (self.dirty, self.muzzy) {
            (Some(dirty), Some(muzzy)) => write!(f, "{dirty}; {muzzy}"),
            (Some(error), None) | (None, Some(error)) => error.fmt(f),
            (None, None) => f.write_str("jemalloc purge failed without a control error"),
        }
    }
}

impl Error for PurgeError {}

/// Ask jemalloc to purge all currently unused dirty and muzzy pages.
///
/// Jemalloc reserves arena index 4096 for all arenas. Writing each arena's
/// current decay value back to its typed control marks existing unused pages as
/// fully decayed. A value of `-1` disables decay, so it is temporarily changed
/// to `0` to perform the purge and then restored. Dirty and muzzy controls are
/// attempted independently; an error is diagnostic and does not imply that no
/// pages were released.
pub fn purge_all_arenas() -> Result<(), PurgeError> {
    debug_assert_eq!(ALL_ARENAS, 4096);

    let dirty = purge_decay(DIRTY_DECAY, "arena.4096.dirty_decay_ms").err();
    let muzzy = purge_decay(MUZZY_DECAY, "arena.4096.muzzy_decay_ms").err();

    if dirty.is_none() && muzzy.is_none() {
        Ok(())
    } else {
        Err(PurgeError { dirty, muzzy })
    }
}

fn purge_decay(name: &'static [u8], display_name: &'static str) -> Result<(), DecayControlError> {
    let control = name.name();
    let decay_ms: isize = control.read().map_err(|source| DecayControlError {
        control: display_name,
        phase: PurgePhase::Read,
        source,
    })?;

    trigger_decay(decay_ms, |value| control.write(value)).map_err(|(phase, source)| {
        DecayControlError {
            control: display_name,
            phase,
            source,
        }
    })
}

fn trigger_decay<E>(
    decay_ms: isize,
    mut write: impl FnMut(isize) -> Result<(), E>,
) -> Result<(), (PurgePhase, E)> {
    if decay_ms == -1 {
        write(0).map_err(|error| (PurgePhase::Trigger, error))?;
        write(-1).map_err(|error| (PurgePhase::Restore, error))
    } else {
        write(decay_ms).map_err(|error| (PurgePhase::Trigger, error))
    }
}

#[cfg(test)]
#[global_allocator]
static TEST_ALLOCATOR: Jemalloc = Jemalloc;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pressure_purge_preserves_decay_settings() {
        let dirty: isize = DIRTY_DECAY.name().read().expect("read dirty decay");
        let muzzy: isize = MUZZY_DECAY.name().read().expect("read muzzy decay");

        purge_all_arenas().expect("purge all jemalloc arenas");

        let dirty_after: isize = DIRTY_DECAY.name().read().unwrap();
        let muzzy_after: isize = MUZZY_DECAY.name().read().unwrap();
        assert_eq!(dirty_after, dirty);
        assert_eq!(muzzy_after, muzzy);
    }

    #[test]
    fn enabled_decay_is_written_back_once() {
        let mut writes = Vec::new();
        trigger_decay(10_000, |value| {
            writes.push(value);
            Ok::<_, ()>(())
        })
        .unwrap();
        assert_eq!(writes, [10_000]);
    }

    #[test]
    fn disabled_decay_is_temporarily_enabled_and_restored() {
        let mut writes = Vec::new();
        trigger_decay(-1, |value| {
            writes.push(value);
            Ok::<_, ()>(())
        })
        .unwrap();
        assert_eq!(writes, [0, -1]);
    }

    #[test]
    fn restore_failure_is_identified() {
        let mut calls = 0;
        let error = trigger_decay(-1, |_| {
            calls += 1;
            if calls == 2 {
                Err("restore")
            } else {
                Ok(())
            }
        })
        .unwrap_err();
        assert_eq!(error, (PurgePhase::Restore, "restore"));
    }
}
