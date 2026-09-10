use std::{
    fs::File,
    path::{Path, PathBuf},
};

use anyhow::{bail, Context, Result};
use fs2::FileExt;

/// Process-lifetime exclusive ownership for a no-Valkey projection writer.
///
/// The adjacent sidecar remains on disk after exit; ownership is the kernel lock,
/// not file existence. Dropping this value releases ownership.
#[derive(Debug)]
pub struct SingletonLock {
    _file: File,
    path: PathBuf,
}

impl SingletonLock {
    pub fn acquire(errors_db: &Path) -> Result<Self> {
        let parent = errors_db
            .parent()
            .filter(|path| !path.as_os_str().is_empty());
        if let Some(parent) = parent {
            std::fs::create_dir_all(parent).with_context(|| {
                format!("creating errors database directory {}", parent.display())
            })?;
        }

        let lock_path = lock_path(errors_db);
        let file = File::options()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&lock_path)
            .with_context(|| format!("opening singleton lock {}", lock_path.display()))?;
        if let Err(error) = file.try_lock_exclusive() {
            if error.kind() == std::io::ErrorKind::WouldBlock {
                bail!(
                    "another no-Valkey errors writer owns {}; only one single-writer process is allowed",
                    lock_path.display()
                );
            }
            return Err(error)
                .with_context(|| format!("locking singleton file {}", lock_path.display()));
        }
        Ok(Self {
            _file: file,
            path: lock_path,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

fn lock_path(errors_db: &Path) -> PathBuf {
    let mut value = errors_db.as_os_str().to_owned();
    value.push(".lock");
    PathBuf::from(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lock_is_exclusive_and_released_on_drop() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("errors.sqlite");
        let first = SingletonLock::acquire(&db).unwrap();
        assert_eq!(first.path(), dir.path().join("errors.sqlite.lock"));

        let error = SingletonLock::acquire(&db).unwrap_err();
        assert!(error
            .to_string()
            .contains("another no-Valkey errors writer"));

        drop(first);
        SingletonLock::acquire(&db).unwrap();
    }
}
