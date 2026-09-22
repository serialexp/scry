use std::{
    fs::File,
    path::{Path, PathBuf},
};

use anyhow::{bail, Context, Result};
use fs2::FileExt;

#[derive(Debug)]
pub struct SingletonLock {
    _file: File,
    #[cfg(test)]
    path: PathBuf,
}

impl SingletonLock {
    pub fn acquire(alerts_db: &Path) -> Result<Self> {
        if let Some(parent) = alerts_db
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
        {
            std::fs::create_dir_all(parent).with_context(|| {
                format!("creating alerts database directory {}", parent.display())
            })?;
        }
        let path = lock_path(alerts_db);
        let file = File::options()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)
            .with_context(|| format!("opening alert singleton lock {}", path.display()))?;
        if let Err(error) = file.try_lock_exclusive() {
            if error.kind() == std::io::ErrorKind::WouldBlock {
                bail!(
                    "another single-writer alert process owns {}",
                    path.display()
                );
            }
            return Err(error).with_context(|| format!("locking {}", path.display()));
        }
        Ok(Self {
            _file: file,
            #[cfg(test)]
            path,
        })
    }

    #[cfg(test)]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

fn lock_path(database: &Path) -> PathBuf {
    let mut value = database.as_os_str().to_owned();
    value.push(".lock");
    PathBuf::from(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lock_excludes_second_process_handle() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("alerts.sqlite");
        let first = SingletonLock::acquire(&db).unwrap();
        assert_eq!(first.path(), dir.path().join("alerts.sqlite.lock"));
        assert!(SingletonLock::acquire(&db).is_err());
        drop(first);
        SingletonLock::acquire(&db).unwrap();
    }
}
