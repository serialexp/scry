//! A swappable, shared read handle to an errors database.
//!
//! Query servers read issues from a local errors database that a background
//! task may replace (for example with a newer snapshot). [`SharedErrorsDb`]
//! lets that task install a new connection atomically while queries keep
//! running: a query clones the current connection `Arc` under a brief read
//! lock and runs on the blocking pool, so a replacement never waits for, or
//! interrupts, an in-flight query. The old connection closes when its last
//! query finishes.
//!
//! One connection serves one query at a time (a `rusqlite::Connection` is not
//! `Sync`); the issue queries are single index range scans bounded by
//! [`crate::sqlite::MAX_ISSUE_PAGE_ROWS`], so queueing on the connection mutex
//! is short.

use std::sync::{Arc, Mutex, RwLock};

use anyhow::{Context, Result};

use crate::sqlite::{ErrorsDb, SqliteError};

#[derive(Clone, Default)]
pub struct SharedErrorsDb {
    current: Arc<RwLock<Option<Arc<Mutex<ErrorsDb>>>>>,
}

impl SharedErrorsDb {
    /// An empty handle: queries report the database as unavailable until a
    /// connection is installed.
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_db(db: ErrorsDb) -> Self {
        let handle = Self::new();
        handle.replace(Some(db));
        handle
    }

    /// Install `db` (or clear the handle). In-flight queries finish on the
    /// connection they started with.
    pub fn replace(&self, db: Option<ErrorsDb>) {
        let next = db.map(|db| Arc::new(Mutex::new(db)));
        *self
            .current
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = next;
    }

    pub fn is_available(&self) -> bool {
        self.current
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_some()
    }

    /// Run `query` against the current connection on the blocking pool.
    /// `Ok(None)` when no database is installed.
    pub async fn read<T, F>(&self, query: F) -> Result<Option<T>>
    where
        F: FnOnce(&ErrorsDb) -> Result<T, SqliteError> + Send + 'static,
        T: Send + 'static,
    {
        let Some(db) = self
            .current
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
        else {
            return Ok(None);
        };
        let result = tokio::task::spawn_blocking(move || {
            // A panic inside a query cannot leave the read-only connection in
            // a torn state, so a poisoned mutex is still usable.
            let db = db.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            query(&db)
        })
        .await
        .context("errors database query task failed")?;
        Ok(Some(result?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn replace_swaps_the_connection_for_new_queries() {
        let handle = SharedErrorsDb::new();
        assert!(!handle.is_available());
        assert!(handle
            .read(|db| Ok(db.deployment_id()))
            .await
            .unwrap()
            .is_none());

        handle.replace(Some(ErrorsDb::open_in_memory([1; 16]).unwrap()));
        let clone = handle.clone();
        assert_eq!(
            clone.read(|db| Ok(db.deployment_id())).await.unwrap(),
            Some([1; 16])
        );
        handle.replace(Some(ErrorsDb::open_in_memory([2; 16]).unwrap()));
        assert_eq!(
            clone.read(|db| Ok(db.deployment_id())).await.unwrap(),
            Some([2; 16])
        );
        handle.replace(None);
        assert!(!clone.is_available());
    }

    #[tokio::test]
    async fn query_errors_propagate() {
        let handle = SharedErrorsDb::with_db(ErrorsDb::open_in_memory([1; 16]).unwrap());
        let error = handle
            .read(|_| -> Result<(), SqliteError> { Err(SqliteError::MissingDeployment) })
            .await
            .unwrap_err();
        assert!(error.downcast_ref::<SqliteError>().is_some());
    }
}
