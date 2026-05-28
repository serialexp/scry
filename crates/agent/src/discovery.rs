//! Discovery: which container log files exist on this node (filesystem
//! scan), and what pod labels enrich them (Kubernetes watch).
//!
//! The two are deliberately decoupled. The CRI path already encodes
//! namespace / pod / uid / container, so tailing works with no cluster at
//! all (local testing, or before the watch has caught up). The Kubernetes
//! watch only *enriches*: it maintains a uid → pod-labels map the batcher
//! consults when building a stream's label set.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use futures::{StreamExt, TryStreamExt};
use k8s_openapi::api::core::v1::Pod;
use kube::runtime::{watcher, WatchStreamExt};
use kube::{Api, Client};
use tokio::sync::{mpsc, watch, RwLock};
use tracing::{info, warn};

use crate::cri::{spawn_tailer, PodPath, RawLog};

/// Shared uid → pod-labels map, written by the watcher, read by the batcher.
pub type PodRegistry = Arc<RwLock<HashMap<String, BTreeMap<String, String>>>>;

pub fn new_registry() -> PodRegistry {
    Arc::new(RwLock::new(HashMap::new()))
}

/// Watch pods scheduled on `node_name` and keep `registry` in sync with their
/// labels. Runs until `shutdown` flips. Best-effort: if we can't build a
/// Kubernetes client (e.g. running outside a cluster), we log and return so
/// the agent still ships with path-derived core labels.
pub fn spawn_pod_watcher(
    node_name: String,
    registry: PodRegistry,
    mut shutdown: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let client = match Client::try_default().await {
            Ok(c) => c,
            Err(e) => {
                warn!(error = %e, "no Kubernetes client; running without pod-label enrichment");
                return;
            }
        };
        let api: Api<Pod> = Api::all(client);
        let cfg = watcher::Config::default().fields(&format!("spec.nodeName={node_name}"));
        info!(node = %node_name, "starting pod watch");

        let mut stream = watcher(api, cfg).default_backoff().boxed();
        loop {
            tokio::select! {
                _ = shutdown.changed() => {
                    if *shutdown.borrow() { break; }
                }
                next = stream.try_next() => {
                    match next {
                        Ok(Some(event)) => apply_event(&registry, event).await,
                        Ok(None) => break,
                        Err(e) => {
                            warn!(error = %e, "pod watch error; backing off");
                            tokio::time::sleep(Duration::from_secs(2)).await;
                        }
                    }
                }
            }
        }
        info!("pod watch stopped");
    })
}

async fn apply_event(registry: &PodRegistry, event: watcher::Event<Pod>) {
    match event {
        watcher::Event::Apply(pod) | watcher::Event::InitApply(pod) => {
            if let Some(uid) = pod.metadata.uid.clone() {
                let labels = pod.metadata.labels.clone().unwrap_or_default();
                registry.write().await.insert(uid, labels);
            }
        }
        watcher::Event::Delete(pod) => {
            if let Some(uid) = pod.metadata.uid.as_ref() {
                registry.write().await.remove(uid);
            }
        }
        watcher::Event::Init | watcher::Event::InitDone => {}
    }
}

/// Periodically scan `logs_root` for container log files and keep one tailer
/// running per file. New files get a tailer; vanished files (deleted pods)
/// have theirs aborted. Tailers feed `tx`.
pub fn spawn_log_scanner(
    logs_root: PathBuf,
    from_start: bool,
    poll: Duration,
    scan_interval: Duration,
    tx: mpsc::Sender<RawLog>,
    mut shutdown: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut active: HashMap<PathBuf, tokio::task::JoinHandle<()>> = HashMap::new();
        loop {
            let found = scan(&logs_root).await;
            let found_set: HashSet<PathBuf> = found.iter().map(|(_, p)| p.clone()).collect();

            for (pod, path) in found {
                if let std::collections::hash_map::Entry::Vacant(slot) =
                    active.entry(path.clone())
                {
                    info!(path = %path.display(), "tailing new container log");
                    let h = spawn_tailer(
                        Arc::new(pod),
                        path,
                        from_start,
                        poll,
                        tx.clone(),
                        shutdown.clone(),
                    );
                    slot.insert(h);
                }
            }
            // Drop tailers whose files vanished.
            active.retain(|path, h| {
                if found_set.contains(path) {
                    true
                } else {
                    h.abort();
                    false
                }
            });

            tokio::select! {
                _ = tokio::time::sleep(scan_interval) => {}
                _ = shutdown.changed() => {
                    if *shutdown.borrow() { break; }
                }
            }
        }
        for (_, h) in active {
            h.abort();
        }
        info!("log scanner stopped");
    })
}

/// Enumerate `<logs_root>/<pod_dir>/<container>/<N>.log`. Rotated files
/// (`<N>.log.<timestamp>`) are skipped — their extension isn't `log`.
async fn scan(logs_root: &PathBuf) -> Vec<(PodPath, PathBuf)> {
    let mut out = Vec::new();
    let mut pod_dirs = match tokio::fs::read_dir(logs_root).await {
        Ok(d) => d,
        Err(e) => {
            warn!(root = %logs_root.display(), error = %e, "cannot read logs root");
            return out;
        }
    };
    while let Ok(Some(pod_dir)) = pod_dirs.next_entry().await {
        if !is_dir(&pod_dir).await {
            continue;
        }
        let mut containers = match tokio::fs::read_dir(pod_dir.path()).await {
            Ok(d) => d,
            Err(_) => continue,
        };
        while let Ok(Some(cdir)) = containers.next_entry().await {
            if !is_dir(&cdir).await {
                continue;
            }
            let mut files = match tokio::fs::read_dir(cdir.path()).await {
                Ok(d) => d,
                Err(_) => continue,
            };
            while let Ok(Some(file)) = files.next_entry().await {
                let path = file.path();
                if path.extension().and_then(|e| e.to_str()) != Some("log") {
                    continue;
                }
                if let Some(pp) = PodPath::from_log_file(&path) {
                    out.push((pp, path));
                }
            }
        }
    }
    out
}

async fn is_dir(entry: &tokio::fs::DirEntry) -> bool {
    entry
        .file_type()
        .await
        .map(|t| t.is_dir())
        .unwrap_or(false)
}
