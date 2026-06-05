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

use scry_proto::LabelPair;

use crate::cri::{spawn_tailer, PodPath, RawLog};
use crate::scrape::{self, ScrapeResult, ScrapeTarget};

/// Shared uid → pod-labels map, written by the watcher, read by the batcher.
pub type PodRegistry = Arc<RwLock<HashMap<String, BTreeMap<String, String>>>>;

pub fn new_registry() -> PodRegistry {
    Arc::new(RwLock::new(HashMap::new()))
}

/// Shared uid → scrape-target map for pods that opt in via `prometheus.io/scrape`.
/// Written by the watcher, read by the scrape scheduler.
pub type ScrapeTargetRegistry = Arc<RwLock<HashMap<String, ScrapeTarget>>>;

pub fn new_target_registry() -> ScrapeTargetRegistry {
    Arc::new(RwLock::new(HashMap::new()))
}

/// Watch pods scheduled on `node_name` and keep `registry` in sync with their
/// labels. Runs until `shutdown` flips. Best-effort: if we can't build a
/// Kubernetes client (e.g. running outside a cluster), we log and return so
/// the agent still ships with path-derived core labels.
pub fn spawn_pod_watcher(
    node_name: String,
    registry: PodRegistry,
    targets: ScrapeTargetRegistry,
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
                        Ok(Some(event)) => apply_event(&registry, &targets, &node_name, event).await,
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

async fn apply_event(
    registry: &PodRegistry,
    targets: &ScrapeTargetRegistry,
    node: &str,
    event: watcher::Event<Pod>,
) {
    match event {
        watcher::Event::Apply(pod) | watcher::Event::InitApply(pod) => {
            if let Some(uid) = pod.metadata.uid.clone() {
                let labels = pod.metadata.labels.clone().unwrap_or_default();
                registry.write().await.insert(uid.clone(), labels);
                // A pod that opts in via annotations becomes a scrape target;
                // one that doesn't (or loses its IP) is removed.
                match build_scrape_target(&pod, node) {
                    Some(t) => {
                        targets.write().await.insert(uid, t);
                    }
                    None => {
                        targets.write().await.remove(&uid);
                    }
                }
            }
        }
        watcher::Event::Delete(pod) => {
            if let Some(uid) = pod.metadata.uid.as_ref() {
                registry.write().await.remove(uid);
                targets.write().await.remove(uid);
            }
        }
        watcher::Event::Init | watcher::Event::InitDone => {}
    }
}

/// Build a [`ScrapeTarget`] from a pod's `prometheus.io/*` annotations, or
/// `None` if the pod hasn't opted in (`prometheus.io/scrape != "true"`), has no
/// IP yet, or declares no port.
///
/// The target's identifying labels mirror the log-stream convention
/// ([`crate::stream::stream_labels`]): `namespace`/`pod`/`node` + `k8s_<label>`,
/// plus the Prometheus-conventional `job` and `instance`. `job` is the first
/// present of the pod's `app.kubernetes.io/name` / `app` / `k8s-app` labels,
/// falling back to the pod name.
fn build_scrape_target(pod: &Pod, node: &str) -> Option<ScrapeTarget> {
    let ann = pod.metadata.annotations.as_ref()?;
    if ann.get("prometheus.io/scrape").map(String::as_str) != Some("true") {
        return None;
    }
    let pod_ip = pod.status.as_ref().and_then(|s| s.pod_ip.clone())?;
    let port = ann.get("prometheus.io/port")?.clone();
    let path = ann
        .get("prometheus.io/path")
        .cloned()
        .unwrap_or_else(|| "/metrics".to_string());
    let path = if path.starts_with('/') {
        path
    } else {
        format!("/{path}")
    };
    let scheme = ann
        .get("prometheus.io/scheme")
        .cloned()
        .unwrap_or_else(|| "http".to_string());
    let url = format!("{scheme}://{pod_ip}:{port}{path}");

    let namespace = pod.metadata.namespace.clone().unwrap_or_default();
    let name = pod.metadata.name.clone().unwrap_or_default();
    let pod_labels = pod.metadata.labels.clone().unwrap_or_default();
    let job = pod_labels
        .get("app.kubernetes.io/name")
        .or_else(|| pod_labels.get("app"))
        .or_else(|| pod_labels.get("k8s-app"))
        .cloned()
        .unwrap_or_else(|| name.clone());

    let mut labels = vec![
        LabelPair {
            key: "job".into(),
            value: job,
        },
        LabelPair {
            key: "instance".into(),
            value: format!("{pod_ip}:{port}"),
        },
        LabelPair {
            key: "namespace".into(),
            value: namespace,
        },
        LabelPair {
            key: "pod".into(),
            value: name,
        },
        LabelPair {
            key: "node".into(),
            value: node.to_string(),
        },
    ];
    for (k, v) in &pod_labels {
        labels.push(LabelPair {
            key: format!("k8s_{k}"),
            value: v.clone(),
        });
    }

    Some(ScrapeTarget {
        url,
        labels,
        bearer: None,
    })
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
                if let std::collections::hash_map::Entry::Vacant(slot) = active.entry(path.clone())
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
    entry.file_type().await.map(|t| t.is_dir()).unwrap_or(false)
}

/// Reconcile the desired scrape-target set (static `static_targets` ∪ the
/// discovered `targets` registry) against running per-target scrape tasks:
/// spawn a task for a newly-seen target URL, abort the task for one that
/// vanished. Mirrors [`spawn_log_scanner`]. Each task scrapes on `interval`
/// and feeds [`ScrapeResult`]s into `metrics_tx`.
pub fn spawn_scrape_scheduler(
    targets: ScrapeTargetRegistry,
    static_targets: Vec<ScrapeTarget>,
    http: reqwest::Client,
    interval: Duration,
    reconcile_interval: Duration,
    metrics_tx: mpsc::Sender<ScrapeResult>,
    mut shutdown: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        // Keyed by target URL so a discovered target and a static one with the
        // same URL collapse to a single task.
        let mut active: HashMap<String, tokio::task::JoinHandle<()>> = HashMap::new();
        loop {
            let mut desired: HashMap<String, ScrapeTarget> = HashMap::new();
            for t in &static_targets {
                desired.insert(t.url.clone(), t.clone());
            }
            for t in targets.read().await.values() {
                desired.insert(t.url.clone(), t.clone());
            }

            for (url, target) in &desired {
                if let std::collections::hash_map::Entry::Vacant(slot) = active.entry(url.clone()) {
                    info!(url = %url, "scraping new target");
                    let h = spawn_scrape_task(
                        target.clone(),
                        http.clone(),
                        interval,
                        metrics_tx.clone(),
                        shutdown.clone(),
                    );
                    slot.insert(h);
                }
            }
            // Drop tasks whose target vanished.
            active.retain(|url, h| {
                if desired.contains_key(url) {
                    true
                } else {
                    info!(url = %url, "scrape target gone");
                    h.abort();
                    false
                }
            });

            tokio::select! {
                _ = tokio::time::sleep(reconcile_interval) => {}
                _ = shutdown.changed() => {
                    if *shutdown.borrow() { break; }
                }
            }
        }
        for (_, h) in active {
            h.abort();
        }
        info!("scrape scheduler stopped");
    })
}

/// One target's scrape loop: GET on `interval`, push the result into `tx`.
/// The first tick fires immediately so a target is scraped as soon as it's
/// discovered.
fn spawn_scrape_task(
    target: ScrapeTarget,
    http: reqwest::Client,
    interval: Duration,
    tx: mpsc::Sender<ScrapeResult>,
    mut shutdown: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    let result = scrape::scrape_once(&http, &target, scrape::now_unix_nano()).await;
                    if tx.send(result).await.is_err() {
                        break; // batcher gone
                    }
                }
                _ = shutdown.changed() => {
                    if *shutdown.borrow() { break; }
                }
            }
        }
    })
}
