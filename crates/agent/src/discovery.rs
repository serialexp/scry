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

use crate::config::PodScrapeJob;
use crate::cri::{spawn_tailer, PodPath, RawLog};
use crate::scrape::{self, ClientPool, ScrapeResult, ScrapeTarget};

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
    // Config-driven pod-label SD jobs: a pod matching a job's selector becomes
    // a scrape target even without `prometheus.io/scrape` annotations.
    scrape_pods: Arc<Vec<PodScrapeJob>>,
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
                        Ok(Some(event)) => apply_event(&registry, &targets, &node_name, &scrape_pods, event).await,
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
    scrape_pods: &[PodScrapeJob],
    event: watcher::Event<Pod>,
) {
    match event {
        watcher::Event::Apply(pod) | watcher::Event::InitApply(pod) => {
            if let Some(uid) = pod.metadata.uid.clone() {
                let labels = pod.metadata.labels.clone().unwrap_or_default();
                registry.write().await.insert(uid.clone(), labels);
                // A pod that opts in via annotations — or matches a configured
                // pod-SD selector — becomes a scrape target; one that doesn't
                // (or loses its IP) is removed.
                match build_scrape_target(&pod, node, scrape_pods) {
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

/// Build a [`ScrapeTarget`] for a pod, or `None` if it isn't scrapable (no IP
/// yet, no opt-in). Two opt-in routes, annotation first:
///
/// 1. **Annotation SD** — `prometheus.io/scrape == "true"` (+ `port`/`path`/`scheme`).
/// 2. **Pod-label SD** — the pod's labels match a configured `[[metrics.scrape_pods]]`
///    `selector` (first job wins); port/path/scheme come from the job.
///
/// The target's identifying labels mirror the log-stream convention
/// ([`crate::stream::stream_labels`]): `namespace`/`pod`/`node` + `k8s_<label>`,
/// plus the Prometheus-conventional `job` and `instance`. `job` is the job's
/// configured name (pod-SD), else the first present of the pod's
/// `app.kubernetes.io/name` / `app` / `k8s-app` labels, falling back to the pod name.
fn build_scrape_target(pod: &Pod, node: &str, jobs: &[PodScrapeJob]) -> Option<ScrapeTarget> {
    let pod_ip = pod.status.as_ref().and_then(|s| s.pod_ip.clone());
    let pod_labels = pod.metadata.labels.clone().unwrap_or_default();

    // 1. Annotation opt-in takes precedence over selector SD.
    if let Some(ann) = pod.metadata.annotations.as_ref() {
        if ann.get("prometheus.io/scrape").map(String::as_str) == Some("true") {
            let pod_ip = pod_ip?;
            let port = ann.get("prometheus.io/port")?.clone();
            let path = ensure_leading_slash(
                ann.get("prometheus.io/path")
                    .map(String::as_str)
                    .unwrap_or("/metrics"),
            );
            let scheme = ann
                .get("prometheus.io/scheme")
                .cloned()
                .unwrap_or_else(|| "http".to_string());
            let job = derive_job(&pod_labels, pod);
            return Some(assemble_pod_target(
                pod, node, &pod_ip, &port, &path, &scheme, job,
            ));
        }
    }

    // 2. Config pod-label SD (first matching job wins).
    for job in jobs {
        if pod_matches(&job.selector, &pod_labels) {
            let pod_ip = pod_ip?;
            let port = job.port.to_string();
            let job_label = job
                .job
                .clone()
                .unwrap_or_else(|| derive_job(&pod_labels, pod));
            return Some(assemble_pod_target(
                pod,
                node,
                &pod_ip,
                &port,
                &job.path,
                &job.scheme,
                job_label,
            ));
        }
    }

    None
}

/// True when every `key = value` pair in `selector` is present (and equal) in
/// `pod_labels` — Kubernetes `matchLabels` (AND) semantics. An empty selector
/// matches everything (intentionally — but config selectors are always non-empty
/// since `selector` is a required field).
fn pod_matches(selector: &BTreeMap<String, String>, pod_labels: &BTreeMap<String, String>) -> bool {
    selector
        .iter()
        .all(|(k, v)| pod_labels.get(k).map(String::as_str) == Some(v.as_str()))
}

/// `job` label: first present of the pod's `app.kubernetes.io/name` / `app` /
/// `k8s-app` labels, falling back to the pod name.
fn derive_job(pod_labels: &BTreeMap<String, String>, pod: &Pod) -> String {
    pod_labels
        .get("app.kubernetes.io/name")
        .or_else(|| pod_labels.get("app"))
        .or_else(|| pod_labels.get("k8s-app"))
        .cloned()
        .unwrap_or_else(|| pod.metadata.name.clone().unwrap_or_default())
}

/// Assemble a [`ScrapeTarget`] with the shared label convention. Static labels +
/// the relabel map are stamped on later by the scrape scheduler.
fn assemble_pod_target(
    pod: &Pod,
    node: &str,
    pod_ip: &str,
    port: &str,
    path: &str,
    scheme: &str,
    job: String,
) -> ScrapeTarget {
    let url = format!("{scheme}://{pod_ip}:{port}{path}");
    let namespace = pod.metadata.namespace.clone().unwrap_or_default();
    let name = pod.metadata.name.clone().unwrap_or_default();
    let pod_labels = pod.metadata.labels.clone().unwrap_or_default();

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

    ScrapeTarget {
        url,
        labels,
        bearer: None,
        tls: Default::default(),
        label_map: Default::default(),
    }
}

/// Ensure a path has a leading slash (`metrics` → `/metrics`).
fn ensure_leading_slash(path: &str) -> String {
    if path.starts_with('/') {
        path.to_string()
    } else {
        format!("/{path}")
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
#[allow(clippy::too_many_arguments)]
pub fn spawn_scrape_scheduler(
    targets: ScrapeTargetRegistry,
    static_targets: Vec<ScrapeTarget>,
    pool: Arc<ClientPool>,
    interval: Duration,
    reconcile_interval: Duration,
    metrics_tx: mpsc::Sender<ScrapeResult>,
    mut shutdown: watch::Receiver<bool>,
    // Metric pipeline (config): injected on every target's series, and the
    // exposed-label rename map. Applied uniformly to static and discovered
    // targets here, so `build_scrape_target` / `build_static_targets` stay simple
    // and the synthesized `up` / `scrape_duration_seconds` inherit static labels.
    static_labels: Vec<LabelPair>,
    label_map: HashMap<String, String>,
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
                    let mut target = target.clone();
                    stamp_target(&mut target, &static_labels, &label_map);
                    // Pick (build) the client matching this target's TLS profile.
                    // A bad CA file fails here; skip + retry next reconcile.
                    let client = match pool.client_for(&target.tls) {
                        Ok(c) => c,
                        Err(e) => {
                            warn!(url = %url, error = %e, "cannot build scrape client; skipping target");
                            continue;
                        }
                    };
                    info!(url = %url, "scraping new target");
                    let h = spawn_scrape_task(
                        target,
                        client,
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

/// Apply the metric pipeline's static labels + relabel map onto one target.
/// Static labels override any same-key target label (operator config wins, e.g.
/// an explicit `metrics.static_labels.job` over `--scrape-default-job`) and are
/// deduped so the label vector never carries two pairs with the same key.
fn stamp_target(
    target: &mut ScrapeTarget,
    static_labels: &[LabelPair],
    label_map: &HashMap<String, String>,
) {
    if !static_labels.is_empty() {
        target
            .labels
            .retain(|l| !static_labels.iter().any(|s| s.key == l.key));
        target.labels.extend_from_slice(static_labels);
    }
    if !label_map.is_empty() {
        target.label_map = label_map.clone();
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::api::core::v1::PodStatus;
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;

    fn sel(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn mk_pod(
        name: &str,
        labels: &[(&str, &str)],
        annotations: &[(&str, &str)],
        ip: Option<&str>,
    ) -> Pod {
        Pod {
            metadata: ObjectMeta {
                name: Some(name.to_string()),
                namespace: Some("ns".to_string()),
                uid: Some(format!("uid-{name}")),
                labels: Some(sel(labels)),
                annotations: if annotations.is_empty() {
                    None
                } else {
                    Some(sel(annotations))
                },
                ..Default::default()
            },
            status: ip.map(|ip| PodStatus {
                pod_ip: Some(ip.to_string()),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn job(selector: &[(&str, &str)], port: u16, name: Option<&str>) -> PodScrapeJob {
        PodScrapeJob {
            job: name.map(String::from),
            selector: sel(selector),
            port,
            path: "/metrics".to_string(),
            scheme: "http".to_string(),
        }
    }

    #[test]
    fn pod_matches_is_and_over_pairs() {
        let labels = sel(&[("app", "node-exporter"), ("tier", "infra")]);
        assert!(pod_matches(&sel(&[("app", "node-exporter")]), &labels));
        assert!(pod_matches(
            &sel(&[("app", "node-exporter"), ("tier", "infra")]),
            &labels
        ));
        // one pair wrong → no match
        assert!(!pod_matches(
            &sel(&[("app", "node-exporter"), ("tier", "frontend")]),
            &labels
        ));
        // absent label → no match
        assert!(!pod_matches(&sel(&[("zone", "a")]), &labels));
        // empty selector matches anything
        assert!(pod_matches(&BTreeMap::new(), &labels));
    }

    #[test]
    fn selector_builds_target_without_annotation() {
        let pod = mk_pod("ne-xyz", &[("app", "node-exporter")], &[], Some("10.1.2.3"));
        let jobs = vec![job(
            &[("app", "node-exporter")],
            9100,
            Some("node-exporter"),
        )];
        let t = build_scrape_target(&pod, "node-a", &jobs).expect("target built");
        assert_eq!(t.url, "http://10.1.2.3:9100/metrics");
        let get = |k: &str| {
            t.labels
                .iter()
                .find(|l| l.key == k)
                .map(|l| l.value.as_str())
        };
        assert_eq!(get("job"), Some("node-exporter"));
        assert_eq!(get("instance"), Some("10.1.2.3:9100"));
        assert_eq!(get("namespace"), Some("ns"));
        assert_eq!(get("pod"), Some("ne-xyz"));
        assert_eq!(get("node"), Some("node-a"));
        assert_eq!(get("k8s_app"), Some("node-exporter"));
        // selector targets use the default (verify-on) TLS profile + no bearer.
        assert_eq!(t.tls, crate::scrape::TlsProfile::default());
        assert!(t.bearer.is_none());
    }

    #[test]
    fn annotation_takes_precedence_over_selector() {
        // Pod both matches the selector AND has scrape annotations; annotation
        // port/path win, job is derived (not the selector's job name).
        let pod = mk_pod(
            "dual",
            &[("app", "node-exporter")],
            &[
                ("prometheus.io/scrape", "true"),
                ("prometheus.io/port", "8080"),
                ("prometheus.io/path", "custom"),
            ],
            Some("10.0.0.9"),
        );
        let jobs = vec![job(&[("app", "node-exporter")], 9100, Some("selector-job"))];
        let t = build_scrape_target(&pod, "n", &jobs).unwrap();
        assert_eq!(t.url, "http://10.0.0.9:8080/custom"); // annotation port + leading-slash path
        let job_lbl = t.labels.iter().find(|l| l.key == "job").unwrap();
        assert_eq!(job_lbl.value, "node-exporter"); // derived from app label, not selector job
    }

    #[test]
    fn no_annotation_no_selector_match_is_none() {
        let pod = mk_pod("other", &[("app", "web")], &[], Some("10.0.0.1"));
        let jobs = vec![job(&[("app", "node-exporter")], 9100, None)];
        assert!(build_scrape_target(&pod, "n", &jobs).is_none());
    }

    #[test]
    fn selector_match_without_ip_is_none() {
        let pod = mk_pod("ne-noip", &[("app", "node-exporter")], &[], None);
        let jobs = vec![job(&[("app", "node-exporter")], 9100, None)];
        assert!(build_scrape_target(&pod, "n", &jobs).is_none());
    }

    #[test]
    fn selector_job_falls_back_to_derived_when_unset() {
        let pod = mk_pod(
            "ksm-1",
            &[("app", "kube-state-metrics")],
            &[],
            Some("10.2.0.4"),
        );
        let jobs = vec![job(&[("app", "kube-state-metrics")], 8080, None)];
        let t = build_scrape_target(&pod, "n", &jobs).unwrap();
        let job_lbl = t.labels.iter().find(|l| l.key == "job").unwrap();
        assert_eq!(job_lbl.value, "kube-state-metrics");
    }
}
