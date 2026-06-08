# CURRENT TASK — scry-agent kubelet/cadvisor scraping + label-selector pod SD (D-048)

## What this is

Two deferred metrics-SD pieces for scry-agent k8s parity with Prometheus/Alloy,
both agent-side, reusing the existing scrape→wire pipeline:

1. **kubelet/cadvisor scraping** — HTTPS :10250 with a ServiceAccount bearer
   token (re-read per scrape for rotation) + configurable TLS (default
   skip-verify). Endpoints `/metrics/cadvisor` (job=cadvisor) + `/metrics`
   (job=kubelet), both on by default, individually togglable.
2. **label-selector pod SD** — scrape pods whose labels match a `selector`
   (`matchLabels` AND), node-local, reusing the existing pod watch (no new pod
   RBAC). Annotation SD still takes precedence.

Locked decisions: config owns the pipeline (`[metrics.kubelet]` + repeatable
`[[metrics.scrape_pods]]`), flags own runtime (`--node-ip`/`NODE_IP` from the
downward API interpolates `${NODE_IP}` in the kubelet address). Kubelet
scraping needs new RBAC `nodes/metrics` + `nodes/proxy`. Plan:
`/home/bart/.claude/plans/buzzing-watching-muffin.md`.

## Status: COMPLETE (uncommitted)

All implementation, manifests, smoke, gates, and docs are done. Nothing is
committed yet — Bart has not asked to commit.

### Done
- **config.rs**: `KubeletSection`/`TlsSection`/`PodScrapeJobSection` file
  structs (`deny_unknown_fields`); runtime `KubeletConfig`/`PodScrapeJob` on
  `MetricPipeline`; `compile()` materialization; 5 new tests.
- **scrape.rs**: `BearerSource` (`Literal | File`, file re-read in `fetch`),
  `TlsProfile` (`Clone+Default+Eq+Hash`), `ClientPool` (one reqwest client per
  TLS profile); `scrape_to_series`/`relabel_scrape` frozen; 4 new tests.
- **discovery.rs**: `pod_matches` (AND), selector path in `build_scrape_target`
  (annotation first), `assemble_pod_target`; `spawn_pod_watcher`/`apply_event`
  thread `scrape_pods`; `spawn_scrape_scheduler` takes a `ClientPool`; 6 tests.
- **main.rs**: `--node-ip`/`NODE_IP` flag; `build_kubelet_targets` +
  `resolve_kubelet_address`; `resolve_bearer` → `BearerSource`;
  `build_static_targets` adds `tls`; `metrics_enabled` includes kubelet/scrape_pods;
  `ClientPool` wired; warn if `--no-discovery` + scrape_pods; 9 tests.
- **deploy/k8s/agent-rbac.yaml**: `nodes/metrics` + `nodes/proxy` GET rule.
- **deploy/k8s/agent-daemonset.yaml**: `NODE_IP` env (`status.hostIP`),
  `--config /etc/scry/agent.toml` arg, ConfigMap volume mount.
- **deploy/k8s/agent-config.example.yaml** (NEW): sample ConfigMap.
- **scripts/smoke-agent-kubelet.sh** (NEW): self-signed HTTPS + bearer-gated
  stub → kubelet config → agent → ingestd → bucket → query; 8 assertions.
- **Docs**: CLAUDE.md (agent paragraph + tooling entry), README.md (config
  example + summary lines), docs/decisions.md (D-048).

### Gates (all green)
- `cargo build --workspace` ✓
- `cargo test --workspace` ✓ (scry-agent: 68 tests)
- `cargo clippy -p scry-agent --all-targets` ✓ (only pre-existing
  binschema-runtime + D-047 stream.rs test-helper lints; none in new code)
- `cargo fmt -p scry-agent -- --check` ✓
- `scripts/smoke-agent-kubelet.sh` → PASS (6 rows, both endpoints, both auth'd)
- `scripts/smoke-agent-metrics.sh` → PASS (no regression)
- `scripts/smoke-agent-config.sh` → PASS (no regression)

## Constraints to keep in mind (from CLAUDE.md)
- Git allowlist: commit/checkout-branch/push/add/read-only only without asking;
  no partial-file commits / `git add -p`. Commit only when Bart asks.
- Don't use the Monitor tool. Don't proclaim success — let Bart judge.
- Commit co-author trailer: `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`.

## Note: prior uncommitted work
D-047 (TOML config pipeline) was completed but is also **uncommitted** — its
file changes are in the same working tree as D-048. Check `git status` /
`git diff` before any commit-splitting decision; if a split needs partial
staging, STOP and ask (Rule #13).
