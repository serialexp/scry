<p align="center">
  <img src="logo.svg" alt="scry logo" width="160" height="160">
</p>

<h1 align="center">scry</h1>

<p align="center"><em>One observability backend. Four signals. One screen of config.</em></p>

`scry` is an opinionated, single-binary replacement for the Grafana
observability stack (Loki + Tempo + Mimir + Pyroscope). It stores
**metrics, logs, traces, and profiles** as immutable parquet blocks in
S3-compatible object storage, and serves queries from the same bucket.

## Why

Running the Grafana stack in production means standing up four services,
each with its own ring, distributor, ingester, querier, compactor, and
store-gateway, each with its own schema/index format, each with its own
retention story, and a per-tenant config matrix on top. The config
surface is enormous and most of it exists to serve scaling concerns that
do not apply to a homelab or a small team.

The observation underneath the four pillars is that they are the same
thing in different costumes:

> A timestamped record with a set of labels and a payload, stored in an
> immutable block in object storage, queried by `(time range, label
> predicate, payload predicate)`.

If you accept that, you get one storage engine, one block format, one
compactor, one retention loop, and four thin query frontends sharing all
of it. That's `scry`.

## What `scry` is

- **One binary for the server** (`scry-ingestd`). Ingest, query,
  compaction, and retention are subsystems of one process, not separate
  services.
- **One native wire protocol.** Producers ship batched, compressed
  records to the server over a single binschema-defined wire
  ([`proto/ingest.schema.json`](proto/ingest.schema.json)) — a flat
  tagged union, big-endian, 32 MiB frame cap. Everything that puts data
  in speaks this one protocol.
- **Two ways to feed it:**
  - the **agent** (`scry-agent`) — a per-node collector that tails CRI
    container logs **and scrapes Prometheus `/metrics` endpoints**
    (static `--scrape-target`, `prometheus.io/scrape` annotations, the
    node's kubelet/cadvisor, and label-selector pod discovery), shipping
    both over the native wire (pprof pull is still planned). A node-side
    **keep-only label allow-list** (`--keep`, opt-in) lets a busy node
    forward only the streams that match, dropping the rest before they
    hit the wire (D-043);
  - the **gateway** (`scry-gateway`) — a **fan-out hub**. It terminates
    *foreign push protocols* (**OTLP/HTTP traces**, **legacy Pyroscope
    `/ingest`**, **Prometheus remote-write**) over HTTP and, opt-in, the
    **native binschema wire** (so the agent can point at it too) — then
    forwards every accepted record, best-effort, to *all* configured
    downstream sinks at once: any of the scry ingest server, **Grafana
    Loki**, **OpenSearch** (both logs only), and/or **Mimir** (metrics only,
    via remote-write). Every sink is opt-in — a gateway that only tees logs
    to Loki/OpenSearch needs no scry server at all. All in → all out, no routing config (for
    anything more selective, run a second gateway). See [Point existing
    telemetry at scry](#point-existing-telemetry-at-scry).
- **Parquet on S3-compatible object storage** as the single source of
  truth. No separate index store, no Cassandra, no Bigtable, no boltdb.
- **WAL on local SSD** as the ingestion buffer and crash-safety
  mechanism. RAM cannot grow unboundedly under load.
- **DataFusion** as the query engine. We don't reinvent column pruning,
  predicate pushdown, or vectorised execution.
- **Multi-writer capable.** Writers never coordinate; the bucket layout
  makes collisions impossible by construction.

## What `scry` is not

- **Not a planetary-scale system.** No hash ring. No memberlist. No
  distributor/ingester/querier/store-gateway split. If you are running
  hundreds of TB/day, `scry` is not the answer; Mimir is.
- **Not multi-tenant.** One deployment, one tenant. Run more deployments
  if you need more tenants. (The gateway ignores `X-Scope-OrgID`.)
- **Not a drop-in for their query APIs.** The native query path is
  DataFusion SQL, surfaced through scry's own UI; PromQL is demoted (the
  own-UI direction removes the need for a Grafana-compat driver), and
  LogQL/TraceQL aren't implemented. The gateway translates a curated subset
  of foreign *push* protocols at the edge, but scry's storage, query, and
  native wire are its own — the reason the upstream protocols are messy is
  precisely the kind of accidental complexity we're escaping.
- **Not (yet) a Grafana drop-in.** scry now has its *own* query UI — a
  desktop app and a browser server (`scry-webui`) with per-signal views, a
  single-trace waterfall, a frames overview, and a logs reader — but
  **Grafana datasource adapters** (keep your existing dashboards) are a
  later milestone, as is flamegraph rendering for profiles.
- **Not configurable for the sake of being configurable.** Every knob
  added is a knob someone has to understand, document, and defend. The
  total config file should fit on one screen. If it grows past that
  without a *very* good reason, we've failed.

## Status

Pre-zero, but the storage + query spine is real. Architecture is settled
in [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md); decisions in
[`docs/decisions.md`](docs/decisions.md); the native wire protocol in
[`proto/ingest.schema.json`](proto/ingest.schema.json).

- **All four signals** — metrics, logs, traces, profiles — flow the whole
  way and query back: producer → native wire → per-writer WAL → parquet
  blocks on S3-compatible storage → SQLite catalog → DataFusion-backed
  query (local `scry-query` CLI or the `scry-queryd` daemon over a
  binschema-framed wire). Milestones v0.1–v0.12 are sealed;
  `scripts/smoke.sh` exercises the full ingest → store → query round-trip
  live for each signal, including a `--trace-id` by-id lookup for traces
  and a `--grep` ≡ `body LIKE` equivalence check for logs.
- **Metrics** and **logs** preselect via a per-block postings sidecar on
  AND'd label matchers. **Traces** and **profiles** carry no postings —
  matcher / time / trace-id filters push down as parquet row predicates
  and row-group statistics prune. Traces additionally support a
  trace-by-id lookup (`--trace-id`, sorted-column pruning) and promoted
  resource-column matchers (`service.name`, …); profiles are retrieval by
  time + label with the raw pprof blob streamed back untouched.
- **Full-text log search** (`--grep` / `body_contains`): a per-block
  byte-trigram **bloom skip sidecar**, built inline at block seal, lets a
  substring query skip whole blocks that can't contain the term (one-sided
  error — false positives cost a scan, never a missed match; the exact
  `contains` predicate is the backstop). ~1–3% storage overhead. See D-035.
- **The gateway** is a **fan-out hub**: it terminates OTLP/HTTP traces,
  legacy Pyroscope `/ingest`, and Prometheus remote-write over HTTP (plus
  the native binschema wire, opt-in), then forwards every accepted record
  best-effort to *all* configured downstream sinks — any of the scry
  server, Grafana Loki, OpenSearch (logs only), and/or **Mimir**
  (metrics only, remote-write — D-044). Every sink is opt-in (no scry
  server required if you only tee to Loki/OpenSearch). All in → all out,
  no routing config (D-041). An optional custom CA (`--ca-cert`) covers
  the HTTP sinks. The
  **OpenSearch sink self-manages** (D-042): `--opensearch-index` is a
  *prefix*, logs route per service to rolling data streams
  `<prefix>-<service>` (or `<prefix>-general`), and the sink creates and
  continuously re-asserts the ISM rollover policy (size+age, no
  auto-delete) and an index template with `flat_object` label mappings —
  so cluster-side drift can't silently break ingest. For Amazon OpenSearch
  Service / Serverless, `--opensearch-aws-sigv4` signs every request with
  AWS SigV4 (creds + region from the default AWS chain — never argv).
  `scripts/smoke-gateway.sh` drives the three HTTP protocols end to end
  against a Garage-backed server.
- **`scry-agent`** is a per-node collector (Alloy replacement): it tails
  Kubernetes pod logs and **scrapes Prometheus `/metrics` endpoints**,
  shipping both over one native-wire connection. Scrape targets come from
  static `--scrape-target` URLs, pods annotated `prometheus.io/scrape`, the
  node's own **kubelet/cadvisor** (HTTPS :10250, SA bearer, configurable TLS),
  and **label-selector pod SD** (`[[metrics.scrape_pods]]`); a **hand-rolled**
  exposition parser maps samples to series, each scrape synthesizes `up` +
  `scrape_duration_seconds`, and a node-side `--keep` allow-list filters both
  logs and metrics. Sealed by `scripts/smoke-agent-metrics.sh` +
  `scripts/smoke-agent-kubelet.sh` (D-043, D-045, D-048).
- **Lifecycle: compaction + retention + multi-instance.** Size-tiered
  compaction merges the K smallest blocks in a `(signal, date, level)`
  partition into the next level (DataFusion sort-merge, sidecars rebuilt;
  D-036) and per-signal retention reaps wholly-expired blocks (opt-in,
  dry-run by default; D-037). 1–N identical instances share one bucket,
  coordinated through **Valkey** — a lease elects a single compaction /
  retention winner (a *correctness* requirement; blocks are UUID- not
  content-addressed) and pub/sub + cursor polling + full-walk converge
  every catalog on the bucket (D-038/D-039). With Valkey absent a single
  instance stays correct. Sealed by `MULTI=1 scripts/smoke.sh`.
- **A query UI.** A SolidJS app runs both as a Tauri **desktop** binary
  (native socket to `scry-queryd`) and in the **browser** via `scry-webui`
  (a password-gated, SSRF-safe byte-pipe that can fan out to several
  `scry-queryd` upstreams, picked by id). It has per-signal views, a
  single-trace waterfall, a frames overview, and a logs reader (D-040,
  D-046). Sealed by `scripts/smoke-webui.sh`.

Still ahead: profiles **flamegraph aggregation** (pprof parse +
stack-merge — backend work for when a UI renders it; Grafana consumes
pre-aggregated data) and **Grafana datasource adapters** (v1.0). PromQL is
demoted now that scry has its own query UI. See the milestone table below.

## Workspace

```
crates/
  binschema-runtime/   vendored binschema Rust runtime (regenerated by scripts/gen-proto.sh)
  proto/               generated bindings + framing/fingerprint/build helpers (scry-proto)
  objstore/            S3-compatible object store wrapper + buffer pool (scry-objstore)
  wal/                 per-writer write-ahead log (scry-wal)
  block/               parquet block builder/reader, per-signal (scry-block)
  catalog/             SQLite block catalog + bucket reconcile (scry-catalog)
  server/              ingest server library: listener, handshake, pipeline (scry-server)
  query/               DataFusion query engine + scry-query CLI (scry-query)
  scry-queryd/         remote query daemon (binschema-framed wire)
  scry-list/           catalog inspector / bucket reconciler
  scry-webui/          browser query UI server: serves the SolidJS app + relays queries to one of N configured scry-queryd targets, selected by id (D-040, D-046)
  compact/             size-tiered compaction engine + scry-compact CLI (D-036)
  retention/           per-signal TTL retention engine + scry-retention CLI (D-037)
  valkey/              Valkey client: lease, block-event pub/sub, sink (scry-valkey, D-038)
  cluster/             multi-instance convergence + lease-guarded maintenance (scry-cluster, D-038/D-039)
  client/              reusable native-wire client, shared by agent + gateway (scry-client)
  agent/               per-node agent: tails CRI logs + scrapes Prometheus /metrics, ships over the wire (scry-agent)
  gateway/             fan-out hub (scry-gateway): native wire + OTLP/Pyroscope/remote-write in → scry + Loki + OpenSearch + Mimir out
  noise-spewer/        TCP client; emits random metrics/logs/traces/profiles
  scry-ingestd/        ingest server daemon binary (wraps scry-server; --mode full runs maintenance)
proto/                 binschema source-of-truth schemas
desktop/               Tauri + SolidJS query app (frontend bundle shared with scry-webui; not a workspace member)
deploy/k8s/            Kubernetes manifests: ingest server (StatefulSet+PVC), query daemon (Deployment), agent (DaemonSet)
Dockerfile             one image, many roles: scry-ingestd + scry-queryd + scry-agent + scry-gateway + scry-list (scry-webui is home-machine only)
scripts/gen-proto.sh        regenerate Rust bindings from proto/*.schema.json
scripts/smoke.sh            end-to-end ingest→store→query exit criterion (metrics/logs; MULTI=1 → two-instance)
scripts/smoke-gateway.sh    end-to-end push-gateway smoke (OTLP + Pyroscope + remote-write)
scripts/smoke-agent-metrics.sh  scry-agent Prometheus scrape → store → query smoke
scripts/smoke-agent-config.sh   scry-agent TOML pipeline (logs json + metric label_map) smoke
scripts/smoke-webui.sh      scry-webui browser surface (auth + multi-target relay)
scripts/dev-garage-up.sh    local single-node Garage (S3) for the smokes
scripts/dev-valkey-up.sh    local single-node Valkey for the multi-instance smoke
```

## Build and run locally

```bash
cargo build --release --workspace

# Ingest server (add --storage --wal-dir … --catalog … to persist; see below):
./target/release/scry-ingestd --listen 127.0.0.1:4000

# Feed it synthetic load over the native wire:
./target/release/noise-spewer --addr 127.0.0.1:4000 --rate 50 --duration 3s
```

You'll see the sink report something like
`batches=150 samples=15200 log_entries=2280 spans=740 profiles=37 rejected=0`.

To collect real telemetry, run the **agent** — it tails CRI container logs
*and* scrapes Prometheus `/metrics` endpoints, shipping both to the server
over one native-wire connection:

```bash
# Logs only: tail this node's container logs (pod-watch enriches labels).
# Local testing against a fixture tree: add --no-discovery --from-start.
./target/release/scry-agent --server-addr 127.0.0.1:4000

# Metrics: scrape one or more static targets (in addition to any pods
# annotated prometheus.io/scrape, when the pod watch is on):
./target/release/scry-agent --server-addr 127.0.0.1:4000 \
  --scrape-target http://127.0.0.1:9100/metrics \
  --scrape-target http://127.0.0.1:8080/metrics \
  --scrape-interval 15s --scrape-default-job node

# Metrics only, no log tailing or k8s — point at static targets:
./target/release/scry-agent --server-addr 127.0.0.1:4000 \
  --no-discovery --scrape-target http://127.0.0.1:9100/metrics
```

Each scraped series carries `__name__` + the target's
`job`/`instance`/`namespace`/`pod`/`node` labels, and every scrape
synthesizes `up` + `scrape_duration_seconds` so a down target is data, not
absence. The same `--keep` allow-list (see below) filters both logs and
metric series. Bearer auth for a scrape target: `--scrape-bearer
@/path/to/token` (the `@` reads from a file so the secret isn't on argv).

Processing-pipeline features are owned by a **TOML config file** (`--config`,
`SCRY_AGENT_CONFIG` env) rather than flags — flags own runtime
(connection/intervals/targets), the file owns label manipulation and field
extraction:

```bash
# agent.toml (usually a ConfigMap mount)
#   Config owns the pipeline; flags own runtime.
#
#   [logs]              per-signal keep, static_labels, label_map
#   [logs.json]         JSON body field extraction
#   [metrics]           per-signal keep, static_labels, label_map
#
# When --config is set, --keep must be empty (move it into the file).

cat > agent.toml <<'TOML'
[logs]
static_labels = { cluster = "gothab-prod" }

[logs.json]
labels = ["level", "app"]
metadata = ["request_id", "trace_id"]
message_field = "msg"

[metrics]
static_labels = { cluster = "gothab-prod" }
label_map = { container_name = "container" }
TOML

scry-agent --server-addr scry:4000 \
  --config agent.toml \
  --scrape-target http://127.0.0.1:9100/metrics
```

Six features (D-047): (1) per-signal `keep`, (2) `label_map` surfacing
(k8s_<key> → chosen name), (3) `static_labels` injection, (4) JSON body
fields → stream labels (→ postings, low cardinality), (5) JSON body fields
→ per-entry structured attributes (`Map<Utf8,Utf8>` column), (6) metric
label key rename + old-key suppression. `deny_unknown_fields` catches typos
loudly at startup. Sealed by `scripts/smoke-agent-config.sh`.

The `[metrics]` section also owns two more SD pieces for k8s parity (D-048):
**kubelet/cadvisor scraping** and **label-selector pod discovery**.

```bash
cat > agent.toml <<'TOML'
[metrics]
static_labels = { cluster = "gothab-prod" }

# Scrape this node's own kubelet over HTTPS (:10250). Needs the NODE_IP
# downward-API env + the nodes/metrics+nodes/proxy RBAC rule.
[metrics.kubelet]
enabled = true
# address  = "https://${NODE_IP}:10250"   # default; ${NODE_IP}/${NODE_NAME} interpolated
cadvisor = true   # /metrics/cadvisor → job=cadvisor (per-container metrics)
kubelet  = true   # /metrics          → job=kubelet  (kubelet self)
# bearer_file = "/var/run/secrets/kubernetes.io/serviceaccount/token"  # default; re-read per scrape
[metrics.kubelet.tls]
insecure_skip_verify = true   # default (kubelet cert rarely has a SAN for the IP)
# ca_file = "/etc/scry/kubelet-ca.pem"   # verify instead of skip-verify

# Node-local pod-label SD: scrape matching pods without prometheus.io/scrape
# annotations (no new RBAC — reuses the pod watch). Repeatable.
[[metrics.scrape_pods]]
job = "node-exporter"
selector = { "app.kubernetes.io/name" = "node-exporter" }
port = 9100
TOML

# In-cluster, wire NODE_IP from the downward API (fieldRef: status.hostIP):
scry-agent --server-addr scry:4000 --config agent.toml --node-ip "$NODE_IP"
```

Kubelet scraping uses a configurable TLS posture (default skip-verify, matching
Prometheus' stock cadvisor job) and a `bearer_file` re-read on every scrape so a
rotating ServiceAccount token is followed. It needs a new ClusterRole rule
(`nodes/metrics` + `nodes/proxy`, GET); pod-label SD needs none. See
`deploy/k8s/agent-rbac.yaml`, `deploy/k8s/agent-daemonset.yaml`, and
`deploy/k8s/agent-config.example.yaml`. Sealed by
`scripts/smoke-agent-kubelet.sh`.

To accept foreign push protocols, run the gateway alongside the server:

```bash
# Terminates OTLP/Pyroscope/remote-write on :4318, forwards to the server:
./target/release/scry-gateway --listen 0.0.0.0:4318 --upstream 127.0.0.1:4000

# Fan-out hub: also accept the native wire (so the agent can point here) and
# tee logs to Loki + OpenSearch alongside scry (all in → all out):
./target/release/scry-gateway \
  --listen 0.0.0.0:4318 --listen-wire 0.0.0.0:4000 \
  --upstream scry-server:4000 \
  --loki-url http://loki:3100 \
  # --opensearch-index is a PREFIX: logs route to per-service rolling data
  # streams <prefix>-<service> (or <prefix>-general); the sink creates and
  # keeps re-asserting the ISM rollover policy + index template itself.
  --opensearch-url http://opensearch:9200 --opensearch-index scry-logs

# Logs-only: no scry server at all, just tee to Loki + OpenSearch:
./target/release/scry-gateway \
  --listen 0.0.0.0:4318 \
  --loki-url http://loki:3100 --opensearch-url http://opensearch:9200

# Amazon OpenSearch Service (managed) / Serverless: sign every request (bulk
# writes AND the self-management calls) with AWS SigV4. Creds + region come
# from the default AWS chain (env / profile / EKS IRSA / IMDS) — never argv.
./target/release/scry-gateway \
  --listen 0.0.0.0:4318 \
  --opensearch-url https://search-foo.us-east-1.es.amazonaws.com \
  --opensearch-index scry-logs \
  --opensearch-aws-sigv4 --opensearch-aws-region us-east-1
  # --opensearch-aws-service defaults to `es`; use `aoss` for Serverless.

# Tee metrics to Mimir (remote-write out). --mimir-tenant sets X-Scope-OrgID
# for multi-tenant Mimir; --ca-cert adds a private CA for the HTTPS sinks:
./target/release/scry-gateway \
  --listen 0.0.0.0:4318 \
  --upstream scry-server:4000 \
  --mimir-url https://mimir:9009 --mimir-tenant team-a \
  --ca-cert /etc/scry/internal-ca.pem
```

Every sink is opt-in (`--upstream`, `--loki-url`, `--opensearch-url`,
`--mimir-url`); at least one must be configured. `--listen-wire` is opt-in too:
with no native listener bound, the gateway serves only the foreign HTTP
protocols. The scry sink connects lazily, so a down/absent scry server never
blocks startup. Loki/OpenSearch are logs-only; Mimir is metrics-only (remote-
write to `{url}/api/v1/push`); traces and profiles go to the scry sink alone.
`--ca-cert` (a PEM bundle) adds a custom CA on top of the built-in roots for
the Loki/OpenSearch/Mimir HTTPS clients. Delivery is best-effort and
independent per sink — a slow or down sink drops + counts without blocking
the inbound or the other sinks (D-041).

End-to-end smoke tests (require a local Garage — `scripts/dev-garage-up.sh`):

```bash
SIGNAL=metrics scripts/smoke.sh   # ingest → store → query round-trip, native wire
SIGNAL=both    scripts/smoke.sh   # metrics + logs through one sink
MULTI=1        scripts/smoke.sh   # two instances on one bucket (needs dev-valkey-up.sh)
scripts/smoke-gateway.sh          # OTLP + Pyroscope + remote-write through the gateway
scripts/smoke-agent-metrics.sh    # scry-agent Prometheus scrape → store → query
scripts/smoke-agent-config.sh     # scry-agent TOML pipeline (logs json + metric label_map)
scripts/smoke-webui.sh            # scry-webui browser surface (auth + multi-target relay)
```

## Deploy (Kubernetes)

One image, `serialexp/scry:latest` (multi-arch `linux/amd64` + `linux/arm64`),
carries every role; the manifest's `command:` selects which binary runs.

**Prerequisite:** an S3-compatible bucket (Cloudflare R2, Hetzner Object
Storage, Garage, MinIO, …). The server reads credentials from
`SCRY_OBJSTORE_*` env, supplied by a Secret.

```bash
kubectl apply -f deploy/k8s/namespace.yaml

# Fill in real bucket credentials, then apply out of band (never commit it):
cp deploy/k8s/objstore-secret.example.yaml deploy/k8s/objstore-secret.yaml
$EDITOR deploy/k8s/objstore-secret.yaml
kubectl apply -f deploy/k8s/objstore-secret.yaml

# Ingest server (StatefulSet + PVC for the WAL/catalog) and its Service:
kubectl apply -f deploy/k8s/server-statefulset.yaml
kubectl apply -f deploy/k8s/server-service.yaml

# Query daemon (Deployment + Service) — serves the binschema query wire
# to scry-query / scry-webui, with its own bucket-reconciled catalog:
kubectl apply -f deploy/k8s/queryd-deployment.yaml
kubectl apply -f deploy/k8s/queryd-service.yaml

# Per-node agent: tails container logs + scrapes Prometheus endpoints
# (DaemonSet + read-only pod-watch RBAC):
kubectl apply -f deploy/k8s/agent-rbac.yaml
kubectl apply -f deploy/k8s/agent-daemonset.yaml
```

The server runs with `--storage --wal-dir=/wal --catalog=/wal/catalog.sqlite`
on a `ReadWriteOnce` PVC, exposes the ingest wire on `:4000` and a live stats
dashboard on `:4098`, and is reachable in-cluster as `scry-server.scry.svc:4000`.
The catalog is rebuildable from the bucket at any time with `scry-list`, so the
PVC is a cache, not a system of record. The query daemon (`scry-queryd`) reads
the same bucket and answers on `:4100`. For **multi-instance** operation, set
`SCRY_VALKEY_URL` and run `scry-ingestd --mode full` so a Valkey lease elects a
single compaction/retention winner and catalogs converge via pub/sub (D-038/D-039).

### The gateway

The gateway runs from the same image (`command: [scry-gateway]`,
`--upstream=scry-server.scry.svc:4000`, listening on `:4318`). A packaged
manifest isn't in `deploy/k8s/` yet — a minimal one looks like:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata: { name: scry-gateway, namespace: scry, labels: { app: scry-gateway } }
spec:
  replicas: 1
  selector: { matchLabels: { app: scry-gateway } }
  template:
    metadata: { labels: { app: scry-gateway } }
    spec:
      containers:
        - name: scry-gateway
          image: serialexp/scry:latest
          command: [scry-gateway, --listen=0.0.0.0:4318, --upstream=scry-server.scry.svc:4000]
          ports: [{ name: http, containerPort: 4318 }]
---
apiVersion: v1
kind: Service
metadata: { name: scry-gateway, namespace: scry }
spec:
  selector: { app: scry-gateway }
  ports: [{ name: http, port: 4318, targetPort: 4318 }]
```

## Point existing telemetry at scry

Aim your existing exporters at the **gateway** (`:4318`). It decodes each
request into one batch and fans it out, best-effort, to every configured
sink (any of scry / Loki / OpenSearch / Mimir; Loki + OpenSearch are
logs-only, Mimir is metrics-only). The caller is ACKed once the batch is
**enqueued**, not once the downstreams confirm —
delivery is best-effort with no local spool, so durability across a
downstream outage is bounded by each sink's in-memory queue depth (D-041).

- **OTLP traces (HTTP/protobuf).** Point any OTLP/HTTP exporter at the
  gateway's `/v1/traces`:

  ```bash
  export OTEL_EXPORTER_OTLP_TRACES_PROTOCOL=http/protobuf
  export OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=http://scry-gateway.scry.svc:4318/v1/traces
  ```

  Traces only for now — there's no `/v1/metrics` or `/v1/logs` receiver
  (use remote-write for metrics; the agent for logs). gRPC OTLP and
  OTLP/JSON are out of scope.

- **Prometheus / VictoriaMetrics remote-write.** Add a `remote_write`
  target (v1 protobuf + snappy; `/api/v1/push` is accepted as a
  Mimir/Cortex alias):

  ```yaml
  remote_write:
    - url: http://scry-gateway.scry.svc:4318/api/v1/write
  ```

  Classic histograms/summaries map natively (their `_bucket`/`_sum`/
  `_count` series land as ordinary samples). Remote-write **v2**, native
  histograms, and exemplars are not handled yet.

- **Pyroscope profiles.** Point a legacy Pyroscope client (e.g.
  [`serialexp/pyroscope-bun`](https://github.com/serialexp/pyroscope-bun))
  at the gateway; it posts gzipped pprof to `/ingest`:

  ```
  server address: http://scry-gateway.scry.svc:4318
  ```

  Profiles are stored opaquely (pprof preserved verbatim); flamegraph
  aggregation is a later milestone.

- **The native wire (the agent, or anything that speaks binschema).** Run
  the gateway with `--listen-wire 0.0.0.0:4000` and point `scry-agent
  --server-addr <gateway>:4000` (or any native producer) at it. This makes
  the gateway a fan-out front-end for the native wire too — the same
  records then tee to scry + Loki + OpenSearch + Mimir.

- **Tee logs to Loki and/or OpenSearch.** Add `--loki-url
  http://loki:3100` and/or `--opensearch-url http://opensearch:9200`
  (`--opensearch-index`, default `scry-logs`). Every log batch that reaches
  the gateway — from any inbound — is mapped to the Loki push format
  (`/loki/api/v1/push`, one stream per scry `LogStream`, severity +
  attributes as structured metadata) and/or the OpenSearch `_bulk` NDJSON
  (one doc per entry, `@timestamp` + `body` + `severity` + labels) and
  shipped alongside the scry sink. Logs only; metrics/traces/profiles go to
  scry alone. For Amazon OpenSearch Service / Serverless, add
  `--opensearch-aws-sigv4 --opensearch-aws-region <region>` (and
  `--opensearch-aws-service aoss` for Serverless) to sign every request via
  AWS SigV4; credentials + region resolve from the default AWS chain (env,
  shared profile, EKS IRSA, EC2/ECS IMDS).

- **Tee metrics to Mimir (`--mimir-url`).** Every metric batch that reaches
  the gateway — from remote-write or the native wire — is re-emitted as
  Prometheus remote-write to `{url}/api/v1/push` (the inverse of the
  remote-write inbound). Add `--mimir-tenant <id>` to set `X-Scope-OrgID` for
  multi-tenant Mimir. Metrics only; logs/traces/profiles go to scry alone.

- **Trust a private CA (`--ca-cert`).** The Loki/OpenSearch/Mimir HTTPS
  clients trust only the system roots by default. Point `--ca-cert` at a PEM
  file (may be a bundle) to add a custom CA on top of the built-in roots for
  endpoints fronted by an internal CA.

- **Restrict what a busy node forwards (`scry-agent --keep` or `--config`).** By default
  the agent ships every container log stream it finds. A repeatable
  keep-only allow-list forwards only matching streams and drops the rest at
  the node, before they go on the wire:

  ```bash
  scry-agent --server-addr scry:4000 \
    --keep 'namespace=~"prod-.*"' \
    --keep k8s_app=api
  # ships only streams in a prod-* namespace AND with pod label app=api;
  # everything else is dropped on the node.
  ```

  When `--config` is used, per-signal keep moves into the TOML file
  (`[logs]` and `[metrics]` sections) and the global `--keep` flag must be
  empty (the agent bails loudly at startup if both are set).

  Matchers are `key=value` | `key!=value` | `key=~regex` | `key!~regex`
  (regex whole-string-anchored; values may be double-quoted), ANDed
  together, and match against the stream labels `namespace` / `pod` /
  `container` / `node` plus pod labels as `k8s_<key>`. Omit `--keep`
  entirely to ship everything (the default). The same allow-list also
  filters scraped metric series.

- **Scrape Prometheus metrics (`scry-agent`).** The agent doubles as an
  Alloy-replacement for the metrics path: it pulls Prometheus `/metrics`
  endpoints and ships them over the same wire as logs. Targets come from
  static `--scrape-target` URLs and/or Kubernetes pods annotated
  `prometheus.io/scrape: "true"` (honoring `prometheus.io/{port,path,scheme}`):

  ```bash
  scry-agent --server-addr scry:4000 \
    --scrape-target http://127.0.0.1:9100/metrics \
    --scrape-interval 15s
  # plus: any pod on this node with prometheus.io/scrape=true is
  # discovered and scraped automatically (pod watch must be enabled).
  ```

  Each series carries `__name__` + the target's `job`/`instance`/k8s
  labels (a colliding exposed label is renamed `exported_<key>`), and
  every scrape synthesizes `up` + `scrape_duration_seconds`. Auth is
  plain HTTP + optional `--scrape-bearer @/path/to/token`. The text
  parser is hand-rolled (no extra dependency); known gaps vs Alloy —
  relabeling, Service/Endpoints SD, per-target TLS/mTLS, native
  histograms, scrape-WAL durability — are deferred (run a real Alloy
  through `scry-gateway` if you need them).

## Scope (v0 → v1)

Reconciled against what actually shipped: the original plan put logs
first (v0.2) and metrics later (v0.5), but in practice metrics drove
the early work — postings + DataFusion are easier to validate against
a numeric workload — and logs landed as the second real signal in v0.4.
The push gateway then landed (unnumbered), carrying traces + profiles
*storage* in ahead of their query paths. The roadmap is a storage-then-
query split: v0.5/v0.6 below are the traces/profiles **query** verticals
that closed that gap (see D-034). Order updated accordingly.

| Milestone | Status | Deliverable |
|-----------|--------|-------------|
| **v0.1**  | ✅     | Storage layer: parquet block writer + WAL + S3 backend + catalog, with a dummy record type. No signals, no query. |
| **v0.2**  | ✅     | Metrics ingest + query: per-block postings sidecar, ingest-side WAL+pipeline, DataFusion-backed CLI querier with row-group pruning, postings cache. |
| **v0.3**  | ✅     | Query daemon (`scry-queryd`): binschema-framed remote query path (see D-031), shared between CLI and future tools. Streaming Arrow IPC batches with mid-stream resource errors. |
| **v0.4**  | ✅     | Logs as the second real signal: stream-label postings (same shape as metrics), per-entry attributes as a `Map<Utf8,Utf8>` column, CLI `--signal logs`, signal byte on the query wire. Body-substring search deferred to its own tantivy phase. |
| **gateway** | ✅   | Push-protocol front-end (`scry-gateway`): OTLP/HTTP traces, legacy Pyroscope `/ingest`, Prometheus remote-write → native wire. Traces + profiles storage paths land end to end. |
| **v0.5**  | ✅     | Traces query: `--trace-id` by-id lookup (sorted-column pruning) + promoted resource-column matchers (`service.name`, …) + `SELECT *` round-trip. Predicate pushdown, no postings. |
| **v0.6**  | ✅     | Profiles query: retrieval by time + label, raw pprof blob streamed back loss-free. Flamegraph aggregation deferred (Grafana renders pre-aggregated data — backend work for when a UI consumes it). |
| **v0.7**  | ✅     | Full-text log search: first-class `--grep` / `body_contains` substring search accelerated by a per-block byte-trigram **bloom skip sidecar** (built inline at seal; one-sided error, exact `contains` backstop). ~1–3% storage overhead, skips whole blocks that can't match. See D-035. (PromQL demoted — own UI removes the Grafana-compat driver.) |
| **v0.8**  | ✅     | Size-tiered **compaction** + per-signal **retention**, single-instance. Compaction: standalone `scry-compact` (`--once` / `--watch`) merges the K smallest blocks in a `(signal, date, level)` partition into one at the next level, DataFusion sort-merge, sidecars rebuilt (postings union, logs body bloom, metrics `series_types`); supersede→grace→delete with `superseded_by IS NULL` query-skip making grace=0 safe; `level` promoted into the sidecar (D-036). Retention: standalone `scry-retention` reaps blocks whose newest record is past a per-signal TTL — opt-in (no implicit deletion), **dry-run by default** (`--apply` to delete), whole-block `ts_max` criterion (D-037). |
| **v0.9**  | ✅     | **Multi-instance**: 1–N identical instances share one bucket via **Valkey**. A **Valkey lease** (`SET NX PX` + Lua compare-and-set renew/release — replacing D-013's `If-None-Match` lease, unbuildable on Garage) gives single-winner compaction/retention; single-winner is a *correctness* requirement because blocks are UUID- not content-addressed (D-038). Catalog **convergence** is three-tier: Valkey pub/sub `BlockEvent`s → cursor-driven incremental poll → periodic full-walk, all converging on the bucket as truth; 404-tolerant reads (`EvictOnNotFound` + one re-plan) heal a peer-deleted block at query time (D-039). Both engines run as background loops in `scry-ingestd --mode full`; `scry-queryd` converges query-only. Sealed by `MULTI=1 scripts/smoke.sh` (two instances: convergence + single-winner compaction + coordinated retention). With Valkey absent the system stays correct: convergence falls back to polling and maintenance pauses. |
| **v0.10** | ✅     | Gateway becomes a **fan-out hub** + the first own-UI step. Gateway: an opt-in native binschema listener (`--listen-wire`) joins the foreign HTTP inbounds, and every accepted record tees best-effort to *all* configured sinks — any of the scry server, **Grafana Loki**, and/or **OpenSearch** (the latter two logs-only); every sink opt-in, at least one required; all in → all out, no routing config; ACK-on-enqueue, independent per-sink bounded queues (drop + count on overflow). Metrics/traces/profiles go to scry alone (D-041). The OpenSearch sink **self-manages**: `--opensearch-index` is a prefix, logs route to per-service rolling data streams `<prefix>-<service>`, and the sink keeps re-asserting its ISM rollover policy (size+age, no auto-delete) + a `flat_object` index template so cluster-side drift can't silently break ingest (D-042). UI: a purpose-built **single-trace waterfall** in the query app (desktop + web), shown when a result has one distinct `trace_id`. The browser server (`scry-webui`) can be pointed at **several `scry-queryd` upstreams** (`--queryd id=host:port`, repeatable) and the UI switches between them by id — the browser never sends a raw address, so the relay stays SSRF-safe (D-046). |
| **v0.11** | ✅     | **Metrics shipping** — scry as an Alloy/Mimir replacement on the metrics path. `scry-agent` becomes a Prometheus scraper: a **hand-rolled** text-exposition parser (counter/gauge/histogram/summary/untyped, Go floats incl. NaN/±Inf, escaped labels, optional ms timestamps; malformed lines skipped + counted), targets from **static** `--scrape-target` URLs and/or **discovered** Kubernetes pods annotated `prometheus.io/scrape`, shipped over the *same* wire/connection as logs (Hello declares logs+metrics). Each series carries `__name__` + target labels (`job`/`instance`/`namespace`/`pod`/`node` + `k8s_<label>`; a colliding exposed label is renamed `exported_<key>`), and every scrape synthesizes `up` + `scrape_duration_seconds` so a down target is data, not absence. The node-side `--keep` allow-list applies to metric series too. Sealed by `scripts/smoke-agent-metrics.sh` (D-045). Gateway gains a **Mimir remote-write sink** (`--mimir-url`, metrics-only — the inverse of the remote-write inbound, symmetric snappy+protobuf encode, optional `X-Scope-OrgID`) and an optional **custom CA** (`--ca-cert`) added on top of the system roots for all HTTP sinks (D-044). (Earlier point release v0.10.1: node-side keep-only log filter (D-043) + OpenSearch AWS SigV4 signing.) |
| **v0.12** | ✅     | **Agent config pipeline + full k8s metrics SD.** A **TOML config file** (`--config`, `SCRY_AGENT_CONFIG`; usually a ConfigMap mount) owns the agent's processing pipeline while flags own runtime: per-signal `keep`, `label_map` surfacing (`k8s_<key>` → chosen name), `static_labels`, JSON body fields → stream labels (postings) and → per-entry attributes, and metric label rename — all backend-free (the store already holds arbitrary labels + an attributes map); `deny_unknown_fields` fails typos loudly (D-047). Metrics service discovery reaches Prometheus/Alloy parity: **kubelet/cadvisor scraping** (`[metrics.kubelet]` — HTTPS `:10250`, `/metrics/cadvisor` + `/metrics`, configurable TLS defaulting to skip-verify, a `bearer_file` re-read per scrape for SA-token rotation, address `${NODE_IP}`-interpolated from the downward API via `--node-ip`) and **label-selector pod SD** (`[[metrics.scrape_pods]]` — `matchLabels` AND, node-local, no new pod RBAC; annotation SD still wins). Per-target `TlsProfile` + `BearerSource` behind a `ClientPool` (one reqwest client per TLS profile). New RBAC `nodes/metrics`+`nodes/proxy`; DaemonSet wires `NODE_IP` + the ConfigMap. Sealed by `scripts/smoke-agent-config.sh` + `scripts/smoke-agent-kubelet.sh` (D-048). |
| later     | —      | Profiles flamegraph aggregation (pprof parse + stack-merge → flame-tree for a UI). |
| **v1.0**  | —      | Grafana datasource adapters (or our own minimal UI — TBD). |

## License

TBD.
