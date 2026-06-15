// Single source of marketing copy + structured data, sourced from the project
// README / ARCHITECTURE. Keeping it here keeps the section components lean.

export const REPO_URL = "https://github.com/serialexp/scry";
export const VERSION = "v0.12";

export type Feature = {
  icon: string; // inline SVG path data (24x24 viewBox)
  title: string;
  body: string;
  tag: string;
};

/** The four first-class signals. */
export const SIGNALS: Feature[] = [
  {
    tag: "metrics",
    title: "Metrics",
    body: "Prometheus remote-write and a native scraper. Series-dictionary compression, a per-block postings index for fast label preselection.",
    icon: "M3 3v18h18M7 15l3-4 3 3 4-6",
  },
  {
    tag: "logs",
    title: "Logs",
    body: "Structured log streams with a postings index and a byte-trigram bloom skip-index, so full-text grep prunes whole blocks before scanning.",
    icon: "M4 6h16M4 12h16M4 18h10",
  },
  {
    tag: "traces",
    title: "Traces",
    body: "OTLP spans with by-trace-id lookup and promoted-column matchers pushed down as parquet row predicates, pruned on row-group stats.",
    icon: "M6 4v6a4 4 0 0 0 4 4h4M6 14v6M14 14a4 4 0 0 1 4 4v2M18 4v6",
  },
  {
    tag: "profiles",
    title: "Profiles",
    body: "Pyroscope pprof profiles stored as immutable blobs, retrieved by time and labels — ready to render in your flamegraph UI of choice.",
    icon: "M4 20V10M9 20V4M14 20v-8M19 20v-5",
  },
];

export type Protocol = {
  name: string;
  detail: string;
  dir: "in" | "out";
};

/** What speaks to scry, and what scry can tee to. */
export const PROTOCOLS_IN: Protocol[] = [
  { name: "OTLP/HTTP", detail: "Traces over the OpenTelemetry protocol", dir: "in" },
  { name: "Prometheus remote-write", detail: "Metrics push, v1 snappy+protobuf", dir: "in" },
  { name: "Pyroscope", detail: "Continuous profiling ingest", dir: "in" },
  { name: "Native wire", detail: "binschema-framed TCP, the fast path", dir: "in" },
];

export const PROTOCOLS_OUT: Protocol[] = [
  { name: "scry", detail: "The native parquet store", dir: "out" },
  { name: "Grafana Loki", detail: "Tee logs to an existing Loki", dir: "out" },
  { name: "OpenSearch", detail: "Self-managing per-service data streams", dir: "out" },
  { name: "Mimir", detail: "Tee metrics back to remote-write", dir: "out" },
];

export type GetStartedTab = {
  id: string;
  label: string;
  note: string;
  code: string;
};

/** Three ways to run the same one binary in its two roles over a bucket. */
export const GET_STARTED: GetStartedTab[] = [
  {
    id: "bare-metal",
    label: "Bare metal",
    note: "Drop the single scry binary on a host and run the role you need; scale by adding hosts.",
    code: `# 1 · point scry at any S3-compatible bucket
export SCRY_OBJSTORE_BUCKET=obs
export SCRY_OBJSTORE_ENDPOINT=https://s3.example.com
export SCRY_OBJSTORE_ACCESS_KEY_ID=…
export SCRY_OBJSTORE_SECRET_ACCESS_KEY=…

# 2 · ingest + compaction/retention node
scry ingest --listen :4000 --mode full \\
  --wal-dir /var/lib/scry/wal --catalog /var/lib/scry/scry.db \\
  --valkey-url redis://valkey:6379

# 3 · query node over the same bucket
scry query --listen :4100 --catalog /var/lib/scry/query.db \\
  --valkey-url redis://valkey:6379`,
  },
  {
    id: "docker-compose",
    label: "Docker Compose",
    note: "One image, two services, a Valkey for coordination.",
    code: `# docker-compose.yml
services:
  valkey:
    image: valkey/valkey:8-alpine

  ingest:
    image: ghcr.io/serialexp/scry:latest
    command: >
      scry ingest --listen :4000 --mode full
      --wal-dir /wal --catalog /data/scry.db
      --valkey-url redis://valkey:6379
    env_file: scry.env
    ports: ["4000:4000"]
    volumes: ["wal:/wal", "data:/data"]

  query:
    image: ghcr.io/serialexp/scry:latest
    command: >
      scry query --listen :4100 --catalog /data/query.db
      --valkey-url redis://valkey:6379
    env_file: scry.env
    ports: ["4100:4100"]
    volumes: ["data:/data"]

volumes: { wal: {}, data: {} }`,
  },
  {
    id: "kubernetes",
    label: "Kubernetes",
    note: "Ingest + query Deployments and a Valkey. Scale each role on its own.",
    code: `# bucket credentials, shared by every scry pod
kubectl create secret generic scry-objstore \\
  --from-literal=SCRY_OBJSTORE_BUCKET=obs \\
  --from-literal=SCRY_OBJSTORE_ENDPOINT=https://s3.example.com \\
  --from-literal=SCRY_OBJSTORE_ACCESS_KEY_ID=… \\
  --from-literal=SCRY_OBJSTORE_SECRET_ACCESS_KEY=…

# ingest + query Deployments, Valkey, and Services
kubectl apply -k github.com/serialexp/scry/deploy/k8s

# queries spiking? scale just the query role
kubectl scale deploy/scry-query --replicas=3`,
  },
];

