import { For, Show } from "solid-js";

import { reveal } from "../lib/reveal";
import styles from "./Problem.module.css";

type Accent = "store" | "coord" | "viz";

type Tier =
  | { label: string; nodes: string[]; accent?: Accent }
  | { label: string; columns: { title: string; nodes: string[] }[] };

type Topology = {
  title: string;
  pill: string;
  tone: "bad" | "good";
  tiers: Tier[];
};

// The usual Grafana path: four independent databases, each exploded into a
// fleet of stateless + stateful microservices, all sharing a gossip hash ring.
const GRAFANA: Topology = {
  title: "Distributed Grafana stack",
  pill: "4 databases",
  tone: "bad",
  tiers: [
    { label: "collect", nodes: ["Grafana Alloy", "Promtail", "OTel Collector"] },
    { label: "route", nodes: ["gateway / LB per stack"] },
    {
      label: "four separate clusters",
      columns: [
        {
          title: "Loki",
          nodes: ["distributor", "ingester", "querier", "query-frontend", "compactor", "index-gateway"],
        },
        {
          title: "Tempo",
          nodes: ["distributor", "ingester", "querier", "query-frontend", "compactor", "metrics-gen"],
        },
        {
          title: "Mimir",
          nodes: ["distributor", "ingester", "querier", "store-gateway", "compactor", "ruler"],
        },
        {
          title: "Pyroscope",
          nodes: ["distributor", "ingester", "querier", "query-frontend", "compactor"],
        },
      ],
    },
    { label: "coordinate", accent: "coord", nodes: ["hash ring · memberlist gossip", "memcached"] },
    { label: "store", accent: "store", nodes: ["object storage — a bucket per stack"] },
    { label: "visualize", accent: "viz", nodes: ["Grafana"] },
  ],
};

// scry: the same four signals, one binary run in two roles over one bucket,
// coordinated by a single small Valkey.
const SCRY: Topology = {
  title: "Distributed scry stack",
  pill: "1 binary",
  tone: "good",
  tiers: [
    { label: "collect", nodes: ["OTLP", "remote-write", "Pyroscope", "native wire", "scry agent"] },
    { label: "route", nodes: ["scry gateway — fan-out (optional)"] },
    { label: "two roles, scale each", nodes: ["scry ingest ×N", "scry query ×N"] },
    { label: "coordinate", accent: "coord", nodes: ["Valkey — lease + pub/sub"] },
    { label: "store", accent: "store", nodes: ["one S3 bucket"] },
    { label: "explore", accent: "viz", nodes: ["scry web", "desktop app", "scry get (cli)"] },
  ],
};

function nodeClass(tone: "bad" | "good", accent?: Accent): string {
  if (accent === "store") return `${styles.node} ${styles.nodeStore}`;
  if (accent === "coord") return `${styles.node} ${styles.nodeCoord}`;
  if (accent === "viz") return `${styles.node} ${styles.nodeViz}`;
  return `${styles.node} ${tone === "bad" ? styles.nodeBad : styles.nodeGood}`;
}

function TierRow(props: { tier: Tier; tone: "bad" | "good" }) {
  const t = props.tier;
  return (
    <div class={styles.tier}>
      <span class={styles.tierLabel}>{t.label}</span>
      <Show
        when={"columns" in t ? t.columns : false}
        fallback={
          <div class={styles.nodes}>
            <For each={"nodes" in t ? t.nodes : []}>
              {(n) => (
                <span class={nodeClass(props.tone, "accent" in t ? t.accent : undefined)}>{n}</span>
              )}
            </For>
          </div>
        }
      >
        {(columns) => (
          <div class={styles.dbGrid}>
            <For each={columns()}>
              {(col) => (
                <div class={styles.dbCol}>
                  <span class={styles.dbTitle}>{col.title}</span>
                  <For each={col.nodes}>
                    {(n) => <span class={`${styles.node} ${styles.nodeBad}`}>{n}</span>}
                  </For>
                </div>
              )}
            </For>
          </div>
        )}
      </Show>
    </div>
  );
}

function Diagram(props: { topo: Topology; delay: number }) {
  const t = props.topo;
  return (
    <div
      class={`${styles.diagram} ${t.tone === "bad" ? styles.before : styles.after}`}
      ref={(el) => reveal(el, props.delay)}
    >
      <header class={styles.cardHead}>
        <span class={styles.label}>{t.title}</span>
        <span class={`${styles.pill} ${t.tone === "bad" ? styles.pillBad : styles.pillGood}`}>
          {t.pill}
        </span>
      </header>
      <div class={styles.flow}>
        <For each={t.tiers}>{(tier) => <TierRow tier={tier} tone={t.tone} />}</For>
      </div>
    </div>
  );
}

export function Problem() {
  return (
    <section class="section" id="why">
      <div class="container">
        <div class={styles.head}>
          <span class="eyebrow" ref={(el) => reveal(el, 0)}>
            Why scry
          </span>
          <h2 class="section-title" ref={(el) => reveal(el, 60)}>
            The observability stack, <span class="grad-text">collapsed into one binary</span>.
          </h2>
          <p class="section-lead" ref={(el) => reveal(el, 120)}>
            The usual path is four databases, each exploded into its own fleet of microservices
            around a gossip hash ring. scry runs the same four signals from one binary — ingesters
            and queriers over a shared bucket, with a small Valkey to keep them in sync.
          </p>
        </div>

        <div class={styles.diagrams}>
          <Diagram topo={GRAFANA} delay={0} />
          <Diagram topo={SCRY} delay={120} />
        </div>
      </div>
    </section>
  );
}
