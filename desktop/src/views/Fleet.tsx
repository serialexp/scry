//! Valkey-aggregated fleet status, fetched through the selected queryd over the
//! normal query protocol. The web server remains a byte-pipe and Tauri gets the
//! same feature through its native transport.

import { For, Match, Show, Switch, createMemo, onCleanup, onMount, type Component } from "solid-js";
import {
  fleetError,
  fleetInstances,
  fleetStatus,
  fleetUpdatedAt,
  refreshFleet,
} from "../store";
import type { FleetInstance } from "../protocol/client";

const POLL_MS = 5_000;

function formatBytes(kib: number | null): string {
  if (kib === null) return "—";
  if (kib < 1024) return `${kib.toFixed(0)} KiB`;
  const mib = kib / 1024;
  return mib < 1024 ? `${mib.toFixed(1)} MiB` : `${(mib / 1024).toFixed(2)} GiB`;
}

function formatDuration(seconds: number): string {
  if (seconds < 60) return `${Math.floor(seconds)}s`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m`;
  if (seconds < 86_400) return `${Math.floor(seconds / 3600)}h ${Math.floor((seconds % 3600) / 60)}m`;
  return `${Math.floor(seconds / 86_400)}d ${Math.floor((seconds % 86_400) / 3600)}h`;
}

function displayValue(value: unknown): string | null {
  if (typeof value === "boolean") return value ? "yes" : "no";
  if (typeof value === "number") return value.toLocaleString();
  if (typeof value === "string") return value;
  return null;
}

const FleetCard: Component<{ instance: FleetInstance }> = (props) => {
  const fields = createMemo(() =>
    Object.entries(props.instance.data)
      .map(([key, value]) => [key, displayValue(value)] as const)
      .filter((entry): entry is readonly [string, string] => entry[1] !== null)
      .slice(0, 8),
  );
  const age = createMemo(() => Math.max(0, Date.now() - props.instance.now_unix_ms));

  return (
    <article class="fleet-card">
      <header class="fleet-card-header">
        <div>
          <span class={`fleet-role fleet-role-${props.instance.role}`}>{props.instance.role}</span>
          <h2>{props.instance.instance_id}</h2>
        </div>
        <span classList={{ "fleet-health": true, stale: age() > 15_000 }}>
          {age() > 15_000 ? "stale" : "live"}
        </span>
      </header>
      <div class="fleet-address">{props.instance.addr || "No advertised address"}</div>
      <dl class="fleet-basics">
        <div><dt>Uptime</dt><dd>{formatDuration(props.instance.uptime_secs)}</dd></div>
        <div><dt>RSS</dt><dd>{formatBytes(props.instance.rss_kib)}</dd></div>
        <div><dt>Report age</dt><dd>{Math.floor(age() / 1000)}s</dd></div>
      </dl>
      <Show when={fields().length > 0}>
        <dl class="fleet-data">
          <For each={fields()}>{([key, value]) => (
            <div><dt>{key.replaceAll("_", " ")}</dt><dd>{value}</dd></div>
          )}</For>
        </dl>
      </Show>
    </article>
  );
};

const Fleet: Component = () => {
  onMount(() => {
    void refreshFleet();
    const timer = window.setInterval(() => void refreshFleet(), POLL_MS);
    onCleanup(() => window.clearInterval(timer));
  });

  const grouped = createMemo(() => {
    const groups = new Map<string, FleetInstance[]>();
    for (const instance of fleetInstances()) {
      const entries = groups.get(instance.role) ?? [];
      entries.push(instance);
      groups.set(instance.role, entries);
    }
    return [...groups.entries()];
  });

  return (
    <main class="fleet-view">
      <header class="fleet-toolbar">
        <div>
          <h1>Fleet</h1>
          <p>Live status from every Scry process registered in Valkey.</p>
        </div>
        <div class="fleet-toolbar-actions">
          <Show when={fleetUpdatedAt()}>
            {(updated) => <span>Updated {new Date(updated()).toLocaleTimeString()}</span>}
          </Show>
          <button type="button" onClick={() => void refreshFleet()} disabled={fleetStatus() === "loading"}>
            {fleetStatus() === "loading" ? "Refreshing…" : "Refresh"}
          </button>
        </div>
      </header>

      <Show when={fleetError()}>
        {(error) => <div class="fleet-error">{error()}</div>}
      </Show>

      <Switch>
        <Match when={fleetStatus() === "loading" && fleetInstances().length === 0}>
          <div class="fleet-empty">Loading fleet status…</div>
        </Match>
        <Match when={fleetStatus() === "error" && fleetInstances().length === 0}>
          <div class="fleet-empty">Fleet status is unavailable. Select a Valkey-connected queryd and retry.</div>
        </Match>
        <Match when={fleetInstances().length === 0}>
          <div class="fleet-empty">No live Scry instances are registered.</div>
        </Match>
        <Match when={true}>
          <div class="fleet-groups">
            <For each={grouped()}>{([role, instances]) => (
              <section class="fleet-group">
                <h2>{role} <span>{instances.length}</span></h2>
                <div class="fleet-grid">
                  <For each={instances}>{(instance) => <FleetCard instance={instance} />}</For>
                </div>
              </section>
            )}</For>
          </div>
        </Match>
      </Switch>
    </main>
  );
};

export default Fleet;
