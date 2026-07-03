//! The query form: connection, signal, matchers, time bounds, SQL,
//! limit, and (for traces) a by-id lookup. Reads/writes the shared store
//! directly — no props.

import { For, Show, createEffect, onMount, type Component } from "solid-js";

import { SIGNAL_NAMES } from "../protocol/constants";
import { isTauri } from "../env";
import {
  state,
  setField,
  addMatcher,
  removeMatcher,
  setMatcher,
  runCurrentQuery,
  runFramesOverview,
  targets,
  labelNames,
  labelValues,
  refreshLabels,
  ensureLabelValues,
  runLogVolume,
} from "../store";
import { snapQuickRangeNs } from "../volume";

const SIGNAL_HINTS: Record<string, string> = {
  Metrics: "Matchers preselect series via postings. SQL runs against the metrics table.",
  Logs: "Matchers preselect streams via postings. SQL runs against the logs table.",
  Traces: "Matchers map to promoted columns (service.name, service.namespace, deployment.environment). Use --trace-id for by-id lookup; other attributes via SQL.",
  Profiles: "Retrieval only: filter by time + SQL against the labels map. Label matchers are rejected — use SQL.",
};

/** Quick time-range presets: label → span in milliseconds. */
const QUICK_RANGES: { label: string; ms: number }[] = [
  { label: "5m", ms: 5 * 60_000 },
  { label: "15m", ms: 15 * 60_000 },
  { label: "1h", ms: 60 * 60_000 },
  { label: "6h", ms: 6 * 60 * 60_000 },
  { label: "24h", ms: 24 * 60 * 60_000 },
  { label: "7d", ms: 7 * 24 * 60 * 60_000 },
];

/** Set ts_min/ts_max to [now - span, now] in unix nanoseconds, snapping the
 *  upper bound down to the range's bucket step. Snapping keeps repeated
 *  refreshes within a bucket on identical bounds, so the queryd result cache
 *  hits instead of re-scanning an ever-shifting "now". */
function applyQuickRange(ms: number): void {
  const { tsMinNs, tsMaxNs } = snapQuickRangeNs(Date.now(), ms);
  setField("tsMin", String(tsMinNs));
  setField("tsMax", String(tsMaxNs));
  void refreshLabels();
  // Refresh the volume histogram to the new range (logs-only; a no-op else).
  if (state.signal === "Logs") void runLogVolume();
}

const QueryForm: Component = () => {
  const isRunning = () => state.status === "running";

  // Warm the label cache whenever the scope changes: signal (tracked here),
  // and — for the browser — the selected target once it's seeded after login.
  // `refreshLabels` no-ops when the scope key is unchanged, so this is cheap.
  createEffect(() => {
    // Touch the reactive deps so the effect re-runs when they change.
    void state.signal;
    void state.target;
    void refreshLabels();
  });
  onMount(() => void refreshLabels());

  return (
    <form
      class="query-form"
      onSubmit={(e) => {
        e.preventDefault();
        if (!isRunning()) void runCurrentQuery();
      }}
    >
      {/* The daemon address only matters for the desktop shell, which dials
          it directly. In the browser, scry-webui relays to its own configured
          upstream, so the field is hidden. */}
      <Show when={isTauri()}>
        <div class="field">
          <label for="addr">Daemon address</label>
          <input
            id="addr"
            type="text"
            value={state.addr}
            spellcheck={false}
            onInput={(e) => setField("addr", e.currentTarget.value)}
            placeholder="127.0.0.1:4100"
          />
        </div>
      </Show>

      {/* In the browser, scry-webui dials one of its configured upstreams; the
          user picks which by id (never a raw address — SSRF-safe). Always shown
          so it's clear which daemon answers, even with a single target. */}
      <Show when={!isTauri()}>
        <div class="field">
          <label for="target">Query target</label>
          <select
            id="target"
            value={state.target}
            onChange={(e) => setField("target", e.currentTarget.value)}
          >
            <For each={targets()}>
              {(t) => <option value={t.id}>{t.label}</option>}
            </For>
          </select>
        </div>
      </Show>

      <div class="field">
        <label for="signal">Signal</label>
        <select
          id="signal"
          value={state.signal}
          onChange={(e) => setField("signal", e.currentTarget.value as typeof state.signal)}
        >
          <For each={SIGNAL_NAMES}>{(s) => <option value={s}>{s}</option>}</For>
        </select>
        <p class="hint">{SIGNAL_HINTS[state.signal]}</p>
      </div>

      <div class="field">
        <div class="field-head">
          <label>Matchers (AND)</label>
          <button type="button" class="link" onClick={addMatcher}>
            + add
          </button>
        </div>
        {/* Shared list of matchable label names for the current signal +
            time window (see D-050). Each matcher name input autocompletes
            against it; the value input against that name's known values. */}
        <datalist id="label-names">
          <For each={labelNames()}>{(n) => <option value={n} />}</For>
        </datalist>
        <For each={state.matchers}>
          {(m, i) => (
            <div class="matcher-row">
              <input
                type="text"
                class="matcher-name"
                placeholder="name"
                spellcheck={false}
                list="label-names"
                value={m.name}
                onInput={(e) => {
                  const v = e.currentTarget.value;
                  setMatcher(i(), "name", v);
                  void ensureLabelValues(v);
                }}
                onFocus={() => void refreshLabels()}
              />
              <span class="eq">=</span>
              <input
                type="text"
                class="matcher-value"
                placeholder="value"
                spellcheck={false}
                list={`label-values-${i()}`}
                value={m.value}
                onInput={(e) => setMatcher(i(), "value", e.currentTarget.value)}
                onFocus={() => void ensureLabelValues(m.name)}
              />
              <datalist id={`label-values-${i()}`}>
                <For each={labelValues()[m.name.trim()] ?? []}>
                  {(v) => <option value={v} />}
                </For>
              </datalist>
              <button
                type="button"
                class="icon-btn"
                title="remove"
                onClick={() => removeMatcher(i())}
              >
                ×
              </button>
            </div>
          )}
        </For>
      </div>

      <Show when={state.signal === "Traces"}>
        <div class="field">
          <label for="trace-id">Trace id (hex, 16 bytes)</label>
          <input
            id="trace-id"
            type="text"
            spellcheck={false}
            value={state.traceId}
            onInput={(e) => setField("traceId", e.currentTarget.value)}
            placeholder="e.g. aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
          />
        </div>
      </Show>

      <div class="field">
        <div class="field-head">
          <label>Quick range</label>
          <button
            type="button"
            class="link"
            onClick={() => {
              setField("tsMin", "");
              setField("tsMax", "");
            }}
          >
            clear
          </button>
        </div>
        <div class="quick-ranges">
          <For each={QUICK_RANGES}>
            {(r) => (
              <button
                type="button"
                class="chip"
                onClick={() => applyQuickRange(r.ms)}
              >
                {r.label}
              </button>
            )}
          </For>
        </div>
        <p class="hint">
          Sets ts_min/ts_max to the last N relative to your browser clock (unix ns).
        </p>
      </div>

      <div class="field-grid">
        <div class="field">
          <label for="ts-min">ts_min (unix ns)</label>
          <input
            id="ts-min"
            type="text"
            inputmode="numeric"
            spellcheck={false}
            value={state.tsMin}
            onInput={(e) => setField("tsMin", e.currentTarget.value)}
            onChange={() => void refreshLabels()}
            placeholder="(none)"
          />
        </div>
        <div class="field">
          <label for="ts-max">ts_max (unix ns)</label>
          <input
            id="ts-max"
            type="text"
            inputmode="numeric"
            spellcheck={false}
            value={state.tsMax}
            onInput={(e) => setField("tsMax", e.currentTarget.value)}
            onChange={() => void refreshLabels()}
            placeholder="(none)"
          />
        </div>
        <div class="field">
          <label for="limit">limit</label>
          <input
            id="limit"
            type="text"
            inputmode="numeric"
            spellcheck={false}
            value={state.limit}
            disabled={state.sql.trim() !== ""}
            onInput={(e) => setField("limit", e.currentTarget.value)}
            placeholder="(no limit)"
          />
        </div>
      </div>

      <div class="field">
        <label for="sql">SQL (optional — overrides the default SELECT *)</label>
        <textarea
          id="sql"
          rows={3}
          spellcheck={false}
          value={state.sql}
          onInput={(e) => setField("sql", e.currentTarget.value)}
          placeholder="SELECT * FROM metrics ORDER BY ts_unix_nano DESC"
        />
      </div>

      <div class="actions">
        <button type="submit" class="run" disabled={isRunning()}>
          {isRunning() ? "Running…" : "Run query"}
        </button>
        <Show when={state.signal === "Traces"}>
          <button
            type="button"
            class="run secondary"
            disabled={isRunning()}
            title="Aggregate one row per trace (frame): duration + span count, slowest first. Click a frame to open its waterfall."
            onClick={() => {
              if (!isRunning()) void runFramesOverview();
            }}
          >
            Frames overview
          </button>
        </Show>
      </div>
    </form>
  );
};

export default QueryForm;
