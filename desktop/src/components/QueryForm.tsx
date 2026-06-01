//! The query form: connection, signal, matchers, time bounds, SQL,
//! limit, and (for traces) a by-id lookup. Reads/writes the shared store
//! directly — no props.

import { For, Show, type Component } from "solid-js";

import { SIGNAL_NAMES } from "../protocol/constants";
import { isTauri } from "../env";
import {
  state,
  setField,
  addMatcher,
  removeMatcher,
  setMatcher,
  runCurrentQuery,
} from "../store";

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

/** Set ts_min/ts_max to [now - span, now] in unix nanoseconds. */
function applyQuickRange(ms: number): void {
  const nowNs = BigInt(Date.now()) * 1_000_000n;
  const spanNs = BigInt(ms) * 1_000_000n;
  setField("tsMin", String(nowNs - spanNs));
  setField("tsMax", String(nowNs));
}

const QueryForm: Component = () => {
  const isRunning = () => state.status === "running";

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
        <For each={state.matchers}>
          {(m, i) => (
            <div class="matcher-row">
              <input
                type="text"
                class="matcher-name"
                placeholder="name"
                spellcheck={false}
                value={m.name}
                onInput={(e) => setMatcher(i(), "name", e.currentTarget.value)}
              />
              <span class="eq">=</span>
              <input
                type="text"
                class="matcher-value"
                placeholder="value"
                spellcheck={false}
                value={m.value}
                onInput={(e) => setMatcher(i(), "value", e.currentTarget.value)}
              />
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
      </div>
    </form>
  );
};

export default QueryForm;
