//! The Explore query bar (Phase 1): the horizontal top strip from the mock —
//! signal tab pills, a token-styled matcher bar, time-range pills, a Run
//! button, and an Advanced popover holding the raw controls (SQL, limit,
//! trace-id, custom bounds, frames overview).
//!
//! Reads/writes the shared store directly — no props (Rule #9). It replaces the
//! old vertical `QueryForm` as the query-editing surface; the connection target
//! moved to the shell topbar and the label browsing to the FieldsStrip.

import {
  For,
  Show,
  createEffect,
  createSignal,
  onMount,
  type Component,
} from "solid-js";

import type { SignalName } from "../../protocol/constants";
import {
  state,
  setField,
  applyLabelMatcher,
  deleteMatcher,
  runCurrentQuery,
  runFramesOverview,
  labelNames,
  refreshLabels,
  QUICK_RANGES,
  applyQuickRange,
  activeRange,
  clearTimeRange,
} from "../../store";

/** Signal tabs, in the mock's order. Dot colour distinguishes them at a
 *  glance; Profiles is appended (the app supports it; the mock showed three). */
const TABS: { name: SignalName; dot: string }[] = [
  { name: "Logs", dot: "var(--teal)" },
  { name: "Traces", dot: "var(--accent)" },
  { name: "Metrics", dot: "#7bbf6a" },
  { name: "Profiles", dot: "var(--muted)" },
];

const SIGNAL_HINTS: Record<string, string> = {
  Metrics:
    "Matchers preselect series via postings. SQL runs against the metrics table.",
  Logs: "Matchers preselect streams via postings. SQL runs against the logs table.",
  Traces:
    "Matchers map to promoted columns (service.name, service.namespace, deployment.environment). Use trace-id for by-id lookup; other attributes via SQL.",
  Profiles:
    "Retrieval only: filter by time + SQL against the labels map. Label matchers are rejected — use SQL.",
};

const QueryBar: Component = () => {
  const isRunning = () => state.status === "running";
  const [draft, setDraft] = createSignal("");
  const [advOpen, setAdvOpen] = createSignal(false);

  // Warm the label cache whenever the scope changes (signal / target / time).
  createEffect(() => {
    void state.signal;
    void state.target;
    void refreshLabels();
  });
  onMount(() => void refreshLabels());

  /** Non-blank matchers, as {index, name, value} — the tokens to render. */
  const tokens = () =>
    state.matchers
      .map((m, i) => ({ i, name: m.name.trim(), value: m.value }))
      .filter((m) => m.name !== "");

  /** Commit the draft text as a `key=value` (or bare `key`) matcher. */
  function commitDraft(): void {
    const raw = draft().trim();
    if (raw === "") return;
    const eq = raw.indexOf("=");
    const name = (eq >= 0 ? raw.slice(0, eq) : raw).trim();
    const value = eq >= 0 ? raw.slice(eq + 1) : "";
    if (name === "") return;
    applyLabelMatcher(name, value);
    setDraft("");
  }

  return (
    <div class="qbar">
      <div class="qbar-row">
        {/* Signal tabs */}
        <div class="seg tabs">
          <For each={TABS}>
            {(t) => (
              <button
                type="button"
                class="seg-pill"
                classList={{ active: state.signal === t.name }}
                onClick={() => setField("signal", t.name)}
              >
                <span class="tab-dot" style={{ background: t.dot }} />
                {t.name}
              </button>
            )}
          </For>
        </div>

        {/* Token matcher bar */}
        <div class="token-bar">
          <For each={tokens()}>
            {(tok) => (
              <span class="qtoken" title={`${tok.name}=${tok.value}`}>
                <span class="qk">{tok.name}</span>
                <span class="qop">=</span>
                <span class="qv">{tok.value}</span>
                <button
                  type="button"
                  class="qtoken-x"
                  title="remove"
                  onClick={() => deleteMatcher(tok.i)}
                >
                  ×
                </button>
              </span>
            )}
          </For>
          <input
            class="token-input"
            type="text"
            spellcheck={false}
            list="qbar-label-names"
            placeholder={tokens().length === 0 ? "key=value  ·  filter by fields…" : ""}
            value={draft()}
            onInput={(e) => setDraft(e.currentTarget.value)}
            onFocus={() => void refreshLabels()}
            onKeyDown={(e) => {
              if (e.key === "Enter") {
                e.preventDefault();
                commitDraft();
              } else if (
                e.key === "Backspace" &&
                draft() === "" &&
                tokens().length > 0
              ) {
                // Backspace on an empty draft pops the last token.
                deleteMatcher(tokens()[tokens().length - 1]!.i);
              }
            }}
          />
          <datalist id="qbar-label-names">
            <For each={labelNames()}>{(n) => <option value={`${n}=`} />}</For>
          </datalist>
          <span class="token-count">
            <Show when={state.status === "done"}>
              {state.totalRows.toLocaleString()} rows · {state.elapsedMs.toFixed(0)}ms
            </Show>
          </span>
        </div>

        {/* Time-range pills */}
        <div class="seg ranges">
          <For each={QUICK_RANGES}>
            {(r) => (
              <button
                type="button"
                class="seg-pill"
                classList={{ active: activeRange() === r.label }}
                onClick={() => applyQuickRange(r.ms, r.label)}
              >
                {r.label}
              </button>
            )}
          </For>
        </div>

        {/* Advanced toggle + Run */}
        <button
          type="button"
          class="adv-toggle"
          classList={{ active: advOpen() }}
          title="Advanced query controls"
          onClick={() => setAdvOpen((v) => !v)}
        >
          ⋯
        </button>
        <button
          type="button"
          class="run"
          disabled={isRunning()}
          onClick={() => {
            if (!isRunning()) void runCurrentQuery();
          }}
        >
          {isRunning() ? "Running…" : "Run"}
        </button>
      </div>

      {/* Advanced panel: the raw controls that don't fit the compact bar. */}
      <Show when={advOpen()}>
        <div class="qbar-adv">
          <p class="hint adv-hint">{SIGNAL_HINTS[state.signal]}</p>

          <div class="adv-grid">
            <Show when={state.signal === "Traces"}>
              <label class="adv-field adv-wide">
                <span>Trace id (hex, 16 bytes)</span>
                <input
                  type="text"
                  spellcheck={false}
                  value={state.traceId}
                  onInput={(e) => setField("traceId", e.currentTarget.value)}
                  placeholder="e.g. aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                />
              </label>
            </Show>

            <label class="adv-field">
              <span>ts_min (unix ns)</span>
              <input
                type="text"
                inputmode="numeric"
                spellcheck={false}
                value={state.tsMin}
                onInput={(e) => setField("tsMin", e.currentTarget.value)}
                onChange={() => void refreshLabels()}
                placeholder="(none)"
              />
            </label>
            <label class="adv-field">
              <span>ts_max (unix ns)</span>
              <input
                type="text"
                inputmode="numeric"
                spellcheck={false}
                value={state.tsMax}
                onInput={(e) => setField("tsMax", e.currentTarget.value)}
                onChange={() => void refreshLabels()}
                placeholder="(none)"
              />
            </label>
            <label class="adv-field">
              <span>limit</span>
              <input
                type="text"
                inputmode="numeric"
                spellcheck={false}
                value={state.limit}
                disabled={state.sql.trim() !== ""}
                onInput={(e) => setField("limit", e.currentTarget.value)}
                placeholder="(no limit)"
              />
            </label>

            <label class="adv-field adv-wide">
              <span>SQL (optional — overrides the default SELECT *)</span>
              <textarea
                rows={2}
                spellcheck={false}
                value={state.sql}
                onInput={(e) => setField("sql", e.currentTarget.value)}
                placeholder="SELECT * FROM metrics ORDER BY ts_unix_nano DESC"
              />
            </label>
          </div>

          <div class="adv-actions">
            <button type="button" class="link" onClick={() => clearTimeRange()}>
              clear time range
            </button>
            <Show when={state.signal === "Traces"}>
              <button
                type="button"
                class="run secondary"
                disabled={isRunning()}
                title="Aggregate one row per trace (frame): duration + span count, slowest first."
                onClick={() => {
                  if (!isRunning()) void runFramesOverview();
                }}
              >
                Frames overview
              </button>
            </Show>
          </div>
        </div>
      </Show>
    </div>
  );
};

export default QueryBar;
