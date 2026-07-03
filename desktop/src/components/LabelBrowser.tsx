//! The label browser: a collapsible panel that lists the label names
//! matchable for the current signal + time window, and — on expanding a
//! name — its distinct values. Clicking a value adds it as a matcher.
//!
//! This is the "browse" half of label discoverability (D-050); the matcher
//! inputs' datalists are the "inline autocomplete" half. Both read the same
//! store caches (`labelNames` / `labelValues`), warmed from the daemon's
//! label metadata over the query transport.

import { For, Show, createSignal, type Component } from "solid-js";

import {
  state,
  labelNames,
  labelStatus,
  labelValues,
  labelValueCounts,
  refreshLabels,
  ensureLabelValues,
  ensureLabelValueCounts,
  drillLabelValue,
} from "../store";

const LabelBrowser: Component = () => {
  // Which label name is currently expanded (only one at a time keeps the
  // panel compact). Null = none.
  const [open, setOpen] = createSignal<string | null>(null);

  const toggle = (name: string) => {
    if (open() === name) {
      setOpen(null);
    } else {
      setOpen(name);
      void ensureLabelValues(name);
      // Per-value entry counts for the current matchers + range (logs only).
      void ensureLabelValueCounts(name);
    }
  };

  /** Count for `name=value` under the current filters, or undefined if the
   *  counts aren't loaded yet (or this signal has none). */
  const countFor = (name: string, value: string): number | undefined =>
    labelValueCounts()[name]?.[value];

  const fmtCount = (n: number): string =>
    n >= 1_000_000
      ? `${(n / 1_000_000).toFixed(1)}M`
      : n >= 1_000
        ? `${(n / 1_000).toFixed(1)}k`
        : String(n);

  // Profiles have no discoverable labels; hide the panel entirely there.
  const hasLabels = () => state.signal !== "Profiles";

  return (
    <Show when={hasLabels()}>
      <div class="label-browser">
        <div class="field-head">
          <label>Labels</label>
          <button
            type="button"
            class="link"
            title="Reload label names for the current signal + time window"
            onClick={() => void refreshLabels(true)}
          >
            refresh
          </button>
        </div>

        <Show
          when={labelStatus() !== "loading"}
          fallback={<p class="hint">Loading labels…</p>}
        >
          <Show when={labelStatus() !== "error"} fallback={<p class="hint">Could not load labels.</p>}>
            <Show
              when={labelNames().length > 0}
              fallback={<p class="hint">No labels for this signal / range.</p>}
            >
              <ul class="label-list">
                <For each={labelNames()}>
                  {(name) => (
                    <li class="label-item">
                      <button
                        type="button"
                        class="label-key"
                        aria-expanded={open() === name}
                        onClick={() => toggle(name)}
                      >
                        <span class="disclosure">{open() === name ? "▾" : "▸"}</span>
                        {name}
                      </button>
                      <Show when={open() === name}>
                        <Show
                          when={labelValues()[name] !== undefined}
                          fallback={<p class="hint value-hint">Loading values…</p>}
                        >
                          <Show
                            when={(labelValues()[name] ?? []).length > 0}
                            fallback={<p class="hint value-hint">No values.</p>}
                          >
                            <div class="label-values">
                              <For each={labelValues()[name] ?? []}>
                                {(value) => {
                                  const c = () => countFor(name, value);
                                  return (
                                    <button
                                      type="button"
                                      class="chip value-chip"
                                      title={`Filter to ${name}=${value}${
                                        c() !== undefined ? ` (${c()} entries)` : ""
                                      }`}
                                      onClick={() => void drillLabelValue(name, value)}
                                    >
                                      {value}
                                      <Show when={c() !== undefined}>
                                        <span class="value-count">{fmtCount(c()!)}</span>
                                      </Show>
                                    </button>
                                  );
                                }}
                              </For>
                            </div>
                          </Show>
                        </Show>
                      </Show>
                    </li>
                  )}
                </For>
              </ul>
            </Show>
          </Show>
        </Show>
      </div>
    </Show>
  );
};

export default LabelBrowser;
