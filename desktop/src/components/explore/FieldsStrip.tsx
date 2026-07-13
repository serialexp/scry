//! The Explore fields strip (Phase 1): the mock's horizontal band of matchable
//! field chips under the query bar. Each chip opens a value popover (distinct
//! values + per-value entry counts, logs only); clicking a value drills the
//! query down to that slice. A "Fields" button opens a fuller overlay listing
//! every indexed field with a detail pane.
//!
//! This is the "browse" half of label discoverability (D-050), re-housed from
//! the old sidebar `LabelBrowser` into the mock's strip + overlay. Same store
//! caches (`labelNames` / `labelValues` / `labelValueCounts`).

import { For, Show, createMemo, createSignal, type Component } from "solid-js";

import {
  state,
  labelNames,
  labelStatus,
  labelError,
  labelValues,
  labelValueCounts,
  refreshLabels,
  ensureLabelValues,
  ensureLabelValueCounts,
  drillLabelValue,
} from "../../store";

/** How many chips to show inline before the rest fold into the overlay. */
const INLINE_CHIPS = 8;

function fmtCount(n: number): string {
  return n >= 1_000_000
    ? `${(n / 1_000_000).toFixed(1)}M`
    : n >= 1_000
      ? `${(n / 1_000).toFixed(1)}k`
      : String(n);
}

const FieldsStrip: Component = () => {
  // Which inline chip's value popover is open (null = none).
  const [openChip, setOpenChip] = createSignal<string | null>(null);
  // The full-fields overlay + its selected field + search filter.
  const [overlayOpen, setOverlayOpen] = createSignal(false);
  const [detail, setDetail] = createSignal<string | null>(null);
  const [filter, setFilter] = createSignal("");

  const hasLabels = () => state.signal !== "Profiles";
  const names = () => labelNames();
  const inline = () => names().slice(0, INLINE_CHIPS);
  const overflow = () => Math.max(0, names().length - INLINE_CHIPS);

  const countFor = (name: string, value: string): number | undefined =>
    labelValueCounts()[name]?.[value];

  function selectValueSource(name: string): void {
    void ensureLabelValues(name);
    void ensureLabelValueCounts(name);
  }

  function toggleChip(name: string): void {
    if (openChip() === name) {
      setOpenChip(null);
    } else {
      setOpenChip(name);
      selectValueSource(name);
    }
  }

  function openOverlay(name?: string): void {
    setOverlayOpen(true);
    setOpenChip(null);
    const first = name ?? names()[0] ?? null;
    setDetail(first);
    if (first) selectValueSource(first);
  }

  const filteredNames = createMemo(() => {
    const q = filter().trim().toLowerCase();
    return q === "" ? names() : names().filter((n) => n.toLowerCase().includes(q));
  });

  /** Value list for a field name, each with an optional entry count. */
  const valuesFor = (name: string) => labelValues()[name] ?? [];
  const maxCount = (name: string): number => {
    const c = labelValueCounts()[name];
    if (!c) return 0;
    let m = 0;
    for (const k in c) if (c[k]! > m) m = c[k]!;
    return m;
  };

  return (
    <Show when={hasLabels()}>
      <div class="fields-strip">
        <span class="fields-label">Fields</span>

        <Show
          when={labelStatus() !== "error"}
          fallback={
            <span class="fields-err" role="alert">
              {labelError() ?? "Could not load labels."}
            </span>
          }
        >
          <Show
            when={labelStatus() !== "loading"}
            fallback={<span class="fields-hint">loading…</span>}
          >
            <Show
              when={names().length > 0}
              fallback={<span class="fields-hint">no fields for this signal / range</span>}
            >
              <div class="fields-chips">
                <For each={inline()}>
                  {(name) => (
                    <div class="chip-wrap">
                      <button
                        type="button"
                        class="field-chip"
                        classList={{ active: openChip() === name }}
                        onClick={() => toggleChip(name)}
                      >
                        <span class="type-dot" />
                        <span class="field-name">{name}</span>
                        <Show when={valuesFor(name).length > 0}>
                          <span class="field-card">{valuesFor(name).length}</span>
                        </Show>
                        <span class="chip-caret">▾</span>
                      </button>

                      <Show when={openChip() === name}>
                        <div class="field-pop">
                          <div class="field-pop-head">
                            <span class="type-dot" />
                            <span class="field-name">{name}</span>
                            <span class="field-pop-count">
                              {valuesFor(name).length} values
                            </span>
                          </div>
                          <div class="field-pop-body">
                            <Show
                              when={labelValues()[name] !== undefined}
                              fallback={<p class="fields-hint pad">loading values…</p>}
                            >
                              <Show
                                when={valuesFor(name).length > 0}
                                fallback={<p class="fields-hint pad">no values</p>}
                              >
                                <For each={valuesFor(name)}>
                                  {(value) => {
                                    const c = () => countFor(name, value);
                                    return (
                                      <button
                                        type="button"
                                        class="field-val"
                                        title={`Filter to ${name}=${value}`}
                                        onClick={() => {
                                          setOpenChip(null);
                                          void drillLabelValue(name, value);
                                        }}
                                      >
                                        <span class="field-val-label">{value}</span>
                                        <Show when={c() !== undefined}>
                                          <span class="field-val-pct">{fmtCount(c()!)}</span>
                                        </Show>
                                      </button>
                                    );
                                  }}
                                </For>
                              </Show>
                            </Show>
                          </div>
                        </div>
                      </Show>
                    </div>
                  )}
                </For>

                <button
                  type="button"
                  class="fields-more"
                  onClick={() => openOverlay()}
                >
                  <Show when={overflow() > 0} fallback={<>All fields</>}>
                    + {overflow()} more
                  </Show>
                </button>
              </div>
            </Show>
          </Show>
        </Show>

        {/* Full-fields overlay */}
        <Show when={overlayOpen()}>
          <div class="fields-overlay-scrim" onClick={() => setOverlayOpen(false)} />
          <div class="fields-overlay">
            <div class="fo-head">
              <span class="fo-title">Fields</span>
              <span class="fo-sub">{names().length} indexed in current query</span>
              <input
                class="fo-filter"
                type="search"
                placeholder="filter fields…"
                value={filter()}
                onInput={(e) => setFilter(e.currentTarget.value)}
              />
              <div class="fo-spacer" />
              <button type="button" class="icon-btn" onClick={() => setOverlayOpen(false)}>
                ×
              </button>
            </div>
            <div class="fo-body">
              <div class="fo-list">
                <div class="fo-list-head">
                  <span>Field</span>
                  <span>Type</span>
                  <span class="fo-card-col">Cardinality</span>
                </div>
                <div class="fo-list-scroll">
                  <For each={filteredNames()}>
                    {(name) => (
                      <button
                        type="button"
                        class="fo-row"
                        classList={{ active: detail() === name }}
                        onClick={() => {
                          setDetail(name);
                          selectValueSource(name);
                        }}
                      >
                        <span class="fo-row-name">
                          <span class="type-dot" />
                          {name}
                        </span>
                        <span class="fo-row-type">string</span>
                        <span class="fo-row-card">
                          {valuesFor(name).length > 0 ? valuesFor(name).length : "—"}
                        </span>
                      </button>
                    )}
                  </For>
                </div>
              </div>
              <div class="fo-detail">
                <Show
                  when={detail()}
                  fallback={<div class="fields-hint pad">Select a field.</div>}
                >
                  {(name) => (
                    <>
                      <div class="fo-detail-head">
                        <span class="type-dot" />
                        <span class="field-name">{name()}</span>
                      </div>
                      <div class="fo-detail-meta">
                        <div>
                          <div class="fo-meta-k">Type</div>
                          <div class="fo-meta-v">string</div>
                        </div>
                        <div>
                          <div class="fo-meta-k">Cardinality</div>
                          <div class="fo-meta-v">
                            {valuesFor(name()).length > 0 ? valuesFor(name()).length : "—"}
                          </div>
                        </div>
                      </div>
                      <div class="fo-values-h">Top values</div>
                      <div class="fo-values">
                        <Show
                          when={labelValues()[name()] !== undefined}
                          fallback={<p class="fields-hint pad">loading values…</p>}
                        >
                          <Show
                            when={valuesFor(name()).length > 0}
                            fallback={<p class="fields-hint pad">no values</p>}
                          >
                            <For each={valuesFor(name())}>
                              {(value) => {
                                const c = () => countFor(name(), value);
                                const max = () => maxCount(name());
                                const pct = () =>
                                  c() !== undefined && max() > 0
                                    ? Math.max(3, Math.round((c()! / max()) * 100))
                                    : 0;
                                return (
                                  <button
                                    type="button"
                                    class="fo-value"
                                    title={`Filter to ${name()}=${value}`}
                                    onClick={() => {
                                      setOverlayOpen(false);
                                      void drillLabelValue(name(), value);
                                    }}
                                  >
                                    <div class="fo-value-row">
                                      <span class="fo-value-label">{value}</span>
                                      <Show when={c() !== undefined}>
                                        <span class="fo-value-pct">{fmtCount(c()!)}</span>
                                      </Show>
                                    </div>
                                    <div class="fo-value-bar">
                                      <div
                                        class="fo-value-fill"
                                        style={{ width: `${pct()}%` }}
                                      />
                                    </div>
                                  </button>
                                );
                              }}
                            </For>
                          </Show>
                        </Show>
                      </div>
                    </>
                  )}
                </Show>
              </div>
            </div>
          </div>
        </Show>

        <button
          type="button"
          class="fields-refresh link"
          title="Reload label names for the current signal + time window"
          onClick={() => void refreshLabels(true)}
        >
          refresh
        </button>
      </div>
    </Show>
  );
};

export default FieldsStrip;
