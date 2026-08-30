//! The `{rows} · {ms}ms` chip, expanded into a per-phase breakdown (D-066).
//!
//! This exists because "1,000 rows · 9868.3 ms" is not actionable. It does not
//! say whether the time went to queueing, planning, scanning, the network, or
//! decoding Arrow in this very browser — and those have entirely different
//! fixes. The chip stays exactly as it was; clicking it reveals the answer.
//!
//! Three presentation rules, each inherited from how the numbers are measured
//! and none of them cosmetic:
//!
//!  1. **The bars are a timeline and sum to the whole.** The server measures its
//!     total independently of its parts, so the unattributed remainder is drawn
//!     as its own `other` bar rather than distributed across the named phases.
//!     A large `other` is a real finding and should look like one.
//!  2. **DataFusion's counters are not bars.** They are summed across
//!     partitions and can exceed the phase that contains them; drawn as slices
//!     they would claim more time than the query took. They get a plain
//!     figures row, explicitly labelled as summed.
//!  3. **The client's own halves are shown.** `transport` and `decode` are
//!     measured here, not by the daemon, and are frequently the answer.

import { For, Show, createMemo, createSignal, onCleanup } from "solid-js";

import type { QueryTiming } from "../../protocol/client";

/** Fixed hues per phase, so the same phase keeps its colour across queries and
 *  a shape you saw yesterday still reads the same today. Tuned to the warm
 *  near-black palette in `styles.css` rather than taken off a generic ramp. */
const PHASE_COLORS: Record<string, string> = {
  admission: "#8a6a3a",
  catalog: "#5bb6a8",
  "cache-lookup": "#3f8f86",
  "live-fetch": "#9b7fd4",
  register: "#6d93c9",
  plan: "#c98fbd",
  execute: "#e8a33d",
  serialize: "#b7c46a",
  write: "#e5695f",
  other: "#4a463d",
  transport: "#8f8a7d",
  decode: "#6f6a5e",
};

/** The generic fallback, for a phase a newer daemon adds before this UI knows
 *  about it — an unrecognised name still gets a bar rather than vanishing. */
const UNKNOWN_PHASE_COLOR = "#4a463d";

function fmtMs(ms: number): string {
  if (ms >= 1000) return `${(ms / 1000).toFixed(2)} s`;
  if (ms >= 1) return `${ms.toFixed(1)} ms`;
  if (ms === 0) return "0";
  return `${ms.toFixed(3)} ms`;
}

function fmtBytes(n: bigint): string {
  const v = Number(n);
  if (v >= 1 << 20) return `${(v / (1 << 20)).toFixed(1)} MiB`;
  if (v >= 1 << 10) return `${(v / (1 << 10)).toFixed(1)} KiB`;
  return `${v} B`;
}

export function TimingPopover(props: {
  rows: bigint;
  elapsedMs: number;
  timing: QueryTiming | null;
}) {
  const [open, setOpen] = createSignal(false);
  let wrap: HTMLSpanElement | undefined;

  // An overlay that only Escape dismisses is a trap for anyone who reaches for
  // the mouse, so it also closes on a click anywhere outside itself. Both
  // listeners are unconditional and cheap; attaching them only while open
  // would race with the very click that opened the panel.
  const onKey = (e: KeyboardEvent) => {
    if (e.key === "Escape") setOpen(false);
  };
  const onPointerDown = (e: PointerEvent) => {
    if (wrap && !wrap.contains(e.target as Node)) setOpen(false);
  };
  window.addEventListener("keydown", onKey);
  document.addEventListener("pointerdown", onPointerDown);
  onCleanup(() => {
    window.removeEventListener("keydown", onKey);
    document.removeEventListener("pointerdown", onPointerDown);
  });

  /**
   * The full round trip, as a timeline: the server's own phases, then the two
   * segments only this client can see. Together these are exactly `elapsedMs`,
   * because `transport` was *defined* as the leftover after the server total
   * and the decode — so the bar always fills.
   */
  const segments = createMemo(() => {
    const t = props.timing;
    if (!t) return [];
    return [
      ...t.phases,
      { label: "transport", ms: t.transportMs },
      { label: "decode", ms: t.decodeMs },
    ].filter((p) => p.ms > 0);
  });

  const total = createMemo(() =>
    segments().reduce((acc, s) => acc + s.ms, 0),
  );

  return (
    <span class="timing-chip-wrap" ref={wrap}>
      <button
        type="button"
        class="timing-chip"
        classList={{ expandable: props.timing !== null, open: open() }}
        // Without stats there is nothing to disclose, so the chip stays inert
        // rather than opening an empty panel.
        disabled={props.timing === null}
        aria-expanded={open()}
        title={
          props.timing
            ? "Show where this query's time went"
            : "This daemon does not report per-phase timing"
        }
        onClick={() => setOpen(!open())}
      >
        {props.rows.toLocaleString()} rows · {props.elapsedMs.toFixed(0)}ms
        <Show when={props.timing}>
          <span class="timing-caret">{open() ? "▾" : "▸"}</span>
        </Show>
      </button>

      <Show when={open() && props.timing}>
        {(t) => (
          <div class="timing-panel" role="dialog" aria-label="Query timing">
            <div class="timing-bar">
              <For each={segments()}>
                {(s) => (
                  <div
                    class="timing-bar-seg"
                    style={{
                      width: `${total() > 0 ? (s.ms / total()) * 100 : 0}%`,
                      background: PHASE_COLORS[s.label] ?? UNKNOWN_PHASE_COLOR,
                    }}
                    title={`${s.label}: ${fmtMs(s.ms)}`}
                  />
                )}
              </For>
            </div>

            <ul class="timing-legend">
              <For each={segments()}>
                {(s) => (
                  <li>
                    <span
                      class="timing-swatch"
                      style={{ background: PHASE_COLORS[s.label] ?? UNKNOWN_PHASE_COLOR }}
                    />
                    <span class="timing-label">{s.label}</span>
                    <span class="timing-value">{fmtMs(s.ms)}</span>
                    <span class="timing-pct">
                      {total() > 0 ? ((s.ms / total()) * 100).toFixed(0) : "0"}%
                    </span>
                  </li>
                )}
              </For>
            </ul>

            <div class="timing-facts">
              <span>
                {t().cacheHit ? "served from cache" : "computed"}
                {t().attempts > 1 ? ` · ${t().attempts} attempts` : ""}
              </span>
              <span>
                {t().blocksScanned}/{t().blocksConsidered} blocks ·{" "}
                {fmtBytes(t().bytesScanned)} scanned
              </span>
              <Show when={t().nodeId}>
                <span class="timing-node">on {t().nodeId}</span>
              </Show>
            </div>

            {/* Deliberately figures, not bars — see rule 2 in the module docs. */}
            <div class="timing-aside">
              <div class="timing-aside-head">
                object store &amp; engine — summed over concurrent work, so these
                can exceed the phase above that contains them
              </div>
              <div class="timing-aside-row">
                <span>postings {fmtMs(t().postingsFetchMs)}</span>
                <span>bloom {fmtMs(t().bloomFetchMs)}</span>
                <span>df-open {fmtMs(t().datafusion.openingMs)}</span>
                <span>df-scan {fmtMs(t().datafusion.scanningMs)}</span>
                <span>df-compute {fmtMs(t().datafusion.computeMs)}</span>
              </div>
            </div>

            <Show when={t().liveNodes.length > 0}>
              <div class="timing-aside">
                <div class="timing-aside-head">live ingesters</div>
                <For each={t().liveNodes}>
                  {(n) => (
                    <div class="timing-aside-row" classList={{ bad: !n.ok }}>
                      <span>{n.addr}</span>
                      <span>{fmtMs(n.ms)}</span>
                      <span>
                        {n.ok ? `${n.rows.toLocaleString()} rows` : "failed"}
                      </span>
                    </div>
                  )}
                </For>
              </div>
            </Show>
          </div>
        )}
      </Show>
    </span>
  );
}
