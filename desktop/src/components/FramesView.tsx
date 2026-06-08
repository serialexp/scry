//! Frames-overview view.
//!
//! Rendered by `ResultsTable` when the result is the traces frames-overview
//! aggregate (one row per trace = per frame; see `store.runFramesOverview`).
//! Each frame is a deterministic `session ++ frame` trace id, so we can label it
//! by frame number and let the user drill into its waterfall with one click.
//! Frames are listed slowest-first (the worst frames are what you came to find),
//! each with a duration bar scaled to the slowest frame in view. A small summary
//! strip carries the count + duration distribution (min / median / p99 / max).

import { For, Show, createMemo, createSignal, type Component } from "solid-js";

import { fmtDuration, fmtTs } from "../format";
import { type FrameRow, type FrameStats } from "../traces";
import { drillIntoFrame, state } from "../store";

export interface FramesData {
  rows: FrameRow[];
  stats: FrameStats;
  shown: number;
  total: number;
}

interface Props {
  data: () => FramesData;
  raw: () => boolean;
  setRaw: (v: boolean) => void;
}

type SortKey = "dur" | "frame";

const FramesView: Component<Props> = (props) => {
  const [filter, setFilter] = createSignal("");
  const [sortKey, setSortKey] = createSignal<SortKey>("dur");

  const maxDur = createMemo(() => {
    const m = props.data().stats.maxNs;
    return m > 0n ? m : 1n;
  });

  const rows = createMemo(() => {
    const all = props.data().rows;
    const q = filter().trim().toLowerCase();
    const filtered =
      q === ""
        ? all
        : all.filter(
            (r) => String(r.frame).includes(q) || r.traceId.toLowerCase().includes(q),
          );
    const sorted = [...filtered];
    if (sortKey() === "frame") {
      sorted.sort((a, b) => (a.frame < b.frame ? -1 : a.frame > b.frame ? 1 : 0));
    } else {
      sorted.sort((a, b) => (a.durNs > b.durNs ? -1 : a.durNs < b.durNs ? 1 : 0));
    }
    return sorted;
  });

  return (
    <>
      <div class="results-meta">
        <span>
          <strong>{props.data().total.toLocaleString()}</strong> frames
        </span>
        <span title="min / median / p99 / max frame duration">
          {fmtDuration(props.data().stats.minNs)} · {fmtDuration(props.data().stats.medianNs)} ·{" "}
          {fmtDuration(props.data().stats.p99Ns)} · {fmtDuration(props.data().stats.maxNs)}
        </span>
        <label class="frames-sort">
          sort
          <select value={sortKey()} onChange={(e) => setSortKey(e.currentTarget.value as SortKey)}>
            <option value="dur">slowest first</option>
            <option value="frame">frame number</option>
          </select>
        </label>
        <input
          class="log-search"
          type="search"
          placeholder="filter frames…"
          value={filter()}
          onInput={(e) => setFilter(e.currentTarget.value)}
        />
        <span>{rows().length.toLocaleString()} shown</span>
        <Show when={props.data().shown < props.data().total}>
          <span class="warn">scanned first {props.data().shown.toLocaleString()}</span>
        </Show>
        <label class="raw-toggle" title="show the underlying columns as a table">
          <input
            type="checkbox"
            checked={props.raw()}
            onInput={(e) => props.setRaw(e.currentTarget.checked)}
          />
          raw
        </label>
      </div>

      <div class="frames-list">
        <For each={rows()}>
          {(r) => {
            const frac = Number(r.durNs) / Number(maxDur());
            const widthPct = frac * 100;
            // Slow frames trend red, fast frames green — the whole point of the
            // overview is to spot the slow ones at a glance.
            const hue = Math.round(120 * (1 - Math.min(1, Math.max(0, frac))));
            return (
              <button
                type="button"
                class="frame-row"
                title={`trace ${r.traceId} — click to open the waterfall`}
                onClick={() => void drillIntoFrame(r.traceId)}
              >
                <span class="frame-num">#{r.frame.toString()}</span>
                <span class="frame-track">
                  <span
                    class="frame-bar"
                    style={{
                      width: `${widthPct.toFixed(2)}%`,
                      background: `hsl(${hue}, 60%, 50%)`,
                    }}
                  />
                  <span class="frame-dur">{fmtDuration(r.durNs)}</span>
                </span>
                <span class="frame-spans">{r.spans.toLocaleString()} spans</span>
                <span class="frame-t0" title={fmtTs(r.t0).full}>
                  {fmtTs(r.t0).short}
                </span>
              </button>
            );
          }}
        </For>
      </div>

      <Show when={state.signal !== "Traces"}>
        {/* Defensive: the overview should only run for Traces. */}
        <div class="frames-empty">frames overview applies to the Traces signal.</div>
      </Show>
    </>
  );
};

export default FramesView;
