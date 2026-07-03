//! Log-volume histogram — a count-over-time of log entries, stacked by
//! severity, over the current matchers + range. Shown above the results table
//! for the Logs signal only (metrics results carry no severity column yet).
//!
//! Rendering is uPlot (tiny canvas plotter). Stacking is the classic uPlot
//! recipe: draw cumulative bars from most-severe (full height) down to
//! least-severe (shortest), so each shorter bar overpaints the lower part of
//! the one before it — producing a stack with the severe bands on top. A
//! drag-select zooms: it sets the form's [ts_min, ts_max] to the brushed span
//! and re-runs the query + volume (the Grafana Explore "brush to zoom" loop).

import {
  Show,
  createEffect,
  onCleanup,
  onMount,
  type Component,
} from "solid-js";
import uPlot from "uplot";
import "uplot/dist/uPlot.min.css";

import { state, volumeData, volumeStatus, setField, runCurrentQuery } from "../store";
import { severityColor } from "../severity";
import type { VolumeData } from "../volume";

/** Build the uPlot data + series config from a decoded `VolumeData`.
 *  Series are ordered most→least severe (paint back-to-front) and carry
 *  cumulative counts so anchored-at-zero bars stack correctly. */
function toPlot(vd: VolumeData): { data: uPlot.AlignedData; opts: Partial<uPlot.Options> } {
  const xs = vd.buckets.map((ms) => ms / 1000); // uPlot time axis is seconds

  // least→most severe, as decoded. Cumulative[i] = sum of counts for bands
  // 0..=i (so the most-severe band's cumulative == the full total per bucket).
  const cumulative: number[][] = [];
  for (let i = 0; i < vd.series.length; i++) {
    const prev = i === 0 ? null : cumulative[i - 1];
    cumulative.push(vd.series[i].counts.map((c, b) => c + (prev ? prev[b] : 0)));
  }

  // Paint order: most-severe first (tallest), least-severe last (shortest,
  // ends up as the bottom visible segment).
  const order = vd.series.map((_, i) => i).reverse();

  const barsPath = uPlot.paths.bars
    ? uPlot.paths.bars({ size: [0.9, 100], align: 0 })
    : undefined;

  const data: uPlot.AlignedData = [
    xs,
    ...order.map((i) => cumulative[i]),
  ];

  const series: uPlot.Series[] = [
    {}, // x
    ...order.map((i) => {
      const s = vd.series[i];
      const color = severityColor(s.label);
      const raw = s.counts;
      return {
        label: s.label,
        stroke: color,
        fill: color,
        width: 0,
        paths: barsPath,
        points: { show: false },
        // Legend shows the raw (per-band) count, not the cumulative value.
        value: (_u: uPlot, _v: number, _si: number, di: number | null) =>
          di == null ? "" : String(raw[di] ?? 0),
      } as uPlot.Series;
    }),
  ];

  return {
    data,
    opts: {
      series,
      // Selection zoom is handled by our setSelect hook; don't let the drag
      // also rescale the plot (setScale:false) — we drive the range instead.
      cursor: { drag: { x: true, y: false, setScale: false } },
      scales: { x: { time: true }, y: { range: (_u, _min, max) => [0, max] } },
      legend: { show: true, live: true },
      axes: [
        {},
        {
          size: 44,
          values: (_u, splits) => splits.map((v) => (v >= 1000 ? `${Math.round(v / 1000)}k` : String(v))),
        },
      ],
    },
  };
}

const VolumePanel: Component = () => {
  let host!: HTMLDivElement;
  let plot: uPlot | null = null;
  let ro: ResizeObserver | null = null;

  const width = () => Math.max(320, host?.clientWidth ?? 640);

  // Handle a completed drag-select: convert the brushed pixel span to a time
  // range, reflect it in the form, and re-run.
  function onSelect(u: uPlot): void {
    const sel = u.select;
    if (!sel || sel.width < 3) return; // ignore stray clicks
    const x0 = u.posToVal(sel.left, "x");
    const x1 = u.posToVal(sel.left + sel.width, "x");
    u.setSelect({ left: 0, top: 0, width: 0, height: 0 }, false);
    const lo = Math.min(x0, x1);
    const hi = Math.max(x0, x1);
    if (!(hi > lo)) return;
    // seconds → unix nanoseconds.
    const tsMinNs = BigInt(Math.floor(lo * 1000)) * 1_000_000n;
    const tsMaxNs = BigInt(Math.ceil(hi * 1000)) * 1_000_000n;
    setField("tsMin", String(tsMinNs));
    setField("tsMax", String(tsMaxNs));
    void runCurrentQuery(); // re-runs the table + volume together
  }

  function destroy(): void {
    plot?.destroy();
    plot = null;
  }

  // (Re)build the plot whenever the decoded data changes. Rebuilding (rather
  // than setData) keeps series count/colors in sync with the severities present.
  createEffect(() => {
    const vd = volumeData();
    destroy();
    if (!vd || vd.buckets.length === 0) return;
    const { data, opts } = toPlot(vd);
    plot = new uPlot(
      {
        width: width(),
        height: 140,
        ...opts,
        hooks: { setSelect: [onSelect] },
      } as uPlot.Options,
      data,
      host,
    );
  });

  onMount(() => {
    ro = new ResizeObserver(() => plot?.setSize({ width: width(), height: 140 }));
    if (host) ro.observe(host);
  });

  onCleanup(() => {
    ro?.disconnect();
    destroy();
  });

  return (
    <Show when={state.signal === "Logs"}>
      <section class="volume-panel">
        <div class="volume-head">
          <span class="volume-title">Log volume</span>
          <Show when={volumeData()}>
            {(vd) => (
              <span class="volume-meta">
                {vd().total.toLocaleString()} entries · {vd().buckets.length} buckets
              </span>
            )}
          </Show>
        </div>

        {/* The uPlot host is always mounted so its ref stays stable; the plot
            is (re)built by the effect only when data is present. Status
            messages show above it when there's nothing to draw. */}
        <Show when={volumeStatus() !== "ready"}>
          <div class="volume-empty">
            {volumeStatus() === "loading"
              ? "Loading volume…"
              : volumeStatus() === "error"
                ? "Volume query failed."
                : volumeStatus() === "empty"
                  ? "No log entries in range — pick a time range and run a query."
                  : "Run a logs query with a time range to see volume."}
          </div>
        </Show>
        <div
          class="volume-plot"
          classList={{ hidden: volumeStatus() !== "ready" }}
          ref={host}
        />
        <Show when={volumeStatus() === "ready"}>
          <div class="volume-hint">Drag across the chart to zoom to a range.</div>
        </Show>
      </section>
    </Show>
  );
};

export default VolumePanel;
