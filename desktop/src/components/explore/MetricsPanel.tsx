//! Metrics time-series chart — a downsampled line chart for the Metrics
//! signal, the counterpart to the logs `VolumePanel`. Shows one aggregated
//! line (avg/sum/min/max/count over the matching series) or one line per
//! series, over the current metric (`__name__`) + matchers + range.
//!
//! Rendering is uPlot (tiny canvas plotter), mirroring `VolumePanel`'s
//! ResizeObserver + destroy/rebuild-on-data + drag-select-to-zoom. Only the
//! series shape differs: lines (no fill, break on gaps) instead of stacked
//! bars, and a y-scale that autoscales rather than anchoring at zero. A
//! drag-select zooms — it sets the form's [ts_min, ts_max] to the brushed
//! span and re-runs the query + chart (the Grafana Explore "brush to zoom").

import {
  For,
  Show,
  createEffect,
  createMemo,
  createSignal,
  onCleanup,
  onMount,
  type Component,
} from "solid-js";
import uPlot from "uplot";
import "uplot/dist/uPlot.min.css";

import {
  state,
  setField,
  runCurrentQuery,
  ensureLabelValues,
  labelValues,
  selectedMetric,
  setMetricName,
  metricAgg,
  setMetricAgg,
  metricGrouped,
  setMetricGrouped,
  metricsChartData,
  metricsChartStatus,
} from "../../store";
import { AGG_FNS, seriesColor, type AggFn, type MetricsChartData } from "../../metricsChart";

/** Build uPlot data + line-series config from a decoded `MetricsChartData`. */
function toPlot(mc: MetricsChartData): {
  data: uPlot.AlignedData;
  opts: Partial<uPlot.Options>;
} {
  const xs = mc.buckets.map((ms) => ms / 1000); // uPlot time axis is seconds
  const data = [xs, ...mc.series.map((s) => s.points)] as uPlot.AlignedData;

  const series: uPlot.Series[] = [
    {}, // x
    ...mc.series.map((s, i) => ({
      label: s.name,
      stroke: seriesColor(i),
      width: 1.5,
      points: { show: false },
      // Break the line across buckets with no sample rather than interpolate.
      spanGaps: false,
    })) as uPlot.Series[],
  ];

  return {
    data,
    opts: {
      series,
      // Our setSelect hook drives the range; don't let the drag rescale too.
      cursor: { drag: { x: true, y: false, setScale: false } },
      scales: { x: { time: true } },
      legend: { show: true, live: true },
      axes: [
        { stroke: "#9a9385", grid: { stroke: "#2b2822" }, ticks: { stroke: "#38342b" } },
        {
          size: 52,
          stroke: "#9a9385",
          grid: { stroke: "#2b2822" },
          ticks: { stroke: "#38342b" },
        },
      ],
    },
  };
}

const MetricsPanel: Component = () => {
  let host!: HTMLDivElement;
  let metricInput!: HTMLInputElement;
  let plot: uPlot | null = null;
  let ro: ResizeObserver | null = null;

  const [metricSearch, setMetricSearch] = createSignal("");
  const [metricPickerOpen, setMetricPickerOpen] = createSignal(false);
  const width = () => Math.max(320, host?.clientWidth ?? 640);
  const metricNames = () => labelValues()["__name__"] ?? [];
  const filteredMetricNames = createMemo(() => {
    const q = metricSearch().trim().toLowerCase();
    return q === ""
      ? metricNames()
      : metricNames().filter((name) => name.toLowerCase().includes(q));
  });

  // Populate the metric picker whenever we're on the Metrics signal.
  createEffect(() => {
    if (state.signal === "Metrics") void ensureLabelValues("__name__");
  });

  // Keep the search input aligned when another control changes the __name__
  // matcher. While the picker is open, preserve what the user is typing.
  createEffect(() => {
    const metric = selectedMetric();
    if (!metricPickerOpen()) setMetricSearch(metric);
  });

  function chooseMetric(name: string): void {
    setMetricSearch(name);
    setMetricPickerOpen(false);
    if (name !== selectedMetric()) setMetricName(name);
  }

  function closeMetricPickerSoon(): void {
    // Let an option's pointer event run before blur closes the popup.
    window.setTimeout(() => {
      setMetricPickerOpen(false);
      setMetricSearch(selectedMetric());
    });
  }

  // Handle a completed drag-select: brushed pixel span → time range → re-run.
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
    void runCurrentQuery(); // re-runs the table + chart together
  }

  function destroy(): void {
    plot?.destroy();
    plot = null;
  }

  // (Re)build the plot whenever the decoded data changes. Rebuilding (rather
  // than setData) keeps series count/colors in sync with the metric/mode.
  createEffect(() => {
    const mc = metricsChartData();
    destroy();
    if (!mc || mc.buckets.length === 0) return;
    const { data, opts } = toPlot(mc);
    plot = new uPlot(
      {
        width: width(),
        height: 180,
        ...opts,
        hooks: { setSelect: [onSelect] },
      } as uPlot.Options,
      data,
      host,
    );
  });

  onMount(() => {
    ro = new ResizeObserver(() => plot?.setSize({ width: width(), height: 180 }));
    if (host) ro.observe(host);
  });

  onCleanup(() => {
    ro?.disconnect();
    destroy();
  });

  return (
    <Show when={state.signal === "Metrics"}>
      <section class="metrics-panel">
        <div class="metrics-controls">
          <label class="metric-field">
            <span>metric</span>
            <div class="metric-combobox">
              <input
                ref={metricInput}
                class="metric-search"
                type="search"
                role="combobox"
                aria-label="metric"
                aria-autocomplete="list"
                aria-expanded={metricPickerOpen()}
                aria-controls="metric-options"
                autocomplete="off"
                placeholder="Search metrics…"
                value={metricSearch()}
                onFocus={() => setMetricPickerOpen(true)}
                onInput={(e) => {
                  setMetricSearch(e.currentTarget.value);
                  setMetricPickerOpen(true);
                }}
                onKeyDown={(e) => {
                  if (e.key === "Escape") {
                    setMetricPickerOpen(false);
                    setMetricSearch(selectedMetric());
                    metricInput.blur();
                  } else if (e.key === "Enter") {
                    e.preventDefault();
                    const exact = metricNames().find(
                      (name) => name.toLowerCase() === metricSearch().trim().toLowerCase(),
                    );
                    const match = exact ?? filteredMetricNames()[0];
                    if (match) chooseMetric(match);
                  }
                }}
                onBlur={closeMetricPickerSoon}
              />
              <Show when={metricPickerOpen()}>
                <div id="metric-options" class="metric-options" role="listbox">
                  <Show
                    when={filteredMetricNames().length > 0}
                    fallback={<div class="metric-option-empty">No matching metrics</div>}
                  >
                    <For each={filteredMetricNames()}>
                      {(name) => (
                        <button
                          type="button"
                          class="metric-option"
                          classList={{ selected: name === selectedMetric() }}
                          role="option"
                          aria-selected={name === selectedMetric()}
                          onPointerDown={(e) => {
                            e.preventDefault();
                            chooseMetric(name);
                          }}
                        >
                          {name}
                        </button>
                      )}
                    </For>
                  </Show>
                </div>
              </Show>
            </div>
          </label>

          <label class="metric-field">
            <span>aggregate</span>
            <select
              class="agg-select"
              value={metricAgg()}
              onChange={(e) => setMetricAgg(e.currentTarget.value as AggFn)}
            >
              <For each={AGG_FNS}>{(fn) => <option value={fn}>{fn}</option>}</For>
            </select>
          </label>

          <label class="metric-check">
            <input
              type="checkbox"
              checked={metricGrouped()}
              onChange={(e) => setMetricGrouped(e.currentTarget.checked)}
            />
            <span>per series</span>
          </label>

          <Show when={metricsChartData()}>
            {(mc) => (
              <span class="metrics-meta">
                {mc().series.length} series · {mc().buckets.length} buckets
                <Show when={mc().truncated > 0}>
                  {" "}
                  · +{mc().truncated} hidden
                </Show>
              </span>
            )}
          </Show>
        </div>

        {/* Host is always mounted so its ref stays stable; the plot is
            (re)built by the effect only when data is present. */}
        <Show when={metricsChartStatus() !== "ready"}>
          <div class="metrics-empty">
            {metricsChartStatus() === "loading"
              ? "Loading chart…"
              : metricsChartStatus() === "error"
                ? "Chart query failed."
                : metricsChartStatus() === "no-data"
                  ? "No data found for this metric in the selected time range."
                  : "Pick a metric and a time range to chart it."}
          </div>
        </Show>
        <div
          class="metrics-plot"
          classList={{ hidden: metricsChartStatus() !== "ready" }}
          ref={host}
        />
        <Show when={metricsChartStatus() === "ready"}>
          <div class="metrics-hint">Drag across the chart to zoom to a range.</div>
        </Show>
      </section>
    </Show>
  );
};

export default MetricsPanel;
