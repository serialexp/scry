//! Single-trace waterfall view.
//!
//! Rendered by `ResultsTable` when a result is a by-id trace lookup (one
//! distinct `trace_id`). Spans are laid out top-down as a call tree (pre-order,
//! indented by depth) with a duration bar positioned against the trace's
//! [t0, t1] window. A per-span expander shows attributes, events, and links; a
//! `raw` toggle drops back to the generic column table.

import { For, Show, createMemo, createSignal, type Component } from "solid-js";

import { fmtDuration, fmtTs } from "../format";
import {
  kindLabel,
  serviceHue,
  statusLabel,
  traceWindow,
  type PlacedSpan,
  type SpanLayout,
} from "../traces";

export interface TraceRow {
  span: PlacedSpan;
  layout: SpanLayout;
}

export interface TraceData {
  traceId: string;
  rows: TraceRow[];
  shown: number;
  total: number;
}

interface Props {
  data: () => TraceData;
  raw: () => boolean;
  setRaw: (v: boolean) => void;
}

const TracesView: Component<Props> = (props) => {
  const [filter, setFilter] = createSignal("");

  const durationNs = createMemo(() => {
    const { t0, t1 } = traceWindow(props.data().rows.map((r) => r.span));
    return t1 - t0;
  });

  const rootService = createMemo(() => {
    const top = props.data().rows.find((r) => r.span.depth === 0);
    return top?.span.service ?? "";
  });

  const filteredRows = createMemo(() => {
    const q = filter().trim().toLowerCase();
    const rows = props.data().rows;
    if (q === "") return rows;
    const hit = (k: string, v: string) =>
      k.toLowerCase().includes(q) || v.toLowerCase().includes(q);
    return rows.filter((r) => {
      const s = r.span;
      return (
        s.name.toLowerCase().includes(q) ||
        s.service.toLowerCase().includes(q) ||
        s.attrs.some(([k, v]) => hit(k, v)) ||
        s.resourceLabels.some(([k, v]) => hit(k, v))
      );
    });
  });

  return (
    <>
      <div class="results-meta">
        <span>
          <strong>{props.data().total.toLocaleString()}</strong> spans
        </span>
        <span class="trace-id" title={props.data().traceId}>
          {props.data().traceId}
        </span>
        <span>{fmtDuration(durationNs())}</span>
        <Show when={rootService()}>
          <span class="trace-root-svc">{rootService()}</span>
        </Show>
        <input
          class="log-search"
          type="search"
          placeholder="filter spans…"
          value={filter()}
          onInput={(e) => setFilter(e.currentTarget.value)}
        />
        <span>{filteredRows().length.toLocaleString()} shown</span>
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

      <div class="trace-list">
        <For each={filteredRows()}>
          {(r) => {
            const s = r.span;
            const st = statusLabel(s.statusCode);
            const hue = serviceHue(s.service);
            const extra = s.attrs.length + s.events.length + s.links.length;
            const dur = fmtDuration(s.end - s.start);
            return (
              <details class={`trace-span status-${st.cls}`}>
                <summary>
                  <div class="trace-label" style={{ "padding-left": `${s.depth * 16}px` }}>
                    <span class="trace-kind">{kindLabel(s.kind)}</span>
                    <span class="trace-name">{s.name}</span>
                    <span class="chip trace-svc" style={{ "--hue": String(hue) }}>
                      {s.service}
                    </span>
                    <Show when={st.label}>
                      <span class={`trace-status ${st.cls}`}>{st.label}</span>
                    </Show>
                  </div>
                  <div class="trace-track">
                    <div
                      class="trace-bar"
                      style={{
                        left: `${(r.layout.leftFrac * 100).toFixed(3)}%`,
                        width: `${(r.layout.widthFrac * 100).toFixed(3)}%`,
                        background: `hsl(${hue}, 55%, 52%)`,
                      }}
                    />
                    <span class="trace-dur">{dur}</span>
                  </div>
                </summary>

                <div class="trace-detail">
                  <div class="trace-ids">
                    <span class="chip"><b>span</b><span>{s.spanId}</span></span>
                    <Show when={s.parentSpanId}>
                      <span class="chip"><b>parent</b><span>{s.parentSpanId}</span></span>
                    </Show>
                    <span class="chip"><b>start</b><span>{fmtTs(s.start).full}</span></span>
                  </div>
                  <Show when={s.statusMessage}>
                    <div class="trace-status-msg">{s.statusMessage}</div>
                  </Show>
                  <Show when={s.attrs.length > 0}>
                    <div class="trace-section">
                      <div class="trace-section-h">attributes</div>
                      <div class="trace-chips">
                        <For each={s.attrs}>
                          {([k, v]) => (
                            <span class="chip attr"><b>{k}</b><span>{v}</span></span>
                          )}
                        </For>
                      </div>
                    </div>
                  </Show>
                  <Show when={s.events.length > 0}>
                    <div class="trace-section">
                      <div class="trace-section-h">events</div>
                      <For each={s.events}>
                        {(ev) => (
                          <div class="trace-event">
                            <span class="trace-ts" title={fmtTs(ev.ts).full}>
                              {fmtTs(ev.ts).short}
                            </span>
                            <span class="trace-event-name">{ev.name}</span>
                            <span class="trace-chips">
                              <For each={ev.attrs}>
                                {([k, v]) => (
                                  <span class="chip attr"><b>{k}</b><span>{v}</span></span>
                                )}
                              </For>
                            </span>
                          </div>
                        )}
                      </For>
                    </div>
                  </Show>
                  <Show when={s.links.length > 0}>
                    <div class="trace-section">
                      <div class="trace-section-h">links</div>
                      <For each={s.links}>
                        {(ln) => (
                          <div class="trace-link">
                            <span class="chip"><b>trace</b><span>{ln.traceId}</span></span>
                            <span class="chip"><b>span</b><span>{ln.spanId}</span></span>
                            <For each={ln.attrs}>
                              {([k, v]) => (
                                <span class="chip attr"><b>{k}</b><span>{v}</span></span>
                              )}
                            </For>
                          </div>
                        )}
                      </For>
                    </div>
                  </Show>
                  <Show when={extra === 0}>
                    <div class="trace-empty">no attributes, events, or links</div>
                  </Show>
                </div>
              </details>
            );
          }}
        </For>
      </div>
    </>
  );
};

export default TracesView;
