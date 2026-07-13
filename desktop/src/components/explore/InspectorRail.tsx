//! The Explore inspector rail (Phase 1): the mock's right-hand detail panel.
//! Shows the currently-selected result item. Logs get a purpose-built event
//! inspector (timestamp, message, fields, a trace jump, raw JSON); other
//! signals show a short prompt until their inspectors land.
//!
//! Reads the shared `selected` signal (set by clicking a log row). No props.

import { For, Show, type Component } from "solid-js";

import { fmtTs } from "../../format";
import { severity } from "../../severity";
import { selected, setSelected, setField, state } from "../../store";

/** Pull a trace id out of the selected log's attributes, if present. */
function traceIdOf(attrs: [string, string][]): string | null {
  for (const [k, v] of attrs) {
    if (k === "trace_id" || k === "traceId" || k === "trace.id") {
      return v.trim() === "" ? null : v;
    }
  }
  return null;
}

const InspectorRail: Component = () => {
  return (
    <aside class="inspector">
      <Show
        when={selected()}
        fallback={
          <div class="inspector-empty">
            <div class="inspector-empty-title">Inspector</div>
            <Show
              when={state.signal === "Logs"}
              fallback={<p>Row inspection is available for logs.</p>}
            >
              <p>Select a log line to see its fields, attributes, and raw event.</p>
            </Show>
          </div>
        }
      >
        {(sel) => {
          const s = sel();
          const sev = severity(s.sev);
          const ts = fmtTs(s.ts);
          const fields = [...s.labels, ...s.attrs];
          const traceId = traceIdOf(s.attrs);
          const raw = JSON.stringify(
            {
              ts: ts.full,
              severity: sev.label,
              body: s.body,
              labels: Object.fromEntries(s.labels),
              attributes: Object.fromEntries(s.attrs),
            },
            null,
            2,
          );
          return (
            <>
              <div class="inspector-head">
                <span class="inspector-title">Event</span>
                <span class={`log-sev ${sev.cls}`}>{sev.label}</span>
                <button
                  type="button"
                  class="inspector-close"
                  title="Clear selection"
                  onClick={() => setSelected(null)}
                >
                  ×
                </button>
              </div>

              <div class="inspector-body">
                <div class="insp-block">
                  <div class="insp-ts" title={ts.full}>
                    {ts.full || ts.short}
                  </div>
                  <div class="insp-msg">{s.body}</div>
                </div>

                <Show when={traceId}>
                  {(tid) => (
                    <button
                      type="button"
                      class="insp-trace-link"
                      title="Open this trace in the Traces tab"
                      onClick={() => {
                        setField("signal", "Traces");
                        setField("traceId", tid());
                      }}
                    >
                      Open trace {tid().slice(0, 8)}… →
                    </button>
                  )}
                </Show>

                <Show when={fields.length > 0}>
                  <div class="insp-fields">
                    <For each={fields}>
                      {([k, v]) => (
                        <div class="insp-field">
                          <span class="insp-field-k">{k}</span>
                          <span class="insp-field-v" title={v}>
                            {v}
                          </span>
                        </div>
                      )}
                    </For>
                  </div>
                </Show>

                <div class="insp-raw-h">Raw</div>
                <pre class="insp-raw">{raw}</pre>
              </div>
            </>
          );
        }}
      </Show>
    </aside>
  );
};

export default InspectorRail;
