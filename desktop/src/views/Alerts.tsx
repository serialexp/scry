import {
  For,
  Match,
  Show,
  Switch,
  createEffect,
  createMemo,
  createSignal,
  onCleanup,
  onMount,
  type Component,
} from "solid-js";

import {
  EMPTY_DRAFT,
  alertStore,
  displayStatus,
  draftFromMonitor,
  monitorFromDraft,
  type AlertMonitor,
  type MonitorDraft,
} from "../alertStore";
import { inBrowser, state, targets } from "../store";

const POLL_MS = 30_000;

function dateTime(nanos: string): string {
  if (!nanos || nanos === "0") return "Never";
  return new Date(Number(BigInt(nanos) / 1_000_000n)).toLocaleString();
}

const Alerts: Component = () => {
  const [editing, setEditing] = createSignal<AlertMonitor | null | undefined>(undefined);
  const [draft, setDraft] = createSignal<MonitorDraft>({ ...EMPTY_DRAFT });
  const [localError, setLocalError] = createSignal<string | null>(null);
  const isOpen = () => editing() !== undefined;
  const isEdit = () => editing() !== null && editing() !== undefined;
  const targetCapability = createMemo(() => targets().find((target) => target.id === state.target));
  const canMutate = () => targetCapability()?.alert_mutations === true;
  const sorted = createMemo(() => [...alertStore.alerts.monitors].sort((a, b) => a.monitor.name.localeCompare(b.monitor.name)));

  const refresh = () => alertStore.load(state.target);

  onMount(() => {
    if (inBrowser) void refresh();
    const timer = window.setInterval(() => {
      if (inBrowser && !isOpen()) void refresh();
    }, POLL_MS);
    onCleanup(() => window.clearInterval(timer));
  });

  let lastTarget = state.target;
  createEffect(() => {
    const target = state.target;
    if (inBrowser && target !== lastTarget) {
      lastTarget = target;
      setEditing(undefined);
      void alertStore.load(target);
    }
  });

  const startCreate = () => {
    setDraft({ ...EMPTY_DRAFT });
    setLocalError(null);
    setEditing(null);
  };

  const startEdit = (monitor: AlertMonitor) => {
    setDraft(draftFromMonitor(monitor));
    setLocalError(null);
    setEditing(monitor);
  };

  const build = (): AlertMonitor | null => {
    try {
      setLocalError(null);
      return monitorFromDraft(draft(), state.target, editing() ?? undefined);
    } catch (error) {
      setLocalError(error instanceof Error ? error.message : String(error));
      return null;
    }
  };

  const validate = async () => {
    const monitor = build();
    if (monitor) await alertStore.validate(state.target, monitor);
  };

  const save = async () => {
    const monitor = build();
    if (!monitor) return;
    const expectedRevision = editing()?.revision;
    if (await alertStore.save(state.target, monitor, expectedRevision)) setEditing(undefined);
  };

  const remove = async () => {
    const monitor = editing();
    if (!monitor || !window.confirm(`Delete “${monitor.name}”? This cannot be undone.`)) return;
    if (await alertStore.remove(state.target, monitor)) setEditing(undefined);
  };

  const field = <K extends keyof MonitorDraft>(key: K, value: MonitorDraft[K]) =>
    setDraft((current) => ({ ...current, [key]: value }));

  return (
    <main class="alerts-view">
      <header class="alerts-toolbar">
        <div>
          <h1>Alerts</h1>
          <p>Restricted scalar monitors evaluated by the selected target.</p>
        </div>
        <div class="alerts-toolbar-actions">
          <button type="button" onClick={() => void refresh()} disabled={alertStore.alerts.loading || !inBrowser}>
            {alertStore.alerts.loading ? "Refreshing…" : "Refresh"}
          </button>
          <button class="primary" type="button" onClick={startCreate} disabled={!inBrowser || !state.target || !canMutate()}>New monitor</button>
        </div>
      </header>

      <div class="alerts-admin-notice">
        <strong>{canMutate() ? "Shared administrator access." : "Read-only alert access."}</strong>{" "}
        {canMutate()
          ? "Every signed-in browser session can create, edit, enable, disable, and delete all monitors. Changes affect the whole deployment."
          : "Alert mutations require HTTPS with Secure session cookies; this target or session does not advertise mutation capability."}
      </div>

      <Show when={!inBrowser}>
        <div class="alerts-error">Alert administration is available in the browser UI through its same-origin control API.</div>
      </Show>
      <Show when={alertStore.alerts.conflict}>
        {(message) => (
          <div class="alerts-conflict" role="alert">
            <span>{message()}</span>
            <button type="button" onClick={() => { setEditing(undefined); void refresh(); }}>Reload latest revision</button>
          </div>
        )}
      </Show>
      <Show when={localError() ?? alertStore.alerts.error}>
        {(message) => <div class="alerts-error" role="alert">{message()}</div>}
      </Show>

      <Switch>
        <Match when={alertStore.alerts.loading && alertStore.alerts.monitors.length === 0}>
          <div class="alerts-empty">Loading monitors…</div>
        </Match>
        <Match when={!isOpen() && alertStore.alerts.monitors.length === 0}>
          <div class="alerts-empty">
            <strong>No monitors configured</strong>
            <span>Create a restricted scalar monitor to start evaluating telemetry.</span>
          </div>
        </Match>
        <Match when={true}>
          <div class="alerts-layout">
            <section class="alerts-list" aria-label="Alert monitors">
              <For each={sorted()}>{(summary) => {
                const status = () => displayStatus(summary);
                return (
                  <button class="alert-row" classList={{ selected: editing()?.id === summary.monitor.id }} type="button" onClick={() => canMutate() && startEdit(summary.monitor)}>
                    <span class={`alert-status status-${status()}`}>{status().replace("_", " ")}</span>
                    <span class="alert-row-main">
                      <strong>{summary.monitor.name}</strong>
                      <small>{summary.monitor.query.signal} · every {summary.monitor.every_seconds}s · revision {summary.monitor.revision}</small>
                    </span>
                    <span class="alert-row-value">
                      <Show when={summary.state?.last_value !== null && summary.state?.last_value !== undefined} fallback="—">
                        {summary.state?.last_value}
                      </Show>
                      <small>{summary.state ? dateTime(summary.state.last_evaluated_at_unix_nano) : "Not evaluated"}</small>
                    </span>
                  </button>
                );
              }}</For>
            </section>

            <Show when={isOpen()}>
              <form class="alert-editor" onSubmit={(event) => { event.preventDefault(); void save(); }}>
                <header>
                  <div>
                    <h2>{isEdit() ? "Edit monitor" : "Create monitor"}</h2>
                    <Show when={editing()}>{(monitor) => <small>Revision {monitor().revision} · {monitor().id}</small>}</Show>
                  </div>
                  <button class="icon-button" type="button" aria-label="Close editor" onClick={() => setEditing(undefined)}>×</button>
                </header>

                <label class="wide">Name<input required maxlength="160" value={draft().name} onInput={(e) => field("name", e.currentTarget.value)} /></label>
                <label>Signal<select value={draft().signal} onChange={(e) => field("signal", e.currentTarget.value as MonitorDraft["signal"])}>
                  <option value="metrics">Metrics</option><option value="logs">Logs</option><option value="traces">Traces</option><option value="profiles">Profiles</option>
                </select></label>
                <label>Lookback (seconds)<input type="number" min="1" step="1" value={draft().lookbackSeconds} onInput={(e) => field("lookbackSeconds", e.currentTarget.value)} /></label>
                <label class="wide">Restricted scalar SQL<textarea required maxlength="16384" rows="5" spellcheck={false} value={draft().sql} onInput={(e) => field("sql", e.currentTarget.value)} /></label>
                <label>Condition<select value={draft().comparator} onChange={(e) => field("comparator", e.currentTarget.value as MonitorDraft["comparator"])}>
                  <option value="gt">greater than</option><option value="gte">greater than or equal</option><option value="lt">less than</option><option value="lte">less than or equal</option><option value="eq">equal</option><option value="ne">not equal</option>
                </select></label>
                <label>Threshold<input type="number" step="any" value={draft().threshold} onInput={(e) => field("threshold", e.currentTarget.value)} /></label>
                <label>Evaluate every (seconds)<input type="number" min="1" step="1" value={draft().everySeconds} onInput={(e) => field("everySeconds", e.currentTarget.value)} /></label>
                <label>Jitter (seconds)<input type="number" min="0" step="1" value={draft().jitterSeconds} onInput={(e) => field("jitterSeconds", e.currentTarget.value)} /></label>
                <label>Fire after (seconds)<input type="number" min="0" step="1" value={draft().forSeconds} onInput={(e) => field("forSeconds", e.currentTarget.value)} /></label>
                <label>Recover after (seconds)<input type="number" min="0" step="1" value={draft().recoverForSeconds} onInput={(e) => field("recoverForSeconds", e.currentTarget.value)} /></label>
                <label>No data<select value={draft().noData} onChange={(e) => field("noData", e.currentTarget.value as MonitorDraft["noData"])}><option value="no_data">Show no data</option><option value="firing">Fire</option></select></label>
                <label>Execution error<select value={draft().executionError} onChange={(e) => field("executionError", e.currentTarget.value as MonitorDraft["executionError"])}><option value="error">Show error</option><option value="keep_last">Keep last (stale)</option><option value="firing">Fire</option></select></label>
                <label class="alert-enabled wide"><input type="checkbox" checked={draft().enabled} onChange={(e) => field("enabled", e.currentTarget.checked)} /> Enabled</label>

                <Show when={alertStore.alerts.validated}><div class="alert-valid wide">Definition is valid.</div></Show>
                <footer class="wide">
                  <Show when={isEdit()}><button class="danger-button" type="button" disabled={alertStore.alerts.saving || !canMutate()} onClick={() => void remove()}>Delete</button></Show>
                  <span />
                  <button type="button" disabled={alertStore.alerts.saving || !canMutate()} onClick={() => void validate()}>Validate</button>
                  <button class="primary" type="submit" disabled={alertStore.alerts.saving || !canMutate()}>{alertStore.alerts.saving ? "Saving…" : "Save monitor"}</button>
                </footer>
              </form>
            </Show>
          </div>
        </Match>
      </Switch>
    </main>
  );
};

export default Alerts;
