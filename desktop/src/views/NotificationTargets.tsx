import { For, Match, Show, Switch, createEffect, createMemo, createSignal, onMount, type Component } from "solid-js";

import {
  EMPTY_NOTIFICATION_TARGET_DRAFT,
  draftFromNotificationTarget,
  notificationTargetFromDraft,
  notificationTargetStore,
  type NotificationTargetDraft,
  type NotificationTargetView,
} from "../notificationTargetStore";
import { inBrowser, state, targets } from "../store";

const NotificationTargets: Component = () => {
  const [editing, setEditing] = createSignal<NotificationTargetView | null | undefined>(undefined);
  const [draft, setDraft] = createSignal<NotificationTargetDraft>({ ...EMPTY_NOTIFICATION_TARGET_DRAFT });
  const [localError, setLocalError] = createSignal<string | null>(null);
  const capability = createMemo(() => targets().find((target) => target.id === state.target));
  const configured = () => capability()?.alerts === true;
  const canMutate = () => capability()?.alert_mutations === true;
  const isOpen = () => editing() !== undefined;
  const isEdit = () => editing() !== null && editing() !== undefined;
  const sorted = createMemo(() => [...notificationTargetStore.notificationTargets.targets].sort((a, b) => a.name.localeCompare(b.name)));

  const refresh = () => configured() ? notificationTargetStore.load(state.target) : Promise.resolve();
  onMount(() => { if (inBrowser) void refresh(); });
  let lastLoadKey = `${state.target}:${configured()}`;
  createEffect(() => {
    const target = state.target;
    const available = configured();
    const loadKey = `${target}:${available}`;
    if (inBrowser && loadKey !== lastLoadKey) {
      lastLoadKey = loadKey;
      setDraft({ ...EMPTY_NOTIFICATION_TARGET_DRAFT });
      setEditing(undefined);
      if (available) void notificationTargetStore.load(target);
    }
  });

  const field = <K extends keyof NotificationTargetDraft>(key: K, value: NotificationTargetDraft[K]) =>
    setDraft((current) => ({ ...current, [key]: value }));
  const startCreate = () => { setDraft({ ...EMPTY_NOTIFICATION_TARGET_DRAFT }); setLocalError(null); setEditing(null); };
  const startEdit = (target: NotificationTargetView) => { setDraft(draftFromNotificationTarget(target)); setLocalError(null); setEditing(target); };
  const build = () => {
    try { setLocalError(null); return notificationTargetFromDraft(draft(), editing() ?? undefined); }
    catch (error) { setLocalError(error instanceof Error ? error.message : String(error)); return null; }
  };
  const validate = async () => {
    const write = build();
    if (write) await notificationTargetStore.validate(state.target, write, editing()?.revision);
  };
  const save = async () => {
    const write = build();
    if (!write) return;
    if (await notificationTargetStore.save(state.target, write, editing() ?? undefined)) {
      // Secrets are write-only and must not remain in component state after use.
      field("secret", "");
      setEditing(undefined);
    }
  };
  const remove = async () => {
    const target = editing();
    if (!target || !window.confirm(`Delete notification target “${target.name}”? This cannot be undone.`)) return;
    if (await notificationTargetStore.remove(state.target, target)) setEditing(undefined);
  };

  return (
    <main class="alerts-view">
      <header class="alerts-toolbar">
        <div><h1>Notification targets</h1><p>HTTPS destinations and target-owned JSON formatting.</p></div>
        <div class="alerts-toolbar-actions">
          <button type="button" onClick={() => void refresh()} disabled={!configured() || notificationTargetStore.notificationTargets.loading}>Refresh</button>
          <button class="primary" type="button" onClick={startCreate} disabled={!configured() || !canMutate()}>New target</button>
        </div>
      </header>

      <Show when={!inBrowser}><div class="alerts-error">Notification target administration is available only in the browser UI.</div></Show>
      <Show when={inBrowser && !configured()}>
        <div class="alerts-admin-notice"><strong>Notification targets are not configured.</strong> The selected target does not advertise alerting capability.</div>
      </Show>
      <Show when={configured()}>
        <div class="alerts-admin-notice">
          <strong>{canMutate() ? "Shared administrator access." : "Read-only notification target access."}</strong>{" "}
          {canMutate() ? "Changes and test sends affect the whole deployment." : "This target or session does not advertise mutation capability."}
        </div>
      </Show>
      <Show when={notificationTargetStore.notificationTargets.conflict}>{(message) => (
        <div class="alerts-conflict" role="alert"><span>{message()}</span><button type="button" onClick={() => { setEditing(undefined); void refresh(); }}>Reload latest revision</button></div>
      )}</Show>
      <Show when={localError() ?? notificationTargetStore.notificationTargets.error}>{(message) => <div class="alerts-error" role="alert">{message()}</div>}</Show>

      <Show when={configured()}>
        <Switch>
          <Match when={notificationTargetStore.notificationTargets.loading && notificationTargetStore.notificationTargets.targets.length === 0}><div class="alerts-empty">Loading notification targets…</div></Match>
          <Match when={!isOpen() && notificationTargetStore.notificationTargets.targets.length === 0}>
            <div class="alerts-empty"><strong>No notification targets configured</strong><span>Create a signed HTTPS webhook target to deliver alerts.</span></div>
          </Match>
          <Match when={true}>
            <div class="alerts-layout">
              <section class="alerts-list" aria-label="Notification targets">
                <For each={sorted()}>{(target) => (
                  <button class="alert-row" classList={{ selected: editing()?.id === target.id }} type="button" onClick={() => canMutate() && startEdit(target)}>
                    <span class={`alert-status status-${target.enabled ? "inactive" : "disabled"}`}>{target.enabled ? "enabled" : "disabled"}</span>
                    <span class="alert-row-main"><strong>{target.name}</strong><small>Webhook · revision {target.revision} · {target.format.type === "builtin" ? target.format.format_id.replace("_", " ") : "custom JSON"}</small></span>
                    <span class="alert-row-value"><small>{target.secret_configured ? "Secret configured" : "No secret"}</small></span>
                  </button>
                )}</For>
              </section>

              <Show when={isOpen()}>
                <form class="alert-editor notification-target-editor" onSubmit={(event) => { event.preventDefault(); void save(); }}>
                  <header><div><h2>{isEdit() ? "Edit notification target" : "Create notification target"}</h2><Show when={editing()}>{(target) => <small>Revision {target().revision} · {target().id}</small>}</Show></div><button class="icon-button" type="button" aria-label="Close editor" onClick={() => { field("secret", ""); setEditing(undefined); }}>×</button></header>
                  <label class="wide">Name<input required maxlength="160" value={draft().name} onInput={(e) => field("name", e.currentTarget.value)} /></label>
                  <label class="wide">Public HTTPS webhook URL<input required type="url" value={draft().url} onInput={(e) => field("url", e.currentTarget.value)} /></label>
                  <label>Timeout (milliseconds)<input required type="number" min="1" step="1" value={draft().timeoutMs} onInput={(e) => field("timeoutMs", e.currentTarget.value)} /></label>
                  <label>Format<select value={draft().formatType} onChange={(e) => field("formatType", e.currentTarget.value as NotificationTargetDraft["formatType"])}><option value="builtin">Built-in</option><option value="custom_json">Custom JSON</option></select></label>
                  <Show when={draft().formatType === "builtin"} fallback={<label class="wide">Custom JSON template<textarea required rows="7" spellcheck={false} value={draft().template} onInput={(e) => field("template", e.currentTarget.value)} /></label>}>
                    <label class="wide">Built-in format<select value={draft().formatId} onChange={(e) => field("formatId", e.currentTarget.value as NotificationTargetDraft["formatId"])}><option value="generic_json">Generic JSON</option><option value="slack_compatible">Slack-compatible</option></select></label>
                  </Show>
                  <label class="wide">Public headers (JSON array of name/value entries)<textarea rows="5" spellcheck={false} value={draft().headersJson} onInput={(e) => field("headersJson", e.currentTarget.value)} /><small>Header values are stored and returned in plaintext. Do not put credentials or other secrets here.</small></label>
                  <Show when={isEdit()} fallback={<label class="wide">Signing secret (write-only)<input required type="password" autocomplete="new-password" value={draft().secret} onInput={(e) => field("secret", e.currentTarget.value)} /></label>}>
                    <label>Signing secret action<select value={draft().secretAction} onChange={(e) => { field("secretAction", e.currentTarget.value as NotificationTargetDraft["secretAction"]); field("secret", ""); }}><option value="unchanged">Keep existing</option><option value="set">Replace</option><option value="clear" disabled={draft().enabled}>Clear (disabled targets only)</option></select></label>
                    <Show when={draft().secretAction === "set"}><label>New signing secret (write-only)<input required type="password" autocomplete="new-password" value={draft().secret} onInput={(e) => field("secret", e.currentTarget.value)} /></label></Show>
                  </Show>
                  <label class="alert-enabled wide"><input type="checkbox" checked={draft().enabled} onChange={(e) => field("enabled", e.currentTarget.checked)} /> Enabled</label>
                  <Show when={notificationTargetStore.notificationTargets.validated}><div class="alert-valid wide">Definition is valid.</div></Show>
                  <Show when={notificationTargetStore.notificationTargets.preview}>{(preview) => <div class="notification-result wide"><strong>Scrubbed payload preview</strong><pre>{preview().body}</pre><small>SHA-256 {preview().body_sha256}; signatures and secret-derived headers are excluded.</small></div>}</Show>
                  <Show when={notificationTargetStore.notificationTargets.testResult}>{(result) => <div class="notification-result wide"><strong>Test: {result().outcome.replaceAll("_", " ")}</strong><small>{result().http_status ? `HTTP ${result().http_status} · ` : ""}{result().duration_ms} ms{result().error_class ? ` · ${result().error_class}` : ""}</small></div>}</Show>
                  <footer class="wide">
                    <Show when={isEdit()}><button class="danger-button" type="button" disabled={notificationTargetStore.notificationTargets.saving || !canMutate()} onClick={() => void remove()}>Delete</button><button type="button" disabled={notificationTargetStore.notificationTargets.saving || !canMutate()} onClick={() => void notificationTargetStore.preview(state.target, editing()!)}>Preview</button><button type="button" disabled={notificationTargetStore.notificationTargets.saving || !canMutate()} onClick={() => void notificationTargetStore.test(state.target, editing()!)}>Test send</button></Show>
                    <span /><button type="button" disabled={notificationTargetStore.notificationTargets.saving || !canMutate()} onClick={() => void validate()}>Validate</button><button class="primary" type="submit" disabled={notificationTargetStore.notificationTargets.saving || !canMutate()}>{notificationTargetStore.notificationTargets.saving ? "Saving…" : "Save target"}</button>
                  </footer>
                </form>
              </Show>
            </div>
          </Match>
        </Switch>
      </Show>
    </main>
  );
};

export default NotificationTargets;
