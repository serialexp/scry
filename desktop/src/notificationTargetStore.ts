import { createStore, produce } from "solid-js/store";

export type NotificationFormatId = "generic_json" | "slack_compatible";

export interface NotificationHeader {
  name: string;
  value: string;
}

export type NotificationFormat =
  | { type: "builtin"; format_id: NotificationFormatId }
  | { type: "custom_json"; template: string };

export interface NotificationTargetView {
  schema_version: 1;
  id: string;
  revision: string;
  name: string;
  enabled: boolean;
  kind: { type: "webhook"; url: string };
  timeout_ms: number;
  headers: NotificationHeader[];
  format: NotificationFormat;
  secret_configured: boolean;
  created_at_unix_nano: string;
  updated_at_unix_nano: string;
}

export type NotificationSecretWrite =
  | { action: "set"; value: string }
  | { action: "unchanged" }
  | { action: "clear" };

export interface NotificationTargetWrite {
  schema_version: 1;
  name: string;
  enabled: boolean;
  kind: { type: "webhook"; url: string };
  timeout_ms: number;
  headers: NotificationHeader[];
  format: NotificationFormat;
  secret: NotificationSecretWrite;
}

export interface NotificationPreview {
  content_type: "application/json";
  body: string;
  body_sha256: string;
}

export interface NotificationTestResult {
  event_id: string;
  outcome: "accepted" | "retryable_failure" | "permanent_failure";
  http_status: number | null;
  error_class: string | null;
  duration_ms: number;
}

export interface NotificationTargetDraft {
  name: string;
  enabled: boolean;
  url: string;
  timeoutMs: string;
  headersJson: string;
  formatType: "builtin" | "custom_json";
  formatId: NotificationFormatId;
  template: string;
  secretAction: NotificationSecretWrite["action"];
  secret: string;
}

export const EMPTY_NOTIFICATION_TARGET_DRAFT: NotificationTargetDraft = {
  name: "",
  enabled: true,
  url: "https://",
  timeoutMs: "10000",
  headersJson: "[]",
  formatType: "builtin",
  formatId: "generic_json",
  template: "{\n  \"text\": \"{{transition}}: {{monitor_name}}\"\n}",
  secretAction: "set",
  secret: "",
};

export class NotificationTargetApiError extends Error {
  constructor(
    message: string,
    readonly status: number,
    readonly conflict = false,
  ) {
    super(message);
    this.name = "NotificationTargetApiError";
  }
}

type Fetch = typeof globalThis.fetch;
const BASE = "/api/v1/alerts/notification-targets";

function targetHeaders(targetId: string): Headers {
  const headers = new Headers();
  if (targetId) headers.set("x-scry-target", targetId);
  return headers;
}

function redactedTarget(value: NotificationTargetView): NotificationTargetView {
  const record = value as NotificationTargetView & Record<string, unknown>;
  if ("secret" in record || "encrypted_secret" in record || "ciphertext" in record) {
    throw new NotificationTargetApiError("Notification target response unexpectedly contained secret material", 502);
  }
  return value;
}

async function responseError(response: Response): Promise<NotificationTargetApiError> {
  let message = `Notification target API request failed (${response.status})`;
  const text = await response.text();
  if (text) {
    try {
      const body = JSON.parse(text) as { error?: string; message?: string };
      message = body.error ?? body.message ?? message;
    } catch {
      message = text;
    }
  }
  return new NotificationTargetApiError(message, response.status, response.status === 409 || response.status === 412);
}

export class NotificationTargetApiClient {
  constructor(private readonly fetcher: Fetch = globalThis.fetch) {}

  private async csrf(): Promise<string> {
    // Deliberately fetch a nonce for every POST/PUT/DELETE. This client survives
    // logout/login, so retaining a nonce across mutations would cross sessions.
    const response = await this.fetcher("/api/csrf", { credentials: "same-origin" });
    if (!response.ok) throw await responseError(response);
    const body = (await response.json()) as { csrfToken?: string; csrf_token?: string };
    const token = body.csrfToken ?? body.csrf_token;
    if (!token) throw new NotificationTargetApiError("CSRF response did not contain a token", 500);
    return token;
  }

  private async mutationHeaders(targetId: string, revision = "0"): Promise<Headers> {
    const headers = targetHeaders(targetId);
    headers.set("content-type", "application/json");
    headers.set("x-scry-csrf", await this.csrf());
    headers.set("idempotency-key", crypto.randomUUID());
    headers.set("if-match", `"${revision}"`);
    return headers;
  }

  async list(targetId: string): Promise<NotificationTargetView[]> {
    const targets: NotificationTargetView[] = [];
    const seenCursors = new Set<string>();
    let next: string | null = null;
    do {
      const query = new URLSearchParams({ limit: "500" });
      if (next) query.set("after", next);
      const response = await this.fetcher(`${BASE}?${query}`, {
        credentials: "same-origin",
        headers: targetHeaders(targetId),
      });
      if (!response.ok) throw await responseError(response);
      const body = (await response.json()) as { targets: NotificationTargetView[]; next?: string | null } | NotificationTargetView[];
      if (Array.isArray(body)) {
        targets.push(...body.map(redactedTarget));
        next = null;
      } else {
        targets.push(...body.targets.map(redactedTarget));
        next = body.next ?? null;
      }
      if (targets.length > 10_000) throw new NotificationTargetApiError("Notification target list exceeds the browser safety limit", 507);
      if (next && seenCursors.has(next)) throw new NotificationTargetApiError("Notification target list returned a repeated cursor", 502);
      if (next) seenCursors.add(next);
    } while (next);
    return targets;
  }

  private async postResult<T>(targetId: string, path: string, body: NotificationTargetWrite | undefined, revision = "0"): Promise<T> {
    const response = await this.fetcher(`${BASE}${path}`, {
      method: "POST",
      credentials: "same-origin",
      headers: await this.mutationHeaders(targetId, revision),
      ...(body === undefined ? {} : { body: JSON.stringify(body) }),
    });
    if (!response.ok) throw await responseError(response);
    return (await response.json()) as T;
  }

  async validate(targetId: string, write: NotificationTargetWrite, revision = "0"): Promise<void> {
    const response = await this.fetcher(`${BASE}/validate`, {
      method: "POST",
      credentials: "same-origin",
      headers: await this.mutationHeaders(targetId, revision),
      body: JSON.stringify(write),
    });
    if (!response.ok) throw await responseError(response);
  }

  async create(targetId: string, write: NotificationTargetWrite): Promise<NotificationTargetView> {
    return redactedTarget(await this.postResult<NotificationTargetView>(targetId, "", write));
  }

  async update(targetId: string, id: string, write: NotificationTargetWrite, revision: string): Promise<NotificationTargetView> {
    const response = await this.fetcher(`${BASE}/${encodeURIComponent(id)}`, {
      method: "PUT", credentials: "same-origin", headers: await this.mutationHeaders(targetId, revision), body: JSON.stringify(write),
    });
    if (!response.ok) throw await responseError(response);
    return redactedTarget((await response.json()) as NotificationTargetView);
  }

  async remove(targetId: string, id: string, revision: string): Promise<void> {
    const response = await this.fetcher(`${BASE}/${encodeURIComponent(id)}`, {
      method: "DELETE", credentials: "same-origin", headers: await this.mutationHeaders(targetId, revision),
    });
    if (!response.ok) throw await responseError(response);
  }

  preview(targetId: string, id: string, revision: string): Promise<NotificationPreview> {
    return this.postResult(targetId, `/${encodeURIComponent(id)}/preview`, undefined, revision);
  }

  test(targetId: string, id: string, revision: string): Promise<NotificationTestResult> {
    return this.postResult(targetId, `/${encodeURIComponent(id)}/test`, undefined, revision);
  }
}

function parseHeaders(text: string): NotificationHeader[] {
  let parsed: unknown;
  try { parsed = JSON.parse(text); } catch { throw new Error("Headers must be valid JSON"); }
  if (Array.isArray(parsed)) {
    if (!parsed.every((value) => value && typeof value === "object" && typeof value.name === "string" && typeof value.value === "string")) {
      throw new Error("Header array entries must contain string name and value fields");
    }
    return parsed as NotificationHeader[];
  }
  if (!parsed || typeof parsed !== "object") throw new Error("Headers must be a JSON object or header array");
  return Object.entries(parsed).map(([name, value]) => {
    if (typeof value !== "string") throw new Error(`Header ${name} must have a string value`);
    return { name, value };
  });
}

export function notificationTargetFromDraft(draft: NotificationTargetDraft, existing?: NotificationTargetView): NotificationTargetWrite {
  const name = draft.name.trim();
  if (!name) throw new Error("Name is required");
  let url: URL;
  try { url = new URL(draft.url.trim()); } catch { throw new Error("Webhook URL must be a valid absolute URL"); }
  if (url.protocol !== "https:") throw new Error("Webhook URL must use public HTTPS");
  if (url.username || url.password || url.hash) throw new Error("Webhook URL cannot contain credentials or a fragment");
  const timeout = Number(draft.timeoutMs);
  if (!Number.isSafeInteger(timeout) || timeout <= 0) throw new Error("Timeout must be a positive whole number of milliseconds");
  const format: NotificationFormat = draft.formatType === "builtin"
    ? { type: "builtin", format_id: draft.formatId }
    : { type: "custom_json", template: draft.template };
  if (format.type === "custom_json") {
    if (!format.template.trim()) throw new Error("Custom JSON template is required");
    try { JSON.parse(format.template); } catch { throw new Error("Custom template must be valid JSON"); }
  }
  let secret: NotificationSecretWrite;
  if (!existing && draft.secretAction !== "set") throw new Error("A signing secret is required for a new target");
  if (draft.secretAction === "set") {
    if (!draft.secret) throw new Error("Signing secret cannot be empty when setting it");
    secret = { action: "set", value: draft.secret };
  } else {
    secret = { action: draft.secretAction };
  }
  if (draft.enabled && secret.action === "clear") {
    throw new Error("Disable the notification target before clearing its signing secret");
  }
  return { schema_version: 1, name, enabled: draft.enabled, kind: { type: "webhook", url: url.toString() }, timeout_ms: timeout, headers: parseHeaders(draft.headersJson), format, secret };
}

export function draftFromNotificationTarget(target: NotificationTargetView): NotificationTargetDraft {
  return {
    name: target.name, enabled: target.enabled, url: target.kind.url, timeoutMs: String(target.timeout_ms),
    headersJson: JSON.stringify(target.headers, null, 2),
    formatType: target.format.type, formatId: target.format.type === "builtin" ? target.format.format_id : "generic_json",
    template: target.format.type === "custom_json" ? target.format.template : EMPTY_NOTIFICATION_TARGET_DRAFT.template,
    secretAction: "unchanged", secret: "",
  };
}

interface NotificationTargetState {
  targets: NotificationTargetView[];
  loading: boolean;
  saving: boolean;
  error: string | null;
  conflict: string | null;
  validated: boolean;
  preview: NotificationPreview | null;
  testResult: NotificationTestResult | null;
}

export function createNotificationTargetStore(client = new NotificationTargetApiClient()) {
  const [notificationTargets, setNotificationTargets] = createStore<NotificationTargetState>({
    targets: [], loading: false, saving: false, error: null, conflict: null, validated: false, preview: null, testResult: null,
  });
  let activeTarget: string | undefined;
  let generation = 0;
  const isCurrent = (targetId: string, operationGeneration: number) => generation === operationGeneration && (activeTarget === undefined || activeTarget === targetId);
  const fail = (error: unknown) => {
    const api = error instanceof NotificationTargetApiError ? error : null;
    const message = error instanceof Error ? error.message : String(error);
    setNotificationTargets({ error: message, conflict: api?.conflict ? `${message}. Reload before applying your changes again.` : null });
  };
  const beginAction = () => setNotificationTargets({ saving: true, error: null, conflict: null, validated: false, preview: null, testResult: null });

  const load = async (targetId: string) => {
    const changed = activeTarget !== undefined && activeTarget !== targetId;
    activeTarget = targetId;
    const operationGeneration = ++generation;
    setNotificationTargets({ ...(changed ? { targets: [] } : {}), loading: true, saving: false, error: null, conflict: null, validated: false, preview: null, testResult: null });
    try {
      const targets = await client.list(targetId);
      if (isCurrent(targetId, operationGeneration)) setNotificationTargets({ targets, loading: false });
    } catch (error) {
      if (isCurrent(targetId, operationGeneration)) { fail(error); setNotificationTargets("loading", false); }
    }
  };

  const run = async <T>(targetId: string, operation: () => Promise<T>, apply: (result: T) => void): Promise<boolean> => {
    // Every operation supersedes earlier work, including another mutation on the
    // same selected target. This prevents late preview/test/save responses from
    // replacing newer state rather than merely guarding target switches.
    const operationGeneration = ++generation;
    beginAction();
    try {
      const result = await operation();
      if (!isCurrent(targetId, operationGeneration)) return false;
      apply(result);
      setNotificationTargets("saving", false);
      return true;
    } catch (error) {
      if (!isCurrent(targetId, operationGeneration)) return false;
      fail(error); setNotificationTargets("saving", false); return false;
    }
  };

  const validate = (targetId: string, write: NotificationTargetWrite, revision?: string) => run(targetId, () => client.validate(targetId, write, revision), () => setNotificationTargets("validated", true));
  const save = (targetId: string, write: NotificationTargetWrite, existing?: NotificationTargetView) => run(
    targetId,
    () => existing ? client.update(targetId, existing.id, write, existing.revision) : client.create(targetId, write),
    (saved) => setNotificationTargets(produce((state) => {
      const index = state.targets.findIndex((target) => target.id === saved.id);
      if (index < 0) state.targets.unshift(saved); else state.targets[index] = saved;
    })),
  );
  const remove = (targetId: string, target: NotificationTargetView) => run(targetId, () => client.remove(targetId, target.id, target.revision), () => setNotificationTargets("targets", (targets) => targets.filter((entry) => entry.id !== target.id)));
  const preview = (targetId: string, target: NotificationTargetView) => run(targetId, () => client.preview(targetId, target.id, target.revision), (value) => setNotificationTargets("preview", value));
  const test = (targetId: string, target: NotificationTargetView) => run(targetId, () => client.test(targetId, target.id, target.revision), (value) => setNotificationTargets("testResult", value));

  return { notificationTargets, load, validate, save, remove, preview, test };
}

export const notificationTargetStore = createNotificationTargetStore();
