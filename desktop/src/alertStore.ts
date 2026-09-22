import { createStore, produce } from "solid-js/store";

export type AlertSignal = "metrics" | "logs" | "traces" | "profiles";
export type AlertComparator = "lt" | "lte" | "gt" | "gte" | "eq" | "ne";
export type NoDataPolicy = "no_data" | "firing";
export type ExecutionErrorPolicy = "keep_last" | "error" | "firing";
export type AlertStatus =
  | "inactive"
  | "pending"
  | "firing"
  | "recovering"
  | "no_data"
  | "error"
  | "disabled";

export interface EqualityMatcher {
  name: string;
  value: string;
}

export interface AlertMonitor {
  schema_version: number;
  id: string;
  revision: string;
  name: string;
  enabled: boolean;
  query: {
    target_id: string;
    signal: AlertSignal;
    matchers: EqualityMatcher[];
    lookback_seconds: number;
    sql: string;
  };
  condition: { comparator: AlertComparator; threshold: number };
  every_seconds: number;
  jitter_seconds: number;
  for_seconds: number;
  recover_for_seconds: number;
  no_data: NoDataPolicy;
  execution_error: ExecutionErrorPolicy;
  labels: [string, string][];
  annotations: [string, string][];
  created_at_unix_nano: string;
  updated_at_unix_nano: string;
}

export interface AlertInstanceState {
  monitor_revision: string;
  status: AlertStatus;
  since_unix_nano: string;
  last_evaluated_at_unix_nano: string;
  last_slot_id: string;
  last_value: number | null;
  last_error_class: string | null;
  transition_sequence: string;
  stale: boolean;
}

export interface MonitorSummary {
  monitor: AlertMonitor;
  state: AlertInstanceState | null;
}

export interface MonitorDraft {
  name: string;
  enabled: boolean;
  signal: AlertSignal;
  sql: string;
  lookbackSeconds: string;
  comparator: AlertComparator;
  threshold: string;
  everySeconds: string;
  jitterSeconds: string;
  forSeconds: string;
  recoverForSeconds: string;
  noData: NoDataPolicy;
  executionError: ExecutionErrorPolicy;
}

export const EMPTY_DRAFT: MonitorDraft = {
  name: "",
  enabled: true,
  signal: "metrics",
  sql: "",
  lookbackSeconds: "300",
  comparator: "gt",
  threshold: "0",
  everySeconds: "60",
  jitterSeconds: "0",
  forSeconds: "0",
  recoverForSeconds: "0",
  noData: "no_data",
  executionError: "error",
};

export class AlertApiError extends Error {
  constructor(
    message: string,
    readonly status: number,
    readonly conflict = false,
  ) {
    super(message);
    this.name = "AlertApiError";
  }
}

type Fetch = typeof globalThis.fetch;

function endpoint(path: string): string {
  return `/api/v1/alerts${path}`;
}

function targetHeaders(targetId: string): Headers {
  const headers = new Headers();
  if (targetId) headers.set("x-scry-target", targetId);
  return headers;
}

async function responseError(response: Response): Promise<AlertApiError> {
  let message = `Alert API request failed (${response.status})`;
  const text = await response.text();
  if (text) {
    try {
      const body = JSON.parse(text) as { error?: string; message?: string };
      message = body.error ?? body.message ?? message;
    } catch {
      message = text;
    }
  }
  return new AlertApiError(message, response.status, response.status === 409 || response.status === 412);
}

export class AlertApiClient {
  constructor(private readonly fetcher: Fetch = globalThis.fetch) {}

  private async csrf(): Promise<string> {
    // Fetch per mutation. A cached nonce would outlive logout/login because this
    // module-level client survives session changes.
    const response = await this.fetcher("/api/csrf", { credentials: "same-origin" });
    if (!response.ok) throw await responseError(response);
    const body = (await response.json()) as { csrfToken?: string; csrf_token?: string };
    const token = body.csrfToken ?? body.csrf_token;
    if (!token) throw new AlertApiError("CSRF response did not contain a token", 500);
    return token;
  }

  private async mutationHeaders(targetId: string, expectedRevision = "0"): Promise<Headers> {
    const headers = targetHeaders(targetId);
    headers.set("content-type", "application/json");
    headers.set("x-scry-csrf", await this.csrf());
    headers.set("Idempotency-Key", crypto.randomUUID());
    headers.set("If-Match", `"${expectedRevision}"`);
    return headers;
  }

  async list(targetId: string): Promise<MonitorSummary[]> {
    const monitors: MonitorSummary[] = [];
    let next: string | null = null;
    do {
      const query = new URLSearchParams({ limit: "500" });
      if (next) query.set("after", next);
      const response = await this.fetcher(`${endpoint("/monitors")}?${query}`, {
        credentials: "same-origin",
        headers: targetHeaders(targetId),
      });
      if (!response.ok) throw await responseError(response);
      const body = (await response.json()) as { monitors: MonitorSummary[]; next?: string | null } | MonitorSummary[];
      if (Array.isArray(body)) {
        monitors.push(...body);
        next = null;
      } else {
        monitors.push(...body.monitors);
        next = body.next ?? null;
      }
      if (monitors.length > 10_000) {
        throw new AlertApiError("Monitor list exceeds the browser safety limit", 507);
      }
    } while (next);
    return monitors;
  }

  async validate(targetId: string, monitor: AlertMonitor): Promise<void> {
    const response = await this.fetcher(endpoint("/monitors/validate"), {
      method: "POST",
      credentials: "same-origin",
      headers: await this.mutationHeaders(targetId),
      body: JSON.stringify(monitor),
    });
    if (!response.ok) throw await responseError(response);
  }

  async create(targetId: string, monitor: AlertMonitor): Promise<MonitorSummary> {
    const response = await this.fetcher(endpoint("/monitors"), {
      method: "POST",
      credentials: "same-origin",
      headers: await this.mutationHeaders(targetId),
      body: JSON.stringify(monitor),
    });
    if (!response.ok) throw await responseError(response);
    return (await response.json()) as MonitorSummary;
  }

  async update(targetId: string, monitor: AlertMonitor, expectedRevision: string): Promise<MonitorSummary> {
    const response = await this.fetcher(endpoint(`/monitors/${encodeURIComponent(monitor.id)}`), {
      method: "PUT",
      credentials: "same-origin",
      headers: await this.mutationHeaders(targetId, expectedRevision),
      body: JSON.stringify(monitor),
    });
    if (!response.ok) throw await responseError(response);
    return (await response.json()) as MonitorSummary;
  }

  async remove(targetId: string, id: string, expectedRevision: string): Promise<void> {
    const response = await this.fetcher(endpoint(`/monitors/${encodeURIComponent(id)}`), {
      method: "DELETE",
      credentials: "same-origin",
      headers: await this.mutationHeaders(targetId, expectedRevision),
    });
    if (!response.ok) throw await responseError(response);
  }
}

function positiveInteger(value: string, label: string, allowZero = false): number {
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < (allowZero ? 0 : 1)) {
    throw new Error(`${label} must be ${allowZero ? "a non-negative" : "a positive"} whole number`);
  }
  return parsed;
}

export function monitorFromDraft(
  draft: MonitorDraft,
  targetId: string,
  current?: AlertMonitor,
  nowMs = Date.now(),
  id: string = crypto.randomUUID(),
): AlertMonitor {
  const threshold = Number(draft.threshold);
  if (!draft.name.trim()) throw new Error("Name is required");
  if (!targetId) throw new Error("Select a query target");
  if (!draft.sql.trim()) throw new Error("A restricted scalar SQL query is required");
  if (!Number.isFinite(threshold)) throw new Error("Threshold must be a finite number");
  const now = (BigInt(nowMs) * 1_000_000n).toString();
  return {
    schema_version: 1,
    id: current?.id ?? id,
    revision: current ? (BigInt(current.revision) + 1n).toString() : "1",
    name: draft.name.trim(),
    enabled: draft.enabled,
    query: {
      target_id: targetId,
      signal: draft.signal,
      matchers: current?.query.matchers ?? [],
      lookback_seconds: positiveInteger(draft.lookbackSeconds, "Lookback"),
      sql: draft.sql.trim(),
    },
    condition: { comparator: draft.comparator, threshold },
    every_seconds: positiveInteger(draft.everySeconds, "Interval"),
    jitter_seconds: positiveInteger(draft.jitterSeconds, "Jitter", true),
    for_seconds: positiveInteger(draft.forSeconds, "For duration", true),
    recover_for_seconds: positiveInteger(draft.recoverForSeconds, "Recovery duration", true),
    no_data: draft.noData,
    execution_error: draft.executionError,
    labels: current?.labels ?? [],
    annotations: current?.annotations ?? [],
    created_at_unix_nano: current?.created_at_unix_nano ?? now,
    updated_at_unix_nano: now,
  };
}

export function draftFromMonitor(monitor: AlertMonitor): MonitorDraft {
  return {
    name: monitor.name,
    enabled: monitor.enabled,
    signal: monitor.query.signal,
    sql: monitor.query.sql,
    lookbackSeconds: String(monitor.query.lookback_seconds),
    comparator: monitor.condition.comparator,
    threshold: String(monitor.condition.threshold),
    everySeconds: String(monitor.every_seconds),
    jitterSeconds: String(monitor.jitter_seconds),
    forSeconds: String(monitor.for_seconds),
    recoverForSeconds: String(monitor.recover_for_seconds),
    noData: monitor.no_data,
    executionError: monitor.execution_error,
  };
}

export type DisplayAlertStatus = AlertStatus | "stale" | "unavailable";

export function displayStatus(summary: MonitorSummary): DisplayAlertStatus {
  if (summary.state && summary.state.monitor_revision !== summary.monitor.revision) return "unavailable";
  if (!summary.monitor.enabled) return "disabled";
  if (!summary.state) return "unavailable";
  if (summary.state.stale) return "stale";
  return summary.state.status;
}

interface AlertStoreState {
  monitors: MonitorSummary[];
  loading: boolean;
  saving: boolean;
  error: string | null;
  conflict: string | null;
  validated: boolean;
}

export function createAlertStore(client = new AlertApiClient()) {
  const [alerts, setAlerts] = createStore<AlertStoreState>({
    monitors: [], loading: false, saving: false, error: null, conflict: null, validated: false,
  });

  let activeTarget: string | undefined;
  let generation = 0;

  const fail = (error: unknown) => {
    const apiError = error instanceof AlertApiError ? error : null;
    const message = error instanceof Error ? error.message : String(error);
    setAlerts({ error: message, conflict: apiError?.conflict ? `${message}. Reload before applying your changes again.` : null });
  };

  const isCurrent = (targetId: string, operationGeneration: number) =>
    generation === operationGeneration && (activeTarget === undefined || activeTarget === targetId);

  const load = async (targetId: string) => {
    const targetChanged = activeTarget !== undefined && activeTarget !== targetId;
    activeTarget = targetId;
    const operationGeneration = ++generation;
    setAlerts({
      ...(targetChanged ? { monitors: [] } : {}),
      loading: true,
      saving: false,
      error: null,
      conflict: null,
      validated: false,
    });
    try {
      const monitors = await client.list(targetId);
      if (isCurrent(targetId, operationGeneration)) setAlerts({ monitors, loading: false });
    } catch (error) {
      if (isCurrent(targetId, operationGeneration)) {
        fail(error);
        setAlerts("loading", false);
      }
    }
  };

  const validate = async (targetId: string, monitor: AlertMonitor) => {
    const operationGeneration = generation;
    setAlerts({ saving: true, error: null, conflict: null, validated: false });
    try {
      await client.validate(targetId, monitor);
      if (!isCurrent(targetId, operationGeneration)) return false;
      setAlerts({ saving: false, validated: true });
      return true;
    } catch (error) {
      if (!isCurrent(targetId, operationGeneration)) return false;
      fail(error);
      setAlerts("saving", false);
      return false;
    }
  };

  const save = async (targetId: string, monitor: AlertMonitor, expectedRevision?: string) => {
    const operationGeneration = generation;
    setAlerts({ saving: true, error: null, conflict: null, validated: false });
    try {
      const summary = expectedRevision === undefined
        ? await client.create(targetId, monitor)
        : await client.update(targetId, monitor, expectedRevision);
      if (!isCurrent(targetId, operationGeneration)) return false;
      setAlerts(produce((state) => {
        const index = state.monitors.findIndex((entry) => entry.monitor.id === summary.monitor.id);
        if (index < 0) state.monitors.unshift(summary);
        else state.monitors[index] = summary;
        state.saving = false;
      }));
      return true;
    } catch (error) {
      if (!isCurrent(targetId, operationGeneration)) return false;
      fail(error);
      setAlerts("saving", false);
      return false;
    }
  };

  const remove = async (targetId: string, monitor: AlertMonitor) => {
    const operationGeneration = generation;
    setAlerts({ saving: true, error: null, conflict: null });
    try {
      await client.remove(targetId, monitor.id, monitor.revision);
      if (!isCurrent(targetId, operationGeneration)) return false;
      setAlerts(produce((state) => {
        state.monitors = state.monitors.filter((entry) => entry.monitor.id !== monitor.id);
        state.saving = false;
      }));
      return true;
    } catch (error) {
      if (!isCurrent(targetId, operationGeneration)) return false;
      fail(error);
      setAlerts("saving", false);
      return false;
    }
  };

  return { alerts, load, validate, save, remove };
}

export const alertStore = createAlertStore();
