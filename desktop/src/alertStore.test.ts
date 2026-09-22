import { describe, expect, it, vi } from "vitest";

import {
  AlertApiClient,
  AlertApiError,
  EMPTY_DRAFT,
  createAlertStore,
  displayStatus,
  monitorFromDraft,
  type AlertInstanceState,
  type AlertMonitor,
  type MonitorSummary,
} from "./alertStore";

function monitor(overrides: Partial<AlertMonitor> = {}): AlertMonitor {
  return {
    ...monitorFromDraft(
      { ...EMPTY_DRAFT, name: "High latency", sql: "SELECT max(value) FROM metrics" },
      "prod",
      undefined,
      1000,
      "00000000-0000-4000-8000-000000000001",
    ),
    ...overrides,
  };
}

function summary(status: AlertInstanceState["status"] = "inactive", stale = false): MonitorSummary {
  return {
    monitor: monitor(),
    state: {
      monitor_revision: "1",
      status,
      since_unix_nano: "1",
      last_evaluated_at_unix_nano: "2",
      last_slot_id: "3",
      last_value: 4,
      last_error_class: null,
      transition_sequence: "1",
      stale,
    },
  };
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((promiseResolve, promiseReject) => {
    resolve = promiseResolve;
    reject = promiseReject;
  });
  return { promise, resolve, reject };
}

describe("monitor form mapping", () => {
  it("builds a bounded scalar monitor using the selected target", () => {
    const result = monitorFromDraft(
      { ...EMPTY_DRAFT, name: " Errors ", sql: " SELECT count(*) FROM logs ", threshold: "12.5" },
      "edge-a",
      undefined,
      42,
      "monitor-id",
    );
    expect(result).toMatchObject({
      id: "monitor-id",
      revision: "1",
      name: "Errors",
      query: { target_id: "edge-a", sql: "SELECT count(*) FROM logs", lookback_seconds: 300 },
      condition: { comparator: "gt", threshold: 12.5 },
      created_at_unix_nano: "42000000",
    });
  });

  it("rejects invalid local scalar fields before sending", () => {
    expect(() => monitorFromDraft({ ...EMPTY_DRAFT, name: "x", sql: "select 1", everySeconds: "0" }, "prod"))
      .toThrow("Interval must be a positive whole number");
    expect(() => monitorFromDraft({ ...EMPTY_DRAFT, name: "x", sql: "select 1", threshold: "NaN" }, "prod"))
      .toThrow("Threshold must be a finite number");
  });
});

describe("status presentation", () => {
  it("distinguishes unavailable, disabled, stale, and engine states", () => {
    expect(displayStatus({ monitor: monitor(), state: null })).toBe("unavailable");
    expect(displayStatus({ monitor: monitor({ enabled: false }), state: null })).toBe("disabled");
    expect(displayStatus(summary("firing", true))).toBe("stale");
    expect(displayStatus({ ...summary("firing"), monitor: monitor({ revision: "2" }) })).toBe("unavailable");
    for (const status of ["inactive", "pending", "firing", "recovering", "no_data", "error", "disabled"] as const) {
      expect(displayStatus(summary(status))).toBe(status);
    }
  });
});

describe("alert store request generations", () => {
  it("ignores out-of-order loads, including responses for an old target", async () => {
    const first = deferred<MonitorSummary[]>();
    const second = deferred<MonitorSummary[]>();
    const client = {
      list: vi.fn((targetId: string) => targetId === "first" ? first.promise : second.promise),
    } as unknown as AlertApiClient;
    const store = createAlertStore(client);

    const firstLoad = store.load("first");
    const secondLoad = store.load("second");
    const current = { ...summary(), monitor: monitor({ id: "second-monitor", name: "Second" }) };
    second.resolve([current]);
    await secondLoad;
    expect(store.alerts.monitors).toEqual([current]);

    first.resolve([{ ...summary(), monitor: monitor({ id: "first-monitor", name: "First" }) }]);
    await firstLoad;
    expect(store.alerts.monitors).toEqual([current]);
    expect(store.alerts.loading).toBe(false);
  });

  it("does not apply a mutation completion after the target changes", async () => {
    const save = deferred<MonitorSummary>();
    const nextTarget = deferred<MonitorSummary[]>();
    const client = {
      list: vi.fn().mockResolvedValueOnce([]).mockImplementationOnce(() => nextTarget.promise),
      create: vi.fn(() => save.promise),
    } as unknown as AlertApiClient;
    const store = createAlertStore(client);
    await store.load("first");

    const saving = store.save("first", monitor());
    const loading = store.load("second");
    save.resolve(summary("firing"));
    expect(await saving).toBe(false);
    expect(store.alerts.monitors).toEqual([]);
    expect(store.alerts.loading).toBe(true);

    const current = { ...summary(), monitor: monitor({ id: "second-monitor" }) };
    nextTarget.resolve([current]);
    await loading;
    expect(store.alerts.monitors).toEqual([current]);
    expect(store.alerts.saving).toBe(false);
  });
});

describe("AlertApiClient", () => {
  it("uses same-origin routes and required mutation headers", async () => {
    const requests: [RequestInfo | URL, RequestInit | undefined][] = [];
    const fetcher = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      requests.push([input, init]);
      if (input === "/api/csrf") return new Response(JSON.stringify({ csrfToken: "nonce" }));
      return new Response(JSON.stringify(summary()), { status: 200, headers: { "content-type": "application/json" } });
    });
    const client = new AlertApiClient(fetcher as typeof fetch);
    const updated = monitor({ revision: "2" });
    await client.update("prod target", updated, "1");

    expect(requests[1][0]).toBe(`/api/v1/alerts/monitors/${updated.id}`);
    const init = requests[1][1]!;
    const headers = new Headers(init.headers);
    expect(init.credentials).toBe("same-origin");
    expect(headers.get("x-scry-target")).toBe("prod target");
    expect(headers.get("x-scry-csrf")).toBe("nonce");
    expect(headers.get("idempotency-key")).toBeTruthy();
    expect(headers.get("if-match")).toBe('"1"');
  });

  it("preserves revision conflicts for the store", async () => {
    const client = {
      list: vi.fn(), validate: vi.fn(), create: vi.fn(), remove: vi.fn(),
      update: vi.fn().mockRejectedValue(new AlertApiError("monitor revision conflict", 409, true)),
    } as unknown as AlertApiClient;
    const store = createAlertStore(client);
    expect(await store.save("prod", monitor({ revision: "2" }), "1")).toBe(false);
    expect(store.alerts.conflict).toContain("Reload before applying");
  });
});
