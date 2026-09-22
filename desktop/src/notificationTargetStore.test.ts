import { describe, expect, it, vi } from "vitest";

import {
  EMPTY_NOTIFICATION_TARGET_DRAFT,
  NotificationTargetApiClient,
  createNotificationTargetStore,
  draftFromNotificationTarget,
  notificationTargetFromDraft,
  type NotificationTargetView,
} from "./notificationTargetStore";

function target(overrides: Partial<NotificationTargetView> = {}): NotificationTargetView {
  return {
    schema_version: 1, id: "target-id", revision: "3", name: "Ops", enabled: true,
    kind: { type: "webhook", url: "https://hooks.example.test/alerts" }, timeout_ms: 5000,
    headers: [{ name: "X-Team", value: "ops" }], format: { type: "builtin", format_id: "generic_json" },
    secret_configured: true, created_at_unix_nano: "1", updated_at_unix_nano: "2", ...overrides,
  };
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((done) => { resolve = done; });
  return { promise, resolve };
}

describe("notification target form mapping", () => {
  it("maps nested editable fields and write-only secret actions", () => {
    const write = notificationTargetFromDraft({
      ...EMPTY_NOTIFICATION_TARGET_DRAFT, name: " Ops ", url: "https://hooks.example.test/path", timeoutMs: "2500",
      headersJson: '{"X-Team":"ops"}', secret: "do-not-return",
    });
    expect(write).toEqual({
      schema_version: 1, name: "Ops", enabled: true, kind: { type: "webhook", url: "https://hooks.example.test/path" },
      timeout_ms: 2500, headers: [{ name: "X-Team", value: "ops" }], format: { type: "builtin", format_id: "generic_json" },
      secret: { action: "set", value: "do-not-return" },
    });
    const edit = draftFromNotificationTarget(target());
    expect(edit.secretAction).toBe("unchanged");
    expect(edit.secret).toBe("");
    expect(notificationTargetFromDraft({ ...edit, enabled: false, secretAction: "clear" }, target()).secret).toEqual({ action: "clear" });
    expect(() => notificationTargetFromDraft({ ...edit, enabled: true, secretAction: "clear" }, target()))
      .toThrow("Disable the notification target");
  });

  it("rejects unsafe URLs and malformed JSON locally", () => {
    const base = { ...EMPTY_NOTIFICATION_TARGET_DRAFT, name: "x", url: "https://example.test/hook", secret: "secret" };
    expect(() => notificationTargetFromDraft({ ...base, url: "http://example.test" })).toThrow("public HTTPS");
    expect(() => notificationTargetFromDraft({ ...base, url: "https://user@example.test" })).toThrow("credentials");
    expect(() => notificationTargetFromDraft({ ...base, headersJson: "{" })).toThrow("valid JSON");
    expect(() => notificationTargetFromDraft({ ...base, formatType: "custom_json", template: "not-json" })).toThrow("valid JSON");
  });
});

describe("NotificationTargetApiClient", () => {
  it("paginates lists with target headers", async () => {
    const fetcher = vi.fn()
      .mockResolvedValueOnce(new Response(JSON.stringify({ targets: [target()], next: "cursor" })))
      .mockResolvedValueOnce(new Response(JSON.stringify({ targets: [target({ id: "second" })], next: null })));
    const result = await new NotificationTargetApiClient(fetcher).list("prod target");
    expect(result.map((entry) => entry.id)).toEqual(["target-id", "second"]);
    expect(fetcher.mock.calls[1][0]).toContain("after=cursor");
    expect(new Headers(fetcher.mock.calls[0][1].headers).get("x-scry-target")).toBe("prod target");
  });

  it("rejects API views that violate the response-redaction contract", async () => {
    const leaked = { ...target(), secret: "plaintext" };
    const fetcher = vi.fn().mockResolvedValue(new Response(JSON.stringify({ targets: [leaked] })));
    await expect(new NotificationTargetApiClient(fetcher).list("prod")).rejects.toThrow("secret material");
  });

  it("uses fresh CSRF, UUID idempotency, If-Match, target headers, and redacted writes", async () => {
    const calls: [RequestInfo | URL, RequestInit | undefined][] = [];
    let nonce = 0;
    const fetcher = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      calls.push([input, init]);
      if (input === "/api/csrf") return new Response(JSON.stringify({ csrfToken: `nonce-${++nonce}` }));
      return new Response(JSON.stringify(target({ revision: "4" })), { status: 200 });
    });
    const client = new NotificationTargetApiClient(fetcher);
    const draft = draftFromNotificationTarget(target());
    const write = notificationTargetFromDraft({ ...draft, secretAction: "set", secret: "new-secret" }, target());
    await client.update("prod", "target/id", write, "3");
    await client.test("prod", "target/id", "4");

    const firstHeaders = new Headers(calls[1][1]!.headers);
    const secondHeaders = new Headers(calls[3][1]!.headers);
    expect(calls[1][0]).toBe("/api/v1/alerts/notification-targets/target%2Fid");
    expect(firstHeaders.get("x-scry-csrf")).toBe("nonce-1");
    expect(secondHeaders.get("x-scry-csrf")).toBe("nonce-2");
    expect(firstHeaders.get("x-scry-target")).toBe("prod");
    expect(firstHeaders.get("if-match")).toBe('"3"');
    expect(firstHeaders.get("idempotency-key")).toMatch(/^[0-9a-f-]{36}$/);
    expect(JSON.parse(String(calls[1][1]!.body)).secret).toEqual({ action: "set", value: "new-secret" });
    expect(calls[3][1]!.body).toBeUndefined();
  });

  it("uses the validate, preview, create, delete, and test route contract", async () => {
    const routes: string[] = [];
    const fetcher = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      if (input === "/api/csrf") return new Response(JSON.stringify({ csrfToken: "nonce" }));
      routes.push(`${init?.method} ${String(input)}`);
      if (String(input).endsWith("/preview")) return new Response(JSON.stringify({ content_type: "application/json", body: "{}", body_sha256: "hash" }));
      if (String(input).endsWith("/test")) return new Response(JSON.stringify({ event_id: "event", outcome: "accepted", http_status: 200, error_class: null, duration_ms: 1 }));
      if (init?.method === "DELETE" || String(input).endsWith("/validate")) return new Response(null, { status: 204 });
      return new Response(JSON.stringify(target()));
    });
    const client = new NotificationTargetApiClient(fetcher);
    const write = notificationTargetFromDraft({ ...EMPTY_NOTIFICATION_TARGET_DRAFT, name: "Ops", url: "https://example.test/hook", secret: "secret" });
    await client.validate("prod", write);
    await client.create("prod", write);
    await client.preview("prod", "target/id", "3");
    await client.test("prod", "target/id", "3");
    await client.remove("prod", "target/id", "3");
    expect(routes).toEqual([
      "POST /api/v1/alerts/notification-targets/validate",
      "POST /api/v1/alerts/notification-targets",
      "POST /api/v1/alerts/notification-targets/target%2Fid/preview",
      "POST /api/v1/alerts/notification-targets/target%2Fid/test",
      "DELETE /api/v1/alerts/notification-targets/target%2Fid",
    ]);
  });
});

describe("notification target store generations", () => {
  it("lets the newest same-target action win", async () => {
    const first = deferred<{ content_type: "application/json"; body: string; body_sha256: string }>();
    const second = deferred<{ content_type: "application/json"; body: string; body_sha256: string }>();
    const client = {
      preview: vi.fn().mockImplementationOnce(() => first.promise).mockImplementationOnce(() => second.promise),
    } as unknown as NotificationTargetApiClient;
    const store = createNotificationTargetStore(client);
    const oldRequest = store.preview("prod", target());
    const newRequest = store.preview("prod", target());
    second.resolve({ content_type: "application/json", body: "new", body_sha256: "new-hash" });
    expect(await newRequest).toBe(true);
    first.resolve({ content_type: "application/json", body: "old", body_sha256: "old-hash" });
    expect(await oldRequest).toBe(false);
    expect(store.notificationTargets.preview?.body).toBe("new");
  });

  it("ignores stale target loads and stale mutation completion", async () => {
    const first = deferred<NotificationTargetView[]>();
    const second = deferred<NotificationTargetView[]>();
    const third = deferred<NotificationTargetView[]>();
    const save = deferred<NotificationTargetView>();
    const client = {
      list: vi.fn((id: string) => id === "first" ? first.promise : id === "second" ? second.promise : third.promise),
      create: vi.fn(() => save.promise),
    } as unknown as NotificationTargetApiClient;
    const store = createNotificationTargetStore(client);
    const firstLoad = store.load("first");
    const secondLoad = store.load("second");
    second.resolve([target({ id: "current" })]);
    await secondLoad;
    first.resolve([target({ id: "stale" })]);
    await firstLoad;
    expect(store.notificationTargets.targets[0].id).toBe("current");

    const write = notificationTargetFromDraft({ ...EMPTY_NOTIFICATION_TARGET_DRAFT, name: "new", url: "https://example.test/hook", secret: "secret" });
    const saving = store.save("second", write);
    const nextLoad = store.load("third");
    save.resolve(target({ id: "stale-save" }));
    expect(await saving).toBe(false);
    third.resolve([]);
    await nextLoad;
    expect(store.notificationTargets.targets).toEqual([]);
  });
});
