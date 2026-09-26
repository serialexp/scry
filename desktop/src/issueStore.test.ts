import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import {
  createIssueStore,
  formatIssueAge,
  pollWhileVisible,
  qualityLabel,
  type IssueFetcher,
  type VisibilitySource,
} from "./issueStore";
import {
  parseIssue,
  parseOccurrence,
  type Issue,
  type IssueDetailResult,
} from "./protocol/client";

function issue(issueId: string, title = "DatabaseError"): Issue {
  return {
    issue_id: issueId,
    app_id: "checkout",
    title,
    grouping_quality: 0,
    first_seen_unix_nano: 1_000,
    last_seen_unix_nano: 2_000,
    occurrence_count: 3,
    max_severity: 17,
    fingerprint_version: 2,
  };
}

/** A promise whose settlement the test controls. */
function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

const TARGET = () => "prod";

describe("createIssueStore", () => {
  it("applies only the newest list response", async () => {
    const first = deferred<Issue[]>();
    const second = deferred<Issue[]>();
    const list = vi
      .fn<IssueFetcher["list"]>()
      .mockReturnValueOnce(first.promise)
      .mockReturnValueOnce(second.promise);
    const store = createIssueStore({ list, detail: vi.fn() }, TARGET, () => 42);

    const older = store.refreshList();
    const newer = store.refreshList();
    second.resolve([issue("b")]);
    await newer;
    first.resolve([issue("a")]);
    await older;

    expect(store.state.issues.map((i) => i.issue_id)).toEqual(["b"]);
    expect(store.state.listStatus).toBe("ready");
    expect(store.state.listUpdatedAt).toBe(42);
  });

  it("keeps the previous list on failure and exposes the error", async () => {
    const list = vi
      .fn<IssueFetcher["list"]>()
      .mockResolvedValueOnce([issue("a")])
      .mockRejectedValueOnce(new Error("issues unavailable"));
    const store = createIssueStore({ list, detail: vi.fn() }, TARGET);

    await store.refreshList();
    await store.refreshList();

    expect(store.state.issues.map((i) => i.issue_id)).toEqual(["a"]);
    expect(store.state.listStatus).toBe("error");
    expect(store.state.listError).toBe("issues unavailable");
  });

  it("clears the old issue on selection and ignores its late response", async () => {
    const slowA = deferred<IssueDetailResult>();
    const detail = vi.fn<IssueFetcher["detail"]>((_destination, issueId) =>
      issueId === "a"
        ? slowA.promise
        : Promise.resolve({ issue: issue("b"), occurrences: [] }),
    );
    const store = createIssueStore({ list: vi.fn(), detail }, TARGET);

    store.selectIssue("a");
    const pendingA = store.refreshDetail();
    store.selectIssue("b");
    expect(store.state.issue).toBeNull();
    expect(store.state.detailStatus).toBe("idle");

    await store.refreshDetail();
    slowA.resolve({
      issue: issue("a"),
      occurrences: [
        { event_id: "e", occurred_at_unix_nano: 1, trace_id: null, span_id: null },
      ],
    });
    await pendingA;

    expect(store.state.issueId).toBe("b");
    expect(store.state.issue?.issue_id).toBe("b");
    expect(store.state.occurrences).toEqual([]);
    expect(detail.mock.calls).toEqual([
      ["prod", "a"],
      ["prod", "b"],
    ]);
  });

  it("clears everything and drops in-flight responses when the target changes", async () => {
    let target = "prod";
    const slowList = deferred<Issue[]>();
    const slowDetail = deferred<IssueDetailResult>();
    const list = vi
      .fn<IssueFetcher["list"]>()
      .mockResolvedValueOnce([issue("a")])
      .mockReturnValueOnce(slowList.promise)
      .mockResolvedValueOnce([issue("z")]);
    const detail = vi
      .fn<IssueFetcher["detail"]>()
      .mockResolvedValueOnce({ issue: issue("a"), occurrences: [] })
      .mockReturnValueOnce(slowDetail.promise)
      .mockResolvedValueOnce({ issue: null, occurrences: [] });
    const store = createIssueStore({ list, detail }, () => target);

    await store.refreshList();
    store.selectIssue("a");
    await store.refreshDetail();
    const pendingList = store.refreshList();
    const pendingDetail = store.refreshDetail();

    target = "staging";
    await store.refreshList();
    expect(store.state.destination).toBe("staging");
    expect(store.state.issue).toBeNull();
    expect(store.state.issues.map((i) => i.issue_id)).toEqual(["z"]);

    slowList.resolve([issue("a")]);
    slowDetail.resolve({ issue: issue("a"), occurrences: [] });
    await Promise.all([pendingList, pendingDetail]);
    expect(store.state.issues.map((i) => i.issue_id)).toEqual(["z"]);
    expect(store.state.issue).toBeNull();

    // The selection survives; the next detail read goes to the new target.
    await store.refreshDetail();
    expect(detail.mock.calls.at(-1)).toEqual(["staging", "a"]);
    expect(store.state.detailStatus).toBe("ready");
    expect(list.mock.calls.map(([destination]) => destination)).toEqual([
      "prod",
      "prod",
      "staging",
    ]);
  });

  it("does not clear or refetch when the same issue is selected again", async () => {
    const detail = vi
      .fn<IssueFetcher["detail"]>()
      .mockResolvedValue({ issue: issue("a"), occurrences: [] });
    const store = createIssueStore({ list: vi.fn(), detail }, TARGET);

    store.selectIssue("a");
    await store.refreshDetail();
    store.selectIssue("a");

    expect(store.state.issue?.issue_id).toBe("a");
    expect(store.state.detailStatus).toBe("ready");
    expect(detail).toHaveBeenCalledTimes(1);
  });

  it("does nothing when no issue is selected", async () => {
    const detail = vi.fn<IssueFetcher["detail"]>();
    const store = createIssueStore({ list: vi.fn(), detail }, TARGET);

    await store.refreshDetail();

    expect(detail).not.toHaveBeenCalled();
    expect(store.state.detailStatus).toBe("idle");
  });
});

class FakeVisibility implements VisibilitySource {
  hidden = false;
  private listeners = new Set<() => void>();
  addEventListener(_type: "visibilitychange", listener: () => void) {
    this.listeners.add(listener);
  }
  removeEventListener(_type: "visibilitychange", listener: () => void) {
    this.listeners.delete(listener);
  }
  set(hidden: boolean) {
    this.hidden = hidden;
    for (const listener of this.listeners) listener();
  }
  get listenerCount() {
    return this.listeners.size;
  }
}

describe("pollWhileVisible", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it("schedules the next run only after the previous one settles", async () => {
    const pending: Array<ReturnType<typeof deferred<void>>> = [];
    const task = vi.fn(() => {
      const next = deferred<void>();
      pending.push(next);
      return next.promise;
    });
    const stop = pollWhileVisible(task, 1_000, new FakeVisibility());

    expect(task).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(5_000);
    expect(task).toHaveBeenCalledTimes(1);

    pending[0].resolve();
    await vi.advanceTimersByTimeAsync(999);
    expect(task).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(1);
    expect(task).toHaveBeenCalledTimes(2);
    stop();
  });

  it("keeps polling after a failed run", async () => {
    const task = vi.fn<() => Promise<void>>().mockRejectedValueOnce(new Error("boom"));
    task.mockResolvedValue(undefined);
    const stop = pollWhileVisible(
      () => task().catch(() => undefined),
      1_000,
      new FakeVisibility(),
    );

    await vi.advanceTimersByTimeAsync(1_000);
    expect(task).toHaveBeenCalledTimes(2);
    stop();
  });

  it("stops running and unsubscribes after stop", async () => {
    const visibility = new FakeVisibility();
    const task = vi.fn(() => Promise.resolve());
    const stop = pollWhileVisible(task, 1_000, visibility);
    await vi.advanceTimersByTimeAsync(0);

    stop();
    await vi.advanceTimersByTimeAsync(10_000);
    visibility.set(false);

    expect(task).toHaveBeenCalledTimes(1);
    expect(visibility.listenerCount).toBe(0);
  });

  it("pauses while hidden and refreshes immediately when visible again", async () => {
    const visibility = new FakeVisibility();
    const task = vi.fn(() => Promise.resolve());
    const stop = pollWhileVisible(task, 1_000, visibility);
    await vi.advanceTimersByTimeAsync(0);
    expect(task).toHaveBeenCalledTimes(1);

    visibility.set(true);
    await vi.advanceTimersByTimeAsync(10_000);
    expect(task).toHaveBeenCalledTimes(1);

    visibility.set(false);
    await vi.advanceTimersByTimeAsync(0);
    expect(task).toHaveBeenCalledTimes(2);
    await vi.advanceTimersByTimeAsync(1_000);
    expect(task).toHaveBeenCalledTimes(3);
    stop();
  });

  it("does not start while hidden", async () => {
    const visibility = new FakeVisibility();
    visibility.hidden = true;
    const task = vi.fn(() => Promise.resolve());
    const stop = pollWhileVisible(task, 1_000, visibility);

    await vi.advanceTimersByTimeAsync(5_000);
    expect(task).not.toHaveBeenCalled();
    visibility.set(false);
    await vi.advanceTimersByTimeAsync(0);
    expect(task).toHaveBeenCalledTimes(1);
    stop();
  });
});

describe("issue display helpers", () => {
  it("labels every grouping quality, including message-only", () => {
    expect(qualityLabel(0)).toBe("Type + Message");
    expect(qualityLabel(1)).toBe("Type Only");
    expect(qualityLabel(2)).toBe("Fallback");
    expect(qualityLabel(3)).toBe("Message Only");
    expect(qualityLabel(9)).toBe("Q9");
  });

  it("formats ages relative to the supplied clock", () => {
    const nowMs = 10 * 86_400_000;
    const nanos = (ms: number) => ms * 1_000_000;
    expect(formatIssueAge(nanos(nowMs - 30_000), nowMs)).toBe("just now");
    expect(formatIssueAge(nanos(nowMs - 5 * 60_000), nowMs)).toBe("5m ago");
    expect(formatIssueAge(nanos(nowMs - 3 * 3_600_000), nowMs)).toBe("3h ago");
    expect(formatIssueAge(nanos(nowMs - 2 * 86_400_000), nowMs)).toBe("2d ago");
  });
});

describe("issue document parsing", () => {
  it("accepts a well-formed issue", () => {
    expect(parseIssue(JSON.stringify(issue("a")))).toEqual(issue("a"));
  });

  it("rejects issues with missing or mistyped fields", () => {
    const { title: _title, ...missing } = issue("a");
    expect(() => parseIssue(JSON.stringify(missing))).toThrow(/invalid issue/);
    expect(() =>
      parseIssue(JSON.stringify({ ...issue("a"), occurrence_count: "3" })),
    ).toThrow(/invalid issue/);
    expect(() => parseIssue("[]")).toThrow(/invalid issue/);
    expect(() => parseIssue("null")).toThrow(/invalid issue/);
  });

  it("accepts occurrences with and without trace context", () => {
    const withTrace = {
      event_id: "e1",
      occurred_at_unix_nano: 5,
      trace_id: "abc",
      span_id: "def",
    };
    const without = { event_id: "e2", occurred_at_unix_nano: 6, trace_id: null, span_id: null };
    expect(parseOccurrence(JSON.stringify(withTrace))).toEqual(withTrace);
    expect(parseOccurrence(JSON.stringify(without))).toEqual(without);
  });

  it("rejects malformed occurrences", () => {
    expect(() =>
      parseOccurrence(JSON.stringify({ event_id: 1, occurred_at_unix_nano: 5, trace_id: null, span_id: null })),
    ).toThrow(/invalid occurrence/);
    expect(() =>
      parseOccurrence(JSON.stringify({ event_id: "e", occurred_at_unix_nano: 5, span_id: null })),
    ).toThrow(/invalid occurrence/);
  });
});
