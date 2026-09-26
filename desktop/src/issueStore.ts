//! Error-tracking issue state: the issue list and one selected issue's detail.
//!
//! Every request carries a generation. A response is applied only if no newer
//! request of the same kind started meanwhile, so a slow response for a
//! previous issue (or an older list refresh) can never overwrite newer state.
//! Selecting a different issue clears the old issue's data immediately, and a
//! change of query destination (target) clears everything and invalidates
//! every in-flight request, so one target's issues never show under another.
//!
//! Polling is completion-relative ([`pollWhileVisible`]): the next refresh is
//! scheduled only after the previous one settles, stops when its owner is
//! disposed, and pauses while the page is hidden.

import { createStore } from "solid-js/store";

import type { Issue, IssueDetailResult, OccurrenceSummary } from "./protocol/client";

export type IssueLoadStatus = "idle" | "loading" | "ready" | "error";

export interface IssueFetcher {
  list(destination: string): Promise<Issue[]>;
  detail(destination: string, issueId: string): Promise<IssueDetailResult>;
}

export interface IssueStoreState {
  /** Destination the data below was fetched from; `null` before any request. */
  destination: string | null;
  issues: Issue[];
  listStatus: IssueLoadStatus;
  listError: string | null;
  listUpdatedAt: number | null;
  issueId: string | null;
  issue: Issue | null;
  occurrences: OccurrenceSummary[];
  detailStatus: IssueLoadStatus;
  detailError: string | null;
}

const message = (error: unknown) => (error instanceof Error ? error.message : String(error));

/** `destination` names the query target to read from; it is re-read on every
 * refresh (and may be reactive, so views can refresh when it changes). */
export function createIssueStore(
  fetcher: IssueFetcher,
  destination: () => string,
  now: () => number = Date.now,
) {
  const [state, setState] = createStore<IssueStoreState>({
    destination: null,
    issues: [],
    listStatus: "idle",
    listError: null,
    listUpdatedAt: null,
    issueId: null,
    issue: null,
    occurrences: [],
    detailStatus: "idle",
    detailError: null,
  });
  let listGeneration = 0;
  let detailGeneration = 0;

  /** Adopt the current destination, dropping all data and in-flight requests
   * from a previous one. */
  const syncDestination = () => {
    const current = destination();
    if (current !== state.destination) {
      listGeneration++;
      detailGeneration++;
      setState({
        destination: current,
        issues: [],
        listStatus: "idle",
        listError: null,
        listUpdatedAt: null,
        issue: null,
        occurrences: [],
        detailStatus: "idle",
        detailError: null,
      });
    }
    return current;
  };

  /** Refresh the list. Failures keep the previous rows and expose the error. */
  const refreshList = async () => {
    const target = syncDestination();
    const generation = ++listGeneration;
    setState({ listStatus: "loading", listError: null });
    try {
      const issues = await fetcher.list(target);
      if (generation !== listGeneration) return;
      setState({ issues, listStatus: "ready", listUpdatedAt: now() });
    } catch (error) {
      if (generation !== listGeneration) return;
      setState({ listStatus: "error", listError: message(error) });
    }
  };

  /** Refresh the selected issue; a no-op when none is selected. */
  const refreshDetail = async () => {
    const target = syncDestination();
    const issueId = state.issueId;
    if (issueId === null) return;
    const generation = ++detailGeneration;
    setState({ detailStatus: "loading", detailError: null });
    try {
      const result = await fetcher.detail(target, issueId);
      if (generation !== detailGeneration) return;
      setState({
        issue: result.issue,
        occurrences: result.occurrences,
        detailStatus: "ready",
      });
    } catch (error) {
      if (generation !== detailGeneration) return;
      setState({ detailStatus: "error", detailError: message(error) });
    }
  };

  /** Select `issueId` (or none). A different issue clears the previous
   * issue's data and invalidates its in-flight request. */
  const selectIssue = (issueId: string | null) => {
    if (issueId === state.issueId) return;
    detailGeneration++;
    setState({
      issueId,
      issue: null,
      occurrences: [],
      detailStatus: "idle",
      detailError: null,
    });
  };

  return { state, destination, refreshList, refreshDetail, selectIssue };
}

export type IssueStore = ReturnType<typeof createIssueStore>;

/** The page-visibility surface [`pollWhileVisible`] needs; `document` in the app. */
export interface VisibilitySource {
  readonly hidden: boolean;
  addEventListener(type: "visibilitychange", listener: () => void): void;
  removeEventListener(type: "visibilitychange", listener: () => void): void;
}

/** Run `task` now and then `intervalMs` after each completion, skipping
 * refreshes while `visibility` is hidden and refreshing as soon as it becomes
 * visible again. Returns a stop function; nothing runs after it is called. */
export function pollWhileVisible(
  task: () => Promise<void>,
  intervalMs: number,
  visibility: VisibilitySource | undefined = globalThis.document,
): () => void {
  let stopped = false;
  let running = false;
  let timer: ReturnType<typeof setTimeout> | undefined;

  const schedule = () => {
    if (stopped || visibility?.hidden) return;
    timer = setTimeout(run, intervalMs);
  };
  const run = async () => {
    timer = undefined;
    if (stopped || running || visibility?.hidden) return;
    running = true;
    try {
      await task();
    } finally {
      running = false;
      schedule();
    }
  };
  const onVisibility = () => {
    if (stopped || visibility?.hidden) return;
    if (timer !== undefined) clearTimeout(timer);
    void run();
  };

  visibility?.addEventListener("visibilitychange", onVisibility);
  void run();
  return () => {
    stopped = true;
    if (timer !== undefined) clearTimeout(timer);
    visibility?.removeEventListener("visibilitychange", onVisibility);
  };
}

const QUALITY_LABELS: Record<number, string> = {
  0: "Type + Message",
  1: "Type Only",
  2: "Fallback",
  3: "Message Only",
};

/** Display label for an issue's `grouping_quality`. */
export function qualityLabel(quality: number): string {
  return QUALITY_LABELS[quality] ?? `Q${quality}`;
}

export function formatIssueTime(nanos: number): string {
  return new Date(nanos / 1_000_000).toLocaleString();
}

export function formatIssueAge(nanos: number, nowMs: number = Date.now()): string {
  const ago = nowMs - nanos / 1_000_000;
  if (ago < 60_000) return "just now";
  if (ago < 3_600_000) return `${Math.floor(ago / 60_000)}m ago`;
  if (ago < 86_400_000) return `${Math.floor(ago / 3_600_000)}h ago`;
  return `${Math.floor(ago / 86_400_000)}d ago`;
}
