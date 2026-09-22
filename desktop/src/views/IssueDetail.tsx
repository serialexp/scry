//! Issue detail page: issue metadata header + occurrences list.
//!
//! Reached via `/errors/:issueId` after clicking an issue row in the
//! Errors list view. Polls for updates every 10 seconds (same cadence
//! as the list view).

import {
  For,
  Match,
  Show,
  Switch,
  onCleanup,
  onMount,
  type Component,
} from "solid-js";
import { useParams, A } from "@solidjs/router";
import {
  issueDetailStatus,
  currentIssue,
  issueOccurrences,
  issueDetailError,
  refreshIssueDetail,
} from "../store";
import { severity } from "../severity";

const POLL_MS = 10_000;

const QUALITY_LABELS: Record<number, string> = {
  0: "Type + Message",
  1: "Type Only",
  2: "Fallback",
};

function qualityLabel(q: number): string {
  return QUALITY_LABELS[q] ?? `Q${q}`;
}

function formatTime(nanos: number): string {
  const ms = nanos / 1_000_000;
  return new Date(ms).toLocaleString();
}

function formatAge(nanos: number): string {
  const ms = nanos / 1_000_000;
  const ago = Date.now() - ms;
  if (ago < 60_000) return "just now";
  if (ago < 3_600_000) return `${Math.floor(ago / 60_000)}m ago`;
  if (ago < 86_400_000) return `${Math.floor(ago / 3_600_000)}h ago`;
  return `${Math.floor(ago / 86_400_000)}d ago`;
}

const IssueDetail: Component = () => {
  const params = useParams<{ issueId: string }>();

  onMount(() => {
    void refreshIssueDetail(params.issueId);
    const timer = window.setInterval(
      () => void refreshIssueDetail(params.issueId),
      POLL_MS,
    );
    onCleanup(() => window.clearInterval(timer));
  });

  return (
    <main class="errors-view">
      <header class="fleet-toolbar">
        <div>
          <A href="/errors" class="issue-back">
            &larr; All Issues
          </A>
          <Show when={currentIssue()}>
            {(issue) => {
              const sev = () => severity(issue().max_severity);
              return (
                <>
                  <h1>{issue().title}</h1>
                  <dl class="issue-detail-meta">
                    <div>
                      <dt>Severity</dt>
                      <dd>
                        <span class={`log-sev ${sev().cls}`}>
                          {sev().label}
                        </span>
                      </dd>
                    </div>
                    <div>
                      <dt>Occurrences</dt>
                      <dd>{issue().occurrence_count}</dd>
                    </div>
                    <div>
                      <dt>Quality</dt>
                      <dd>{qualityLabel(issue().grouping_quality)}</dd>
                    </div>
                    <div>
                      <dt>First seen</dt>
                      <dd
                        title={formatTime(issue().first_seen_unix_nano)}
                      >
                        {formatAge(issue().first_seen_unix_nano)}
                      </dd>
                    </div>
                    <div>
                      <dt>Last seen</dt>
                      <dd
                        title={formatTime(issue().last_seen_unix_nano)}
                      >
                        {formatAge(issue().last_seen_unix_nano)}
                      </dd>
                    </div>
                    <div>
                      <dt>Issue ID</dt>
                      <dd class="issue-id-mono">{issue().issue_id}</dd>
                    </div>
                  </dl>
                </>
              );
            }}
          </Show>
        </div>
        <div class="fleet-toolbar-actions">
          <button
            type="button"
            onClick={() => void refreshIssueDetail(params.issueId)}
            disabled={issueDetailStatus() === "loading"}
          >
            {issueDetailStatus() === "loading" ? "Refreshing..." : "Refresh"}
          </button>
        </div>
      </header>

      <Show when={issueDetailError()}>
        {(error) => <div class="fleet-error">{error()}</div>}
      </Show>

      <Switch>
        <Match
          when={
            issueDetailStatus() === "loading" &&
            issueOccurrences().length === 0
          }
        >
          <div class="fleet-empty">Loading occurrences...</div>
        </Match>
        <Match
          when={
            issueDetailStatus() === "error" &&
            issueOccurrences().length === 0
          }
        >
          <div class="fleet-empty">Could not load occurrences.</div>
        </Match>
        <Match when={issueOccurrences().length === 0}>
          <div class="fleet-empty">
            No occurrences found for this issue.
          </div>
        </Match>
        <Match when={true}>
          <h2 class="occ-section-heading">
            Occurrences ({issueOccurrences().length})
          </h2>
          <table class="issue-table">
            <thead>
              <tr>
                <th>Event ID</th>
                <th>Occurred At</th>
                <th>Trace ID</th>
              </tr>
            </thead>
            <tbody>
              <For each={issueOccurrences()}>
                {(occ) => (
                  <tr class="issue-row">
                    <td class="issue-title">{occ.event_id}</td>
                    <td
                      class="issue-time"
                      title={formatTime(occ.occurred_at_unix_nano)}
                    >
                      {formatAge(occ.occurred_at_unix_nano)}
                    </td>
                    <td class="issue-time">{occ.trace_id ?? "---"}</td>
                  </tr>
                )}
              </For>
            </tbody>
          </table>
        </Match>
      </Switch>
    </main>
  );
};

export default IssueDetail;
