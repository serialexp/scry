//! Error tracking issue list, fetched through the selected queryd over the
//! normal query protocol. Shows grouped error issues with occurrence counts,
//! severity, and timing.

import { For, Match, Show, Switch, onCleanup, onMount, type Component } from "solid-js";
import { useNavigate } from "@solidjs/router";
import {
  issueListStatus,
  issues,
  issueListError,
  issueListUpdatedAt,
  refreshIssues,
} from "../store";
import type { Issue } from "../protocol/client";
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

const IssueRow: Component<{ issue: Issue }> = (props) => {
  const sev = () => severity(props.issue.max_severity);
  const navigate = useNavigate();

  return (
    <tr
      class="issue-row"
      style={{ cursor: "pointer" }}
      onClick={() => navigate(`/errors/${props.issue.issue_id}`)}
    >
      <td class="issue-title" title={props.issue.issue_id}>
        {props.issue.title}
      </td>
      <td class="issue-count">{props.issue.occurrence_count}</td>
      <td>
        <span class={`log-sev ${sev().cls}`}>{sev().label}</span>
      </td>
      <td class="issue-quality">{qualityLabel(props.issue.grouping_quality)}</td>
      <td class="issue-time" title={formatTime(props.issue.first_seen_unix_nano)}>
        {formatAge(props.issue.first_seen_unix_nano)}
      </td>
      <td class="issue-time" title={formatTime(props.issue.last_seen_unix_nano)}>
        {formatAge(props.issue.last_seen_unix_nano)}
      </td>
    </tr>
  );
};

const Errors: Component = () => {
  onMount(() => {
    void refreshIssues();
    const timer = window.setInterval(() => void refreshIssues(), POLL_MS);
    onCleanup(() => window.clearInterval(timer));
  });

  return (
    <main class="errors-view">
      <header class="fleet-toolbar">
        <div>
          <h1>Errors</h1>
          <p>Grouped error issues from the errors database.</p>
        </div>
        <div class="fleet-toolbar-actions">
          <Show when={issueListUpdatedAt()}>
            {(updated) => <span>Updated {new Date(updated()).toLocaleTimeString()}</span>}
          </Show>
          <button
            type="button"
            onClick={() => void refreshIssues()}
            disabled={issueListStatus() === "loading"}
          >
            {issueListStatus() === "loading" ? "Refreshing..." : "Refresh"}
          </button>
        </div>
      </header>

      <Show when={issueListError()}>
        {(error) => <div class="fleet-error">{error()}</div>}
      </Show>

      <Switch>
        <Match when={issueListStatus() === "loading" && issues().length === 0}>
          <div class="fleet-empty">Loading issues...</div>
        </Match>
        <Match when={issueListStatus() === "error" && issues().length === 0}>
          <div class="fleet-empty">
            Issue list is unavailable. Ensure queryd is started with --errors-db.
          </div>
        </Match>
        <Match when={issues().length === 0}>
          <div class="fleet-empty">No issues have been recorded yet.</div>
        </Match>
        <Match when={true}>
          <table class="issue-table">
            <thead>
              <tr>
                <th>Title</th>
                <th>Count</th>
                <th>Severity</th>
                <th>Quality</th>
                <th>First seen</th>
                <th>Last seen</th>
              </tr>
            </thead>
            <tbody>
              <For each={issues()}>{(issue) => <IssueRow issue={issue} />}</For>
            </tbody>
          </table>
        </Match>
      </Switch>
    </main>
  );
};

export default Errors;
