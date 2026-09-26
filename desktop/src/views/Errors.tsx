//! Error tracking issue list, fetched through the selected queryd over the
//! normal query protocol. Shows grouped error issues with occurrence counts,
//! severity, and timing.

import {
  For,
  Match,
  Show,
  Switch,
  createEffect,
  on,
  onCleanup,
  onMount,
  type Component,
} from "solid-js";
import { A, useNavigate } from "@solidjs/router";
import { issueStore } from "../store";
import {
  formatIssueAge as formatAge,
  formatIssueTime as formatTime,
  pollWhileVisible,
  qualityLabel,
} from "../issueStore";
import { ISSUE_PAGE_LIMIT, type Issue } from "../protocol/client";
import { severity } from "../severity";

const POLL_MS = 10_000;

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
        {/* The link makes each issue reachable by keyboard; the row click is a
            pointer convenience. */}
        <A
          href={`/errors/${props.issue.issue_id}`}
          class="issue-title-link"
          onClick={(event) => event.stopPropagation()}
        >
          {props.issue.title}
        </A>
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
  const state = issueStore.state;

  onMount(() => {
    const stop = pollWhileVisible(issueStore.refreshList, POLL_MS);
    onCleanup(stop);
  });
  // A target switch clears the old target's rows; reload without waiting for
  // the next poll.
  createEffect(
    on(issueStore.destination, () => void issueStore.refreshList(), { defer: true }),
  );

  return (
    <main class="errors-view">
      <header class="fleet-toolbar">
        <div>
          <h1>Errors</h1>
          <p>Grouped error issues from the errors database.</p>
        </div>
        <div class="fleet-toolbar-actions">
          <Show when={state.listUpdatedAt}>
            {(updated) => <span>Updated {new Date(updated()).toLocaleTimeString()}</span>}
          </Show>
          <button
            type="button"
            onClick={() => void issueStore.refreshList()}
            disabled={state.listStatus === "loading"}
          >
            {state.listStatus === "loading" ? "Refreshing..." : "Refresh"}
          </button>
        </div>
      </header>

      <Show when={state.listError}>
        {(error) => <div class="fleet-error">{error()}</div>}
      </Show>

      <Switch>
        <Match when={state.listStatus === "loading" && state.issues.length === 0}>
          <div class="fleet-empty">Loading issues...</div>
        </Match>
        <Match when={state.listStatus === "error" && state.issues.length === 0}>
          <div class="fleet-empty">The issue list is unavailable.</div>
        </Match>
        <Match when={state.issues.length === 0}>
          <div class="fleet-empty">No issues have been recorded yet.</div>
        </Match>
        <Match when={true}>
          <Show when={state.issues.length >= ISSUE_PAGE_LIMIT}>
            <p class="issue-page-note">
              Showing the {state.issues.length} most recently seen issues.
            </p>
          </Show>
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
              <For each={state.issues}>{(issue) => <IssueRow issue={issue} />}</For>
            </tbody>
          </table>
        </Match>
      </Switch>
    </main>
  );
};

export default Errors;
