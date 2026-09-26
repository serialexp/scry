//! Issue detail page: issue metadata header + occurrences list.
//!
//! Reached via `/errors/:issueId` after clicking an issue row in the
//! Errors list view. Polls for updates every 10 seconds (same cadence
//! as the list view), completion-relative and paused while the page is
//! hidden. Navigating between issues clears the previous issue's data and
//! discards its in-flight responses (see `issueStore.ts`).

import { For, Match, Show, Switch, createEffect, on, onCleanup, type Component } from "solid-js";
import { useParams, A } from "@solidjs/router";
import { issueStore } from "../store";
import {
  formatIssueAge as formatAge,
  formatIssueTime as formatTime,
  pollWhileVisible,
  qualityLabel,
} from "../issueStore";
import { ISSUE_PAGE_LIMIT } from "../protocol/client";
import { severity } from "../severity";

const POLL_MS = 10_000;

const IssueDetail: Component = () => {
  const params = useParams<{ issueId: string }>();
  const state = issueStore.state;

  createEffect(
    on(
      () => params.issueId,
      (issueId) => {
        issueStore.selectIssue(issueId);
        const stop = pollWhileVisible(issueStore.refreshDetail, POLL_MS);
        onCleanup(stop);
      },
    ),
  );
  // A target switch clears the old target's data; reload without waiting for
  // the next poll.
  createEffect(
    on(issueStore.destination, () => void issueStore.refreshDetail(), { defer: true }),
  );

  const notFound = () => state.detailStatus === "ready" && state.issue === null;

  return (
    <main class="errors-view">
      <header class="fleet-toolbar">
        <div>
          <A href="/errors" class="issue-back">
            &larr; All Issues
          </A>
          <Show when={state.issue}>
            {(issue) => {
              const sev = () => severity(issue().max_severity);
              return (
                <>
                  <h1>{issue().title}</h1>
                  <dl class="issue-detail-meta">
                    <div>
                      <dt>Severity</dt>
                      <dd>
                        <span class={`log-sev ${sev().cls}`}>{sev().label}</span>
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
                      <dd title={formatTime(issue().first_seen_unix_nano)}>
                        {formatAge(issue().first_seen_unix_nano)}
                      </dd>
                    </div>
                    <div>
                      <dt>Last seen</dt>
                      <dd title={formatTime(issue().last_seen_unix_nano)}>
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
            onClick={() => void issueStore.refreshDetail()}
            disabled={state.detailStatus === "loading"}
          >
            {state.detailStatus === "loading" ? "Refreshing..." : "Refresh"}
          </button>
        </div>
      </header>

      <Show when={state.detailError}>
        {(error) => <div class="fleet-error">{error()}</div>}
      </Show>

      <Switch>
        <Match when={notFound()}>
          <div class="fleet-empty">This issue does not exist in the errors database.</div>
        </Match>
        <Match
          when={
            (state.detailStatus === "idle" || state.detailStatus === "loading") &&
            state.occurrences.length === 0
          }
        >
          <div class="fleet-empty">Loading occurrences...</div>
        </Match>
        <Match when={state.detailStatus === "error" && state.occurrences.length === 0}>
          <div class="fleet-empty">Could not load occurrences.</div>
        </Match>
        <Match when={state.occurrences.length === 0}>
          <div class="fleet-empty">No occurrences found for this issue.</div>
        </Match>
        <Match when={true}>
          <h2 class="occ-section-heading">
            <Show
              when={state.occurrences.length >= ISSUE_PAGE_LIMIT}
              fallback={<>Occurrences ({state.occurrences.length})</>}
            >
              Latest {state.occurrences.length} occurrences
            </Show>
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
              <For each={state.occurrences}>
                {(occ) => (
                  <tr class="issue-row">
                    <td class="issue-title">{occ.event_id}</td>
                    <td class="issue-time" title={formatTime(occ.occurred_at_unix_nano)}>
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
