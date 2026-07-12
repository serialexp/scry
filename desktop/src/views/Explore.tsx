//! Explore — the logs/traces/metrics query view.
//!
//! This is the app's original single screen, lifted verbatim into a route so
//! the routed shell (App.tsx) can host it alongside Dashboards / Alerts /
//! Fleet. Behaviour is unchanged; the Phase 1 re-skin (tab pills, field
//! browser overlay, inspector rail) builds on top of this.

import { Show, type Component } from "solid-js";

import QueryForm from "../components/QueryForm";
import LabelBrowser from "../components/LabelBrowser";
import ResultsTable from "../components/ResultsTable";
import VolumePanel from "../components/VolumePanel";
import { state } from "../store";

const Explore: Component = () => {
  return (
    <div class="app-body">
      <aside class="sidebar">
        <QueryForm />
        <LabelBrowser />
      </aside>
      <main class="main">
        <Show when={state.status === "error" && state.error}>
          <div class="error-banner" role="alert">
            {state.error}
          </div>
        </Show>
        <VolumePanel />
        <ResultsTable />
      </main>
    </div>
  );
};

export default Explore;
