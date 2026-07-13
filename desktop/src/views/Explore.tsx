//! Explore — the logs/traces/metrics query view (Phase 1 layout).
//!
//! Rebuilt to the mock's shape: a horizontal query bar (signal tabs + token
//! matcher bar + range pills + Run), a fields strip, then a results row with
//! the main result pane (volume + table/reader) and a right-hand inspector
//! rail. All behaviour reads/writes the shared store — no prop drilling.

import { Show, type Component } from "solid-js";

import QueryBar from "../components/explore/QueryBar";
import FieldsStrip from "../components/explore/FieldsStrip";
import InspectorRail from "../components/explore/InspectorRail";
import ResultsTable from "../components/ResultsTable";
import VolumePanel from "../components/VolumePanel";
import MetricsPanel from "../components/explore/MetricsPanel";
import { state } from "../store";

const Explore: Component = () => {
  return (
    <div class="explore">
      <QueryBar />
      <FieldsStrip />
      <div class="explore-results">
        <main class="explore-main">
          <Show when={state.status === "error" && state.error}>
            <div class="error-banner" role="alert">
              {state.error}
            </div>
          </Show>
          <VolumePanel />
          <MetricsPanel />
          <ResultsTable />
        </main>
        <InspectorRail />
      </div>
    </div>
  );
};

export default Explore;
