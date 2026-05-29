//! Top-level layout: a query form in the sidebar, results on the right,
//! and an error banner driven by run status.

import { Show, type Component } from "solid-js";

import QueryForm from "./components/QueryForm";
import ResultsTable from "./components/ResultsTable";
import { state } from "./store";

const App: Component = () => {
  return (
    <div class="app">
      <header class="app-header">
        <h1>scry</h1>
        <span class="subtitle">query</span>
      </header>
      <div class="app-body">
        <aside class="sidebar">
          <QueryForm />
        </aside>
        <main class="main">
          <Show when={state.status === "error" && state.error}>
            <div class="error-banner" role="alert">
              {state.error}
            </div>
          </Show>
          <ResultsTable />
        </main>
      </div>
    </div>
  );
};

export default App;
