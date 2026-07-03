//! Top-level layout: a query form in the sidebar, results on the right,
//! and an error banner driven by run status.
//!
//! In the browser shell (served by `scry-webui`) the whole app is gated behind
//! a password → cookie session: until `/api/me` confirms a session we show a
//! loading placeholder, then either the login form or the app. The desktop
//! (Tauri) shell talks straight to the daemon and is always "authed".

import { Show, onMount, type Component } from "solid-js";

import QueryForm from "./components/QueryForm";
import LabelBrowser from "./components/LabelBrowser";
import ResultsTable from "./components/ResultsTable";
import VolumePanel from "./components/VolumePanel";
import LoginForm from "./components/LoginForm";
import {
  state,
  inBrowser,
  authed,
  authChecked,
  checkSession,
  logout,
} from "./store";

const App: Component = () => {
  // Browser shell: probe the existing session cookie once on startup.
  onMount(() => {
    void checkSession();
  });

  return (
    <div class="app">
      <header class="app-header">
        <h1>scry</h1>
        <span class="subtitle">query</span>
        <span class="version" title="scry version">
          v{__APP_VERSION__}
        </span>
        <Show when={inBrowser && authed()}>
          <button type="button" class="logout" onClick={() => void logout()}>
            Log out
          </button>
        </Show>
      </header>

      <Show
        when={authChecked()}
        fallback={<div class="app-loading">Loading…</div>}
      >
        <Show when={authed()} fallback={<LoginForm />}>
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
        </Show>
      </Show>
    </div>
  );
};

export default App;
