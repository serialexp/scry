//! Top-level routed shell.
//!
//! In the browser shell (served by `scry-webui`) the whole app is gated behind
//! a password → cookie session: until `/api/me` confirms a session we show a
//! loading placeholder, then either the login form or the app. The desktop
//! (Tauri) shell talks straight to the daemon and is always "authed".
//!
//! Once authed, the app is a `@solidjs/router` with four views — Explore,
//! Dashboards, Alerts, Fleet — hosted under a shared shell (brand + nav +
//! version + logout). The query path lives entirely in Explore; the other
//! views are placeholders until their phases land.

import { Show, onMount, type Component, type JSX } from "solid-js";
import { HashRouter, Route, Navigate, A } from "@solidjs/router";

import LoginForm from "./components/LoginForm";
import ConnectionPicker from "./components/ConnectionPicker";
import Explore from "./views/Explore";
import Dashboards from "./views/Dashboards";
import Alerts from "./views/Alerts";
import Fleet from "./views/Fleet";
import { inBrowser, authed, authChecked, checkSession, logout } from "./store";

/** Shared chrome: brand, primary nav, version, logout. Wraps every route. */
const Shell: Component<{ children?: JSX.Element }> = (props) => {
  return (
    <div class="app">
      <header class="app-header">
        <div class="brand">
          <span class="brand-mark" aria-hidden="true" />
          <span class="brand-name">scry</span>
        </div>
        <nav class="app-nav">
          <A href="/explore" class="nav-link" activeClass="active">
            Explore
          </A>
          <A href="/dashboards" class="nav-link" activeClass="active">
            Dashboards
          </A>
          <A href="/alerts" class="nav-link" activeClass="active">
            Alerts
          </A>
          <A href="/fleet" class="nav-link" activeClass="active">
            Fleet
          </A>
        </nav>
        <div class="app-header-right">
          <ConnectionPicker />
          <span class="version" title="scry version">
            v{__APP_VERSION__}
          </span>
          <Show when={inBrowser && authed()}>
            <button type="button" class="logout" onClick={() => void logout()}>
              Log out
            </button>
          </Show>
        </div>
      </header>
      {props.children}
    </div>
  );
};

const App: Component = () => {
  // Browser shell: probe the existing session cookie once on startup.
  onMount(() => {
    void checkSession();
  });

  return (
    <Show
      when={authChecked()}
      fallback={<div class="app-loading">Loading…</div>}
    >
      <Show when={authed()} fallback={<LoginForm />}>
        <HashRouter root={Shell}>
          <Route path="/" component={() => <Navigate href="/explore" />} />
          <Route path="/explore" component={Explore} />
          <Route path="/dashboards" component={Dashboards} />
          <Route path="/alerts" component={Alerts} />
          <Route path="/fleet" component={Fleet} />
          {/* Unknown paths land on Explore. */}
          <Route path="*" component={Explore} />
        </HashRouter>
      </Show>
    </Show>
  );
};

export default App;
