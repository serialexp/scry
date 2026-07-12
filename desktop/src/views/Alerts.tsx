//! Alerts (Phase 4 — inert). The alerting *engine* (rule storage, evaluation,
//! ok/pending/firing, notifiers) is deliberately deferred past v1.0
//! (docs/design/v1.0-web-ui.md). The view exists so the four-view layout is
//! complete, but it is explicitly gated behind a "no backend support yet"
//! state — a placeholder, not a half-built feature.

import { type Component } from "solid-js";

const Alerts: Component = () => {
  return (
    <div class="view-placeholder">
      <h2>Alerts</h2>
      <div class="inert-badge">No backend support yet</div>
      <p>
        Alert rules and notifications are not evaluated by scry yet. This view
        is a placeholder for a future alerting engine; nothing here is live.
      </p>
    </div>
  );
};

export default Alerts;
