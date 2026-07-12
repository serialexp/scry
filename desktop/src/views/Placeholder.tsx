//! Shared "not built yet" placeholder for routes whose UI lands in later
//! phases (Dashboards — Phase 3; Fleet — Phase 2). Alerts uses its own inert
//! state (see Alerts.tsx) because its engine is deliberately deferred, not
//! merely unbuilt.

import { type Component } from "solid-js";

const Placeholder: Component<{ title: string; note: string }> = (props) => {
  return (
    <div class="view-placeholder">
      <h2>{props.title}</h2>
      <p>{props.note}</p>
    </div>
  );
};

export default Placeholder;
