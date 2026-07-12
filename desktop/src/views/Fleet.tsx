//! Fleet status (Phase 2). Renders the Valkey-aggregated fleet that queryd
//! forwards over new FleetStatus wire frames. Placeholder until those frames +
//! the queryd handler land.

import { type Component } from "solid-js";
import Placeholder from "./Placeholder";

const Fleet: Component = () => (
  <Placeholder
    title="Fleet status"
    note="Live instance status arrives in a later phase, forwarded by queryd from Valkey. Requires a Valkey-connected queryd."
  />
);

export default Fleet;
