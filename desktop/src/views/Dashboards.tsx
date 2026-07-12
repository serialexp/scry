//! Dashboards (Phase 3). Object-store-persisted panels indexed in the catalog.
//! Placeholder until the panel model + persistence land.

import { type Component } from "solid-js";
import Placeholder from "./Placeholder";

const Dashboards: Component = () => (
  <Placeholder
    title="Dashboards"
    note="Saved panels arrive in a later phase. Dashboards will persist to the object store, indexed by the catalog."
  />
);

export default Dashboards;
