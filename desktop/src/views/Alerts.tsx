import { createSignal, type Component } from "solid-js";

import AlertMonitors from "./AlertMonitors";
import NotificationTargets from "./NotificationTargets";

const Alerts: Component = () => {
  const [section, setSection] = createSignal<"monitors" | "targets">("monitors");
  return (
    <div class="alerts-page">
      <nav class="alerts-sections" aria-label="Alerts sections">
        <button type="button" classList={{ active: section() === "monitors" }} aria-current={section() === "monitors" ? "page" : undefined} onClick={() => setSection("monitors")}>Monitors</button>
        <button type="button" classList={{ active: section() === "targets" }} aria-current={section() === "targets" ? "page" : undefined} onClick={() => setSection("targets")}>Notification targets</button>
      </nav>
      {section() === "monitors" ? <AlertMonitors /> : <NotificationTargets />}
    </div>
  );
};

export default Alerts;
