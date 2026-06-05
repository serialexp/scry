import { defineConfig } from "vitest/config";

// Standalone test config (does not extend vite.config.ts, so the solid plugin
// and the tauri.conf.json version read are skipped). The tested modules
// (traces.ts, format.ts) are pure and DOM-free, so a node environment suffices.
export default defineConfig({
  test: {
    environment: "node",
    include: ["src/**/*.test.ts"],
  },
});
