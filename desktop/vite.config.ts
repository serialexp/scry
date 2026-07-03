import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import { defineConfig } from "vite";
import solid from "vite-plugin-solid";

// Single source of truth for the displayed app version: src-tauri/tauri.conf.json,
// which `scripts/stamp-version.mjs` rewrites from the workspace Cargo.toml
// `[workspace.package].version` — that script runs first in the `build` npm
// script (`bun ../scripts/stamp-version.mjs && vite build`), so the number here
// can never drift from the crate version. We bake it into the bundle as the
// compile-time constant `__APP_VERSION__` so BOTH shells show the same number:
// the Tauri desktop bundle and the browser bundle embedded by scry-webui. No
// runtime Tauri API is involved, so it works in the browser, which has none.
const tauriConf = JSON.parse(
  readFileSync(
    fileURLToPath(new URL("./src-tauri/tauri.conf.json", import.meta.url)),
    "utf8",
  ),
) as { version: string };

// Tauri expects a fixed dev port and surfaces Rust errors clearly, so we
// disable Vite's screen-clearing and pin the port. `TAURI_*` env vars are
// injected by the Tauri CLI during `tauri dev` / `tauri build`.
export default defineConfig({
  plugins: [solid()],
  clearScreen: false,
  server: {
    port: 1420,
    strictPort: true,
  },
  envPrefix: ["VITE_", "TAURI_"],
  define: {
    __APP_VERSION__: JSON.stringify(tauriConf.version),
  },
  build: {
    target: "esnext",
    outDir: "dist",
    emptyOutDir: true,
  },
});
