import { defineConfig } from "vite";
import solid from "vite-plugin-solid";

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
  build: {
    target: "esnext",
    outDir: "dist",
    emptyOutDir: true,
  },
});
