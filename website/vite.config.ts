import { defineConfig } from "vite";
import solid from "vite-plugin-solid";

// Plain static SPA — no backend, no env coupling. `bun run build` emits a
// fully self-contained bundle into dist/, which the nginx image serves as-is.
export default defineConfig({
  plugins: [solid()],
  server: {
    // Listen on all interfaces and accept any Host header, so the dev server is
    // reachable through a reverse proxy / tunnel (e.g. home.serial-experiments.com)
    // rather than only localhost. Dev-only — the production build is static.
    host: true,
    allowedHosts: true,
  },
  build: {
    target: "esnext",
    outDir: "dist",
    emptyOutDir: true,
  },
});
