# CURRENT_TASK — v1.0 web UI (own observability frontend)

## What
Build scry's own operator frontend (the v1.0 "own UI" milestone), replacing the
Grafana-adapter direction. Design target = a Claude Design mock
(`scry - Redesign (standalone).html` + `scry-source/`, both gitignored) — a
visual spec in Claude's dc-runtime DSL, NOT portable code. Full design +
phasing + decisions live in **docs/design/v1.0-web-ui.md**.

Four views in one routed SolidJS app (`desktop/`, served in-browser by
scry-webui): **Explore** (Logs/Traces/Metrics query), **Dashboards**,
**Alerts**, **Fleet status**.

## Decisions (settled with Bart)
- **Scope:** full v1.0 UI, built in `desktop/` + scry-webui. Daemons' standalone
  stats.rs pages on :4098 stay untouched.
- **Fleet data path:** the UI asks queryd (new FleetStatusRequest/Response
  frames on the query wire); queryd forwards what it pulls from Valkey via
  discover_status_blobs. scry-webui stays a dumb byte-pipe. No Valkey ⇒ queryd
  refuses with StreamError ("fleet requires Valkey") — no single-instance fake.
- **Dashboards persistence:** object store = source of truth (reserved-prefix
  JSON), catalog table = runtime index. Not Valkey.
- **Alerts:** engine deferred past v1.0. Ship the view inert, gated behind a
  "no backend support yet" state.

## Phasing (see design doc checklist)
- Phase 0 — design tokens + @solidjs/router + routed app shell (nav). ← NEXT
- Phase 1 / 1a — Explore rebuild (logs/traces re-skin) + Metrics tab.
- Phase 2 — Fleet view + FleetStatus wire frames + queryd handler.
- Phase 3 — Dashboards (object-store persistence + catalog index + grid UI).
- Phase 4 — Alerts view (inert).

## Still open (non-blocking for Phases 0–2)
- Tauri parity for Fleet (browser-only vs desktop too).
- Metrics query shape — does the wire return what a multi-series chart needs, or
  is a server-side step/downsample needed? (Investigate in Phase 1a.)
- Auth surface — confirm single-password webui session suffices for all views.

## Status
Design doc written + scope decided. No UI code yet — starting Phase 0.
Prior work (D-057 status pages + webui relay timeout + label errors) committed
in 614f29b.
