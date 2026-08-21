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

## 2026-08-20 queryd OOM / fields follow-up

- Production `scry-queryd` v0.13.0 was OOM-killed at its 1536 MiB cgroup
  limit. The browser's empty-frame errors were the resulting early TCP close.
- The local UI now starts on a snapped 15-minute range (`desktop/src/store.ts`)
  and surfaces metric-name (`__name__`) metadata errors instead of silently
  rendering an empty picker. TypeScript typecheck + 25 frontend tests pass.
- Direct wire probes against production after queryd restarted prove metadata
  itself works for bounded 1h requests: metrics returned 156 label names and
  999 metric names; logs returned 5 label names and expected values. Each
  request took ~2 seconds. The earlier "no fields" state was caused by the
  wedged/OOM queryd, compounded by swallowed metric-picker errors.
- Queryd RSS rose from 222 MiB after restart to 846 MiB after four bounded
  metadata probes because lazy metadata warming fills the postings cache. Its
  defaults reserve 1024 MiB DataFusion + 256 MiB postings + 256 MiB result +
  64 MiB bloom under a 1536 MiB pod limit; those independent budgets are not a
  safe aggregate process budget.
- Follow-up implemented after Bart authorized proceeding:
  - metadata warming now projects only `(label_name,label_value)` and persists
    one cold block at a time; it neither decodes fingerprint lists nor fills the
    full postings cache;
  - the default query window now applies to metadata frames too;
  - queryd detects a finite cgroup-v2 `memory.max`, keeps 256 MiB headroom by
    default (`--query-memory-reserve-mib`), refuses new data/metadata requests at
    the threshold, and races metadata warming, planning, and batch streaming
    against a 100ms memory monitor;
  - resource refusal is `QUERY_ERR_RESOURCES` with the client-visible message
    `Query too large, reduce range, increase memory or add extra queriers.`;
  - UI defaults to a snapped 15m range and surfaces metric-picker metadata
    errors.
- Verification: `cargo test -p scry-query -p scry-server -p scry-queryd` passed;
  desktop typecheck and all 25 frontend tests passed. Deployment was not changed.
