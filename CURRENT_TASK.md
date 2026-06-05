# CURRENT TASK — traces waterfall view (v1.0 UI) — COMPLETE (uncommitted)

## What this was

The first feature step of v1.0's own-UI direction: give traces a purpose-built
**single-trace waterfall** view in the query app (`desktop/`, also served to the
browser by `scry-webui`), mirroring the logs reader view. Backend was already
complete (v0.5 trace-id lookup + promoted matchers + SELECT *) — this is
**frontend-only**, no backend / wire / scry-webui change.

Plan: `~/.claude/plans/eager-noodling-sketch.md`. Scope confirmed with Bart:
- **Single-trace only** — waterfall renders when the result has exactly one
  distinct `trace_id`; multi-trace results fall back to the generic table.
- **Add vitest** for the pure span-tree + layout logic.

## What shipped (all in desktop/)

**New files:**
- `src/format.ts` — shared formatting helpers lifted out of `ResultsTable.tsx`
  (`toHex`, `fmtCell`, `attrVal`, `attrEntries`, `fmtTs`) + new `fmtDuration`.
- `src/traces.ts` — pure, DOM-free trace logic: `Span`/`PlacedSpan`/`SpanLayout`
  types, `decodeSpan`/`decodeSpans`, `singleTraceId` (dispatch key),
  `buildSpanTree` (pre-order + depth, orphan-as-root, cycle-guarded),
  `traceWindow`, `layoutSpans` ([0,1] fractions, zero-width → full), `kindLabel`,
  `statusLabel`, `serviceHue`.
- `src/traces.test.ts` — 13 vitest cases (tree/depth/orphan/multi-root/cycle,
  layout fractions + zero-width, singleTraceId 0/1/2, decodeSpan hex+fallback,
  serviceHue stability).
- `src/components/TracesView.tsx` — the waterfall: meta strip (trace id, span
  count, total duration, root service, span filter, raw toggle), per-span row
  (depth indent, kind, name, hue-coloured service chip, status, positioned
  duration bar, error outline), `<details>` expander (ids, status message,
  attributes, events, links).
- `vitest.config.ts` — standalone (node env, no solid plugin).
- `scripts/trace-render-check.ts` — **manual** headless e2e aid (not CI): logs
  into a running scry-webui, does a real by-id lookup through `/api/query`,
  decodes Arrow, runs the production traces.ts fns, asserts a sane tree. Keep or
  drop as you like.

**Modified:**
- `src/components/ResultsTable.tsx` — dropped the moved helpers (import from
  `format.ts`); added the `traces` memo (trace cols present + `singleTraceId`);
  dispatch branch `!raw() && traces()` → `<TracesView/>`; generic-table + raw
  toggle conditions now account for traces too. Logs view untouched.
- `src/styles.css` — `.trace-*` waterfall styles (existing theme vars).
- `package.json` — `vitest` devDep + `"test": "vitest run"`.

## Verification (all green)

- `bun run test` → 13/13 pass.
- `bun run typecheck` → clean.
- `bun run build` → clean; `transport-tauri` still its own chunk (browser bundle
  stays `@tauri-apps`-free).
- `scripts/smoke-webui.sh` → all 8 checks pass (relay path unaffected).
- **Live data e2e:** ingested 3200 spans via noise-spewer → 2 trace blocks →
  scry-queryd → scry-webui; `scripts/trace-render-check.ts` confirmed a by-id
  lookup returns one trace's 4 spans, `singleTraceId` routes to the waterfall,
  `buildSpanTree` yields root+3 children with correct depths/parents, services
  decode (api/worker), geometry in [0,1]. Proves the real Arrow `toJSON()` shapes
  match the code's assumptions.
- **Visual render NOT eyeballed** — the browser MCP tool refuses local/LAN
  addresses (`ERR_BLOCKED_BY_CLIENT`), so the pixel layout still wants a human
  look. To do it: `(cd desktop && bun run build) && cargo build --release -p
  scry-webui`, run a queryd with trace data, then
  `SCRY_WEBUI_PASSWORD=… scry-webui --listen 0.0.0.0:8080 --queryd <addr>` and
  open it; Traces → paste a trace_id → waterfall.

## NOT yet done

- **No commits.** Suggested whole-file split (Rule #13): (1) `format.ts` +
  `ResultsTable.tsx`, (2) `traces.ts` + `traces.test.ts` + `vitest.config.ts` +
  `package.json`, (3) `TracesView.tsx` + `styles.css`, (4) the optional
  `scripts/trace-render-check.ts`. Or bundle as you prefer.
- **Docs not updated** — the plan scoped only the view + tests. README milestone
  table, a decision record (D-041?), and CLAUDE.md still describe v1.0 traces as
  view-less. Worth a follow-up if you want the docs to track this.
- Multi-trace overview/list and profiles flamegraph remain out of scope.
