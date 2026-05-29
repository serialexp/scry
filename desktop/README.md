# scry desktop — query app

A small desktop app for running queries against a `scry-queryd` daemon —
the GUI alternative to the `scry-query` CLI. Built as a **TypeScript
implementation of the scry query wire protocol** wrapped in a
[Tauri](https://tauri.app) (v2) window with a [SolidJS](https://solidjs.com)
frontend.

## Why a desktop app

A browser can't open the raw TCP socket `scry-queryd` listens on. A
desktop shell can. So the app opens a **native socket** and speaks the
real binschema query protocol directly — no HTTP shim, no WebSocket
bridge, no second API to keep in sync with the wire.

## Architecture

The query protocol lives **entirely in TypeScript**; Rust is only a
dumb byte pipe for the one thing a webview can't do (open a socket).

```
 SolidJS UI ─▶ store.ts ─▶ protocol/client.ts ─▶ protocol/transport.ts
                                │                        │ invoke("run_query")
                                │ binschema encode/decode ▼
                          proto/generated.ts        src-tauri (Rust)
                          (vendored, generated)     tokio TcpStream ──▶ scry-queryd
                                ▲ Arrow IPC decode (apache-arrow)
```

- **`src/proto/`** — the TypeScript query-protocol bindings, generated
  from `proto/query.schema.json` by binschema (`scripts/gen-proto-ts.sh`,
  the TS counterpart to `scripts/gen-proto.sh`). Vendored + committed, so
  a normal `bun install && bun run build` never needs binschema.
- **`src/protocol/`** — hand-written client layer over the bindings:
  - `constants.ts` — `Signal` + `QUERY_ERR_*` mirrored from
    `crates/proto/src/constants.rs`.
  - `framing.ts` — `[len:u32 BE][body]` framing (mirrors
    `crates/proto/src/framing.rs`).
  - `client.ts` — builds a `QueryRequest`, drives a `Transport`, decodes
    the `SchemaMsg`/`BatchMsg*`/`EndOfStream`/`StreamError` response, and
    concatenates the Arrow IPC payloads into one stream for
    `apache-arrow` to parse.
  - `transport.ts` — `Transport` interface + `TauriTransport` (calls the
    Rust `run_query` command). The interface keeps the protocol
    transport-agnostic; a future WebSocket transport for a pure-browser
    build would slot in here.
- **`src-tauri/`** — the Rust shell. `run_query(addr, request)` connects,
  writes the already-framed request bytes verbatim, reads to EOF (one TCP
  connection per query), and returns the raw bytes as a
  `tauri::ipc::Response` (an `ArrayBuffer` on the JS side). It contains
  **zero** protocol knowledge, so it never needs touching when the wire
  schema evolves — only the TS bindings re-generate.

## Develop / run

Prereqs: `bun`, a Rust toolchain, and Tauri's Linux deps
(`webkit2gtk-4.1`, `libsoup-3.0`). All present on the dev box.

```bash
cd desktop
bun install
bun run app:dev      # launches the Tauri window (cargo tauri dev)
```

Point the "Daemon address" field at a running `scry-queryd` (default
`127.0.0.1:4100`), pick a signal, add matchers / time bounds / SQL /
(for traces) a trace id, and Run.

Build a distributable bundle:

```bash
bun run app:build    # cargo tauri build
```

## Install

`install.sh` is the user-facing installer (same shape as the `dbui`
installer).

**On this dev box, right now — build from source and install:**

```bash
desktop/install.sh --local
```

This compiles the release binary (`cargo tauri build --no-bundle`) and
drops it at `~/.local/bin/scry-desktop` with an XDG desktop entry + icon,
so "scry" shows up in your application menu. No root, and no AppImage
bundle (which would need `librsvg2-dev` at bundle time) — the binary
links against the system `webkit2gtk` already present. The server binary
is `scry`; the app installs as `scry-desktop` so they don't collide.

**From a published release (any machine):**

```bash
desktop/install.sh
```

Detects the platform and installs the latest published `desktop-v*`
GitHub release — AppImage (preferred) or `.deb` on Linux, `.dmg` on
macOS, `.msi` instructions on Windows.

## Releases / CI

`.github/workflows/release-desktop.yml` builds the per-platform bundles
with [`tauri-apps/tauri-action`](https://github.com/tauri-apps/tauri-action)
across a macOS (arm + intel) / Ubuntu / Windows matrix and attaches them
to a **draft** GitHub Release. (The core server crates are not built here
— they're a separate Docker pipeline, and the desktop crate is excluded
from the cargo workspace.)

The jobs run on **[Depot](https://depot.dev) GitHub Actions runners**
(`depot-ubuntu-22.04`, `depot-macos-latest`, `depot-windows-latest`).
This requires a **one-time, dashboard-only** step: connect the `serialexp`
GitHub org to Depot (install the Depot GitHub App + enable runners). Until
that's done, `depot-*` jobs queue with no runner. Runners are an
org-level Depot feature, independent of the `depot.json` project id (which
only governs container builds / `depot bake`). Depot's macOS runners are
Apple Silicon only, so the Intel `.dmg` is cross-compiled from the ARM
runner — both Mac architectures still ship. To fall back to GitHub-hosted
runners, drop the `depot-` prefix from each `runs-on` label.

To cut a release:

```bash
git tag desktop-v0.1.0
git push origin desktop-v0.1.0   # → workflow runs, creates a draft release
```

Review the draft's assets, then publish it. `install.sh` (release mode)
pulls the latest *published* `desktop-v*` release, so it only sees it
once published. The Ubuntu runner `apt install`s `libwebkit2gtk-4.1-dev`
+ `librsvg2-dev` so the AppImage actually bundles there.

## Regenerating the protocol bindings

After changing `proto/query.schema.json`:

```bash
scripts/gen-proto-ts.sh          # from the repo root
# or: cd desktop && bun run gen-proto
```

This re-vendors `src/proto/*` (generated code + binschema TS runtime).

## Headless protocol smoke

`scripts/smoke-protocol.ts` exercises the **exact** production code path
(`runQuery` + the generated bindings + Arrow decode) against a live
`scry-queryd`, but over a `node:net` transport instead of Tauri — so it
runs without a display. Useful to prove the protocol independently of the
GUI:

```bash
# with a scry-queryd running on 127.0.0.1:4100 (see scripts/smoke.sh for
# how to stand one up with data)
cd desktop && bun run scripts/smoke-protocol.ts 127.0.0.1:4100
```

## Known issue: binschema TS generator

The binschema 0.6.x **TypeScript** generator emits code that does not
satisfy a strict `tsconfig`:

- discriminated-union members are *typed* as a bare union
  (`QueryRequestOutput | SchemaMsgOutput | …`) but the emitted
  encoder/decoder use a tagged `{ type, value }` envelope at runtime —
  so the generator's own code references `.type`/`.value` on a type that
  doesn't declare them;
- the generated decoder reaches into the runtime base class's **private**
  `byteOffset`;
- unused locals/params under `noUnusedLocals`/`noUnusedParameters`.

The **runtime behaviour is correct** (verified end-to-end against a live
daemon) — these are purely static-typing defects in the generator. Two
consequences here:

1. `scripts/gen-proto-ts.sh` stamps a `// @ts-nocheck` banner onto each
   vendored file so our own source still typechecks strictly.
2. `src/protocol/client.ts` bridges the bare-union-vs-tagged mismatch
   with a single `as unknown as` cast at each boundary (see the
   `TaggedFrame` note there).

The Rust generator does not have this bug (its discriminated unions are
proper enums). Worth fixing upstream in binschema so the TS bindings are
strict-clean and the casts/`@ts-nocheck` can go away.
