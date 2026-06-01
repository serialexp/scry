# CURRENT TASK — scry-webui (browser query UI) — COMPLETE

## What this was

Turn the existing Tauri + SolidJS desktop query app (`desktop/`) into a
browser-accessible web service so the query console is reachable remotely
(home-machine deployment) without the desktop binary. Confirmed design with Bart:
- Remote access by running the UI on the home machine; it connects to
  `scry-queryd` exactly as the Tauri client does.
- Gated by a simple password → cookie.
- **Keep both** desktop and browser (dual-mode frontend).
- **New standalone binary** (`scry-webui`), not folded into `scry-queryd`.

All 7 plan phases are implemented, built, and tested green. See **D-040** for the
full rationale.

## What shipped

**New crate `crates/scry-webui`** (axum 0.8):
- `src/assets.rs` — `rust-embed` of `desktop/dist` (single binary in release;
  disk-read in debug). `#[folder = "../../desktop/dist"]` (relative to crate
  root — NOT `$CARGO_MANIFEST_DIR/...`, which needs the `interpolate-folder-path`
  feature). `use rust_embed::RustEmbed;` alone suffices (the derive generates an
  inherent `get`).
- `src/auth.rs` — `/api/login` `/api/logout` `/api/me`; signed session cookie
  `scry_session` (value = expiry unix-secs), `HttpOnly` + `SameSite=Strict`, NOT
  `Secure` (plain HTTP over LAN). Constant-time password compare.
- `src/query.rs` — `POST /api/query`: auth-gated dumb byte-pipe. Dials the
  server's own `--queryd` (ignores any client addr → SSRF-safe), writes the
  framed request, reads to EOF, returns bytes. 401 unauth, 502 upstream-down.
- `src/lib.rs` — `AppState(Arc<Inner>)` + `FromRef<AppState> for Key`; `router()`.
- `src/main.rs` — clap `--listen`/`--queryd`/`--session-ttl`; password from
  `SCRY_WEBUI_PASSWORD` (never argv); cookie `Key` HKDF-derived from the password
  (domain-separated + padded to the ≥32-byte floor `Key::derive_from` requires).
- `tests/auth.rs` (4) + `tests/query.rs` (3) — tower `oneshot` integration tests.

**Root `Cargo.toml`** — added `crates/scry-webui` member + workspace deps
`axum-extra` (0.10, `cookie`+`cookie-signed`), `cookie` (0.18, `key-expansion`),
`rust-embed` (8, `mime-guess`), `mime_guess`, `time`.

**Frontend (`desktop/src/`) — dual-mode:**
- `protocol/transport.ts` — now the `Transport` interface ONLY (no `@tauri-apps`).
- `protocol/transport-tauri.ts` — `TauriTransport` (native socket).
- `protocol/transport-http.ts` — `HttpTransport` (`fetch` `/api/query`) +
  `UnauthorizedError`.
- `env.ts` — `isTauri()` (checks `window.__TAURI_INTERNALS__`).
- `store.ts` — lazy `getTransport()` (dynamic `import()`); auth state (`authed`,
  `authChecked`, `inBrowser`, `checkSession`/`login`/`logout`); relay 401 → logout.
- `components/LoginForm.tsx` (new, default export); `App.tsx` (gate + logout
  button, browser only — default export, matches `index.tsx`); `QueryForm.tsx`
  (Daemon field hidden when `!isTauri()`).
- `styles.css` — login form + logout button styles (CSS lives here, NOT App.css).

**`scripts/smoke-webui.sh`** — builds bundle + release binary, stub upstream,
asserts SPA serve + `/api/me` 401→204 + wrong/right login + `/api/query` auth
gate + byte-pipe relay + logout. Self-contained. **PASSES (8/8).**

**Docs:** D-040 in `docs/decisions.md`; CLAUDE.md (Web UI crate + desktop
dual-mode + smoke-webui command/tooling); README.md (workspace line).

## Verification status (all green)

- `cargo test -p scry-webui` → 4 auth + 3 query tests pass.
- `cargo build --workspace` → clean.
- `desktop` `bun run build` → clean (163 modules; transport-tauri in its own
  chunk so the browser bundle never loads `@tauri-apps`).
- `scripts/smoke-webui.sh` → ALL 8 CHECKS PASSED.

## NOT yet done

- **No commits.** Per the per-phase workflow, nothing committed — Bart commits
  when he asks. Suggested whole-file split (Rule #13): (1) scry-webui crate +
  root Cargo.toml + Cargo.lock, (2) frontend dual-mode (desktop/src/*), (3)
  smoke-webui.sh, (4) docs (README/CLAUDE/decisions). Or bundle as Bart prefers.
- TLS: cookie is not `Secure`; for non-LAN exposure put it behind a TLS proxy
  and flip `Secure`.

## Environment caveat this session

The tool-output renderer intermittently corrupted/duplicated/blanked Bash and
Read output. Two real consequences were caught and repaired: (a) `App.tsx` got
overwritten against fabricated file contents (wrong export style, imports of a
nonexistent `./components/ResultTable` + `./App.css`) — rewritten to match the
real conventions (default exports, `ResultsTable`, `styles.css`); (b) the
CLAUDE.md doc edits silently no-op'd because the file hadn't been Read — re-applied
after a real Read. Everything was re-verified via `cargo`/`bun`/smoke exit codes
and `git status`, not by trusting rendered stdout.
