// stamp-version.mjs — align the frontend version with the workspace Cargo version.
//
// Single source of truth for every first-party version is
// `[workspace.package].version` in the root Cargo.toml (see scripts/release.sh).
// The desktop bundle, however, bakes its displayed version (`__APP_VERSION__`,
// via desktop/vite.config.ts) from desktop/src-tauri/tauri.conf.json — and Tauri
// packaging reads both that and desktop/package.json. Those two JSON files used
// to be hand-maintained and drifted (frozen at 0.9.1 while the crates moved on).
//
// This script re-reads the Cargo version and writes it into both JSON files, so
// `bun run build` (which invokes this first) can never ship a stale UI version.
// It is idempotent: it only rewrites a file when the version actually changes,
// so a no-op run leaves the tree clean (CI won't see a spurious diff).
//
// Run via `bun scripts/stamp-version.mjs` (bun is the project's JS runtime and is
// guaranteed present wherever the frontend is built). No git dependency — it
// reads Cargo.toml, so it also works from a source tarball.

import { readFileSync, writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL("..", import.meta.url));

/** Extract [workspace.package].version from the root Cargo.toml (section-aware). */
function workspaceVersion() {
  const toml = readFileSync(new URL("../Cargo.toml", import.meta.url), "utf8");
  const lines = toml.split(/\r?\n/);
  let inSection = false;
  for (const line of lines) {
    const s = line.trim();
    if (s === "[workspace.package]") {
      inSection = true;
      continue;
    }
    if (inSection && s.startsWith("[") && s !== "[workspace.package]") break;
    if (inSection) {
      const m = s.match(/^version\s*=\s*"([^"]+)"/);
      if (m) return m[1];
    }
  }
  throw new Error("stamp-version: [workspace.package].version not found in Cargo.toml");
}

/**
 * Rewrite the first top-level `"version": "..."` in a JSON file, preserving all
 * formatting/indentation (targeted regex, not parse+stringify). Returns true if
 * the file was changed.
 */
function stampJson(relPath, version) {
  const url = new URL(relPath, import.meta.url);
  const before = readFileSync(url, "utf8");
  const re = /("version"\s*:\s*")[^"]*(")/;
  if (!re.test(before)) {
    throw new Error(`stamp-version: no "version" field in ${relPath}`);
  }
  const after = before.replace(re, `$1${version}$2`);
  if (after === before) return false;
  writeFileSync(url, after);
  return true;
}

const version = workspaceVersion();
const targets = ["../desktop/src-tauri/tauri.conf.json", "../desktop/package.json"];
let changed = 0;
for (const t of targets) {
  if (stampJson(t, version)) {
    changed++;
    console.error(`stamp-version: ${t.replace("../", "")} -> ${version}`);
  }
}
console.error(
  changed === 0
    ? `stamp-version: already at ${version} (no changes)`
    : `stamp-version: stamped ${changed} file(s) to ${version}`,
);
