#!/usr/bin/env node
// Stamp a version string into src-tauri/tauri.conf.json so the built desktop
// bundles / installers carry the release version instead of the hardcoded
// placeholder. CI runs this before `tauri build` with the git tag, e.g.
//
//   bun scripts/stamp-version.mjs v0.6.0     # -> tauri.conf.json "version": "0.6.0"
//
// A leading `v` is stripped. The result must be valid for every bundler we
// ship — notably the Windows MSI (WiX), whose ProductVersion is numeric
// `major.minor.patch[.build]` ONLY: a pre-release/build suffix (`-rc1`,
// `+meta`) will break the Windows job. Keep release tags plain `vX.Y.Z`.
//
// Path override for testing: set SCRY_TAURI_CONF to a file path.

import { readFileSync, writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

const raw = (process.argv[2] ?? process.env.SCRY_VERSION ?? "").trim();
const version = raw.replace(/^v/, "");

// Strict numeric semver core only (no -prerelease / +build) — see note above.
if (!/^\d+\.\d+\.\d+$/.test(version)) {
  console.error(
    `stamp-version: refusing version '${raw}': need plain MAJOR.MINOR.PATCH ` +
      `(optionally v-prefixed). Pre-release/build suffixes break the Windows MSI.`,
  );
  process.exit(1);
}

const here = dirname(fileURLToPath(import.meta.url));
const confPath =
  process.env.SCRY_TAURI_CONF ?? join(here, "..", "src-tauri", "tauri.conf.json");

const conf = JSON.parse(readFileSync(confPath, "utf8"));
const prev = conf.version;
conf.version = version;
writeFileSync(confPath, JSON.stringify(conf, null, 2) + "\n");

console.log(`stamp-version: ${prev} -> ${version} (${confPath})`);
