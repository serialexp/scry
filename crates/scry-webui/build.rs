//! Ensure the rust-embed asset folder exists at compile time — and, opt-in,
//! build the frontend bundle so a single `cargo` invocation embeds a fresh UI.
//!
//! `assets.rs` embeds `desktop/dist` via `#[derive(RustEmbed)]`, whose derive
//! macro errors at compile time if the folder is missing. `desktop/dist` is a
//! gitignored Vite build artifact, so in a clean checkout — CI's
//! `cargo build --workspace`, a fresh clone, anyone who hasn't run
//! `bun run build` — it doesn't exist, and the crate fails to compile even
//! when nobody cares about the web UI.
//!
//! ## Default (no env flag): node-free
//! Creating the directory (empty) lets the build proceed. An empty bundle is
//! harmless: every asset lookup misses and `assets::serve` returns the runtime
//! "bundle missing — run `bun run build`" 500. When the real bundle is present
//! (the documented flow, `scripts/smoke-webui.sh`, the home deploy) this is a
//! no-op and the actual assets are embedded. A plain `cargo build` never needs
//! node/bun — this is a deliberate, load-bearing property (CI relies on it).
//!
//! ## Opt-in (`SCRY_EMBED_WEBUI=1`): build + embed in one command
//! When the flag is truthy we run `bun install` + `bun run build` in `desktop/`
//! first, so `rust-embed` embeds a freshly built, correctly-versioned bundle
//! (`bun run build` stamps the version via `scripts/stamp-version.mjs`). Used by
//! the home deploy: `SCRY_EMBED_WEBUI=1 cargo install --path crates/scry`. If
//! the flag is set and the build can't run, we panic — never silently ship the
//! empty-bundle fallback when the caller explicitly asked to embed.

use std::env;
use std::path::{Path, PathBuf};
use std::process::Command;

fn main() {
    let manifest = env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR is set by cargo");
    let desktop = Path::new(&manifest).join("../../desktop");
    let dist = desktop.join("dist");

    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-env-changed=SCRY_EMBED_WEBUI");

    if embed_requested() {
        // Rebuild the embedded bundle whenever the frontend inputs change.
        for f in ["package.json", "src-tauri/tauri.conf.json"] {
            println!("cargo:rerun-if-changed={}", desktop.join(f).display());
        }
        watch_tree(&desktop.join("src"));
        build_frontend(&desktop);
    }

    // Best-effort: a no-op when the real bundle already exists (built above, by
    // a prior `bun run build`, or shipped by CI).
    let _ = std::fs::create_dir_all(&dist);
}

/// `SCRY_EMBED_WEBUI` truthy? (`1`/`true`/`yes`/`on`, case-insensitive.)
fn embed_requested() -> bool {
    match env::var("SCRY_EMBED_WEBUI") {
        Ok(v) => matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        ),
        Err(_) => false,
    }
}

/// Run `bun install` then `bun run build` in `desktop/`, panicking on failure.
fn build_frontend(desktop: &Path) {
    let bun = env::var("BUN").unwrap_or_else(|_| "bun".to_string());
    println!(
        "cargo:warning=SCRY_EMBED_WEBUI set — building frontend bundle with `{bun}` in {}",
        desktop.display()
    );
    run(&bun, &["install"], desktop);
    run(&bun, &["run", "build"], desktop);
}

fn run(program: &str, args: &[&str], cwd: &Path) {
    let status = Command::new(program)
        .args(args)
        .current_dir(cwd)
        .status()
        .unwrap_or_else(|e| {
            panic!(
                "SCRY_EMBED_WEBUI=1 but `{program} {}` could not start in {}: {e}. \
                 Install bun (or set BUN=/path/to/bun), or unset SCRY_EMBED_WEBUI to \
                 use the empty-bundle fallback.",
                args.join(" "),
                cwd.display()
            )
        });
    if !status.success() {
        panic!(
            "SCRY_EMBED_WEBUI=1 but `{program} {}` failed in {} ({status})",
            args.join(" "),
            cwd.display()
        );
    }
}

/// Emit `cargo:rerun-if-changed` for every file under `dir` (recursively) so a
/// frontend source edit re-triggers the embed. Silent if `dir` is absent.
fn watch_tree(dir: &Path) {
    let mut stack: Vec<PathBuf> = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&d) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else {
                println!("cargo:rerun-if-changed={}", path.display());
            }
        }
    }
}
