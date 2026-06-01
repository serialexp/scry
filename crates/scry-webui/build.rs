//! Ensure the rust-embed asset folder exists at compile time.
//!
//! `assets.rs` embeds `desktop/dist` via `#[derive(RustEmbed)]`, whose derive
//! macro errors at compile time if the folder is missing. `desktop/dist` is a
//! gitignored Vite build artifact, so in a clean checkout — CI's
//! `cargo build --workspace`, a fresh clone, anyone who hasn't run
//! `bun run build` — it doesn't exist, and the crate fails to compile even
//! when nobody cares about the web UI.
//!
//! Creating the directory (empty) lets the build proceed. An empty bundle is
//! harmless: every asset lookup misses and `assets::serve` returns the runtime
//! "bundle missing — run `bun run build`" 500. When the real bundle is present
//! (the documented flow, `scripts/smoke-webui.sh`, the home deploy) this is a
//! no-op and the actual assets are embedded.

use std::env;
use std::path::Path;

fn main() {
    let manifest = env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR is set by cargo");
    let dist = Path::new(&manifest).join("../../desktop/dist");
    // Best-effort: a no-op when the real bundle already exists.
    let _ = std::fs::create_dir_all(&dist);
    println!("cargo:rerun-if-changed=build.rs");
}
