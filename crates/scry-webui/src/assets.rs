//! Embedded SolidJS bundle (the `desktop/` Vite build output).
//!
//! `rust-embed` embeds `desktop/dist` into the binary at **compile time** in
//! release builds (so `scry-webui` ships as a single self-contained file), and
//! reads it from disk at runtime in debug builds (fast frontend iteration —
//! re-run `bun run build` in `desktop/`, no `cargo` rebuild needed).
//!
//! Because the embed happens at compile time, `desktop/dist` must exist when
//! `cargo build -p scry-webui` runs — so `build.rs` creates it (empty) if it's
//! absent, letting the crate compile in a clean checkout (CI, fresh clone).
//! An empty bundle serves the "bundle missing" 500 below; run `bun run build`
//! in `desktop/` first for the real assets.

use axum::http::{header, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use rust_embed::RustEmbed;

#[derive(RustEmbed)]
#[folder = "$CARGO_MANIFEST_DIR/../../desktop/dist"]
struct Assets;

/// Serve an embedded asset by request path, falling back to `index.html` for
/// any path that isn't a real asset (single-page-app client routing). The
/// `/api/*` routes are matched by the API router before this fallback runs, so
/// they never reach here.
pub async fn serve(uri: Uri) -> Response {
    let path = uri.path().trim_start_matches('/');
    let path = if path.is_empty() { "index.html" } else { path };

    if let Some(file) = Assets::get(path) {
        let mime = mime_guess::from_path(path).first_or_octet_stream();
        return (
            [(header::CONTENT_TYPE, mime.as_ref().to_owned())],
            file.data.into_owned(),
        )
            .into_response();
    }

    // SPA fallback: serve index.html so the client router can take over.
    match Assets::get("index.html") {
        Some(index) => (
            [(header::CONTENT_TYPE, "text/html")],
            index.data.into_owned(),
        )
            .into_response(),
        None => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "web UI bundle missing — run `bun run build` in desktop/ before building scry-webui",
        )
            .into_response(),
    }
}
