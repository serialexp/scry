//! Runtime environment detection.
//!
//! The same SolidJS app runs in two shells: the Tauri desktop window and a
//! plain browser tab served by `scry-webui`. Tauri injects
//! `window.__TAURI_INTERNALS__`; its absence means we're in a browser.

/** True when running inside the Tauri desktop shell. */
export function isTauri(): boolean {
  return typeof window !== "undefined" && "__TAURI_INTERNALS__" in window;
}
