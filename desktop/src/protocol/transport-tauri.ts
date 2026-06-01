//! Desktop transport: the Rust `run_query` command over a native TCP socket.
//!
//! This module statically imports `@tauri-apps/api`, so it must only ever be
//! loaded inside the Tauri shell. `store.ts` reaches it via a dynamic
//! `import()` gated on `isTauri()`, keeping it out of the browser bundle.

import { invoke } from "@tauri-apps/api/core";
import type { Transport } from "./transport";

/** Transport backed by the Rust `run_query` command (native TCP socket). */
export class TauriTransport implements Transport {
  async query(addr: string, request: Uint8Array): Promise<Uint8Array> {
    // The request frame is small (tens of bytes to a few KB), so passing
    // it as a JSON number array is fine. The *response* comes back as a
    // raw ArrayBuffer (the Rust command returns `tauri::ipc::Response`),
    // avoiding a number-array round-trip for multi-MB Arrow payloads.
    const res = await invoke<ArrayBuffer>("run_query", {
      addr,
      request: Array.from(request),
    });
    return new Uint8Array(res);
  }
}
