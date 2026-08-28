//! Desktop transport: raw TCP sockets via the Rust `run_query` / `tail_start`
//! commands.
//!
//! This module statically imports `@tauri-apps/api`, so it must only ever be
//! loaded inside the Tauri shell. `store.ts` reaches it via a dynamic
//! `import()` gated on `isTauri()`, keeping it out of the browser bundle.

import { Channel, invoke } from "@tauri-apps/api/core";
import { FrameStream } from "./framing";
import type { FrameHandler, Transport } from "./transport";

/** Transport backed by the Rust socket commands (native TCP). */
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

  async tail(
    addr: string,
    request: Uint8Array,
    onFrame: FrameHandler,
    signal: AbortSignal,
  ): Promise<void> {
    const frames = new FrameStream();
    let finish!: () => void;
    let fail!: (e: unknown) => void;
    const ended = new Promise<void>((resolve, reject) => {
      finish = resolve;
      fail = reject;
    });

    // Each message is one socket read, not one frame; `FrameStream` finds the
    // boundaries. An empty buffer is the Rust side's end-of-stream marker.
    const channel = new Channel<ArrayBuffer>((message) => {
      const chunk = new Uint8Array(message);
      if (chunk.length === 0) {
        finish();
        return;
      }
      try {
        for (const body of frames.push(chunk)) onFrame(body);
      } catch (e) {
        fail(e);
      }
    });

    const id = await invoke<number>("tail_start", {
      addr,
      request: Array.from(request),
      onFrame: channel,
    });

    // The subscription may have been aborted while `tail_start` was in flight.
    if (signal.aborted) {
      await invoke("tail_stop", { id });
      return;
    }
    const stop = () => {
      void invoke("tail_stop", { id });
      finish();
    };
    signal.addEventListener("abort", stop, { once: true });
    try {
      await ended;
    } finally {
      signal.removeEventListener("abort", stop);
    }
  }
}
