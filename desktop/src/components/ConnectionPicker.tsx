//! The connection picker in the shell topbar (Phase 1): which daemon answers
//! queries. Moved out of the old vertical query form so it's shared chrome
//! across every view.
//!
//! - Browser shell: a `<select>` of the scry-webui `--queryd` allowlist; the
//!   user picks a target *id* (never a raw address — SSRF-safe).
//! - Tauri shell: a raw `host:port` the native transport dials directly.

import { For, Show, type Component } from "solid-js";

import { isTauri } from "../env";
import { state, setField, targets } from "../store";

const ConnectionPicker: Component = () => {
  return (
    <>
      <Show when={isTauri()}>
        <input
          class="conn-addr"
          type="text"
          spellcheck={false}
          value={state.addr}
          onInput={(e) => setField("addr", e.currentTarget.value)}
          placeholder="127.0.0.1:4100"
          title="scry query daemon address"
        />
      </Show>
      <Show when={!isTauri()}>
        <select
          class="conn-target"
          value={state.target}
          title="Query target"
          onChange={(e) => setField("target", e.currentTarget.value)}
        >
          <For each={targets()}>{(t) => <option value={t.id}>{t.label}</option>}</For>
        </select>
      </Show>
    </>
  );
};

export default ConnectionPicker;
