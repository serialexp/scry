import { createSignal, For } from "solid-js";

import { reveal } from "../lib/reveal";
import { GET_STARTED } from "../content";
import styles from "./GetStarted.module.css";

export function GetStarted() {
  const [active, setActive] = createSignal(0);
  const tab = () => GET_STARTED[active()];

  return (
    <section class="section" id="get-started">
      <div class="container">
        <div class={styles.head}>
          <span class="eyebrow" ref={(el) => reveal(el, 0)}>
            Get started
          </span>
          <h2 class="section-title" ref={(el) => reveal(el, 60)}>
            Up and running in <span class="grad-text">a few commands</span>.
          </h2>
          <p class="section-lead" ref={(el) => reveal(el, 120)}>
            One binary, run as ingest and query roles over your bucket — with a small Valkey to
            coordinate. Pick your platform.
          </p>
        </div>

        <div class={styles.panel} ref={(el) => reveal(el, 0)}>
          <div class={styles.tabs} role="tablist" aria-label="Deployment target">
            <For each={GET_STARTED}>
              {(t, i) => (
                <button
                  type="button"
                  role="tab"
                  aria-selected={active() === i()}
                  class={`${styles.tab} ${active() === i() ? styles.tabActive : ""}`}
                  onClick={() => setActive(i())}
                >
                  {t.label}
                </button>
              )}
            </For>
          </div>

          <p class={styles.note}>{tab().note}</p>
          <pre class={styles.code}>
            <code>{tab().code}</code>
          </pre>
        </div>
      </div>
    </section>
  );
}
