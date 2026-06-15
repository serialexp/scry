import { For } from "solid-js";

import { reveal } from "../lib/reveal";
import { PROTOCOLS_IN, PROTOCOLS_OUT, type Protocol } from "../content";
import styles from "./Protocols.module.css";

export function Protocols() {
  return (
    <section class="section" id="protocols">
      <div class="container">
        <div class={styles.head}>
          <span class="eyebrow" ref={(el) => reveal(el, 0)}>
            Drop-in compatible
          </span>
          <h2 class="section-title" ref={(el) => reveal(el, 60)}>
            Speaks the protocols <span class="grad-text">you already emit</span>.
          </h2>
          <p class="section-lead" ref={(el) => reveal(el, 120)}>
            The gateway terminates the formats your apps already speak and tees every record,
            best-effort, to each configured sink — keep an existing backend while you migrate.
          </p>
        </div>

        <div class={styles.cols}>
          <div class={styles.col} ref={(el) => reveal(el, 0)}>
            <span class={styles.colLabel}>
              <Dot in /> Ingest
            </span>
            <For each={PROTOCOLS_IN}>{(p) => <Row p={p} />}</For>
          </div>

          <div class={styles.col} ref={(el) => reveal(el, 120)}>
            <span class={styles.colLabel}>
              <Dot /> Fan-out sinks
            </span>
            <For each={PROTOCOLS_OUT}>{(p) => <Row p={p} />}</For>
          </div>
        </div>
      </div>
    </section>
  );
}

function Row(props: { p: Protocol }) {
  return (
    <div class={styles.row}>
      <span class={styles.name}>{props.p.name}</span>
      <span class={styles.detail}>{props.p.detail}</span>
    </div>
  );
}

function Dot(props: { in?: boolean }) {
  return <span class={`${styles.dot} ${props.in ? styles.dotIn : styles.dotOut}`} aria-hidden="true" />;
}
