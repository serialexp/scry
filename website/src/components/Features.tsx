import { For } from "solid-js";

import { reveal } from "../lib/reveal";
import { SIGNALS, type Feature } from "../content";
import styles from "./Features.module.css";

export function Features() {
  return (
    <section class="section" id="signals">
      <div class="container">
        <div class={styles.head}>
          <span class="eyebrow" ref={(el) => reveal(el, 0)}>
            Four signals, one engine
          </span>
          <h2 class="section-title" ref={(el) => reveal(el, 60)}>
            Every telemetry type, <span class="grad-text">first-class</span>.
          </h2>
          <p class="section-lead" ref={(el) => reveal(el, 120)}>
            Each signal gets the storage shape it deserves — postings, blooms, predicate
            pushdown — while sharing one block format, one catalog, and one query engine.
          </p>
        </div>

        <div class={styles.grid}>
          <For each={SIGNALS}>
            {(f, i) => <Card f={f} delay={i() * 70} accent />}
          </For>
        </div>
      </div>
    </section>
  );
}

function Card(props: { f: Feature; delay: number; accent?: boolean }) {
  return (
    <article class={`${styles.card} ${props.accent ? styles.cardAccent : ""}`} ref={(el) => reveal(el, props.delay)}>
      <div class={styles.icon}>
        <svg width="22" height="22" viewBox="0 0 24 24" fill="none" aria-hidden="true">
          <path
            d={props.f.icon}
            stroke="currentColor"
            stroke-width="1.8"
            stroke-linecap="round"
            stroke-linejoin="round"
          />
        </svg>
      </div>
      <span class={styles.tag}>{props.f.tag}</span>
      <h4 class={styles.title}>{props.f.title}</h4>
      <p class={styles.body}>{props.f.body}</p>
    </article>
  );
}
