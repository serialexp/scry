import { reveal } from "../lib/reveal";
import { REPO_URL } from "../content";
import styles from "./CTA.module.css";

export function CTA() {
  return (
    <section class="section">
      <div class="container">
        <div class={styles.card} ref={(el) => reveal(el, 0)}>
          <div class={styles.glow} aria-hidden="true" />
          <h2 class={styles.title}>
            Run the whole stack <span class="grad-text">from one binary.</span>
          </h2>
          <p class={styles.lead}>
            scry is open source. Point it at a bucket and start sending metrics, logs, traces and
            profiles today.
          </p>
          <div class={styles.actions}>
            <a class="btn btn-primary" href={REPO_URL} target="_blank" rel="noreferrer">
              Star on GitHub
              <span aria-hidden="true">→</span>
            </a>
            <a class="btn btn-ghost" href={`${REPO_URL}#readme`} target="_blank" rel="noreferrer">
              Read the docs
            </a>
          </div>
        </div>
      </div>
    </section>
  );
}
