import { reveal } from "../lib/reveal";
import { REPO_URL, VERSION } from "../content";
import styles from "./Hero.module.css";

export function Hero() {
  return (
    <section class={styles.hero} id="top">
      <div class={`container ${styles.inner}`}>
        <div class={styles.copy}>
          <span class={`eyebrow ${styles.badge}`} ref={(el) => reveal(el, 0)}>
            {VERSION} · single-binary observability
          </span>

          <h1 class={styles.title} ref={(el) => reveal(el, 80)}>
            One binary.
            <br />
            <span class="grad-text">Every signal.</span>
          </h1>

          <p class={styles.lead} ref={(el) => reveal(el, 160)}>
            scry replaces Loki, Tempo, Mimir and Pyroscope with a single binary — deployed as
            ingest and query roles over one S3-compatible bucket, coordinated by a small Valkey.
            All four signals, one tenant, no hash ring, no four-database sprawl.
          </p>

          <div class={styles.actions} ref={(el) => reveal(el, 240)}>
            <a class="btn btn-primary" href={REPO_URL} target="_blank" rel="noreferrer">
              Get started
              <span aria-hidden="true">→</span>
            </a>
            <a class="btn btn-ghost" href="#get-started">
              Quick start
            </a>
          </div>
        </div>

        <div class={styles.visual} ref={(el) => reveal(el, 200)}>
          <Terminal />
        </div>
      </div>
    </section>
  );
}

function Terminal() {
  return (
    <div class={styles.term}>
      <div class={styles.termBar}>
        <span class={styles.dot} style={{ background: "#ff5f57" }} />
        <span class={styles.dot} style={{ background: "#febc2e" }} />
        <span class={styles.dot} style={{ background: "#28c840" }} />
        <span class={styles.termTitle}>one bucket, every signal</span>
      </div>
      <pre class={styles.termBody}>
        <code>
          <span class={styles.cmt}># point it at any S3-compatible bucket</span>
          {"\n"}
          <span class={styles.prompt}>$</span> scry ingest --listen :4000 \{"\n"}
          {"    "}--bucket s3://obs --catalog ./scry.db
          {"\n\n"}
          <span class={styles.ok}>✓</span> metrics{"   "}
          <span class={styles.dim}>postings index · remote-write</span>
          {"\n"}
          <span class={styles.ok}>✓</span> logs{"      "}
          <span class={styles.dim}>trigram bloom · full-text grep</span>
          {"\n"}
          <span class={styles.ok}>✓</span> traces{"    "}
          <span class={styles.dim}>OTLP · predicate pushdown</span>
          {"\n"}
          <span class={styles.ok}>✓</span> profiles{"  "}
          <span class={styles.dim}>pprof · time + label lookup</span>
          {"\n\n"}
          <span class={styles.cmt}># queries read straight from the same bucket</span>
          {"\n"}
          <span class={styles.prompt}>$</span> scry get --bucket s3://obs \{"\n"}
          {"    "}--signal logs --grep "timeout"
          {"\n"}
          <span class={styles.dim}>scan: 3 blocks skipped via bloom · 1 scanned</span>
        </code>
      </pre>
    </div>
  );
}
