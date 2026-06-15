import { Logo } from "./Logo";
import { REPO_URL, VERSION } from "../content";
import styles from "./Footer.module.css";

export function Footer() {
  return (
    <footer class={styles.footer}>
      <div class={`container ${styles.inner}`}>
        <div class={styles.brand}>
          <a href="#top" class={styles.logo} aria-label="scry home">
            <Logo size={26} />
            <span>scry</span>
          </a>
          <p class={styles.tag}>One binary for metrics, logs, traces &amp; profiles.</p>
        </div>

        <nav class={styles.links}>
          <a href="#why">Why</a>
          <a href="#signals">Signals</a>
          <a href="#get-started">Get started</a>
          <a href="#protocols">Protocols</a>
          <a href={REPO_URL} target="_blank" rel="noreferrer">
            GitHub ↗
          </a>
        </nav>
      </div>

      <div class={`container ${styles.base}`}>
        <span>© {new Date().getFullYear()} scry · MIT / Apache-2.0</span>
        <span class={styles.version}>{VERSION}</span>
      </div>
    </footer>
  );
}
