import styles from "./Background.module.css";

/**
 * Fixed, non-interactive backdrop: two slow-drifting accent auroras over a
 * faint grid, plus a vignette. Pure CSS — cheap and reduced-motion aware.
 */
export function Background() {
  return (
    <div class={styles.bg} aria-hidden="true">
      <div class={styles.grid} />
      <div class={`${styles.aurora} ${styles.auroraTeal}`} />
      <div class={`${styles.aurora} ${styles.auroraViolet}`} />
      <div class={styles.vignette} />
    </div>
  );
}
