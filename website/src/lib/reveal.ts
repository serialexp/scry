import { getOwner, onCleanup } from "solid-js";

// A single shared IntersectionObserver drives every reveal-on-scroll element.
// Elements get the `reveal` base class immediately (so they start hidden) and
// flip to `is-visible` once they enter the viewport — a one-shot transition.
let observer: IntersectionObserver | undefined;

function getObserver(): IntersectionObserver | undefined {
  if (typeof IntersectionObserver === "undefined") return undefined;
  if (!observer) {
    observer = new IntersectionObserver(
      (entries, obs) => {
        for (const entry of entries) {
          if (entry.isIntersecting) {
            entry.target.classList.add("is-visible");
            obs.unobserve(entry.target);
          }
        }
      },
      { rootMargin: "0px 0px -8% 0px", threshold: 0.12 },
    );
  }
  return observer;
}

/**
 * Fade + lift an element in when it scrolls into view. Use as a ref callback:
 *
 *   <div ref={(el) => reveal(el, 80)}>…</div>
 *
 * `delay` is an optional stagger in milliseconds.
 */
export function reveal(el: HTMLElement, delay?: number): void {
  el.classList.add("reveal");
  if (delay) el.style.transitionDelay = `${delay}ms`;

  const obs = getObserver();
  if (!obs) {
    // No IntersectionObserver (or reduced-motion CSS already neutralized it):
    // just show it.
    el.classList.add("is-visible");
    return;
  }
  obs.observe(el);
  // Ref callbacks run within the component's reactive owner, so onCleanup is
  // valid — but guard anyway in case reveal() is ever called bare.
  if (getOwner()) onCleanup(() => obs.unobserve(el));
}
