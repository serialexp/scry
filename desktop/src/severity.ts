//! OTEL severity-number → display label + CSS class, shared by the results
//! table (per-row badge) and the volume histogram (per-band color) so both
//! agree on the six severity buckets and their palette.

export interface SeverityInfo {
  label: string;
  cls: string;
}

/// Map an OTEL severity number (1–24) to its display bucket. The buckets follow
/// the OTEL spec's ranges: TRACE 1–4, DEBUG 5–8, INFO 9–12, WARN 13–16,
/// ERROR 17–20, FATAL 21–24. 0 / unknown → the neutral "—" bucket.
export function severity(sev: number): SeverityInfo {
  if (sev >= 21) return { label: "FATAL", cls: "sev-fatal" };
  if (sev >= 17) return { label: "ERROR", cls: "sev-error" };
  if (sev >= 13) return { label: "WARN", cls: "sev-warn" };
  if (sev >= 9) return { label: "INFO", cls: "sev-info" };
  if (sev >= 5) return { label: "DEBUG", cls: "sev-debug" };
  if (sev >= 1) return { label: "TRACE", cls: "sev-trace" };
  return { label: "—", cls: "sev-none" };
}

/// Concrete band color for a severity class — mirrors the `.log-sev.*` CSS
/// palette in `styles.css`, but as literal strings because uPlot fills/strokes
/// want colors, not CSS classes. Kept here so the table badges and the volume
/// bands stay visually consistent.
export function severityColor(label: string): string {
  switch (label) {
    case "FATAL":
      return "#ff7ad0";
    case "ERROR":
      return "#ff6b6b";
    case "WARN":
      return "#f0b429";
    case "INFO":
      return "#6aa3ff";
    case "DEBUG":
      return "#4bb2a8";
    case "TRACE":
      return "#8a8fa3";
    default:
      return "#5a6072";
  }
}

/// Representative severity number for a class label — the low end of each OTEL
/// range — so the volume bands can be ordered least→most severe deterministically.
export function severityRank(label: string): number {
  switch (label) {
    case "TRACE":
      return 1;
    case "DEBUG":
      return 5;
    case "INFO":
      return 9;
    case "WARN":
      return 13;
    case "ERROR":
      return 17;
    case "FATAL":
      return 21;
    default:
      return 0;
  }
}
