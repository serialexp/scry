//! Shared value/time formatting helpers for the result views.
//!
//! Lifted out of `ResultsTable.tsx` so the logs reader, the traces waterfall,
//! and the generic table all coerce Arrow cells the same way. Pure and
//! DOM-free.

/** Lowercase hex of a byte buffer (ids, opaque binary cells). */
export function toHex(bytes: Uint8Array): string {
  let s = "";
  for (const b of bytes) s += b.toString(16).padStart(2, "0");
  return s;
}

/** Render an arbitrary Arrow cell to a display string (generic table). */
export function fmtCell(v: unknown): string {
  if (v === null || v === undefined) return "";
  if (v instanceof Uint8Array) return toHex(v);
  if (typeof v === "bigint") return v.toString();
  if (typeof v === "object") {
    try {
      return JSON.stringify(v, (_k, val) => (typeof val === "bigint" ? val.toString() : val));
    } catch {
      return String(v);
    }
  }
  return String(v);
}

/** Render a single attribute/label value (scalar-leaning; objects via fmtCell). */
export function attrVal(v: unknown): string {
  if (v === null || v === undefined) return "";
  if (typeof v === "bigint") return v.toString();
  if (typeof v === "object") return fmtCell(v);
  return String(v);
}

/** Coerce an Arrow Map cell (object / Map / array-of-pairs) into entries. */
export function attrEntries(v: unknown): [string, string][] {
  if (v === null || v === undefined) return [];
  if (v instanceof Map) {
    return [...v.entries()].map(([k, val]) => [String(k), attrVal(val)]);
  }
  if (Array.isArray(v)) {
    return v.map((e) =>
      Array.isArray(e)
        ? [String(e[0]), attrVal(e[1])]
        : [String((e as { key: unknown }).key), attrVal((e as { value: unknown }).value)],
    );
  }
  if (typeof v === "object") {
    return Object.entries(v as Record<string, unknown>).map(([k, val]) => [k, attrVal(val)]);
  }
  return [];
}

const pad = (n: number, w = 2): string => String(n).padStart(w, "0");

/** Unix-nanos → a compact local time plus a full ISO title. */
export function fmtTs(ns: bigint): { short: string; full: string } {
  const d = new Date(Number(ns / 1_000_000n));
  if (Number.isNaN(d.getTime())) return { short: "—", full: "" };
  const short = `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}.${pad(d.getMilliseconds(), 3)}`;
  return { short, full: d.toISOString() };
}

/** Human-readable duration from a nanosecond span (bigint-safe). */
export function fmtDuration(ns: bigint): string {
  if (ns < 0n) ns = 0n;
  const nsNum = Number(ns);
  if (nsNum < 1_000) return `${nsNum}ns`;
  if (nsNum < 1_000_000) return `${(nsNum / 1_000).toFixed(1)}µs`;
  if (nsNum < 1_000_000_000) return `${(nsNum / 1_000_000).toFixed(2)}ms`;
  return `${(nsNum / 1_000_000_000).toFixed(2)}s`;
}
