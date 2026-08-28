//! The shared log-row shape: how the two sources decode into it, and the rules
//! that let a live stream and a finished query share one list.
//!
//! The merge/cap/seam trio is where a live pane goes wrong in ways nobody
//! notices — a duplicated line, an unbounded buffer, a pane that stops growing
//! at the cap — so each one is pinned here.

import { describe, expect, it } from "vitest";

import {
  appendCapped,
  isAfterSeam,
  mergeLogRows,
  newestTs,
  stripAnsi,
  tailRecordToLogRow,
  type LogRow,
} from "./logs";
import type { TailRecord } from "./protocol/tail";

function row(ts: number, body = `line-${ts}`): LogRow {
  return { ts: BigInt(ts), sev: 9, body, labels: [], attrs: [] };
}

describe("stripAnsi", () => {
  it("removes colour sequences but keeps the text", () => {
    expect(stripAnsi("\u001b[31mred\u001b[0m")).toBe("red");
  });

  it("keeps tab and newline, drops other control chars", () => {
    expect(stripAnsi("a\tb\nc\u0000d\u0007e")).toBe("a\tb\ncde");
  });
});

describe("tailRecordToLogRow", () => {
  it("carries the record across unchanged, minus escapes", () => {
    const rec: TailRecord = {
      tsUnixNano: 42n,
      severity: 17,
      body: "\u001b[1mboom\u001b[0m",
      labels: [["service", "api"]],
      attrs: [["stream", "stderr"]],
    };
    expect(tailRecordToLogRow(rec)).toEqual({
      ts: 42n,
      sev: 17,
      body: "boom",
      labels: [["service", "api"]],
      attrs: [["stream", "stderr"]],
    });
  });
});

describe("newestTs", () => {
  it("is null for no rows — an empty history has no seam", () => {
    expect(newestTs([])).toBeNull();
  });

  it("finds the max regardless of order", () => {
    expect(newestTs([row(5), row(9), row(2)])).toBe(9n);
  });
});

describe("mergeLogRows", () => {
  it("appends live after history, in arrival order", () => {
    const out = mergeLogRows([row(1), row(2)], [row(3), row(4)], 10);
    expect(out.map((r) => r.ts)).toEqual([1n, 2n, 3n, 4n]);
  });

  /// The point of the cap direction: a running tail must scroll, not freeze.
  it("keeps the NEWEST rows when the pair overruns the cap", () => {
    const history = [row(1), row(2), row(3)];
    const live = [row(4), row(5)];
    expect(mergeLogRows(history, live, 3).map((r) => r.ts)).toEqual([3n, 4n, 5n]);
  });

  it("truncates history from the front when there is no live stream", () => {
    // Nothing is arriving, so the first N scanned rows are what was asked for.
    expect(mergeLogRows([row(1), row(2), row(3)], [], 2).map((r) => r.ts)).toEqual([
      1n,
      2n,
    ]);
  });
});

describe("appendCapped", () => {
  it("returns the same buffer when nothing arrived", () => {
    const buf = [row(1)];
    expect(appendCapped(buf, [], 10)).toBe(buf);
  });

  it("drops the oldest past the cap", () => {
    const out = appendCapped([row(1), row(2)], [row(3), row(4)], 3);
    expect(out.map((r) => r.ts)).toEqual([2n, 3n, 4n]);
  });

  it("keeps only the newest when one batch alone exceeds the cap", () => {
    const out = appendCapped([], [row(1), row(2), row(3), row(4)], 2);
    expect(out.map((r) => r.ts)).toEqual([3n, 4n]);
  });
});

describe("isAfterSeam", () => {
  it("admits everything when the history came back empty", () => {
    expect(isAfterSeam(row(1), null)).toBe(true);
  });

  it("drops rows at or below the seam", () => {
    expect(isAfterSeam(row(5), 5n)).toBe(false);
    expect(isAfterSeam(row(4), 5n)).toBe(false);
  });

  it("admits rows strictly after the seam", () => {
    expect(isAfterSeam(row(6), 5n)).toBe(true);
  });
});
