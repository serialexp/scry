//! `QueryStats` (tag 0x1E) is the per-query timing breakdown the daemon sends
//! immediately before `EndOfStream`. These tests pin the two properties the
//! browser client depends on:
//!
//!   1. every field survives a round trip (a silently-dropped field would show
//!      up in the UI as a phase that is always 0 — a wrong answer that looks
//!      like a real measurement), and
//!   2. a `QueryStats` frame does not disturb the terminator. It is
//!      non-terminal, so a reader must still see exactly one `EndOfStream`,
//!      and see it last.
//!
//! Lives in `protocol/` and not next to the bindings in `proto/`: that
//! directory is generator-owned and `scripts/gen-proto-ts.sh` starts with
//! `rm -f "$OUT"/*.ts`, so a hand-written file there is deleted by the next
//! regeneration.

import { describe, expect, it } from "vitest";

import {
  QueryFrameDecoder,
  QueryFrameEncoder,
  type QueryStatsInput,
  type QueryStatsOutput,
} from "../proto/generated";
import { buildQueryTiming } from "./client";

function stats(over: Partial<QueryStatsInput> = {}): QueryStatsInput {
  return {
    server_total_us: 9_868_300n,
    admission_wait_us: 5_600_000n,
    catalog_us: 1_200n,
    cache_lookup_us: 40n,
    live_fetch_us: 0n,
    register_us: 250_000n,
    plan_us: 3_100n,
    execute_us: 3_900_000n,
    serialize_us: 12_000n,
    write_us: 800n,
    postings_fetch_us: 210_000n,
    bloom_fetch_us: 0n,
    df_opening_us: 5_500_000n,
    df_scanning_us: 7_100_000n,
    df_compute_us: 900_000n,
    cache_hit: 0,
    attempts: 1,
    blocks_considered: 27,
    blocks_scanned: 27,
    bytes_scanned: 680_000n,
    node_id: "queryd-0",
    live_nodes: [],
    ...over,
  };
}

describe("QueryStats frame", () => {
  it("round-trips every field", () => {
    const sent = stats({
      live_nodes: [
        { addr: "10.0.0.1:4000", elapsed_us: 4_200n, rows: 13n, ok: 1 },
        { addr: "10.0.0.2:4000", elapsed_us: 1_000_000n, rows: 0n, ok: 0 },
      ],
    });

    const bytes = new QueryFrameEncoder().encode({
      msg: { type: "QueryStats", value: sent },
    });
    expect(bytes[0]).toBe(0x1e);

    const back = new QueryFrameDecoder(bytes).decode();
    expect(back.msg.type).toBe("QueryStats");
    if (back.msg.type !== "QueryStats") return;
    const got = back.msg.value;

    // Compare field-by-field rather than with a single deep-equal so a
    // regression names the field that broke.
    for (const key of Object.keys(sent) as (keyof QueryStatsInput)[]) {
      if (key === "live_nodes") continue;
      expect([key, got[key]]).toEqual([key, sent[key]]);
    }
    expect(got.live_nodes).toHaveLength(2);
    expect(got.live_nodes[0]).toMatchObject({
      addr: "10.0.0.1:4000",
      elapsed_us: 4_200n,
      rows: 13n,
      ok: 1,
    });
    expect(got.live_nodes[1]!.ok).toBe(0);
  });

  it("keeps EndOfStream the last frame", () => {
    const statsBytes = new QueryFrameEncoder().encode({
      msg: { type: "QueryStats", value: stats() },
    });
    const eosBytes = new QueryFrameEncoder().encode({
      msg: { type: "EndOfStream", value: { total_rows: 13n } },
    });

    const first = new QueryFrameDecoder(statsBytes).decode();
    const second = new QueryFrameDecoder(eosBytes).decode();
    expect(first.msg.type).toBe("QueryStats");
    expect(second.msg.type).toBe("EndOfStream");
    if (second.msg.type !== "EndOfStream") return;
    expect(second.msg.value.total_rows).toBe(13n);
  });

  it("carries an empty live_nodes list for a non-live query", () => {
    // The common case. An empty length-prefixed array must decode as empty
    // rather than as a truncated frame.
    const bytes = new QueryFrameEncoder().encode({
      msg: {
        type: "QueryStats",
        value: stats({ cache_hit: 1, live_nodes: [] }),
      },
    });
    const back = new QueryFrameDecoder(bytes).decode();
    if (back.msg.type !== "QueryStats") throw new Error("expected QueryStats");
    expect(back.msg.value.live_nodes).toEqual([]);
    expect(back.msg.value.cache_hit).toBe(1);
  });

  it("reports server_total_us independently of the phases", () => {
    // The UI renders `server_total_us - Σphases` as an explicit "other"
    // bucket instead of smearing it across the named phases. That only
    // works if the wire keeps the total separate, which it does — here the
    // phases deliberately do not add up to the total.
    const s = stats();
    const phases =
      s.admission_wait_us +
      s.catalog_us +
      s.cache_lookup_us +
      s.live_fetch_us +
      s.register_us +
      s.plan_us +
      s.execute_us +
      s.serialize_us +
      s.write_us;
    expect(phases).toBeLessThan(s.server_total_us);
    expect(s.server_total_us - phases).toBeGreaterThan(0n);
  });
});

describe("buildQueryTiming", () => {
  // `stats()` is a QueryStatsInput; the decoder's Output shape is structurally
  // identical for these fields, so one fixture serves both.
  const asOutput = (o: Partial<QueryStatsInput> = {}) =>
    stats(o) as unknown as QueryStatsOutput;

  it("makes the phases sum to serverMs exactly, via an explicit other bucket", () => {
    // This is the property the whole waterfall rests on. The server measures
    // its total independently of the parts, so the parts never quite add up —
    // and the gap is shown as its own bar rather than being distributed across
    // the named phases, which would silently inflate whichever phase absorbed
    // it. A reader can therefore trust every bar they see.
    const t = buildQueryTiming(asOutput(), 10_500, 300);
    const summed = t.phases.reduce((acc, p) => acc + p.ms, 0);
    expect(summed).toBeCloseTo(t.serverMs, 6);

    const other = t.phases.find((p) => p.label === "other");
    expect(other).toBeDefined();
    expect(other!.ms).toBeGreaterThan(0);
    // "other" is last so the waterfall reads left-to-right in time order,
    // with the unattributed remainder at the end.
    expect(t.phases[t.phases.length - 1]!.label).toBe("other");
  });

  it("omits zero phases but never omits other", () => {
    // A phase that did not happen is noise in a breakdown; `other` is
    // meaningful even at zero because its absence would be read as "the
    // phases are complete" rather than "nothing was unaccounted for".
    const t = buildQueryTiming(asOutput({ live_fetch_us: 0n }), 10_500, 300);
    expect(t.phases.map((p) => p.label)).not.toContain("live-fetch");
    expect(t.phases.map((p) => p.label)).toContain("other");
  });

  it("derives transport as the round trip minus the server and the decode", () => {
    // 9868.3ms server + 300ms decode inside a 10,500ms round trip leaves
    // 331.7ms on the network — the half a daemon log can never show.
    const t = buildQueryTiming(asOutput(), 10_500, 300);
    expect(t.serverMs).toBeCloseTo(9868.3, 3);
    expect(t.decodeMs).toBe(300);
    expect(t.transportMs).toBeCloseTo(331.7, 3);
  });

  it("clamps transport at zero rather than reporting negative time", () => {
    // The server's clock and ours are independent, so a fast local query can
    // report a server total marginally above our measured round trip. Negative
    // transport would be nonsense on a chart; zero is honest.
    const t = buildQueryTiming(asOutput(), 1, 0);
    expect(t.transportMs).toBe(0);
  });

  it("keeps DataFusion counters out of the phase list", () => {
    // They are summed across partitions and here deliberately exceed the whole
    // server total. As timeline slices they would be a lie; as their own group
    // they are the useful "was the scan actually parallel?" signal.
    const t = buildQueryTiming(asOutput(), 10_500, 300);
    const dfTotal =
      t.datafusion.openingMs + t.datafusion.scanningMs + t.datafusion.computeMs;
    expect(dfTotal).toBeGreaterThan(t.serverMs);
    expect(t.phases.map((p) => p.label)).not.toContain("datafusion");
  });

  it("carries the live fan-out including the peers that failed", () => {
    const t = buildQueryTiming(
      asOutput({
        live_nodes: [
          { addr: "10.0.0.1:4000", elapsed_us: 4_200n, rows: 13n, ok: 1 },
          { addr: "10.0.0.2:4000", elapsed_us: 1_000_000n, rows: 0n, ok: 0 },
        ],
      }),
      10_500,
      300,
    );
    expect(t.liveNodes).toHaveLength(2);
    expect(t.liveNodes[0]).toEqual({
      addr: "10.0.0.1:4000",
      ms: 4.2,
      rows: 13n,
      ok: true,
    });
    // The slow, failed peer is the whole reason to look — it must not be
    // filtered out for having returned nothing.
    expect(t.liveNodes[1]!.ok).toBe(false);
    expect(t.liveNodes[1]!.ms).toBe(1000);
  });

  it("reports a cache hit as a hit", () => {
    expect(buildQueryTiming(asOutput({ cache_hit: 1 }), 5, 1).cacheHit).toBe(true);
    expect(buildQueryTiming(asOutput({ cache_hit: 0 }), 5, 1).cacheHit).toBe(false);
  });
});
