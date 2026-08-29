//! Live-tail client: the pipelined handshake it produces, the records it
//! decodes, and how it surfaces a server refusal.
//!
//! The encode/decode round trip here is the only place the ingest-schema
//! bindings are exercised in-process; crossing to the real Rust server is the
//! job of `scripts/smoke-webui-tail.sh`.

import { describe, expect, it } from "vitest";

import { FrameDecoder, FrameEncoder, type FrameInput } from "../proto/generated-ingest";
import { deframe, frame } from "./framing";
import {
  PROTOCOL_VERSION_V0,
  SIGNAL_BIT_LOGS,
  SIGNAL_BIT_METRICS,
  Signal,
  TailErrCode,
} from "./constants";
import {
  TailError,
  buildSubscribeRequest,
  equalityMatcher,
  runTail,
  type TailRecord,
  type TailSample,
} from "./tail";
import type { FrameHandler, Transport } from "./transport";

function encode(type: string, value: unknown): Uint8Array {
  return frame(new FrameEncoder().encode({ msg: { type, value } } as unknown as FrameInput));
}

const HELLO_ACK = encode("HelloAck", {
  protocol_version: PROTOCOL_VERSION_V0,
  writer_id: "writer-1",
  session_id: 1n,
  capabilities: 0,
  suggested_batch_bytes: 1024,
  max_batch_bytes: 4096,
  max_inflight_batches: 8,
});

function record(body: string, ts: bigint, severity = 9): Uint8Array {
  return encode("TailRecord", {
    signal: Signal.Logs,
    ts_unix_nano: ts,
    severity,
    labels: [{ key: "service", value: "api" }],
    body,
    attributes: [{ key: "stream", value: "stdout" }],
  });
}

/** Transport that replays a canned frame sequence, capturing the request. */
class ReplayTransport implements Transport {
  request: Uint8Array | null = null;

  constructor(private readonly frames: Uint8Array[]) {}

  async query(): Promise<Uint8Array> {
    throw new Error("this stub only tails");
  }

  async tail(
    _addr: string,
    request: Uint8Array,
    onFrame: FrameHandler,
    _signal: AbortSignal,
  ): Promise<void> {
    this.request = request;
    for (const f of this.frames) {
      for (const body of deframe(f)) onFrame(body);
    }
  }
}

describe("buildSubscribeRequest", () => {
  it("pipelines Hello and Subscribe into one body", () => {
    const bytes = buildSubscribeRequest({ matchers: ['service="api"'] }, "9.9.9");
    const frames = deframe(bytes);
    expect(frames).toHaveLength(2);

    const hello = new FrameDecoder(frames[0]!).decode() as any;
    expect(hello.msg.type).toBe("Hello");
    expect(hello.msg.value.protocol_version).toBe(PROTOCOL_VERSION_V0);
    expect(hello.msg.value.agent_version).toBe("9.9.9");
    expect(hello.msg.value.signals).toBe(SIGNAL_BIT_LOGS);
    expect(hello.msg.value.agent_id).toHaveLength(16);

    const sub = new FrameDecoder(frames[1]!).decode() as any;
    expect(sub.msg.type).toBe("Subscribe");
    expect(sub.msg.value.signal).toBe(Signal.Logs);
    expect(sub.msg.value.matchers).toEqual([{ spec: 'service="api"' }]);
  });

  it("carries no matchers when none are set (tail everything)", () => {
    const frames = deframe(buildSubscribeRequest({ matchers: [] }, "1.0.0"));
    const sub = new FrameDecoder(frames[1]!).decode() as any;
    expect(sub.msg.value.matchers).toEqual([]);
  });

  it("announces the metrics bit and signal when tailing metrics (D-065)", () => {
    const frames = deframe(
      buildSubscribeRequest({ matchers: [], signal: Signal.Metrics }, "1.0.0"),
    );
    const hello = new FrameDecoder(frames[0]!).decode() as any;
    const sub = new FrameDecoder(frames[1]!).decode() as any;
    // The handshake gates which signals the connection may carry, so the
    // Hello bit and the Subscribe signal have to agree.
    expect(hello.msg.value.signals).toBe(SIGNAL_BIT_METRICS);
    expect(sub.msg.value.signal).toBe(Signal.Metrics);
  });
});

describe("equalityMatcher", () => {
  it("quotes the value so separators inside it are not syntax", () => {
    expect(equalityMatcher("pod", "web-1")).toBe('pod="web-1"');
    expect(equalityMatcher("path", "a,b=c")).toBe('path="a,b=c"');
  });

  it("escapes quotes and backslashes", () => {
    expect(equalityMatcher("msg", 'say "hi"')).toBe('msg="say \\"hi\\""');
    expect(equalityMatcher("win", "C:\\tmp")).toBe('win="C:\\\\tmp"');
  });
});

describe("runTail", () => {
  it("decodes pushed records after the handshake", async () => {
    const transport = new ReplayTransport([HELLO_ACK, record("one", 5n), record("two", 6n)]);
    const seen: TailRecord[] = [];
    let subscribed = 0;

    await runTail(
      transport,
      "target",
      { matchers: ['service="api"'] },
      "1.2.3",
      { onRecord: (r) => seen.push(r), onSubscribed: () => subscribed++ },
      new AbortController().signal,
    );

    expect(subscribed).toBe(1);
    expect(seen.map((r) => r.body)).toEqual(["one", "two"]);
    expect(seen[0]).toMatchObject({
      tsUnixNano: 5n,
      severity: 9,
      labels: [["service", "api"]],
      attrs: [["stream", "stdout"]],
    });
    expect(transport.request).not.toBeNull();
  });

  it("decodes pushed metric samples, fingerprint and value intact", async () => {
    const sample = encode("TailSample", {
      signal: Signal.Metrics,
      ts_unix_nano: 1_700_000_000_000_000_000n,
      metric_type: 2,
      series_fingerprint: 0x0123_4567_89ab_cdefn,
      // Not representable in binary32 — proves the f64 survives the wire.
      value: 0.1 + 0.2,
      labels: [
        { key: "__name__", value: "reqs" },
        { key: "job", value: "api" },
      ],
    });
    const transport = new ReplayTransport([HELLO_ACK, sample]);
    const seen: TailSample[] = [];

    await runTail(
      transport,
      "target",
      { matchers: [], signal: Signal.Metrics },
      "1.2.3",
      { onSample: (s) => seen.push(s) },
      new AbortController().signal,
    );

    expect(seen).toHaveLength(1);
    expect(seen[0]).toEqual({
      tsUnixNano: 1_700_000_000_000_000_000n,
      metricType: 2,
      seriesFingerprint: 0x0123_4567_89ab_cdefn,
      value: 0.1 + 0.2,
      labels: [
        ["__name__", "reqs"],
        ["job", "api"],
      ],
    });
  });

  /// A logs subscription with no `onSample` must not crash when a stray sample
  /// arrives, and vice versa — both callbacks are optional.
  it("ignores a record shape the caller did not ask for", async () => {
    const transport = new ReplayTransport([HELLO_ACK, record("one", 5n)]);
    const samples: TailSample[] = [];
    await runTail(
      transport,
      "target",
      { matchers: [], signal: Signal.Metrics },
      "1.0.0",
      { onSample: (s) => samples.push(s) },
      new AbortController().signal,
    );
    expect(samples).toEqual([]);
  });

  /// A Valkey-less query daemon refuses rather than streaming nothing — the UI
  /// has to be able to tell that apart from "no logs matched".
  it("throws the server's refusal as a TailError", async () => {
    const transport = new ReplayTransport([
      encode("Error", { code: TailErrCode.TAIL_UNAVAILABLE, message: "no valkey" }),
    ]);
    await expect(
      runTail(
        transport,
        "target",
        { matchers: [] },
        "1.0.0",
        { onRecord: () => {} },
        new AbortController().signal,
      ),
    ).rejects.toMatchObject({
      name: "TailError",
      code: TailErrCode.TAIL_UNAVAILABLE,
    });
  });

  it("rejects a record that arrives before the handshake", async () => {
    const transport = new ReplayTransport([record("early", 1n)]);
    await expect(
      runTail(
        transport,
        "target",
        { matchers: [] },
        "1.0.0",
        { onRecord: () => {} },
        new AbortController().signal,
      ),
    ).rejects.toThrow(/before the handshake/);
  });

  it("ends quietly on a server Goodbye", async () => {
    const transport = new ReplayTransport([
      HELLO_ACK,
      record("last", 9n),
      encode("Goodbye", { reason: 0, message: "draining" }),
    ]);
    const seen: string[] = [];
    await expect(
      runTail(
        transport,
        "target",
        { matchers: [] },
        "1.0.0",
        { onRecord: (r) => seen.push(r.body) },
        new AbortController().signal,
      ),
    ).resolves.toBeUndefined();
    expect(seen).toEqual(["last"]);
  });

  it("reports the first error, not the ones that follow it", async () => {
    const transport = new ReplayTransport([
      encode("Error", { code: TailErrCode.BAD_MATCHER, message: "first" }),
      encode("Error", { code: TailErrCode.INTERNAL, message: "second" }),
    ]);
    await expect(
      runTail(
        transport,
        "target",
        { matchers: ["bad~~"] },
        "1.0.0",
        { onRecord: () => {} },
        new AbortController().signal,
      ),
    ).rejects.toMatchObject({ code: TailErrCode.BAD_MATCHER });
  });
});

describe("TailError", () => {
  it("names the numeric code", () => {
    expect(new TailError(TailErrCode.TAIL_UNAVAILABLE, "nope").message).toContain(
      "ERR_TAIL_UNAVAILABLE",
    );
  });
});
