//! The live-tail client, in TypeScript (D-052/D-053).
//!
//! A tail is not a query. It speaks the **ingest** `Frame` union, not the
//! query one, because queryd's tail front-door is a transparent relay of what
//! ingesters already emit. Hence the separate `../proto/generated-ingest`
//! bindings.
//!
//! The sub-protocol is Hello → HelloAck → Subscribe → `TailRecord`*, which
//! looks like it needs a round trip in the middle. It doesn't: the server
//! reads frames sequentially off a buffered reader and does not care that the
//! Subscribe arrived before it had replied. So we **pipeline** — one write of
//! both frames — and validate the ordering on the way back. That keeps a tail
//! the same "send bytes, read a stream" shape as a query, which is what lets
//! `scry web` relay it without knowing any protocol.
//!
//! Records are best-effort by design: dropped under load, unordered across
//! ingesters, never deduplicated against stored history. This is "what is
//! happening right now?", not "give me a complete log".

import {
  FrameEncoder,
  FrameDecoder,
  type FrameInput,
  type HelloAckOutput,
  type TailRecordOutput,
  // Named `Error_Output` because binschema renames a schema type that collides
  // with a JS global (`Error` → `Error_`). That rename is deliberate and now
  // consistent across declaration and reference sites, so we import the real
  // name and alias it locally for readability.
  type Error_Output as ErrorOutput,
  type GoodbyeOutput,
} from "../proto/generated-ingest";
import { frame } from "./framing";
import {
  PROTOCOL_VERSION_V0,
  SIGNAL_BIT_LOGS,
  Signal,
  tailErrName,
} from "./constants";
import type { Transport } from "./transport";

// Same generator-bug bridge as the query client: the emitted encoder/decoder
// use a tagged `{ type, value }` envelope at runtime while the declared type is
// a bare union. See `client.ts` for the full note.
type TaggedFrame =
  | { type: "HelloAck"; value: HelloAckOutput }
  | { type: "TailRecord"; value: TailRecordOutput }
  | { type: "Goodbye"; value: GoodbyeOutput }
  | { type: "Error"; value: ErrorOutput }
  | { type: string; value: unknown };

/** One live record, as pushed by the server. */
export interface TailRecord {
  tsUnixNano: bigint;
  severity: number;
  body: string;
  labels: [string, string][];
  attrs: [string, string][];
}

/** A protocol-level ingest-wire `Error` frame, surfaced as an exception. */
export class TailError extends Error {
  constructor(
    public readonly code: number,
    public readonly serverMessage: string,
  ) {
    super(`${tailErrName(code)} (${code}): ${serverMessage}`);
    this.name = "TailError";
  }
}

/** What the UI has to say to describe a subscription. */
export interface TailSpec {
  /** Prometheus-style matcher specs, ANDed server-side by `scry-match`. */
  matchers: string[];
}

/** Render a label name/value pair as a matcher spec the server will parse.
 *  Values are double-quoted (the grammar allows it) so spaces, `=` and `,`
 *  inside a value can't be mistaken for syntax. */
export function equalityMatcher(name: string, value: string): string {
  const escaped = value.replace(/\\/g, "\\\\").replace(/"/g, '\\"');
  return `${name}="${escaped}"`;
}

/** A throwaway per-subscription id. A tail client never writes records, so
 *  this only ever shows up in the server's connection logs. */
function randomAgentId(): number[] {
  const bytes = new Uint8Array(16);
  crypto.getRandomValues(bytes);
  return Array.from(bytes);
}

/**
 * Build the single request body for a subscription: `Hello` and `Subscribe`
 * back to back, each length-prefixed.
 */
export function buildSubscribeRequest(spec: TailSpec, version: string): Uint8Array {
  const hello = frame(
    new FrameEncoder().encode({
      msg: {
        type: "Hello",
        value: {
          protocol_version: PROTOCOL_VERSION_V0,
          agent_id: randomAgentId(),
          agent_version: version,
          hostname: "scry-ui",
          // Logs is the only tailable signal today.
          signals: SIGNAL_BIT_LOGS,
          capabilities: 0,
          resource_attrs: [],
        },
      },
    } as unknown as FrameInput),
  );
  const subscribe = frame(
    new FrameEncoder().encode({
      msg: {
        type: "Subscribe",
        value: {
          signal: Signal.Logs,
          matchers: spec.matchers.map((m) => ({ spec: m })),
        },
      },
    } as unknown as FrameInput),
  );

  const out = new Uint8Array(hello.length + subscribe.length);
  out.set(hello, 0);
  out.set(subscribe, hello.length);
  return out;
}

function toPairs(pairs: { key: string; value: string }[]): [string, string][] {
  return pairs.map((p) => [p.key, p.value]);
}

/** Decode a `TailRecord` frame body into the UI's record shape. */
export function decodeTailRecord(value: TailRecordOutput): TailRecord {
  return {
    tsUnixNano: value.ts_unix_nano,
    severity: value.severity,
    body: value.body,
    labels: toPairs(value.labels),
    attrs: toPairs(value.attributes),
  };
}

export interface TailCallbacks {
  /** Fires once the server has acknowledged the handshake. */
  onSubscribed?: () => void;
  /** Fires per pushed record. */
  onRecord: (record: TailRecord) => void;
}

/**
 * Subscribe over `transport` and pump records into `callbacks` until `signal`
 * aborts or the server ends the stream.
 *
 * Resolves when the stream ends. Rejects with a `TailError` for a protocol
 * refusal (notably `ERR_TAIL_UNAVAILABLE`, which is a query daemon with no
 * Valkey), or the transport's own error otherwise.
 */
export async function runTail(
  transport: Transport,
  addr: string,
  spec: TailSpec,
  version: string,
  callbacks: TailCallbacks,
  signal: AbortSignal,
): Promise<void> {
  const request = buildSubscribeRequest(spec, version);
  let sawHelloAck = false;
  let failure: unknown = null;

  await transport.tail(
    addr,
    request,
    (body) => {
      // One error ends the subscription; ignore anything after it rather than
      // reporting a cascade of consequences.
      if (failure !== null) return;
      const decoded = new FrameDecoder(body).decode();
      const msg = (decoded as unknown as { msg: TaggedFrame }).msg;
      switch (msg.type) {
        case "HelloAck":
          sawHelloAck = true;
          callbacks.onSubscribed?.();
          break;
        case "TailRecord":
          if (!sawHelloAck) {
            failure = new Error("record received before the handshake completed");
            break;
          }
          callbacks.onRecord(decodeTailRecord(msg.value as TailRecordOutput));
          break;
        case "Goodbye":
          // Server-initiated close; the stream ends on its own right after.
          break;
        case "Error": {
          const e = msg.value as ErrorOutput;
          failure = new TailError(e.code, e.message);
          break;
        }
        default:
          // Ping/Pong and ingest-only frames have no meaning on a tail
          // connection; the CLI ignores them too.
          break;
      }
    },
    signal,
  );

  if (failure !== null) throw failure;
}
