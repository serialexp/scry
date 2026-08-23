import { tableFromArrays, tableToIPC } from "apache-arrow";
import { describe, expect, it } from "vitest";
import {
  QueryFrameDecoder,
  QueryFrameEncoder,
  type QueryFrameInput,
} from "../proto/generated";
import { runQuery } from "./client";
import { QUERY_CAP_ATTEMPT_SUPERSESSION, Signal } from "./constants";
import { deframe, frame } from "./framing";
import type { Transport } from "./transport";

function responseFrame(type: string, value: unknown): Uint8Array {
  const input = { msg: { type, value } } as unknown as QueryFrameInput;
  return frame(new QueryFrameEncoder().encode(input));
}

function responseStream(...frames: Uint8Array[]): Uint8Array {
  const length = frames.reduce((total, current) => total + current.length, 0);
  const bytes = new Uint8Array(length);
  let offset = 0;
  for (const current of frames) {
    bytes.set(current, offset);
    offset += current.length;
  }
  return bytes;
}

function arrowStream(attempt: string): number[] {
  return Array.from(tableToIPC(tableFromArrays({ attempt: [attempt] }), "stream"));
}

class StubTransport implements Transport {
  constructor(private readonly response: Uint8Array) {}

  async query(_addr: string, request: Uint8Array): Promise<Uint8Array> {
    const decoded = new QueryFrameDecoder(deframe(request)[0]).decode() as unknown as {
      msg: { type: string; value: { capabilities: number } };
    };
    expect(decoded.msg.type).toBe("QueryRequest");
    expect(decoded.msg.value.capabilities).toBe(QUERY_CAP_ATTEMPT_SUPERSESSION);
    return this.response;
  }
}

const spec = { signal: Signal.Logs, matchers: [] };

function run(...frames: Uint8Array[]) {
  return runQuery(new StubTransport(responseStream(...frames)), "queryd", spec);
}

const schema = (ipc_bytes: number[] = [0]) => responseFrame("SchemaMsg", { ipc_bytes });
const superseded = (superseded_attempt: number, next_attempt: number) =>
  responseFrame("ResponseSuperseded", {
    superseded_attempt,
    next_attempt,
    reason: 1,
  });
const end = (total_rows = 1n) => responseFrame("EndOfStream", { total_rows });

describe("runQuery attempt supersession", () => {
  it("discards a successful old attempt and accepts the replacement Arrow stream", async () => {
    const result = await run(
      schema(arrowStream("old")),
      superseded(0, 1),
      schema(arrowStream("new")),
      end(),
    );

    expect(result.rowCount).toBe(1);
    expect(result.totalRows).toBe(1n);
    expect(Array.from(result.table.getChild("attempt")!.toArray())).toEqual(["new"]);
  });

  it("rejects supersession before the current attempt has a schema", async () => {
    await expect(run(superseded(0, 1))).rejects.toThrow(
      "invalid ResponseSuperseded attempt transition",
    );
  });

  it.each([
    [1, 2, "a stale superseded-attempt id"],
    [0, 2, "a skipped next-attempt id"],
  ])("rejects %s -> %s (%s)", async (oldAttempt, nextAttempt) => {
    await expect(run(schema(), superseded(oldAttempt, nextAttempt))).rejects.toThrow(
      "invalid ResponseSuperseded attempt transition",
    );
  });

  it("requires a fresh schema immediately after supersession", async () => {
    await expect(run(schema(), superseded(0, 1), end(0n))).rejects.toThrow(
      "EndOfStream received before schema",
    );
  });

  it("rejects a batch while waiting for the replacement schema", async () => {
    await expect(
      run(
        schema(),
        superseded(0, 1),
        responseFrame("BatchMsg", { ipc_bytes: [0] }),
      ),
    ).rejects.toThrow("batch received before schema");
  });

  it("tracks attempt ids across more than one valid transition", async () => {
    await expect(
      run(
        schema(),
        superseded(0, 1),
        schema(),
        superseded(0, 1),
      ),
    ).rejects.toThrow("invalid ResponseSuperseded attempt transition");
  });
});
