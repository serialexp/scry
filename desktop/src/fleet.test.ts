import { describe, expect, it } from "vitest";
import { QueryFrameDecoder, QueryFrameEncoder, type QueryFrameInput } from "./proto/generated";
import { fetchFleetStatus } from "./protocol/client";
import { deframe, frame } from "./protocol/framing";
import type { Transport } from "./protocol/transport";

function response(type: string, value: unknown): Uint8Array {
  const input = { msg: { type, value } } as unknown as QueryFrameInput;
  return frame(new QueryFrameEncoder().encode(input));
}

class StubTransport implements Transport {
  constructor(private readonly result: Uint8Array) {}

  async query(_addr: string, request: Uint8Array): Promise<Uint8Array> {
    const decoded = new QueryFrameDecoder(deframe(request)[0]).decode() as unknown as {
      msg: { type: string };
    };
    expect(decoded.msg.type).toBe("FleetStatusRequest");
    return this.result;
  }
}

describe("fetchFleetStatus", () => {
  it("requests fleet status and parses typed snapshots", async () => {
    const snapshot = {
      role: "agent",
      instance_id: "agent/node-a",
      addr: "node-a",
      now_unix_ms: 123,
      uptime_secs: 42,
      rss_kib: 2048,
      data: { queue_depth: 1 },
    };
    const transport = new StubTransport(
      response("FleetStatusResponse", { instances_json: [JSON.stringify(snapshot)] }),
    );

    await expect(fetchFleetStatus(transport, "target")).resolves.toEqual([snapshot]);
  });

  it("surfaces fleet-unavailable as a QueryError", async () => {
    const transport = new StubTransport(
      response("StreamError", { code: 0x0006, message: "fleet unavailable" }),
    );

    await expect(fetchFleetStatus(transport, "target")).rejects.toMatchObject({
      code: 0x0006,
      serverMessage: "fleet unavailable",
    });
  });

  it("rejects malformed status documents", async () => {
    const transport = new StubTransport(
      response("FleetStatusResponse", { instances_json: [JSON.stringify({ role: "agent" })] }),
    );

    await expect(fetchFleetStatus(transport, "target")).rejects.toThrow(
      "invalid fleet status document",
    );
  });
});
