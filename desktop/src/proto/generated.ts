// @ts-nocheck — VENDORED binschema-generated output. Do not hand-edit;
// regenerate with scripts/gen-proto-ts.sh. The binschema 0.6.x TS
// generator emits code that does not satisfy our strict tsconfig
// (bare-union variants used as { type, value }, cross-class private
// access, unused locals). Runtime behaviour is correct; only the
// emitted static types are at fault. Tracked upstream in binschema.

import { BitStreamEncoder, Endianness } from "./bit-stream.js";
import { SeekableBitStreamDecoder } from "./seekable-bit-stream.js";
import { createReader } from "./binary-reader.js";
import { crc32 } from "./crc32.js";
import { evaluateExpression } from "./expression-evaluator.js";
import { BinSchemaError, ErrorCode } from "./errors.js";

/**
 * Top-level discriminated union of all query-protocol messages. Peek-discriminated on the message-type byte; each variant struct begins with a const-tagged uint8 that matches the discriminator. Client → server: Request (exactly one). Server → client: SchemaMsg (exactly one), then BatchMsg* , then an optional QueryStats, then EndOfStream OR StreamError (exactly one terminator). QueryStats is non-terminal and sits immediately before EndOfStream so that a client's 'read until terminator' loop keeps its shape; a client that does not know the tag skips the frame and still terminates correctly.
 */
export interface QueryFrameInput {
  /**
   * Discriminated Union
   * Type that can be one of several variants, chosen based on a discriminator value. Supports peek-based (read ahead) or field-based (reference earlier field) discrimination.
   *
   * @remarks
   *
   * Discriminator: peek uint8
   * Variants: 13
   * - QueryRequest (when value === 0x01)
   * - LabelNamesRequest (when value === 0x02)
   * - LabelValuesRequest (when value === 0x03)
   * - FleetStatusRequest (when value === 0x04)
   * - SchemaMsg (when value === 0x10)
   * - BatchMsg (when value === 0x11)
   * - ResponseSuperseded (when value === 0x12)
   * - QueryStats (when value === 0x1E)
   * - EndOfStream (when value === 0x1F)
   * - LabelNamesResponse (when value === 0x20)
   * - LabelValuesResponse (when value === 0x21)
   * - FleetStatusResponse (when value === 0x22)
   * - StreamError (when value === 0xF0)
   */
  msg: { type: 'QueryRequest'; value: QueryRequestInput } | { type: 'LabelNamesRequest'; value: LabelNamesRequestInput } | { type: 'LabelValuesRequest'; value: LabelValuesRequestInput } | { type: 'FleetStatusRequest'; value: FleetStatusRequestInput } | { type: 'SchemaMsg'; value: SchemaMsgInput } | { type: 'BatchMsg'; value: BatchMsgInput } | { type: 'ResponseSuperseded'; value: ResponseSupersededInput } | { type: 'QueryStats'; value: QueryStatsInput } | { type: 'EndOfStream'; value: EndOfStreamInput } | { type: 'LabelNamesResponse'; value: LabelNamesResponseInput } | { type: 'LabelValuesResponse'; value: LabelValuesResponseInput } | { type: 'FleetStatusResponse'; value: FleetStatusResponseInput } | { type: 'StreamError'; value: StreamErrorInput };
}

/**
 * Top-level discriminated union of all query-protocol messages. Peek-discriminated on the message-type byte; each variant struct begins with a const-tagged uint8 that matches the discriminator. Client → server: Request (exactly one). Server → client: SchemaMsg (exactly one), then BatchMsg* , then an optional QueryStats, then EndOfStream OR StreamError (exactly one terminator). QueryStats is non-terminal and sits immediately before EndOfStream so that a client's 'read until terminator' loop keeps its shape; a client that does not know the tag skips the frame and still terminates correctly.
 */
export interface QueryFrameOutput {
  /**
   * Discriminated Union
   * Type that can be one of several variants, chosen based on a discriminator value. Supports peek-based (read ahead) or field-based (reference earlier field) discrimination.
   *
   * @remarks
   *
   * Discriminator: peek uint8
   * Variants: 13
   * - QueryRequest (when value === 0x01)
   * - LabelNamesRequest (when value === 0x02)
   * - LabelValuesRequest (when value === 0x03)
   * - FleetStatusRequest (when value === 0x04)
   * - SchemaMsg (when value === 0x10)
   * - BatchMsg (when value === 0x11)
   * - ResponseSuperseded (when value === 0x12)
   * - QueryStats (when value === 0x1E)
   * - EndOfStream (when value === 0x1F)
   * - LabelNamesResponse (when value === 0x20)
   * - LabelValuesResponse (when value === 0x21)
   * - FleetStatusResponse (when value === 0x22)
   * - StreamError (when value === 0xF0)
   */
  msg: { type: 'QueryRequest'; value: QueryRequestOutput } | { type: 'LabelNamesRequest'; value: LabelNamesRequestOutput } | { type: 'LabelValuesRequest'; value: LabelValuesRequestOutput } | { type: 'FleetStatusRequest'; value: FleetStatusRequestOutput } | { type: 'SchemaMsg'; value: SchemaMsgOutput } | { type: 'BatchMsg'; value: BatchMsgOutput } | { type: 'ResponseSuperseded'; value: ResponseSupersededOutput } | { type: 'QueryStats'; value: QueryStatsOutput } | { type: 'EndOfStream'; value: EndOfStreamOutput } | { type: 'LabelNamesResponse'; value: LabelNamesResponseOutput } | { type: 'LabelValuesResponse'; value: LabelValuesResponseOutput } | { type: 'FleetStatusResponse'; value: FleetStatusResponseOutput } | { type: 'StreamError'; value: StreamErrorOutput };
}

export type QueryFrame = QueryFrameOutput;

/**
 * Variant tags for QueryFrame.msg
 */
export const enum QueryFrameMsgVariant {
  QueryRequest = 'QueryRequest',
  LabelNamesRequest = 'LabelNamesRequest',
  LabelValuesRequest = 'LabelValuesRequest',
  FleetStatusRequest = 'FleetStatusRequest',
  SchemaMsg = 'SchemaMsg',
  BatchMsg = 'BatchMsg',
  ResponseSuperseded = 'ResponseSuperseded',
  QueryStats = 'QueryStats',
  EndOfStream = 'EndOfStream',
  LabelNamesResponse = 'LabelNamesResponse',
  LabelValuesResponse = 'LabelValuesResponse',
  FleetStatusResponse = 'FleetStatusResponse',
  StreamError = 'StreamError',
}

export class QueryFrameEncoder extends BitStreamEncoder {
  private compressionDict: Map<string, number> = new Map();

  constructor() {
    super("msb_first");
  }

  encode(value: QueryFrameInput): Uint8Array {
    // Reset compression dictionary for each encode
    this.compressionDict.clear();

    if (value.msg.type === 'QueryRequest') {
      const encoder_value = new QueryRequestEncoder();
      const encoded_value = encoder_value.encode(value.msg.value);
      for (const byte of encoded_value) {
        this.writeUint8(byte);
      }
    }
    else if (value.msg.type === 'LabelNamesRequest') {
      const encoder_value = new LabelNamesRequestEncoder();
      const encoded_value = encoder_value.encode(value.msg.value);
      for (const byte of encoded_value) {
        this.writeUint8(byte);
      }
    }
    else if (value.msg.type === 'LabelValuesRequest') {
      const encoder_value = new LabelValuesRequestEncoder();
      const encoded_value = encoder_value.encode(value.msg.value);
      for (const byte of encoded_value) {
        this.writeUint8(byte);
      }
    }
    else if (value.msg.type === 'FleetStatusRequest') {
      const encoder_value = new FleetStatusRequestEncoder();
      const encoded_value = encoder_value.encode(value.msg.value);
      for (const byte of encoded_value) {
        this.writeUint8(byte);
      }
    }
    else if (value.msg.type === 'SchemaMsg') {
      const encoder_value = new SchemaMsgEncoder();
      const encoded_value = encoder_value.encode(value.msg.value);
      for (const byte of encoded_value) {
        this.writeUint8(byte);
      }
    }
    else if (value.msg.type === 'BatchMsg') {
      const encoder_value = new BatchMsgEncoder();
      const encoded_value = encoder_value.encode(value.msg.value);
      for (const byte of encoded_value) {
        this.writeUint8(byte);
      }
    }
    else if (value.msg.type === 'ResponseSuperseded') {
      const encoder_value = new ResponseSupersededEncoder();
      const encoded_value = encoder_value.encode(value.msg.value);
      for (const byte of encoded_value) {
        this.writeUint8(byte);
      }
    }
    else if (value.msg.type === 'QueryStats') {
      const encoder_value = new QueryStatsEncoder();
      const encoded_value = encoder_value.encode(value.msg.value);
      for (const byte of encoded_value) {
        this.writeUint8(byte);
      }
    }
    else if (value.msg.type === 'EndOfStream') {
      const encoder_value = new EndOfStreamEncoder();
      const encoded_value = encoder_value.encode(value.msg.value);
      for (const byte of encoded_value) {
        this.writeUint8(byte);
      }
    }
    else if (value.msg.type === 'LabelNamesResponse') {
      const encoder_value = new LabelNamesResponseEncoder();
      const encoded_value = encoder_value.encode(value.msg.value);
      for (const byte of encoded_value) {
        this.writeUint8(byte);
      }
    }
    else if (value.msg.type === 'LabelValuesResponse') {
      const encoder_value = new LabelValuesResponseEncoder();
      const encoded_value = encoder_value.encode(value.msg.value);
      for (const byte of encoded_value) {
        this.writeUint8(byte);
      }
    }
    else if (value.msg.type === 'FleetStatusResponse') {
      const encoder_value = new FleetStatusResponseEncoder();
      const encoded_value = encoder_value.encode(value.msg.value);
      for (const byte of encoded_value) {
        this.writeUint8(byte);
      }
    }
    else if (value.msg.type === 'StreamError') {
      const encoder_value = new StreamErrorEncoder();
      const encoded_value = encoder_value.encode(value.msg.value);
      for (const byte of encoded_value) {
        this.writeUint8(byte);
      }
    } else {
      throw new BinSchemaError(ErrorCode.INVALID_VARIANT, `Unknown variant type: ${(value.msg as any).type}`);
    }
    return this.finish();
  }

  /**
   * Calculate the encoded size of a QueryFrame value.
   * Used for from_after_field computed lengths and buffer pre-allocation.
   */
  calculateSize(value: QueryFrame): number {
    let size = 0;
    if (value.msg.type === 'QueryRequest') {
      const _enc = new QueryRequestEncoder();
      size += _enc.calculateSize(value.msg.value);
    }
    else if (value.msg.type === 'LabelNamesRequest') {
      const _enc = new LabelNamesRequestEncoder();
      size += _enc.calculateSize(value.msg.value);
    }
    else if (value.msg.type === 'LabelValuesRequest') {
      const _enc = new LabelValuesRequestEncoder();
      size += _enc.calculateSize(value.msg.value);
    }
    else if (value.msg.type === 'FleetStatusRequest') {
      const _enc = new FleetStatusRequestEncoder();
      size += _enc.calculateSize(value.msg.value);
    }
    else if (value.msg.type === 'SchemaMsg') {
      const _enc = new SchemaMsgEncoder();
      size += _enc.calculateSize(value.msg.value);
    }
    else if (value.msg.type === 'BatchMsg') {
      const _enc = new BatchMsgEncoder();
      size += _enc.calculateSize(value.msg.value);
    }
    else if (value.msg.type === 'ResponseSuperseded') {
      const _enc = new ResponseSupersededEncoder();
      size += _enc.calculateSize(value.msg.value);
    }
    else if (value.msg.type === 'QueryStats') {
      const _enc = new QueryStatsEncoder();
      size += _enc.calculateSize(value.msg.value);
    }
    else if (value.msg.type === 'EndOfStream') {
      const _enc = new EndOfStreamEncoder();
      size += _enc.calculateSize(value.msg.value);
    }
    else if (value.msg.type === 'LabelNamesResponse') {
      const _enc = new LabelNamesResponseEncoder();
      size += _enc.calculateSize(value.msg.value);
    }
    else if (value.msg.type === 'LabelValuesResponse') {
      const _enc = new LabelValuesResponseEncoder();
      size += _enc.calculateSize(value.msg.value);
    }
    else if (value.msg.type === 'FleetStatusResponse') {
      const _enc = new FleetStatusResponseEncoder();
      size += _enc.calculateSize(value.msg.value);
    }
    else if (value.msg.type === 'StreamError') {
      const _enc = new StreamErrorEncoder();
      size += _enc.calculateSize(value.msg.value);
    }
    else {
      throw new BinSchemaError(ErrorCode.INVALID_VARIANT, `Unknown variant type for msg: ${(value.msg as any).type}`);
    }
    return size;
  }
}

export class QueryFrameDecoder extends SeekableBitStreamDecoder {
  constructor(input: Uint8Array | number[] | string, private context?: any) {
    const reader = createReader(input);
    super(reader, "msb_first");
  }

  decode(): QueryFrameOutput {
    const value: any = {};

    const discriminator = this.peekUint8();
    if (discriminator === 0x01) {
      const decoder = new QueryRequestDecoder(this.bytes.slice(this.byteOffset), value);
      const decodedValue = decoder.decode();
      this.byteOffset += decoder.byteOffset;
      value.msg = { type: 'QueryRequest', value: decodedValue };
    }
    else if (discriminator === 0x02) {
      const decoder = new LabelNamesRequestDecoder(this.bytes.slice(this.byteOffset), value);
      const decodedValue = decoder.decode();
      this.byteOffset += decoder.byteOffset;
      value.msg = { type: 'LabelNamesRequest', value: decodedValue };
    }
    else if (discriminator === 0x03) {
      const decoder = new LabelValuesRequestDecoder(this.bytes.slice(this.byteOffset), value);
      const decodedValue = decoder.decode();
      this.byteOffset += decoder.byteOffset;
      value.msg = { type: 'LabelValuesRequest', value: decodedValue };
    }
    else if (discriminator === 0x04) {
      const decoder = new FleetStatusRequestDecoder(this.bytes.slice(this.byteOffset), value);
      const decodedValue = decoder.decode();
      this.byteOffset += decoder.byteOffset;
      value.msg = { type: 'FleetStatusRequest', value: decodedValue };
    }
    else if (discriminator === 0x10) {
      const decoder = new SchemaMsgDecoder(this.bytes.slice(this.byteOffset), value);
      const decodedValue = decoder.decode();
      this.byteOffset += decoder.byteOffset;
      value.msg = { type: 'SchemaMsg', value: decodedValue };
    }
    else if (discriminator === 0x11) {
      const decoder = new BatchMsgDecoder(this.bytes.slice(this.byteOffset), value);
      const decodedValue = decoder.decode();
      this.byteOffset += decoder.byteOffset;
      value.msg = { type: 'BatchMsg', value: decodedValue };
    }
    else if (discriminator === 0x12) {
      const decoder = new ResponseSupersededDecoder(this.bytes.slice(this.byteOffset), value);
      const decodedValue = decoder.decode();
      this.byteOffset += decoder.byteOffset;
      value.msg = { type: 'ResponseSuperseded', value: decodedValue };
    }
    else if (discriminator === 0x1E) {
      const decoder = new QueryStatsDecoder(this.bytes.slice(this.byteOffset), value);
      const decodedValue = decoder.decode();
      this.byteOffset += decoder.byteOffset;
      value.msg = { type: 'QueryStats', value: decodedValue };
    }
    else if (discriminator === 0x1F) {
      const decoder = new EndOfStreamDecoder(this.bytes.slice(this.byteOffset), value);
      const decodedValue = decoder.decode();
      this.byteOffset += decoder.byteOffset;
      value.msg = { type: 'EndOfStream', value: decodedValue };
    }
    else if (discriminator === 0x20) {
      const decoder = new LabelNamesResponseDecoder(this.bytes.slice(this.byteOffset), value);
      const decodedValue = decoder.decode();
      this.byteOffset += decoder.byteOffset;
      value.msg = { type: 'LabelNamesResponse', value: decodedValue };
    }
    else if (discriminator === 0x21) {
      const decoder = new LabelValuesResponseDecoder(this.bytes.slice(this.byteOffset), value);
      const decodedValue = decoder.decode();
      this.byteOffset += decoder.byteOffset;
      value.msg = { type: 'LabelValuesResponse', value: decodedValue };
    }
    else if (discriminator === 0x22) {
      const decoder = new FleetStatusResponseDecoder(this.bytes.slice(this.byteOffset), value);
      const decodedValue = decoder.decode();
      this.byteOffset += decoder.byteOffset;
      value.msg = { type: 'FleetStatusResponse', value: decodedValue };
    }
    else if (discriminator === 0xF0) {
      const decoder = new StreamErrorDecoder(this.bytes.slice(this.byteOffset), value);
      const decodedValue = decoder.decode();
      this.byteOffset += decoder.byteOffset;
      value.msg = { type: 'StreamError', value: decodedValue };
    } else {
      throw new BinSchemaError(ErrorCode.INVALID_VARIANT, `Unknown discriminator: 0x${discriminator.toString(16)}`);
    }
    return value;
  }
}

/**
 * Sent by the client at the start of every query connection. Carries the target signal byte (1 = metrics, 2 = logs, 3 = traces, 4 = profiles — values match scry_proto::constants::Signal), the AND'd matcher set + time bounds (the postings preselect), optional SQL against the registered table for that signal, an optional row limit, an optional caller-supplied tracing correlation id, and an optional trace_id (16 raw bytes; empty = absent) for the traces signal's by-id lookup, and an optional body_contains substring (empty = absent) for the logs signal's full-text search, and a `live` flag (0/1) requesting the merged history+live view (D-054): when 1 and signal = logs the server unions the stored blocks with the still-in-flight records fanned in from the ingesters, deduplicated across the block-commit seam by WAL-segment watermark. `live` requires Valkey for ingester discovery — the server fails with QUERY_ERR_LIVE_UNAVAILABLE if it has none. `live` is ignored for non-logs signals in v1. A `with_labels` flag (0/1) requesting the opt-in fingerprint→label join for the metrics signal: when 1 and signal = metrics the server appends a synthesised `labels` Map<Utf8,Utf8> column to the result rows (each series' resolved labels, inverted from the postings sidecars), so a series can be named by its labels instead of its opaque fingerprint. Off by default so fingerprint-only metrics queries stay cheap; materialised only when the projection actually selects `labels`. Ignored for non-metrics signals (logs already carry a `labels` column unconditionally). Receiver fails with QUERY_ERR_BAD_REQUEST if `signal` is 0 or names an unimplemented signal. NOTE: 'optional' fields would be the natural shape here, but as of binschema 0.5.x the Rust generator emits NotImplemented when 'optional' appears inside a discriminated_union variant (works fine in plain structs — see ingest's Span.parent_span_id). We model each optional with an explicit '*_present: uint8' companion (0 = absent, 1 = present); when absent the value field is still serialised but should be ignored by the receiver. Switch back to 'optional' once binschema gains support.
 */
export interface QueryRequestInput {
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  signal: number;
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint16
   */
  matchers: MatcherInput[];
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  ts_min_present: number;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  ts_min: bigint;
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  ts_max_present: number;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  ts_max: bigint;
  /**
   * String kind: length_prefixed
   * Encoding: utf8
   * Length prefix type: uint32
   */
  sql: string;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  limit: bigint;
  /**
   * String kind: length_prefixed
   * Encoding: ascii
   * Length prefix type: uint16
   */
  request_id: string;
  /**
   * Bytes
   * Raw byte array. Sugar for array of uint8 — same wire format, simpler schema definition.
   */
  trace_id: number[];
  /**
   * String kind: length_prefixed
   * Encoding: utf8
   * Length prefix type: uint32
   */
  body_contains: string;
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  live: number;
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  with_labels: number;
  /**
   * 32-bit Unsigned Integer
   * Fixed-width 32-bit unsigned integer (0-4294967295). Respects endianness configuration.
   */
  capabilities: number;
}

/**
 * Sent by the client at the start of every query connection. Carries the target signal byte (1 = metrics, 2 = logs, 3 = traces, 4 = profiles — values match scry_proto::constants::Signal), the AND'd matcher set + time bounds (the postings preselect), optional SQL against the registered table for that signal, an optional row limit, an optional caller-supplied tracing correlation id, and an optional trace_id (16 raw bytes; empty = absent) for the traces signal's by-id lookup, and an optional body_contains substring (empty = absent) for the logs signal's full-text search, and a `live` flag (0/1) requesting the merged history+live view (D-054): when 1 and signal = logs the server unions the stored blocks with the still-in-flight records fanned in from the ingesters, deduplicated across the block-commit seam by WAL-segment watermark. `live` requires Valkey for ingester discovery — the server fails with QUERY_ERR_LIVE_UNAVAILABLE if it has none. `live` is ignored for non-logs signals in v1. A `with_labels` flag (0/1) requesting the opt-in fingerprint→label join for the metrics signal: when 1 and signal = metrics the server appends a synthesised `labels` Map<Utf8,Utf8> column to the result rows (each series' resolved labels, inverted from the postings sidecars), so a series can be named by its labels instead of its opaque fingerprint. Off by default so fingerprint-only metrics queries stay cheap; materialised only when the projection actually selects `labels`. Ignored for non-metrics signals (logs already carry a `labels` column unconditionally). Receiver fails with QUERY_ERR_BAD_REQUEST if `signal` is 0 or names an unimplemented signal. NOTE: 'optional' fields would be the natural shape here, but as of binschema 0.5.x the Rust generator emits NotImplemented when 'optional' appears inside a discriminated_union variant (works fine in plain structs — see ingest's Span.parent_span_id). We model each optional with an explicit '*_present: uint8' companion (0 = absent, 1 = present); when absent the value field is still serialised but should be ignored by the receiver. Switch back to 'optional' once binschema gains support.
 */
export interface QueryRequestOutput {
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  tag: number;
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  signal: number;
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint16
   */
  matchers: MatcherOutput[];
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  ts_min_present: number;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  ts_min: bigint;
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  ts_max_present: number;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  ts_max: bigint;
  /**
   * String kind: length_prefixed
   * Encoding: utf8
   * Length prefix type: uint32
   */
  sql: string;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  limit: bigint;
  /**
   * String kind: length_prefixed
   * Encoding: ascii
   * Length prefix type: uint16
   */
  request_id: string;
  /**
   * Bytes
   * Raw byte array. Sugar for array of uint8 — same wire format, simpler schema definition.
   */
  trace_id: number[];
  /**
   * String kind: length_prefixed
   * Encoding: utf8
   * Length prefix type: uint32
   */
  body_contains: string;
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  live: number;
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  with_labels: number;
  /**
   * 32-bit Unsigned Integer
   * Fixed-width 32-bit unsigned integer (0-4294967295). Respects endianness configuration.
   */
  capabilities: number;
}

export type QueryRequest = QueryRequestOutput;

export class QueryRequestEncoder extends BitStreamEncoder {
  private compressionDict: Map<string, number> = new Map();

  constructor() {
    super("msb_first");
  }

  encode(value: QueryRequestInput): Uint8Array {
    // Reset compression dictionary for each encode
    this.compressionDict.clear();

    this.writeUint8(1);
    this.writeUint8(value.signal);
    this.writeUint16(value.matchers.length, "big_endian");
    for (let value_matchers__iter_index = 0; value_matchers__iter_index < value.matchers.length; value_matchers__iter_index++) {
      const value_matchers__iter = value.matchers[value_matchers__iter_index];
      const encoder_value_matchers__iter = new MatcherEncoder();
      const encoded_value_matchers__iter = encoder_value_matchers__iter.encode(value_matchers__iter);
      for (const byte of encoded_value_matchers__iter) {
        this.writeUint8(byte);
      }
    }
    this.writeUint8(value.ts_min_present);
    this.writeUint64(value.ts_min, "big_endian");
    this.writeUint8(value.ts_max_present);
    this.writeUint64(value.ts_max, "big_endian");
    const value_sql_bytes = new TextEncoder().encode(value.sql);
    this.writeUint32(value_sql_bytes.length, "big_endian");
    for (const byte of value_sql_bytes) {
      this.writeUint8(byte);
    }
    this.writeUint64(value.limit, "big_endian");
    const value_request_id_bytes = Array.from(value.request_id, c => c.charCodeAt(0));
    this.writeUint16(value_request_id_bytes.length, "big_endian");
    for (const byte of value_request_id_bytes) {
      this.writeUint8(byte);
    }
    this.writeUint16(value.trace_id.length, "big_endian");
    for (let value_trace_id__iter_index = 0; value_trace_id__iter_index < value.trace_id.length; value_trace_id__iter_index++) {
      const value_trace_id__iter = value.trace_id[value_trace_id__iter_index];
      this.writeUint8(value_trace_id__iter);
    }
    const value_body_contains_bytes = new TextEncoder().encode(value.body_contains);
    this.writeUint32(value_body_contains_bytes.length, "big_endian");
    for (const byte of value_body_contains_bytes) {
      this.writeUint8(byte);
    }
    this.writeUint8(value.live);
    this.writeUint8(value.with_labels);
    this.writeUint32(value.capabilities, "big_endian");
    return this.finish();
  }

  /**
   * Calculate the encoded size of a QueryRequest value.
   * Used for from_after_field computed lengths and buffer pre-allocation.
   */
  calculateSize(value: QueryRequest): number {
    let size = 0;
    size += 2; // tag (const) + signal
    // matchers: array (kind: length_prefixed)
    for (const item of value.matchers) {
      const matchers_itemEncoder = new MatcherEncoder();
      size += matchers_itemEncoder.calculateSize(item);
    }
    size += 20; // length prefix (uint16) + ts_min_present + ts_min + ts_max_present + ts_max
    // sql: string (utf8)
    size += new TextEncoder().encode(value.sql).length;
    size += 8; // limit
    // request_id: string (ascii)
    size += value.request_id.length;
    // trace_id: bytes (kind: length_prefixed)
    size += value.trace_id.length;
    size += 2; // length prefix (uint16)
    // body_contains: string (utf8)
    size += new TextEncoder().encode(value.body_contains).length;
    size += 6; // live + with_labels + capabilities
    return size;
  }
}

export class QueryRequestDecoder extends SeekableBitStreamDecoder {
  constructor(input: Uint8Array | number[] | string, private context?: any) {
    const reader = createReader(input);
    super(reader, "msb_first");
  }

  decode(): QueryRequestOutput {
    const value: any = {};

    value.tag = this.readUint8();
    value.signal = this.readUint8();
    value.matchers = [];
    const matchers_length = this.readUint16("big_endian");
    for (let i = 0; i < matchers_length; i++) {
      let matchers__iter: any;
      matchers__iter = {};
      const matchers__iter_name_length = this.readUint16("big_endian");
      const matchers__iter_name_bytes = this.readBytesSlice(matchers__iter_name_length);
      try {
        matchers__iter.name = new TextDecoder("utf-8", { fatal: true }).decode(matchers__iter_name_bytes);
      } catch (e) {
        throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
      }
      const matchers__iter_value_length = this.readUint16("big_endian");
      const matchers__iter_value_bytes = this.readBytesSlice(matchers__iter_value_length);
      try {
        matchers__iter.value = new TextDecoder("utf-8", { fatal: true }).decode(matchers__iter_value_bytes);
      } catch (e) {
        throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
      }
      value.matchers.push(matchers__iter);
    }
    value.ts_min_present = this.readUint8();
    value.ts_min = this.readUint64("big_endian");
    value.ts_max_present = this.readUint8();
    value.ts_max = this.readUint64("big_endian");
    const sql_length = this.readUint32("big_endian");
    const sql_bytes = this.readBytesSlice(sql_length);
    try {
      value.sql = new TextDecoder("utf-8", { fatal: true }).decode(sql_bytes);
    } catch (e) {
      throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
    }
    value.limit = this.readUint64("big_endian");
    const request_id_length = this.readUint16("big_endian");
    const request_id_bytes = this.readBytesSlice(request_id_length);
    value.request_id = String.fromCharCode(...request_id_bytes);
    value.trace_id = [];
    const trace_id_length = this.readUint16("big_endian");
    for (let i = 0; i < trace_id_length; i++) {
      let trace_id__iter: any;
      trace_id__iter = this.readUint8();
      value.trace_id.push(trace_id__iter);
    }
    const body_contains_length = this.readUint32("big_endian");
    const body_contains_bytes = this.readBytesSlice(body_contains_length);
    try {
      value.body_contains = new TextDecoder("utf-8", { fatal: true }).decode(body_contains_bytes);
    } catch (e) {
      throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
    }
    value.live = this.readUint8();
    value.with_labels = this.readUint8();
    value.capabilities = this.readUint32("big_endian");
    return value;
  }
}

/**
 * One equality label matcher (name = value). The matcher set is AND'd on the server before postings resolve.
 */
export interface MatcherInput {
  /**
   * String kind: length_prefixed
   * Encoding: utf8
   * Length prefix type: uint16
   */
  name: string;
  /**
   * String kind: length_prefixed
   * Encoding: utf8
   * Length prefix type: uint16
   */
  value: string;
}

/**
 * One equality label matcher (name = value). The matcher set is AND'd on the server before postings resolve.
 */
export interface MatcherOutput {
  /**
   * String kind: length_prefixed
   * Encoding: utf8
   * Length prefix type: uint16
   */
  name: string;
  /**
   * String kind: length_prefixed
   * Encoding: utf8
   * Length prefix type: uint16
   */
  value: string;
}

export type Matcher = MatcherOutput;

export class MatcherEncoder extends BitStreamEncoder {
  private compressionDict: Map<string, number> = new Map();

  constructor() {
    super("msb_first");
  }

  encode(value: MatcherInput): Uint8Array {
    // Reset compression dictionary for each encode
    this.compressionDict.clear();

    const value_name_bytes = new TextEncoder().encode(value.name);
    this.writeUint16(value_name_bytes.length, "big_endian");
    for (const byte of value_name_bytes) {
      this.writeUint8(byte);
    }
    const value_value_bytes = new TextEncoder().encode(value.value);
    this.writeUint16(value_value_bytes.length, "big_endian");
    for (const byte of value_value_bytes) {
      this.writeUint8(byte);
    }
    return this.finish();
  }

  /**
   * Calculate the encoded size of a Matcher value.
   * Used for from_after_field computed lengths and buffer pre-allocation.
   */
  calculateSize(value: Matcher): number {
    let size = 0;
    // name: string (utf8)
    size += new TextEncoder().encode(value.name).length;
    // value: string (utf8)
    size += new TextEncoder().encode(value.value).length;
    return size;
  }
}

export class MatcherDecoder extends SeekableBitStreamDecoder {
  constructor(input: Uint8Array | number[] | string, private context?: any) {
    const reader = createReader(input);
    super(reader, "msb_first");
  }

  decode(): MatcherOutput {
    const value: any = {};

    const name_length = this.readUint16("big_endian");
    const name_bytes = this.readBytesSlice(name_length);
    try {
      value.name = new TextDecoder("utf-8", { fatal: true }).decode(name_bytes);
    } catch (e) {
      throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
    }
    const value_length = this.readUint16("big_endian");
    const value_bytes = this.readBytesSlice(value_length);
    try {
      value.value = new TextDecoder("utf-8", { fatal: true }).decode(value_bytes);
    } catch (e) {
      throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
    }
    return value;
  }
}

/**
 * Client → server. Metadata request for label DISCOVERABILITY, not data: 'what label names can I match on?' Queryd answers from its process-wide bounded suggestion view (D-069): it warms candidate blocks overlapping [ts_min, ts_max], merges what it learns with labels retained from other ranges, and may therefore return useful suggestions outside the requested window. The one-shot local CLI retains exact range semantics. Time bounds use the same `*_present: uint8` convention as QueryRequest. The server replies with exactly one LabelNamesResponse and closes, or a StreamError.
 */
export interface LabelNamesRequestInput {
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  signal: number;
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  ts_min_present: number;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  ts_min: bigint;
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  ts_max_present: number;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  ts_max: bigint;
  /**
   * 32-bit Unsigned Integer
   * Fixed-width 32-bit unsigned integer (0-4294967295). Respects endianness configuration.
   */
  capabilities: number;
}

/**
 * Client → server. Metadata request for label DISCOVERABILITY, not data: 'what label names can I match on?' Queryd answers from its process-wide bounded suggestion view (D-069): it warms candidate blocks overlapping [ts_min, ts_max], merges what it learns with labels retained from other ranges, and may therefore return useful suggestions outside the requested window. The one-shot local CLI retains exact range semantics. Time bounds use the same `*_present: uint8` convention as QueryRequest. The server replies with exactly one LabelNamesResponse and closes, or a StreamError.
 */
export interface LabelNamesRequestOutput {
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  tag: number;
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  signal: number;
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  ts_min_present: number;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  ts_min: bigint;
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  ts_max_present: number;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  ts_max: bigint;
  /**
   * 32-bit Unsigned Integer
   * Fixed-width 32-bit unsigned integer (0-4294967295). Respects endianness configuration.
   */
  capabilities: number;
}

export type LabelNamesRequest = LabelNamesRequestOutput;

export class LabelNamesRequestEncoder extends BitStreamEncoder {
  private compressionDict: Map<string, number> = new Map();

  constructor() {
    super("msb_first");
  }

  encode(value: LabelNamesRequestInput): Uint8Array {
    // Reset compression dictionary for each encode
    this.compressionDict.clear();

    this.writeUint8(2);
    this.writeUint8(value.signal);
    this.writeUint8(value.ts_min_present);
    this.writeUint64(value.ts_min, "big_endian");
    this.writeUint8(value.ts_max_present);
    this.writeUint64(value.ts_max, "big_endian");
    this.writeUint32(value.capabilities, "big_endian");
    return this.finish();
  }

  /**
   * Calculate the encoded size of a LabelNamesRequest value.
   * Used for from_after_field computed lengths and buffer pre-allocation.
   */
  calculateSize(value: LabelNamesRequest): number {
    return 24; // tag (const) + signal + ts_min_present + ts_min + ts_max_present + ts_max + capabilities
  }
}

export class LabelNamesRequestDecoder extends SeekableBitStreamDecoder {
  constructor(input: Uint8Array | number[] | string, private context?: any) {
    const reader = createReader(input);
    super(reader, "msb_first");
  }

  decode(): LabelNamesRequestOutput {
    const value: any = {};

    value.tag = this.readUint8();
    value.signal = this.readUint8();
    value.ts_min_present = this.readUint8();
    value.ts_min = this.readUint64("big_endian");
    value.ts_max_present = this.readUint8();
    value.ts_max = this.readUint64("big_endian");
    value.capabilities = this.readUint32("big_endian");
    return value;
  }
}

/**
 * Client → server. Metadata suggestion request: 'what known values does label `label_name` take?' Queryd uses the same process-wide, range-expanding semantics as LabelNamesRequest and returns a bounded deterministic set (ordinary labels 1,000 by default; metrics `__name__` 10,000). Suggestions may come from another previously warmed range. The server replies with exactly one LabelValuesResponse and closes, or a StreamError.
 */
export interface LabelValuesRequestInput {
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  signal: number;
  /**
   * String kind: length_prefixed
   * Encoding: utf8
   * Length prefix type: uint16
   */
  label_name: string;
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  ts_min_present: number;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  ts_min: bigint;
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  ts_max_present: number;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  ts_max: bigint;
  /**
   * 32-bit Unsigned Integer
   * Fixed-width 32-bit unsigned integer (0-4294967295). Respects endianness configuration.
   */
  capabilities: number;
}

/**
 * Client → server. Metadata suggestion request: 'what known values does label `label_name` take?' Queryd uses the same process-wide, range-expanding semantics as LabelNamesRequest and returns a bounded deterministic set (ordinary labels 1,000 by default; metrics `__name__` 10,000). Suggestions may come from another previously warmed range. The server replies with exactly one LabelValuesResponse and closes, or a StreamError.
 */
export interface LabelValuesRequestOutput {
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  tag: number;
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  signal: number;
  /**
   * String kind: length_prefixed
   * Encoding: utf8
   * Length prefix type: uint16
   */
  label_name: string;
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  ts_min_present: number;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  ts_min: bigint;
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  ts_max_present: number;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  ts_max: bigint;
  /**
   * 32-bit Unsigned Integer
   * Fixed-width 32-bit unsigned integer (0-4294967295). Respects endianness configuration.
   */
  capabilities: number;
}

export type LabelValuesRequest = LabelValuesRequestOutput;

export class LabelValuesRequestEncoder extends BitStreamEncoder {
  private compressionDict: Map<string, number> = new Map();

  constructor() {
    super("msb_first");
  }

  encode(value: LabelValuesRequestInput): Uint8Array {
    // Reset compression dictionary for each encode
    this.compressionDict.clear();

    this.writeUint8(3);
    this.writeUint8(value.signal);
    const value_label_name_bytes = new TextEncoder().encode(value.label_name);
    this.writeUint16(value_label_name_bytes.length, "big_endian");
    for (const byte of value_label_name_bytes) {
      this.writeUint8(byte);
    }
    this.writeUint8(value.ts_min_present);
    this.writeUint64(value.ts_min, "big_endian");
    this.writeUint8(value.ts_max_present);
    this.writeUint64(value.ts_max, "big_endian");
    this.writeUint32(value.capabilities, "big_endian");
    return this.finish();
  }

  /**
   * Calculate the encoded size of a LabelValuesRequest value.
   * Used for from_after_field computed lengths and buffer pre-allocation.
   */
  calculateSize(value: LabelValuesRequest): number {
    let size = 0;
    size += 2; // tag (const) + signal
    // label_name: string (utf8)
    size += new TextEncoder().encode(value.label_name).length;
    size += 22; // ts_min_present + ts_min + ts_max_present + ts_max + capabilities
    return size;
  }
}

export class LabelValuesRequestDecoder extends SeekableBitStreamDecoder {
  constructor(input: Uint8Array | number[] | string, private context?: any) {
    const reader = createReader(input);
    super(reader, "msb_first");
  }

  decode(): LabelValuesRequestOutput {
    const value: any = {};

    value.tag = this.readUint8();
    value.signal = this.readUint8();
    const label_name_length = this.readUint16("big_endian");
    const label_name_bytes = this.readBytesSlice(label_name_length);
    try {
      value.label_name = new TextDecoder("utf-8", { fatal: true }).decode(label_name_bytes);
    } catch (e) {
      throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
    }
    value.ts_min_present = this.readUint8();
    value.ts_min = this.readUint64("big_endian");
    value.ts_max_present = this.readUint8();
    value.ts_max = this.readUint64("big_endian");
    value.capabilities = this.readUint32("big_endian");
    return value;
  }
}

/**
 * Server → client. The distinct, sorted label names for a LabelNamesRequest. One frame, terminal — the connection closes after it. `names` is deduplicated and lexicographically sorted across all candidate blocks.
 */
export interface LabelNamesResponseInput {
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint32
   */
  names: string[];
}

/**
 * Server → client. The distinct, sorted label names for a LabelNamesRequest. One frame, terminal — the connection closes after it. `names` is deduplicated and lexicographically sorted across all candidate blocks.
 */
export interface LabelNamesResponseOutput {
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  tag: number;
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint32
   */
  names: string[];
}

export type LabelNamesResponse = LabelNamesResponseOutput;

export class LabelNamesResponseEncoder extends BitStreamEncoder {
  private compressionDict: Map<string, number> = new Map();

  constructor() {
    super("msb_first");
  }

  encode(value: LabelNamesResponseInput): Uint8Array {
    // Reset compression dictionary for each encode
    this.compressionDict.clear();

    this.writeUint8(32);
    this.writeUint32(value.names.length, "big_endian");
    for (let value_names__iter_index = 0; value_names__iter_index < value.names.length; value_names__iter_index++) {
      const value_names__iter = value.names[value_names__iter_index];
      const value_names__iter_bytes = new TextEncoder().encode(value_names__iter);
      this.writeUint16(value_names__iter_bytes.length, "big_endian");
      for (const byte of value_names__iter_bytes) {
        this.writeUint8(byte);
      }
    }
    return this.finish();
  }

  /**
   * Calculate the encoded size of a LabelNamesResponse value.
   * Used for from_after_field computed lengths and buffer pre-allocation.
   */
  calculateSize(value: LabelNamesResponse): number {
    let size = 0;
    size += 1; // tag (const)
    // names: array (kind: length_prefixed)
    for (const item of value.names) {
      size += 0;
    }
    size += 4; // length prefix (uint32)
    return size;
  }
}

export class LabelNamesResponseDecoder extends SeekableBitStreamDecoder {
  constructor(input: Uint8Array | number[] | string, private context?: any) {
    const reader = createReader(input);
    super(reader, "msb_first");
  }

  decode(): LabelNamesResponseOutput {
    const value: any = {};

    value.tag = this.readUint8();
    value.names = [];
    const names_length = this.readUint32("big_endian");
    for (let i = 0; i < names_length; i++) {
      let names__iter: any;
      const names__iter_length = this.readUint16("big_endian");
      const names__iter_bytes = this.readBytesSlice(names__iter_length);
      try {
        names__iter = new TextDecoder("utf-8", { fatal: true }).decode(names__iter_bytes);
      } catch (e) {
        throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
      }
      value.names.push(names__iter);
    }
    return value;
  }
}

/**
 * Server → client. The distinct, sorted values for a LabelValuesRequest. One frame, terminal — the connection closes after it.
 */
export interface LabelValuesResponseInput {
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint32
   */
  values: string[];
}

/**
 * Server → client. The distinct, sorted values for a LabelValuesRequest. One frame, terminal — the connection closes after it.
 */
export interface LabelValuesResponseOutput {
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  tag: number;
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint32
   */
  values: string[];
}

export type LabelValuesResponse = LabelValuesResponseOutput;

export class LabelValuesResponseEncoder extends BitStreamEncoder {
  private compressionDict: Map<string, number> = new Map();

  constructor() {
    super("msb_first");
  }

  encode(value: LabelValuesResponseInput): Uint8Array {
    // Reset compression dictionary for each encode
    this.compressionDict.clear();

    this.writeUint8(33);
    this.writeUint32(value.values.length, "big_endian");
    for (let value_values__iter_index = 0; value_values__iter_index < value.values.length; value_values__iter_index++) {
      const value_values__iter = value.values[value_values__iter_index];
      const value_values__iter_bytes = new TextEncoder().encode(value_values__iter);
      this.writeUint16(value_values__iter_bytes.length, "big_endian");
      for (const byte of value_values__iter_bytes) {
        this.writeUint8(byte);
      }
    }
    return this.finish();
  }

  /**
   * Calculate the encoded size of a LabelValuesResponse value.
   * Used for from_after_field computed lengths and buffer pre-allocation.
   */
  calculateSize(value: LabelValuesResponse): number {
    let size = 0;
    size += 1; // tag (const)
    // values: array (kind: length_prefixed)
    for (const item of value.values) {
      size += 0;
    }
    size += 4; // length prefix (uint32)
    return size;
  }
}

export class LabelValuesResponseDecoder extends SeekableBitStreamDecoder {
  constructor(input: Uint8Array | number[] | string, private context?: any) {
    const reader = createReader(input);
    super(reader, "msb_first");
  }

  decode(): LabelValuesResponseOutput {
    const value: any = {};

    value.tag = this.readUint8();
    value.values = [];
    const values_length = this.readUint32("big_endian");
    for (let i = 0; i < values_length; i++) {
      let values__iter: any;
      const values__iter_length = this.readUint16("big_endian");
      const values__iter_bytes = this.readBytesSlice(values__iter_length);
      try {
        values__iter = new TextDecoder("utf-8", { fatal: true }).decode(values__iter_bytes);
      } catch (e) {
        throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
      }
      value.values.push(values__iter);
    }
    return value;
  }
}

/**
 * Client → server. Requests the complete live fleet snapshot currently registered in the query daemon's Valkey. The request has no fields. A Valkey-connected daemon replies with one FleetStatusResponse and closes; a daemon without fleet discovery replies with QUERY_ERR_FLEET_UNAVAILABLE.
 */
export interface FleetStatusRequestInput {
}

/**
 * Client → server. Requests the complete live fleet snapshot currently registered in the query daemon's Valkey. The request has no fields. A Valkey-connected daemon replies with one FleetStatusResponse and closes; a daemon without fleet discovery replies with QUERY_ERR_FLEET_UNAVAILABLE.
 */
export interface FleetStatusRequestOutput {
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  tag: number;
}

export type FleetStatusRequest = FleetStatusRequestOutput;

export class FleetStatusRequestEncoder extends BitStreamEncoder {
  private compressionDict: Map<string, number> = new Map();

  constructor() {
    super("msb_first");
  }

  encode(value: FleetStatusRequestInput): Uint8Array {
    // Reset compression dictionary for each encode
    this.compressionDict.clear();

    this.writeUint8(4);
    return this.finish();
  }

  /**
   * Calculate the encoded size of a FleetStatusRequest value.
   * Used for from_after_field computed lengths and buffer pre-allocation.
   */
  calculateSize(value: FleetStatusRequest): number {
    return 1; // tag (const)
  }
}

export class FleetStatusRequestDecoder extends SeekableBitStreamDecoder {
  constructor(input: Uint8Array | number[] | string, private context?: any) {
    const reader = createReader(input);
    super(reader, "msb_first");
  }

  decode(): FleetStatusRequestOutput {
    const value: any = {};

    value.tag = this.readUint8();
    return value;
  }
}

/**
 * Server → client. One terminal response containing the valid live StatusSnapshot JSON documents discovered from Valkey. Records are canonical JSON strings so the query protocol remains independent of role-specific status payload evolution.
 */
export interface FleetStatusResponseInput {
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint32
   */
  instances_json: string[];
}

/**
 * Server → client. One terminal response containing the valid live StatusSnapshot JSON documents discovered from Valkey. Records are canonical JSON strings so the query protocol remains independent of role-specific status payload evolution.
 */
export interface FleetStatusResponseOutput {
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  tag: number;
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint32
   */
  instances_json: string[];
}

export type FleetStatusResponse = FleetStatusResponseOutput;

export class FleetStatusResponseEncoder extends BitStreamEncoder {
  private compressionDict: Map<string, number> = new Map();

  constructor() {
    super("msb_first");
  }

  encode(value: FleetStatusResponseInput): Uint8Array {
    // Reset compression dictionary for each encode
    this.compressionDict.clear();

    this.writeUint8(34);
    this.writeUint32(value.instances_json.length, "big_endian");
    for (let value_instances_json__iter_index = 0; value_instances_json__iter_index < value.instances_json.length; value_instances_json__iter_index++) {
      const value_instances_json__iter = value.instances_json[value_instances_json__iter_index];
      const value_instances_json__iter_bytes = new TextEncoder().encode(value_instances_json__iter);
      this.writeUint32(value_instances_json__iter_bytes.length, "big_endian");
      for (const byte of value_instances_json__iter_bytes) {
        this.writeUint8(byte);
      }
    }
    return this.finish();
  }

  /**
   * Calculate the encoded size of a FleetStatusResponse value.
   * Used for from_after_field computed lengths and buffer pre-allocation.
   */
  calculateSize(value: FleetStatusResponse): number {
    let size = 0;
    size += 1; // tag (const)
    // instances_json: array (kind: length_prefixed)
    for (const item of value.instances_json) {
      size += 0;
    }
    size += 4; // length prefix (uint32)
    return size;
  }
}

export class FleetStatusResponseDecoder extends SeekableBitStreamDecoder {
  constructor(input: Uint8Array | number[] | string, private context?: any) {
    const reader = createReader(input);
    super(reader, "msb_first");
  }

  decode(): FleetStatusResponseOutput {
    const value: any = {};

    value.tag = this.readUint8();
    value.instances_json = [];
    const instances_json_length = this.readUint32("big_endian");
    for (let i = 0; i < instances_json_length; i++) {
      let instances_json__iter: any;
      const instances_json__iter_length = this.readUint32("big_endian");
      const instances_json__iter_bytes = this.readBytesSlice(instances_json__iter_length);
      try {
        instances_json__iter = new TextDecoder("utf-8", { fatal: true }).decode(instances_json__iter_bytes);
      } catch (e) {
        throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
      }
      value.instances_json.push(instances_json__iter);
    }
    return value;
  }
}

/**
 * Server → client. The Arrow IPC schema message, exactly one per query, sent before any BatchMsg. `ipc_bytes` is the output of arrow::ipc::writer::write_message for the schema EncodedData — continuation marker, length prefix, flatbuf, padding, body, all included — so the client can feed it directly into an arrow::ipc::reader::StreamDecoder without reconstructing the IPC framing.
 */
export interface SchemaMsgInput {
  /**
   * Bytes
   * Raw byte array. Sugar for array of uint8 — same wire format, simpler schema definition.
   */
  ipc_bytes: number[];
}

/**
 * Server → client. The Arrow IPC schema message, exactly one per query, sent before any BatchMsg. `ipc_bytes` is the output of arrow::ipc::writer::write_message for the schema EncodedData — continuation marker, length prefix, flatbuf, padding, body, all included — so the client can feed it directly into an arrow::ipc::reader::StreamDecoder without reconstructing the IPC framing.
 */
export interface SchemaMsgOutput {
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  tag: number;
  /**
   * Bytes
   * Raw byte array. Sugar for array of uint8 — same wire format, simpler schema definition.
   */
  ipc_bytes: number[];
}

export type SchemaMsg = SchemaMsgOutput;

export class SchemaMsgEncoder extends BitStreamEncoder {
  private compressionDict: Map<string, number> = new Map();

  constructor() {
    super("msb_first");
  }

  encode(value: SchemaMsgInput): Uint8Array {
    // Reset compression dictionary for each encode
    this.compressionDict.clear();

    this.writeUint8(16);
    this.writeUint32(value.ipc_bytes.length, "big_endian");
    for (let value_ipc_bytes__iter_index = 0; value_ipc_bytes__iter_index < value.ipc_bytes.length; value_ipc_bytes__iter_index++) {
      const value_ipc_bytes__iter = value.ipc_bytes[value_ipc_bytes__iter_index];
      this.writeUint8(value_ipc_bytes__iter);
    }
    return this.finish();
  }

  /**
   * Calculate the encoded size of a SchemaMsg value.
   * Used for from_after_field computed lengths and buffer pre-allocation.
   */
  calculateSize(value: SchemaMsg): number {
    let size = 0;
    size += 1; // tag (const)
    // ipc_bytes: bytes (kind: length_prefixed)
    size += value.ipc_bytes.length;
    size += 4; // length prefix (uint32)
    return size;
  }
}

export class SchemaMsgDecoder extends SeekableBitStreamDecoder {
  constructor(input: Uint8Array | number[] | string, private context?: any) {
    const reader = createReader(input);
    super(reader, "msb_first");
  }

  decode(): SchemaMsgOutput {
    const value: any = {};

    value.tag = this.readUint8();
    value.ipc_bytes = [];
    const ipc_bytes_length = this.readUint32("big_endian");
    for (let i = 0; i < ipc_bytes_length; i++) {
      let ipc_bytes__iter: any;
      ipc_bytes__iter = this.readUint8();
      value.ipc_bytes.push(ipc_bytes__iter);
    }
    return value;
  }
}

/**
 * Server → client. One fully IPC-framed message (record batch or dictionary batch). Server produces these via arrow::ipc::writer::write_message; client feeds them to arrow::ipc::reader::StreamDecoder verbatim. Carries dictionary-batch messages too — they're indistinguishable on the wire and StreamDecoder routes by the IPC message header.
 */
export interface BatchMsgInput {
  /**
   * Bytes
   * Raw byte array. Sugar for array of uint8 — same wire format, simpler schema definition.
   */
  ipc_bytes: number[];
}

/**
 * Server → client. One fully IPC-framed message (record batch or dictionary batch). Server produces these via arrow::ipc::writer::write_message; client feeds them to arrow::ipc::reader::StreamDecoder verbatim. Carries dictionary-batch messages too — they're indistinguishable on the wire and StreamDecoder routes by the IPC message header.
 */
export interface BatchMsgOutput {
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  tag: number;
  /**
   * Bytes
   * Raw byte array. Sugar for array of uint8 — same wire format, simpler schema definition.
   */
  ipc_bytes: number[];
}

export type BatchMsg = BatchMsgOutput;

export class BatchMsgEncoder extends BitStreamEncoder {
  private compressionDict: Map<string, number> = new Map();

  constructor() {
    super("msb_first");
  }

  encode(value: BatchMsgInput): Uint8Array {
    // Reset compression dictionary for each encode
    this.compressionDict.clear();

    this.writeUint8(17);
    this.writeUint32(value.ipc_bytes.length, "big_endian");
    for (let value_ipc_bytes__iter_index = 0; value_ipc_bytes__iter_index < value.ipc_bytes.length; value_ipc_bytes__iter_index++) {
      const value_ipc_bytes__iter = value.ipc_bytes[value_ipc_bytes__iter_index];
      this.writeUint8(value_ipc_bytes__iter);
    }
    return this.finish();
  }

  /**
   * Calculate the encoded size of a BatchMsg value.
   * Used for from_after_field computed lengths and buffer pre-allocation.
   */
  calculateSize(value: BatchMsg): number {
    let size = 0;
    size += 1; // tag (const)
    // ipc_bytes: bytes (kind: length_prefixed)
    size += value.ipc_bytes.length;
    size += 4; // length prefix (uint32)
    return size;
  }
}

export class BatchMsgDecoder extends SeekableBitStreamDecoder {
  constructor(input: Uint8Array | number[] | string, private context?: any) {
    const reader = createReader(input);
    super(reader, "msb_first");
  }

  decode(): BatchMsgOutput {
    const value: any = {};

    value.tag = this.readUint8();
    value.ipc_bytes = [];
    const ipc_bytes_length = this.readUint32("big_endian");
    for (let i = 0; i < ipc_bytes_length; i++) {
      let ipc_bytes__iter: any;
      ipc_bytes__iter = this.readUint8();
      value.ipc_bytes.push(ipc_bytes__iter);
    }
    return value;
  }
}

/**
 * Server → client. Non-terminal reset: all schema, dictionaries, batches, rows, and counts from superseded_attempt are invalid. The next frame must be a fresh SchemaMsg for next_attempt.
 */
export interface ResponseSupersededInput {
  /**
   * 32-bit Unsigned Integer
   * Fixed-width 32-bit unsigned integer (0-4294967295). Respects endianness configuration.
   */
  superseded_attempt: number;
  /**
   * 32-bit Unsigned Integer
   * Fixed-width 32-bit unsigned integer (0-4294967295). Respects endianness configuration.
   */
  next_attempt: number;
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  reason: number;
}

/**
 * Server → client. Non-terminal reset: all schema, dictionaries, batches, rows, and counts from superseded_attempt are invalid. The next frame must be a fresh SchemaMsg for next_attempt.
 */
export interface ResponseSupersededOutput {
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  tag: number;
  /**
   * 32-bit Unsigned Integer
   * Fixed-width 32-bit unsigned integer (0-4294967295). Respects endianness configuration.
   */
  superseded_attempt: number;
  /**
   * 32-bit Unsigned Integer
   * Fixed-width 32-bit unsigned integer (0-4294967295). Respects endianness configuration.
   */
  next_attempt: number;
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  reason: number;
}

export type ResponseSuperseded = ResponseSupersededOutput;

export class ResponseSupersededEncoder extends BitStreamEncoder {
  private compressionDict: Map<string, number> = new Map();

  constructor() {
    super("msb_first");
  }

  encode(value: ResponseSupersededInput): Uint8Array {
    // Reset compression dictionary for each encode
    this.compressionDict.clear();

    this.writeUint8(18);
    this.writeUint32(value.superseded_attempt, "big_endian");
    this.writeUint32(value.next_attempt, "big_endian");
    this.writeUint8(value.reason);
    return this.finish();
  }

  /**
   * Calculate the encoded size of a ResponseSuperseded value.
   * Used for from_after_field computed lengths and buffer pre-allocation.
   */
  calculateSize(value: ResponseSuperseded): number {
    return 10; // tag (const) + superseded_attempt + next_attempt + reason
  }
}

export class ResponseSupersededDecoder extends SeekableBitStreamDecoder {
  constructor(input: Uint8Array | number[] | string, private context?: any) {
    const reader = createReader(input);
    super(reader, "msb_first");
  }

  decode(): ResponseSupersededOutput {
    const value: any = {};

    value.tag = this.readUint8();
    value.superseded_attempt = this.readUint32("big_endian");
    value.next_attempt = this.readUint32("big_endian");
    value.reason = this.readUint8();
    return value;
  }
}

/**
 * One ingester's contribution to a `live: true` logs query (D-054). A query is NOT scattered across query daemons — each daemon converges its own catalog and reads blocks itself — so this is the only server-side fan-out there is, and it exists only for the live merge.
 */
export interface LiveNodeTimingInput {
  /**
   * String kind: length_prefixed
   * Encoding: ascii
   * Length prefix type: uint16
   */
  addr: string;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  elapsed_us: bigint;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  rows: bigint;
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  ok: number;
}

/**
 * One ingester's contribution to a `live: true` logs query (D-054). A query is NOT scattered across query daemons — each daemon converges its own catalog and reads blocks itself — so this is the only server-side fan-out there is, and it exists only for the live merge.
 */
export interface LiveNodeTimingOutput {
  /**
   * String kind: length_prefixed
   * Encoding: ascii
   * Length prefix type: uint16
   */
  addr: string;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  elapsed_us: bigint;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  rows: bigint;
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  ok: number;
}

export type LiveNodeTiming = LiveNodeTimingOutput;

export class LiveNodeTimingEncoder extends BitStreamEncoder {
  private compressionDict: Map<string, number> = new Map();

  constructor() {
    super("msb_first");
  }

  encode(value: LiveNodeTimingInput): Uint8Array {
    // Reset compression dictionary for each encode
    this.compressionDict.clear();

    const value_addr_bytes = Array.from(value.addr, c => c.charCodeAt(0));
    this.writeUint16(value_addr_bytes.length, "big_endian");
    for (const byte of value_addr_bytes) {
      this.writeUint8(byte);
    }
    this.writeUint64(value.elapsed_us, "big_endian");
    this.writeUint64(value.rows, "big_endian");
    this.writeUint8(value.ok);
    return this.finish();
  }

  /**
   * Calculate the encoded size of a LiveNodeTiming value.
   * Used for from_after_field computed lengths and buffer pre-allocation.
   */
  calculateSize(value: LiveNodeTiming): number {
    let size = 0;
    // addr: string (ascii)
    size += value.addr.length;
    size += 17; // elapsed_us + rows + ok
    return size;
  }
}

export class LiveNodeTimingDecoder extends SeekableBitStreamDecoder {
  constructor(input: Uint8Array | number[] | string, private context?: any) {
    const reader = createReader(input);
    super(reader, "msb_first");
  }

  decode(): LiveNodeTimingOutput {
    const value: any = {};

    const addr_length = this.readUint16("big_endian");
    const addr_bytes = this.readBytesSlice(addr_length);
    value.addr = String.fromCharCode(...addr_bytes);
    value.elapsed_us = this.readUint64("big_endian");
    value.rows = this.readUint64("big_endian");
    value.ok = this.readUint8();
    return value;
  }
}

/**
 * Server → client. Non-terminal per-query timing breakdown, sent immediately before EndOfStream. Answers 'was that transfer, planning, or execution?' without reading the daemon's logs. All durations are MICROseconds (milliseconds would round an entire cache-hit path to 0) and are measured on the server that served this query. Sent unconditionally rather than behind an opt-in flag: it is ~120 bytes once per query, and an opt-in would make the numbers missing exactly when someone is chasing a slow query.
 * Deliberately its OWN frame rather than extra fields on EndOfStream: the terminator is written through the result cache's tee and is therefore part of the cached entry, so timings living inside it would replay the original miss's breakdown on every subsequent 2 ms cache hit — stale precisely when it matters. QueryStats is written outside the tee and is always fresh; on a cache hit it reports the hit's own (small) numbers with cache_hit = 1.
 * NOTHING IS SMEARED: server_total_us is measured independently of the phases, so a client should render `server_total_us - Σ(phases)` as an explicit 'other' bucket rather than distributing it. The df_* fields are DataFusion's own leaf metrics summed ACROSS PARTITIONS and can legitimately exceed the wall-clock phase that contains them — they are a labelled detail group, never timeline slices. A phase that did not run is 0 (e.g. live_fetch_us on a non-live query, plan_us on a cache hit).
 */
export interface QueryStatsInput {
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  server_total_us: bigint;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  admission_wait_us: bigint;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  catalog_us: bigint;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  cache_lookup_us: bigint;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  live_fetch_us: bigint;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  register_us: bigint;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  plan_us: bigint;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  execute_us: bigint;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  serialize_us: bigint;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  write_us: bigint;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  postings_fetch_us: bigint;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  bloom_fetch_us: bigint;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  df_opening_us: bigint;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  df_scanning_us: bigint;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  df_compute_us: bigint;
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  cache_hit: number;
  /**
   * 32-bit Unsigned Integer
   * Fixed-width 32-bit unsigned integer (0-4294967295). Respects endianness configuration.
   */
  attempts: number;
  /**
   * 32-bit Unsigned Integer
   * Fixed-width 32-bit unsigned integer (0-4294967295). Respects endianness configuration.
   */
  blocks_considered: number;
  /**
   * 32-bit Unsigned Integer
   * Fixed-width 32-bit unsigned integer (0-4294967295). Respects endianness configuration.
   */
  blocks_scanned: number;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  bytes_scanned: bigint;
  /**
   * String kind: length_prefixed
   * Encoding: ascii
   * Length prefix type: uint16
   */
  node_id: string;
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint16
   */
  live_nodes: LiveNodeTimingInput[];
}

/**
 * Server → client. Non-terminal per-query timing breakdown, sent immediately before EndOfStream. Answers 'was that transfer, planning, or execution?' without reading the daemon's logs. All durations are MICROseconds (milliseconds would round an entire cache-hit path to 0) and are measured on the server that served this query. Sent unconditionally rather than behind an opt-in flag: it is ~120 bytes once per query, and an opt-in would make the numbers missing exactly when someone is chasing a slow query.
 * Deliberately its OWN frame rather than extra fields on EndOfStream: the terminator is written through the result cache's tee and is therefore part of the cached entry, so timings living inside it would replay the original miss's breakdown on every subsequent 2 ms cache hit — stale precisely when it matters. QueryStats is written outside the tee and is always fresh; on a cache hit it reports the hit's own (small) numbers with cache_hit = 1.
 * NOTHING IS SMEARED: server_total_us is measured independently of the phases, so a client should render `server_total_us - Σ(phases)` as an explicit 'other' bucket rather than distributing it. The df_* fields are DataFusion's own leaf metrics summed ACROSS PARTITIONS and can legitimately exceed the wall-clock phase that contains them — they are a labelled detail group, never timeline slices. A phase that did not run is 0 (e.g. live_fetch_us on a non-live query, plan_us on a cache hit).
 */
export interface QueryStatsOutput {
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  tag: number;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  server_total_us: bigint;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  admission_wait_us: bigint;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  catalog_us: bigint;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  cache_lookup_us: bigint;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  live_fetch_us: bigint;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  register_us: bigint;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  plan_us: bigint;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  execute_us: bigint;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  serialize_us: bigint;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  write_us: bigint;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  postings_fetch_us: bigint;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  bloom_fetch_us: bigint;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  df_opening_us: bigint;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  df_scanning_us: bigint;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  df_compute_us: bigint;
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  cache_hit: number;
  /**
   * 32-bit Unsigned Integer
   * Fixed-width 32-bit unsigned integer (0-4294967295). Respects endianness configuration.
   */
  attempts: number;
  /**
   * 32-bit Unsigned Integer
   * Fixed-width 32-bit unsigned integer (0-4294967295). Respects endianness configuration.
   */
  blocks_considered: number;
  /**
   * 32-bit Unsigned Integer
   * Fixed-width 32-bit unsigned integer (0-4294967295). Respects endianness configuration.
   */
  blocks_scanned: number;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  bytes_scanned: bigint;
  /**
   * String kind: length_prefixed
   * Encoding: ascii
   * Length prefix type: uint16
   */
  node_id: string;
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint16
   */
  live_nodes: LiveNodeTimingOutput[];
}

export type QueryStats = QueryStatsOutput;

export class QueryStatsEncoder extends BitStreamEncoder {
  private compressionDict: Map<string, number> = new Map();

  constructor() {
    super("msb_first");
  }

  encode(value: QueryStatsInput): Uint8Array {
    // Reset compression dictionary for each encode
    this.compressionDict.clear();

    this.writeUint8(30);
    this.writeUint64(value.server_total_us, "big_endian");
    this.writeUint64(value.admission_wait_us, "big_endian");
    this.writeUint64(value.catalog_us, "big_endian");
    this.writeUint64(value.cache_lookup_us, "big_endian");
    this.writeUint64(value.live_fetch_us, "big_endian");
    this.writeUint64(value.register_us, "big_endian");
    this.writeUint64(value.plan_us, "big_endian");
    this.writeUint64(value.execute_us, "big_endian");
    this.writeUint64(value.serialize_us, "big_endian");
    this.writeUint64(value.write_us, "big_endian");
    this.writeUint64(value.postings_fetch_us, "big_endian");
    this.writeUint64(value.bloom_fetch_us, "big_endian");
    this.writeUint64(value.df_opening_us, "big_endian");
    this.writeUint64(value.df_scanning_us, "big_endian");
    this.writeUint64(value.df_compute_us, "big_endian");
    this.writeUint8(value.cache_hit);
    this.writeUint32(value.attempts, "big_endian");
    this.writeUint32(value.blocks_considered, "big_endian");
    this.writeUint32(value.blocks_scanned, "big_endian");
    this.writeUint64(value.bytes_scanned, "big_endian");
    const value_node_id_bytes = Array.from(value.node_id, c => c.charCodeAt(0));
    this.writeUint16(value_node_id_bytes.length, "big_endian");
    for (const byte of value_node_id_bytes) {
      this.writeUint8(byte);
    }
    this.writeUint16(value.live_nodes.length, "big_endian");
    for (let value_live_nodes__iter_index = 0; value_live_nodes__iter_index < value.live_nodes.length; value_live_nodes__iter_index++) {
      const value_live_nodes__iter = value.live_nodes[value_live_nodes__iter_index];
      const encoder_value_live_nodes__iter = new LiveNodeTimingEncoder();
      const encoded_value_live_nodes__iter = encoder_value_live_nodes__iter.encode(value_live_nodes__iter);
      for (const byte of encoded_value_live_nodes__iter) {
        this.writeUint8(byte);
      }
    }
    return this.finish();
  }

  /**
   * Calculate the encoded size of a QueryStats value.
   * Used for from_after_field computed lengths and buffer pre-allocation.
   */
  calculateSize(value: QueryStats): number {
    let size = 0;
    size += 142; // tag (const) + server_total_us + admission_wait_us + catalog_us + cache_lookup_us + live_fetch_us + register_us + plan_us + execute_us + serialize_us + write_us + postings_fetch_us + bloom_fetch_us + df_opening_us + df_scanning_us + df_compute_us + cache_hit + attempts + blocks_considered + blocks_scanned + bytes_scanned
    // node_id: string (ascii)
    size += value.node_id.length;
    // live_nodes: array (kind: length_prefixed)
    for (const item of value.live_nodes) {
      const live_nodes_itemEncoder = new LiveNodeTimingEncoder();
      size += live_nodes_itemEncoder.calculateSize(item);
    }
    size += 2; // length prefix (uint16)
    return size;
  }
}

export class QueryStatsDecoder extends SeekableBitStreamDecoder {
  constructor(input: Uint8Array | number[] | string, private context?: any) {
    const reader = createReader(input);
    super(reader, "msb_first");
  }

  decode(): QueryStatsOutput {
    const value: any = {};

    value.tag = this.readUint8();
    value.server_total_us = this.readUint64("big_endian");
    value.admission_wait_us = this.readUint64("big_endian");
    value.catalog_us = this.readUint64("big_endian");
    value.cache_lookup_us = this.readUint64("big_endian");
    value.live_fetch_us = this.readUint64("big_endian");
    value.register_us = this.readUint64("big_endian");
    value.plan_us = this.readUint64("big_endian");
    value.execute_us = this.readUint64("big_endian");
    value.serialize_us = this.readUint64("big_endian");
    value.write_us = this.readUint64("big_endian");
    value.postings_fetch_us = this.readUint64("big_endian");
    value.bloom_fetch_us = this.readUint64("big_endian");
    value.df_opening_us = this.readUint64("big_endian");
    value.df_scanning_us = this.readUint64("big_endian");
    value.df_compute_us = this.readUint64("big_endian");
    value.cache_hit = this.readUint8();
    value.attempts = this.readUint32("big_endian");
    value.blocks_considered = this.readUint32("big_endian");
    value.blocks_scanned = this.readUint32("big_endian");
    value.bytes_scanned = this.readUint64("big_endian");
    const node_id_length = this.readUint16("big_endian");
    const node_id_bytes = this.readBytesSlice(node_id_length);
    value.node_id = String.fromCharCode(...node_id_bytes);
    value.live_nodes = [];
    const live_nodes_length = this.readUint16("big_endian");
    for (let i = 0; i < live_nodes_length; i++) {
      let live_nodes__iter: any;
      live_nodes__iter = {};
      const live_nodes__iter_addr_length = this.readUint16("big_endian");
      const live_nodes__iter_addr_bytes = this.readBytesSlice(live_nodes__iter_addr_length);
      live_nodes__iter.addr = String.fromCharCode(...live_nodes__iter_addr_bytes);
      live_nodes__iter.elapsed_us = this.readUint64("big_endian");
      live_nodes__iter.rows = this.readUint64("big_endian");
      live_nodes__iter.ok = this.readUint8();
      value.live_nodes.push(live_nodes__iter);
    }
    return value;
  }
}

/**
 * Server → client. Signals normal completion of the query. total_rows is the server-computed row count across all emitted BatchMsg payloads; the client can cross-check its own count against this value.
 */
export interface EndOfStreamInput {
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  total_rows: bigint;
}

/**
 * Server → client. Signals normal completion of the query. total_rows is the server-computed row count across all emitted BatchMsg payloads; the client can cross-check its own count against this value.
 */
export interface EndOfStreamOutput {
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  tag: number;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  total_rows: bigint;
}

export type EndOfStream = EndOfStreamOutput;

export class EndOfStreamEncoder extends BitStreamEncoder {
  private compressionDict: Map<string, number> = new Map();

  constructor() {
    super("msb_first");
  }

  encode(value: EndOfStreamInput): Uint8Array {
    // Reset compression dictionary for each encode
    this.compressionDict.clear();

    this.writeUint8(31);
    this.writeUint64(value.total_rows, "big_endian");
    return this.finish();
  }

  /**
   * Calculate the encoded size of a EndOfStream value.
   * Used for from_after_field computed lengths and buffer pre-allocation.
   */
  calculateSize(value: EndOfStream): number {
    return 9; // tag (const) + total_rows
  }
}

export class EndOfStreamDecoder extends SeekableBitStreamDecoder {
  constructor(input: Uint8Array | number[] | string, private context?: any) {
    const reader = createReader(input);
    super(reader, "msb_first");
  }

  decode(): EndOfStreamOutput {
    const value: any = {};

    value.tag = this.readUint8();
    value.total_rows = this.readUint64("big_endian");
    return value;
  }
}

/**
 * Server → client. Signals abnormal termination. The connection is closed after this frame; the client should not expect any further frames. code is one of the QUERY_ERR_* constants; message is human-readable context for logging.
 */
export interface StreamErrorInput {
  /**
   * 16-bit Unsigned Integer
   * Fixed-width 16-bit unsigned integer (0-65535). Respects endianness configuration (big-endian or little-endian).
   */
  code: number;
  /**
   * String kind: length_prefixed
   * Encoding: utf8
   * Length prefix type: uint16
   */
  message: string;
}

/**
 * Server → client. Signals abnormal termination. The connection is closed after this frame; the client should not expect any further frames. code is one of the QUERY_ERR_* constants; message is human-readable context for logging.
 */
export interface StreamErrorOutput {
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  tag: number;
  /**
   * 16-bit Unsigned Integer
   * Fixed-width 16-bit unsigned integer (0-65535). Respects endianness configuration (big-endian or little-endian).
   */
  code: number;
  /**
   * String kind: length_prefixed
   * Encoding: utf8
   * Length prefix type: uint16
   */
  message: string;
}

export type StreamError = StreamErrorOutput;

export class StreamErrorEncoder extends BitStreamEncoder {
  private compressionDict: Map<string, number> = new Map();

  constructor() {
    super("msb_first");
  }

  encode(value: StreamErrorInput): Uint8Array {
    // Reset compression dictionary for each encode
    this.compressionDict.clear();

    this.writeUint8(240);
    this.writeUint16(value.code, "big_endian");
    const value_message_bytes = new TextEncoder().encode(value.message);
    this.writeUint16(value_message_bytes.length, "big_endian");
    for (const byte of value_message_bytes) {
      this.writeUint8(byte);
    }
    return this.finish();
  }

  /**
   * Calculate the encoded size of a StreamError value.
   * Used for from_after_field computed lengths and buffer pre-allocation.
   */
  calculateSize(value: StreamError): number {
    let size = 0;
    size += 3; // tag (const) + code
    // message: string (utf8)
    size += new TextEncoder().encode(value.message).length;
    return size;
  }
}

export class StreamErrorDecoder extends SeekableBitStreamDecoder {
  constructor(input: Uint8Array | number[] | string, private context?: any) {
    const reader = createReader(input);
    super(reader, "msb_first");
  }

  decode(): StreamErrorOutput {
    const value: any = {};

    value.tag = this.readUint8();
    value.code = this.readUint16("big_endian");
    const message_length = this.readUint16("big_endian");
    const message_bytes = this.readBytesSlice(message_length);
    try {
      value.message = new TextDecoder("utf-8", { fatal: true }).decode(message_bytes);
    } catch (e) {
      throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
    }
    return value;
  }
}

