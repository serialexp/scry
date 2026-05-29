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

function __bs_get<T>(expr: () => T): T | undefined {
  try {
    return expr();
  } catch {
    return undefined;
  }
}

function __bs_numeric(value: any): any {
  if (typeof value === "bigint") {
    return value;
  }
  if (typeof value === "number" && Number.isInteger(value)) {
    return BigInt(value);
  }
  return value;
}

function __bs_literal(value: number): number | bigint {
  if (Number.isInteger(value)) {
    return BigInt(value);
  }
  return value;
}

function __bs_checkCondition(expr: () => any): boolean {
  try {
    const result = expr();
    if (typeof result === "bigint") {
      return result !== 0n;
    }
    return !!result;
  } catch {
    return false;
  }
}

/**
 * Top-level discriminated union of all query-protocol messages. Peek-discriminated on the message-type byte; each variant struct begins with a const-tagged uint8 that matches the discriminator. Client → server: Request (exactly one). Server → client: SchemaMsg (exactly one), then BatchMsg* , then EndOfStream OR StreamError (exactly one terminator).
 */
export interface QueryFrameInput {
  /**
   * Discriminated Union
   * Type that can be one of several variants, chosen based on a discriminator value. Supports peek-based (read ahead) or field-based (reference earlier field) discrimination.
   *
   * @remarks
   *
   * Discriminator: peek uint8
   * Variants: 5
   * - QueryRequest (when value === 0x01)
   * - SchemaMsg (when value === 0x10)
   * - BatchMsg (when value === 0x11)
   * - EndOfStream (when value === 0x1F)
   * - StreamError (when value === 0xF0)
   */
  msg: QueryRequestInput | SchemaMsgInput | BatchMsgInput | EndOfStreamInput | StreamErrorInput;
}

/**
 * Top-level discriminated union of all query-protocol messages. Peek-discriminated on the message-type byte; each variant struct begins with a const-tagged uint8 that matches the discriminator. Client → server: Request (exactly one). Server → client: SchemaMsg (exactly one), then BatchMsg* , then EndOfStream OR StreamError (exactly one terminator).
 */
export interface QueryFrameOutput {
  /**
   * Discriminated Union
   * Type that can be one of several variants, chosen based on a discriminator value. Supports peek-based (read ahead) or field-based (reference earlier field) discrimination.
   *
   * @remarks
   *
   * Discriminator: peek uint8
   * Variants: 5
   * - QueryRequest (when value === 0x01)
   * - SchemaMsg (when value === 0x10)
   * - BatchMsg (when value === 0x11)
   * - EndOfStream (when value === 0x1F)
   * - StreamError (when value === 0xF0)
   */
  msg: QueryRequestOutput | SchemaMsgOutput | BatchMsgOutput | EndOfStreamOutput | StreamErrorOutput;
}

export type QueryFrame = QueryFrameOutput;

/**
 * Variant tags for QueryFrame.msg
 */
export const enum QueryFrameMsgVariant {
  QueryRequest = 'QueryRequest',
  SchemaMsg = 'SchemaMsg',
  BatchMsg = 'BatchMsg',
  EndOfStream = 'EndOfStream',
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
    else if (value.msg.type === 'EndOfStream') {
      const encoder_value = new EndOfStreamEncoder();
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
    else if (value.msg.type === 'SchemaMsg') {
      const _enc = new SchemaMsgEncoder();
      size += _enc.calculateSize(value.msg.value);
    }
    else if (value.msg.type === 'BatchMsg') {
      const _enc = new BatchMsgEncoder();
      size += _enc.calculateSize(value.msg.value);
    }
    else if (value.msg.type === 'EndOfStream') {
      const _enc = new EndOfStreamEncoder();
      size += _enc.calculateSize(value.msg.value);
    }
    else if (value.msg.type === 'StreamError') {
      const _enc = new StreamErrorEncoder();
      size += _enc.calculateSize(value.msg.value);
    }
    else {
      throw new BinSchemaError(ErrorCode.INVALID_VARIANT, `Unknown variant type for msg: ${value.msg.type}`);
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
    else if (discriminator === 0x1F) {
      const decoder = new EndOfStreamDecoder(this.bytes.slice(this.byteOffset), value);
      const decodedValue = decoder.decode();
      this.byteOffset += decoder.byteOffset;
      value.msg = { type: 'EndOfStream', value: decodedValue };
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
 * Sent by the client at the start of every query connection. Carries the target signal byte (1 = metrics, 2 = logs, 3 = traces, 4 = profiles — values match scry_proto::constants::Signal), the AND'd matcher set + time bounds (the postings preselect), optional SQL against the registered table for that signal, an optional row limit, an optional caller-supplied tracing correlation id, and an optional trace_id (16 raw bytes; empty = absent) for the traces signal's by-id lookup. Receiver fails with QUERY_ERR_BAD_REQUEST if `signal` is 0 or names an unimplemented signal. NOTE: 'optional' fields would be the natural shape here, but as of binschema 0.5.x the Rust generator emits NotImplemented when 'optional' appears inside a discriminated_union variant (works fine in plain structs — see ingest's Span.parent_span_id). We model each optional with an explicit '*_present: uint8' companion (0 = absent, 1 = present); when absent the value field is still serialised but should be ignored by the receiver. Switch back to 'optional' once binschema gains support.
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
}

/**
 * Sent by the client at the start of every query connection. Carries the target signal byte (1 = metrics, 2 = logs, 3 = traces, 4 = profiles — values match scry_proto::constants::Signal), the AND'd matcher set + time bounds (the postings preselect), optional SQL against the registered table for that signal, an optional row limit, an optional caller-supplied tracing correlation id, and an optional trace_id (16 raw bytes; empty = absent) for the traces signal's by-id lookup. Receiver fails with QUERY_ERR_BAD_REQUEST if `signal` is 0 or names an unimplemented signal. NOTE: 'optional' fields would be the natural shape here, but as of binschema 0.5.x the Rust generator emits NotImplemented when 'optional' appears inside a discriminated_union variant (works fine in plain structs — see ingest's Span.parent_span_id). We model each optional with an explicit '*_present: uint8' companion (0 = absent, 1 = present); when absent the value field is still serialised but should be ignored by the receiver. Switch back to 'optional' once binschema gains support.
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
    return this.finish();
  }

  /**
   * Calculate the encoded size of a QueryRequest value.
   * Used for from_after_field computed lengths and buffer pre-allocation.
   */
  calculateSize(value: QueryRequest): number {
    let size = 0;
    size += 1; // tag (const)
    size += 1; // signal
    // matchers: array (kind: length_prefixed)
    for (const item of value.matchers) {
      const matchers_itemEncoder = new MatcherEncoder();
      size += matchers_itemEncoder.calculateSize(item);
    }
    size += 1; // ts_min_present
    size += 8; // ts_min
    size += 1; // ts_max_present
    size += 8; // ts_max
    // sql: string (utf8)
    size += new TextEncoder().encode(value.sql).length;
    size += 8; // limit
    // request_id: string (ascii)
    size += value.request_id.length;
    // trace_id: custom type (bytes)
    const trace_id_encoder = new bytesEncoder();
    size += trace_id_encoder.calculateSize(value.trace_id);
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
    // ipc_bytes: custom type (bytes)
    const ipc_bytes_encoder = new bytesEncoder();
    size += ipc_bytes_encoder.calculateSize(value.ipc_bytes);
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
    // ipc_bytes: custom type (bytes)
    const ipc_bytes_encoder = new bytesEncoder();
    size += ipc_bytes_encoder.calculateSize(value.ipc_bytes);
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
    let size = 0;
    size += 1; // tag (const)
    size += 8; // total_rows
    return size;
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
    size += 1; // tag (const)
    size += 2; // code
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

