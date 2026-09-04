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
 * Top-level discriminated union of all messages. Peek-discriminated on the message-type byte; each variant struct begins with a const-tagged uint8 that matches the discriminator.
 */
export interface FrameInput {
  /**
   * Discriminated Union
   * Type that can be one of several variants, chosen based on a discriminator value. Supports peek-based (read ahead) or field-based (reference earlier field) discrimination.
   *
   * @remarks
   *
   * Discriminator: peek uint8
   * Variants: 15
   * - Hello (when value === 0x01)
   * - HelloAck (when value === 0x02)
   * - Batch (when value === 0x10)
   * - BatchAck (when value === 0x11)
   * - FlowControl (when value === 0x20)
   * - AgentStatus (when value === 0x21)
   * - Ping (when value === 0x30)
   * - Pong (when value === 0x31)
   * - Goodbye (when value === 0x40)
   * - Subscribe (when value === 0x50)
   * - TailRecord (when value === 0x51)
   * - LiveQuery (when value === 0x52)
   * - LiveBatch (when value === 0x53)
   * - TailSample (when value === 0x54)
   * - Error (when value === 0xF0)
   */
  msg: { type: 'Hello'; value: HelloInput } | { type: 'HelloAck'; value: HelloAckInput } | { type: 'Batch'; value: BatchInput } | { type: 'BatchAck'; value: BatchAckInput } | { type: 'FlowControl'; value: FlowControlInput } | { type: 'AgentStatus'; value: AgentStatusInput } | { type: 'Ping'; value: PingInput } | { type: 'Pong'; value: PongInput } | { type: 'Goodbye'; value: GoodbyeInput } | { type: 'Subscribe'; value: SubscribeInput } | { type: 'TailRecord'; value: TailRecordInput } | { type: 'LiveQuery'; value: LiveQueryInput } | { type: 'LiveBatch'; value: LiveBatchInput } | { type: 'TailSample'; value: TailSampleInput } | { type: 'Error'; value: Error_Input };
}

/**
 * Top-level discriminated union of all messages. Peek-discriminated on the message-type byte; each variant struct begins with a const-tagged uint8 that matches the discriminator.
 */
export interface FrameOutput {
  /**
   * Discriminated Union
   * Type that can be one of several variants, chosen based on a discriminator value. Supports peek-based (read ahead) or field-based (reference earlier field) discrimination.
   *
   * @remarks
   *
   * Discriminator: peek uint8
   * Variants: 15
   * - Hello (when value === 0x01)
   * - HelloAck (when value === 0x02)
   * - Batch (when value === 0x10)
   * - BatchAck (when value === 0x11)
   * - FlowControl (when value === 0x20)
   * - AgentStatus (when value === 0x21)
   * - Ping (when value === 0x30)
   * - Pong (when value === 0x31)
   * - Goodbye (when value === 0x40)
   * - Subscribe (when value === 0x50)
   * - TailRecord (when value === 0x51)
   * - LiveQuery (when value === 0x52)
   * - LiveBatch (when value === 0x53)
   * - TailSample (when value === 0x54)
   * - Error (when value === 0xF0)
   */
  msg: { type: 'Hello'; value: HelloOutput } | { type: 'HelloAck'; value: HelloAckOutput } | { type: 'Batch'; value: BatchOutput } | { type: 'BatchAck'; value: BatchAckOutput } | { type: 'FlowControl'; value: FlowControlOutput } | { type: 'AgentStatus'; value: AgentStatusOutput } | { type: 'Ping'; value: PingOutput } | { type: 'Pong'; value: PongOutput } | { type: 'Goodbye'; value: GoodbyeOutput } | { type: 'Subscribe'; value: SubscribeOutput } | { type: 'TailRecord'; value: TailRecordOutput } | { type: 'LiveQuery'; value: LiveQueryOutput } | { type: 'LiveBatch'; value: LiveBatchOutput } | { type: 'TailSample'; value: TailSampleOutput } | { type: 'Error'; value: Error_Output };
}

export type Frame = FrameOutput;

/**
 * Variant tags for Frame.msg
 */
export const enum FrameMsgVariant {
  Hello = 'Hello',
  HelloAck = 'HelloAck',
  Batch = 'Batch',
  BatchAck = 'BatchAck',
  FlowControl = 'FlowControl',
  AgentStatus = 'AgentStatus',
  Ping = 'Ping',
  Pong = 'Pong',
  Goodbye = 'Goodbye',
  Subscribe = 'Subscribe',
  TailRecord = 'TailRecord',
  LiveQuery = 'LiveQuery',
  LiveBatch = 'LiveBatch',
  TailSample = 'TailSample',
  Error = 'Error',
}

export class FrameEncoder extends BitStreamEncoder {
  private compressionDict: Map<string, number> = new Map();

  constructor() {
    super("msb_first");
  }

  encode(value: FrameInput): Uint8Array {
    // Reset compression dictionary for each encode
    this.compressionDict.clear();

    if (value.msg.type === 'Hello') {
      const encoder_value = new HelloEncoder();
      const encoded_value = encoder_value.encode(value.msg.value);
      for (const byte of encoded_value) {
        this.writeUint8(byte);
      }
    }
    else if (value.msg.type === 'HelloAck') {
      const encoder_value = new HelloAckEncoder();
      const encoded_value = encoder_value.encode(value.msg.value);
      for (const byte of encoded_value) {
        this.writeUint8(byte);
      }
    }
    else if (value.msg.type === 'Batch') {
      const encoder_value = new BatchEncoder();
      const encoded_value = encoder_value.encode(value.msg.value);
      for (const byte of encoded_value) {
        this.writeUint8(byte);
      }
    }
    else if (value.msg.type === 'BatchAck') {
      const encoder_value = new BatchAckEncoder();
      const encoded_value = encoder_value.encode(value.msg.value);
      for (const byte of encoded_value) {
        this.writeUint8(byte);
      }
    }
    else if (value.msg.type === 'FlowControl') {
      const encoder_value = new FlowControlEncoder();
      const encoded_value = encoder_value.encode(value.msg.value);
      for (const byte of encoded_value) {
        this.writeUint8(byte);
      }
    }
    else if (value.msg.type === 'AgentStatus') {
      const encoder_value = new AgentStatusEncoder();
      const encoded_value = encoder_value.encode(value.msg.value);
      for (const byte of encoded_value) {
        this.writeUint8(byte);
      }
    }
    else if (value.msg.type === 'Ping') {
      const encoder_value = new PingEncoder();
      const encoded_value = encoder_value.encode(value.msg.value);
      for (const byte of encoded_value) {
        this.writeUint8(byte);
      }
    }
    else if (value.msg.type === 'Pong') {
      const encoder_value = new PongEncoder();
      const encoded_value = encoder_value.encode(value.msg.value);
      for (const byte of encoded_value) {
        this.writeUint8(byte);
      }
    }
    else if (value.msg.type === 'Goodbye') {
      const encoder_value = new GoodbyeEncoder();
      const encoded_value = encoder_value.encode(value.msg.value);
      for (const byte of encoded_value) {
        this.writeUint8(byte);
      }
    }
    else if (value.msg.type === 'Subscribe') {
      const encoder_value = new SubscribeEncoder();
      const encoded_value = encoder_value.encode(value.msg.value);
      for (const byte of encoded_value) {
        this.writeUint8(byte);
      }
    }
    else if (value.msg.type === 'TailRecord') {
      const encoder_value = new TailRecordEncoder();
      const encoded_value = encoder_value.encode(value.msg.value);
      for (const byte of encoded_value) {
        this.writeUint8(byte);
      }
    }
    else if (value.msg.type === 'LiveQuery') {
      const encoder_value = new LiveQueryEncoder();
      const encoded_value = encoder_value.encode(value.msg.value);
      for (const byte of encoded_value) {
        this.writeUint8(byte);
      }
    }
    else if (value.msg.type === 'LiveBatch') {
      const encoder_value = new LiveBatchEncoder();
      const encoded_value = encoder_value.encode(value.msg.value);
      for (const byte of encoded_value) {
        this.writeUint8(byte);
      }
    }
    else if (value.msg.type === 'TailSample') {
      const encoder_value = new TailSampleEncoder();
      const encoded_value = encoder_value.encode(value.msg.value);
      for (const byte of encoded_value) {
        this.writeUint8(byte);
      }
    }
    else if (value.msg.type === 'Error') {
      const encoder_value = new Error_Encoder();
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
   * Calculate the encoded size of a Frame value.
   * Used for from_after_field computed lengths and buffer pre-allocation.
   */
  calculateSize(value: Frame): number {
    let size = 0;
    if (value.msg.type === 'Hello') {
      const _enc = new HelloEncoder();
      size += _enc.calculateSize(value.msg.value);
    }
    else if (value.msg.type === 'HelloAck') {
      const _enc = new HelloAckEncoder();
      size += _enc.calculateSize(value.msg.value);
    }
    else if (value.msg.type === 'Batch') {
      const _enc = new BatchEncoder();
      size += _enc.calculateSize(value.msg.value);
    }
    else if (value.msg.type === 'BatchAck') {
      const _enc = new BatchAckEncoder();
      size += _enc.calculateSize(value.msg.value);
    }
    else if (value.msg.type === 'FlowControl') {
      const _enc = new FlowControlEncoder();
      size += _enc.calculateSize(value.msg.value);
    }
    else if (value.msg.type === 'AgentStatus') {
      const _enc = new AgentStatusEncoder();
      size += _enc.calculateSize(value.msg.value);
    }
    else if (value.msg.type === 'Ping') {
      const _enc = new PingEncoder();
      size += _enc.calculateSize(value.msg.value);
    }
    else if (value.msg.type === 'Pong') {
      const _enc = new PongEncoder();
      size += _enc.calculateSize(value.msg.value);
    }
    else if (value.msg.type === 'Goodbye') {
      const _enc = new GoodbyeEncoder();
      size += _enc.calculateSize(value.msg.value);
    }
    else if (value.msg.type === 'Subscribe') {
      const _enc = new SubscribeEncoder();
      size += _enc.calculateSize(value.msg.value);
    }
    else if (value.msg.type === 'TailRecord') {
      const _enc = new TailRecordEncoder();
      size += _enc.calculateSize(value.msg.value);
    }
    else if (value.msg.type === 'LiveQuery') {
      const _enc = new LiveQueryEncoder();
      size += _enc.calculateSize(value.msg.value);
    }
    else if (value.msg.type === 'LiveBatch') {
      const _enc = new LiveBatchEncoder();
      size += _enc.calculateSize(value.msg.value);
    }
    else if (value.msg.type === 'TailSample') {
      const _enc = new TailSampleEncoder();
      size += _enc.calculateSize(value.msg.value);
    }
    else if (value.msg.type === 'Error') {
      const _enc = new Error_Encoder();
      size += _enc.calculateSize(value.msg.value);
    }
    else {
      throw new BinSchemaError(ErrorCode.INVALID_VARIANT, `Unknown variant type for msg: ${(value.msg as any).type}`);
    }
    return size;
  }
}

export class FrameDecoder extends SeekableBitStreamDecoder {
  constructor(input: Uint8Array | number[] | string, private context?: any) {
    const reader = createReader(input);
    super(reader, "msb_first");
  }

  decode(): FrameOutput {
    const value: any = {};

    const discriminator = this.peekUint8();
    if (discriminator === 0x01) {
      const decoder = new HelloDecoder(this.bytes.slice(this.byteOffset), value);
      const decodedValue = decoder.decode();
      this.byteOffset += decoder.byteOffset;
      value.msg = { type: 'Hello', value: decodedValue };
    }
    else if (discriminator === 0x02) {
      const decoder = new HelloAckDecoder(this.bytes.slice(this.byteOffset), value);
      const decodedValue = decoder.decode();
      this.byteOffset += decoder.byteOffset;
      value.msg = { type: 'HelloAck', value: decodedValue };
    }
    else if (discriminator === 0x10) {
      const decoder = new BatchDecoder(this.bytes.slice(this.byteOffset), value);
      const decodedValue = decoder.decode();
      this.byteOffset += decoder.byteOffset;
      value.msg = { type: 'Batch', value: decodedValue };
    }
    else if (discriminator === 0x11) {
      const decoder = new BatchAckDecoder(this.bytes.slice(this.byteOffset), value);
      const decodedValue = decoder.decode();
      this.byteOffset += decoder.byteOffset;
      value.msg = { type: 'BatchAck', value: decodedValue };
    }
    else if (discriminator === 0x20) {
      const decoder = new FlowControlDecoder(this.bytes.slice(this.byteOffset), value);
      const decodedValue = decoder.decode();
      this.byteOffset += decoder.byteOffset;
      value.msg = { type: 'FlowControl', value: decodedValue };
    }
    else if (discriminator === 0x21) {
      const decoder = new AgentStatusDecoder(this.bytes.slice(this.byteOffset), value);
      const decodedValue = decoder.decode();
      this.byteOffset += decoder.byteOffset;
      value.msg = { type: 'AgentStatus', value: decodedValue };
    }
    else if (discriminator === 0x30) {
      const decoder = new PingDecoder(this.bytes.slice(this.byteOffset), value);
      const decodedValue = decoder.decode();
      this.byteOffset += decoder.byteOffset;
      value.msg = { type: 'Ping', value: decodedValue };
    }
    else if (discriminator === 0x31) {
      const decoder = new PongDecoder(this.bytes.slice(this.byteOffset), value);
      const decodedValue = decoder.decode();
      this.byteOffset += decoder.byteOffset;
      value.msg = { type: 'Pong', value: decodedValue };
    }
    else if (discriminator === 0x40) {
      const decoder = new GoodbyeDecoder(this.bytes.slice(this.byteOffset), value);
      const decodedValue = decoder.decode();
      this.byteOffset += decoder.byteOffset;
      value.msg = { type: 'Goodbye', value: decodedValue };
    }
    else if (discriminator === 0x50) {
      const decoder = new SubscribeDecoder(this.bytes.slice(this.byteOffset), value);
      const decodedValue = decoder.decode();
      this.byteOffset += decoder.byteOffset;
      value.msg = { type: 'Subscribe', value: decodedValue };
    }
    else if (discriminator === 0x51) {
      const decoder = new TailRecordDecoder(this.bytes.slice(this.byteOffset), value);
      const decodedValue = decoder.decode();
      this.byteOffset += decoder.byteOffset;
      value.msg = { type: 'TailRecord', value: decodedValue };
    }
    else if (discriminator === 0x52) {
      const decoder = new LiveQueryDecoder(this.bytes.slice(this.byteOffset), value);
      const decodedValue = decoder.decode();
      this.byteOffset += decoder.byteOffset;
      value.msg = { type: 'LiveQuery', value: decodedValue };
    }
    else if (discriminator === 0x53) {
      const decoder = new LiveBatchDecoder(this.bytes.slice(this.byteOffset), value);
      const decodedValue = decoder.decode();
      this.byteOffset += decoder.byteOffset;
      value.msg = { type: 'LiveBatch', value: decodedValue };
    }
    else if (discriminator === 0x54) {
      const decoder = new TailSampleDecoder(this.bytes.slice(this.byteOffset), value);
      const decodedValue = decoder.decode();
      this.byteOffset += decoder.byteOffset;
      value.msg = { type: 'TailSample', value: decodedValue };
    }
    else if (discriminator === 0xF0) {
      const decoder = new Error_Decoder(this.bytes.slice(this.byteOffset), value);
      const decodedValue = decoder.decode();
      this.byteOffset += decoder.byteOffset;
      value.msg = { type: 'Error', value: decodedValue };
    } else {
      throw new BinSchemaError(ErrorCode.INVALID_VARIANT, `Unknown discriminator: 0x${discriminator.toString(16)}`);
    }
    return value;
  }
}

/**
 * First message from agent. Identifies agent, advertises capabilities, and lists the signals it intends to send. Sent exactly once at connection open.
 */
export interface HelloInput {
  /**
   * 16-bit Unsigned Integer
   * Fixed-width 16-bit unsigned integer (0-65535). Respects endianness configuration (big-endian or little-endian).
   */
  protocol_version: number;
  /**
   * Bytes
   * Raw byte array. Sugar for array of uint8 — same wire format, simpler schema definition.
   */
  agent_id: number[];
  /**
   * String kind: length_prefixed
   * Encoding: ascii
   * Length prefix type: uint8
   */
  agent_version: string;
  /**
   * String kind: length_prefixed
   * Encoding: utf8
   * Length prefix type: uint8
   */
  hostname: string;
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  signals: number;
  /**
   * 32-bit Unsigned Integer
   * Fixed-width 32-bit unsigned integer (0-4294967295). Respects endianness configuration.
   */
  capabilities: number;
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint16
   */
  resource_attrs: LabelPairInput[];
}

/**
 * First message from agent. Identifies agent, advertises capabilities, and lists the signals it intends to send. Sent exactly once at connection open.
 */
export interface HelloOutput {
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  tag: number;
  /**
   * 16-bit Unsigned Integer
   * Fixed-width 16-bit unsigned integer (0-65535). Respects endianness configuration (big-endian or little-endian).
   */
  protocol_version: number;
  /**
   * Bytes
   * Raw byte array. Sugar for array of uint8 — same wire format, simpler schema definition.
   */
  agent_id: number[];
  /**
   * String kind: length_prefixed
   * Encoding: ascii
   * Length prefix type: uint8
   */
  agent_version: string;
  /**
   * String kind: length_prefixed
   * Encoding: utf8
   * Length prefix type: uint8
   */
  hostname: string;
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  signals: number;
  /**
   * 32-bit Unsigned Integer
   * Fixed-width 32-bit unsigned integer (0-4294967295). Respects endianness configuration.
   */
  capabilities: number;
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint16
   */
  resource_attrs: LabelPairOutput[];
}

export type Hello = HelloOutput;

export class HelloEncoder extends BitStreamEncoder {
  private compressionDict: Map<string, number> = new Map();

  constructor() {
    super("msb_first");
  }

  encode(value: HelloInput): Uint8Array {
    // Reset compression dictionary for each encode
    this.compressionDict.clear();

    this.writeUint8(1);
    this.writeUint16(value.protocol_version, "big_endian");
    // Validate fixed-length array
    if (value.agent_id.length !== 16) {
      throw new Error(`Array 'agent_id' must have exactly 16 elements, got ${value.agent_id.length}`);
    }
    for (let value_agent_id__iter_index = 0; value_agent_id__iter_index < value.agent_id.length; value_agent_id__iter_index++) {
      const value_agent_id__iter = value.agent_id[value_agent_id__iter_index];
      this.writeUint8(value_agent_id__iter);
    }
    const value_agent_version_bytes = Array.from(value.agent_version, c => c.charCodeAt(0));
    this.writeUint8(value_agent_version_bytes.length);
    for (const byte of value_agent_version_bytes) {
      this.writeUint8(byte);
    }
    const value_hostname_bytes = new TextEncoder().encode(value.hostname);
    this.writeUint8(value_hostname_bytes.length);
    for (const byte of value_hostname_bytes) {
      this.writeUint8(byte);
    }
    this.writeUint8(value.signals);
    this.writeUint32(value.capabilities, "big_endian");
    this.writeUint16(value.resource_attrs.length, "big_endian");
    for (let value_resource_attrs__iter_index = 0; value_resource_attrs__iter_index < value.resource_attrs.length; value_resource_attrs__iter_index++) {
      const value_resource_attrs__iter = value.resource_attrs[value_resource_attrs__iter_index];
      const encoder_value_resource_attrs__iter = new LabelPairEncoder();
      const encoded_value_resource_attrs__iter = encoder_value_resource_attrs__iter.encode(value_resource_attrs__iter);
      for (const byte of encoded_value_resource_attrs__iter) {
        this.writeUint8(byte);
      }
    }
    return this.finish();
  }

  /**
   * Calculate the encoded size of a Hello value.
   * Used for from_after_field computed lengths and buffer pre-allocation.
   */
  calculateSize(value: Hello): number {
    let size = 0;
    size += 3; // tag (const) + protocol_version
    // agent_id: bytes (kind: fixed)
    size += value.agent_id.length;
    // agent_version: string (ascii)
    size += value.agent_version.length;
    // hostname: string (utf8)
    size += new TextEncoder().encode(value.hostname).length;
    size += 5; // signals + capabilities
    // resource_attrs: array (kind: length_prefixed)
    for (const item of value.resource_attrs) {
      const resource_attrs_itemEncoder = new LabelPairEncoder();
      size += resource_attrs_itemEncoder.calculateSize(item);
    }
    size += 2; // length prefix (uint16)
    return size;
  }
}

export class HelloDecoder extends SeekableBitStreamDecoder {
  constructor(input: Uint8Array | number[] | string, private context?: any) {
    const reader = createReader(input);
    super(reader, "msb_first");
  }

  decode(): HelloOutput {
    const value: any = {};

    value.tag = this.readUint8();
    value.protocol_version = this.readUint16("big_endian");
    value.agent_id = [];
    for (let i = 0; i < 16; i++) {
      let agent_id__iter: any;
      agent_id__iter = this.readUint8();
      value.agent_id.push(agent_id__iter);
    }
    const agent_version_length = this.readUint8();
    const agent_version_bytes = this.readBytesSlice(agent_version_length);
    value.agent_version = String.fromCharCode(...agent_version_bytes);
    const hostname_length = this.readUint8();
    const hostname_bytes = this.readBytesSlice(hostname_length);
    try {
      value.hostname = new TextDecoder("utf-8", { fatal: true }).decode(hostname_bytes);
    } catch (e) {
      throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
    }
    value.signals = this.readUint8();
    value.capabilities = this.readUint32("big_endian");
    value.resource_attrs = [];
    const resource_attrs_length = this.readUint16("big_endian");
    for (let i = 0; i < resource_attrs_length; i++) {
      let resource_attrs__iter: any;
      resource_attrs__iter = {};
      const resource_attrs__iter_key_length = this.readUint8();
      const resource_attrs__iter_key_bytes = this.readBytesSlice(resource_attrs__iter_key_length);
      try {
        resource_attrs__iter.key = new TextDecoder("utf-8", { fatal: true }).decode(resource_attrs__iter_key_bytes);
      } catch (e) {
        throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
      }
      const resource_attrs__iter_value_length = this.readUint16("big_endian");
      const resource_attrs__iter_value_bytes = this.readBytesSlice(resource_attrs__iter_value_length);
      try {
        resource_attrs__iter.value = new TextDecoder("utf-8", { fatal: true }).decode(resource_attrs__iter_value_bytes);
      } catch (e) {
        throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
      }
      value.resource_attrs.push(resource_attrs__iter);
    }
    return value;
  }
}

/**
 * Server's response to Hello. Confirms protocol version, returns the server's writer_id and a session_id valid for the lifetime of this connection.
 */
export interface HelloAckInput {
  /**
   * 16-bit Unsigned Integer
   * Fixed-width 16-bit unsigned integer (0-65535). Respects endianness configuration (big-endian or little-endian).
   */
  protocol_version: number;
  /**
   * String kind: length_prefixed
   * Encoding: ascii
   * Length prefix type: uint8
   */
  writer_id: string;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  session_id: bigint;
  /**
   * 32-bit Unsigned Integer
   * Fixed-width 32-bit unsigned integer (0-4294967295). Respects endianness configuration.
   */
  capabilities: number;
  /**
   * 32-bit Unsigned Integer
   * Fixed-width 32-bit unsigned integer (0-4294967295). Respects endianness configuration.
   */
  suggested_batch_bytes: number;
  /**
   * 32-bit Unsigned Integer
   * Fixed-width 32-bit unsigned integer (0-4294967295). Respects endianness configuration.
   */
  max_batch_bytes: number;
  /**
   * 16-bit Unsigned Integer
   * Fixed-width 16-bit unsigned integer (0-65535). Respects endianness configuration (big-endian or little-endian).
   */
  max_inflight_batches: number;
}

/**
 * Server's response to Hello. Confirms protocol version, returns the server's writer_id and a session_id valid for the lifetime of this connection.
 */
export interface HelloAckOutput {
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  tag: number;
  /**
   * 16-bit Unsigned Integer
   * Fixed-width 16-bit unsigned integer (0-65535). Respects endianness configuration (big-endian or little-endian).
   */
  protocol_version: number;
  /**
   * String kind: length_prefixed
   * Encoding: ascii
   * Length prefix type: uint8
   */
  writer_id: string;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  session_id: bigint;
  /**
   * 32-bit Unsigned Integer
   * Fixed-width 32-bit unsigned integer (0-4294967295). Respects endianness configuration.
   */
  capabilities: number;
  /**
   * 32-bit Unsigned Integer
   * Fixed-width 32-bit unsigned integer (0-4294967295). Respects endianness configuration.
   */
  suggested_batch_bytes: number;
  /**
   * 32-bit Unsigned Integer
   * Fixed-width 32-bit unsigned integer (0-4294967295). Respects endianness configuration.
   */
  max_batch_bytes: number;
  /**
   * 16-bit Unsigned Integer
   * Fixed-width 16-bit unsigned integer (0-65535). Respects endianness configuration (big-endian or little-endian).
   */
  max_inflight_batches: number;
}

export type HelloAck = HelloAckOutput;

export class HelloAckEncoder extends BitStreamEncoder {
  private compressionDict: Map<string, number> = new Map();

  constructor() {
    super("msb_first");
  }

  encode(value: HelloAckInput): Uint8Array {
    // Reset compression dictionary for each encode
    this.compressionDict.clear();

    this.writeUint8(2);
    this.writeUint16(value.protocol_version, "big_endian");
    const value_writer_id_bytes = Array.from(value.writer_id, c => c.charCodeAt(0));
    this.writeUint8(value_writer_id_bytes.length);
    for (const byte of value_writer_id_bytes) {
      this.writeUint8(byte);
    }
    this.writeUint64(value.session_id, "big_endian");
    this.writeUint32(value.capabilities, "big_endian");
    this.writeUint32(value.suggested_batch_bytes, "big_endian");
    this.writeUint32(value.max_batch_bytes, "big_endian");
    this.writeUint16(value.max_inflight_batches, "big_endian");
    return this.finish();
  }

  /**
   * Calculate the encoded size of a HelloAck value.
   * Used for from_after_field computed lengths and buffer pre-allocation.
   */
  calculateSize(value: HelloAck): number {
    let size = 0;
    size += 3; // tag (const) + protocol_version
    // writer_id: string (ascii)
    size += value.writer_id.length;
    size += 22; // session_id + capabilities + suggested_batch_bytes + max_batch_bytes + max_inflight_batches
    return size;
  }
}

export class HelloAckDecoder extends SeekableBitStreamDecoder {
  constructor(input: Uint8Array | number[] | string, private context?: any) {
    const reader = createReader(input);
    super(reader, "msb_first");
  }

  decode(): HelloAckOutput {
    const value: any = {};

    value.tag = this.readUint8();
    value.protocol_version = this.readUint16("big_endian");
    const writer_id_length = this.readUint8();
    const writer_id_bytes = this.readBytesSlice(writer_id_length);
    value.writer_id = String.fromCharCode(...writer_id_bytes);
    value.session_id = this.readUint64("big_endian");
    value.capabilities = this.readUint32("big_endian");
    value.suggested_batch_bytes = this.readUint32("big_endian");
    value.max_batch_bytes = this.readUint32("big_endian");
    value.max_inflight_batches = this.readUint16("big_endian");
    return value;
  }
}

/**
 * A compressed, signal-specific batch of records. The payload, after decompression, is a SignalBatch variant.
 */
export interface BatchInput {
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  session_id: bigint;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  batch_id: bigint;
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  signal: number;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  ts_min_unix_nano: bigint;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  ts_max_unix_nano: bigint;
  /**
   * 32-bit Unsigned Integer
   * Fixed-width 32-bit unsigned integer (0-4294967295). Respects endianness configuration.
   */
  record_count: number;
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  compression: number;
  /**
   * 32-bit Unsigned Integer
   * Fixed-width 32-bit unsigned integer (0-4294967295). Respects endianness configuration.
   */
  uncompressed_size: number;
  /**
   * Bytes
   * Raw byte array. Sugar for array of uint8 — same wire format, simpler schema definition.
   */
  payload: number[];
}

/**
 * A compressed, signal-specific batch of records. The payload, after decompression, is a SignalBatch variant.
 */
export interface BatchOutput {
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  tag: number;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  session_id: bigint;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  batch_id: bigint;
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  signal: number;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  ts_min_unix_nano: bigint;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  ts_max_unix_nano: bigint;
  /**
   * 32-bit Unsigned Integer
   * Fixed-width 32-bit unsigned integer (0-4294967295). Respects endianness configuration.
   */
  record_count: number;
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  compression: number;
  /**
   * 32-bit Unsigned Integer
   * Fixed-width 32-bit unsigned integer (0-4294967295). Respects endianness configuration.
   */
  uncompressed_size: number;
  /**
   * Bytes
   * Raw byte array. Sugar for array of uint8 — same wire format, simpler schema definition.
   */
  payload: number[];
}

export type Batch = BatchOutput;

export class BatchEncoder extends BitStreamEncoder {
  private compressionDict: Map<string, number> = new Map();

  constructor() {
    super("msb_first");
  }

  encode(value: BatchInput): Uint8Array {
    // Reset compression dictionary for each encode
    this.compressionDict.clear();

    this.writeUint8(16);
    this.writeUint64(value.session_id, "big_endian");
    this.writeUint64(value.batch_id, "big_endian");
    this.writeUint8(value.signal);
    this.writeUint64(value.ts_min_unix_nano, "big_endian");
    this.writeUint64(value.ts_max_unix_nano, "big_endian");
    this.writeUint32(value.record_count, "big_endian");
    this.writeUint8(value.compression);
    this.writeUint32(value.uncompressed_size, "big_endian");
    this.writeUint32(value.payload.length, "big_endian");
    for (let value_payload__iter_index = 0; value_payload__iter_index < value.payload.length; value_payload__iter_index++) {
      const value_payload__iter = value.payload[value_payload__iter_index];
      this.writeUint8(value_payload__iter);
    }
    return this.finish();
  }

  /**
   * Calculate the encoded size of a Batch value.
   * Used for from_after_field computed lengths and buffer pre-allocation.
   */
  calculateSize(value: Batch): number {
    let size = 0;
    size += 43; // tag (const) + session_id + batch_id + signal + ts_min_unix_nano + ts_max_unix_nano + record_count + compression + uncompressed_size
    // payload: bytes (kind: length_prefixed)
    size += value.payload.length;
    size += 4; // length prefix (uint32)
    return size;
  }
}

export class BatchDecoder extends SeekableBitStreamDecoder {
  constructor(input: Uint8Array | number[] | string, private context?: any) {
    const reader = createReader(input);
    super(reader, "msb_first");
  }

  decode(): BatchOutput {
    const value: any = {};

    value.tag = this.readUint8();
    value.session_id = this.readUint64("big_endian");
    value.batch_id = this.readUint64("big_endian");
    value.signal = this.readUint8();
    value.ts_min_unix_nano = this.readUint64("big_endian");
    value.ts_max_unix_nano = this.readUint64("big_endian");
    value.record_count = this.readUint32("big_endian");
    value.compression = this.readUint8();
    value.uncompressed_size = this.readUint32("big_endian");
    value.payload = [];
    const payload_length = this.readUint32("big_endian");
    for (let i = 0; i < payload_length; i++) {
      let payload__iter: any;
      payload__iter = this.readUint8();
      value.payload.push(payload__iter);
    }
    return value;
  }
}

/**
 * Server's response per Batch. Either accepted (durable in WAL), throttled (slow down before retrying), or rejected (do not retry).
 */
export interface BatchAckInput {
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  session_id: bigint;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  batch_id: bigint;
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  status: number;
  /**
   * 32-bit Unsigned Integer
   * Fixed-width 32-bit unsigned integer (0-4294967295). Respects endianness configuration.
   */
  retry_after_ms: number;
  /**
   * 16-bit Unsigned Integer
   * Fixed-width 16-bit unsigned integer (0-65535). Respects endianness configuration (big-endian or little-endian).
   */
  reason_code: number;
  /**
   * String kind: length_prefixed
   * Encoding: utf8
   * Length prefix type: uint16
   */
  message: string;
}

/**
 * Server's response per Batch. Either accepted (durable in WAL), throttled (slow down before retrying), or rejected (do not retry).
 */
export interface BatchAckOutput {
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  tag: number;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  session_id: bigint;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  batch_id: bigint;
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  status: number;
  /**
   * 32-bit Unsigned Integer
   * Fixed-width 32-bit unsigned integer (0-4294967295). Respects endianness configuration.
   */
  retry_after_ms: number;
  /**
   * 16-bit Unsigned Integer
   * Fixed-width 16-bit unsigned integer (0-65535). Respects endianness configuration (big-endian or little-endian).
   */
  reason_code: number;
  /**
   * String kind: length_prefixed
   * Encoding: utf8
   * Length prefix type: uint16
   */
  message: string;
}

export type BatchAck = BatchAckOutput;

export class BatchAckEncoder extends BitStreamEncoder {
  private compressionDict: Map<string, number> = new Map();

  constructor() {
    super("msb_first");
  }

  encode(value: BatchAckInput): Uint8Array {
    // Reset compression dictionary for each encode
    this.compressionDict.clear();

    this.writeUint8(17);
    this.writeUint64(value.session_id, "big_endian");
    this.writeUint64(value.batch_id, "big_endian");
    this.writeUint8(value.status);
    this.writeUint32(value.retry_after_ms, "big_endian");
    this.writeUint16(value.reason_code, "big_endian");
    const value_message_bytes = new TextEncoder().encode(value.message);
    this.writeUint16(value_message_bytes.length, "big_endian");
    for (const byte of value_message_bytes) {
      this.writeUint8(byte);
    }
    return this.finish();
  }

  /**
   * Calculate the encoded size of a BatchAck value.
   * Used for from_after_field computed lengths and buffer pre-allocation.
   */
  calculateSize(value: BatchAck): number {
    let size = 0;
    size += 24; // tag (const) + session_id + batch_id + status + retry_after_ms + reason_code
    // message: string (utf8)
    size += new TextEncoder().encode(value.message).length;
    return size;
  }
}

export class BatchAckDecoder extends SeekableBitStreamDecoder {
  constructor(input: Uint8Array | number[] | string, private context?: any) {
    const reader = createReader(input);
    super(reader, "msb_first");
  }

  decode(): BatchAckOutput {
    const value: any = {};

    value.tag = this.readUint8();
    value.session_id = this.readUint64("big_endian");
    value.batch_id = this.readUint64("big_endian");
    value.status = this.readUint8();
    value.retry_after_ms = this.readUint32("big_endian");
    value.reason_code = this.readUint16("big_endian");
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

/**
 * Asynchronous server-to-agent rate hint, per signal.
 */
export interface FlowControlInput {
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  session_id: bigint;
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  signal: number;
  /**
   * 32-bit Unsigned Integer
   * Fixed-width 32-bit unsigned integer (0-4294967295). Respects endianness configuration.
   */
  max_bytes_per_sec: number;
  /**
   * 16-bit Unsigned Integer
   * Fixed-width 16-bit unsigned integer (0-65535). Respects endianness configuration (big-endian or little-endian).
   */
  max_batches_inflight: number;
  /**
   * 32-bit Unsigned Integer
   * Fixed-width 32-bit unsigned integer (0-4294967295). Respects endianness configuration.
   */
  valid_for_ms: number;
}

/**
 * Asynchronous server-to-agent rate hint, per signal.
 */
export interface FlowControlOutput {
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  tag: number;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  session_id: bigint;
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  signal: number;
  /**
   * 32-bit Unsigned Integer
   * Fixed-width 32-bit unsigned integer (0-4294967295). Respects endianness configuration.
   */
  max_bytes_per_sec: number;
  /**
   * 16-bit Unsigned Integer
   * Fixed-width 16-bit unsigned integer (0-65535). Respects endianness configuration (big-endian or little-endian).
   */
  max_batches_inflight: number;
  /**
   * 32-bit Unsigned Integer
   * Fixed-width 32-bit unsigned integer (0-4294967295). Respects endianness configuration.
   */
  valid_for_ms: number;
}

export type FlowControl = FlowControlOutput;

export class FlowControlEncoder extends BitStreamEncoder {
  private compressionDict: Map<string, number> = new Map();

  constructor() {
    super("msb_first");
  }

  encode(value: FlowControlInput): Uint8Array {
    // Reset compression dictionary for each encode
    this.compressionDict.clear();

    this.writeUint8(32);
    this.writeUint64(value.session_id, "big_endian");
    this.writeUint8(value.signal);
    this.writeUint32(value.max_bytes_per_sec, "big_endian");
    this.writeUint16(value.max_batches_inflight, "big_endian");
    this.writeUint32(value.valid_for_ms, "big_endian");
    return this.finish();
  }

  /**
   * Calculate the encoded size of a FlowControl value.
   * Used for from_after_field computed lengths and buffer pre-allocation.
   */
  calculateSize(value: FlowControl): number {
    return 20; // tag (const) + session_id + signal + max_bytes_per_sec + max_batches_inflight + valid_for_ms
  }
}

export class FlowControlDecoder extends SeekableBitStreamDecoder {
  constructor(input: Uint8Array | number[] | string, private context?: any) {
    const reader = createReader(input);
    super(reader, "msb_first");
  }

  decode(): FlowControlOutput {
    const value: any = {};

    value.tag = this.readUint8();
    value.session_id = this.readUint64("big_endian");
    value.signal = this.readUint8();
    value.max_bytes_per_sec = this.readUint32("big_endian");
    value.max_batches_inflight = this.readUint16("big_endian");
    value.valid_for_ms = this.readUint32("big_endian");
    return value;
  }
}

/**
 * Best-effort agent-to-ingester operational status. Sent only when HelloAck advertises CAP_AGENT_STATUS. The server validates the live session, canonicalizes the JSON StatusSnapshot against Hello identity, and relays it to the fleet registry without acknowledging it.
 */
export interface AgentStatusInput {
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  session_id: bigint;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  sequence: bigint;
  /**
   * String kind: length_prefixed
   * Encoding: utf8
   * Length prefix type: uint32
   */
  snapshot_json: string;
}

/**
 * Best-effort agent-to-ingester operational status. Sent only when HelloAck advertises CAP_AGENT_STATUS. The server validates the live session, canonicalizes the JSON StatusSnapshot against Hello identity, and relays it to the fleet registry without acknowledging it.
 */
export interface AgentStatusOutput {
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  tag: number;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  session_id: bigint;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  sequence: bigint;
  /**
   * String kind: length_prefixed
   * Encoding: utf8
   * Length prefix type: uint32
   */
  snapshot_json: string;
}

export type AgentStatus = AgentStatusOutput;

export class AgentStatusEncoder extends BitStreamEncoder {
  private compressionDict: Map<string, number> = new Map();

  constructor() {
    super("msb_first");
  }

  encode(value: AgentStatusInput): Uint8Array {
    // Reset compression dictionary for each encode
    this.compressionDict.clear();

    this.writeUint8(33);
    this.writeUint64(value.session_id, "big_endian");
    this.writeUint64(value.sequence, "big_endian");
    const value_snapshot_json_bytes = new TextEncoder().encode(value.snapshot_json);
    this.writeUint32(value_snapshot_json_bytes.length, "big_endian");
    for (const byte of value_snapshot_json_bytes) {
      this.writeUint8(byte);
    }
    return this.finish();
  }

  /**
   * Calculate the encoded size of a AgentStatus value.
   * Used for from_after_field computed lengths and buffer pre-allocation.
   */
  calculateSize(value: AgentStatus): number {
    let size = 0;
    size += 17; // tag (const) + session_id + sequence
    // snapshot_json: string (utf8)
    size += new TextEncoder().encode(value.snapshot_json).length;
    return size;
  }
}

export class AgentStatusDecoder extends SeekableBitStreamDecoder {
  constructor(input: Uint8Array | number[] | string, private context?: any) {
    const reader = createReader(input);
    super(reader, "msb_first");
  }

  decode(): AgentStatusOutput {
    const value: any = {};

    value.tag = this.readUint8();
    value.session_id = this.readUint64("big_endian");
    value.sequence = this.readUint64("big_endian");
    const snapshot_json_length = this.readUint32("big_endian");
    const snapshot_json_bytes = this.readBytesSlice(snapshot_json_length);
    try {
      value.snapshot_json = new TextDecoder("utf-8", { fatal: true }).decode(snapshot_json_bytes);
    } catch (e) {
      throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
    }
    return value;
  }
}

/**
 * Keepalive request. Either side may send.
 */
export interface PingInput {
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  nonce: bigint;
}

/**
 * Keepalive request. Either side may send.
 */
export interface PingOutput {
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  tag: number;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  nonce: bigint;
}

export type Ping = PingOutput;

export class PingEncoder extends BitStreamEncoder {
  private compressionDict: Map<string, number> = new Map();

  constructor() {
    super("msb_first");
  }

  encode(value: PingInput): Uint8Array {
    // Reset compression dictionary for each encode
    this.compressionDict.clear();

    this.writeUint8(48);
    this.writeUint64(value.nonce, "big_endian");
    return this.finish();
  }

  /**
   * Calculate the encoded size of a Ping value.
   * Used for from_after_field computed lengths and buffer pre-allocation.
   */
  calculateSize(value: Ping): number {
    return 9; // tag (const) + nonce
  }
}

export class PingDecoder extends SeekableBitStreamDecoder {
  constructor(input: Uint8Array | number[] | string, private context?: any) {
    const reader = createReader(input);
    super(reader, "msb_first");
  }

  decode(): PingOutput {
    const value: any = {};

    value.tag = this.readUint8();
    value.nonce = this.readUint64("big_endian");
    return value;
  }
}

/**
 * Keepalive response. Echoes the Ping nonce.
 */
export interface PongInput {
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  nonce: bigint;
}

/**
 * Keepalive response. Echoes the Ping nonce.
 */
export interface PongOutput {
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  tag: number;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  nonce: bigint;
}

export type Pong = PongOutput;

export class PongEncoder extends BitStreamEncoder {
  private compressionDict: Map<string, number> = new Map();

  constructor() {
    super("msb_first");
  }

  encode(value: PongInput): Uint8Array {
    // Reset compression dictionary for each encode
    this.compressionDict.clear();

    this.writeUint8(49);
    this.writeUint64(value.nonce, "big_endian");
    return this.finish();
  }

  /**
   * Calculate the encoded size of a Pong value.
   * Used for from_after_field computed lengths and buffer pre-allocation.
   */
  calculateSize(value: Pong): number {
    return 9; // tag (const) + nonce
  }
}

export class PongDecoder extends SeekableBitStreamDecoder {
  constructor(input: Uint8Array | number[] | string, private context?: any) {
    const reader = createReader(input);
    super(reader, "msb_first");
  }

  decode(): PongOutput {
    const value: any = {};

    value.tag = this.readUint8();
    value.nonce = this.readUint64("big_endian");
    return value;
  }
}

/**
 * Graceful close request. Either side may send.
 */
export interface GoodbyeInput {
  /**
   * 16-bit Unsigned Integer
   * Fixed-width 16-bit unsigned integer (0-65535). Respects endianness configuration (big-endian or little-endian).
   */
  reason_code: number;
  /**
   * String kind: length_prefixed
   * Encoding: utf8
   * Length prefix type: uint16
   */
  message: string;
}

/**
 * Graceful close request. Either side may send.
 */
export interface GoodbyeOutput {
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  tag: number;
  /**
   * 16-bit Unsigned Integer
   * Fixed-width 16-bit unsigned integer (0-65535). Respects endianness configuration (big-endian or little-endian).
   */
  reason_code: number;
  /**
   * String kind: length_prefixed
   * Encoding: utf8
   * Length prefix type: uint16
   */
  message: string;
}

export type Goodbye = GoodbyeOutput;

export class GoodbyeEncoder extends BitStreamEncoder {
  private compressionDict: Map<string, number> = new Map();

  constructor() {
    super("msb_first");
  }

  encode(value: GoodbyeInput): Uint8Array {
    // Reset compression dictionary for each encode
    this.compressionDict.clear();

    this.writeUint8(64);
    this.writeUint16(value.reason_code, "big_endian");
    const value_message_bytes = new TextEncoder().encode(value.message);
    this.writeUint16(value_message_bytes.length, "big_endian");
    for (const byte of value_message_bytes) {
      this.writeUint8(byte);
    }
    return this.finish();
  }

  /**
   * Calculate the encoded size of a Goodbye value.
   * Used for from_after_field computed lengths and buffer pre-allocation.
   */
  calculateSize(value: Goodbye): number {
    let size = 0;
    size += 3; // tag (const) + reason_code
    // message: string (utf8)
    size += new TextEncoder().encode(value.message).length;
    return size;
  }
}

export class GoodbyeDecoder extends SeekableBitStreamDecoder {
  constructor(input: Uint8Array | number[] | string, private context?: any) {
    const reader = createReader(input);
    super(reader, "msb_first");
  }

  decode(): GoodbyeOutput {
    const value: any = {};

    value.tag = this.readUint8();
    value.reason_code = this.readUint16("big_endian");
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

/**
 * Protocol-level error. Receiving an Error closes the connection.
 */
export interface Error_Input {
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
 * Protocol-level error. Receiving an Error closes the connection.
 */
export interface Error_Output {
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

export type Error_ = Error_Output;

export class Error_Encoder extends BitStreamEncoder {
  private compressionDict: Map<string, number> = new Map();

  constructor() {
    super("msb_first");
  }

  encode(value: Error_Input): Uint8Array {
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
   * Calculate the encoded size of a Error_ value.
   * Used for from_after_field computed lengths and buffer pre-allocation.
   */
  calculateSize(value: Error_): number {
    let size = 0;
    size += 3; // tag (const) + code
    // message: string (utf8)
    size += new TextEncoder().encode(value.message).length;
    return size;
  }
}

export class Error_Decoder extends SeekableBitStreamDecoder {
  constructor(input: Uint8Array | number[] | string, private context?: any) {
    const reader = createReader(input);
    super(reader, "msb_first");
  }

  decode(): Error_Output {
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

/**
 * Live-tail subscription request (scry tail, D-050). After the Hello handshake a tail client sends this to switch the connection into server-push mode: the server sprays matching records back down the same connection until the client closes it. The reply frame depends on `signal`: Logs (2) yields TailRecord, Metrics (1) yields TailSample (D-065). Best-effort — records may be dropped under load; there is no ack and no durability.
 */
export interface SubscribeInput {
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
  matchers: MatcherSpecInput[];
}

/**
 * Live-tail subscription request (scry tail, D-050). After the Hello handshake a tail client sends this to switch the connection into server-push mode: the server sprays matching records back down the same connection until the client closes it. The reply frame depends on `signal`: Logs (2) yields TailRecord, Metrics (1) yields TailSample (D-065). Best-effort — records may be dropped under load; there is no ack and no durability.
 */
export interface SubscribeOutput {
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
  matchers: MatcherSpecOutput[];
}

export type Subscribe = SubscribeOutput;

export class SubscribeEncoder extends BitStreamEncoder {
  private compressionDict: Map<string, number> = new Map();

  constructor() {
    super("msb_first");
  }

  encode(value: SubscribeInput): Uint8Array {
    // Reset compression dictionary for each encode
    this.compressionDict.clear();

    this.writeUint8(80);
    this.writeUint8(value.signal);
    this.writeUint16(value.matchers.length, "big_endian");
    for (let value_matchers__iter_index = 0; value_matchers__iter_index < value.matchers.length; value_matchers__iter_index++) {
      const value_matchers__iter = value.matchers[value_matchers__iter_index];
      const encoder_value_matchers__iter = new MatcherSpecEncoder();
      const encoded_value_matchers__iter = encoder_value_matchers__iter.encode(value_matchers__iter);
      for (const byte of encoded_value_matchers__iter) {
        this.writeUint8(byte);
      }
    }
    return this.finish();
  }

  /**
   * Calculate the encoded size of a Subscribe value.
   * Used for from_after_field computed lengths and buffer pre-allocation.
   */
  calculateSize(value: Subscribe): number {
    let size = 0;
    size += 2; // tag (const) + signal
    // matchers: array (kind: length_prefixed)
    for (const item of value.matchers) {
      const matchers_itemEncoder = new MatcherSpecEncoder();
      size += matchers_itemEncoder.calculateSize(item);
    }
    size += 2; // length prefix (uint16)
    return size;
  }
}

export class SubscribeDecoder extends SeekableBitStreamDecoder {
  constructor(input: Uint8Array | number[] | string, private context?: any) {
    const reader = createReader(input);
    super(reader, "msb_first");
  }

  decode(): SubscribeOutput {
    const value: any = {};

    value.tag = this.readUint8();
    value.signal = this.readUint8();
    value.matchers = [];
    const matchers_length = this.readUint16("big_endian");
    for (let i = 0; i < matchers_length; i++) {
      let matchers__iter: any;
      matchers__iter = {};
      const matchers__iter_spec_length = this.readUint16("big_endian");
      const matchers__iter_spec_bytes = this.readBytesSlice(matchers__iter_spec_length);
      try {
        matchers__iter.spec = new TextDecoder("utf-8", { fatal: true }).decode(matchers__iter_spec_bytes);
      } catch (e) {
        throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
      }
      value.matchers.push(matchers__iter);
    }
    return value;
  }
}

/**
 * One Prometheus-style label matcher spec string (key=value | key!=value | key=~regex | key!~regex), parsed server-side by scry-match. ANDed with the others in a Subscribe.
 */
export interface MatcherSpecInput {
  /**
   * String kind: length_prefixed
   * Encoding: utf8
   * Length prefix type: uint16
   */
  spec: string;
}

/**
 * One Prometheus-style label matcher spec string (key=value | key!=value | key=~regex | key!~regex), parsed server-side by scry-match. ANDed with the others in a Subscribe.
 */
export interface MatcherSpecOutput {
  /**
   * String kind: length_prefixed
   * Encoding: utf8
   * Length prefix type: uint16
   */
  spec: string;
}

export type MatcherSpec = MatcherSpecOutput;

export class MatcherSpecEncoder extends BitStreamEncoder {
  private compressionDict: Map<string, number> = new Map();

  constructor() {
    super("msb_first");
  }

  encode(value: MatcherSpecInput): Uint8Array {
    // Reset compression dictionary for each encode
    this.compressionDict.clear();

    const value_spec_bytes = new TextEncoder().encode(value.spec);
    this.writeUint16(value_spec_bytes.length, "big_endian");
    for (const byte of value_spec_bytes) {
      this.writeUint8(byte);
    }
    return this.finish();
  }

  /**
   * Calculate the encoded size of a MatcherSpec value.
   * Used for from_after_field computed lengths and buffer pre-allocation.
   */
  calculateSize(value: MatcherSpec): number {
    let size = 0;
    // spec: string (utf8)
    size += new TextEncoder().encode(value.spec).length;
    return size;
  }
}

export class MatcherSpecDecoder extends SeekableBitStreamDecoder {
  constructor(input: Uint8Array | number[] | string, private context?: any) {
    const reader = createReader(input);
    super(reader, "msb_first");
  }

  decode(): MatcherSpecOutput {
    const value: any = {};

    const spec_length = this.readUint16("big_endian");
    const spec_bytes = this.readBytesSlice(spec_length);
    try {
      value.spec = new TextDecoder("utf-8", { fatal: true }).decode(spec_bytes);
    } catch (e) {
      throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
    }
    return value;
  }
}

/**
 * Server-to-client push of one LOG record matching a live-tail Subscribe (signal = Logs): the stream labels, entry timestamp/severity/body, and per-entry attributes. Metrics use the sibling TailSample frame instead of widening this one, so a logs tail carries no bytes it does not use. Fire-and-forget; the client never acks.
 */
export interface TailRecordInput {
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  signal: number;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  ts_unix_nano: bigint;
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  severity: number;
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint16
   */
  labels: LabelPairInput[];
  /**
   * String kind: length_prefixed
   * Encoding: utf8
   * Length prefix type: uint32
   */
  body: string;
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint16
   */
  attributes: LabelPairInput[];
}

/**
 * Server-to-client push of one LOG record matching a live-tail Subscribe (signal = Logs): the stream labels, entry timestamp/severity/body, and per-entry attributes. Metrics use the sibling TailSample frame instead of widening this one, so a logs tail carries no bytes it does not use. Fire-and-forget; the client never acks.
 */
export interface TailRecordOutput {
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
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  ts_unix_nano: bigint;
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  severity: number;
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint16
   */
  labels: LabelPairOutput[];
  /**
   * String kind: length_prefixed
   * Encoding: utf8
   * Length prefix type: uint32
   */
  body: string;
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint16
   */
  attributes: LabelPairOutput[];
}

export type TailRecord = TailRecordOutput;

export class TailRecordEncoder extends BitStreamEncoder {
  private compressionDict: Map<string, number> = new Map();

  constructor() {
    super("msb_first");
  }

  encode(value: TailRecordInput): Uint8Array {
    // Reset compression dictionary for each encode
    this.compressionDict.clear();

    this.writeUint8(81);
    this.writeUint8(value.signal);
    this.writeUint64(value.ts_unix_nano, "big_endian");
    this.writeUint8(value.severity);
    this.writeUint16(value.labels.length, "big_endian");
    for (let value_labels__iter_index = 0; value_labels__iter_index < value.labels.length; value_labels__iter_index++) {
      const value_labels__iter = value.labels[value_labels__iter_index];
      const encoder_value_labels__iter = new LabelPairEncoder();
      const encoded_value_labels__iter = encoder_value_labels__iter.encode(value_labels__iter);
      for (const byte of encoded_value_labels__iter) {
        this.writeUint8(byte);
      }
    }
    const value_body_bytes = new TextEncoder().encode(value.body);
    this.writeUint32(value_body_bytes.length, "big_endian");
    for (const byte of value_body_bytes) {
      this.writeUint8(byte);
    }
    this.writeUint16(value.attributes.length, "big_endian");
    for (let value_attributes__iter_index = 0; value_attributes__iter_index < value.attributes.length; value_attributes__iter_index++) {
      const value_attributes__iter = value.attributes[value_attributes__iter_index];
      const encoder_value_attributes__iter = new LabelPairEncoder();
      const encoded_value_attributes__iter = encoder_value_attributes__iter.encode(value_attributes__iter);
      for (const byte of encoded_value_attributes__iter) {
        this.writeUint8(byte);
      }
    }
    return this.finish();
  }

  /**
   * Calculate the encoded size of a TailRecord value.
   * Used for from_after_field computed lengths and buffer pre-allocation.
   */
  calculateSize(value: TailRecord): number {
    let size = 0;
    size += 11; // tag (const) + signal + ts_unix_nano + severity
    // labels: array (kind: length_prefixed)
    for (const item of value.labels) {
      const labels_itemEncoder = new LabelPairEncoder();
      size += labels_itemEncoder.calculateSize(item);
    }
    size += 2; // length prefix (uint16)
    // body: string (utf8)
    size += new TextEncoder().encode(value.body).length;
    // attributes: array (kind: length_prefixed)
    for (const item of value.attributes) {
      const attributes_itemEncoder = new LabelPairEncoder();
      size += attributes_itemEncoder.calculateSize(item);
    }
    size += 2; // length prefix (uint16)
    return size;
  }
}

export class TailRecordDecoder extends SeekableBitStreamDecoder {
  constructor(input: Uint8Array | number[] | string, private context?: any) {
    const reader = createReader(input);
    super(reader, "msb_first");
  }

  decode(): TailRecordOutput {
    const value: any = {};

    value.tag = this.readUint8();
    value.signal = this.readUint8();
    value.ts_unix_nano = this.readUint64("big_endian");
    value.severity = this.readUint8();
    value.labels = [];
    const labels_length = this.readUint16("big_endian");
    for (let i = 0; i < labels_length; i++) {
      let labels__iter: any;
      labels__iter = {};
      const labels__iter_key_length = this.readUint8();
      const labels__iter_key_bytes = this.readBytesSlice(labels__iter_key_length);
      try {
        labels__iter.key = new TextDecoder("utf-8", { fatal: true }).decode(labels__iter_key_bytes);
      } catch (e) {
        throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
      }
      const labels__iter_value_length = this.readUint16("big_endian");
      const labels__iter_value_bytes = this.readBytesSlice(labels__iter_value_length);
      try {
        labels__iter.value = new TextDecoder("utf-8", { fatal: true }).decode(labels__iter_value_bytes);
      } catch (e) {
        throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
      }
      value.labels.push(labels__iter);
    }
    const body_length = this.readUint32("big_endian");
    const body_bytes = this.readBytesSlice(body_length);
    try {
      value.body = new TextDecoder("utf-8", { fatal: true }).decode(body_bytes);
    } catch (e) {
      throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
    }
    value.attributes = [];
    const attributes_length = this.readUint16("big_endian");
    for (let i = 0; i < attributes_length; i++) {
      let attributes__iter: any;
      attributes__iter = {};
      const attributes__iter_key_length = this.readUint8();
      const attributes__iter_key_bytes = this.readBytesSlice(attributes__iter_key_length);
      try {
        attributes__iter.key = new TextDecoder("utf-8", { fatal: true }).decode(attributes__iter_key_bytes);
      } catch (e) {
        throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
      }
      const attributes__iter_value_length = this.readUint16("big_endian");
      const attributes__iter_value_bytes = this.readBytesSlice(attributes__iter_value_length);
      try {
        attributes__iter.value = new TextDecoder("utf-8", { fatal: true }).decode(attributes__iter_value_bytes);
      } catch (e) {
        throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
      }
      value.attributes.push(attributes__iter);
    }
    return value;
  }
}

/**
 * Server-to-client push of one METRIC sample matching a live-tail Subscribe (signal = Metrics, D-065) — the TailRecord sibling for a signal whose record is a float, not a body. `series_fingerprint` is the server's own xxh3-64 over the sorted labels, carried so a client can line a live line up with a stored series (the UI chart groups on exactly this column) without re-deriving it. `metric_type` mirrors SeriesDictEntry.metric_type. `labels` repeats per sample, as it does per TailRecord: a tail is bounded by its drop-on-full channel, not by its encoding. Fire-and-forget; the client never acks.
 */
export interface TailSampleInput {
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  signal: number;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  ts_unix_nano: bigint;
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  metric_type: number;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  series_fingerprint: bigint;
  /**
   * 64-bit Floating Point
   * IEEE 754 double-precision floating point (64-bit). Provides ~15 decimal digits of precision.
   */
  value: number;
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint16
   */
  labels: LabelPairInput[];
}

/**
 * Server-to-client push of one METRIC sample matching a live-tail Subscribe (signal = Metrics, D-065) — the TailRecord sibling for a signal whose record is a float, not a body. `series_fingerprint` is the server's own xxh3-64 over the sorted labels, carried so a client can line a live line up with a stored series (the UI chart groups on exactly this column) without re-deriving it. `metric_type` mirrors SeriesDictEntry.metric_type. `labels` repeats per sample, as it does per TailRecord: a tail is bounded by its drop-on-full channel, not by its encoding. Fire-and-forget; the client never acks.
 */
export interface TailSampleOutput {
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
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  ts_unix_nano: bigint;
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  metric_type: number;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  series_fingerprint: bigint;
  /**
   * 64-bit Floating Point
   * IEEE 754 double-precision floating point (64-bit). Provides ~15 decimal digits of precision.
   */
  value: number;
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint16
   */
  labels: LabelPairOutput[];
}

export type TailSample = TailSampleOutput;

export class TailSampleEncoder extends BitStreamEncoder {
  private compressionDict: Map<string, number> = new Map();

  constructor() {
    super("msb_first");
  }

  encode(value: TailSampleInput): Uint8Array {
    // Reset compression dictionary for each encode
    this.compressionDict.clear();

    this.writeUint8(84);
    this.writeUint8(value.signal);
    this.writeUint64(value.ts_unix_nano, "big_endian");
    this.writeUint8(value.metric_type);
    this.writeUint64(value.series_fingerprint, "big_endian");
    this.writeFloat64(value.value, "big_endian");
    this.writeUint16(value.labels.length, "big_endian");
    for (let value_labels__iter_index = 0; value_labels__iter_index < value.labels.length; value_labels__iter_index++) {
      const value_labels__iter = value.labels[value_labels__iter_index];
      const encoder_value_labels__iter = new LabelPairEncoder();
      const encoded_value_labels__iter = encoder_value_labels__iter.encode(value_labels__iter);
      for (const byte of encoded_value_labels__iter) {
        this.writeUint8(byte);
      }
    }
    return this.finish();
  }

  /**
   * Calculate the encoded size of a TailSample value.
   * Used for from_after_field computed lengths and buffer pre-allocation.
   */
  calculateSize(value: TailSample): number {
    let size = 0;
    size += 27; // tag (const) + signal + ts_unix_nano + metric_type + series_fingerprint + value
    // labels: array (kind: length_prefixed)
    for (const item of value.labels) {
      const labels_itemEncoder = new LabelPairEncoder();
      size += labels_itemEncoder.calculateSize(item);
    }
    size += 2; // length prefix (uint16)
    return size;
  }
}

export class TailSampleDecoder extends SeekableBitStreamDecoder {
  constructor(input: Uint8Array | number[] | string, private context?: any) {
    const reader = createReader(input);
    super(reader, "msb_first");
  }

  decode(): TailSampleOutput {
    const value: any = {};

    value.tag = this.readUint8();
    value.signal = this.readUint8();
    value.ts_unix_nano = this.readUint64("big_endian");
    value.metric_type = this.readUint8();
    value.series_fingerprint = this.readUint64("big_endian");
    value.value = this.readFloat64("big_endian");
    value.labels = [];
    const labels_length = this.readUint16("big_endian");
    for (let i = 0; i < labels_length; i++) {
      let labels__iter: any;
      labels__iter = {};
      const labels__iter_key_length = this.readUint8();
      const labels__iter_key_bytes = this.readBytesSlice(labels__iter_key_length);
      try {
        labels__iter.key = new TextDecoder("utf-8", { fatal: true }).decode(labels__iter_key_bytes);
      } catch (e) {
        throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
      }
      const labels__iter_value_length = this.readUint16("big_endian");
      const labels__iter_value_bytes = this.readBytesSlice(labels__iter_value_length);
      try {
        labels__iter.value = new TextDecoder("utf-8", { fatal: true }).decode(labels__iter_value_bytes);
      } catch (e) {
        throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
      }
      value.labels.push(labels__iter);
    }
    return value;
  }
}

/**
 * Merged-query live snapshot request (D-054). The query daemon sends this to each discovered ingester on its ingest port after the Hello handshake: 'return your retained recent records for `signal` matching these predicates.' The ingester replies with exactly one LiveBatch, then the connection is done. Logs only in v1. ts_min_unix_nano = 0 means no lower bound; ts_max_unix_nano = 0 means no upper bound; body_contains empty means no substring filter. matchers are ANDed (scry-match).
 */
export interface LiveQueryInput {
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
  matchers: MatcherSpecInput[];
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  ts_min_unix_nano: bigint;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  ts_max_unix_nano: bigint;
  /**
   * String kind: length_prefixed
   * Encoding: utf8
   * Length prefix type: uint32
   */
  body_contains: string;
}

/**
 * Merged-query live snapshot request (D-054). The query daemon sends this to each discovered ingester on its ingest port after the Hello handshake: 'return your retained recent records for `signal` matching these predicates.' The ingester replies with exactly one LiveBatch, then the connection is done. Logs only in v1. ts_min_unix_nano = 0 means no lower bound; ts_max_unix_nano = 0 means no upper bound; body_contains empty means no substring filter. matchers are ANDed (scry-match).
 */
export interface LiveQueryOutput {
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
  matchers: MatcherSpecOutput[];
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  ts_min_unix_nano: bigint;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  ts_max_unix_nano: bigint;
  /**
   * String kind: length_prefixed
   * Encoding: utf8
   * Length prefix type: uint32
   */
  body_contains: string;
}

export type LiveQuery = LiveQueryOutput;

export class LiveQueryEncoder extends BitStreamEncoder {
  private compressionDict: Map<string, number> = new Map();

  constructor() {
    super("msb_first");
  }

  encode(value: LiveQueryInput): Uint8Array {
    // Reset compression dictionary for each encode
    this.compressionDict.clear();

    this.writeUint8(82);
    this.writeUint8(value.signal);
    this.writeUint16(value.matchers.length, "big_endian");
    for (let value_matchers__iter_index = 0; value_matchers__iter_index < value.matchers.length; value_matchers__iter_index++) {
      const value_matchers__iter = value.matchers[value_matchers__iter_index];
      const encoder_value_matchers__iter = new MatcherSpecEncoder();
      const encoded_value_matchers__iter = encoder_value_matchers__iter.encode(value_matchers__iter);
      for (const byte of encoded_value_matchers__iter) {
        this.writeUint8(byte);
      }
    }
    this.writeUint64(value.ts_min_unix_nano, "big_endian");
    this.writeUint64(value.ts_max_unix_nano, "big_endian");
    const value_body_contains_bytes = new TextEncoder().encode(value.body_contains);
    this.writeUint32(value_body_contains_bytes.length, "big_endian");
    for (const byte of value_body_contains_bytes) {
      this.writeUint8(byte);
    }
    return this.finish();
  }

  /**
   * Calculate the encoded size of a LiveQuery value.
   * Used for from_after_field computed lengths and buffer pre-allocation.
   */
  calculateSize(value: LiveQuery): number {
    let size = 0;
    size += 2; // tag (const) + signal
    // matchers: array (kind: length_prefixed)
    for (const item of value.matchers) {
      const matchers_itemEncoder = new MatcherSpecEncoder();
      size += matchers_itemEncoder.calculateSize(item);
    }
    size += 18; // length prefix (uint16) + ts_min_unix_nano + ts_max_unix_nano
    // body_contains: string (utf8)
    size += new TextEncoder().encode(value.body_contains).length;
    return size;
  }
}

export class LiveQueryDecoder extends SeekableBitStreamDecoder {
  constructor(input: Uint8Array | number[] | string, private context?: any) {
    const reader = createReader(input);
    super(reader, "msb_first");
  }

  decode(): LiveQueryOutput {
    const value: any = {};

    value.tag = this.readUint8();
    value.signal = this.readUint8();
    value.matchers = [];
    const matchers_length = this.readUint16("big_endian");
    for (let i = 0; i < matchers_length; i++) {
      let matchers__iter: any;
      matchers__iter = {};
      const matchers__iter_spec_length = this.readUint16("big_endian");
      const matchers__iter_spec_bytes = this.readBytesSlice(matchers__iter_spec_length);
      try {
        matchers__iter.spec = new TextDecoder("utf-8", { fatal: true }).decode(matchers__iter_spec_bytes);
      } catch (e) {
        throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
      }
      value.matchers.push(matchers__iter);
    }
    value.ts_min_unix_nano = this.readUint64("big_endian");
    value.ts_max_unix_nano = this.readUint64("big_endian");
    const body_contains_length = this.readUint32("big_endian");
    const body_contains_bytes = this.readBytesSlice(body_contains_length);
    try {
      value.body_contains = new TextDecoder("utf-8", { fatal: true }).decode(body_contains_bytes);
    } catch (e) {
      throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
    }
    return value;
  }
}

/**
 * An ingester's reply to a LiveQuery (D-054): its retained recent records matching the request, each tagged with the WAL (shard, segment) the merged query dedups against. `writer_uuid` identifies this ingester's WAL instance (combined with signal + each record's shard). Exactly one LiveBatch is sent per LiveQuery, then the connection closes.
 */
export interface LiveBatchInput {
  /**
   * Bytes
   * Raw byte array. Sugar for array of uint8 — same wire format, simpler schema definition.
   */
  writer_uuid: number[];
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint32
   */
  records: LiveRecordInput[];
}

/**
 * An ingester's reply to a LiveQuery (D-054): its retained recent records matching the request, each tagged with the WAL (shard, segment) the merged query dedups against. `writer_uuid` identifies this ingester's WAL instance (combined with signal + each record's shard). Exactly one LiveBatch is sent per LiveQuery, then the connection closes.
 */
export interface LiveBatchOutput {
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  tag: number;
  /**
   * Bytes
   * Raw byte array. Sugar for array of uint8 — same wire format, simpler schema definition.
   */
  writer_uuid: number[];
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint32
   */
  records: LiveRecordOutput[];
}

export type LiveBatch = LiveBatchOutput;

export class LiveBatchEncoder extends BitStreamEncoder {
  private compressionDict: Map<string, number> = new Map();

  constructor() {
    super("msb_first");
  }

  encode(value: LiveBatchInput): Uint8Array {
    // Reset compression dictionary for each encode
    this.compressionDict.clear();

    this.writeUint8(83);
    // Validate fixed-length array
    if (value.writer_uuid.length !== 16) {
      throw new Error(`Array 'writer_uuid' must have exactly 16 elements, got ${value.writer_uuid.length}`);
    }
    for (let value_writer_uuid__iter_index = 0; value_writer_uuid__iter_index < value.writer_uuid.length; value_writer_uuid__iter_index++) {
      const value_writer_uuid__iter = value.writer_uuid[value_writer_uuid__iter_index];
      this.writeUint8(value_writer_uuid__iter);
    }
    this.writeUint32(value.records.length, "big_endian");
    for (let value_records__iter_index = 0; value_records__iter_index < value.records.length; value_records__iter_index++) {
      const value_records__iter = value.records[value_records__iter_index];
      const encoder_value_records__iter = new LiveRecordEncoder();
      const encoded_value_records__iter = encoder_value_records__iter.encode(value_records__iter);
      for (const byte of encoded_value_records__iter) {
        this.writeUint8(byte);
      }
    }
    return this.finish();
  }

  /**
   * Calculate the encoded size of a LiveBatch value.
   * Used for from_after_field computed lengths and buffer pre-allocation.
   */
  calculateSize(value: LiveBatch): number {
    let size = 0;
    size += 1; // tag (const)
    // writer_uuid: bytes (kind: fixed)
    size += value.writer_uuid.length;
    // records: array (kind: length_prefixed)
    for (const item of value.records) {
      const records_itemEncoder = new LiveRecordEncoder();
      size += records_itemEncoder.calculateSize(item);
    }
    size += 4; // length prefix (uint32)
    return size;
  }
}

export class LiveBatchDecoder extends SeekableBitStreamDecoder {
  constructor(input: Uint8Array | number[] | string, private context?: any) {
    const reader = createReader(input);
    super(reader, "msb_first");
  }

  decode(): LiveBatchOutput {
    const value: any = {};

    value.tag = this.readUint8();
    value.writer_uuid = [];
    for (let i = 0; i < 16; i++) {
      let writer_uuid__iter: any;
      writer_uuid__iter = this.readUint8();
      value.writer_uuid.push(writer_uuid__iter);
    }
    value.records = [];
    const records_length = this.readUint32("big_endian");
    for (let i = 0; i < records_length; i++) {
      let records__iter: any;
      records__iter = {};
      records__iter.wal_shard = this.readUint32("big_endian");
      records__iter.wal_seg = this.readUint64("big_endian");
      records__iter.ts_unix_nano = this.readUint64("big_endian");
      records__iter.severity = this.readUint8();
      records__iter.labels = [];
      const records__iter_labels_length = this.readUint16("big_endian");
      for (let i = 0; i < records__iter_labels_length; i++) {
        let records__iter_labels__iter: any;
        records__iter_labels__iter = {};
        const records__iter_labels__iter_key_length = this.readUint8();
        const records__iter_labels__iter_key_bytes = this.readBytesSlice(records__iter_labels__iter_key_length);
        try {
          records__iter_labels__iter.key = new TextDecoder("utf-8", { fatal: true }).decode(records__iter_labels__iter_key_bytes);
        } catch (e) {
          throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
        }
        const records__iter_labels__iter_value_length = this.readUint16("big_endian");
        const records__iter_labels__iter_value_bytes = this.readBytesSlice(records__iter_labels__iter_value_length);
        try {
          records__iter_labels__iter.value = new TextDecoder("utf-8", { fatal: true }).decode(records__iter_labels__iter_value_bytes);
        } catch (e) {
          throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
        }
        records__iter.labels.push(records__iter_labels__iter);
      }
      const records__iter_body_length = this.readUint32("big_endian");
      const records__iter_body_bytes = this.readBytesSlice(records__iter_body_length);
      try {
        records__iter.body = new TextDecoder("utf-8", { fatal: true }).decode(records__iter_body_bytes);
      } catch (e) {
        throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
      }
      records__iter.attributes = [];
      const records__iter_attributes_length = this.readUint16("big_endian");
      for (let i = 0; i < records__iter_attributes_length; i++) {
        let records__iter_attributes__iter: any;
        records__iter_attributes__iter = {};
        const records__iter_attributes__iter_key_length = this.readUint8();
        const records__iter_attributes__iter_key_bytes = this.readBytesSlice(records__iter_attributes__iter_key_length);
        try {
          records__iter_attributes__iter.key = new TextDecoder("utf-8", { fatal: true }).decode(records__iter_attributes__iter_key_bytes);
        } catch (e) {
          throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
        }
        const records__iter_attributes__iter_value_length = this.readUint16("big_endian");
        const records__iter_attributes__iter_value_bytes = this.readBytesSlice(records__iter_attributes__iter_value_length);
        try {
          records__iter_attributes__iter.value = new TextDecoder("utf-8", { fatal: true }).decode(records__iter_attributes__iter_value_bytes);
        } catch (e) {
          throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
        }
        records__iter.attributes.push(records__iter_attributes__iter);
      }
      value.records.push(records__iter);
    }
    return value;
  }
}

/**
 * One retained log record in a LiveBatch. `wal_shard`/`wal_seg` are the dedup tag: the merged query keeps this record iff `wal_seg > H(writer_uuid, signal, wal_shard)`, the catalog's durable per-WAL-instance segment high-water. Everything else mirrors a stored logs row (ts/severity/labels/body/attributes).
 */
export interface LiveRecordInput {
  /**
   * 32-bit Unsigned Integer
   * Fixed-width 32-bit unsigned integer (0-4294967295). Respects endianness configuration.
   */
  wal_shard: number;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  wal_seg: bigint;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  ts_unix_nano: bigint;
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  severity: number;
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint16
   */
  labels: LabelPairInput[];
  /**
   * String kind: length_prefixed
   * Encoding: utf8
   * Length prefix type: uint32
   */
  body: string;
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint16
   */
  attributes: LabelPairInput[];
}

/**
 * One retained log record in a LiveBatch. `wal_shard`/`wal_seg` are the dedup tag: the merged query keeps this record iff `wal_seg > H(writer_uuid, signal, wal_shard)`, the catalog's durable per-WAL-instance segment high-water. Everything else mirrors a stored logs row (ts/severity/labels/body/attributes).
 */
export interface LiveRecordOutput {
  /**
   * 32-bit Unsigned Integer
   * Fixed-width 32-bit unsigned integer (0-4294967295). Respects endianness configuration.
   */
  wal_shard: number;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  wal_seg: bigint;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  ts_unix_nano: bigint;
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  severity: number;
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint16
   */
  labels: LabelPairOutput[];
  /**
   * String kind: length_prefixed
   * Encoding: utf8
   * Length prefix type: uint32
   */
  body: string;
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint16
   */
  attributes: LabelPairOutput[];
}

export type LiveRecord = LiveRecordOutput;

export class LiveRecordEncoder extends BitStreamEncoder {
  private compressionDict: Map<string, number> = new Map();

  constructor() {
    super("msb_first");
  }

  encode(value: LiveRecordInput): Uint8Array {
    // Reset compression dictionary for each encode
    this.compressionDict.clear();

    this.writeUint32(value.wal_shard, "big_endian");
    this.writeUint64(value.wal_seg, "big_endian");
    this.writeUint64(value.ts_unix_nano, "big_endian");
    this.writeUint8(value.severity);
    this.writeUint16(value.labels.length, "big_endian");
    for (let value_labels__iter_index = 0; value_labels__iter_index < value.labels.length; value_labels__iter_index++) {
      const value_labels__iter = value.labels[value_labels__iter_index];
      const encoder_value_labels__iter = new LabelPairEncoder();
      const encoded_value_labels__iter = encoder_value_labels__iter.encode(value_labels__iter);
      for (const byte of encoded_value_labels__iter) {
        this.writeUint8(byte);
      }
    }
    const value_body_bytes = new TextEncoder().encode(value.body);
    this.writeUint32(value_body_bytes.length, "big_endian");
    for (const byte of value_body_bytes) {
      this.writeUint8(byte);
    }
    this.writeUint16(value.attributes.length, "big_endian");
    for (let value_attributes__iter_index = 0; value_attributes__iter_index < value.attributes.length; value_attributes__iter_index++) {
      const value_attributes__iter = value.attributes[value_attributes__iter_index];
      const encoder_value_attributes__iter = new LabelPairEncoder();
      const encoded_value_attributes__iter = encoder_value_attributes__iter.encode(value_attributes__iter);
      for (const byte of encoded_value_attributes__iter) {
        this.writeUint8(byte);
      }
    }
    return this.finish();
  }

  /**
   * Calculate the encoded size of a LiveRecord value.
   * Used for from_after_field computed lengths and buffer pre-allocation.
   */
  calculateSize(value: LiveRecord): number {
    let size = 0;
    size += 21; // wal_shard + wal_seg + ts_unix_nano + severity
    // labels: array (kind: length_prefixed)
    for (const item of value.labels) {
      const labels_itemEncoder = new LabelPairEncoder();
      size += labels_itemEncoder.calculateSize(item);
    }
    size += 2; // length prefix (uint16)
    // body: string (utf8)
    size += new TextEncoder().encode(value.body).length;
    // attributes: array (kind: length_prefixed)
    for (const item of value.attributes) {
      const attributes_itemEncoder = new LabelPairEncoder();
      size += attributes_itemEncoder.calculateSize(item);
    }
    size += 2; // length prefix (uint16)
    return size;
  }
}

export class LiveRecordDecoder extends SeekableBitStreamDecoder {
  constructor(input: Uint8Array | number[] | string, private context?: any) {
    const reader = createReader(input);
    super(reader, "msb_first");
  }

  decode(): LiveRecordOutput {
    const value: any = {};

    value.wal_shard = this.readUint32("big_endian");
    value.wal_seg = this.readUint64("big_endian");
    value.ts_unix_nano = this.readUint64("big_endian");
    value.severity = this.readUint8();
    value.labels = [];
    const labels_length = this.readUint16("big_endian");
    for (let i = 0; i < labels_length; i++) {
      let labels__iter: any;
      labels__iter = {};
      const labels__iter_key_length = this.readUint8();
      const labels__iter_key_bytes = this.readBytesSlice(labels__iter_key_length);
      try {
        labels__iter.key = new TextDecoder("utf-8", { fatal: true }).decode(labels__iter_key_bytes);
      } catch (e) {
        throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
      }
      const labels__iter_value_length = this.readUint16("big_endian");
      const labels__iter_value_bytes = this.readBytesSlice(labels__iter_value_length);
      try {
        labels__iter.value = new TextDecoder("utf-8", { fatal: true }).decode(labels__iter_value_bytes);
      } catch (e) {
        throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
      }
      value.labels.push(labels__iter);
    }
    const body_length = this.readUint32("big_endian");
    const body_bytes = this.readBytesSlice(body_length);
    try {
      value.body = new TextDecoder("utf-8", { fatal: true }).decode(body_bytes);
    } catch (e) {
      throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
    }
    value.attributes = [];
    const attributes_length = this.readUint16("big_endian");
    for (let i = 0; i < attributes_length; i++) {
      let attributes__iter: any;
      attributes__iter = {};
      const attributes__iter_key_length = this.readUint8();
      const attributes__iter_key_bytes = this.readBytesSlice(attributes__iter_key_length);
      try {
        attributes__iter.key = new TextDecoder("utf-8", { fatal: true }).decode(attributes__iter_key_bytes);
      } catch (e) {
        throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
      }
      const attributes__iter_value_length = this.readUint16("big_endian");
      const attributes__iter_value_bytes = this.readBytesSlice(attributes__iter_value_length);
      try {
        attributes__iter.value = new TextDecoder("utf-8", { fatal: true }).decode(attributes__iter_value_bytes);
      } catch (e) {
        throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
      }
      value.attributes.push(attributes__iter);
    }
    return value;
  }
}

/**
 * A key-value label, used by every signal. UTF-8 throughout.
 */
export interface LabelPairInput {
  /**
   * String kind: length_prefixed
   * Encoding: utf8
   * Length prefix type: uint8
   */
  key: string;
  /**
   * String kind: length_prefixed
   * Encoding: utf8
   * Length prefix type: uint16
   */
  value: string;
}

/**
 * A key-value label, used by every signal. UTF-8 throughout.
 */
export interface LabelPairOutput {
  /**
   * String kind: length_prefixed
   * Encoding: utf8
   * Length prefix type: uint8
   */
  key: string;
  /**
   * String kind: length_prefixed
   * Encoding: utf8
   * Length prefix type: uint16
   */
  value: string;
}

export type LabelPair = LabelPairOutput;

export class LabelPairEncoder extends BitStreamEncoder {
  private compressionDict: Map<string, number> = new Map();

  constructor() {
    super("msb_first");
  }

  encode(value: LabelPairInput): Uint8Array {
    // Reset compression dictionary for each encode
    this.compressionDict.clear();

    const value_key_bytes = new TextEncoder().encode(value.key);
    this.writeUint8(value_key_bytes.length);
    for (const byte of value_key_bytes) {
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
   * Calculate the encoded size of a LabelPair value.
   * Used for from_after_field computed lengths and buffer pre-allocation.
   */
  calculateSize(value: LabelPair): number {
    let size = 0;
    // key: string (utf8)
    size += new TextEncoder().encode(value.key).length;
    // value: string (utf8)
    size += new TextEncoder().encode(value.value).length;
    return size;
  }
}

export class LabelPairDecoder extends SeekableBitStreamDecoder {
  constructor(input: Uint8Array | number[] | string, private context?: any) {
    const reader = createReader(input);
    super(reader, "msb_first");
  }

  decode(): LabelPairOutput {
    const value: any = {};

    const key_length = this.readUint8();
    const key_bytes = this.readBytesSlice(key_length);
    try {
      value.key = new TextDecoder("utf-8", { fatal: true }).decode(key_bytes);
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
 * Body of a metrics Batch after decompression. Series dictionary plus samples that reference fingerprints.
 */
export interface MetricsBatchInput {
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint32
   */
  series: SeriesDictEntryInput[];
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint32
   */
  samples: MetricSampleInput[];
}

/**
 * Body of a metrics Batch after decompression. Series dictionary plus samples that reference fingerprints.
 */
export interface MetricsBatchOutput {
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint32
   */
  series: SeriesDictEntryOutput[];
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint32
   */
  samples: MetricSampleOutput[];
}

export type MetricsBatch = MetricsBatchOutput;

export class MetricsBatchEncoder extends BitStreamEncoder {
  private compressionDict: Map<string, number> = new Map();

  constructor() {
    super("msb_first");
  }

  encode(value: MetricsBatchInput): Uint8Array {
    // Reset compression dictionary for each encode
    this.compressionDict.clear();

    this.writeUint32(value.series.length, "big_endian");
    for (let value_series__iter_index = 0; value_series__iter_index < value.series.length; value_series__iter_index++) {
      const value_series__iter = value.series[value_series__iter_index];
      const encoder_value_series__iter = new SeriesDictEntryEncoder();
      const encoded_value_series__iter = encoder_value_series__iter.encode(value_series__iter);
      for (const byte of encoded_value_series__iter) {
        this.writeUint8(byte);
      }
    }
    this.writeUint32(value.samples.length, "big_endian");
    for (let value_samples__iter_index = 0; value_samples__iter_index < value.samples.length; value_samples__iter_index++) {
      const value_samples__iter = value.samples[value_samples__iter_index];
      const encoder_value_samples__iter = new MetricSampleEncoder();
      const encoded_value_samples__iter = encoder_value_samples__iter.encode(value_samples__iter);
      for (const byte of encoded_value_samples__iter) {
        this.writeUint8(byte);
      }
    }
    return this.finish();
  }

  /**
   * Calculate the encoded size of a MetricsBatch value.
   * Used for from_after_field computed lengths and buffer pre-allocation.
   */
  calculateSize(value: MetricsBatch): number {
    let size = 0;
    // series: array (kind: length_prefixed)
    for (const item of value.series) {
      const series_itemEncoder = new SeriesDictEntryEncoder();
      size += series_itemEncoder.calculateSize(item);
    }
    size += 4; // length prefix (uint32)
    // samples: array (kind: length_prefixed)
    for (const item of value.samples) {
      const samples_itemEncoder = new MetricSampleEncoder();
      size += samples_itemEncoder.calculateSize(item);
    }
    size += 4; // length prefix (uint32)
    return size;
  }
}

export class MetricsBatchDecoder extends SeekableBitStreamDecoder {
  constructor(input: Uint8Array | number[] | string, private context?: any) {
    const reader = createReader(input);
    super(reader, "msb_first");
  }

  decode(): MetricsBatchOutput {
    const value: any = {};

    value.series = [];
    const series_length = this.readUint32("big_endian");
    for (let i = 0; i < series_length; i++) {
      let series__iter: any;
      series__iter = {};
      series__iter.fingerprint = this.readUint64("big_endian");
      series__iter.metric_type = this.readUint8();
      series__iter.labels = [];
      const series__iter_labels_length = this.readUint16("big_endian");
      for (let i = 0; i < series__iter_labels_length; i++) {
        let series__iter_labels__iter: any;
        series__iter_labels__iter = {};
        const series__iter_labels__iter_key_length = this.readUint8();
        const series__iter_labels__iter_key_bytes = this.readBytesSlice(series__iter_labels__iter_key_length);
        try {
          series__iter_labels__iter.key = new TextDecoder("utf-8", { fatal: true }).decode(series__iter_labels__iter_key_bytes);
        } catch (e) {
          throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
        }
        const series__iter_labels__iter_value_length = this.readUint16("big_endian");
        const series__iter_labels__iter_value_bytes = this.readBytesSlice(series__iter_labels__iter_value_length);
        try {
          series__iter_labels__iter.value = new TextDecoder("utf-8", { fatal: true }).decode(series__iter_labels__iter_value_bytes);
        } catch (e) {
          throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
        }
        series__iter.labels.push(series__iter_labels__iter);
      }
      value.series.push(series__iter);
    }
    value.samples = [];
    const samples_length = this.readUint32("big_endian");
    for (let i = 0; i < samples_length; i++) {
      let samples__iter: any;
      samples__iter = {};
      samples__iter.fingerprint = this.readUint64("big_endian");
      samples__iter.ts_unix_nano = this.readUint64("big_endian");
      samples__iter.value = this.readFloat64("big_endian");
      value.samples.push(samples__iter);
    }
    return value;
  }
}

/**
 * Declares a series (set of labels) and its xxh3-64 fingerprint. The agent computes the fingerprint; the server validates it on receipt.
 */
export interface SeriesDictEntryInput {
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  fingerprint: bigint;
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  metric_type: number;
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint16
   */
  labels: LabelPairInput[];
}

/**
 * Declares a series (set of labels) and its xxh3-64 fingerprint. The agent computes the fingerprint; the server validates it on receipt.
 */
export interface SeriesDictEntryOutput {
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  fingerprint: bigint;
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  metric_type: number;
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint16
   */
  labels: LabelPairOutput[];
}

export type SeriesDictEntry = SeriesDictEntryOutput;

export class SeriesDictEntryEncoder extends BitStreamEncoder {
  private compressionDict: Map<string, number> = new Map();

  constructor() {
    super("msb_first");
  }

  encode(value: SeriesDictEntryInput): Uint8Array {
    // Reset compression dictionary for each encode
    this.compressionDict.clear();

    this.writeUint64(value.fingerprint, "big_endian");
    this.writeUint8(value.metric_type);
    this.writeUint16(value.labels.length, "big_endian");
    for (let value_labels__iter_index = 0; value_labels__iter_index < value.labels.length; value_labels__iter_index++) {
      const value_labels__iter = value.labels[value_labels__iter_index];
      const encoder_value_labels__iter = new LabelPairEncoder();
      const encoded_value_labels__iter = encoder_value_labels__iter.encode(value_labels__iter);
      for (const byte of encoded_value_labels__iter) {
        this.writeUint8(byte);
      }
    }
    return this.finish();
  }

  /**
   * Calculate the encoded size of a SeriesDictEntry value.
   * Used for from_after_field computed lengths and buffer pre-allocation.
   */
  calculateSize(value: SeriesDictEntry): number {
    let size = 0;
    size += 9; // fingerprint + metric_type
    // labels: array (kind: length_prefixed)
    for (const item of value.labels) {
      const labels_itemEncoder = new LabelPairEncoder();
      size += labels_itemEncoder.calculateSize(item);
    }
    size += 2; // length prefix (uint16)
    return size;
  }
}

export class SeriesDictEntryDecoder extends SeekableBitStreamDecoder {
  constructor(input: Uint8Array | number[] | string, private context?: any) {
    const reader = createReader(input);
    super(reader, "msb_first");
  }

  decode(): SeriesDictEntryOutput {
    const value: any = {};

    value.fingerprint = this.readUint64("big_endian");
    value.metric_type = this.readUint8();
    value.labels = [];
    const labels_length = this.readUint16("big_endian");
    for (let i = 0; i < labels_length; i++) {
      let labels__iter: any;
      labels__iter = {};
      const labels__iter_key_length = this.readUint8();
      const labels__iter_key_bytes = this.readBytesSlice(labels__iter_key_length);
      try {
        labels__iter.key = new TextDecoder("utf-8", { fatal: true }).decode(labels__iter_key_bytes);
      } catch (e) {
        throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
      }
      const labels__iter_value_length = this.readUint16("big_endian");
      const labels__iter_value_bytes = this.readBytesSlice(labels__iter_value_length);
      try {
        labels__iter.value = new TextDecoder("utf-8", { fatal: true }).decode(labels__iter_value_bytes);
      } catch (e) {
        throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
      }
      value.labels.push(labels__iter);
    }
    return value;
  }
}

/**
 * One scalar observation. Histograms and summaries are exploded into multiple samples before serialisation.
 */
export interface MetricSampleInput {
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  fingerprint: bigint;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  ts_unix_nano: bigint;
  /**
   * 64-bit Floating Point
   * IEEE 754 double-precision floating point (64-bit). Provides ~15 decimal digits of precision.
   */
  value: number;
}

/**
 * One scalar observation. Histograms and summaries are exploded into multiple samples before serialisation.
 */
export interface MetricSampleOutput {
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  fingerprint: bigint;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  ts_unix_nano: bigint;
  /**
   * 64-bit Floating Point
   * IEEE 754 double-precision floating point (64-bit). Provides ~15 decimal digits of precision.
   */
  value: number;
}

export type MetricSample = MetricSampleOutput;

export class MetricSampleEncoder extends BitStreamEncoder {
  private compressionDict: Map<string, number> = new Map();

  constructor() {
    super("msb_first");
  }

  encode(value: MetricSampleInput): Uint8Array {
    // Reset compression dictionary for each encode
    this.compressionDict.clear();

    this.writeUint64(value.fingerprint, "big_endian");
    this.writeUint64(value.ts_unix_nano, "big_endian");
    this.writeFloat64(value.value, "big_endian");
    return this.finish();
  }

  /**
   * Calculate the encoded size of a MetricSample value.
   * Used for from_after_field computed lengths and buffer pre-allocation.
   */
  calculateSize(value: MetricSample): number {
    return 24; // fingerprint + ts_unix_nano + value
  }
}

export class MetricSampleDecoder extends SeekableBitStreamDecoder {
  constructor(input: Uint8Array | number[] | string, private context?: any) {
    const reader = createReader(input);
    super(reader, "msb_first");
  }

  decode(): MetricSampleOutput {
    const value: any = {};

    value.fingerprint = this.readUint64("big_endian");
    value.ts_unix_nano = this.readUint64("big_endian");
    value.value = this.readFloat64("big_endian");
    return value;
  }
}

/**
 * Body of a logs Batch after decompression. Records are pre-grouped by label set at the agent.
 */
export interface LogsBatchInput {
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint32
   */
  streams: LogStreamInput[];
}

/**
 * Body of a logs Batch after decompression. Records are pre-grouped by label set at the agent.
 */
export interface LogsBatchOutput {
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint32
   */
  streams: LogStreamOutput[];
}

export type LogsBatch = LogsBatchOutput;

export class LogsBatchEncoder extends BitStreamEncoder {
  private compressionDict: Map<string, number> = new Map();

  constructor() {
    super("msb_first");
  }

  encode(value: LogsBatchInput): Uint8Array {
    // Reset compression dictionary for each encode
    this.compressionDict.clear();

    this.writeUint32(value.streams.length, "big_endian");
    for (let value_streams__iter_index = 0; value_streams__iter_index < value.streams.length; value_streams__iter_index++) {
      const value_streams__iter = value.streams[value_streams__iter_index];
      const encoder_value_streams__iter = new LogStreamEncoder();
      const encoded_value_streams__iter = encoder_value_streams__iter.encode(value_streams__iter);
      for (const byte of encoded_value_streams__iter) {
        this.writeUint8(byte);
      }
    }
    return this.finish();
  }

  /**
   * Calculate the encoded size of a LogsBatch value.
   * Used for from_after_field computed lengths and buffer pre-allocation.
   */
  calculateSize(value: LogsBatch): number {
    let size = 0;
    // streams: array (kind: length_prefixed)
    for (const item of value.streams) {
      const streams_itemEncoder = new LogStreamEncoder();
      size += streams_itemEncoder.calculateSize(item);
    }
    size += 4; // length prefix (uint32)
    return size;
  }
}

export class LogsBatchDecoder extends SeekableBitStreamDecoder {
  constructor(input: Uint8Array | number[] | string, private context?: any) {
    const reader = createReader(input);
    super(reader, "msb_first");
  }

  decode(): LogsBatchOutput {
    const value: any = {};

    value.streams = [];
    const streams_length = this.readUint32("big_endian");
    for (let i = 0; i < streams_length; i++) {
      let streams__iter: any;
      streams__iter = {};
      streams__iter.fingerprint = this.readUint64("big_endian");
      streams__iter.labels = [];
      const streams__iter_labels_length = this.readUint16("big_endian");
      for (let i = 0; i < streams__iter_labels_length; i++) {
        let streams__iter_labels__iter: any;
        streams__iter_labels__iter = {};
        const streams__iter_labels__iter_key_length = this.readUint8();
        const streams__iter_labels__iter_key_bytes = this.readBytesSlice(streams__iter_labels__iter_key_length);
        try {
          streams__iter_labels__iter.key = new TextDecoder("utf-8", { fatal: true }).decode(streams__iter_labels__iter_key_bytes);
        } catch (e) {
          throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
        }
        const streams__iter_labels__iter_value_length = this.readUint16("big_endian");
        const streams__iter_labels__iter_value_bytes = this.readBytesSlice(streams__iter_labels__iter_value_length);
        try {
          streams__iter_labels__iter.value = new TextDecoder("utf-8", { fatal: true }).decode(streams__iter_labels__iter_value_bytes);
        } catch (e) {
          throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
        }
        streams__iter.labels.push(streams__iter_labels__iter);
      }
      streams__iter.entries = [];
      const streams__iter_entries_length = this.readUint32("big_endian");
      for (let i = 0; i < streams__iter_entries_length; i++) {
        let streams__iter_entries__iter: any;
        streams__iter_entries__iter = {};
        streams__iter_entries__iter.ts_unix_nano = this.readUint64("big_endian");
        streams__iter_entries__iter.severity = this.readUint8();
        const streams__iter_entries__iter_body_length = this.readUint32("big_endian");
        const streams__iter_entries__iter_body_bytes = this.readBytesSlice(streams__iter_entries__iter_body_length);
        try {
          streams__iter_entries__iter.body = new TextDecoder("utf-8", { fatal: true }).decode(streams__iter_entries__iter_body_bytes);
        } catch (e) {
          throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
        }
        streams__iter_entries__iter.attributes = [];
        const streams__iter_entries__iter_attributes_length = this.readUint16("big_endian");
        for (let i = 0; i < streams__iter_entries__iter_attributes_length; i++) {
          let streams__iter_entries__iter_attributes__iter: any;
          streams__iter_entries__iter_attributes__iter = {};
          const streams__iter_entries__iter_attributes__iter_key_length = this.readUint8();
          const streams__iter_entries__iter_attributes__iter_key_bytes = this.readBytesSlice(streams__iter_entries__iter_attributes__iter_key_length);
          try {
            streams__iter_entries__iter_attributes__iter.key = new TextDecoder("utf-8", { fatal: true }).decode(streams__iter_entries__iter_attributes__iter_key_bytes);
          } catch (e) {
            throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
          }
          const streams__iter_entries__iter_attributes__iter_value_length = this.readUint16("big_endian");
          const streams__iter_entries__iter_attributes__iter_value_bytes = this.readBytesSlice(streams__iter_entries__iter_attributes__iter_value_length);
          try {
            streams__iter_entries__iter_attributes__iter.value = new TextDecoder("utf-8", { fatal: true }).decode(streams__iter_entries__iter_attributes__iter_value_bytes);
          } catch (e) {
            throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
          }
          streams__iter_entries__iter.attributes.push(streams__iter_entries__iter_attributes__iter);
        }
        streams__iter.entries.push(streams__iter_entries__iter);
      }
      value.streams.push(streams__iter);
    }
    return value;
  }
}

/**
 * A run of log entries sharing the same labels. Fingerprint follows the same xxh3-64 convention as metrics.
 */
export interface LogStreamInput {
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  fingerprint: bigint;
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint16
   */
  labels: LabelPairInput[];
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint32
   */
  entries: LogEntryInput[];
}

/**
 * A run of log entries sharing the same labels. Fingerprint follows the same xxh3-64 convention as metrics.
 */
export interface LogStreamOutput {
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  fingerprint: bigint;
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint16
   */
  labels: LabelPairOutput[];
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint32
   */
  entries: LogEntryOutput[];
}

export type LogStream = LogStreamOutput;

export class LogStreamEncoder extends BitStreamEncoder {
  private compressionDict: Map<string, number> = new Map();

  constructor() {
    super("msb_first");
  }

  encode(value: LogStreamInput): Uint8Array {
    // Reset compression dictionary for each encode
    this.compressionDict.clear();

    this.writeUint64(value.fingerprint, "big_endian");
    this.writeUint16(value.labels.length, "big_endian");
    for (let value_labels__iter_index = 0; value_labels__iter_index < value.labels.length; value_labels__iter_index++) {
      const value_labels__iter = value.labels[value_labels__iter_index];
      const encoder_value_labels__iter = new LabelPairEncoder();
      const encoded_value_labels__iter = encoder_value_labels__iter.encode(value_labels__iter);
      for (const byte of encoded_value_labels__iter) {
        this.writeUint8(byte);
      }
    }
    this.writeUint32(value.entries.length, "big_endian");
    for (let value_entries__iter_index = 0; value_entries__iter_index < value.entries.length; value_entries__iter_index++) {
      const value_entries__iter = value.entries[value_entries__iter_index];
      const encoder_value_entries__iter = new LogEntryEncoder();
      const encoded_value_entries__iter = encoder_value_entries__iter.encode(value_entries__iter);
      for (const byte of encoded_value_entries__iter) {
        this.writeUint8(byte);
      }
    }
    return this.finish();
  }

  /**
   * Calculate the encoded size of a LogStream value.
   * Used for from_after_field computed lengths and buffer pre-allocation.
   */
  calculateSize(value: LogStream): number {
    let size = 0;
    size += 8; // fingerprint
    // labels: array (kind: length_prefixed)
    for (const item of value.labels) {
      const labels_itemEncoder = new LabelPairEncoder();
      size += labels_itemEncoder.calculateSize(item);
    }
    size += 2; // length prefix (uint16)
    // entries: array (kind: length_prefixed)
    for (const item of value.entries) {
      const entries_itemEncoder = new LogEntryEncoder();
      size += entries_itemEncoder.calculateSize(item);
    }
    size += 4; // length prefix (uint32)
    return size;
  }
}

export class LogStreamDecoder extends SeekableBitStreamDecoder {
  constructor(input: Uint8Array | number[] | string, private context?: any) {
    const reader = createReader(input);
    super(reader, "msb_first");
  }

  decode(): LogStreamOutput {
    const value: any = {};

    value.fingerprint = this.readUint64("big_endian");
    value.labels = [];
    const labels_length = this.readUint16("big_endian");
    for (let i = 0; i < labels_length; i++) {
      let labels__iter: any;
      labels__iter = {};
      const labels__iter_key_length = this.readUint8();
      const labels__iter_key_bytes = this.readBytesSlice(labels__iter_key_length);
      try {
        labels__iter.key = new TextDecoder("utf-8", { fatal: true }).decode(labels__iter_key_bytes);
      } catch (e) {
        throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
      }
      const labels__iter_value_length = this.readUint16("big_endian");
      const labels__iter_value_bytes = this.readBytesSlice(labels__iter_value_length);
      try {
        labels__iter.value = new TextDecoder("utf-8", { fatal: true }).decode(labels__iter_value_bytes);
      } catch (e) {
        throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
      }
      value.labels.push(labels__iter);
    }
    value.entries = [];
    const entries_length = this.readUint32("big_endian");
    for (let i = 0; i < entries_length; i++) {
      let entries__iter: any;
      entries__iter = {};
      entries__iter.ts_unix_nano = this.readUint64("big_endian");
      entries__iter.severity = this.readUint8();
      const entries__iter_body_length = this.readUint32("big_endian");
      const entries__iter_body_bytes = this.readBytesSlice(entries__iter_body_length);
      try {
        entries__iter.body = new TextDecoder("utf-8", { fatal: true }).decode(entries__iter_body_bytes);
      } catch (e) {
        throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
      }
      entries__iter.attributes = [];
      const entries__iter_attributes_length = this.readUint16("big_endian");
      for (let i = 0; i < entries__iter_attributes_length; i++) {
        let entries__iter_attributes__iter: any;
        entries__iter_attributes__iter = {};
        const entries__iter_attributes__iter_key_length = this.readUint8();
        const entries__iter_attributes__iter_key_bytes = this.readBytesSlice(entries__iter_attributes__iter_key_length);
        try {
          entries__iter_attributes__iter.key = new TextDecoder("utf-8", { fatal: true }).decode(entries__iter_attributes__iter_key_bytes);
        } catch (e) {
          throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
        }
        const entries__iter_attributes__iter_value_length = this.readUint16("big_endian");
        const entries__iter_attributes__iter_value_bytes = this.readBytesSlice(entries__iter_attributes__iter_value_length);
        try {
          entries__iter_attributes__iter.value = new TextDecoder("utf-8", { fatal: true }).decode(entries__iter_attributes__iter_value_bytes);
        } catch (e) {
          throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
        }
        entries__iter.attributes.push(entries__iter_attributes__iter);
      }
      value.entries.push(entries__iter);
    }
    return value;
  }
}

/**
 * One log line plus per-entry attributes that aren't part of the stream's stable label set.
 */
export interface LogEntryInput {
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  ts_unix_nano: bigint;
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  severity: number;
  /**
   * String kind: length_prefixed
   * Encoding: utf8
   * Length prefix type: uint32
   */
  body: string;
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint16
   */
  attributes: LabelPairInput[];
}

/**
 * One log line plus per-entry attributes that aren't part of the stream's stable label set.
 */
export interface LogEntryOutput {
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  ts_unix_nano: bigint;
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  severity: number;
  /**
   * String kind: length_prefixed
   * Encoding: utf8
   * Length prefix type: uint32
   */
  body: string;
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint16
   */
  attributes: LabelPairOutput[];
}

export type LogEntry = LogEntryOutput;

export class LogEntryEncoder extends BitStreamEncoder {
  private compressionDict: Map<string, number> = new Map();

  constructor() {
    super("msb_first");
  }

  encode(value: LogEntryInput): Uint8Array {
    // Reset compression dictionary for each encode
    this.compressionDict.clear();

    this.writeUint64(value.ts_unix_nano, "big_endian");
    this.writeUint8(value.severity);
    const value_body_bytes = new TextEncoder().encode(value.body);
    this.writeUint32(value_body_bytes.length, "big_endian");
    for (const byte of value_body_bytes) {
      this.writeUint8(byte);
    }
    this.writeUint16(value.attributes.length, "big_endian");
    for (let value_attributes__iter_index = 0; value_attributes__iter_index < value.attributes.length; value_attributes__iter_index++) {
      const value_attributes__iter = value.attributes[value_attributes__iter_index];
      const encoder_value_attributes__iter = new LabelPairEncoder();
      const encoded_value_attributes__iter = encoder_value_attributes__iter.encode(value_attributes__iter);
      for (const byte of encoded_value_attributes__iter) {
        this.writeUint8(byte);
      }
    }
    return this.finish();
  }

  /**
   * Calculate the encoded size of a LogEntry value.
   * Used for from_after_field computed lengths and buffer pre-allocation.
   */
  calculateSize(value: LogEntry): number {
    let size = 0;
    size += 9; // ts_unix_nano + severity
    // body: string (utf8)
    size += new TextEncoder().encode(value.body).length;
    // attributes: array (kind: length_prefixed)
    for (const item of value.attributes) {
      const attributes_itemEncoder = new LabelPairEncoder();
      size += attributes_itemEncoder.calculateSize(item);
    }
    size += 2; // length prefix (uint16)
    return size;
  }
}

export class LogEntryDecoder extends SeekableBitStreamDecoder {
  constructor(input: Uint8Array | number[] | string, private context?: any) {
    const reader = createReader(input);
    super(reader, "msb_first");
  }

  decode(): LogEntryOutput {
    const value: any = {};

    value.ts_unix_nano = this.readUint64("big_endian");
    value.severity = this.readUint8();
    const body_length = this.readUint32("big_endian");
    const body_bytes = this.readBytesSlice(body_length);
    try {
      value.body = new TextDecoder("utf-8", { fatal: true }).decode(body_bytes);
    } catch (e) {
      throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
    }
    value.attributes = [];
    const attributes_length = this.readUint16("big_endian");
    for (let i = 0; i < attributes_length; i++) {
      let attributes__iter: any;
      attributes__iter = {};
      const attributes__iter_key_length = this.readUint8();
      const attributes__iter_key_bytes = this.readBytesSlice(attributes__iter_key_length);
      try {
        attributes__iter.key = new TextDecoder("utf-8", { fatal: true }).decode(attributes__iter_key_bytes);
      } catch (e) {
        throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
      }
      const attributes__iter_value_length = this.readUint16("big_endian");
      const attributes__iter_value_bytes = this.readBytesSlice(attributes__iter_value_length);
      try {
        attributes__iter.value = new TextDecoder("utf-8", { fatal: true }).decode(attributes__iter_value_bytes);
      } catch (e) {
        throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
      }
      value.attributes.push(attributes__iter);
    }
    return value;
  }
}

/**
 * Body of a traces Batch after decompression. OTel-aligned with per-batch resource/scope dictionaries.
 */
export interface TracesBatchInput {
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint16
   */
  resources: ResourceEntryInput[];
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint16
   */
  scopes: ScopeEntryInput[];
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint32
   */
  spans: SpanInput[];
}

/**
 * Body of a traces Batch after decompression. OTel-aligned with per-batch resource/scope dictionaries.
 */
export interface TracesBatchOutput {
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint16
   */
  resources: ResourceEntryOutput[];
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint16
   */
  scopes: ScopeEntryOutput[];
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint32
   */
  spans: SpanOutput[];
}

export type TracesBatch = TracesBatchOutput;

export class TracesBatchEncoder extends BitStreamEncoder {
  private compressionDict: Map<string, number> = new Map();

  constructor() {
    super("msb_first");
  }

  encode(value: TracesBatchInput): Uint8Array {
    // Reset compression dictionary for each encode
    this.compressionDict.clear();

    this.writeUint16(value.resources.length, "big_endian");
    for (let value_resources__iter_index = 0; value_resources__iter_index < value.resources.length; value_resources__iter_index++) {
      const value_resources__iter = value.resources[value_resources__iter_index];
      const encoder_value_resources__iter = new ResourceEntryEncoder();
      const encoded_value_resources__iter = encoder_value_resources__iter.encode(value_resources__iter);
      for (const byte of encoded_value_resources__iter) {
        this.writeUint8(byte);
      }
    }
    this.writeUint16(value.scopes.length, "big_endian");
    for (let value_scopes__iter_index = 0; value_scopes__iter_index < value.scopes.length; value_scopes__iter_index++) {
      const value_scopes__iter = value.scopes[value_scopes__iter_index];
      const encoder_value_scopes__iter = new ScopeEntryEncoder();
      const encoded_value_scopes__iter = encoder_value_scopes__iter.encode(value_scopes__iter);
      for (const byte of encoded_value_scopes__iter) {
        this.writeUint8(byte);
      }
    }
    this.writeUint32(value.spans.length, "big_endian");
    for (let value_spans__iter_index = 0; value_spans__iter_index < value.spans.length; value_spans__iter_index++) {
      const value_spans__iter = value.spans[value_spans__iter_index];
      const encoder_value_spans__iter = new SpanEncoder();
      const encoded_value_spans__iter = encoder_value_spans__iter.encode(value_spans__iter);
      for (const byte of encoded_value_spans__iter) {
        this.writeUint8(byte);
      }
    }
    return this.finish();
  }

  /**
   * Calculate the encoded size of a TracesBatch value.
   * Used for from_after_field computed lengths and buffer pre-allocation.
   */
  calculateSize(value: TracesBatch): number {
    let size = 0;
    // resources: array (kind: length_prefixed)
    for (const item of value.resources) {
      const resources_itemEncoder = new ResourceEntryEncoder();
      size += resources_itemEncoder.calculateSize(item);
    }
    size += 2; // length prefix (uint16)
    // scopes: array (kind: length_prefixed)
    for (const item of value.scopes) {
      const scopes_itemEncoder = new ScopeEntryEncoder();
      size += scopes_itemEncoder.calculateSize(item);
    }
    size += 2; // length prefix (uint16)
    // spans: array (kind: length_prefixed)
    for (const item of value.spans) {
      const spans_itemEncoder = new SpanEncoder();
      size += spans_itemEncoder.calculateSize(item);
    }
    size += 4; // length prefix (uint32)
    return size;
  }
}

export class TracesBatchDecoder extends SeekableBitStreamDecoder {
  constructor(input: Uint8Array | number[] | string, private context?: any) {
    const reader = createReader(input);
    super(reader, "msb_first");
  }

  decode(): TracesBatchOutput {
    const value: any = {};

    value.resources = [];
    const resources_length = this.readUint16("big_endian");
    for (let i = 0; i < resources_length; i++) {
      let resources__iter: any;
      resources__iter = {};
      resources__iter.labels = [];
      const resources__iter_labels_length = this.readUint16("big_endian");
      for (let i = 0; i < resources__iter_labels_length; i++) {
        let resources__iter_labels__iter: any;
        resources__iter_labels__iter = {};
        const resources__iter_labels__iter_key_length = this.readUint8();
        const resources__iter_labels__iter_key_bytes = this.readBytesSlice(resources__iter_labels__iter_key_length);
        try {
          resources__iter_labels__iter.key = new TextDecoder("utf-8", { fatal: true }).decode(resources__iter_labels__iter_key_bytes);
        } catch (e) {
          throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
        }
        const resources__iter_labels__iter_value_length = this.readUint16("big_endian");
        const resources__iter_labels__iter_value_bytes = this.readBytesSlice(resources__iter_labels__iter_value_length);
        try {
          resources__iter_labels__iter.value = new TextDecoder("utf-8", { fatal: true }).decode(resources__iter_labels__iter_value_bytes);
        } catch (e) {
          throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
        }
        resources__iter.labels.push(resources__iter_labels__iter);
      }
      value.resources.push(resources__iter);
    }
    value.scopes = [];
    const scopes_length = this.readUint16("big_endian");
    for (let i = 0; i < scopes_length; i++) {
      let scopes__iter: any;
      scopes__iter = {};
      const scopes__iter_name_length = this.readUint8();
      const scopes__iter_name_bytes = this.readBytesSlice(scopes__iter_name_length);
      try {
        scopes__iter.name = new TextDecoder("utf-8", { fatal: true }).decode(scopes__iter_name_bytes);
      } catch (e) {
        throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
      }
      const scopes__iter_version_length = this.readUint8();
      const scopes__iter_version_bytes = this.readBytesSlice(scopes__iter_version_length);
      scopes__iter.version = String.fromCharCode(...scopes__iter_version_bytes);
      value.scopes.push(scopes__iter);
    }
    value.spans = [];
    const spans_length = this.readUint32("big_endian");
    for (let i = 0; i < spans_length; i++) {
      let spans__iter: any;
      spans__iter = {};
      spans__iter.resource_idx = this.readUint16("big_endian");
      spans__iter.scope_idx = this.readUint16("big_endian");
      spans__iter.trace_id = [];
      for (let i = 0; i < 16; i++) {
        let spans__iter_trace_id__iter: any;
        spans__iter_trace_id__iter = this.readUint8();
        spans__iter.trace_id.push(spans__iter_trace_id__iter);
      }
      spans__iter.span_id = [];
      for (let i = 0; i < 8; i++) {
        let spans__iter_span_id__iter: any;
        spans__iter_span_id__iter = this.readUint8();
        spans__iter.span_id.push(spans__iter_span_id__iter);
      }
      const spans__iter_parent_span_id_present = this.readUint8();
      if (spans__iter_parent_span_id_present !== 0) {
        spans__iter.parent_span_id = [];
        for (let i = 0; i < 8; i++) {
          let spans__iter_parent_span_id__iter: any;
          spans__iter_parent_span_id__iter = this.readUint8();
          spans__iter.parent_span_id.push(spans__iter_parent_span_id__iter);
        }
      }
      const spans__iter_name_length = this.readUint16("big_endian");
      const spans__iter_name_bytes = this.readBytesSlice(spans__iter_name_length);
      try {
        spans__iter.name = new TextDecoder("utf-8", { fatal: true }).decode(spans__iter_name_bytes);
      } catch (e) {
        throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
      }
      spans__iter.kind = this.readUint8();
      spans__iter.start_unix_nano = this.readUint64("big_endian");
      spans__iter.end_unix_nano = this.readUint64("big_endian");
      spans__iter.status_code = this.readUint8();
      const spans__iter_status_message_length = this.readUint16("big_endian");
      const spans__iter_status_message_bytes = this.readBytesSlice(spans__iter_status_message_length);
      try {
        spans__iter.status_message = new TextDecoder("utf-8", { fatal: true }).decode(spans__iter_status_message_bytes);
      } catch (e) {
        throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
      }
      spans__iter.attributes = [];
      const spans__iter_attributes_length = this.readUint16("big_endian");
      for (let i = 0; i < spans__iter_attributes_length; i++) {
        let spans__iter_attributes__iter: any;
        spans__iter_attributes__iter = {};
        const spans__iter_attributes__iter_key_length = this.readUint8();
        const spans__iter_attributes__iter_key_bytes = this.readBytesSlice(spans__iter_attributes__iter_key_length);
        try {
          spans__iter_attributes__iter.key = new TextDecoder("utf-8", { fatal: true }).decode(spans__iter_attributes__iter_key_bytes);
        } catch (e) {
          throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
        }
        const spans__iter_attributes__iter_value_length = this.readUint16("big_endian");
        const spans__iter_attributes__iter_value_bytes = this.readBytesSlice(spans__iter_attributes__iter_value_length);
        try {
          spans__iter_attributes__iter.value = new TextDecoder("utf-8", { fatal: true }).decode(spans__iter_attributes__iter_value_bytes);
        } catch (e) {
          throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
        }
        spans__iter.attributes.push(spans__iter_attributes__iter);
      }
      spans__iter.events = [];
      const spans__iter_events_length = this.readUint16("big_endian");
      for (let i = 0; i < spans__iter_events_length; i++) {
        let spans__iter_events__iter: any;
        spans__iter_events__iter = {};
        spans__iter_events__iter.ts_unix_nano = this.readUint64("big_endian");
        const spans__iter_events__iter_name_length = this.readUint16("big_endian");
        const spans__iter_events__iter_name_bytes = this.readBytesSlice(spans__iter_events__iter_name_length);
        try {
          spans__iter_events__iter.name = new TextDecoder("utf-8", { fatal: true }).decode(spans__iter_events__iter_name_bytes);
        } catch (e) {
          throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
        }
        spans__iter_events__iter.attributes = [];
        const spans__iter_events__iter_attributes_length = this.readUint8();
        for (let i = 0; i < spans__iter_events__iter_attributes_length; i++) {
          let spans__iter_events__iter_attributes__iter: any;
          spans__iter_events__iter_attributes__iter = {};
          const spans__iter_events__iter_attributes__iter_key_length = this.readUint8();
          const spans__iter_events__iter_attributes__iter_key_bytes = this.readBytesSlice(spans__iter_events__iter_attributes__iter_key_length);
          try {
            spans__iter_events__iter_attributes__iter.key = new TextDecoder("utf-8", { fatal: true }).decode(spans__iter_events__iter_attributes__iter_key_bytes);
          } catch (e) {
            throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
          }
          const spans__iter_events__iter_attributes__iter_value_length = this.readUint16("big_endian");
          const spans__iter_events__iter_attributes__iter_value_bytes = this.readBytesSlice(spans__iter_events__iter_attributes__iter_value_length);
          try {
            spans__iter_events__iter_attributes__iter.value = new TextDecoder("utf-8", { fatal: true }).decode(spans__iter_events__iter_attributes__iter_value_bytes);
          } catch (e) {
            throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
          }
          spans__iter_events__iter.attributes.push(spans__iter_events__iter_attributes__iter);
        }
        spans__iter.events.push(spans__iter_events__iter);
      }
      spans__iter.links = [];
      const spans__iter_links_length = this.readUint8();
      for (let i = 0; i < spans__iter_links_length; i++) {
        let spans__iter_links__iter: any;
        spans__iter_links__iter = {};
        spans__iter_links__iter.trace_id = [];
        for (let i = 0; i < 16; i++) {
          let spans__iter_links__iter_trace_id__iter: any;
          spans__iter_links__iter_trace_id__iter = this.readUint8();
          spans__iter_links__iter.trace_id.push(spans__iter_links__iter_trace_id__iter);
        }
        spans__iter_links__iter.span_id = [];
        for (let i = 0; i < 8; i++) {
          let spans__iter_links__iter_span_id__iter: any;
          spans__iter_links__iter_span_id__iter = this.readUint8();
          spans__iter_links__iter.span_id.push(spans__iter_links__iter_span_id__iter);
        }
        spans__iter_links__iter.attributes = [];
        const spans__iter_links__iter_attributes_length = this.readUint8();
        for (let i = 0; i < spans__iter_links__iter_attributes_length; i++) {
          let spans__iter_links__iter_attributes__iter: any;
          spans__iter_links__iter_attributes__iter = {};
          const spans__iter_links__iter_attributes__iter_key_length = this.readUint8();
          const spans__iter_links__iter_attributes__iter_key_bytes = this.readBytesSlice(spans__iter_links__iter_attributes__iter_key_length);
          try {
            spans__iter_links__iter_attributes__iter.key = new TextDecoder("utf-8", { fatal: true }).decode(spans__iter_links__iter_attributes__iter_key_bytes);
          } catch (e) {
            throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
          }
          const spans__iter_links__iter_attributes__iter_value_length = this.readUint16("big_endian");
          const spans__iter_links__iter_attributes__iter_value_bytes = this.readBytesSlice(spans__iter_links__iter_attributes__iter_value_length);
          try {
            spans__iter_links__iter_attributes__iter.value = new TextDecoder("utf-8", { fatal: true }).decode(spans__iter_links__iter_attributes__iter_value_bytes);
          } catch (e) {
            throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
          }
          spans__iter_links__iter.attributes.push(spans__iter_links__iter_attributes__iter);
        }
        spans__iter.links.push(spans__iter_links__iter);
      }
      value.spans.push(spans__iter);
    }
    return value;
  }
}

export interface ResourceEntryInput {
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint16
   */
  labels: LabelPairInput[];
}

export interface ResourceEntryOutput {
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint16
   */
  labels: LabelPairOutput[];
}

export type ResourceEntry = ResourceEntryOutput;

export class ResourceEntryEncoder extends BitStreamEncoder {
  private compressionDict: Map<string, number> = new Map();

  constructor() {
    super("msb_first");
  }

  encode(value: ResourceEntryInput): Uint8Array {
    // Reset compression dictionary for each encode
    this.compressionDict.clear();

    this.writeUint16(value.labels.length, "big_endian");
    for (let value_labels__iter_index = 0; value_labels__iter_index < value.labels.length; value_labels__iter_index++) {
      const value_labels__iter = value.labels[value_labels__iter_index];
      const encoder_value_labels__iter = new LabelPairEncoder();
      const encoded_value_labels__iter = encoder_value_labels__iter.encode(value_labels__iter);
      for (const byte of encoded_value_labels__iter) {
        this.writeUint8(byte);
      }
    }
    return this.finish();
  }

  /**
   * Calculate the encoded size of a ResourceEntry value.
   * Used for from_after_field computed lengths and buffer pre-allocation.
   */
  calculateSize(value: ResourceEntry): number {
    let size = 0;
    // labels: array (kind: length_prefixed)
    for (const item of value.labels) {
      const labels_itemEncoder = new LabelPairEncoder();
      size += labels_itemEncoder.calculateSize(item);
    }
    size += 2; // length prefix (uint16)
    return size;
  }
}

export class ResourceEntryDecoder extends SeekableBitStreamDecoder {
  constructor(input: Uint8Array | number[] | string, private context?: any) {
    const reader = createReader(input);
    super(reader, "msb_first");
  }

  decode(): ResourceEntryOutput {
    const value: any = {};

    value.labels = [];
    const labels_length = this.readUint16("big_endian");
    for (let i = 0; i < labels_length; i++) {
      let labels__iter: any;
      labels__iter = {};
      const labels__iter_key_length = this.readUint8();
      const labels__iter_key_bytes = this.readBytesSlice(labels__iter_key_length);
      try {
        labels__iter.key = new TextDecoder("utf-8", { fatal: true }).decode(labels__iter_key_bytes);
      } catch (e) {
        throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
      }
      const labels__iter_value_length = this.readUint16("big_endian");
      const labels__iter_value_bytes = this.readBytesSlice(labels__iter_value_length);
      try {
        labels__iter.value = new TextDecoder("utf-8", { fatal: true }).decode(labels__iter_value_bytes);
      } catch (e) {
        throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
      }
      value.labels.push(labels__iter);
    }
    return value;
  }
}

export interface ScopeEntryInput {
  /**
   * String kind: length_prefixed
   * Encoding: utf8
   * Length prefix type: uint8
   */
  name: string;
  /**
   * String kind: length_prefixed
   * Encoding: ascii
   * Length prefix type: uint8
   */
  version: string;
}

export interface ScopeEntryOutput {
  /**
   * String kind: length_prefixed
   * Encoding: utf8
   * Length prefix type: uint8
   */
  name: string;
  /**
   * String kind: length_prefixed
   * Encoding: ascii
   * Length prefix type: uint8
   */
  version: string;
}

export type ScopeEntry = ScopeEntryOutput;

export class ScopeEntryEncoder extends BitStreamEncoder {
  private compressionDict: Map<string, number> = new Map();

  constructor() {
    super("msb_first");
  }

  encode(value: ScopeEntryInput): Uint8Array {
    // Reset compression dictionary for each encode
    this.compressionDict.clear();

    const value_name_bytes = new TextEncoder().encode(value.name);
    this.writeUint8(value_name_bytes.length);
    for (const byte of value_name_bytes) {
      this.writeUint8(byte);
    }
    const value_version_bytes = Array.from(value.version, c => c.charCodeAt(0));
    this.writeUint8(value_version_bytes.length);
    for (const byte of value_version_bytes) {
      this.writeUint8(byte);
    }
    return this.finish();
  }

  /**
   * Calculate the encoded size of a ScopeEntry value.
   * Used for from_after_field computed lengths and buffer pre-allocation.
   */
  calculateSize(value: ScopeEntry): number {
    let size = 0;
    // name: string (utf8)
    size += new TextEncoder().encode(value.name).length;
    // version: string (ascii)
    size += value.version.length;
    return size;
  }
}

export class ScopeEntryDecoder extends SeekableBitStreamDecoder {
  constructor(input: Uint8Array | number[] | string, private context?: any) {
    const reader = createReader(input);
    super(reader, "msb_first");
  }

  decode(): ScopeEntryOutput {
    const value: any = {};

    const name_length = this.readUint8();
    const name_bytes = this.readBytesSlice(name_length);
    try {
      value.name = new TextDecoder("utf-8", { fatal: true }).decode(name_bytes);
    } catch (e) {
      throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
    }
    const version_length = this.readUint8();
    const version_bytes = this.readBytesSlice(version_length);
    value.version = String.fromCharCode(...version_bytes);
    return value;
  }
}

/**
 * One span. References resource and scope dictionaries by index.
 */
export interface SpanInput {
  /**
   * 16-bit Unsigned Integer
   * Fixed-width 16-bit unsigned integer (0-65535). Respects endianness configuration (big-endian or little-endian).
   */
  resource_idx: number;
  /**
   * 16-bit Unsigned Integer
   * Fixed-width 16-bit unsigned integer (0-65535). Respects endianness configuration (big-endian or little-endian).
   */
  scope_idx: number;
  /**
   * Bytes
   * Raw byte array. Sugar for array of uint8 — same wire format, simpler schema definition.
   */
  trace_id: number[];
  /**
   * Bytes
   * Raw byte array. Sugar for array of uint8 — same wire format, simpler schema definition.
   */
  span_id: number[];
  /**
   * Optional
   * Field that may or may not be present. Uses a presence indicator (byte or bit) followed by the value if present.
   */
  parent_span_id: number[] | undefined;
  /**
   * String kind: length_prefixed
   * Encoding: utf8
   * Length prefix type: uint16
   */
  name: string;
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  kind: number;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  start_unix_nano: bigint;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  end_unix_nano: bigint;
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  status_code: number;
  /**
   * String kind: length_prefixed
   * Encoding: utf8
   * Length prefix type: uint16
   */
  status_message: string;
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint16
   */
  attributes: LabelPairInput[];
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint16
   */
  events: SpanEventInput[];
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint8
   */
  links: SpanLinkInput[];
}

/**
 * One span. References resource and scope dictionaries by index.
 */
export interface SpanOutput {
  /**
   * 16-bit Unsigned Integer
   * Fixed-width 16-bit unsigned integer (0-65535). Respects endianness configuration (big-endian or little-endian).
   */
  resource_idx: number;
  /**
   * 16-bit Unsigned Integer
   * Fixed-width 16-bit unsigned integer (0-65535). Respects endianness configuration (big-endian or little-endian).
   */
  scope_idx: number;
  /**
   * Bytes
   * Raw byte array. Sugar for array of uint8 — same wire format, simpler schema definition.
   */
  trace_id: number[];
  /**
   * Bytes
   * Raw byte array. Sugar for array of uint8 — same wire format, simpler schema definition.
   */
  span_id: number[];
  /**
   * Optional
   * Field that may or may not be present. Uses a presence indicator (byte or bit) followed by the value if present.
   */
  parent_span_id: number[] | undefined;
  /**
   * String kind: length_prefixed
   * Encoding: utf8
   * Length prefix type: uint16
   */
  name: string;
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  kind: number;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  start_unix_nano: bigint;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  end_unix_nano: bigint;
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  status_code: number;
  /**
   * String kind: length_prefixed
   * Encoding: utf8
   * Length prefix type: uint16
   */
  status_message: string;
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint16
   */
  attributes: LabelPairOutput[];
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint16
   */
  events: SpanEventOutput[];
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint8
   */
  links: SpanLinkOutput[];
}

export type Span = SpanOutput;

export class SpanEncoder extends BitStreamEncoder {
  private compressionDict: Map<string, number> = new Map();

  constructor() {
    super("msb_first");
  }

  encode(value: SpanInput): Uint8Array {
    // Reset compression dictionary for each encode
    this.compressionDict.clear();

    this.writeUint16(value.resource_idx, "big_endian");
    this.writeUint16(value.scope_idx, "big_endian");
    // Validate fixed-length array
    if (value.trace_id.length !== 16) {
      throw new Error(`Array 'trace_id' must have exactly 16 elements, got ${value.trace_id.length}`);
    }
    for (let value_trace_id__iter_index = 0; value_trace_id__iter_index < value.trace_id.length; value_trace_id__iter_index++) {
      const value_trace_id__iter = value.trace_id[value_trace_id__iter_index];
      this.writeUint8(value_trace_id__iter);
    }
    // Validate fixed-length array
    if (value.span_id.length !== 8) {
      throw new Error(`Array 'span_id' must have exactly 8 elements, got ${value.span_id.length}`);
    }
    for (let value_span_id__iter_index = 0; value_span_id__iter_index < value.span_id.length; value_span_id__iter_index++) {
      const value_span_id__iter = value.span_id[value_span_id__iter_index];
      this.writeUint8(value_span_id__iter);
    }
    if (value.parent_span_id === undefined || value.parent_span_id === null) {
      this.writeUint8(0);
    } else {
      this.writeUint8(1);
      // Validate fixed-length array
      if (value.parent_span_id.length !== 8) {
        throw new Error(`Array 'parent_span_id' must have exactly 8 elements, got ${value.parent_span_id.length}`);
      }
      for (let value_parent_span_id__iter_index = 0; value_parent_span_id__iter_index < value.parent_span_id.length; value_parent_span_id__iter_index++) {
        const value_parent_span_id__iter = value.parent_span_id[value_parent_span_id__iter_index];
        this.writeUint8(value_parent_span_id__iter);
      }
    }
    const value_name_bytes = new TextEncoder().encode(value.name);
    this.writeUint16(value_name_bytes.length, "big_endian");
    for (const byte of value_name_bytes) {
      this.writeUint8(byte);
    }
    this.writeUint8(value.kind);
    this.writeUint64(value.start_unix_nano, "big_endian");
    this.writeUint64(value.end_unix_nano, "big_endian");
    this.writeUint8(value.status_code);
    const value_status_message_bytes = new TextEncoder().encode(value.status_message);
    this.writeUint16(value_status_message_bytes.length, "big_endian");
    for (const byte of value_status_message_bytes) {
      this.writeUint8(byte);
    }
    this.writeUint16(value.attributes.length, "big_endian");
    for (let value_attributes__iter_index = 0; value_attributes__iter_index < value.attributes.length; value_attributes__iter_index++) {
      const value_attributes__iter = value.attributes[value_attributes__iter_index];
      const encoder_value_attributes__iter = new LabelPairEncoder();
      const encoded_value_attributes__iter = encoder_value_attributes__iter.encode(value_attributes__iter);
      for (const byte of encoded_value_attributes__iter) {
        this.writeUint8(byte);
      }
    }
    this.writeUint16(value.events.length, "big_endian");
    for (let value_events__iter_index = 0; value_events__iter_index < value.events.length; value_events__iter_index++) {
      const value_events__iter = value.events[value_events__iter_index];
      const encoder_value_events__iter = new SpanEventEncoder();
      const encoded_value_events__iter = encoder_value_events__iter.encode(value_events__iter);
      for (const byte of encoded_value_events__iter) {
        this.writeUint8(byte);
      }
    }
    this.writeUint8(value.links.length);
    for (let value_links__iter_index = 0; value_links__iter_index < value.links.length; value_links__iter_index++) {
      const value_links__iter = value.links[value_links__iter_index];
      const encoder_value_links__iter = new SpanLinkEncoder();
      const encoded_value_links__iter = encoder_value_links__iter.encode(value_links__iter);
      for (const byte of encoded_value_links__iter) {
        this.writeUint8(byte);
      }
    }
    return this.finish();
  }

  /**
   * Calculate the encoded size of a Span value.
   * Used for from_after_field computed lengths and buffer pre-allocation.
   */
  calculateSize(value: Span): number {
    let size = 0;
    size += 4; // resource_idx + scope_idx
    // trace_id: bytes (kind: fixed)
    size += value.trace_id.length;
    // span_id: bytes (kind: fixed)
    size += value.span_id.length;
    // parent_span_id: optional (presence: uint8)
    size += 1; // parent_span_id presence indicator
    if (value.parent_span_id !== undefined && value.parent_span_id !== null) {
      // parent_span_id: bytes (kind: fixed)
      size += value.parent_span_id.length;
    }
    // name: string (utf8)
    size += new TextEncoder().encode(value.name).length;
    size += 18; // kind + start_unix_nano + end_unix_nano + status_code
    // status_message: string (utf8)
    size += new TextEncoder().encode(value.status_message).length;
    // attributes: array (kind: length_prefixed)
    for (const item of value.attributes) {
      const attributes_itemEncoder = new LabelPairEncoder();
      size += attributes_itemEncoder.calculateSize(item);
    }
    size += 2; // length prefix (uint16)
    // events: array (kind: length_prefixed)
    for (const item of value.events) {
      const events_itemEncoder = new SpanEventEncoder();
      size += events_itemEncoder.calculateSize(item);
    }
    size += 2; // length prefix (uint16)
    // links: array (kind: length_prefixed)
    for (const item of value.links) {
      const links_itemEncoder = new SpanLinkEncoder();
      size += links_itemEncoder.calculateSize(item);
    }
    size += 1; // length prefix (uint8)
    return size;
  }
}

export class SpanDecoder extends SeekableBitStreamDecoder {
  constructor(input: Uint8Array | number[] | string, private context?: any) {
    const reader = createReader(input);
    super(reader, "msb_first");
  }

  decode(): SpanOutput {
    const value: any = {};

    value.resource_idx = this.readUint16("big_endian");
    value.scope_idx = this.readUint16("big_endian");
    value.trace_id = [];
    for (let i = 0; i < 16; i++) {
      let trace_id__iter: any;
      trace_id__iter = this.readUint8();
      value.trace_id.push(trace_id__iter);
    }
    value.span_id = [];
    for (let i = 0; i < 8; i++) {
      let span_id__iter: any;
      span_id__iter = this.readUint8();
      value.span_id.push(span_id__iter);
    }
    const parent_span_id_present = this.readUint8();
    if (parent_span_id_present !== 0) {
      value.parent_span_id = [];
      for (let i = 0; i < 8; i++) {
        let parent_span_id__iter: any;
        parent_span_id__iter = this.readUint8();
        value.parent_span_id.push(parent_span_id__iter);
      }
    }
    const name_length = this.readUint16("big_endian");
    const name_bytes = this.readBytesSlice(name_length);
    try {
      value.name = new TextDecoder("utf-8", { fatal: true }).decode(name_bytes);
    } catch (e) {
      throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
    }
    value.kind = this.readUint8();
    value.start_unix_nano = this.readUint64("big_endian");
    value.end_unix_nano = this.readUint64("big_endian");
    value.status_code = this.readUint8();
    const status_message_length = this.readUint16("big_endian");
    const status_message_bytes = this.readBytesSlice(status_message_length);
    try {
      value.status_message = new TextDecoder("utf-8", { fatal: true }).decode(status_message_bytes);
    } catch (e) {
      throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
    }
    value.attributes = [];
    const attributes_length = this.readUint16("big_endian");
    for (let i = 0; i < attributes_length; i++) {
      let attributes__iter: any;
      attributes__iter = {};
      const attributes__iter_key_length = this.readUint8();
      const attributes__iter_key_bytes = this.readBytesSlice(attributes__iter_key_length);
      try {
        attributes__iter.key = new TextDecoder("utf-8", { fatal: true }).decode(attributes__iter_key_bytes);
      } catch (e) {
        throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
      }
      const attributes__iter_value_length = this.readUint16("big_endian");
      const attributes__iter_value_bytes = this.readBytesSlice(attributes__iter_value_length);
      try {
        attributes__iter.value = new TextDecoder("utf-8", { fatal: true }).decode(attributes__iter_value_bytes);
      } catch (e) {
        throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
      }
      value.attributes.push(attributes__iter);
    }
    value.events = [];
    const events_length = this.readUint16("big_endian");
    for (let i = 0; i < events_length; i++) {
      let events__iter: any;
      events__iter = {};
      events__iter.ts_unix_nano = this.readUint64("big_endian");
      const events__iter_name_length = this.readUint16("big_endian");
      const events__iter_name_bytes = this.readBytesSlice(events__iter_name_length);
      try {
        events__iter.name = new TextDecoder("utf-8", { fatal: true }).decode(events__iter_name_bytes);
      } catch (e) {
        throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
      }
      events__iter.attributes = [];
      const events__iter_attributes_length = this.readUint8();
      for (let i = 0; i < events__iter_attributes_length; i++) {
        let events__iter_attributes__iter: any;
        events__iter_attributes__iter = {};
        const events__iter_attributes__iter_key_length = this.readUint8();
        const events__iter_attributes__iter_key_bytes = this.readBytesSlice(events__iter_attributes__iter_key_length);
        try {
          events__iter_attributes__iter.key = new TextDecoder("utf-8", { fatal: true }).decode(events__iter_attributes__iter_key_bytes);
        } catch (e) {
          throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
        }
        const events__iter_attributes__iter_value_length = this.readUint16("big_endian");
        const events__iter_attributes__iter_value_bytes = this.readBytesSlice(events__iter_attributes__iter_value_length);
        try {
          events__iter_attributes__iter.value = new TextDecoder("utf-8", { fatal: true }).decode(events__iter_attributes__iter_value_bytes);
        } catch (e) {
          throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
        }
        events__iter.attributes.push(events__iter_attributes__iter);
      }
      value.events.push(events__iter);
    }
    value.links = [];
    const links_length = this.readUint8();
    for (let i = 0; i < links_length; i++) {
      let links__iter: any;
      links__iter = {};
      links__iter.trace_id = [];
      for (let i = 0; i < 16; i++) {
        let links__iter_trace_id__iter: any;
        links__iter_trace_id__iter = this.readUint8();
        links__iter.trace_id.push(links__iter_trace_id__iter);
      }
      links__iter.span_id = [];
      for (let i = 0; i < 8; i++) {
        let links__iter_span_id__iter: any;
        links__iter_span_id__iter = this.readUint8();
        links__iter.span_id.push(links__iter_span_id__iter);
      }
      links__iter.attributes = [];
      const links__iter_attributes_length = this.readUint8();
      for (let i = 0; i < links__iter_attributes_length; i++) {
        let links__iter_attributes__iter: any;
        links__iter_attributes__iter = {};
        const links__iter_attributes__iter_key_length = this.readUint8();
        const links__iter_attributes__iter_key_bytes = this.readBytesSlice(links__iter_attributes__iter_key_length);
        try {
          links__iter_attributes__iter.key = new TextDecoder("utf-8", { fatal: true }).decode(links__iter_attributes__iter_key_bytes);
        } catch (e) {
          throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
        }
        const links__iter_attributes__iter_value_length = this.readUint16("big_endian");
        const links__iter_attributes__iter_value_bytes = this.readBytesSlice(links__iter_attributes__iter_value_length);
        try {
          links__iter_attributes__iter.value = new TextDecoder("utf-8", { fatal: true }).decode(links__iter_attributes__iter_value_bytes);
        } catch (e) {
          throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
        }
        links__iter.attributes.push(links__iter_attributes__iter);
      }
      value.links.push(links__iter);
    }
    return value;
  }
}

export interface SpanEventInput {
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  ts_unix_nano: bigint;
  /**
   * String kind: length_prefixed
   * Encoding: utf8
   * Length prefix type: uint16
   */
  name: string;
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint8
   */
  attributes: LabelPairInput[];
}

export interface SpanEventOutput {
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  ts_unix_nano: bigint;
  /**
   * String kind: length_prefixed
   * Encoding: utf8
   * Length prefix type: uint16
   */
  name: string;
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint8
   */
  attributes: LabelPairOutput[];
}

export type SpanEvent = SpanEventOutput;

export class SpanEventEncoder extends BitStreamEncoder {
  private compressionDict: Map<string, number> = new Map();

  constructor() {
    super("msb_first");
  }

  encode(value: SpanEventInput): Uint8Array {
    // Reset compression dictionary for each encode
    this.compressionDict.clear();

    this.writeUint64(value.ts_unix_nano, "big_endian");
    const value_name_bytes = new TextEncoder().encode(value.name);
    this.writeUint16(value_name_bytes.length, "big_endian");
    for (const byte of value_name_bytes) {
      this.writeUint8(byte);
    }
    this.writeUint8(value.attributes.length);
    for (let value_attributes__iter_index = 0; value_attributes__iter_index < value.attributes.length; value_attributes__iter_index++) {
      const value_attributes__iter = value.attributes[value_attributes__iter_index];
      const encoder_value_attributes__iter = new LabelPairEncoder();
      const encoded_value_attributes__iter = encoder_value_attributes__iter.encode(value_attributes__iter);
      for (const byte of encoded_value_attributes__iter) {
        this.writeUint8(byte);
      }
    }
    return this.finish();
  }

  /**
   * Calculate the encoded size of a SpanEvent value.
   * Used for from_after_field computed lengths and buffer pre-allocation.
   */
  calculateSize(value: SpanEvent): number {
    let size = 0;
    size += 8; // ts_unix_nano
    // name: string (utf8)
    size += new TextEncoder().encode(value.name).length;
    // attributes: array (kind: length_prefixed)
    for (const item of value.attributes) {
      const attributes_itemEncoder = new LabelPairEncoder();
      size += attributes_itemEncoder.calculateSize(item);
    }
    size += 1; // length prefix (uint8)
    return size;
  }
}

export class SpanEventDecoder extends SeekableBitStreamDecoder {
  constructor(input: Uint8Array | number[] | string, private context?: any) {
    const reader = createReader(input);
    super(reader, "msb_first");
  }

  decode(): SpanEventOutput {
    const value: any = {};

    value.ts_unix_nano = this.readUint64("big_endian");
    const name_length = this.readUint16("big_endian");
    const name_bytes = this.readBytesSlice(name_length);
    try {
      value.name = new TextDecoder("utf-8", { fatal: true }).decode(name_bytes);
    } catch (e) {
      throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
    }
    value.attributes = [];
    const attributes_length = this.readUint8();
    for (let i = 0; i < attributes_length; i++) {
      let attributes__iter: any;
      attributes__iter = {};
      const attributes__iter_key_length = this.readUint8();
      const attributes__iter_key_bytes = this.readBytesSlice(attributes__iter_key_length);
      try {
        attributes__iter.key = new TextDecoder("utf-8", { fatal: true }).decode(attributes__iter_key_bytes);
      } catch (e) {
        throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
      }
      const attributes__iter_value_length = this.readUint16("big_endian");
      const attributes__iter_value_bytes = this.readBytesSlice(attributes__iter_value_length);
      try {
        attributes__iter.value = new TextDecoder("utf-8", { fatal: true }).decode(attributes__iter_value_bytes);
      } catch (e) {
        throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
      }
      value.attributes.push(attributes__iter);
    }
    return value;
  }
}

export interface SpanLinkInput {
  /**
   * Bytes
   * Raw byte array. Sugar for array of uint8 — same wire format, simpler schema definition.
   */
  trace_id: number[];
  /**
   * Bytes
   * Raw byte array. Sugar for array of uint8 — same wire format, simpler schema definition.
   */
  span_id: number[];
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint8
   */
  attributes: LabelPairInput[];
}

export interface SpanLinkOutput {
  /**
   * Bytes
   * Raw byte array. Sugar for array of uint8 — same wire format, simpler schema definition.
   */
  trace_id: number[];
  /**
   * Bytes
   * Raw byte array. Sugar for array of uint8 — same wire format, simpler schema definition.
   */
  span_id: number[];
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint8
   */
  attributes: LabelPairOutput[];
}

export type SpanLink = SpanLinkOutput;

export class SpanLinkEncoder extends BitStreamEncoder {
  private compressionDict: Map<string, number> = new Map();

  constructor() {
    super("msb_first");
  }

  encode(value: SpanLinkInput): Uint8Array {
    // Reset compression dictionary for each encode
    this.compressionDict.clear();

    // Validate fixed-length array
    if (value.trace_id.length !== 16) {
      throw new Error(`Array 'trace_id' must have exactly 16 elements, got ${value.trace_id.length}`);
    }
    for (let value_trace_id__iter_index = 0; value_trace_id__iter_index < value.trace_id.length; value_trace_id__iter_index++) {
      const value_trace_id__iter = value.trace_id[value_trace_id__iter_index];
      this.writeUint8(value_trace_id__iter);
    }
    // Validate fixed-length array
    if (value.span_id.length !== 8) {
      throw new Error(`Array 'span_id' must have exactly 8 elements, got ${value.span_id.length}`);
    }
    for (let value_span_id__iter_index = 0; value_span_id__iter_index < value.span_id.length; value_span_id__iter_index++) {
      const value_span_id__iter = value.span_id[value_span_id__iter_index];
      this.writeUint8(value_span_id__iter);
    }
    this.writeUint8(value.attributes.length);
    for (let value_attributes__iter_index = 0; value_attributes__iter_index < value.attributes.length; value_attributes__iter_index++) {
      const value_attributes__iter = value.attributes[value_attributes__iter_index];
      const encoder_value_attributes__iter = new LabelPairEncoder();
      const encoded_value_attributes__iter = encoder_value_attributes__iter.encode(value_attributes__iter);
      for (const byte of encoded_value_attributes__iter) {
        this.writeUint8(byte);
      }
    }
    return this.finish();
  }

  /**
   * Calculate the encoded size of a SpanLink value.
   * Used for from_after_field computed lengths and buffer pre-allocation.
   */
  calculateSize(value: SpanLink): number {
    let size = 0;
    // trace_id: bytes (kind: fixed)
    size += value.trace_id.length;
    // span_id: bytes (kind: fixed)
    size += value.span_id.length;
    // attributes: array (kind: length_prefixed)
    for (const item of value.attributes) {
      const attributes_itemEncoder = new LabelPairEncoder();
      size += attributes_itemEncoder.calculateSize(item);
    }
    size += 1; // length prefix (uint8)
    return size;
  }
}

export class SpanLinkDecoder extends SeekableBitStreamDecoder {
  constructor(input: Uint8Array | number[] | string, private context?: any) {
    const reader = createReader(input);
    super(reader, "msb_first");
  }

  decode(): SpanLinkOutput {
    const value: any = {};

    value.trace_id = [];
    for (let i = 0; i < 16; i++) {
      let trace_id__iter: any;
      trace_id__iter = this.readUint8();
      value.trace_id.push(trace_id__iter);
    }
    value.span_id = [];
    for (let i = 0; i < 8; i++) {
      let span_id__iter: any;
      span_id__iter = this.readUint8();
      value.span_id.push(span_id__iter);
    }
    value.attributes = [];
    const attributes_length = this.readUint8();
    for (let i = 0; i < attributes_length; i++) {
      let attributes__iter: any;
      attributes__iter = {};
      const attributes__iter_key_length = this.readUint8();
      const attributes__iter_key_bytes = this.readBytesSlice(attributes__iter_key_length);
      try {
        attributes__iter.key = new TextDecoder("utf-8", { fatal: true }).decode(attributes__iter_key_bytes);
      } catch (e) {
        throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
      }
      const attributes__iter_value_length = this.readUint16("big_endian");
      const attributes__iter_value_bytes = this.readBytesSlice(attributes__iter_value_length);
      try {
        attributes__iter.value = new TextDecoder("utf-8", { fatal: true }).decode(attributes__iter_value_bytes);
      } catch (e) {
        throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
      }
      value.attributes.push(attributes__iter);
    }
    return value;
  }
}

/**
 * Body of a profiles Batch after decompression. v0.1 stores opaque pprof_gz blobs; the structured schema lands in v0.4.
 */
export interface ProfilesBatchInput {
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint32
   */
  samples: ProfileBlobInput[];
}

/**
 * Body of a profiles Batch after decompression. v0.1 stores opaque pprof_gz blobs; the structured schema lands in v0.4.
 */
export interface ProfilesBatchOutput {
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint32
   */
  samples: ProfileBlobOutput[];
}

export type ProfilesBatch = ProfilesBatchOutput;

export class ProfilesBatchEncoder extends BitStreamEncoder {
  private compressionDict: Map<string, number> = new Map();

  constructor() {
    super("msb_first");
  }

  encode(value: ProfilesBatchInput): Uint8Array {
    // Reset compression dictionary for each encode
    this.compressionDict.clear();

    this.writeUint32(value.samples.length, "big_endian");
    for (let value_samples__iter_index = 0; value_samples__iter_index < value.samples.length; value_samples__iter_index++) {
      const value_samples__iter = value.samples[value_samples__iter_index];
      const encoder_value_samples__iter = new ProfileBlobEncoder();
      const encoded_value_samples__iter = encoder_value_samples__iter.encode(value_samples__iter);
      for (const byte of encoded_value_samples__iter) {
        this.writeUint8(byte);
      }
    }
    return this.finish();
  }

  /**
   * Calculate the encoded size of a ProfilesBatch value.
   * Used for from_after_field computed lengths and buffer pre-allocation.
   */
  calculateSize(value: ProfilesBatch): number {
    let size = 0;
    // samples: array (kind: length_prefixed)
    for (const item of value.samples) {
      const samples_itemEncoder = new ProfileBlobEncoder();
      size += samples_itemEncoder.calculateSize(item);
    }
    size += 4; // length prefix (uint32)
    return size;
  }
}

export class ProfilesBatchDecoder extends SeekableBitStreamDecoder {
  constructor(input: Uint8Array | number[] | string, private context?: any) {
    const reader = createReader(input);
    super(reader, "msb_first");
  }

  decode(): ProfilesBatchOutput {
    const value: any = {};

    value.samples = [];
    const samples_length = this.readUint32("big_endian");
    for (let i = 0; i < samples_length; i++) {
      let samples__iter: any;
      samples__iter = {};
      samples__iter.ts_unix_nano = this.readUint64("big_endian");
      samples__iter.duration_nano = this.readUint64("big_endian");
      samples__iter.labels = [];
      const samples__iter_labels_length = this.readUint16("big_endian");
      for (let i = 0; i < samples__iter_labels_length; i++) {
        let samples__iter_labels__iter: any;
        samples__iter_labels__iter = {};
        const samples__iter_labels__iter_key_length = this.readUint8();
        const samples__iter_labels__iter_key_bytes = this.readBytesSlice(samples__iter_labels__iter_key_length);
        try {
          samples__iter_labels__iter.key = new TextDecoder("utf-8", { fatal: true }).decode(samples__iter_labels__iter_key_bytes);
        } catch (e) {
          throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
        }
        const samples__iter_labels__iter_value_length = this.readUint16("big_endian");
        const samples__iter_labels__iter_value_bytes = this.readBytesSlice(samples__iter_labels__iter_value_length);
        try {
          samples__iter_labels__iter.value = new TextDecoder("utf-8", { fatal: true }).decode(samples__iter_labels__iter_value_bytes);
        } catch (e) {
          throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
        }
        samples__iter.labels.push(samples__iter_labels__iter);
      }
      samples__iter.format = this.readUint8();
      samples__iter.data = [];
      const samples__iter_data_length = this.readUint32("big_endian");
      for (let i = 0; i < samples__iter_data_length; i++) {
        let samples__iter_data__iter: any;
        samples__iter_data__iter = this.readUint8();
        samples__iter.data.push(samples__iter_data__iter);
      }
      value.samples.push(samples__iter);
    }
    return value;
  }
}

export interface ProfileBlobInput {
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  ts_unix_nano: bigint;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  duration_nano: bigint;
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint16
   */
  labels: LabelPairInput[];
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  format: number;
  /**
   * Bytes
   * Raw byte array. Sugar for array of uint8 — same wire format, simpler schema definition.
   */
  data: number[];
}

export interface ProfileBlobOutput {
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  ts_unix_nano: bigint;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  duration_nano: bigint;
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint16
   */
  labels: LabelPairOutput[];
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  format: number;
  /**
   * Bytes
   * Raw byte array. Sugar for array of uint8 — same wire format, simpler schema definition.
   */
  data: number[];
}

export type ProfileBlob = ProfileBlobOutput;

export class ProfileBlobEncoder extends BitStreamEncoder {
  private compressionDict: Map<string, number> = new Map();

  constructor() {
    super("msb_first");
  }

  encode(value: ProfileBlobInput): Uint8Array {
    // Reset compression dictionary for each encode
    this.compressionDict.clear();

    this.writeUint64(value.ts_unix_nano, "big_endian");
    this.writeUint64(value.duration_nano, "big_endian");
    this.writeUint16(value.labels.length, "big_endian");
    for (let value_labels__iter_index = 0; value_labels__iter_index < value.labels.length; value_labels__iter_index++) {
      const value_labels__iter = value.labels[value_labels__iter_index];
      const encoder_value_labels__iter = new LabelPairEncoder();
      const encoded_value_labels__iter = encoder_value_labels__iter.encode(value_labels__iter);
      for (const byte of encoded_value_labels__iter) {
        this.writeUint8(byte);
      }
    }
    this.writeUint8(value.format);
    this.writeUint32(value.data.length, "big_endian");
    for (let value_data__iter_index = 0; value_data__iter_index < value.data.length; value_data__iter_index++) {
      const value_data__iter = value.data[value_data__iter_index];
      this.writeUint8(value_data__iter);
    }
    return this.finish();
  }

  /**
   * Calculate the encoded size of a ProfileBlob value.
   * Used for from_after_field computed lengths and buffer pre-allocation.
   */
  calculateSize(value: ProfileBlob): number {
    let size = 0;
    size += 16; // ts_unix_nano + duration_nano
    // labels: array (kind: length_prefixed)
    for (const item of value.labels) {
      const labels_itemEncoder = new LabelPairEncoder();
      size += labels_itemEncoder.calculateSize(item);
    }
    size += 3; // length prefix (uint16) + format
    // data: bytes (kind: length_prefixed)
    size += value.data.length;
    size += 4; // length prefix (uint32)
    return size;
  }
}

export class ProfileBlobDecoder extends SeekableBitStreamDecoder {
  constructor(input: Uint8Array | number[] | string, private context?: any) {
    const reader = createReader(input);
    super(reader, "msb_first");
  }

  decode(): ProfileBlobOutput {
    const value: any = {};

    value.ts_unix_nano = this.readUint64("big_endian");
    value.duration_nano = this.readUint64("big_endian");
    value.labels = [];
    const labels_length = this.readUint16("big_endian");
    for (let i = 0; i < labels_length; i++) {
      let labels__iter: any;
      labels__iter = {};
      const labels__iter_key_length = this.readUint8();
      const labels__iter_key_bytes = this.readBytesSlice(labels__iter_key_length);
      try {
        labels__iter.key = new TextDecoder("utf-8", { fatal: true }).decode(labels__iter_key_bytes);
      } catch (e) {
        throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
      }
      const labels__iter_value_length = this.readUint16("big_endian");
      const labels__iter_value_bytes = this.readBytesSlice(labels__iter_value_length);
      try {
        labels__iter.value = new TextDecoder("utf-8", { fatal: true }).decode(labels__iter_value_bytes);
      } catch (e) {
        throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
      }
      value.labels.push(labels__iter);
    }
    value.format = this.readUint8();
    value.data = [];
    const data_length = this.readUint32("big_endian");
    for (let i = 0; i < data_length; i++) {
      let data__iter: any;
      data__iter = this.readUint8();
      value.data.push(data__iter);
    }
    return value;
  }
}

/**
 * Body of a Batch whose signal is Dummy (0xFF). v0.1-only placeholder used to exercise the storage pipeline before any real signal lands. Goes away when the first real signal arrives.
 */
export interface DummyBatchInput {
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint32
   */
  records: DummyRecordInput[];
}

/**
 * Body of a Batch whose signal is Dummy (0xFF). v0.1-only placeholder used to exercise the storage pipeline before any real signal lands. Goes away when the first real signal arrives.
 */
export interface DummyBatchOutput {
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint32
   */
  records: DummyRecordOutput[];
}

export type DummyBatch = DummyBatchOutput;

export class DummyBatchEncoder extends BitStreamEncoder {
  private compressionDict: Map<string, number> = new Map();

  constructor() {
    super("msb_first");
  }

  encode(value: DummyBatchInput): Uint8Array {
    // Reset compression dictionary for each encode
    this.compressionDict.clear();

    this.writeUint32(value.records.length, "big_endian");
    for (let value_records__iter_index = 0; value_records__iter_index < value.records.length; value_records__iter_index++) {
      const value_records__iter = value.records[value_records__iter_index];
      const encoder_value_records__iter = new DummyRecordEncoder();
      const encoded_value_records__iter = encoder_value_records__iter.encode(value_records__iter);
      for (const byte of encoded_value_records__iter) {
        this.writeUint8(byte);
      }
    }
    return this.finish();
  }

  /**
   * Calculate the encoded size of a DummyBatch value.
   * Used for from_after_field computed lengths and buffer pre-allocation.
   */
  calculateSize(value: DummyBatch): number {
    let size = 0;
    // records: array (kind: length_prefixed)
    for (const item of value.records) {
      const records_itemEncoder = new DummyRecordEncoder();
      size += records_itemEncoder.calculateSize(item);
    }
    size += 4; // length prefix (uint32)
    return size;
  }
}

export class DummyBatchDecoder extends SeekableBitStreamDecoder {
  constructor(input: Uint8Array | number[] | string, private context?: any) {
    const reader = createReader(input);
    super(reader, "msb_first");
  }

  decode(): DummyBatchOutput {
    const value: any = {};

    value.records = [];
    const records_length = this.readUint32("big_endian");
    for (let i = 0; i < records_length; i++) {
      let records__iter: any;
      records__iter = {};
      records__iter.ts_unix_nano = this.readUint64("big_endian");
      const records__iter_key_length = this.readUint16("big_endian");
      const records__iter_key_bytes = this.readBytesSlice(records__iter_key_length);
      try {
        records__iter.key = new TextDecoder("utf-8", { fatal: true }).decode(records__iter_key_bytes);
      } catch (e) {
        throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
      }
      records__iter.value = [];
      const records__iter_value_length = this.readUint32("big_endian");
      for (let i = 0; i < records__iter_value_length; i++) {
        let records__iter_value__iter: any;
        records__iter_value__iter = this.readUint8();
        records__iter.value.push(records__iter_value__iter);
      }
      value.records.push(records__iter);
    }
    return value;
  }
}

/**
 * v0.1-only record type. A timestamp, an arbitrary key, and an opaque value. The storage layer treats this as the single record shape until real signals come online.
 */
export interface DummyRecordInput {
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  ts_unix_nano: bigint;
  /**
   * String kind: length_prefixed
   * Encoding: utf8
   * Length prefix type: uint16
   */
  key: string;
  /**
   * Bytes
   * Raw byte array. Sugar for array of uint8 — same wire format, simpler schema definition.
   */
  value: number[];
}

/**
 * v0.1-only record type. A timestamp, an arbitrary key, and an opaque value. The storage layer treats this as the single record shape until real signals come online.
 */
export interface DummyRecordOutput {
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  ts_unix_nano: bigint;
  /**
   * String kind: length_prefixed
   * Encoding: utf8
   * Length prefix type: uint16
   */
  key: string;
  /**
   * Bytes
   * Raw byte array. Sugar for array of uint8 — same wire format, simpler schema definition.
   */
  value: number[];
}

export type DummyRecord = DummyRecordOutput;

export class DummyRecordEncoder extends BitStreamEncoder {
  private compressionDict: Map<string, number> = new Map();

  constructor() {
    super("msb_first");
  }

  encode(value: DummyRecordInput): Uint8Array {
    // Reset compression dictionary for each encode
    this.compressionDict.clear();

    this.writeUint64(value.ts_unix_nano, "big_endian");
    const value_key_bytes = new TextEncoder().encode(value.key);
    this.writeUint16(value_key_bytes.length, "big_endian");
    for (const byte of value_key_bytes) {
      this.writeUint8(byte);
    }
    this.writeUint32(value.value.length, "big_endian");
    for (let value_value__iter_index = 0; value_value__iter_index < value.value.length; value_value__iter_index++) {
      const value_value__iter = value.value[value_value__iter_index];
      this.writeUint8(value_value__iter);
    }
    return this.finish();
  }

  /**
   * Calculate the encoded size of a DummyRecord value.
   * Used for from_after_field computed lengths and buffer pre-allocation.
   */
  calculateSize(value: DummyRecord): number {
    let size = 0;
    size += 8; // ts_unix_nano
    // key: string (utf8)
    size += new TextEncoder().encode(value.key).length;
    // value: bytes (kind: length_prefixed)
    size += value.value.length;
    size += 4; // length prefix (uint32)
    return size;
  }
}

export class DummyRecordDecoder extends SeekableBitStreamDecoder {
  constructor(input: Uint8Array | number[] | string, private context?: any) {
    const reader = createReader(input);
    super(reader, "msb_first");
  }

  decode(): DummyRecordOutput {
    const value: any = {};

    value.ts_unix_nano = this.readUint64("big_endian");
    const key_length = this.readUint16("big_endian");
    const key_bytes = this.readBytesSlice(key_length);
    try {
      value.key = new TextDecoder("utf-8", { fatal: true }).decode(key_bytes);
    } catch (e) {
      throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
    }
    value.value = [];
    const value_length = this.readUint32("big_endian");
    for (let i = 0; i < value_length; i++) {
      let value__iter: any;
      value__iter = this.readUint8();
      value.value.push(value__iter);
    }
    return value;
  }
}

/**
 * Protocol-v2 canonical structured metrics payload. Unlike MetricsBatch v1, structured values are never exploded into synthetic scalar series. A fixed magic prefix makes WAL replay and defensive decoders distinguish it from positional v1 payloads.
 */
export interface MetricsBatchV2Input {
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint32
   */
  descriptors: MetricDescriptorV2Input[];
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint32
   */
  points: MetricPointV2Input[];
}

/**
 * Protocol-v2 canonical structured metrics payload. Unlike MetricsBatch v1, structured values are never exploded into synthetic scalar series. A fixed magic prefix makes WAL replay and defensive decoders distinguish it from positional v1 payloads.
 */
export interface MetricsBatchV2Output {
  /**
   * 32-bit Unsigned Integer
   * Fixed-width 32-bit unsigned integer (0-4294967295). Respects endianness configuration.
   */
  magic: number;
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint32
   */
  descriptors: MetricDescriptorV2Output[];
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint32
   */
  points: MetricPointV2Output[];
}

export type MetricsBatchV2 = MetricsBatchV2Output;

export class MetricsBatchV2Encoder extends BitStreamEncoder {
  private compressionDict: Map<string, number> = new Map();

  constructor() {
    super("msb_first");
  }

  encode(value: MetricsBatchV2Input): Uint8Array {
    // Reset compression dictionary for each encode
    this.compressionDict.clear();

    this.writeUint32(1397568000, "big_endian");
    this.writeUint32(value.descriptors.length, "big_endian");
    for (let value_descriptors__iter_index = 0; value_descriptors__iter_index < value.descriptors.length; value_descriptors__iter_index++) {
      const value_descriptors__iter = value.descriptors[value_descriptors__iter_index];
      const encoder_value_descriptors__iter = new MetricDescriptorV2Encoder();
      const encoded_value_descriptors__iter = encoder_value_descriptors__iter.encode(value_descriptors__iter);
      for (const byte of encoded_value_descriptors__iter) {
        this.writeUint8(byte);
      }
    }
    this.writeUint32(value.points.length, "big_endian");
    for (let value_points__iter_index = 0; value_points__iter_index < value.points.length; value_points__iter_index++) {
      const value_points__iter = value.points[value_points__iter_index];
      const encoder_value_points__iter = new MetricPointV2Encoder();
      const encoded_value_points__iter = encoder_value_points__iter.encode(value_points__iter);
      for (const byte of encoded_value_points__iter) {
        this.writeUint8(byte);
      }
    }
    return this.finish();
  }

  /**
   * Calculate the encoded size of a MetricsBatchV2 value.
   * Used for from_after_field computed lengths and buffer pre-allocation.
   */
  calculateSize(value: MetricsBatchV2): number {
    let size = 0;
    size += 4; // magic (const)
    // descriptors: array (kind: length_prefixed)
    for (const item of value.descriptors) {
      const descriptors_itemEncoder = new MetricDescriptorV2Encoder();
      size += descriptors_itemEncoder.calculateSize(item);
    }
    size += 4; // length prefix (uint32)
    // points: array (kind: length_prefixed)
    for (const item of value.points) {
      const points_itemEncoder = new MetricPointV2Encoder();
      size += points_itemEncoder.calculateSize(item);
    }
    size += 4; // length prefix (uint32)
    return size;
  }
}

export class MetricsBatchV2Decoder extends SeekableBitStreamDecoder {
  constructor(input: Uint8Array | number[] | string, private context?: any) {
    const reader = createReader(input);
    super(reader, "msb_first");
  }

  decode(): MetricsBatchV2Output {
    const value: any = {};

    value.magic = this.readUint32("big_endian");
    value.descriptors = [];
    const descriptors_length = this.readUint32("big_endian");
    for (let i = 0; i < descriptors_length; i++) {
      let descriptors__iter: any;
      descriptors__iter = {};
      descriptors__iter.id = this.readUint32("big_endian");
      const descriptors__iter_name_length = this.readUint16("big_endian");
      const descriptors__iter_name_bytes = this.readBytesSlice(descriptors__iter_name_length);
      try {
        descriptors__iter.name = new TextDecoder("utf-8", { fatal: true }).decode(descriptors__iter_name_bytes);
      } catch (e) {
        throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
      }
      const descriptors__iter_description_length = this.readUint16("big_endian");
      const descriptors__iter_description_bytes = this.readBytesSlice(descriptors__iter_description_length);
      try {
        descriptors__iter.description = new TextDecoder("utf-8", { fatal: true }).decode(descriptors__iter_description_bytes);
      } catch (e) {
        throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
      }
      const descriptors__iter_unit_length = this.readUint16("big_endian");
      const descriptors__iter_unit_bytes = this.readBytesSlice(descriptors__iter_unit_length);
      try {
        descriptors__iter.unit = new TextDecoder("utf-8", { fatal: true }).decode(descriptors__iter_unit_bytes);
      } catch (e) {
        throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
      }
      descriptors__iter.metric_kind = this.readUint8();
      descriptors__iter.temporality = this.readUint8();
      descriptors__iter.monotonic = this.readUint8();
      descriptors__iter.resource_attrs = [];
      const descriptors__iter_resource_attrs_length = this.readUint16("big_endian");
      for (let i = 0; i < descriptors__iter_resource_attrs_length; i++) {
        let descriptors__iter_resource_attrs__iter: any;
        descriptors__iter_resource_attrs__iter = {};
        const descriptors__iter_resource_attrs__iter_key_length = this.readUint8();
        const descriptors__iter_resource_attrs__iter_key_bytes = this.readBytesSlice(descriptors__iter_resource_attrs__iter_key_length);
        try {
          descriptors__iter_resource_attrs__iter.key = new TextDecoder("utf-8", { fatal: true }).decode(descriptors__iter_resource_attrs__iter_key_bytes);
        } catch (e) {
          throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
        }
        const descriptors__iter_resource_attrs__iter_value_length = this.readUint16("big_endian");
        const descriptors__iter_resource_attrs__iter_value_bytes = this.readBytesSlice(descriptors__iter_resource_attrs__iter_value_length);
        try {
          descriptors__iter_resource_attrs__iter.value = new TextDecoder("utf-8", { fatal: true }).decode(descriptors__iter_resource_attrs__iter_value_bytes);
        } catch (e) {
          throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
        }
        descriptors__iter.resource_attrs.push(descriptors__iter_resource_attrs__iter);
      }
      const descriptors__iter_scope_name_length = this.readUint8();
      const descriptors__iter_scope_name_bytes = this.readBytesSlice(descriptors__iter_scope_name_length);
      try {
        descriptors__iter.scope_name = new TextDecoder("utf-8", { fatal: true }).decode(descriptors__iter_scope_name_bytes);
      } catch (e) {
        throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
      }
      const descriptors__iter_scope_version_length = this.readUint8();
      const descriptors__iter_scope_version_bytes = this.readBytesSlice(descriptors__iter_scope_version_length);
      try {
        descriptors__iter.scope_version = new TextDecoder("utf-8", { fatal: true }).decode(descriptors__iter_scope_version_bytes);
      } catch (e) {
        throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
      }
      descriptors__iter.scope_attrs = [];
      const descriptors__iter_scope_attrs_length = this.readUint16("big_endian");
      for (let i = 0; i < descriptors__iter_scope_attrs_length; i++) {
        let descriptors__iter_scope_attrs__iter: any;
        descriptors__iter_scope_attrs__iter = {};
        const descriptors__iter_scope_attrs__iter_key_length = this.readUint8();
        const descriptors__iter_scope_attrs__iter_key_bytes = this.readBytesSlice(descriptors__iter_scope_attrs__iter_key_length);
        try {
          descriptors__iter_scope_attrs__iter.key = new TextDecoder("utf-8", { fatal: true }).decode(descriptors__iter_scope_attrs__iter_key_bytes);
        } catch (e) {
          throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
        }
        const descriptors__iter_scope_attrs__iter_value_length = this.readUint16("big_endian");
        const descriptors__iter_scope_attrs__iter_value_bytes = this.readBytesSlice(descriptors__iter_scope_attrs__iter_value_length);
        try {
          descriptors__iter_scope_attrs__iter.value = new TextDecoder("utf-8", { fatal: true }).decode(descriptors__iter_scope_attrs__iter_value_bytes);
        } catch (e) {
          throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
        }
        descriptors__iter.scope_attrs.push(descriptors__iter_scope_attrs__iter);
      }
      value.descriptors.push(descriptors__iter);
    }
    value.points = [];
    const points_length = this.readUint32("big_endian");
    for (let i = 0; i < points_length; i++) {
      let points__iter: any;
      points__iter = {};
      const discriminator = this.peekUint8();
      if (discriminator === 1) {
        const decoder = new ScalarPointV2Decoder(this.bytes.slice(this.byteOffset), points__iter);
        const decodedValue = decoder.decode();
        this.byteOffset += decoder.byteOffset;
        points__iter.value = { type: 'ScalarPointV2', value: decodedValue };
      }
      else if (discriminator === 2) {
        const decoder = new HistogramPointV2Decoder(this.bytes.slice(this.byteOffset), points__iter);
        const decodedValue = decoder.decode();
        this.byteOffset += decoder.byteOffset;
        points__iter.value = { type: 'HistogramPointV2', value: decodedValue };
      }
      else if (discriminator === 3) {
        const decoder = new ExponentialHistogramPointV2Decoder(this.bytes.slice(this.byteOffset), points__iter);
        const decodedValue = decoder.decode();
        this.byteOffset += decoder.byteOffset;
        points__iter.value = { type: 'ExponentialHistogramPointV2', value: decodedValue };
      }
      else if (discriminator === 4) {
        const decoder = new SummaryPointV2Decoder(this.bytes.slice(this.byteOffset), points__iter);
        const decodedValue = decoder.decode();
        this.byteOffset += decoder.byteOffset;
        points__iter.value = { type: 'SummaryPointV2', value: decodedValue };
      } else {
        throw new BinSchemaError(ErrorCode.INVALID_VARIANT, `Unknown discriminator: 0x${discriminator.toString(16)}`);
      }
      value.points.push(points__iter);
    }
    return value;
  }
}

/**
 * Batch-local metric identity and common OpenTelemetry/Prometheus metadata.
 */
export interface MetricDescriptorV2Input {
  /**
   * 32-bit Unsigned Integer
   * Fixed-width 32-bit unsigned integer (0-4294967295). Respects endianness configuration.
   */
  id: number;
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
  description: string;
  /**
   * String kind: length_prefixed
   * Encoding: utf8
   * Length prefix type: uint16
   */
  unit: string;
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  metric_kind: number;
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  temporality: number;
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  monotonic: number;
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint16
   */
  resource_attrs: LabelPairInput[];
  /**
   * String kind: length_prefixed
   * Encoding: utf8
   * Length prefix type: uint8
   */
  scope_name: string;
  /**
   * String kind: length_prefixed
   * Encoding: utf8
   * Length prefix type: uint8
   */
  scope_version: string;
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint16
   */
  scope_attrs: LabelPairInput[];
}

/**
 * Batch-local metric identity and common OpenTelemetry/Prometheus metadata.
 */
export interface MetricDescriptorV2Output {
  /**
   * 32-bit Unsigned Integer
   * Fixed-width 32-bit unsigned integer (0-4294967295). Respects endianness configuration.
   */
  id: number;
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
  description: string;
  /**
   * String kind: length_prefixed
   * Encoding: utf8
   * Length prefix type: uint16
   */
  unit: string;
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  metric_kind: number;
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  temporality: number;
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  monotonic: number;
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint16
   */
  resource_attrs: LabelPairOutput[];
  /**
   * String kind: length_prefixed
   * Encoding: utf8
   * Length prefix type: uint8
   */
  scope_name: string;
  /**
   * String kind: length_prefixed
   * Encoding: utf8
   * Length prefix type: uint8
   */
  scope_version: string;
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint16
   */
  scope_attrs: LabelPairOutput[];
}

export type MetricDescriptorV2 = MetricDescriptorV2Output;

export class MetricDescriptorV2Encoder extends BitStreamEncoder {
  private compressionDict: Map<string, number> = new Map();

  constructor() {
    super("msb_first");
  }

  encode(value: MetricDescriptorV2Input): Uint8Array {
    // Reset compression dictionary for each encode
    this.compressionDict.clear();

    this.writeUint32(value.id, "big_endian");
    const value_name_bytes = new TextEncoder().encode(value.name);
    this.writeUint16(value_name_bytes.length, "big_endian");
    for (const byte of value_name_bytes) {
      this.writeUint8(byte);
    }
    const value_description_bytes = new TextEncoder().encode(value.description);
    this.writeUint16(value_description_bytes.length, "big_endian");
    for (const byte of value_description_bytes) {
      this.writeUint8(byte);
    }
    const value_unit_bytes = new TextEncoder().encode(value.unit);
    this.writeUint16(value_unit_bytes.length, "big_endian");
    for (const byte of value_unit_bytes) {
      this.writeUint8(byte);
    }
    this.writeUint8(value.metric_kind);
    this.writeUint8(value.temporality);
    this.writeUint8(value.monotonic);
    this.writeUint16(value.resource_attrs.length, "big_endian");
    for (let value_resource_attrs__iter_index = 0; value_resource_attrs__iter_index < value.resource_attrs.length; value_resource_attrs__iter_index++) {
      const value_resource_attrs__iter = value.resource_attrs[value_resource_attrs__iter_index];
      const encoder_value_resource_attrs__iter = new LabelPairEncoder();
      const encoded_value_resource_attrs__iter = encoder_value_resource_attrs__iter.encode(value_resource_attrs__iter);
      for (const byte of encoded_value_resource_attrs__iter) {
        this.writeUint8(byte);
      }
    }
    const value_scope_name_bytes = new TextEncoder().encode(value.scope_name);
    this.writeUint8(value_scope_name_bytes.length);
    for (const byte of value_scope_name_bytes) {
      this.writeUint8(byte);
    }
    const value_scope_version_bytes = new TextEncoder().encode(value.scope_version);
    this.writeUint8(value_scope_version_bytes.length);
    for (const byte of value_scope_version_bytes) {
      this.writeUint8(byte);
    }
    this.writeUint16(value.scope_attrs.length, "big_endian");
    for (let value_scope_attrs__iter_index = 0; value_scope_attrs__iter_index < value.scope_attrs.length; value_scope_attrs__iter_index++) {
      const value_scope_attrs__iter = value.scope_attrs[value_scope_attrs__iter_index];
      const encoder_value_scope_attrs__iter = new LabelPairEncoder();
      const encoded_value_scope_attrs__iter = encoder_value_scope_attrs__iter.encode(value_scope_attrs__iter);
      for (const byte of encoded_value_scope_attrs__iter) {
        this.writeUint8(byte);
      }
    }
    return this.finish();
  }

  /**
   * Calculate the encoded size of a MetricDescriptorV2 value.
   * Used for from_after_field computed lengths and buffer pre-allocation.
   */
  calculateSize(value: MetricDescriptorV2): number {
    let size = 0;
    size += 4; // id
    // name: string (utf8)
    size += new TextEncoder().encode(value.name).length;
    // description: string (utf8)
    size += new TextEncoder().encode(value.description).length;
    // unit: string (utf8)
    size += new TextEncoder().encode(value.unit).length;
    size += 3; // metric_kind + temporality + monotonic
    // resource_attrs: array (kind: length_prefixed)
    for (const item of value.resource_attrs) {
      const resource_attrs_itemEncoder = new LabelPairEncoder();
      size += resource_attrs_itemEncoder.calculateSize(item);
    }
    size += 2; // length prefix (uint16)
    // scope_name: string (utf8)
    size += new TextEncoder().encode(value.scope_name).length;
    // scope_version: string (utf8)
    size += new TextEncoder().encode(value.scope_version).length;
    // scope_attrs: array (kind: length_prefixed)
    for (const item of value.scope_attrs) {
      const scope_attrs_itemEncoder = new LabelPairEncoder();
      size += scope_attrs_itemEncoder.calculateSize(item);
    }
    size += 2; // length prefix (uint16)
    return size;
  }
}

export class MetricDescriptorV2Decoder extends SeekableBitStreamDecoder {
  constructor(input: Uint8Array | number[] | string, private context?: any) {
    const reader = createReader(input);
    super(reader, "msb_first");
  }

  decode(): MetricDescriptorV2Output {
    const value: any = {};

    value.id = this.readUint32("big_endian");
    const name_length = this.readUint16("big_endian");
    const name_bytes = this.readBytesSlice(name_length);
    try {
      value.name = new TextDecoder("utf-8", { fatal: true }).decode(name_bytes);
    } catch (e) {
      throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
    }
    const description_length = this.readUint16("big_endian");
    const description_bytes = this.readBytesSlice(description_length);
    try {
      value.description = new TextDecoder("utf-8", { fatal: true }).decode(description_bytes);
    } catch (e) {
      throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
    }
    const unit_length = this.readUint16("big_endian");
    const unit_bytes = this.readBytesSlice(unit_length);
    try {
      value.unit = new TextDecoder("utf-8", { fatal: true }).decode(unit_bytes);
    } catch (e) {
      throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
    }
    value.metric_kind = this.readUint8();
    value.temporality = this.readUint8();
    value.monotonic = this.readUint8();
    value.resource_attrs = [];
    const resource_attrs_length = this.readUint16("big_endian");
    for (let i = 0; i < resource_attrs_length; i++) {
      let resource_attrs__iter: any;
      resource_attrs__iter = {};
      const resource_attrs__iter_key_length = this.readUint8();
      const resource_attrs__iter_key_bytes = this.readBytesSlice(resource_attrs__iter_key_length);
      try {
        resource_attrs__iter.key = new TextDecoder("utf-8", { fatal: true }).decode(resource_attrs__iter_key_bytes);
      } catch (e) {
        throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
      }
      const resource_attrs__iter_value_length = this.readUint16("big_endian");
      const resource_attrs__iter_value_bytes = this.readBytesSlice(resource_attrs__iter_value_length);
      try {
        resource_attrs__iter.value = new TextDecoder("utf-8", { fatal: true }).decode(resource_attrs__iter_value_bytes);
      } catch (e) {
        throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
      }
      value.resource_attrs.push(resource_attrs__iter);
    }
    const scope_name_length = this.readUint8();
    const scope_name_bytes = this.readBytesSlice(scope_name_length);
    try {
      value.scope_name = new TextDecoder("utf-8", { fatal: true }).decode(scope_name_bytes);
    } catch (e) {
      throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
    }
    const scope_version_length = this.readUint8();
    const scope_version_bytes = this.readBytesSlice(scope_version_length);
    try {
      value.scope_version = new TextDecoder("utf-8", { fatal: true }).decode(scope_version_bytes);
    } catch (e) {
      throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
    }
    value.scope_attrs = [];
    const scope_attrs_length = this.readUint16("big_endian");
    for (let i = 0; i < scope_attrs_length; i++) {
      let scope_attrs__iter: any;
      scope_attrs__iter = {};
      const scope_attrs__iter_key_length = this.readUint8();
      const scope_attrs__iter_key_bytes = this.readBytesSlice(scope_attrs__iter_key_length);
      try {
        scope_attrs__iter.key = new TextDecoder("utf-8", { fatal: true }).decode(scope_attrs__iter_key_bytes);
      } catch (e) {
        throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
      }
      const scope_attrs__iter_value_length = this.readUint16("big_endian");
      const scope_attrs__iter_value_bytes = this.readBytesSlice(scope_attrs__iter_value_length);
      try {
        scope_attrs__iter.value = new TextDecoder("utf-8", { fatal: true }).decode(scope_attrs__iter_value_bytes);
      } catch (e) {
        throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
      }
      value.scope_attrs.push(scope_attrs__iter);
    }
    return value;
  }
}

/**
 * A tagged canonical metric point. Tag 1 scalar, 2 explicit histogram, 3 exponential/native histogram, 4 summary.
 */
export interface MetricPointV2Input {
  /**
   * Discriminated Union
   * Type that can be one of several variants, chosen based on a discriminator value. Supports peek-based (read ahead) or field-based (reference earlier field) discrimination.
   *
   * @remarks
   *
   * Discriminator: peek uint8
   * Variants: 4
   * - ScalarPointV2 (when value === 1)
   * - HistogramPointV2 (when value === 2)
   * - ExponentialHistogramPointV2 (when value === 3)
   * - SummaryPointV2 (when value === 4)
   */
  value: { type: 'ScalarPointV2'; value: ScalarPointV2Input } | { type: 'HistogramPointV2'; value: HistogramPointV2Input } | { type: 'ExponentialHistogramPointV2'; value: ExponentialHistogramPointV2Input } | { type: 'SummaryPointV2'; value: SummaryPointV2Input };
}

/**
 * A tagged canonical metric point. Tag 1 scalar, 2 explicit histogram, 3 exponential/native histogram, 4 summary.
 */
export interface MetricPointV2Output {
  /**
   * Discriminated Union
   * Type that can be one of several variants, chosen based on a discriminator value. Supports peek-based (read ahead) or field-based (reference earlier field) discrimination.
   *
   * @remarks
   *
   * Discriminator: peek uint8
   * Variants: 4
   * - ScalarPointV2 (when value === 1)
   * - HistogramPointV2 (when value === 2)
   * - ExponentialHistogramPointV2 (when value === 3)
   * - SummaryPointV2 (when value === 4)
   */
  value: { type: 'ScalarPointV2'; value: ScalarPointV2Output } | { type: 'HistogramPointV2'; value: HistogramPointV2Output } | { type: 'ExponentialHistogramPointV2'; value: ExponentialHistogramPointV2Output } | { type: 'SummaryPointV2'; value: SummaryPointV2Output };
}

export type MetricPointV2 = MetricPointV2Output;

/**
 * Variant tags for MetricPointV2.value
 */
export const enum MetricPointV2ValueVariant {
  ScalarPointV2 = 'ScalarPointV2',
  HistogramPointV2 = 'HistogramPointV2',
  ExponentialHistogramPointV2 = 'ExponentialHistogramPointV2',
  SummaryPointV2 = 'SummaryPointV2',
}

export class MetricPointV2Encoder extends BitStreamEncoder {
  private compressionDict: Map<string, number> = new Map();

  constructor() {
    super("msb_first");
  }

  encode(value: MetricPointV2Input): Uint8Array {
    // Reset compression dictionary for each encode
    this.compressionDict.clear();

    if (value.value.type === 'ScalarPointV2') {
      const encoder_value = new ScalarPointV2Encoder();
      const encoded_value = encoder_value.encode(value.value.value);
      for (const byte of encoded_value) {
        this.writeUint8(byte);
      }
    }
    else if (value.value.type === 'HistogramPointV2') {
      const encoder_value = new HistogramPointV2Encoder();
      const encoded_value = encoder_value.encode(value.value.value);
      for (const byte of encoded_value) {
        this.writeUint8(byte);
      }
    }
    else if (value.value.type === 'ExponentialHistogramPointV2') {
      const encoder_value = new ExponentialHistogramPointV2Encoder();
      const encoded_value = encoder_value.encode(value.value.value);
      for (const byte of encoded_value) {
        this.writeUint8(byte);
      }
    }
    else if (value.value.type === 'SummaryPointV2') {
      const encoder_value = new SummaryPointV2Encoder();
      const encoded_value = encoder_value.encode(value.value.value);
      for (const byte of encoded_value) {
        this.writeUint8(byte);
      }
    } else {
      throw new BinSchemaError(ErrorCode.INVALID_VARIANT, `Unknown variant type: ${(value.value as any).type}`);
    }
    return this.finish();
  }

  /**
   * Calculate the encoded size of a MetricPointV2 value.
   * Used for from_after_field computed lengths and buffer pre-allocation.
   */
  calculateSize(value: MetricPointV2): number {
    let size = 0;
    if (value.type === 'ScalarPointV2') {
      const _enc = new ScalarPointV2Encoder();
      size += _enc.calculateSize(value.value);
    }
    else if (value.type === 'HistogramPointV2') {
      const _enc = new HistogramPointV2Encoder();
      size += _enc.calculateSize(value.value);
    }
    else if (value.type === 'ExponentialHistogramPointV2') {
      const _enc = new ExponentialHistogramPointV2Encoder();
      size += _enc.calculateSize(value.value);
    }
    else if (value.type === 'SummaryPointV2') {
      const _enc = new SummaryPointV2Encoder();
      size += _enc.calculateSize(value.value);
    }
    else {
      throw new BinSchemaError(ErrorCode.INVALID_VARIANT, `Unknown variant type for value: ${(value as any).type}`);
    }
    return size;
  }
}

export class MetricPointV2Decoder extends SeekableBitStreamDecoder {
  constructor(input: Uint8Array | number[] | string, private context?: any) {
    const reader = createReader(input);
    super(reader, "msb_first");
  }

  decode(): MetricPointV2Output {
    const value: any = {};

    const discriminator = this.peekUint8();
    if (discriminator === 1) {
      const decoder = new ScalarPointV2Decoder(this.bytes.slice(this.byteOffset), value);
      const decodedValue = decoder.decode();
      this.byteOffset += decoder.byteOffset;
      value.value = { type: 'ScalarPointV2', value: decodedValue };
    }
    else if (discriminator === 2) {
      const decoder = new HistogramPointV2Decoder(this.bytes.slice(this.byteOffset), value);
      const decodedValue = decoder.decode();
      this.byteOffset += decoder.byteOffset;
      value.value = { type: 'HistogramPointV2', value: decodedValue };
    }
    else if (discriminator === 3) {
      const decoder = new ExponentialHistogramPointV2Decoder(this.bytes.slice(this.byteOffset), value);
      const decodedValue = decoder.decode();
      this.byteOffset += decoder.byteOffset;
      value.value = { type: 'ExponentialHistogramPointV2', value: decodedValue };
    }
    else if (discriminator === 4) {
      const decoder = new SummaryPointV2Decoder(this.bytes.slice(this.byteOffset), value);
      const decodedValue = decoder.decode();
      this.byteOffset += decoder.byteOffset;
      value.value = { type: 'SummaryPointV2', value: decodedValue };
    } else {
      throw new BinSchemaError(ErrorCode.INVALID_VARIANT, `Unknown discriminator: 0x${discriminator.toString(16)}`);
    }
    return value;
  }
}

/**
 * An exact signed integer or IEEE-754 double point.
 */
export interface ScalarPointV2Input {
  /**
   * 32-bit Unsigned Integer
   * Fixed-width 32-bit unsigned integer (0-4294967295). Respects endianness configuration.
   */
  descriptor_id: number;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  start_unix_nano: bigint;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  ts_unix_nano: bigint;
  /**
   * 32-bit Unsigned Integer
   * Fixed-width 32-bit unsigned integer (0-4294967295). Respects endianness configuration.
   */
  flags: number;
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint16
   */
  attributes: LabelPairInput[];
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint16
   */
  exemplars: MetricExemplarV2Input[];
  /**
   * Lossless scalar value union.
   */
  number: MetricNumberV2Input;
}

/**
 * An exact signed integer or IEEE-754 double point.
 */
export interface ScalarPointV2Output {
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  tag: number;
  /**
   * 32-bit Unsigned Integer
   * Fixed-width 32-bit unsigned integer (0-4294967295). Respects endianness configuration.
   */
  descriptor_id: number;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  start_unix_nano: bigint;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  ts_unix_nano: bigint;
  /**
   * 32-bit Unsigned Integer
   * Fixed-width 32-bit unsigned integer (0-4294967295). Respects endianness configuration.
   */
  flags: number;
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint16
   */
  attributes: LabelPairOutput[];
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint16
   */
  exemplars: MetricExemplarV2Output[];
  /**
   * Lossless scalar value union.
   */
  number: MetricNumberV2Output;
}

export type ScalarPointV2 = ScalarPointV2Output;

export class ScalarPointV2Encoder extends BitStreamEncoder {
  private compressionDict: Map<string, number> = new Map();

  constructor() {
    super("msb_first");
  }

  encode(value: ScalarPointV2Input): Uint8Array {
    // Reset compression dictionary for each encode
    this.compressionDict.clear();

    this.writeUint8(1);
    this.writeUint32(value.descriptor_id, "big_endian");
    this.writeUint64(value.start_unix_nano, "big_endian");
    this.writeUint64(value.ts_unix_nano, "big_endian");
    this.writeUint32(value.flags, "big_endian");
    this.writeUint16(value.attributes.length, "big_endian");
    for (let value_attributes__iter_index = 0; value_attributes__iter_index < value.attributes.length; value_attributes__iter_index++) {
      const value_attributes__iter = value.attributes[value_attributes__iter_index];
      const encoder_value_attributes__iter = new LabelPairEncoder();
      const encoded_value_attributes__iter = encoder_value_attributes__iter.encode(value_attributes__iter);
      for (const byte of encoded_value_attributes__iter) {
        this.writeUint8(byte);
      }
    }
    this.writeUint16(value.exemplars.length, "big_endian");
    for (let value_exemplars__iter_index = 0; value_exemplars__iter_index < value.exemplars.length; value_exemplars__iter_index++) {
      const value_exemplars__iter = value.exemplars[value_exemplars__iter_index];
      const encoder_value_exemplars__iter = new MetricExemplarV2Encoder();
      const encoded_value_exemplars__iter = encoder_value_exemplars__iter.encode(value_exemplars__iter);
      for (const byte of encoded_value_exemplars__iter) {
        this.writeUint8(byte);
      }
    }
    const encoder_number = new MetricNumberV2Encoder();
    const encoded_number = encoder_number.encode(value.number);
    for (const byte of encoded_number) {
      this.writeUint8(byte);
    }
    return this.finish();
  }

  /**
   * Calculate the encoded size of a ScalarPointV2 value.
   * Used for from_after_field computed lengths and buffer pre-allocation.
   */
  calculateSize(value: ScalarPointV2): number {
    let size = 0;
    size += 25; // tag (const) + descriptor_id + start_unix_nano + ts_unix_nano + flags
    // attributes: array (kind: length_prefixed)
    for (const item of value.attributes) {
      const attributes_itemEncoder = new LabelPairEncoder();
      size += attributes_itemEncoder.calculateSize(item);
    }
    size += 2; // length prefix (uint16)
    // exemplars: array (kind: length_prefixed)
    for (const item of value.exemplars) {
      const exemplars_itemEncoder = new MetricExemplarV2Encoder();
      size += exemplars_itemEncoder.calculateSize(item);
    }
    size += 2; // length prefix (uint16)
    // number: custom type (MetricNumberV2)
    const number_encoder = new MetricNumberV2Encoder();
    size += number_encoder.calculateSize(value.number);
    return size;
  }
}

export class ScalarPointV2Decoder extends SeekableBitStreamDecoder {
  constructor(input: Uint8Array | number[] | string, private context?: any) {
    const reader = createReader(input);
    super(reader, "msb_first");
  }

  decode(): ScalarPointV2Output {
    const value: any = {};

    value.tag = this.readUint8();
    value.descriptor_id = this.readUint32("big_endian");
    value.start_unix_nano = this.readUint64("big_endian");
    value.ts_unix_nano = this.readUint64("big_endian");
    value.flags = this.readUint32("big_endian");
    value.attributes = [];
    const attributes_length = this.readUint16("big_endian");
    for (let i = 0; i < attributes_length; i++) {
      let attributes__iter: any;
      attributes__iter = {};
      const attributes__iter_key_length = this.readUint8();
      const attributes__iter_key_bytes = this.readBytesSlice(attributes__iter_key_length);
      try {
        attributes__iter.key = new TextDecoder("utf-8", { fatal: true }).decode(attributes__iter_key_bytes);
      } catch (e) {
        throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
      }
      const attributes__iter_value_length = this.readUint16("big_endian");
      const attributes__iter_value_bytes = this.readBytesSlice(attributes__iter_value_length);
      try {
        attributes__iter.value = new TextDecoder("utf-8", { fatal: true }).decode(attributes__iter_value_bytes);
      } catch (e) {
        throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
      }
      value.attributes.push(attributes__iter);
    }
    value.exemplars = [];
    const exemplars_length = this.readUint16("big_endian");
    for (let i = 0; i < exemplars_length; i++) {
      let exemplars__iter: any;
      exemplars__iter = {};
      exemplars__iter.ts_unix_nano = this.readUint64("big_endian");
      exemplars__iter.number = {};
      const discriminator = this.peekUint8();
      if (discriminator === 1) {
        const decoder = new IntegerValueV2Decoder(this.bytes.slice(this.byteOffset), exemplars__iter.number);
        const decodedValue = decoder.decode();
        this.byteOffset += decoder.byteOffset;
        exemplars__iter.number.value = { type: 'IntegerValueV2', value: decodedValue };
      }
      else if (discriminator === 2) {
        const decoder = new DoubleValueV2Decoder(this.bytes.slice(this.byteOffset), exemplars__iter.number);
        const decodedValue = decoder.decode();
        this.byteOffset += decoder.byteOffset;
        exemplars__iter.number.value = { type: 'DoubleValueV2', value: decodedValue };
      } else {
        throw new BinSchemaError(ErrorCode.INVALID_VARIANT, `Unknown discriminator: 0x${discriminator.toString(16)}`);
      }
      exemplars__iter.filtered_attrs = [];
      const exemplars__iter_filtered_attrs_length = this.readUint16("big_endian");
      for (let i = 0; i < exemplars__iter_filtered_attrs_length; i++) {
        let exemplars__iter_filtered_attrs__iter: any;
        exemplars__iter_filtered_attrs__iter = {};
        const exemplars__iter_filtered_attrs__iter_key_length = this.readUint8();
        const exemplars__iter_filtered_attrs__iter_key_bytes = this.readBytesSlice(exemplars__iter_filtered_attrs__iter_key_length);
        try {
          exemplars__iter_filtered_attrs__iter.key = new TextDecoder("utf-8", { fatal: true }).decode(exemplars__iter_filtered_attrs__iter_key_bytes);
        } catch (e) {
          throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
        }
        const exemplars__iter_filtered_attrs__iter_value_length = this.readUint16("big_endian");
        const exemplars__iter_filtered_attrs__iter_value_bytes = this.readBytesSlice(exemplars__iter_filtered_attrs__iter_value_length);
        try {
          exemplars__iter_filtered_attrs__iter.value = new TextDecoder("utf-8", { fatal: true }).decode(exemplars__iter_filtered_attrs__iter_value_bytes);
        } catch (e) {
          throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
        }
        exemplars__iter.filtered_attrs.push(exemplars__iter_filtered_attrs__iter);
      }
      exemplars__iter.trace_id = [];
      for (let i = 0; i < 16; i++) {
        let exemplars__iter_trace_id__iter: any;
        exemplars__iter_trace_id__iter = this.readUint8();
        exemplars__iter.trace_id.push(exemplars__iter_trace_id__iter);
      }
      exemplars__iter.span_id = [];
      for (let i = 0; i < 8; i++) {
        let exemplars__iter_span_id__iter: any;
        exemplars__iter_span_id__iter = this.readUint8();
        exemplars__iter.span_id.push(exemplars__iter_span_id__iter);
      }
      value.exemplars.push(exemplars__iter);
    }
    value.number = {};
    const discriminator = this.peekUint8();
    if (discriminator === 1) {
      const decoder = new IntegerValueV2Decoder(this.bytes.slice(this.byteOffset), value.number);
      const decodedValue = decoder.decode();
      this.byteOffset += decoder.byteOffset;
      value.number.value = { type: 'IntegerValueV2', value: decodedValue };
    }
    else if (discriminator === 2) {
      const decoder = new DoubleValueV2Decoder(this.bytes.slice(this.byteOffset), value.number);
      const decodedValue = decoder.decode();
      this.byteOffset += decoder.byteOffset;
      value.number.value = { type: 'DoubleValueV2', value: decodedValue };
    } else {
      throw new BinSchemaError(ErrorCode.INVALID_VARIANT, `Unknown discriminator: 0x${discriminator.toString(16)}`);
    }
    return value;
  }
}

/**
 * Explicit-bound histogram; bucket_counts length must equal explicit_bounds length + 1.
 */
export interface HistogramPointV2Input {
  /**
   * 32-bit Unsigned Integer
   * Fixed-width 32-bit unsigned integer (0-4294967295). Respects endianness configuration.
   */
  descriptor_id: number;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  start_unix_nano: bigint;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  ts_unix_nano: bigint;
  /**
   * 32-bit Unsigned Integer
   * Fixed-width 32-bit unsigned integer (0-4294967295). Respects endianness configuration.
   */
  flags: number;
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint16
   */
  attributes: LabelPairInput[];
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint16
   */
  exemplars: MetricExemplarV2Input[];
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  count: bigint;
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  has_sum: number;
  /**
   * 64-bit Floating Point
   * IEEE 754 double-precision floating point (64-bit). Provides ~15 decimal digits of precision.
   */
  sum: number;
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  has_min: number;
  /**
   * 64-bit Floating Point
   * IEEE 754 double-precision floating point (64-bit). Provides ~15 decimal digits of precision.
   */
  min: number;
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  has_max: number;
  /**
   * 64-bit Floating Point
   * IEEE 754 double-precision floating point (64-bit). Provides ~15 decimal digits of precision.
   */
  max: number;
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint32
   */
  explicit_bounds: number[];
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint32
   */
  bucket_counts: bigint[];
}

/**
 * Explicit-bound histogram; bucket_counts length must equal explicit_bounds length + 1.
 */
export interface HistogramPointV2Output {
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  tag: number;
  /**
   * 32-bit Unsigned Integer
   * Fixed-width 32-bit unsigned integer (0-4294967295). Respects endianness configuration.
   */
  descriptor_id: number;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  start_unix_nano: bigint;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  ts_unix_nano: bigint;
  /**
   * 32-bit Unsigned Integer
   * Fixed-width 32-bit unsigned integer (0-4294967295). Respects endianness configuration.
   */
  flags: number;
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint16
   */
  attributes: LabelPairOutput[];
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint16
   */
  exemplars: MetricExemplarV2Output[];
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  count: bigint;
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  has_sum: number;
  /**
   * 64-bit Floating Point
   * IEEE 754 double-precision floating point (64-bit). Provides ~15 decimal digits of precision.
   */
  sum: number;
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  has_min: number;
  /**
   * 64-bit Floating Point
   * IEEE 754 double-precision floating point (64-bit). Provides ~15 decimal digits of precision.
   */
  min: number;
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  has_max: number;
  /**
   * 64-bit Floating Point
   * IEEE 754 double-precision floating point (64-bit). Provides ~15 decimal digits of precision.
   */
  max: number;
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint32
   */
  explicit_bounds: number[];
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint32
   */
  bucket_counts: bigint[];
}

export type HistogramPointV2 = HistogramPointV2Output;

export class HistogramPointV2Encoder extends BitStreamEncoder {
  private compressionDict: Map<string, number> = new Map();

  constructor() {
    super("msb_first");
  }

  encode(value: HistogramPointV2Input): Uint8Array {
    // Reset compression dictionary for each encode
    this.compressionDict.clear();

    this.writeUint8(2);
    this.writeUint32(value.descriptor_id, "big_endian");
    this.writeUint64(value.start_unix_nano, "big_endian");
    this.writeUint64(value.ts_unix_nano, "big_endian");
    this.writeUint32(value.flags, "big_endian");
    this.writeUint16(value.attributes.length, "big_endian");
    for (let value_attributes__iter_index = 0; value_attributes__iter_index < value.attributes.length; value_attributes__iter_index++) {
      const value_attributes__iter = value.attributes[value_attributes__iter_index];
      const encoder_value_attributes__iter = new LabelPairEncoder();
      const encoded_value_attributes__iter = encoder_value_attributes__iter.encode(value_attributes__iter);
      for (const byte of encoded_value_attributes__iter) {
        this.writeUint8(byte);
      }
    }
    this.writeUint16(value.exemplars.length, "big_endian");
    for (let value_exemplars__iter_index = 0; value_exemplars__iter_index < value.exemplars.length; value_exemplars__iter_index++) {
      const value_exemplars__iter = value.exemplars[value_exemplars__iter_index];
      const encoder_value_exemplars__iter = new MetricExemplarV2Encoder();
      const encoded_value_exemplars__iter = encoder_value_exemplars__iter.encode(value_exemplars__iter);
      for (const byte of encoded_value_exemplars__iter) {
        this.writeUint8(byte);
      }
    }
    this.writeUint64(value.count, "big_endian");
    this.writeUint8(value.has_sum);
    this.writeFloat64(value.sum, "big_endian");
    this.writeUint8(value.has_min);
    this.writeFloat64(value.min, "big_endian");
    this.writeUint8(value.has_max);
    this.writeFloat64(value.max, "big_endian");
    this.writeUint32(value.explicit_bounds.length, "big_endian");
    for (let value_explicit_bounds__iter_index = 0; value_explicit_bounds__iter_index < value.explicit_bounds.length; value_explicit_bounds__iter_index++) {
      const value_explicit_bounds__iter = value.explicit_bounds[value_explicit_bounds__iter_index];
      this.writeFloat64(value_explicit_bounds__iter, "big_endian");
    }
    this.writeUint32(value.bucket_counts.length, "big_endian");
    for (let value_bucket_counts__iter_index = 0; value_bucket_counts__iter_index < value.bucket_counts.length; value_bucket_counts__iter_index++) {
      const value_bucket_counts__iter = value.bucket_counts[value_bucket_counts__iter_index];
      this.writeUint64(value_bucket_counts__iter, "big_endian");
    }
    return this.finish();
  }

  /**
   * Calculate the encoded size of a HistogramPointV2 value.
   * Used for from_after_field computed lengths and buffer pre-allocation.
   */
  calculateSize(value: HistogramPointV2): number {
    let size = 0;
    size += 25; // tag (const) + descriptor_id + start_unix_nano + ts_unix_nano + flags
    // attributes: array (kind: length_prefixed)
    for (const item of value.attributes) {
      const attributes_itemEncoder = new LabelPairEncoder();
      size += attributes_itemEncoder.calculateSize(item);
    }
    size += 2; // length prefix (uint16)
    // exemplars: array (kind: length_prefixed)
    for (const item of value.exemplars) {
      const exemplars_itemEncoder = new MetricExemplarV2Encoder();
      size += exemplars_itemEncoder.calculateSize(item);
    }
    size += 37; // length prefix (uint16) + count + has_sum + sum + has_min + min + has_max + max
    // explicit_bounds: array (kind: length_prefixed)
    for (const item of value.explicit_bounds) {
      size += 8;
    }
    size += 4; // length prefix (uint32)
    // bucket_counts: array (kind: length_prefixed)
    for (const item of value.bucket_counts) {
      size += 8;
    }
    size += 4; // length prefix (uint32)
    return size;
  }
}

export class HistogramPointV2Decoder extends SeekableBitStreamDecoder {
  constructor(input: Uint8Array | number[] | string, private context?: any) {
    const reader = createReader(input);
    super(reader, "msb_first");
  }

  decode(): HistogramPointV2Output {
    const value: any = {};

    value.tag = this.readUint8();
    value.descriptor_id = this.readUint32("big_endian");
    value.start_unix_nano = this.readUint64("big_endian");
    value.ts_unix_nano = this.readUint64("big_endian");
    value.flags = this.readUint32("big_endian");
    value.attributes = [];
    const attributes_length = this.readUint16("big_endian");
    for (let i = 0; i < attributes_length; i++) {
      let attributes__iter: any;
      attributes__iter = {};
      const attributes__iter_key_length = this.readUint8();
      const attributes__iter_key_bytes = this.readBytesSlice(attributes__iter_key_length);
      try {
        attributes__iter.key = new TextDecoder("utf-8", { fatal: true }).decode(attributes__iter_key_bytes);
      } catch (e) {
        throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
      }
      const attributes__iter_value_length = this.readUint16("big_endian");
      const attributes__iter_value_bytes = this.readBytesSlice(attributes__iter_value_length);
      try {
        attributes__iter.value = new TextDecoder("utf-8", { fatal: true }).decode(attributes__iter_value_bytes);
      } catch (e) {
        throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
      }
      value.attributes.push(attributes__iter);
    }
    value.exemplars = [];
    const exemplars_length = this.readUint16("big_endian");
    for (let i = 0; i < exemplars_length; i++) {
      let exemplars__iter: any;
      exemplars__iter = {};
      exemplars__iter.ts_unix_nano = this.readUint64("big_endian");
      exemplars__iter.number = {};
      const discriminator = this.peekUint8();
      if (discriminator === 1) {
        const decoder = new IntegerValueV2Decoder(this.bytes.slice(this.byteOffset), exemplars__iter.number);
        const decodedValue = decoder.decode();
        this.byteOffset += decoder.byteOffset;
        exemplars__iter.number.value = { type: 'IntegerValueV2', value: decodedValue };
      }
      else if (discriminator === 2) {
        const decoder = new DoubleValueV2Decoder(this.bytes.slice(this.byteOffset), exemplars__iter.number);
        const decodedValue = decoder.decode();
        this.byteOffset += decoder.byteOffset;
        exemplars__iter.number.value = { type: 'DoubleValueV2', value: decodedValue };
      } else {
        throw new BinSchemaError(ErrorCode.INVALID_VARIANT, `Unknown discriminator: 0x${discriminator.toString(16)}`);
      }
      exemplars__iter.filtered_attrs = [];
      const exemplars__iter_filtered_attrs_length = this.readUint16("big_endian");
      for (let i = 0; i < exemplars__iter_filtered_attrs_length; i++) {
        let exemplars__iter_filtered_attrs__iter: any;
        exemplars__iter_filtered_attrs__iter = {};
        const exemplars__iter_filtered_attrs__iter_key_length = this.readUint8();
        const exemplars__iter_filtered_attrs__iter_key_bytes = this.readBytesSlice(exemplars__iter_filtered_attrs__iter_key_length);
        try {
          exemplars__iter_filtered_attrs__iter.key = new TextDecoder("utf-8", { fatal: true }).decode(exemplars__iter_filtered_attrs__iter_key_bytes);
        } catch (e) {
          throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
        }
        const exemplars__iter_filtered_attrs__iter_value_length = this.readUint16("big_endian");
        const exemplars__iter_filtered_attrs__iter_value_bytes = this.readBytesSlice(exemplars__iter_filtered_attrs__iter_value_length);
        try {
          exemplars__iter_filtered_attrs__iter.value = new TextDecoder("utf-8", { fatal: true }).decode(exemplars__iter_filtered_attrs__iter_value_bytes);
        } catch (e) {
          throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
        }
        exemplars__iter.filtered_attrs.push(exemplars__iter_filtered_attrs__iter);
      }
      exemplars__iter.trace_id = [];
      for (let i = 0; i < 16; i++) {
        let exemplars__iter_trace_id__iter: any;
        exemplars__iter_trace_id__iter = this.readUint8();
        exemplars__iter.trace_id.push(exemplars__iter_trace_id__iter);
      }
      exemplars__iter.span_id = [];
      for (let i = 0; i < 8; i++) {
        let exemplars__iter_span_id__iter: any;
        exemplars__iter_span_id__iter = this.readUint8();
        exemplars__iter.span_id.push(exemplars__iter_span_id__iter);
      }
      value.exemplars.push(exemplars__iter);
    }
    value.count = this.readUint64("big_endian");
    value.has_sum = this.readUint8();
    value.sum = this.readFloat64("big_endian");
    value.has_min = this.readUint8();
    value.min = this.readFloat64("big_endian");
    value.has_max = this.readUint8();
    value.max = this.readFloat64("big_endian");
    value.explicit_bounds = [];
    const explicit_bounds_length = this.readUint32("big_endian");
    for (let i = 0; i < explicit_bounds_length; i++) {
      let explicit_bounds__iter: any;
      explicit_bounds__iter = this.readFloat64("big_endian");
      value.explicit_bounds.push(explicit_bounds__iter);
    }
    value.bucket_counts = [];
    const bucket_counts_length = this.readUint32("big_endian");
    for (let i = 0; i < bucket_counts_length; i++) {
      let bucket_counts__iter: any;
      bucket_counts__iter = this.readUint64("big_endian");
      value.bucket_counts.push(bucket_counts__iter);
    }
    return value;
  }
}

/**
 * Exponential/native histogram with sparse positive/negative buckets, integer or float counts, optional custom bounds, and reset hint.
 */
export interface ExponentialHistogramPointV2Input {
  /**
   * 32-bit Unsigned Integer
   * Fixed-width 32-bit unsigned integer (0-4294967295). Respects endianness configuration.
   */
  descriptor_id: number;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  start_unix_nano: bigint;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  ts_unix_nano: bigint;
  /**
   * 32-bit Unsigned Integer
   * Fixed-width 32-bit unsigned integer (0-4294967295). Respects endianness configuration.
   */
  flags: number;
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint16
   */
  attributes: LabelPairInput[];
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint16
   */
  exemplars: MetricExemplarV2Input[];
  /**
   * Histogram count union for integer-count and float-count native histograms.
   */
  count: MetricCountV2Input;
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  has_sum: number;
  /**
   * 64-bit Floating Point
   * IEEE 754 double-precision floating point (64-bit). Provides ~15 decimal digits of precision.
   */
  sum: number;
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  has_min: number;
  /**
   * 64-bit Floating Point
   * IEEE 754 double-precision floating point (64-bit). Provides ~15 decimal digits of precision.
   */
  min: number;
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  has_max: number;
  /**
   * 64-bit Floating Point
   * IEEE 754 double-precision floating point (64-bit). Provides ~15 decimal digits of precision.
   */
  max: number;
  /**
   * 32-bit Signed Integer
   * Fixed-width 32-bit signed integer (-2147483648 to 2147483647). Respects endianness configuration.
   */
  scale: number;
  /**
   * 64-bit Floating Point
   * IEEE 754 double-precision floating point (64-bit). Provides ~15 decimal digits of precision.
   */
  zero_threshold: number;
  /**
   * Histogram count union for integer-count and float-count native histograms.
   */
  zero_count: MetricCountV2Input;
  /**
   * Sparse exponential histogram bucket run. Deltas are signed offsets; counts share one numeric representation.
   */
  positive: SparseBucketsV2Input;
  /**
   * Sparse exponential histogram bucket run. Deltas are signed offsets; counts share one numeric representation.
   */
  negative: SparseBucketsV2Input;
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint32
   */
  custom_bounds: number[];
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  reset_hint: number;
}

/**
 * Exponential/native histogram with sparse positive/negative buckets, integer or float counts, optional custom bounds, and reset hint.
 */
export interface ExponentialHistogramPointV2Output {
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  tag: number;
  /**
   * 32-bit Unsigned Integer
   * Fixed-width 32-bit unsigned integer (0-4294967295). Respects endianness configuration.
   */
  descriptor_id: number;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  start_unix_nano: bigint;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  ts_unix_nano: bigint;
  /**
   * 32-bit Unsigned Integer
   * Fixed-width 32-bit unsigned integer (0-4294967295). Respects endianness configuration.
   */
  flags: number;
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint16
   */
  attributes: LabelPairOutput[];
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint16
   */
  exemplars: MetricExemplarV2Output[];
  /**
   * Histogram count union for integer-count and float-count native histograms.
   */
  count: MetricCountV2Output;
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  has_sum: number;
  /**
   * 64-bit Floating Point
   * IEEE 754 double-precision floating point (64-bit). Provides ~15 decimal digits of precision.
   */
  sum: number;
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  has_min: number;
  /**
   * 64-bit Floating Point
   * IEEE 754 double-precision floating point (64-bit). Provides ~15 decimal digits of precision.
   */
  min: number;
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  has_max: number;
  /**
   * 64-bit Floating Point
   * IEEE 754 double-precision floating point (64-bit). Provides ~15 decimal digits of precision.
   */
  max: number;
  /**
   * 32-bit Signed Integer
   * Fixed-width 32-bit signed integer (-2147483648 to 2147483647). Respects endianness configuration.
   */
  scale: number;
  /**
   * 64-bit Floating Point
   * IEEE 754 double-precision floating point (64-bit). Provides ~15 decimal digits of precision.
   */
  zero_threshold: number;
  /**
   * Histogram count union for integer-count and float-count native histograms.
   */
  zero_count: MetricCountV2Output;
  /**
   * Sparse exponential histogram bucket run. Deltas are signed offsets; counts share one numeric representation.
   */
  positive: SparseBucketsV2Output;
  /**
   * Sparse exponential histogram bucket run. Deltas are signed offsets; counts share one numeric representation.
   */
  negative: SparseBucketsV2Output;
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint32
   */
  custom_bounds: number[];
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  reset_hint: number;
}

export type ExponentialHistogramPointV2 = ExponentialHistogramPointV2Output;

export class ExponentialHistogramPointV2Encoder extends BitStreamEncoder {
  private compressionDict: Map<string, number> = new Map();

  constructor() {
    super("msb_first");
  }

  encode(value: ExponentialHistogramPointV2Input): Uint8Array {
    // Reset compression dictionary for each encode
    this.compressionDict.clear();

    this.writeUint8(3);
    this.writeUint32(value.descriptor_id, "big_endian");
    this.writeUint64(value.start_unix_nano, "big_endian");
    this.writeUint64(value.ts_unix_nano, "big_endian");
    this.writeUint32(value.flags, "big_endian");
    this.writeUint16(value.attributes.length, "big_endian");
    for (let value_attributes__iter_index = 0; value_attributes__iter_index < value.attributes.length; value_attributes__iter_index++) {
      const value_attributes__iter = value.attributes[value_attributes__iter_index];
      const encoder_value_attributes__iter = new LabelPairEncoder();
      const encoded_value_attributes__iter = encoder_value_attributes__iter.encode(value_attributes__iter);
      for (const byte of encoded_value_attributes__iter) {
        this.writeUint8(byte);
      }
    }
    this.writeUint16(value.exemplars.length, "big_endian");
    for (let value_exemplars__iter_index = 0; value_exemplars__iter_index < value.exemplars.length; value_exemplars__iter_index++) {
      const value_exemplars__iter = value.exemplars[value_exemplars__iter_index];
      const encoder_value_exemplars__iter = new MetricExemplarV2Encoder();
      const encoded_value_exemplars__iter = encoder_value_exemplars__iter.encode(value_exemplars__iter);
      for (const byte of encoded_value_exemplars__iter) {
        this.writeUint8(byte);
      }
    }
    const encoder_count = new MetricCountV2Encoder();
    const encoded_count = encoder_count.encode(value.count);
    for (const byte of encoded_count) {
      this.writeUint8(byte);
    }
    this.writeUint8(value.has_sum);
    this.writeFloat64(value.sum, "big_endian");
    this.writeUint8(value.has_min);
    this.writeFloat64(value.min, "big_endian");
    this.writeUint8(value.has_max);
    this.writeFloat64(value.max, "big_endian");
    this.writeInt32(value.scale, "big_endian");
    this.writeFloat64(value.zero_threshold, "big_endian");
    const encoder_zero_count = new MetricCountV2Encoder();
    const encoded_zero_count = encoder_zero_count.encode(value.zero_count);
    for (const byte of encoded_zero_count) {
      this.writeUint8(byte);
    }
    const encoder_positive = new SparseBucketsV2Encoder();
    const encoded_positive = encoder_positive.encode(value.positive);
    for (const byte of encoded_positive) {
      this.writeUint8(byte);
    }
    const encoder_negative = new SparseBucketsV2Encoder();
    const encoded_negative = encoder_negative.encode(value.negative);
    for (const byte of encoded_negative) {
      this.writeUint8(byte);
    }
    this.writeUint32(value.custom_bounds.length, "big_endian");
    for (let value_custom_bounds__iter_index = 0; value_custom_bounds__iter_index < value.custom_bounds.length; value_custom_bounds__iter_index++) {
      const value_custom_bounds__iter = value.custom_bounds[value_custom_bounds__iter_index];
      this.writeFloat64(value_custom_bounds__iter, "big_endian");
    }
    this.writeUint8(value.reset_hint);
    return this.finish();
  }

  /**
   * Calculate the encoded size of a ExponentialHistogramPointV2 value.
   * Used for from_after_field computed lengths and buffer pre-allocation.
   */
  calculateSize(value: ExponentialHistogramPointV2): number {
    let size = 0;
    size += 25; // tag (const) + descriptor_id + start_unix_nano + ts_unix_nano + flags
    // attributes: array (kind: length_prefixed)
    for (const item of value.attributes) {
      const attributes_itemEncoder = new LabelPairEncoder();
      size += attributes_itemEncoder.calculateSize(item);
    }
    size += 2; // length prefix (uint16)
    // exemplars: array (kind: length_prefixed)
    for (const item of value.exemplars) {
      const exemplars_itemEncoder = new MetricExemplarV2Encoder();
      size += exemplars_itemEncoder.calculateSize(item);
    }
    size += 2; // length prefix (uint16)
    // count: custom type (MetricCountV2)
    const count_encoder = new MetricCountV2Encoder();
    size += count_encoder.calculateSize(value.count);
    size += 39; // has_sum + sum + has_min + min + has_max + max + scale + zero_threshold
    // zero_count: custom type (MetricCountV2)
    const zero_count_encoder = new MetricCountV2Encoder();
    size += zero_count_encoder.calculateSize(value.zero_count);
    // positive: custom type (SparseBucketsV2)
    const positive_encoder = new SparseBucketsV2Encoder();
    size += positive_encoder.calculateSize(value.positive);
    // negative: custom type (SparseBucketsV2)
    const negative_encoder = new SparseBucketsV2Encoder();
    size += negative_encoder.calculateSize(value.negative);
    // custom_bounds: array (kind: length_prefixed)
    for (const item of value.custom_bounds) {
      size += 8;
    }
    size += 5; // length prefix (uint32) + reset_hint
    return size;
  }
}

export class ExponentialHistogramPointV2Decoder extends SeekableBitStreamDecoder {
  constructor(input: Uint8Array | number[] | string, private context?: any) {
    const reader = createReader(input);
    super(reader, "msb_first");
  }

  decode(): ExponentialHistogramPointV2Output {
    const value: any = {};

    value.tag = this.readUint8();
    value.descriptor_id = this.readUint32("big_endian");
    value.start_unix_nano = this.readUint64("big_endian");
    value.ts_unix_nano = this.readUint64("big_endian");
    value.flags = this.readUint32("big_endian");
    value.attributes = [];
    const attributes_length = this.readUint16("big_endian");
    for (let i = 0; i < attributes_length; i++) {
      let attributes__iter: any;
      attributes__iter = {};
      const attributes__iter_key_length = this.readUint8();
      const attributes__iter_key_bytes = this.readBytesSlice(attributes__iter_key_length);
      try {
        attributes__iter.key = new TextDecoder("utf-8", { fatal: true }).decode(attributes__iter_key_bytes);
      } catch (e) {
        throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
      }
      const attributes__iter_value_length = this.readUint16("big_endian");
      const attributes__iter_value_bytes = this.readBytesSlice(attributes__iter_value_length);
      try {
        attributes__iter.value = new TextDecoder("utf-8", { fatal: true }).decode(attributes__iter_value_bytes);
      } catch (e) {
        throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
      }
      value.attributes.push(attributes__iter);
    }
    value.exemplars = [];
    const exemplars_length = this.readUint16("big_endian");
    for (let i = 0; i < exemplars_length; i++) {
      let exemplars__iter: any;
      exemplars__iter = {};
      exemplars__iter.ts_unix_nano = this.readUint64("big_endian");
      exemplars__iter.number = {};
      const discriminator = this.peekUint8();
      if (discriminator === 1) {
        const decoder = new IntegerValueV2Decoder(this.bytes.slice(this.byteOffset), exemplars__iter.number);
        const decodedValue = decoder.decode();
        this.byteOffset += decoder.byteOffset;
        exemplars__iter.number.value = { type: 'IntegerValueV2', value: decodedValue };
      }
      else if (discriminator === 2) {
        const decoder = new DoubleValueV2Decoder(this.bytes.slice(this.byteOffset), exemplars__iter.number);
        const decodedValue = decoder.decode();
        this.byteOffset += decoder.byteOffset;
        exemplars__iter.number.value = { type: 'DoubleValueV2', value: decodedValue };
      } else {
        throw new BinSchemaError(ErrorCode.INVALID_VARIANT, `Unknown discriminator: 0x${discriminator.toString(16)}`);
      }
      exemplars__iter.filtered_attrs = [];
      const exemplars__iter_filtered_attrs_length = this.readUint16("big_endian");
      for (let i = 0; i < exemplars__iter_filtered_attrs_length; i++) {
        let exemplars__iter_filtered_attrs__iter: any;
        exemplars__iter_filtered_attrs__iter = {};
        const exemplars__iter_filtered_attrs__iter_key_length = this.readUint8();
        const exemplars__iter_filtered_attrs__iter_key_bytes = this.readBytesSlice(exemplars__iter_filtered_attrs__iter_key_length);
        try {
          exemplars__iter_filtered_attrs__iter.key = new TextDecoder("utf-8", { fatal: true }).decode(exemplars__iter_filtered_attrs__iter_key_bytes);
        } catch (e) {
          throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
        }
        const exemplars__iter_filtered_attrs__iter_value_length = this.readUint16("big_endian");
        const exemplars__iter_filtered_attrs__iter_value_bytes = this.readBytesSlice(exemplars__iter_filtered_attrs__iter_value_length);
        try {
          exemplars__iter_filtered_attrs__iter.value = new TextDecoder("utf-8", { fatal: true }).decode(exemplars__iter_filtered_attrs__iter_value_bytes);
        } catch (e) {
          throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
        }
        exemplars__iter.filtered_attrs.push(exemplars__iter_filtered_attrs__iter);
      }
      exemplars__iter.trace_id = [];
      for (let i = 0; i < 16; i++) {
        let exemplars__iter_trace_id__iter: any;
        exemplars__iter_trace_id__iter = this.readUint8();
        exemplars__iter.trace_id.push(exemplars__iter_trace_id__iter);
      }
      exemplars__iter.span_id = [];
      for (let i = 0; i < 8; i++) {
        let exemplars__iter_span_id__iter: any;
        exemplars__iter_span_id__iter = this.readUint8();
        exemplars__iter.span_id.push(exemplars__iter_span_id__iter);
      }
      value.exemplars.push(exemplars__iter);
    }
    value.count = {};
    const discriminator = this.peekUint8();
    if (discriminator === 1) {
      const decoder = new IntegerCountV2Decoder(this.bytes.slice(this.byteOffset), value.count);
      const decodedValue = decoder.decode();
      this.byteOffset += decoder.byteOffset;
      value.count.value = { type: 'IntegerCountV2', value: decodedValue };
    }
    else if (discriminator === 2) {
      const decoder = new FloatCountV2Decoder(this.bytes.slice(this.byteOffset), value.count);
      const decodedValue = decoder.decode();
      this.byteOffset += decoder.byteOffset;
      value.count.value = { type: 'FloatCountV2', value: decodedValue };
    } else {
      throw new BinSchemaError(ErrorCode.INVALID_VARIANT, `Unknown discriminator: 0x${discriminator.toString(16)}`);
    }
    value.has_sum = this.readUint8();
    value.sum = this.readFloat64("big_endian");
    value.has_min = this.readUint8();
    value.min = this.readFloat64("big_endian");
    value.has_max = this.readUint8();
    value.max = this.readFloat64("big_endian");
    value.scale = this.readInt32("big_endian");
    value.zero_threshold = this.readFloat64("big_endian");
    value.zero_count = {};
    const discriminator = this.peekUint8();
    if (discriminator === 1) {
      const decoder = new IntegerCountV2Decoder(this.bytes.slice(this.byteOffset), value.zero_count);
      const decodedValue = decoder.decode();
      this.byteOffset += decoder.byteOffset;
      value.zero_count.value = { type: 'IntegerCountV2', value: decodedValue };
    }
    else if (discriminator === 2) {
      const decoder = new FloatCountV2Decoder(this.bytes.slice(this.byteOffset), value.zero_count);
      const decodedValue = decoder.decode();
      this.byteOffset += decoder.byteOffset;
      value.zero_count.value = { type: 'FloatCountV2', value: decodedValue };
    } else {
      throw new BinSchemaError(ErrorCode.INVALID_VARIANT, `Unknown discriminator: 0x${discriminator.toString(16)}`);
    }
    value.positive = {};
    value.positive.offset = this.readInt32("big_endian");
    value.positive.deltas = [];
    const positive_deltas_length = this.readUint32("big_endian");
    for (let i = 0; i < positive_deltas_length; i++) {
      let positive_deltas__iter: any;
      positive_deltas__iter = this.readInt32("big_endian");
      value.positive.deltas.push(positive_deltas__iter);
    }
    value.positive.counts = [];
    const positive_counts_length = this.readUint32("big_endian");
    for (let i = 0; i < positive_counts_length; i++) {
      let positive_counts__iter: any;
      positive_counts__iter = {};
      const discriminator = this.peekUint8();
      if (discriminator === 1) {
        const decoder = new IntegerCountV2Decoder(this.bytes.slice(this.byteOffset), positive_counts__iter);
        const decodedValue = decoder.decode();
        this.byteOffset += decoder.byteOffset;
        positive_counts__iter.value = { type: 'IntegerCountV2', value: decodedValue };
      }
      else if (discriminator === 2) {
        const decoder = new FloatCountV2Decoder(this.bytes.slice(this.byteOffset), positive_counts__iter);
        const decodedValue = decoder.decode();
        this.byteOffset += decoder.byteOffset;
        positive_counts__iter.value = { type: 'FloatCountV2', value: decodedValue };
      } else {
        throw new BinSchemaError(ErrorCode.INVALID_VARIANT, `Unknown discriminator: 0x${discriminator.toString(16)}`);
      }
      value.positive.counts.push(positive_counts__iter);
    }
    value.negative = {};
    value.negative.offset = this.readInt32("big_endian");
    value.negative.deltas = [];
    const negative_deltas_length = this.readUint32("big_endian");
    for (let i = 0; i < negative_deltas_length; i++) {
      let negative_deltas__iter: any;
      negative_deltas__iter = this.readInt32("big_endian");
      value.negative.deltas.push(negative_deltas__iter);
    }
    value.negative.counts = [];
    const negative_counts_length = this.readUint32("big_endian");
    for (let i = 0; i < negative_counts_length; i++) {
      let negative_counts__iter: any;
      negative_counts__iter = {};
      const discriminator = this.peekUint8();
      if (discriminator === 1) {
        const decoder = new IntegerCountV2Decoder(this.bytes.slice(this.byteOffset), negative_counts__iter);
        const decodedValue = decoder.decode();
        this.byteOffset += decoder.byteOffset;
        negative_counts__iter.value = { type: 'IntegerCountV2', value: decodedValue };
      }
      else if (discriminator === 2) {
        const decoder = new FloatCountV2Decoder(this.bytes.slice(this.byteOffset), negative_counts__iter);
        const decodedValue = decoder.decode();
        this.byteOffset += decoder.byteOffset;
        negative_counts__iter.value = { type: 'FloatCountV2', value: decodedValue };
      } else {
        throw new BinSchemaError(ErrorCode.INVALID_VARIANT, `Unknown discriminator: 0x${discriminator.toString(16)}`);
      }
      value.negative.counts.push(negative_counts__iter);
    }
    value.custom_bounds = [];
    const custom_bounds_length = this.readUint32("big_endian");
    for (let i = 0; i < custom_bounds_length; i++) {
      let custom_bounds__iter: any;
      custom_bounds__iter = this.readFloat64("big_endian");
      value.custom_bounds.push(custom_bounds__iter);
    }
    value.reset_hint = this.readUint8();
    return value;
  }
}

/**
 * Summary point with exact count, sum, and sorted quantile/value pairs.
 */
export interface SummaryPointV2Input {
  /**
   * 32-bit Unsigned Integer
   * Fixed-width 32-bit unsigned integer (0-4294967295). Respects endianness configuration.
   */
  descriptor_id: number;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  start_unix_nano: bigint;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  ts_unix_nano: bigint;
  /**
   * 32-bit Unsigned Integer
   * Fixed-width 32-bit unsigned integer (0-4294967295). Respects endianness configuration.
   */
  flags: number;
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint16
   */
  attributes: LabelPairInput[];
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint16
   */
  exemplars: MetricExemplarV2Input[];
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  count: bigint;
  /**
   * 64-bit Floating Point
   * IEEE 754 double-precision floating point (64-bit). Provides ~15 decimal digits of precision.
   */
  sum: number;
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint16
   */
  quantiles: QuantileValueV2Input[];
}

/**
 * Summary point with exact count, sum, and sorted quantile/value pairs.
 */
export interface SummaryPointV2Output {
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  tag: number;
  /**
   * 32-bit Unsigned Integer
   * Fixed-width 32-bit unsigned integer (0-4294967295). Respects endianness configuration.
   */
  descriptor_id: number;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  start_unix_nano: bigint;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  ts_unix_nano: bigint;
  /**
   * 32-bit Unsigned Integer
   * Fixed-width 32-bit unsigned integer (0-4294967295). Respects endianness configuration.
   */
  flags: number;
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint16
   */
  attributes: LabelPairOutput[];
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint16
   */
  exemplars: MetricExemplarV2Output[];
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  count: bigint;
  /**
   * 64-bit Floating Point
   * IEEE 754 double-precision floating point (64-bit). Provides ~15 decimal digits of precision.
   */
  sum: number;
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint16
   */
  quantiles: QuantileValueV2Output[];
}

export type SummaryPointV2 = SummaryPointV2Output;

export class SummaryPointV2Encoder extends BitStreamEncoder {
  private compressionDict: Map<string, number> = new Map();

  constructor() {
    super("msb_first");
  }

  encode(value: SummaryPointV2Input): Uint8Array {
    // Reset compression dictionary for each encode
    this.compressionDict.clear();

    this.writeUint8(4);
    this.writeUint32(value.descriptor_id, "big_endian");
    this.writeUint64(value.start_unix_nano, "big_endian");
    this.writeUint64(value.ts_unix_nano, "big_endian");
    this.writeUint32(value.flags, "big_endian");
    this.writeUint16(value.attributes.length, "big_endian");
    for (let value_attributes__iter_index = 0; value_attributes__iter_index < value.attributes.length; value_attributes__iter_index++) {
      const value_attributes__iter = value.attributes[value_attributes__iter_index];
      const encoder_value_attributes__iter = new LabelPairEncoder();
      const encoded_value_attributes__iter = encoder_value_attributes__iter.encode(value_attributes__iter);
      for (const byte of encoded_value_attributes__iter) {
        this.writeUint8(byte);
      }
    }
    this.writeUint16(value.exemplars.length, "big_endian");
    for (let value_exemplars__iter_index = 0; value_exemplars__iter_index < value.exemplars.length; value_exemplars__iter_index++) {
      const value_exemplars__iter = value.exemplars[value_exemplars__iter_index];
      const encoder_value_exemplars__iter = new MetricExemplarV2Encoder();
      const encoded_value_exemplars__iter = encoder_value_exemplars__iter.encode(value_exemplars__iter);
      for (const byte of encoded_value_exemplars__iter) {
        this.writeUint8(byte);
      }
    }
    this.writeUint64(value.count, "big_endian");
    this.writeFloat64(value.sum, "big_endian");
    this.writeUint16(value.quantiles.length, "big_endian");
    for (let value_quantiles__iter_index = 0; value_quantiles__iter_index < value.quantiles.length; value_quantiles__iter_index++) {
      const value_quantiles__iter = value.quantiles[value_quantiles__iter_index];
      const encoder_value_quantiles__iter = new QuantileValueV2Encoder();
      const encoded_value_quantiles__iter = encoder_value_quantiles__iter.encode(value_quantiles__iter);
      for (const byte of encoded_value_quantiles__iter) {
        this.writeUint8(byte);
      }
    }
    return this.finish();
  }

  /**
   * Calculate the encoded size of a SummaryPointV2 value.
   * Used for from_after_field computed lengths and buffer pre-allocation.
   */
  calculateSize(value: SummaryPointV2): number {
    let size = 0;
    size += 25; // tag (const) + descriptor_id + start_unix_nano + ts_unix_nano + flags
    // attributes: array (kind: length_prefixed)
    for (const item of value.attributes) {
      const attributes_itemEncoder = new LabelPairEncoder();
      size += attributes_itemEncoder.calculateSize(item);
    }
    size += 2; // length prefix (uint16)
    // exemplars: array (kind: length_prefixed)
    for (const item of value.exemplars) {
      const exemplars_itemEncoder = new MetricExemplarV2Encoder();
      size += exemplars_itemEncoder.calculateSize(item);
    }
    size += 18; // length prefix (uint16) + count + sum
    // quantiles: array (kind: length_prefixed)
    for (const item of value.quantiles) {
      const quantiles_itemEncoder = new QuantileValueV2Encoder();
      size += quantiles_itemEncoder.calculateSize(item);
    }
    size += 2; // length prefix (uint16)
    return size;
  }
}

export class SummaryPointV2Decoder extends SeekableBitStreamDecoder {
  constructor(input: Uint8Array | number[] | string, private context?: any) {
    const reader = createReader(input);
    super(reader, "msb_first");
  }

  decode(): SummaryPointV2Output {
    const value: any = {};

    value.tag = this.readUint8();
    value.descriptor_id = this.readUint32("big_endian");
    value.start_unix_nano = this.readUint64("big_endian");
    value.ts_unix_nano = this.readUint64("big_endian");
    value.flags = this.readUint32("big_endian");
    value.attributes = [];
    const attributes_length = this.readUint16("big_endian");
    for (let i = 0; i < attributes_length; i++) {
      let attributes__iter: any;
      attributes__iter = {};
      const attributes__iter_key_length = this.readUint8();
      const attributes__iter_key_bytes = this.readBytesSlice(attributes__iter_key_length);
      try {
        attributes__iter.key = new TextDecoder("utf-8", { fatal: true }).decode(attributes__iter_key_bytes);
      } catch (e) {
        throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
      }
      const attributes__iter_value_length = this.readUint16("big_endian");
      const attributes__iter_value_bytes = this.readBytesSlice(attributes__iter_value_length);
      try {
        attributes__iter.value = new TextDecoder("utf-8", { fatal: true }).decode(attributes__iter_value_bytes);
      } catch (e) {
        throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
      }
      value.attributes.push(attributes__iter);
    }
    value.exemplars = [];
    const exemplars_length = this.readUint16("big_endian");
    for (let i = 0; i < exemplars_length; i++) {
      let exemplars__iter: any;
      exemplars__iter = {};
      exemplars__iter.ts_unix_nano = this.readUint64("big_endian");
      exemplars__iter.number = {};
      const discriminator = this.peekUint8();
      if (discriminator === 1) {
        const decoder = new IntegerValueV2Decoder(this.bytes.slice(this.byteOffset), exemplars__iter.number);
        const decodedValue = decoder.decode();
        this.byteOffset += decoder.byteOffset;
        exemplars__iter.number.value = { type: 'IntegerValueV2', value: decodedValue };
      }
      else if (discriminator === 2) {
        const decoder = new DoubleValueV2Decoder(this.bytes.slice(this.byteOffset), exemplars__iter.number);
        const decodedValue = decoder.decode();
        this.byteOffset += decoder.byteOffset;
        exemplars__iter.number.value = { type: 'DoubleValueV2', value: decodedValue };
      } else {
        throw new BinSchemaError(ErrorCode.INVALID_VARIANT, `Unknown discriminator: 0x${discriminator.toString(16)}`);
      }
      exemplars__iter.filtered_attrs = [];
      const exemplars__iter_filtered_attrs_length = this.readUint16("big_endian");
      for (let i = 0; i < exemplars__iter_filtered_attrs_length; i++) {
        let exemplars__iter_filtered_attrs__iter: any;
        exemplars__iter_filtered_attrs__iter = {};
        const exemplars__iter_filtered_attrs__iter_key_length = this.readUint8();
        const exemplars__iter_filtered_attrs__iter_key_bytes = this.readBytesSlice(exemplars__iter_filtered_attrs__iter_key_length);
        try {
          exemplars__iter_filtered_attrs__iter.key = new TextDecoder("utf-8", { fatal: true }).decode(exemplars__iter_filtered_attrs__iter_key_bytes);
        } catch (e) {
          throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
        }
        const exemplars__iter_filtered_attrs__iter_value_length = this.readUint16("big_endian");
        const exemplars__iter_filtered_attrs__iter_value_bytes = this.readBytesSlice(exemplars__iter_filtered_attrs__iter_value_length);
        try {
          exemplars__iter_filtered_attrs__iter.value = new TextDecoder("utf-8", { fatal: true }).decode(exemplars__iter_filtered_attrs__iter_value_bytes);
        } catch (e) {
          throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
        }
        exemplars__iter.filtered_attrs.push(exemplars__iter_filtered_attrs__iter);
      }
      exemplars__iter.trace_id = [];
      for (let i = 0; i < 16; i++) {
        let exemplars__iter_trace_id__iter: any;
        exemplars__iter_trace_id__iter = this.readUint8();
        exemplars__iter.trace_id.push(exemplars__iter_trace_id__iter);
      }
      exemplars__iter.span_id = [];
      for (let i = 0; i < 8; i++) {
        let exemplars__iter_span_id__iter: any;
        exemplars__iter_span_id__iter = this.readUint8();
        exemplars__iter.span_id.push(exemplars__iter_span_id__iter);
      }
      value.exemplars.push(exemplars__iter);
    }
    value.count = this.readUint64("big_endian");
    value.sum = this.readFloat64("big_endian");
    value.quantiles = [];
    const quantiles_length = this.readUint16("big_endian");
    for (let i = 0; i < quantiles_length; i++) {
      let quantiles__iter: any;
      quantiles__iter = {};
      quantiles__iter.quantile = this.readFloat64("big_endian");
      quantiles__iter.value = this.readFloat64("big_endian");
      value.quantiles.push(quantiles__iter);
    }
    return value;
  }
}

/**
 * Lossless scalar value union.
 */
export interface MetricNumberV2Input {
  /**
   * Discriminated Union
   * Type that can be one of several variants, chosen based on a discriminator value. Supports peek-based (read ahead) or field-based (reference earlier field) discrimination.
   *
   * @remarks
   *
   * Discriminator: peek uint8
   * Variants: 2
   * - IntegerValueV2 (when value === 1)
   * - DoubleValueV2 (when value === 2)
   */
  value: { type: 'IntegerValueV2'; value: IntegerValueV2Input } | { type: 'DoubleValueV2'; value: DoubleValueV2Input };
}

/**
 * Lossless scalar value union.
 */
export interface MetricNumberV2Output {
  /**
   * Discriminated Union
   * Type that can be one of several variants, chosen based on a discriminator value. Supports peek-based (read ahead) or field-based (reference earlier field) discrimination.
   *
   * @remarks
   *
   * Discriminator: peek uint8
   * Variants: 2
   * - IntegerValueV2 (when value === 1)
   * - DoubleValueV2 (when value === 2)
   */
  value: { type: 'IntegerValueV2'; value: IntegerValueV2Output } | { type: 'DoubleValueV2'; value: DoubleValueV2Output };
}

export type MetricNumberV2 = MetricNumberV2Output;

/**
 * Variant tags for MetricNumberV2.value
 */
export const enum MetricNumberV2ValueVariant {
  IntegerValueV2 = 'IntegerValueV2',
  DoubleValueV2 = 'DoubleValueV2',
}

export class MetricNumberV2Encoder extends BitStreamEncoder {
  private compressionDict: Map<string, number> = new Map();

  constructor() {
    super("msb_first");
  }

  encode(value: MetricNumberV2Input): Uint8Array {
    // Reset compression dictionary for each encode
    this.compressionDict.clear();

    if (value.value.type === 'IntegerValueV2') {
      const encoder_value = new IntegerValueV2Encoder();
      const encoded_value = encoder_value.encode(value.value.value);
      for (const byte of encoded_value) {
        this.writeUint8(byte);
      }
    }
    else if (value.value.type === 'DoubleValueV2') {
      const encoder_value = new DoubleValueV2Encoder();
      const encoded_value = encoder_value.encode(value.value.value);
      for (const byte of encoded_value) {
        this.writeUint8(byte);
      }
    } else {
      throw new BinSchemaError(ErrorCode.INVALID_VARIANT, `Unknown variant type: ${(value.value as any).type}`);
    }
    return this.finish();
  }

  /**
   * Calculate the encoded size of a MetricNumberV2 value.
   * Used for from_after_field computed lengths and buffer pre-allocation.
   */
  calculateSize(value: MetricNumberV2): number {
    let size = 0;
    if (value.type === 'IntegerValueV2') {
      const _enc = new IntegerValueV2Encoder();
      size += _enc.calculateSize(value.value);
    }
    else if (value.type === 'DoubleValueV2') {
      const _enc = new DoubleValueV2Encoder();
      size += _enc.calculateSize(value.value);
    }
    else {
      throw new BinSchemaError(ErrorCode.INVALID_VARIANT, `Unknown variant type for value: ${(value as any).type}`);
    }
    return size;
  }
}

export class MetricNumberV2Decoder extends SeekableBitStreamDecoder {
  constructor(input: Uint8Array | number[] | string, private context?: any) {
    const reader = createReader(input);
    super(reader, "msb_first");
  }

  decode(): MetricNumberV2Output {
    const value: any = {};

    const discriminator = this.peekUint8();
    if (discriminator === 1) {
      const decoder = new IntegerValueV2Decoder(this.bytes.slice(this.byteOffset), value);
      const decodedValue = decoder.decode();
      this.byteOffset += decoder.byteOffset;
      value.value = { type: 'IntegerValueV2', value: decodedValue };
    }
    else if (discriminator === 2) {
      const decoder = new DoubleValueV2Decoder(this.bytes.slice(this.byteOffset), value);
      const decodedValue = decoder.decode();
      this.byteOffset += decoder.byteOffset;
      value.value = { type: 'DoubleValueV2', value: decodedValue };
    } else {
      throw new BinSchemaError(ErrorCode.INVALID_VARIANT, `Unknown discriminator: 0x${discriminator.toString(16)}`);
    }
    return value;
  }
}

/**
 * Histogram count union for integer-count and float-count native histograms.
 */
export interface MetricCountV2Input {
  /**
   * Discriminated Union
   * Type that can be one of several variants, chosen based on a discriminator value. Supports peek-based (read ahead) or field-based (reference earlier field) discrimination.
   *
   * @remarks
   *
   * Discriminator: peek uint8
   * Variants: 2
   * - IntegerCountV2 (when value === 1)
   * - FloatCountV2 (when value === 2)
   */
  value: { type: 'IntegerCountV2'; value: IntegerCountV2Input } | { type: 'FloatCountV2'; value: FloatCountV2Input };
}

/**
 * Histogram count union for integer-count and float-count native histograms.
 */
export interface MetricCountV2Output {
  /**
   * Discriminated Union
   * Type that can be one of several variants, chosen based on a discriminator value. Supports peek-based (read ahead) or field-based (reference earlier field) discrimination.
   *
   * @remarks
   *
   * Discriminator: peek uint8
   * Variants: 2
   * - IntegerCountV2 (when value === 1)
   * - FloatCountV2 (when value === 2)
   */
  value: { type: 'IntegerCountV2'; value: IntegerCountV2Output } | { type: 'FloatCountV2'; value: FloatCountV2Output };
}

export type MetricCountV2 = MetricCountV2Output;

/**
 * Variant tags for MetricCountV2.value
 */
export const enum MetricCountV2ValueVariant {
  IntegerCountV2 = 'IntegerCountV2',
  FloatCountV2 = 'FloatCountV2',
}

export class MetricCountV2Encoder extends BitStreamEncoder {
  private compressionDict: Map<string, number> = new Map();

  constructor() {
    super("msb_first");
  }

  encode(value: MetricCountV2Input): Uint8Array {
    // Reset compression dictionary for each encode
    this.compressionDict.clear();

    if (value.value.type === 'IntegerCountV2') {
      const encoder_value = new IntegerCountV2Encoder();
      const encoded_value = encoder_value.encode(value.value.value);
      for (const byte of encoded_value) {
        this.writeUint8(byte);
      }
    }
    else if (value.value.type === 'FloatCountV2') {
      const encoder_value = new FloatCountV2Encoder();
      const encoded_value = encoder_value.encode(value.value.value);
      for (const byte of encoded_value) {
        this.writeUint8(byte);
      }
    } else {
      throw new BinSchemaError(ErrorCode.INVALID_VARIANT, `Unknown variant type: ${(value.value as any).type}`);
    }
    return this.finish();
  }

  /**
   * Calculate the encoded size of a MetricCountV2 value.
   * Used for from_after_field computed lengths and buffer pre-allocation.
   */
  calculateSize(value: MetricCountV2): number {
    let size = 0;
    if (value.type === 'IntegerCountV2') {
      const _enc = new IntegerCountV2Encoder();
      size += _enc.calculateSize(value.value);
    }
    else if (value.type === 'FloatCountV2') {
      const _enc = new FloatCountV2Encoder();
      size += _enc.calculateSize(value.value);
    }
    else {
      throw new BinSchemaError(ErrorCode.INVALID_VARIANT, `Unknown variant type for value: ${(value as any).type}`);
    }
    return size;
  }
}

export class MetricCountV2Decoder extends SeekableBitStreamDecoder {
  constructor(input: Uint8Array | number[] | string, private context?: any) {
    const reader = createReader(input);
    super(reader, "msb_first");
  }

  decode(): MetricCountV2Output {
    const value: any = {};

    const discriminator = this.peekUint8();
    if (discriminator === 1) {
      const decoder = new IntegerCountV2Decoder(this.bytes.slice(this.byteOffset), value);
      const decodedValue = decoder.decode();
      this.byteOffset += decoder.byteOffset;
      value.value = { type: 'IntegerCountV2', value: decodedValue };
    }
    else if (discriminator === 2) {
      const decoder = new FloatCountV2Decoder(this.bytes.slice(this.byteOffset), value);
      const decodedValue = decoder.decode();
      this.byteOffset += decoder.byteOffset;
      value.value = { type: 'FloatCountV2', value: decodedValue };
    } else {
      throw new BinSchemaError(ErrorCode.INVALID_VARIANT, `Unknown discriminator: 0x${discriminator.toString(16)}`);
    }
    return value;
  }
}

/**
 * Signed 64-bit scalar.
 */
export interface IntegerValueV2Input {
  /**
   * 64-bit Signed Integer
   * Fixed-width 64-bit signed integer (-9223372036854775808 to 9223372036854775807). Respects endianness configuration.
   */
  value: bigint;
}

/**
 * Signed 64-bit scalar.
 */
export interface IntegerValueV2Output {
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  tag: number;
  /**
   * 64-bit Signed Integer
   * Fixed-width 64-bit signed integer (-9223372036854775808 to 9223372036854775807). Respects endianness configuration.
   */
  value: bigint;
}

export type IntegerValueV2 = IntegerValueV2Output;

export class IntegerValueV2Encoder extends BitStreamEncoder {
  private compressionDict: Map<string, number> = new Map();

  constructor() {
    super("msb_first");
  }

  encode(value: IntegerValueV2Input): Uint8Array {
    // Reset compression dictionary for each encode
    this.compressionDict.clear();

    this.writeUint8(1);
    this.writeInt64(value.value, "big_endian");
    return this.finish();
  }

  /**
   * Calculate the encoded size of a IntegerValueV2 value.
   * Used for from_after_field computed lengths and buffer pre-allocation.
   */
  calculateSize(value: IntegerValueV2): number {
    return 9; // tag (const) + value
  }
}

export class IntegerValueV2Decoder extends SeekableBitStreamDecoder {
  constructor(input: Uint8Array | number[] | string, private context?: any) {
    const reader = createReader(input);
    super(reader, "msb_first");
  }

  decode(): IntegerValueV2Output {
    const value: any = {};

    value.tag = this.readUint8();
    value.value = this.readInt64("big_endian");
    return value;
  }
}

/**
 * IEEE-754 scalar.
 */
export interface DoubleValueV2Input {
  /**
   * 64-bit Floating Point
   * IEEE 754 double-precision floating point (64-bit). Provides ~15 decimal digits of precision.
   */
  value: number;
}

/**
 * IEEE-754 scalar.
 */
export interface DoubleValueV2Output {
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  tag: number;
  /**
   * 64-bit Floating Point
   * IEEE 754 double-precision floating point (64-bit). Provides ~15 decimal digits of precision.
   */
  value: number;
}

export type DoubleValueV2 = DoubleValueV2Output;

export class DoubleValueV2Encoder extends BitStreamEncoder {
  private compressionDict: Map<string, number> = new Map();

  constructor() {
    super("msb_first");
  }

  encode(value: DoubleValueV2Input): Uint8Array {
    // Reset compression dictionary for each encode
    this.compressionDict.clear();

    this.writeUint8(2);
    this.writeFloat64(value.value, "big_endian");
    return this.finish();
  }

  /**
   * Calculate the encoded size of a DoubleValueV2 value.
   * Used for from_after_field computed lengths and buffer pre-allocation.
   */
  calculateSize(value: DoubleValueV2): number {
    return 9; // tag (const) + value
  }
}

export class DoubleValueV2Decoder extends SeekableBitStreamDecoder {
  constructor(input: Uint8Array | number[] | string, private context?: any) {
    const reader = createReader(input);
    super(reader, "msb_first");
  }

  decode(): DoubleValueV2Output {
    const value: any = {};

    value.tag = this.readUint8();
    value.value = this.readFloat64("big_endian");
    return value;
  }
}

/**
 * Exact non-negative count.
 */
export interface IntegerCountV2Input {
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  value: bigint;
}

/**
 * Exact non-negative count.
 */
export interface IntegerCountV2Output {
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  tag: number;
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  value: bigint;
}

export type IntegerCountV2 = IntegerCountV2Output;

export class IntegerCountV2Encoder extends BitStreamEncoder {
  private compressionDict: Map<string, number> = new Map();

  constructor() {
    super("msb_first");
  }

  encode(value: IntegerCountV2Input): Uint8Array {
    // Reset compression dictionary for each encode
    this.compressionDict.clear();

    this.writeUint8(1);
    this.writeUint64(value.value, "big_endian");
    return this.finish();
  }

  /**
   * Calculate the encoded size of a IntegerCountV2 value.
   * Used for from_after_field computed lengths and buffer pre-allocation.
   */
  calculateSize(value: IntegerCountV2): number {
    return 9; // tag (const) + value
  }
}

export class IntegerCountV2Decoder extends SeekableBitStreamDecoder {
  constructor(input: Uint8Array | number[] | string, private context?: any) {
    const reader = createReader(input);
    super(reader, "msb_first");
  }

  decode(): IntegerCountV2Output {
    const value: any = {};

    value.tag = this.readUint8();
    value.value = this.readUint64("big_endian");
    return value;
  }
}

/**
 * Floating-point count.
 */
export interface FloatCountV2Input {
  /**
   * 64-bit Floating Point
   * IEEE 754 double-precision floating point (64-bit). Provides ~15 decimal digits of precision.
   */
  value: number;
}

/**
 * Floating-point count.
 */
export interface FloatCountV2Output {
  /**
   * 8-bit Unsigned Integer
   * Fixed-width 8-bit unsigned integer (0-255). Single byte, no endianness concerns.
   */
  tag: number;
  /**
   * 64-bit Floating Point
   * IEEE 754 double-precision floating point (64-bit). Provides ~15 decimal digits of precision.
   */
  value: number;
}

export type FloatCountV2 = FloatCountV2Output;

export class FloatCountV2Encoder extends BitStreamEncoder {
  private compressionDict: Map<string, number> = new Map();

  constructor() {
    super("msb_first");
  }

  encode(value: FloatCountV2Input): Uint8Array {
    // Reset compression dictionary for each encode
    this.compressionDict.clear();

    this.writeUint8(2);
    this.writeFloat64(value.value, "big_endian");
    return this.finish();
  }

  /**
   * Calculate the encoded size of a FloatCountV2 value.
   * Used for from_after_field computed lengths and buffer pre-allocation.
   */
  calculateSize(value: FloatCountV2): number {
    return 9; // tag (const) + value
  }
}

export class FloatCountV2Decoder extends SeekableBitStreamDecoder {
  constructor(input: Uint8Array | number[] | string, private context?: any) {
    const reader = createReader(input);
    super(reader, "msb_first");
  }

  decode(): FloatCountV2Output {
    const value: any = {};

    value.tag = this.readUint8();
    value.value = this.readFloat64("big_endian");
    return value;
  }
}

/**
 * Sparse exponential histogram bucket run. Deltas are signed offsets; counts share one numeric representation.
 */
export interface SparseBucketsV2Input {
  /**
   * 32-bit Signed Integer
   * Fixed-width 32-bit signed integer (-2147483648 to 2147483647). Respects endianness configuration.
   */
  offset: number;
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint32
   */
  deltas: number[];
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint32
   */
  counts: MetricCountV2Input[];
}

/**
 * Sparse exponential histogram bucket run. Deltas are signed offsets; counts share one numeric representation.
 */
export interface SparseBucketsV2Output {
  /**
   * 32-bit Signed Integer
   * Fixed-width 32-bit signed integer (-2147483648 to 2147483647). Respects endianness configuration.
   */
  offset: number;
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint32
   */
  deltas: number[];
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint32
   */
  counts: MetricCountV2Output[];
}

export type SparseBucketsV2 = SparseBucketsV2Output;

export class SparseBucketsV2Encoder extends BitStreamEncoder {
  private compressionDict: Map<string, number> = new Map();

  constructor() {
    super("msb_first");
  }

  encode(value: SparseBucketsV2Input): Uint8Array {
    // Reset compression dictionary for each encode
    this.compressionDict.clear();

    this.writeInt32(value.offset, "big_endian");
    this.writeUint32(value.deltas.length, "big_endian");
    for (let value_deltas__iter_index = 0; value_deltas__iter_index < value.deltas.length; value_deltas__iter_index++) {
      const value_deltas__iter = value.deltas[value_deltas__iter_index];
      this.writeInt32(value_deltas__iter, "big_endian");
    }
    this.writeUint32(value.counts.length, "big_endian");
    for (let value_counts__iter_index = 0; value_counts__iter_index < value.counts.length; value_counts__iter_index++) {
      const value_counts__iter = value.counts[value_counts__iter_index];
      const encoder_value_counts__iter = new MetricCountV2Encoder();
      const encoded_value_counts__iter = encoder_value_counts__iter.encode(value_counts__iter);
      for (const byte of encoded_value_counts__iter) {
        this.writeUint8(byte);
      }
    }
    return this.finish();
  }

  /**
   * Calculate the encoded size of a SparseBucketsV2 value.
   * Used for from_after_field computed lengths and buffer pre-allocation.
   */
  calculateSize(value: SparseBucketsV2): number {
    let size = 0;
    size += 4; // offset
    // deltas: array (kind: length_prefixed)
    for (const item of value.deltas) {
      size += 4;
    }
    size += 4; // length prefix (uint32)
    // counts: array (kind: length_prefixed)
    for (const item of value.counts) {
      const counts_itemEncoder = new MetricCountV2Encoder();
      size += counts_itemEncoder.calculateSize(item);
    }
    size += 4; // length prefix (uint32)
    return size;
  }
}

export class SparseBucketsV2Decoder extends SeekableBitStreamDecoder {
  constructor(input: Uint8Array | number[] | string, private context?: any) {
    const reader = createReader(input);
    super(reader, "msb_first");
  }

  decode(): SparseBucketsV2Output {
    const value: any = {};

    value.offset = this.readInt32("big_endian");
    value.deltas = [];
    const deltas_length = this.readUint32("big_endian");
    for (let i = 0; i < deltas_length; i++) {
      let deltas__iter: any;
      deltas__iter = this.readInt32("big_endian");
      value.deltas.push(deltas__iter);
    }
    value.counts = [];
    const counts_length = this.readUint32("big_endian");
    for (let i = 0; i < counts_length; i++) {
      let counts__iter: any;
      counts__iter = {};
      const discriminator = this.peekUint8();
      if (discriminator === 1) {
        const decoder = new IntegerCountV2Decoder(this.bytes.slice(this.byteOffset), counts__iter);
        const decodedValue = decoder.decode();
        this.byteOffset += decoder.byteOffset;
        counts__iter.value = { type: 'IntegerCountV2', value: decodedValue };
      }
      else if (discriminator === 2) {
        const decoder = new FloatCountV2Decoder(this.bytes.slice(this.byteOffset), counts__iter);
        const decodedValue = decoder.decode();
        this.byteOffset += decoder.byteOffset;
        counts__iter.value = { type: 'FloatCountV2', value: decodedValue };
      } else {
        throw new BinSchemaError(ErrorCode.INVALID_VARIANT, `Unknown discriminator: 0x${discriminator.toString(16)}`);
      }
      value.counts.push(counts__iter);
    }
    return value;
  }
}

/**
 * One summary quantile.
 */
export interface QuantileValueV2Input {
  /**
   * 64-bit Floating Point
   * IEEE 754 double-precision floating point (64-bit). Provides ~15 decimal digits of precision.
   */
  quantile: number;
  /**
   * 64-bit Floating Point
   * IEEE 754 double-precision floating point (64-bit). Provides ~15 decimal digits of precision.
   */
  value: number;
}

/**
 * One summary quantile.
 */
export interface QuantileValueV2Output {
  /**
   * 64-bit Floating Point
   * IEEE 754 double-precision floating point (64-bit). Provides ~15 decimal digits of precision.
   */
  quantile: number;
  /**
   * 64-bit Floating Point
   * IEEE 754 double-precision floating point (64-bit). Provides ~15 decimal digits of precision.
   */
  value: number;
}

export type QuantileValueV2 = QuantileValueV2Output;

export class QuantileValueV2Encoder extends BitStreamEncoder {
  private compressionDict: Map<string, number> = new Map();

  constructor() {
    super("msb_first");
  }

  encode(value: QuantileValueV2Input): Uint8Array {
    // Reset compression dictionary for each encode
    this.compressionDict.clear();

    this.writeFloat64(value.quantile, "big_endian");
    this.writeFloat64(value.value, "big_endian");
    return this.finish();
  }

  /**
   * Calculate the encoded size of a QuantileValueV2 value.
   * Used for from_after_field computed lengths and buffer pre-allocation.
   */
  calculateSize(value: QuantileValueV2): number {
    return 16; // quantile + value
  }
}

export class QuantileValueV2Decoder extends SeekableBitStreamDecoder {
  constructor(input: Uint8Array | number[] | string, private context?: any) {
    const reader = createReader(input);
    super(reader, "msb_first");
  }

  decode(): QuantileValueV2Output {
    const value: any = {};

    value.quantile = this.readFloat64("big_endian");
    value.value = this.readFloat64("big_endian");
    return value;
  }
}

/**
 * Exemplar attached to a metric point.
 */
export interface MetricExemplarV2Input {
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  ts_unix_nano: bigint;
  /**
   * Lossless scalar value union.
   */
  number: MetricNumberV2Input;
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint16
   */
  filtered_attrs: LabelPairInput[];
  /**
   * Bytes
   * Raw byte array. Sugar for array of uint8 — same wire format, simpler schema definition.
   */
  trace_id: number[];
  /**
   * Bytes
   * Raw byte array. Sugar for array of uint8 — same wire format, simpler schema definition.
   */
  span_id: number[];
}

/**
 * Exemplar attached to a metric point.
 */
export interface MetricExemplarV2Output {
  /**
   * 64-bit Unsigned Integer
   * Fixed-width 64-bit unsigned integer (0-18446744073709551615). Respects endianness configuration.
   */
  ts_unix_nano: bigint;
  /**
   * Lossless scalar value union.
   */
  number: MetricNumberV2Output;
  /**
   * Array
   * Collection of elements of the same type. Supports fixed-length, length-prefixed, byte-length-prefixed, field-referenced, and null-terminated arrays.
   *
   * @remarks
   *
   * Array kind: length_prefixed
   * Length prefix type: uint16
   */
  filtered_attrs: LabelPairOutput[];
  /**
   * Bytes
   * Raw byte array. Sugar for array of uint8 — same wire format, simpler schema definition.
   */
  trace_id: number[];
  /**
   * Bytes
   * Raw byte array. Sugar for array of uint8 — same wire format, simpler schema definition.
   */
  span_id: number[];
}

export type MetricExemplarV2 = MetricExemplarV2Output;

export class MetricExemplarV2Encoder extends BitStreamEncoder {
  private compressionDict: Map<string, number> = new Map();

  constructor() {
    super("msb_first");
  }

  encode(value: MetricExemplarV2Input): Uint8Array {
    // Reset compression dictionary for each encode
    this.compressionDict.clear();

    this.writeUint64(value.ts_unix_nano, "big_endian");
    const encoder_number = new MetricNumberV2Encoder();
    const encoded_number = encoder_number.encode(value.number);
    for (const byte of encoded_number) {
      this.writeUint8(byte);
    }
    this.writeUint16(value.filtered_attrs.length, "big_endian");
    for (let value_filtered_attrs__iter_index = 0; value_filtered_attrs__iter_index < value.filtered_attrs.length; value_filtered_attrs__iter_index++) {
      const value_filtered_attrs__iter = value.filtered_attrs[value_filtered_attrs__iter_index];
      const encoder_value_filtered_attrs__iter = new LabelPairEncoder();
      const encoded_value_filtered_attrs__iter = encoder_value_filtered_attrs__iter.encode(value_filtered_attrs__iter);
      for (const byte of encoded_value_filtered_attrs__iter) {
        this.writeUint8(byte);
      }
    }
    // Validate fixed-length array
    if (value.trace_id.length !== 16) {
      throw new Error(`Array 'trace_id' must have exactly 16 elements, got ${value.trace_id.length}`);
    }
    for (let value_trace_id__iter_index = 0; value_trace_id__iter_index < value.trace_id.length; value_trace_id__iter_index++) {
      const value_trace_id__iter = value.trace_id[value_trace_id__iter_index];
      this.writeUint8(value_trace_id__iter);
    }
    // Validate fixed-length array
    if (value.span_id.length !== 8) {
      throw new Error(`Array 'span_id' must have exactly 8 elements, got ${value.span_id.length}`);
    }
    for (let value_span_id__iter_index = 0; value_span_id__iter_index < value.span_id.length; value_span_id__iter_index++) {
      const value_span_id__iter = value.span_id[value_span_id__iter_index];
      this.writeUint8(value_span_id__iter);
    }
    return this.finish();
  }

  /**
   * Calculate the encoded size of a MetricExemplarV2 value.
   * Used for from_after_field computed lengths and buffer pre-allocation.
   */
  calculateSize(value: MetricExemplarV2): number {
    let size = 0;
    size += 8; // ts_unix_nano
    // number: custom type (MetricNumberV2)
    const number_encoder = new MetricNumberV2Encoder();
    size += number_encoder.calculateSize(value.number);
    // filtered_attrs: array (kind: length_prefixed)
    for (const item of value.filtered_attrs) {
      const filtered_attrs_itemEncoder = new LabelPairEncoder();
      size += filtered_attrs_itemEncoder.calculateSize(item);
    }
    size += 2; // length prefix (uint16)
    // trace_id: bytes (kind: fixed)
    size += value.trace_id.length;
    // span_id: bytes (kind: fixed)
    size += value.span_id.length;
    return size;
  }
}

export class MetricExemplarV2Decoder extends SeekableBitStreamDecoder {
  constructor(input: Uint8Array | number[] | string, private context?: any) {
    const reader = createReader(input);
    super(reader, "msb_first");
  }

  decode(): MetricExemplarV2Output {
    const value: any = {};

    value.ts_unix_nano = this.readUint64("big_endian");
    value.number = {};
    const discriminator = this.peekUint8();
    if (discriminator === 1) {
      const decoder = new IntegerValueV2Decoder(this.bytes.slice(this.byteOffset), value.number);
      const decodedValue = decoder.decode();
      this.byteOffset += decoder.byteOffset;
      value.number.value = { type: 'IntegerValueV2', value: decodedValue };
    }
    else if (discriminator === 2) {
      const decoder = new DoubleValueV2Decoder(this.bytes.slice(this.byteOffset), value.number);
      const decodedValue = decoder.decode();
      this.byteOffset += decoder.byteOffset;
      value.number.value = { type: 'DoubleValueV2', value: decodedValue };
    } else {
      throw new BinSchemaError(ErrorCode.INVALID_VARIANT, `Unknown discriminator: 0x${discriminator.toString(16)}`);
    }
    value.filtered_attrs = [];
    const filtered_attrs_length = this.readUint16("big_endian");
    for (let i = 0; i < filtered_attrs_length; i++) {
      let filtered_attrs__iter: any;
      filtered_attrs__iter = {};
      const filtered_attrs__iter_key_length = this.readUint8();
      const filtered_attrs__iter_key_bytes = this.readBytesSlice(filtered_attrs__iter_key_length);
      try {
        filtered_attrs__iter.key = new TextDecoder("utf-8", { fatal: true }).decode(filtered_attrs__iter_key_bytes);
      } catch (e) {
        throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
      }
      const filtered_attrs__iter_value_length = this.readUint16("big_endian");
      const filtered_attrs__iter_value_bytes = this.readBytesSlice(filtered_attrs__iter_value_length);
      try {
        filtered_attrs__iter.value = new TextDecoder("utf-8", { fatal: true }).decode(filtered_attrs__iter_value_bytes);
      } catch (e) {
        throw new BinSchemaError(ErrorCode.INVALID_UTF8, "Invalid UTF-8 in decoded string", { cause: e as Error });
      }
      value.filtered_attrs.push(filtered_attrs__iter);
    }
    value.trace_id = [];
    for (let i = 0; i < 16; i++) {
      let trace_id__iter: any;
      trace_id__iter = this.readUint8();
      value.trace_id.push(trace_id__iter);
    }
    value.span_id = [];
    for (let i = 0; i < 8; i++) {
      let span_id__iter: any;
      span_id__iter = this.readUint8();
      value.span_id.push(span_id__iter);
    }
    return value;
  }
}

