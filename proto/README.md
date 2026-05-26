# scry wire protocols

This directory holds the [binschema](../../binschema) source-of-truth
definitions for every wire format scry speaks. Encoders, decoders, and
human-readable protocol docs are *generated* from these — do not
write Rust/Go/TypeScript bindings by hand.

## Schemas

| File | Protocol | Status |
|------|----------|--------|
| [`ingest.schema.json`](./ingest.schema.json) | Agent → server ingest stream (D-006) | v0.1 draft |

Future additions (scatter-gather worker RPCs, control-plane pub/sub
payloads, snapshot upload format) will land alongside as separate
schemas.

## Generating code

```bash
# Rust (server, agent)
binschema generate --language rust --schema proto/ingest.schema.json \
  --out crates/ingest-proto/src/generated

# TypeScript (UI, debugging tools)
binschema generate --language ts --schema proto/ingest.schema.json \
  --out tools/ingest-debug/src/generated
```

## Generating docs

```bash
binschema docs build --schema proto/ingest.schema.json \
  --out docs/generated/ingest-protocol.html
```

The generated HTML belongs in `docs/generated/` and is checked in so
operators can read the protocol without a binschema install.

## Conventions

- **Big-endian everywhere.** We accept the tiny performance cost for
  the readability win (`xxd` on a captured stream is human-readable).
- **Tag bytes are the discriminator.** Every message variant begins
  with a `uint8 tag` matching its `Frame` discriminator value, so a
  random byte offset can be re-synced to the next message.
- **Reasons are numeric.** Free-text `message` fields exist for
  operator logs only; agents and servers must decide based on the
  numeric `reason_code` / `code`. Keeps i18n and operator output
  decoupled from protocol semantics.
- **Versioning is in the handshake.** `Hello.protocol_version` is
  the only place we negotiate; no per-message version bytes. To
  evolve the protocol incrementally, bump the version and let the
  server pick `min(agent, server)`.
