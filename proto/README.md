# scry wire protocols

This directory holds the binschema source-of-truth definitions for every
wire format scry speaks. Rust and TypeScript bindings are generated and
committed so ordinary builds do not need binschema; do not edit generated
bindings or vendored runtime files by hand.

## Schemas

| File | Protocol | Status |
|------|----------|--------|
| [`ingest.schema.json`](./ingest.schema.json) | Producer ↔ ingest server, including tail and merged-live frames (D-006/D-052/D-054) | implemented |
| [`query.schema.json`](./query.schema.json) | Client ↔ query daemon (D-031) | implemented |
| [`query-worker.schema.json`](./query-worker.schema.json) | Private queryd ↔ queryd control plane | Phase 0/1 control plane implemented; distributed execution disabled |

## Generating code

From the repository root, use the wrapper so Rust and browser bindings cannot
drift:

```bash
scripts/gen-proto-all.sh
# Override the default $HOME/Projects/binschema checkout when needed:
BINSCHEMA_DIR=/path/to/binschema scripts/gen-proto-all.sh
```

The generators own:

- `crates/proto/src/generated*.rs` and `crates/binschema-runtime/src/*.rs`
  (all three schemas);
- `desktop/src/proto/*.ts` (`generated-ingest.ts` and `generated.ts` for the
  ingest/public-query browser bindings plus the shared TypeScript runtime; the
  private worker protocol has no browser binding).

`scripts/gen-proto.sh` and `scripts/gen-proto-ts.sh` are the language-specific
entry points. `scripts/gen-proto-check.sh` regenerates everything and checks for
committed drift.

## Conventions

- **Big-endian everywhere.** We accept the tiny performance cost for
  the readability win (`xxd` on a captured stream is human-readable).
- **Tag bytes are the discriminator.** Within each length-delimited frame, the
  message begins with a `uint8` tag matching its union variant. A tag does not
  establish a stream boundary; framing must remain synchronized on the outer
  length prefix.
- **Reasons are numeric.** Free-text `message` fields exist for
  operator logs only; agents and servers must decide based on the
  numeric `reason_code` / `code`. Keeps i18n and operator output
  decoupled from protocol semantics.
- **Ingest versioning is in the handshake.** `Hello.protocol_version` must be
  one of the server's explicitly supported versions; `HelloAck` echoes the
  accepted version. There are no per-message version bytes. The private worker
  protocol likewise requires an exact supported version; the public query wire
  currently has no handshake version field.
