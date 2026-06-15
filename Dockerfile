# syntax=docker/dockerfile:1
#
# One image, one binary, several roles. The single `scry` binary runs every
# operator role via a subcommand: `scry ingest` (ingest server), `scry query`
# (query daemon — the architectural counterpart to ingest, serving QueryService
# over the binschema-over-TCP wire), `scry get` (one-shot query CLI), `scry list`
# (catalog reconcile/inspect — also a long-running `--interval` reconciler
# sidecar), `scry agent` (log-collection + Prometheus-scrape agent), `scry
# gateway` (foreign-protocol push hub — OTLP traces / Pyroscope profiles /
# Prometheus remote-write), `scry web` (browser query UI), `scry compact`, and
# `scry retention`. The Kubernetes manifests pick the role via `command:`
# (e.g. `["scry","ingest",…]`).
#
# kube-rs 3.x sets the toolchain floor (MSRV 1.88) and uses rustls, so the
# runtime needs no OpenSSL — only CA certificates for TLS to R2/Hetzner.
#
# Runtime is distroless/cc (Debian 12 / bookworm glibc, libgcc, and the CA
# bundle — no shell, no package manager). Built on bookworm so the binary's
# glibc matches the distroless base. The `:nonroot` tag runs as UID 65532, so
# the image ships a non-root default user with no extra Dockerfile plumbing.

# ── builder ────────────────────────────────────────────────────────────
FROM rust:1.95-bookworm AS builder
WORKDIR /src

# Cache the dependency graph: copy manifests first, then sources.
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY proto ./proto

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --bin scry \
    && mkdir -p /out \
    && cp target/release/scry /out/

# ── runtime ────────────────────────────────────────────────────────────
# distroless/cc-debian12 ships glibc + libgcc + the CA bundle and nothing else
# (no shell, no apt) — minimal CVE surface. The `:nonroot` tag sets USER 65532.
FROM gcr.io/distroless/cc-debian12:nonroot AS runtime

COPY --from=builder /out/scry /usr/local/bin/scry

# Explicit non-root user (the :nonroot tag already sets this; stated for clarity
# and so image scanners detect a non-root default unambiguously).
USER 65532:65532

# No ENTRYPOINT: the workload manifest sets `command:` to pick the role, e.g.
# `["scry","ingest",…]` / `["scry","query",…]` / `["scry","agent",…]`.
# Distroless has no shell, so manifests must use the exec form (a JSON array).
