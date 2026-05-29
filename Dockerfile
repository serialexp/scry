# syntax=docker/dockerfile:1
#
# One image, several roles. The same binary set runs the scry ingest server
# (scry-ingestd), the log-collection agent (scry-agent), and the foreign-protocol
# push gateway (scry-gateway, terminating OTLP traces / Pyroscope profiles /
# Prometheus remote-write); the Kubernetes manifests pick the role via
# `command:`. scry-list ships too for catalog reconcile/inspection from inside
# the cluster.
#
# kube-rs 3.x sets the toolchain floor (MSRV 1.88) and uses rustls, so the
# runtime needs no OpenSSL — only CA certificates for TLS to R2/Hetzner.

# ── builder ────────────────────────────────────────────────────────────
FROM rust:1.88-bookworm AS builder
WORKDIR /src

# Cache the dependency graph: copy manifests first, then sources.
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY proto ./proto

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release \
        --bin scry-ingestd \
        --bin scry-agent \
        --bin scry-list \
        --bin scry-gateway \
    && mkdir -p /out \
    && cp target/release/scry-ingestd target/release/scry-agent target/release/scry-list target/release/scry-gateway /out/

# ── runtime ────────────────────────────────────────────────────────────
FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /out/scry-ingestd /usr/local/bin/scry-ingestd
COPY --from=builder /out/scry-agent   /usr/local/bin/scry-agent
COPY --from=builder /out/scry-list    /usr/local/bin/scry-list
COPY --from=builder /out/scry-gateway /usr/local/bin/scry-gateway

# No ENTRYPOINT: the workload manifest sets `command:` (server / agent / gateway).
