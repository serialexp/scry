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
#
# Runtime is distroless/cc (Debian 12 / bookworm glibc, libgcc, and the CA
# bundle — no shell, no package manager). Built on bookworm so the binaries'
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
    cargo build --release \
        --bin scry-ingestd \
        --bin scry-agent \
        --bin scry-list \
        --bin scry-gateway \
    && mkdir -p /out \
    && cp target/release/scry-ingestd target/release/scry-agent target/release/scry-list target/release/scry-gateway /out/

# ── runtime ────────────────────────────────────────────────────────────
# distroless/cc-debian12 ships glibc + libgcc + the CA bundle and nothing else
# (no shell, no apt) — minimal CVE surface. The `:nonroot` tag sets USER 65532.
FROM gcr.io/distroless/cc-debian12:nonroot AS runtime

COPY --from=builder /out/scry-ingestd /usr/local/bin/scry-ingestd
COPY --from=builder /out/scry-agent   /usr/local/bin/scry-agent
COPY --from=builder /out/scry-list    /usr/local/bin/scry-list
COPY --from=builder /out/scry-gateway /usr/local/bin/scry-gateway

# Explicit non-root user (the :nonroot tag already sets this; stated for clarity
# and so image scanners detect a non-root default unambiguously).
USER 65532:65532

# No ENTRYPOINT: the workload manifest sets `command:` (server / agent / gateway).
# Distroless has no shell, so manifests must use the exec form (a JSON array).
