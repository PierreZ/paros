# syntax=docker/dockerfile:1
#
# The paros image (#196): `parosd` and `parosctl` in a slim runtime. A plain
# multi-stage Rust build, not Nix `dockerTools`, so a fresh clone runs with
# `docker compose` alone — the one documented exception to the repository's
# Nix-only tooling (AGENTS.md). The builder's Rust version must equal the
# channel of `rust-toolchain.toml`; CI fails when they differ.

ARG RUST_VERSION=1.99.0

FROM rust:${RUST_VERSION}-slim-bookworm AS builder
ARG RUST_VERSION
# `crates/paros/build.rs` compiles the wire contract with prost-build.
RUN apt-get update \
 && apt-get install -y --no-install-recommends protobuf-compiler \
 && rm -rf /var/lib/apt/lists/*
# The image's toolchain, not the one `rust-toolchain.toml` would have rustup
# install with its extra components (the same channel, CI-checked).
ENV RUSTUP_TOOLCHAIN=${RUST_VERSION}
WORKDIR /src
COPY . .
# No `--locked`: the workspace commits no `Cargo.lock` (a library crate,
# `.gitignore`), so the build resolves its dependencies as every build does.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/src/target \
    cargo build --release -p parosd \
 && cp target/release/parosd target/release/parosctl target/release/paros-frontend /usr/local/bin/

FROM debian:bookworm-slim
RUN useradd --system --uid 10001 --home-dir /var/lib/paros paros \
 && mkdir -p /var/lib/paros \
 && chown paros:paros /var/lib/paros
COPY --from=builder /usr/local/bin/parosd /usr/local/bin/parosctl /usr/local/bin/paros-frontend /usr/local/bin/
USER paros
# A named volume mounted here takes the directory's ownership on first use.
VOLUME ["/var/lib/paros"]
ENV PAROS_DATA_DIR=/var/lib/paros
EXPOSE 4500
ENTRYPOINT ["parosd"]
