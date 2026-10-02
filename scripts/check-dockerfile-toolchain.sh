#!/usr/bin/env bash
# The Dockerfile's Rust version must be rust-toolchain.toml's channel (#196):
# the image is the one build outside Nix, and it must not drift from the
# toolchain every other build uses.
set -euo pipefail
cd "$(dirname "$0")/.."
channel=$(sed -n 's/^channel = "\(.*\)"$/\1/p' rust-toolchain.toml)
image=$(sed -n 's/^ARG RUST_VERSION=\(.*\)$/\1/p' Dockerfile)
if [[ -z "$channel" || -z "$image" ]]; then
  echo "cannot read the channel ($channel) or the Dockerfile's RUST_VERSION ($image)" >&2
  exit 1
fi
if [[ "$channel" != "$image" ]]; then
  echo "Dockerfile builds with Rust $image, rust-toolchain.toml pins $channel" >&2
  exit 1
fi
echo "Dockerfile and rust-toolchain.toml agree on Rust $channel"
