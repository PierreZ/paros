{
  description = "Paros - Paxos in Rust";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    # Zola for the site (#254). Goyo's templates need Zola 0.22: 0.23 changed the
    # template engine and refuses them. nixos-26.05 ships 0.22.1; unstable has 0.23.
    # Fetched over git so the session proxy (which refuses GitHub tarballs) can lock it.
    nixpkgs-zola.url = "git+https://github.com/NixOS/nixpkgs?ref=nixos-26.05&shallow=1";
    # The Goyo theme (v0.7.1), pinned by rev, not a flake. website/zola/build.sh copies
    # it into website/zola/themes/goyo (decided on 2026-10-07: no git submodule).
    goyo = {
      url = "git+https://github.com/hahwul/goyo?rev=25054f8f9f0b4eafa3d3380ec120b7e9cd7fd021&shallow=1";
      flake = false;
    };
  };

  outputs = { self, nixpkgs, flake-utils, rust-overlay, nixpkgs-zola, goyo }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        overlays = [ (import rust-overlay) ];
        pkgs = import nixpkgs {
          inherit system overlays;
        };
        zola = nixpkgs-zola.legacyPackages.${system}.zola;

        # Read rust toolchain version from rust-toolchain.toml
        toolchainFile = builtins.fromTOML (builtins.readFile ./rust-toolchain.toml);
        rustVersion = toolchainFile.toolchain.channel;
        rustComponents = toolchainFile.toolchain.components or [];
        rustTargets = toolchainFile.toolchain.targets or [];

        # Create rust toolchain with specified version, components, and targets
        rust-toolchain = pkgs.rust-bin.stable.${rustVersion}.default.override {
          extensions = rustComponents;
          targets = rustTargets;
        };

      in
      {
        devShells.default = pkgs.mkShell {
          buildInputs = with pkgs; [
 in crates/paros-play/Cargo.toml (a mismatch fails bindgen with an# Rust toolchain from oxalica
 in crates/paros-play/Cargo.toml (a mismatch fails bindgen with anrust-toolchain

 in crates/paros-play/Cargo.toml (a mismatch fails bindgen with an# Build tools
 in crates/paros-play/Cargo.toml (a mismatch fails bindgen with angcc

 in crates/paros-play/Cargo.toml (a mismatch fails bindgen with an# Development tools
 in crates/paros-play/Cargo.toml (a mismatch fails bindgen with ancargo-nextest
 in crates/paros-play/Cargo.toml (a mismatch fails bindgen with ancargo-edit
 in crates/paros-play/Cargo.toml (a mismatch fails bindgen with an# Mutation testing of paros-core with the simulation as the test
 in crates/paros-play/Cargo.toml (a mismatch fails bindgen with an# (`cargo xtask mutants`, the weekly `mutants` workflow, #269).
 in crates/paros-play/Cargo.toml (a mismatch fails bindgen with ancargo-mutants
 in crates/paros-play/Cargo.toml (a mismatch fails bindgen with anprotobuf

 in crates/paros-play/Cargo.toml (a mismatch fails bindgen with an# The site (website/zola/, Zola + Goyo, #254): website/zola/build.sh.
 in crates/paros-play/Cargo.toml (a mismatch fails bindgen with anzola

 in crates/paros-play/Cargo.toml (a mismatch fails bindgen with an# paros play: the interactive game (crates/paros-play + website/play).
 in crates/paros-play/Cargo.toml (a mismatch fails bindgen with an# wasm-bindgen-cli's version MUST equal the `wasm-bindgen` crate pin
 in crates/paros-play/Cargo.toml (a mismatch fails bindgen with an# in crates/paros-play/Cargo.toml (a mismatch fails bindgen with an
 in crates/paros-play/Cargo.toml (a mismatch fails bindgen with an# opaque schema-version error); bump both together.
 in crates/paros-play/Cargo.toml (a mismatch fails bindgen with anwasm-bindgen-cli
 in crates/paros-play/Cargo.toml (a mismatch fails bindgen with anbinaryen
 in crates/paros-play/Cargo.toml (a mismatch fails bindgen with annodejs_22
          ];

          shellHook = ''
 in crates/paros-play/Cargo.toml (a mismatch fails bindgen with anecho "🏛️  Paros development environment loaded"
 in crates/paros-play/Cargo.toml (a mismatch fails bindgen with anecho "Rust version: $(rustc --version)"
 in crates/paros-play/Cargo.toml (a mismatch fails bindgen with anecho "Cargo version: $(cargo --version)"

 in crates/paros-play/Cargo.toml (a mismatch fails bindgen with an# Set environment variables
 in crates/paros-play/Cargo.toml (a mismatch fails bindgen with anexport RUST_BACKTRACE=1
 in crates/paros-play/Cargo.toml (a mismatch fails bindgen with an# The pinned Goyo theme; website/zola/build.sh copies it into website/zola/themes/goyo.
 in crates/paros-play/Cargo.toml (a mismatch fails bindgen with anexport PAROS_GOYO="${goyo}"
 in crates/paros-play/Cargo.toml (a mismatch fails bindgen with anexport RUST_LOG=debug
 in crates/paros-play/Cargo.toml (a mismatch fails bindgen with an# RUSTC_WRAPPER for selective LLVM SanitizerCoverage instrumentation,
 in crates/paros-play/Cargo.toml (a mismatch fails bindgen with an# gated by SANCOV_CRATES (see scripts/sancov-rustc.sh). No-op unless
 in crates/paros-play/Cargo.toml (a mismatch fails bindgen with an# SANCOV_CRATES is set (e.g. by `cargo xtask sim run`).
 in crates/paros-play/Cargo.toml (a mismatch fails bindgen with anexport RUSTC_WRAPPER="$PWD/scripts/sancov-rustc.sh"

 in crates/paros-play/Cargo.toml (a mismatch fails bindgen with an# Inform about available tools
 in crates/paros-play/Cargo.toml (a mismatch fails bindgen with anecho "Available tools:"
 in crates/paros-play/Cargo.toml (a mismatch fails bindgen with anecho "  • rustc, cargo, rustfmt, clippy, rust-analyzer"
 in crates/paros-play/Cargo.toml (a mismatch fails bindgen with anecho "  • cargo-nextest for better test management"
 in crates/paros-play/Cargo.toml (a mismatch fails bindgen with anecho "  • Use 'cargo build' to build the project"
 in crates/paros-play/Cargo.toml (a mismatch fails bindgen with anecho "  • Use 'cargo test' to run tests"
 in crates/paros-play/Cargo.toml (a mismatch fails bindgen with anecho "  • Use 'cargo nextest run' for better test output with timeouts"
 in crates/paros-play/Cargo.toml (a mismatch fails bindgen with anecho "  • Use 'cargo fmt' to format code"
 in crates/paros-play/Cargo.toml (a mismatch fails bindgen with anecho "  • wasm-bindgen $(wasm-bindgen --version | cut -d' ' -f2), node $(node --version): scripts/build-play.sh builds the game"
 in crates/paros-play/Cargo.toml (a mismatch fails bindgen with anecho "  • zola $(zola --version | cut -d' ' -f2): website/zola/build.sh builds the site"
          '';

          # Environment variables
          RUST_SRC_PATH = "${rust-toolchain}/lib/rustlib/src/rust/library";
        };
      }
    );
}
