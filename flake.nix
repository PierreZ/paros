{
  description = "Paros - Paxos in Rust";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { self, nixpkgs, flake-utils, rust-overlay }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        overlays = [ (import rust-overlay) ];
        pkgs = import nixpkgs {
          inherit system overlays;
        };

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
            # Rust toolchain from oxalica
            rust-toolchain

            # Build tools
            gcc

            # Development tools
            cargo-nextest
            cargo-edit
            protobuf

            # mdBook: the GitHub Pages book.
            mdbook
            mdbook-toc
            mdbook-mermaid

            # paros play: the interactive game (crates/paros-play + web/play).
            # wasm-bindgen-cli's version MUST equal the `wasm-bindgen` crate pin
            # in crates/paros-play/Cargo.toml (a mismatch fails bindgen with an
            # opaque schema-version error); bump both together.
            wasm-bindgen-cli
            binaryen
            nodejs_22
          ];

          shellHook = ''
            echo "🏛️  Paros development environment loaded"
            echo "Rust version: $(rustc --version)"
            echo "Cargo version: $(cargo --version)"

            # Set environment variables
            export RUST_BACKTRACE=1
            export RUST_LOG=debug
            # RUSTC_WRAPPER for selective LLVM SanitizerCoverage instrumentation,
            # gated by SANCOV_CRATES (see scripts/sancov-rustc.sh). No-op unless
            # SANCOV_CRATES is set (e.g. by `cargo xtask sim run`).
            export RUSTC_WRAPPER="$PWD/scripts/sancov-rustc.sh"

            # Inform about available tools
            echo "Available tools:"
            echo "  • rustc, cargo, rustfmt, clippy, rust-analyzer"
            echo "  • cargo-nextest for better test management"
            echo "  • Use 'cargo build' to build the project"
            echo "  • Use 'cargo test' to run tests"
            echo "  • Use 'cargo nextest run' for better test output with timeouts"
            echo "  • Use 'cargo fmt' to format code"
            echo "  • wasm-bindgen $(wasm-bindgen --version | cut -d' ' -f2), node $(node --version): scripts/build-play.sh builds the game"
          '';

          # Environment variables
          RUST_SRC_PATH = "${rust-toolchain}/lib/rustlib/src/rust/library";
        };
      }
    );
}
