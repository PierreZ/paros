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
    # The Goyo theme (v0.7.1), pinned by rev, not a flake. web/site/build.sh copies
    # it into web/site/themes/goyo (decided on 2026-10-07: no git submodule).
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
            # Rust toolchain from oxalica
            rust-toolchain

            # Build tools
            gcc

            # Development tools
            cargo-nextest
            cargo-edit
            # Mutation testing of paros-core with the simulation as the test
            # (`cargo xtask mutants`, the weekly `mutants` workflow, #269).
            cargo-mutants
            protobuf

            # The site (web/site/, Zola + Goyo, #254): web/site/build.sh.
            zola

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
            # The pinned Goyo theme; web/site/build.sh copies it into web/site/themes/goyo.
            export PAROS_GOYO="${goyo}"
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
            echo "  • zola $(zola --version | cut -d' ' -f2): web/site/build.sh builds the site"
          '';

          # Environment variables
          RUST_SRC_PATH = "${rust-toolchain}/lib/rustlib/src/rust/library";
        };
      }
    );
}
