{
  description = "Paros - Paxos in Rust";

  inputs = {
    # Every input is fetched over git, not as a GitHub tarball (`github:`): the cloud
    # session proxy refuses GitHub tarballs, so `github:` inputs cannot lock there.
    nixpkgs.url = "git+https://github.com/NixOS/nixpkgs?ref=nixos-unstable&shallow=1";
    systems.url = "git+https://github.com/nix-systems/default?shallow=1";
    flake-utils = {
      url = "git+https://github.com/numtide/flake-utils?shallow=1";
      inputs.systems.follows = "systems";
    };
    rust-overlay = {
      url = "git+https://github.com/oxalica/rust-overlay?shallow=1";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    # Zola for the site (#254). Goyo's templates need Zola 0.22: 0.23 changed the
    # template engine and refuses them. nixos-26.05 ships 0.22.1; unstable has 0.23.
    nixpkgs-zola.url = "git+https://github.com/NixOS/nixpkgs?ref=nixos-26.05&shallow=1";
    # The Goyo theme (v0.7.1), pinned by rev, not a flake. website/zola/build.sh copies
    # it into website/zola/themes/goyo (decided on 2026-10-07: no git submodule).
    goyo = {
      url = "git+https://github.com/hahwul/goyo?rev=25054f8f9f0b4eafa3d3380ec120b7e9cd7fd021&shallow=1";
      flake = false;
    };
  };

  outputs = { self, nixpkgs, systems, flake-utils, rust-overlay, nixpkgs-zola, goyo }:
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

        # The rust toolchain of rust-toolchain.toml as the rustc/cargo of nixpkgs' Rust builders.
        rustPlatform = pkgs.makeRustPlatform {
          cargo = rust-toolchain;
          rustc = rust-toolchain;
        };

        # The three binaries of the paros image (#196), built from the committed Cargo.lock.
        # The git dependencies (moonpool, biscuit-auth) are fetched by rev.
        paros = rustPlatform.buildRustPackage {
          pname = "paros";
          version = "0.0.0";
          src = pkgs.lib.cleanSourceWith {
            src = ./.;
            # Keep the site, build output and VCS data out of the build.
            filter = path: _type:
              !(builtins.elem (baseNameOf path) [ "target" "website" ".git" ]);
          };
          cargoLock = {
            lockFile = ./Cargo.lock;
            allowBuiltinFetchGit = true;
          };
          # prost-build (paros-wire) runs protoc.
          nativeBuildInputs = [ pkgs.protobuf pkgs.removeReferencesTo ];
          # The binaries embed the toolchain's store path (panic locations): drop that
          # reference so the image does not carry rustc, gcc and the docs.
          postFixup = ''
            remove-references-to -t ${rust-toolchain} $out/bin/*
          '';
          cargoBuildFlags = [ "-p" "parosd" "-p" "parosctl" "-p" "paros-bench" ];
          # CI tests separately.
          doCheck = false;
        };

      in
      {
        packages = {
          inherit paros;
          default = paros;

          # The same image as the Dockerfile, without a Docker Hub pull:
          # `nix build .#image && docker load < result` gives `paros:local`.
          image = pkgs.dockerTools.buildLayeredImage {
            name = "paros";
            tag = "local";
            # The binaries land in /bin, on the PATH of the image.
            contents = [ paros ];
            # A named volume mounted at /var/lib/paros takes this ownership on first use.
            fakeRootCommands = ''
              mkdir -p ./var/lib/paros
              chown 10001:10001 ./var/lib/paros
            '';
            config = {
              Entrypoint = [ "parosd" ];
              User = "10001:10001";
              Env = [ "PAROS_DATA_DIR=/var/lib/paros" ];
              ExposedPorts."4500/tcp" = { };
              Volumes."/var/lib/paros" = { };
            };
          };
        };

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

            # The site (website/zola/, Zola + Goyo, #254): website/zola/build.sh.
            zola

            # The site's wasm tools (crates/support/paros-wasm, #540: the game at
            # website/play, the playground at website/sandbox) and the paros-sim
            # wasm check (crates/sim/paros-sim-wasm-check, #307).
            # wasm-bindgen-cli's version MUST equal the `wasm-bindgen` crate pin
            # in crates/support/paros-wasm/Cargo.toml and crates/sim/paros-sim-wasm-check/Cargo.toml
            # (a mismatch fails bindgen with an opaque schema-version error);
            # bump all three together.
            wasm-bindgen-cli
            binaryen
            nodejs_22

            # Terminal recordings (`.cast` files) of demos.
            asciinema
          ];

          shellHook = ''
            echo "🏛️  Paros development environment loaded"
            echo "Rust version: $(rustc --version)"
            echo "Cargo version: $(cargo --version)"

            # Set environment variables
            export RUST_BACKTRACE=1
            # The pinned Goyo theme; website/zola/build.sh copies it into website/zola/themes/goyo.
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
            echo "  • wasm-bindgen $(wasm-bindgen --version | cut -d' ' -f2), node $(node --version): scripts/build-wasm.sh builds the wasm tools"
            echo "  • zola $(zola --version | cut -d' ' -f2): website/zola/build.sh builds the site"
          '';

          # Environment variables
          RUST_SRC_PATH = "${rust-toolchain}/lib/rustlib/src/rust/library";
          # ring's C code (rustls, under paros-sim's TLS) built for wasm32
          # (#307 (paros-sim in wasm)): an unwrapped clang targets wasm32, which the
          # gcc wrapper cannot.
          CC_wasm32_unknown_unknown = "${pkgs.llvmPackages.clang-unwrapped}/bin/clang";
          AR_wasm32_unknown_unknown = "${pkgs.llvmPackages.bintools-unwrapped}/bin/llvm-ar";
        };
      }
    );
}
