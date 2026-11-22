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
  };

  outputs = { self, nixpkgs, systems, flake-utils, rust-overlay }:
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

        # LS4, the PTL prover that TLAPS calls for a temporal step such as
        # `Spec => []Inv`, and `ptl_to_trp`, the translator that tlapm pipes
        # into it (`ptl_to_trp -i $file | ls4`). nixpkgs's tlaps has neither.
        # Both are built from source as tlapm's own installer builds them:
        # LS4 1.0 with the `mkLit` patch of tlapm's `deps/ls4/ls4.patch`, and
        # the `translate` folder of tlapm. Sources come over git, never as a
        # GitHub tarball (#727 (flake inputs over git)).
        ls4 = pkgs.stdenv.mkDerivation {
          pname = "ls4";
          version = "1.0";
          src = pkgs.fetchgit {
            url = "https://github.com/quickbeam123/ls4";
            rev = "f7383bd5d3f1c1e9399a23a16e7f12f8153d41e5"; # tag v1.0
            hash = "sha256-yZuxc3cFVVMx1sWfEpWUe6TdO4zovK/5Q/CHtksKg6E=";
          };
          buildInputs = [ pkgs.zlib ];
          postPatch = ''
            substituteInPlace core/SolverTypes.h \
              --replace-fail "friend Lit mkLit(Var var, bool sign = false);" \
                             "friend Lit mkLit(Var var, bool sign);" \
              --replace-fail "inline  Lit  mkLit     (Var var, bool sign) {" \
                             "inline  Lit  mkLit     (Var var, bool sign = false) {"
          '';
          # The repository ships a prebuilt aiger.o; build it from aiger.c.
          buildPhase = ''
            runHook preBuild
            cd core
            rm -f aiger.o
            $CC -O2 -c aiger.c -o aiger.o
            make r
            runHook postBuild
          '';
          installPhase = ''
            runHook preInstall
            install -Dm755 ls4_release $out/bin/ls4
            runHook postInstall
          '';
        };

        ptl-to-trp = pkgs.stdenv.mkDerivation {
          pname = "ptl_to_trp";
          version = "2024-06-24";
          src = pkgs.fetchgit {
            url = "https://github.com/tlaplus/tlapm";
            rev = "c26e34795928f107bd208768e0b9df4d9659e53d"; # tag 2024_06_24_UTC_06_19
            sparseCheckout = [ "translate" ];
            hash = "sha256-QFKNGg0eCef812udbMUiAME9tmuVEn5fTmRFvKowZxY=";
          };
          nativeBuildInputs = [ pkgs.ocaml ];
          buildPhase = ''
            runHook preBuild
            cd translate
            ocamlopt -c fotypes.mli
            ocamllex folex.mll
            ocamlopt -c fofunctions.ml
            ocamlyacc foyacc.mly
            ocamlopt -c foyacc.mli
            ocamlopt -c folex.ml
            ocamlopt -c foyacc.ml
            ocamlopt -c main.ml
            ocamlopt -o ptl_to_trp fofunctions.cmx folex.cmx foyacc.cmx main.cmx
            runHook postBuild
          '';
          installPhase = ''
            runHook preInstall
            install -Dm755 ptl_to_trp $out/bin/ptl_to_trp
            runHook postInstall
          '';
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
            # Mutation testing of paros-consensus (#356 (core mutants)).
            cargo-mutants

            # The site (website/site/, Astro, #810) needs only node and npm.
            # The site's wasm tool (crates/support/paros-wasm, #540: the paxos
            # bundle for the Paxos pages).
            # wasm-bindgen-cli's version MUST equal the `wasm-bindgen` crate pin
            # in crates/support/paros-wasm/Cargo.toml (a mismatch fails bindgen
            # with an opaque schema-version error); bump both together.
            wasm-bindgen-cli
            binaryen
            nodejs_22

            # TLC, the TLA+ model checker, for the protocol specs in specs/tla/
            # (`scripts/check-tla.sh`, #579 (zero-stall reconfiguration)).
            tlaplus
          ];

          shellHook = ''
            echo "🏛️  Paros development environment loaded"
            echo "Rust version: $(rustc --version)"
            echo "Cargo version: $(cargo --version)"

            # Set environment variables
            export RUST_BACKTRACE=1
            export RUST_LOG=debug

            # Inform about available tools
            echo "Available tools:"
            echo "  • rustc, cargo, rustfmt, clippy, rust-analyzer"
            echo "  • cargo-nextest for better test management"
            echo "  • Use 'cargo build' to build the project"
            echo "  • Use 'cargo test' to run tests"
            echo "  • Use 'cargo nextest run' for better test output with timeouts"
            echo "  • Use 'cargo fmt' to format code"
            echo "  • wasm-bindgen $(wasm-bindgen --version | cut -d' ' -f2), node $(node --version): scripts/build-wasm.sh builds the wasm tool"
            echo "  • website/site: npm ci && npm run build builds the site (Astro)"
          '';

          # Environment variables
          RUST_SRC_PATH = "${rust-toolchain}/lib/rustlib/src/rust/library";
        };

        # The browser smoke test of the site (website/site/tests/, #1362
        # (browser smoke test and link check)): node and the Chromium of
        # nixpkgs's Playwright. `@playwright/test` in website/site/package.json
        # MUST have the version of `playwright-driver` here; bump both together.
        devShells.site = pkgs.mkShell {
          buildInputs = [ pkgs.nodejs_22 ];
          PLAYWRIGHT_BROWSERS_PATH = "${pkgs.playwright-driver.browsers.override { withFirefox = false; withWebkit = false; withFfmpeg = false; }}";
          PLAYWRIGHT_SKIP_VALIDATE_HOST_REQUIREMENTS = "true";
          PLAYWRIGHT_DRIVER_VERSION = pkgs.playwright-driver.version;
        };

        # TLC alone, without the Rust toolchain: what CI's `tla` workflow enters
        # (`nix develop .#tla --command scripts/check-tla.sh`). Python runs the
        # guard mutation of the specs (`scripts/mutate-tla.py`, #1311).
        devShells.tla = pkgs.mkShell {
          buildInputs = [ pkgs.tlaplus pkgs.python3 ];
        };

        # TLAPS, the TLA+ proof system, with Z3 (its default SMT backend) and
        # LS4 with ptl_to_trp (its PTL backend, built above), for
        # the proofs of specs/tla/ (`nix develop .#proofs --command
        # scripts/check-proofs.sh`, #1307 (inductive invariants, then proofs)).
        devShells.proofs = pkgs.mkShell {
          buildInputs = [ pkgs.tlaps pkgs.z3 ls4 ptl-to-trp ];
        };
      }
    );
}
