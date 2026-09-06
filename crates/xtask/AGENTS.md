# xtask

`cargo xtask sim {list|run <filter> [-- args]|run-all}` (alias in
`.cargo/config.toml`). It builds a registered simulation binary with sancov
instrumentation so moonpool's `until_coverage_stable` is guided by real edge
coverage instead of the assertion fallback: `cargo run --bin <name>
--target-dir target/sancov` with `SANCOV_CRATES` set, and
`scripts/sancov-rustc.sh` as the `RUSTC_WRAPPER` (the flake's `shellHook`
exports it).

- `SIM_BINARIES` in `src/main.rs` is the registry; today it holds one entry,
  `sim-paros-chain` with `sancov_crates: "paros_core,paros"`, so `run
  paros-chain` and `run-all` are the same sweep. A second campaign is a
  second entry.
- Crate names in `SANCOV_CRATES` use underscores; xtask normalizes to hyphens
  for `cargo clean`.
- `SANCOV_CRATES` is not part of cargo's fingerprint, so xtask stamps the
  active whitelist at `target/sancov/.sancov-crates` and cleans the symmetric
  difference when it changes; do not remove that or a stale instrumented
  build silently guides the sweep.
- The wrapper passes through when `SANCOV_CRATES` is unset and never
  instruments build scripts or proc-macros.
