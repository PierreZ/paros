# paros-bench

The load generator (`publish = false`, #517 (`parosctl bench`), #194 (throughput benchmark)),
against a cell or a local one (`--local N`). It was `parosctl bench` until #722 (three products)
and keeps the same command line. It depends on `paros` and on `parosctl`'s support.

## Map

- `src/main.rs` → the command line, then `bench::run`.
- `build.rs` → `PAROS_COMMIT` (from the environment, else `git rev-parse HEAD`), the commit `paros-bench --json` reports (#194 (throughput benchmark)).
- `src/bench.rs` → the load generator (#517 (`parosctl bench`), #194 (throughput benchmark)), the load generator: clients over `paros::client`, closed loop or open loop (`--rate`, each latency counted from the moment the call was due), `write` (client `i` to journal `i % N`, one shared `paros::client::Pipeline` per journal, so its clients' writes are in flight at once, #597 (several writes in flight per journal); the report names the writes in flight per journal and the writes sent again), `read-tail` and `read-start` (every byte checked) and `set-leader` drawn by `--mix` weights (`bench/mix.rs`), `truncate` on `--truncate-every`; the 1 s tick, the ramp, Ctrl-C, `--tolerate-errors`; `--json` prints one report (`bench/summary.rs`: per call ops, bytes, errors, mean, p50, p90, p99, p99.9 and max, with the commit, the command line, the CPU count, the kernel and the stores' filesystem). `bench/local.rs` → `--local N`: N shipped `run_machine`s on loopback in a thread and multi-thread runtime of their own, stores in a new directory under `--local-dir` (deleted after), formed by `paros::client::initialize`; CI's `test` job runs `bench --local 3 --duration 5s`; `LocalCell::servers` waits until every machine answers its `Inspect`. `bench/topology.rs` → `Topology`, `Topologies` → `--topology 3x1,3x3` (`ROWSxCOLUMNS`, a `Durability` of `F = ROWS/2` over `COLUMNS` partitions): each runs in turn under one seed (`Plan`, `run_one`, `finish`), a new local cell per topology, then one total row per topology and call. `bench/setup.rs` creates the tenant and the journals `bench-RxC-K` when missing (`--store-durability`, #599) and claims them; `bench/histograms.rs` → per-client HDR histograms swapped and merged each tick, the `--histograms` JSON lines (V2, base64; temp name, fsync, rename) and `read`, their reading half; `bench/records.rs` → `Rng` (SplitMix64 from `(seed, client)`), record `i` a pure function of `(seed, journal, i)` and `check`. No sim path: it calls only `paros::client`.
- `src/bench/{histograms,local,mix,records,setup,summary,topology}.rs` → the HDR histograms,
  the local cell, the call mix, the record bytes, the tenant and its journals, the report, and
  the `--topology` sets.

## Tests & gates

- `cargo nextest run -p paros-bench`.
- CI's `test` job runs `cargo run -p paros-bench -- --local 3 --duration 5s`: a check that it
  works, never a number (shared runners are noise).
