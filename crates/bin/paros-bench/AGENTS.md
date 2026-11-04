# paros-bench

The load generator (`publish = false`, #517 (`parosctl bench`), #194 (throughput benchmark),
#724 (bench roles), #725 (bench phases)), against a cell or a local one (`run --local N`). It
was `parosctl bench` until #722 (three products). It depends on `paros` and on `parosctl`'s
support. Call it "bench" or "load generator", never "workload".

## Map

- `src/main.rs` → the subcommands `init`, `run`, `clean`, `report`, then `bench::{init,run,clean}` or `bench::report`; `run --local N` starts a local cell per topology.
- `build.rs` → `PAROS_COMMIT` (from the environment, else `git rev-parse HEAD`), the commit `--json` reports.
- `src/bench.rs` → the driver: `Settings` (one phase, resolved), `Plan` (settings and seed; `run_one` per topology, `finish`), `prepare` (tenant, journals, claimed writers), `drive` (spawn the roles, the 1 s tick, the ramp, the warmup, Ctrl-C, `--max-ops`, the backlog), the roles `writer` (records stamped with their send time, through the journal's shared `paros::client::Pipeline`, #597 (several writes in flight per journal)), `tailer` (long-polled reads from the tail, `e2e` per record), `reader` (`--read-from`, a journal `Picker` draws), and `schedule` (set-leader and truncate). `Backlog` is the catch-up state machine: waiting, filling (tailers stopped), draining, done. Errors count by kind (`Failure::kind`, the outcome's variant). `bench/tests.rs` holds its unit tests and one local-cell test of `init`, `run` and `clean`.
- `bench/load.rs` → `WriteFlags`, `LoadFlags`, `Preset` (`append-only`, `pub-sub`, `catch-up`, `leader-churn`: defaults only, an explicit flag wins), `Load` (resolved), `ReadFrom`, `Distribution` and `Picker` (uniform or Zipfian, θ = 0.99).
- `bench/pace.rs` → `Pace`: calls/s or bytes/s, fixed or moving (`sin`, `square`, `pulse`, after `mako`), each paced client's share asked at each due time.
- `bench/expect.rs` → `--expect` limits (`e2e.p50<5ms`, `throughput>100MiB/s`, `errors<1`, `catch-up<10s`), checked against the report document.
- `bench/report.rs` → `paros-bench report`: merge `--json` reports (raw HDR histograms add without loss; refused across roles, preset or topology).
- `bench/summary.rs` → the report document: per call ops, bytes, errors by kind, rates, percentiles and the raw histogram (V2, base64), with the commit, the command line and the host.
- `bench/histograms.rs` → `Call` (`write`, `read`, `tail`, `e2e`, `set-leader`, `truncate`), per-client HDR histograms swapped and merged each tick, the `--histograms` JSON lines.
- `bench/records.rs` → `Rng` (SplitMix64 from `(seed, client)`), record `i` a pure function of `(seed, journal, i)` with a 24-byte header (seed, position, send time; the send time is not checked) and `check`.
- `bench/setup.rs` → the tenant (created tagged `paros-bench/created:true`), the journals `bench-RxC-K`, the claims, and `clean`.
- `bench/local.rs` → `--local N`: N shipped `run_machine`s on loopback in a thread of their own, stores under `--local-dir` (deleted after), formed by `paros::client::initialize`.
- `bench/topology.rs` → `Topology`, `Topologies` (`--topology 3x1,3x3`). `bench/units.rs` → durations, sizes, byte rates, times.

No sim path: it calls only `paros::client`.

## Tests & gates

- `cargo nextest run -p paros-bench` (the local-cell test takes about 15 s).
- CI's `test` job runs `cargo run -p paros-bench -- run --local 3 --duration 5s --preset pub-sub --readers-per-journal 1 --expect 'errors<1'`: a check that it works, never a number (shared runners are noise).
