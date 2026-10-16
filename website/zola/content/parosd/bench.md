+++
title = "Bench"
description = "Measure throughput and latency per call with parosctl bench"
weight = 4
+++

`parosctl bench` is the load generator of `parosd`. It runs clients against a cell and prints
the throughput and the latency of each call, every second, then the totals. Use it to compare
two commits on the same machine. The baseline is on issue #194 (throughput benchmark).

## Run it

On one machine, with no cell to start first, `--local 3` starts a cell of three machines in
the `parosctl` process, on loopback:

```sh
parosctl bench --local 3 --duration 60s --record-size 1024 --records-per-call 16
```

Each machine is the shipped `parosd` code. Its stores go in a new directory under
`--local-dir` (the system's temporary directory by default), with real syncs. The bench deletes
the directory when the run ends. Choose `--local-dir` on the disk you want to measure: a
temporary directory is often in memory (`tmpfs`).

Against a running cell, such as the Compose cell (`DEMO.md` in the repository):

```sh
docker compose run --rm parosctl bench --tenant bench --journals 4 --clients 8 --duration 60s --ramp 10s
```

The bench creates the tenant and the journals `bench-0` to `bench-3` if they are missing. It
names them by their string names. Ctrl-C stops the run and still prints the totals.

## What it does

- Each client does one call at a time. `--mix` gives each call a weight, for example
  `--mix write=80,read-tail=15,set-leader=5`. The default is `write` only.
- `write` writes `--records-per-call` records of `--record-size` bytes at the tail of the
  client's own journal. Clients that share a journal share its one writer.
- `read-tail` reads the last records of a random journal and checks every byte.
- `read-start` reads the next page of a random journal from a cursor that starts at its first
  position, and checks every byte. At the tail, the cursor starts again at the first position.
- `set-leader` makes the writer of the client's journal take a new term under a new uuid.
- `--truncate-every 10s` truncates every journal on that schedule, and keeps its last
  `--truncate-keep` records.
- Without `--rate`, a client starts its next call when its last call ends (closed loop): this
  finds the peak throughput. With `--rate 500`, the clients together start 500 calls per second
  on a fixed schedule (open loop). The latency of a call counts from the moment it was due, so a
  slow call also counts against the calls that waited for it.
- `--ramp` starts the clients one by one over that time. The bench then clears what it
  recorded, and `--duration` starts.
- Without `--tolerate-errors`, the first error stops the run. With it, the bench logs each
  error and continues. Errors are counted apart from the latencies.

Every random choice comes from the seed and the client's index. The bench logs the seed, and
`--seed` sets it. Record `i` of a journal is a function of the seed, the journal and `i`, so a
reader can check the bytes of a record that another run wrote.

## Output

One line per second per call, then a total row per call, then the result row (`write`):

```text
 elapsed   errors    ops/s(inst)     ops/s(cum)  p50(ms)  p95(ms)  p99(ms)  max(ms)  call
    1.0s        0          178.0          177.7      5.4     11.9     17.4     53.6  write
    1.0s        0           94.0           93.8      2.6      8.1     12.7     12.7  read-tail

 elapsed   errors   ops(total)   ops/s(cum)     MiB/s  avg(ms)  p50(ms)  p90(ms)  p99(ms) p99.9(ms)  max(ms)  total
    5.0s        0         1022        203.8      0.05     15.2     14.8     21.0     33.3      60.4     60.4  write
    5.0s        0          434         86.5      0.02     10.2      9.8     14.1     20.8      43.9     43.9  read-tail
```

`parosctl --json bench` prints no line per second. It prints one JSON report when the run
ends: for each call, the calls and the bytes, per second too, the errors and the latency (mean,
p50, p90, p99, p99.9 and max, in milliseconds). The report also records what the numbers depend
on: the commit, the command line, the CPU count, the kernel, and the filesystem of a local
cell's stores.

`--histograms FILE` also writes the raw histograms: one JSON line per second per call, with
`name`, `seed`, `unix_ms`, `elapsed_ms`, `errors`, `bytes` and `histogram` (HdrHistogram's V2 encoding
in base64, nanoseconds, three significant figures). The bench writes the file under a
temporary name and renames it when the run ends. A later tool can merge runs from these
files and compute any percentile. The bench itself never decides pass or fail.

## Exit codes

`0` when the run ends, by `--duration` or Ctrl-C. When an error stops the run, the code is
the one of that call: `3` refused, `4` ambiguous, `5` no server answered. `3` also when a
read found a wrong byte. `5` when a local cell did not form.
