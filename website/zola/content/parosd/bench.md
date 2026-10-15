+++
title = "Bench"
description = "Measure throughput and latency per call with parosctl bench"
weight = 4
+++

`parosctl bench` is the load generator of `parosd`. It runs clients against a cell and prints
the throughput and the latency of each call, every second, then the totals. Use it to compare
two commits on the same machine. A number from a laptop is not a baseline.

## Run it

Start the Compose cell (`DEMO.md` in the repository), then:

```sh
docker compose run --rm parosctl bench --tenant bench --journals 4 --clients 8 --duration 60s --ramp 10s
```

The bench creates the tenant and the journals `bench-0` to `bench-3` if they are missing. It
names them by their string names. Ctrl-C stops the run and still prints the totals.

## What it does

- Each client does one call at a time (a closed loop). A call is `write` or `read-tail`.
  `--read-percent` sets the share of `read-tail`.
- `write` writes `--records-per-call` records of `--record-size` bytes at the tail of the
  client's own journal. Clients that share a journal share its one writer.
- `read-tail` reads the last records of a random journal and checks every byte.
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

 elapsed   errors   ops(total)   ops/s(cum)  avg(ms)  p50(ms)  p95(ms)  p99(ms)  max(ms)  total
    5.0s        0         1022        203.8     15.2     14.8     25.3     33.3     60.4  write
    5.0s        0          434         86.5     10.2      9.8     16.7     20.8     43.9  read-tail
```

`--histograms FILE` also writes the raw histograms: one JSON line per second per call, with
`name`, `seed`, `unix_ms`, `elapsed_ms`, `errors` and `histogram` (HdrHistogram's V2 encoding
in base64, nanoseconds, three significant figures). The bench writes the file under a
temporary name and renames it when the run ends. A later tool can merge runs from these
files and compute any percentile. The bench itself never decides pass or fail.

## Exit codes

`0` when the run ends, by `--duration` or Ctrl-C. When an error stops the run, the code is
the one of that call: `3` refused, `4` ambiguous, `5` no server answered. `3` also when
`read-tail` found a wrong byte.
