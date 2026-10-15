+++
title = "Metrics and probes"
description = "Read the metrics, the probes and the usage per journal of a parosd machine"
weight = 6
+++

Each `parosd` machine counts the calls that it serves. It shows the counts in the Prometheus
text format, and it answers two probes for an orchestrator. The counts are for an operator.
The machine never reads them to make a decision.

## The endpoint

Set `PAROS_HTTP_LISTEN` to an address, for example `0.0.0.0:9100`. The machine then answers
three paths there:

| Path | Answer |
|---|---|
| `/metrics` | `200` and every metric of the machine, in the Prometheus text format |
| `/healthz` | `200` while the process answers |
| `/readyz` | `200` when the machine serves its cell. Before that, `503` and its phase: `starting` or `idle` |

When `PAROS_HTTP_LISTEN` is not set, the machine does not listen for HTTP. It still counts.

## The metrics

| Metric | Kind | Labels | What it counts |
|---|---|---|---|
| `paros_calls_total` | counter | `call`, `tenant`, `journal` | the data-plane calls (`write`, `read`, `set_leader`, `truncate`) that reach a journal of the machine |
| `paros_call_seconds` | histogram | `call` | the time from a call to its answer |
| `paros_written_bytes_total` | counter | `tenant`, `journal` | the record bytes of each accepted `Write` |
| `paros_read_bytes_total` | counter | `tenant`, `journal` | the record bytes of each `Read` page |
| `paros_applied_slot`, `paros_chosen_slot` | gauge | `tenant`, `journal` | the last applied and chosen slot: the difference is the apply lag |
| `paros_settle_seconds` | histogram | | the time to persist (with the sync), send and apply one batch |
| `paros_machine_cpu_cores`, `paros_machine_cpu_ratio`, `paros_machine_disk_busy_ratio`, `paros_machine_disk_queue_depth` | gauge | | the last busyness window (the `Load` call) |
| `paros_machine_phase` | gauge | | `0` starting, `1` idle, `2` serving |

The labels name the tenant and the journal by their names. The cell's own journals are
`cell/registry`, `cell/election` and `universe/directory`. A tenant's control journal is
`<tenant>/control`. A journal whose names the machine does not know yet has its ids in hex.

The counters start at zero when the process starts.

## Usage per journal

`parosctl status --tenant <name>` adds a usage table: for each journal of the tenant, the
`write`, `read`, `set-leader` and `truncate` calls, and the bytes written and read. The command
asks every machine of the cell with the `Load` call and adds the counts. A machine that does not
answer is not in the sum, and the table says how many machines answered. `--json` gives the same
numbers in a `usage` block.

## In the simulation

The simulation runs the same code. On some seeds, every machine runs with its metrics off, and
the same checks judge the run: the metrics never change what a client sees. On the other seeds,
the simulation checks that the counters of every machine equal its own count of the calls.
