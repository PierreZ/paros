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
| `paros_calls_total` | counter | `call`, journal | the data-plane calls (`write`, `read`, `set_leader`, `truncate`) that reach a journal of the machine |
| `paros_call_answers_total` | counter | `call`, `outcome`, journal | the answers to those calls, by outcome |
| `paros_call_answer_seconds_total` | counter | `call`, journal | the time from each answered call to its answer, added up |
| `paros_written_bytes_total` | counter | journal | the record bytes of each accepted `Write` |
| `paros_read_bytes_total` | counter | journal | the record bytes of each `Read` page |
| `paros_applied_slot`, `paros_chosen_slot` | gauge | journal | the last applied and chosen slot: the difference is the apply lag |
| `paros_settles_total`, `paros_settle_seconds_total` | counter | | the batches persisted (with the sync), sent and applied, and their time added up |
| `paros_machine_cpu_cores`, `paros_machine_cpu_ratio`, `paros_machine_disk_busy_ratio`, `paros_machine_disk_queue_depth` | gauge | | the last busyness window (the `Load` call) |
| `paros_machine_load_windows_total` | counter | | the busyness windows: when it stops, the numbers are old |
| `paros_machine_phase` | gauge | | `0` starting, `1` idle, `2` serving |

The `outcome` label is one of `ok`, `duplicate`, `refused`, `wrong_mode`, `truncated`,
`too_large`, `redirected`, `unknown_journal` and `unserved`. A refusal or a call the machine did
not serve is counted, not only a success. A call that has no answer yet is
`paros_calls_total` minus `paros_call_answers_total`.

There is no histogram. To get the mean time of an answer over a window, divide the two rates:

```
rate(paros_call_answer_seconds_total[5m]) / rate(paros_call_answers_total[5m])
```

Use the traces to find one slow call.

"journal" is four labels. `tenant` and `journal` are the names. `tenant_id` and `journal_id`
are the ids in hex. The names are the names `parosctl` prints. The cell's own journals are
`cell/(control)`, `cell/(election)` and `universe/(directory)`. A tenant's control journal is
`<tenant>/(control)`. A journal whose names the machine does not know yet has its ids in hex in
place of the names.

The counters start at zero when the process starts.

## OpenTelemetry

The machine can also push its spans and its metrics to an OpenTelemetry collector, over
OTLP/gRPC. Prometheus `/metrics` stays on.

| Variable | Meaning |
|---|---|
| `PAROS_OTLP_ENDPOINT` | the collector, for example `http://collector:4317`: spans and metrics go there |
| `PAROS_OTLP_TRACES_ENDPOINT` | the collector for the spans only, in place of `PAROS_OTLP_ENDPOINT` |
| `PAROS_OTLP_METRICS_ENDPOINT` | the collector for the metrics only, in place of `PAROS_OTLP_ENDPOINT` |
| `PAROS_OTLP_INTERVAL_MS` | how often the metrics are pushed (default `10000`, at least `100`) |

The metrics are the same as on `/metrics`, with the same labels. A counter loses its `_total`
in its OpenTelemetry name: `paros_calls_total` is `paros_calls`. The spans are the ones that
`RUST_LOG` lets through. The resource names the service `parosd` and the machine by its
`PAROS_NAME` (else its address).

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
