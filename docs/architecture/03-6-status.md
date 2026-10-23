[← 03-5-frontend-and-resolver.md](03-5-frontend-and-resolver.md) · [Index](README.md) · [03-7-universe.md →](03-7-universe.md)

### 3.6 Status

`parosctl status [--tenant t] [--cell c]` shows three columns, per tenant, per cell and for the
universe: desired (what the tenant asked for), available (machines registered, not drained, seen
alive) and current (what is placed and serving, with each journal's word: Healthy, Degraded,
Unavailable). The tenant view folds the tenant's control journal, the cell view the cell control
journal, the universe view the universe directory with each cell entry's state. Status is computed live
from those folds and `Inspect`, never written back: there is no separate monitoring store.

It starts small, like `fdbcli status`, and grows by views, each one an admin RPC scoped by the
caller's authorization: **role slots** per machine (class, total, booked, and what holds each
slot), per tenant (its slots and footprint), per cell (free and booked per class) and for the
universe (decided on 2026-10-04).

**The words** (#191 (status), landed 2026-10-11; `paros::status`). Each journal of a cell or
tenant view carries its desired acceptor count, its placed configuration, the word the answer
judged (`observed`) and the word the cell shows (`word`):

- **Healthy**: the placed shape (acceptor count and quorum system) is the desired shape, and
  every member is up and not draining.
- **Degraded**: the members that are up hold a Phase-1 and a Phase-2 quorum, but the shape
  differs (a member down, draining or retired, or another count).
- **Unavailable**: the members that are up hold no Phase-1 or no Phase-2 quorum.

Every quorum question goes through the placed `AcceptorConfig`. A journal the cell made
(control, election, universe directory) is desired on every founding member, a majority. The
words compare shapes only: a `failures-per-partition=0` journal is Healthy when its one member
is up, and the zone rule (the `failures-per-partition=0` exemption, the degraded trap) comes with
#215. The desired column shows the durability's label, partition count included (#523). **Hysteresis**: the
founding member that answers shows a new word only once it held for `status_hold` (a
`DriverTunables` field; floor 0, which shows every answer's own word; production 20 s, two
`machine_down_after`). The member keeps this in memory and never writes it. When the universe
directory cannot be read, the member still answers from the registry (static stability).
`parosctl status [--tenant t] [--cell c]` prints the universe line (when the servers' cell hosts
its directory), the cell, its coordinator, the machines available, the slots per class and every
journal's desired, current and available acceptors with its word.

**The first views** (#399, decided on 2026-10-10). Each view is a request to **one cell**, and the
request chooses the cell: `parosctl` asks the servers it names, and a founding member of that
cell answers. The member reads the cell's own journals: the registry, the universe directory
when the cell hosts it, the election journal and the tenant control journals. A view changes
nothing. The RPC is `View` (`paros.view.View`, method `0x5041_0309`), with three queries:

- **Cell**: every machine (name, address, class, capacity, bookings, standing, up or down,
  founding member), every hosted tenant with its journals and their acceptors, and the cell
  coordinator. Admin only.
- **Tenant**, by name: the tenant's journals and the machines they use. A tenant hosted by
  another cell is refused `other_cell` with the name of that cell.
- **Universe**: the cells and the tenants of the universe directory. Admin only. Only the cell
  that hosts the universe tenant answers it.

**The server filters by scope; the caller never filters.** An `admin` sees every detail. A
`tenant` caller sees only its own tenant, and of each machine that the tenant uses only its name,
its `paros/zone` and `paros/rack` tags and whether it is up. Any other query from a `tenant` caller is refused
`forbidden`. The frontend checks tokens on the four data-plane calls since #192 (the frontend),
but a `View` does not go through it yet: the caller states its scope and
`paros::view::authorize` takes it as given. That function is the seam where the Biscuit roles of
section 3.5 (`admin`, `tenant`, `view.detail`) will decide the scope.

`parosctl` shows the views as tables, and `--json` gives one document for scripts:
`machine list|show`, `cell list|show`, `tenant list|show` and `roles` (who holds which role: the
cell coordinator, the acceptors and matchmakers of each journal, and the capacity bookings).
`--as-tenant NAME` asks in the scope of one tenant. `parosctl status` (above) stays the target
that these views grow into.

**Busyness** (#424, decided on 2026-10-10; the plan is `design/busyness-metrics.md` of the
project). `parosctl machine list` shows how busy each machine is, as FDB's status does: `CPU`
(`cpu.usage_cores`), `MACHINE` (`machine.cpu.logical_core_utilization`) and `DISK`
(`disk.busy`, the share of the window with at least one I/O in flight). `machine show` adds a
`load` block with the run loop, the queue depth, the reads and the writes per second. `--json`
uses FDB's key names.

- **Measure.** moonpool's sixth provider, `SystemProvider`, gives cumulative counters: the
  process CPU time (`getrusage`), the machine CPU (`/proc/stat`), the run loop's busy time
  (the CPU time of `parosd`'s one compio thread) and the data disk (`/proc/diskstats`, found from the
  data directory). `paros::load::Busyness::between` turns two samples into one window. It is
  pure arithmetic; a counter that goes back gives no window.
- **Sample.** Every machine samples itself every `load_interval` (default 5 s, floor 1 s,
  `PAROS_LOAD_INTERVAL_MS`) and keeps only its last full window.
- **Pull.** Every machine answers `Load` (`paros.machine.Load`, method `0x5041_030B`) in every
  phase, from that window. Nothing is pushed and nothing is written into a journal.
  `paros::client::load::ask_all` asks every machine of a cell view at once. A machine that does
  not answer in time shows `no metrics`, the fdbcli wording; a field with no value shows `-`.
  `Load` is for the admin scope. It does not go through the frontend yet, so no token is checked.
- **Simulation.** The same code runs there. moonpool's simulated disk counts its own activity
  per process, and the operator's `LOAD` operation checks that every answer is exactly
  `Busyness::between` of two samples the simulator handed to that machine.
  On about half of the seeds, moonpool's CPU model gives each process one core: every poll,
  every byte sent and every byte written costs time on it, so a busy machine answers late and
  its CPU counters are real (#424 (busyness metrics), 2026-10-11). Gray failures keep one
  acceptor and one machine alive and slow (CPU, disk or network, by the seed's mask) for a long
  part of the chaos window. Before a cell forms, `LOAD` asks the machines the operator was
  given, so the chaos window sees answers too. A cell forms only after the chaos window on most
  seeds, so a slow-machine scenario slows a founding member of the formed cell for a few
  seconds, at most `LATE_WINDOW` into the tail. The gates show a disk and a CPU over 90 % busy,
  and a slow machine that reports more busyness than every healthy machine of its cell. paros adds no `cpu_charge` yet: the byte
  costs already charge sends and writes, and a charge goes only where a profile shows real work.

**Metrics, probes and usage** (#366 (metrics, health and usage), landed 2026-10-15;
`paros::metrics`). Every machine counts the data-plane calls it serves in a plain `prometheus`
registry, provider-free and wasm-safe. The counters are for an operator, never for a decision:
the code that serves never reads them.

- **The rules** (decided on 2026-10-15, after
  [the practical guide to application metrics](https://pierrezemb.fr/posts/practical-guide-to-application-metrics/)).
  Counters and gauges only: no histogram. A time is a seconds counter beside its count, so
  `rate(seconds) / rate(count)` is the mean over any window, and one slow call is found in the
  traces. Every failure path is counted beside the successes. Each unit is in the name. A label
  takes only a bounded value: a call, an outcome, a journal the machine runs; never a request,
  a client or an address. A heartbeat shows that the numbers are fresh. The metrics are
  recorded once, and every export reads that one registry.
- **What it counts.** `paros_calls_total` per call, counted when the call reaches a journal
  the machine runs; `paros_call_answers_total` per call and `outcome` (`ok`, `duplicate`,
  `refused`, `wrong_mode`, `truncated`, `too_large`, `redirected`, `unknown_journal`,
  `unserved`), counted when the answer leaves, and `paros_call_answer_seconds_total`, the time
  of those answers; a call received and not answered is the difference.
  `paros_written_bytes_total` (accepted `Write`s) and `paros_read_bytes_total` (`Read` pages);
  `paros_applied_slot` and `paros_chosen_slot`, whose difference is the apply lag;
  `paros_settles_total` and `paros_settle_seconds_total`, a batch persisted (sync included),
  sent and applied; the last busyness window as `paros_machine_*` gauges, with
  `paros_machine_load_windows_total` as the heartbeat; and `paros_machine_phase` (starting,
  idle, serving). A journal's series carry four labels: `tenant` and `journal`, the names, as
  every output gives them (§3.5): the cell's own journals are `cell/(control)`,
  `cell/(election)` and `universe/(directory)`, and a tenant's control journal is
  `<tenant>/(control)`. `tenant_id` and `journal_id` are the ids in hex. A journal whose names
  the machine does not know is named by its ids in hex.
- **Where it shows.** `parosd` serves the registry as Prometheus text on `/metrics` when
  `PAROS_HTTP_LISTEN` is set, with two probes: `/healthz` (`200` while the process answers) and
  `/readyz` (`200` once the machine serves its cell, `503` with its phase before). The HTTP
  server is plain, holds no state and is not in the simulation.
- **OpenTelemetry** (decided on 2026-10-15: OTLP beside Prometheus, never in place of it).
  With `PAROS_OTLP_ENDPOINT` (or one signal's `PAROS_OTLP_TRACES_ENDPOINT`,
  `PAROS_OTLP_METRICS_ENDPOINT`), `parosd` pushes over OTLP/gRPC. The spans are the `tracing`
  spans, through `tracing-opentelemetry`. The metrics are the same registry: one observable
  instrument per family reads it at each push (`PAROS_OTLP_INTERVAL_MS`), and a counter's
  OpenTelemetry name drops its `_total`. Only `parosd` links OpenTelemetry: the library and the
  simulation do not.
- **Usage.** `Load` (above) also carries each journal's counts and bytes since the machine's
  process started (`LoadAck.usage`), and `parosctl status --tenant t` sums them over the
  machines of the cell into a usage table per journal. A machine that does not answer is left
  out, and the table says how many machines answered.
- **Simulation.** The same code runs there, and the harness keeps each machine's metrics for
  its address, so a reboot finds its counters again. On some seeds every
  machine runs with metrics off, and the same oracles judge the run: metrics never change an
  outcome. The audit hears each counted call through `Audit::call_received` and
  `Audit::call_answered`, in the same step as the counter, and the workload's `check()`
  asserts that the counters (calls and answers per call, bytes) equal the audit's own fold of
  the calls.
- **Not yet.** A trace context carried in the RPC envelope (a span stops at its machine), an audit log that names each
  caller, and a metric for the store's sync time alone stay open in follow-up issues.

**The benchmark** (#194 (throughput benchmark), landed 2026-10-15). `parosctl bench` runs
clients over `paros::client` against the servers (`--servers`) or against a cell of N machines
it starts in its own process (`--local N`): the shipped `run_machine` on loopback, with its
stores on a real filesystem with real syncs, formed by `paros::client::initialize`. The calls are
`write`, `read-tail`, `read-start` and `set-leader`, drawn by `--mix` weights, and `truncate` on
a schedule. The clients run closed loop, or open loop at `--rate` calls per second, where a
call's latency counts from the moment it was due (no coordinated omission). `--json` prints one
report: throughput (calls and bytes per second) and latency (mean, p50, p90, p99, p99.9, max)
per call, with the commit, the command line, the CPU count, the kernel and the filesystem of a
local cell's stores. CI only checks that `parosctl bench --local 3 --duration 5s` runs: numbers
on shared runners are noise. The baseline is posted on #194. `--topology 3x1,3x3` compares
journal shapes (decided on 2026-10-16, asked to see whether a grid behaves better): each
`ROWSxCOLUMNS` runs in turn under one seed on journals of its own, on a new local cell for
each topology, then one total row per topology and call. The clients of one journal share
its writer through `paros::client::Pipeline` (#597 (several writes in flight per journal)):
`--clients N` on one journal keeps N writes in flight on it, so the bench measures what a cell
can do, not one write at a time. The report names the most writes in flight per journal and
counts the writes that arrived early and were sent again.
