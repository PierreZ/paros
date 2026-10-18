+++
title = "parosd"
description = "The paros journal service"
weight = 1
sort_by = "weight"
+++

`parosd` is a multi-tenant journal service built on paros. One binary runs on each machine.
The machines form a **cell**, and a cell hosts **tenants**. Each tenant owns **journals**. Above
the cells is the **universe**.

A journal is an ordered log of records. Its data plane has four calls:

- `Write` appends a batch of records.
- `Read` returns records from a position.
- `Truncate` removes the records below a position.
- `SetLeader` changes which writer the journal accepts.

Paxos decides every call. A journal is single-writer or multi-writer, and its creator sets the
mode. In a single-writer journal, the writer holds a leader uuid, and `Write` and `Truncate`
must carry it. `SetLeader(new, old)` replaces the uuid only if the current one is `old`.

`parosd` uses paros's own journals, leader election and Paxos flavors for its control plane.
For example, `cell init` is a single-decree Paxos over the founding members. A founding member
that was down when the decree chose the plan learns it later from a formed member, which sends
the operator's token again. `parosctl init` makes the first three listed machines the founding
members and admits the others, so the control journals have three acceptors. `--failures-per-partition F` sets `2F+1` founding members.

**Stopping a machine.** Ctrl-C (`SIGINT`) and `SIGTERM` stop a machine cleanly. The machine
refuses new calls with `stopping`, so a client retries on another machine. It answers the calls
it already took. It hands each journal it leads to another member with the same ballot, so the
journal needs no election, and it hands the cell coordinator's term to the next founding
member. Then its disk writes complete and it exits with code 0. `PAROS_SHUTDOWN_WITHIN_MS`
(default 30 s) limits the stop. A second Ctrl-C stops the machine at once, which is the same as
a crash: the next start recovers from the disk.

[The journal API](@/parosd/journal-api.md) describes the four calls, the two writer modes and
the limits. [Admin views](@/parosd/admin-views.md) shows the machines, tenants and roles of a
cell with `parosctl`. [TLS](@/parosd/tls.md) encrypts every session and proves every caller with
mutual TLS. [Tokens and root keys](@/parosd/tokens.md) signs tokens offline, pins the machines
and rotates the root keys. [Bench](@/parosd/bench.md) measures throughput and latency per call
with `parosctl bench`. [Metrics and probes](@/parosd/metrics.md) reads the counts, the probes and
the usage per journal of each machine. `parosd` is a work in progress. Later pages of this part will describe the control
hierarchy, the bootstrap, multi-tenancy, the cell design and how to run the Compose demo
(`DEMO.md` in the repository).
