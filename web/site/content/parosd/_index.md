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
For example, `cell init` is a single-decree Paxos over the founding members.

[The journal API](@/parosd/journal-api.md) describes the four calls, the two writer modes and
the limits. `parosd` is a work in progress. Later pages of this part will describe the control
hierarchy, the bootstrap, multi-tenancy, the cell design and how to run the Compose demo
(`DEMO.md` in the repository).
