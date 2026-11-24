---
layout: ../../layouts/Doc.astro
kicker: Journal
section: journal/
status: journal
lab: live
title: The journal, live
description: The whole journal v1 runs in this page, on six hosts. Append records, read them back, roll to a new segment, and crash or restart a host. Every answer comes from the library code.
---

## What runs here

This page runs `paros-journal` and `paros-journal-io`, compiled to wasm. It is the same scenario
as the facade example
[`full_journal`](https://github.com/PierreZ/paros/blob/main/crates/lib/paros/examples/full_journal.rs),
which runs the journal on six tokio tasks. Here the six hosts run in one loop of the page, on the
in-memory backends of `paros-journal-io`.

- **Six hosts.** Each host is one `Driver` of `paros-journal-io`, with its own disk. A crash
  stops the driver and keeps the disk. A restart builds a new driver on the same disk.
- **Three matchmakers**, on hosts 1, 2 and 3. They hold the journal's register: one
  `RegisterMember` each, and one `RegisterCall` for each change (a read at a majority, the
  guard, a write at a majority). See [Matchmakers and the register](../matchmakers/).
- **Six acceptors** on a grid of two columns, `{1, 2, 3}` and `{4, 5, 6}`, so `F = 1`. A write
  is chosen at `F + 1` acceptors of one column. Phase 1 needs `F + 1` acceptors of each column.
  See [Quorums and failures](../quorums-and-failures/).
- **Three replicas for each segment.** The replicas of segment 1 are on hosts 1, 3 and 5, and
  those of segment 2 on hosts 2, 4 and 6. A write needs `F + 1` replicas of its segment.
- **One leader** for each segment, on a host of its replicas. A roll starts the leader of the
  new segment, which seals the segment before. See [Segments](../segments/).
- **One writer, A**, the pure client of `paros_journal::client`. It claims the journal with
  `SetLeader`, writes with `expected_seq`, and reads each segment from its replicas.

The page only runs ticks of the clock and plays your moves. It holds no journal rule. Each line
of the hosts, matchmakers and segments is read from the roles and the register.

Three things are simpler than in a real cell. The controller is the page: it opens segment 1,
asks for each new segment with one register change, and starts the new leader from the front
of the replicas of the old segment, where the journal has [the controller](../controller/) role. A
leader also moves only at a roll. And the register runs one operation at a time, so no
write stays at a minority of the matchmakers. A leader stops today when its own seal comes back
refused after a partial write; the fix is open in the library.

## Things to try

- Press **Append a record** a few times, then **Read the journal**.
- **Crash host 3.** Writes still land: acceptors 1 and 2 are a quorum of column one, replicas
  11 and 13 are two of three, and matchmakers 1 and 2 are a majority. Restart host 3 and watch
  replica 12 catch up.
- Crash hosts 2, 3, 5 and 6. Each column keeps one acceptor, so no column holds `F + 1`: the
  next write waits. Restart hosts 3 and 5 and the write lands.
- **Crash host 1**, which runs leader L1. Writes stop: a leader moves only at a roll. Press
  **Roll to a new segment**: the leader of segment 2 starts on host 2, seals segment 1, and the
  write that waited lands.
- Press **Roll to a new segment**. The leader of segment 2 seals segment 1, and the writes go to
  segment 2. Segment 1 retires once `F + 1` of its replicas hold it and its boundary state is written. A second roll
  right after is refused: the register opens a new segment only when every older segment is
  retired and the newest one is in force.
- Read the whole journal after a roll: the client reads the sealed segment, then the tail.

<div data-live-journal></div>

## In paros

- The engine of this page: `crates/support/paros-wasm/src/journal/live.rs`
  ([source](https://github.com/PierreZ/paros/blob/main/crates/support/paros-wasm/src/journal/live.rs)).
- The same run on tokio tasks: `crates/lib/paros/examples/full_journal.rs`.
- The roles: `paros_journal::roles`. The drivers and backends: `paros-journal-io`. The rules:
  [`docs/journal/`](https://github.com/PierreZ/paros/blob/main/docs/journal/README.md).
