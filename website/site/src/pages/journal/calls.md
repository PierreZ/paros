---
layout: ../../layouts/Doc.astro
kicker: Journal
section: journal/
status: journal
lab: journal
title: The calls
description: Four calls make the data plane. One writer gets exactly-once writes with no dedup table; with no leader, many writers append at-least-once.
---

## The problem

A writer sends a write, and the answer does not come back. Did the write land? If the writer
sends it again, the journal can hold it two times. If it does not, the journal can lose it. A
journal must give the writer a way to know, with no table of past requests to keep and expire.

## The four calls

| Call | What it does |
|---|---|
| `Write(uuid, expected_seq, batch)` | Appends a batch at `expected_seq`, fenced by the writer's uuid. With no leader: `Write(batch)`, unfenced. |
| `Read(from_seq, limit, wait_ms)` | The records from `from_seq` in order, with `first_seq`, `next_seq` and the leader. At the tail it waits up to `wait_ms`. |
| `Truncate(uuid, up_to_seq)` | Moves the floor `first_seq` up. Fenced like a write; open to all with no leader. |
| `SetLeader(new, old)` | A compare-and-set of the leader uuid. Its answer is the view after it: the leader, `next_seq`, `first_seq`. |

`Resolve` is a fifth, read-only call: the client uses it to learn the fate of a write across a
seal (below). §J2 of
[`02-data-plane.md`](https://github.com/PierreZ/paros/blob/main/docs/journal/02-data-plane.md)
holds every rule of this page.

## Positions and slots

`seq` is the position of a record. It is dense: a batch of `n` records takes `[seq, seq + n)`,
in one Paxos slot, accepted or refused whole. Slots stay inside the journal. A `Noop`, a
`SetLeader`, a refused write and a `Truncate` each take a slot and no `seq`, so a reader never
sees a hole (§J2.1).

## One writer: the log is the dedup table

The leader uuid is a 128-bit random value. A writer draws a new one for each term in which it
leads, so a writer that wins again also fences its own old writes. `SetLeader(new, old)` is the
whole election: it wins only when `old` is the current leader. There is no lease and no clock
(§J2.3). This uuid is not the Paxos ballot. The ballot says which machine runs the consensus
leader; the uuid says which client may write. Neither layer sees the other.

A fenced write names the position it wants, `expected_seq`. At apply, in slot order, each
replica judges it in this order (§J2.4.1):

1. Below `first_seq`: **`Truncated`**. Its fate is unknown.
2. Below `next_seq`: **`Duplicate`** when the record there is the same write (same uuid, same
   batch), else refused: the position is taken.
3. A uuid that is not the leader: refused, **`fenced`**. Uuids are fresh, so it never lands.
4. Above `next_seq`: **early**. Send it again later.
5. Else: written, with its `seq`.

So a writer that lost an answer sends the same write again, at the same position. The log
answers from what it holds: `Duplicate` means written, a refusal means not written, for ever.
The position comes before the fence: with the fence first, a write that landed before a
`SetLeader` would come back `fenced`, and the writer would write it a second time. TLC finds
that bug in [`multi-writer/`](https://github.com/PierreZ/paros/tree/main/specs/tla/multi-writer)
(controls `dedup_after`, `dedup_after_twice`).

## No leader: many writers

`SetLeader(M, old)` makes the journal multi-writer, and `SetLeader(new, M)` makes it
single-writer again (§J2.4). `M` is a multi-writer mode with a fresh tag that the caller draws, as
it draws a uuid. A request can stay in the network and apply late: with one value for every
multi-writer mode, a late copy of an old `SetLeader(u, M)` would match again and bring `u` back.
With a tag for each mode, it never matches a later mode (§J2.4.1). The lab names the modes `M1`,
`M2`. With no leader, `Write(batch)` is unfenced, and the journal gives it
its `seq` at apply. A batcher can merge many writers' writes into one slot. A write sent again
after a lost answer can land two times: multi-writer is at-least-once, and the writers own that.

## A leader that loses its leadership

The consensus leader holds the answer of each write until its slot applies. After a leader
change, the slot of a deposed leader can choose another command, or a `Noop`. So that leader
answers **`Unknown`**, never "not the leader": a client that heard "not the leader" would send
the write again, and both copies could land. A single writer settles `Unknown` with the same
write at the same `expected_seq` (§J2.2, checked in
[`compartments/`](https://github.com/PierreZ/paros/tree/main/specs/tla/compartments)).

## Across a seal: `Below`, then `Resolve`

The fold of a [segment](../segments/) starts from the state at the seal, not from the records of
the segment before it. So when a resend names a position of an older segment, the fold cannot
tell `Duplicate` from "taken". It answers **`Below`**. The client then asks a replica of the old
segment: `Resolve(uuid, expected_seq, write)`. A sealed segment does not change, so that answer
is final.

<div class="seq" style="--lanes: 3">
<table>
<thead><tr><th>#</th><th>writer</th><th>segment 2</th><th>segment 1 replica</th><th class="note">what happens</th></tr></thead>
<tbody>
<tr><td>1</td><td class="msg">Write @7 →</td><td></td><td></td><td class="note">The answer is lost. Then a reshape seals segment 1 at <code>seq</code> 9.</td></tr>
<tr><td>2</td><td class="msg">Write @7 →</td><td>Below</td><td></td><td class="note">The resend reaches the fold of segment 2, which starts at 9.</td></tr>
<tr><td>3</td><td class="msg">Resolve @7 →</td><td></td><td>written, 7</td><td class="note">The replica compares the same uuid and the same write at 7, as apply does.</td></tr>
</tbody>
</table>
</div>

`Resolve` only reads. It never writes to the sealed segment. After "not written", the writer
reads the tail and sends the write to the segment in force, at a new `expected_seq`. TLC checks
the call in [`journal-state/`](https://github.com/PierreZ/paros/tree/main/specs/tla/journal-state)
(`ResolveTrue`; a compare of the uuid only and an answer before the fold covers the position are
the controls).

## The answers

| Answer | Final? | What the writer does |
|---|---|---|
| ok, `seq` | yes | Nothing. The write holds `[seq, seq + n)`. |
| `Duplicate`, `seq` | yes | Nothing. The write was already there. |
| taken, `fenced` | yes | Not written. Read the view and decide. |
| early | no | Send it again when the tail reaches its position. |
| `Unknown` | no | Send the same write at the same position. |
| `Below` | no | `Resolve` at a replica of the old segment. |
| `Truncated` | no | The fate is unknown. Read the tail. |
| `WRONG_MODE` | for that copy | The call is shaped for the other mode. |
| `TooLarge`, `busy` | no | The write is in no slot. Make it smaller, or wait. |

The full table, with `Empty` and `Reserved`, is at the top of §J2.

## Reads and truncation

A read never goes through the Paxos leader. A replica asks a Phase-1 quorum of acceptors for
their vote watermarks, waits until its own log covers the highest one, and answers (§J2.5). Each
answer carries the acceptor's promise, so a newer leader ends the read: a read can wait during an
election, and it is never stale. The [Quorums](../../paxos/quorums/) page steps through a
leaderless read.

`Truncate` is a decided command like a write. It never lowers `first_seq`, and every node
drops the records below it when its walk reaches the slot. The leader keeps its own checkpoint
first; the journal does not check it (§J2.6).

## Headers

Each record carries two lists of headers: the client's, which the journal never reads, and
paros's own. In v1 paros adds only the leader time, a hint with no order promise. It is not part
of the identity of a write, so a resend with a new time is the same write (§J2.8).

## Your turn

Make the calls of three writers, A, B and C. `paros-journal` runs in this page, compiled to
wasm, and answers each call: the fold `JournalState::apply`, and `resolve` for `Resolve`. It is
the code a replica runs. Here each call is one decided slot, applied at once: no consensus runs,
and there is no network, disk or clock. A seal starts the next segment from the state at the
seal.

<div data-journal-lab="journal-duplicate journal-fenced journal-multi journal-below journal-truncated"></div>

## In paros

- [`paros-journal`](https://github.com/PierreZ/paros/tree/main/crates/lib/paros-journal):
  the fold `JournalState` and its `Answer`s ([`fold.rs`](https://github.com/PierreZ/paros/blob/main/crates/lib/paros-journal/src/fold.rs)),
  the `resolve` call ([`resolve.rs`](https://github.com/PierreZ/paros/blob/main/crates/lib/paros-journal/src/resolve.rs)),
  the held answers ([`held.rs`](https://github.com/PierreZ/paros/blob/main/crates/lib/paros-journal/src/held.rs)),
  the commands and headers ([`command.rs`](https://github.com/PierreZ/paros/blob/main/crates/lib/paros-journal/src/command.rs))
  and the codec ([`codec.rs`](https://github.com/PierreZ/paros/blob/main/crates/lib/paros-journal/src/codec.rs)).
- [`journal-sim`](https://github.com/PierreZ/paros/tree/main/crates/sim/journal-sim): three
  writers resend across seals, switch modes and truncate; four oracles judge the fold against a
  fold of the whole log with no segment.
- [`paros-wasm`](https://github.com/PierreZ/paros/tree/main/crates/support/paros-wasm), feature
  `journal`: the exercises above, a driver of `paros-journal` with no journal rule of its own.
- Not built yet: the client (#1331) and the roles that serve the calls (#1326).
