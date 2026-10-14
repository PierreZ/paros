+++
title = "The journal API"
description = "Write, Read, Truncate and SetLeader, the two writer modes and the limits"
weight = 1
+++

A journal is an ordered log of records. Each record has a position, its `seq`. The journal
keeps two positions: `first_seq`, the first record it still holds, and `next_seq`, the
position the next record gets. Paxos decides every call that changes the journal, and every
node judges the call when it applies the decided slot, in slot order. So all nodes give the
same answer to the same call.

The data plane has four calls:

| Call | Single-writer journal | Multi-writer journal |
|---|---|---|
| `Write` | `Write(leader_uuid, seq, batch)`: fenced, at an exact position, a retry is safe | `Write(batch)`: not fenced, the journal gives the position |
| `Read(from_seq, limit, wait_ms)` | the records from `from_seq`, with `first_seq`, `next_seq` and the leader uuid | the same |
| `Truncate` | `Truncate(leader_uuid, up_to)`: fenced | `Truncate(up_to)`: anyone |
| `SetLeader(new, old)` | compare-and-set of the leader uuid | refused |

## Two writer modes

The creator of a journal sets its mode, and the mode does not change. A call shaped for the
other mode gets `WrongMode`, and nothing moves. A single-writer client that meets a
multi-writer journal never writes without its fence, and the reverse.

**Single-writer.** One writer at a time owns the journal. The writer holds a **leader uuid**:
128 random bits, drawn once for each leadership term. The uuid is not a secret, and it is the
only fence. `SetLeader(new, old)` makes `new` the leader only if `old` is the current leader.
The first `SetLeader` names no `old`. A `Write` and a `Truncate` carry the uuid, and the
journal accepts them only if the uuid is the current leader. A superseded writer gets
`Refused`, with the current leader and `next_seq`, so it knows what happened.

A `Write` names the position of its first record. The journal accepts it only at `next_seq`,
so the records of one leader are dense and in order. A retry of a write that the journal
accepted (the same uuid, the same position, the same records) gets `Duplicate`, and nothing
moves. A client that did not get an answer can send the same write again, at any time, to any
node.

{% mermaid() %}
sequenceDiagram
  autonumber
  participant A as Writer A, uuid a
  participant J as Journal
  participant B as Writer B, uuid b
  A->>J: SetLeader(new a, old none)
  J-->>A: Leader, next_seq 0
  A->>J: Write(a, seq 0, [r0, r1])
  J-->>A: Accepted at 0, count 2
  B->>J: SetLeader(new b, old a)
  J-->>B: Leader, next_seq 2
  rect rgba(200, 70, 70, 0.25)
  A->>J: Write(a, seq 2, [r2])
  J-->>A: Refused, leader b, next_seq 2
  end
  rect rgba(70, 170, 110, 0.25)
  B->>J: Write(b, seq 2, [r2'])
  J-->>B: Accepted at 2, count 1
  end
{% end %}

The journal trusts its clients to draw a fresh uuid for each term. A uuid that led before can
win a `SetLeader` again. The guarantees above still hold when it does, and the simulation
reinstates uuids on purpose to prove it. A well-behaved client never does it.

**Multi-writer.** Anyone with access appends. A `Write` carries no uuid and no position: the
journal gives the batch the position `next_seq` when it applies the slot. There is no
deduplication. When a client gets no answer, the write may still land, and a retry may land a
second time. Delivery is at least once, and the writers own that. The client library never
sends a multi-writer write again on its own. Anyone may `Truncate`, and `SetLeader` is
refused.

## Reads

A `Read` returns the records from `from_seq`, dense from it, plus `first_seq`, `next_seq` and
the current leader uuid. If `from_seq` is below `first_seq`, the answer is `Truncated`, and it
tells where the journal starts. Any node or replica can serve a read. The server first asks a
quorum of the acceptors for their vote watermarks. Then it answers from its own copy, after
that copy holds every slot the quorum voted for. So a read sees every write that was
acknowledged before the read started. The [Paxos chapter on reads](@/paxos/linearizable-reads.md)
explains why this costs a round trip.

A read that starts at `next_seq` or after it has nothing to return yet. It can wait at the tail
for `wait_ms`. If a record comes in that time, the server answers at once. If not, the answer
is an empty page.

A server that cannot confirm the read in time answers **unserved**. The client asks another
server. An unserved answer is never wrong, because it carries no records.

## Truncation

`Truncate(up_to)` drops every record below `up_to`. It never lowers `first_seq`, and the
journal clamps it to `next_seq`. In a single-writer journal only the leader truncates, with its
uuid. The leader truncates only after it saved the checkpoint it needs; paros does not check
this. A reader below `first_seq` gets `Truncated`, and the application decides where to
restart.

## Limits

The limits are part of the API. Each node has its own values, so a retry to a different node
can get a different answer.

| Limit | Default | What happens above it |
|---|---|---|
| records in one `Write` | 1,024 | `TooLarge`, before consensus: the write is in no slot |
| record bytes in one `Write` | 1 MiB | `TooLarge`, before consensus |
| records in one `Read` page | 256 | the page is cut; the client reads on from the page's end |
| record bytes in one `Read` page | 64 KiB | the page is cut, but it always holds one record |
| `wait_ms` | 1 s in `parosd`, at most | the wait is cut to the maximum |
| `wait_ms` | 0, at least | a shorter non-zero wait is raised to the minimum |

A `Read` with `limit` 0 gets the full page. A `Read` with `wait_ms` 0 never waits. A
`TooLarge` answer names the two batch limits of the node that refused it, so the client can
split the batch. An operator sets each limit with a `PAROS_*` variable, for example
`PAROS_MAX_READ_RECORDS` or `PAROS_MAX_WAIT_MS`.

## Ids

Every journal call names its journal by the pair `(tenant, journal)`. Each half is a random
`u64`, and `0` means "not set": a call with a `0` half is refused as an unknown journal. Every
`seq` is a `u64`. The leader uuid is 128 bits.

## From the command line

`parosctl` sends these calls. A journal is named `TENANT/JOURNAL` (or `paros://TENANT/JOURNAL`),
or by its hex ids, `id:TENANT/JOURNAL`. `parosctl write acme/orders hello --leader 7` claims the
journal under the uuid `7` if it does not lead yet, and then writes at the tail. `parosctl
set-leader acme/orders --new 8` takes the journal from the current leader. `parosctl read
acme/orders --limit 10 --wait-ms 500` reads ten records and waits half a second at the tail. `parosctl write --multi` and
`parosctl truncate --multi` send the calls of a multi-writer journal. The `DEMO.md` file in
the repository shows a full session.
