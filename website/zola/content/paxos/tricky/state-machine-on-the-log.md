+++
title = "The state machine on the log"
weight = 21
+++

Paxos decides **what** is in the log. It does not decide **who** may write: a client that lost
its leadership can still send a write, and Paxos will choose it. So paros puts a small state
machine on the log, `JournalState`. Every node applies each chosen slot to it, in slot order,
and the state machine decides the verdict of each call there. The Paxos ballot fences machines;
the journal's leader uuid fences clients.

{{ scenario(name="act3/chosen-is-not-applied") }}

## The failure

**Two writers at once.** Writer A leads the journal. A pauses (a long GC, a network split).
Writer B claims the journal and writes. A wakes up and sends the write it held. Paxos chooses it,
because Paxos only orders. The journal now holds A's write after B's claim.

**A verdict at the leader only.** The leader checks the uuid when the write arrives and answers
"accepted". Before the slot is chosen, a new leader orders B's claim in a lower slot. Every node
applies the claim first, so the write that was "accepted" is behind the claim of another writer.

**A retry that writes twice.** A's write is chosen but the answer is lost. A sends the same write
again, and it lands a second time.

## The rule

1. `SetLeader(new, old)` is a compare-and-set **in the log**. It wins only when `old` is the
   uuid in force.
2. Each `Write(uuid, expected_seq, records)` is accepted only when `uuid` is the uuid in force and
   `expected_seq` is the next position. It then occupies `[seq, seq + n)`.
3. The verdict is taken **at apply**, in slot order, on every node, by `JournalState::apply`.
   A refused call consumes a slot and no position.
4. A retry below the next position that is the **identical** write is an acknowledgement
   (`Outcome::Duplicate`). Anything else there is refused. The log is the deduplication table.
5. Chosen is not applied: the answer to a writer waits until its slot is applied, with the
   verdict the fold gave it.

## Why the rule is enough

Every node applies the same chosen slots in the same order, so every node reaches the same
verdict for every call. A `SetLeader` is itself a chosen slot: every call after it in the log sees
the new uuid, and every call before it saw the old one. There is no window in which two uuids
are in force. A retry is an acknowledgement only when it is the same write at the same position,
so a re-send can never write twice, and no session table has to expire. No clock is involved.

## The code map

| What | Where |
|---|---|
| The state machine | `crates/lib/paros-core/src/journal/state.rs:97` `JournalState` |
| One verdict | `crates/lib/paros-core/src/journal/state.rs:195` `JournalState::apply` |
| The compare-and-set | `crates/lib/paros-core/src/journal/state.rs:455` `apply_set_leader` |
| The verdicts | `crates/lib/paros-core/src/journal/state.rs:128` `Outcome` |
| The fold, one slot at a time | `crates/lib/paros-core/src/replica.rs:916` `fold_one` |
| Single writer or multi writer | `crates/lib/paros-core/src/journal/state.rs:82` `WriterMode` |

## The proof

The simulation folds every node's verdicts against a model of the journal
(`crates/sim/paros-sim/src/audit/journal_model.rs`: "journal: a SetLeader wins only over the
leader in force"), and it searches the client's own history for a linearization against the
sequential model (`audit/linearizability.rs`). The workload makes writers lose their leadership
and claim it again, so a superseded writer's write is sent and must be refused.
