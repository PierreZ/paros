+++
title = "Quorum reads and the watermark"
weight = 9
+++

A linearizable read must see every write that was acknowledged before the read started. A
follower does not know if its log is current. The leader does not know if it is still the
leader. paros reads without the leader and without a clock: it asks a Phase-1 quorum how far to
read, then waits until its own log reaches that point.

{{ scenario(name="act4/quorum-reads") }}

## The failure

**A stale read from a follower.** Three acceptors, A, B and C. Leader A chooses slot 7 with the
votes of A and B. C did not vote, and it has applied only up to slot 6. A client reads from C.
If C answers from its own log, the read misses slot 7, and the client already has the
acknowledgement for slot 7. The history is not linearizable.

**A stale read during a campaign** (#260 (stale read after a Prepare)). The configuration
`C6 = {0, 1, 3}` has a Phase-2 quorum of one acceptor, so its leader chooses a slot with its own
vote only. The leader then campaigns at ballot 7 for `C7 = {0, 2, 3}`, and it stops. Node 3
promised that Prepare. If node 3 reads over the majority `{2, 3}` of `C7`, it misses the slot:
neither 2 nor 3 voted for it. The Prepare named a configuration that no leadership ever won.

## The rule

1. Each acceptor keeps a **vote watermark**: the highest slot it voted in.
2. A reader asks one Phase-1 quorum for its watermarks. On a grid, this is one row. Under a
   majority or a flexible split, it is the whole membership.
3. The tally completes when the answers form a Phase-1 quorum (`has_phase1_quorum_in`). The
   read index is the **maximum** watermark of the answers.
4. Each read is bound to a **read basis** (`ReadBasis`): a configuration, its ballot and a
   fence. A node learns a basis only from a leadership that won its ballot: its own election,
   a handoff install, or a Heartbeat. A node never learns a basis from a Prepare. Without a
   basis, no read opens.
5. The node serves the read when its replica **covers** the larger of the read index and the
   fence. A newer configuration abandons every open read.

During a campaign, a read is unavailable. It is never stale.

## Why the rule is enough

A write acknowledged before the read was chosen by some Phase-2 quorum. Every Phase-1 quorum
meets every Phase-2 quorum, so at least one answer saw the write. The maximum watermark is
therefore at least the slot of that write. The fence covers what earlier configurations chose,
because the Phase 1 that won the basis covered each of them. No step uses a clock or a lease,
so a slow clock cannot break the read.

## The code map

| What | Where |
|---|---|
| The read basis, and the safety argument in the module doc | `crates/lib/paros-core/src/quorum_read.rs:103` `ReadBasis` |
| Serve a read when the prefix covers it | `crates/lib/paros-core/src/quorum_read.rs:457` `QuorumReads::serve` |
| An acceptor's vote watermark | `crates/lib/paros-core/src/acceptor.rs:356` `Acceptor::vote_watermark` |
| A replica covers a slot | `crates/lib/paros-core/src/replica.rs:382` `Replica::covers` |
| "No basis, no read" | `crates/lib/paros-core/src/node/quorum_reads.rs:76` `ColocatedNode::quorum_read_in` |
| Where a node keeps its basis | `crates/lib/paros-core/src/node/view.rs:216` `read_basis` |

## The proof

The simulation checks the read on the server side and on the client side.

- **The read floor.** When a read opens, the audit records the highest slot chosen at that
  moment (`crates/sim/paros-sim/src/audit/node.rs:1001`). When the read is served, it asserts
  that "a read serves every slot chosen before it opened"
  (`crates/sim/paros-sim/src/audit/node.rs:1047`). This oracle catches a stale read that no
  client saw.
- **Covered before served.** The audit asserts that a read is served only when its prefix
  covers the row's watermark (`crates/sim/paros-sim/src/audit/node.rs:1061`).
- **The interesting cases happen.** `assert_sometimes!` checks that a non-leader serves a read,
  that a row's watermark is past the server's prefix, and that a read is served after a leader
  change (`crates/sim/paros-sim/src/audit/state.rs:830`).
- **The client's own history.** The linearizability checker searches every read and write
  attempt for a valid order (`crates/sim/paros-sim/src/audit/linearizability.rs:666`).
- **Unit tests.** `a_campaign_configuration_is_no_read_basis`
  (`crates/lib/paros-core/src/node/tests/quorum_reads.rs:380`) pins #260.
  `a_read_completes_on_its_row_at_the_maximum_watermark` and `a_read_waits_on_its_basis_fence`
  are in `crates/lib/paros-core/src/quorum_read/tests.rs`.

The field guide chapter [Why reads are not free](@/paxos/linearizable-reads.md) has the papers
and the strawman reads. The level
[`act2/the-read-that-lies`](../../../play/#act2/the-read-that-lies) shows why a follower cannot
answer from its own log.
