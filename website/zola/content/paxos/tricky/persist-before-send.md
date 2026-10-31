+++
title = "Persist before send, and the boot path"
weight = 6
+++

A Promise and an Accepted are promises about the future. A proposer counts them, and it never
asks again. So each one must survive a crash of the node that sent it. paros writes before it
sends, rebuilds a restarted node from its disk alone, and refuses to boot a node that lost its
disk.

{{ scenario(name="act2/persist-before-send") }}

## The failure

**A promise that is not on disk.**

1. Acceptor A2 sends Promise(5) to proposer P5 before the promise is on disk. Then A2 crashes.
2. A2 restarts. Its promise is back at 0.
3. An old leader at ballot 3 sends Accept(slot 0, X). A2 and A3 vote for it, and X is chosen.
4. P5 counted A1 and A2 as a quorum at ballot 5. Neither reported X, so P5 chooses Y for slot 0.

Slot 0 now has two chosen values.

**A wiped disk.** A node that lost its disk boots as a new node. It answers "nothing accepted"
for slots it voted in, and it makes promises below the promise it already gave. This is
**amnesia**.

## The rule

- The core gives the driver a `Ready` batch. The driver must persist `writes()` first and send
  `messages()` after.
- `MustSync::for_batch` decides if the batch needs an fsync. It does when the batch raises a
  promise, records an accept or a learned value, truncates, or jumps to a trim point.
- One exception (`split_early`, #578 (early Accept)): a leader may send its Accept requests
  before its own flush, if the batch does not raise its promise. If the leader loses its own
  vote, no decision is lost, because the vote was not yet counted.
- On boot, `ColocatedNode::new` rebuilds the node from the disk alone, as a follower. Volatile
  state such as `next_slot` is derived from the records read back. It is not stored beside
  them, so it cannot disagree with them.
- Before the core reads anything, `check_format_marker` compares what the operator says
  (`BootKind`) with the format marker of the store. An existing member with no marker is
  refused as `Amnesia`. An existing member whose marker names another configuration is refused
  as `ConfigMismatch`. A first boot writes the marker and syncs it before its first promise.

## Why the rule is enough

Every Promise and every Accepted that a proposer counts is on disk before it leaves. A crash
cannot take one back. A node without its disk never rejoins with its old identity. The cell
gets its redundancy back by a reconfiguration, which adds a new identity in place of the lost
one.

## The code map

| What | Where |
|---|---|
| The durable state of a node | `crates/lib/paros-core/src/contract/hard_state.rs:26` `HardState` |
| When a batch needs an fsync | `crates/lib/paros-core/src/contract/write.rs:151` `MustSync` |
| Writes first, messages after | `crates/lib/paros-core/src/node/ready.rs:46` `Ready` |
| Rebuild a node from its disk | `crates/lib/paros-core/src/node/boot.rs:37` `ColocatedNode::new` |
| The format marker check | `crates/lib/paros-roles/src/node/boot.rs:38` `check_format_marker` |
| The three refusals | `crates/lib/paros-storage/src/boot.rs:36` `BootRefusal` |
| The early Accept | `crates/lib/paros-roles/src/node/ready.rs:507` `split_early` |

## The proof

- **At the send seam.** The audit asserts that "a sent Promise carries a durably promised
  ballot" (`crates/sim/paros-sim/src/audit/node.rs:349`).
- **Across restarts.** The audit asserts that a node's promised ballot never decreases
  (`crates/sim/paros-sim/src/audit/state.rs:1031`), and that a restart never changes an
  accepted value that the node had before the crash (`crates/sim/paros-sim/src/audit/state.rs:1062`).
  Under mutation testing, a version that let a wiped node rejoin turned the first assertion red.
- **The refusals happen.** The simulation reaches the `Amnesia` and the `ConfigMismatch`
  refusals (`crates/sim/paros-sim/src/audit/machine.rs:96`).
- **Unit tests.** `every_acceptor_mutation_emits_its_own_fsynced_write`
  (`crates/lib/paros-core/src/acceptor/tests.rs:75`) and
  `a_store_formatted_under_another_configuration_is_refused`
  (`crates/lib/paros-storage/src/provision.rs:130`).

The levels [`act2/what-survives-a-crash`](../../../play/#act2/what-survives-a-crash) and
[`act4/the-wiped-node`](../../../play/#act4/the-wiped-node) show the rest by hand. The field
guide is [Crash and restart safety](@/paxos/restart-safety.md).
