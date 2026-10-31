+++
title = "GC and watermarks: when an old set may be forgotten"
weight = 14
+++

Each reconfiguration leaves one more set in the matchmakers, and each later Phase 1 must win a
quorum of every set they report. So old sets must become forgettable. The condition is exact:
**a set may be forgotten only when no future leader needs its Phase-1 quorum to learn a value
that its Phase-2 quorum possibly chose.** A leader proves this about its own log, the
matchmakers raise a floor, and only then may an operator stop a removed acceptor.

{{ scenario(name="act4/garbage-collection") }}

## The failure

**No GC.** Without a floor, `H_b` only grows. Each election waits for a quorum of every set the
cell ever used. A removed machine can never be stopped, because some future Phase 1 may still
need its promise.

**"Installed, therefore deletable."** This is the rule DPaxos uses, and the Matchmaker Paxos
paper (Appendix D) shows that it is wrong. `C_old = {0, 1, 2}` chose slot 5 with 0 and 1. A
leader L wins ballot 9 with `C_b = {0, 3, 4}`. Its Phase 1 covered `C_old`, so L knows the slot
5 value and proposes it again at ballot 9. L forgets `C_old` at once, because `C_b` is
installed, and then stops before slot 5 is chosen at ballot 9. The next candidate asks 3 and 4,
a quorum of `C_b`. Neither holds slot 5, and `C_old` is no longer in `H_b`. The candidate fills
slot 5 with a `Noop`, over a chosen value.

**A floor used before it is in force.** One matchmaker raises its floor and the leader names
node 1 as retirable at once. The operator stops node 1. The next candidate's matchmaker quorum
misses that matchmaker, so `H_b` still names `C_old`, and Phase 1 waits for a node that is gone.

**Retire on a belief.** Node 1 restarts and goes back to its bootstrap belief. That belief does
not name node 1, so node 1 says "I am not a member", and the operator stops it. But a set that
is still alive names node 1.

**GC forgets the set in force.** The floor is the leader's own ballot. An ordinary leader
registers only a belief, so the floor often rises over the record of the last reconfiguration.
After that, no history names a reconfiguration, and a node that restarts with its bootstrap
belief is elected under the old set. The cell rolls back.

## The rule

1. **Split the log at the fence.** A leader at ballot `b` with set `C_b` has an election
   fence `F`, the highest slot any promise reported. Above `F`, Phase 1 found nothing accepted,
   so no old set matters. From the first unchosen slot to `F`, the leader proposes again or
   fills every slot at `b`, under `C_b`. Below that, the chosen prefix: a node that learns a
   slot chosen writes the value as its own accepted record before its chosen index moves.
2. **The condition.** The leadership is settled (no recovery and no repair probe open), and a
   **Phase-2 quorum of `C_b` reports a chosen index at or past `F`**. Acceptors report their
   chosen index on each heartbeat ack. Replicas never count.
3. **Raise the floor.** The leader asks every matchmaker to raise the GC watermark to `b`. A
   matchmaker writes the floor to disk **before** it acks, and refuses campaigns below it.
4. **In force at a quorum.** The floor is in force only when a matchmaker quorum acked it. Only
   then does the leader name the retirable acceptors: the members of `H_b` that are not in
   `C_b`.
5. **Retire with evidence.** The operator reads the floor from a leader's `Inspect` and sends
   it in the retire request. The node agrees only if it has matchmakers, it is not in the set
   it believes in force, it is not the leader, the floor is above every ballot of a set that
   named it, and its belief is bound to exactly that floor. Otherwise it refuses, for example
   as `stale`.
6. **Keep the fact.** Each matchmaker keeps the set in force as a durable value beside the
   floor. GC never collects it, and every answer reports it.

## Why the rule is enough

A future leader's Phase 1 meets a Phase-2 quorum of `C_b`. Each member of that quorum covers
the chosen prefix up to `F`: it answers a `Prepare` with the chosen value, or it refuses one
below its truncation floor. So `C_b` alone finds every value that `C_old` chose, and `C_old` is
no longer needed. The DPaxos rule lacks exactly this: an installed `C_b` whose members did not
yet learn the prefix.

A floor held by a matchmaker quorum is in every later matchmaker quorum, and the candidate
filters with the **maximum** floor it hears. So every future `H_b` leaves out what was
collected. Retirement waits for that, so nobody stops a node that a future `H_b` still names.

## The code map

| What | Where |
|---|---|
| The forgettability condition | `crates/lib/paros-core/src/node/gc.rs:153` `gc_covered` |
| A Phase-2 quorum of `C_b` at or past the fence | `crates/lib/paros-core/src/collector.rs:289` `Collector::covered` |
| Ask for the floor | `crates/lib/paros-core/src/node/gc.rs:172` `try_gc` |
| Raise the floor, one durable write | `crates/lib/paros-core/src/matchmaker.rs:881` `Matchmaker::advance_gc_watermark` |
| In force at a quorum of acks; name the retirable | `crates/lib/paros-core/src/collector.rs:334` `Collector::fold_ack` |
| The five retire conditions | `crates/lib/paros-core/src/node/gc.rs:334` `ColocatedNode::may_retire` |
| The set in force, never collected | `crates/lib/paros-core/src/matchmaker/state.rs:240` `JournalScalars::effective` |

## The proof

The oracles are in `crates/sim/paros-sim/src/audit/`.

- "gc: a GC request is licensed by a quorum holding the fence" (`matchmaking.rs:866`).
- "gc: a floor reported effective is held durably by a matchmaker quorum"
  (`matchmaking.rs:949`).
- "gc: a node retires only after an effective floor named it retirable" (`matchmaking.rs:1040`)
  and "gc: a retired acceptor is outside the configuration in force" (`matchmaking.rs:1004`).
- "gc: the effective configuration survives the watermark" (`matchmaker.rs:609`) and
  "matchmaker: the gc watermark never regresses" (`matchmaker.rs:1251`).
- The handover model checker (`crates/lib/paros-core/src/models/handover.rs`) raises floors at
  random. Its claim 5 asserts that the set in force reaches every campaign, collected or not,
  and its claim 10 that a floor of one journal never moves because of another journal.
- Unit tests: `a_stale_candidate_adopts_the_effective_configuration_after_gc` and
  `a_belief_older_than_the_floor_refuses_to_retire`
  (`crates/lib/paros-core/src/node/tests/matchmaking.rs`), and
  `a_request_below_the_watermark_is_refused_and_the_history_starts_at_it`
  (`crates/lib/paros-core/src/matchmaker/tests.rs`).
