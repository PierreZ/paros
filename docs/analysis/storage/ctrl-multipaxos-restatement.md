# CTRL restated for Multi-Paxos: protocol-aware recovery (Stage 8)

Stage 8 (issue #21) design note. CTRL (*Protocol-Aware Recovery for Consensus-Based
Storage*, FAST '18) is written against Raft/ZAB. Three of its rules are **wrong for
Multi-Paxos as written**; this note is the load-bearing restatement, reviewed before any
code. It builds on the Stage-7 contract in `clstore-record-contract.md` (detection +
classification, the `CorruptionVerdict` surface) and the #70/#71 decisions.

The guarantee being bought (CTRL §3.2):

> if there exists at least one correct copy of a committed data item, it will be
> recovered or the system will wait for that item to be fixed; committed data is never
> lost. Where all copies of a committed item are faulty, the system correctly remains
> unavailable. Uncommitted faulty items are decided as early as possible, for
> availability.

## R1 — "Leader completeness" is post-Phase-1, not a log property

A Raft leader holds all committed entries on disk at election. A Paxos leader holds all
*potentially-chosen* values only **after** its promise quorum replies, in the merged
Phase-1 result (P2c). Every "the leader knows entry X is uncommitted" step below is
therefore gated on **Phase 1 complete** and consults **own accepted log ∪ recovered
set**, never the raw local log.

## R2 — The Promise response IS the recovery query, and needs a tri-state

A corrupted accepted value at slot `s` must be reported in the Promise as a third state,
never as "nothing accepted here" (that is the CTRL Figure-2 bug class wearing a protocol
hat). Per queried slot the acceptor answers one of:

- `have(ballot, value)` — intact accepted value (`Promise.accepted`);
- `none` — genuinely nothing accepted (absence from both maps of a page that covers `s`);
- `faulty(ballot)` — the identifier survived (slot and accepted ballot are known) but the
  value is lost (`Promise.faulty`).

TigerBeetle states the same rule as "we do not nack corrupt entries, since they might be
the prepare being requested": **faulty means silence toward the none-tally, never
denial.** A `faulty` report never counts toward `none`. CTRL's model checker finds a
safety violation immediately when this is weakened; the `faulty-as-none` red demo pins
the same result here (an acceptor that misreports its rotted copy as `none` lets a
unanimous-looking quorum no-op-fill a chosen slot: two values chosen for one slot).

Which local evidence maps to `faulty(ballot)` is Stage 7's classification, unchanged:
only records whose **identity is known** are recoverable — `CorruptionBelowTail`,
`IdentifierFaulty` (the entry's checksummed region still names slot+ballot), and
`LastEntryAmbiguity` (CTRL Thm A.1's proven-undecidable row: distributed commitment
determination decides it). A record whose entry *and* identifier are both lost is
unidentifiable — the node cannot even ask peers the right question — and stays a crash,
as do FS-metadata faults and a double-lost `HardState` (#20's split).

## R3 — "Discard the entry and all subsequent entries" does NOT port

Raft commits in order; Multi-Paxos pipelines and commits slots out of order (paros's own
election-gap-fill doctrine exists because of this). The Paxos-correct analogue of CTRL's
Case 2:

> when the new leader's ballot-`b` promise quorum Q1 **unanimously** reports `none` at
> slot `s`, no value can have been chosen at `s` below `b` (any chosen value's Q2
> intersects Q1, and every Q1 member has promised `b`). The safe action is **decide
> `Control::Noop` at slot `s`** — the existing gap fill — and leave every other slot
> alone.

Strictly better than Raft's truncate: nothing is discarded, the slot is decided as a
no-op. The threshold is **a full Q1 quorum of `none`** — under flexible quorums, whatever
Q1 the configuration defines (`QuorumSystem::quorum_size`), never a hardcoded majority.
The tally is per slot: a node counts toward `none` at `s` only when it has answered the
whole suffix covering `s` and reported neither `have` nor `faulty` there. The quorum of
`none` need not be the quorum that elected the leader — any Q1-sized set of none-reporters
suffices (each promised `b`, so the intersection argument goes through unchanged), which
is what lets a straggler's late answer resolve a slot its faulty reporter blocked.

Three cases per faulty slot, any arrival order, first of Case 1/2 wins:

- **Case 1 — any `have(b, v)`:** repair by re-proposing `v` at the new ballot through the
  normal P2c path (the leader's ordinary recovered-slot re-propose; the `Accept`
  overwrites the faulty local record on every holder).
- **Case 2 — a full Q1 of `none`:** decide `Noop` at that slot only. This is also the one
  legal "discard" of a follower's faulty record, and it is leader-instructed *through
  ordinary consensus* (the decided Noop replicates and overwrites), never a local
  decision.
- **Case 3 — neither:** **wait.** The slot stays undecided and unapplied; the leader
  keeps querying stragglers (re-sending `Prepare` at its ballot to peers that have not
  answered). A leader that cannot finish recovery resigns after a timeout (CTRL §4.2) so
  another node — possibly one holding the missing copy — can try.

Out-of-order application never becomes possible: a faulty slot inside or below the chosen
prefix stalls the contiguous `Ready` apply walk exactly like an unchosen gap, and the
stall is surfaced through the existing `chosen_gap()` / `Audit::chosen_gap` seam rather
than a parallel mechanism.

## R4 — Ballot monotonicity across slots does not exist

Restated from #20: ballots are legitimately non-monotonic across slots in Multi-Paxos, so
every sanity check on the recovery path ports on `slot` only, never on term-runs.

## The single most important rule

Recovery may **never delete promised- or accepted-ballot state.** Delete-and-refetch can
erase a promise already given and violate safety (the MarkNonVoting bug, #19's amnesia
demo). Mechanically: repair *fills or replaces-with-proven-identical* —

- a faulty slot is only ever overwritten by an `Accept` at a ballot ≥ the node's promise
  (≥ the lost record's ballot, so nothing newer is clobbered; at an equal ballot P2b
  makes the value identical), or by a **chosen** value (quorum-decided; if the lost
  record's ballot was higher, P2c made its value equal to the chosen one);
- nothing on the recovery path lowers the promise (`set_promise` is the asserted
  choke point), un-accepts a slot, or rewinds the chosen index;
- `HardState` copies repair only from the local twin, never from peers — the promise is a
  statement about this node's own future behaviour that no quorum knows; both copies lost
  ⇒ crash (recover-or-wait has nothing to recover from).

## Where each repair actually flows (Boxes B/C/D, fused into existing paths)

paros deliberately grows **no dedicated recovery RPC**. Every repair rides a path that
already exists, which is what keeps the M5 compartment split clean:

| Faulty item | Recovery path |
|---|---|
| chosen slot, value lost locally (below/at the chosen index) | commit-replay catch-up: the node pulls from its first unhealed slot (`CatchUpRequest`); any peer with the slot decided serves it. A peer never serves past its *own* faulty slot (per-slot attribution: silence, not garbage). |
| accepted-but-undecided slot, value lost | the leader's Phase-1 tri-state (R2/R3) at the next election; before that, the leader's ordinary `Accept` re-send or the slot's `Commit` overwrites it. |
| chosen prefix truncated on every peer that could serve it | whole-blob `InstallSnapshot`, the existing below-floor path (chunk-level snapshot repair is #101). |
| local application snapshot corrupted, log still covers it (floor = 0) | rebuild locally by replaying the retained log — CTRL's cheap path; a rot mid-log falls through to catch-up for the rest. |
| local application snapshot corrupted, log truncated | remote `InstallSnapshot`; until one lands the node serves consensus for every slot it can read but applies nothing (wait, not fabricate). |

A faulty acceptor stays a **full voting member for every other slot** — fault attribution
is per-slot, never per-node. It answers `Prepare`/`Accept` for every slot it can read,
and never acks (or serves) a slot whose record is not clean-and-durable.

## Oracle flip

Stage 7's oracle was *detect ⇒ crash* (availability disaster, safety intact). Stage 8
asserts the full CTRL guarantee, three legs:

1. **Safety:** chosen values survive while ≥ 1 clean copy exists (unchanged
   at-most-one-value + the cross-restart promise/accepted audits).
2. **Recover:** with a clean copy reachable, the cluster converges — the wedge gate
   (`chosen_gap` streak) stays armed, so an unhealed hole with a clean copy somewhere is
   a red run.
3. **Wait:** with **no** clean copy of a committed item (budget-off runs only — the
   per-record budget forbids it otherwise), unavailability is *correct* and must be
   *exercised*: the wedge/convergence gates are excused only by the world's ground truth
   ("no clean copy exists"), and the WAITED leg carries its own `assert_sometimes!` gate
   so a sweep cannot go green having only ever recovered.

`RecoveryOracle` divergence stays explained-only (#71): a recovered log may omit a
persisted record only after a detected-corruption crash **or** a reported-faulty /
peer-repair event in the audit — never for unexplained divergence.

The repair-cost metric (CTRL §5.2) is recorded beside the oracle: bytes shipped to repair
one corrupted slot (a protocol-aware repair moves one entry, ~KB; a truncate-and-refill
re-ships the log, ~MB) — it distinguishes the real implementation from a degenerate one
better than any green light.
