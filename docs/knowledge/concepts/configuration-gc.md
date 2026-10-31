# Configuration GC and retirement

Group: Reconfiguration. Status: verified 2026-10-31.

Every configuration a matchmaker returns costs the next candidate a Phase-1 quorum of it, and its
acceptors cannot be shut down while a candidate may still ask them. **Garbage collection** (GC)
raises the matchmakers' watermark so they never return the configurations below it again
([MMP §3.4]). A configuration may be forgotten only when **no future leader can need its Phase-1
quorum** to learn a value it may have chosen ([MMP §3.5]). In paros a leader's floor is
**effective only once a matchmaker quorum acked it durably**; only then are the old members
**retirable**, and a node honors `Retire` only with that floor as evidence.

## 1. Problem

Without GC, `H_b` grows with every reconfiguration, and every old acceptor must stay up for ever.
With a wrong GC rule, a value is lost. [MMP Appendix D] walks a DPaxos execution where GC lets a
leader miss an already-chosen value `x` and choose `z`: a committed value is overwritten. The
`node/gc.rs` module doc gives the paros shape of the same bug: the new members had not learned
the prefix, a leader GCs and dies, and the next candidate's Phase 1 finds nothing at a slot the
old configuration chose, so it fills the slot with a `Noop`.

## 2. Model

- The Matchmaker Paxos model ([matchmaking.md](matchmaking.md) §2).
- A leader at ballot `b` with configuration `C_b`, elected over `H_b`, has an election **fence**
  `F = next_slot − 1`: the highest slot any promise reported.
- In paros every acceptor is also a learner: it records a chosen value as its authoritative
  accepted record (`mark_chosen`) before its chosen index moves ([replica tier](replica-tier.md)).

## 3. Roles and state

| Role | State | Durable? |
|---|---|---|
| Leader (`Collector`) | the fence, the prior configurations `H_b`, the chosen indices reported by `C_b`, the matchmakers that acked the floor | no |
| Matchmaker | the GC watermark `w`; the effective configuration, which the watermark never touches | yes, before it acks |
| Node | `last_member_ballot`, its belief and the ballot it is bound to (`acceptors_since`) | no |
| Operator (the tenant coordinator in a cell) | the effective watermark it read from the leader's `Inspect` | no |

## 4. Normal flow

1. **Covered.** The leader waits until a Phase-2 quorum of `C_b` reports a chosen index at or past
   `F` (`Collector::covered`, from the `HeartbeatAck`s).
2. **Request.** It sends `GcRequest(b)` to every matchmaker of the current generation, and re-sends
   it on the driver's cadence (`ColocatedNode::resend_gc`).
3. **Ack.** Each matchmaker deletes every registration below `b`, raises `w` to `b` durably, and
   acks with its durable watermark.
4. **Effective.** On a matchmaker quorum of acks the floor is effective
   (`GcStep::Effective`), and the leader names the retirable acceptors: the members of `H_b`
   outside `C_b`.
5. **Retire.** The operator reads the watermark from `Inspect` (`gc_watermark`) and sends `Retire`
   with it. The node checks `ColocatedNode::may_retire` and shuts its journal down.

```mermaid
sequenceDiagram
    participant L as Leader (b, C_b)
    participant M as Matchmakers
    participant Op as Operator
    participant X as Old member (in C_old only)
    Note over L: a Phase-2 quorum of C_b holds chosen index >= F
    L->>M: GcRequest(b)
    M->>L: GcAck(w = b), durable
    Note over L: quorum of acks: floor effective; X retirable
    Op->>L: Inspect
    L->>Op: gc_watermark = b
    Op->>X: Retire(watermark b)
    Note over X: five legs hold: shut down
```

## 5. Recovery flow

- **A lost request or ack.** The leader re-sends; a lost message only stalls GC.
- **The leader is deposed before the quorum.** It never reports the floor effective, so nothing
  is retired. The next leader collects at its own ballot.
- **A new matchmaker generation.** The tally restarts for the new set
  (`Collector::reset_for_generation`); a request for another generation is refused.
- **A refused retirement.** The node answers why (`RetireRefusal`, for example
  leader, member, stale or not collected); the operator re-reads `Inspect` and asks again. The reason is only a diagnosis;
  the decision is `may_retire`'s.

## 6. Why it is safe

[MMP §4.5] maps the paper's three scenarios to the log's regions. paros discharges each for its
fence (the `node/gc.rs` module doc):

- **Above the fence (Region 3, Scenario 2).** Phase 1 found nothing accepted above `F` at a
  quorum of every configuration in `H_b`, so nothing was chosen there below `b`.
- **Recovered and gap-filled slots (Region 2, Scenario 1).** Every slot up to `F` is re-proposed
  or filled at `b` under `C_b`; once chosen, a Phase-1 quorum of `C_b` re-learns it.
- **The chosen prefix (Region 1).** paros's condition is "a Phase-2 quorum of `C_b` reports a
  chosen index at or past `F`". Any future Phase-1 quorum of `C_b` meets it, and a member that
  compacted past a slot refuses a `Prepare` below its floor.

A floor is effective only once a matchmaker quorum acked it, so every future matchmaking quorum
meets that quorum, and the **maximum** watermark filters every future `H` ([MMP §3.4]). [MMP
Appendix A] extends the safety proof to the collected configurations.

**GC never forgets the configuration in force.** The floor routinely rises over the last
reconfiguration's record. The matchmakers keep the fact as a durable scalar,
`MatchmakerHardState::effective`, that the watermark never touches (review finding P1).

**Retirement carries the evidence.** `may_retire(w)` holds only when all five legs hold:

1. the deployment names matchmakers;
2. the node is not a member of the configuration it believes in force;
3. it is not the leader;
4. `w` is strictly above `last_member_ballot`;
5. its belief is bound to exactly `w` (`acceptors_since == w`).

Leg 4 makes the other legs mean something; leg 5 makes leg 2 a fact: the belief is `C_w`, the
configuration the floor was computed over. The **operator's half** (#198): the watermark proves
every configuration below it forgotten, never that no configuration above it names the node. The
operator retires no node that a reconfiguration it asked for above the floor names.

## 7. Liveness

- GC needs a matchmaker quorum and a Phase-2 quorum of `C_b` caught up to `F`.
- Client commands go on during GC ([MMP §4.5]). GC adds no stall.
- The compaction floor and the GC watermark never wait on each other: the first is per node and
  says "these slots are chosen and gone here"; the second is per matchmaker and says "these
  configurations will never be returned again".

## 8. Costs

- One round trip to the matchmakers per leadership, off the critical path.
- [MMP §7.1]: old acceptors were collected within 5 ms, so the matchmakers returned one
  configuration; [MMP §4.5]: `H_b` almost always holds just the previous configuration.
- One configuration per matchmaker, kept for ever: the effective scalar.

## 9. In paros

| What | Where |
|---|---|
| The tally | `paros-core/src/collector.rs`: `Collector`, `covered`, `fold_ack`, `effective`, `GcStep` |
| The wiring and retirement | `paros-core/src/node/gc.rs`: `resend_gc`, `gc_effective`, `may_retire` |
| The matchmaker's watermark and effective scalar | `paros-core/src/matchmaker/state.rs`: `MatchmakerHardState` |
| The client | `paros-client/src/lib.rs`: `retire` |
| The node's answer | `paros-roles/src/node/operator.rs`: `retire_refusal` |
| The operator in a cell | `paros-coordinator/src/tenant/moves.rs`: step 3 of a move retires the old member with the watermark as evidence |
| The analysis | `docs/analysis/consensus/matchmaker-gc-and-generations.md` |

**Sim gates and oracles** (`paros-sim/src/audit/matchmaking.rs` unless named):

- `"gc: a GC request is licensed by a quorum holding the fence"`.
- `"gc: a floor is effective only once a quorum of the generation's matchmakers acked"`.
- `"gc: a node retires only after an effective floor named it retirable"`.
- `"gc: the effective configuration survives the watermark"` (`audit/matchmaker.rs`).
- The rare gates `"gc: a retired acceptor is genuinely shut down"` and `"gc: a retirement is
  withheld while a reconfiguration naming the node may be registered above its floor"`
  (`paros-sim-runner/src/gates.rs`).
- A driver site drops a `GcAck` reply (`paros-roles/src/node/reply.rs`).

**Divergences from the sources:**

- [MMP §3.5] Scenario 3 needs the chosen prefix on `f + 1` non-acceptor replicas. paros does not
  use it, even with a replica tier (#144): counting replicas would let a configuration be
  forgotten while a Phase-2 quorum of `C_b` has not learned the prefix. paros keeps the stronger
  rule.
- [MMP §3.4] sends `GarbageA` and shuts acceptors down. paros adds the operator's `Retire`, judged
  by the node against the evidence, because a node's belief is volatile.

## 10. Pitfalls

- **"Installed, therefore deletable."** The DPaxos rule overwrites a chosen value
  ([MMP Appendix D]). In paros it would be a `Noop` gap fill over a chosen value, two values for
  one slot (the `node/gc.rs` module doc). paros waits for `C_b` to hold the prefix.
- **Forgetting the configuration in force.** Without the effective scalar, an ordinary leader's GC
  erased the last reconfiguration's record, and a rebooted candidate was elected under the
  superseded configuration (review finding P1).
- **Retiring a current member.** A node can be a member of `C_w` without having heard it: a spare
  whose `Prepare` was lost, or a member that rebooted to its bootstrap belief. Without leg 5 it
  honored a `Retire`; the sweep's recipe `11365151955225522550 [(4114, 1006316943509197070)]` on
  `ee9d078` shut down node 5 of `C_9` (#165, commit `8cf2984`).
- **A reconfiguration above the floor.** A configuration that re-adds a node may be registered and
  not yet heard. Retiring that node loses a member for good; a grid's column never decides again
  (#198, commit `65ac850`).

## 11. Relatives

- [Matchmaking and Phase 1 over every configuration](matchmaking.md): the `H_b` GC keeps small.
- [Reconfiguration as a round change](reconfiguration.md): what makes old members.
- [Matchmaker generations and handover](matchmaker-generations.md): the watermark crosses a
  handover.
- [Election, P2c merge and gap fill](election.md): the fence and the gap fill GC relies on.
- [Replica tier](replica-tier.md): why acceptors stay learners.

## 12. Sources

- [MMP] Whittaker et al., *Matchmaker Paxos: A Reconfigurable Consensus Protocol*:
  `docs/references/papers/matchmaker-paxos/transcript.md` §3.4 (how), §3.5 (when), §4.5 (in
  MultiPaxos), §7.1, Appendix A (GC safety), Appendix D (the DPaxos bug).
- frankenpaxos: `docs/references/frankenpaxos/05-matchmaker-reconfiguration.md` §4 (the three
  scenarios).

[MMP]: ../../references/papers/matchmaker-paxos/transcript.md
[MMP §3.4]: ../../references/papers/matchmaker-paxos/transcript.md
[MMP §3.5]: ../../references/papers/matchmaker-paxos/transcript.md
[MMP §4.5]: ../../references/papers/matchmaker-paxos/transcript.md
[MMP §7.1]: ../../references/papers/matchmaker-paxos/transcript.md
[MMP Appendix A]: ../../references/papers/matchmaker-paxos/transcript.md
[MMP Appendix D]: ../../references/papers/matchmaker-paxos/transcript.md
