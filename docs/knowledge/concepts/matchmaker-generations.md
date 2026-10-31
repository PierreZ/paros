# Matchmaker generations and handover

Group: Reconfiguration. Status: verified 2026-10-31.

Matchmakers also fail and move, so the matchmaker set must change too. Matchmakers are idle while
a leader is stable, so their reconfiguration must be safe but need not be fast ([MMP §5]). paros
numbers each set as a **generation** `g`. A handover **stops** a quorum of `M_g`, **bootstraps**
the successor with the union of their registries above the maximum watermark, has `M_g`
**choose** the successor by single-decree Paxos, and **publishes** the chosen set. A stopped
matchmaker points late callers at its successor.

## 1. Problem

Two failures must be impossible: two matchmaker sets that both act as generation `g + 1` (a
candidate could matchmake at one and miss a registration at the other), and a successor that lacks
a registration a candidate completed at `g` (a configuration `H_b` must cover would be missing).
And the matchmakers of `g` must not keep registering after the successor took their state.

## 2. Model

- `M_g` is `2f + 1` matchmakers; matchmaker quorums and the decree's quorums are **majorities
  only** (root `AGENTS.md`).
- Matchmakers crash and restart from their disk. A wiped matchmaker never rejoins: a handover
  replaces it.
- Any node may drive a handover, and two may run at once with different targets.

## 3. Roles and state

| Role | State | Durable? |
|---|---|---|
| Matchmaker (`generation.rs`) | its generation and phase (active, `Stopped`), a pending bootstrap, its decree acceptor record, the chosen successor once learned | yes |
| Reconfigurer (`MatchmakerReconfigurer`) | the phase (`Idle`, `Stopping`, `Bootstrapping`, `Deciding`, `Publishing`), the stop answers, the reconstruction, the decree's proposer | no |
| Node | the matchmaker set it believes in force, per tenant | learned from the wire |

## 4. Normal flow

1. **Stop.** The reconfigurer sends `Stop` to every member of `M_g` and waits for a quorum of
   answers. A stopped matchmaker freezes: it never registers again, and answers its registry, its
   watermark and its decree promise.
2. **Bootstrap.** It reconstructs: `w` = the maximum watermark, `H` = the union of the histories
   at or above `w`. It sends `Bootstrap { g + 1, target, w, H }` to **every** member of the target
   and waits for all of them: a set is chosen only once fully initialized.
3. **Decide.** It runs single-decree Paxos with `M_g` as acceptors (`DecreePrepare`,
   `DecreeAccept`), at a ballot strictly above the stop quorum's maximum decree promise. If a
   competing proposal was already voted, P2c makes it propose that one.
4. **Publish.** It sends `Chosen { g, successor }` to `M_g ∪ successor`, until a quorum of each
   learned it. A successor member activates its pending bootstrap; an old member records the link
   and stays frozen.

```mermaid
sequenceDiagram
    participant R as Reconfigurer
    participant Old as M_g (a quorum)
    participant New as M_g+1 (all members)
    R->>Old: Stop
    Old->>R: Stopped(registry, w, decree promise)
    Note over R: w = max; H = union above w
    R->>New: Bootstrap(g+1, target, w, H)
    New->>R: ack (pending bootstrap durable)
    R->>Old: DecreePrepare / DecreeAccept(target)
    Old->>R: majority voted
    R->>Old: Chosen(g, successor)
    R->>New: Chosen(g, successor)
    Note over New: activate generation g+1
```

## 5. Recovery flow

- **The reconfigurer dies.** It holds no durable state. A later attempt, by this node or another,
  re-runs the steps: `Stop` is idempotent, the bootstrap is keyed by the proposed set, and the
  decree's votes are durable at `M_g`. The retry adopts what was chosen or completes what was
  started.
- **A node meets a frozen generation.** `Stopped { successor: None }` means a handover stalled
  mid-way. Any node that meets it finishes the handover (`MatchmakerReconfigurer::finish`), with
  the members that answered its freeze as the target.
- **A straggler.** A member that never heard `Chosen` is told again by any node that meets it.
- **A stalled handover.** After a stall budget the driver abandons it (`abandon`); a node that
  meets the frozen set finishes it.
- **A proposer at the old generation.** A frozen matchmaker refuses its registration and names the
  successor once one is chosen; the node adopts only a chosen set.

## 6. Why it is safe

[MMP Appendix B] extends the safety proof across matchmaker sets:

- **No lost registration.** Every completed registration of `g` reached a quorum of `M_g`, and so
  meets the stop quorum. The stopped registries are immutable when unioned, so the reconstruction
  carries it into `g + 1`.
- **One successor.** The decree serializes competing reconfigurers: the loser's Phase 1 sees the
  winner's vote and proposes it. `Chosen` is sent only from `Publishing`, entered only on a
  Phase-2 quorum, so the published successor is always the chosen set, never a proposal.
- **No ballot reuse.** A rebooted reconfigurer must never reuse a decree ballot of its earlier
  incarnation; the decree opens strictly above every stopped member's promise.
- **The fact survives.** The effective configuration and the watermark are carried into every
  successor generation ([configuration GC](configuration-gc.md)).

Every matchmaking message is fenced by generation, so a candidate can never mix answers of two
generations in one quorum.

## 7. Liveness

- A handover needs a quorum of `M_g` and **every** member of the target up.
- A frozen matchmaker refuses registrations until the successor is chosen, so campaigns wait
  during the handover; a stable leader does not notice ([MMP §7.3]).
- The handover reuses the shared `Proposer` and `Acceptor` over a one-slot log (`Decree`): there
  is no second Paxos kernel.

## 8. Costs

- Five round trips: stop, bootstrap, the decree's two phases, publish.
- [MMP §7.3]: reconfiguring the matchmakers had no effect on latency or throughput.
- A target member must store the whole reconstruction before the decree.

## 9. In paros

| What | Where |
|---|---|
| The matchmaker side | `paros-core/src/matchmaker/generation.rs`; `matchmaker/state.rs` (the successor link, the pending bootstrap) |
| The reconfigurer | `paros-core/src/matchmaker/reconfigurer.rs`: `MatchmakerReconfigurer`, `ReconfigurerPhase`, `start`, `finish`, `abandon` |
| The decree | `paros-core/src/decree.rs`: `Decree<Id, V>` |
| The model checker | `paros-core/src/models/handover.rs` |
| The driver | `paros-roles/src/node/operator.rs`: `reconfigure_matchmakers`; `paros-roles/src/node/shared.rs` (the stall budget) |
| Who asks in a cell | `paros-coordinator/src/tenant/sets.rs`: places each tenant's set and moves it off a draining machine (#501) |

**Sim gates and oracles:**

- `"generation: at most one matchmaker set is authoritative per generation"`, `"generation: a
  reconstruction is the union of the frozen quorum above its maximum watermark"`, `"generation: a
  frozen generation never thaws"` (`audit/matchmaker.rs`).
- `"generation: a chosen set was voted by a majority of its old generation at one ballot"`,
  `"generation: a matchmaker-set handover completes"`, `"generation: a node finishes a handover
  whose reconfigurer died"` (`audit/matchmaking.rs`).
- Driver sites (`paros-roles/src/node/draws.rs`, `shared.rs`): abandon a handover while stopping,
  bootstrapping or deciding; `"generation: a freeze closes on its first quorum"`; `"generation: the driver skips a
  due handover re-send"`.

**Divergences from the sources:**

- [MMP §5] persists the successor in a name service such as DNS. paros keeps the link at the
  frozen matchmakers, and a node learns a set only from its matchmaker pool.
- One matchmaker set per tenant keeps one registry per journal (#190); the model checker asserts
  each journal's reconstruction separately.
- The handover's decree is also what `cell init` uses to choose its plan (#277).

## 10. Pitfalls

- **Decree ballot reuse.** A fresh reconfigurer reused a ballot its earlier incarnation may have
  had a value accepted at: two values at one ballot (the handover model checker, seed 103).
- **Publishing a proposal.** A successor published before it was chosen could leave two sets
  acting as `g + 1`. `Chosen` leaves only from `Publishing`.
- **A set chosen half-initialized.** A member that never got the bootstrap would answer with an
  empty registry. The reconfigurer waits for every target member before the decree.

## 11. Relatives

- [Matchmaking and Phase 1 over every configuration](matchmaking.md): what the registries serve.
- [Configuration GC and retirement](configuration-gc.md): the watermark a handover carries.
- [Single-decree Paxos](single-decree-paxos.md): the decree.
- [Reconfiguration as a round change](reconfiguration.md): the acceptors' side.

## 12. Sources

- [MMP] Whittaker et al., *Matchmaker Paxos: A Reconfigurable Consensus Protocol*:
  `docs/references/papers/matchmaker-paxos/transcript.md` §5 (reconfiguring matchmakers), §7.3,
  Appendix B (safety across matchmaker sets).
- frankenpaxos: `docs/references/frankenpaxos/05-matchmaker-reconfiguration.md` §5 (the
  `Reconfigurer`).

[MMP]: ../../references/papers/matchmaker-paxos/transcript.md
[MMP §5]: ../../references/papers/matchmaker-paxos/transcript.md
[MMP §7.3]: ../../references/papers/matchmaker-paxos/transcript.md
[MMP Appendix B]: ../../references/papers/matchmaker-paxos/transcript.md
