# 05 — Matchmaker reconfiguration: changing the acceptor set safely

> [!IMPORTANT]
> **This is NOT for paros v1.** v1 is a single-leader, **fixed-membership** Multi-Paxos: the
> acceptor set is hardcoded and never changes. This document is a *design horizon* — it
> records how to one day change the acceptor set (reconfiguration) without breaking safety,
> so the v1 core doesn't paint itself into a corner. Nothing here should be built until a
> fixed-membership core exists, is tested simulator-first, and a real need for membership
> change appears. Read it as "here's the shape of the trap, and the shape of the escape."

This is the reconfiguration counterpart to the algorithm parts (`02`, `03`). It analyzes
frankenpaxos's `matchmakerpaxos/` (single-decree) and `matchmakermultipaxos/` (SMR)
packages, the reference implementation of **Matchmaker Paxos** by the protocol's own author.
Everything below is grounded in the paper transcript at
[`../papers/matchmaker-paxos/transcript.md`](../papers/matchmaker-paxos/transcript.md);
section numbers like *(§3.5)* point there.

> **Line numbers** are from the checkout this was written against and will drift; symbol
> names are stable. Paths are relative to the frankenpaxos repo root
> (`shared/src/main/scala/frankenpaxos/…`).

As with the other parts, every section has three labelled pieces:

- **(a) What frankenpaxos does** — verbatim Scala with a `file:line` reference.
- **(b) The principle** — the language-neutral idea.
- **(c) Maps to sans-IO Multi-Paxos** — how it lands in a caller-driven Rust core, and which
  coupling to invert.

---

## Table of contents

1. [The problem — why naive reconfiguration is unsafe](#1-the-problem)
2. [Matchmaker = a per-round configuration registry](#2-matchmaker--a-per-round-configuration-registry)
3. [Safety = prior-quorum intersection](#3-safety--prior-quorum-intersection)
4. [Garbage collection — the three scenarios](#4-garbage-collection--the-three-scenarios)
5. [The Reconfigurer — reconfiguring the matchmakers themselves](#5-the-reconfigurer)
6. [Stall avoidance — overlapping rounds off the critical path](#6-stall-avoidance--overlapping-rounds)
7. [(c) Synthesis — reconfiguration as a pluggable trait](#7-synthesis)

---

## 1. The problem

### (a) What frankenpaxos does

There is no single line that *is* the problem — the problem is what every other line in
these packages exists to avoid. The paper states it directly (*§2.3*, Flexible Paxos): a
**configuration** is `C = (A; P1; P2)` — a set of acceptors `A`, a set of Phase 1 quorums
`P1`, and a set of Phase 2 quorums `P2`, with the invariant that **every P1 quorum
intersects every P2 quorum**. Paxos safety rests entirely on that intersection: Phase 1 of a
later round is guaranteed to *see* any value a Phase 2 of an earlier round may have chosen,
because their quorums share an acceptor.

Reconfiguration breaks the obvious version of this. If round `i` uses config `Cold` and you
simply switch round `i+1` to a disjoint `Cnew`, then a Phase 1 quorum of `Cnew` need *not*
intersect a Phase 2 quorum of `Cold`. The new leader can run Phase 1, see nothing, and
choose a *different* value than `Cold` already chose — two values chosen for one slot. The
paper's framing (*§3.1*): the proposer of round `i` must contact **all configurations used in
rounds less than `i`** in Phase 1. The whole protocol is machinery to discover *which* those
configs are and to keep contacting them until it is provably safe to stop.

### (b) The principle

A configuration change is safe only if it **preserves quorum intersection with every prior
config that could still hold a chosen-but-unlearned value.** You cannot just swap the
acceptor set: the new round's Phase 1 must still intersect the old rounds' Phase 2. So
reconfiguration reduces to two obligations — (1) *remember* every config ever used, and
(2) *re-read* the prior configs during recovery until you can prove a config is retired.
"Naive" reconfiguration fails because it forgets (1) and therefore can't honor the
intersection.

### (c) Maps to sans-IO Multi-Paxos

In a sans-IO core this is a statement about **state**, not I/O: a single fixed `QuorumSystem`
is a v1 simplification, and reconfiguration generalizes it to *a config per round, plus the
history of configs the recovery path must intersect with*. The danger is entirely capturable
in the deterministic simulator (part `01`): a property test that reconfigures mid-run and
asserts the single-value-per-slot invariant will catch a broken intersection instantly,
with no network. **v1 should make the quorum system a value carried in state from the start**
(even if there's only ever one), so that "config per round" is later an extension, not a
rewrite.

---

## 2. Matchmaker = a per-round configuration registry

### (a) What frankenpaxos does

A **matchmaker** is a tiny replicated map from round → configuration. The single-decree
version is the cleanest expression of the idea: a sorted map keyed by round, and one
handler.

```scala
// matchmakerpaxos/Matchmaker.scala:80
@JSExport
protected val acceptorGroups = mutable.SortedMap[Round, AcceptorGroup]()
```

```scala
// matchmakerpaxos/Matchmaker.scala:120
private def handleMatchRequest(
    src: Transport#Address,
    matchRequest: MatchRequest
): Unit = {
  val leader = chan[Leader[Transport]](src, Leader.serializer)
  // ...
  if (!acceptorGroups.isEmpty &&
      matchRequest.acceptorGroup.round <= acceptorGroups.lastKey) {
    // ... nack: a config for round <= lastKey was already registered ...
    leader.send(
      LeaderInbound()
        .withMatchmakerNack(MatchmakerNack(round = acceptorGroups.lastKey))
    )
    return
  }

  // Send back all previous acceptor groups and store the new acceptor group.
  leader.send(
    LeaderInbound().withMatchReply(
      MatchReply(
        round = matchRequest.acceptorGroup.round,
        matchmakerIndex = index,
        acceptorGroup = acceptorGroups.values.toSeq
      )
    )
  )
  acceptorGroups(matchRequest.acceptorGroup.round) =
    matchRequest.acceptorGroup
}
```

That is the entire registry contract: "query the past, update the present" in one message —
the reply carries *all previously registered configs*, and only then is the new config
stored. The leader starts a round by sending its chosen config and learns the prior ones in
the same round trip (`matchmakerpaxos/Leader.scala:201` builds the `MatchRequest`).

The MultiPaxos matchmaker is the same idea with three additions that the single-decree one
doesn't need: a **GC watermark**, an **epoch** (so the matchmakers themselves can be
reconfigured — see §5), and per-epoch state because a physical matchmaker plays several
logical roles over time. The log is documented inline:

```scala
// matchmakermultipaxos/Matchmaker.scala:127
@JSExportAll
case class Log(
    gcWatermark: Int,
    configurations: mutable.SortedMap[Round, Configuration]
)
```

```scala
// matchmakermultipaxos/Matchmaker.scala:142
@JSExportAll
case class Normal(
    gcWatermark: Int,
    configurations: mutable.SortedMap[Round, Configuration]
) extends MatchmakerState
```

The richer `handleMatchRequest` enforces both the GC floor and the monotonic round, then
replies with **only the prior configs below the requested round** and the watermark:

```scala
// matchmakermultipaxos/Matchmaker.scala:373
// Send back all previous configurations and store the (potentially new)
// acceptor group.
val matchReply = MatchReply(
  epoch = epoch,
  round = matchRequest.configuration.round,
  matchmakerIndex = index,
  gcWatermark = normal.gcWatermark,
  configuration = normal.configurations.values
    .takeWhile(_.round < matchRequest.configuration.round)
    .toSeq
)
// ...
normal.configurations(matchRequest.configuration.round) =
  matchRequest.configuration
```

### (b) The principle

Separate the *who-are-the-acceptors* decision from the *what-value-is-chosen* decision. A
matchmaker is a **source of truth for configurations, indexed by round** — not an acceptor.
An acceptor votes on values and lives on the hot path of every command; a matchmaker stores
configs and is touched **only on a round change** (leader change / reconfiguration), which is
rare. Because each round is statically owned by one proposer that picks one config,
matchmakers can never disagree about a round's config — the registry needs no consensus among
themselves for the normal case (*§3.2*). The reply is the join of "everything before me," so
a single round trip both *reads the past* and *writes the present*.

### (c) Maps to sans-IO Multi-Paxos

A matchmaker is not a new actor in a sans-IO design — it's a **separate handle the caller
drives**, distinct from the acceptor handle. Sketch:

```rust
/// Off the critical path: consulted only when the leader changes the round/config.
/// Pure: returns a reply describing prior configs; never does I/O.
trait MatchmakerCore {
    /// Register `config` for `round`, returning all configs registered for rounds < `round`
    /// (or a nack carrying the highest round already taken / the GC watermark).
    fn match_request(&mut self, round: Round, config: Configuration) -> MatchReply;
}
```

The acceptor core (part `03`) stays exactly as in v1 — votes on `(slot, ballot, value)`. The
registry is a `SortedMap<Round, Configuration>` plus a `gc_watermark: Round`. Crucially, in
the v1 fixed-membership core there *is* no matchmaker: the single config is a constant. This
trait is what you add when membership becomes a variable, and the acceptor core does not
change to accommodate it.

---

## 3. Safety = prior-quorum intersection

### (a) What frankenpaxos does

When the leader has collected `MatchReply`s from a quorum (`f+1`) of matchmakers, it
**unions all prior configs** they returned — this union is the paper's `Hi`. Every config in
the union must have its Phase 1 quorum contacted before the leader may propose:

```scala
// matchmakermultipaxos/Leader.scala:1097
val pendingRounds = mutable.Set[Round]()
val previousQuorumSystems =
  mutable.Map[Round, QuorumSystem[AcceptorIndex]]()
val acceptorIndices = mutable.Set[AcceptorIndex]()
val acceptorToRounds = mutable.Map[AcceptorIndex, mutable.Set[Round]]()

val gcWatermark = matchmaking.matchReplies.values.map(_.gcWatermark).max
for {
  reply <- matchmaking.matchReplies.values
  configuration <- reply.configuration
  if configuration.round >= gcWatermark
} {
  if (pendingRounds.contains(configuration.round)) {
    // Do nothing. We've already processed this configuration.
  } else {
    pendingRounds += configuration.round
    val quorumSystem = QuorumSystem.fromProto(configuration.quorumSystem)
    previousQuorumSystems(configuration.round) = quorumSystem
    acceptorIndices ++= quorumSystem.nodes()

    for (index <- quorumSystem.nodes) {
      acceptorToRounds
        .getOrElseUpdate(index, mutable.Set[Round]())
        .add(configuration.round)
    }
  }
}
```

If the union is empty there is nothing to intersect with — skip straight to Phase 2
(`Leader.scala:1131`). Otherwise, the leader **must** run Phase 1 against the read quorums of
*every* prior config before it can propose:

```scala
// matchmakermultipaxos/Leader.scala:1151
} else {
  // Send Phase1as to acceptors.
  val phase1a =
    Phase1a(round = matchmaking.round, chosenWatermark = chosenWatermark)
  for (index <- acceptorIndices) {
    acceptors(index).send(AcceptorInbound().withPhase1A(phase1a))
  }

  // Update our state.
  return Some(
    Left(
      Phase1(
        round = matchmaking.round,
        quorumSystem = matchmaking.quorumSystem,
        previousQuorumSystems = previousQuorumSystems.toMap,
        acceptorToRounds = acceptorToRounds.toMap,
        pendingRounds = pendingRounds,
        phase1bs = mutable.Map(),
        pendingClientRequests = matchmaking.pendingClientRequests,
        resendPhase1as =
          makeResendPhase1asTimer(phase1a, acceptorToRounds.keys.toSet)
      )
    )
  )
}
```

`processPhase1b` then waits for a *read quorum of every round in `pendingRounds`* (not just a
single quorum) before it can compute the safe values — `acceptorToRounds` is the index that
makes "have I heard from a read quorum of round `r`?" cheap (`Leader.scala:1194`).

### (b) The principle

The safety argument is the paper's invariant `P(i)` (*§3.3*) and it is an *intersection*
argument with a beautiful disjunction:

> Either the leader **learns** a prior config `Cj` during matchmaking (and then its Phase 1
> quorum of `Cj` blocks round `j` and intersects any Phase 2 quorum of `Cj`, so it sees any
> value `j` chose) — **or** the `f+1` matchmakers `Mi` it queried **never returned** `Cj`,
> which means they never processed round `j` and never will, and because any two `f+1`
> matchmaker sets intersect, no future matchmaker set will return `Cj` to a later round
> either.

Either way round `j` cannot choose a value the new leader would miss. The leader's only job
is to *honor* the disjunction: contact the Phase 1 quorum of **every** config the registry
hands back. This is why the union (not "the latest config") is what matters, and why a
quorum of matchmakers — not one — is required.

### (c) Maps to sans-IO Multi-Paxos

This generalizes the single recovery quorum that v1's Phase 1 already needs. In v1 a new
leader contacts one Phase 1 quorum of the one config. With reconfiguration the *output* of
matchmaking is a **set of prior `QuorumSystem`s**, and Phase 1 isn't done until the core has
a read quorum *for each*. Sketch of the state the core carries between Phase 1 messages:

```rust
struct Phase1 {
    round: Round,
    quorum_system: QuorumSystem,                       // config for *this* round
    previous_quorum_systems: HashMap<Round, QuorumSystem>, // Hi — all prior configs
    acceptor_to_rounds: HashMap<AcceptorIndex, HashSet<Round>>, // reverse index
    pending_rounds: HashSet<Round>,                    // rounds still missing a read quorum
    // ...
}
```

The completion predicate changes from "1 quorum" to "∀ r ∈ pending_rounds: a read quorum of
`previous_quorum_systems[r]` has replied." Everything else (value selection from the highest
`(ballot, value)` seen) is unchanged. The sans-IO core never sends a `Phase1a`; it *emits*
the set of acceptors to contact in its `Ready` and the caller fans out — the `acceptorIndices`
loop above becomes a list of outbound messages.

---

## 4. Garbage collection — the three scenarios

### (a) What frankenpaxos does

Remembering every config forever is unbounded, and old acceptors can never be shut down. GC
is what lets a config be *forgotten* — but only once it is provably safe, i.e. once a value
is chosen / persisted / replicated such that no future leader needs that config's Phase 1
quorum. The matchmaker side of GC is small: raise the watermark, drop the prefix, ack.

```scala
// matchmakermultipaxos/Matchmaker.scala:441
val gcWatermark = Math.max(normal.gcWatermark, garbageCollect.gcWatermark)
leader.send(
  LeaderInbound().withGarbageCollectAck(
    GarbageCollectAck(
      epoch = epoch,
      matchmakerIndex = index,
      gcWatermark = gcWatermark
    )
  )
)

// Garbage collect configurations.
val configurations = normal.configurations.dropWhile({
  case (round, _) => round < gcWatermark
})
matchmakerStates(epoch) = normal.copy(
  gcWatermark = gcWatermark,
  configurations = configurations
)
```

Once the watermark is raised, the matchmaker *refuses* (nacks) any `MatchRequest` for a round
below it (`Matchmaker.scala:340`) and reports the watermark in every reply — so the
intersection argument of §3 now applies to "rounds ≥ watermark." After `f+1` matchmakers ack
a watermark, those configs will never be returned again, and their acceptors can be retired.

The acceptor mirrors this with a **persisted watermark**: all slots below it are known
replicated on `f+1` replicas, so the acceptor may drop them and tell any inquiring leader
"already chosen" rather than re-voting.

```scala
// matchmakermultipaxos/Acceptor.scala:140
// This acceptor knows that all log entries less than persistedWatermark have
// been persisted on at least f+1 replicas. The acceptor is free to garbage
// collect all log entries less than persistedWatermark. If a leader contacts
// the acceptor about one of these log entries, the acceptor will inform the
// leader that the value was already chosen.
@JSExport
protected var persistedWatermark: Int = 0
```

```scala
// matchmakermultipaxos/Acceptor.scala:303
private def handlePersisted(
    src: Transport#Address,
    persisted: Persisted
): Unit = {
  // ... we don't ignore stale persisted requests ...
  persistedWatermark =
    Math.max(persistedWatermark, persisted.persistedWatermark)
  metrics.persistedWatermark.set(persistedWatermark)
  val leader = chan[Leader[Transport]](src, Leader.serializer)
  leader.send(
    LeaderInbound().withPersistedAck(
      PersistedAck(
        acceptorIndex = index,
        persistedWatermark = persistedWatermark
      )
    )
  )
}
```

The leader's GC state machine drives this in stages — `QueryingReplicas → PushingToAcceptors
→ WaitingForLargerChosenWatermark → GarbageCollecting → Done` — each a case of a
`GarbageCollection` trait (`Leader.scala:360`), with the comment block at `Leader.scala:346`
spelling out the exact order: query `f+1` replicas have executed up to `chosenWatermark`,
push that watermark to the acceptors, wait until `maxSlot` is chosen, then `GarbageCollect`
the matchmakers.

### (b) The principle

A config can be dropped only once **every slot it might hold has reached a safe terminal
state** — and the paper enumerates exactly three (*§3.5*, mapped to the MultiPaxos log regions
in *§4.5*):

- **Scenario 1** — the leader got a value *chosen* in this round (Region 2, after its commands
  are chosen). Future leaders re-learn it; lower rounds are redundant.
- **Scenario 2** — Phase 1 found *no value chosen* below this round (`k = -1`, Region 3).
  Nothing to preserve.
- **Scenario 3** — a value is chosen *and persisted on `f+1` non-acceptor machines* (replicas)
  (Region 1). After informing a Phase 2 quorum of the new config, future leaders contact the
  new config's Phase 1 quorum, learn it's chosen, and fetch the value from a replica — the old
  acceptors are no longer needed.

GC is therefore a **watermark protocol layered on intersection**: raise the floor on both the
matchmakers (which configs can still be returned) and the acceptors (which slots can still be
voted on), and only once `f+1` of each ack the new floor is it safe to retire hardware.

### (c) Maps to sans-IO Multi-Paxos

GC is pure bookkeeping over watermarks — a natural fit for a sans-IO core, because every
"safe to drop now" decision is a function of state the core already tracks (`chosen_watermark`,
prior-config set) and of acks the caller feeds back. Model it as a small explicit state
machine returned in `Ready` (e.g. `GcStep::QueryReplicas { up_to }`, `GcStep::PushPersisted
{ watermark, to }`, `GcStep::Forget { rounds }`) so the caller performs each round trip and
reports back. The acceptor's `persisted_watermark` and the matchmaker's `gc_watermark` are
two `Round`/`Slot` fields with monotone-max update — trivial to test in the simulator by
asserting a forgotten config is never contacted afterward. **For v1 this is entirely absent**:
fixed membership never retires anything.

---

## 5. The Reconfigurer

### (a) What frankenpaxos does

There's a recursion problem: the matchmakers are the source of truth, so *who reconfigures
the matchmakers?* The answer (*§5*) is **Paxos-over-Paxos** — the old matchmaker set doubles
as a set of Paxos acceptors, and the new matchmaker set is *chosen* via consensus on the old
set, so two concurrent reconfigurations to disjoint sets cannot both win. A **Reconfigurer**
runs that meta-consensus as an explicit four-state machine:

```scala
// matchmakermultipaxos/Reconfigurer.scala:120
@JSExportAll
case class Idle(
    configuration: MatchmakerConfiguration
) extends State

@JSExportAll
case class Stopping(
    configuration: MatchmakerConfiguration,
    newConfiguration: MatchmakerConfiguration,
    stopAcks: mutable.Map[MatchmakerIndex, StopAck],
    resendStops: Transport#Timer
) extends State

@JSExportAll
case class Bootstrapping(
    configuration: MatchmakerConfiguration,
    newConfiguration: MatchmakerConfiguration,
    bootstrapAcks: mutable.Map[MatchmakerIndex, BootstrapAck],
    resendBootstraps: Transport#Timer
) extends State

@JSExportAll
case class Phase1(
    configuration: MatchmakerConfiguration,
    newConfiguration: MatchmakerConfiguration,
    round: Int,
    matchPhase1bs: mutable.Map[MatchmakerIndex, MatchPhase1b],
    resendMatchPhase1as: Transport#Timer
) extends State

@JSExportAll
case class Phase2(
    configuration: MatchmakerConfiguration,
    newConfiguration: MatchmakerConfiguration,
    round: Int,
    matchPhase2bs: mutable.Map[MatchmakerIndex, MatchPhase2b],
    resendMatchPhase2as: Transport#Timer
) extends State
```

The flow is **Stop old → Bootstrap new → Phase 1 → Phase 2**:

1. **Stop.** Send `Stop` to `Mold`; each matchmaker transitions to `HasStopped` and replies
   `StopAck(gcWatermark, configurations)` (`Matchmaker.scala:462`). On `f+1` acks, the
   Reconfigurer **unions the logs and trims below the max watermark** — the new set's initial
   state:

   ```scala
   // matchmakermultipaxos/Reconfigurer.scala:425
   // Union logs, trim garbage, and send to new matchmakers.
   val gcWatermark = stopping.stopAcks.values.map(_.gcWatermark).max
   val configurations =
     stopping.stopAcks.values
       .flatMap(_.configuration)
       .toSet
       .filter(_.round >= gcWatermark)
       .toSeq
   val bootstrap = Bootstrap(
     epoch = stopping.newConfiguration.epoch,
     reconfigurerIndex = index,
     gcWatermark = gcWatermark,
     configuration = configurations
   )
   ```

2. **Bootstrap.** Send that `Bootstrap` to `Mnew`; each new matchmaker stores it as a
   `Pending` log (`Matchmaker.scala:482`) and acks. Wait for **all** `2f+1`
   (`Reconfigurer.scala:486`) — the new set must be fully initialized before it can be chosen.

3. **Phase 1 / Phase 2.** Now run ordinary Paxos *over the old matchmakers as acceptors* to
   choose `newConfiguration` as the value: `MatchPhase1a`/`MatchPhase1b` then
   `MatchPhase2a`/`MatchPhase2b` (`Matchmaker.scala:527` and `:581`). Value selection is the
   classic Paxos rule — adopt the highest-round vote, else propose your own
   (`Reconfigurer.scala:553`):

   ```scala
   // matchmakermultipaxos/Reconfigurer.scala:553
   val votes = phase1.matchPhase1bs.values.flatMap(_.vote)
   val value = if (votes.isEmpty) {
     phase1.newConfiguration
   } else {
     votes.maxBy(_.voteRound).voteValue
   }
   ```

   On a Phase 2 quorum, the new configuration is **chosen**; the Reconfigurer broadcasts
   `MatchChosen` to leaders, other reconfigurers, and the new matchmakers, and returns to
   `Idle` on the new config (`Reconfigurer.scala:618`).

### (b) The principle

You cannot bootstrap trust from nothing: the registry's own membership is itself a consensus
decision, so reconfiguring the matchmakers is **meta-reconfiguration via consensus on the old
matchmaker set**. The stop-the-world approach is acceptable *here specifically* because
matchmakers are idle whenever there is a stable leader (*§5*) — reconfiguring them must be
*safe* but need not be *efficient*. Two safeguards make it safe: (1) the old set is frozen
(`HasStopped`) before anything new starts, so its logs are immutable when unioned; (2) the new
set is *chosen* by Paxos over the old set, so disjoint concurrent reconfigurations cannot both
commit. Each new matchmaker set is initialized from a quorum of the previous one, so configs
and GC watermarks propagate forward (the safety proof, *§Appendix B*).

### (c) Maps to sans-IO Multi-Paxos

This is "Paxos inside Paxos," and in a sans-IO design it is **literally a second instance of
your single-decree Paxos core** (part `02`) whose acceptors are the old matchmakers and whose
chosen value is the new matchmaker set. So you don't write a new consensus algorithm — you
reuse the kernel. Model the Reconfigurer as its own caller-driven handle with the four-state
enum verbatim:

```rust
enum ReconfigurerState {
    Idle { config: MatchmakerConfig },
    Stopping { config, new_config, stop_acks: HashMap<MatchmakerIndex, StopAck> },
    Bootstrapping { config, new_config, bootstrap_acks: HashMap<MatchmakerIndex, ()> },
    Phase1 { config, new_config, round: Round, votes: HashMap<MatchmakerIndex, Vote> },
    Phase2 { config, new_config, round: Round, acks: HashMap<MatchmakerIndex, ()> },
}
```

It emits messages and consumes acks through the same `step`/`Ready` boundary as everything
else — no I/O in the core. The epoch field threaded through every matchmaker message
(`matchmakerConfiguration.epoch`) is the fence that makes stale-set messages safely ignorable;
a sans-IO core gets this for free by tagging messages with the epoch and dropping mismatches.
**This is the deepest part of the design horizon — do not attempt it before the base core and
the acceptor-set reconfiguration of §2–§4 exist and are simulator-tested.**

---

## 6. Stall avoidance — overlapping rounds

### (a) What frankenpaxos does

The naive reconfiguration would stop the world: finish round `i`, run matchmaking + Phase 1
for round `i+1`, *then* resume taking commands. The paper's optimization (*§4.4*) is to run
rounds `i` and `i+1` **at the same time** — the leader stays in Phase 2 of round `i` for
in-flight commands while it proactively matchmakes and Phase-1s round `i+1`. frankenpaxos
encodes this overlap directly in the leader's state types:

```scala
// matchmakermultipaxos/Leader.scala:446
// When a leader performs a reconfiguration in round i, it operates in both
// rounds i and i+1 at the same time. It is in Phase 2 in round i, but
// proceeds through the Matchmaking phase, Phase 1, and Phase 2 in round i+1.
// Phase2Matchmaking, Phase212, and Phase22 encompass this transition period.
//
// With Phase2Matchmaking, the leader is in Phase 2 in round i and in the
// Matchmaking phase in round i + 1.
@JSExportAll
case class Phase2Matchmaking(
    phase2: Phase2,
    matchmaking: Matchmaking,
    startNanos: Long
) extends State
```

Reconfiguration is *triggered* by `becomeIIPlusOneLeader` — instead of abandoning the current
Phase 2, the leader starts matchmaking round `i+1` *while keeping* the round-`i` `Phase2`:

```scala
// matchmakermultipaxos/Leader.scala:986
case phase2: Phase2 =>
  if (roundSystem.leader(phase2.round + 1) == index) {
    // ...
    val matchmaking = startMatchmaking(
      round = phase2.round + 1,
      pendingClientRequests = mutable.Buffer(),
      qs,
      qsp
    )
    stopTimers(state)
    state = Phase2Matchmaking(phase2, matchmaking, System.nanoTime)
  } else {
    // ... fall back to a full leader change ...
```

When matchmaking finishes, the leader transitions to `Phase212` (Phase 2 in round `i`,
*both* Phase 1 and Phase 2 in round `i+1` simultaneously). The key to no stall is that the
old Phase 2's pending GC is cancelled rather than blocked on, and new commands are routed to
fresh slots in `i+1`:

```scala
// matchmakermultipaxos/Leader.scala:1503
// Next, we transition to Phase 1 and Phase 2.
state = Phase212(
  oldPhase2 = phase2Matchmaking.phase2.copy(gc = Cancelled),
  newPhase1 = newPhase1,
  newPhase2 = newPhase2,
  startNanos = startNanos
)
```

The `Cancelled` GC case exists precisely for this overlap:

```scala
// matchmakermultipaxos/Leader.scala:411
// During an i/i+1 configuration, we cancel any pending garbage collection
// being performed during round i. This is not necessary for correctness, but
// simplifies the implementation. ...
@JSExportAll
case object Cancelled extends GarbageCollection
```

The acceptor side has a matching subtlety: during an `i/i+1` overlap an acceptor may receive a
round-`i+1` `Phase2a` *before* the round-`i+1` `Phase1a`, so on Phase 1 it must **not** return
votes it cast in the current round (`Acceptor.scala:228`, the `state.voteRound < round`
guard) — otherwise it would feed the leader values the leader itself just proposed.

### (b) The principle

Don't stop-the-world for a reconfiguration: **overlap the old and new rounds** so command
processing never pauses. Matchmaking and Phase 1 of the new config run *proactively, off the
critical path* (exactly as a stable leader proactively runs Phase 1 before any client request,
*§3.6*). Commands that arrive during matchmaking are chosen by the old config in round `i`;
commands that arrive after are chosen by the new config in round `i+1`; **none is delayed**
(*§4.4*). The enabler is *Phase 1 bypassing* — once matchmaking establishes that all slots
above some `k` are empty, those slots satisfy the Phase 1 preconditions and the leader goes
straight to Phase 2 with the new config. That is what gives Matchmaker MultiPaxos "no α to
tune" and < ~4% reconfiguration overhead.

### (c) Maps to sans-IO Multi-Paxos

The lesson for the core's *state shape*: a leader's state must be able to represent **two
active rounds at once** during a reconfiguration, not a single `(round, phase)`. Model it as
an explicit transitional variant rather than mutating one round in place:

```rust
enum LeaderState {
    // ... steady-state variants (Phase1, Phase2) ...
    Phase2Matchmaking { old: Phase2, matchmaking: Matchmaking },
    Phase212        { old: Phase2, new_phase1: Phase1, new_phase2: Phase2 },
    Phase22         { old: Phase2, new_phase2: Phase2 },
}
```

Because the core does no I/O and time is caller-driven (part `01`/`03`), "proactive,
off-critical-path matchmaking" is just: the caller decides *when* to call
`reconfigure(new_config)`, and the core emits matchmaking messages in `Ready` while continuing
to accept and emit round-`i` Phase 2 traffic in the same `Ready`. The simulator can drive a
reconfiguration concurrently with a command stream and assert zero stalled commands plus the
single-value-per-slot invariant. Carry the `voteRound < round` guard over verbatim — it is a
genuine correctness condition for overlapping rounds, not a performance hack.

---

## 7. Synthesis

Reconfiguration touches five concerns, and the sans-IO discipline keeps each a **separate,
caller-driven, zero-I/O piece** rather than a rewrite of the v1 core:

| Concern | v1 (fixed membership) | With reconfiguration |
|---|---|---|
| Quorum system | one constant `QuorumSystem` | a `QuorumSystem` carried **per round** in leader/acceptor state |
| Recovery (Phase 1) | one read quorum of the one config | a read quorum of **every prior config** in `Hi` (§3) |
| Config registry | none | a `MatchmakerCore` handle: round → config map (§2) |
| Retiring old hardware | never | GC watermarks on matchmakers + acceptors, three scenarios (§4) |
| Changing the registry itself | n/a | a `Reconfigurer` running Paxos-over-Paxos (§5) |
| Avoiding stalls | n/a | overlapping `i`/`i+1` leader states (§6) |

The unifying idea: **reconfiguration is a pluggable layer over the sans-IO core, not a
property of it.** Concretely —

- Make the **`QuorumSystem` a value in state from day one**, even when there is exactly one and
  it never changes. This is the single decision that keeps v1 from painting itself into a
  corner — "config per round" then becomes data, not a refactor.
- Express the new collaborators as **separate handle traits** driven through the same
  `step`/`Ready` boundary as the acceptor/leader cores: `MatchmakerCore` (§2) and a
  `Reconfigurer` (§5). None of them does I/O; the caller fans out their emitted messages and
  feeds back acks, exactly as for the base protocol.
- Keep the **safety obligation explicit in the type that drives Phase 1**: a `previous_quorum_systems`
  map and a `pending_rounds` set whose emptiness is the completion gate (§3). The intersection
  argument lives in that one predicate.

> **Closing note for paros v1.** Build a single-leader, **fixed-membership** Multi-Paxos with a
> hardcoded acceptor set and a single `QuorumSystem` (carried in state, but constant). Do not
> implement matchmakers, GC, the Reconfigurer, or overlapping rounds. Add them later, in the
> order §2 → §3 → §4 → §6 → §5 (registry and prior-quorum intersection first; the
> matchmaker-of-matchmakers last), each developed and verified simulator-first against the
> single-value-per-slot invariant.
