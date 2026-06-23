# Canonical single-decree Paxos in `frankenpaxos`

The algorithmic kernel. A single-decree Paxos instance chooses **one** value, once,
and never un-chooses it. Everything in Multi-Paxos (next file) is *this*, replicated
per slot: one independent single-decree instance per log index, sharing leader and
round machinery. Master this file and Multi-Paxos is bookkeeping.

This document studies the `frankenpaxos.paxos` package — Dan Ports / Michael Whittaker's
teaching implementation of textbook Paxos in Scala. It is an actor-model codebase: every
node is an `Actor` with a single `receive(src, inbound)` dispatch, and side effects happen
inline via `chan.send(...)`. Our job is to extract the *algorithm* from that I/O coupling
and re-state it for a **sans-IO** Rust core that does zero I/O — one `step(event)` entry
point, side effects returned in a `Ready`-style struct, persistence and transport owned by
the caller. (See `analysis/go-raft/etcd-raft-sans-io-patterns.md` for the sans-IO contract
itself; this file is about *what algorithm flows through it*.)

*Line numbers are from the checkout this was written against and will drift; symbol names
are stable.*

Every section has three parts:

- **(a) What frankenpaxos does** — a short verbatim Scala snippet, with a `file:line` comment.
- **(b) The principle** — the language-neutral rule.
- **(c) Maps to sans-IO Multi-Paxos** — how it lands in a pure Rust core.

---

## Table of contents

1. [Acceptor — the safety-keeping replica](#1-acceptor)
2. [Leader (proposer) — driving the two phases](#2-leader-proposer)
3. [Client — propose and resend](#3-client)
4. [Wire messages — two phases × (request, response)](#4-wire-messages)
5. [RoundSystem — round→leader assignment as pure logic](#5-roundsystem)
6. [Leader-selection auxiliaries — orthogonal soft state](#6-leader-selection-auxiliaries)

---

## 1. Acceptor

The acceptor is the keeper of safety. It holds three numbers and obeys two rules. Those
two rules — the *promise* rule (Phase 1) and the *vote* rule (Phase 2) — are the entirety
of why Paxos is correct. Everything else is liveness and engineering.

### (a) What frankenpaxos does

The acceptor's complete persistent state is three fields:

```scala
// shared/src/main/scala/frankenpaxos/paxos/Acceptor.scala:36
// The largest round in which this acceptor has received a message.
var round: Int = -1;

// The largest round in which this acceptor has voted.
var voteRound: Int = -1;

// The value that this acceptor voted for in voteRound, or None if the
// acceptor hasn't voted yet.
var voteValue: Option[String] = None;
```

**Phase 1a (the promise rule).** On a prepare for round `r`, reject anything not strictly
greater than the highest round seen; otherwise adopt `r` and report back what (if anything)
was previously voted:

```scala
// shared/src/main/scala/frankenpaxos/paxos/Acceptor.scala:60
private def handlePhase1a(src: Transport#Address, phase1a: Phase1a): Unit = {
  // Ignore messages from previous rounds.
  if (phase1a.round <= round) {
    // ... log and return ...
    return
  }

  // Bump our round and send the leader our vote round and vote value.
  round = phase1a.round
  val leader = chan[Leader[Transport]](src, Leader.serializer)
  leader.send(
    LeaderInbound().withPhase1B(
      Phase1b(
        round = round,
        acceptorId = index,
        voteRound = voteRound
      ).update(_.optionalVoteValue := voteValue)
    )
  )
}
```

**Phase 2a (the vote rule).** Accept (vote for) a value in round `r` only if the acceptor
has not promised to a *higher* round, and has not already voted in `r`:

```scala
// shared/src/main/scala/frankenpaxos/paxos/Acceptor.scala:84
private def handlePhase2a(src: Transport#Address, phase2a: Phase2a): Unit = {
  // Ignore messages from smaller rounds.
  if (phase2a.round < round) {
    // ... log and return ...
    return
  }

  // Ignore messages from our current round if we've already voted.
  if (phase2a.round == round && phase2a.round == voteRound) {
    // ... log and return ...
    return
  }

  // Update our state and send back an ack to the leader.
  logger.checkGe(phase2a.round, round)
  round = phase2a.round
  voteRound = phase2a.round
  voteValue = Some(phase2a.value)

  val leader = chan[Leader[Transport]](src, Leader.serializer)
  leader.send(
    LeaderInbound().withPhase2B(Phase2b(acceptorId = index, round = round))
  )
}
```

### (b) The principle

An acceptor is a tiny, monotone state machine over `(round, voteRound, voteValue)`. It
makes exactly two promises: *(1)* once it has seen round `r`, it will never engage with a
round `< r` (promise rule), and *(2)* it votes at most once per round, and only if no
higher round has intervened (vote rule). The Phase 1b reply carries the acceptor's last
vote so the proposer can recover any value that might already have been chosen. `round`
only ever increases; `voteRound` only ever increases. These two monotonicity invariants
are the safety of Paxos — nothing else in the system is allowed to violate them.

### (c) Maps to sans-IO Multi-Paxos

The acceptor is a **pure state machine**; persistence is the caller's job. In the Rust core:

```rust
struct AcceptorSlot {
    round: i64,            // highest round seen (Paxos "rnd"/"promise")
    vote_round: i64,       // round of last vote   (Paxos "vrnd")
    vote_value: Option<Value>,
}

// One entry point; no I/O, no send().
match event {
    Event::Phase1a { round, from } => {
        if round <= self.round { return; }          // promise rule
        self.round = round;
        // safety: round changed -> caller MUST fsync before the reply leaves
        ready.persist = true;
        ready.messages.push(Out::Phase1b { round, vote_round: self.vote_round,
                                           vote_value: self.vote_value.clone(), to: from });
    }
    Event::Phase2a { round, value, from } => {
        if round < self.round { return; }            // vote rule (stale)
        if round == self.round && round == self.vote_round { return; } // already voted
        self.round = round;
        self.vote_round = round;
        self.vote_value = Some(value);
        ready.persist = true;                         // MUST fsync before ack
        ready.messages.push(Out::Phase2b { round, to: from });
    }
    _ => {}
}
```

Two things the sans-IO boundary forces us to make explicit that the actor code leaves
implicit:

- **Durability ordering.** frankenpaxos `send`s immediately; correctness silently assumes
  the field write happened first. In the core we surface this as a `persist` obligation in
  `Ready`: the caller must make `round`/`voteRound`/`voteValue` durable **before** any
  reply hits the wire. Send-before-fsync would break Paxos.
- **Per-slot replication.** In Multi-Paxos each `Phase1a`/`Phase2a` is scoped to a slot
  index, so the core holds a `Vec<AcceptorSlot>` (or a map), and Phase 1a is generalized
  to return the votes for a *range* of slots at once (see next file).

---

## 2. Leader (proposer)

The leader runs the two phases and contains the single most important rule in Paxos: how
to **choose which value to propose** in Phase 2. Get that rule wrong and you can choose two
different values for one instance — total failure.

### (a) What frankenpaxos does

The leader's state: a round, a status, the response sets, and the value in flight:

```scala
// shared/src/main/scala/frankenpaxos/paxos/Leader.scala:51
// The leader's round number. With n leaders, leader i uses round
// numbers i, i + n, i + 2n, i + 3n, etc.
@JSExport
protected var round: Int = -1

// The current status of the leader. A leader is either idle, running
// phase 1, running phase 2, or has learned that a value is chosen.
object Status extends Enumeration {
  type Status = Value
  val Idle, Phase1, Phase2, Chosen = Value
}
import Status._
@JSExport
protected var status: Status = Idle

// The value currently being proposed in round `round`.
@JSExport
protected var proposedValue: Option[String] = None

// The set of phase 1b and phase 2b responses from the current round.
@JSExport
protected var phase1bResponses = mutable.Set[Phase1b]()
@JSExport
protected var phase2bResponses = mutable.Set[Phase2b]()

// The chosen value.
var chosenValue: Option[String] = None
```

One `receive` routes every inbound message by type:

```scala
// shared/src/main/scala/frankenpaxos/paxos/Leader.scala:79
override def receive(
    src: Transport#Address,
    inbound: LeaderInbound
): Unit = {
  import LeaderInbound.Request
  inbound.request match {
    case Request.ProposeRequest(r) => handleProposeRequest(src, r)
    case Request.Phase1B(r)        => handlePhase1b(src, r)
    case Request.Phase2B(r)        => handlePhase2b(src, r)
    case Request.Empty => {
      logger.fatal("Empty LeaderInbound encountered.")
    }
  }
}
```

A propose starts a fresh, *strictly larger* round (the `i, i+n, i+2n, …` spacing makes
rounds globally unique per leader) and broadcasts Phase 1a; Phase 1b responses accumulate
until a quorum (`f + 1`) is reached.

**The value-selection rule — the safety heart of Paxos.** On a Phase 1 quorum, the leader
does *not* get to propose its own value freely. It must adopt the value voted in the
highest `voteRound` reported by the quorum; only if *no one* in the quorum has ever voted
(`k == -1`) is it free to use its own proposal:

```scala
// shared/src/main/scala/frankenpaxos/paxos/Leader.scala:168
// Select the largest vote round k, and the corresponding vote value v. If
// we decide not to go with our initially proposed value, make sure not to
// forget to update the proposed value.
val k = phase1bResponses.maxBy(_.voteRound).voteRound
val v = {
  if (k == -1) {
    logger.check(proposedValue.isDefined)
    proposedValue.get
  } else {
    val vs =
      phase1bResponses.filter(_.voteRound == k).map(_.voteValue.get)
    logger.checkEq(vs.size, 1)
    vs.iterator.next()
  }
}
proposedValue = Some(v)

// Start phase 2.
for (acceptor <- acceptors) {
  acceptor.send(
    AcceptorInbound().withPhase2A(Phase2a(round = round, value = v))
  )
}
status = Phase2
```

A Phase 2b quorum means the value is chosen; the leader records it, asserts it never
contradicts a previously chosen value, and replies to waiting clients:

```scala
// shared/src/main/scala/frankenpaxos/paxos/Leader.scala:222
// At this point, the proposed value is chosen.
logger.check(proposedValue.isDefined)
val chosen = proposedValue.get

// Make sure the chosen value is the same as any previously chosen value.
chosenValue match {
  case Some(oldChosen) => {
    logger.checkEq(oldChosen, chosen)
  }
  case None => {}
}
chosenValue = Some(chosen)
status = Chosen
```

### (b) The principle

A proposer drives two round-trips: **Phase 1** (prepare/promise) reserves a round and
*discovers* any value that may already be chosen; **Phase 2** (accept/accepted) gets that
value voted by a quorum, which chooses it. The crux is the recovery rule: among the `f+1`
promises, the value attached to the *highest* `voteRound` is the only value the proposer is
permitted to push — because that value might already have reached a quorum in an earlier
round and thus might already be chosen. Free choice is allowed only when the quorum proves
nobody has ever voted. Two intersecting quorums + "adopt the highest prior vote" =
at most one value is ever chosen.

### (c) Maps to sans-IO Multi-Paxos

The leader is the natural home of the `step → Ready` loop, doing no I/O. Status, round,
and the response sets are core state; outbound Phase1a/Phase2a become entries in `Ready`.

```rust
enum Status { Idle, Phase1, Phase2, Chosen }

struct Leader {
    round: i64,
    status: Status,
    proposed_value: Option<Value>,
    phase1b: Vec<Phase1b>,   // dedup by acceptor_id
    phase2b: Vec<Phase2b>,   // dedup by acceptor_id
    chosen_value: Option<Value>,
    quorum: usize,           // f + 1
    round_system: RoundSystem, // see §5: who owns which round
}

// The safety heart, verbatim in spirit:
fn pick_value(&self) -> Value {
    let k = self.phase1b.iter().map(|p| p.vote_round).max().unwrap();
    if k == -1 {
        self.proposed_value.clone().expect("free choice needs a proposal")
    } else {
        // exactly one distinct value at vote_round == k
        self.phase1b.iter()
            .find(|p| p.vote_round == k)
            .and_then(|p| p.vote_value.clone())
            .expect("highest-voted promise must carry a value")
    }
}
```

Notes for the port:

- **Dedup quorums, don't size-count raw responses.** frankenpaxos uses a `Set[Phase1b]`;
  a retransmitted Phase1b is structurally equal and collapses. In Rust, count *distinct
  `acceptor_id`s*, not message arrivals, or a duplicate could fake a quorum.
- **The two assertions are real invariants, keep them.** `vs.size == 1` (one value per
  vote round) and `oldChosen == chosen` (a slot never chooses twice) are exactly the
  properties a sans-IO core should `debug_assert!` — they catch a broken acceptor or a
  routing bug immediately.
- **Per slot in Multi-Paxos.** Generalize all of this to `Vec<LeaderSlot>`: round and
  status become per-instance (or are shared across slots once Phase 1 is done leader-wide —
  the optimization in the next file), and `pick_value` runs independently per slot from the
  Phase 1b votes for that slot.
- **`handleProposeRequest`'s fast path** (already-chosen → reply directly, no acceptor
  round-trip) maps to: if `chosen_value.is_some()`, the core emits a `ProposeReply` in
  `Ready` and skips Phase 1/2 entirely.

---

## 3. Client

The client is the simplest actor: pick a leader, propose, and resend on a timer until it
hears back. Its only subtlety is *idempotence* — once a client has proposed a value it
never proposes a different one.

### (a) What frankenpaxos does

A repropose timer fans the proposal out to *all* leaders if no reply arrives (the first
attempt went to just one leader, which may be dead):

```scala
// shared/src/main/scala/frankenpaxos/paxos/Client.scala:56
// A timer to resend a value proposal.
private val reproposeTimer: Transport#Timer =
  timer(
    "reproposeTimer",
    java.time.Duration.ofSeconds(5),
    () => {
      proposedValue match {
        case Some(v) => {
          for (leader <- leaders) {
            leader.send(
              LeaderInbound().withProposeRequest(ProposeRequest(v = v))
            )
          }
        }
        case None => {
          logger.fatal(
            "Attempting to repropose value, but no value was ever proposed."
          )
        }
      }
      reproposeTimer.start()
    }
  );
```

The propose entry: chosen → reply now; already-proposed → just register the waiter;
otherwise pick one leader, send once, and arm the resend timer:

```scala
// shared/src/main/scala/frankenpaxos/paxos/Client.scala:111
private def _propose(v: String, promise: Promise[String]): Unit = {
  // If a value has already been chosen, then there's no need to propose a
  // new value. We simply call the callback immediately.
  chosenValue match {
    case Some(chosen) => {
      promise.success(chosen)
      return
    }
    case None => {}
  }

  // If a value has already been proposed, then there's no need to propose a
  // new value. We simply record the callback to be invoked once the value
  // has been chosen.
  proposedValue match {
    case Some(_) => {
      promises += promise
      return
    }
    case None => {}
  }

  // Send the value to one arbitrarily chosen leader. If this leader
  // happens to be dead, we'll resend the proposal to all the leaders on
  // timeout.
  proposedValue = Some(v)
  leaders.iterator
    .next()
    .send(LeaderInbound().withProposeRequest(ProposeRequest(v = v)))
  reproposeTimer.start()
}
```

### (b) The principle

A Paxos client is an at-least-once retry loop wrapped around an idempotent server. It
latches the first value it proposes (`proposedValue` is set once and never changed) so
retries are safe, optimistically targets a single leader to save messages, and falls back
to broadcasting on timeout to survive a dead leader. Duplicate `ProposeReply`s are harmless
because choosing is idempotent.

### (c) Maps to sans-IO Multi-Paxos

In sans-IO the retry timer is **a caller-driven tick, not a transport callback**. The core
holds no `java.time` timer; the caller calls `tick()` (or `step(Event::Tick)`) on its own
clock, and the core decides whether enough logical time has elapsed to re-emit the propose.

```rust
struct Client {
    proposed_value: Option<Value>,
    chosen_value: Option<Value>,
    elapsed_since_send: u64,   // ticks since last (re)propose
    repropose_after: u64,      // threshold in ticks
}

fn propose(&mut self, v: Value, ready: &mut Ready) {
    if let Some(c) = &self.chosen_value { ready.reply(c.clone()); return; }
    if self.proposed_value.is_some() { return; }     // latched; ignore new value
    self.proposed_value = Some(v.clone());
    self.elapsed_since_send = 0;
    ready.messages.push(Out::ProposeRequest { v, to: One(self.pick_leader()) });
}

fn tick(&mut self, ready: &mut Ready) {
    let Some(v) = &self.proposed_value else { return };
    if self.chosen_value.is_some() { return; }
    self.elapsed_since_send += 1;
    if self.elapsed_since_send >= self.repropose_after {
        self.elapsed_since_send = 0;
        ready.messages.push(Out::ProposeRequest { v: v.clone(), to: AllLeaders });
    }
}
```

The frankenpaxos `Promise[String]`/`Future[String]` machinery is pure I/O glue and does
**not** belong in the core — the core just emits a `ProposeReply` in `Ready` and lets the
caller wake whoever is waiting.

---

## 4. Wire messages

The protocol is exactly four message types, in two request/response pairs. Reading the
`.proto` is the fastest way to understand the protocol's shape.

### (a) What frankenpaxos does

```proto
// shared/src/main/scala/frankenpaxos/paxos/Paxos.proto:21
// Messages sent between propsers and acceptors.
message Phase1a {
  required int32 round = 1;
}

message Phase1b {
  required int32 round = 1;
  required int32 acceptor_id = 2;
  required int32 vote_round = 3;
  optional string vote_value = 4;
}

message Phase2a {
  required int32 round = 1;
  required string value = 2;
}

message Phase2b {
  required int32 acceptor_id = 1;
  required int32 round = 2;
}
```

### (b) The principle

Single-decree Paxos is **2 phases × (request, response)**:
`Phase1a` = "prepare round r" → `Phase1b` = "promised; here is my last vote `(vote_round,
vote_value)`"; `Phase2a` = "accept value v in round r" → `Phase2b` = "voted in r". The only
field carrying recovery information is `Phase1b.vote_round`/`vote_value` — that is what
feeds the value-selection rule of §2. Everything else is a round number and a sender id.
(`ProposeRequest`/`ProposeReply` are the client↔leader pair and are not part of the core
consensus protocol.)

### (c) Maps to sans-IO Multi-Paxos

These four messages become the `Event` inputs and `Ready.messages` outputs of the core —
plain Rust enums, no serialization in the core itself (the caller owns the wire). Two
deliberate divergences for Multi-Paxos:

- **`vote_value` is `optional`** precisely to encode "never voted" (`vote_round == -1`,
  no value). In Rust that is `Option<Value>`, and the `-1`/`None` pairing must stay
  consistent — the `k == -1` branch in `pick_value` depends on it.
- **Add a slot index** to every message in Multi-Paxos, and let Phase 1 carry *ranges*
  (one prepare reserves all future slots for a round; one Phase1b reports votes for many
  slots). That batching is the whole point of Multi-Paxos and the subject of the next file.

The `acceptor_id` field exists so quorum sets dedup by identity, not by arrival — see the
dedup note in §2(c).

---

## 5. RoundSystem

Who is allowed to use round `r`? In single-decree Paxos with multiple would-be leaders, you
need a globally agreed, collision-free assignment of rounds to leaders. frankenpaxos
factors this into a standalone, pure trait — no actor, no I/O, just arithmetic.

### (a) What frankenpaxos does

The trait: a round→leader map plus round classification (classic/fast for Fast Paxos), and
helpers to find a leader's next round:

```scala
// shared/src/main/scala/frankenpaxos/roundsystem/RoundSystem.scala:14
trait RoundSystem {
  type Round = Int
  type LeaderIndex = Int

  // The number of leaders that this round system is designed for.
  def numLeaders(): Int

  // The leader of round `round`.
  def leader(round: Round): LeaderIndex

  // The type of round `round`.
  def roundType(round: Round): RoundType

  // The smallest classic round for `leaderIndex` greater than `round`. ...
  def nextClassicRound(leaderIndex: LeaderIndex, round: Round): Round

  // The smallest fast round for `leaderIndex` greater than `round`. ...
  def nextFastRound(leaderIndex: LeaderIndex, round: Round): Option[Round]
}
```

The canonical implementation is plain round-robin (leader `i` owns rounds `i, i+n,
i+2n, …`), matching the `round += config.leaderAddresses.size` spacing in `Leader.scala`:

```scala
// shared/src/main/scala/frankenpaxos/roundsystem/RoundSystem.scala:60
class ClassicRoundRobin(private val n: Int) extends RoundSystem {
  override def toString(): String = s"ClassicRoundRobin($n)"
  override def numLeaders(): Int = n
  override def leader(round: Round): LeaderIndex = round % n
  override def roundType(round: Round): RoundType = ClassicRound

  override def nextClassicRound(
      leaderIndex: LeaderIndex,
      round: Round
  ): Round = {
    if (round < 0) {
      leaderIndex
    } else {
      val smallestMultipleOfN = n * (round / n)
      val offset = leaderIndex % n
      if (smallestMultipleOfN + offset > round) {
        smallestMultipleOfN + offset
      } else {
        smallestMultipleOfN + n + offset
      }
    }
  }

  override def nextFastRound(
      leaderIndex: LeaderIndex,
      round: Round
  ): Option[Round] = None
}
```

The same trait has several other implementations, all pure functions: `ClassicStutteredRoundRobin`
(each leader owns runs of contiguous rounds), `RoundZeroFast`/`MixedRoundRobin` (interleave
Fast-Paxos fast rounds), and `RenamedRoundSystem`/`RotatedRoundSystem` (permute or rotate
which physical leader holds each logical slot — e.g. `RotatedClassicRoundRobin(n,
firstLeader)`).

### (b) The principle

Ballot ownership is **pure, deterministic, pluggable logic with no I/O**. Given the number
of leaders and a round, every node can compute *locally* who owns that round and what the
next round it may use is — no agreement needed, because it is a closed-form function all
nodes share. Separating this from the consensus actor means the *policy* (round-robin,
rotated, stuttered, fast/classic mix) is swappable without touching the safety-critical
acceptor/leader code.

### (c) Maps to sans-IO Multi-Paxos

Carry a `RoundSystem` as a field in the sans-IO core's state and call it as a pure function;
it never appears in `step`'s I/O at all.

```rust
trait RoundSystem {
    fn num_leaders(&self) -> usize;
    fn leader(&self, round: i64) -> usize;            // who owns this round
    fn next_classic_round(&self, leader: usize, round: i64) -> i64;
}

struct ClassicRoundRobin { n: usize }
impl RoundSystem for ClassicRoundRobin {
    fn num_leaders(&self) -> usize { self.n }
    fn leader(&self, round: i64) -> usize { (round.rem_euclid(self.n as i64)) as usize }
    fn next_classic_round(&self, leader: usize, round: i64) -> i64 { /* as above */ }
}
```

This replaces the open-coded `if round == -1 { round = index } else { round +=
leaderAddresses.size }` in `Leader.scala` with `self.round =
self.round_system.next_classic_round(self.index, self.round)`. For v1, ship only
`ClassicRoundRobin`; the Fast/rotated/stuttered variants are real but out of scope until we
do Fast Paxos. Keep the trait so they slot in later.

---

## 6. Leader-selection auxiliaries

frankenpaxos ships two small services that are easy to mistake for "part of Paxos" but are
not: a **leader election** participant and a **heartbeat** failure detector. They produce
*soft state* — a guess about who the leader is and who is alive. Consensus safety never
depends on them being right; they only affect liveness and which node bothers to drive a
round.

### (a) What frankenpaxos does

**Leader election** (`election/basic`) is ping-based: the leader pings; a follower that
hears no ping for a randomized timeout bumps the round, declares *itself* leader, and fires
registered callbacks. It explicitly does **not** guarantee one leader per round — that is
fine, because Paxos itself tolerates duelling proposers:

```scala
// shared/src/main/scala/frankenpaxos/election/basic/Participant.scala:64
class Participant[Transport <: frankenpaxos.Transport[Transport]](
    address: Transport#Address,
    transport: Transport,
    logger: Logger,
    addresses: Seq[Transport#Address],
    initialLeaderIndex: Int = 0,
    options: ElectionOptions = ElectionOptions.default
) extends Actor(address, transport, logger) {
```

```scala
// shared/src/main/scala/frankenpaxos/election/basic/Participant.scala:118
private lazy val noPingTimer: Transport#Timer = timer(
  "noPingTimer",
  frankenpaxos.Util.randomDuration(options.noPingTimeoutMin,
                                   options.noPingTimeoutMax),
  () => {
    round += 1
    leaderIndex = index
    changeState(Leader)
  }
)
```

When leadership changes, it just notifies callbacks — it hands out "who is leader", nothing
more:

```scala
// shared/src/main/scala/frankenpaxos/election/basic/Participant.scala:153
case (Follower, Leader) =>
  noPingTimer.stop()
  pingTimer.start()
  state = Leader
  ping(round = round, leaderIndex = index)
  callbacks.foreach(c => c(leaderIndex))
```

**Heartbeat** (`heartbeat`) is a best-effort failure detector: ping/pong with retry, and a
node is "dead" after `numRetries` unanswered pings. Its only outputs are an alive-set and
an estimated network delay:

```scala
// shared/src/main/scala/frankenpaxos/heartbeat/Participant.scala:166
private def fail(index: Index): Unit = {
  numRetries(index) += 1
  if (numRetries(index) >= options.numRetries) {
    alive -= addresses(index)
  }
  chans(index).send(
    ParticipantInbound()
      .withPing(Ping(index = index, nanotime = System.nanoTime))
  )
  failTimers(index).start()
}
```

```scala
// shared/src/main/scala/frankenpaxos/heartbeat/Participant.scala:208
// Returns the set of addresses that this participant thinks are alive. ...
def unsafeAlive(): Set[Transport#Address] = Set() ++ alive
```

### (b) The principle

Leader election and failure detection are **orthogonal services that produce soft state**
("who do I think is leader", "who do I think is alive"). They are *not* part of consensus.
Paxos is safe no matter how wrong these guesses are: two nodes both thinking they're leader
just causes duelling rounds (a liveness hiccup, resolved by randomized timeouts), never a
double-choose. This is why frankenpaxos can afford an election that admits multiple leaders
per round and only needs `f+1`, not `2f+1`, participants.

### (c) Maps to sans-IO Multi-Paxos

Keep these as **separate modules outside the consensus core**; the core just reads "current
leader" from soft state. Concretely:

- The election and heartbeat protocols, *if* we implement them, are their own sans-IO state
  machines (their own `step`/`tick`/`Ready`) — never entangled with acceptor/leader code.
  Their timers become caller-driven ticks, exactly like §3.
- The consensus core takes the current-leader hint as **an input/configuration value**, not
  a dependency: e.g. the caller passes `is_leader: bool` (or the core compares
  `round_system.leader(round) == self.index`) to decide whether to *initiate* a round. A
  wrong hint costs a round, not correctness.
- For a v1 we can skip both entirely: drive proposals from the client/caller and let the
  `RoundSystem` plus duelling-proposer tolerance handle contention. Add the election and
  failure detector later as bolt-on liveness improvements, behind the same soft-state
  boundary.

---

### File map / suggested reading order

| Concern                     | frankenpaxos file                                              |
|-----------------------------|---------------------------------------------------------------|
| Acceptor (safety)           | `shared/src/main/scala/frankenpaxos/paxos/Acceptor.scala`     |
| Leader (two phases + pick)  | `shared/src/main/scala/frankenpaxos/paxos/Leader.scala`       |
| Client (propose/retry)      | `shared/src/main/scala/frankenpaxos/paxos/Client.scala`       |
| Wire protocol               | `shared/src/main/scala/frankenpaxos/paxos/Paxos.proto`        |
| Round→leader assignment     | `shared/src/main/scala/frankenpaxos/roundsystem/RoundSystem.scala` |
| Leader election (soft state)| `shared/src/main/scala/frankenpaxos/election/basic/Participant.scala` |
| Failure detector (soft state)| `shared/src/main/scala/frankenpaxos/heartbeat/Participant.scala`     |

Read in order: §1 Acceptor → §2 Leader → §4 wire (to see the messages connecting them) →
§3 Client → §5 RoundSystem → §6 auxiliaries. Then move to the Multi-Paxos file, where every
one of these becomes per-slot.
