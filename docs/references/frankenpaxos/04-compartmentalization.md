# Compartmentalization in `frankenpaxos` — a throughput-scaling decomposition

> [!IMPORTANT]
> **This is NOT for paros v1.** Compartmentalized MultiPaxos is a throughput-scaling
> *decomposition* of the protocol, documented here as a **design horizon**, not a feature to
> build. Ship a monolithic leader first. The single thing to internalize from this file is
> *why* the decomposition is possible at all — and why, in a sans-IO design, every split-out
> role is just **another pure `step()`**, while the splitting itself is a **deployment /
> topology concern the consensus core need not bake in.** Read it for the architectural
> insight, then put it down until you have a measured bottleneck.

The author of `frankenpaxos`, Michael Whittaker, invented Compartmentalized MultiPaxos.
The accompanying paper is in
[`../papers/scaling-rsm-compartmentalization/transcript.md`](../papers/scaling-rsm-compartmentalization/transcript.md).
Its thesis (§1, §3.1): *compartmentalization = **decoupling** individual bottlenecks into
distinct components + **scaling** those components independently.* Compartmentalizing
MultiPaxos buys 6× throughput on writes and 16× on a mixed read/write workload, on commodity
hardware, with no algorithm change.

*Line numbers are from the checkout this was written against and will drift; symbol names are
stable.*

This document follows the house style of its siblings. Every major section has three parts:

- **(a) What frankenpaxos does** — a short verbatim Scala snippet, preceded by a `// path:line`
  comment.
- **(b) The principle** — the language-neutral pattern worth copying.
- **(c) Maps to sans-IO Multi-Paxos** — how the role lands as a pure, I/O-free `step()` in a
  caller-driven Rust core.

A note on the source you're reading: every `frankenpaxos` role is an `Actor` whose only entry
point is `receive(src, inbound)` and whose only egress is `chan.send(...)`. That is *already*
a `step(msg) -> outbound` machine wearing an actor costume. The actor mailbox and the `Chan`
are the I/O; strip them and each role is a pure function from `(state, message)` to
`(state', messages)`. Keep that lens for the whole file — it is the punchline.

---

## Table of contents

1. [`DistributionScheme` — co-locate the monolith, or spread to scale](#1-distributionscheme)
2. [`Batcher` — write-side input batching](#2-batcher)
3. [`ProxyLeader` — off-leader Phase-2 quorum collection (the keystone)](#3-proxyleader)
4. [`ProxyReplica` — reply-side batching](#4-proxyreplica)
5. [`ReadBatcher` — read scaling, off the write path](#5-readbatcher)
6. [Synthesis — roles are pure `step()`s; the split is wiring, not consensus](#6-synthesis)

---

## 1. `DistributionScheme`

### (a) What frankenpaxos does

```scala
// shared/src/main/scala/frankenpaxos/multipaxos/DistributionScheme.scala:1
package frankenpaxos.multipaxos

// Regular MultiPaxos has clients, leaders, acceptors, and replicas. Decoupled
// MultiPaxos adds batchers, proxy leaders, and proxy replicas.
//
// To avoid having to implement MultiPaxos _and_ decoupled MultiPaxos, we
// implement only decoupled MultiPaxos. Then, to simulate MultiPaxos, we run
// every leader with a co-located batcher and proxy leader, and we run every
// replica with a co-located proxy replica. When we do this co-location,
// clients send to the batcher co-located with the leader, leaders send to the
// co-located proxy leader, and replicas send to the co-located proxy replica.
sealed trait DistributionScheme
case object Hash extends DistributionScheme
case object Colocated extends DistributionScheme
```

### (b) The principle

There is **one** protocol — the decoupled one. The monolith is not a separate codebase; it
is the decoupled protocol *deployed differently*. `Colocated` pins each auxiliary role onto
the same process as the role it serves (a leader runs its batcher and proxy leader in-process,
a replica runs its proxy replica in-process), which reproduces classic MultiPaxos's message
flow. `Hash` spreads the auxiliary roles across their own machines and load-balances across
them. The algorithm is identical in both; only the *placement of roles* changes.

This is the crux of the whole file: **monolith vs. scaled-out is a topology choice, not a
protocol choice.** The author built it the hard way once (fully decoupled) and recovered the
easy thing (the monolith) by co-locating.

### (c) Maps to sans-IO Multi-Paxos

This maps onto sans-IO *for free*, and far more cleanly than into an actor system. A sans-IO
core is "just a value with a `step()`"; whether two such values live in the same process or on
two machines is entirely the caller's wiring. So:

- **v1 (Colocated, by construction):** one `Replica` core that internally plays leader,
  acceptor, and learner. No batcher/proxy types exist yet. There is nothing to "co-locate"
  because there is nothing separate.
- **Later (Hash, optional):** lift a role out into its own small core with its own `step()`,
  and let the driver route messages to it over a socket instead of via an in-process call.

The decisive contrast with frankenpaxos: there, `DistributionScheme` is a runtime config that
the protocol logic must *thread through* (channels are pre-wired to either local or remote
addresses). In a sans-IO core, the core never knows. `step()` consumes a message and emits
messages addressed by *role id*; whether that id resolves to `self` or to a TCP connection is
a routing-table entry in the driver. **The core should not have a `DistributionScheme` field
at all.** That absence is the design win.

---

## 2. `Batcher`

### (a) What frankenpaxos does

A batcher accumulates client commands and forwards a single `ClientRequestBatch` to the leader
once the batch is full:

```scala
// shared/src/main/scala/frankenpaxos/multipaxos/Batcher.scala:148
private def handleClientRequest(
    src: Transport#Address,
    clientRequest: ClientRequest
): Unit = {
  growingBatch += clientRequest.command
  if (growingBatch.size >= options.batchSize) {
    val leader = leaders(roundSystem.leader(round))
    leader.send(
      LeaderInbound().withClientRequestBatch(
        ClientRequestBatch(batch = CommandBatch(command = growingBatch.toSeq))
      )
    )
    growingBatch.clear()
    metrics.batchesSent.inc()
  }
}
```

Its other job is staying pointed at the *current* leader. The batcher caches its best guess
of the leader's round; on a `NotLeaderBatcher` rejection it asks all leaders who's in charge,
and when it learns of a newer round whose leader differs, it re-sends the in-flight batches to
the new leader:

```scala
// shared/src/main/scala/frankenpaxos/multipaxos/Batcher.scala:177
private def handleLeaderInfo(
    src: Transport#Address,
    leaderInfo: LeaderInfoReplyBatcher
): Unit = {
  if (leaderInfo.round <= round) {
    // ... stale, ignore ...
    return
  }

  // Update our round.
  val oldRound = round
  val newRound = leaderInfo.round
  round = leaderInfo.round

  // We've sent all of our batches to the leader of round `round`, but we
  // just learned about a new round `leaderInfo.round`. If the leader of the
  // new round is different than the leader of the old round, then we have to
  // re-send our messages.
  if (roundSystem.leader(oldRound) != roundSystem.leader(newRound)) {
    val leader = leaders(roundSystem.leader(newRound))
    for (batch <- pendingResendBatches) {
      leader.send(LeaderInbound().withClientRequestBatch(batch))
    }
  }
  pendingResendBatches.clear()
}
```

### (b) The principle

**Amortize per-command overhead, and decouple clients from the leader's critical path.** The
paper's framing (§4.1): the leader's cost is *linear in commands*, so feeding it whole batches
instead of individual commands cuts the messages it must process from `n` to `1` per batch of
`n`. The batcher absorbs the fan-in from many clients so the leader sees a trickle of fat
messages instead of a flood of thin ones. The leader-tracking logic is the price of putting a
relay in front of a role whose identity changes on failover: the relay must discover and chase
the current leader, and must not lose commands across a leadership change.

### (c) Maps to sans-IO Multi-Paxos

A batcher is the simplest possible sans-IO core: pure accumulation with a flush rule. No
consensus state, no durability.

```rust
// Sketch — not paros v1.
pub struct Batcher {
    growing: Vec<Command>,
    pending_resend: Vec<CommandBatch>,
    round: u64,                 // best guess of the leader's round
    batch_size: usize,
}

pub enum BatcherIn {
    ClientRequest(Command),
    NotLeader { batch: CommandBatch },
    LeaderInfo { round: u64 },
    Tick,                       // timeout-driven flush; see ReadBatcher §5
}

pub enum BatcherOut {
    ToLeader { round: u64, batch: CommandBatch },
}

impl Batcher {
    pub fn step(&mut self, msg: BatcherIn) -> Vec<BatcherOut> { /* ... */ }
}
```

Two sans-IO observations:

- **The flush trigger is `step()`, not a timer.** frankenpaxos's batchers flush on size only;
  read batchers (§5) add a timeout via `Transport#Timer`. In sans-IO there is no timer in the
  core — the *driver* owns the clock and delivers a `Tick` event, exactly like raft's
  `tick()`. The core stays deterministic and simulator-friendly.
- **Leader tracking is the same `step()`.** `NotLeader` / `LeaderInfo` are just more input
  variants. The "re-send pending batches to the new leader" rule becomes: emit `ToLeader`
  outputs addressed to the new round's leader. The core decides *what* to send and *to which
  role*; the driver decides *which socket* that is.

A batcher is *optional* and *front-of-line*: paros v1 simply lets the application call
`propose(command)` directly on the `Replica`. Batching is a later, transparent insert.

---

## 3. `ProxyLeader`

This is the **keystone of compartmentalization** (paper §3.1), so it gets the most space. The
leader's expensive job is not *deciding* a command's slot/round — that is `O(1)` sequencing —
it is *gathering the Phase-2 quorum*: broadcasting Phase2a to acceptors, collecting `f+1`
Phase2b acks, and notifying replicas of the chosen value. A proxy leader is a horizontally
scalable, **near-stateless relay** that does exactly that gathering, freeing the real leader to
do nothing but sequence. Per the paper, this drops the leader from `3f+4` messages per command
to **2**.

### (a) What frankenpaxos does

**Per-(slot, round) state is tiny and ephemeral.** A proxy leader holds, for each in-flight
`SlotRound`, either a `Pending` (the Phase2a it's broadcasting plus the Phase2bs gathered so
far) or `Done`:

```scala
// shared/src/main/scala/frankenpaxos/multipaxos/ProxyLeader.scala:86
@JSExportAll
case class SlotRound(slot: Int, round: Int)

@JSExportAll
sealed trait State

@JSExportAll
case class Pending(
    phase2a: Phase2a,
    phase2bs: mutable.Map[(GroupIndex, AcceptorIndex), Phase2b]
) extends State

@JSExportAll
object Done extends State
```

```scala
// shared/src/main/scala/frankenpaxos/multipaxos/ProxyLeader.scala:134
@JSExport
protected var states = mutable.Map[SlotRound, State]()
```

**Phase2a fan-out.** On a Phase2a from the leader, the proxy leader picks a quorum of
acceptors — a *thrifty* `f+1`-sized subset of the slot's acceptor group (or a grid write
quorum under flexible quorums) — broadcasts to them, and records the pending state:

```scala
// shared/src/main/scala/frankenpaxos/multipaxos/ProxyLeader.scala:175
private def handlePhase2a(src: Transport#Address, phase2a: Phase2a): Unit = {
  val slotround = SlotRound(slot = phase2a.slot, round = phase2a.round)
  states.get(slotround) match {
    case Some(_) =>
      logger.debug( /* duplicate Phase2a; ignore */ )

    case None =>
      // Select the appropriate acceptors to talk to.
      val quorum = if (!config.flexible) {
        // Pick a the correct acceptor group (according to the slot), and
        // then randomly select a thrifty quorum of it.
        val group = acceptors(phase2a.slot % config.numAcceptorGroups)
        scala.util.Random.shuffle(group).take(config.f + 1)
      } else {
        // Pick a random write quorum.
        grid
          .randomWriteQuorum()
          .map({ case (row, col) => acceptors(row)(col) })
      }

      if (options.flushPhase2asEveryN == 1) {
        quorum.foreach(_.send(AcceptorInbound().withPhase2A(phase2a)))
      } else {
        // ... sendNoFlush + batched flush every N ...
      }

      // Update our state.
      states(slotround) = Pending(phase2a = phase2a, phase2bs = mutable.Map())
  }
}
```

**Phase2b: collect a quorum, then broadcast `Chosen`.** Each Phase2b is recorded against its
`(slot, round)`; once `f+1` have arrived (or a grid write quorum is satisfied), the value is
*chosen* — the proxy leader tells every replica and flips the state to `Done`:

```scala
// shared/src/main/scala/frankenpaxos/multipaxos/ProxyLeader.scala:217
private def handlePhase2b(src: Transport#Address, phase2b: Phase2b): Unit = {
  val slotround = SlotRound(slot = phase2b.slot, round = phase2b.round)
  states.get(slotround) match {
    case None =>
      logger.fatal( /* Phase2b without a Phase2a — impossible */ )

    case Some(Done) =>
      logger.debug( /* already chosen; ignore */ )

    case Some(pending: Pending) =>
      // Wait until we receive a quorum of Phase2bs.
      val phase2bs = pending.phase2bs
      phase2bs((phase2b.groupIndex, phase2b.acceptorIndex)) = phase2b
      if (!config.flexible && phase2bs.size < config.f + 1) {
        return
      }
      if (config.flexible && !grid.isWriteQuorum(phase2bs.keys.toSet)) {
        return
      }

      // Let the replicas know that the value has been chosen.
      replicas.foreach(
        _.send(
          ReplicaInbound().withChosen(
            Chosen(slot = phase2b.slot,
                   commandBatchOrNoop = pending.phase2a.commandBatchOrNoop)
          )
        )
      )

      // Update our state.
      states(slotround) = Done
  }
}
```

Note what is *absent*: no ballot/round adoption, no leader election, no log, no durability.
The proxy leader has no opinion about which round is current — the leader stamped the round
into the Phase2a, and the proxy leader just carries it. It is a quorum-counting relay with a
two-state-per-slot ledger.

### (b) The principle

**Separate cheap sequencing from expensive broadcast, then scale the expensive part
out.** Sequencing — assigning slot `i` and round `r` to a command — is inherently serial and
unscalable (only the leader may do it, to keep the log consistent). Broadcasting and
quorum-collection is *embarrassingly parallel*: each command's Phase-2 round-trip is
independent of every other's. So you peel the parallel part off the serial part and run many
copies of it. The leader sequences a command, hands the Phase2a to *any* proxy leader (round
robin / load-balanced), and is done; the proxy leader eats the `O(f)` fan-out and fan-in.
Because each proxy leader keeps only ephemeral per-slot state and shares nothing with the
others, you scale by adding more of them until they're no longer the bottleneck (paper §3.1).

The reason this is *safe* — why correctness doesn't care that broadcast moved off the leader —
is that the proxy leader contributes **nothing to the consensus decision**. It does not vote,
does not order, does not adopt rounds. Choice is defined entirely by acceptors accepting a
`(slot, round, value)` and a quorum existing; the proxy leader merely *observes* that quorum
and announces it. Moving an observer around can't break agreement.

### (c) Maps to sans-IO Multi-Paxos

A proxy leader is a textbook sans-IO core: an input function, an output buffer, and a small
map of ephemeral state. In paros v1 this logic lives *inside* the leader's `step()` — it is
the Phase-2 path. Documenting it as a standalone core is just showing where the seam is.

```rust
// Sketch — not paros v1. In v1 this is the leader's own Phase-2 path.
struct Pending {
    phase2a: Phase2a,
    acks: HashMap<AcceptorId, Phase2b>,
}

enum SlotState {
    Pending(Pending),
    Done,
}

pub struct ProxyLeader {
    states: HashMap<(Slot, Round), SlotState>,
    quorum_size: usize,        // f + 1 (or a grid write-quorum predicate)
}

pub enum ProxyLeaderIn {
    Phase2a(Phase2a),          // from the leader
    Phase2b(Phase2b),          // from an acceptor
}

pub enum ProxyLeaderOut {
    ToAcceptor { who: AcceptorId, msg: Phase2a },
    Chosen { who: ReplicaId, slot: Slot, value: ValueOrNoop },
}

impl ProxyLeader {
    pub fn step(&mut self, msg: ProxyLeaderIn) -> Vec<ProxyLeaderOut> {
        match msg {
            ProxyLeaderIn::Phase2a(p) => {
                let key = (p.slot, p.round);
                if self.states.contains_key(&key) {
                    return vec![]; // duplicate; ignore (matches handlePhase2a)
                }
                // pick a quorum of acceptors; emit a Phase2a to each.
                let out = self.select_quorum(&p)
                    .map(|who| ProxyLeaderOut::ToAcceptor { who, msg: p.clone() })
                    .collect();
                self.states.insert(key, SlotState::Pending(Pending {
                    phase2a: p, acks: HashMap::new(),
                }));
                out
            }
            ProxyLeaderIn::Phase2b(b) => {
                let key = (b.slot, b.round);
                match self.states.get_mut(&key) {
                    None | Some(SlotState::Done) => vec![], // impossible / already chosen
                    Some(SlotState::Pending(p)) => {
                        p.acks.insert(b.acceptor, b);
                        if p.acks.len() < self.quorum_size {
                            return vec![]; // not yet a quorum
                        }
                        let value = p.phase2a.value.clone();
                        let slot = key.0;
                        self.states.insert(key, SlotState::Done);
                        // broadcast Chosen to all replicas.
                        self.replicas()
                            .map(|who| ProxyLeaderOut::Chosen { who, slot, value: value.clone() })
                            .collect()
                    }
                }
            }
        }
    }
}
```

Sans-IO-specific notes:

- **The two `logger.fatal` cases are invariants, not I/O.** "Phase2b without a Phase2a" and a
  duplicate Phase2a are impossible under correct routing; in Rust they're `debug_assert!` /
  ignored arms, exercised by the deterministic simulator rather than by a logger.
- **`flushPhase2asEveryN` does not belong in the core.** frankenpaxos batches socket flushes
  for throughput (`sendNoFlush` then a periodic `flush()`). In sans-IO the core *always*
  emits a message value into the output buffer; *when bytes hit the wire* is the driver's
  call. Egress batching is a driver optimization, invisible to the algorithm — exactly the
  kind of I/O policy a sans-IO boundary is meant to keep out of the core.
- **State cleanup.** frankenpaxos leaves `Done` entries in the map (a GC concern). A v1 core
  can drop the entry once `Chosen` is emitted, or keep it for idempotent dedup — a memory
  policy, not a correctness one.
- **The keystone insight, restated for sans-IO:** in a monolith this whole core *is* the
  leader's Phase-2 handler. Splitting it out later is "move this `step()` to another process
  and route Phase2a/Phase2b to it" — **no line of the consensus logic changes.** That is the
  payoff of having written the leader as a pure `step()` in the first place.

---

## 4. `ProxyReplica`

### (a) What frankenpaxos does

The mirror image of the batcher, on the output side: a replica executes a batch and hands the
result *batch* to a proxy replica, which fans the individual replies back out to each client:

```scala
// shared/src/main/scala/frankenpaxos/multipaxos/ProxyReplica.scala:139
private def handleClientReplyBatch(
    src: Transport#Address,
    clientReplyBatch: ClientReplyBatch
): Unit = {
  for (clientReply <- clientReplyBatch.batch) {
    val clientAddress = transport.addressSerializer
      .fromBytes(clientReply.commandId.clientAddress.toByteArray())
    val client = clients.getOrElseUpdate(
      clientAddress,
      chan[Client[Transport]](clientAddress, Client.serializer)
    )

    if (options.batchFlush) {
      client.sendNoFlush(ClientInbound().withClientReply(clientReply))
      // Do nothing here. We'll flush at the end of the batch.
    } else if (options.flushEveryN == 1) {
      client.send(ClientInbound().withClientReply(clientReply))
    } else {
      // ... sendNoFlush + flush every N ...
    }
  }

  if (options.batchFlush) {
    clients.values.foreach(_.flush())
  }
}
```

It also relays two control messages straight through to the leaders — `ChosenWatermark` (how
far the replica has chosen) and `Recover` — without transforming them:

```scala
// shared/src/main/scala/frankenpaxos/multipaxos/ProxyReplica.scala:203
private def handleChosenWatermark(
    src: Transport#Address,
    chosenWatermark: ChosenWatermark
): Unit = {
  leaders.foreach(
    _.send(LeaderInbound().withChosenWatermark(chosenWatermark))
  )
}
```

### (b) The principle

**Batch on the output side too.** The paper calls these *unbatchers* (§4.2): a replica's two
jobs are *executing* batches and *replying* to clients; the second is `O(commands)` fan-out
that has nothing to do with state-machine execution, so peel it off onto a scalable relay. The
replica emits one fat reply batch; the proxy replica scatters individual replies. Same
amortization as the batcher, run in reverse, on the egress path.

### (c) Maps to sans-IO Multi-Paxos

Even simpler than the batcher — there is *no* accumulation, just a scatter:

```rust
// Sketch — not paros v1.
pub enum ProxyReplicaIn {
    ClientReplyBatch(Vec<ClientReply>),
    ReadReplyBatch(Vec<ReadReply>),
    ChosenWatermark(ChosenWatermark),  // passthrough to leaders
    Recover(Recover),                  // passthrough to leaders
}

pub enum ProxyReplicaOut {
    ToClient { who: ClientId, reply: ClientReply },
    ToLeader { who: LeaderId, msg: LeaderInbound },
}
```

Sans-IO notes:

- **The passthrough handlers prove the point.** `ChosenWatermark` and `Recover` are forwarded
  verbatim. A pure relay's `step()` is sometimes just `input -> re-addressed output` — and
  that is fine; not every role mutates state.
- **`flushEveryN` / `batchFlush` are, again, driver concerns** (see §3). The core emits one
  `ToClient` per reply; the driver decides when to flush sockets.
- In paros v1 there is no proxy replica: the `Replica` core surfaces executed results in its
  `Ready` (alongside chosen entries and outbound messages), and the application replies to
  clients however it likes. Reply batching is a later egress optimization.

---

## 5. `ReadBatcher`

### (a) What frankenpaxos does

A read batcher scales *reads* on a path entirely separate from writes. It accumulates read
commands and, for linearizable reads, first asks a quorum of acceptors for their max voted
slot (a `BatchMaxSlotRequest`) before issuing the read at a safe slot — Paxos Quorum Reads:

```scala
// shared/src/main/scala/frankenpaxos/multipaxos/ReadBatcher.scala:444
private def handleReadRequest(
    src: Transport#Address,
    readRequest: ReadRequest
): Unit = {
  linearizableBatch += readRequest.command

  options.readBatchingScheme match {
    case ReadBatchingScheme.Size(batchSize, _) =>
      // Wait for the batch to exceed or match the batch size.
      if (linearizableBatch.size < batchSize) {
        return
      }

      // Send a BatchMaxSlotRequest to a randomly chosen acceptor group and
      // update our batch.
      val group = acceptors(rand.nextInt(acceptors.size))
      val quorum = scala.util.Random.shuffle(group).take(config.f + 1)
      quorum.foreach(
        _.send(
          AcceptorInbound().withBatchMaxSlotRequest(
            BatchMaxSlotRequest(
              readBatcherIndex = index,
              readBatcherId = linearizableId
            )
          )
        )
      )

      batchMaxSlotReplies(linearizableId) = mutable.Map()
      pendingLinearizableBatches(linearizableId) = linearizableBatch
      linearizableId += 1
      linearizableBatch = mutable.Buffer()
      linearizableTimer.foreach(_.reset())

    case _: ReadBatchingScheme.Time => ()        // timer triggers the batch
    case ReadBatchingScheme.Adaptive => ()       // BatchMaxSlotReply triggers it
  }
}
```

The three batching schemes (`Size`, `Time`, `Adaptive`) are declared near the top of the file:

```scala
// shared/src/main/scala/frankenpaxos/multipaxos/ReadBatcher.scala:31
@JSExportAll
sealed trait ReadBatchingScheme

@JSExportAll
object ReadBatchingScheme {
  case class Size(batchSize: Int, timeout: java.time.Duration)
      extends ReadBatchingScheme
  case class Time(timeout: java.time.Duration) extends ReadBatchingScheme
  case object Adaptive extends ReadBatchingScheme
  // ...
}
```

### (b) The principle

**Reads don't mutate state, so take them off the write path entirely.** The paper's
leaderless-reads compartmentalization (§3.4, §4.1): writes go through the leader and all
replicas; reads commute and only need a *single* replica to execute, plus a quorum check to
stay linearizable. So reads get their *own* batchers, their own scaling knob, and their own
batching policy (size / time / adaptive), none of which touches the write pipeline. This is
why the mixed-workload speedup (16×) dwarfs the write-only one (6×): reads stop competing with
writes for the leader.

### (c) Maps to sans-IO Multi-Paxos

A read batcher carries more state than the others — pending batches keyed by id, awaiting an
acceptor quorum — but it is still a pure `step()`:

```rust
// Sketch — not paros v1.
pub enum ReadBatcherIn {
    ReadRequest(Command),
    BatchMaxSlotReply(BatchMaxSlotReply),  // acceptor's max voted slot
    Tick,                                  // drives Time / Size-timeout flush
}

pub enum ReadBatcherOut {
    ToAcceptor { who: AcceptorId, req: BatchMaxSlotRequest },
    ToReplica  { who: ReplicaId, batch: ReadRequestBatch },  // includes the safe slot
}
```

Sans-IO notes:

- **The timers must die in the core.** This is the most timer-laden role in frankenpaxos —
  `makeLinearizableTimer`, `makeSequentialTimer`, `makeEventualTimer`, each calling
  `Transport#Timer`. In sans-IO every one of those collapses into the driver's `Tick`:
  the core exposes a deadline (or just reacts to a `Tick` input) and the driver owns wall
  time. This is the single biggest sans-IO transformation in the file, and it is exactly
  raft's `tick()` lesson applied to batching.
- **Pending-batch state is ephemeral, like the proxy leader's.** Keyed by `readBatcherId`,
  dropped once the `BatchMaxSlotReply` quorum resolves to a slot. No durability.
- Reads are well past v1. paros v1 can serve a linearizable read the dumb way — propose a
  no-op and read at its slot — long before a dedicated, scalable read path is worth building.

---

## 6. Synthesis

Look back at all five roles. Strip the `Actor`/`Chan`/`Transport` scaffolding and every one of
them is the same shape:

```text
step(message) -> outbound messages     (+ a little ephemeral, non-durable state)
```

- **Batcher:** accumulate commands, emit a batch to the leader.
- **ProxyLeader:** broadcast Phase2a, count Phase2b to a quorum, emit `Chosen`.
- **ProxyReplica:** scatter a reply batch to clients; relay control messages.
- **ReadBatcher:** accumulate reads, quorum-check via acceptors, emit a read batch to a replica.

None of them holds durable consensus state. None of them votes or orders. Each is a small,
pure state machine that the deterministic simulator can drive with a stream of messages and
`Tick`s. That uniformity is not an accident of frankenpaxos's style — it is what
compartmentalization *is*: chopping a monolithic role into independent pieces is only possible
because each piece was already a pure function of `(state, message)`.

**Here is the closing insight, and the reason this file exists.**

In a sans-IO design you **ship the monolith first** — all roles fused into one `Replica` core,
the `Colocated` scheme by construction (§1) — and you can **split roles across processes
later WITHOUT changing the algorithm.** The Phase-2 path you wrote inside the leader's
`step()` (§3) *is* the proxy leader; lifting it out is moving a pure function to another
process and pointing the driver's routing table at a socket. The consensus logic — ballots,
quorums, choice — does not move and does not change.

This is precisely what frankenpaxos discovered the expensive way and encoded in
`DistributionScheme`: it had to build the fully-decoupled protocol and then *recover* the
monolith by co-location. A sans-IO core gets the same property for free and from the opposite
direction — it builds the monolith and *defers* decoupling — because **the decoupling is
wiring, not consensus.** The core's contract (`step()` in, `Ready` out, messages addressed by
role) is identical whether a role lives in-process or across the network. So the right move
for paros is to keep `DistributionScheme` *out* of the core entirely: the core should never
know how many proxy leaders exist or where they run. When throughput finally demands it,
compartmentalization is a deployment topology you layer on top — not a rewrite of the
algorithm you already have.
