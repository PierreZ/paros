# The actor/transport skeleton and deterministic simulation in `frankenpaxos`

A reference for building a **sans-IO Multi-Paxos** library, extracted from
[`frankenpaxos`](https://github.com/mwhittaker/frankenpaxos), a Scala research codebase
that implements a dozen consensus protocols (Paxos, MultiPaxos, EPaxos, Mencius, Scalog,
…). Unlike etcd/raft, frankenpaxos is **not** sans-IO: it is **actor/transport-based**.
Each protocol participant is an `Actor` whose `receive(src, message)` callback is *driven
by a transport* that owns the socket and the clock.

So why study it for a sans-IO library? Because frankenpaxos pairs that actor model with a
**deterministic, in-memory simulator** (`FakeTransport` + `Simulator`) that property-tests
every protocol — generating random message-delivery/timer schedules and shrinking any
invariant violation to a minimal failing trace. That simulator is the enduring lesson. A
sans-IO core *inverts* frankenpaxos's structure (the caller drives `step()` instead of the
transport driving `receive()`), and that inversion makes the simulator the **default**
runtime rather than a test-only bolt-on. This document reads frankenpaxos's framework with
that goal in mind.

*Line numbers are from the checkout this was written against and will drift; symbol names
are stable.*

Every section has three parts:

- **(a) What frankenpaxos does** — verbatim Scala with a `file:line` reference.
- **(b) The principle** — the language-neutral pattern worth copying.
- **(c) Maps to sans-IO Multi-Paxos** — how it lands in a Rust core that does **zero I/O**,
  exposes one `step(event)` entry point, and drains side effects through a `Ready`-style
  struct.

---

## Table of contents

1. [The Actor contract — object owns logic, transport owns I/O](#1-the-actor-contract)
2. [Two transports — deterministic sim vs real Netty](#2-two-transports)
3. [Serialization — a pluggable codec at the edge](#3-serialization)
4. [The property-based Simulator — generate, check, shrink](#4-the-property-based-simulator)
5. [The central inversion — transport-drives-actor vs caller-drives-step](#5-the-central-inversion)

---

## 1. The Actor contract

### (a) What frankenpaxos does

Every participant subclasses `Actor`. The contract is one abstract callback plus a
serializer; the transport delivers inbound bytes to `receive`:

```scala
// shared/src/main/scala/frankenpaxos/Actor.scala:14
  // Interface.
  type InboundMessage
  def serializer: Serializer[InboundMessage]
  def receive(src: Transport#Address, message: InboundMessage): Unit
```

Construction implicitly registers the actor with the transport, so the transport can route
messages to it later:

```scala
// shared/src/main/scala/frankenpaxos/Actor.scala:18
  // Implementation.
  logger.info(s"Actor $this registering on address $address.")
  transport.register(address, this)
```

Outbound messaging and timers are **delegated to the transport**, not performed by the
actor. `send`/`sendNoFlush`/`flush` just forward to the transport; `timer` asks the
transport to schedule a callback:

```scala
// shared/src/main/scala/frankenpaxos/Actor.scala:33
  // Sending and flushing.
  def send(dst: Transport#Address, bytes: Array[Byte]): Unit =
    transport.send(this, dst, bytes)
  def sendNoFlush(dst: Transport#Address, bytes: Array[Byte]): Unit =
    transport.sendNoFlush(this, dst, bytes)
  def flush(dst: Transport#Address): Unit = transport.flush(this, dst)
```

```scala
// shared/src/main/scala/frankenpaxos/Actor.scala:44
  def timer(
      name: String,
      delay: java.time.Duration,
      f: () => Unit
  ): Transport#Timer = {
    transport.timer(address, name, delay, f)
  }
```

In practice an actor talks to a peer through a typed `Chan`, which serializes the
in-memory message and hands the bytes to the transport — the actor never touches bytes on
the egress path:

```scala
// shared/src/main/scala/frankenpaxos/Chan.scala:12
  def send(msg: DstActor#InboundMessage): Unit =
    transport.send(srcActor, dst, serializer.toBytes(msg))
  def sendNoFlush(msg: DstActor#InboundMessage): Unit =
    transport.sendNoFlush(srcActor, dst, serializer.toBytes(msg))
  def flush(): Unit = transport.flush(srcActor, dst)
```

The contract that makes any of this reasoning-about-able is **single-threadedness**, stated
on the `Transport` trait:

```scala
// shared/src/main/scala/frankenpaxos/Transport.scala:37
// # Threading
// All Transport implementations MUST be single-threaded. Actor `receive`
// methods and timer callbacks must be called serially on a single thread.
```

### (b) The principle

Separate **what a node decides** (protocol logic) from **how messages and time get to it**
(the transport). The actor implements one inbound callback and never opens a socket or
reads a clock itself — it asks the transport to send and to schedule timers. The whole
model rests on a serialization guarantee: callbacks fire one at a time on a single thread,
so the logic can be written as straight-line state mutation with no locks.

### (c) Maps to sans-IO Multi-Paxos

frankenpaxos pushes I/O *down* into the transport but keeps **control flow inverted**: the
transport calls *into* the actor (`actor.receive(...)`), and the actor calls *back out* to
the transport (`transport.send(...)`) as an immediate side effect. A sans-IO core keeps the
same logic/IO split but flips both arrows so the **caller** is in charge:

```rust
// One inbound entry point — frankenpaxos's `receive(src, message)` and its
// timer callbacks all collapse into a single `step(event)`.
pub enum Event {
    Message { from: NodeId, msg: Message }, // was: receive(src, message)
    Timeout(TimerId),                       // was: a fired transport timer
    Propose(Command),                       // was: a client calling client.write(...)
    Tick,                                   // was: the transport's wall clock
}

impl PaxosNode {
    pub fn step(&mut self, event: Event) { /* pure state mutation, no I/O */ }

    // Egress is *returned*, not performed. frankenpaxos's `transport.send(...)`
    // becomes "push onto a buffer that surfaces in the next Ready".
    pub fn ready(&mut self) -> Ready { /* drain outbox, timer (re)arms, applied */ }
}
```

- `receive(src, message)` → an `Event::Message { from, msg }` arm of `step`.
- `transport.send(...)` (an eager call) → an entry in `Ready.messages` the caller sends.
- `transport.timer(name, delay, f)` (registers a callback) → `Ready.timers` describing
  arm/disarm requests; the caller owns the clock and feeds `Event::Timeout` back in.
- The "single-threaded transport" rule becomes *free*: a `&mut self` `step` is inherently
  serial — there is no thread to share.

---

## 2. Two transports

The same `Actor` runs against two interchangeable `Transport` implementations: an in-memory
deterministic simulator for testing, and a real Netty TCP transport for deployment. The
contrast is the whole point.

### (a) What frankenpaxos does

**`FakeTransport` — deterministic, in-memory.** `send` does not touch a network; it appends
the message to a buffer. Timers are just registered, not scheduled against a clock:

```scala
// shared/src/main/scala/frankenpaxos/FakeTransport.scala:89
  override def send(
      actor: Actor[FakeTransport],
      dst: FakeTransport#Address,
      bytes: Array[Byte]
  ): Unit = {
    messages += FakeTransportMessage(actor.address, dst, bytes.to[Vector])
  }
```

Nothing happens until the *driver* explicitly chooses to deliver a buffered message. That
is when (and only when) the actor's `receive` runs — note the deserialize-then-dispatch:

```scala
// shared/src/main/scala/frankenpaxos/FakeTransport.scala:142
  def deliverMessage(msg: FakeTransportMessage): Unit = {
    if (!messages.contains(msg)) {
      logger.warn(s"Attempted to deliver unsent message $msg.")
      return
    }
    messages -= msg

    actors.get(msg.dst) match {
      case Some(actor) =>
        actor.receive(msg.src, actor.serializer.fromBytes(msg.bytes.to[Array]))
```

Timers are the same story: a running timer fires only when the driver explicitly triggers
it — there is no wall clock involved:

```scala
// shared/src/main/scala/frankenpaxos/FakeTransport.scala:161
  def triggerTimer(timerId: Int): Unit = {
    if (!timers.contains(timerId)) {
      logger.warn(
        s"Attempted to trigger timer $timerId, but no such timer is registered."
      )
      return
    }

    val timer = timers(timerId)
    if (!timer.running) {
      logger.warn(/* ... not running ... */)
      return
    }

    timer.run()
  }
```

The key move that makes this *property-testable*: `FakeTransport` can enumerate **every
possible next event** as a `Command` (deliver any buffered message, or fire any running
timer), each weighted by how many of that kind exist:

```scala
// shared/src/main/scala/frankenpaxos/FakeTransport.scala:194
  // Generate a FakeTransport command. Every possible command (i.e. delivering
  // a message or triggering a running timer) has equal probability.
  def generateCommand(fakeTransport: FakeTransport): Option[Gen[Command]] = {
    var subgens = mutable.Buffer[(Int, Gen[Command])]()

    if (fakeTransport.messages.size > 0) {
      subgens += fakeTransport.messages.size ->
        Gen.oneOf(fakeTransport.messages).map(DeliverMessage(_))
    }

    if (fakeTransport.runningTimers().size > 0) {
      subgens += fakeTransport.runningTimers().size ->
        Gen
          .oneOf(fakeTransport.runningTimers().to[Seq])
          .map(timer => {
            TriggerTimer(address = timer.address,
                         name = timer.name(),
                         timerId = timer.id)
          })
    }

    if (subgens.size > 0) {
      Some(Gen.frequency(subgens: _*))
    } else {
      None
    }
  }
```

Because messages live in an unordered buffer and any one may be picked next, the simulator
naturally explores reordering, delay, and (under shrinking — see §4) dropped messages.

**`NettyTcpTransport` — real async I/O.** The same `timer` method instead schedules a real
callback on a real event loop against the wall-clock `delay`:

```scala
// shared/src/main/scala/frankenpaxos/NettyTcpTransport.scala:96
      case None =>
        val callable = new Callable[Unit]() {
          override def call(): Unit = {
            scheduledFuture = None
            f()
          }
        }
        scheduledFuture = Some(
          eventLoop.schedule(
            callable,
            delay.toNanos(),
            java.util.concurrent.TimeUnit.NANOSECONDS
          )
        )
```

And inbound bytes arrive from a socket via Netty's `channelRead`, which deserializes and
calls the same `actor.receive` — the actor cannot tell which transport it is running under:

```scala
// shared/src/main/scala/frankenpaxos/NettyTcpTransport.scala:129
    override def channelRead(ctx: ChannelHandlerContext, msg: Object): Unit = {
      val localAddress = NettyTcpAddress(ctx.channel.localAddress)
      val remoteAddress = NettyTcpAddress(ctx.channel.remoteAddress)
      msg match {
        case bytes: Array[Byte] => {
          actor.receive(remoteAddress, actor.serializer.fromBytes(bytes))
        }
```

### (b) The principle

Make I/O and time a swappable boundary behind one interface, then provide two
implementations: a **deterministic in-memory** one where nothing happens except by an
explicit driver call (no real sockets, no real clock — messages sit in a buffer, timers
fire only when triggered), and a **real** one (sockets + scheduled callbacks). The
deterministic side gains the ability to *enumerate every legal next event*, which is what
turns "run the system" into "exhaustively/randomly explore schedules". The logic above the
boundary is identical in both worlds.

### (c) Maps to sans-IO Multi-Paxos

A sans-IO core has **no transport boundary to swap** — the core already does zero I/O, so
`FakeTransport`'s "buffer the message, deliver later" *is* the core's natural behavior, not
a special mock:

- `FakeTransport.messages` (the pending buffer) ↔ a driver-owned in-flight message set. In
  sans-IO, the core hands outbound messages to the driver via `Ready.messages`; the driver
  holds the network "in flight" set and decides delivery order.
- `deliverMessage(msg)` ↔ `node.step(Event::Message { from, msg })`. Deserialization
  happens at the driver edge (the core works on typed `Message`, see §3).
- `triggerTimer(id)` ↔ `node.step(Event::Timeout(id))`; `runningTimers()` ↔ the set of
  arm requests the core last surfaced in `Ready.timers`.
- `generateCommand` is the model for a **sim driver**: from `{pending messages} ∪ {armed
  timers} ∪ {client proposals}`, pick one, feed it to `step`, then drain `Ready` back into
  the in-flight set. The Netty transport collapses to a thin driver: read socket →
  `step(Message)`; on `Ready`, write sockets and `eventLoop.schedule` the timers.

---

## 3. Serialization

### (a) What frankenpaxos does

Codecs are abstracted behind a tiny `Serializer[A]` trait — bytes in, typed value out:

```scala
// shared/src/main/scala/frankenpaxos/Serializer.scala:5
trait Serializer[A] {
  def toBytes(x: A): Array[Byte]
  def fromBytes(bytes: Array[Byte]): A
  def toPrettyString(x: A): String = ""
  def fromByteString(bytes: ByteString): A = fromBytes(bytes.toByteArray)
}
```

The production implementation wraps scalapb-generated protobuf messages — `toBytes` /
`fromBytes` are just protobuf encode/decode:

```scala
// shared/src/main/scala/frankenpaxos/ProtoSerializer.scala:1
package frankenpaxos

class ProtoSerializer[
    Proto <: scalapb.GeneratedMessage with scalapb.Message[Proto]
](
    implicit companion: scalapb.GeneratedMessageCompanion[Proto]
) extends Serializer[Proto] {
  override def toBytes(x: Proto): Array[Byte] = x.toByteArray
  override def fromBytes(bytes: Array[Byte]): Proto = companion.parseFrom(bytes)
  override def toPrettyString(x: Proto): String = x.toProtoString
}
```

Crucially, the serializer is only invoked **at the transport edge**: `Chan.send` calls
`serializer.toBytes` on the way out (§1), and `deliverMessage`/`channelRead` call
`serializer.fromBytes` on the way in (§2). Actor logic only ever sees the decoded
`InboundMessage`, never raw bytes. (`toPrettyString` exists purely so the simulator can
render a failing trace legibly — see `commandToString` in §4.)

### (b) The principle

The protocol core operates on **deserialized, in-memory typed messages**; the wire codec is
a pluggable component invoked only at the I/O boundary. This keeps the logic independent of
the wire format (protobuf today, anything tomorrow) and keeps the deterministic simulator
free of encoding noise — it can compare and shrink *structured* messages.

### (c) Maps to sans-IO Multi-Paxos

The core's `step` takes a typed `Message` enum (`Prepare`/`Promise`/`Accept`/`Accepted`/
`Commit`), never `&[u8]`. Encoding/decoding lives entirely in the driver:

```rust
// Driver edge — the only place bytes exist. The core never sees them.
fn on_socket_bytes(node: &mut PaxosNode, from: NodeId, bytes: &[u8]) {
    let msg = Codec::decode(bytes);            // ~ serializer.fromBytes
    node.step(Event::Message { from, msg });   // ~ actor.receive(src, message)
}
// On Ready: for each (to, msg) in ready.messages { socket.send(Codec::encode(&msg)) }
```

This means the deterministic simulator (§4) can skip the codec entirely and pass `Message`
values around by reference — faster, and it shrinks structured traces, not byte blobs. A
`Debug`/pretty impl on `Message` is the analog of `toPrettyString` for readable failures.

---

## 4. The property-based Simulator

### (a) What frankenpaxos does

`Simulator.simulate` runs many random schedules; after **every** step it checks invariants,
and on the first violation it **minimizes** the failing trace:

```scala
// shared/src/test/scala/simulator/Simulator.scala:28
  def simulate[Sim <: SimulatedSystem](
      sim: Sim,
      runLength: Int,
      numRuns: Int
  ): Option[BadHistory[Sim]] = {
    for (_ <- 1 to numRuns) {
      simulateOne(sim, runLength) match {
        case badHistory @ Some(_) => return badHistory
        case None                 =>
      }
    }

    None
  }
```

Minimization leans on ScalaCheck: it asserts "every *subsequence* of the failing run is a
*good* history" — which is false — and lets the shrinker hunt for the smallest subsequence
that still fails. Dropping events from the run is exactly how message loss / partial
delivery gets explored:

```scala
// shared/src/test/scala/simulator/Simulator.scala:43
  def minimize[Sim <: SimulatedSystem { type Command = C }, C](
      sim: Sim,
      seed: Long,
      run: Seq[C]
  ): Option[BadHistory[Sim]] = {
    // We check that every subrun of `run` is a good history (i.e. a history
    // that does not lead to an invariant violation). ... Scalacheck
    // will find a minimal subsequence of `run` that also violates the
    // invariant.
    val prop = Prop.forAll(Gen.someOf(run)) { subrun =>
      runOne[sim.type, sim.Command](sim, seed, subrun).isSuccess
    }
```

A protocol plugs in by implementing the `SimulatedSystem` trait: three associated types
(`System`, `State`, `Command`), a way to make/run/observe a system, a command generator,
and up to three invariant hooks:

```scala
// shared/src/test/scala/simulator/SimulatedSystem.scala:171
  def newSystem(seed: Long): System
  def getState(system: System): State
  def generateCommand(system: System): Option[Command]
  def runCommand(system: System, command: Command): System
  // ...
  def stateInvariantHolds(state: State): InvariantResult = InvariantHolds
  def stepInvariantHolds(oldState: State, newState: State): InvariantResult = InvariantHolds
  def historyInvariantHolds(history: Seq[State]): InvariantResult = InvariantHolds
```

**Concrete instance: MultiPaxos.** `SimulatedMultiPaxos` builds a full cluster (clients,
leaders, acceptors, replicas) all wired to a single shared `FakeTransport`, and defines
its observable `State` as each replica's executed log prefix:

```scala
// shared/src/test/scala/multipaxos/MultiPaxos.scala:204
  override type System = MultiPaxos
  // For every replica, we record the prefix of the log that has been executed.
  override type State = mutable.Buffer[Seq[CommandBatchOrNoop]]
  override type Command = SimulatedMultiPaxos.Command
```

Its `generateCommand` mixes **protocol-level** commands (client writes/reads) with
**transport-level** commands obtained from `FakeTransport.generateCommandWithFrequency` —
so a single random schedule interleaves "client proposes X" with "deliver message Y" and
"fire timer Z":

```scala
// shared/src/test/scala/multipaxos/MultiPaxos.scala:258
    FakeTransport
      .generateCommandWithFrequency(paxos.transport)
      .foreach({
        case (frequency, gen) =>
          subgens += frequency -> gen.map(TransportCommand(_))
      })

    val gen: Gen[Command] = Gen.frequency(subgens: _*)
    gen.apply(Gen.Parameters.default, Seed.random())
```

The safety invariants are the heart of it. The **state invariant** is Multi-Paxos
agreement, phrased as log compatibility — for any two replicas, one log is a prefix of the
other:

```scala
// shared/src/test/scala/multipaxos/MultiPaxos.scala:291
  override def stateInvariantHolds(
      state: State
  ): SimulatedSystem.InvariantResult = {
    for (logs <- state.combinations(2)) {
      val lhs = logs(0)
      val rhs = logs(1)
      if (!isPrefix(lhs, rhs) && !isPrefix(rhs, lhs)) {
        return SimulatedSystem.InvariantViolated(
          s"Logs $lhs and $rhs are not compatible."
        )
      }
    }

    SimulatedSystem.InvariantHolds
  }
```

…and the **step invariant** is stability — a replica's executed log only ever grows, never
rewrites a prefix:

```scala
// shared/src/test/scala/multipaxos/MultiPaxos.scala:307
  override def stepInvariantHolds(
      oldState: State,
      newState: State
  ): SimulatedSystem.InvariantResult = {
    for ((oldLog, newLog) <- oldState.zip(newState)) {
      if (!isPrefix(oldLog, newLog)) {
        return SimulatedSystem.InvariantViolated(
          s"Logs $oldLog is not a prefix of $newLog."
        )
      }
    }

    SimulatedSystem.InvariantHolds
  }
```

The actual test just wires it together across configurations (`f`, batched, flexible),
runs 500 schedules of up to 250 commands, and on failure prints the *minimized* trace:

```scala
// shared/src/test/scala/multipaxos/MultiPaxosTest.scala:20
      Simulator
        .simulate(sim, runLength = runLength, numRuns = numRuns)
        .flatMap(b => Simulator.minimize(sim, b.seed, b.history)) match {
        case Some(BadHistory(seed, history, throwable)) => {
          // ...
          fail(s"Seed: $seed\n$sw\n${sim.historyToString(history)}")
        }
        case None => {}
      }
```

The trace is rendered with the per-command `toPrettyString` from §3, so a failure reads as
"proposed X, delivered Prepare from Leader 1 to Acceptor 0.2, fired timer …".

### (b) The principle

Property-test a stateful distributed system as: **(1)** a generator of the next legal event
(protocol-level *and* schedule-level events drawn from the same distribution), **(2)** a
runner that applies one event, and **(3)** invariant predicates checked after *every* step
— state invariants (true of each state), step invariants (true of each transition), and
history invariants (true of the whole run). On the first violation, **shrink**: search for
the smallest subsequence of events that still violates the invariant, and report that
minimal trace plus its seed. Dropping events during shrinking is how you discover the
fault-injection (loss, partition) that breaks safety.

### (c) Maps to sans-IO Multi-Paxos

This is where a sans-IO core *wins*: it makes the `SimulatedSystem` trait nearly trivial,
because the core already has the exact `step`/observe shape the simulator wants.

- `System` ↔ a cluster of `PaxosNode`s plus a driver-held in-flight message set (the
  sans-IO analog of `FakeTransport.messages`).
- `Command` ↔ the same union frankenpaxos uses: client `Propose` events + transport events
  (`DeliverMessage`/`TriggerTimer`) — i.e. exactly the `Event` enum from §1, plus a "pick a
  pending message to deliver" command.
- `runCommand(system, cmd)` ↔ `node.step(event)` followed by draining `node.ready()` into
  the in-flight set. No `FakeTransport` indirection is needed — `step`/`ready` *is* the
  fake transport.
- `getState` ↔ read each node's chosen/executed log prefix.
- `stateInvariantHolds` / `stepInvariantHolds` ↔ port the two MultiPaxos invariants
  verbatim: any two logs are prefix-compatible (agreement); each log only grows
  (stability). These are the safety contract your core must never violate.

```rust
// A sans-IO sim driver is a few lines, because step/ready already are the model.
fn run_command(sim: &mut Cluster, cmd: Command) {
    match cmd {
        Command::Propose { node, c } => sim.nodes[node].step(Event::Propose(c)),
        Command::Deliver(msg)        => sim.nodes[msg.to].step(Event::Message { .. }),
        Command::Fire(t)             => sim.nodes[t.node].step(Event::Timeout(t.id)),
    }
    drain_ready_into_inflight(sim);          // ~ FakeTransport buffering
    check_invariants(sim);                    // logs prefix-compatible & monotone
}
```

---

## 5. The central inversion

This is the single most valuable takeaway.

In frankenpaxos, **the transport drives the actor**. `FakeTransport.deliverMessage` reaches
*into* the actor and calls `actor.receive(...)`; inside that callback the actor reaches
*back out* and calls `transport.send(...)` to emit follow-up messages. Control and I/O are
braided together at the actor's edge:

```scala
// shared/src/main/scala/frankenpaxos/FakeTransport.scala:151
        actor.receive(msg.src, actor.serializer.fromBytes(msg.bytes.to[Array]))
```

A sans-IO core inverts both arrows. **The caller drives the core.** Inbound events are
*fed* to a single `step(event)`; outbound side effects are *described* in a `Ready` struct
the caller drains and performs. The core calls nothing and blocks on nothing:

```rust
node.step(Event::Message { from, msg });  // caller feeds the event in
let ready = node.ready();                  // caller pulls the side effects out
for (to, m) in ready.messages { net.send(to, m); }
for arm in ready.timers       { clock.arm(arm); }
for c   in ready.committed    { app.apply(c);   }
```

The two structures line up cleanly:

| frankenpaxos (actor/transport)        | sans-IO Multi-Paxos (caller-driven)         |
| ------------------------------------- | ------------------------------------------- |
| `actor.receive(src, msg)`             | `node.step(Event::Message { from, msg })`   |
| timer callback fires                  | `node.step(Event::Timeout(id))`             |
| `transport.send(dst, bytes)` (eager)  | entry in `ready.messages` (deferred)        |
| `transport.timer(name, delay, f)`     | entry in `ready.timers` (caller owns clock) |
| `FakeTransport.messages` buffer       | driver-held in-flight set                   |
| `FakeTransport` (test-only)           | the **default** runtime                     |
| `NettyTcpTransport` (production)      | a thin driver: socket → `step`, `ready` → socket |

And here is the payoff. In frankenpaxos, `FakeTransport` is a *test double* — the real
system runs on Netty, and the deterministic simulator is a parallel apparatus that only
exists under `src/test`. In a sans-IO design **that relationship flips**: because the core
already buffers messages and never touches I/O, the deterministic simulator is just "drive
`step`, drain `Ready`" — it is the *natural*, always-available way to run the core. The
real network is the bolt-on: a thin event loop that reads a socket into `step` and writes
`Ready.messages` back out. You get frankenpaxos's killer feature — exhaustive, shrinkable,
deterministic schedule exploration of the actual production code path — without maintaining
a separate fake transport, because the production code path *is* the simulator.
