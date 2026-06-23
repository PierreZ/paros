# The MultiPaxos slot log — the heart of paros

This is the most important reference for **paros v1**. It extracts, from the
[frankenpaxos](https://github.com/mwhittaker/frankenpaxos) Scala codebase, the machinery
that turns single-decree Paxos into a replicated *log*.

The one-line framing: **MultiPaxos = single-decree Paxos (see the previous file) run
independently per log slot, plus the machinery to execute a log in order.** Each slot is
its own little consensus instance choosing one `CommandBatchOrNoop`; everything in this
document is the bookkeeping *around* those instances — assigning slots, filling holes,
executing the chosen prefix, deduplicating clients, and serving reads.

frankenpaxos is heavily *disaggregated*: leaders, proxy leaders, acceptors, replicas, and
batchers are separate actors that talk over a `Transport`. paros collapses all of these
into a single sans-IO core, so throughout we read across actor boundaries and reassemble
the logic into one `step(event) -> Ready` loop.

This document is **algorithm-faithful but architecture-translating**. Every section has
three parts:

- **(a) What frankenpaxos does** — a short verbatim Scala snippet, preceded by a comment
  with its `path:line`.
- **(b) The principle** — the language-neutral pattern worth copying.
- **(c) Maps to sans-IO Multi-Paxos** — how it lands in a Rust core that does **zero I/O**,
  driven by the caller through one `step(event)` entry point with side effects surfaced in a
  `Ready`-style struct. The replicated object is a log of slots, each holding a chosen value
  (`BTreeMap<Slot, Value>` plus an executed watermark).

> Line numbers are from the checkout this was written against and will drift; symbol names
> are stable.

---

## Table of contents

1. [The log of slots](#1-the-log-of-slots)
2. [In-order execution](#2-in-order-execution)
3. [Slot assignment & Phase-1 gap fill](#3-slot-assignment--phase-1-gap-fill)
4. [Watermarks & recovery](#4-watermarks--recovery)
5. [ClientTable (exactly-once)](#5-clienttable-exactly-once)
6. [StateMachine interface](#6-statemachine-interface)
7. [Quorum systems](#7-quorum-systems)
8. [Reads — linearizable / sequential / eventual](#8-reads--linearizable--sequential--eventual)

---

## 1. The log of slots

### (a) What frankenpaxos does

A replica's log is a **sparse slot → value map**, plus two integers that track progress. It
is *not* a contiguous array; a slot stays empty (`None`) until a `Chosen` message arrives
for it. The value type is `CommandBatchOrNoop` — a chosen slot holds either a batch of
client commands or a `Noop`.

```scala
// shared/src/main/scala/frankenpaxos/multipaxos/Replica.scala:191
// The log of commands. We implement the log as a BufferMap as opposed to
// something like a SortedMap for efficiency. `log` is public for testing.
@JSExport
val log =
  new util.BufferMap[CommandBatchOrNoop](options.logGrowSize)
```

```scala
// shared/src/main/scala/frankenpaxos/multipaxos/Replica.scala:213
// Every log entry less than `executedWatermark` has been executed. There may
// be commands larger than `executedWatermark` pending execution.
// `executedWatermark` is public for testing.
@JSExport
var executedWatermark: Int = 0

// The number of log entries that have been chosen and placed in `log`. We
// use `numChosen` and `executedWatermark` to know whether there are commands
// pending execution. If `numChosen == executedWatermark`, then all chosen
// commands have been executed. Otherwise, there are commands waiting to get
// executed.
@JSExport
protected var numChosen: Int = 0
```

The `BufferMap` itself is a growable ring with a watermark, so `get`/`put` are O(1) and the
prefix below the watermark can be reclaimed:

```scala
// shared/src/main/scala/frankenpaxos/util/BufferMap.scala:8
class BufferMap[V](val growSize: Int = 5000) {
  protected val buffer: mutable.Buffer[Option[V]] =
    mutable.Buffer.fill(growSize)(None)
  protected var watermark: Int = 0
  protected var largestKey: Int = -1
  private def normalize(key: Int): Int = key - watermark
```

The two counters carry distinct meaning, and the doc comment above spells out the
invariant: `numChosen` counts how many slots have a value, `executedWatermark` is the
length of the executed prefix, and `numChosen == executedWatermark` means "nothing is
pending." When they diverge there is a hole somewhere between the executed prefix and the
chosen slots above it (this is what arms the recover timer — see §4).

### (b) The principle

Represent the log as a **sparse map keyed by slot**, not a dense array, because slots get
chosen out of order (a later slot can be chosen before an earlier one). Track exactly two
scalars: a *chosen* count and an *executed watermark*. Their relationship (`chosen ==
executed`?) is the cheapest possible test for "is there pending work or a hole?" and drives
both execution and recovery. The value stored per slot must include a `Noop` variant: gap
filling (§3) needs a benign value to make a slot decidable without running a real command.

### (c) Maps to sans-IO Multi-Paxos

The core owns the log directly; no I/O is involved in mutating it.

```rust
/// One chosen entry per slot. A slot is absent until consensus picks a value.
pub enum Value {
    Noop,
    Batch(Vec<Command>),
}

pub struct Log {
    /// Sparse: slot -> chosen value. `BTreeMap` keeps slots ordered for the
    /// execute walk and makes the "first hole" query trivial.
    chosen: BTreeMap<Slot, Value>,
    /// Every slot `< executed_watermark` has been applied to the state machine.
    executed_watermark: Slot,
    /// How many slots have a chosen value. `num_chosen == executed_watermark`
    /// means the log is fully drained; a gap otherwise.
    num_chosen: u64,
}
```

A `BTreeMap<Slot, Value>` is the Rust analogue of the `BufferMap`; you can swap in a
ring-buffer-with-watermark later for the same O(1)/reclaimable properties, but the map is
the right shape to start. Note frankenpaxos splits the role of *acceptor* (which holds the
per-slot vote `State(voteRound, voteValue)`, the single-decree state from the previous
file) from *replica* (which holds only chosen values). paros keeps both in one core: the
per-slot Paxos state from file 02, and this chosen-value log on top.

---

## 2. In-order execution

### (a) What frankenpaxos does

The execute loop walks slots upward from `executedWatermark` and **stops at the first
hole**. This is the entire reason the protocol needs gap filling and recovery: a single
missing slot blocks everything above it.

```scala
// shared/src/main/scala/frankenpaxos/multipaxos/Replica.scala:394
private def executeLog(): ClientReplyBatch = {
  val clientReplies = mutable.Buffer[ClientReply]()

  while (true) {
    log.get(executedWatermark) match {
      case None =>
        // We have to execute the log in prefix order, so if we hit an empty
        // slot, we have to stop executing.
        return ClientReplyBatch(batch = clientReplies.toSeq)

      case Some(commandBatchOrNoop) =>
        val slot = executedWatermark
        executeCommandBatchOrNoop(slot, commandBatchOrNoop, clientReplies)
        // ... (deferred reads for this slot, then) ...
        executedWatermark += 1
```

A `Noop` slot is "executed" by simply advancing past it — it produces no client reply:

```scala
// shared/src/main/scala/frankenpaxos/multipaxos/Replica.scala:346
private def executeCommandBatchOrNoop(
    slot: Slot,
    commandBatchOrNoop: CommandBatchOrNoop,
    clientReplies: mutable.Buffer[ClientReply]
): Unit = {
  commandBatchOrNoop.value match {
    case CommandBatchOrNoop.Value.Noop(Noop()) =>
      metrics.executedLogEntriesTotal.labels("noop").inc()
    case CommandBatchOrNoop.Value.CommandBatch(batch) =>
      batch.command.foreach(executeCommand(slot, _, clientReplies))
      metrics.executedLogEntriesTotal.labels("command").inc()
    case CommandBatchOrNoop.Value.Empty =>
      logger.fatal("Empty CommandBatchOrNoop encountered.")
  }
}
```

`executeLog()` is called right after a slot is filled in `handleChosen`
(`Replica.scala:590`): on every `Chosen` message the replica re-runs the walk, which makes
progress only if the newly chosen slot happened to be the one plugging the hole at the
watermark.

### (b) The principle

Execution is a **prefix fold**: apply slots in order, halt at the first gap. Out-of-order
chosen slots accumulate but stay dormant until their prefix completes. The state machine
is therefore only ever fed a contiguous, monotonically-growing sequence — which is exactly
what makes it deterministic and what makes "applied index" a meaningful single number.
Filler values (Noops) must be skipped without side effects.

### (c) Maps to sans-IO Multi-Paxos

The core exposes "execute up to the first gap" and the **caller applies to the state
machine**. The core never calls `state_machine.run` during the walk in an I/O sense — it
hands the caller the ordered list of `(slot, value)` to apply, or (cleaner) calls a
caller-supplied `StateMachine` trait object (§6) that is pure compute.

```rust
impl Log {
    /// Advance the executed prefix as far as the chosen slots allow.
    /// Returns the slots newly applied this call, in order, for the caller to
    /// surface results to clients. Stops at the first hole.
    fn execute(&mut self, sm: &mut dyn StateMachine, out: &mut Ready) {
        while let Some(value) = self.chosen.get(&self.executed_watermark) {
            let slot = self.executed_watermark;
            match value {
                Value::Noop => {}                       // advance past, no reply
                Value::Batch(cmds) => self.apply_batch(slot, cmds, sm, out),
            }
            self.executed_watermark += 1;
        }
    }
}
```

`Ready` collects the client replies produced during the walk so the *caller* sends them;
the core just appends to a buffer. This mirrors etcd-raft's `CommittedEntries` (see the
go-raft analysis): the core decides *what* is applied, the driver does the application and
the network egress.

---

## 3. Slot assignment & Phase-1 gap fill

### (a) What frankenpaxos does

**Steady state — slot assignment.** The active leader hands out slots from a monotonic
counter. Notably it keeps *no log at all*: disaggregation means the leader only needs the
next index.

```scala
// shared/src/main/scala/frankenpaxos/multipaxos/Leader.scala:175
// The next available slot in the log. Even though we have a next slot into
// the log, you'll note that we don't even have a log! Because we've
// decoupled aggressively, leaders don't actually need a log at all.
@JSExport
protected var nextSlot: Slot = 0
```

Each client request batch is proposed at `nextSlot`, then the counter advances
(`Leader.scala:367` builds the `Phase2a`; `Leader.scala:406` does `nextSlot += 1`).

**Leader recovery — Phase-1 gap fill.** When a new leader runs Phase 1, acceptors return
their votes for every slot at or above the leader's `chosenWatermark`:

```scala
// shared/src/main/scala/frankenpaxos/multipaxos/Acceptor.scala:166
round = phase1a.round
val phase1b = Phase1b(
  groupIndex = groupIndex,
  acceptorIndex = index,
  round = round,
  info = states
    .iteratorFrom(phase1a.chosenWatermark)
    .map({
      case (slot, state) =>
        Phase1bSlotInfo(slot = slot,
                        voteRound = state.voteRound,
                        voteValue = state.voteValue)
    })
    .toSeq
)
leader.send(LeaderInbound().withPhase1B(phase1b))
```

Once the leader has a read quorum of `Phase1b`s, it finds the largest voted slot and, for
**every** slot from `chosenWatermark` up to that max, proposes a safe value — re-proposing
a possibly-chosen value where one exists, and a `Noop` where the slot is empty:

```scala
// shared/src/main/scala/frankenpaxos/multipaxos/Leader.scala:541
// Find the largest slot with a vote.
val maxSlot = {
  for {
    groupPhase1bs <- phase1.phase1bs
    phase1b <- groupPhase1bs.values
  } yield maxPhase1bSlot(phase1b)
}.max

// Now, we iterate from chosenWatermark to maxSlot proposing safe
// values to fill in the log.
for (slot <- chosenWatermark to maxSlot) {
  val group = phase1.phase1bs(slot % config.numAcceptorGroups)
  getProxyLeader().send(
    ProxyLeaderInbound().withPhase2A(
      Phase2a(
        slot = slot,
        round = round,
        commandBatchOrNoop = safeValue(group.values, slot)
      )
    )
  )
}

// We've filled in every slot until and including maxSlot, so the next
// slot is maxSlot + 1.
nextSlot = maxSlot + 1
```

The "safe value" rule is the classic single-decree Phase-2a value choice, applied
per-slot: highest-vote-round value if any acceptor voted in this slot, otherwise `Noop`.

```scala
// shared/src/main/scala/frankenpaxos/multipaxos/Leader.scala:314
// Given a quorum of Phase1b messages, `safeValue` finds a value that is safe
// to propose in a particular slot. If the Phase1b messages have at least one
// vote in the slot, then the value with the highest vote round is safe.
// Otherwise, everything is safe. In this case, we return Noop.
private def safeValue(
    phase1bs: Iterable[Phase1b],
    slot: Slot
): CommandBatchOrNoop = {
  val slotInfos =
    phase1bs.flatMap(phase1b => phase1b.info.find(_.slot == slot))
  if (slotInfos.isEmpty) {
    CommandBatchOrNoop().withNoop(Noop())
  } else {
    slotInfos.maxBy(_.voteRound).voteValue
  }
}
```

### (b) The principle

Two distinct jobs share one slot counter. In **steady state** the leader just assigns the
next slot to each incoming proposal. On **leader recovery**, correctness requires the new
leader to re-propose *any value that might already have been chosen* in any slot it doesn't
yet know is committed, and to **fill every hole below the highest voted slot with a Noop**
so that execution (§2) can make progress past them. The `chosenWatermark` lets recovery
skip the already-committed prefix, so Phase 1 only re-derives the tail. The per-slot
safe-value choice (highest vote round, else Noop) is single-decree Paxos applied
independently to each slot — exactly the "run Paxos per slot" framing.

### (c) Maps to sans-IO Multi-Paxos

`next_slot` is a field; assigning a proposal a slot and pushing a `Phase2a` onto the
outbound buffer happens inside `step`. Gap fill is a pure function over the collected
Phase-1 votes:

```rust
/// After a read-quorum of Phase1b replies, re-derive the safe value for every
/// slot in [chosen_watermark, max_voted_slot] and emit a Phase2a for each.
fn on_phase1_quorum(&mut self, votes: &Phase1Votes, ready: &mut Ready) {
    let max_slot = votes.max_voted_slot(); // -1 if none
    for slot in self.chosen_watermark..=max_slot {
        let value = match votes.highest_vote_for(slot) {
            Some(v) => v,            // possibly-chosen: must re-propose it
            None    => Value::Noop,  // hole: fill so execution can pass it
        };
        ready.send.push(Message::Phase2a { slot, round: self.round, value });
        self.next_slot = max_slot + 1;
    }
}
```

This is the only place the core *manufactures* values (Noops) rather than proposing client
input — it is the liveness/safety bridge that makes the per-slot instances composable into
an executable log. The vote collection itself reuses the single-decree quorum logic from
file 02, per slot.

---

## 4. Watermarks & recovery

### (a) What frankenpaxos does

**ChosenWatermark — garbage-collect the committed prefix.** Replicas periodically tell
leaders how far the contiguous chosen prefix reaches, so a freshly elected leader can skip
Phase 1 over it. The send is round-robined across replicas and fired from the execute loop:

```scala
// shared/src/main/scala/frankenpaxos/multipaxos/Replica.scala:415
// Replicas send a ChosenWatermark message to the leaders every
// `options.sendChosenWatermarkEveryNEntries` command entries. The
// responsibility of sending the ChosenWatermark message is
// distributed round robin across the replicas.
// ...
val mod = executedWatermark % options.sendChosenWatermarkEveryNEntries
val div = executedWatermark / options.sendChosenWatermarkEveryNEntries
if (mod == 0 && div % config.numReplicas == index) {
  // ... send ChosenWatermark(slot = executedWatermark) ...
  metrics.chosenWatermarksSentTotal.inc()
}
```

The leader simply takes the max it has heard (`chosenWatermark` is what Phase-1a carries in
§3):

```scala
// shared/src/main/scala/frankenpaxos/multipaxos/Leader.scala:699
private def handleChosenWatermark(
    src: Transport#Address,
    msg: ChosenWatermark
): Unit = {
  chosenWatermark = Math.max(chosenWatermark, msg.slot)
}
```

```protobuf
// shared/src/main/scala/frankenpaxos/multipaxos/MultiPaxos.proto:462
message ChosenWatermark {
  // Replicas periodically send ChosenWatermark messages to the leaders
  // informing them that every log entry smaller than `slot` has been chosen.
  // For example, if `slot` is 3, then slots 0, 1, and 2 have been chosen.
  // Slots above `slot` may also be chosen, but that's okay.
  required int32 slot = 1;
}
```

**Recover — liveness for a stuck hole.** If a replica has a hole at its watermark for too
long (a randomized timer, `Replica.scala:239`, sending `Recover(slot = executedWatermark)`),
it asks the leader to make sure that slot gets chosen. The leader's response is just to run
a leader change / Phase 1, which by §3 fills *all* holes below the highest voted slot:

```scala
// shared/src/main/scala/frankenpaxos/multipaxos/Leader.scala:706
private def handleRecover(
    src: Transport#Address,
    recover: Recover
): Unit = {
  // Note that we don't actually use `recover.slot` anywhere. This is
  // actually ok. If there is a slot in the log that needs recovering, then
  // some slot above that slot was chosen. This means that when the leader
  // runs phase 1, it will see this larger slot and recover all instances
  // below it, including `recover.slot`.
  state match {
    case Inactive =>
    // Do nothing. The active leader will recover.
    case _: Phase1 | _: Phase2 =>
      // Leader change to make sure the slot is chosen.
      leaderChange(isNewLeader = true)
  }
}
```

```protobuf
// shared/src/main/scala/frankenpaxos/multipaxos/MultiPaxos.proto:477
message Recover {
  // Replicas execute logs in prefix order. Thus, if the log permanently has a
  // hole in it, the algorithm remains forever blocked. To solve this, if a
  // replica notices a hole in its log for a certain amount of time, it sends a
  // Recover message to the leader to get the hole plugged.
  required int32 slot = 1;
}
```

### (b) The principle

Two complementary watermarks make the log finite and live. **ChosenWatermark** flows
*replica → leader*: it bounds Phase-1 work and lets everyone garbage-collect the committed
prefix. **Recover** flows *replica → leader* as a liveness backstop: a hole that lingers
triggers a Phase 1, and because gap fill (§3) plugs every hole below the max voted slot, a
single recover heals the whole stuck tail. Critically, `recover.slot` is advisory — running
Phase 1 at all is what fixes it — which keeps the recovery path stateless and idempotent.

### (c) Maps to sans-IO Multi-Paxos

Both are caller-driven inputs/outputs; the timer is *not* in the core.

- **Time is a tick.** The recover timer becomes `step(Event::Tick)` plus a deadline the
  core tracks; when a hole at the watermark has persisted past the deadline, `step` emits a
  `Recover` (or directly a "start Phase 1") into `Ready.send`. The randomized period is the
  caller's concern; the core just exposes "should I recover?" as deterministic logic over
  `num_chosen != executed_watermark`.
- **ChosenWatermark out.** When the executed watermark crosses a configured stride, the
  execute walk (§2) pushes a `ChosenWatermark { slot: executed_watermark }` onto
  `Ready.send`.
- **ChosenWatermark / Recover in.** `step(Event::Message(ChosenWatermark))` does
  `self.chosen_watermark = self.chosen_watermark.max(slot)`. `step(Event::Message(Recover))`
  triggers the Phase-1 path if this node is (or becomes) leader; the carried slot is
  ignored, exactly as above.

```rust
match event {
    Event::Tick => self.maybe_emit_recover(ready),
    Event::Message(Msg::ChosenWatermark { slot }) =>
        self.chosen_watermark = self.chosen_watermark.max(slot),
    Event::Message(Msg::Recover { .. }) => self.start_phase1_if_leader(ready),
    // ...
}
```

---

## 5. ClientTable (exactly-once)

### (a) What frankenpaxos does

A state machine replication protocol promises linearizability: each client request is
executed **exactly once**, even though a client may *re-send* it after a timeout. The
dedup structure is a *client table*.

For MultiPaxos specifically — where the log is totally ordered and executed strictly in
slot order — a **single max-id per client suffices**, and the Replica uses a plain map (it
explicitly notes it does *not* need the heavier `ClientTable` class):

```scala
// shared/src/main/scala/frankenpaxos/multipaxos/Replica.scala:226
// The client table used to ensure exactly once execution semantics. Every
// entry in the client table is keyed by a clients address and its pseudonym
// and maps to the largest executed id for the client and the result of
// executing the command. Note that unlike with generalized protocols like
// BPaxos and EPaxos, we don't need to use the more complex ClientTable
// class. A simple map suffices.
@JSExport
protected var clientTable =
  mutable.Map[(ByteString, ClientPseudonym), (ClientId, ByteString)]()
```

The dedup logic at execution time: unseen client → run and cache; same id as cached →
re-send the cached result (a retry); smaller id → stale, drop; larger id → run and update.

```scala
// shared/src/main/scala/frankenpaxos/multipaxos/Replica.scala:312
clientTable.get(clientIdentity) match {
  case None =>
    val result =
      ByteString.copyFrom(stateMachine.run(command.command.toByteArray()))
    clientTable(clientIdentity) = (commandId.clientId, result)
    // ... maybe append ClientReply ...

  case Some((largestClientId, cachedResult)) =>
    if (commandId.clientId < largestClientId) {
      metrics.reduntantlyExecutedCommandsTotal.inc()
    } else if (commandId.clientId == largestClientId) {
      clientReplies += ClientReply(commandId = commandId,
                                   slot = slot,
                                   result = cachedResult)
      metrics.reduntantlyExecutedCommandsTotal.inc()
    } else {
      val result =
        ByteString.copyFrom(stateMachine.run(command.command.toByteArray()))
      clientTable(clientIdentity) = (commandId.clientId, result)
      // ... maybe append ClientReply ...
    }
}
```

**The key insight — why a single max-id is *not* always enough.** The general-purpose
`clienttable/ClientTable.scala` exists precisely for protocols (EPaxos, Generalized Paxos)
that execute commands **out of order**. Its doc comment is the clearest statement of the
problem:

```scala
// shared/src/main/scala/frankenpaxos/clienttable/ClientTable.scala:42
// For protocols like EPaxos or Generalized Paxos, things are more complicated.
// Consider the following scenario:
//
//   - A client issues command x with id 0.
//   - x is committed by the protocol.
//   - Replica A executes x and sends the result to the client.
//   - The client receives the result.
//   - The client issues unconflicting command y with id 1.
//   - y is committed by the protocol.
//   - Replica B learns that y is committed and executes it, then sends a reply
//     to the client.
//
// Note that B executed y (id 1) before x (id 0)! Unlike with VRR or Raft,
// replicas may execute commands out of command id order. Thus, B cannot simply
// record the largest client id executed for every client. If it did, it would
// never execute x because x has a smaller id than y.
```

So that table tracks the cached result for the *largest* id **plus the full set of executed
ids**, as an `IntPrefixSet` (compact because executed ids are usually a dense prefix):

```scala
// shared/src/main/scala/frankenpaxos/clienttable/ClientTable.scala:98
case class ClientState[Output](
    largestId: ClientId,
    largestOutput: Output,
    executedIds: IntPrefixSet
)
```

```scala
// shared/src/main/scala/frankenpaxos/clienttable/ClientTable.scala:147
def executed(
    clientAddress: ClientAddress,
    clientId: ClientId
): ClientTable.ExecutedResult[Output] = {
  table.get(clientAddress) match {
    case None => ClientTable.NotExecuted
    case Some(state) =>
      if (clientId == state.largestId) {
        ClientTable.Executed(Some(state.largestOutput))
      } else if (state.executedIds.contains(clientId)) {
        ClientTable.Executed(None)
      } else {
        ClientTable.NotExecuted
      }
  }
}
```

### (b) The principle

De-duplication is part of the *deterministic state* of the replicated machine, not an I/O
concern — every replica must reach the same dedup verdict for the same command, so it has to
live in the core. *Which* structure you need depends on execution order: a totally ordered
log (MultiPaxos) executes each client's commands in id order, so a single max-executed-id +
its cached result is sufficient. A generalized protocol can execute a higher id *before* a
lower one, so a single max-id would permanently shadow the lower command; you must track the
**set** of executed ids (compactly, since it is usually a contiguous prefix). The cached
result for the latest id exists so that client *retries* get an answer without re-running
the command.

### (c) Maps to sans-IO Multi-Paxos

paros v1 has a totally ordered log, so the **simple max-id table is correct** — but model
it as a trait so a future generalized variant can swap in the set-based one without
changing call sites. Dedup runs inside the execute walk (§2), before the state machine is
invoked, and its decision (run / replay cached / drop) is fully deterministic.

```rust
struct ClientEntry { largest_id: ClientId, cached: Vec<u8> }

enum Dedup { Fresh, Replay(Vec<u8>), Stale }

impl ClientTable {
    fn check(&self, id: ClientId, who: &ClientKey) -> Dedup {
        match self.0.get(who) {
            None => Dedup::Fresh,
            Some(e) if id == e.largest_id => Dedup::Replay(e.cached.clone()),
            Some(e) if id < e.largest_id  => Dedup::Stale,
            Some(_)                       => Dedup::Fresh, // id > largest
        }
    }
}
```

The cached result feeds a `ClientReply` into `Ready` on a retry without touching the state
machine — the core decides, the caller transmits.

---

## 6. StateMachine interface

### (a) What frankenpaxos does

The state machine is a **deterministic, pure-compute** object the protocol calls. It has no
knowledge of the log, slots, or the network — `run(input) -> output`, plus
conflict-detection and snapshot hooks.

```scala
// shared/src/main/scala/frankenpaxos/statemachine/StateMachine.scala:11
trait StateMachine {
  // `run(input)` executes a state machine command. The state machine
  // transitions to a new state and outputs an output.
  def run(input: Array[Byte]): Array[Byte]

  // `conflicts(x, y)` returns whether commands x and y conflict. ...
  def conflicts(firstCommand: Array[Byte], secondCommand: Array[Byte]): Boolean

  // `toBytes` returns a snapshot of the state machine ...
  def toBytes(): Array[Byte]

  // `fromBytes(snapshot)` reads in a snapshot produced by `toBytes`. ...
  def fromBytes(snapshot: Array[Byte]): Unit

  // Returns a conflict index. ...
  def conflictIndex[Key](): ConflictIndex[Key, Array[Byte]]
  // ...
}
```

A concrete instance — the key/value store — is just an in-memory map; `run` dispatches a
decoded request and returns a decoded reply, and `conflicts` is true only when two
operations touch overlapping keys with at least one write:

```scala
// shared/src/main/scala/frankenpaxos/statemachine/KeyValueStore.scala:48
override def typedRun(input: KeyValueStoreInput): KeyValueStoreOutput = {
  import KeyValueStoreInput.Request
  input.request match {
    case Request.GetRequest(GetRequest(keys)) =>
      KeyValueStoreOutput().withGetReply(
        GetReply(keys.map(k => GetKeyValuePair(k, kvs.get(k))))
      )

    case Request.SetRequest(SetRequest(keyValues)) =>
      keyValues.foreach({ case SetKeyValuePair(k, v) => kvs(k) = v })
      KeyValueStoreOutput().withSetReply(SetReply())

    case Request.Empty =>
      throw new IllegalStateException()
  }
}
```

```scala
// shared/src/main/scala/frankenpaxos/statemachine/KeyValueStore.scala:77
override def typedConflicts(
    firstCommand: KeyValueStoreInput,
    secondCommand: KeyValueStoreInput
): Boolean = {
  import KeyValueStoreInput.Request
  (firstCommand.request, secondCommand.request) match {
    case (Request.GetRequest(_), Request.GetRequest(_)) =>
      false // Get requests do not conflict.
    case (Request.SetRequest(_), Request.GetRequest(_)) |
        (Request.GetRequest(_), Request.SetRequest(_)) |
        (Request.SetRequest(_), Request.SetRequest(_)) =>
      keys(firstCommand).intersect(keys(secondCommand)).nonEmpty
    case (Request.Empty, _) | (_, Request.Empty) =>
      throw new IllegalStateException()
  }
}
```

### (b) The principle

The application state machine is a **caller-supplied, deterministic function** behind a
narrow interface: apply a command, snapshot, restore. `run` must be a pure function of
(current state, input) so that every replica applying the same prefix reaches the same
state. `conflicts` and the conflict index are only needed by *generalized* protocols (which
order non-conflicting commands concurrently); a classic MultiPaxos core never calls them.
Snapshot (`toBytes`/`fromBytes`) is the seam for log compaction and for catching up a lagging
replica.

### (c) Maps to sans-IO Multi-Paxos

This becomes a Rust trait the **caller implements**; the core holds a `&mut dyn StateMachine`
and calls `apply` during the execute walk (§2). The core itself does no I/O — applying a
command is in-memory compute, and persisting/snapshotting is surfaced to the driver.

```rust
pub trait StateMachine {
    /// Deterministic: same (state, input) -> same (state', output) on every replica.
    fn apply(&mut self, input: &[u8]) -> Vec<u8>;

    /// Snapshot for compaction / catch-up.
    fn snapshot(&self) -> Vec<u8>;
    fn restore(&mut self, snapshot: &[u8]);

    /// Only needed if/when paros grows a generalized (out-of-order) mode.
    fn conflicts(&self, _a: &[u8], _b: &[u8]) -> bool { true }
}
```

For v1, `conflicts` can default to "always conflict" (i.e. fully serialize) since MultiPaxos
imposes a total order anyway — keep the method so the trait is forward-compatible, but don't
build the conflict-index machinery yet.

---

## 7. Quorum systems

### (a) What frankenpaxos does

MultiPaxos does not hard-code "majority." It is parameterized over a **read-write quorum
system**: any pair (read quorum, write quorum) that is guaranteed to intersect.

```scala
// shared/src/main/scala/frankenpaxos/quorums/QuorumSystem.scala:3
// A _quorum system_ is a set X along with a set of subsets Q of X such that
// for every p, q in Q, p and q intersect [2]. A read-write quorum system is a
// set X along with two sets of subsets R and W of X such that for every r in R
// and for every w in W, r and w intersect.
// ...
// MultiPaxos really only requires a read-write quorum system. Here, we codify
// read-write quorum systems, but call them just `QuorumSystem`s.
trait QuorumSystem[T] {
  def nodes(): Set[T]
  def randomReadQuorum(): Set[T]
  def randomWriteQuorum(): Set[T]
  def isReadQuorum(nodes: Set[T]): Boolean
  def isWriteQuorum(nodes: Set[T]): Boolean
  def isSuperSetOfReadQuorum(nodes: Set[T]): Boolean
  def isSuperSetOfWriteQuorum(nodes: Set[T]): Boolean
}
```

In Paxos terms, **Phase 1 needs a read quorum and Phase 2 needs a write quorum**, and any
read quorum intersects any write quorum (so a chosen value is always visible to the next
leader's Phase 1). The `Grid` system (Flexible Paxos) makes this concrete and asymmetric:
arrange acceptors in a grid, a *row* is a read quorum, a *column* is a write quorum — every
row and column intersect in exactly one cell.

```scala
// shared/src/main/scala/frankenpaxos/quorums/Grid.scala:3
// A Grid quorum system arranges nodes into an n x m grid. Every row is a read
// quorum, and one entry from every row is a write column.
class Grid[T](
    private[quorums] val grid: Seq[Seq[T]],
    seed: Long = System.currentTimeMillis
) extends QuorumSystem[T] {
  // ...
  override def randomReadQuorum(): Set[T] = gridSeqSet(rand.nextInt(grid.size))

  override def randomWriteQuorum(): Set[T] = {
    val i = rand.nextInt(grid(0).size)
    grid.map(row => row(i)).toSet
  }

  override def isReadQuorum(xs: Set[T]): Boolean = {
    require(xs.subsetOf(nodes), /* ... */)
    gridSetSet.exists(row => row.subsetOf(xs))
  }

  override def isWriteQuorum(xs: Set[T]): Boolean = {
    require(xs.subsetOf(nodes), /* ... */)
    gridSetSet.forall(row => row.exists(x => xs.contains(x)))
  }
}
```

The leader uses exactly these abstractions: `grid.randomReadQuorum()` to pick whom to send
Phase-1a to (`Leader.scala:419`) and `grid.isReadQuorum(...)` to decide when it has enough
Phase-1b replies (`Leader.scala:536`).

### (b) The principle

The only property Paxos needs from its quorums is **read-write intersection**: every Phase-1
(read) quorum must overlap every Phase-2 (write) quorum, guaranteeing that a value chosen by
a write quorum is observed by the next leader's read quorum. Simple majority is just the
special case where read = write = ⌈(n+1)/2⌉. Flexible/grid quorums trade a larger read
quorum for a smaller write quorum (cheaper steady-state commits, costlier recovery), so the
quorum system should be a **pluggable policy**, not a constant.

### (c) Maps to sans-IO Multi-Paxos

A trait the core consults to (a) decide whom to address and (b) test whether a collected set
of responders constitutes a quorum. It is pure logic — no I/O — so it lives comfortably in
the core and is trivially deterministic for simulation.

```rust
pub trait QuorumSystem {
    /// Membership for "do I have enough Phase-1b replies?"
    fn is_read_quorum(&self, responders: &BTreeSet<NodeId>) -> bool;
    /// ... and "enough Phase-2b acks?"
    fn is_write_quorum(&self, responders: &BTreeSet<NodeId>) -> bool;
}

pub struct Majority { n: usize }
pub struct Grid { rows: Vec<Vec<NodeId>> } // read = a row, write = a column
```

Start with `Majority` for v1; the trait keeps `Grid` (Flexible Paxos) a drop-in addition.
Quorum counting from file 02's single-decree logic now flows through this trait instead of a
hard-coded `> n/2`.

---

## 8. Reads — linearizable / sequential / eventual

### (a) What frankenpaxos does

frankenpaxos offers **three read tiers**, trading consistency for cost. The protocol-level
contract is documented in the proto, and the replica implements all three through one
"deferred read" mechanism keyed by slot.

**Linearizable** — the client probes a quorum of acceptors for their max voted slot, takes
the max `i`, and reads at slot `i + n - 1` (n = number of acceptor groups):

```protobuf
// shared/src/main/scala/frankenpaxos/multipaxos/MultiPaxos.proto:36
// ## Linearizable Reads
//
// We implement scalable linearizable reads using a modification of the
// technique described in [2]. First, a client contacts a quorum of some
// acceptor group. Each replica responds with the largest log entry in which
// its voted. The client computes the maximum such log entry, call it i, and
// then issues a read to any replica at log entry i + n - 1 where n is the
// number of acceptor groups.
```

The acceptor's contribution is just exposing its max voted slot (`Acceptor.scala:222`,
`handleMaxSlotRequest`, returning `maxVotedSlot`).

**Sequential** — no acceptor round-trip; the read carries the client's own last-seen slot
and simply waits for the replica to execute that far:

```protobuf
// shared/src/main/scala/frankenpaxos/multipaxos/MultiPaxos.proto:67
// ## Sequentially Consistent Reads
//
// To implement sequentially consistent reads, we have to ensure that
// every client's read is at a log entry larger than any previous read or
// write. To ensure this, writes and reads all return the log entry in which
// they occur. Future reads wait to occur after the largest such log entry.
// Note that sequentially consistent reads do not have to contact the
// acceptors.
```

**Eventual** — read any replica's current state immediately, no waiting at all:

```protobuf
// shared/src/main/scala/frankenpaxos/multipaxos/MultiPaxos.proto:96
// ## Eventually Consistent Reads
//
// We call an "eventually consistent read" any read that takes place on some
// prefix of the committed log entries. To implement an eventually consistent
// read, a client sends a read to any replica, and the replica executes the
// read immediately. Simple as that.
```

All three reach the replica as messages; linearizable and sequential share the **defer**
path — if the target slot is not yet executed, the read is parked in a per-slot buffer and
run later by the execute loop (§2):

```scala
// shared/src/main/scala/frankenpaxos/multipaxos/Replica.scala:455
private def handleDeferrableRead(
    src: Transport#Address,
    slot: Int,
    command: Command
): Unit = {
  // We have to wait for `slot` to be executed before we execute the read. If
  // `slot >= executedWatermark`, then the slot hasn't yet been executed and
  // we have to defer the read to later.
  if (slot >= executedWatermark) {
    val read =
      DeferredRead(command = command, startTimeNanos = System.nanoTime)
    deferredReads.get(slot) match {
      case None             => deferredReads.put(slot, mutable.Buffer(read))
      case Some(otherReads) => otherReads += read
    }
    metrics.deferredReadsTotal.inc()
    return
  }

  val client = chan[Client[Transport]](src, Client.serializer)
  client.send(ClientInbound().withReadReply(executeRead(command)))
}
```

Eventual reads skip the defer logic entirely and execute on the spot:

```scala
// shared/src/main/scala/frankenpaxos/multipaxos/Replica.scala:645
private def handleEventualReadRequest(
    src: Transport#Address,
    eventualReadRequest: EventualReadRequest
): Unit = {
  val client = chan[Client[Transport]](src, Client.serializer)
  client.send(
    ClientInbound()
      .withReadReply(executeRead(eventualReadRequest.command))
  )
}
```

The parked reads are drained inside the execute walk, right after the owning slot executes:

```scala
// shared/src/main/scala/frankenpaxos/multipaxos/Replica.scala:407
deferredReads.get(slot) match {
  case None =>
  case Some(reads) =>
    processDeferredReads(reads)
    metrics.deferredReadBatchSize.observe(reads.size)
}
```

Note the read reply reports the slot it observed (`executedWatermark - 1`,
`Replica.scala:526`) — that returned slot is what lets sequential reads chain.

### (b) The principle

Reads come in **three cost/consistency tiers** built on one primitive — "execute this read
at (or after) slot `s`":

- **Linearizable**: `s` = the max voted slot across a quorum of acceptors (a quorum probe
  guarantees you see every write that *could* have been chosen before the read began), then
  defer until executed.
- **Sequential**: `s` = the client's own last-write/last-read slot (monotonic per client, no
  quorum needed) — cheaper, gives "read your writes / no time travel" per client.
- **Eventual**: `s` = now; read whatever prefix the replica has executed — cheapest, no
  ordering guarantee.

A read that targets an unexecuted slot is **deferred** (parked per-slot) and run by the same
in-order execution loop, which is why reads slot naturally into the log model without a
separate code path.

### (c) Maps to sans-IO Multi-Paxos

All three are expressible as core inputs/outputs over the slot log; none require the core to
do I/O.

- A read is `step(Event::Read { tier, slot, command })`. The core either executes it
  immediately (target slot already below `executed_watermark`) and pushes a `ReadReply` into
  `Ready`, or parks it in a `BTreeMap<Slot, Vec<DeferredRead>>` and the execute walk (§2)
  drains it when that slot is applied.
- **Linearizable** needs a quorum max-slot probe. The core emits `MaxSlotRequest`s in
  `Ready.send`, collects `MaxSlotReply`s through `step`, computes the target slot, then
  defers — i.e. the probe is just more messages through the same loop.
- **Sequential** carries the client's last slot in the event; the core defers to it without
  any outbound probe.
- **Eventual** executes against the current `executed_watermark` immediately.

```rust
pub enum ReadTier { Linearizable, Sequential, Eventual }

fn on_read(&mut self, tier: ReadTier, slot: Slot, cmd: Command, ready: &mut Ready) {
    if slot < self.log.executed_watermark {
        ready.read_replies.push(self.execute_read(cmd)); // run now
    } else {
        self.deferred_reads.entry(slot).or_default().push(cmd); // park
    }
}
```

The reply carries the observed slot (`executed_watermark - 1`) so a client can feed it back
as the `slot` of its next sequential read — the chaining mechanism, expressed purely as
data in/out.

---

## File map / suggested reading order

| Concern                         | frankenpaxos source                                              |
|---------------------------------|-----------------------------------------------------------------|
| Log, execution, dedup, reads    | `multipaxos/Replica.scala`                                       |
| Slot assignment, gap fill, recovery trigger | `multipaxos/Leader.scala`                           |
| Per-slot Phase-1/Phase-2 votes  | `multipaxos/Acceptor.scala`                                      |
| Wire contract & read tiers      | `multipaxos/MultiPaxos.proto`                                    |
| Sparse slot storage             | `util/BufferMap.scala`                                           |
| Out-of-order dedup (generalized)| `clienttable/ClientTable.scala`                                  |
| Application interface           | `statemachine/StateMachine.scala`, `statemachine/KeyValueStore.scala` |
| Quorum policy                   | `quorums/QuorumSystem.scala`, `quorums/Grid.scala`              |

Read `Replica.scala` first (it is the whole log lifecycle), then `Leader.scala`'s
`handlePhase1b` (gap fill is the subtle, correctness-critical part), then the proto for the
message contract. The single-decree per-slot mechanics live in the previous file; this one
is everything *around* them.
