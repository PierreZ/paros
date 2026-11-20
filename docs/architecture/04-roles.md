[← 03-8-identifiers-checkpoints-recovery-security.md](03-8-identifiers-checkpoints-recovery-security.md) · [Index](README.md) · [05-failure-model-and-zones.md →](05-failure-model-and-zones.md)

## 4. Roles

All six roles of Compartmentalized Paxos, plus the matchmakers of Matchmaker Paxos, each
placed by the cell from the counts that a person typed for a journal (rule 7, section 3.4):

| Role | Class | In paros |
|---|---|---|
| Leader (proposer) | storage | `Proposer` inside `ColocatedNode` |
| Proxy leader | stateless | `ProxyLeader`; booked per journal by `proxies` (1 by default, decision 22) and routed from the live registry |
| Acceptor, grid quorums | storage | `Acceptor`, `QuorumSystem::Majority` or `QuorumSystem::Grid` (decision 27) |
| Replica | storage | `ReplicaNode`; booked per journal by `replicas` (1 by default); serves `Read` |
| Batcher | stateless | `paros_consensus::Batcher`, `paros::roles::batcher`; booked per journal by `batchers` (1 by default); multi-writer journals only (section 2.4) |
| Unbatcher | stateless | `paros_consensus::Unbatcher`, in the batcher's process (section 2.4) |
| Matchmaker | storage | `Matchmaker`, `run_matchmaker`; one logical set per tenant, processes shared; holds the register of rule 1 for each journal of its tenant |
| Frontend (the entry role) | frontend | `paros::roles::frontend`; served by the machines that the operator gives the `frontend` role (decision 15, question 6) and named by `Resolve`; authorizes, resolves names and forwards each call to the role that serves it (section 3.5) |
| Resolver | frontend | universe level, shared by every tenant; redirects a client to its namespace's cell, never carries data (section 3.5); served beside the frontend |
| Coordinator (universe, cell, tenant) | none | not a role of a machine: any machine of the cell that proved it serves can win the term with `SetLeader`, and a stateless machine tries first (decisions 1, 7 and 15, question 5; section 3.3) |

### 4.1 How the pieces fit

Two papers, one per concern, both in `docs/references/papers/`. **Compartmentalized Paxos**
(Whittaker et al.) splits the leader's work into roles that scale on their own; **Matchmaker
Paxos** (Whittaker et al.) lets the acceptor set change without stopping. paros runs both per
journal, and the journal's leader-uuid rules (section 2) sit on top, judged at apply.

**The write path.** The leader only *sequences*: it gives each command a slot and hands it off.
A proxy leader does the Phase-2 work for that slot, so the leader's CPU and network stop being the
bottleneck. Acceptors may form acceptor partitions, so a Phase-2 quorum is a majority of one
acceptor partition and not a majority of all (#532 (acceptor partitions)).
Replicas learn chosen slots, walk the contiguous prefix and apply them; that apply is where the
journal state machine judges each `Write`, `Truncate` and `SetLeader`. Batchers and unbatchers
(multi-writer journals only, section 2.4) group many clients' writes into one slot and fan the
answers back out.

```
 clients ──► batchers ──► LEADER ──────► proxy leaders ──► acceptors (grid)
   ▲         (opt-in)    (Proposer:      (Phase 2a for      ┌────┬────┬────┐
   │                      slot = next)    one slot)         │ a1 │ a2 │ a3 │  Phase 2 quorum
   │                                         ▲              ├────┼────┼────┤  = one column
   │                                         │ 2b acks      │ a4 │ a5 │ a6 │  Phase 1 quorum
   │                                         └──────────────└────┴────┴────┘  = one row
   │                                         │ chosen(slot)
   │                                         ▼
   └──── unbatchers ◄──── replicas: walk the chosen prefix, apply in slot order
          (opt-in)        (journal state machine: leader uuid, expected_seq, first_seq)
```

**The read path.** A read never touches the leader (Compartmentalized Paxos §3.4, paros's
`QuorumRead`). The server holding the journal's replica state asks one Phase-1 quorum (a row) for
the highest slot each acceptor has voted in. Any chosen slot was voted by some acceptor in every
row, so the maximum covers everything chosen. The server waits until its applied prefix reaches
that maximum, then answers. The drawings show a full grid, where a row is a Phase-1 quorum. In
the partitioned grid of the service, Phase 1 takes F+1 of every column (section 3.4). The row
read on a full grid is safe across a change of members only with a Phase 1 that reaches each
row of the old grid (decision 31, section 2.5).

```
 client ──Read──► replica ──"highest vote?"──► one row of acceptors
                     │    ◄── max watermark w ──┘
                     │  wait until applied prefix ≥ w
 client ◄──records───┘
```

**Rule 1: seal the latest configuration** (decision 2; #1042 (retire fence rework); steps 1.6,
2.2 to 2.5). **Not proved** until the TLA+ gate of phase 2 is green: the model `config-register`
(Agreement with three ballots and a candidate that stops after any step, also with a partitioned
layout, a full grid and a successor of another size; with at most F acceptors gone, a campaign
wins in the end), whose controls (a switch before the recovered slots are held, a stored stale
belief, an abandoned successor, a store released before the switch) fail. It changes the
Matchmaker Paxos paper as paros built it, so its model comes first (`AGENTS.md`: a change to a
paper needs a TLA+ check).

**For each journal, the matchmakers hold a register: the configuration in force, and at most one
open successor.** Each configuration has an epoch, the high part of its ballot.

- **A campaign registers nothing.** A candidate reads the register. Phase 1 asks a quorum of the
  configuration in force, and of the successor when there is one; Phase 2 addresses one
  configuration.
- **A move has two events.** The **seal** is the Phase 1 of the next epoch that stops the old
  set. The **switch** is the register write that puts the successor in force. A move announces
  the successor at the matchmakers, seals the old set, copies what the old set reports, then
  switches the register.
- **The switch is the last step of a move, never a step before Phase 2.** A successor becomes the
  configuration in force only when its leader holds every recovered slot at a Phase-2 quorum of
  it (FoundationDB's order). The other order is unsafe: the review of 2026-11-16 ran TLC on
  `specs/tla/cas-register`, where the new configuration is written to the matchmakers before
  Phase 2, and `Agreement` fails in 25 steps (ballot 1 chooses a value in A, ballot 2 writes B
  to the register and stops before Phase 2, ballot 3 reads B, sees no vote and chooses a second
  value). That check is `cas_long`, and its trace is the first control of `config-register`
  (`switch_before`, step 1.6).
- **A new member withholds its ack until it holds the proof that the old set is sealed**
  (TigerBeetle's proof).
- **An open successor is always finished forward, never abandoned.** While a successor is open,
  Phase 2 writes to the successor only, and no candidate writes a third configuration. A candidate
  that finishes a successor it read announces it again at a matchmaker majority first, because
  a read can return a successor that only one matchmaker holds (`config-register`, control
  `direct`).
- **A stale belief cannot be stored**, so no election waits for a node outside these two
  configurations. A node learns its configuration from the register only (step 2.5).
- **A removed node may stop as soon as the switch is durable.** Durable means that a matchmaker
  majority holds the same switch write; a majority of registers from different writes is not enough
  (`config-register`, control `mixed`). A machine that left a configuration in force comes back
  only under a new id, with a new store (rule 5; control `rejoin`). A machine releases its store
  for a journal when the register shows a configuration in force that does not name it (rule 3,
  section 3.3). There is no retire check and no retire mark.

This deletes the retire stack (step 2.4): the registration fence scalar, the sixth leg of
`may_retire`, `retire_check.rs`, `MatchRefusal::Retired`, `RetireMark` and its incarnations,
`MatchPurpose::Rejoin`, the owner testimony and the skip of an abandoned registration (section
11 keeps them). The models `retire-fence` and `abandoned-ballot` leave CI with their rules; their
traces stay as controls of `config-register`, and the 2x2 grid of `abandoned-ballot` moves to
`config-register` first (step 1.13).

```
 candidate (ballot b, epoch e)
   │ 1. read the register ──────────► matchmakers (majority of the tenant's set)
   │ ◄──────────── C_in_force, and C_successor when one is open
   │ 2. Phase 1a(b) ────────────────► a quorum of C_in_force  (AND of C_successor, if open)
   │ ◄──────────── promises + votes    (adopt the highest-ballot vote per slot)
   │ 3. Phase 2a(b) ────────────────► one configuration, slot by slot (through proxy leaders)
   ▼
 a move A → B: announce B as successor, seal A (Phase 1 at epoch e+1), copy,
 hold every recovered slot at a Phase-2 quorum of B, then switch the register: B in force.
```

**Rule 2 at the matchmakers: sets never shrink** (#1045 (finish proposes the target); steps 1.8
and 2.1). **Not proved** until the TLA+ gate of phase 2 is green: the model `handover` (extends
`handover-gc`: only a resize record changes the size; the first write needs every new member;
the floor and the register cross the handover, also while a journal has an open successor; a
handover with at most F members down ends), whose control, the answerer rule, fails. The model
passes and its controls fail (`specs/tla/handover/`, #1097 (rework 1.8)); the code comes with
step 2.1. The model shows that the first-write rule is the size rule, not a safety rule: with a
majority, the floor and the register still cross, but the set serves with a member that holds
nothing.

- A set has a durable size, `2F+1` for each acceptor partition, and a failure never changes it.
  The size is the recorded number of members; a member that is gone does not change it.
- **The caller names the target set.** A generation handover's `Stop` carries the target set,
  and a finish proposes it again. An answer to a freeze proves only that the old set stopped; it
  never chooses the members. Every handover record asserts the size.
- A change that cannot fill the size waits and reports a typed reason.
- The first write to a new set needs every new member.
- Only a resize record changes the size (section 3.3). Rule 1 does not need the two sets to have
  the same size.
- **A resize waits for the handover** (Pierre, 2026-11-20, #1230 (rules no check tests)). The
  operator records a new size only while no handover target is named. The decree of a handover
  runs over the old matchmakers, which do not read the control journal, so a size recorded after
  the target is named would be lost: the handover would choose a set of the old size. The model
  `handover/` checks the rule (`ResizeGuard`), and its control `resize_during` breaks `SizeKept`.

The answerer rule (`odd_answerers`, a finish that proposes the members that answered, pending
bootstraps that grow) is deleted (step 2.1).

**Matchmaker sets are generations** (#563): every matchmaking message is fenced by generation,
and so is each order of the coordinator to the matchmakers (rule 3); matchmaker quorums are
**majorities only**; the handover's decree is the shared `Proposer` + `Acceptor` over a one-slot
log (no second Paxos kernel). The node that runs the handover keeps its decree round on disk:
it writes the round before the `Prepare` of that round leaves, and a new incarnation starts from
the round on disk (Pierre, 2026-11-19, #1229 (a stale `Stopped` reply reuses a decree ballot)).
A `Stopped` reply carries no incarnation, so a reply sent to an earlier incarnation can reach a
later one, and the floor of the stop quorum alone does not keep the round above the rounds of
the earlier incarnation (`specs/tla/experiments/stale-stopped/`). The decree still opens above
that floor, which skips rounds that the matchmakers would refuse. A handover is paged (#439): a set keeps one register per journal of
its tenant, so a `Stop` and its answers carry one page at a time.

**GC.** A configuration is forgotten once it is sealed and its successor is in force: no future
leader can need its Phase-1 quorum. The compaction floor and the GC watermark never wait on each
other.

Matchmakers are mandatory in the service (section 3.1): every journal has its tenant's set,
because reconfiguration is how a dead disk is replaced, a machine drained, a journal moved and a
quorum system changed.

### 4.2 The journal's sans-IO contract

**The shape of `step`** (decided by Pierre on 2026-11-19, #1255 (journal sans-IO contract)): the
journal's roles and their driver follow five rules, findings 1 to 5 of the second review of the
journal design (architecture, layering, testability). The rules fix the boundary between the
pure roles and the driver before any journal code exists. The journal core is not built yet
(step 3 of #69 (plan of record)), so the names below are a sketch: each exact type is **Open**,
and the journal core step decides it.

A role is a pure state machine. A step takes one input and returns its effects:

```rust
// A sketch. The names are Open until the journal core is built.
fn step(&mut self, now: Instant, input: Input) -> Vec<Effect>;

enum Input {
    Recovered(RecoveredState),            // restart: the durable state and its damage report
    Message { from: Address, msg: Message },
    Persisted(PersistId),                 // every batch up to this id is durable
    ReadDone { read: ReadId, records: Records },
    TimerFired(TimerId),
    Random(u64),                          // a draw the role asked for
    // client requests, upload done, ...
}

enum Effect {
    Persist { id: PersistId, ops: Vec<Op>, sync: bool },
    Send { to: Address, msg: Message, after: PersistId },
    Read { read: ReadId, range: Range },
    SetTimer { timer: TimerId, at: Instant },
    // replies to clients (each names the persist id it waits on), upload, ...
}
```

**Rule 1: the flight rule.** Each persist batch has an id. Each send names the persist id that it
waits on, and the driver transmits the send only once that batch is durable. Durability comes
back to the role as `Input::Persisted(id)`. The driver may commit several batches in one storage
commit (group commit), always in id order, and it may run the next step while a batch is in
flight. A reply that a step sends with no write of its own (a second copy of a `Prepare` at the
promised ballot, or of an `Accept` that the acceptor already holds) waits on the last batch that
the role emitted, because the batch that recorded the state the reply reports may still be in
flight. Batch ids grow across a restart: after `Input::Recovered`, the next id is above every
durable id.

*Why.* A contract with only two bags of effects, persist and send, leaves pipelining to the
driver: either the driver waits for every fsync (no group commit), or it picks an order itself,
and that order is a protocol decision that the trace check cannot see. With the flight rule,
every ordering decision is in the pure role, and the trace records it.

*Proof.* `specs/tla/flight-rule/`: single-decree Paxos on three acceptors whose driver holds
each send until the batch that the send names is durable, with group commit and a crash that
drops the batches in flight and the held sends. With the rule, `Agreement` and `SentIsDurable`
(what a peer holds is on its sender's disk) pass. Two controls break `Agreement`: a send that
waits on nothing (`send_early`), and a send that waits only on the batch of its own step
(`own_batch`: a second copy of a `Prepare` gets a `Promise` out before the first copy's batch is
durable; the acceptor crashes, forgets the promise and votes for the lower ballot, which is
chosen; the higher ballot counted the lost promise and chooses a second value). The checks run
in CI with two ballots and one crash, and with three ballots and two crashes (`flight_wide`).

**Rule 2: every input is recorded.** A restart is `Input::Recovered(state)`. A read from storage
is an effect, and its result comes back as an input. `now` comes with every input, timers are
`Effect::SetTimer` and `Input::TimerFired`, and the driver draws every random value and passes
it in.

*Why.* The effects of a step are then a function of the role's inputs alone. Any run, in
simulation or in production, replays from its recorded inputs, and a failing run of the whole
journal under moonpool-sim replays exactly.

**Rule 3: a pure `Host`.** A process serves many roles of many journals. The `Host` is a pure
state machine in the core: it maps (journal, role, instance) to a role, routes each input to it
and merges the effects. The driver drives one `Host`. A message between two roles of one process
goes through the same queue as a message to another process.

*Why.* Without the `Host`, each driver writes its own routing, and nothing tests it. With one
queue for local and remote delivery, simulation can reorder and drop local messages too, so a
role never depends on being colocated with another (the colocation rule of section 4.1). The
`Host` supports one process with every role and one process for each role (decided by Pierre on
2026-11-19).

**Rule 4: storage is one trait.** The trait has an atomic `commit(batch of typed ops, sync)` and
a range read. The core defines the ops and owns the on-disk format (decided by Pierre on
2026-11-19); each op is namespaced by (journal, role). `open()` returns a damage report: the
records it found corrupt, so that the role can report and repair them (CTRL). An error from the
trait means "unknown": the batch may or may not be durable, and the role of that journal stops.

*Why.* An acceptor, a replica and a matchmaker need different storage, but one trait with typed
ops serves all three, and one contract suite tests every backend. An atomic commit gives rule 1
its meaning: a batch is durable or not, never torn. A contract suite certifies the API only;
crash safety is claimed only on a faultable disk (decided by Pierre on 2026-11-19).

**Rule 5: two ports.** `Events` carries the recorded (input, effects) pair of each `Host` step,
after durability. The trace check reads it, and so does the grid controller, for its signals.
`Assignments` says which journals and roles the process serves, and which it stops.

*Why.* The trace check and the controller then read the same record that replay uses, and no
role grows a private hook for them. Placement reaches a process through one port, so parosd has
no private API into the journal.

Related decisions of the same night: the runtime is compio through moonpool's `CompioProviders`,
so no trait requires `Send`; every header carries a release number (v1 scope); a write that holds
a slot is answered `Unknown`, never `NotLeader` (#1232 (deposed leader and a held write)).

**The contract against `ColocatedNode` today.** `ColocatedNode` (`paros-consensus`) is the sans-IO
core of plain Multi-Paxos and Matchmaker Paxos: `step`/`tick` in, one `Ready` out, `advance()`.
Each difference below is a **change** (the journal follows the rule above) or a **proposal**
(nobody has decided it; it waits for the journal core step or for Pierre).

| `ColocatedNode` today | The journal contract | Kind |
|---|---|---|
| One batch in flight: after `advance()`, the driver must not call `step`, `tick` or `propose*` until the batch is flushed (`Ready`'s docs). The borrow guard covers the buckets only; the driver holds the rest. | Persist ids and named sends (rule 1). The driver may pipeline and group commit, and the role decides each order. | Change |
| A reply with no write relies on the earlier batch being durable: "an acceptor emits no write for a `Prepare` at the ballot it already promised" (`Ready`'s docs). One flight makes this safe. | The reply waits on the last batch id the role emitted. The `own_batch` control of `flight-rule/` is the bug without it. | Change |
| `Ready` is seven buckets (`writes`, `messages`, `committed`, `read_states`, `recovery_batch`, `match_requests`, `gc_requests`), and the order in which to process them is written in prose. | One effect list; each send and each client reply names the persist id it waits on. | Change |
| A restart is `ColocatedNode::new(&impl Storage, Limits)`: the core calls the read-only `Storage` trait (`initial_state`, `accepted`, `sealed_state`, `faulty_entries`, ...) during construction. | `Input::Recovered(state)`, built from what `open()` returned (rule 2). | Change |
| The retained log is in memory, read once at boot. | Reads from storage are effects; their results come back as `ReadDone` inputs (rule 2), so a role does not hold whole segments in memory. | Change |
| Time is a logical `tick()` with no `now`. | `now` on every input, with `SetTimer` and `TimerFired` (rule 2). | Change |
| The driver draws the election jitter and sets it when the node raises its flag for a new timeout. | The same idea as an input, so the draw is recorded (rule 2). | Change |
| `Storage` is read-only; the driver applies `WriteOp`s on a store of its own, and fsyncs when `MustSync::for_batch` says so. | One trait: `commit(batch, sync)` and a range read; ops typed by the core and namespaced by (journal, role); `open()` gives a damage report, which takes the place of `faulty_entries` (rule 4). | Change |
| Each deployment (`ColocatedNode`, `Matchmaker`, `ProxyLeader`, `ReplicaNode`) has its own `ready`/`advance`, and the driver routes between them (the removed `paros-roles` did this). | One pure `Host` routes, with one queue for local and remote delivery (rule 3). | Change |
| No trace port: `consensus-sim` builds each trace from its own walk. | `Events` (rule 5). | Change |
| No assignment port: the configuration is read at construction, with `Retire` and `rejoin` as calls. | `Assignments` (rule 5). | Change |
| The borrow guard makes a second `ready()` before `advance()` a compile error, and an async driver copies the buckets out before it awaits. | `step` returns owned effects. With rule 1, nothing needs the guard: the role, not the borrow checker, says what may go out. | Proposal |
| `ColocatedNode` keeps its contract. | Move `ColocatedNode` to the flight rule too, so that the examples, the walks and the journal share one driver contract. | Proposal |

### 4.3 The consensus core is a library for everyone

**`paros-consensus` stays usable by other people** (decided by Pierre on 2026-11-20). Anyone can
use it to run plain Multi-Paxos, single-decree Paxos or any other flavor that it builds
(leader handoff, grid quorums, matchmakers, proxy leaders, replicas, leaderless reads), with no
part of the paros service. What paros needs comes from **optional behaviour and software
composition**: a role or a policy that a caller opts into, or a layer above the core that
composes the roles. The core never takes a concept of the service, such as a cell, a tenant
policy or `cell init` (#1264 (remove the cell from paros-consensus), section 3.7). A paros need
that the core cannot meet by composition is a new optional behaviour of the core, and its plain
arm stays unchanged. This extends "plain Multi-Paxos is a shape of the core" of AGENTS.md to
every flavor.
