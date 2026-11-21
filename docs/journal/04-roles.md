[← 03-segments-and-records.md](03-segments-and-records.md) · [Index](README.md) · [05-quorums-and-failures.md →](05-quorums-and-failures.md)

## J4. Roles, contract and crates

The journal runs the roles of Compartmentalized Paxos with the matchmakers of Matchmaker Paxos,
for each journal. The paper names are the vocabulary, in the code and in these files
(`AGENTS.md`). This chapter lists the roles (§J4.1), the contract between the pure roles and the
driver (§J4.2), and the crates and the layering rule (§J4.3). How the roles of the core fit
together on the write path and the read path, with drawings, is architecture §4.1.

### J4.1 The roles

Each role is a pure state machine under the flight rule (§J4.2). The core column names the
`paros-consensus` code that the journal composes; the journal crates add what the core does not
have, with no journal concept in the core (§J4.3).

| Role | State | Job | Core it composes | Rules |
|---|---|---|---|---|
| Batcher, unbatcher | none | Multi-writer journals only: merge writes into one slot; the unbatcher gives each write its own range | `batcher.rs` (`Batcher`, `Unbatcher`) | §J2.4, `compartments/` (#1190) |
| Leader (proposer) | ballot, next slot | Gives each command a slot. Holds each answer until its slot is applied; answers `Unknown`, never `NotLeader`, for a write that holds a slot. Seals the old segment after its Phase 1 | `proposer.rs`, `proposer/` | §J2.2, §J3.1 (#1232, #1213) |
| Proxy leader | none | Phase 2 for one slot on one column; `Commit` to the replica set of the slot's segment | `proxy_leader.rs` | `proxy-leader/` (#1188), `segment-replicas/` (#1271) |
| Acceptor | promise, votes | The grid of one segment. Default: one column of three (`F = 1`), a plain majority. Else the partitioned grid, `rows = 2F + 1`, `acks = F + 1` | `acceptor.rs`, `membership/` | §J5.1 (#1257) |
| Replica | the chosen log of its segments | Applies in slot order, folds the journal state, serves reads (leaderless), writes the boundary state of a sealed segment. One replica set for each segment | `replica.rs`, `replica_node.rs`, `quorum_read.rs` | §J2.5, §J3.1 (#1271) |
| Matchmaker | the configurations of the journal | One set of `2F + 1` for each journal, majority quorums; the set hands over by generations | `matchmaker.rs`, `matchmaker/reconfigurer.rs` | architecture §4.1, `handover/` (#1097) |
| Matchmaker register (optional role) | the segment records and the controller term | One compare-and-set register for each journal, beside the paper's registration matchmaker | `paros-journal` (`register`), by composition (#1325) | §J3.4 (#1275, #1315) |
| Grid controller | none (the term is in the register) | Elected by a term in the register; runs on stateless machines; asks, never decides safety. v1: heal and scrub | none | §J6 (#1259, #1260) |

**The default deployment** is three machines, each with every role in one process (Pierre, in
the journal design); the `Host` also supports one process for each role (§J4.2, rule 3).

**Not a role of the journal.** The frontend, the resolver and the coordinators of cells and
tenants are parts of the service (architecture §3.3, §3.5). The offloader is a role of the
offload design, after v1 (§J3.1).

### J4.2 The journal's sans-IO contract

*Moved here from `docs/architecture/04-roles.md` §4.2 (#1332 (journal architecture docs)).*

**The shape of `step`** (decided by Pierre on 2026-11-19, #1255 (journal sans-IO contract)): the
journal's roles and their driver follow five rules, findings 1 to 5 of the second review of the
journal design (architecture, layering, testability). The rules fix the boundary between the
pure roles and the driver before any journal role exists. The pure journal core
(`paros-journal`, #1324 (paros-journal)) holds the fold, the records and the codec, and no role
yet. So the names below are a sketch: each exact type is **Open**, and #1326 (the journal roles
over the core) decides it.

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

*The order of an acceptor's writes* (#1309 (the state is the disk), `specs/tla/journal-state/`).
An `Accept` that raises the promise writes the promise and the vote in one batch, or the promise
in a batch before the vote. A vote that is durable before its promise breaks safety: the acceptor
crashes, starts again with its old promise, and takes a lower ballot's `Accept` over a vote that
was chosen (control `order_vote_first`).

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
role never depends on being colocated with another (the colocation rule of architecture §4.1). The
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

### J4.3 Crates and layering

**The crates of the journal.** The plan (#1323 (build the paros journal v1), section J8)
creates these crates, each in its own step. `paros-journal` and `journal-sim` exist (#1324
(paros-journal)); the others do not exist yet:

| Crate | What it holds | Step |
|---|---|---|
| `paros-journal` (`crates/lib/`) | The pure journal core: the journal fold (`fold`), the `Resolve` call (`resolve`), the held answers (`held`), the segment record types (`segment`), the matchmaker register and its handover (`register`), the codec with a release number in every header (`codec`), the record headers (`command`); later the pure client (no network) | #1324 (paros-journal), #1325 (the matchmaker register), #1331 (the pure journal client) |
| `paros-journal-control` (`crates/lib/`) | The pure `decide` of the v1 controller and the controller role | #1328 (paros-journal-control) |
| `paros-journal-io` (`crates/lib/`) | The traits that a backend implements (one storage trait, the transport, the clock, the `Host` inputs), the generic driver under the flight rule, many journals in one process, and the contract suites | #1329 (paros-journal-io) |
| `paros-journal-moonpool` (`crates/lib/`) | The adapter of the `paros-journal-io` traits on moonpool providers: compio through moonpool's `CompioProviders`, no trait requires `Send` (Pierre, 2026-11-19) | #1330 (paros-journal-moonpool) |
| `journal-sim` (`crates/sim/`) | The seeded walks over the journal's pure roles, the oracles, the trace check against #1189 and the coverage gate (section J7). Today: the walk of the fold over a chain of segments, its trace check against `journal-state/FoldTrace.tla`; the walk of the register across handovers, its trace check against `segment-records/RegisterTrace.tla` | #1327 (journal-sim) |

The journal roles that compose the core (segments, seal, reads, retire, truncate,
re-replication) are #1326 (the journal roles). **Open**: their crate (#1326 decides). The
matchmaker register is `paros_journal::register` (#1325 (the matchmaker register as an optional
role), §J3.4). The `ObjectStore` trait of the journal design waits
with offload (#1329 (paros-journal-io)). The `paros` facade re-exports each layer as a module (`AGENTS.md`).

**The layering rule.** `paros-consensus` stays a library for everyone: plain Multi-Paxos,
single-decree Paxos and every flavor it builds keep their messages, scalars and API. What the
journal needs comes from optional behaviour and software composition, and a need that
composition cannot meet is a new optional behaviour of the core with the plain arm unchanged.
The rule and its reasons are architecture §4.3 (decided by Pierre on 2026-11-20). Cells and
every other service concept stay out of both the core and the journal crates (#1264 (remove the
cell from paros-consensus), PR #1268 (no cell in the core)).

*Moved here from `docs/architecture/04-roles.md` §4.3, where the journal applies the rule
(#1332 (journal architecture docs)):*

**Two decisions of 2026-11-20 apply the rule to the journal** (Pierre, #1315 (bring docs and
models in step with the v1 scope)):

- **The journal fold stays out of the core.** `Write`, `Truncate` and `SetLeader`, with
  `SetLeader(None)`, the unset leader, the order "position before fence" and the answer `Below`,
  live only in the journal crates. The core's `Command`, `Outcome` and fold
  (`journal/command.rs`, `journal/state.rs`) stay as they are today. The journal does not make
  the core generic over the command for this.
- **The journal's compare-and-set register is an optional role.** The registration matchmaker of
  the Matchmaker Paxos paper stays in the core, with what it builds. The register sits beside it
  (§J3.4).
