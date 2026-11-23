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

**The roles built so far** (#1326 (the journal roles over the core), first rule group: segments,
the seal and writes). They are in `paros_journal::roles`, each a pure state machine under the
flight rule:

- `JournalAcceptor` (`roles/acceptor.rs`): the core's `Acceptor` over the journal's `Command`.
  One acceptor serves every segment whose grid names it, with one promise. An `Accept` that raises
  the promise writes the promise and the vote in one batch (#1309).
- `JournalLeader` (`roles/leader.rs`, `roles/leader/phase1.rs`): the leader of one ballot, bound to
  the newest segment. Its Phase 1 reads every segment that is not retired, a sealed one only below
  its end, and needs `F + 1` of every column of every grid. A first leader of a pending segment
  writes the seal end (one above the highest vote of the old grid, a faulty vote included,
  #1313) and opens only on a done write of its own end. It writes again every slot from its first
  slot: the vote its Phase 1 saw, else a `Noop` (#1233), except a slot whose top report is a
  faulty vote. It writes each slot in one column of its own grid with the core's `Rounds`, sends
  each chosen slot to the replica set of the slot's segment (#1271), and holds each answer until
  a replica applies the slot (#1232).
- `SegmentReplica` (`roles/replica.rs`): one replica of one segment. It takes its base from the
  boundary state of the segment just before, applies in slot order, learns the end of its segment
  from a done seal, writes the boundary state when it is full, pulls missing slots from its
  peers, and answers `Resolve` (#1320).

The retire and the re-replication (#1314) are built too. The leader asks each replica of the
sealed segment whether it holds the whole segment (`Holding`, `Holds`) and writes `retired` at
`F + 1` holders (`LeaderEffect::Retired` once the write is seen). A new replica of a retired
segment joins for one copy of a heal, copies from the full members of the set and reports
`Copied`; a replica that is in no set and runs no copy drops its copy (§J3.1, "As built").

The proxy leader, the leaderless read and the trim are built too (2026-11-22):

- `JournalProxy` (`roles/proxy.rs`): the proxy leader of Compartmentalized Paxos. A leader
  delegates a round (`Delegate`) to a proxy, which sends the `Accept`s, counts `F + 1` votes of
  the round's column on the configuration that the round carries, and sends `Chosen` to the leader
  and `Commit` to the replica set (`specs/tla/proxy-leader/`). The leader sends the same `Commit`
  on `Chosen` and again until a replica applies it, because the proxy keeps nothing. A proxy
  `Nack`s the leader of each round that a higher ballot dropped, and a delegation below its ballot.
  The `Host` runs one role for each address (journal, node), so a proxy needs a node where the
  journal runs no other role.
- The leaderless read (`roles/replica/read.rs`, §J2.5, §J3.3): `SegmentReplica::read` asks `F + 1`
  of each column of its segment's grid for their vote watermarks (`PreRead`, `PreReadAck`) and
  answers once its fold covers the highest; an answer that names a later segment ends the read
  (`Superseded`). A read of a sealed segment asks nobody. Each answer waits on the replica's last
  batch.
- The trim (§J3.2): a replica writes `Trim` for each retired segment below its own once its fold's
  floor covers the segment's last `seq`, and every replica of a trimmed segment drops its copy.
  The reader of `journal-sim`'s roles walk and the client (`paros_journal::client`) take the
  floor from the segment in force before they read: the client sends `Request::Floor` first, ends
  the read `Truncated` itself below that floor, and sends `Request::Read` only at or above it.
- The leader stamps the leader time of each write at propose (§J2.8, #1299).

- The compaction (§J3.2, "As built: the compaction"): each replica compacts its log to a `Trim`,
  the leader sends each acceptor the point that `F + 1` replicas of the set reached
  (`Compaction`, `CompactedTo`, `Compact`), the acceptor drops the votes below it and names the
  dropped ranges in its promises, and a laggard jumps to a peer's `Trim` (`TrimmedTo`).

The repair of a blocked slot (CTRL) is the last rule group of #1326.

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
yet. So the names below are a sketch. #1326 (the journal roles over the core) fixed rule 1 in
the roles (below, "What the roles build today"), and #1329 (paros-journal-io) built the `Host`,
the traits and the driver (below, "What the driver builds").

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

*What the roles build today* (#1326). Each role with a disk (the acceptor, the replica) gives
every persist batch a `PersistId` (`roles::Flight`) and names on each send the batch it waits
on; a send with no write of its own waits on the last batch the role emitted. A restart is
`recover(disk)`, and the ids go on above the highest durable id. The driver holds each send
until its batch is durable, so the role needs no `Persisted` input. The leader has no disk (a
ballot belongs to one leader in memory, as in `journal-state/`), so its sends wait on nothing.
The inputs are `step(from, message)`, `observe(register value)` (a value that a done write
gave, §J3.4), and `tick(page)`: the driver paces the re-sends with `page`, because a re-send of
every round floods a bounded network and loses the messages that would close the rounds (a
finding of the roles walk). A compare-and-set of the register is an effect: the leader's
(`Register(Change)`) waits on nothing, and the replica's boundary write (`Register { change,
after }`) waits on the batch of the last slot it folds, so the register takes only a fold that is
on the replica's disk. The answer to a `Resolve` reads the fold too, and goes once the batch it
names is durable. A leader takes what its Phase 1 reads (the segments that are not retired, the
end of each sealed one) and the first slot it writes from the register value of its campaign, and
keeps them; the driver computes that first slot from the replicas' disks. `RolesTrace.tla`
checks each of these against `Journal.tla` (§J7.3).

*What the driver builds* (#1329 (paros-journal-io), `crates/lib/paros-journal-io`). The crate is
pure (§J4.3): the four traits that a backend implements, the `Host`, the generic `Driver`, the
backends in memory (`mem`) and the contract suites (`contract`).

- *Rule 1.* The `Driver` steps the `Host` at once for each input and keeps every persist batch of
  a round in one group. At the end of `Driver::poll` it commits the group in one storage write
  (group commit), in id order for each role, then transmits the sends and runs the register
  changes that waited on a batch of the group. A commit that fails stops every role that had a
  batch in it (`Input::Failed`, `Stop::Failed`): the batch may or may not be durable, so what
  waited on it never goes. A register change carries `after` too, so a change that a role asks
  after a batch waits on that batch.
- *Rule 2.* A role with a disk starts with `Effect::Read`; the driver scans the role's records
  and gives them back as `Input::Recovered`, and learns from them the role's highest durable
  batch. The driver gives the time (`Time`) with every input of the `Host`. The `Host` arms one
  timer (`Effect::SetTimer`); when it fires (`Input::TimerFired`), each leader and each replica
  takes its periodic step. The leader takes the time of each proposal from its caller
  (`JournalLeader::propose`, the leader time of §J2.8). A client read is an input
  (`Input::Read`) to a replica, answered by `Effect::ReadReply` once the replica's batch is
  durable; the driver gives it back as `ReadReply` (`Driver::read`, `Driver::take_reads`).
  **Open**: timers and random draws inside each role (the roles keep `tick(page)` and take no
  random value yet), and reads of a slot range as effects (`ReadDone`), which wait for the
  offload.
- *Rule 3.* The `Host` maps each `Address` (a journal, a node, and the segment of a replica) to
  its role, routes each input, and turns the role's effects into `Effect`s that name their journal
  and their sender. A replica node serves one replica for each segment of its journal, so a heal
  keeps the live replicas of a column (Pierre, 2026-11-22, #1445 (simulate the whole journal in
  memory)); an `Unassign` of the node with no segment stops every role of the node. The driver gives every
  send to the `Transport`, a send between two roles of one process too. One process may serve
  every role, or one role; the `Host` does not change.
- *Rule 4.* `Storage` is one trait: `commit(ops, sync)`, atomic, and `scan(address)`, every
  record of one role in key order. The format is the crate's (`layout`): a `Key` is the journal,
  the node, the segment of a replica (0 for every other role), the kind of the record and an index (a slot), and its bytes sort as the key does,
  so each op is namespaced by (journal, role). Each batch ends with the role's `Durable` record.
  The damage report is per role: a record the backend reports corrupt (`Entry::Corrupt`), or that
  does not decode as its kind, stops the role with `Stop::Damaged` and its keys. A
  `StorageError` means "unknown". The contract suites (`contract::storage`,
  `contract::storage_reopen`, `contract::transport`, `contract::clock`) certify a backend's API
  only.
- *Rule 5.* The `Events` port reports each step of the `Host` (its time, its input and its
  effects) once every batch of the step is settled: durable, or in a commit that failed. The
  `Assignments` port is `Input::Assign` and `Input::Unassign` (`Driver::assign`,
  `Driver::unassign`); a role is an acceptor, a replica of one segment, or a leader of one
  campaign.
- *The register.* `Register` is a port of its own: run a change, and read the value of a done
  write. In one process it is `mem::MemRegister`. **Open** (#1417 (the moonpool adapter on the generic host), with #1330): the
  register on the journal's matchmakers (`RegisterCall`, `RegisterMember`) as roles of the
  `Host`.
- *Choices for Pierre's final review* (#1329). The traits are synchronous and never block: a
  program calls `Driver::poll` in its own loop, and an async adapter (#1417, outside v1) runs the driver's
  I/O through them or drives the `Host` itself. There is no `open()` call: the damage report
  comes with the scan of each role, at its start. The storage holds one format for every role;
  a backend stores keys and bytes and never reads them.

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
after durability. The trace check reads it, live and chunk by chunk (moonpool-tla `Live`,
§J7.3, #1310 (check long runs live)), and so does the grid controller, for its signals.
`Assignments` says which journals and roles the process serves, and which it stops.

*Why.* The trace check and the controller then read the same record that replay uses, and no
role grows a private hook for them. Placement reaches a process through one port, so parosd has
no private API into the journal.

Related decisions of the same night: the runtime of the moonpool adapter is compio through
moonpool's `CompioProviders`, so no trait requires `Send` (the pure crates name no runtime,
§J4.3); every header carries a release number (v1 scope); a write that holds
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
(paros-journal)), and `paros-journal-io` (#1329); the others do not exist yet:

| Crate | What it holds | Step |
|---|---|---|
| `paros-journal` (`crates/lib/`) | The pure journal core: the journal fold (`fold`), the `Resolve` call (`resolve`), the held answers (`held`), the segment record types (`segment`), the matchmaker register and its handover (`register`), the journal roles (`roles`, #1326), the codec with a release number in every header (`codec`), the record headers (`command`), the pure client with no network (`client`, #1331) | #1324 (paros-journal), #1325 (the matchmaker register), #1331 (the pure journal client) |
| `paros-journal-control` (`crates/lib/`) | The pure `decide` of the v1 controller and the controller role | #1328 (paros-journal-control) |
| `paros-journal-io` (`crates/lib/`) | The traits that a backend implements (one storage trait, the transport, the clock, the register), the on-disk format, the pure `Host` (many journals in one process), the generic driver under the flight rule, the `Events` port, backends in memory and the contract suites. Its examples `in_memory` and `custom_storage` run the roles on it. Its traits are not stable until the moonpool adapter of #1417 runs on them (2026-11-22) | #1329 (paros-journal-io) |
| `paros-journal-moonpool` (`crates/lib/`) | Not in v1: the adapter of the `paros-journal-io` traits on moonpool providers (compio through moonpool's `CompioProviders`, no trait requires `Send`; Pierre, 2026-11-19), with moonpool RPC and protobuf as its transport ("The wire" below). It is built by the layered parosd (Pierre, 2026-11-22) | #1417 (the moonpool adapter on the generic host), with #1330 |
| `journal-sim` (`crates/sim/`) | The seeded walks over the journal's pure roles, the oracles, the trace check against #1189 and the coverage gate (section J7). Today: the walk of the fold over a chain of segments, its trace check against `journal-state/FoldTrace.tla`; the walk of the register across handovers, its trace check against `segment-records/RegisterTrace.tla`; the walk of the roles, its trace check against `journal-compact/RolesTrace.tla` | #1327 (journal-sim) |

The journal roles that compose the core (segments, seal, reads, retire, truncate,
re-replication) are #1326 (the journal roles). They live in `paros-journal`, module `roles`
(#1326): they are pure and use only the core and the journal's own types, so they need no crate
of their own. The
matchmaker register is `paros_journal::register` (#1325 (the matchmaker register as an optional
role), §J3.4). The `ObjectStore` trait of the journal design waits
with offload (after v1, §J3.1). The `paros` facade re-exports each layer as a module (`AGENTS.md`).

**The layering rule.** `paros-consensus` stays a library for everyone: plain Multi-Paxos,
single-decree Paxos and every flavor it builds keep their messages, scalars and API. What the
journal needs comes from optional behaviour and software composition, and a need that
composition cannot meet is a new optional behaviour of the core with the plain arm unchanged.
The rule and its reasons are architecture §4.3 (decided by Pierre on 2026-11-20). Cells and
every other service concept stay out of both the core and the journal crates (#1264 (remove the
cell from paros-consensus), PR #1268 (no cell in the core)).

**No runtime or storage dependency** (Pierre, 2026-11-21). paros is a library that a program
uses by implementing its traits: the storage trait, the transport, the clock and the register of
`paros-journal-io` (#1329), and the `Storage` contract of `paros-consensus`. The pure crates
(`paros-consensus`, `paros-journal`, and `paros-journal-io`, `paros-journal-control` and the
client when they exist) therefore depend on no async runtime (tokio, compio, smol), no storage
engine and no network or file crate. Their traits do not require `Send`, an executor or a
runtime type. With no feature on, a pure crate depends on workspace crates only. Its optional
features only observe (`serde`, `tracing`, `assertions`): they decide nothing and do no I/O. The
generic driver of `paros-journal-io` is written against the traits only. A crate that does I/O
is an adapter in a crate of its own (`paros-journal-moonpool`, #1417, outside v1), and the facade `paros`
re-exports it only behind an optional feature that is off by default. The facade is the place
where paros bundles moonpool's storage and network for a program that wants them (Pierre,
2026-11-21): the bundle is a feature of `paros`, never a dependency of a pure crate. CI runs
`scripts/check-core-deps.sh`: it fails when a pure crate, or the facade with its default
features, gains a dependency outside the workspace and the observation crates. The pure crates
are the crates of layers 0 to 5 of the layer table `scripts/layers.sh` (architecture §4.4), so a
new pure crate is checked as soon as the pull request that creates it gives it its row. The examples of #1370 (journal examples)
show the rule: a backend written by hand, in memory, with no runtime crate (`paros-journal-io`'s
`custom_storage`).

**The wire: moonpool RPC with protobuf** (Pierre, 2026-11-21). The end goal of the journal's
network is moonpool RPC, with every role message and every client call as a protobuf message.
The moonpool adapter builds its `Transport` on it, and the service above the journal uses the
same messages. The adapter is not part of the journal v1: it moved to the layered parosd (Pierre, 2026-11-22), as #1417 (the moonpool adapter on the generic host) under #1409 (build the layered parosd), which takes #1330 in. The protobuf schema and its generated code
live in the adapter (or in a wire crate of their own beside it), never in a pure crate: a pure
crate gains no protobuf dependency (`scripts/check-core-deps.sh`). The pure crates keep their
own byte codec for what they own: the records, the segment records and the on-disk format
(`paros-journal`'s `codec`, `paros-journal-io`'s `layout`). A transport of the journal over plain
`std::net`, with a hand-written codec of the role messages, is not built: it takes the work away
from that goal (Pierre, 2026-11-21, #1391 (a multi-process example)). A program that brings its
own transport still implements the `Transport` trait. **Open** (#1417): the `.proto` files
(their place and their version rule against the release number of each header, §J2), and
the mapping of a moonpool RPC endpoint to a role of the `Host`.

*Moved here from `docs/architecture/04-roles.md` §4.3, where the journal applies the rule
(#1332 (journal architecture docs)):*

**Two decisions of 2026-11-20 apply the rule to the journal** (Pierre, #1315 (bring docs and
models in step with the v1 scope)):

- **The journal fold stays out of the core.** `Write`, `Truncate` and `SetLeader`, with
  `SetLeader(None)` (a multi-writer mode with its tag, §J2.4.1), the unset leader, the order
  "position before fence" and the answer `Below`,
  live only in the journal crates. The core's `Command`, `Outcome` and fold
  (`journal/command.rs`, `journal/state.rs`) stay as they are today. The journal does not make
  the core generic over the command for this.
- **The journal's compare-and-set register is an optional role.** The registration matchmaker of
  the Matchmaker Paxos paper stays in the core, with what it builds. The register sits beside it
  (§J3.4).
