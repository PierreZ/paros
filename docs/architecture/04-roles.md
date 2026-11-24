[← 03-8-identifiers-checkpoints-recovery-security.md](03-8-identifiers-checkpoints-recovery-security.md) · [Index](README.md) · [04-5-old-duties.md →](04-5-old-duties.md)

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
| Matchmaker | storage | `Matchmaker`, `run_matchmaker`; one logical set per journal (decided by Pierre on 2026-11-19), processes shared; the journal's register is an optional role beside the paper's registration matchmaker (section 2.16) |
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

**Rule 1 and the journal register** (Pierre, 2026-11-20, #1315 (bring docs and models in step
with the v1 scope), decision 1). The registration matchmaker of the paper stays in
`paros-consensus` for the users of the library (section 4.3). The register of the journal is an
optional role beside it, and section 2.16 lists its fields, writers and guards. There, "in force"
means the state that the seal end of the predecessor gives, and the step that lets old acceptors
stop is the retire (section 2.16); the "switch" below is the word of rule 1 for the core. **Open**
(#1189 and step 2.3 decide): how steps 2.3 to 2.5 change, since they turn the core's matchmaker
into the register and delete the registrations.

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
      │ 1. read the register ──────────► matchmakers (majority of the journal's set)
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
that floor, which skips rounds that the matchmakers would refuse. A handover is paged (#439): a `Stop` and its answers carry one page
at a time. With one set per journal (2026-11-19) a set holds the state of one journal.

**GC.** A configuration is forgotten once it is sealed and its successor is in force: no future
leader can need its Phase-1 quorum. The compaction floor and the GC watermark never wait on each
other.

Matchmakers are mandatory in the service (section 3.1): every journal has its own set,
because reconfiguration is how a dead disk is replaced, a machine drained, a journal moved and a
quorum system changed.

### 4.2 The journal's sans-IO contract

Moved to [docs/journal/04-roles.md](../journal/04-roles.md) §J4.2 (#1332 (journal architecture docs), 2026-11-20): the shape of `step`, the flight rule, recorded inputs, a pure `Host`, one storage trait, the `Events` and `Assignments` ports, and the table of differences from `ColocatedNode`.

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

**How the journal applies the rule** (the journal fold stays out of the core; the journal's register is an optional role; Pierre, 2026-11-20, #1315 (bring docs and models in step with the v1 scope)) is in [docs/journal/04-roles.md](../journal/04-roles.md) §J4.3.

### 4.4 The layers of the service

**The service is a stack of layers, and each layer is a crate that stands alone** (Pierre,
2026-11-21 and 2026-11-22; goals 7, 8 and 10 of section 1.1). The features of the service at the tag
`v0.2.0-alpha.1` stay. Its structure changes: the old crates were cut by kind of code (one wire
crate, one model crate, one knob struct and one observation trait for every layer), and the new
crates are cut by layer of meaning. Each layer owns its messages, its knobs, its events, its
model and its walk. The test for each crate is: can a program that is not paros use it alone?
Nothing of this section is built before the journal v1 is done (#1323 (build the paros journal
v1)). The crate names are **Open**.

These points are decided:

- **A role is an actor.** A role (an acceptor, a leader, a coordinator, the frontend) is a pure
  `step` under the flight rule of the journal ([docs/journal/04-roles.md](../journal/04-roles.md)
  §J4.2): a send waits for the persist batch that it names, the step reads no clock and draws no
  random number, and a refusal is data. One host of actors serves every role of a process. The
  host is `paros-host` (#1410 (split the generic host of actors)): the `Role` trait, the `Host`
  over an opaque address and the generic driver, with no journal in it; the journal roles of
  `paros-journal-io` run on it, and its example runs a toy coordinator.
- **The frontend is a role like the others.** It has no layer and no crate of its own.
- **The election library is its own crate: a coordination store over a journal** (Pierre,
  2026-11-24, #1420 (paros-election)). The rules are in "The coordination store" below. A user
  elects a leader with it; a coordinator wins its term with the plain `SetLeader` of section 3.3.
- **The store is a thin layer on the journal client, not an actor on the host** (Pierre,
  2026-11-23, kept on 2026-11-24). It keeps nothing on disk: its state is the journal. It owns
  one pure client (`paros_journal::client`), and it has the shape of that client: calls in,
  requests out, answers in. The program gives it the time and each draw (a session id, a jitter)
  as values. It makes no message of its own: its network is the network of the client, in memory
  or through the adapter. The fence of a term outside the journal is not in the store: each
  receiver of an order checks the term in the same write as the effect (the rules of the
  takeover, below). So the crate depends only on `paros-journal`, and a program that has only
  the journal client can elect.
- **One crate for each control level** (universe, cell, tenant), and each coordinator is a pure
  state machine as the journal roles are.
- **The core has no tenant.** `paros-consensus` and `paros-journal` know no tenant. A crate that
  wraps them adds the tenant by composition (#1402 (remove the tenant from the core), section
  3.8).
- **The service wire.** The pure crates define their messages as Rust types. protobuf over
  moonpool RPC is in the adapter only. A gRPC frontend comes later, for integrations that do not
  use the simulation.
- **The host of actors is pure and stays in paros. moonpool gets no actor layer** (Pierre,
  2026-11-22; it replaces the decision of 2026-11-21 that put an actor layer over the RPC in
  moonpool). The pure host holds the role contract, the flight rule and the group commit, with no
  runtime. moonpool RPC is enough as it is: the activation of a role registers a dynamic group
  of endpoints, and its deactivation drops the group (a role that placement released stays
  active until its protocol guard holds: a released acceptor answers until `may_retire` holds).
  The paros adapter keeps the map from the id of a role to its group.
- **A typed interface for each kind of role.** In the adapter, each kind of role has one
  interface of moonpool RPC, with one one-way method for each message of the role. A reply of
  the protocol (a `Promise`) is a later one-way message, because the step sends it after its
  persist. The calls of a client at the frontend are calls with replies. No role takes opaque
  bytes through one method. **Open** (#1330 (the moonpool adapter)): the protobuf types are
  written by hand or generated from the Rust types of the pure crates.
- **An info table at each control level** (Pierre, 2026-11-22; the equivalent of FoundationDB's
  `ServerDBInfo`). The holder of the term of a level keeps a transient table of references, with
  a generation, and pushes it: the machines for a cell, the roles of the journals for a tenant,
  the cells for the universe. A reader long-polls with the generation it knows. It accepts a
  table only from the term that it read in the control journal of the level, and only with a
  newer generation. The table is a hint: the fold stays the truth, each term builds the table
  again from registrations, and no safety rests on it. A machine tells the cell coordinator that
  it is up (its address and its incarnation), and it tells each tenant coordinator the roles of
  that tenant that it hosts, a few at a time. The table is pure state of the coordinator; only
  the push is in the adapter. The core still knows no tenant: a journal role takes the
  references of its peers through a port. **Open**: the exact fields, and the tenant and cell
  tables are built before the universe table.
- **A machine answers "which reference serves this id now"** with one fixed method. A caller
  whose reference is refused asks it one time. So the peers of a role that restarted find it
  when the coordinator of the tenant is down (static stability). There is no fallback to a
  fixed endpoint of the role.
- **A reference changes at each restart; a few services are fixed.** A dynamic reference of
  moonpool RPC names one incarnation of a process, so a message for an old activation never
  reaches a new one. A service is fixed (well-known) when a caller must reach it with an address
  only: the directory of a machine, the bootstrap and the admission of a machine, the frontend
  service for clients, and the health and inspect calls. Every journal role, coordinator and
  controller is dynamic. A reference is not a credential: the right stays with the token at the
  frontend (section 3.5), and the fences stay with the terms and the ballots.
- **The takeover by a new coordinator** (#1422 (the takeover by a new controller); TLC checks it in
  `specs/tla/takeover/`). A coordinator that wins a term knows only its control journal and what
  the machines tell it in that term, and it acts on that view (it frees, moves or heals a role).
  The pattern is FoundationDB's `moveKeysLock`. These rules hold together, and each has a control
  in the model:
  1. **A machine reports the roles that it hosts when it registers** with the coordinator of a
     term. A new term builds its view from the journal and from the registrations and acks of its
     own term, never from the memory of an earlier term. A coordinator that crashed acts again
     only after it wins a new term.
  2. **A registration with term t raises the machine's highest term to t in the same write.** So
     an order of an earlier term that arrives after the registration is refused, and the
     registration names every role that the machine hosts without an order of the new term.
  3. **An order is checked and done in one atomic write**: the machine writes the term of the
     order and the effect in one batch, and refuses an order below its highest term.
  4. **A booking names a machine by its durable id, never by a reference.** The reference of an
     order comes from the registration of the current term.
  5. **A coordinator gets a new reference at each term.** No message falls back to a fixed
     endpoint of a coordinator. A machine gets a new reference at each incarnation (a dynamic
     reference of moonpool RPC), so an order to an incarnation that is gone comes back to its
     sender as an error.
  6. **`Assign` is idempotent by its request id.** **Open** (#1419 (the tenancy wrapper and
     paros-names)): the exact form of the birth nonce of the key that `Assign` carries.
  7. **A deposed coordinator stops** on a refused order (a newer term began) and on a refused write
     to its control journal.
  8. **The error of an order to a gone incarnation marks the machine as unknown** in the view of
     its coordinator, which asks the machine's fixed directory ("which reference serves this id
     now") for its current registration. Without it, a coordinator whose view names a dead
     incarnation orders it for ever. With it, a late registration of an older incarnation does no
     harm, so a view needs no rule on the order of incarnations.
  A machine sends its registration again until the coordinator acks it, and a coordinator sends
  an order again until an ack or a registration shows the role. `paros-cell` and `paros-machine`
  build these rules (#1422).

- **Everything simulates and compiles to wasm** (goal 10). Each pure layer builds for
  `wasm32-unknown-unknown`, as `paros-consensus` and `paros-journal` do today, and the whole
  service runs in the deterministic simulation and in a browser. A crate that cannot build for
  wasm is a finding: `paros-authz-biscuit` does not build for it today.

- **The cut of the old tenant coordinator** (Pierre, 2026-11-22: "The cut is good"). The loop
  that changes the membership of one journal (book, reconfigure, wait for the GC watermark,
  release) is the work of the grid controller of that journal
  ([docs/journal/06-controller.md](../journal/06-controller.md)). The questions that the loop
  asks are questions of the whole cell, and the cell answers them through a **placement port**:
  which member must leave (its machine drains, is slow, is held for maintenance or is gone),
  where a new member goes, and how many moves run at one time. A grant of the port is also the
  token of the rate limit of the cell. "Gone" is a durable record of the cell, not the memory of
  one coordinator. The sets of a tenant, the frontends of a tenant, the move of a tenant between
  cells and the `Configure` of a tenant stay with the tenant and the cell. Section 4.5 gives each
  module of the old coordinator and machine its new home (#1412 (the duty table)).
- **The placement across cells is the one of section 3.7** (Pierre, 2026-11-22: "follow the hash
  assignment" of his post "Notes about cell-based architecture"). The override table is read
  first, then the hash of the name gives a bucket, and a fixed table gives the cell of each
  bucket. The hash, the two tables and their fold are pure and belong to the universe layer. The
  frontend and the resolver are the thin router and serve from the last directory they read. A
  tenant keeps its registration record in the universe directory: it is one write at creation,
  and it is where the fold checks the `TenantId` (section 3.8).

#### The coordination store

**The election library owns a journal and gives a ZooKeeper-style client API over it** (Pierre,
2026-11-24, #1420 (paros-election): "I want the leader election library to own a journal and
write data to it, inspired from zookeeper client"; he chose option B of
`/mnt/project-files/design/election-zookeeper-2026-11-24/proposal.md`, "the log is the server").
The prior art is Zelos, the ZooKeeper clone that Facebook built over the Delos shared log
(`docs/references/papers/delos-log-structured-protocols/`). The crate name, the record header key
and the model folder name are **Open** until the crate is built; the model is `specs/tla/coord/`.

- **The log is the server.** The store owns one journal in multi-writer mode
  ([docs/journal/02-data-plane.md](../journal/02-data-plane.md) §J2.4): every member appends, the
  leader and the candidates too. The journal arbitrates nothing. Every member runs the same
  **fold**: a pure function from the records, in order, to a tree of nodes and a table of
  sessions. The fold judges each record at its position and applies it or voids it, so every
  member computes the same result at the same position. A member learns the result of its write
  when its fold reaches the record (Delos: `propose`, then play the log to that entry).
- **The ZooKeeper concepts.** `seq` is the zxid. A session opens with a record that names a
  session id the program draws (fresh, as a leader uuid, §J2.3). Each record of a session renews
  it, and an idle session writes a heartbeat. A node has data, a version and the positions of its
  create and its last change; an ephemeral node names its session; a sequential node takes the
  position of its create as its name. A record can hold several operations, applied all or none
  (ZooKeeper's `multi`, with `check(path, version)`). A watch is a filter on the fold's changes
  while the member tails the journal, so no change between two events is lost. ZooKeeper's
  `sync` is a read at the tail, which is linearizable (§J2.5). ACLs are out: the token at the
  frontend decides who may touch the journal (section 3.5).
- **Exactly-once effects over an at-least-once journal.** Each record of a session carries
  `<<session, op_seq>>`, and a session sends one record at a time. The fold keeps, for each live
  session, the last `op_seq` it applied: a copy at or below it does nothing (the
  `SessionOrderEngine` of Delos). The table is in the store's fold, not in the journal, so the
  journal keeps no dedup table (§J2.4.1). **An expired session leaves a tombstone**: its id stays
  in the fold, in every checkpoint, and a late copy of any of its records (its `open` too) does
  nothing. Without the tombstone, a late `open` brings the session back with an empty row, and a
  late copy of its first write applies two times. A member settles its record only from its own
  fold. TLC: `OnceEach` (controls `no_dedup` and `no_tomb`), `EphemeralsEnd` (control `no_void`),
  `SettledTrue`. **Open**: the tombstones grow the state; how to drop old ones (for example a
  session id that names the position of its `open`).
- **Expiry is a compare-and-set on the session's last position.** The store reads no clock. A
  member that saw no record of a session since position `last` for one timeout, on the clock the
  program passes, appends `Expire(session, last)`, after a jitter. The fold applies it only if the
  last record of the session is still at `last`; the expiry deletes the session's ephemeral nodes
  in the same step (ZooKeeper's `killSession`). A slow or skewed clock can only expire a live
  session early: that costs availability, never safety. So the compare-and-set on `last` is an
  availability rule: a session that keeps writing is not killed by a stale expiry. TLC checks only
  that the fold carries it out (`ExpireOnSilence`, control `expire_blind`); every safety property
  holds without it.
- **The latch and its term.** The recipes are pure functions of the fold, so no recipe watches
  its predecessor (there is no herd effect: every member tails the journal anyway). The leader
  latch is Curator's `LeaderLatch`: the live session whose node has the lowest position holds
  it. **The term is the position of the record at which the holder changed**: one holder for each
  term, and the term grows (TLC: `TermStep`, `OneSender`). Curator's `LeaderSelector`, locks, semaphores, groups, queues,
  counters, barriers and caches map the same way (the proposal, section 3).
- **Two fences.** Inside the journal, a record can carry a guard ("my session holds the latch
  at term `t`", or a `check`), and the fold voids a record whose guard fails: a deposed holder can
  still append, and its guarded writes do nothing. The guard is needed when the holder is deposed
  while its session lives: another member deletes its node (ZooKeeper allows it), or the session
  has several records in flight. Outside the journal, each order carries the term, and the
  receiver checks it in the same write as the effect (the takeover rules below). `ActsByHolder`
  and `NoRegress` state these two rules; their controls (`no_guard`, `no_fence`) show that the fold
  and the receiver carry them out. A holder that saw
  none of its own records land for part of the timeout stops sending orders (Curator's
  `SUSPENDED`); this shortens the time with two active holders, and safety does not rest on it.
- **Trim to a checkpoint only.** In multi-writer mode anyone with access may `Truncate` (§J2.4),
  and the journal cannot refuse it. So the store's safety has a precondition: only the store
  truncates its journal. The token at the frontend (section 3.5) must give the right to
  truncate to the store alone; a client that truncates elsewhere breaks the fold of every new
  member (control `trim_any`). A checkpoint record holds the whole state of the fold at the read
  position `at` of the member that wrote it. The fold takes a checkpoint only when it lands at
  `at + 1`: a checkpoint behind other records would skip them. A truncate goes only to a
  checkpoint that the fold took, so the first record above the floor gives a new member the
  whole state. TLC: `ViewTrue` (controls `ckpt_stale` and `trim_any`). The rule "the checkpoint
  lands at `at + 1`" is a finding of the model: the proposal had only "truncate to a checkpoint".
  It can starve: under steady writes, a record often lands between the read and the checkpoint.
- **Not proved**: the liveness of the latch (after the holder stops and faults stop, a new holder
  leads), and the liveness of the checkpoint (one checkpoint lands at `at + 1` under steady
  writes; a design that tolerates records between the read and the checkpoint is **Open**). **Open**: who sends expiries and checkpoints (any member, or the holder of an internal
  latch), the heartbeat and timeout values, the size limit of a checkpoint, and whether `TTL` and
  `CONTAINER` nodes are in the first version.

**How CI holds the layers** (#1411 (CI checks that hold the layering)). The layer of each crate
is one table, `scripts/layers.sh`. `scripts/check-layers.sh` reads the manifests through `cargo metadata` (no build) and the
sources, and fails when:

1. a crate of the workspace has no row, or a row names a crate that is gone;
2. a crate depends on a crate of the same layer or of a higher layer (normal, build and dev
   dependencies);
3. a crate of layers 0 to 6 has the identifier of a higher crate (`paros_journal_io`) in its
   `src/`, in code or in a doc comment. The prose name of a crate (`paros-journal-io`) is not an
   item, so a doc comment may cite it. The facade `paros` is not checked: it defines no item, and
   each `paros::` path is a re-export of a lower layer;
4. a crate of layers 0 to 5 has a fault switch (`buggify!` or a fault `hint!`) outside a comment.

The pure crates of `scripts/check-core-deps.sh` (no dependency outside the workspace with no
feature on, observation crates only with every feature on) are the crates of layers 0 to 5 of
the same table, and `scripts/check-layers.sh --wasm` builds each of them for
`wasm32-unknown-unknown`. `scripts/check-examples.sh` stays the check that each composing crate
has an example. A new crate gets its row in the pull request that creates it.

The table today, as a proposal (the names and the numbers are **Open**, open point 1 of #1409):

| Layer | Crates today | Why there |
|---|---|---|
| 0 | `paros-consensus`, `paros-host` | The Paxos flavors, and the pure host of actors (#1410 (split the generic host of actors)): the `Role` trait, the host of many roles and the generic driver under the flight rule. The host depends on no crate, so it sits at the bottom, below every crate whose roles run on it. |
| 1 | `paros-journal` | The journal v1. |
| 2 | `paros-journal-control`, `paros-journal-io` | Both depend on `paros-journal`. The table of #1409 puts the controller in layer 1, but a dependency inside one layer is sideways, so the controller is one layer up. |
| 6 | `paros-authz-biscuit` | It implements the `Authz` port over an external crate (`biscuit-auth`, which draws on `getrandom`), so it cannot be pure. It is an adapter, as the moonpool adapters are. |
| 7 | `paros` | The facade. It will bundle the adapters of layer 6 behind a feature. |
| 8 | `consensus-sim`, `journal-sim`, `paros-wasm` | Crates that only test or show the others. Nothing depends on them. |

The calls of the placement port are `standing`, `place` and `release`, and the cell places its
own control journal through the same port (Pierre, 2026-11-22, #1421 (the placement port);
`docs/journal/06-controller.md` §J6.3, `specs/tla/placement-port/`).

**Open**: the layer table. It is a proposal until Pierre agrees.
