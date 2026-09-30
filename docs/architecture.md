# paros: the journal service

This is the end goal. AGENTS.md describes what paros is today and the doctrine every change
follows; this document describes what paros is becoming, so that every issue, plan and session
aims at the same target. Where the two disagree, AGENTS.md is the present and this is the
direction. Decided on 2026-09-30; the milestones at the end carry the issue numbers.

## 1. Goal

`parosd`: one binary on N machines, started with `docker compose`, serving journals to tenants
over a four-call data plane, healing itself through reconfiguration when a disk or a machine is
lost. The control plane (tenants, journals, machines, placement, desired state) is itself stored
in journals of an admin tenant and coordinated through the same election primitive the tenants
use: paros eats its own food. Single region, several failure domains. Compaction and snapshots
are the user's business: paros owns the log, never the state.

The model is the AWS Journal, the replicated log behind Aurora DSQL, MemoryDB and Lambda, as
described in the public sources listed in section 10: a durable, ordered, fenced log that stores
decided outcomes, that a single writer appends to under a generation, and that every consumer
tails without a second protocol.

The first deliverable is a toy: a local Docker Compose cluster an operator can create a tenant
on, write to, read from, break and watch heal. The homelab and anything beyond one region are
not in scope.

## 2. The data plane

Every journal exposes four calls. Every rule below is judged at apply time, in slot order, by a
small per-journal state machine `(owner, generation, next_seq, first_seq)` that lives in
`paros-core` beside the replica's walk.

| Call | Meaning |
|---|---|
| `Write(generation, owner, seq, batch)` | Append `batch` at `seq`. Fenced by `(generation, owner)`, contiguous by `seq`, idempotent on retry, pipelineable. |
| `Read(from_seq, limit, wait_ms?)` | The committed records from `from_seq`, plus `first_seq`, `next_seq` and `cur_gen`, or `Truncated` when `from_seq < first_seq`. Long-polls at the tail for `wait_ms`. |
| `Truncate(up_to_seq)` | Drop every record below `up_to_seq`. Monotone. The only retention API. |
| `SetLeader(expected_gen, new_owner)` | Compare-and-swap the owner. Returns `{ generation, next_seq }`. |

### 2.1 Positions

`seq` is the dense position of accepted records, assigned at apply. One `seq` per record: a
batch of `n` records occupies `[seq, seq + n)`, in one Paxos slot, accepted or refused whole.
Paxos slots stay internal: a `Noop` a new leader fills a hole with, a control command, a
generation change and a refused `Write` each consume a slot and no `seq`. Readers never see a
hole.

### 2.2 Writes and fencing

A `Write` is accepted iff its `(generation, owner)` is the journal's current one and
`seq == next_seq`. Otherwise it is refused in place, and the refusal names the current
generation and `next_seq` so the owner can continue or learn it was superseded.

Retries are answered from the log itself: a `Write` with `seq < next_seq` whose batch hash
matches the records at `seq` is an idempotent ack; a different hash is refused. A retry whose
`seq < first_seq` is answered `Truncated`, and the owner treats it as ambiguous and reads the
tail. The log is the deduplication table: there is no per-client session ledger and nothing to
expire.

A leader may refuse a `Write` from its own fold at propose time, so a superseded owner's
pipelined burst does not burn slots. That is an optimisation; the apply-time check is the safety.

### 2.3 Ownership

Ownership is a pure compare-and-swap, no lease and no clock. `SetLeader(expected_gen, new_owner)`
succeeds iff `expected_gen` is the current generation; the journal assigns `generation + 1`,
records the change as an ordinary log entry, and answers `{ generation, next_seq }` so the new
owner can continue the sequence. Every tailer learns the owner changed in-band, without a side
channel.

A superseded owner's writes are refused at apply, which is the whole safety argument. What keeps
a superseded owner from *serving* stale data is a rule on the owner, not on paros: an owner
serves nothing from local state it did not read back from the journal (DSQL's adjudicator is a
rebuildable cache over the log and holds no truth of its own). MemoryDB's lease and self-demotion
are deliberately not implemented; the sources are in section 10.

Two fences, two layers, never confused: the Paxos ballot says which *machine* runs a journal's
leader and is invisible to clients; the generation says which *client* may write and is
invisible to Paxos.

### 2.4 Reads

`Read` is served by the replica tier through the leaderless read of Compartmentalized Paxos
§3.4 (paros's `QuorumRead`): the replica asks a Phase-1 quorum of the acceptors for their vote
watermarks, waits until its own applied prefix covers the maximum, and answers. No read goes
through the leader, so reads scale with replicas and cost the acceptors one watermark round per
page. The read-index path and `CheckTail` retire.

### 2.5 Truncation

`Truncate(up_to_seq)` is proposed through consensus and applied lazily by every node when its
contiguous chosen walk reaches it, exactly today's `Truncate` control command. The caller
truncates only after it has secured whatever snapshot it needs; paros does not check that, does
not verify snapshots and never will. A reader below `first_seq` is told `Truncated` and nothing
else: where it gets a fresher snapshot is the application's contract with itself.

### 2.6 Underneath

Paxos is unchanged: replication, holes, gap fills, the contiguous chosen prefix, CTRL, the trim
point, matchmaker reconfiguration. Every proof paros has holds per journal; the one new thing to
prove is the control state machine's rules, and they are judged in the simulation like every
other rule.

## 3. The control plane

### 3.1 The admin tenant

Tenants, journals, machines, placement and desired state are journals of an admin tenant, served
by the same `parosd`s, the same Paxos and the same stores as every tenant's journals. Today's
system journals are its first three: journal 1 the directory, 2 the machine registry, 3 the
desired state. Every tenant gets a control journal when it is created.

Bootstrap: the admin tenant's control journal starts as the seeds' static plain journal, exactly
as the system journals do today. The first coordinator claims it with
`SetLeader(expected_gen = 0, me)` and writes journals 1 to 3 under its generation from then on.
System journals are written with `Write` like any journal; there is no special path.

### 3.2 Machines

Every `parosd` is uniform. At start it registers in the machine registry with its address, its
failure domain, its class and its capacity, and it starts the driver for every
`(tenant, journal, role)` assigned to it. There is no cluster file: a machine holds one
rendezvous name that resolves to whoever serves the admin tenant's journals, publishes its RPC
interface reference (moonpool-rpc's incarnation-bearing `InterfaceRef`) in its registration,
and learns every other address from the registry fold. A rebooted machine publishes a new
reference, holders of the old one are refused by the transport, and the coordinator re-places
on the publication rather than on a heartbeat window. The decision and its alternatives are
#216. Classes are FDB's:

- `storage`: anything with a durable store. Acceptors, replicas, matchmakers.
- `stateless`: the front door, proxy leaders, batchers, unbatchers, tenant coordinators.

Scaling a role for a tenant is adding machines of the right class and raising the tenant's
desired counts.

### 3.3 Coordinators and placement

One coordinator per tenant, elected by `SetLeader` on the tenant's control journal, so the
coordinator holds a generation and is fenced like any writer. It computes placement
deterministically from the registry, the failure domains and the tenant's desired state, and
writes it as fenced entries into the control journal. Two coordinators booking the same capacity
are resolved by the same rule as everything else: judged at apply in slot order, the loser's
claim is refused and it recomputes. Machines act on what they fold.

`Reconfigure` is how a journal moves: off a drained machine, off a dead identity, onto a spare,
between quorum systems. The admin tenant's coordinator is the cluster-wide reconciler and
watches every tenant's coordinator.

A coordinator holds no state the control journal does not: a new one resumes from a fold.

### 3.4 Tenant modes

A tenant's desired state names, per journal or as a tenant default, its quorum system
(majority, flexible `{q1, q2}`, grid `{rows, cols}`), its replication count and, per role, how
many proxies, replicas and batchers it wants. This is FDB's `configure`, applied by the
reconciler through reconfiguration. "Classic Multi-Paxos" is quorum system = majority.

Every tenant has one matchmaker set: per tenant, not per journal (the registry is keyed by
journal inside the set, so a set per journal buys nothing) and not shared across tenants (a
shared set is one role that could not scale per tenant and a blast radius across tenants).
Reconfiguration is the operational primitive for everything, so no tenant opts out of it. The
matchmaker-free plain deployment stays what AGENTS.md says it is: a permanent library-level
configuration, not a tenant mode.

### 3.5 The front door

A stateless process in front of the machines. It authorizes the caller through an `Authz` trait
whose first implementation verifies a signed JWT carrying the tenant as a claim; it resolves a
tenant's journal names from a local fold of the directory; it enforces quotas; and it routes each
call to the machine serving the journal, so a client never knows placement. Past the front door
nothing knows a tenant, only a journal id.

Tenants are created and administered through the same front door with an admin-tenant JWT: one
API, one `Authz` trait, exercised in the simulation like every other call.

The front door is not the batcher. The batcher is a data-plane role of Compartmentalized Paxos
that coalesces writes before a leader; the front door is authorization, naming, quotas and
routing.

### 3.6 Status

`paros status [--tenant t]` shows three columns, globally and per tenant: desired (what the
tenant asked for), available (machines registered, not drained, seen alive) and current (what
is placed and serving, with each journal's word: Healthy, Degraded, Unavailable). All three are
folds of the admin tenant's journals. There is no separate monitoring store.

## 4. Roles

All six roles of Compartmentalized Paxos, plus the matchmakers of Matchmaker Paxos, each
scalable independently per tenant by the reconciler:

| Role | Class | In paros |
|---|---|---|
| Leader (proposer) | storage | `Proposer` inside `ColocatedNode` |
| Proxy leader | stateless | `ProxyLeader`, `run_proxy` |
| Acceptor, grid quorums | storage | `Acceptor`, `QuorumSystem::Grid` |
| Replica | storage | `ReplicaNode`, `run_replica`; serves `Read` |
| Batcher | stateless | to build |
| Unbatcher | stateless | to build |
| Matchmaker | storage | `Matchmaker`, `run_matchmaker`; one set per tenant |
| Front door | stateless | to build |
| Coordinator | stateless | to build |

## 5. Failure model and zones

The service survives disk loss and machine loss in one region: a corrupted record is CTRL's
`faulty` and repaired by the protocol, a wiped disk is refused at boot as amnesia and the
identity is replaced by reconfiguration, a crashed machine restarts as an existing member, a dead
machine is reconfigured out and its journals placed elsewhere. This is what the simulation
already exercises; what changes is who drives the healing: today the harness's client composes
the reconfigurations, in the service the tenant's coordinator does, from desired state.
Cross-region replication and witness replicas are out of scope.

Zones are failure domains. What the WPaxos read (section 10) established for one region with
several availability zones:

- Surviving one zone loss needs `fz = 1`, and then every Phase 2 spans two zones, exactly like a
  zone-balanced majority, while tolerating fewer node failures than that majority. WPaxos's
  latency win exists only at `fz = 0`, which gives up zone survival. Its per-zone quorum system
  is therefore not adopted.
- paros's grid cannot express zone survival either: rows and columns are positional over sorted
  ids, and with column = zone a zone loss kills every row, with row = zone it kills every column.
  The grid stays the throughput mode, with its cost (one dead acceptor freezes its column until
  reconfiguration) stated to the tenant that picks it.
- What is adopted: zone labels live inside `AcceptorConfig`, bound to the ballot with the
  configuration, because two nodes that disagree on a member's zone evaluate different quorums
  (the registry's failure domain is the composer's input, never read live by a tally); the
  reconciler's placement rule is judged through `QuorumSystem`, never a count: removing any one
  zone must leave a Phase-1 and a Phase-2 quorum, and every Phase-2 quorum spans at least two
  zones; a journal's leader is placed toward the zone that writes to it through `relinquish_to`,
  which is WPaxos's steal without a Phase 1; matchmaker sets are zone-spread, since their quorums
  are majorities; and the simulation gains a zone-kill attrition mode and a zone-aware copy
  budget, without which it cannot prove zone survival.

## 6. Verification

Simulation is the investment. Every milestone lands with its share of:

- Invariants in the audit, where the fact arrives: one owner per generation, generations
  monotone and present in the log, `seq` dense per journal, a `Write` never re-accepted with
  other bytes, `Truncate` monotone and `first_seq` never above a served cursor, a `SetLeader`
  winning at most once per `expected_gen`, placement never double-booked, a tenant never reaching
  a journal outside its prefix.
- A real linearizability checker in the workload's `check()`, over the four-call history with
  `Ambiguous` outcomes, against the sequential model of a journal (an owner, a generation, a
  dense log, a floor). It replaces the per-operation rules of today's `ClientHistory`.
- The three races made likely rather than lucky, each a knob or a hook with its own BUGGIFY
  location and its reachable: a `SetLeader` drawn in the middle of a pipelined burst, a client
  timeout shorter than the ack so a retry crosses an ownership change, a `Truncate` racing a
  reader's cursor.
- New BUGGIFY sites for every new decision the driver, the front door and the coordinator take,
  and the coverage-guided sweep saturating over them.

No new model checker and no separate specification: the two existing sans-IO model checkers
stay as they are.

## 7. What changes against today

- The journal API of #185 (`Append`, `Read`, `CheckTail`, `Trim`) is cut over to the four calls.
  No compatibility layer. The chain workload's operation ids for retired calls stay reserved.
- The "#186: paros runs no application" line becomes: paros runs no *user* application, and one
  journal-control state machine per journal, in `paros-core`, judged at apply.
- The `(client, seq)` at-most-once session ledger goes away; the log is the deduplication table.
- The read-index path retires; the leaderless read serves `Read`.
- Only the first user journal of a process may have matchmakers, proxies or replicas today; per
  tenant modes on many journals per process make journal-tagged planes and per-tenant matchmaker
  sets prerequisites, not options.
- `parosd` is uniform with a class, not one role per process.
- The homelab is not a target; Docker Compose on one host is.

## 8. Milestones

Milestones are labels (`milestone:M7` and up); `milestone:M6` stays the epic's umbrella. The
toy is the end of M9. The epic is #184, the backlog pointer #69, the verification track #24.

| Milestone | Name | Content |
|---|---|---|
| M7 | Journal API (#204, #205) | the four calls, the journal state machine in core, the wire and the driver, the chain workload's alphabet, the linearizability checker, the race knobs and hooks, the cut-over |
| M8 | parosd deployable (#206 to #209, #196; #176, #201, #202 join it) | Tokio providers linked, the stores on a real filesystem for the first time, the `JournalStores` opener, `Config` durable at `format`, `parosd provision`, the uniform binary with class and capacity, Compose, the `paros` CLI, a tracing subscriber, exit codes; the API-independent parts start in parallel with M7 |
| M9 | Tenants and control plane (#216, #192, #211, #190, #210, #212, #191, #213) | the admin tenant over the system journals, the tenant and journal creation API, the machine registry with class and capacity, the per-tenant coordinator via `SetLeader`, placement as fenced writes, the front door with JWT `Authz`, per-tenant matchmaker sets, `paros status` |
| M10 | Roles per tenant (#193, #214, #194, #145, #195) | journal-tagged proxies and replicas, batchers and unbatchers, tenant modes applied by the reconciler, the benchmark, then scale work |
| M11 | Zones (#215) | zone labels in `AcceptorConfig`, the placement rule, leader placement toward the writer's zone, zone-kill attrition and a zone-aware budget in the simulation, zone-spread matchmaker sets |

Verification is not a milestone: every milestone carries its own share of section 6.

## 9. The toy, done means

From a fresh clone: `docker compose --profile provision up`, then `docker compose up`. Create a
tenant and mint its JWT. Create a journal. `write`, `read` and `tail` from the CLI. `set-leader`
to a second client and see the first one refused. Kill one `storage` and one `stateless`
container and keep writing. Wipe one volume, see the amnesia refusal, and see the journal healed
by reconfiguration onto another machine. `paros status` shows desired, available and current,
per tenant and globally. The simulation is green in every shape and the coverage-guided sweep
saturates.

## 10. Sources

The AWS Journal, as publicly described:

- Brooker, "MemoryDB: Speed, Durability, and Composition",
  <https://brooker.co.za/blog/2024/04/25/memorydb.html>: the API evolution `write` and `read`,
  then lease-fenced writes, then `set_leader` as a compare-and-swap.
- Bowes, "The Adjudicator", <https://marc-bowes.com/dsql-adjudicator.html>: a superseded writer's
  commit is rejected; pipelining by expected sequence number, a refusal cascading to the writes
  behind it; generations as a later precondition.
- Bowes, "Meet Coupler", <https://marc-bowes.com/dsql-coupler.html>: journals added and removed
  transparently; every consumer a tailer.
- Brooker, "DSQL Vignette: Transactions and Durability",
  <https://brooker.co.za/blog/2024/12/05/inside-dsql-writes.html>: the journal stores decided
  outcomes, never proposals; durability is the journal's commit.
- Brooker, "Why Strong Consistency?", <https://brooker.co.za/blog/2025/11/18/consistency.html>:
  strictly monotone per-journal streams plus a reader that waits until caught up are the whole
  consistency mechanism.
- Brooker, "Wait! Isn't That Impossible?",
  <https://brooker.co.za/blog/2024/12/06/inside-dsql-cap.html>: the adjudicator holds no durable
  state and is rebuilt from the committed transactions; the witness region.
- Brooker, "Control Planes vs Data Planes", <https://brooker.co.za/blog/2019/03/17/control.html>:
  what belongs on the request path and what scales with the fleet.
- Amazon MemoryDB, SIGMOD 2024,
  <https://cdn.amazon.science/e0/1b/ba6c28034babbc1b18f54aa8102e/amazon-memorydb-a-fast-and-durable-memory-first-cloud-database.pdf>:
  §4.1 conditional append and leadership as one more conditional append, the lease and
  self-demotion; §7.2.1 snapshot verification by a checksum carried in the log.
- Aurora DSQL, arXiv 2607.13276, <https://arxiv.org/html/2607.13276v2>: §5 the journal's
  precondition on timestamp monotonicity, §6 TLA+ and P then deterministic simulation.
- Demirbas's MemoryDB summary,
  <http://muratbuffalo.blogspot.com/2024/05/amazon-memorydb-fast-and-durable-memory.html>, and
  the 2025 Journal reconstruction,
  <https://ajalab.github.io/posts/2025-08-14-journal-distributed-log-replication-behind-aws/>:
  secondary readings; the latter states plainly that retention and truncation are undocumented.

Zones:

- Ailijiang, Charapko, Demirbas, Tasci, "WPaxos: Wide Area Network Flexible Consensus", IEEE
  TPDS 2019, <https://arxiv.org/abs/1703.08905>: §3.1 the per-zone quorums `fz`, `fn`; §3.2 to
  §4 object stealing; §5.3 degraded operation and reconfiguration. The printed TLA+ quorum
  definition does not intersect; only the floor form the proof uses is sound.

Compartmentalized Paxos and Matchmaker Paxos are in `docs/references/papers/`.
