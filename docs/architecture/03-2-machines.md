[← 03-1-cell-tenant-and-bootstrap.md](03-1-cell-tenant-and-bootstrap.md) · [Index](README.md) · [03-3-coordinators-and-placement.md →](03-3-coordinators-and-placement.md)

### 3.2 Machines

**Rule 5: a machine is an id.** A machine is its id and its incarnation. Its address is a hint,
and "gone" is a record. **Not proved** until the TLA+ gate of phase 5 (`cell-control` with a
machine that is wiped and admitted again) is green.

Every `parosd` is uniform. **Identity** has two parts and one hint (decided on 2026-10-02,
#225; the address became a hint with decision 8):

- `node_id`: random, minted at format, stored beside the format marker (#147). It is the
  machine's identity and the registry key. A wiped disk gets a new `node_id` (decision 4), so "a
  wiped identity never rejoins" holds by construction.
- `incarnation`: moonpool-rpc's per-start `Incarnation`, carried in the `InterfaceRef`. Before
  any step that cannot be undone, a member echoes a fresh nonce for its incarnation (step 4.2),
  and an order to a machine names the incarnation that this echo proved (rule 3).
- `addr`: a hint, the machine's advertised address (`PAROS_ADVERTISE`, below). It may change
  across restarts. Consensus never waits for it.

**One thread** (decided on 2026-10-22): every `parosd` and `parosctl` runs on one compio runtime
thread (io_uring on Linux, polling where a seccomp profile refuses it). The library is written
against moonpool's local provider traits (`paros::ParosProviders`), so its futures need not be
`Send`. The simulation runs the same code on `SimProviders`. The only other thread is the OTLP
exporters' Tokio runtime, and only when `PAROS_OTLP_*` asks for one.

The same `node_id` with a new incarnation is a reboot: the machine rejoins in place and keeps its
assignments. The same `node_id` with a new `addr` is a machine that moved; ScyllaDB moved from
IP-based to host-ID-based identity for exactly this reason.

**Roles of a machine** (decision 15, question 6). The operator gives roles to a machine:
`PAROS_ROLES` is a set of `storage`, `stateless` and `frontend`, and `all` (the default) is the
three. The reserved tag `paros/roles` carries it (section 3.7). `PAROS_ROLES` is set at format. A
restart may widen it. A restart that narrows the roles of a machine that joined a cell is
refused, at the machine and at the registry: the cell may book a role on it that it would no
longer take. Each role instance is of one kind, as FDB's classes:

- `storage`: anything with a durable store. Acceptors, replicas, matchmakers.
- `stateless`: proxy leaders, batchers, unbatchers.
- `frontend`: the entry role of section 3.5 and the resolver.

On a machine that takes every role, placement still keeps a journal's kinds apart: a proxy leader
or a batcher never shares a machine with an acceptor or a replica of its own journal. **There
is no coordinator role** (decisions 7 and 15, question 5): any machine of the cell that proved
it serves can be the coordinator, and a stateless machine tries first (section 3.3).

**Static and dynamic endpoints** (decided on 2026-10-10, landed on 2026-10-11, #404 (static vs
dynamic endpoints)), after FDB's `WellKnownEndpoints.h`. A static (well-known) endpoint is what an
operator or a new client reaches with an address alone. Everything found through one is a moonpool
dynamic group (`RpcInterface`), whose reference names one instance: a request to another instance
fails at the transport (`StaleIncarnation`, `EndpointNotFound`).

| Static (well-known) | Why |
|---|---|
| `Identify` | bootstrap; the coordinator's liveness probe |
| `PrepareCell`, `FormCell`, `CellInit` | the `cell init` decree, before a registry exists |
| `Admit` | `cell add-machine` reaches an idle machine by address |
| `Inspect`, `View`, `Load` | operator and simulation calls by address (FDB `WLTOKEN_PROCESS`) |
| `Resolve` | the client entry (FDB `OPENDATABASE`) |

| Dynamic group | One per | Published in |
|---|---|---|
| `MachineInterface` (`0x5041_1001`): `Deliver`; the matchmaker calls (the register of rule 1, garbage collection, the handover); the data plane `Write`, `Read`, `Truncate`, `SetLeader`; the orders of the coordinator | incarnation of a machine | `IdentifyAck.machine_interface`, the node-only `InspectReply.machine_interface` and the boot announcement, kept by the registry fold by `node_id`; a `Resolve` answer gives each machine's (`Member.machine_interface`) |
| `CoordinatorInterface` (`0x5041_1002`): the requests of machines, operators and tenants | coordinator term | the first record of the term in the cell control journal: its `interface` address and its `reference` (rule 3) |


**Addresses are hints** (decision 8, step 5.2; FoundationDB's model). A machine's configuration
carries two addresses, as FDB's `listen_address` / `public_address` and CockroachDB's
`--listen-addr` / `--advertise-addr` do: `PAROS_LISTEN`, what it binds (may be a wildcard), and
`PAROS_ADVERTISE`, what peers and clients dial (may be a hostname). `PAROS_ADVERTISE` defaults to
`PAROS_LISTEN` when that is not a wildcard, and start is refused when `PAROS_LISTEN` is a
wildcard and `PAROS_ADVERTISE` is unset. An advertised name is kept as a name and resolved at
dial time (`paros::Names`), so a machine whose IP changes heals with no registry write.

- **Entry names.** Each machine keeps a short list of **entry names** in its configuration: the
  names of machines of its cell. At least one must still answer after a restart. The word is
  "entry names" and never "seed", which means a simulation seed in paros.
- **The boot announcement.** At each boot a machine announces its id, its incarnation and its
  current address to its entry names and to the peers that it saved. This is not a gossip
  protocol: one announcement per boot, to known peers.
- **Peers learn from proven senders.** A peer keeps addresses in memory. It learns an address
  from a sender with a proven identity (the sender's id, address and machine interface ride
  every peer batch, `Deliver.from*`) and from its cached registry. A batch of an incarnation it
  saw replaced never moves the lane back.
- **The registry records the address later.** The coordinator writes the new address in the cell
  control journal as a record, and consensus never waits for it. An address is never written
  through a journal that the moved machine itself serves (the limit of the old design: a cell of
  two could not heal a moved member). A wrong address costs liveness, never safety: every batch
  names its cell.

**The cached registry** (#211). Every machine of a cell keeps the cell's address book at one
registry position in a file beside its machine record, rewritten whole and atomically. At every
start it dials the machines of its cell where the cache says. A cache is a hint, never a fact: a
cache another identity left, or one that does not parse, is ignored. The durable cached registry
plays the role CockroachDB gives gossip (node addresses off the consensus path): it lets machines
find each other while the registry is unavailable. It is a static-stability requirement, not an
optimisation.

**A missing machine is in one of three states** (rule 5, step 5.3; FoundationDB's model):

- **Unreachable** is the transport's verdict, and it can end. One keep-alive for each peer
  decides it (rule 6, below). A reboot inside the bound rejoins in place and keeps its roles, so
  a whole cell restarting does not trigger a re-placement storm.
- **Incarnation gone** is proved by a live peer: the machine answers as a new incarnation, or a
  store that fails every open is gone for its journal (step 5.5). It is acted on at once: the
  member is replaced.
- **Machine gone** is a record, `MachineGone`, in the cell control journal. The coordinator
  writes it after the routine of rule 4 replaced the machine in each set, one for one, so the
  record makes no set smaller (rule 2). It removes the id from the registry and from the count
  of machines that the cell can place on, and the registry names no dead id after it.

**With no machine to take the place** (decisions 17 and 24). The coordinator does not write
`MachineGone` by itself: the id stays an unreachable member, each set keeps its size, serves
with its live members, and the cell reports "cannot place". The set heals when a machine joins.
A machine that returns with a new id (decision 4) is a machine that can take the place. The
operator can also declare a machine gone with one command before a replacement exists, as with
FoundationDB's `exclude failed`. The cell then refuses that id if it returns, each set keeps its
size with a member that is gone, and the cell still reports "cannot place" until a machine takes
the place. A safety check refuses the command when the live members without that machine are not
a quorum of a set. No word skips the check (FoundationDB's `FORCE` has no paros twin). The
command is `parosctl cell remove-machine --failed <machine>` (decision 35): the word comes from
FoundationDB's `exclude failed`, and the verb is the `remove-machine` that paros has for a
drained machine.

FoundationDB does the same (read at commit `04c62ec`): data distribution marks a storage server
failed after `DATA_DISTRIBUTION_FAILURE_REACTION_TIME` (60 s) and copies its data elsewhere; the
server leaves the metadata only when all its data moved, so with no spare machine it stays and
the replication is not lowered. `exclude failed` declares a machine dead behind a safety check,
and the machine rejoins only after `include failed`.

**Liveness records** (decided on 2026-10-04). The cell coordinator watches the cell's machines
with the transport's failure detector and writes only the *changes* into the cell control journal
(`Down`, `Up`), so control writes stay rare (section 3.9) and "seen alive" is a fold, not a
heartbeat log. A machine never heartbeats into a journal. The registry refuses a liveness entry
that changes nothing.

**Rule 6: one timing model** (steps 6.1 to 6.3; ledger pattern 5). **Not proved** for its
election part until the TLA+ gate of phase 6 (the `election` model with pre-vote) is green.

- **One `Backoff` type and one classifier for every refusal**, in the client and the drivers. A
  duel backs off toward about 1 s. An overload or a frozen set backs off toward about 30 s. A
  lagging peer is skipped.
- **Timeouts come from the measured round trip** and the measured fsync time, inside floors and
  ceilings that a start-up check enforces (#977 (floors and knob ranges)).
- **An unreachable peer is the transport's job.** The application does not time requests, each
  peer has one keep-alive, a watch is a long poll that the server holds, and one helper routes a
  call to a journal's leader (it replaces the three leader discovery loops of the client, the
  frontend and the batcher).
- **A timer that fires twice with no reset is a panic** in the simulation (moonpool).
- **Pre-vote** (#580, step 6.7): a candidate first asks whether it could win, so a healed
  partition does not depose a live leader. The stickiness test uses the same clock as the
  election timer, and a handoff successor must pass it.
- **Cell policy is one record** in the cell control journal (step 6.4, #364). The environment of
  a process keeps node-local pacing only.

**Clean stop** (#560 (clean Ctrl-C shutdown), landed on 2026-10-18). `SIGINT` (Ctrl-C) and
`SIGTERM` give the same clean stop. `parosd` only turns the signal into the machine's shutdown
token; the stop is the library's, so the simulation runs the same code. The node loop serves on
while it stops:

1. **Stop accepting.** A `Write`, `SetLeader`, `Truncate` or `Read` that arrives is refused at
   once, `stopping: true` in its ack, with no verdict: retry elsewhere. Peer traffic goes on,
   because the handoffs need it.
2. **Finish the work in flight.** Every held call is answered from the slot it waits on, and
   every read waits out its own bound.
3. **Hand the leaderships on**, at the same time as step 2. Each journal the node leads hands its
   Phase-2 authority to a member (`relinquish_to`, the cooperative handoff). The stop waits for
   the successor's first beat at that ballot. A coordinator that runs on the machine stops
   writing; the next candidate wins the term with `SetLeader` after one lease (section 3.3).
4. **Flush.** Every batch whose flush flies lands.
5. **Exit.** The loop returns, `parosd` logs `parosd_stopped` and exits 0.

The bound is the `shutdown_within` tunable (`PAROS_SHUTDOWN_WITHIN_MS`, 30 s in the shipped
profile, floor 0): when it runs out, the loop returns anyway and logs `stop_timed_out`. A second
signal exits at once with code 130. Each of these is a crash, which recovery already handles, so
no bound is a safety rule. A machine's frontend stops with it: it refuses new calls
(`stopping`), waits for the ones it took and returns.

**Slow machines** (#361 (slow machines), 2026-10-16; gray failure). The failure detector above is
binary, but a slow disk or a starved CPU answers, late, and slows every write of every journal
whose Phase-2 quorum holds it. So each watch round also measures every machine that answered:
its `Identify` round trip on the coordinator's clock, and its disk and CPU busy ratios from its
last `Load` window (#424 (busyness metrics)). `paros::machine::slow::SlowJudge` judges them with
hysteresis: a machine leans slow when a busy ratio is at or above `slow_busy_permille`, or its
round trip is at or above `slow_rtt` and four times the round's median (a slow coordinator sees
every round trip long and marks nobody for it); it leans fast below both with a margin (200 ‰,
half the round-trip bounds). Only `slow_rounds` rounds in a row against what the registry holds
change the judgement, written `MachineSlow` (changes only, with the round's facts: `rtt_us`,
`disk_busy_permille`, `cpu_busy_permille`). A new incarnation starts fast. **A slow mark never
takes a quorum**: at most a minority of the pool is held slow (`SlowJudge::cap`), and a mark
removes nothing by itself. A slow machine keeps no role, like a draining one
(`Registry::keeps_roles`): the cell moves each journal off it with the routine of rule 4 (section 3.3), placement books it last,
as a down machine. An open maintenance
window (#359) suspends this, as it suspends a down machine's re-placement: the mark stays
written, but no slot moves off a machine the window covers. A grid leader sends a
round's Phase 2 to a column with no slow acceptor when its own column holds one, and a quorum read
asks such a row (`fast_column`, `fast_row` in `crates/lib/paros-roles/src/node.rs`); a majority already decides on the
fastest replies. `parosctl` shows the machine `slow` (`MachineView.slow`). The simulation's
slow-machine scenario holds a machine that holds a slot slow for 10–16 s on half its strikes in the chaos
window; its oracles: a slow mark never takes a quorum, and after chaos no journal with a machine
to go to keeps a slot on a slow machine.

**Full disks** (#360 (disk full), decided on 2026-10-18). A full disk is a normal result, never a
crash or a lost write. The journal store preallocates its segments, so a write inside a segment
never needs space; only a segment roll or a new journal's files do. Such a write that finds the
disk full fails with `ENOSPC` before any byte lands, and the store answers
`StorageError::Full` (known lost). The driver decides it `StorageFaultDecision::ReadOnly`, not
`Crash`:

- The journal is quarantined, as on any storage fault, because its core may hold the refused
  batch's effects. A journal a full disk took never ends the node, even when it was the last one
  live.
- It re-opens **read-only**: a state of the live journal, not a longer quarantine. The decision
  is a new state, so the node keeps its Phase-1 promises and its reads. A read-only journal votes
  in Phase 1 and serves reads. It refuses every Phase-2 vote (`Accept`), the records of a
  catch-up and a handoff to it, as lost messages, and it never campaigns. The reason is typed:
  `reason = "disk_full"` on `journal_read_only` and `phase2_refused`.
- `read_only_ticks` (30 s in the shipped profile, floor 1) after the fault, it is writable
  again. The time counts from the fault, not the re-open, so a full disk holds a journal
  unwritable for the longer of the quarantine and `read_only_ticks`, not their sum. A write
  that still finds the disk full makes it read-only again. A truncation frees space: the store
  removes its dead segments once the floor passes them.

Each watch round also reads the free bytes in every machine's `Load` answer (`has_space`,
`free_bytes`, `total_bytes`, from moonpool's `SystemSample.space`, `statvfs` in production). A
machine below `free_bytes_floor` (1 GiB in the shipped profile, floor 0: none) is written
`MachineSpace { low: true }`, and `low: false` once it is above again (changes only, for the
incarnation the registry holds; a new incarnation starts with space). Placement books a machine
low on space last, as a down one; the mark moves no role already placed. The simulation's
disk-full scenario (`paros_sim::world::disk_full`) gives the machines' and the acceptors' disks
a capacity a little above what they use, sometimes counting each byte two to four times
(moonpool#343 (disk capacity and ENOSPC)), then grows them back; recovery mode lifts every
capacity. Its gates: a store went read-only and came back, a machine marked low on space and with
space again. Its oracles are the campaign's: no acknowledged write is lost, and the audit judges
every full disk `ReadOnly`, never a crash.

**Finding the cell.** There is no cluster file and no rendezvous name. A machine learns its cell
when it is admitted (`FormCell` or `Admit`, section 3.1) and caches it durably. On every later
start it finds the cell from its entry names and its cached registry. If none of them answers,
an admin admits it again (YugabyteDB makes the same trade).

A client's only static input is an **entry endpoint**: a few addresses, or a DNS name the
operator puts in front of them, never stored by paros (as Kafka's `bootstrap.servers`). From
there it calls `Resolve` (section 3.5).

- **No journal is found by convention** (decided on 2026-10-04, section 3.8): there is no
  well-known tenant or journal id. Every machine of a formed cell answers `Inspect` (and
  `Resolve`) with its `cell_id` and the control `JournalIdentifier`s, read from its durable
  cell plan and its registry. **An `Inspect` names its journal or asks for the node alone**
  (decided on 2026-10-05, #243); one that names no journal without asking for the node alone is
  refused (`unset`).
- **`cell_id` rides every peer batch** (decided on 2026-10-10, #216): a receiver of another cell
  refuses the whole batch before it decodes a message, and a matchmaker host refuses a request of
  another cell before it reads the tenant (#539 (matchmaker cell fence)). A move between cells
  opens this fence for the moving namespace only (section 3.7).
- **Well-known endpoints** are the bootstrap set, `Inspect` and `Resolve`; everything else is a
  dynamic reference (the tables above).

**No capacity in role slots; the cell reports busyness** (decided by Pierre on 2026-11-01,
#837 (remove role slots); it replaces "capacity is role slots" of 2026-10-04). A machine has
no capacity. A booking records where one role instance (an acceptor, replica, matchmaker,
proxy leader, batcher or unbatcher) of one journal or tenant runs. It is keyed
`(tenant, journal or matchmaker set, role)`, and only the cell coordinator writes bookings.
Placement spreads roles by busyness, then by the count of roles each machine hosts (section
3.4). Each machine measures its busyness (CPU, run loop and disk busy, #424 (busyness metrics))
and answers it through `Load`. The slow judge moves roles off a machine that stays too busy
(#361 (slow machines)), and the balancer (#687 (balance a cell)) evens out the rest. Why: a slot count was a
guess of the operator that said nothing of the real load. A heal or a drain also waited forever
when no machine had a free slot. `PAROS_CAPACITY` is ignored, and an old machine record or
registry entry that names a capacity still reads. Scaling a role for a journal is adding
machines that take the role and raising the journal's counts with `configure` (rule 7).

**Placement counts the disk each store allocates** (decided by Pierre on 2026-11-11, #985
(placement counts store disk); it amends "no capacity" above for the disk alone). Every store
paros opens allocates its first segment whole when it is created: 64 MiB and 76 KiB in the one
geometry of #600 (drop the small layout), which stays as it is. A journal's acceptor, a replica
and a matchmaker set each open one store per machine, so a machine with a 100 GiB disk holds
about 1,600 stores before one holds a record. The rules:

- **The cost comes from the store's configuration.** `JournalStoreConfig::store_bytes` is the
  first segment (two header blocks, the persist log, the gap and the entry log) and the meta
  file. It counts no spare segment: a new store holds none, and a spare is a dead segment kept
  for the next roll, never an allocation of its own. A unit test pins it against the files a
  created store holds (`paros-storage`, `journal/tests/cost.rs`).
- **A machine reports its disk with its class and locality.** `IdentifyAck` and `RegisterNode`
  carry `disk_bytes` (`PAROS_DISK_BYTES`, else the size of the file system under
  `PAROS_DATA_DIR`) and `store_bytes` (the cost above, from the machine's own store
  configuration). The registry keeps the latest registration's (`RegisteredNode::disk`) and
  checkpoints it. `0` for either is a disk the machine did not report, which holds any number of
  stores: a registration older than #985 still reads. A restart may report another disk.
- **The registry judges a booking at apply**, as it judges the class. A booking opens a store
  when its role is a storage role of a journal or a matchmaker set (`Booking::opens_store`); a
  tenant-level slot opens none. Every store counts against the disk, but only a user journal's
  store must fit (`Registry::disk_bound`): a journal of a hosted tenant other than its control
  journal. Such a booking on a machine whose disk holds no more store (`Registry::store_room`
  is `0`) is refused `DiskFull`. A grant with such a slot books none of its slots. A release
  gives the store back: the count is the live bookings, so nothing else moves.
- **The control plane is never refused for the disk.** A hosted tenant's control journal, the
  cell plan's journals and every matchmaker set count against the room but always land. They
  are few per tenant, and a disk full of user journals must not stop them from growing or
  moving, nor a tenant's birth from landing: in the simulation, a birth the registry refused
  for the disk left the tenant `REGISTERING` forever (PR #990, recipe
  `8423251458913441447 141811:15210365126398617793` on 427aff2). The low-space mark covers
  them.
- **Placement books a different machine** (rule 7 of `paros::model::placement`, `Want::disk`):
  a machine with no room takes no user journal's store. When no machine has room, the create is
  recorded `Unplaceable` (#212 (placement)), and the create is refused with that reason (rule 7); a move waits. The balancer
  never moves an acceptor onto a machine with no room.
- **The low-space mark stays** (#360 (disk full)). It covers the records a store holds past its
  first segment; the new count covers the allocation. The stores `init` creates before any booking, a released replica's store (kept on its disk) and a frozen
  matchmaker set are outside the count, and the low-space mark covers them too.

The simulation draws the disk per seed (`paros_sim::shape::layout`, a `buggify_knob!`): no
reported size by default; at the extreme every machine reports a disk of `CONTROL_STORES` stores
(the cell's control journals, its sets, and one control journal and one set per
tenant the workload may create) plus zero to three, so the user journals fill it. Its gate:
placement passes over a machine with a full disk.
