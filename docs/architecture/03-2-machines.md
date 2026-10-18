[← 03-1-cell-tenant-and-bootstrap.md](03-1-cell-tenant-and-bootstrap.md) · [Index](README.md) · [03-3-coordinators-and-placement.md →](03-3-coordinators-and-placement.md)

### 3.2 Machines

Every `parosd` is uniform. **Identity** has three parts (decided on 2026-10-02, #225):

- `node_id`: random, minted at format, stored beside the format marker (#147). It is the
  member's identity and the registry key. A wiped disk gets a new `node_id`, so "a wiped
  identity never rejoins" holds by construction.
- `addr`: an attribute that may change across restarts, the machine's advertised address
  (`PAROS_ADVERTISE`, below).
- `incarnation`: moonpool-rpc's per-start `Incarnation`, carried in the `InterfaceRef`.

The same `node_id` with a new incarnation is a reboot: the machine rejoins in place and keeps its
assignments. It is re-placed only if it does not come back within a bound (a cell tunable, drawn
per seed in simulation), so a whole cell
restarting does not trigger a re-placement storm (CockroachDB saw the equivalent: after a mass
restart, lease renewals flooded the cluster until liveness heartbeats timed out). The same
`node_id` with a new `addr` is a machine that moved; ScyllaDB moved from IP-based to host-ID-based
identity for exactly this reason.

At start a machine registers in the cell control journal with
`RegisterNode { node_id, addr, machine_interface, capacity, tags }`, publishing
its machine interface (moonpool-rpc's incarnation-bearing `InterfaceRef`, #404), and it starts the driver
for every `(tenant, journal, role)` assigned to it. Its `tags` are its reserved tags only:
`paros/class`, `paros/zone` and `paros/rack` (section 3.7, #575 (key:value tags)). A
`RegisterNode` for an existing `node_id`
with a new incarnation is the reboot signal: the cell coordinator learns at publication time, not
after a heartbeat window (Akka identifies nodes by `host:port:uid`, and a new incarnation joining
at the same address removes the old member). Peers holding the old reference learn on their next
request, refused with `StaleIncarnation`. That pull model is sufficient: Delos clients refresh
their cached view only when an append fails on a sealed loglet. No push detection is asked of
moonpool.

`StaleIncarnation` needs dynamic references (amended on 2026-10-10, #216). moonpool-rpc refuses a
stale reference only on a dynamic endpoint. A well-known endpoint answers every incarnation. A
restart at the same address rejoins in place, and that is the decided behaviour. A machine that
moved is found through the registry fold (#349, #211). #216 does not add an application-level
copy of the check.

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
| `Retire` | operator call by address |
| `Resolve` | the client entry (FDB `OPENDATABASE`) |

| Dynamic group | One per | Published in |
|---|---|---|
| `MachineInterface` (`0x5041_1001`): `Deliver`; the matchmaker calls `Matchmake`, `GarbageCollect`, `MatchmakerReconfigure`; the data plane `Write`, `Read`, `Truncate`, `SetLeader`, `Reconfigure`, `ReconfigureMatchmakers` | incarnation of a founding member | `IdentifyAck.machine_interface` and the node-only `InspectReply.machine_interface`, kept by `RegisterNode` in the registry fold by `node_id`; a `Resolve` answer gives each machine's (`Member.machine_interface`) |
| `CoordinatorInterface` (`0x5041_1002`): `Register`, `JournalRequest` | coordinator term | the election record's `reference`, beside its `interface` address |

Rules:

- **An address says where, a reference says which instance.** A machine keeps its advertised
  `Address` (#257) as the source of truth for where it is. The reference keeps its incarnation
  and token. A caller resolves the address and aims the reference at it (`aimed`), because the
  address inside a reference is the bound one, which may be a wildcard.
- **`Deliver` stays one endpoint per machine**, not one per journal: the `(TenantId,
  JournalId)` demux does not change. A founding member serves it in its machine group and at
  its well-known id, and both feed one lane. Acceptor and matchmaker configurations stay keyed
  by `node_id`.
- **A lane dials the reference the registry fold holds** for its peer (the peer book), and the
  well-known `Deliver` while the fold holds none: a static deployment (plain Multi-Paxos without
  a registry is permanent), or a member not registered yet. A delivery the reference refuses for good marks it stale, and the lane
  dials the well-known `Deliver` until the fold names another reference. Amended on 2026-10-11:
  the issue's "never fall back while the fold names a reference" is dropped, because a
  restarted founding member's new reference is written into a registry that may need that
  member's own vote (a cell of two), so a lane without the fallback could never heal.
- **Every method of the machine group keeps its well-known endpoint** (landed on 2026-10-11,
  #404 steps 4 and 5). Plain Multi-Paxos, a matchmaker process, a proxy and a replica have no
  registry, so their peers and clients reach them by address alone; a founding member serves
  both, and both feed one queue (`Inbound::in_group`). The reference is the fence for every
  caller that holds one: a peer lane, a matchmaker link, a frontend, a client of a `Resolve`
  answer.
- **A matchmaker link dials as a lane does.** A founding member hosts its tenant's matchmaker
  sets (#190), so a node's link to that matchmaker follows the peer book: the member's
  reference when the fold holds one, else the well-known endpoint, and the well-known endpoint
  after the reference went stale until the fold names another.
- **The data plane goes through the reference its caller learned.** A frontend learns each
  founding member's reference from its node-only `Inspect` (`majority_cell`) and sends the four
  calls through it (`NodeClient::with_interface`). A server that restarted since refuses the
  call unrun: the frontend moves to the next server and forgets the cell, so its next call
  learns every reference again (a BUGGIFY decision keeps the stale cell one more call). A client
  of a `Resolve` answer sends through the references the registry holds (`Resolved::servers`).
  A `NodeClient` with a reference never falls back to the well-known endpoint on its own: its
  caller learns the new reference. The control plane's own clients (the follower, the
  coordinator, the resolver, `init`) dial by address: they bootstrap from the plan, before a
  registry exists.
- **Every start registers once.** A restarted founding member serves a new machine interface.
  Its `Register` carries it, and the coordinator writes `RegisterNode` when the address, the name
  or the machine interface changed; the watch writes it for a new incarnation too.
- **A coordinator registers its group when it serves a term** and publishes its reference with
  the address. It drops the group when the term ends (resign, deposed, fenced, hand-off), so a
  request to an old term fails at the transport. `Register` and `JournalRequest` have no
  well-known endpoint. A group may outlive its term by one step (a BUGGIFY decision): a request
  that reaches it is refused `not_coordinator` by the application.

**Listen and advertised addresses** (decided on 2026-10-07, #257). A machine's configuration
carries two addresses, as FDB's `listen_address` / `public_address` and CockroachDB's
`--listen-addr` / `--advertise-addr` do: `PAROS_LISTEN`, what it binds (may be a wildcard), and
`PAROS_ADVERTISE`, what peers and clients dial (may be a hostname). `PAROS_ADVERTISE` defaults to
`PAROS_LISTEN` when that is not a wildcard, and start is refused when `PAROS_LISTEN` is a
wildcard and `PAROS_ADVERTISE` is unset. A machine always binds `PAROS_LISTEN`, never an address
read from the cell plan or the registry; `RegisterNode`'s `addr` is the advertised address, and
peer address books are folds of the registry (#216), never of the plan `init` wrote. An
advertised name is kept as a name and resolved at dial time, so a machine whose IP changes heals
with no registry write (amends #209's "names resolved once at startup" for peer addresses; the
listen side still resolves once). A changed `PAROS_ADVERTISE` across a restart is the
machine-moved case above: same `node_id`, new `addr`.

Landed on 2026-10-10 (#257). `paros::Address` is the advertised `HOST:PORT`, kept as written.
`paros::Names` is the one resolver all dialers of a machine share. A peer lane resolves its
peer's name before each batch and forgets the result after a failed delivery (FDB's
`removeCachedDNS`). The client and `parosctl` resolve per call. `MachineAddresses` refuses a
wildcard listen address with no advertised one, and a wildcard advertised one. A machine takes
part in `cell init` only when the member list names its own advertised address, so
`parosctl init --members` lists the advertised addresses. The simulation advertises a name on
half the seeds, and a rebooted machine on such a seed comes back at a new IP behind its name
(`move_pct`).

Landed on 2026-10-10 (#349, registry address books). A changed `PAROS_ADVERTISE` across a
restart is supported:

- **The registry holds every machine's address.** A founding member registers too: its
  `RegisterNode` replaces the address the cell plan names. It stays in the genesis pool and is
  never drained. `paros::machine::address_book` is the founding members at their registered
  addresses, else at the plan's; `cell_book` adds every registered machine not retired.
- **The request path.** On every start, a formed or admitted machine reads the election
  journal, finds the coordinator's published interface, and sends it `Register` (machine
  method `0x5041_0307`, in the coordinator's term group since #404) with its identity, the
  address it advertises now and its machine interface. The coordinator calls `Identify` at
  that address. When the machine answers there as the same incarnation, and the registry holds
  another address, name or machine interface, the coordinator writes `RegisterNode`. A term
  that ended refuses at the transport, or `not_coordinator` while its group lingers; the
  machine asks again after one renewal period.
- **The peer book.** A founding member folds its own copy of the cell control journal after
  each tick. When the address book moves a peer, that peer's lane dials the new address from
  its next batch on. The coordinator's watch also dials each machine where the book says.
- **The limit.** Until the registry holds the new address, no peer can send to the moved
  machine. So the registry write needs a majority of the other members. A cell of two cannot
  heal a moved member, and neither can a cell that lost another member. A machine whose cached
  book names no reachable machine cannot find its coordinator: the same static-stability limit
  as a machine whose cached members are all gone (below). The coordinator still reaches an
  admitted machine that did not move, and hands it the cell's book again (#505, below). The durable cached registry fold
  (below) lets a machine that restarts after others moved dial them where they are.
- **The simulation.** On a seed whose machines advertise names, a cell machine's reboot comes
  back under a new name at a new IP (`rename_pct`). Its old name keeps the old IP, where
  nobody listens. The operators' entry for the machine follows it. A founding member is renamed
  only once every renamed admitted machine is registered at its new name. Otherwise the run
  passes the limit above: the admitted machine's book names only the founder's old name, and
  the registry names the machine only at its old address (#501 (matchmaker sets off the
  founders)). After chaos, the oracle judges that the registry holds every renamed founding
  member and every renamed admitted machine at its new name.

**Liveness** (decided on 2026-10-04). The cell coordinator watches the cell's machines with the
transport's failure detector and writes only the *changes* into the cell control journal (`Down`,
`Up`), so control writes stay rare (section 3.9) and the registry's "seen alive" is a fold, not a
heartbeat log. A machine `Down` past the re-placement bound has its roles re-placed. A machine
never heartbeats into a journal.

Landed in #211: the cell coordinator's `Watch` (`paros::machine::coordinator`) sends `Identify` to
every founding member and every registered machine at each renewal period. The detector's timeout
is `machine_down_after` (a driver tunable, floor above `election_renew`): a machine silent that
long is written `MachineDown`, one held down or seen as another incarnation `MachineUp`, and a new
incarnation of a registered machine registers again. The registry refuses a liveness entry that
changes nothing (`LivenessUnchanged`), and the simulation asserts that none is ever written. The
watch writes only while its term's session holds the cell control journal; its first write that
does not land ends the watch for the term. `RegisterNode` and `IdentifyAck` carry the machine's RPC
incarnation and, since #404, its machine interface. Capacity bookings are keyed `(node, journal or matchmaker set, role)`,
the role names the class, and a booking id is never booked twice, across checkpoints (the
registry keeps the spent ids).

Landed in #475 (re-place after the Down bound, 2026-10-15): the re-placement bound is
`machine_replace_after` (a driver tunable, floor above `machine_down_after`; 300 s in the shipped
profile). The watch names a registered machine *gone* when the registry holds it down and the
watch did not hear it for that long. A machine that answers again inside the bound keeps its
roles: a reboot rejoins in place. The tenant coordinator moves the journals and matchmaker sets
off a gone machine as it moves them off a draining one, and a gone machine takes no new slot. A
gone machine that answers again before its move ends keeps the slot the move did not release
yet; at an idle round the coordinator releases each slot outside the configuration the
journal's leader holds in force. The bound counts on the coordinator's own clock, so a new term
counts again from its start.

**Clean stop** (#560 (clean Ctrl-C shutdown), landed on 2026-10-18). `SIGINT` (Ctrl-C) and
`SIGTERM` give the same clean stop. `parosd` only turns the signal into the machine's shutdown
token; the stop is the library's (`driver/stop.rs` in the node loop, `machine/coordinator.rs`),
so the simulation runs the same code. moonpool's graceful reboot cancels the same token, then
kills the process after a grace period. The node loop serves on while it stops:

1. **Stop accepting.** A `Write`, `SetLeader`, `Truncate` or `Read` that arrives is refused at
   once, `stopping: true` in its ack, with no verdict: retry elsewhere. The client library reads
   it as a redirect with no hint. Peer traffic goes on, because the handoffs need it. Operator
   calls (`Reconfigure`, `Retire`, `Inspect`) are still served.
2. **Finish the work in flight.** Every held call is answered from the slot it waits on, and
   every read waits out its own bound.
3. **Hand the leaderships on**, at the same time as step 2. Each journal the node leads hands its
   Phase-2 authority to a member drawn at random (`relinquish_to`, the cooperative handoff). The
   successor goes on under the same ballot, with no election, and answers the held calls too. The
   stop waits for the successor's first beat at that ballot (or any beat above it). A journal with
   no other member keeps its leadership. Some leaderships cannot hand off: they are not settled
   yet, or they were handed to this node. The stop waits for them for one election timeout, then
   leaves them to an election. The cell coordinator (also the tenant coordinator, until #476)
   resigns its term naming the next founding member and moves the cell control journal's leader
   uuid to it, as the hand-off BUGGIFY decision does. A lone founding member keeps its term.
4. **Flush.** Every batch whose flush flies lands.
5. **Exit.** The loop returns, `parosd` logs `parosd_stopped` and exits 0.

The bound is the `shutdown_within` tunable (`PAROS_SHUTDOWN_WITHIN_MS`, 30 s in the shipped
profile, floor 0): when it runs out, the loop returns anyway and logs `stop_timed_out` with the
step that did not complete. A second signal exits at once with code 130, and so does a stop that
is still running 5 s past its bound. Each of these is a crash, which recovery already handles, so
no bound is a safety rule. Each step logs with the machine's name (`stop_started`,
`stop_handoff_sent`, `stop_handoff_taken`, `stop_completed`). `paros-frontend` stops the same way
with no leadership and no store: it refuses new calls (`stopping`), waits for the ones it took
and returns. In the simulation, a stopping frontend sometimes waits out its whole bound,
as a frontend behind a slow load balancer does. It refuses the calls that still arrive.

The simulation draws a graceful reboot for about a third of the machine and frontend groups'
attrition on the seeds whose swarm keeps it, with a 1–3 s grace around the drawn `shutdown_within` (0–4 s for a machine, 0–2 s for a frontend), so
a stop ends on its own on some seeds and is cut by the kill on others. Gates: a stopping machine
refuses a call, a stopping leader hands its journal on, a successor takes it, a coordinator hands
its term on, a stop completes, a stop runs out of time. The existing oracles judge the rest:
linearizability over every call, one leader per term, and the crash oracles for a cut stop.

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
(`Registry::keeps_roles`): the tenant coordinator moves each journal off it (book elsewhere,
reconfigure, record the configuration in force as `Placed`, then release), placement books it last,
as a down machine, and the mode changes leave its journals to the moves. An open maintenance
window (#359) suspends this, as it suspends a down machine's re-placement: the mark stays
written, but no slot moves off a machine the window covers. A grid leader sends a
round's Phase 2 to a column with no slow acceptor when its own column holds one, and a quorum read
asks such a row (`fast_column`, `fast_row` in `driver/mod.rs`); a majority already decides on the
fastest replies. `parosctl` shows the machine `slow` (`MachineView.slow`). The simulation's
slow-machine scenario holds a founding member slow for 10–16 s on half its strikes in the chaos
window; its oracles: a slow mark never takes a quorum, and after chaos no journal with a machine
to go to keeps a slot on a slow machine.

Landed in #211 (2026-10-10): the **durable cached registry fold**
(`paros::machine::CachedRegistry`). Every machine of a cell keeps the cell's address book
(`cell_book`) at one registry position in a file `registry` beside its record, rewritten whole
and atomically, as the record is:

- **Who writes it.** A founding member offers each book its own registry fold reaches
  (`driver/book.rs`). An admitted machine serves no journal, so it folds the registry through
  the machines it knows, each renewal period (`machine/follow.rs`). It learns the genesis pool
  from the control journal's membership. A writer task beside the machine writes a book only
  at a later position and only when the book changed. The node loop never waits on the disk.
- **Who reads it.** At every start, a formed or admitted machine dials the machines of its
  cell where the cache says, else where its plan or admission says (`starting_book`): its peer
  lanes, its control-journal seeds, its coordinator client and its registration (#349). A new
  incarnation folds the registry again from position 0. Its book moves no lane until the fold
  passes the cache's position: below it, the cache is the newer book.
- **A hint, never a fact.** The cache names its machine (`node`), and a cache another
  identity left, or one that does not parse, is ignored. A wrong address costs liveness, never
  safety: every peer batch names its cell (#216).
- **The simulation.** A durable cache only moves forward, and a boot reads only a cache its
  machine wrote. The gate is a boot that dials a moved machine from its cache, the
  static-stability case.

The same change fixed a restore bug from #349: a founding member that registered its address
broke every restore of a registry checkpoint, because the restore refused a genesis node in the
state.

It also fixed #390 (renamed founder never registers). A founding member that comes back at a
new address hears none of its peers: they send to the address the registry knows. Its election
clocks still fired, and each campaign raised its peers' promises above the sitting leader's. The
cell control journal then stalled, so the registration that would move the peers' lanes never
landed. Now such a member holds its journals' clocks until its registration ends
(`driver/mod.rs`). It still answers every message that reaches it. A lone founding member is its
own quorum, so it never holds.

Landed in #505 (stranded admitted machine, 2026-10-15). An admitted machine reads the registry
through the founding members its book names. When every one of them moved or was wiped, its
fold stops, and nothing booked on it can start. The coordinator heals it in its incarnation:

- **The machine reports its fold.** An admitted machine's `IdentifyAck` carries
  `registry_position`, the position of its newest book. Other machines report 0.
- **The watch sees a stuck fold.** At each round, the coordinator compares the position an
  admitted machine that serves the cell reports with the position at its last answer. When the
  position did not move and is below the coordinator's own fold, the coordinator sends `Admit`
  again (`CellSession::readmit`). That `Admit` carries the cell's machines it knows now (the
  founding members at their registered addresses) and its fold's position
  (`Admit.registry_position`).
- **The machine adopts a newer book.** An `Admit` into its own cell, taken past the machine's
  newest fold and with other machines, is offered to its cache. The follow dials its next read
  from that book. The node loop moves each control-journal seed and its lane to the address the
  book names (`ControlFollower::adopt`). A registration the fold meets below that position
  moves no seed. A registration at or above it moves the seed too (`ControlFollower::reseed`),
  so a founding member that moves later is followed in the same incarnation.
- **The limit.** A machine that moved itself while its book names no reachable machine cannot
  register its new address, so the coordinator cannot reach it either. An admin admits it
  again.
- **The simulation.** The matchmaker sets' oracle no longer exempts a machine whose founding
  members all moved (PR #503's `stranded_ids` is gone). The gates are a coordinator that hands
  a stuck machine the book again and a machine that adopts it.

#506 (renamed founder not registered) added the #390 hold to a moved founding member's candidacy for the cell
coordinator (`machine/coordinator.rs`). Unregistered, such a member read the election through its
own node, whose reads never confirm. A term it won kept a stale view for many seconds, and each
of its claims fenced the terms its peers won. No term then answered `Register` for about 25 s
(witness 12633892831069003880 on a rename-heavy hunt). Now it campaigns only once its
registration ends.

**Finding the cell** (amended on 2026-10-09). There is no cluster file and no rendezvous name.
A machine's configuration names no cell and no peer: it learns its cell when it is admitted
(`FormCell` or `Admit`, section 3.1) and caches it durably. On every later start it finds the cell
from its **cached registry fold**. If every cached member is gone, the machine cannot find its
cell. The coordinator hands its book again to an admitted machine it still reaches (#505).
Otherwise an admin admits it again (YugabyteDB makes the same trade: a tserver none of whose
configured masters remain cannot rejoin).

A client's only static input is an **entry endpoint**: a few addresses, or a DNS name the
operator puts in front of them, never stored by paros (as Kafka's `bootstrap.servers`). From
there it calls `Resolve` (below).

- The durable cached registry fold plays the role CockroachDB gives gossip (node addresses off the
  consensus path): it lets machines find each other while the registry is unavailable. It is a
  static-stability requirement, not an optimisation.
- **No journal is found by convention** (decided on 2026-10-04, section 3.8): there is no
  well-known tenant or journal id. Every machine of a formed cell answers `Inspect` (and
  `Resolve`) with its `cell_id`, the cell tenant's control `JournalIdentifier` and, once the cell is
  admitted, the universe tenant's `JournalIdentifier`, all read from its durable cell plan, plus the
  cell that hosts the universe tenant now, read from its universe pointer (section 3.7). A client or an operator handed
  only addresses learns the control `JournalIdentifier`s from any machine, then resolves everything else
  through them: the universe directory gives a tenant's cell and the `JournalIdentifier` of its control journal, and
  that control journal gives the tenant's journals. A re-run of an admin call learns the cell's
  ids the same way. **An `Inspect` names its journal or asks for the node alone** (decided on 2026-10-05,
  #243): a node-only `Inspect` answers the machine's own facts — its `node_id`, its `cell_id` and
  the control `JournalIdentifier`s — and nothing about any journal, which is how a client handed
  only addresses starts; an `Inspect` that names no journal without asking for the node alone is
  refused (`unset`), never read as "the node's first journal", and one naming a journal the
  machine does not serve is refused (`unknown_journal`). The machine's own facts ride every
  answer, refusals included.
- `cell_id` is carried in the session `Hello`; a peer with another id is refused.
  ScyllaDB carries its cluster id in gossip for the same reason: nodes from different clusters
  cannot talk after a bad configuration. The `Hello` is the peer lane's `Deliver` batch (decided
  on 2026-10-10, #216): every batch carries the sender's `cell_id`, and a receiver of another
  cell refuses the whole batch before it decodes a message. The simulation makes the shape: an operator founds another cell on the
  machine that replaced a wiped member, and the first cell still sends to that address.
  The matchmaker requests carry the same `cell_id` (amended on 2026-10-15, #539 (matchmaker
  cell fence)): `Matchmake`, `GarbageCollect` and `Reconfigure` go outside the peer lane. A
  matchmaker host refuses a request of another cell before it reads the tenant. Without this
  fence, a machine at an address another cell left provisioned a second set of generation 0
  for that cell's tenant, and two sets were authoritative for one generation.
  Discovery has the same hazard, so a re-run `init` learns the cell a majority of the founding
  members serve, and `cell init` never adopts a vote for another list's plan.
  **`universe_id` is not on the peer batch** (amended on 2026-10-10, #216). Peer traffic stays
  inside one cell, and a cell joins one universe once (`JoinFleet`), so two machines with the
  same `cell_id` always share a universe: a check on the batch could never fire. The
  `universe_id` rides the `Resolve` answer, and it joins the peer batch with the first call
  that crosses cells (M12). It is minted at `universe init`, after the cell plan, so the cell
  holds it in its registry fold (`JoinFleet`), not in the plan.
- **Well-known endpoints** are the bootstrap set — `Identify`, `PrepareCell`, `FormCell`,
  `CellInit`, `Admit`, `Inspect`, `Retire` — and **`Resolve`**, keyed by tenant: "which
  references serve tenant T" (named on 2026-10-09). Everything else is a dynamic reference
  (amended on 2026-10-04: the bootstrap calls were well known already). The tables and rules
  are in "Static and dynamic endpoints" above (#404). One call, two answerers (decided on 2026-10-07,
  #233): a resolver answers the cell half (the tenant's cell and that cell's entry references),
  any machine of that cell the frontend half (the tenant's frontends, from its registry fold),
  section 3.5. `Resolve` landed on 2026-10-10 (#216, `0x5041_030A`), section 3.5.

The decision and its alternatives are #216. Classes are FDB's:

- `storage`: anything with a durable store. Acceptors, replicas, matchmakers.
- `stateless`: frontends, resolvers, proxy leaders, batchers, unbatchers, coordinators. The cell
  and universe coordinators run on the founding members until a `stateless` machine registers
  (section 3.3).

**Capacity is role slots** (decided on 2026-10-04). A machine offers `capacity` opaque slots of
its class; one slot holds one role instance (an acceptor, replica, matchmaker, coordinator,
frontend, batcher or unbatcher) of one journal or tenant. A booking is keyed
`(tenant, journal or matchmaker set, role)` and holds one slot; only the cell coordinator writes
bookings. Bytes, IOPS and weighted roles are not modelled. Scaling a role for a tenant is adding
machines of the right class and raising the tenant's desired counts.
