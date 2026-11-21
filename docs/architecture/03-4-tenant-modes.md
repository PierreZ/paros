[← 03-3-coordinators-and-placement.md](03-3-coordinators-and-placement.md) · [Index](README.md) · [03-5-frontend-and-resolver.md →](03-5-frontend-and-resolver.md)

### 3.4 Journal configuration

**Rule 7: the person types, the cell places.** A person types what a journal must survive and
its counts. The cell chooses the machines, refuses what it cannot place, and keeps the journal at
what was typed (decisions 15, 18, 21, 22 and 23; steps 5.7, 6.5, 6.6 and 6.8). This replaces the
tenant modes, the tenant default and its "follow" code, `--layout` and the silent cut of a count.

**One `configure` command sets one journal**, in the shape of FoundationDB's `configure`
(decision 15, question 3). Its words are counts:

| Word | Means | At create |
|---|---|---|
| `survive` | how many machines the journal survives (F); each acceptor partition has `2F+1` acceptors | required, no default |
| `zones` | how many zones the journal survives | off unless typed |
| `regions` | how many regions the journal survives | off unless typed |
| `partitions` | how many acceptor partitions the writes spread over | 1 |
| `proxies` | proxy leaders, on stateless machines | 1 |
| `batchers` | batchers, on stateless machines (multi-writer journals only) | 1 |
| `replicas` | replicas, on storage machines apart from the acceptors | 1 |

Example: `parosctl configure acme/billing/invoices survive=1 zones=1`.

- **A create always names its survive number** (decision 18). The survive word is required and
  has no default, because paros never guesses a number for the person (Pierre: "never try to
  guess").
- **A journal survives no more than its tenant** (decision 35). A tenant has its own typed
  survive number, which sizes its matchmaker set and its tenant control journal (section 3.3).
  The cell refuses a create or a `configure` of a journal with a survive number above that of
  its tenant, with a typed reason, so a person raises the tenant first.
- **The other words have a default** (decision 22): 1 partition, and 1 proxy, 1 batcher and 1
  replica, each apart from the acceptors, on a stateless machine. The cell refuses the create
  when it has no stateless machine. A machine has the storage, stateless and frontend roles by
  default (section 3.2), so a cell of one machine can still place such a journal (decision 5).
- **Zones and regions are words of their own** (decision 21). A missing `zones` or `regions`
  word means that the person asked for no zone or region promise, and the cell never promises
  one by itself. This joins the one number of decision 15 with the decision of 2026-10-28 in
  #748 (journal survive), which had the three parts in one word: zone survival is still never
  lowered without a word.
- **The cell refuses what it cannot place** (decision 15, question 1). A create or a `configure`
  that the placement predicate (section 3.3) cannot place is refused with a typed reason that
  names the shortfall and its cure, for example `proxy-leader short by 1, stateless, any zone`.
  Nothing is placed in part and no count is cut in silence (#591 (refuse a create below
  2F+1)). The table "required against available" of `configure --plan` (#749) is that reason,
  shown before the change.
- **No journal changes by itself** (decision 18). Only a `configure` changes the numbers of a
  journal. No journal follows the cell, and none grows when the cell grows. The tenant default
  and its follow code are deleted (step 5.7).
- **Who may send a `configure`** (decision 23): a person with admin rights on the tenant, for the
  journals of that tenant, and the operator of the cell, for any journal of the cell. A control
  plane never sends a `configure` by itself in this rework.
- **Later, outside this rework:** a tenant can turn on a special mode in which its tenant control
  plane takes care of its journals. It sends the same `configure` requests by itself, as records
  with an idempotency id, so each change stays visible. One command, two possible callers, and
  never a hidden rule in the cell.
- **A journal with `survive=0`** is allowed when a person types it (decision 5). The status says
  that it does not heal a lost disk.
- **The status lists each role of a journal with its machine** (section 3.6).
- **One writer or many stays a word of the create** (`--writers single|multi`). It never changes.

**The cell's own control journals** (decision 5; `init --survive` and `cell configure
--control-plane` are not changed by decision 15). An `admin` call sets how the cell chooses the
survive number of its own control journals:

- `parosctl cell configure --control-plane auto [--survive T]`: the cell raises the survive
  number of its own control journals as it grows, up to `T`, when it has `2F+1` storage machines
  for the next step. This is the one case where a number grows without a `configure` per
  journal, and it covers the cell's own control journals only: no journal of a tenant follows it
  (decision 18).
- `parosctl cell configure --control-plane manual --survive F`: the number stays as it is.
- In both modes the cell never lowers the number by itself (decision 17): only an operator
  lowers it, with a resize record (rule 2, section 3.3).

**Acceptor partitions are a grid** (decision 15, question 2 (a); decision 27). The service keeps
two acceptor layouts, and the person chooses for each journal with `partitions`:

- **`partitions=1`**, the majority: `2F+1` acceptors, any F of them may fail.
- **`partitions=P` above 1**, the partitioned grid: `QuorumSystem::Grid { rows: 2F+1, cols: P,
  acks: F+1 }`. A write goes to one acceptor partition (a column) and is done at F+1 of it; an
  election or a tail read needs F+1 of every column. Acks from two acceptor partitions never add
  up. Elections and tail reads stop when any acceptor partition loses its majority, so their
  availability is lower than one big majority. The plan states this cost in one line. Every
  step still waits for a quorum only: with F down in every partition, writes, elections, reads
  and the segment steps complete (section 2.11, #1257 (quorum everywhere)).
- **A change of `partitions` is a full reconfiguration**; a change of another count changes no
  quorum.
- **A stuck round moves** (#533): a Phase-2 round that stays open for
  `DriverTunables::partition_move_resends` re-sends moves to the next acceptor partition, with
  the same slot, ballot and value. Inside one ballot a slot has one value, and every Phase-1
  quorum meets every acceptor partition, so a later election finds the value or proves it was not
  chosen.
- **The quorum systems are the majority and the grid.** Flexible quorums are deleted from the
  service and from `paros-consensus` (decision 27, steps 0.11 and 0.12): the variant
  `QuorumSystem::Flexible`, `QUORUM_SYSTEM_FLEXIBLE` and `reconfigure --quorum flexible`. The
  grid keeps its two quorum shapes (Phase 2 in one column, Phase 1 in every column).
- **The grid in the models** (decision 31, step 1.13, done). A grid quorum is not a majority,
  so `config-register` runs its safety also with a full grid, and the reads model takes the grid
  quorums as a parameter. A full grid keeps its change of members, and its Phase 1 reaches each
  row of the old grid (section 2.5).
- **Derived numbers are shown, never typed, and the partition count is always shown**:
  `survive 1 machine · 1 partition (any 1) · 3 acceptors`;
  `survive 1 machine · 3 partitions (any 1, up to 3 spread) · 9 acceptors`.

**Stateless counts leave the Paxos configuration** (rule 7, step 6.5). `proxies` and `replicas`
are not part of `AcceptorConfig`: the core binds to the ballot only what Phase 1 and Phase 2
need. Proxy leaders and replicas are routed from the live registry: rank `r` of a journal's
proxy leaders is the machine of its `r`-th proxy leader booking, and the leader sends to it
through that machine's peer lane, or runs the round itself when it holds no lane for that rank.
So a change of `proxies` or `replicas` changes bookings and routing, not the configuration
bound to the ballot.

**How roles are sized and placed** (decided on 2026-10-04; per journal since 2026-10-28, #753).
Storage roles (acceptors, replicas) are **per journal**, because they hold that journal's data;
proxy leaders and batchers are per-journal counts too; matchmakers are one set per journal
(2026-11-19, below).
Processes stay shared and keyed by `JournalIdentifier`; only the bookings are per journal.
Every role's instances go to the eligible machines of its class zone by zone in round robin
(shuffle sharding inside the cell), and one journal's storage instances never share a machine.
A stateless booking reserves almost no capacity, so idle journals do not fill the cell.

- **Proxy leaders on machines** (#765). A machine that holds a proxy leader slot of a journal it
  does not serve steps that journal's delegated `Accept`, `Accepted` and `Nack` into a proxy
  leader, born at the first delegation and stopped when the machine loses the slot.
- **Replicas on machines** (#765). A `storage` machine that holds a replica slot runs the
  journal's replica in its node loop. A replica holds no promise, so a lost store is a first
  boot that learns again. `Resolve` names the replicas, so a client can send its reads there.

**The tenant** keeps `survives: az | region` as a constraint on the kind of cell it may live in
(decided on 2026-10-07, #252, section 3.7). A journal that asks for more than its cell kind gives
is refused, with the reason.

**The leader follows its writer** (decided on 2026-10-07, #215), the main latency lever inside a
cell: with a journal's leader in its writer's zone a write costs one cross-zone round trip,
elsewhere two. The `Resolve` answer tags each reference with its zone (section 3.5), the client
prefers a frontend in its own zone, and the frontend stamps the origin zone on every write it
forwards. The leader's driver in `paros`, never the core, counts origins per zone over a window
and hands off through `relinquish_to` when another zone dominates past a hysteresis threshold
(WPaxos §5.1's majority-zone policy). Not built yet (#581).

**Every journal has one matchmaker set** (decided by Pierre on 2026-11-19, in the journal
design; it replaces the set per tenant of 2026-10-09): `2F + 1` matchmakers that hold the
journal's register (section 2.16). A set is never shared across tenants. A set is logical:
matchmaker *processes* are shared, each hosting many journals' sets keyed by
`JournalIdentifier`. **One store per matchmaker set** (decided on 2026-10-09, #190): the register
of the set's journal is inside it. Every journal, the control journals included, has its set
from its first entry; there is no bootstrap exception. The cell places the set and moves it by a
generation handover onto a target that the caller names (rule 2, section 4.1). A matchmaker
request names the matchmaker it addresses, and a process that is another one drops it.
Reconfiguration is the operational primitive for everything, so no journal of the service opts
out of it.
