[← 04-roles.md](04-roles.md) · [Index](README.md) · [06-verification.md →](06-verification.md)

## 5. Failure model and zones

The service survives disk loss and machine loss in one region: a corrupted record is CTRL's
`faulty` and repaired by the protocol; a wiped disk is refused at boot as amnesia, and the machine
mints a new id and is admitted again by credential (decision 4); a crashed machine restarts as an
existing member; a missing machine is unreachable, incarnation gone or machine gone (rule 5,
section 3.2), and the routine of rule 4 replaces it one for one in each set (section 3.3). **The
cell drives every heal**, from what a person typed, with the recovery pass of each term; no
harness and no operator composes a reconfiguration for it (rule 8, section 6). Losing a control
quorum will be recoverable at both the cell and the universe level, without touching user data
(section 3.10, deferred). Beyond one region, a multi-region cell survives the loss of a region
(M12, below).

**Offline inspection** (#365, M9). A refused boot (exit 78) tells the operator not to restart
blindly; `parosctl store inspect <data-dir>` shows why, with `parosd` stopped: the machine
record, and each store's format marker, promise, chosen index, floor, sealed state, faulty
slots and, with `--records`, its accepted slots. It never writes the disk: opening a
moonpool-journal store recovers it, and recovery writes, so `paros::inspect` copies each store
to a scratch directory and opens the copy with the shipped boot scan. Names come from the
cell's journals the disk holds, folded over their accepted prefix: a label, never evidence of
what the cell chose.

**Background scrubbing** (#362 (background scrubbing), M12, decided on 2026-10-15, built on
2026-10-16). A record that rots on a disk at rest stays hidden until something reads it: the node
holds every accepted record in memory after its boot scan, and a read is served from memory. So
the tenant level of the control plane reads every record of every live journal of its tenant in the background,
at a low rate, and repairs the rot before a reader needs the record. Eat your own food: the
scrubber is one duty of the tenant control plane (section 3.3), never a task of the
driver.

- **Each copy, never a quorum read.** A quorum read returns one good copy and hides the rot on
  the others. The scrubber asks each machine that holds an acceptor slot of the journal for the
  same slot range (`Scrub`, an internal call). The acceptor reads the range back from its disk
  (`LogStorage::scrub`, moonpool-journal's `replay`), checks each record's checksum locally, and
  answers each record's ballot and value fingerprint, so the values never cross the network.
- **Repair is CTRL's.** A record that fails its check is CTRL's `faulty`. The node still holds
  the vote it cast, so the acceptor writes the same record again from its own copy
  (`ColocatedNode::repair_record`), and the node loop's settle tail makes it durable before the
  answer leaves. The promise, the vote watermark and the chosen prefix do not move. A node that
  reboots onto the damage before a scrub reaches it reports the slot faulty at boot, and the
  CTRL repair path fixes it from its peers.
- **A mismatch is a safety fact.** Paxos accepts one value per slot per ballot. Two acceptors
  that hold one slot under one ballot with two value fingerprints are a mismatch: counted,
  logged, recorded, and asserted never to happen in the simulation.
- **Progress is a journal entry.** The scrubber flushes where its pass stands and what it found
  to the tenant's control journal (`TenantCommand::Scrubbed`: the pass, the journal and next
  slot, this pass's counts and the totals), every few pages, at each journal's end and when a
  page finds damage. A coordinator of a new term resumes from it; the fold refuses a record
  behind the one it holds. `parosctl status --tenant` shows it, and each machine counts the
  records it scrubbed, found damaged and repaired (`paros_scrub_*_total`).
- **Pacing.** Three tunables with documented floors: `scrub_rate` (bytes per second per tenant,
  a token bucket; floor 1 KiB/s), `scrub_pass_within` (the full-pass bound: a pass starts no
  sooner than this after the last one this term started, and a pass that outlives it is recorded
  late; floor 1 s) and `scrub_page` (records per request; floor 1). Production reads 4 MiB/s per
  tenant and a full pass each week. A new term with no pass running starts the next one at once:
  its clock knows nothing of the last term's, and the rate bounds the cost.
- **Not yet scrubbed.** The tenant control journals and the cell's own journals (the registry,
  the universe directory) are not scrubbed: a later issue.

The simulation proves it with the cold-rot scenario (`paros_sim::world::cold_rot`): a task
beside a machine rots the entry of one cold record of a created journal on that machine's disk,
at most one machine per journal and four records each, on journals of three members or more
under a majority. Its runs last 120 s, the top of the `run_end` knob, because created journals
come late in a run. In 200 seeds of the sweep's exploring campaign, the scrubber repaired 24
records that no client read. The
aim comes from the shipped store's own layout, read from a scratch copy of the store, never from
a format the harness knows, and the bytes flip through the machine's own disk, as the boot-time
injector's do. The issue asked for cold rot as a moonpool fault first. moonpool already damages
bytes at rest; what this fault needs is the aim (which journal, which machine, which cold
record), and that needs paros's placement and its stores' layout. So the injector lives in
`paros-sim`, beside the harness's other disk-rot injectors. moonpool's random rot families stay
masked (#176).

**The storage contract.** paros's stores are moonpool-journal's (`paros::storage::journal`), and the
simulation must give them at least the chaos the harness's in-memory stores carry today (decided
on 2026-10-04, #176, #202). Every storage fault moonpool can inject is eventually in contract.
Three families start masked through moonpool's `storage_fault_mask` (moonpool#292), each with
an issue to lift the mask: phantom writes (a write acknowledged as synced that never lands loses
an acknowledged vote, which no quorum survives), a hung disk (`DiskFailure`: it needs a storage
watchdog in the driver) and stall/throttle episodes (`Degradation`). Failed writebacks
(`WritebackError`, fsyncgate, moonpool#355) run unmasked (#720 (fsyncgate), decided on
2026-10-26). The kernel marks the pages it could not write clean: a retried sync succeeds with
the bytes not durable, and the page cache keeps serving them, across a process restart too. So
a fail-stop does not help, and a rewrite of the files can tear records that were durable. Every
store open (boot, restart, the re-open after a quarantine) first drops the clean cached pages
of the store's files (`paros::storage::journal::drop_caches`, `StorageFile::drop_cache`,
`posix_fadvise(DONTNEED)`, moonpool#361). The open then reads what the device holds. It does
not sync: a dirty page a failed commit left still reads as written and lands at the next sync
(#348), and a sync at every open could fail and take the open with it. A one-sync commit
(the matchmaker registry; the batched journal store until step 0.10 deletes it) could still
keep a record and lose its entry.
So when its sync fails, moonpool-journal writes the batch again and syncs again, at most three
syncs (moonpool#362). The batch is not acknowledged yet, so that rewrite is safe. A storage fault still quarantines its
journal, not the process. A matchmaker process hosts one registry per tenant set. A registry found torn at
boot is lost for good: the process drops that set alone and serves its other sets, and the set
heals around the lost member by a handover (decided on 2026-11-01, the fix of main's red after
#834 (matchmaker shards)). Barrier violations ("sync
lies") and moonpool's slow-disk extremes run unmasked (decided on 2026-10-06, #176): no moonpool
profile draws barrier violations, and the slow-disk extremes are a performance knob the mask
does not cover; if paros does not stay live on them, that is a finding, not a knob to clamp.
Matchmaker stores take damage too, under a minority or rolling pattern counted as the run's one
matchmaker loss and exclusive with the matchmaker wipe; matchmakers get their failure domains
as a second `.cluster()` group with its own `replicated_storage_faults` call (moonpool#297
gives `.processes()` groups no locality). moonpool now lets a `.cluster()` group draw zero
processes (moonpool#310, pinned since 0a68199), so nothing blocks the move; matchmakers stay a
`.processes()` group until the replicated fault patterns that need them land (#202; an
implementation deferral of 2026-10-08, #176, not a change of direction). moonpool reports no per-process damage (moonpool#295,
closed as not planned), so the ground truth stays the ledgered injector's and the journal's
own verdicts.

**Moonpool owns fault placement** (decided on 2026-10-28, #760; steps 1.9 and 7.1). The code
says how each journal is replicated, and moonpool keeps every destructive fault inside that, as
FoundationDB's `canKillProcesses` and TigerBeetle's fault atlas do. **moonpool decides each
destructive fault with its shard gate, from what the shipped code declares with `shard!`**
(step 1.9, and #1179 (the kill policy as shards), decided by Pierre on 2026-11-17: the quorum
checks of the kill policy move into `shard!` declarations, and the policy is deleted). The
simulator refuses first each fault that the cell does not promise to survive: a wipe, or a crash
held down with no scheduled restart, that would take the quorum of a declared shard becomes a
crash or a restart in place. The shards are the plan of `cell init` (each machine that holds a
vote declares a majority of the plan's members), each journal's configurations in force (its
leader declares them), each tenant's matchmaker set and each handover's successor. A renamed
machine's old name reaches nothing, so the gate counts it as a lost member, as production does.
An outage's planned loss keeps a quorum of clean copies, because a slot left with fewer is
beyond the promise of its set (decided by Pierre on 2026-11-17, #1098 (`can_kill` policy)). The
journal stores publish their regions in the journal's shard, and moonpool places rot with its
atlas (step 7.1). The harness ledger (`world/cut.rs`,
the injector's planning, the budgets, `plan_quorum_holders` and the other fault avoiders) is
deleted once no oracle reads it.

**The journal is CLSTORE, and paros uses it directly** (decided on 2026-10-07, moonpool#308). The
first moonpool-journal was a dense Raft-shaped log; paros journaled operations into it and folded
them at boot, with checkpoint brackets. It is rewritten as a protocol-free CLSTORE journal: an entry
is a `u64` position plus an opaque identity, written in any order (laggy slots), with overwrites,
tombstones and a floor; a persist record far from each entry; two-copy metainfo in its own file,
the copies written one after the other; no snapshots and no compaction. A commit is one sync
(CLSTORE's, the last batch ambiguous) or two (always decided), recorded per batch; paros uses
two, for every store (decision 15, question 4, section 2.9). paros stores the state itself: a slot is a position, its
ballot the entry's identity, the promise, chosen index, floor, sealed state and format marker the
metainfo; a matchmaker registration is a position of its own, its generation in its identity.
A matchmaker's scalars grow with its tenant's journals and its pending bootstraps, and the
metainfo is one block, so they are entries too (#439 (operator calls name their journal)): the
scalars are encoded and cut into chunks of `JournalStoreConfig::meta_chunk` bytes, and the
metainfo is a header that holds the format marker, an epoch and the chunks' positions. Each
write of the scalars takes a fresh epoch and fresh positions in the commit of its header, and
the old chunks go with the clears.
**A commit is atomic one way only**: the journal writes a commit's metainfo only once its batch
is durable (moonpool#309, pinned since 0a68199), so a durable metainfo vouches for its batch,
but a crash between the two lands the batch without its metainfo. paros puts a batch and the
metainfo that depends on it in one commit, and orders the reverse across commits, each durable
before the next (#264, #176): a node commits a raised promise alone (an entry never lands above
its promise), then its entries with its floor and metainfo (a chosen index never lands ahead of
the entry that makes its slot chosen; it rides the writes that happen anyway, and forces a
metainfo write once it runs 64 slots ahead of the durable one, so a reboot recovers about one
page, #602 (dark after a full restart)); a matchmaker commits its new registrations with its
metainfo, then its clears, and a boot keeps only the registrations the durable metainfo vouches
for (at or above its watermark, of its generation). Storage chaos on journal seeds is the
local half of this contract: moonpool's crash damage, failed syncs, short transfers and lost
directory entries, a crash at a driver `hint!`, and a power loss inside a commit (the journal's
own `hint!`s, #294); rot waits for
replicated fault patterns. The simulation was proven to catch journal durability bugs by
mutation (an unsynced commit, a recovery that drops an acknowledged batch, an unsynced promise:
each red).

**Two faults on one journal: five narrow changes** (decided on 2026-11-11 by Pierre, the
strategy "moonpool-journal: verified diagnosis and strategy"; moonpool #410 to #413, merged as
moonpool #421, then paros #991 (metainfo write on idle) and #992 (rolled-back promise, scrub of
records)). Two holes needed two faults on the same journal: two faults on an acknowledged last
batch made recovery discard a vote as a torn tail, and two lost metainfo writes rolled the
promise back with no check. paros keeps the format, the two logs and the API, and does no
rewrite. The journal refuses where it guessed, and it gets a witness for its tail:

- A commit's metainfo names its own batch as the high-water mark. At or below the mark, two
  faults on a slot are `DoubleFault`, never a torn tail (moonpool #411). A batch of a later open
  generation than the metainfo is `MetaRolledBack` (moonpool #412). paros maps both to a
  corruption verdict that quarantines the journal.
- An idle journal writes its metainfo once (#991): a store whose last batch has no witness,
  and that saw no commit for `idle_witness_ticks` beats (production 2 s, a `buggify_knob!` in
  the simulation, floor 1), writes the metainfo alone (`LogStorage::idle_beat`). The cost is
  two durable writes for each idle period of each journal. The busy case was already bounded:
  the chosen index forces a metainfo write each 64 slots.
- An acceptor's boot checks its promise against every ballot it holds, accepted or faulty
  (#992, `paros::storage::boot::check_promise`). A promise below a held ballot is a corruption
  of the promise for that journal, never the process. The core's boot assert stays as the
  second half of the pair. A replica's store is not checked: its learned records carry ballots
  it never promised.
- The scrub reads the persist records again (#992, moonpool-journal's `verify_records`): a
  record that rots is found while its entry stands, and the next append of the slot writes it
  again, as for a damaged entry.

Two cases stay open, and each needs two lost writes that the device reported durable: a
metainfo rollback inside one open with no entry above the old promise, and a device that lies
about a flush. Only a protocol rule could cover the second (an acceptor that does not vote
after an unclean restart until it sees a newer ballot), and it costs availability at every
crash, so the risk is accepted. The performance work (pack the persist records, write the
metainfo less often) needs a format change and a measurement first: a separate decision.

**One store geometry** (decided on 2026-10-16, #600 (drop the small layout), moonpool#335
(512 batches per segment)). Every deployment uses the library's default geometry. There is no
`small` layout and no `PAROS_STORE_LAYOUT`. Every batch starts on a fresh 4 KiB block, so the
geometry is balanced: about 7,900 blocks in the persist log and in the entry log of each
segment. A store made with another geometry does not open: no store made with the old
default is kept (decided on 2026-10-16). Only tests and the browser sandbox use a compact geometry, to cross rollovers fast.

**Recycled segments and durable writes** (#600 (journal commit stalls), moonpool#352 (recycle
segments), moonpool#339; decided on 2026-10-20, it replaces the background spare of moonpool#338).
No journal store zero-fills a segment. A new segment gets its space with `fallocate`. When a
truncate makes a whole segment dead, the store renames it to a spare instead of deleting it. The
next roll adopts the spare: a header write and a rename, on blocks the disk already holds. The
old records in a recycled segment are stale: their batch is below the segment's first batch, so
the scan reads them as a torn tail, never as evidence. An open keeps one whole spare and deletes
the rest. The journal format is version 3; no store of an earlier version is kept. Each step of
an ordered commit is one durable write (`pwritev2(RWF_DSYNC)` on Linux), not a write and an
`fsync`. The simulation runs the same path.

Zones are failure domains, and a cell spans enough of them for its quorums (section 3.7). What
the WPaxos read (section 10) established for one region with several availability zones:

- Surviving one zone loss needs `fz = 1`, and then every Phase 2 spans two zones, exactly like a
  zone-balanced majority, while tolerating fewer node failures than that majority. WPaxos's
  latency win exists only at `fz = 0`, which gives up zone survival. Its per-zone quorum system
  is therefore not adopted.
- paros's grid cannot express zone survival either: rows and columns are positional over sorted
  ids, and with column = zone a zone loss kills every row, with row = zone it kills every column.
  Acceptor partitions stay the opt-in throughput layout (section 3.4, `partitions` above 1,
  #532 (acceptor partitions)), with their cost (an election needs a majority of every acceptor
  partition) stated to the tenant that picks them; one acceptor partition is the
  zone-surviving default.
- What is adopted (decided on 2026-10-07, #215), detailed below: failure domains inside
  `AcceptorConfig`, a placement rule judged through `QuorumSystem` and never a count, the leader
  following its writer through `relinquish_to` (WPaxos's steal without a Phase 1, section 3.4),
  zone-spread matchmaker sets, and in the simulation a zone-kill attrition mode and a zone-aware
  copy budget, without which it cannot prove zone survival (section 6).

**Failure domains in the configuration.** `AcceptorConfig` carries a vector of
`FailureDomain { region, az }` parallel to its sorted members. `az` holds the machine's `paros/zone`
tag from the registry (`RegisterNode`, section 3.2, e.g. `"eu-west-1a"`). `region` is empty today:
`paros/region` is a tag of a cell, not of a machine (section 3.7), and one cell is one region until
#253 (multi-region cells), so `paros_model::placement::domain` leaves it empty. The names are
short bounded strings, compared for equality only, never parsed or ordered by the core. Names mean
the same thing universe-wide, so a move between cells (section 3.7) compares domains correctly; a
cell-scoped pair of integers would let one cell's `az 2` equal another's and the zone rule could
accept a placement that does not survive a zone. The cost is a few dozen bytes per member in a
configuration of at most nine members. The vector is bound to the ballot with the
configuration, because two nodes that disagree on a member's failure domain evaluate different
quorums; the registry's zone tag is the composer's input, copied when it composes, never
read live by a tally. An empty vector is the plain arm, byte-identical to today's configuration,
so plain Multi-Paxos is unchanged. The configuration names no cell (section 3.7, #1264 (remove
the cell from paros-consensus)).

**The zone rule** is two predicates over a configuration's members `M`, built from
`has_phase1_quorum` and `has_phase2_quorum` over subsets of voters, never a count:

1. **It survives any one failure domain**: for every domain `d`, both `has_phase1_quorum` and
   `has_phase2_quorum` hold on `M` minus `d`.
2. **No domain commits alone**: for every domain `d`, `has_phase2_quorum` fails on `M`
   intersected with `d`.

The rule is valid for `Majority` and `Grid` alike (flexible quorums are deleted, decision 27). It is one pure function in
`membership/`, shared by the cell's placement predicate (section 3.3) and the simulation's composer.

**As built** (2026-10-16, #215 (zones)). `paros_consensus::FailureDomain { region, az }` and
`AcceptorConfig::zoned` carry the labels; `AcceptorConfig::zone_rule` is the rule, answered
`Unlabeled`, `Holds`, `LosesQuorum(d)` or `CommitsAlone(d)`. The labels were in the wire's
`paros.common.v1.AcceptorConfig` and in the stores' format version 5 at the tag
`v0.2.0-alpha.1`. The configuration carried a `cell` beside them until 2026-11-20, when #1264
(remove the cell from paros-consensus) removed it from the core. The registry's
`failure_domain` is the `az`; the core gives the `region` no meaning, and the upper stack leaves
it empty until multi-region cells (#253). Under acceptor
partitions the rule applies per partition (#532 (acceptor partitions)), because the grid's own
predicates do. The placement predicate labels the planned members from the registry and owns the
configuration with its cell, for a create and for every step of the routine of rule 4. **Where
the rule binds** (decision 21): the journal's typed words say it, never the cell's shape. The
rule binds when a person typed `zones=1` (or `regions=1`); with no such word the cell promises
no zone or region survival. A journal that asks for zone survival where the cell cannot give it
is refused with the typed reason (rule 7, section 3.4). Where it
binds, a plan that breaks it is planned again under another tie-break draw, up to
`placement::ZONE_DRAWS` (8), and the create is recorded `Unplaceable` when none holds (a
four-member majority over three zones, 2/1/1, is one). Not yet built: the rule on a move's
replacement (the degraded trap above), the leader that follows its writer (#581), and the
zone-pair latency model.

**The shape of a regional cell.** A regional cell spans at least three zones (section 3.7). Its
minimum production shape is two `storage` machines and one `stateless` machine per zone, nine
machines: `survive=2` needs five acceptors on five distinct machines and survives
a zone only when spread 2/2/1.

- **Acceptors**: `survive=2` 2/2/1, the lone acceptor preferably in the zone that
  writes least (a later reconfiguration adjusts it); `survive=1` 1/1/1;
  `survive=0` none (section 3.4); with `partitions` above 1 the rule applies to
  each partition (amended on 2026-10-14, #532 (acceptor partitions); see grids below).
- **Matchmakers**: 1/1/1 or 2/2/1, majorities only; a tenant's matchmaker preferably never
  shares a machine with its acceptors, while capacity permits.
- **Stateless roles**: at least one frontend per zone. A journal with three proxy leaders or
  more has at least one per zone; proxy leaders and batchers are per-journal counts, spread zone
  by zone (amended on 2026-10-28, #753 (per-journal proxies)). A coordinator term is won again in a
  surviving zone after a zone loss, by a stateless machine first (section 3.3).
- **Replicas** spread over the zones, and a `Read` goes to the replica in the reader's zone.

**The degraded trap.** After the loss of a zone that held two of a `survive=2` journal's acceptors,
three of five remain with zero slack, and the zone rule cannot be met again over two zones. The
journal keeps its size, serves, reports "cannot place" with the reason (section 3.6), and it is
never re-placed into two zones; it is re-placed into a fourth zone only
where the region has one. Its status line says so (section 3.6): `wants 2 machines,1 zone · has
2 machines,0 zones · zone b lost, waiting for a 4th zone`.

**Grids and failure domains** (decided on 2026-10-28, #748 (survive), #749 (configure plan),
#755 (partitions leave durability)).

- **The zone rule applies per acceptor partition** (per column). Each column spreads over the
  zones like one majority. A column inside one zone is refused by the rule's second predicate
  (no domain commits alone). Such a column would also block every later reconfiguration,
  because Phase 1 needs a majority of every old column.
- **A zone loss strikes every column at the same time.** After it, each column has spent its
  margin, and one more failure in any of the `P` columns stops elections. The plan states it,
  for example `after a zone loss: 0 spare per column, 3 columns at risk`. The grid controller
  heals then only where a placement meets the zone rule; on three zones none does, so it holds
  and alerts once for the zone (section 5.1, #1260 (journal health measures)).
- **Regions are the same.** With regions as the failure domain the same holds. Region survival
  and AZ survival stay separate guarantees (below). For a multi-region journal with partitions,
  the plan recommends 3/3/3 per column.

**A zone loss, a region loss.** A regional cell serves through the loss of one zone. The loss of
its region takes it down by design: cells are not failover domains (section 3.7), resolvers
elsewhere keep pointing at it, and nothing fails over. **Static stability** holds inside the
cell: the data plane and the `Resolve` answers come from folds, so only placement, new tenants,
new capacity and moves wait.

```
                AZ a                   AZ b                   AZ c
 entry          frontend T             frontend T             frontend T
 stateless      proxy leader           proxy leader           proxy leader, coordinator T
 journal J      acc1 (LEADER*), acc2   acc3, acc4             acc5             majority 3 of 5
                replica r1             replica r2
 tenant T       matchmaker m1          matchmaker m2          matchmaker m3    majority 2 of 3
 cell tenant    registry acceptor      registry acceptor      registry acceptor
 * the leader follows its writer's zone (relinquish_to, with hysteresis)
 zone c lost: 4 of 5 acceptors, 2 of 3 matchmakers, serves
 zone a lost: 3 of 5, zero slack, serves; cannot place, never re-placed into two zones
```

**Multi-region cells** (M12, decided on 2026-10-07, #253; M13 folded into M12 on 2026-10-09). Failure domains are two-level,
`(region, az)`: in a regional cell every member shares one region and the rule runs over AZs; in a
multi-region cell it runs at both levels. A `survive=2` journal is placed 2/2/1 over the
three regions, its lone acceptor in the witness region, and each region's acceptors of one
journal sit in distinct AZs. The rule: for every region `r`, both quorums hold on `M` minus `r`;
for every AZ `a`, both hold on `M` minus `a`; and no region alone holds a Phase-2 quorum.
**Region loss and AZ loss are separate guarantees, not a joint one**: a region lost together
with an AZ elsewhere leaves two of five, and the journal stops. Joint survival needs 3/3/3 (nine
acceptors, a majority of five), a later mode.

- **Latency.** A write commits from the leader's region with its two local acceptors plus the
  fastest remote one: one round trip to the nearest second region, DSQL's measured behaviour. A
  read costs one cross-region round trip (section 2.5). After a region loss three of five remain,
  with zero slack, and every slot waits for the farthest survivor (Brooker's 2-of-2, section 10).
- **The witness region** holds acceptors and matchmakers with full records, bytes included, and
  no other role: no frontend, replica, batcher, proxy leader or coordinator slot. A server answers
  only from records it holds (section 2.5), and Phase-1 recovery and CTRL repair are unchanged.
  `ColocatedNode` folds by default, so "no replica" means no `Read` routed there and no replica
  slot booked, not a node without bytes. Any `ColocatedNode` may campaign, so keeping the leader
  out of the witness region is a driver placement policy (an election backoff by placement),
  never the core's.
- **The leader moves cheaply.** It holds no durable leadership state, so it moves as cheaply as
  DSQL's adjudicator: one `Relinquish` with its pending rounds (section 3.4).
- **A partition through the witness.** When regions W and C cannot reach each other and both
  reach the witness N, writers commit through N, and the far side's readers stay live and catch
  up from N through ordinary peer catch-up (the 2025 reconstruction's third region that forwards,
  with no relay code). The hazard: both sides can form three of five through N's acceptor, so
  elections can duel across the partition with the witness as tiebreaker. It needs a simulation
  shape and a liveness oracle, leadership settling after the chaos window (section 6); the
  election backoff and the handoff hysteresis are its knobs.
- **Control journals** of a multi-region cell (the cell tenant's, the universe's and the
  tenant control journals) follow 2/2/1, the witness holding one acceptor, and the coordinators
  live in the two full regions. A matchmaker quorum, two of three, needs one remote matchmaker
  per campaign, which is rare.
- **A region loss** leaves a multi-region cell serving, with its leaders in the surviving full
  region; an AZ loss leaves it serving as it does a regional cell.

```
 ┌─ region W ──────────────────┐ ┌─ region C ──────────────────┐ ┌─ region N (witness) ─┐
 │ frontends T (one per AZ)    │ │ frontends T (one per AZ)    │ │ az-a: acceptor n1    │
 │ proxy leaders T(W)          │ │ proxy leaders T(C)          │ │       matchmaker m3  │
 │ batchers T(W), coordinator T│ │ coordinator of cell MR1     │ │ no frontend, replica,│
 │ az-b: acc w1 (LEADER), m1   │ │ az-b: acc c1, m2            │ │ proxy leader, batcher│
 │ az-c: acc w2, replica r1    │ │ az-c: acc c2, replica r2    │ │ or coordinator       │
 └─────────────────────────────┘ └─────────────────────────────┘ └──────────────────────┘
 write from W: w1, w2 + the fastest of c1, c2, n1 = one RTT to the nearest second region
 read from C:  replica r2 asks 3 of 5 for watermarks, so it crosses a region
 region W lost: c1, c2, n1 = 3 of 5, zero slack, a leader elected in C
 region N lost: 4 of 5, writes unchanged
```

**Hierarchical quorums are deferred** (decided on 2026-10-07, #253). `QuorumSystem::Zoned { fz, fn }`
in WPaxos's floor form (Phase 1: `fn + 1` nodes in each of `Z - fz` zones; Phase 2: `l - fn`
nodes in each of `fz + 1` zones) is a legitimate later variant, deferred until the simulation
runs nine acceptors. On three regions of three AZs it gives quorums of four instead of a
majority of five: the same round trip to the nearest region, one ack fewer, and a different
tolerance shape (better against a region plus one straggler in each survivor, worse against two
losses inside one region). It trades acks and tolerance, never latency. The printed WPaxos
set-builder definition does not intersect: on the paper's Figure 3b grid, with `Z = 4`, `l = 3`
and `fz = fn = 1`, `q1 = {A1, A2, B1, C1, D1, D2}` and `q2 = {A3, B2, C2, D3}` satisfy the
printed cardinality and per-zone cap, yet share no node. The printed form only bounds a zone's
contribution from above, while Lemma 1's proof uses exact counts, which is the floor form.

### 5.1 Journal health

**Every part of a journal has a named health measure, and every measure has a fixed action**
(decided by Pierre on 2026-11-20, #1260 (journal health measures)). Pierre asked for the full
list, so that the system and its tests know the state of every part. The other decisions of that
day bind this section: the grid controller (section 2.12) is the journal's own elected role on
stateless machines and also owns scrubbing; after a zone loss it holds and alerts; v1 has no
unsafe recover; the autoscaling decisions get heavy tests. **The v1 controller has no service
level objectives and no scaling driven by latency or load** (Pierre, 2026-11-20): it heals,
rolls, scrubs and offloads, and it changes a count only to replace a member or a role that is
gone. The minimum interval between two reshapes, the suspicion windows and the cap of the rate
limiter are **Open**: no number is set until Pierre gives one or a source gives one (the
controller's crate, after #1189 (journal state model), decides them).

**Two kinds of measure.**

- **P, a protocol fact.** A pure function over the configuration and the set of live members,
  built on the predicates of `membership/` (`has_phase1_quorum`, `has_phase2_quorum_in`,
  `MatchmakerSet::has_quorum`, `zone_rule`), or a fact of the segment table at the matchmakers.
  Each one is named below. The functions that are not in `membership/` today come with the
  controller's crate (`paros-journal-control`, after #1189). A P measure never counts a member
  against a threshold of its own: it crosses `membership/`, as a tally does.
- **O, an operational signal.** A count or a time from the `Events` port of the journal's step
  contract (section 4.2, rule 5, #1255 (journal sans-IO contract)): latencies, rates, queue depths.
  In v1 only the O measures of liveness (`live`, `flap_rate`, `live_count`, `zone_live`) and of
  the controller's own pace (`seal_age`, `signal_age`, `since_last_reshape`, `tokens_held`) make
  the controller act. The O measures of latency, load and errors are shown on the status and
  trigger nothing in v1: the table marks them "shown". An O measure never changes a quorum.

**The controller decides when and where; the protocol decides whether it is safe.** Each action
below is a request that passes the guards of its step: the fenced reshape (section 2.12), the
hold of a bypass (section 2.13), the retire at `F + 1` holders (section 2.10), the repair of CTRL
(#1263 (CTRL recovery in the journal)). A wrong measure can make the controller ask at the wrong
time. It can never make an unsafe step pass.

#### The spares of a column

A column has two spares, because Phase 1 and Phase 2 need different counts of it (`grid/Grid.tla`):

- `phase2_spare(column) = live − acks`: how many more members the column can lose and still
  choose a slot.
- `phase1_spare(column) = live − (rows − acks + 1)`: how many more members the column can lose
  and still give the `rows − acks + 1` promises that a Phase-1 quorum takes of every column.
- `spare(column)` is the lower of the two.

On the partitioned grid of section 2.11 (`rows = 2F + 1`, `acks = F + 1`) and on the majority
(one column) the two spares are one number, and the status shows one spare per column. Other
grids inside the bound of the fenced reshape (`F + 1 <= acks <= rows - F`) split them, and the
catalogue keeps both:

- `rows = 4`, `acks = 2`, a column with 2 live: it still writes (`phase2_spare = 0`), but no
  election can run (`phase1_spare = −1`).
- `rows = 4`, `acks = 3`, every column with 2 live: an election can run (`phase1_spare = 0`), but
  no column can write (`phase2_spare = −1`). A new leader can still reshape, because a reshape
  needs Phase 1 of the old grid and Phase 2 of the new grid only.

`specs/tla/health/` checks every count of this section against the quorums of `grid/Grid.tla`,
on every grid that the fenced reshape accepts up to 9 acceptors, for every set of live members
(`health`, 29 thousand states):

- **The counts say what the quorums say.** Every column has `phase1_spare >= 0` exactly when a
  Phase-1 quorum is live (`electable`). A column has `phase2_spare >= 0` exactly when it can
  choose a slot (`writable`). A count that reads the wrong threshold fails: electability judged
  on `acks` per column (`p2_only`), on a majority of all members (`majority`), and writability
  judged on `rows − acks + 1` (`p1_only`) each give a wrong answer on a grid of four rows.
- **A spare in every column means one more loss is safe.** `spare >= 1` in every column exactly
  when any one more loss leaves every column writable and an election possible.
- **The hold of a bypass can complete exactly when a column can write.** The hold of section 2.13
  waits until the promises meet every read quorum of the old grid. A set of live members meets
  every Phase-1 quorum exactly when it holds `acks` members of one column, which is
  `phase2_spare >= 0` in that column. With one promise less (`hold_short`) the count is wrong.

#### The journal states

The grid part of a journal's health is one of seven states. The states are a partition: each
set of live members falls in exactly one (`OneState`). Each state has one fixed action, and the
model checks that the action can run in that state, or that no action can (`ActionCanRun`).

| State | Condition | Action |
|---|---|---|
| Healthy | `spare >= 1` in every column | none |
| AtRisk | a column has `spare = 0`; electable and writable; a placement that meets the zone rule exists | one heal reshape (it can run its full Phase 1). Two columns at risk: one reshape heals both |
| AtRisk-Unplaceable | as AtRisk, and no placement meets the zone rule (a zone loss on three zones) | hold and alert: the degraded trap above |
| WritesDegraded | not electable; a column can write; the leader is live | writes skip the columns that cannot write (#533 (move a stuck round)); a heal by the bypass of section 2.13, whose hold can complete |
| ElectionBlocked | not electable; a column can write; no live leader | hold and page: no leader can be elected, so no write and no reshape until a member comes back |
| WritesBlocked | electable; no column can write | a new leader heals by a full reshape (only on a grid where `acks > rows − acks + 1`) |
| Stopped | not electable; no column can write | hold and page: no election, no write and no bypass hold |

The first draft of this table (in the journal design document) named WritesDegraded and
ElectionBlocked by "a column below quorum" and the leader alone, and had no state for a grid with
no writable column. TLC breaks both: with no column that can write, a live leader put the journal
in WritesDegraded, whose bypass hold cannot complete (`draft_states`), and an electable grid with
no writable column had no state (`draft_one_state`). The table above adds WritesBlocked and
Stopped.

Beside the grid state, three flags change what the controller may start. Each one comes from a
rule that a TLA+ model checked:

| Flag | Condition | Action |
|---|---|---|
| Reshaping | a segment is sealed and not retired (section 2.12: no reshape while a segment is not retired) | no second reshape; the heal waits for the retire |
| ReshapeStalled | the seal is older than the stall window (**Open**) | alert with the cause: a replica set below `F + 1` holders, a matchmaker set below a majority, or an old grid that cannot give Phase 1 or the bypass hold |
| ControlFrozen | the journal's matchmakers are below a majority | the data plane serves under the standing leader, and leaderless reads go on (they read no matchmaker, section 2.10); no election, seal, retire, reshape, offload mark or controller election can run (section 2.11). The action is a matchmaker handover when a majority of the old set is live, else hold and page |

**A column lost for good is beyond the contract.** v1 has no unsafe recover (Pierre,
2026-11-20). A column that lost more than `F` disks for good leaves the state Stopped or
ElectionBlocked for ever, and the journal says so. A slot that only that column holds, above
the applied prefix of the replicas, can be lost. The journal reports it as data loss
(`data_lost` below). It never fills such a slot with a `Noop`: the tri-state tally of CTRL
blocks on a slot that it cannot prove empty (#1216 (truncation model, part 2)).

#### The catalogue

P = a protocol fact; O = an operational signal. A window, a target or a rate without a number
is **Open** (the first paragraph of this section). "Shown" means the status shows the measure and
the v1 controller does not act on it.

| Level | Measure | Kind | Definition | Unit | States and the fixed action |
|---|---|---|---|---|---|
| Acceptor | `live` | O | the acceptor answered inside the suspicion window | up, suspect, down | suspect and down count as lost for the spares |
| Acceptor | `ack_latency` | O | p99 time from `Accept` to `Accepted`, against the median of its column | ratio | shown (no latency-driven action in v1) |
| Acceptor | `flap_rate` | O | changes between up and down in one window | changes per window | flapping: held as suspect; replaced only after N quiet windows, longer than the minimum reshape interval |
| Acceptor | `faulty_slots` | P | CTRL faulty records (#1216, #1263) that are not repaired | slots | above 0: a repair through the protocol (the CTRL repair probe, a chosen value written under the record's ballot in one write); stuck: alert |
| Column | `phase2_spare` | P | `live − acks` (`has_phase2_quorum_in` on the live set) | acceptors | `>= 1` healthy; `0` AtRisk; `< 0` writes skip the column |
| Column | `phase1_spare` | P | `live − (rows − acks + 1)` (`phase1_per_column`) | acceptors | `>= 1` healthy; `0` AtRisk; `< 0` not electable: WritesDegraded, ElectionBlocked or Stopped |
| Column | `move_rate` | O | rounds of the column that moved to another column (#533), per round | ratio | shown; a high rate while `phase2_spare >= 0` points to a gray failure |
| Grid | `electable` | P | a Phase-1 quorum of every configuration of `H_b` is live (`has_phase1_quorum`) | yes, no | no: see the journal states |
| Grid | `writable` | P | some column can choose a slot | yes, no | no: WritesBlocked or Stopped |
| Grid | `bypass_hold` | P | the live members meet every read quorum of the configuration in force (section 2.13); equal to `writable` | yes, no | the WritesDegraded heal can run |
| Grid | `zone_rule` | P | `AcceptorConfig::zone_rule` on the configuration and on its live members | Holds, LosesQuorum, CommitsAlone, Unlabeled | not Holds on a plan: the plan is refused |
| Grid | `zone_margin` | P | the lowest `phase1_spare` and `phase2_spare` after the loss of any one zone | acceptors | `< 0`: a zone loss stops elections or writes; shown on the status |
| Grid | `segments_unretired` | P | segments that are open or sealed (segment table) | segments | above 1: the Reshaping flag |
| Grid | `seal_age` | O | time since the seal of the oldest segment that is not retired | seconds | above the stall window: ReshapeStalled |
| Journal | `leader_known` | P | a won leadership with a fresh beat | led, electing, leaderless | leaderless while not electable: ElectionBlocked |
| Journal | `commit_latency` | O | time from propose to chosen | seconds | shown |
| Journal | `open_rounds` | O | allocator frontier minus chosen prefix | slots | shown |
| Journal | `apply_lag` | O | chosen prefix minus applied prefix at the replicas | slots | shown |
| Journal | `election_rate` | O | won elections per window | elections per window | shown |
| Matchmakers | `mm_spare` | P | live members minus a majority (`MatchmakerSet::quorum_size`) | matchmakers | `0`: a matchmaker handover; `< 0`: ControlFrozen |
| Matchmakers | `generation_frozen` | P | a `Stopped { successor: None }` is seen, so a handover is in progress | yes, with an age | alert on age: any node that sees it finishes the handover |
| Replicas | `holders(segment)` | P | replicas of the segment's set that hold the whole segment with a good scrub | replicas | below `F + 1`: no retire (safety); re-replicate from a holder. A found damage that is not repaired also blocks the retire (`repairs_pending`, a margin) |
| Replicas | `boundary_written(segment)` | P | the boundary state of the seal is at the matchmakers (section 2.10) | yes, no | no while sealed: no retire; the first replica that applied every slot writes it |
| Proxy leaders, batchers, unbatchers | `live_count`, `queue_depth`, `batch_fill`, `utilisation` | O | per process | count, entries, ratio, ratio | a role below its count is started again (`live_count`); the others shown |
| Offload | `offload_backlog` | P | segments that are retired and not marked offloaded | segments | late: alert; more offloaders |
| Offload | `store_errors` | O | failed puts and reads of the object store | errors per window | shown; the offloader backs off on each error |
| Offload | `drop_before_mark` | P | a replica dropped its copy before the record said offloaded | events | must be 0: a safety alert (section 2.10) |
| Offload | `orphan_objects` | P | objects whose segment is below the trim floor | objects | above 0: the controller's scrub deletes them (#1256 (offload races)); an object of a segment that is not trimmed is never deleted |
| Scrub | `scrub_coverage(segment, member)` | P | share of the member's copy of the segment that a scrub read in this pass | ratio | below 1 at the end of a pass: the pass is late |
| Scrub | `scrub_age(segment, member)` | O | time since the last scrub of that copy | seconds | above the pass bound: the next scrub goes there first |
| Scrub | `damage_found` | P | records, replica log entries and objects that failed their checksum | records | above 0: a repair is queued |
| Scrub | `repairs_pending` | P | damage found and not repaired, per segment | records | above 0 on a segment: no retire of that segment until the repair is done (a margin, kept by Pierre on 2026-11-20, #1263: `F + 1` holders already leave a good copy, so safety does not need it); above 0 for N windows: alert |
| Data | `data_lost` | P | a slot or a segment with no good copy at the acceptors, the replicas or the object store | slots | must be 0; above 0: page. A report is always true (#1263); no unsafe recover in v1 |
| Controller | `term_current` | P | the controller's term is the term of the matchmaker record | yes, no | no: stop (section 2.12) |
| Controller | `signal_age` | O | age of the oldest input of a plan | seconds | stale: no expensive plan |
| Controller | `since_last_reshape` | O | time since the last reshape of the journal | seconds | inside the minimum interval: no reshape |
| Controller | `plans_refused`, `direction_changes` | O | refused calls by reason; up-then-down changes of a count | count per window | shown; a direction change in one window is an alert |
| Controller | `tokens_held` | O | expensive actions in flight, against the per-cell cap of the rate limiter | actions | at the cap: the action waits |
| Zone | `zone_live` | O | share of a zone's machines that answer | ratio | lost: one correlated decision for every journal of the zone, not one per journal |

#### A zone loss

**A zone loss takes every column to its lowest spare at once.** Each column spreads over the
zones like one majority. With `F = 1` over three zones the layout is row = zone, so a zone loss
takes one acceptor from every column, and every column drops to `spare = 0` together
(`ZoneLossEveryColumn`; on the partitioned grid with `2F + 1` zones, to `F − 1`). A rule "heal a
column when its spare reaches 0" would then start one heal for every column of every journal of
the zone.

**The controller heals after a zone loss only where a placement meets the zone rule.** No
placement over two zones survives the loss of one of them, on any grid: a Phase-1 quorum inside
one zone misses a Phase-2 quorum inside the other (`NoPlacement`, checked over every map of up to
six acceptors to two zones; with three zones TLC finds a placement, `zones3_placeable`). So on a
cell of three zones, every journal of the lost zone is AtRisk-Unplaceable: it serves, it holds
its configuration, and the controller alerts once for the zone (`zone_live`), as the degraded
trap above says. It is re-placed only into a fourth zone, where the region has one. A heal that
replaces one acceptor can move survivors to other columns (the layout is a function of the
sorted members), so the controller judges the zone rule and the spares on the new layout, and
the move itself is safe (`config-register/heal_partitioned`, section 2.13).

#### Tests of the health measures and the decisions

The pure function `decide` of the controller takes the measures and returns at most one plan.
Its tests come with its crate, after #1189, and follow the plan of the design document:

1. Property tests of `decide`: every plan meets the zone rule on the new layout; no reshape
   inside the minimum interval; no plan from a latency, load or error signal (v1); no plan on
   stale signals or while a segment is not retired; the answers are monotone, deterministic and idempotent, with
   hysteresis.
2. A reference `decide` written again from this section, with no shared code, compared with
   `decide` on every small view (`P <= 3`, `F <= 2`, three or four zones).
3. A `consensus-sim` flavor where a controller step is a walk move, with two controllers of
   different terms, trace-checked against `specs/tla/grid-controller/` (#1261 (grid controller
   decision tests)).
4. A moonpool-sim scenario library: loads (ramp, step, spike, diurnal, a sawtooth at the
   controller's own period, a load that sits on a threshold, one hot journal among many) and
   faults (kills during a seal, a retire and an offload, a matchmaker loss during a reshape, a
   controller crash between plan and call, a gray acceptor, a flapping acceptor, a zone kill).
5. Oracles, each with a planted mutant: `NO_UNSAFE_PLAN`, `BOUNDED_RESHAPE_RATE`,
   `NO_OSCILLATION`, `CONVERGES`, `COST_BOUND`, `FENCED`, `NO_LOAD_PLAN` (no plan follows a
   change of load alone), and one that recomputes
   each P measure from the walk's own state, with no shared code, and compares.
6. A `probe!` on every branch and every refusal reason of `decide`; the coverage gate requires
   all of them, with no excused probe (the absolute rule of `AGENTS.md`).
