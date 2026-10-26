[← 04-roles.md](04-roles.md) · [Index](README.md) · [06-verification.md →](06-verification.md)

## 5. Failure model and zones

The service survives disk loss and machine loss in one region: a corrupted record is CTRL's
`faulty` and repaired by the protocol, a wiped disk is refused at boot as amnesia and the
identity is replaced by reconfiguration, a crashed machine restarts as an existing member, a dead
machine is reconfigured out and its journals placed elsewhere. This is what the simulation
already exercises; what changes is who drives the healing: today the harness's client composes
the reconfigurations, in the service the tenant's coordinator does, from desired state. Losing a
control quorum will be recoverable at both the cell and the universe level, without touching user
data (section 3.10, deferred). Beyond one region, a multi-region cell survives the loss of a
region (M12, below).

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
the tenant coordinator reads every record of every live journal of its tenant in the background,
at a low rate, and repairs the rot before a reader needs the record. Eat your own food: the
scrubber is one duty of the elected tenant coordinator (one per tenant), never a task of the
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
  page finds damage. A newly elected coordinator resumes from it; the fold refuses a record
  behind the one it holds. `parosctl status --tenant` shows it, and each machine counts the
  records it scrubbed, found damaged and repaired (`paros_scrub_*_total`).
- **Pacing.** Three tunables with documented floors: `scrub_rate` (bytes per second per tenant,
  a token bucket; floor 1 KiB/s), `scrub_pass_within` (the full-pass bound: a pass starts no
  sooner than this after the last one this term started, and a pass that outlives it is recorded
  late; floor 1 s) and `scrub_page` (records per request; floor 1). Production reads 4 MiB/s per
  tenant and a full pass each week. A new term with no pass running starts the next one at once:
  its clock knows nothing of the last term's, and the rate bounds the cost.
- **Not yet scrubbed.** The tenant control journals and the cell's own journals (the registry,
  the universe directory, the election journal) are not scrubbed: a later issue.

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

**The storage contract.** paros's stores are moonpool-journal's (`paros::journal`), and the
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
of the store's files (`paros::journal::drop_caches`, `StorageFile::drop_cache`,
`posix_fadvise(DONTNEED)`, moonpool#361). The open then reads what the device holds. It does
not sync: a dirty page a failed commit left still reads as written and lands at the next sync
(#348), and a sync at every open could fail and take the open with it. A storage fault still quarantines its
journal, not the process. Barrier violations ("sync
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

**The journal is CLSTORE, and paros uses it directly** (decided on 2026-10-07, moonpool#308). The
first moonpool-journal was a dense Raft-shaped log; paros journaled operations into it and folded
them at boot, with checkpoint brackets. It is rewritten as a protocol-free CLSTORE journal: an entry
is a `u64` position plus an opaque identity, written in any order (laggy slots), with overwrites,
tombstones and a floor; a persist record far from each entry; two-copy metainfo in its own file,
the copies written one after the other; no snapshots and no compaction. A commit is one sync
(CLSTORE's, the last batch ambiguous) or two (always decided), recorded per batch; production
ships two, the simulation draws either. paros stores the state itself: a slot is a position, its
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
`FailureDomain { region, az }` parallel to its sorted members, holding the names copied from the
registry's `paros/region` and `paros/zone` tags (`RegisterNode`, section 3.2, e.g. `("eu-west-1", "eu-west-1a")`):
short bounded strings, compared for equality only, never parsed or ordered by the core. Names mean
the same thing universe-wide, so a move between cells (section 3.7) compares domains correctly; a
cell-scoped pair of integers would let one cell's `az 2` equal another's and the zone rule could
accept a placement that does not survive a zone. The cost is a few dozen bytes per member in a
configuration of at most nine members. The vector is bound to the ballot with the
configuration, because two nodes that disagree on a member's failure domain evaluate different
quorums; the registry's zone tag is the composer's input, copied when it composes, never
read live by a tally. An empty vector is the plain arm, byte-identical to today's configuration,
so plain Multi-Paxos is unchanged. The configuration's `cell_id` rides the same single format bump
(section 3.7).

**The zone rule** is two predicates over a configuration's members `M`, built from
`has_phase1_quorum` and `has_phase2_quorum` over subsets of voters, never a count:

1. **It survives any one failure domain**: for every domain `d`, both `has_phase1_quorum` and
   `has_phase2_quorum` hold on `M` minus `d`.
2. **No domain commits alone**: for every domain `d`, `has_phase2_quorum` fails on `M`
   intersected with `d`.

The rule is valid for `Majority`, `Flexible` and `Grid` alike. It is one pure function in
`membership/`, shared by the tenant coordinator's placement and the simulation's composer.

**As built** (2026-10-16, #215 (zones)). `paros_core::FailureDomain { region, az }` and
`AcceptorConfig::zoned` carry the labels; `AcceptorConfig::zone_rule` is the rule, answered
`Unlabeled`, `Holds`, `LosesQuorum(d)` or `CommitsAlone(d)`. The configuration's `cell` rides
with the labels (`AcceptorConfig::with_cell`, `Config::cell_id`), and both are in the wire's
`paros.common.v1.AcceptorConfig` (fields 7 and 8, absent when empty) and in the stores' format
version 5: a store of version 4 does not open. The registry's `failure_domain` is the `az`; the
`region` stays empty, the cell's own region, until multi-region cells (#253). Under acceptor
partitions the rule applies per partition (#532 (acceptor partitions)), because the grid's own
predicates do. The tenant coordinator's create (`machine/tenants.rs`) labels the planned members
from the registry and owns the configuration with its cell; a move (`machine/moves.rs`) does the
same for the machines that stay. **Where the rule binds** (the open question of #215, default
taken on 2026-10-16): a durability with `failures-per-partition` 1 or more, over storage machines
that span three zones or more (`paros::placement::zone_rule_binds`). Over one or two zones no
configuration can meet it, so the create is placed as before, without zone survival. Where it
binds, a plan that breaks it is planned again under another tie-break draw, up to
`placement::ZONE_DRAWS` (8), and the create is recorded `Unplaceable` when none holds (a
four-member majority over three zones, 2/1/1, is one). Not yet built: the rule on a move's
replacement (the degraded trap above), the leader that follows its writer (#581), and the
zone-pair latency model.

**The shape of a regional cell.** A regional cell spans at least three zones (section 3.7). Its
minimum production shape is two `storage` machines and one `stateless` machine per zone, nine
machines: `failures-per-partition=2` needs five acceptors on five distinct machines and survives
a zone only when spread 2/2/1.

- **Acceptors**: `failures-per-partition=2` 2/2/1, the lone acceptor preferably in the zone that
  writes least (a later reconfiguration adjusts it); `failures-per-partition=1` 1/1/1;
  `failures-per-partition=0` none (section 3.4); with `partitions` above 1 the rule applies to
  each partition (amended on 2026-10-14, #532 (acceptor partitions)).
- **Matchmakers**: 1/1/1 or 2/2/1, majorities only; a tenant's matchmaker preferably never
  shares a machine with its acceptors, while capacity permits.
- **Stateless roles**: once a tenant's pool has three instances, at least one frontend and one
  proxy leader per zone. Coordinators run on `stateless` machines and are re-elected in a
  surviving zone after a zone loss.
- **Replicas** spread over the zones, and a `Read` goes to the replica in the reader's zone.

**The degraded trap.** After the loss of a zone that held two of a `failures-per-partition=2` journal's acceptors,
three of five remain with zero slack, and the zone rule cannot be met again over two zones. The
coordinator waits out the re-placement bound (section 3.2), the journal shows `Degraded`
(section 3.6), and it is never re-placed into two zones; it is re-placed into a fourth zone only
where the region has one.

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
 zone a lost: 3 of 5, zero slack, serves; Degraded, never re-placed into two zones
```

**Multi-region cells** (M12, decided on 2026-10-07, #253; M13 folded into M12 on 2026-10-09). Failure domains are two-level,
`(region, az)`: in a regional cell every member shares one region and the rule runs over AZs; in a
multi-region cell it runs at both levels. A `failures-per-partition=2` journal is placed 2/2/1 over the
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
- **Control journals** of a multi-region cell (the cell tenant's, the election journals, the
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
