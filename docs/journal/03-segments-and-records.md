[← 02-data-plane.md](02-data-plane.md) · [Index](README.md) · [04-roles.md →](04-roles.md)

## J3. Segments and records

**A segment is the run of slots of one configuration** (one grid and one replica set). The
matchmakers of the journal hold one record for each segment, from the reshape that opens it to
its trim (design A, decided by Pierre on 2026-11-19 after the TLA+ experiment of #1213 (segments
in the protocol or in storage), `specs/tla/experiments/segments/`). In v1 only a reshape opens a
segment; a leader change opens none, and there is no size or age roll (§J1.1).

The life of a segment, with the section of each step:

1. **Open**: the grid controller's fenced `reshape` call writes a new record in state `pending`
   (§J6.1, §J3.4).
2. **Seal**: the first leader of the new configuration writes the end slot of the old segment
   once, one above the highest vote that its Phase 1 saw in the old grid, and fills the slots
   below it in its own grid (§J3.1). A CTRL faulty record counts as a vote for the end (#1313).
   The new segment is then in force (§J3.4).
3. **Boundary**: the first replica of the old set that applied every slot writes the boundary
   state (`next_seq`, `first_seq`, the writer uuid) into the old record; the next segment folds
   from it. Two seals in a row and an empty segment need no `Fence` slot (§J3.1, #1303).
4. **Retire**: after `F + 1` replicas of the set hold the segment and the boundary is written,
   the leader marks the record retired, and only then do the old acceptors stop (§J3.1, §J3.4).
5. **Re-replicate** (v1): a sealed segment whose replica set lost a member is copied to a new
   member (§J3.1, #1314).
6. **Offloaded** (prepared, not built in v1): the state and the read path exist; nothing sets it
   (§J3.1).
7. **Trim**: once a decided `Truncate` puts the floor over the whole segment (§J3.2).

Reads route by the `seq` range of the records and take their floor from the segment in force
(§J3.2); a read across a reshape is ordered by the read fence of the core (§J3.3).

### J3.1 Segments

*Moved here from `docs/architecture/02-data-plane.md` §2.10 (#1332 (journal architecture docs)).*

**The matchmakers of a journal hold its segment table** (decided by Pierre on 2026-11-19, with
the TLA+ experiment of #1213 (segments in the protocol or in storage),
`specs/tla/experiments/segments/`). A segment is the run of slots that one configuration (one
grid) holds. Only a reshape opens a segment in v1 (no size or age roll, §J1.1); a leader
change opens none. The
matchmakers keep one record for each segment until trim: its start slot, its end slot, its grid
and its state (open, sealed, retired, offloaded, trimmed). A segment whose only copy is in the
object store is still in the table, so the grid controller, placement, the offload policy and
the readers see where each slot range lives as decided state. §J3.4 lists every field of
the register, its writer and its guard. The rules that the experiment
checked, each with a control that TLC breaks:

- The first leader of the next configuration writes the end slot once, by compare-and-set, after
  its Phase 1. The end is **one above the highest slot where its Phase 1 saw a vote in the old
  grid**, not the first uncommitted slot: with that end, the new grid writes a second value at a
  chosen slot above the end, and the retire stops the acceptors that hold the chosen vote. The
  sealing leader proposes again, in its own grid, each vote below the end that the replicas did
  not apply.
- **A faulty vote counts for the end** (#1313 (the seal end against a CTRL faulty record), checked
  on 2026-11-20 by `specs/tla/seal-ctrl/`). "Saw a vote" includes a CTRL faulty report (a record
  that kept its ballot and lost its value), so the end is the frontier of the sealer's Phase 1 over
  the old grid. An end of readable votes only (`end_readable`) lands at or below a chosen slot whose
  top copy rotted, and the next leader chooses a second value there. The
  seal does not wait for the repair probe: a blocked slot below the end stays in the tri-state tally
  of every later Phase 1. Two other candidates keep one value but leave a chosen slot of the old
  grid at or above an end of readable votes, out of the boundary state and the retire: a seal that
  waits for its repair probe (`wait_cover`), and a Phase 1 that reads the whole old grid
  (`end_readable_cover`); the latter also keeps the old grid in every later Phase 1.
- **A leader of the configuration in force writes every slot above the applied prefix and below
  the start of its segment**: the vote that its Phase 1 saw, or a `Noop` where it saw none (the
  election gap fill of the core, #1233 (segments gap fill)). Without the `Noop`, a slot below
  the end with no vote is a hole that no ballot can write, because the old grid is sealed: the
  replicas stop at it for ever, so retire, offload and trim never come.
- **A later Phase 1 reads every slot of every configuration that is not retired**, sealed or not,
  below its end and above it (decided by Pierre on 2026-11-23, review of segments in Paxos, finding
  3; checked by `specs/tla/journal-read/`, `end_applied_safe`, with the control
  `end_applied_end` that reads a sealed grid only below its end and chooses two values). One
  value for each slot rests on the promises alone: the end routes a slot to its replica set and
  decides the retire, and it never hides a vote. It is a pure journal fact.
- **A leader binds its ballot before its Phase 1** (decided on 2026-11-23, review of segments in
  Paxos, finding 2; `specs/tla/journal-read/`, control `bind_stale`). The leader writes
  `Bind(ballot)` on the register by compare-and-set, and takes its configuration (the segment
  the register bound the ballot to) and the segments its Phase 1 reads from the value of that
  done write. The register takes a bind only above every bound ballot, so a higher ballot is
  bound to the same segment or a later one. A bind of the bound ballot is "ok" again while the
  newest segment is still the one it is bound to (a lost reply). **Open**: a leader that
  restarts and campaigns again at the same ballot is outside the model; the step that builds a
  durable ballot for the leader decides it. A bound leader may take a later value than the
  one of its own done write before its Phase 1 (a value of another write, or the "ok" again
  of a resent bind): it reads the newer ends and retires, and one value stays chosen
  (`journal-read/`, `bind_late`). A ballot bound from a value read before a seal reads only the sealed grid as its own, and two values
  are chosen. This is the registration of Matchmaker Paxos, kept in the journal's register.
- A leader's writes are fenced by its promise quorum, never by the table version, so a change of
  a sealed segment (retire, offload, trim) never stops the tail (as in Delos).
- A replica drops its copy of a segment only after the record says offloaded. The object key is
  the journal and the segment, so the record stores no location. **"Says offloaded" means a
  write of the mark that is done at a matchmaker majority** (#1275 (segment records across a
  matchmaker handover), checked by `specs/tla/segment-records/`): the offloader's own
  compare-and-set, after which it tells the replicas, or the replica's own compare-and-set of the
  same value. A read that shows the mark is not enough: the mark can be on a minority only, and
  another majority, or the successor of a matchmaker handover, says retired (control
  `drop_read`). Each step that acts on a record (the retire of the old acceptors, a drop) acts on
  a write that it saw done, never on a read alone.
- **The records cross a handover of the matchmaker set** (#1275, `specs/tla/segment-records/`).
  The records and the matchmaker record are one compare-and-set register for each journal, fenced
  by the matchmaker generation. The handover's successor takes the highest write of the stop
  quorum and the highest read generation, as for the configuration register (`handover/`), and
  a frozen matchmaker takes nothing. So each record write that a majority took is in the
  successor, no record moves back, the seal end is written once, a compare-and-set on the old
  set after the stop fails, and a deposed controller's reshape fails across the handover too.
  Controls: a successor that copies one member of the stop quorum (`close_one`,
  `close_one_term`), a frozen member that takes writes (`frozen_serves`).
- **Offload is prepared, not built, in v1** (Pierre, 2026-11-20). v1 keeps the offloaded state in
  the segment record, the read path that checks it, and the record rules of #1256 and #1276
  (user truncate across segments). The offloader role, the object store code and the scrub of the
  objects come later. The rules below are the design for that day.
- **The offloader** (a stateless role, Pierre 2026-11-20; checked by `specs/tla/offload/`, #1256
  (offload races and the offloader role)) offloads a retired segment in four steps: it reads the
  segment from a replica that applied every slot up to the `next_seq` of the record, builds the
  object with deterministic bytes, writes it with a conditional put at the key, and marks
  `offloaded` by compare-and-set from "not offloaded and not trimmed". The races of two
  offloaders, a lost mark, an offloader that stops between the put and the mark, and a trim that
  comes during the upload need these rules, each with a control that TLC breaks:
  - A put that finds an object is not a failure: the offloader reads the object and marks when it
    holds the whole segment, whatever the release in its header. Giving up never marks after a
    lost mark; comparing bytes never marks across a rolling upgrade.
  - A failed mark deletes the key only when the record says trimmed (the late upload after
    trim). A delete on any failed mark deletes the object that the other offloader marked.
  - The trim deletes the object by its key, marked or not, and the controller scrubs: it lists
    the store and deletes each object of a segment below the trim floor. The scrub never deletes
    an object of a segment that is not trimmed, even with no mark: that object can be between
    its put and its mark. A tombstone at trim was checked too; it also needs a list (a
    controller can stop between the trim and the tombstone) and stays for good, so the design
    takes the scrub.
    - Offload does not need the retire: one complete copy and `F + 1` holders until the mark are
    enough. The controller still offloads retired segments only.
  - **Open** (the offload design, after v1): the repair of a damaged object. The check of CTRL
    in the journal (#1263, `specs/tla/ctrl-segments/`, §J5.3) puts a damaged object again from a replica, but the put is
    conditional, and no rule here deletes the object of a segment that is not trimmed. A replace
    needs a delete that the mark fences.
- **A sealed segment is re-replicated in v1** (decided by Pierre on 2026-11-20, #1315,
  decision 4). With offload deferred, a sealed segment lives only on its replica set until the
  user truncates over it. When a member of that set is lost, the controller copies the segment
  from a holder to a new member and changes the replica set in the record. Six rules make this
  heal safe and live (#1314 (re-replication of a sealed segment's replica set), checked on
  2026-11-20 by `specs/tla/re-replication/`). With them, every slot at or above the floor of a
  segment that is not trimmed stays on a live replica, and the margin of the promise holds: with
  `D` members of the set down, at least `F + 1 - D` live members hold the whole segment (`F + 1`
  with no member down). Each rule has a control that TLC breaks:
  - **A new replica counts only on its own complete copy.** The controller writes the new set
    only after the new replica reports that its copy reached the segment end (the `next_seq` of
    the record). A replica counted when its copy starts (`count_start`) lets the next failures
    lose a slot.
  - **The copy comes from a member of the set that holds every slot up to the segment end**, as
    the offloader. A source that holds only a prefix (`copy_any`) gives a copy that looks complete
    and is not, and a slot is lost. A source outside the set (`live_outside`) can drop its copy
    during the copy, and the heal stops.
  - **The change of the set is a compare-and-set on the version of the register that the
    controller read.** The version covers the state of the record, the set and the controller
    term. A write checked on the term only writes the record of an old read and undoes a trim
    (`cas_term`). A write with no check lets a deposed controller and an old read put back a
    replica that dropped its copy (`cas_blind`). With the version, the term check adds no safety
    (`version_only` passes), because the election writes the register; it stays, to tell a
    deposed controller to stop (§J6.1). The model never asks a node twice for a copy. In the
    code a later plan can pick the same node, and there the term check is needed: a deposed
    controller's compare-and-set, on a record that did not change, could count a copy that the
    new holder's copy wiped (`specs/tla/heal-reuse/`, control `version_only`; `journal-sim`'s
    heal walk, mutant `NoTermCheck`; #1314).
  - **A node runs one copy of a segment, the highest `CopyId` it was asked** (the term of the
    plan, then its number in that term; #1314, found by the heal walk). A higher copy replaces
    the one it runs, and a lower one, from a deposed controller, is refused (`heal-reuse/`,
    control `copy_any`; mutant `AnyCopy`). `specs/tla/heal-reuse/` models a node that a later
    plan asks again, requests and answers that the network delivers twice and late, and a node
    that knows the set only from the last register value it saw; it checks the five rules of
    this list and of "As built" below that `re-replication/` cannot see, each with a control.
    The highest-copy rule and the fresh-value rule are a contract of the driver that starts and
    abandons copies: the `Host` of `paros-journal-io` keeps them (`Input::Join`,
    `Input::Abandon`; `Driver::join` and `Driver::abandon` read the register first).
  - **A "done" report belongs to one copy.** A new plan starts a new copy. A report kept from an
    aborted copy (`report_sticky`) counts a replica that dropped its copy since.
  - **A replica that does not hold a slot refuses the read**, and the reader reads the record and
    the floor again. A read that took the set before a heal stays correct: a removed replica
    still holds the slot (the bytes of a sealed slot never change) or refuses. A replica whose
    copy ends below the slot must not answer "no record" as at the tail of an open segment
    (`answer_partial`).
  - **A replica that is not in the set and is no plan's new replica drops its copy.** A removed
    replica that comes back drops it too and never counts again on its old copy: to join again it
    copies again. A trim during the heal fails the heal's compare-and-set (the state is in the
    version), the plan ends, the new replica drops its copy, and the trimmed record stays trimmed.

  The heal waits for one member that holds the segment and a matchmaker majority (§J5.1). The
  model has no register across a matchmaker handover (`segment-records/` checks it) and no replica
  that comes back with an empty disk (a new replica stands for it).

  **As built** (#1314): the register's `Change::Rereplicate { segment, read, term, remove, add }`
  writes the new set only if the record equals `read` and the term is the register's, and only
  from `retired` (`SegmentRecord::replace_replica`); the same change again answers "ok". The
  record equality stands for the version: the record holds the state, the set, the boundary and
  `heals`, the count of the heals that changed the set. The set is the one field that can return
  to an earlier value (a removed member that came back is a candidate again), so without the
  count a heal delayed in the network could apply a second time on a set that only looks like
  the one it read (`heal-reuse/`, control `no_heals`; `journal-sim`'s heal walk, mutant
  `NoHealCount`, oracle `COMPARED`; found by the review of PR #1470).
  - **A heal whose compare-and-set was sent ends only on an answer, or on a record that is
    neither its read nor its result** (found by the heal walk with delayed copies of requests).
    A copy of the change can still be in the network; if the heal ended when its new replica
    went down, the replica would drop its copy when it came back and the late copy would write an
    empty member (`heal-reuse/`, control `sent_abort`). A record that moved past the read can
    never come back to it (the heal count), so the change can no longer apply. A down new replica
    that holds its copy is a member that a later heal replaces.
  - **A node acts on an abandon or a copy ask only on a register value that it saw done after
    the message arrived** (found by the review of `heal-reuse/`). A refusal or an old read does
    not prove that the heal did not commit: the "ok" of an earlier copy of the change can be
    lost, and the record can move on. A node that has not seen the commit that named it would
    take the abandon and then a late copy ask, which wipes a member (`heal-reuse/`, control
    `stale_view`). A change that the register refused, or that an old read ended, can never
    commit later, so a value seen after the abandon shows whether it did: if the value names the
    node, it is a member and keeps its copy (`SegmentReplica::abandon` takes that value).
  The controller role plans one heal at a time, the first member that is down, asks the new
  replica to copy (`Ask::Copy` with its `CopyId`), counts it on its own report of that copy, and
  gives the compare-and-set until it is answered; a read that changed, a new replica that is down
  or a refusal ends the heal (`Ask::Abandon`; a sent heal ends only on a read that changed or on
  an answer). The new replica (`SegmentReplica::join`) persists
  its copy id (`ReplicaOp::Joining`, so a restart keeps the copy while the plan runs), pulls only
  from members of the set that say they hold the whole segment (`Holding`, `Holds`), and reports
  `Copied` when it is full. A replica in no set that runs no copy, or of a trimmed segment, drops
  its copy and stops (`Stop::Dropped` in the `Host`). A node that serves the segment as a member
  refuses every copy. `decide` heals only a retired segment: a sealed one retires first.
  The `Host` runs `Ask::Copy` and `Ask::Abandon`, and the whole-journal walk drives them (#1445).
  **A copy of a term that is over ends** (orphan copies): a node that reads a register value
  that does not name it and whose term is above the term of its copy ends the copy and drops it
  (`SegmentReplica::observe`), and the `Host` refuses a copy ask of a term that is over and starts
  no copy from a joining record of such a term at boot. This is safe because the register refuses
  the compare-and-set of an old term (`TermFence`), so that copy can never be counted. A
  controller that a view shows deposed and whose heal sent no compare-and-set ends the heal with
  `Ask::Abandon` and stands down. Checked by `specs/tla/heal-reuse/` (`orphans`: every copy that
  the register does not hold ends once the faults stop; controls `orphan_kept` and
  `orphan_same`). **Open**: a copy of the term in force whose abandon is lost runs until the next
  term or a trim; a heal in flight holds back the controller's
  other decisions until it ends; the storage has no delete, so the records of an earlier copy and
  of a dropped replica stay on the disk (true chosen slots of the segment, a disk leak, where the
  model wipes them).
- **Each segment has its own replica set, and a write reaches only that set** (#1271 (a write
  reaches only the replica set of its segment), checked by `specs/tla/segment-replicas/`). Over
  the whole journal the replicas therefore do not bound write throughput, as they do in
  Compartmentalized Paxos (every replica executes every write, sections 3.3 and 8.5); inside one
  segment they still do. Because the journal state is folded at apply (§J2.1 and §J2.2), a
  replica of the next segment starts from the **boundary state**, the journal state at the seal:
  `next_seq`, the trim floor `first_seq` (#1276 (user truncate across segments),
  `specs/tla/segment-truncate/`, below) and the leader uuid. **The whole boundary state is in
  the segment record** (#1303 (two seals in a row), checked by `specs/tla/seal-chain/`; decided
  by Pierre on 2026-11-20 after the check). The first replica of the old set
  that applied every slot of the segment writes it once, by compare-and-set at the matchmakers,
  into the record of that segment. A replica of the next segment starts from the record of the
  segment just before it, never from the newest record written (control `record_latest`: a
  deposed writer writes). An empty segment, sealed before its first slot, gets the record of its
  predecessor again, and a `Noop` at any slot changes nothing, so no slot of the next segment is
  needed for the boundary. `next_seq` and `first_seq` never move back across a chain of seals.
  The first form of the rule put the uuid in the journal's matchmaker record and a `Fence(uuid)`
  at the first slot of the next segment (#1271 (a write reaches only the replica set of its
  segment), `specs/tla/segment-replicas/`, `meta`). Through two seals in a row it is also safe,
  but it needs two more rules, each with a control where a deposed writer writes: the leader
  reads the uuid in the same read as the record of the segment before (`meta_any`), and the seal
  of a segment with no `Fence` leaves the field as it is (`meta_blank`). It also needs the
  `Fence` slot and the refusal of every command before it when the gap fill takes that slot
  (#1302 (the Fence slot against the Noop gap fill)). The segment record needs none of these. A
  record with `next_seq` and no uuid lets a writer that a `SetLeader` of the old segment deposed
  write again. A reader routes a read by the `seq` range of the segment records. A segment
  retires after `F + 1` replicas of its set hold it and its boundary state is written; after
  that the next segment never needs its slots.

### J3.2 A user truncate across segments

**A user truncate across segments** (#1276 (user truncate across segments), checked by
`specs/tla/segment-truncate/`). A `Truncate` stays a decided command of the log, judged at apply
(§J2.6). With segments, six rules keep it safe and live, each with a control that TLC
breaks:

- **The boundary state holds `first_seq`.** The replicas of the next segment check every read
  against their own fold, and a floor that a `Truncate` of the old segment decided is in no slot
  that they apply. Without `first_seq` in the boundary state (`nofirst`), or with a floor taken
  from the trimmed segment records only (`trimstate`, wrong when the floor lands inside a
  segment), a read returns a record below the floor. The writer of the boundary state writes
  `first_seq` with `next_seq`, in the same compare-and-set; both are fixed at the seal.
- **A read takes its floor from the segment in force.** A replica of a sealed segment folds only
  up to the seal, so its own `first_seq` misses every later `Truncate`. The reader gets the floor
  from a replica of the segment in force, at the basis of its leaderless read, and asks the
  holder of an older segment only for a `seq` at or above that floor (`holder` returns a record
  below the floor). Above the floor, every holder still has the record: its own floor is never
  higher.
- **A read of an offloaded segment checks the floor first.** The object holds the whole segment,
  also the records below a floor that lands inside it. The replica that serves the object (or
  its cache) answers `Truncated` below its floor (`cache_unchecked`).
- **A segment is trimmed only once the floor is decided over it, and only when it is sealed.**
  The record of a segment becomes trimmed when a replica's fold has `first_seq` at or above the
  segment's last `seq` (the `next_seq` of its boundary state), from sealed, retired or offloaded;
  a trimmed record never becomes live again, and the trim deletes the object by its key. A trim
  on the intent to truncate, before the `Truncate` is decided (`trim_intent`), and a trim of the
  open segment, whose range so far is below the floor (`trim_open`), lose records above the floor.
  This agrees with the offloader of #1256 (offload races): the controller scrubs only objects of
  segments below the trim floor, and a failed mark deletes the key only when the record says
  trimmed.
- **The acceptors of a segment compact only to a slot that `F + 1` replicas of its set compacted
  to.** Each of these replicas applied and folded every slot below it, so with `F` of them down
  one is still live to give a laggard its trim point. The acceptor never reads the chosen prefix
  (`AGENTS.md`), so it does not count the replicas itself: the replicas report their compaction
  point to the leader side, which counts them with a predicate of `membership/` on the replica
  set (as `collector.rs` counts the GC acks today) and sends the acceptors a floor. This is the
  layering rule applied to the new step; the message is new. The step waits for the reports of
  `F + 1` replicas only, and the floor that it sends waits for no answer, so it completes with `F`
  replicas of the set down; one more down blocks it (`quorum-everywhere/`, `compact`,
  `compact_replica`; #1318 (quorum-everywhere rows for the boundary write and the acceptor
  compaction)).
- **The acceptors keep their floor for each segment, keyed by the replica set of the slot, not
  by the grid where the vote lives** (#1318, checked on 2026-11-21 by `segment-truncate/`,
  `acc_grid`). The sealing leader proposes again, in its own grid, each vote below the end that
  the replicas did not apply (§J3.1). Those slots live at the acceptors of the new grid but
  belong to the old segment and its replica set. So the acceptors drop such a slot only at the
  floor that `F + 1` replicas of the old set reported, or at the retire of the old segment. A
  grid that keeps one floor, counted on the replica set of the segment in force, drops them as
  soon as `F + 1` replicas of the new set start: one replica of the old set applied the segment
  (it wrote the boundary state), the others did not, and when it stops no live member and no
  acceptor holds the slot (`Findable` fails). Without a `Truncate` the acceptors keep every vote of the open segment;
  in v1 only a reshape and its retire free the votes of a segment. Acceptors that compact to the decided
  floor alone (`acc_floor`) can drop the slots of a segment that no replica applied yet: nobody
  can then fold the segment, its boundary state is never written, and the next segment never
  starts. In the colocated core an acceptor compacts at the walk of its own node, so the case
  does not come up; Compartmentalized Paxos does not state a rule for it, so this model is its
  check.
- **A laggard jumps.** A replica behind the floor of its segment, whose next slot no acceptor
  and no peer holds, takes the trim point of a peer (`TrimmedTo`, as in `truncation/`) and never
  fetches a slot of a trimmed segment (`nojump` stops it for ever).

**As built** (#1326 (the journal roles over the core), 2026-11-22). A replica writes `Trim` for
each retired segment below its own once its fold's `first_seq` covers the segment's last `seq`,
after the batch of the slot that moved the floor; it waits for the retire because the controller
opens a segment only over retired ones (`ReshapeGuard`), and a trimmed record never retires.
Every replica of a trimmed segment drops its copy, when it hears the trim: a replica that lags
can still apply a slot of the segment before then, and the model has the drop as a step of its
own (`Drop` in `Journal.tla`). The reader of `journal-sim`'s roles walk takes its floor from a
leaderless read at the segment in force before it reads an older segment, and answers
`Truncated` itself below it. The client does the same (`Request::Floor`, then `Request::Read`
at or above the floor), judged by the client walk's oracle `FLOOR` (mutant `FloorFromOldest`: the
program asks the floor of the oldest segment); the client takes the floor only from an unsealed page,
so a copy that a stale register value routes to a sealed segment asks again. The roles walk's oracles `TRIM` (mutant `TrimIntent`) and `FLOOR` (mutant
`FloorFromHolder`) judge the two rules, and the trace check binds each trim to `Trim(p, r)`.

**As built: the compaction** (#1326 (the journal roles over the core), 2026-11-23, model
`specs/tla/journal-compact/`). The rules above, in the roles:

- **The replica compacts its log.** On each tick a replica finds its compaction point: the first
  slot that holds a write with a record at or above its fold's `first_seq`, else its front. It
  writes one `Trim` record (the point, the fold there and the floor `first_seq`) and keeps no slot
  below the point; storage deletes nothing, so the `Trim` hides the older records of the replica
  log (`layout` kind `Trim`). The fold at the point may be **blind**: its `first_seq` is below the
  `Trim`'s floor, because the `Truncate` that raised the floor lies above the point. A blind fold
  gives no answer (no `Applied` answer, no `Resolve` but `NotCovered`, no read), and it does not
  compact again until it applies that `Truncate`; every write below the floor is then below the
  floor whatever its answer was. The model has the floor (`rfloor`) and `Blind`, and a check
  that the walk reaches a blind fold (`reach_blind`).
- **The leader counts the reports.** The leader asks each replica of each segment it reads for
  its point (`Compaction`); a replica answers its point and the slot below which it holds no answer
  (`CompactedTo`). The point that `F + 1` replicas of the set reached goes to every acceptor of
  every segment the leader reads (`Compact`). The leader closes its open rounds below that point,
  and answers `Unknown` for a held command whose slot is below the slot that `F + 1` replicas
  answered: those answers are gone.
- **The acceptor drops the votes.** An acceptor drops its votes in the range (`[start, below)` of
  the segment) in a batch of its own, writes a `Dropped` record, and remembers the range. It answers
  no `Accept` in a dropped range. Its promise pages carry the dropped ranges: Phase 1 takes no value
  and fills no `Noop` in a range that any acceptor of the quorum dropped, the seal end counts each
  dropped range as voted, and the next slot of the leader is above them. The read watermark of a
  `PreReadAck` covers the dropped ranges too. A crash before the batch is durable brings the votes
  back from the disk (`compact_g1`).
- **A laggard jumps.** A replica asked for a slot below its point (`Pull`) answers `TrimmedTo`
  (its `Trim`) and then the slots above. A replica whose front is below that point and inside its
  segment takes it: it writes the `Trim`, and its fold starts there, blind until it applies the
  `Truncate` again.
- **A leader whose segment a later leader sealed stops**, and so does every round of a retired
  segment: a leader answers `Unknown` for each held command in such a segment, since the acceptors
  of a retired segment stop and its replicas drop their copies, and its resends would wait for
  ever.

`journal-sim`'s roles walk judges the compaction with the oracle `COMPACT` (each `Compact` is at or
below the trims of `F + 1` replicas of the set, and every dropped slot is chosen; mutant
`ForgedCompacted`), `AGREE` (mutant `DroppedHidden`: Phase 1 without the dropped ranges) and
`PROXY`, which counts the votes that a compaction dropped. A walk move makes one replica lose its
messages while a `Truncate` cuts a write in flight, so the walk reaches a jump into a blind fold;
the trace check binds `compact`, `compact_replica`, `jump` and the blind `apply` to
`specs/tla/journal-ctrl/RolesTrace.tla`.

### J3.3 A leaderless read of the tail across a reshape

**A leaderless read of the tail across a reshape** (#1254 (leaderless reads across a reshape),
checked on 2026-11-20 in `specs/tla/experiments/segments/reads/`). A reader that holds the record
of an old segment asks the old grid. With no fence, its answers need not see a slot that the new
grid chose, and the read is stale (controls `part_bare` on the partitioned grid, `bare` on the
full grid). The fence of the code closes it: each `PreReadAck` carries the read fence of the
acceptor (`wire_read_fence`: the higher of its promise and its belief ballot), and an answer above
the record ends the read. On the partitioned grid (the default grid, §J5.1) two Phase-1
quorums meet, so the fence alone passes (`part_fence`); on a full grid it also needs the seal of
each row (decision 31, §J2.5, #1209; `fence_rows`). The record check that the journal design
proposed (the reader also reads the segment table at a matchmaker quorum, rule 1 of #1254) adds
nothing while each acceptor keeps its promise. It adds safety only when an acceptor of a retired
segment loses its state and answers as an empty store (`part_wipe_fence` and `wipe_fence_rows` fail
with the promise alone); the belief half of the fence, with the refusal of an acceptor that did
not hear what is in force, closes that case too (`part_wipe_belief`, `wipe_belief` pass).
**Decided** (Pierre, 2026-11-20, #1254): the journal keeps the fence of the code, both halves
(the promise and the belief ballot), with the refusal of an acceptor that did not hear what is in
force and, on a full grid, the seal of each row. A read does not read the segment table at a
matchmaker quorum. The journal state model (`journal-state/`, #1189) has no leaderless read: the
rule stays checked by `reads/` and `experiments/segments/reads/`. Across a Phase-1 bypass
(#579) the promise fence alone is not enough, because the old acceptors never see the new ballot
(#1258 (reshapes with a column below quorum), PR #1283). Since a read has no record check, the
bypass closes that case with its own hold, §J5.5: the bypassing leader sends no `Accept` at its new ballot until its promises meet every read quorum of the old grid.

The rejected design is in architecture §11 (alternatives). The code follows these models.

### J3.4 The journal's matchmaker register

*Moved here from `docs/architecture/02-1-journal-register.md` §2.16 (#1332 (journal architecture docs)).*

This section lists every record that the matchmakers of a journal hold, who writes each one, the
guard of each compare-and-set, and the TLA+ model that checks each rule (#1315 (bring docs and
models in step with the v1 scope); Pierre agreed on 2026-11-20 to this page). The rules are in
§J3.1 to §J3.3, §J5.1, §J5.5 and §J6.1; this section collects them in one place. The journal state model
(`journal-state/`, #1189) holds the seal end, the boundary state, `retired` and `trimmed` of each
segment, and the configuration that each ballot registers; it leaves out the controller term and
the matchmaker set handover (`grid-controller/`, `segment-records/`). The types of the segment
record, its states and the guard of each compare-and-set of this section are
`paros_journal::segment` (#1324 (paros-journal)); the register that holds them is
`paros_journal::register` (#1325 (the matchmaker register as an optional role)).

#### What the register is

**Each journal has its own matchmaker set** (decided by Pierre on 2026-11-19, in the journal
design): `2F + 1` matchmakers with majority quorums. The sets are logical: one matchmaker process
hosts the sets of many journals, keyed by `JournalIdentifier` (architecture §3.4).

**The set holds one compare-and-set register for the journal.** It is the register of
`specs/tla/cas-register/` (FoundationDB's `LocalGenerationReg`): an operation reads at a majority
of one matchmaker generation, checks its guard on the value of the highest write, and writes at
that generation. **An operation is done when a majority took its write.** Every message is
fenced by the matchmaker generation. A handover of the matchmaker set carries the register: the
successor takes the highest write of the stop quorum and the highest read generation, and a
frozen member takes no write (`specs/tla/segment-records/`, controls `close_one`,
`close_one_term`, `frozen_serves`; §J3.1).

**The register is an optional role** (decided by Pierre on 2026-11-20, #1315, decision 1). The
registration matchmaker of the Matchmaker Paxos paper stays in `paros-consensus` for the users
of the library (architecture §4.3), with what it builds today. The register of the journal is a second
role beside it, which a caller opts into.

**The register lives in `paros-journal`, by composition** (#1325 (the matchmaker register as an
optional role)). `paros_journal::register` builds it from the core's types and roles, and the core
does not change: `paros-consensus` gets no message, no `WriteOp` and no field for it, and its
registration matchmaker and its handover (`matchmaker/reconfigurer.rs`) stay as they are. The
module has five parts:

- `value`: the value of the register (`RegisterValue`: the controller term and the records) and
  each compare-and-set of the table below as a `Change` with its guard (`RegisterValue::apply`).
  A refusal (`ChangeRefusal`) changes nothing.
- `message`: the requests and the replies between an operation, a handover and the members. Every
  message names the matchmaker generation.
- `member`: one member of a set (`RegisterMember`). It raises its read generation on a read, takes
  a write only if its read generation is at most the operation and its write is below it, freezes
  on a stop, stores a proposed successor durably, and votes in the decree. The decree half is the
  core's `Acceptor` on one slot. Each step gives the state to persist before its reply leaves.
- `call`: one operation (`RegisterCall`). It reads at a majority of one generation, applies its
  change to the highest write, and writes at that generation. It is done at a majority of writes.
  A conflict, a refusal and a later set each end it.
- `handover`: the handover of the set (`RegisterHandover`). It stops a majority of the old set,
  builds the successor from the highest write and the highest read generation of that majority,
  stores it at a majority of the next set, chooses it with the core's `Decree` over the old set,
  then publishes it. The decree opens above the round on the caller's disk and above every
  promise that the stop majority reported.

A caller acts on a write that its own call saw done, never on a read (§J3.4 below, #1275). The
rework steps 2.3 to 2.5 (architecture §7), which turn the core's matchmaker into a register, are
not done by this module. They stay **Open** until they are restated, as architecture §7 says.
`journal-state/` (#1189) holds the register as one value, and does not change.

**The register has its walk and its trace check** (`crates/sim/journal-sim`, §J7.3). The walk
runs operations of every change and handovers of the set, with crashes and restarts, and its
oracles check that every later serving majority holds a done write (`CARRIED`) and that a step
acts only on a value that every serving majority holds (`ACTED`). The traced walk is a trace of `specs/tla/segment-records/RegisterTrace.tla`,
which extends `SegmentRecords.tla` and defines no step of its own.

#### The records

The register holds the controller term and one record for each segment, from the reshape that
opens the segment to its trim. A segment is the run of slots of one configuration (section
2.10).

| Field | Written by | When, and its guard | Checked by |
|---|---|---|---|
| Controller term and its holder | a controller candidate | The candidate reads the term `t`, then writes `t + 1` by compare-and-set on `t`. Of two candidates that read `t`, one loses | `grid-controller/` (`Raise`; control `elect_noraise`); `segment-records/` (`control`; across a handover, `close_one_term`) |
| New segment: its grid, in state `pending` | the grid controller, by `reshape(expected configuration, new grid, term)` | The term is the term of the register (`TermCheck`); the expected configuration is the newest of the table (`ExpCheck`); the newest configuration is in force and every older one is retired (`RetireGuard`); the grid is a v1 grid (`BoundGuard`, below); a call that the register already took gets "ok" again (`Idempotent`) | `grid-controller/` (controls `noterm`, `noexp_noterm`, `noretire`, `nobound`, `live_nobound`, `noidem`); `segment-records/` (`control`) |
| Identity of the opening call: term, expected configuration, grid | the grid controller, in the same write | Written once, with the new segment. The register compares a retry with it | `grid-controller/` (`noidem`, `noexp_noidem`) |
| Release number of the segment | the writer of the new segment | Written once, with the new segment. A role refuses a release it does not know (upgrade minimum of v1, Pierre, 2026-11-19, journal design) | no model: the field enters no safety rule |
| Replica set of the segment | the grid controller, in the reshape call that opens the segment: the call names the grid and the replica set of the new segment, in the same write. A heal of a sealed segment changes it later (decision 4 of §J1.1) | Written with the new segment, under the guards of the reshape call. A change of a sealed segment's set (a heal, §J3.1): by the grid controller, a compare-and-set on the register version that it read (and on its term), only from `retired`, removing a member and adding a new replica whose own report says its copy, from a full member of the set, reached the segment end (#1314) | `journal-state/` (each segment's set comes with its reshape; `retire_early`, `Readable`); `segment-replicas/` checks a fixed set; `re-replication/` checks a change (`count_start`, `copy_any`, `live_outside`, `cas_term`, `cas_blind`, `report_sticky`) |
| Highest bound ballot and its segment | a leader, before its Phase 1 | `Bind(ballot)`: the ballot is above the bound ballot, which binds it to the newest segment, or it is the bound ballot and the newest segment is the one it is bound to ("ok" again, no change). The leader takes its configuration and its Phase 1 from the value of this done write | `journal-state/` (`Register`); `journal-read/` (control `bind_stale`); `register/value.rs` tests |
| Seal end of the predecessor; the predecessor becomes `sealed` and the new segment `in force` | the first leader of the new configuration, after its Phase 1 over the old and the new grid | Written once, by compare-and-set from "no end". The end is one above the highest slot where its Phase 1 saw a vote in the old grid. A CTRL faulty vote counts, so the end is the frontier of that Phase 1 over the old grid (#1313 (the seal end against a CTRL faulty record), §J3.1) | `experiments/segments/a/` (the end, `end_uncommitted_*`); `seal-ctrl/` (`end_readable`, `wait_cover`, `end_readable_cover`); `segment-records/` (`seal`, `SealOnce`); `grid-controller/` (`SealIn`) |
| Boundary state of a sealed segment: `next_seq`, `first_seq` and the writer uuid | the first replica of the segment's set that applied every slot of the segment | Written once, by compare-and-set from "no boundary", in one write with all three fields. The value is the fold at the end of the segment. A replica of the next segment starts from the record of the segment just before it, never from the newest record | `segment-replicas/` (`boundary_early`, `boundary_seq`, `retire_norecord`); `segment-truncate/` (`nofirst`, `trimstate`); `seal-chain/` (`record_latest`, `record_reach_empty`) |
| `retired` | the leader | From `sealed`, once `F + 1` replicas of the set hold the segment and the boundary state is written. A found damage that is not repaired also blocks it (a margin, Pierre, 2026-11-20, #1263). The old acceptors stop only after this write is done | `segment-replicas/` (`retire_early`, `retire_norecord`); `segment-records/` (`seal`, `RetireKept`); `ctrl-segments/` (`retire_f`, `live_retire_f`; the margin: `retire_any` passes) |
| `offloaded` | the offloader (after v1) | From "not offloaded and not trimmed". In v1 the state and the read path that checks it exist, and nothing writes it (§J3.1) | `offload/`; `segment-records/` (`offload`, `drop_read`); `ctrl-segments/` (`upload_any`, `drop_retired`) |
| `trimmed` | a replica of the segment's set whose fold has `first_seq` at or above the last `seq` of the segment, by compare-and-set (#1189) | From `sealed`, `retired` or `offloaded`, once a replica's fold has `first_seq` at or above the last `seq` of the segment. Never from the open segment, never on the intent to truncate, and a trimmed record never becomes live again | `segment-truncate/` (`trim_open`, `trim_intent`, `TrimStays`); `journal-state/` (`trim_open`, `TrimStays`, `reach_trim_retired`) |

**The v1 grids** (decided by Pierre on 2026-11-20, #1315, decision 5). The register takes only
the partitioned grid: `rows = 2F + 1`, `acks = F + 1`, with one or more columns. The majority is
the case of one column. `grid-controller/` checks the wider bound
`F + 1 <= acks <= rows - F` (§J5.1); a v1 grid is inside it, so the model covers v1.

**What the register does not hold.** The configuration register of `config-register/` and
`handover/` (the configurations of the core's rule 1, architecture §4.1) is not modelled together with
the segment records with its handover. `journal-state/` (#1189) holds the configuration of each
ballot and the segment records as one register, with no handover.

#### The events of a segment, and the one meaning of "in force"

A segment goes through these states, in this order. Each step is one compare-and-set above.

| State | Entered by | Meaning |
|---|---|---|
| `pending` | the reshape call | The configuration is announced. No leader writes in it yet |
| `in force` | the seal end of the predecessor | A leader of this configuration chose its ballot and sealed the predecessor. Leaders write here. A Phase 1 still reads every slot of the predecessor until the predecessor retires |
| `sealed` | the seal end, written by the first leader of the next configuration | No new slot. The slots below the end may still be written again in the new grid, and the replicas may still apply them |
| `retired` | the leader's write, after `F + 1` holders and the boundary state | The old acceptors stop. The next segment never needs these slots |
| `offloaded` | the offloader's mark (after v1) | The object holds the segment. A replica may drop its copy |
| `trimmed` | the trim | The segment is below the trim floor. Its object is deleted |

**"In force" has one meaning in the journal: the state that the seal end of the predecessor
gives** (`GridController.tla`, `SealIn`: one step makes `g - 1` sealed and `g` in force). One
configuration is in force, and at most one older configuration is not retired (`OneUnretired`).
architecture §4.1 uses the words for rule 1 of the core: there, the **switch** puts a successor in
force once its leader holds every recovered slot at a Phase-2 quorum of it, and a removed node
stops when the switch is durable. In the journal, the step that lets the old acceptors stop is
the **retire**, not the seal: the seal end makes the new configuration in force, and a later
Phase 1 still reads every slot of the old grid, so a slot that the old grid chose is never lost
before the retire (`experiments/segments/a/`). The leaderless read (§J5.1, "the
configuration in force") and the belief fence (§J3.1, "an acceptor that did not hear what
is in force") use the journal meaning.

#### Act on a write seen done, never on a read

**A step that acts on a record acts on a write of that record that it saw done at a majority,
never on a read alone** (#1275 (segment records across a matchmaker handover),
`specs/tla/segment-records/`, control `drop_read`). A read can show a write that only a minority
took; another majority, or the successor of a handover, can then return an older value. These
steps depend on the rule:

- **The old acceptors stop** only after the write of `retired` is done (`RetireKept`).
- **A replica drops its copy** only after a write of `offloaded` is done: the offloader's own
  compare-and-set, after which it tells the replicas, or the replica's own compare-and-set of the
  same value (`DropKept`, control `drop_read`).
- **The seal end** is written once: two first leaders that race to seal write one end, and
  every serving majority returns that end or none (`segment-records/`, `seal`, `SealOnce`).
- **A replica of the next segment** starts from the boundary state in the record of the segment
  just before it. **It may act on a boundary write that is still partial** (`journal-state/`,
  `register_bound_read` passes): the boundary state is a function of the chosen log up to a
  done end, so every write of it carries the same value. **The end is the step that must be done**:
  a replica that stops, or a leader that opens, at a partial end can write the boundary state at
  the wrong end, when another sealer then writes a different end that is done (control
  `end_read`, `RecordTrue` fails). So a replica stops at an end, and a leader opens its segment,
  only on an end write seen done.
- **A controller** acts on its own election write and its own reshape call. A refusal for the
  term tells it to stop (`grid-controller/`).

#### A change asked again

**A change that the value already holds is "ok" again, and the operation writes the value
again** (2026-11-24, found by #1510 (full journal examples); `specs/tla/segment-records/`, checks
`again` and `again_retire`, control `again_refuse`). A caller whose answer was lost (a lost
request, a lost reply, or a write that only a minority took) asks the same change again. Its
first write may be at a minority, and a later operation that read it may have carried it on
under its own change: a replica's boundary state over a seal, a bind, an election. The second
ask reads that write. If the guard refused it there, the caller would take its own write for a
loss: a first leader would stop for good on its own seal end, and a leader would ask a retire for
ever. So each of these changes is "ok" on a value that holds it, and the write of the value
again makes the first write done:

- **`Seal`** of the end that the record holds, in any later state of the record.
- **`Boundary`** of the boundary state that the record holds.
- **`Retire`** of a retired record, when its own guard holds (`F + 1` holders, no damage).
- **`Trim`** of a trimmed record, when its floor is at or above the last `seq`.

`Reshape` and `Rereplicate` were already "ok" again (their guards); a `Bind` of the bound
ballot is "ok" again while the newest segment is the one it is bound to, and refused once a
later segment opened, which stops the leader as it must; `Elect` is never refused, and `Confirm`
writes the value again by its definition. A sealing leader that sees its own segment sealed by a
later leader stops, whatever the end of the segment before. A refusal of a change
asked again so says that the value holds another change: a first leader stops only on another
end (`SealRefusedOnOther`), and a leader stops asking a retire only on a record that is not
retired (`RetireRefusedOnOther`, check `retire_again`, control `retire_again_refuse`). The rule changes no done value: the second ask writes the value that
the first write gave, so `SealOnce`, `Carried` and `NoBackward` hold as before. In
`journal-state/`, the second ask is the step `EndDone` (or `BoundDone`) that the model already
has. `journal-sim`'s whole walk runs the register on three matchmakers whose write one member
can take, and judges every refusal of a change asked again (`AGAIN`, mutant `RefuseAgain`).

#### Quorum everywhere

Each write of the register waits for a matchmaker majority only (§J5.1,
`specs/tla/quorum-everywhere/`). The model has a row for the seal, the retire, the offload mark,
the trim, the controller election, the reshape call and the boundary write. The boundary write
waits for one live replica of the old set that applied the segment, then for a matchmaker
majority (`boundary`): `F + 1` matchmakers down block it (`boundary_mm`), and it still completes
with `F + 1` replicas of the set down while one lives (`boundary_replica`; #1318
(quorum-everywhere rows for the boundary write and the acceptor compaction)).

#### Matchmakers below a majority

**When the matchmakers of a journal are below a majority while a reshape is in flight, acked
writes stop** (accepted for v1 by Pierre on 2026-11-20, #1315, decision 3). Between the seal end
and the boundary write, the new segment chooses slots, but no replica of its set can fold them
before the boundary state of the predecessor is written, so no write is acked and no tail read is
served until a matchmaker majority is back. This is more than `F` matchmaker failures, outside
the promise of §J5.1. With no reshape in flight the data plane serves under the standing
leader (§J6.2, ControlFrozen). v1 opens a segment only on a reshape (§J1.1), so the
window comes only with a heal.
