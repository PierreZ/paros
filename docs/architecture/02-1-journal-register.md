[← 02-data-plane.md](02-data-plane.md) · [Index](README.md) · [03-1-cell-tenant-and-bootstrap.md →](03-1-cell-tenant-and-bootstrap.md)

### 2.16 The journal's matchmaker register

This page lists every record that the matchmakers of a journal hold, who writes each one, the
guard of each compare-and-set, and the TLA+ model that checks each rule (#1315 (bring docs and
models in step with the v1 scope); Pierre agreed on 2026-11-20 to this page). The rules are in
sections 2.10 to 2.13; this page collects them in one place. The journal state model (#1189)
takes the register as one variable, with the fields below.

#### What the register is

**Each journal has its own matchmaker set** (decided by Pierre on 2026-11-19, in the journal
design): `2F + 1` matchmakers with majority quorums. The sets are logical: one matchmaker process
hosts the sets of many journals, keyed by `JournalIdentifier` (section 3.4).

**The set holds one compare-and-set register for the journal.** It is the register of
`specs/tla/cas-register/` (FoundationDB's `LocalGenerationReg`): an operation reads at a majority
of one matchmaker generation, checks its guard on the value of the highest write, and writes at
that generation. **An operation is done when a majority took its write.** Every message is
fenced by the matchmaker generation. A handover of the matchmaker set carries the register: the
successor takes the highest write of the stop quorum and the highest read generation, and a
frozen member takes no write (`specs/tla/segment-records/`, controls `close_one`,
`close_one_term`, `frozen_serves`; section 2.10).

**The register is an optional role** (decided by Pierre on 2026-11-20, #1315, decision 1). The
registration matchmaker of the Matchmaker Paxos paper stays in `paros-consensus` for the users
of the library (section 4.3), with what it builds today. The register of the journal is a second
role beside it, which a caller opts into. The handover (`matchmaker/reconfigurer.rs`) must then
carry the value of this register too. **Open** (#1189 decides): whether the register role lives
in `paros-consensus` as an optional role or in a journal crate that composes the handover of the
core; and how the rework steps 2.3 to 2.5 (section 7), which turn the core's matchmaker into a
register, change with this decision.

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
| Replica set of the segment | **Open** (#1189): the reshape call of section 2.12 names a grid only, and the journal v1 plan lists the replica set as a field of the record. A re-replication of a sealed segment changes it (decision 4 of section 2.15) | **Open**: the guard of a change of the set is the result of #1314 (re-replication of a sealed segment's replica set) | `segment-replicas/` checks a fixed set; #1314 (running) checks a change |
| Seal end of the predecessor; the predecessor becomes `sealed` and the new segment `in force` | the first leader of the new configuration, after its Phase 1 over the old and the new grid | Written once, by compare-and-set from "no end". The end is one above the highest slot where its Phase 1 saw a vote in the old grid. What a faulty record of CTRL counts for is the result of #1313 (seal end against a faulty record) | `experiments/segments/a/` (the end, `end_uncommitted_*`); `segment-records/` (`seal`, `SealOnce`); `grid-controller/` (`SealIn`) |
| Boundary state of a sealed segment: `next_seq`, `first_seq` and the writer uuid | the first replica of the segment's set that applied every slot of the segment | Written once, by compare-and-set from "no boundary", in one write with all three fields. The value is the fold at the end of the segment. A replica of the next segment starts from the record of the segment just before it, never from the newest record | `segment-replicas/` (`boundary_early`, `boundary_seq`, `retire_norecord`); `segment-truncate/` (`nofirst`, `trimstate`); `seal-chain/` (`record_latest`, `record_reach_empty`) |
| `retired` | the leader | From `sealed`, once `F + 1` replicas of the set hold the segment and the boundary state is written. A found damage that is not repaired also blocks it (a margin, Pierre, 2026-11-20, #1263). The old acceptors stop only after this write is done | `segment-replicas/` (`retire_early`, `retire_norecord`); `segment-records/` (`seal`, `RetireKept`) |
| `offloaded` | the offloader (after v1) | From "not offloaded and not trimmed". In v1 the state and the read path that checks it exist, and nothing writes it (section 2.10) | `offload/`; `segment-records/` (`offload`, `drop_read`) |
| `trimmed` | **Open** (#1189 names the writer) | From `sealed`, `retired` or `offloaded`, once a replica's fold has `first_seq` at or above the last `seq` of the segment. Never from the open segment, never on the intent to truncate, and a trimmed record never becomes live again | `segment-truncate/` (`trim_open`, `trim_intent`, `TrimStays`) |

**The v1 grids** (decided by Pierre on 2026-11-20, #1315, decision 5). The register takes only
the partitioned grid: `rows = 2F + 1`, `acks = F + 1`, with one or more columns. The majority is
the case of one column. `grid-controller/` checks the wider bound
`F + 1 <= acks <= rows - F` (section 2.11); a v1 grid is inside it, so the model covers v1.

**What the register does not hold.** The configuration register of `config-register/` and
`handover/` (the configurations of the core's rule 1, section 4.1) is not modelled together with
the segment records: no model holds both. The journal state model (#1189) holds them as one
register.

#### The events of a segment, and the one meaning of "in force"

A segment goes through these states, in this order. Each step is one compare-and-set above.

| State | Entered by | Meaning |
|---|---|---|
| `pending` | the reshape call | The configuration is announced. No leader writes in it yet |
| `in force` | the seal end of the predecessor | A leader of this configuration chose its ballot and sealed the predecessor. Leaders write here. A Phase 1 still reads the predecessor below its end until the predecessor retires |
| `sealed` | the seal end, written by the first leader of the next configuration | No new slot. The slots below the end may still be written again in the new grid, and the replicas may still apply them |
| `retired` | the leader's write, after `F + 1` holders and the boundary state | The old acceptors stop. The next segment never needs these slots |
| `offloaded` | the offloader's mark (after v1) | The object holds the segment. A replica may drop its copy |
| `trimmed` | the trim | The segment is below the trim floor. Its object is deleted |

**"In force" has one meaning in the journal: the state that the seal end of the predecessor
gives** (`GridController.tla`, `SealIn`: one step makes `g - 1` sealed and `g` in force). One
configuration is in force, and at most one older configuration is not retired (`OneUnretired`).
Section 4.1 uses the words for rule 1 of the core: there, the **switch** puts a successor in
force once its leader holds every recovered slot at a Phase-2 quorum of it, and a removed node
stops when the switch is durable. In the journal, the step that lets the old acceptors stop is
the **retire**, not the seal: the seal end makes the new configuration in force, and a later
Phase 1 still reads the old grid below the end, so a slot that the old grid chose is never lost
before the retire (`experiments/segments/a/`). The leaderless read (section 2.11, "the
configuration in force") and the belief fence (section 2.10, "an acceptor that did not hear what
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
  just before it. **Not proved**: that this replica must act on a boundary write done at a
  majority, and not on a read alone. `segment-records/` has no boundary write, and
  `segment-replicas/` and `seal-chain/` hold the register as one atomic value. The journal state
  model (#1189) checks it, with `drop_read` as the control.
- **A controller** acts on its own election write and its own reshape call. A refusal for the
  term tells it to stop (`grid-controller/`).

#### Quorum everywhere

Each write of the register waits for a matchmaker majority only (section 2.11,
`specs/tla/quorum-everywhere/`). The model has a row for the seal, the retire, the offload mark,
the trim, the controller election and the reshape call. **Not proved**: the boundary write has
no row yet; it is one wait for a matchmaker majority after a replica applied the segment
(#1318 (quorum-everywhere rows for the boundary write and the acceptor compaction)).

#### Matchmakers below a majority

**When the matchmakers of a journal are below a majority while a reshape is in flight, acked
writes stop** (accepted for v1 by Pierre on 2026-11-20, #1315, decision 3). Between the seal end
and the boundary write, the new segment chooses slots, but no replica of its set can fold them
before the boundary state of the predecessor is written, so no write is acked and no tail read is
served until a matchmaker majority is back. This is more than `F` matchmaker failures, outside
the promise of section 2.11. With no reshape in flight the data plane serves under the standing
leader (section 5.1, ControlFrozen). v1 opens a segment only on a reshape (section 2.15), so the
window comes only with a heal.
