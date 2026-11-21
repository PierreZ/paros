[← 06-controller.md](06-controller.md) · [Index](README.md) · [08-build-steps.md →](08-build-steps.md)

## J7. Verification

**No code before its model** (#1323 (build the paros journal v1)). Each journal rule that code
builds has a TLA+ check in `specs/tla/`: a check that passes with the rule and a control that
fails without it (`AGENTS.md`, "When in doubt about safety, use TLA+"). The journal then has the
same three judges as the consensus core, on the same code: model-based trace checking,
deterministic simulation testing and coverage-driven simulation. The verification of the whole
service (rule 8, the quiescence oracle, the checks beside the simulation) is architecture §6.

### J7.1 The model of each rule

`specs/tla/README.md` is the index of every model, with how to run it, its checks and its
controls. This table maps each journal rule to its model. "Open" models are not merged.

| Rule | Section | Model | Issue |
|---|---|---|---|
| The flight rule of the sans-IO contract | §J4.2 | `flight-rule/` | #1255 (journal sans-IO contract) |
| Held writes: `Unknown`, never `NotLeader` (`AckedChosen`) | §J2.2 | `compartments/` | #1232 (held writes) |
| Multi-writer mode through `SetLeader(None)`, position before fence, the resend across a seal (`Below`) | §J2.2, §J2.4.1 | `multi-writer/` | #1274 (multi-writer mode), #1301 (a resent write across a seal) |
| Leaderless reads: the read fence on the promise and the belief ballot | §J2.5 | `reads/` | #1016 (model of leaderless reads), #1099 (rework 1.10), #1228 (stale read by a lagging reader) |
| The row seal of a full grid (not a v1 grid) | §J2.5 | `reads/` (`grid_full_seal`) | #1102 (rework 1.13), #1209 (seal each row of a full grid) |
| Truncation on plain Multi-Paxos: the floor, the trim-point jump | §J2.6 | `truncation/` | #1017 (truncation model), #1216 (truncation model, part 2) |
| Segments in the matchmakers, the seal end, the gap fill | §J3.1 | `experiments/segments/a/` | #1213 (segments in the protocol or in storage), #1233 (segments gap fill) |
| A write reaches only the replica set of its segment; retire at `F + 1` holders | §J3.1 | `segment-replicas/` | #1271 (a write reaches only the replica set of its segment) |
| The whole boundary state in the segment record; two seals in a row | §J3.1 | `seal-chain/` | #1303 (two seals in a row) |
| A user truncate across segments | §J3.2 | `segment-truncate/` | #1276 (user truncate across segments); #1321 (make `crash_all` and `live_all` finish) |
| A leaderless read of the tail across a reshape | §J3.3 | `experiments/segments/reads/` | #1254 (leaderless reads across a reshape) |
| Segment records across a matchmaker set handover; act on a write seen done | §J3.4 | `segment-records/` | #1275 (segment records across a matchmaker handover) |
| The register as a compare-and-set at a majority of one generation | §J3.4 | `cas-register/`, `handover/` | PR #1064 (gone-node gap), #1097 (rework 1.8) |
| The offloader and the races of offload (after v1) | §J3.1 | `offload/` | #1256 (offload races and the offloader role) |
| Quorum everywhere and the grid bound | §J5.1 | `quorum-everywhere/`, `grid/` | #1257 (quorum everywhere); #1318 (two rows **Not proved**) |
| CTRL on a grid and across segments | §J5.3 | `ctrl-grid/`, `ctrl-segments/` | #1263 (CTRL recovery in the journal) |
| The seal end against a faulty record: a faulty vote counts | §J3.1, §J5.3 | `seal-ctrl/` | #1313 (the seal end against a CTRL faulty record) |
| Re-replication of a sealed segment | §J5.2 | open | #1314 (re-replication of a sealed segment's replica set) |
| A column below quorum, the bypass and its hold (after v1) | §J5.5 | `degraded-reshape/`, `zero-stall/` | #1258 (reshape with a column below quorum), #1018 (zero-stall model, more rules) |
| The controller's election and fenced reshape | §J6.1 | `grid-controller/` | #1259 (the fenced reshape call of the grid controller) |
| The health measures and the journal states | §J6.2 | `health/`; its v1 switch is #1319 (a v1 switch in health/) | #1260 (journal health measures) |

### J7.2 The composed model

**#1189 (the journal state model) composes the rules above** into one model: at least three
segments, the rules of chapters J2 to J6 with the control of each, and reach checks for the
seams between them (the consolidated v1 plan, section 8). Its state is the disk, and its
messages come from the disk (#1309 (the state is the disk)). It decides the points that these
files mark **Open** for #1189: the leader of a fresh journal (§J2.4.1), the wire shape of `Below`
with #1320 (the wire shape that resolves a `Below` answer) (§J2.2), the writers of the replica set and of `trimmed` (§J3.4), and that a replica
of the next segment acts on a boundary write seen done (§J3.4). It is step 2 of the build
(section J8), after this chapter and before any journal code.

### J7.3 `journal-sim`

**The journal gets its own walk crate**, `crates/sim/journal-sim`, beside `consensus-sim`
(Pierre, 2026-11-20). `consensus-sim` stays on `paros-consensus` only. `journal-sim` is built
with the steps of the journal core and the roles, in the same pull requests (#1327 (journal-sim)):

- Seeded random walks (moonpool-walk) over the journal's pure roles, with faults on: crashes that
  lose unsynced writes, disk rot inside the contract, message loss and reorder, leader changes,
  reshapes, seals, matchmaker handovers, retire, truncate, re-replication, and two controllers of
  different terms (#1261 (grid controller decision tests)).
- Safety oracles that share no code with the journal, and a planted mutant that each oracle
  catches.
- The trace check of the walks against #1189 with TLC (moonpool-tla); each trace spec names the
  steps that its checked walks must take.
- A coverage gate over every journal probe, with no excused probe, inside the CI budget of 30
  minutes per job (shards if needed; Pierre, 2026-11-20).

### J7.4 The whole journal under moonpool-sim

A moonpool-sim of the whole journal runs the driver, the adapter, many journals in one process,
disk faults inside the contract, machine and zone loss, the controller and clients that retry
(#1330 (paros-journal-moonpool)). Every input is recorded (§J4.2, rule 2), so a red run replays.
A contract suite certifies a backend's API only; crash safety is claimed only on a disk that can
inject faults (Pierre, 2026-11-19).

### J7.5 The controller's `decide`

Property tests, a second `decide` with no shared code, and the controller walks in
`journal-sim` (§J6.2, "Tests of the health measures and the decisions"; #1328 (paros-journal-control)).

### J7.6 Mutants

The `mutants` job of CI runs cargo-mutants on the changed lines of `paros-consensus` today
(`AGENTS.md`, "Build, test, gates"). **Open** (#1324 (paros-journal) decides): the mutants job of the journal
crates.

### J7.7 The absolute rule

**No exception in simulation** (`AGENTS.md`, the absolute rule, Pierre, 2026-11-19): no excuse,
no allow list, no skip, no weaker check, no retry. A failure of a walk is a finding: fix the
code, or fix the walk when it does a thing that the contract of the code does not permit. This
binds `journal-sim` and the moonpool-sim of the journal in full.
