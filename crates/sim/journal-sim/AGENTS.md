# journal-sim

Deterministic simulation testing of `paros-journal` (#1327 (journal-sim), built with #1324
(paros-journal pure core) in the same PR; `docs/journal/07-verification.md`). A seeded random
walk (moonpool-walk) drives the journal fold over a chain of segments: three writers, resends
across seals, `SetLeader` with `None`, truncates, crashes of the replica that refold its segment,
and the `Resolve` call that settles each `Below`. A second walk drives the journal's matchmaker
register across handovers of the set (#1325). A third walk drives the journal roles
(`paros_journal::roles`, #1326): acceptors, leaders and segment replicas with disks, over a chain
of segments. Test only: `publish = false`.

## Map

- `src/oracle.rs`: `Truth`, the fold of the whole log written again from
  `docs/journal/02-data-plane.md`, with no segment. It shares no code with `paros_journal::fold`.
- `src/fold.rs`: the walk (`FoldModel`), its shapes (`Shape::TRACED`, small enough for TLC;
  `Shape::WIDE`; `Shape::LONG`, the traced walk over 200 segments for the live check), its events
  port (`FoldModel::build_reported`, `Reported`), its oracles (`FOLD`, `VIEW`, `RESOLVE`, `REFOLD`), its reach sites and its
  planted mutants (`FoldMutant`).
- `src/register.rs` (+ `src/register/order.rs`): the walk of the matchmaker register
  (`RegisterModel`, #1325) across handovers of the set, its shapes (`TRACED`, `WIDE`), its oracles
  (`CARRIED`, `ACTED`), its reach sites and its planted mutants (`RegisterMutant`).
- `src/roles.rs`: the walk of the roles (`RolesModel`, #1326), its shapes (`NARROW`, the default
  grid; `WIDE`, two columns and promise pages of two), its oracles (`AGREE`, `SEALED`, `FOLD`,
  `VIEW`, `BOUNDARY`, `RESOLVE`; the truth is `Truth` over the chosen log), its reach sites and
  its planted mutants (`RolesMutant`). Its trace check against `Journal.tla` is not built yet.
- `src/campaign.rs`: the campaign, which owns the assertion table and judges the reach sites.
- `src/trace.rs`: the events of a walk as a trace of `specs/tla/journal-state/FoldTrace.tla`.
- `src/trace/register.rs`: the events of a register walk as a trace of
  `specs/tla/segment-records/RegisterTrace.tla`, and the named edits of its recorded traces.
- `src/trace/mutate.rs`: the named edits of recorded traces (#1308 (mutate recorded traces)).
- `tests/walks.rs`, `tests/mutants.rs`, `tests/traces.rs`; `tests/roles.rs` (the roles walks and
  their mutants); `tests/register.rs` (the register
  walks and their mutants) and `tests/register_traces.rs` (the register trace check, its rules,
  its control `ForgedBootstrap` and its edits); `tests/live.rs` (#1310 (check long runs live)): a
  long fold walk checked live through its events port by moonpool-tla's `Live`, and the control
  `BelowInSegment` stopped at its first wrong answer; `LIVE_MINUTES` runs one walk with no end for
  that long (the `Journal live` workflow, four hours each night).

## Rules

The rules of `crates/sim/consensus-sim/AGENTS.md` hold here too. In short:

- **No excuse, no allow list, no skip** (root `AGENTS.md`). A red seed is a finding.
- **An oracle shares no code with the code it judges.** `Truth` uses the journal's types as data
  only.
- **Every oracle has a control**: a `FoldMutant` (or a `RegisterMutant`) in the driver, and a
  test in `tests/mutants.rs` (or `tests/register.rs`) that the oracle catches it. A new oracle gets its mutant in the same PR.
- **Every reach site is reached** by the campaign of its shape; a site the walk does not reach
  gets a walk move, never a larger budget alone.
- **`FoldTrace` extends `Journal.tla` and defines no step of its own** (#1306 (trace specs extend
  the checked model)): each event applies the model's operators (`Res`, `Step`, `Bound`,
  `CoversOn`, `ResolveOn`). A step of the code that the model does not have becomes a step of the
  model, with its check, in the same PR.
- **`RegisterTrace` extends `SegmentRecords.tla`** in the same way: each event is an action of
  the model.
- **The trace check has a control** (`BelowInSegment`; `ForgedBootstrap` for the register: the
  oracles pass it, TLC refuses it) and **refuses the edits** of the recorded walks at the edited event or after it: at least 20 edits
  with a place. A new event kind gets its edits in the same PR.
- **The checked walks meet every rule** of `RULES` in `tests/traces.rs` and
  `tests/register_traces.rs` (a resend answered
  `Below`, a `Resolve` answered both ways, two seals, and more). A rule no walk meets fails the
  test.
- TLC must run: `require_tlc` fails the test when `tlc` is missing (the default nix shell has it).
