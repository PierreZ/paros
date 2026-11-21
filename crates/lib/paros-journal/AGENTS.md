# paros-journal

The pure core of the journal v1 (#1324 (paros-journal pure core), step 3 of
`docs/journal/08-build-steps.md`). It holds the parts of the journal that are pure data and pure
functions: no I/O, no clock, no randomness, no dependency but `paros-consensus` with its features
off. It builds for `wasm32-unknown-unknown`. Every rule here has its home in `docs/journal/`
(§J numbers) and a TLA+ model in `specs/tla/journal-state/`. Read the section before you change
the code, and change the section in the same PR when a rule changes.

## Map

| Module | What it holds | Rule |
|---|---|---|
| `command.rs` | `Leader`, `Fence`, `Record` with its `Headers` (the paros section: leader time; the client section: opaque key-values), `Write`, `Command` (`Noop`, `Write`, `Truncate`, `SetLeader`, `Upgrade`) | §J2.1, §J2.8 |
| `fold.rs` | `JournalState`, the fold of one segment judged at apply; `BoundaryState`; `JournalView`; `Answer` | §J2.2, §J2.4.1, §J2.6 |
| `resolve.rs` | the read-only `Resolve` call that settles a `Below` answer | §J2.2 (#1320) |
| `held.rs` | `Held`: the answers a leader holds until the slot is applied; `Unknown` for a slot that chose another command | §J2.2 (#1232) |
| `segment.rs` | `SegmentRecord` and its states, `Grid`, the guard of each compare-and-set of the register | §J3.4 |
| `codec.rs` | the bytes of each type, the release number in every header, `DecodeError` | §J1.1, §J2.8 |

Each module keeps its unit tests in `<module>/tests.rs`.

## Rules

- **Journal only.** The core is not generic over the command (Pierre, 2026-11-20, §J4.3): the
  journal fold lives here, not in `paros-consensus`. This crate uses the core's identifiers
  (`Seq`, `LeaderUuid`, `NodeId`, `Slot`) as data only and adds no concept to the core.
- **The fold is judged at apply, in slot order, with no term.** A write's position is judged
  before its fence, so a resend of a held write answers `Duplicate` or `Taken` and never
  `Fenced` (§J2.4.1). A position below the start of the segment in force answers `Below`; the
  client settles it with `Resolve` at a replica of the old segment.
- **The identity of a write is its fence, its client headers and its payloads.** The leader time
  is not part of it: a resend with a new leader time is the same write.
- **No dedup table**: single-writer exactly-once comes from `expected_seq` alone.
- **Tiger Style assertions**, as in `paros-consensus`: hard `assert!`, on in release, pre- and
  postconditions, paired checks, `# Panics` sections. Never assert on what a client or a peer can
  send: it is an answer (`Empty`, `Reserved`, `WrongMode`) or a `DecodeError`.
- **The codec refuses what it does not know**: an unknown release, kind, tag or paros header key
  is an error, never a guess. A new field is a new release.
- **Every rule has its walk.** A rule added here gets its oracle, its control and, when the model
  has it, its trace events in `crates/sim/journal-sim` in the same PR.

## Gates

`cargo nextest run -p paros-journal -p journal-sim`, `cargo clippy --all-targets -- -D warnings`,
`RUSTDOCFLAGS="-D warnings" cargo doc -p paros-journal --no-deps` and
`cargo check --target wasm32-unknown-unknown -p paros-journal`, all in `nix develop`.
