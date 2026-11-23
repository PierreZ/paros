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
| `register.rs` (+ `register/`) | the journal's matchmaker register, an optional role by composition: `value` (`RegisterValue`, each `Change` with its guard), `message`, `member` (`RegisterMember`, the decree half on the core's `Acceptor`), `call` (`RegisterCall`, one compare-and-set at a majority), `handover` (`RegisterHandover`: stop, close, bootstrap, the core's `Decree`, publish) | §J3.4 (#1325) |
| `read.rs` | `ReadAnswer`, the answer of a leaderless read (a page, `Truncated`, `Below`, `Superseded`, `Unserved`), shared by the replica and the client | §J2.5, §J3.3 |
| `roles.rs` (+ `roles/`) | the journal roles under the flight rule (#1326): `Flight` and `PersistId`; `message` (the messages between roles); `acceptor` (`JournalAcceptor`, the core's `Acceptor` over `Command`, with the read fence of `PreRead`); `leader` (`JournalLeader`: Phase 1 over the segments in `leader/phase1.rs`, the seal, the gap fill, Phase 2 in one column with the core's `Rounds` or through a proxy, the held answers, the leader time of each write at propose); `proxy` (`JournalProxy`, the proxy leader); `replica` (`SegmentReplica`: base, apply in order, boundary write, pull, `Resolve`, the trim of a retired segment under its floor; `replica/read.rs`, the leaderless read) | §J4.1, §J4.2, §J3.1, §J3.2, §J2.8 |
| `client.rs` | the pure journal client (#1331): `Client` with the four calls, `Request`, `Response`, `Outcome`, `Mode`; one call in flight; exactly-once for a single writer through `expected_seq`, `Unknown` and lost answers settled by an identical resend, `Below` by `Resolve`, `Truncated` final; each read takes its floor from the segment in force first (§J3.2); at-least-once unfenced; a lost `SetLeader` that won (to a uuid or to a tagged multi-writer mode) settled by its resend; the next position follows each view that names the client's uuid. Walked by `journal-sim`'s client walk | §J2.2, §J2.3, §J2.4, §J3.2 |
| `codec.rs` | the bytes of each type, the release number in every header, `DecodeError` | §J1.1, §J2.8 |

Each module keeps its unit tests in `<module>/tests.rs`.

`examples/` (#1370 (journal examples)): `fold` (the fold alone) and `register` (the
compare-and-sets of the matchmaker register, with their guards). Shared code is in
`examples/common/mod.rs`. Each example asserts its result and runs in the `checks` job of CI;
`README.md` lists them in reading order. An example uses no crate from outside the workspace
(`scripts/check-core-deps.sh`). The examples that run the roles (`in_memory`, `custom_storage`) and
the client (`single_writer`, `multi_writer`) are in `paros-journal-io`, on its traits and its driver: a change to a role's effects or its disk
updates `paros-journal-io` (its `Host` and its `layout`) and those examples in the same PR.

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
- **The register adds nothing to the core.** It composes the core's `Acceptor`, `Decree`,
  `Ballot` and `MatchmakerSet`; the core's registration matchmaker and its handover do not change
  (§J3.4). Each member step names the state to persist before its reply leaves, and a caller acts
  only on a write its call saw done, never on a read.
- **No dedup table**: single-writer exactly-once comes from `expected_seq` alone.
- **Tiger Style assertions**, as in `paros-consensus`: hard `assert!`, on in release, pre- and
  postconditions, paired checks, `# Panics` sections. Never assert on what a client or a peer can
  send: it is an answer (`Empty`, `Reserved`, `WrongMode`) or a `DecodeError`.
- **The codec refuses what it does not know**: an unknown release, kind, tag or paros header key
  is an error, never a guess. A new field is a new release.
- **A role is pure and paced by its driver**: it emits each persist batch with an id and names
  on each send the batch it waits on; the driver chooses the re-send page (`tick(page)`) and the
  promise page. It acts on a register value only when a done write gave it (§J3.4).
- **Every rule has its walk.** A rule added here gets its oracle, its control and, when the model
  has it, its trace events in `crates/sim/journal-sim` in the same PR.

## Gates

`cargo nextest run -p paros-journal -p journal-sim`, `cargo run -p paros-journal --example fold`
and `--example register`,
`cargo clippy --all-targets -- -D warnings`,
`RUSTDOCFLAGS="-D warnings" cargo doc -p paros-journal --no-deps` and
`cargo check --target wasm32-unknown-unknown -p paros-journal`, all in `nix develop`.
