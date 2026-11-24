# paros-election

The coordination store over a journal (#1420 (paros-election), Pierre 2026-11-24: "I want the
leader election library to own a journal and write data to it, inspired from zookeeper client";
option B of `/mnt/project-files/design/election-zookeeper-2026-11-24/proposal.md`, "the log is
the server"). Every rule has its home in `docs/architecture/04-roles.md` §4.4, "The coordination
store", and its TLA+ model in `specs/tla/coord/`. Read the section before you change the code,
and change the section in the same PR when a rule changes.

## Map

| Module | What it holds |
|---|---|
| `path.rs` | `Path`: the root, names, the parent, the sequential name of a position |
| `entry.rs` | `SessionId`, `Term`, `Op`, `Check`, `Txn`, `Entry` (`Open`, `Txn`, `Expire`, `Checkpoint`) |
| `tree.rs` | `Tree`, the fold: `apply` judges each record at its position; `Fate`, `Change`, `Turn`, `TxnError`; `from_checkpoint`; `is_consistent` for a decoded state |
| `codec.rs` | the bytes of an entry in one journal record, marked by the client header `HEADER_KEY` |
| `session.rs` | `Member`: one session over one `paros_journal::client::Client`; tail, one entry in flight, heartbeats, expiries, checkpoints, trims; `Event` |
| `recipes.rs` (+ `recipes/`) | `Latch`, `Group`, `Counter`, `ensure` |

Each module keeps its unit tests in `<module>/tests.rs`. `examples/latch.rs` (with
`examples/common/mod.rs`, a journal in memory) runs in the `checks` job.

## Rules

- **The fold is the store.** A member settles its entry only from its own fold, never from the
  journal's answer: the journal is at-least-once, and the fold applies the first copy only.
- **What a peer writes is data.** A record that is not an entry, an entry that does not decode,
  a checkpoint whose state is not the fold's: each is void, the same on every member. Never
  assert on them.
- **Pure.** No clock, no random number, no runtime: the program gives the time and the draws.
  `scripts/check-core-deps.sh` and `scripts/check-layers.sh` (layer 2) hold it.
- **A rule has its model and its walk.** A rule of the fold gets its TLA+ check in
  `specs/tla/coord/` (with a control) and its oracle in `journal-sim`'s election walk (with a
  mutant planted in the walk) in the same PR.

## Gates

`cargo nextest run -p paros-election -p journal-sim -E 'binary(election) | test(election)'`,
`cargo run -p paros-election --example latch`, `cargo clippy --all-targets -- -D warnings`,
`RUSTDOCFLAGS="-D warnings" cargo doc -p paros-election --no-deps` and
`cargo check --target wasm32-unknown-unknown -p paros-election`, all in `nix develop`.
