//! **consensus-sim**: seeded random walks over the sans-IO roles of
//! `paros-consensus`, with [`moonpool_walk`] (moonpool #554).
//!
//! There is no network, no disk and no clock. A seed draws a schedule: the
//! walk delivers, drops, duplicates and reorders messages, crashes a party
//! and restarts it from its disk, and makes the moves of the clients and
//! the clocks. After every step the oracles judge what the run did. A seed
//! is a run: the same seed gives the same walk
//! (`MOONPOOL_WALK_SEED=17 cargo nextest run -p consensus-sim`).
//!
//! | Part | Role |
//! |------|------|
//! | [`cell`] | the [`Cell`](cell::Cell) model: `ColocatedNode`s, proxy leaders and replicas of one journal, one [`Flavor`](cell::Flavor) per deployment |
//! | [`decree`] | single-decree Paxos (`Decree`) under the walk |
//! | [`batcher`] | the batcher's lanes and the unbatcher under the walk |
//! | [`oracle`] | the safety judges, written again from the papers |
//! | [`trace`] | model-based trace checking: a walk's steps checked by TLC against the flavor's TLA+ model |
//!
//! The walks themselves are the tests of `tests/`: one per flavor, and one
//! control per oracle that plants a bug in the driver and expects that
//! oracle to find it (`tests/mutants.rs`).

pub mod batcher;
pub mod campaign;
pub mod cell;
pub mod decree;
pub mod oracle;
pub mod trace;
