//! `journal-sim` — deterministic simulation testing of the pure journal
//! core, `paros-journal` (#1327 (journal-sim), built with #1324
//! (paros-journal) in the same pull request, `docs/journal/07-verification.md`).
//!
//! A seeded random walk (moonpool-walk) drives the journal fold over a
//! chain of segments, with oracles that share no code with the fold, and
//! TLC checks each walk against `specs/tla/journal-state/FoldTrace.tla`
//! (moonpool-tla). Test only: `publish = false`, never a dependency of
//! another crate.

pub mod campaign;
pub mod fold;
pub mod host;
pub mod oracle;
pub mod register;
pub mod roles;
pub mod trace;
