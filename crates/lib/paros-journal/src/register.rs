//! The **journal's matchmaker register** (#1325 (the matchmaker register as
//! an optional role), `docs/journal/03-segments-and-records.md` §J3.4).
//!
//! The matchmakers of a journal hold one compare-and-set register: the
//! controller term and one [`SegmentRecord`](crate::SegmentRecord) for each
//! segment. It is the register of `specs/tla/cas-register/` (the
//! `LocalGenerationReg`), carried across a handover of the matchmaker set by
//! the rules of `specs/tla/segment-records/`.
//!
//! **An optional role, built by composition** (Pierre, 2026-11-20, §J4.3).
//! The registration matchmaker of the Matchmaker Paxos paper stays in
//! `paros-consensus` as it is. The register lives here, beside it, and uses
//! the core only as a library: [`MatchmakerSet`] for every quorum question,
//! the core's `Decree` for the decree of its handover and the core's
//! `Acceptor` for each member's half of it. A matchmaker process of the
//! journal hosts both roles; the core's handover and this one move to the
//! same next set.
//!
//! The parts, each a pure state machine:
//!
//! - [`value`]: the value, the [`Change`] of each record and its guard;
//! - [`member`]: one member of the set, the register replica;
//! - [`call`]: one operation: read at a majority, check the guard, write at
//!   a majority, done;
//! - [`handover`]: the handover of the register to the next set;
//! - [`message`]: the requests and the replies between them.
//!
//! [`MatchmakerSet`]: paros_consensus::membership::MatchmakerSet

pub mod call;
pub mod handover;
pub mod member;
pub mod message;
pub mod value;

pub use call::{CallPhase, CallStep, RegisterCall};
pub use handover::{HandoverPhase, HandoverRefusal, HandoverStep, RegisterHandover};
pub use member::{MemberState, MemberStep, RegisterMember};
pub use message::{MemberPhase, Reply, Request, Successor, Written};
pub use value::{Change, ChangeRefusal, RegisterValue, Term};

/// The core's types that the register's API names: an operation and a
/// decree generation, and the matchmaker set with its members.
pub use paros_consensus::Ballot;
pub use paros_consensus::membership::{MatchmakerGeneration, MatchmakerId, MatchmakerSet};
pub use paros_consensus::types::Fingerprint;

#[cfg(test)]
mod test_support;
