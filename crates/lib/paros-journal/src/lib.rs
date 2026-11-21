//! `paros-journal` — the pure journal core of paros (#1324 (paros-journal),
//! step 3 of `docs/journal/08-build-steps.md`).
//!
//! The journal v1 is one ordered log for each journal id, with Paxos inside
//! each segment (`docs/journal/`). This crate holds the parts of it that are
//! pure data and pure functions, with no I/O, no clock and no randomness:
//!
//! - [`fold`]: the journal fold, judged at apply in slot order. `Write`,
//!   `Truncate` and `SetLeader` with `SetLeader(None)` (multi-writer), the
//!   order "position before fence" and the answer `Below` for a resend across
//!   a seal (`docs/journal/02-data-plane.md` §J2.2, §J2.4.1, §J2.6).
//! - [`mod@resolve`]: the read-only `Resolve` call that settles a `Below` answer
//!   (§J2.2, #1320).
//! - [`held`]: the answers that a consensus leader holds until the slot of a
//!   write is applied, and `Unknown` for a slot that chose another command
//!   (§J2.2, #1232).
//! - [`segment`]: the record that the journal's matchmakers hold for each
//!   segment, with the guard of each of its compare-and-sets
//!   (`docs/journal/03-segments-and-records.md` §J3.4).
//! - [`register`]: the journal's matchmaker register, an optional role
//!   beside the core's registration matchmaker: its value, its members,
//!   its operations and its handover (§J3.4, #1325).
//! - [`command`]: the commands of the journal and its records with their
//!   headers (§J2.8).
//! - [`codec`]: the bytes of each of these, with a release number in every
//!   header (§J1.1).
//!
//! **The journal fold is not in `paros-consensus`** (Pierre, 2026-11-20,
//! §J4.3). The core keeps its own `Command` and fold for the users of the
//! library; this crate uses the core's identifiers as data only and adds no
//! concept to the core.
//!
//! **Hard assertions** (Tiger Style, as in the core): every function asserts
//! its preconditions, postconditions and invariants with `assert!`, on in
//! release. What a client or a peer can send is never asserted on: it is an
//! answer or a refusal.

pub mod codec;
pub mod command;
pub mod fold;
pub mod held;
pub mod register;
pub mod resolve;
pub mod roles;
pub mod segment;

pub use codec::{DecodeError, RELEASE, Release, decode_message, encode_message};
pub use command::{Command, Fence, Header, Headers, Leader, Record, Write};
pub use fold::{Answer, BoundaryState, JournalState, JournalView};
pub use held::{Held, Reply};
pub use resolve::{ResolveAnswer, resolve};
pub use segment::{
    Grid, GridError, OpeningCall, RecordRefusal, SegmentId, SegmentRecord, SegmentState,
};

pub use paros_consensus::journal::{LeaderUuid, Seq};
pub use paros_consensus::{NodeId, Slot};
