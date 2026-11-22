//! `paros-journal-control` — the grid controller of the paros journal
//! (#1328 (paros-journal-control), step 7 of
//! `docs/journal/08-build-steps.md`; `docs/journal/06-controller.md` §J6).
//!
//! **The controller decides when and where; the protocol decides whether it
//! is safe.** Each journal has one grid controller, elected by a term in the
//! journal's matchmaker register; its only effect on the journal is the
//! fenced `reshape` call (§J6.1), whose guards live in
//! `paros_journal::register`. This crate holds the controller's pure
//! decision, with no I/O, no clock and no randomness:
//!
//! - [`measure`]: the protocol facts of a v1 grid (the spares of each
//!   column, `electable`, `writable`, the journal state), each asked
//!   through `paros_consensus::membership`.
//! - [`placement`]: the zone rule of a grid, and the members that replace
//!   the members that are down.
//! - [`mod@decide`]: [`decide()`], the one decision of the v1 controller: heal,
//!   repair, re-replicate, or hold and alert.
//! - [`role`]: the controller role, its election by a term and its fenced
//!   reshape call, as asks to a driver that runs each register operation.
//! - [`signals`]: what the processes of a journal publish to its controller
//!   ([`Report`]), and the liveness the controller reads from them.
//! - [`driver`]: [`ControlDriver`], the driver of one journal's controller:
//!   the signals, the register and the host's placement and token, through
//!   two ports ([`ControlRegister`], [`Cell`]).
//!
//! The numbers of §J6 (the stale age, the minimum interval between two
//! reshapes, the stall window, the suspicion window and the cap of the rate
//! limiter) are **Open**: the host gives them ([`Tunables`], the liveness
//! of the view, the token of the view), and this crate sets none.

// First: `probe!` is a textual-scope macro the modules below use.
#[macro_use]
mod probe;

pub mod decide;
pub mod driver;
pub mod measure;
pub mod placement;
pub mod role;
pub mod signals;

pub use decide::{Action, Alert, Decision, PROBES, Tunables, View, Wait, decide};
pub use driver::{Cell, ControlConfig, ControlDriver, ControlRegister, Polled};
pub use measure::{GridState, Liveness, Spares};
pub use role::{Ask, Controller, Outcome, Standing};
pub use signals::{ProcessId, Report, Signals, Windows};
// The membership types of a view, so a host needs no second import path.
pub use paros_consensus::membership::{FailureDomain, MatchmakerId, MatchmakerSet};
