//! `paros-election` — the election over a journal (#1420 (paros-election),
//! `docs/architecture/03-3-coordinators-and-placement.md` §3.3, decision 13).
//!
//! A user elects any actor with it, and the coordinators of the universe, of
//! a cell and of a tenant use it over their own control journal. It needs
//! only a journal with a compare-and-set of the leader (`SetLeader(new,
//! old)`) and a fenced write. It is a thin layer on the pure journal client,
//! [`paros_journal::client`]: an [`Election`] owns one client, and the
//! program gives it the time ([`Election::tick`]), the draws ([`Draws`]) and
//! the answers of the journal ([`Election::answer`]). It keeps nothing on
//! disk, reads no clock and draws no random number.
//!
//! The model is `specs/tla/election/`. The steps and their rules are on
//! [`election`].

pub mod config;
pub mod election;
pub mod record;

pub use config::Config;
pub use election::{Draws, Election, Event, Handoff, Refusal, WriteId};
pub use record::{ElectionRecord, Kind, Term};
