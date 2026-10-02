//! The client's observation port: every attempt at the four journal calls,
//! reported where it is sent and where its answer is judged — the seam a
//! history checker reads, the way [`crate::Audit`] is the driver's.
//!
//! Observation, never perturbation: an observer returns nothing the client
//! acts on (its token is only handed back), draws no randomness, and
//! removing it changes no call. Production passes [`NoObserver`].

use super::outcome::{ReadOutcome, SetLeaderOutcome, TruncateOutcome, WriteOutcome};
use crate::rpc::{Read, SetLeader, Truncate, Write};

/// An attempt, as it leaves the client.
#[derive(Clone, Copy, Debug)]
pub enum Attempted<'a> {
    /// A `Write`.
    Write(&'a Write),
    /// A `SetLeader`.
    SetLeader(&'a SetLeader),
    /// A `Read`.
    Read(&'a Read),
    /// A `Truncate`.
    Truncate(&'a Truncate),
}

impl Attempted<'_> {
    /// The journal the attempt names.
    #[must_use]
    pub fn journal(&self) -> u64 {
        match self {
            Self::Write(w) => w.journal,
            Self::SetLeader(s) => s.journal,
            Self::Read(r) => r.journal,
            Self::Truncate(t) => t.journal,
        }
    }
}

/// An attempt's outcome, as the client judged it.
#[derive(Clone, Copy, Debug)]
pub enum Answered<'a> {
    /// A `Write`'s.
    Write(&'a WriteOutcome),
    /// A `SetLeader`'s.
    SetLeader(&'a SetLeaderOutcome),
    /// A `Read`'s.
    Read(&'a ReadOutcome),
    /// A `Truncate`'s.
    Truncate(&'a TruncateOutcome),
}

/// What the client reports about its calls.
///
/// `invoked` is called when an attempt's request is built — before it is
/// sent — and returns a token (`None`: not interested); `answered` is called
/// with that token once the attempt comes back, whatever it came back with
/// (an [`WriteOutcome::Ambiguous`] transport failure included). An attempt
/// whose future is dropped — a caller's timeout, a shutdown — is never
/// answered: its outcome is unknown, which is exactly what it is.
pub trait CallObserver: Send + Sync + 'static {
    /// An attempt is about to leave.
    fn invoked(&self, attempt: Attempted<'_>) -> Option<u64>;

    /// The attempt `token` came back with `answer`.
    fn answered(&self, token: u64, answer: Answered<'_>);
}

/// The production observer: reports nothing.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoObserver;

impl CallObserver for NoObserver {
    fn invoked(&self, _attempt: Attempted<'_>) -> Option<u64> {
        None
    }

    fn answered(&self, _token: u64, _answer: Answered<'_>) {}
}
