//! The frontend's authorization seam (#192 (the frontend), §3.5): the
//! [`Authz`] trait every frontend call is checked through.
//!
//! paros carries a token as opaque bytes. The implementation that reads
//! them is not in this crate: Biscuit stays out of `paros` and `paros-core`
//! (§3.5), and `paros-authz-biscuit` implements [`Authz`]. The request is
//! stated in names, never ids (decided on 2026-10-10): a token names a
//! tenant and a journal by their string names.

use std::time::Duration;

use crate::rpc::FrontendVerdict;

/// The call a token is checked for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Operation {
    /// A `Write`.
    Write,
    /// A `Read`.
    Read,
    /// A `Truncate`.
    Truncate,
    /// A `SetLeader`.
    SetLeader,
}

impl Operation {
    /// Whether the call changes the journal.
    #[must_use]
    pub fn writes(self) -> bool {
        !matches!(self, Self::Read)
    }

    /// The operation's name, as a log reads it.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Write => "write",
            Self::Read => "read",
            Self::Truncate => "truncate",
            Self::SetLeader => "set_leader",
        }
    }
}

/// The journal a call targets, as the entry names it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Target<'a> {
    /// A journal of a `users` tenant, by its names.
    Users {
        /// The tenant's name.
        tenant: &'a str,
        /// The journal's name inside the tenant.
        journal: &'a str,
    },
    /// A journal named by its ids: an internal tenant's (the universe
    /// tenant's, a cell tenant's). It has no resolvable name, so only an
    /// `admin` token reaches it.
    Internal,
}

/// What a frontend asks its [`Authz`] for one call.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Request<'a> {
    /// The call.
    pub operation: Operation,
    /// Its journal.
    pub target: Target<'a>,
    /// The frontend's wall clock, since the Unix epoch: its settings'
    /// epoch plus its provider's time (never `SystemTime::now()`).
    pub now: Duration,
}

/// Why a frontend refused a call before it reached any machine. Judged by
/// kind only.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Denial {
    /// The token does not parse, or no trusted root key signed it.
    InvalidToken,
    /// The token expired.
    Expired,
    /// The token does not allow the call on its journal.
    Forbidden,
    /// The entry is malformed: a journal name without a tenant name, or a
    /// name that is not a valid name.
    Malformed,
}

impl Denial {
    /// The denial's name, as a log reads it.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::InvalidToken => "invalid_token",
            Self::Expired => "expired",
            Self::Forbidden => "forbidden",
            Self::Malformed => "malformed",
        }
    }

    /// The wire verdict that carries it.
    #[must_use]
    pub fn verdict(self) -> FrontendVerdict {
        match self {
            Self::InvalidToken => FrontendVerdict::InvalidToken,
            Self::Expired => FrontendVerdict::Expired,
            Self::Forbidden => FrontendVerdict::Forbidden,
            Self::Malformed => FrontendVerdict::Malformed,
        }
    }

    /// The denial a wire verdict carries, if it carries one.
    #[must_use]
    pub fn of(verdict: FrontendVerdict) -> Option<Self> {
        match verdict {
            FrontendVerdict::InvalidToken => Some(Self::InvalidToken),
            FrontendVerdict::Expired => Some(Self::Expired),
            FrontendVerdict::Forbidden => Some(Self::Forbidden),
            FrontendVerdict::Malformed => Some(Self::Malformed),
            FrontendVerdict::None | FrontendVerdict::Unanswered => None,
        }
    }
}

/// Checks a token for a call (§3.5). An implementation draws no randomness
/// and reads no clock of its own: the time is the request's, so the
/// simulation replays every decision.
pub trait Authz: Send + Sync + 'static {
    /// Allow `request` for the bearer of `token`, or say why not.
    ///
    /// # Errors
    ///
    /// The [`Denial`] that stops the call: never [`Denial::Malformed`],
    /// which the frontend decides before it asks.
    fn authorize(&self, token: &[u8], request: &Request<'_>) -> Result<(), Denial>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_denial_round_trips_through_its_verdict() {
        for denial in [
            Denial::InvalidToken,
            Denial::Expired,
            Denial::Forbidden,
            Denial::Malformed,
        ] {
            assert_eq!(Denial::of(denial.verdict()), Some(denial));
        }
        assert_eq!(Denial::of(FrontendVerdict::None), None);
        assert_eq!(Denial::of(FrontendVerdict::Unanswered), None);
    }
}
