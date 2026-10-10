//! The frontend's [`Authz`] (#192 (the frontend)): a request the frontend
//! states in names, checked as a Biscuit [`Request`] against a [`KeyRing`].
//!
//! The frontend adds the time from its provider's clock (`epoch` plus the
//! provider's `now`), so the simulation replays every decision.

use std::time::{Duration, SystemTime};

use paros::frontend::{self, Authz, Denial, Target};

use crate::{KeyRing, Operation, Refusal, Request, TargetKind, Token, authorize};

/// The Biscuit [`Authz`]: every token checked against the root public keys
/// of a ring, with the one policy (`crate::POLICY`).
#[derive(Clone, Debug)]
pub struct BiscuitAuthz {
    ring: KeyRing,
}

impl BiscuitAuthz {
    /// Check tokens against the keys of `ring`.
    #[must_use]
    pub fn new(ring: KeyRing) -> Self {
        Self { ring }
    }

    /// The ring it trusts.
    #[must_use]
    pub fn ring(&self) -> &KeyRing {
        &self.ring
    }
}

/// The operation a frontend call is checked as.
#[must_use]
pub fn operation(operation: frontend::Operation) -> Operation {
    match operation {
        frontend::Operation::Write => Operation::JournalWrite,
        frontend::Operation::Read => Operation::JournalRead,
        frontend::Operation::Truncate => Operation::JournalTruncate,
        frontend::Operation::SetLeader => Operation::JournalSetLeader,
    }
}

impl Authz for BiscuitAuthz {
    fn authorize(&self, token: &[u8], request: &frontend::Request<'_>) -> Result<(), Denial> {
        let (tenant, kind, journal) = match request.target {
            Target::Users { tenant, journal } => {
                (Some(tenant), Some(TargetKind::Users), Some(journal))
            }
            Target::Internal => (None, Some(TargetKind::Internal), None),
        };
        let checked = Request {
            operation: operation(request.operation),
            tenant,
            kind,
            journal,
            now: SystemTime::UNIX_EPOCH + request.now,
        };
        authorize(&Token::from_bytes(token.to_vec()), &self.ring, &checked).map_err(|refusal| {
            match refusal {
                Refusal::InvalidToken => Denial::InvalidToken,
                Refusal::Expired => Denial::Expired,
                Refusal::Forbidden => Denial::Forbidden,
            }
        })
    }
}

/// The time since the Unix epoch that `now` is, for a frontend's settings.
#[must_use]
pub fn since_epoch(now: SystemTime) -> Duration {
    now.duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
}
