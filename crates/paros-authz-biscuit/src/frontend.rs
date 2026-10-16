//! The frontend's [`Authz`] (#192 (the frontend)): a request the frontend
//! states in names, checked as a Biscuit [`Request`] against a [`KeyRing`].
//!
//! The frontend adds the time from its provider's clock (`epoch` plus the
//! provider's `now`), so the simulation replays every decision.
//!
//! **Which keys** (#245 (Biscuit tokens)): the universe entry's root public
//! keys the request carries rule when there are any; else the ring this
//! [`BiscuitAuthz`] was built with, the bootstrap pin (`paros-frontend`'s
//! keys, `parosd --root-public-key`). A key of the entry that does not
//! parse is left out: it verifies nothing.

use std::time::{Duration, SystemTime};

use paros::frontend::{self, Authz, Denial, Target};

use paros::fleet::RootKeyEntry;

use crate::{KeyRing, Operation, Refusal, Request, RootPublicKey, TargetKind, Token, authorize};

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
        frontend::Operation::FormCell => Operation::CellForm,
        frontend::Operation::Admit => Operation::MachineAdmit,
        frontend::Operation::SetTags => Operation::JournalTag,
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
        let entry;
        let ring = match request.keys {
            Some(keys) if !keys.is_empty() => {
                entry = ring_of(keys);
                &entry
            }
            _ => &self.ring,
        };
        authorize(&Token::from_bytes(token.to_vec()), ring, &checked).map_err(|refusal| {
            match refusal {
                Refusal::InvalidToken => Denial::InvalidToken,
                Refusal::Expired => Denial::Expired,
                Refusal::Forbidden => Denial::Forbidden,
            }
        })
    }
}

/// The ring of the universe entry's `keys`: every key that parses.
fn ring_of(keys: &[RootKeyEntry]) -> KeyRing {
    let parsed = keys
        .iter()
        .filter_map(|key| RootPublicKey::from_parts(key.key_id, &key.label, &key.public_key).ok());
    // The entry never holds two keys under one id (its fold refuses it).
    KeyRing::new(parsed).unwrap_or_default()
}

/// The time since the Unix epoch that `now` is, for a frontend's settings.
#[must_use]
pub fn since_epoch(now: SystemTime) -> Duration {
    now.duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
}
