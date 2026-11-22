//! The **storage trait** (§J4.2, rule 4): the one trait that every backend
//! of the journal implements, for every role.
//!
//! A backend stores keys and bytes ([`Op`]) and reads a role's records back
//! as one range. The format is not the backend's: [`crate::layout`] builds
//! every key and every value. So one backend serves the acceptor, the
//! replica and later roles, and one contract suite
//! ([`crate::contract::storage`]) tests every backend.

use crate::layout::{Entry, Key, Op};
use crate::wire::Address;

/// The error of a storage call. It means "unknown": the batch may or may
/// not be durable. The roles of the batch stop (§J4.2, rule 4).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageError(pub String);

impl core::fmt::Display for StorageError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "storage: {}", self.0)
    }
}

impl core::error::Error for StorageError {}

/// A disk of the journal roles.
///
/// The contract, which [`crate::contract::storage`] checks:
///
/// - **Atomic.** [`commit`](Storage::commit) makes every op of the batch
///   durable, or none of them: a batch is never torn, even by a crash.
/// - **In order.** The ops of a batch apply in order: a later op at the
///   same key wins. A commit that returned `Ok` is seen by every later
///   [`scan`](Storage::scan).
/// - **Durable.** With `sync`, a batch that returned `Ok` survives a crash.
///   Without `sync`, it may be lost by a crash, but only together with every
///   later batch.
/// - **Ranges.** [`scan`](Storage::scan) gives every record of one role, in
///   the order of [`Key::to_bytes`], and no record of another role.
///
/// A contract suite certifies the API only. Crash safety is claimed only on
/// a disk that can inject faults (Pierre, 2026-11-19).
pub trait Storage {
    /// Make `ops` durable as one atomic batch.
    ///
    /// # Errors
    ///
    /// The batch may or may not be durable.
    fn commit(&mut self, ops: &[Op], sync: bool) -> Result<(), StorageError>;

    /// Every record of the role at `address`, in key order. A record the
    /// backend finds corrupt is [`Entry::Corrupt`].
    ///
    /// # Errors
    ///
    /// The backend cannot read the range; the role does not start.
    fn scan(&mut self, address: Address) -> Result<Vec<(Key, Entry)>, StorageError>;
}
