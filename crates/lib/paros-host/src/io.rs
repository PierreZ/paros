//! The **traits a backend implements** (§J4.2, rules 2 to 4): the storage,
//! the transport and the clock of a process. Each is generic over the
//! [`Family`] of the roles, so one backend serves every role of its family.
//!
//! No trait requires `Send`, an executor or a runtime type (§J4.3): a
//! backend may run on compio, tokio, a thread, or nothing.

use crate::family::Family;
use crate::time::Time;

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

/// A disk of the roles of one family.
///
/// The contract:
///
/// - **Atomic.** [`commit`](Storage::commit) makes every op of the batch
///   durable, or none of them: a batch is never torn, even by a crash.
/// - **In order.** The ops of a batch apply in order: a later op at the
///   same key wins. A commit that returned `Ok` is seen by every later
///   [`scan`](Storage::scan).
/// - **Durable.** With `sync`, a batch that returned `Ok` survives a crash.
///   Without `sync`, it may be lost by a crash, but only together with every
///   later batch.
/// - **Ranges.** [`scan`](Storage::scan) gives every record of one role,
///   and no record of another role.
pub trait Storage<F: Family> {
    /// Make `ops` durable as one atomic batch.
    ///
    /// # Errors
    ///
    /// The batch may or may not be durable.
    fn commit(&mut self, ops: &[F::Op], sync: bool) -> Result<(), StorageError>;

    /// Every record of the role at `address`.
    ///
    /// # Errors
    ///
    /// The backend cannot read the range; the role does not start.
    fn scan(&mut self, address: F::Address) -> Result<F::Records, StorageError>;
}

/// A network of the roles of one family.
///
/// The driver gives every send to the transport, a send between two roles
/// of one process too (§J4.2, rule 3): one queue for local and remote
/// delivery, so a test can reorder and drop local messages too. A message
/// that arrives is the message that was sent, to the role it names. A
/// message may be lost, repeated or reordered: every role takes that.
pub trait Transport<F: Family> {
    /// Send `envelope` to its receiver. A send never blocks and never fails:
    /// a message that cannot go is lost.
    fn send(&mut self, envelope: F::Envelope);

    /// The next message for this process, if one has arrived.
    fn receive(&mut self) -> Option<F::Envelope>;
}

/// A clock of one process. The time never goes back.
pub trait Clock {
    /// The time now.
    fn now(&self) -> Time;
}
