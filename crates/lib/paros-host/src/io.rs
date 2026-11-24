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
/// - **Durable.** A batch that returned `Ok` survives a crash: every batch
///   of a role carries state that a send of the role may wait on (the flight
///   rule), so the driver has no batch that a crash may lose.
/// - **Ranges.** [`scan`](Storage::scan) gives every record of one role,
///   and no record of another role.
pub trait Storage<F: Family> {
    /// Make `ops` durable as one atomic batch.
    ///
    /// # Errors
    ///
    /// The batch may or may not be durable.
    fn commit(&mut self, ops: &[F::Op]) -> Result<(), StorageError>;

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

/// The random source of one process. A role draws no random value: it asks
/// for one ([`crate::Effect::Draw`]), the driver draws it here, and the value
/// comes back as a recorded input (§J4.2, rule 2). A simulation gives a
/// seeded source, so a seed is a run.
pub trait Entropy {
    /// A new random value.
    fn draw(&mut self) -> u64;
}

/// The random source of a driver whose roles ask no draw.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct NoDraws;

impl Entropy for NoDraws {
    /// # Panics
    ///
    /// Always: a role asked a draw from a driver that has no random source
    /// ([`crate::Driver::with_entropy`] gives one).
    fn draw(&mut self) -> u64 {
        unreachable!("a role asked a draw from a driver with no random source")
    }
}
