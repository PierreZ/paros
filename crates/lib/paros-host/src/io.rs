//! The **traits a backend implements** (§J4.2, rules 2 to 4): the storage,
//! the transport and the clock of a process. Each is generic over the
//! [`Family`] of the roles, so one backend serves every role of its family.
//!
//! No trait requires `Send`, an executor or a runtime type (§J4.3): a
//! backend may run on compio, tokio, a thread, or nothing.

use crate::family::Family;
use crate::time::Time;

/// A fault of a storage call, typed (§J4.2, rule 4). Each fault has one
/// verdict ([`StorageError::verdict`]), the same for every call that
/// reports it: the driver reads the verdict, never the call.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StorageError {
    /// The call did nothing: a commit wrote no op of its batch (the disk is
    /// full, or the backend refused the batch), a scan read nothing. The
    /// roles of the call stop and may start again from a scan.
    Full,
    /// The disk lost a record of the role. Its list still names the role
    /// (faulty, not empty), and each scan of it reports `Lost`: the role
    /// stops for good, so placement never starts it again from what the
    /// loss left.
    Lost,
    /// The backend does not know what the call did (fsyncgate): a commit
    /// may or may not be durable, and the page cache may still show a batch
    /// that the disk does not hold. Nothing that this process reads from
    /// the disk can be trusted again: the process stops, and a new process
    /// reads the disk.
    Unknown,
}

/// What the driver does on a [`StorageError`]. `specs/tla/flight-rule/`
/// checks each verdict (`storage`) and a control for each wrong one
/// (`lost_restore`, `list_readable`, `unknown_rescan`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Verdict {
    /// The roles of the call stop: [`Family::failed`] for a commit,
    /// [`Family::unreadable`] for a scan. Placement may start them again,
    /// from a scan.
    Again,
    /// The roles of the call stop for good ([`Family::lost`]).
    ForGood,
    /// The process stops: the driver halts ([`crate::Driver::halted`]),
    /// and the program drops it and starts a new one on the same disk.
    Halt,
}

impl StorageError {
    /// The one verdict of this fault.
    #[must_use]
    pub fn verdict(self) -> Verdict {
        match self {
            Self::Full => Verdict::Again,
            Self::Lost => Verdict::ForGood,
            Self::Unknown => Verdict::Halt,
        }
    }
}

impl core::fmt::Display for StorageError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        let what = match self {
            Self::Full => "the call did nothing",
            Self::Lost => "the disk lost a record of the role",
            Self::Unknown => "the backend does not know what the call did",
        };
        write!(f, "storage: {what}")
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
/// - **Listed.** [`list`](Storage::list) names every role that has a record
///   on the disk, and every role whose records the disk lost: a damaged role
///   is faulty, not empty, so a process that boots never starts it again
///   empty.
/// - **Typed faults.** A call that fails says how ([`StorageError`]). A
///   `Full` commit wrote nothing; a scan of a role that lost a record
///   reports `Lost` each time; a backend that does not know what it wrote
///   reports `Unknown`.
///
/// A role that keeps no record (a frontend) needs nothing of the storage:
/// it starts with no read ([`crate::Start::Running`]) and writes no batch.
pub trait Storage<F: Family> {
    /// Make `ops` durable as one atomic batch.
    ///
    /// # Errors
    ///
    /// `Full`: no op is durable. `Lost`: a role of the batch lost a record.
    /// `Unknown`: the batch may or may not be durable.
    fn commit(&mut self, ops: &[F::Op]) -> Result<(), StorageError>;

    /// Every record of the role at `address`.
    ///
    /// # Errors
    ///
    /// `Lost`: the disk lost a record of the role. `Full` or `Unknown`: the
    /// backend cannot read the range. The role does not start.
    fn scan(&mut self, address: F::Address) -> Result<F::Records, StorageError>;

    /// The address of every role that has records on the disk, or lost
    /// some, in address order: what a process that boots serves again
    /// before it reads its assignments.
    ///
    /// # Errors
    ///
    /// The backend cannot list the disk: the process cannot boot.
    fn list(&mut self) -> Result<Vec<F::Address>, StorageError>;
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
