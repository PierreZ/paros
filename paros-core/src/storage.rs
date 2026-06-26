//! The read-only [`Storage`] port the core depends on.

use crate::state::{Config, HardState};
use crate::types::{Ballot, Entry, Slot};

/// A read-only recovery/serving port. The **application** implements it and owns
/// *all* writes; the core only ever *reads back* state the application has
/// already persisted (per the [`crate::Ready`] handshake's durability ordering).
///
/// This mirrors etcd-raft's `Storage`: every method is a read. Writers
/// (`append`, `set_hard_state`, `apply_snapshot`, …) live on the *concrete* type
/// the application drives while processing a [`crate::Ready`] — never on this
/// trait — which keeps the core trivially testable against an in-memory fake.
///
/// Bootstrap and restart are the same path: the core reads durable state back in
/// on construction and resumes. A fresh node is just an empty/sentinel
/// `Storage`.
///
/// All methods are infallible in Stage 0; error sentinels (`ErrCompacted`,
/// `ErrUnavailable`, …) are deferred to a later stage.
pub trait Storage {
    /// The durable [`HardState`] and static [`Config`] to initialize the node
    /// with. Called once, at construction.
    fn initial_state(&self) -> (HardState, Config);

    /// The `(ballot, entry)` accepted for `slot`, if any.
    fn accepted(&self, slot: Slot) -> Option<(Ballot, Entry)>;

    /// The first slot still available (slots below it have been compacted away).
    fn first_slot(&self) -> Slot;

    /// The last slot present in storage.
    fn last_slot(&self) -> Slot;

    /// The most recent snapshot, if any. The `Vec<u8>` is opaque to the core
    /// (the application owns snapshot encoding); `None` means no snapshot yet.
    fn snapshot(&self) -> Option<Vec<u8>>;
}
