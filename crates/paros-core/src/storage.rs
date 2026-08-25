//! The read-only [`Storage`] port the core depends on.

use crate::state::{Config, HardState};
use crate::types::{Ballot, Command, SessionEntry, Slot};

/// A read-only recovery/serving port. The **application** implements it and owns
/// *all* writes; the core only ever *reads back* state the application has
/// already persisted (per the [`crate::Ready`] handshake's durability ordering).
///
/// This mirrors etcd-raft's `Storage`: every method is a read. Writers
/// (`append`, `set_hard_state`, `truncate`, ...) live on the *concrete* type
/// the application drives while processing a [`crate::Ready`], never on this
/// trait, which keeps the core trivially testable against an in-memory fake.
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

    /// The `(ballot, command)` accepted for `slot`, if any.
    fn accepted(&self, slot: Slot) -> Option<(Ballot, Command)>;

    /// The first slot still available (slots below it have been compacted away).
    fn first_slot(&self) -> Slot;

    /// The last slot present in storage.
    fn last_slot(&self) -> Slot;

    /// The **sealed** at-most-once session ledger: every `(client, seq) -> slot`
    /// record persisted when truncation (or a snapshot install) dropped the log
    /// records it was derived from. Read once at construction and merged under
    /// the walk-derived ledger, so a restart after truncation reproduces the
    /// same duplicate-suppression decisions as a node that never restarted
    /// (#94). Defaults to empty: a storage that has never truncated has nothing
    /// sealed.
    fn sealed_sessions(&self) -> Vec<SessionEntry> {
        Vec::new()
    }
}
