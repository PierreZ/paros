//! The read-only [`Storage`] port the core depends on.

use crate::journal_state::JournalState;
use crate::state::{Config, HardState};
use crate::types::{Ballot, Command, Slot};

/// A read-only recovery/serving port. The **application** implements it and owns
/// *all* writes; the core only ever *reads back* state the application has
/// already persisted (per the [`crate::Ready`] handshake's durability ordering).
///
/// This mirrors etcd-raft's `Storage`: every method is a read. Writers
/// (`persist_ballot`, `append_accepted`, `set_chosen_index`, `truncate`, ...
/// on `paros::LogStorage`) live on the store the driver writes while
/// processing a [`crate::Ready`], never on this trait, which keeps the core
/// trivially testable against an in-memory fake.
///
/// Bootstrap and restart are the same path: the core reads durable state back in
/// on construction and resumes. A fresh node is just an empty/sentinel
/// `Storage`.
///
/// All methods are infallible: verifying and classifying durable records is
/// the storage layer's job (its boot scan), done before the core reads
/// anything back through this port.
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

    /// The **sealed** journal state (#204): what the slots below
    /// [`Storage::first_slot`] folded to, persisted when truncation (or a
    /// trim-point jump) dropped them. Read once at construction: the journal
    /// fold starts from it at the floor and replays the retained log, so a
    /// restart after truncation reaches the state of a node that never
    /// restarted. Defaults to the journal's birth: a storage that has never
    /// truncated has nothing sealed.
    fn sealed_state(&self) -> JournalState {
        JournalState::default()
    }

    /// The **recoverable faulty entries** the boot scan classified (Stage 8,
    /// CTRL): retained log slots whose accepted *value* is lost but whose
    /// identity — `(slot, accepted_ballot)` — survived (the identifier, or the
    /// entry's own checksummed identity region). Read once at construction; the
    /// core reports each as `faulty(ballot)` in its Promise tri-state (never as
    /// "nothing accepted here" — the CTRL Figure-2 bug class) and repairs it in
    /// place from peers. A record whose identity is *also* lost must not appear
    /// here: it is unidentifiable and stays a crash at the scan. Defaults to
    /// empty: a storage that cannot rot has nothing faulty.
    ///
    /// **`faulty` may never count toward the none-tally.** This is CTRL
    /// §5.1.1's first known-fatal mutation, and it was proven load-bearing by
    /// making it: a boot scan that classifies a rotted record normally but
    /// *withholds* it from the tri-state makes the acceptor answer "nothing
    /// accepted here" for a slot whose value it lost. A promise quorum that
    /// excludes the record's surviving clean copy then sees a unanimous `none`
    /// and no-op-fills (or re-allocates) an already-chosen slot — two values
    /// chosen for one slot, which turned the agreement oracles ("a durable
    /// accept quorum never decides two values for a slot", "at most one value
    /// is ever chosen for a slot") red with their full apply-time cascade.
    /// The mutation's other fate is the boot read-back's completeness assert:
    /// when the withheld record's hole lands *below* the chosen prefix, the
    /// node refuses to boot at all, before the misreport can reach a Promise.
    fn faulty_entries(&self) -> Vec<(Slot, Ballot)> {
        Vec::new()
    }
}
