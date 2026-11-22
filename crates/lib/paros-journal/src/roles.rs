//! The **journal roles** (#1326 (the journal roles over the core),
//! `docs/journal/04-roles.md` §J4.1): the acceptor, the leader and the
//! segment replica of the journal v1, as pure state machines under the
//! flight rule (§J4.2).
//!
//! Each role composes the roles of `paros-consensus` and adds no concept to
//! the core (§J4.3):
//!
//! - [`acceptor`]: [`JournalAcceptor`], one acceptor of the journal. It is
//!   the core's [`Acceptor`](paros_consensus::acceptor::Acceptor) over the
//!   journal's [`Command`](crate::Command). An `Accept` that raises the
//!   promise writes the promise and the vote in one batch (#1309).
//! - [`leader`]: [`JournalLeader`], the consensus leader of one ballot. Its
//!   Phase 1 reads every segment that is not retired, a sealed one only
//!   below its end; it seals the segment before its own at one above the
//!   highest vote that its Phase 1 saw in the old grid (a CTRL faulty vote
//!   counts, #1313); it fills every slot below the start of its segment
//!   with the vote it saw or a `Noop` (#1233); it writes each slot in one
//!   column of its own grid (#1257) with the core's Phase-2 tally
//!   ([`Rounds`](paros_consensus::proposer::Rounds)); it sends each chosen
//!   slot to the replica set of the slot's segment (#1271); it holds the
//!   answer of each command it proposed until the slot is applied (#1232).
//! - [`replica`]: [`SegmentReplica`], one replica of one segment. It applies
//!   the chosen slots of its segment in order and folds them from the
//!   boundary state of the segment before it (#1303), stops at the end of
//!   its segment once the write of the end is done, writes the boundary
//!   state of its sealed segment, catches up from the peers of its set, and
//!   answers `Resolve` (#1320).
//!
//! **The flight rule** (§J4.2, rule 1). A role that has a disk emits each
//! persist batch with a [`PersistId`], and each send names the batch that
//! it waits on ([`Flight`]): the driver transmits the send only once that
//! batch is durable. A reply that a step sends with no write of its own
//! waits on the last batch that the role emitted. After a restart, the ids
//! go on above every durable id. The leader has no disk (a ballot belongs to
//! one leader in memory, as in `specs/tla/journal-state/`), so its sends wait
//! on nothing.
//!
//! **The register is a port.** A role asks a compare-and-set of the
//! journal's matchmaker register as an effect ([`Change`](crate::register::Change)),
//! and the driver runs it with [`RegisterCall`](crate::register::RegisterCall).
//! A role acts only on a register value that a write of its own made done,
//! or that the driver read from a write that is done (§J3.4, "act on a write
//! seen done").

pub mod acceptor;
pub mod leader;
pub mod message;
pub mod replica;

pub use acceptor::{AcceptorDisk, AcceptorEffect, AcceptorOp, JournalAcceptor};
pub use leader::{Campaign, JournalLeader, LeaderEffect, LeaderPhase, Proposed};
pub use message::{Message, PromisePage};
pub use paros_consensus::contract::limits::{PROMISE_BATCH, RESEND_BATCH};
pub use paros_consensus::contract::write::AcceptorWrite;
pub use replica::{CopyId, ReplicaDisk, ReplicaEffect, ReplicaOp, SegmentReplica};

/// The id of one persist batch of a role (§J4.2, rule 1). Ids grow for the
/// whole life of the role, across its restarts.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PersistId(pub u64);

/// The persist ids of one role: the next id to give, and the last one given.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Flight {
    next: u64,
    last: Option<PersistId>,
}

impl Flight {
    /// The ids of a role that starts again: every new id is above
    /// `durable`, the highest batch that its disk holds, and a reply with no
    /// write waits on `durable` (it is durable already, so it waits on
    /// nothing in fact).
    ///
    /// # Panics
    ///
    /// When an assertion on its own postconditions fails: a programmer
    /// error, never an operating condition.
    #[must_use]
    pub fn after(durable: Option<PersistId>) -> Self {
        let flight = Self {
            next: durable.map_or(0, |PersistId(id)| id + 1),
            last: durable,
        };
        assert!(
            flight.last.is_none_or(|PersistId(id)| id < flight.next),
            "a new id is above every durable id"
        );
        flight
    }

    /// A new batch id.
    ///
    /// # Panics
    ///
    /// When an assertion on its own postconditions fails: a programmer
    /// error, never an operating condition.
    pub fn batch(&mut self) -> PersistId {
        let id = PersistId(self.next);
        self.next += 1;
        self.last = Some(id);
        assert!(self.next > id.0, "ids only grow");
        assert!(self.last == Some(id), "the new id is the last id");
        id
    }

    /// The batch that a reply with no write of its own waits on: the last
    /// batch that the role emitted, which may still be in flight
    /// (`specs/tla/flight-rule/`, control `own_batch`).
    #[must_use]
    pub fn last(&self) -> Option<PersistId> {
        self.last
    }
}

#[cfg(test)]
mod tests;
