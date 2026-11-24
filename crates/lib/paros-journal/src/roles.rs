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
//! - [`leader`]: [`JournalLeader`], the consensus leader of one ballot. It
//!   binds its ballot to the newest segment on the register first; its
//!   Phase 1 reads every slot of every segment that is not retired; it seals the segment before its own at one above the
//!   highest vote that its Phase 1 saw in the old grid (a CTRL faulty vote
//!   counts, #1313); it fills every slot below the start of its segment
//!   with the vote it saw or a `Noop` (#1233); it writes each slot in one
//!   column of its own grid (#1257) with the core's Phase-2 tally
//!   ([`Rounds`](paros_consensus::proposer::Rounds)); it sends each chosen
//!   slot to the replica set of the slot's segment (#1271); it holds the
//!   answer of each command it proposed until the slot is applied (#1232).
//! - [`proxy`]: [`JournalProxy`], a proxy leader: the Phase 2 of the rounds
//!   that a leader delegates to it, on the column that the delegation names,
//!   and the `Commit` to the replica set of the slot's segment
//!   (Compartmentalized Paxos §3.1, `specs/tla/proxy-leader/`).
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
pub mod proxy;
pub(crate) mod ranges;
pub mod replica;

pub use acceptor::{AcceptorDisk, AcceptorEffect, AcceptorOp, JournalAcceptor};
pub use leader::{Campaign, JournalLeader, LeaderEffect, LeaderPhase, Proposed};
pub use message::{Message, PromisePage, ReadTag};
pub use paros_consensus::contract::limits::{PROMISE_BATCH, RESEND_BATCH};
pub use paros_consensus::contract::write::AcceptorWrite;
pub use proxy::{JournalProxy, ProxyEffect};
pub use replica::{
    CopyId, ReadAnswer, ReplicaDisk, ReplicaEffect, ReplicaOp, SegmentReplica, Trim,
};

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

/// The message of every `probe!(reachable, ..)` of the roles. The coverage
/// gate of `journal-sim` (`tests/roles_coverage.rs`) requires each one, with
/// no excused probe; a new branch gets its probe and its line here in the
/// same PR, and a message is never reworded.
pub const REACHABLE: &[&str] = &[
    "acceptor: a Prepare below the promise is refused",
    "acceptor: an Accept in a dropped range is not answered",
    "acceptor: an Accept below the promise is refused",
    "acceptor: a read finds no belief and is not answered",
    "acceptor: a compaction drops votes",
    "acceptor: a Repair of a readable vote writes nothing",
    "leader: a promise page asks for the next page",
    "leader: a replica gives a blocked slot its command",
    "leader: a higher promise stops the leader",
    "leader: a replica holds the sealed segment",
    "leader: a seal of its own segment stops the leader",
    "leader: a retire seen in Phase 1 is kept",
    "leader: a done seal end opens the leader",
    "leader: a seal end of another stops the leader",
    "leader: a refused seal stops the leader",
    "leader: an open round is sent again",
    "leader: a chosen slot is committed again",
    "leader: F + 1 holders retire the sealed segment",
    "leader: a done value shows the retire",
    "leader: a chosen slot of a retired segment is answered Unknown",
    "leader: a first leader writes the seal end",
    "leader: a leader of a segment in force opens",
    "leader: a blocked slot that an answer dropped is skipped",
    "leader: a tick asks for a blocked slot",
    "leader: a dropped slot is not proposed",
    "leader: a blocked slot is not proposed",
    "leader: a proxy chose a delegated round",
    "leader: a chosen blocked slot is repaired",
    "leader: F + 1 replicas compacted a segment",
    "leader: an open round below a compaction closes",
    "leader: a chosen slot whose answer is gone is answered Unknown",
    "leader: a retire before the bind raises the first slot",
    "leader: a done bind starts Phase 1",
    "leader: a full window refuses a proposal",
    "leader: a higher bound ballot stops the leader",
    "leader: a refused bind stops the leader",
    "proxy: a stalled round is dropped",
    "proxy: an open round is sent again",
    "proxy: a delegation of a lower ballot is not taken",
    "proxy: a higher ballot drops the rounds below",
    "proxy: a delegation of an open round sends it again",
    "proxy: a column chooses a slot",
    "replica: a replica outside the set drops its copy",
    "replica: a new replica becomes a member",
    "replica: it compacts its log",
    "replica: a laggard jumps to a peer's trim",
    "replica: a Resolve before the base is not covered",
    "replica: a Resolve at a dropped copy is not covered",
    "replica: a Resolve below the trim floor is not covered",
    "replica: a Commit below its compaction point is dropped",
    "replica: a floor over a retired segment trims it",
    "replica: a new replica finds a full source",
    "replica: a new replica reports its copy",
    "replica: a Learn of a slot it did not apply is not answered",
    "replica: a Learn is answered",
    "replica: a Pull below its compaction point gets the trim",
    "replica: a full replica writes the boundary state",
    "replica: a read it cannot serve is unserved",
    "replica: a read meets a later segment",
    "replica: a read that waits too long is unserved",
    "replica: a read below the segment is Below",
    "replica: a read below the floor is Truncated",
];

/// The message of every `probe!(sometimes, ..)` of the roles: each must be
/// true at least once in the coverage gate, as [`REACHABLE`].
pub const SOMETIMES: &[&str] = &[
    "acceptor: a Prepare raises the promise",
    "acceptor: a Prepare at the promise writes nothing",
    "acceptor: an Accept it holds writes nothing",
    "acceptor: an Accept raises the promise",
    "acceptor: a read meets a newer segment",
    "acceptor: a read watermark counts a dropped slot",
    "acceptor: a Repair keeps the ballot of the faulty vote",
    "acceptor: a Repair writes at the choosing ballot",
    "leader: a stopped leader answers its held commands",
    "leader: a late answer decides a blocked slot",
    "leader: the gap fill writes a Noop",
    "leader: the gap fill writes a vote again",
    "leader: a stalled round moves to another proxy",
    "leader: Phase 1 skips a vote in a dropped range",
    "leader: the tally decides a faulty slot with a Noop",
    "leader: the tally decides a faulty slot with its readable vote",
    "proxy: a Nack goes to the leader",
    "replica: it holds the whole segment",
    "replica: it does not hold the whole segment yet",
    "replica: a chosen slot waits behind a hole",
    "replica: a Pull is served from its slots",
    "replica: a slot below the trim floor applies with no answer",
    "replica: a read waits for the fold to reach the trim floor",
    "replica: a read reaches the sealed end",
    "replica: a read of the open segment serves records",
    "replica: an answer to an earlier incarnation's read",
];

#[cfg(test)]
mod tests;
