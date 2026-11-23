//! The messages between a register operation, a handover and the members
//! of the journal's matchmaker set (§J3.4). Every message names the
//! matchmaker generation it is for, and a member answers only the
//! generation it serves.

use paros_consensus::Ballot;
use paros_consensus::membership::{MatchmakerGeneration, MatchmakerSet};
use paros_consensus::types::{FNV_OFFSET, Fingerprint, Weigh, fnv1a};

use super::value::RegisterValue;
use crate::codec::encode_segment;

/// The value a member holds and the generation of the write that put it
/// there (the `LocalGenerationReg`: the write generation and the
/// value).
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct Written {
    /// The operation generation of the write, `Ballot::zero()` for the
    /// register of a new journal.
    pub op: Ballot,
    /// The value it wrote.
    pub value: RegisterValue,
}

/// The state a handover gives the next generation: the set, the highest
/// write of the stop quorum and its highest read generation
/// (`specs/tla/segment-records/`, `Close` with `CloseFrom = "max"`).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Successor {
    /// The next set, one generation above the old one.
    pub set: MatchmakerSet,
    /// The highest write of the stop quorum.
    pub written: Written,
    /// The highest read generation of the stop quorum.
    pub read: Ballot,
}

impl Weigh for Successor {
    /// The bytes of the successor's scalars and of its members' ids.
    fn weight(&self) -> u64 {
        let members = core::mem::size_of_val(self.set.members());
        u64::try_from(core::mem::size_of::<Self>() + members).expect("a successor fits in u64")
    }
}

impl Fingerprint for Successor {
    fn fingerprint(&self) -> u64 {
        let mut hash = fnv1a(FNV_OFFSET, &self.set.generation.0.to_le_bytes());
        for member in self.set.members() {
            hash = fnv1a(hash, &member.0.to_le_bytes());
        }
        for ballot in [self.written.op, self.read] {
            hash = fnv1a(hash, &ballot.round.to_le_bytes());
            hash = fnv1a(hash, &ballot.node.0.to_le_bytes());
        }
        let term = self.written.value.term();
        hash = fnv1a(hash, &term.number.to_le_bytes());
        hash = fnv1a(hash, &term.holder.map_or(u64::MAX, |n| n.0).to_le_bytes());
        for record in self.written.value.segments().values() {
            hash = fnv1a(hash, &encode_segment(record));
        }
        hash
    }
}

/// What a member does in its generation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum MemberPhase {
    /// Not a member of a generation yet: a spare, until a handover brings
    /// it in.
    Inactive,
    /// It serves reads and writes of its generation.
    Active,
    /// A handover stopped its generation: it takes no read and no write,
    /// and still answers the decree of the handover.
    Frozen,
}

/// A request to one member.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Request {
    /// The read of operation `op`: the member raises its read generation to
    /// `op` and answers what it holds.
    Read {
        /// The generation the operation runs at.
        generation: MatchmakerGeneration,
        /// The operation generation.
        op: Ballot,
    },
    /// The write of operation `op`: the member takes it only if its read
    /// generation is at most `op` and its write generation is below `op`.
    Write {
        /// The generation the operation read.
        generation: MatchmakerGeneration,
        /// The operation generation.
        op: Ballot,
        /// The new value.
        value: RegisterValue,
    },
    /// A handover stops `generation`: the member freezes and reports what
    /// it holds.
    Stop {
        /// The generation that hands over.
        generation: MatchmakerGeneration,
    },
    /// A member of the next set stores the proposed state, durably.
    Bootstrap {
        /// The proposed state.
        successor: Successor,
    },
    /// Phase 1 of the handover's decree over the old set.
    Prepare {
        /// The generation that hands over.
        generation: MatchmakerGeneration,
        /// The decree ballot.
        ballot: Ballot,
    },
    /// Phase 2 of the handover's decree over the old set.
    Accept {
        /// The generation that hands over.
        generation: MatchmakerGeneration,
        /// The decree ballot.
        ballot: Ballot,
        /// The value of the decree.
        successor: Successor,
    },
    /// The decree chose `successor`: a member of the old set records it,
    /// a member of the next set serves it.
    Chosen {
        /// The chosen state.
        successor: Successor,
    },
}

/// A member's answer to one [`Request`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Reply {
    /// The answer to a read: what the member holds.
    Value {
        /// The operation generation.
        op: Ballot,
        /// The highest write the member holds.
        written: Written,
    },
    /// The member took the write of `op`.
    Wrote {
        /// The operation generation.
        op: Ballot,
    },
    /// The member refused the write of `op`: a later operation read or
    /// wrote here. The caller opens a new operation above both.
    Conflict {
        /// The operation generation.
        op: Ballot,
        /// The member's read generation.
        read: Ballot,
        /// The member's write generation.
        written: Ballot,
    },
    /// The member froze: what the successor is built from.
    Stopped {
        /// The generation that hands over.
        generation: MatchmakerGeneration,
        /// The highest write the member holds.
        written: Written,
        /// Its read generation.
        read: Ballot,
        /// Its decree promise: the decree opens above it.
        promised: Ballot,
    },
    /// The member stored the proposed state.
    Bootstrapped {
        /// The fingerprint of the state it stored.
        successor: u64,
    },
    /// A decree promise.
    Promised {
        /// The decree ballot.
        ballot: Ballot,
        /// The member's vote, if any.
        vote: Option<(Ballot, Successor)>,
    },
    /// A decree vote.
    Accepted {
        /// The decree ballot.
        ballot: Ballot,
    },
    /// A decree refusal: the member promised a higher ballot.
    Nacked {
        /// The decree ballot.
        ballot: Ballot,
        /// The promise that refused it.
        promised: Ballot,
    },
    /// The member learned the chosen state.
    Learned {
        /// The generation of the chosen state.
        generation: MatchmakerGeneration,
    },
    /// The member does not serve the generation asked.
    Refused {
        /// The set the member is in.
        set: MatchmakerSet,
        /// What it does there.
        phase: MemberPhase,
        /// The successor of that set, once the member learned it.
        successor: Option<MatchmakerSet>,
    },
}
