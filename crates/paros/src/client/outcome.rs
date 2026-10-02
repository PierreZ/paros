//! Typed outcomes: what one call (or one policy loop) came back with, judged
//! off the wire once, here, so no caller re-derives a verdict from a reply's
//! flags or compares a refusal label as a string.
//!
//! Every judge is pure: a reply in, an outcome out. A transport error is
//! **ambiguous**, never "not done": the node may have run the call.

use moonpool_rpc::RpcError;
use paros_core::{JournalState, ReconfigureRefusal};

use crate::rpc::public::WriteOutcome as WireWriteOutcome;
use crate::rpc::{
    ReadAck, ReconfigureAck, ReconfigureMatchmakersAck, RetireAck, SetLeaderAck, TruncateAck,
    WriteAck, journal_state_from_proto,
};

/// One `Write` attempt's outcome (#204).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WriteOutcome {
    /// The journal holds the batch at `[seq, seq + count)`: accepted now,
    /// or (`duplicate`) answered from the log as the retry of a write
    /// accepted there earlier.
    Written {
        /// The first record's position.
        seq: u64,
        /// The records the batch holds.
        count: u64,
        /// Whether the log answered it as a write it already held.
        duplicate: bool,
    },
    /// Refused in place: a stale or foreign writer, a position that is not
    /// the next one, or a retry whose bytes differ. `state` names the
    /// journal's writer and next position.
    Refused {
        /// The journal state the write was judged against.
        state: JournalState,
    },
    /// The position is below `first_seq`: whether it was written is
    /// unknowable, and `state` says where the journal stands.
    Truncated {
        /// The journal state the write was judged against.
        state: JournalState,
    },
    /// No verdict: the node does not lead the journal (`leader` is its
    /// hint, when it has one).
    Redirect {
        /// The node id the answering node believes leads.
        leader: Option<u64>,
    },
    /// The node does not serve the journal.
    UnknownJournal,
    /// The reply named a journal state that does not decode: no verdict.
    Malformed,
    /// No answer: the write may or may not be in the journal.
    Ambiguous,
}

impl WriteOutcome {
    /// Judge one `Write` RPC's reply.
    #[must_use]
    pub fn judge(response: &Result<WriteAck, RpcError>) -> Self {
        let Ok(ack) = response else {
            return Self::Ambiguous;
        };
        if ack.unknown_journal {
            return Self::UnknownJournal;
        }
        let state = || journal_state_from_proto(ack.state).ok();
        match ack.outcome() {
            WireWriteOutcome::Accepted | WireWriteOutcome::Duplicate => Self::Written {
                seq: ack.seq,
                count: ack.count,
                duplicate: ack.outcome() == WireWriteOutcome::Duplicate,
            },
            WireWriteOutcome::Refused => {
                state().map_or(Self::Malformed, |state| Self::Refused { state })
            }
            WireWriteOutcome::Truncated => {
                state().map_or(Self::Malformed, |state| Self::Truncated { state })
            }
            WireWriteOutcome::None => Self::Redirect { leader: ack.leader },
        }
    }

    /// Whether this is a verdict the journal state machine gave (written,
    /// refused or truncated), rather than no answer about the write.
    #[must_use]
    pub fn is_verdict(&self) -> bool {
        matches!(
            self,
            Self::Written { .. } | Self::Refused { .. } | Self::Truncated { .. }
        )
    }
}

/// One `SetLeader` attempt's outcome (#204).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SetLeaderOutcome {
    /// The compare-and-swap won: `state` is the new generation.
    Won {
        /// The journal state after the swap.
        state: JournalState,
    },
    /// It lost: `state` names the current writer.
    Lost {
        /// The journal state it lost against.
        state: JournalState,
    },
    /// No verdict: the node does not lead the journal.
    Redirect {
        /// The node id the answering node believes leads.
        leader: Option<u64>,
    },
    /// The node does not serve the journal.
    UnknownJournal,
    /// The reply named a journal state that does not decode.
    Malformed,
    /// No answer: the swap may or may not have been decided.
    Ambiguous,
}

impl SetLeaderOutcome {
    /// Judge one `SetLeader` RPC's reply.
    #[must_use]
    pub fn judge(response: &Result<SetLeaderAck, RpcError>) -> Self {
        let Ok(ack) = response else {
            return Self::Ambiguous;
        };
        if ack.unknown_journal {
            return Self::UnknownJournal;
        }
        if !ack.decided {
            return Self::Redirect { leader: ack.leader };
        }
        let Ok(state) = journal_state_from_proto(ack.state) else {
            return Self::Malformed;
        };
        if ack.won {
            Self::Won { state }
        } else {
            Self::Lost { state }
        }
    }
}

/// What a claim — a read of where the journal stands, then a `SetLeader`
/// against the generation read — came back with.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClaimOutcome {
    /// The claim won: `state` is the generation it minted.
    Won {
        /// The journal state after the swap.
        state: JournalState,
    },
    /// It lost: another owner's claim was decided first.
    Lost {
        /// The journal state it lost against.
        state: JournalState,
    },
    /// Not asked: the read already names the claimant the owner (an
    /// earlier claim of its own won and its answer was lost), so it adopts
    /// `state` instead of superseding itself.
    Owned {
        /// The journal state the read was served from.
        state: JournalState,
    },
    /// The `SetLeader` was not decided by the node asked.
    Redirect {
        /// The node id the answering node believes leads.
        leader: Option<u64>,
    },
    /// The node asked does not serve the journal.
    UnknownJournal,
    /// A reply named a journal state that does not decode.
    Malformed,
    /// No server served the read the claim starts with: nothing was asked.
    Unread,
    /// The `SetLeader` went unanswered.
    Ambiguous,
}

impl From<SetLeaderOutcome> for ClaimOutcome {
    fn from(outcome: SetLeaderOutcome) -> Self {
        match outcome {
            SetLeaderOutcome::Won { state } => Self::Won { state },
            SetLeaderOutcome::Lost { state } => Self::Lost { state },
            SetLeaderOutcome::Redirect { leader } => Self::Redirect { leader },
            SetLeaderOutcome::UnknownJournal => Self::UnknownJournal,
            SetLeaderOutcome::Malformed => Self::Malformed,
            SetLeaderOutcome::Ambiguous => Self::Ambiguous,
        }
    }
}

/// One `Read` attempt's outcome (#204).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReadOutcome {
    /// A page: the records from `from` up, and the state it was served
    /// from (its `next_seq` is the tail the page was read against).
    Page {
        /// The position of the page's first record.
        from: u64,
        /// The records, in position order.
        records: Vec<Vec<u8>>,
        /// The journal state the page was served from.
        state: JournalState,
    },
    /// The position asked for is below `first_seq`: nothing is served, and
    /// `state.first_seq` is where a reader resumes.
    Truncated {
        /// The journal state the read was judged against.
        state: JournalState,
    },
    /// Not served: the node's quorum read did not confirm in time. An
    /// honest unavailability — ask another server.
    Unserved,
    /// The node does not serve the journal.
    UnknownJournal,
    /// The reply named a journal state that does not decode.
    Malformed,
    /// No answer.
    Ambiguous,
}

impl ReadOutcome {
    /// Judge one `Read` RPC's reply.
    #[must_use]
    pub fn judge(response: Result<ReadAck, RpcError>) -> Self {
        let Ok(ack) = response else {
            return Self::Ambiguous;
        };
        if ack.unknown_journal {
            return Self::UnknownJournal;
        }
        if !ack.served {
            return Self::Unserved;
        }
        let Ok(state) = journal_state_from_proto(ack.state) else {
            return Self::Malformed;
        };
        if ack.truncated {
            Self::Truncated { state }
        } else {
            Self::Page {
                from: ack.from_seq,
                records: ack.records,
                state,
            }
        }
    }

    /// Whether a server answered this read for the journal (a page or a
    /// truncation): a read that is neither is asked of the next server.
    #[must_use]
    pub fn is_served(&self) -> bool {
        matches!(self, Self::Page { .. } | Self::Truncated { .. })
    }

    /// The journal state the answer was served from, when it was.
    #[must_use]
    pub fn state(&self) -> Option<JournalState> {
        match self {
            Self::Page { state, .. } | Self::Truncated { state } => Some(*state),
            _ => None,
        }
    }
}

/// One `Truncate` attempt's (or a truncation's) outcome (#204).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TruncateOutcome {
    /// The leader decided it: `state.first_seq` is the floor now.
    Applied {
        /// The journal state after the truncation.
        state: JournalState,
    },
    /// Not decided by the node asked.
    Redirect {
        /// The node id the answering node believes leads.
        leader: Option<u64>,
    },
    /// The node does not serve the journal.
    UnknownJournal,
    /// The reply named a journal state that does not decode.
    Malformed,
    /// No answer, or the re-ask budget ran out.
    Ambiguous,
}

impl TruncateOutcome {
    /// Judge one `Truncate` RPC's reply.
    #[must_use]
    pub fn judge(response: &Result<TruncateAck, RpcError>) -> Self {
        let Ok(ack) = response else {
            return Self::Ambiguous;
        };
        if ack.unknown_journal {
            return Self::UnknownJournal;
        }
        if !ack.decided {
            return Self::Redirect { leader: ack.leader };
        }
        journal_state_from_proto(ack.state).map_or(Self::Malformed, |state| Self::Applied { state })
    }
}

/// One acceptor-set reconfiguration's outcome (#122).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReconfigureOutcome {
    /// The leader started it: it campaigns at `round` for the new set.
    Started {
        /// The node that answered, as it names itself (its leader hint).
        leader: Option<u64>,
        /// The round the reconfiguration campaigns at.
        round: u64,
    },
    /// The node asked does not lead.
    NotLeader {
        /// The node id the answering node believes leads.
        leader: Option<u64>,
    },
    /// Refused, for a reason the request cannot change by being re-sent
    /// elsewhere (`Unsettled` aside: a settled leader may take it later).
    Refused {
        /// The answering node's leader hint.
        leader: Option<u64>,
        /// Why.
        refusal: ReconfigureRefusal,
    },
    /// A refusal label this client does not know: a newer server.
    Unrecognized {
        /// The answering node's leader hint.
        leader: Option<u64>,
    },
    /// No answer, or the re-ask budget ran out.
    Ambiguous,
}

impl ReconfigureOutcome {
    /// Judge one `Reconfigure` RPC's reply.
    #[must_use]
    pub fn judge(response: Result<ReconfigureAck, RpcError>) -> Self {
        let Ok(ack) = response else {
            return Self::Ambiguous;
        };
        let leader = ack.leader;
        if ack.accepted {
            return Self::Started {
                leader,
                round: ack.round.unwrap_or(0),
            };
        }
        let refusal = match ack.refusal.as_str() {
            "not_leader" => return Self::NotLeader { leader },
            "no_matchmakers" => ReconfigureRefusal::NoMatchmakers,
            "unchanged" => ReconfigureRefusal::Unchanged,
            "unknown_member" => ReconfigureRefusal::UnknownMember,
            "malformed" => ReconfigureRefusal::Malformed,
            "unsettled" => ReconfigureRefusal::Unsettled,
            "round_exhausted" => ReconfigureRefusal::RoundExhausted,
            _ => return Self::Unrecognized { leader },
        };
        Self::Refused { leader, refusal }
    }
}

/// Why a matchmaker-set reconfiguration was refused (#125).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MatchmakersRefusal {
    /// The deployment names no matchmakers.
    NoMatchmakers,
    /// The requested set is empty.
    Empty,
    /// The requested set names a matchmaker the node has no link to.
    UnknownMatchmaker,
    /// A handover is already in flight at the node asked.
    Busy,
    /// A label this client does not know: a newer server.
    Unrecognized,
}

/// One matchmaker-set reconfiguration's outcome (#125).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReconfigureMatchmakersOutcome {
    /// The node started the handover towards `generation`.
    Started {
        /// The generation the handover installs.
        generation: u64,
    },
    /// Refused.
    Refused(MatchmakersRefusal),
    /// No answer, or the re-ask budget ran out.
    Ambiguous,
}

impl ReconfigureMatchmakersOutcome {
    /// Judge one `ReconfigureMatchmakers` RPC's reply.
    #[must_use]
    pub fn judge(response: Result<ReconfigureMatchmakersAck, RpcError>) -> Self {
        let Ok(ack) = response else {
            return Self::Ambiguous;
        };
        if ack.accepted {
            return Self::Started {
                generation: ack.generation.unwrap_or(0),
            };
        }
        Self::Refused(match ack.refusal.as_str() {
            "no_matchmakers" => MatchmakersRefusal::NoMatchmakers,
            "empty" => MatchmakersRefusal::Empty,
            "unknown_matchmaker" => MatchmakersRefusal::UnknownMatchmaker,
            "busy" => MatchmakersRefusal::Busy,
            _ => MatchmakersRefusal::Unrecognized,
        })
    }
}

/// Why a node refused to retire (#123, #165).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RetireRefusal {
    /// The deployment names no matchmakers: nothing is ever collected.
    Plain,
    /// The node leads.
    Leader,
    /// The node is a member of the configuration it believes in force.
    Member,
    /// The node's belief is not bound to the watermark sent: it has not
    /// heard the configuration the floor kept (re-read `Inspect`).
    Stale,
    /// No effective floor above the node's membership fence.
    NotCollected,
    /// A label this client does not know: a newer server.
    Unrecognized,
}

/// One retirement's outcome (#123).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RetireOutcome {
    /// The node retired.
    Retired,
    /// The node refused.
    Refused(RetireRefusal),
    /// No answer: the node may have retired.
    Ambiguous,
}

impl RetireOutcome {
    /// Judge one `Retire` RPC's reply.
    #[must_use]
    pub fn judge(response: Result<RetireAck, RpcError>) -> Self {
        let Ok(ack) = response else {
            return Self::Ambiguous;
        };
        if ack.accepted {
            return Self::Retired;
        }
        Self::Refused(match ack.refusal.as_str() {
            "plain" => RetireRefusal::Plain,
            "leader" => RetireRefusal::Leader,
            "member" => RetireRefusal::Member,
            "stale" => RetireRefusal::Stale,
            "not_collected" => RetireRefusal::NotCollected,
            _ => RetireRefusal::Unrecognized,
        })
    }
}
