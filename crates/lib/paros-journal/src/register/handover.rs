//! The **handover of the register** to the next matchmaker set (§J3.4;
//! `specs/tla/segment-records/`: `Stop`, `Close`, `Bootstrap`, `Decide`,
//! `Activate`).
//!
//! ```text
//! Stopping       Stop -> every member of the old set; wait for a majority
//!   |            of Stopped. The successor state is the highest write of
//!   v            that majority and its highest read generation (CloseFrom "max").
//! Bootstrapping  Bootstrap -> every member of the next set; wait for all.
//!   v
//! Deciding       the core's single-decree Paxos (`Decree`) over the old
//!   |            set, majority quorums, at a ballot above the round this
//!   v            handover used and above every promise the stop reported.
//! Publishing     Chosen -> the old and the next set; done at a majority of each.
//! ```
//!
//! Why it is safe (`segment-records/`, `Carried`): a write that is done
//! reached a majority of the old set, so it meets the stop majority, and the
//! successor takes the highest write of it. A frozen member takes no write
//! after it answered the stop (control `frozen_serves`), so the stop reports
//! stay true. Two handovers that propose different states are serialized by
//! the decree: the later one's Phase 1 sees the earlier vote and proposes
//! it. Taking one member's write in place of the highest breaks it (control
//! `close_one`).
//!
//! This is composition, not a second kernel: the decree is the core's
//! [`Decree`] and each member's half is the core's acceptor
//! (`member.rs`). The core's own matchmaker handover
//! (`paros_consensus::matchmaker::MatchmakerReconfigurer`) is unchanged. The
//! journal hands the register over beside it, to the same next set
//! (§J3.4).
//!
//! The durable state is the decree round: the caller persists
//! [`RegisterHandover::round`] before the requests of a new round leave,
//! and a new incarnation starts from it, so it never uses a decree ballot
//! of an earlier incarnation again (#1229 (a stale `Stopped` reply reuses a
//! decree ballot)).

use std::collections::{BTreeMap, BTreeSet};

use paros_consensus::decree::{AcceptFold, Decree, DecreePromise};
use paros_consensus::membership::{
    AcceptorConfig, MatchmakerGeneration, MatchmakerId, MatchmakerSet, QuorumSystem,
};
use paros_consensus::types::Fingerprint;
use paros_consensus::{Ballot, NodeId};

use super::message::{Reply, Request, Successor, Written};

/// The decree of a handover: over the members of the old set.
pub type HandoverDecree = Decree<MatchmakerId, Successor>;

/// Where a handover stands.
#[derive(Clone, Debug)]
pub enum HandoverPhase {
    /// Nothing running.
    Idle,
    /// Freezing the old set.
    Stopping {
        /// The set that hands over.
        old: MatchmakerSet,
        /// The members of the next set.
        target: Vec<MatchmakerId>,
        /// The stop reports so far: the highest write and read generation.
        stopped: BTreeMap<MatchmakerId, (Written, Ballot)>,
        /// The highest decree promise the stop reported.
        floor: Ballot,
    },
    /// Storing the proposed state at every member of the next set.
    Bootstrapping {
        /// The set that hands over.
        old: MatchmakerSet,
        /// The proposed state.
        successor: Successor,
        /// The members that stored it.
        acks: BTreeSet<MatchmakerId>,
        /// The highest decree promise the stop reported.
        floor: Ballot,
    },
    /// The decree over the old set.
    Deciding {
        /// The set that hands over.
        old: MatchmakerSet,
        /// The state this handover proposes.
        proposal: Successor,
        /// The decree in flight.
        decree: Box<HandoverDecree>,
    },
    /// Telling both sets the chosen state.
    Publishing {
        /// The set that handed over.
        old: MatchmakerSet,
        /// The chosen state.
        successor: Successor,
        /// The members that learned it.
        learned: BTreeSet<MatchmakerId>,
    },
}

/// What one reply did to a handover.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HandoverStep {
    /// Nothing the caller acts on.
    Pending,
    /// The stop majority answered: the handover proposes this state.
    Closed(Successor),
    /// The decree opened a new round: persist [`RegisterHandover::round`]
    /// before its requests leave.
    Round(u64),
    /// The decree chose this state; it is now published.
    Chosen(Successor),
    /// A majority of each set learned the chosen state: the handover is
    /// over.
    Done(Successor),
    /// A member is in a later set already: another handover won. This one
    /// is over.
    Superseded(MatchmakerSet),
}

/// Why a handover does not start.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HandoverRefusal {
    /// A handover runs here already.
    Busy,
    /// The next set names no member.
    Empty,
}

/// The handover of the register from one matchmaker set to the next.
#[derive(Clone, Debug)]
pub struct RegisterHandover {
    node: NodeId,
    round: u64,
    phase: HandoverPhase,
}

impl RegisterHandover {
    /// The handover role of `node`, which used the decree rounds up to
    /// `round` in its earlier incarnations (0 for none).
    #[must_use]
    pub fn new(node: NodeId, round: u64) -> Self {
        Self {
            node,
            round,
            phase: HandoverPhase::Idle,
        }
    }

    /// The highest decree round this role used: durable.
    #[must_use]
    pub fn round(&self) -> u64 {
        self.round
    }

    /// Where it stands.
    #[must_use]
    pub fn phase(&self) -> &HandoverPhase {
        &self.phase
    }

    /// Start to hand `old` over to the members `target`.
    ///
    /// # Errors
    ///
    /// When a handover runs already, or `target` is empty.
    pub fn start(
        &mut self,
        old: MatchmakerSet,
        target: Vec<MatchmakerId>,
    ) -> Result<(), HandoverRefusal> {
        if !matches!(self.phase, HandoverPhase::Idle) {
            return Err(HandoverRefusal::Busy);
        }
        if target.is_empty() {
            return Err(HandoverRefusal::Empty);
        }
        self.phase = HandoverPhase::Stopping {
            old,
            target,
            stopped: BTreeMap::new(),
            floor: Ballot::zero(),
        };
        Ok(())
    }

    /// Give the handover up (the caller lost interest or saw it stall). A
    /// later handover redoes every step; the decree votes stay at the
    /// members.
    pub fn abandon(&mut self) {
        self.phase = HandoverPhase::Idle;
    }

    /// The requests of the phase in flight, to the members that did not
    /// answer it: what the caller sends, and sends again on a timeout.
    #[must_use]
    pub fn requests(&self) -> Vec<(MatchmakerId, Request)> {
        match &self.phase {
            HandoverPhase::Idle => Vec::new(),
            HandoverPhase::Stopping { old, stopped, .. } => old
                .members()
                .iter()
                .filter(|member| !stopped.contains_key(member))
                .map(|member| {
                    (
                        *member,
                        Request::Stop {
                            generation: old.generation,
                        },
                    )
                })
                .collect(),
            HandoverPhase::Bootstrapping {
                successor, acks, ..
            } => successor
                .set
                .members()
                .iter()
                .filter(|member| !acks.contains(member))
                .map(|member| {
                    (
                        *member,
                        Request::Bootstrap {
                            successor: successor.clone(),
                        },
                    )
                })
                .collect(),
            HandoverPhase::Deciding { old, decree, .. } => {
                let generation = old.generation;
                let ballot = decree.ballot();
                decree
                    .unanswered()
                    .into_iter()
                    .map(|member| {
                        let request = match decree.value() {
                            None => Request::Prepare { generation, ballot },
                            Some(successor) => Request::Accept {
                                generation,
                                ballot,
                                successor: successor.clone(),
                            },
                        };
                        (member, request)
                    })
                    .collect()
            }
            HandoverPhase::Publishing {
                old,
                successor,
                learned,
            } => old
                .members()
                .iter()
                .chain(successor.set.members())
                .collect::<BTreeSet<_>>()
                .into_iter()
                .filter(|member| !learned.contains(member))
                .map(|member| {
                    (
                        *member,
                        Request::Chosen {
                            successor: successor.clone(),
                        },
                    )
                })
                .collect(),
        }
    }

    /// Fold the reply of member `from`.
    ///
    /// # Panics
    ///
    /// When an assertion on its own invariants or postconditions fails: a
    /// programmer error, never an operating condition.
    pub fn on_reply(&mut self, from: MatchmakerId, reply: Reply) -> HandoverStep {
        if let Reply::Refused { set, successor, .. } = &reply
            && let Some(old) = self.old()
        {
            let later = successor
                .clone()
                .filter(|next| next.generation > old.generation)
                .or_else(|| (set.generation > old.generation).then(|| set.clone()));
            let publishing = matches!(self.phase, HandoverPhase::Publishing { .. });
            if let Some(later) = later
                && !publishing
            {
                self.phase = HandoverPhase::Idle;
                return HandoverStep::Superseded(later);
            }
        }
        match reply {
            Reply::Stopped {
                generation,
                written,
                read,
                promised,
            } => self.on_stopped(from, generation, written, read, promised),
            Reply::Bootstrapped { successor } => self.on_bootstrapped(from, successor),
            Reply::Promised { ballot, vote } => self.on_promised(from, ballot, vote),
            Reply::Accepted { ballot } => self.on_accepted(from, ballot),
            Reply::Nacked { ballot, promised } => self.on_nacked(ballot, promised),
            Reply::Learned { generation } => self.on_learned(from, generation),
            _ => HandoverStep::Pending,
        }
    }

    fn old(&self) -> Option<&MatchmakerSet> {
        match &self.phase {
            HandoverPhase::Idle => None,
            HandoverPhase::Stopping { old, .. }
            | HandoverPhase::Bootstrapping { old, .. }
            | HandoverPhase::Deciding { old, .. }
            | HandoverPhase::Publishing { old, .. } => Some(old),
        }
    }

    fn on_stopped(
        &mut self,
        from: MatchmakerId,
        generation: MatchmakerGeneration,
        written: Written,
        read: Ballot,
        promised: Ballot,
    ) -> HandoverStep {
        let HandoverPhase::Stopping {
            old,
            target,
            stopped,
            floor,
        } = &mut self.phase
        else {
            return HandoverStep::Pending;
        };
        if generation != old.generation || !old.contains(from) {
            return HandoverStep::Pending;
        }
        stopped.insert(from, (written, read));
        *floor = (*floor).max(promised);
        let voters: BTreeSet<MatchmakerId> = stopped.keys().copied().collect();
        if !old.has_quorum(&voters) {
            return HandoverStep::Pending;
        }
        // Close: the highest write of the stop majority, and its highest
        // read generation.
        let (top, _) = stopped
            .values()
            .max_by_key(|(written, _)| written.op)
            .expect("a quorum stopped");
        assert!(
            stopped
                .values()
                .filter(|(written, _)| written.op == top.op)
                .all(|(written, _)| written.value == top.value),
            "one write generation holds one value"
        );
        let successor = Successor {
            set: MatchmakerSet::new(
                MatchmakerGeneration(old.generation.0 + 1),
                std::mem::take(target),
            ),
            written: top.clone(),
            read: stopped
                .values()
                .map(|(_, read)| *read)
                .max()
                .expect("a quorum stopped"),
        };
        assert!(
            stopped.values().all(
                |(written, read)| written.op <= successor.written.op && *read <= successor.read
            ),
            "the successor takes the highest write and read generation"
        );
        self.phase = HandoverPhase::Bootstrapping {
            old: old.clone(),
            successor: successor.clone(),
            acks: BTreeSet::new(),
            floor: *floor,
        };
        HandoverStep::Closed(successor)
    }

    fn on_bootstrapped(&mut self, from: MatchmakerId, fingerprint: u64) -> HandoverStep {
        let HandoverPhase::Bootstrapping {
            old,
            successor,
            acks,
            floor,
        } = &mut self.phase
        else {
            return HandoverStep::Pending;
        };
        if fingerprint != successor.fingerprint() || !successor.set.contains(from) {
            return HandoverStep::Pending;
        }
        acks.insert(from);
        if !successor.set.has_all(acks) {
            return HandoverStep::Pending;
        }
        let old = old.clone();
        let proposal = successor.clone();
        let floor = *floor;
        self.open(old, proposal, floor)
    }

    /// Open a decree round strictly above every round this role used and
    /// above `floor`.
    fn open(&mut self, old: MatchmakerSet, proposal: Successor, floor: Ballot) -> HandoverStep {
        let round = self.round.max(floor.round) + 1;
        let ballot = Ballot {
            round,
            node: self.node,
        };
        assert!(ballot > floor, "a decree round opens above the floor");
        assert!(round > self.round, "a decree round is never used twice");
        self.round = round;
        let acceptors = AcceptorConfig::new(old.members().to_vec(), QuorumSystem::Majority);
        let decree = Decree::new(ballot, acceptors, proposal.clone());
        self.phase = HandoverPhase::Deciding {
            old,
            proposal,
            decree: Box::new(decree),
        };
        HandoverStep::Round(round)
    }

    fn on_promised(
        &mut self,
        from: MatchmakerId,
        ballot: Ballot,
        vote: Option<(Ballot, Successor)>,
    ) -> HandoverStep {
        let HandoverPhase::Deciding { decree, .. } = &mut self.phase else {
            return HandoverStep::Pending;
        };
        if ballot != decree.ballot() {
            return HandoverStep::Pending;
        }
        // A quorum opens Phase 2 with the value P2c selects: `requests`
        // then sends its `Accept`.
        let _: DecreePromise<Successor> = decree.on_promise(from, vote);
        HandoverStep::Pending
    }

    fn on_accepted(&mut self, from: MatchmakerId, ballot: Ballot) -> HandoverStep {
        let HandoverPhase::Deciding { old, decree, .. } = &mut self.phase else {
            return HandoverStep::Pending;
        };
        if ballot != decree.ballot() {
            return HandoverStep::Pending;
        }
        match decree.on_accepted(from) {
            AcceptFold::Chosen(successor) => {
                assert!(
                    successor.set.generation.0 == old.generation.0 + 1,
                    "the chosen state is one generation above the old set"
                );
                self.phase = HandoverPhase::Publishing {
                    old: old.clone(),
                    successor: successor.clone(),
                    learned: BTreeSet::new(),
                };
                HandoverStep::Chosen(successor)
            }
            AcceptFold::Ignored | AcceptFold::Counted { .. } => HandoverStep::Pending,
        }
    }

    fn on_nacked(&mut self, ballot: Ballot, promised: Ballot) -> HandoverStep {
        let HandoverPhase::Deciding {
            old,
            proposal,
            decree,
        } = &mut self.phase
        else {
            return HandoverStep::Pending;
        };
        if ballot != decree.ballot() || promised <= ballot {
            return HandoverStep::Pending;
        }
        decree.on_nack(promised);
        let floor = decree.preempted().expect("a nack preempts the decree");
        let old = old.clone();
        let proposal = proposal.clone();
        self.open(old, proposal, floor)
    }

    fn on_learned(&mut self, from: MatchmakerId, generation: MatchmakerGeneration) -> HandoverStep {
        let HandoverPhase::Publishing {
            old,
            successor,
            learned,
        } = &mut self.phase
        else {
            return HandoverStep::Pending;
        };
        if generation != successor.set.generation {
            return HandoverStep::Pending;
        }
        learned.insert(from);
        if old.has_quorum(learned) && successor.set.has_quorum(learned) {
            let successor = successor.clone();
            self.phase = HandoverPhase::Idle;
            HandoverStep::Done(successor)
        } else {
            HandoverStep::Pending
        }
    }
}

#[cfg(test)]
mod tests;
