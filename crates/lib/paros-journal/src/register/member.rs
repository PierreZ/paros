//! One **member** of the journal's matchmaker set, as a register replica
//! (§J3.4; `specs/tla/cas-register/`, `specs/tla/segment-records/`).
//!
//! A member holds, for one journal, its read generation, the highest write
//! it took, the phase of its matchmaker generation and its half of the
//! handover's decree. It answers one [`Request`] with one [`Reply`]:
//!
//! - a read raises the read generation to the operation and answers the
//!   highest write;
//! - a write lands only if the read generation is at most the operation and
//!   the write generation is below it (the compare-and-set of
//!   `LocalGenerationReg`);
//! - a member serves reads and writes only of the generation it is active
//!   for. A stopped (frozen) member takes no read and no write, and still
//!   answers the decree of its handover (control `frozen_serves`);
//! - a member of the next set that learns the chosen state serves the next
//!   generation, and keeps its own write and read generation if they are
//!   higher (`Activate`).
//!
//! The decree half is the core's [`Acceptor`] over one slot, as the core's
//! matchmaker runs its own handover decree: no second Paxos kernel. A
//! member is a pure state machine: the caller persists
//! [`RegisterMember::state`] before it sends the reply of a step whose
//! [`MemberStep::persist`] is set.

use std::collections::BTreeMap;

use paros_consensus::Ballot;
use paros_consensus::acceptor::{AcceptOutcome, Acceptor, PrepareOutcome};
use paros_consensus::contract::write::AcceptorWrite;
use paros_consensus::decree::DECREE_SLOT;
use paros_consensus::membership::{MatchmakerGeneration, MatchmakerId, MatchmakerSet};
use paros_consensus::types::Fingerprint;

use super::message::{MemberPhase, Reply, Request, Successor, Written};

/// The durable state of one member, for one journal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MemberState {
    /// The set the member is in, or, for a spare, the set it was told of.
    pub set: MatchmakerSet,
    /// What the member does in `set`.
    pub phase: MemberPhase,
    /// The highest operation generation that read here.
    pub read: Ballot,
    /// The highest write taken here.
    pub written: Written,
    /// The decree promise of the handover of `set`.
    pub promised: Ballot,
    /// The decree vote of the handover of `set`.
    pub vote: Option<(Ballot, Successor)>,
    /// The proposed next state the member stored, if any.
    pub staged: Option<Successor>,
    /// The successor of `set`, once the member learned it.
    pub successor: Option<MatchmakerSet>,
}

impl MemberState {
    /// The state of matchmaker `id` at the start of a journal whose first
    /// set is `set`: active if `id` is in it, else a spare.
    #[must_use]
    pub fn first(id: MatchmakerId, set: MatchmakerSet) -> Self {
        let phase = if set.contains(id) {
            MemberPhase::Active
        } else {
            MemberPhase::Inactive
        };
        Self {
            set,
            phase,
            read: Ballot::zero(),
            written: Written::default(),
            promised: Ballot::zero(),
            vote: None,
            staged: None,
            successor: None,
        }
    }
}

/// What one step did: the reply, and whether the state changed and must be
/// durable before the reply leaves.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MemberStep {
    /// Persist [`RegisterMember::state`] before the reply leaves.
    pub persist: bool,
    /// The answer to the request.
    pub reply: Reply,
}

/// One member of the journal's matchmaker set, as a register replica.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RegisterMember {
    id: MatchmakerId,
    state: MemberState,
}

impl RegisterMember {
    /// Matchmaker `id` over the durable `state` it read back.
    ///
    /// # Panics
    ///
    /// When the state breaks a rule of [`RegisterMember::assert_invariants`]:
    /// a damaged store, never an operating condition.
    #[must_use]
    pub fn new(id: MatchmakerId, state: MemberState) -> Self {
        let member = Self { id, state };
        member.assert_invariants();
        member
    }

    /// The matchmaker.
    #[must_use]
    pub fn id(&self) -> MatchmakerId {
        self.id
    }

    /// The durable state.
    #[must_use]
    pub fn state(&self) -> &MemberState {
        &self.state
    }

    /// Whether the member serves reads and writes of `generation`.
    #[must_use]
    pub fn serves(&self, generation: MatchmakerGeneration) -> bool {
        self.state.phase == MemberPhase::Active
            && self.state.set.generation == generation
            && self.state.set.contains(self.id)
    }

    /// Whether the member answers the handover of `generation`: the set it
    /// is active or frozen in.
    fn hands_over(&self, generation: MatchmakerGeneration) -> bool {
        matches!(self.state.phase, MemberPhase::Active | MemberPhase::Frozen)
            && self.state.set.generation == generation
            && self.state.set.contains(self.id)
    }

    /// Answer one request.
    ///
    /// # Panics
    ///
    /// When an assertion on its own invariants or postconditions fails: a
    /// programmer error, never an operating condition. What a peer sends is
    /// answered, never asserted on.
    pub fn step(&mut self, request: Request) -> MemberStep {
        self.assert_invariants();
        let before = self.state.clone();
        let reply = match request {
            Request::Read { generation, op } => self.on_read(generation, op),
            Request::Write {
                generation,
                op,
                value,
            } => self.on_write(generation, op, value),
            Request::Stop { generation } => self.on_stop(generation),
            Request::Bootstrap { successor } => self.on_bootstrap(successor),
            Request::Prepare { generation, ballot } => self.on_prepare(generation, ballot),
            Request::Accept {
                generation,
                ballot,
                successor,
            } => self.on_accept(generation, ballot, successor),
            Request::Chosen { successor } => self.on_chosen(&successor),
        };
        self.assert_invariants();
        let after = &self.state;
        assert!(
            after.set.generation >= before.set.generation,
            "a member never moves back a generation"
        );
        assert!(after.read >= before.read, "the read generation never falls");
        assert!(
            after.written.op >= before.written.op,
            "the write generation never falls"
        );
        if before.phase == MemberPhase::Frozen && after.set.generation == before.set.generation {
            assert!(
                after.written == before.written && after.read == before.read,
                "a frozen member takes no read and no write"
            );
        }
        MemberStep {
            persist: *after != before,
            reply,
        }
    }

    fn refusal(&self) -> Reply {
        Reply::Refused {
            set: self.state.set.clone(),
            phase: self.state.phase,
            successor: self.state.successor.clone(),
        }
    }

    fn on_read(&mut self, generation: MatchmakerGeneration, op: Ballot) -> Reply {
        if !self.serves(generation) {
            return self.refusal();
        }
        self.state.read = self.state.read.max(op);
        assert!(self.state.read >= op, "a read raises the read generation");
        Reply::Value {
            op,
            written: self.state.written.clone(),
        }
    }

    fn on_write(
        &mut self,
        generation: MatchmakerGeneration,
        op: Ballot,
        value: super::RegisterValue,
    ) -> Reply {
        if !self.serves(generation) {
            return self.refusal();
        }
        if self.state.written.op == op && self.state.written.value == value {
            // The same write again: one operation writes one value.
            return Reply::Wrote { op };
        }
        if self.state.read <= op && self.state.written.op < op {
            self.state.written = Written { op, value };
            assert!(
                self.state.written.op == op,
                "the write lands at its generation"
            );
            Reply::Wrote { op }
        } else {
            assert!(
                self.state.read > op || self.state.written.op >= op,
                "a conflict names a later generation"
            );
            Reply::Conflict {
                op,
                read: self.state.read,
                written: self.state.written.op,
            }
        }
    }

    fn on_stop(&mut self, generation: MatchmakerGeneration) -> Reply {
        if !self.hands_over(generation) {
            return self.refusal();
        }
        self.state.phase = MemberPhase::Frozen;
        Reply::Stopped {
            generation,
            written: self.state.written.clone(),
            read: self.state.read,
            promised: self.state.promised,
        }
    }

    fn on_bootstrap(&mut self, successor: Successor) -> Reply {
        if !successor.set.contains(self.id) || self.state.set.generation >= successor.set.generation
        {
            return self.refusal();
        }
        let fingerprint = successor.fingerprint();
        self.state.staged = Some(successor);
        Reply::Bootstrapped {
            successor: fingerprint,
        }
    }

    /// This member's half of the handover's decree: the core's acceptor
    /// over the one slot of the decree.
    fn acceptor(&self) -> Acceptor<Successor> {
        let records = self
            .state
            .vote
            .clone()
            .map_or_else(BTreeMap::new, |vote| BTreeMap::from([(DECREE_SLOT, vote)]));
        Acceptor::new(self.state.promised, records, DECREE_SLOT, BTreeMap::new())
    }

    fn store(&mut self, acceptor: &Acceptor<Successor>) {
        assert!(
            acceptor.promised() >= self.state.promised,
            "the decree promise never falls"
        );
        self.state.promised = acceptor.promised();
        self.state.vote = acceptor.record(DECREE_SLOT).cloned();
    }

    fn on_prepare(&mut self, generation: MatchmakerGeneration, ballot: Ballot) -> Reply {
        if !self.hands_over(generation) {
            return self.refusal();
        }
        let mut acceptor = self.acceptor();
        let mut writes: Vec<AcceptorWrite<Successor>> = Vec::new();
        match acceptor.prepare(ballot, DECREE_SLOT, &mut writes) {
            PrepareOutcome::Promised { .. } => {
                self.store(&acceptor);
                Reply::Promised {
                    ballot,
                    vote: self.state.vote.clone(),
                }
            }
            PrepareOutcome::Refused | PrepareOutcome::BelowFloor => Reply::Nacked {
                ballot,
                promised: acceptor.promised(),
            },
        }
    }

    fn on_accept(
        &mut self,
        generation: MatchmakerGeneration,
        ballot: Ballot,
        successor: Successor,
    ) -> Reply {
        if !self.hands_over(generation) || successor.set.generation.0 != generation.0 + 1 {
            return self.refusal();
        }
        let mut acceptor = self.acceptor();
        let mut writes: Vec<AcceptorWrite<Successor>> = Vec::new();
        match acceptor.admit(ballot, DECREE_SLOT) {
            AcceptOutcome::Admitted => {
                acceptor.set_promise(ballot, &mut writes);
                acceptor.record_accepted(DECREE_SLOT, ballot, successor, &mut writes);
                self.store(&acceptor);
                Reply::Accepted { ballot }
            }
            AcceptOutcome::Refused | AcceptOutcome::BelowFloor => Reply::Nacked {
                ballot,
                promised: acceptor.promised(),
            },
        }
    }

    fn on_chosen(&mut self, successor: &Successor) -> Reply {
        let next = successor.set.generation;
        let mine = self.state.set.generation;
        if mine.0 + 1 == next.0 && self.state.set.contains(self.id) {
            self.state.successor = Some(successor.set.clone());
        }
        if successor.set.contains(self.id) && mine < next {
            // Activate: the member serves the next generation, and keeps its
            // own write and read generation where they are higher.
            let written = [&successor.written, &self.state.written]
                .into_iter()
                .max_by_key(|written| written.op)
                .expect("two writes")
                .clone();
            self.state = MemberState {
                set: successor.set.clone(),
                phase: MemberPhase::Active,
                read: self.state.read.max(successor.read),
                written,
                promised: Ballot::zero(),
                vote: None,
                staged: None,
                successor: None,
            };
        }
        let learned = self.state.set.generation >= next
            || self
                .state
                .successor
                .as_ref()
                .is_some_and(|set| set.generation == next);
        if learned {
            Reply::Learned { generation: next }
        } else {
            self.refusal()
        }
    }

    /// The member's own rules.
    ///
    /// # Panics
    ///
    /// When a member outside its set is not a spare, a spare holds a
    /// decree state, a successor is not one generation above the set, or a
    /// vote stands above the promise.
    pub fn assert_invariants(&self) {
        let state = &self.state;
        assert!(
            (state.phase == MemberPhase::Inactive) != state.set.contains(self.id),
            "a member of its set is active or frozen, any other is a spare"
        );
        assert!(
            state
                .successor
                .as_ref()
                .is_none_or(|next| next.generation.0 == state.set.generation.0 + 1),
            "a successor is one generation above the set"
        );
        assert!(
            state
                .vote
                .as_ref()
                .is_none_or(|(ballot, _)| *ballot <= state.promised),
            "a decree vote never stands above the promise"
        );
        assert!(
            state
                .staged
                .as_ref()
                .is_none_or(|next| next.set.generation > state.set.generation),
            "a staged state is for a later generation"
        );
    }
}

#[cfg(test)]
mod tests;
