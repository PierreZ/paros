//! The leadership's **standing Phase-2 authority**: the fence a fresh
//! leadership must cover before its inherited suffix is settled, and the
//! `CheckQuorum` window that keeps proving the authority still holds.
//!
//! Both die with the leadership ([`Proposer::abandon`]). They are one
//! **standalone tally**, [`Authority`], that the [`Proposer`] embeds and
//! delegates to, exactly as it embeds its Phase-2 [`Rounds`](super::Rounds):
//! none of it is a Paxos tally — it is what a *leadership* holds beside its
//! rounds. The tally only counts; how many ticks are too many, and what to
//! do when the window empties, stay with the wiring.
//!
//! The read-index rounds that once lived here retired with the read-index
//! path (#243): every read is a leaderless quorum read
//! ([`crate::quorum_read`]), which touches no leader state.

use std::collections::BTreeSet;

use super::Proposer;
use crate::membership::AcceptorConfig;
use crate::types::Slot;

/// The leadership's **standing authority**: the fence and the `CheckQuorum`
/// window (see the module doc). Volatile, like everything the proposer
/// holds: it dies whole with the leadership ([`Authority::clear`]).
#[derive(Clone, Debug)]
pub struct Authority<Id> {
    /// The fresh-leader fence (see [`Authority::fence`]).
    fence: Option<Slot>,
    /// `CheckQuorum` (#95): the distinct acceptors (incl. self) whose
    /// ballot-matching `HeartbeatAck` or `Accepted` arrived inside the
    /// current window.
    quorum_acked_by: BTreeSet<Id>,
    /// `CheckQuorum`: ticks since the window last closed with a quorum.
    quorum_elapsed: u64,
}

impl<Id> Default for Authority<Id> {
    fn default() -> Self {
        let authority = Self {
            fence: None,
            quorum_acked_by: BTreeSet::new(),
            quorum_elapsed: 0,
        };
        assert!(
            authority.fence.is_none(),
            "a default authority has no fence"
        );
        assert!(
            authority.quorum_elapsed == 0,
            "a default authority's window is fresh"
        );
        authority
    }
}

impl<Id: Copy + Ord> Authority<Id> {
    /// An authority with no fence and an empty window.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn new() -> Self {
        let authority = Self::default();
        assert!(authority.fence.is_none(), "a fresh authority has no fence");
        assert!(
            authority.quorum_acked_by.is_empty(),
            "a fresh authority has no ack"
        );
        authority
    }

    /// Drop the fence and the window: the authority dies whole with the
    /// leadership that held it.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    pub fn clear(&mut self) {
        *self = Self::default();
        assert!(self.fence.is_none(), "a cleared authority has no fence");
        assert!(
            self.quorum_acked_by.is_empty(),
            "a cleared authority has no ack"
        );
        assert!(
            self.quorum_elapsed == 0,
            "a cleared authority's window is fresh"
        );
    }

    // ---- the fence ----------------------------------------------------------

    /// The fresh-leader fence: the highest slot the winning prepare quorum
    /// reported (`next_slot - 1` at election, the inherited frontier after a
    /// handoff). Everything a previous leader may have acked sits at or below
    /// it (quorum intersection + the `Prepare` floor guard). The GC campaign
    /// counts chosen indices past it, and a handoff-installed leadership that
    /// cannot cover it resigns to an ordinary Phase 1.
    #[must_use]
    pub fn fence(&self) -> Option<Slot> {
        self.fence
    }

    /// Open a fresh leadership's authority: install its fence and start a
    /// fresh `CheckQuorum` window holding `own_vote` (the leader's own
    /// acceptor vote, absent when it is not a member of its own
    /// configuration).
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    pub fn open(&mut self, fence: Option<Slot>, own_vote: Option<Id>) {
        self.fence = fence;
        self.renew(own_vote);
        assert!(self.fence == fence, "an opened authority holds its fence");
        assert!(
            self.quorum_elapsed == 0,
            "an opened authority's window is fresh"
        );
    }

    // ---- the CheckQuorum window ---------------------------------------------

    /// Start the ack window again from `own_vote` (self is always reachable —
    /// when it is an acceptor at all).
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    pub fn renew(&mut self, own_vote: Option<Id>) {
        self.quorum_elapsed = 0;
        self.quorum_acked_by.clear();
        if let Some(me) = own_vote {
            self.quorum_acked_by.insert(me);
        }
        // A fresh window holds the leader's own vote and nothing else.
        assert!(
            self.quorum_acked_by.len() == usize::from(own_vote.is_some()),
            "a renewed window holds only the own vote"
        );
        assert!(
            self.quorum_elapsed == 0,
            "a renewed window starts at age zero"
        );
    }

    /// Credit `from` to the current ack window: an ack (a beat ack or an
    /// `Accepted`) at the leadership's own ballot is proof this peer can
    /// still reach us and has not promised past us.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    pub fn credit(&mut self, from: Id) {
        let before = self.quorum_acked_by.len();
        self.quorum_acked_by.insert(from);
        assert!(
            self.quorum_acked_by.contains(&from),
            "a credited peer is in the window"
        );
        assert!(
            self.quorum_acked_by.len() >= before,
            "a credit never shrinks the window"
        );
    }

    /// Advance the window's clock by one driver tick and report its new age.
    /// The caller owns the *policy* (how long a window may run); the
    /// proposer only counts, exactly as it does for the repair probe.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    pub fn tick(&mut self) -> u64 {
        let before = self.quorum_elapsed;
        self.quorum_elapsed = self.quorum_elapsed.saturating_add(1);
        assert!(self.quorum_elapsed > 0, "a ticked window has aged");
        assert!(
            self.quorum_elapsed >= before,
            "a window's age never decreases"
        );
        self.quorum_elapsed
    }

    /// Whether the window holds a **Phase-2** quorum of `config` — the
    /// leader's standing authority.
    ///
    /// Not a Phase-1 question: Phase 1 asks what an earlier ballot *could
    /// have chosen*, and a standing authority asks the opposite — that no
    /// later ballot has decided anything behind this leader's back. A
    /// Phase-2 quorum of this ballot's configuration that acked at this
    /// ballot answers it: every future Phase-1 quorum intersects it
    /// ([`crate::QuorumSystem::cross_intersects`]), so a successor's election
    /// must meet an acceptor that still held this ballot's promise. Under a
    /// flexible quorum system that is a strictly weaker requirement than a
    /// Phase-1 quorum, which is exactly why the tag matters.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn holds(&self, config: &AcceptorConfig<Id>) -> bool {
        let holds = config.has_phase2_quorum(&self.quorum_acked_by);
        if holds {
            assert!(
                !self.quorum_acked_by.is_empty(),
                "a held authority rests on acks"
            );
        }
        holds
    }
}

impl<Id: Copy + Ord, V> Proposer<Id, V> {
    // ---- the standing authority: delegated to the embedded `Authority` ----

    /// The leadership's standing authority, whole.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn authority(&self) -> &Authority<Id> {
        // The fence lies below the frontier the leadership allocates from.
        if let Some(fence) = self.authority.fence() {
            assert!(
                fence < self.next_slot,
                "a leadership's fence lies below its frontier"
            );
            assert!(
                self.election.is_none(),
                "a campaign holds no standing authority"
            );
        }
        &self.authority
    }

    /// The fresh-leader fence ([`Authority::fence`]).
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn fence(&self) -> Option<Slot> {
        let fence = self.authority.fence();
        if let Some(fence) = fence {
            assert!(
                fence < self.next_slot,
                "a leadership's fence lies below its frontier"
            );
            assert!(self.election.is_none(), "a campaign holds no fence");
        }
        fence
    }

    /// Open a fresh leadership's authority ([`Authority::open`]).
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    pub fn open_authority(&mut self, fence: Option<Slot>, own_vote: Option<Id>) {
        // The fence is everything a predecessor could have acked: it sits
        // below the allocator frontier a fresh leadership installed first.
        if let Some(fence) = fence {
            assert!(
                fence < self.next_slot,
                "a leadership's fence lies below its frontier"
            );
        }
        self.authority.open(fence, own_vote);
        assert!(self.fence() == fence, "an opened authority holds its fence");
    }

    /// Start the `CheckQuorum` window again ([`Authority::renew`]).
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    pub fn renew_authority(&mut self, own_vote: Option<Id>) {
        assert!(
            self.election.is_none(),
            "a candidate has no authority to renew"
        );
        let fence = self.authority.fence();
        self.authority.renew(own_vote);
        assert!(
            self.authority.fence() == fence,
            "a renewed window keeps its fence"
        );
    }

    /// Credit `from` to the current window ([`Authority::credit`]): an ack
    /// at the leadership's own ballot. On a delegated round the votes are
    /// the proxy's and never reach this tally, so a leader whose rounds all
    /// run through proxies keeps its authority on `HeartbeatAck` alone.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    pub fn credit_authority(&mut self, from: Id) {
        assert!(
            self.election.is_none(),
            "a candidate has no authority to credit"
        );
        self.authority.credit(from);
    }

    /// Advance the window's clock by one tick and report its age
    /// ([`Authority::tick`]).
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    pub fn tick_authority(&mut self) -> u64 {
        assert!(self.election.is_none(), "only a leadership's window ages");
        let age = self.authority.tick();
        assert!(age > 0, "a ticked window has aged");
        age
    }

    /// Whether the window holds a Phase-2 quorum of `config`
    /// ([`Authority::holds`]).
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn authority_holds(&self, config: &AcceptorConfig<Id>) -> bool {
        assert!(
            config.is_well_formed(),
            "authority is judged over a well-formed configuration"
        );
        let holds = self.authority.holds(config);
        if holds {
            assert!(
                self.election.is_none(),
                "a campaign holds no standing authority"
            );
        }
        holds
    }
}
