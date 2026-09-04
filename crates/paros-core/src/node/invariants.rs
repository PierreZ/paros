//! The node's **cross-field invariant checker**: the one place that says what
//! a `ColocatedNode`'s volatile state may never look like.
//!
//! It runs unconditionally — TigerBeetle-style, no `debug_assert!` — at boot
//! and at the exit of every public mutating entry point, so a broken coupling
//! surfaces at the step that broke it rather than three messages later. Four
//! groups, each its own function: the ordering chain and the roles' own
//! checkers, the deployment couplings (plain Multi-Paxos versus a matchmaker
//! deployment), the per-role state machine, and the volatile leadership state
//! that may exist only on a leader.

use super::{Ballot, ColocatedNode, LeadershipOrigin, NodeRole};

impl ColocatedNode {
    /// Assert every cross-field invariant of the node's volatile state, plus
    /// each role's own. Most checks are O(1) or O(log n) (min-key probes);
    /// the role checkers add bounded structural scans over the retained log,
    /// the faulty set and the configuration in force. All of it runs
    /// unconditionally — TigerBeetle-style — at boot and at the exit of every
    /// public mutating entry point: the maps are small, and crash beats
    /// corruption.
    pub(super) fn assert_invariants(&self) {
        // Ordering chain: only chosen slots are ever dropped, so the compaction
        // floor never passes the first unchosen slot.
        assert!(
            self.acceptor.first_slot() <= self.first_unchosen(),
            "the compaction floor never outruns the chosen prefix"
        );
        // The replica's and the proposer's own maps against the acceptor's
        // floor.
        self.acceptor.assert_invariants();
        self.replica.assert_invariants(self.acceptor.first_slot());
        self.proposer.assert_invariants();
        // The repair clock lives inside the probe it times, so closing a
        // probe — by a decision, a commit, a snapshot install or an
        // abandoned leadership — takes the clock with it and no path has to
        // remember to reset one. What the wiring owns is who advances it.
        assert!(
            self.proposer
                .probe_elapsed()
                .is_none_or(|elapsed| elapsed == 0 || self.role == NodeRole::Leader),
            "only a leader's repair probe ages"
        );
        // The cross-role couplings against the acceptor's floor: a compaction
        // and a snapshot install both retain the rounds above the floor they
        // raise, so an in-flight Phase-2 round below it would address a slot
        // whose record is gone.
        assert!(
            self.proposer
                .rounds()
                .keys()
                .next()
                .is_none_or(|s| *s >= self.acceptor.first_slot()),
            "no in-flight round survives below the compaction floor"
        );
        self.assert_deployment_invariants();
        self.assert_role_invariants();
        self.assert_leadership_state_invariants();
    }

    /// The deployment couplings: plain Multi-Paxos never matchmakes and never
    /// leaves its bootstrap configuration; a matchmaker deployment runs
    /// Phase 2 under a configuration drawn from the pool.
    fn assert_deployment_invariants(&self) {
        // NOT a global invariant: a still-Leader node can learn a higher-ballot
        // `Commit` (raising its promise via `mark_chosen`) before any deposing
        // message arrives — `start_accept_round`'s self-accept guard is the
        // designed defense. It holds only for a *fresh* leader
        // (see `try_become_leader`).
        // The deployment couplings: plain Multi-Paxos never matchmakes and
        // never leaves its bootstrap configuration; a matchmaker deployment
        // runs Phase 2 under a configuration drawn from the pool.
        if self.config.has_matchmakers() {
            assert!(
                !self.matchmakers.members.is_empty(),
                "a matchmaker deployment always believes in a matchmaker set"
            );
        } else {
            assert!(
                self.matchmaking.is_none(),
                "a plain deployment never opens a matchmaking phase"
            );
            assert!(
                self.acceptors.members() == self.config.peers,
                "a plain deployment keeps its bootstrap configuration"
            );
            assert!(
                self.acceptors_since == Ballot::zero(),
                "a plain deployment's configuration is bound to no ballot"
            );
            assert!(
                self.matchmakers.members.is_empty(),
                "a plain deployment names no matchmaker set"
            );
            assert!(
                self.gc.is_none(),
                "a plain deployment never opens a GC campaign"
            );
        }
        if self.gc.is_some() {
            assert!(
                self.role == NodeRole::Leader,
                "only a leader holds an open GC campaign"
            );
        }
        assert!(
            self.acceptors.members().iter().all(|m| self.in_pool(*m)),
            "the active configuration is drawn from the node pool"
        );
        // The retirement fence (#123): `last_member_ballot` is the highest
        // `acceptors_since` of every configuration that named this node, so
        // while this node *is* a member it is at least the ballot its
        // configuration is bound to. It may sit *above* it: adopting an
        // honored reconfiguration binds the node to that reconfiguration's
        // own, possibly older, ballot (`node/matchmaking.rs`), and the fence
        // keeps the newer membership it already recorded — `may_retire`
        // needs the maximum, never the current binding.
        assert!(
            !self.is_acceptor() || self.last_member_ballot >= self.acceptors_since,
            "a member's fence is at least the ballot its configuration is bound to"
        );
    }

    /// The per-role state machine: what a leader, a candidate and a follower
    /// may each hold.
    fn assert_role_invariants(&self) {
        match self.role {
            NodeRole::Leader => {
                assert!(
                    self.proposer.election().is_none(),
                    "a leader has no open campaign"
                );
                assert!(
                    self.matchmaking.is_none(),
                    "a leader has no open matchmaking phase"
                );
                assert!(
                    self.leader == Some(self.config.id),
                    "a leader knows itself as leader"
                );
                // Whose node id the operating ballot names is exactly what
                // separates the two leadership origins — an elected leader owns
                // its ballot, a handoff leader is exercising a predecessor's.
                match self.leadership_origin {
                    LeadershipOrigin::Elected => assert!(
                        self.ballot.node == self.config.id,
                        "an elected leader's ballot names its own node"
                    ),
                    LeadershipOrigin::Handoff { from } => {
                        // The ballot names whoever *minted* it, which after a
                        // chain of handoffs is neither this node nor its
                        // immediate predecessor — so only the predecessor is
                        // pinned here, and it is always someone else.
                        assert!(
                            from != self.config.id,
                            "a handoff leader inherited its authority from another node"
                        );
                        assert!(
                            self.in_pool(from),
                            "a handoff leader inherited from a pooled node"
                        );
                    }
                }
                // The #67/#88 allocator bound, gated exactly like the note
                // above: a still-Leader that learned a higher-ballot `Commit`
                // (or replayed catch-up decided past it) can see the chosen
                // prefix pass its allocator before any deposing message
                // arrives — but while its ballot still covers its own promise,
                // quorum intersection guarantees the winning Phase 1 reported
                // everything decided, so the allocator sits at or past the
                // prefix.
                if self.ballot >= self.acceptor.promised() {
                    assert!(
                        self.proposer.next_slot() >= self.first_unchosen(),
                        "a leader's next slot never falls inside the chosen prefix"
                    );
                }
                assert!(
                    self.proposer
                        .rounds()
                        .keys()
                        .next_back()
                        .is_none_or(|s| *s < self.proposer.next_slot()),
                    "a leader never allocates at or below an in-flight round"
                );
                // Every in-flight round runs at the leadership ballot: rounds
                // are opened only by this leader, and every promise-raising
                // path that could strand one demotes (clearing `proposer`)
                // first. O(N) structural, always-on by choice.
                assert!(
                    self.proposer
                        .rounds()
                        .values()
                        .all(|p| p.ballot() == self.ballot),
                    "a leader's in-flight rounds all run at its own ballot"
                );
            }
            NodeRole::Candidate => {
                // Exactly one campaign phase is open: matchmaking (the
                // registration round trip, #120) or Phase 1 — never both,
                // never neither. The boundary between them is
                // `start_phase1`.
                assert!(
                    self.proposer.election().is_some() != self.matchmaking.is_some(),
                    "a candidate holds exactly one open campaign phase"
                );
                assert!(
                    self.proposer
                        .election()
                        .is_none_or(|e| e.ballot() == self.ballot),
                    "a candidate's campaign runs at its own operating ballot"
                );
                assert!(
                    self.matchmaking
                        .as_ref()
                        .is_none_or(|m| m.ballot() == self.ballot),
                    "a candidate's matchmaking runs at its own operating ballot"
                );
                assert!(
                    self.ballot.node == self.config.id,
                    "an operating ballot names its own node"
                );
            }
            NodeRole::Follower => {
                assert!(
                    self.proposer.election().is_none(),
                    "a follower has no open campaign"
                );
                assert!(
                    self.matchmaking.is_none(),
                    "a follower has no open matchmaking phase"
                );
            }
        }
    }

    /// Volatile leadership state exists only on a leader.
    fn assert_leadership_state_invariants(&self) {
        // A leadership origin is leadership state: it is cleared with the rest
        // of it (`become_follower`), so only a leader ever carries a handoff.
        if self.role != NodeRole::Leader {
            assert!(
                self.leadership_origin == LeadershipOrigin::Elected,
                "only a leader carries an inherited leadership origin"
            );
        }
        // Volatile leadership state exists only on a leader.
        if !self.proposer.rounds().is_empty() {
            assert!(
                self.role == NodeRole::Leader,
                "only a leader holds in-flight accept rounds"
            );
        }
        if !self.proposer.read_rounds().is_empty() {
            assert!(
                self.role == NodeRole::Leader,
                "only a leader holds pending read rounds"
            );
        }
        if self.proposer.recovery().is_some() {
            assert!(
                self.role == NodeRole::Leader,
                "only a leader holds deferred recovery work"
            );
        }
        if self.proposer.probe().is_some() {
            assert!(
                self.role == NodeRole::Leader,
                "only a leader holds an open repair probe"
            );
        }
    }
}
