//! The node's **read-only accessors**: what a driver, an oracle or an
//! example reads, never a mutation.

use super::{
    Acceptor, AcceptorConfig, Ballot, BeliefSource, ColocatedNode, Command, Config, HardState,
    LeadershipOrigin, Limits, MatchmakerId, MembershipProbe, NodeId, NodeRole, Proposer, ProxyId,
    QuorumReads, ReadBasis, Replica, Seq, Slot,
};

impl ColocatedNode {
    /// The slots this leader currently holds **delegated** to a proxy, with
    /// the proxy each went to — for drivers / oracles (a node that is not the
    /// leader holds none).
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn delegated_rounds(&self) -> Vec<(Slot, ProxyId)> {
        let delegated: Vec<(Slot, ProxyId)> = self
            .proposer
            .rounds()
            .iter()
            .filter_map(|(slot, round)| round.proxy().map(|proxy| (*slot, proxy)))
            .collect();
        // Delegation is the opt-in of a leader on a deployment with proxies.
        if !delegated.is_empty() {
            assert!(
                self.role == NodeRole::Leader,
                "only a leader holds delegated rounds"
            );
            assert!(
                self.config.proxy_count > 0,
                "only a proxy deployment delegates"
            );
        }
        delegated
    }

    /// Whether this leader has Phase-2 rounds whose `Accept`s can be re-sent.
    /// Drivers use this to avoid consulting optional policy hooks when skipping
    /// a re-send would have no observable effect.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn has_pending_accepts(&self) -> bool {
        let pending = self.role == NodeRole::Leader && !self.proposer.rounds().is_empty();
        // Rounds outlive no leadership: a non-leader holds none to re-send.
        if self.role != NodeRole::Leader {
            assert!(
                self.proposer.rounds().is_empty(),
                "a non-leader holds no round"
            );
        }
        pending
    }

    /// The page sizes and the read window this node runs under, fixed at
    /// boot. The read window is the one the core keeps: the driver's,
    /// raised to its floor.
    #[must_use]
    pub fn limits(&self) -> Limits {
        self.limits
    }

    /// The election timeout in force (in ticks; zero until the driver set
    /// one) — the unit the driver paces its other timeouts in.
    #[must_use]
    pub fn election_timeout(&self) -> u64 {
        self.election_timeout
    }

    /// Ticks since the election clock last reset — how far toward
    /// [`ColocatedNode::election_timeout`] the timer has run. Read-only: the
    /// clock resets wherever the core hears from a live leadership or starts
    /// a campaign, and a caller that wants to draw it reads it here rather
    /// than re-deriving those reset sites.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn election_elapsed(&self) -> u64 {
        // A leader's election clock is stopped at zero: only a follower or a
        // candidate counts toward a timeout.
        if self.role == NodeRole::Leader {
            assert!(
                self.election_elapsed == 0,
                "a leader's election clock is stopped"
            );
        }
        self.election_elapsed
    }

    /// This node's static configuration (identity, bootstrap membership, pool
    /// and matchmaker set).
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn config(&self) -> &Config {
        assert!(
            self.config.pool().binary_search(&self.config.id).is_ok(),
            "a node's boot pool names the node itself"
        );
        assert!(
            self.config.has_matchmakers() == self.matchmakers.is_some(),
            "a matchmaker set is believed exactly on a matchmaker deployment"
        );
        &self.config
    }

    /// The addressable pool in force (#189): `Config::pool`, plus every node
    /// [`ColocatedNode::extend_pool`] admitted since this incarnation booted.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn pool(&self) -> &[NodeId] {
        assert!(
            self.pool.windows(2).all(|w| w[0] < w[1]),
            "the pool is sorted and deduplicated"
        );
        assert!(
            self.pool.len() >= self.config.pool().len(),
            "the pool only grows from boot"
        );
        &self.pool
    }

    /// The matchmaker pool in force (#501 (matchmaker sets off the
    /// founders)): `Config::matchmaker_pool`, plus every matchmaker
    /// [`ColocatedNode::extend_matchmaker_pool`] admitted since this
    /// incarnation booted.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants fails: a programmer error.
    #[must_use]
    pub fn matchmaker_pool(&self) -> &[MatchmakerId] {
        assert!(
            self.matchmaker_pool.windows(2).all(|w| w[0] < w[1]),
            "the matchmaker pool is sorted and deduplicated"
        );
        assert!(
            self.matchmaker_pool.len() >= self.config.matchmaker_pool().len(),
            "the matchmaker pool only grows from boot"
        );
        &self.matchmaker_pool
    }

    /// The acceptor configuration in force for the highest ballot this node
    /// has seen registered: on a leader, the one its Phase 2 quorums are
    /// counted over; on a follower, its belief about the latest one. The
    /// bootstrap configuration on plain Multi-Paxos, always.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn acceptors(&self) -> &AcceptorConfig {
        assert!(
            self.acceptors.is_drawn_from(&self.pool),
            "the configuration in force is drawn from the pool"
        );
        if !self.config.has_matchmakers() {
            assert!(
                self.acceptors.members() == self.config.peers,
                "a plain deployment runs its bootstrap membership"
            );
        }
        &self.acceptors
    }

    /// The ballot [`ColocatedNode::acceptors`] was registered under
    /// (`Ballot::zero()` for the bootstrap configuration).
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn acceptors_since(&self) -> Ballot {
        if self.acceptors_since != Ballot::zero() {
            assert!(
                self.belief_source == BeliefSource::Heard,
                "a configuration bound to a ballot is a heard belief"
            );
        }
        self.acceptors_since
    }

    /// The basis this node's quorum reads are judged over (#260,
    /// [`ReadBasis`]), or `None` when it may open none: on a matchmaker
    /// deployment, before any won leadership was heard this incarnation, or
    /// while its own belief is bound above the basis — it promised a newer
    /// campaign, which may have chosen under a configuration the basis's
    /// quorums need not meet. On plain Multi-Paxos, always the static
    /// configuration with no fence.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn read_basis(&self) -> Option<ReadBasis<NodeId>> {
        if !self.config.has_matchmakers() {
            assert!(
                self.read_basis.is_none(),
                "a plain node stores no read basis"
            );
            return Some(ReadBasis {
                config: self.acceptors.clone(),
                since: self.acceptors_since,
                fence: None,
            });
        }
        let basis = self
            .read_basis
            .clone()
            .filter(|basis| basis.since >= self.acceptors_since);
        if let Some(basis) = &basis {
            assert!(
                basis.config.is_drawn_from(&self.pool),
                "a read basis is drawn from the pool"
            );
        }
        basis
    }

    /// Where [`ColocatedNode::acceptors`] came from: the bootstrap default,
    /// or something this incarnation heard (#173, [`BeliefSource`]).
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn belief_source(&self) -> BeliefSource {
        if self.belief_source == BeliefSource::Bootstrap {
            assert!(
                self.acceptors_since == Ballot::zero(),
                "the bootstrap belief is bound to no ballot"
            );
        }
        self.belief_source
    }

    /// The open membership probe, if any (#173) — read-only, like
    /// [`ColocatedNode::matchmaking_role`].
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn membership_probe(&self) -> Option<&MembershipProbe> {
        if self.probe.is_some() {
            assert!(
                self.config.has_matchmakers(),
                "only a matchmaker deployment probes"
            );
            assert!(
                self.matchmaking.is_none(),
                "a probe never overlaps a campaign"
            );
        }
        self.probe.as_deref()
    }

    /// The membership fence [`ColocatedNode::may_retire`] reads: the highest
    /// ballot a configuration naming this node was bound to, as far as this
    /// incarnation heard (volatile; `Ballot::zero()` at boot).
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn last_member_ballot(&self) -> Ballot {
        if self.is_acceptor() {
            assert!(
                self.last_member_ballot >= self.acceptors_since,
                "a member's fence covers its configuration's ballot"
            );
        }
        self.last_member_ballot
    }

    /// Whether this node is a member of its active configuration
    /// ([`ColocatedNode::acceptors`]) — a real acceptor whose own vote counts.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn is_acceptor(&self) -> bool {
        let member = self.acceptors.contains(self.config.id);
        if member {
            assert!(
                self.in_pool(self.config.id),
                "a member of the configuration is pooled"
            );
        }
        if !self.config.has_matchmakers() {
            assert!(member, "a plain deployment's node is always a member");
        }
        member
    }

    /// The current durable scalars (promised ballot, chosen
    /// index), composed from the components that own them.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn hard_state(&self) -> HardState {
        let hard_state = HardState {
            max_promised_ballot: self.acceptor.promised(),
            chosen_index: self.replica.chosen_index(),
        };
        // The two scalars as the boot read-back will see them: a chosen index
        // never lags the compaction floor, and every record sits under the
        // promise (`Acceptor::assert_invariants` is the other half).
        assert!(
            self.acceptor.first_slot() <= self.replica.first_unchosen(),
            "the durable floor never outruns the durable chosen index"
        );
        assert!(
            self.acceptor
                .records()
                .values()
                .next_back()
                .is_none_or(|(b, _)| *b <= hard_state.max_promised_ballot),
            "the durable promise dominates the last record"
        );
        hard_state
    }

    /// The node's **acceptor** role: the durable promise, the per-slot
    /// accepted log, the compaction floor and the CTRL tri-state. A read
    /// view for drivers and oracles; every write goes through the [`crate::Ready`]
    /// batch this node emits.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn acceptor(&self) -> &Acceptor<Command> {
        // The cross-role coupling a reader of the acceptor relies on.
        assert!(
            self.acceptor.first_slot() <= self.first_unchosen(),
            "the compaction floor never outruns the chosen prefix"
        );
        assert!(
            self.acceptor
                .faulty()
                .keys()
                .all(|s| *s >= self.acceptor.first_slot()),
            "no faulty record survives below the floor"
        );
        &self.acceptor
    }

    /// The node's **replica** role: the chosen log, the contiguous apply
    /// walk and the journal state it folds. A
    /// read view, like [`ColocatedNode::acceptor`].
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn replica(&self) -> &Replica {
        assert!(
            self.replica.folded() >= self.acceptor.first_slot(),
            "the journal fold starts at the compaction floor"
        );
        assert!(
            self.replica.folded() <= self.replica.first_unchosen(),
            "the journal fold never passes the chosen prefix"
        );
        &self.replica
    }

    /// A **journal read** (#204) from this node's journal fold:
    /// [`Replica::read`]. A pure read — nothing in the node moves.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn read_log(&self, from: Seq, limit: usize, max_bytes: usize) -> crate::replica::LogRead {
        let read = self.replica.read(from, limit, max_bytes);
        // A read is served from the fold's head, as it stands.
        if let crate::replica::LogRead::Page(page) = &read {
            assert!(
                page.state == self.replica.journal().view(),
                "a page names the fold's head"
            );
            assert!(page.from == from, "a page starts where the read asked");
        }
        read
    }

    /// The node's **proposer** role: the open Phase 1, the CTRL repair
    /// probe, the in-flight Phase-2 rounds, the allocator frontier and the
    /// leadership's standing authority. A read view, like
    /// [`ColocatedNode::acceptor`].
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn proposer(&self) -> &Proposer<NodeId, Command> {
        if self.role != NodeRole::Leader {
            assert!(
                self.proposer.rounds().is_empty(),
                "a non-leader holds no round"
            );
            assert!(
                self.proposer.recovery().is_none(),
                "a non-leader holds no recovery"
            );
        }
        &self.proposer
    }

    /// The quorum reads this node has open (#143, [`crate::quorum_read`]),
    /// for drivers / oracles.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn quorum_reads(&self) -> &QuorumReads<NodeId> {
        assert!(
            self.quorum_reads
                .pending()
                .iter()
                .all(|r| r.config().is_drawn_from(&self.pool)),
            "an open read asks a configuration drawn from the pool"
        );
        &self.quorum_reads
    }

    /// This node's current role.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn role(&self) -> NodeRole {
        match self.role {
            NodeRole::Leader => {
                assert!(
                    self.leader == Some(self.config.id),
                    "a leader knows itself as leader"
                );
            }
            NodeRole::Candidate => {
                assert!(self.leader.is_none(), "a candidate follows no leader");
                assert!(
                    self.ballot.node == self.config.id,
                    "a candidate runs its own ballot"
                );
            }
            NodeRole::Follower => {}
        }
        self.role
    }

    /// The node this one believes is leader, if any.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn leader(&self) -> Option<NodeId> {
        if self.role == NodeRole::Leader {
            assert!(
                self.leader == Some(self.config.id),
                "a leader knows itself as leader"
            );
        }
        if self.role == NodeRole::Candidate {
            assert!(self.leader.is_none(), "a candidate follows no leader");
        }
        self.leader
    }

    /// Whether this node is currently the leader.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn is_leader(&self) -> bool {
        let leader = self.role == NodeRole::Leader;
        if leader {
            assert!(
                self.proposer.election().is_none(),
                "a leader has no open campaign"
            );
            assert!(
                self.matchmaking.is_none(),
                "a leader has no open matchmaking"
            );
        }
        leader
    }

    /// This node's current operating ballot.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn ballot(&self) -> Ballot {
        if self.role == NodeRole::Candidate {
            assert!(
                self.ballot.node == self.config.id,
                "a candidate runs its own ballot"
            );
        }
        if self.role == NodeRole::Leader {
            assert!(
                self.ballot > Ballot::zero(),
                "a leader runs a minted ballot"
            );
        }
        self.ballot
    }

    /// Whether the driver should feed a fresh randomized election timeout (the
    /// election clock just reset).
    #[must_use]
    pub fn needs_election_timeout(&self) -> bool {
        self.needs_election_timeout
    }

    /// How this node came to hold its current leadership: won by ordinary
    /// Phase 1, or installed from a predecessor's cooperative handoff.
    /// [`LeadershipOrigin::Elected`] on any non-leader.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn leadership_origin(&self) -> LeadershipOrigin {
        if let LeadershipOrigin::Handoff { from } = self.leadership_origin {
            assert!(
                self.role == NodeRole::Leader,
                "only a leader carries a handoff origin"
            );
            assert!(from != self.config.id, "a handoff comes from another node");
        }
        self.leadership_origin
    }
}
