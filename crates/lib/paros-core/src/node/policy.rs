//! The node's **driver-policy** methods: the moves a driver makes on its
//! own clock or judgement (re-send, take back a delegated round, step down,
//! campaign above a ballot, the election timeout, a grown pool). Each one has
//! an honest contract and decides nothing the protocol needs.

use super::{
    Ballot, ColocatedNode, MatchmakerId, NodeId, NodeRole, ProxyId, QuorumSystem, Round, Slot,
};

impl ColocatedNode {
    /// Re-broadcast a fair bounded page of this leader's in-flight `Accept`
    /// rounds. A no-op on a node that is not the leader, and on a leader with
    /// nothing pending.
    ///
    /// **The driver is expected to call this on each heartbeat beat**, right after
    /// [`ColocatedNode::tick`] — that is what lets a peer that lost the original
    /// `Accept` (or was down when it went out) catch up without waiting for an
    /// election.
    ///
    /// **Skipping a call is always safe.** Re-sending is pure optimization:
    /// nothing in Paxos safety depends on it, because the round's first broadcast
    /// already went out and a round that never gathers a quorum is simply
    /// *undecided*, which is a state the protocol is built to survive. A skipped
    /// round stalls until a later call re-sends it, or until an election recovers
    /// it. That is precisely why this is a *method* rather than something `tick`
    /// does implicitly: the decision to skip is the one whose rare omission makes
    /// the #54 election hole reachable — an undecided slot sitting *below* a
    /// decided one, which no environmental fault can produce on its own (a
    /// partition takes slots away in contiguous runs, never one here and one
    /// there) and which the `Control::Noop` gap fill exists to close. The
    /// deterministic simulation drives exactly that by skipping calls; production
    /// never skips.
    ///
    /// **A delegated round is re-delegated** (#142): its re-send goes to
    /// the proxy it was handed to, whose re-fan-out is P2b-idempotent, and
    /// the round counts the re-delegation so a proxy that never answers
    /// becomes visible ([`ColocatedNode::take_back_delegated`]).
    ///
    /// # Panics
    ///
    /// If an internal invariant is broken (a programmer error, never an
    /// operating condition).
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "debug", skip_all, fields(node = self.config.id.0)))]
    pub fn resend_pending(&mut self) {
        if self.role != NodeRole::Leader {
            return;
        }
        let marks = self.durable_marks();
        let pending = self.proposer.resend_page(self.limits.resend_page);
        // A re-send carries the leadership's own rounds, nothing else.
        assert!(
            pending.iter().all(|accept| accept.ballot == self.ballot),
            "a re-sent Accept runs at the leader's ballot"
        );
        for accept in pending {
            // The same column the round was opened against: a re-send never
            // widens a grid round to another column — and the same proxy.
            self.send_accept(
                accept.slot,
                accept.ballot,
                accept.command,
                accept.column,
                accept.proxy,
            );
        }
        self.assert_marks_monotone(marks);
        self.assert_invariants();
    }

    /// **Take back** every delegated round re-delegated at least
    /// `after_resends` times without the proxy's `Commit` arriving, and run
    /// each colocated from here on (#142). A no-op on a node that is not
    /// the leader, on a deployment without proxies, and while every
    /// delegated round is young.
    ///
    /// **The driver is expected to call this each beat**, right after
    /// [`ColocatedNode::resend_pending`], with its own budget: how many
    /// re-delegations a proxy may swallow is driver policy (a tunable, born
    /// buggified), not a constant of the state machine, exactly as the
    /// handoff-fence and repair budgets are. Liveness under a dead proxy is
    /// the leader's, and this is the whole of it: the fallback is always
    /// today's colocated Phase 2, as a failed handoff's fallback is an
    /// election. **Skipping a call is always safe** — a delegated round
    /// nobody takes back is simply undecided until its proxy answers or the
    /// next leadership recovers it — and so is taking a round back early:
    /// the proxy may still decide it behind this call, and two fan-outs of
    /// one `(slot, ballot, command)` are P2b-idempotent, so the two
    /// verdicts can only agree.
    ///
    /// Returns the rounds it took back, each with the proxy it was
    /// delegated to — what a driver reports and an oracle counts; empty
    /// whenever the call was a no-op.
    ///
    /// # Panics
    ///
    /// If an internal invariant is broken (a programmer error, never an
    /// operating condition).
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "debug", skip_all, fields(node = self.config.id.0, after_resends)))]
    pub fn take_back_delegated(&mut self, after_resends: u64) -> Vec<(Slot, ProxyId)> {
        if self.role != NodeRole::Leader {
            return Vec::new();
        }
        let mut taken = Vec::new();
        for slot in self.proposer.stalled_delegations(after_resends) {
            if let Some(proxy) = self.proposer.rounds().get(&slot).and_then(Round::proxy) {
                self.take_back(slot);
                taken.push((slot, proxy));
            }
        }
        // A round taken back is colocated (or already decided and closed).
        assert!(
            taken.iter().all(|(slot, _)| self
                .proposer
                .rounds()
                .get(slot)
                .is_none_or(|r| r.proxy().is_none())),
            "a round taken back runs colocated"
        );
        assert!(
            taken
                .iter()
                .all(|(_, proxy)| proxy.is_in(self.config.proxy_count)),
            "a round was delegated only to a deployed proxy"
        );
        // Read from the rounds, apart from `stalled_delegations` (#269): no
        // delegation past the budget stays with its proxy.
        assert!(
            self.proposer
                .rounds()
                .values()
                .all(|r| r.proxy().is_none() || r.resends() < after_resends),
            "no delegation past the take-back budget stays delegated"
        );
        self.assert_invariants();
        taken
    }

    /// **Move a stuck round to another column** (#533 (move a stuck round)):
    /// the leader's open, colocated Phase-2 round at `slot` is addressed from
    /// here on to `column` of the grid in force — the same slot, ballot and
    /// command — and its tally counts only the new column's accepts. Returns
    /// whether it moved: `false` on a node that is not the leader, under a
    /// majority or a flexible split (no column), with no round open at
    /// `slot`, for a delegated round (its votes are the proxy's; take it
    /// back first), or when the round already runs in `column`.
    ///
    /// **The driver calls this** for a round that stayed open for its own
    /// budget of re-sends ([`Round::resends`]): a column that cannot reach
    /// its quorum (one acceptor of a full-column grid down, a majority of a
    /// partition down) otherwise holds its slots, and the apply walk stalls
    /// behind the first one. Which column, and when, is driver policy.
    ///
    /// **Always safe**, like the column override of
    /// [`ColocatedNode::propose_in`]: inside one ballot a slot has one value,
    /// so the votes left in the old column are harmless; every column is a
    /// Phase-2 quorum group and every Phase-1 quorum meets every column's
    /// Phase-2 quorum, so a later election finds the value or proves it was
    /// not chosen. Slots are independent, so the new column needs no
    /// backfill.
    ///
    /// # Panics
    ///
    /// If `column` is not a column of the grid in force — the driver derives
    /// it from [`ColocatedNode::acceptors`], so this is a programmer error —
    /// or an internal invariant is broken.
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "debug", skip_all, fields(node = self.config.id.0, slot = slot.0, column)))]
    pub fn move_round(&mut self, slot: Slot, column: usize) -> bool {
        if self.role != NodeRole::Leader {
            return false;
        }
        let QuorumSystem::Grid { cols, .. } = self.acceptors.quorum_system() else {
            return false;
        };
        assert!(column < cols, "a round moves to a column of the grid");
        let marks = self.durable_marks();
        let moved = self.move_round_to(slot, column);
        // A moved round runs in its new column, or it decided at once.
        if moved && let Some(round) = self.proposer.rounds().get(&slot) {
            assert!(
                round.column() == Some(column),
                "a moved round runs in its new column"
            );
        }
        self.assert_marks_monotone(marks);
        self.assert_invariants();
        moved
    }

    /// Advance the next bounded page of deferred chosen-prefix application or
    /// inherited leader-recovery rounds after the caller has fully processed
    /// the previous [`crate::Ready`] batch. A no-op when neither continuation exists.
    ///
    /// Drivers call this after persistence, sends, and application complete;
    /// keeping it separate from [`Ready::advance`](crate::Ready::advance)
    /// prevents a single-node recovery from advancing the in-memory chosen
    /// prefix ahead of the batch the driver is still persisting.
    ///
    /// # Panics
    ///
    /// If an internal role/recovery invariant is broken.
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "debug", skip_all, fields(node = self.config.id.0)))]
    pub fn advance_recovery(&mut self) {
        let marks = self.durable_marks();
        let writes_before = self.pending_writes.len();
        self.advance_chosen_index();
        if self.pending_writes.len() != writes_before {
            self.assert_marks_monotone(marks);
            self.assert_invariants();
            return;
        }
        self.pump_leader_recovery();
        self.assert_marks_monotone(marks);
        self.assert_invariants();
    }

    /// Voluntarily resign the leadership: Leader → Follower, keeping every
    /// durable commitment (the promised ballot and the accepted log are
    /// untouched) and dropping only the volatile leadership state — the in-flight
    /// Phase-2 `proposer` map and the standing authority. A no-op on a
    /// node that is not the leader.
    ///
    /// A legitimate operational primitive: a node may want to hand leadership on
    /// before a planned restart or a rebalance (etcd-raft exposes the same idea as
    /// leadership transfer), and stepping down is always sound — Paxos never
    /// requires a leader to *stay* one. The slots the resigning leader was still
    /// re-proposing are simply undecided; the next leader recovers them from its
    /// promise quorum, or fills the ones the quorum never saw with a
    /// [`crate::journal::Control::Noop`].
    ///
    /// In the deterministic simulation this is the decision that makes an
    /// undecided slot **permanent**: the hole a skipped
    /// [`resend_pending`](ColocatedNode::resend_pending) leaves behind heals for as long
    /// as its holder keeps re-proposing it, and stops healing the moment that node
    /// stops being leader (#54) — and the leadership churn it creates is what
    /// #67's arc needs.
    ///
    /// # Panics
    ///
    /// If an internal invariant is broken (a programmer error, never an
    /// operating condition).
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "debug", skip_all, fields(node = self.config.id.0)))]
    pub fn step_down(&mut self) {
        if self.role != NodeRole::Leader {
            return;
        }
        let marks = self.durable_marks();
        self.become_follower(None);
        // Stepping down keeps every durable commitment and drops every
        // volatile leadership tally.
        assert!(
            self.proposer.rounds().is_empty(),
            "a resigned leader holds no round"
        );
        assert!(
            self.proposer.fence().is_none(),
            "a resigned leader holds no fence"
        );
        self.assert_marks_monotone(marks);
        self.assert_invariants();
    }

    /// Campaign now, strictly above `seen`, as if the election clock had
    /// just run out: a follower or a candidate opens a fresh campaign (or its
    /// membership probe, the same path a timeout takes) at a round above
    /// `seen.round`. A no-op on the leader and on a campaign that still waits
    /// on its matchmakers (its ballot is already promised).
    ///
    /// Always sound: any node may campaign at any time, at any fresh round,
    /// and a campaign costs availability only. Paxos safety never depends on
    /// who campaigns or when. `seen` only raises the volatile round floor, as
    /// a matchmaker's `Stale` refusal does. The driver calls it for an
    /// acceptor about to overwrite a value it accepted with a different one
    /// at `seen` (#376): the campaign promises above the overwriting
    /// `Accept`, so that `Accept` is refused, and its Phase 1 reads both
    /// values for the slot, where P2c keeps the higher ballot.
    ///
    /// # Panics
    ///
    /// If an internal invariant is broken (a programmer error, never an
    /// operating condition).
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "debug", skip_all, fields(node = self.config.id.0, round = seen.round)))]
    pub fn campaign_above(&mut self, seen: Ballot) {
        if self.role == NodeRole::Leader || self.matchmaking.is_some() {
            return;
        }
        let marks = self.durable_marks();
        let ballot = self.ballot;
        let promised = self.acceptor.promised();
        self.round_floor = self.round_floor.max(seen.round);
        // The timeout's own bookkeeping: the clock restarts and the driver
        // draws the next timeout.
        self.election_elapsed = 0;
        self.needs_election_timeout = true;
        self.on_check_leader();
        assert!(
            self.round_floor >= seen.round,
            "the round floor covers the ballot seen"
        );
        assert!(
            self.ballot >= ballot,
            "a campaign never lowers the operating ballot"
        );
        assert!(
            self.acceptor.promised() >= promised,
            "a campaign never lowers the promise"
        );
        self.assert_marks_monotone(marks);
        self.assert_invariants();
    }

    /// The driver supplies a randomized election timeout (in ticks, jitter drawn
    /// from its `RandomProvider`). Clears the [`ColocatedNode::needs_election_timeout`]
    /// flag.
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "debug", skip_all, fields(node = self.config.id.0, ticks)))]
    pub fn set_election_timeout(&mut self, ticks: u64) {
        self.election_timeout = ticks;
        self.needs_election_timeout = false;
        assert!(
            self.election_timeout == ticks,
            "the driver's timeout is the one in force"
        );
        assert!(
            !self.needs_election_timeout,
            "a fed timeout clears the request"
        );
    }

    /// Admit `nodes` to the addressable pool (#189): a node registered with
    /// the deployment at runtime becomes one this node follows, counts and
    /// answers — a configuration may name it, a reconfiguration may pull it
    /// in. **Grow-only**: a node leaves no pool here (a retired node is kept
    /// out by the caller, who stops talking to it; a configuration that
    /// still names it must stay one this node can run). Pure hygiene: it
    /// emits nothing. Refused — `false`, nothing moves — on a deployment
    /// without matchmakers: plain Multi-Paxos never reconfigures, so a node
    /// admitted to its pool could never be named by a configuration, and its
    /// learner traffic stays exactly today's. Returns whether the pool grew.
    ///
    /// # Panics
    ///
    /// If an internal invariant is broken (a programmer error).
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "debug", skip_all, fields(node = self.config.id.0, nodes = nodes.len())))]
    pub fn extend_pool(&mut self, nodes: &[NodeId]) -> bool {
        if !self.config.has_matchmakers() {
            return false;
        }
        let before = self.pool.len();
        self.pool.extend_from_slice(nodes);
        self.pool.sort_unstable();
        self.pool.dedup();
        let grew = self.pool.len() > before;
        // Postconditions: the pool only grew, and still holds the boot pool.
        assert!(self.pool.len() >= before, "the pool never shrinks");
        assert!(
            self.config
                .pool()
                .iter()
                .all(|n| self.pool.binary_search(n).is_ok()),
            "the pool always holds the boot pool"
        );
        self.assert_invariants();
        grew
    }

    /// Admit `matchmakers` to the matchmaker pool (#501 (matchmaker sets
    /// off the founders)): a machine registered with the deployment at
    /// runtime may host a matchmaker of a later generation, so a set that
    /// names it is one this node can learn. **Grow-only**, like
    /// [`ColocatedNode::extend_pool`]. Refused (`false`, nothing moves) on
    /// a deployment without matchmakers: plain Multi-Paxos has no set to
    /// learn. Pure hygiene: it emits nothing. Returns whether the pool
    /// grew.
    ///
    /// # Panics
    ///
    /// If an internal invariant is broken (a programmer error).
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "debug", skip_all, fields(node = self.config.id.0, matchmakers = matchmakers.len())))]
    pub fn extend_matchmaker_pool(&mut self, matchmakers: &[MatchmakerId]) -> bool {
        if !self.config.has_matchmakers() {
            assert!(
                self.matchmakers.is_none(),
                "a plain deployment believes no matchmaker set"
            );
            return false;
        }
        let before = self.matchmaker_pool.len();
        self.matchmaker_pool.extend_from_slice(matchmakers);
        self.matchmaker_pool.sort_unstable();
        self.matchmaker_pool.dedup();
        let grew = self.matchmaker_pool.len() > before;
        // Postconditions: the pool only grew, and still holds the boot pool.
        assert!(
            self.matchmaker_pool.len() >= before,
            "the matchmaker pool never shrinks"
        );
        assert!(
            self.config
                .matchmaker_pool()
                .iter()
                .all(|m| self.matchmaker_pool.binary_search(m).is_ok()),
            "the matchmaker pool always holds the boot pool"
        );
        self.assert_invariants();
        grew
    }
}
