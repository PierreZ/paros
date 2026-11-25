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

use super::{Ballot, BeliefSource, ColocatedNode, LeadershipOrigin, NodeRole, Slot};
use crate::contract::write::RetireMark;
use crate::membership::AcceptorConfig;
use crate::proposer::Round;

/// The three durable watermarks a node only ever raises: the promise, the
/// chosen index and the compaction floor. Captured at a public entry point
/// and compared at its exit ([`ColocatedNode::assert_marks_monotone`]).
pub(super) type DurableMarks = (Ballot, Option<Slot>, Slot);

impl ColocatedNode {
    /// At the entry of every public mutating call: no batch is in flight
    /// ([`crate::node::Ready::release`]), so the call changes nothing before
    /// it panics.
    pub(super) fn assert_landed(&self) {
        assert!(
            self.flight.is_none_or(|batch| batch < self.flights),
            "a batch in flight has a number already drawn"
        );
        assert!(
            self.flight.is_none(),
            "no input reaches a node while its batch is in flight"
        );
    }

    /// The durable watermarks as they stand: see [`DurableMarks`].
    pub(super) fn durable_marks(&self) -> DurableMarks {
        let marks = (
            self.acceptor.promised(),
            self.replica.chosen_index(),
            self.acceptor.first_slot(),
        );
        assert!(
            marks.2 <= self.first_unchosen(),
            "the compaction floor never outruns the chosen prefix"
        );
        marks
    }

    /// Whether every member of `config` is in this node's pool, restated
    /// over the pool itself rather than through
    /// [`AcceptorConfig::is_drawn_from`], the guard every learning path
    /// asks (#343): the invariant must not lean on the guard it checks.
    pub(super) fn pooled_all(&self, config: &AcceptorConfig) -> bool {
        config
            .members()
            .iter()
            .all(|m| self.pool.binary_search(m).is_ok())
    }

    /// The write-side half of the boot read-back: across any public entry
    /// point, the promise, the chosen index and the compaction floor only
    /// rise — what the persist-before-send batch writes is what a reboot may
    /// rely on never to regress.
    pub(super) fn assert_marks_monotone(&self, before: DurableMarks) {
        let (promised, chosen, floor) = self.durable_marks();
        assert!(promised >= before.0, "a node's promise never decreases");
        assert!(chosen >= before.1, "a node's chosen index never decreases");
        assert!(
            floor >= before.2,
            "a node's compaction floor never decreases"
        );
    }

    /// Assert every cross-field invariant of the node's volatile state, plus
    /// each role's own. Most checks are O(1) or O(log n) (min-key probes);
    /// the role checkers add bounded structural scans over the retained log,
    /// the faulty set and the configuration in force. All of it runs
    /// unconditionally — TigerBeetle-style — at boot and at the exit of every
    /// public mutating entry point: the maps are small, and crash beats
    /// corruption.
    pub(super) fn assert_invariants(&self) {
        // One batch in flight (`Ready::release`): no input reaches the node
        // until the driver gives the batch back as durable. An acceptor emits
        // no write for a `Prepare` at the ballot it already promised, so a
        // `Promise` sent before the earlier batch is durable is a vote that a
        // crash takes back.
        assert!(
            self.flight.is_none(),
            "no input reaches a node while its batch is in flight"
        );
        // The believed matchmaker set is drawn from the matchmaker pool
        // (#501 (matchmaker sets off the founders)): a set outside it is
        // never learned.
        if let Some(set) = self.matchmakers.as_ref() {
            assert!(
                set.members()
                    .iter()
                    .all(|m| self.matchmaker_pool.binary_search(m).is_ok()),
                "the believed matchmaker set is drawn from the matchmaker pool"
            );
        }
        // Ordering chain: only chosen slots are ever dropped, so the compaction
        // floor never passes the first unchosen slot.
        assert!(
            self.acceptor.first_slot() <= self.first_unchosen(),
            "the compaction floor never outruns the chosen prefix"
        );
        // The unprepared campaign is this node's own ballot, never above
        // its promise, and no Phase 1 runs at it: `start_phase1` clears it
        // before the first `Prepare` leaves. Without this pair a path that
        // sends a `Prepare` another way leaves the read gate of #890 open.
        if let Some(own) = self.unprepared_campaign {
            assert!(
                own.node == self.config.id,
                "an unprepared campaign is this node's own ballot"
            );
            assert!(
                own <= self.acceptor.promised(),
                "an unprepared campaign is never above the promise"
            );
            if self.proposer.election().is_some() {
                assert!(
                    own != self.ballot,
                    "a Phase 1 never runs at an unprepared campaign"
                );
            }
        }
        // The replica's and the proposer's own maps against the acceptor's
        // floor.
        self.acceptor.assert_invariants();
        self.replica.assert_invariants(self.acceptor.first_slot());
        self.proposer.assert_invariants();
        // The recovery page size stays inside its tunable range (#330).
        assert!(
            self.limits.recovery_page > 0,
            "a recovery page is never empty"
        );
        assert!(
            self.limits.recovery_page <= crate::contract::limits::RECOVERY_BATCH,
            "a recovery page never exceeds its ceiling"
        );
        // So do the promise and re-send page sizes (#338).
        assert!(
            self.limits.promise_page > 0,
            "a promise page is never empty"
        );
        assert!(
            self.limits.promise_page <= crate::contract::limits::PROMISE_BATCH,
            "a promise page never exceeds its ceiling"
        );
        assert!(self.limits.resend_page > 0, "a re-send page is never empty");
        assert!(
            self.limits.resend_page <= crate::contract::limits::RESEND_BATCH,
            "a re-send page never exceeds its ceiling"
        );
        // The apply page the replica walks is the node's, and the read
        // window keeps its floor (#386).
        assert!(
            self.replica.apply_page() == self.limits.apply_page,
            "the driver's apply page is in force"
        );
        assert!(
            self.limits.read_window >= super::READ_TTL_TICKS,
            "a read window keeps its floor"
        );
        // The repair clock lives inside the probe it times, so closing a
        // probe — by a decision, a commit, a trim-point jump or an
        // abandoned leadership — takes the clock with it and no path has to
        // remember to reset one. What the wiring owns is who advances it.
        assert!(
            self.proposer
                .probe_elapsed()
                .is_none_or(|elapsed| elapsed == 0 || self.role == NodeRole::Leader),
            "only a leader's repair probe ages"
        );
        // The cross-role couplings against the acceptor's floor: a compaction
        // and a trim-point jump both retain the rounds above the floor they
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
        self.assert_hole_invariants();
        self.assert_deployment_invariants();
        self.assert_registration_fence();
        self.assert_read_basis_invariants();
        self.assert_role_invariants();
        self.assert_leadership_state_invariants();
        self.assert_plain_arm_invariants();
        self.assert_pooled_configurations();
        self.assert_chosen_records();
        self.assert_pull_invariants();
        self.belief.assert_invariants(&self.pool);
    }

    /// The `None` arm (root `AGENTS.md`, "Plain Multi-Paxos is a shape of
    /// the core"): a plain deployment hears no GC floor and queues no
    /// matchmaker or GC request. The `Ready` getters and the floor's getter
    /// check the same, but only when a driver calls them.
    fn assert_plain_arm_invariants(&self) {
        if self.config.has_matchmakers() {
            return;
        }
        assert!(
            self.gc_floor_heard.is_none(),
            "a plain deployment hears no GC floor"
        );
        assert!(
            self.pending_match_requests.is_empty(),
            "a plain deployment queues no matchmaking request"
        );
        assert!(
            self.pending_gc_requests.is_empty(),
            "a plain deployment queues no GC request"
        );
    }

    /// Every configuration the node holds is drawn from its pool: the
    /// belief (checked in [`Self::assert_deployment_invariants`]), the
    /// configuration and the prior configurations of an open election, the
    /// configuration of an open matchmaking phase, and the prior
    /// configurations of the GC campaign. A quorum of a configuration with
    /// a member outside the pool counts votes that no handler takes.
    pub(super) fn assert_pooled_configurations(&self) {
        if let Some(election) = self.proposer.election() {
            assert!(
                self.pooled_all(election.config()),
                "an open election's configuration is drawn from the pool"
            );
            assert!(
                election.prior().iter().all(|c| self.pooled_all(c)),
                "an open election's prior configurations are drawn from the pool"
            );
        }
        if let Some(matchmaking) = self.matchmaking.as_ref() {
            assert!(
                self.pooled_all(matchmaking.config()),
                "a matchmaking phase registers a configuration drawn from the pool"
            );
        }
        if let Some(gc) = self.gc.as_ref() {
            assert!(
                gc.prior().iter().all(|c| self.pooled_all(c)),
                "a GC campaign's prior configurations are drawn from the pool"
            );
        }
    }

    /// The read side of the pair that `mark_chosen` asserts at the write: a
    /// slot the replica holds chosen, at or above the floor, has its
    /// authoritative accepted record, which is the node's one copy of the
    /// chosen command (the replica reads it through its `ChosenLog` port).
    /// The check reads the lowest and the highest chosen slot, so it stays
    /// O(log n) at every entry point.
    fn assert_chosen_records(&self) {
        let floor = self.acceptor.first_slot();
        let chosen = self.replica.chosen();
        let ends = [
            chosen.range(floor..).next(),
            chosen.range(floor..).next_back(),
        ];
        for slot in ends.into_iter().flatten() {
            let record = self.acceptor.record(*slot);
            assert!(
                record.is_some(),
                "a chosen slot holds its authoritative accepted record"
            );
        }
    }

    /// The beat-triggered pull ([`super::PULL_FALLBACK_BEATS`]): the count
    /// belongs to a chosen index that this node held, and the chosen index
    /// never falls, so the pull's index is never above it.
    fn assert_pull_invariants(&self) {
        assert!(
            self.pull_index <= self.replica.chosen_index(),
            "a pull counts at a chosen index this node held"
        );
    }

    /// The repair hole (#343): a faulty record under the chosen prefix is a
    /// chosen slot whose value this node does not hold, so the fold stops at
    /// or below it. The fold's hole alone then starts both a campaign's
    /// recovery range and the per-tick repair pull.
    fn assert_hole_invariants(&self) {
        let first_unchosen = self.first_unchosen();
        let hole = self.replica.fold_hole();
        if let Some(faulty) = self
            .acceptor
            .faulty()
            .keys()
            .next()
            .filter(|s| **s < first_unchosen)
        {
            assert!(
                hole.is_some_and(|h| h <= *faulty),
                "the fold's hole covers every faulty slot under the chosen prefix"
            );
        }
        if let Some(h) = hole {
            assert!(
                h < first_unchosen,
                "a fold hole lies below the first unchosen slot"
            );
            assert!(
                !self.replica.chosen().contains(&h),
                "a fold hole is a slot not held"
            );
        }
    }

    /// The read basis (#260): only a matchmaker deployment stores one; it is
    /// a won leadership's, drawn from the pool, and on a leader at least its
    /// own ballot.
    fn assert_read_basis_invariants(&self) {
        let Some(basis) = self.belief.stored_basis() else {
            return;
        };
        assert!(
            self.config.has_matchmakers(),
            "a plain deployment stores no read basis"
        );
        assert!(
            self.pooled_all(&basis.config),
            "a read basis is drawn from the pool"
        );
        if self.role == NodeRole::Leader && self.proposer.election().is_none() {
            assert!(
                basis.since >= self.ballot,
                "a leader's read basis is at least its own ballot"
            );
        }
    }

    /// The registration fence (#1002 (retire fence gaps)): the membership
    /// fence covers it, and only a matchmaker deployment registers or checks
    /// its registrations.
    fn assert_registration_fence(&self) {
        assert!(
            self.last_member_ballot >= self.registration_fence,
            "the membership fence covers the registration fence"
        );
        if !self.config.has_matchmakers() {
            assert!(
                self.registration_fence == Ballot::zero(),
                "a plain deployment holds no registration fence"
            );
            assert!(
                self.registrations_checked.is_none(),
                "a plain deployment checks no registrations"
            );
            assert!(
                self.retire_check.is_none(),
                "a plain deployment asks no matchmaker to retire"
            );
            assert!(self.rejoin.is_none(), "a plain deployment never rejoins");
            assert!(
                self.retire_mark == RetireMark::default(),
                "a plain deployment holds no retire mark"
            );
        }
        assert!(
            self.retire_check.is_none() || self.rejoin.is_none(),
            "a rejoining node opens no retirement check"
        );
        if let Some(rejoin) = self.rejoin.as_ref() {
            assert!(
                rejoin.incarnation() >= self.retire_mark.incarnation,
                "a rejoin is never below the durable incarnation"
            );
        }
        if let Some(check) = self.retire_check.as_ref() {
            assert!(
                self.role == NodeRole::Follower,
                "a retirement check runs on a follower"
            );
            assert!(
                self.belief.since() == check.watermark(),
                "a retirement check runs under the belief bound to its floor"
            );
            assert!(
                !self.belief.config().contains(self.config.id),
                "a retirement check runs with the node outside"
            );
        }
    }

    /// The deployment couplings: plain Multi-Paxos never matchmakes and never
    /// leaves its bootstrap configuration; a matchmaker deployment runs
    /// Phase 2 under a configuration drawn from the pool.
    fn assert_deployment_invariants(&self) {
        // The deployment couplings: plain Multi-Paxos never matchmakes and
        // never leaves its bootstrap configuration; a matchmaker deployment
        // runs Phase 2 under a configuration drawn from the pool.
        if self.config.has_matchmakers() {
            assert!(
                self.matchmakers.is_some(),
                "a matchmaker deployment always believes in a matchmaker set"
            );
        } else {
            assert!(
                self.matchmaking.is_none(),
                "a plain deployment never opens a matchmaking phase"
            );
            assert!(
                self.probe.is_none(),
                "a plain deployment never probes its membership"
            );
            assert!(
                self.belief.config().members() == self.config.peers,
                "a plain deployment keeps its bootstrap configuration"
            );
            assert!(
                self.belief.since() == Ballot::zero(),
                "a plain deployment's configuration is bound to no ballot"
            );
            assert!(
                self.matchmakers.is_none(),
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
            self.pooled_all(self.belief.config()),
            "the active configuration is drawn from the node pool"
        );
        // The pool is grow-only from the boot pool, and stays the sorted,
        // deduplicated set `in_pool` binary-searches.
        assert!(
            self.pool.windows(2).all(|w| w[0] < w[1]),
            "the node pool is sorted and deduplicated"
        );
        assert!(
            self.config
                .pool()
                .iter()
                .all(|n| self.pool.binary_search(n).is_ok()),
            "the node pool always holds the boot pool"
        );
        if !self.config.has_matchmakers() {
            assert!(
                self.pool.len() == self.config.pool().len(),
                "a plain deployment's pool never grows"
            );
        }
        // The retirement fence (#123): `last_member_ballot` is the highest
        // `acceptors_since` of every configuration that named this node, so
        // while this node *is* a member it is at least the ballot its
        // configuration is bound to. It may sit *above* it: adopting an
        // honored reconfiguration binds the node to that reconfiguration's
        // own, possibly older, ballot (`node/matchmaking.rs`), and the fence
        // keeps the newer membership it already recorded — `may_retire`
        // needs the maximum, never the current binding.
        if self.is_acceptor() {
            assert!(
                self.last_member_ballot >= self.belief.since(),
                "a member's fence is at least the ballot its configuration is bound to"
            );
        }
        // The belief's provenance (#173): a configuration bound to a ballot
        // was heard (every binding goes through `adopt_configuration`), and
        // a membership probe is open only on the default it asks about or a
        // heard belief leaving this node outside (#270), beside no campaign
        // and no leadership.
        if self.belief.since() != Ballot::zero() {
            assert!(
                self.belief_source == BeliefSource::Heard,
                "a configuration bound to a ballot is a heard belief"
            );
        }
        self.assert_fact_invariants();
        if self.probe.is_some() && self.belief_source == BeliefSource::Heard {
            assert!(
                !self.belief.config().contains(self.config.id),
                "a membership probe asks about a heard belief only from outside it"
            );
        }
        if self.probe.is_some() {
            assert!(
                self.role == NodeRole::Follower,
                "a membership probe never overlaps a leadership or a candidacy"
            );
            assert!(
                self.matchmaking.is_none(),
                "a membership probe never overlaps a campaign"
            );
            assert!(
                self.probe
                    .as_ref()
                    .is_some_and(|p| p.ballot().round <= self.round_floor),
                "every later campaign opens above an open probe's tag"
            );
        }
    }

    /// The matched reconfiguration fact and a closed probe's tag (#278).
    fn assert_fact_invariants(&self) {
        if !self.config.has_matchmakers() {
            assert!(
                self.closed_probe.is_none(),
                "a plain deployment never keeps a closed probe's tag"
            );
            assert!(
                self.belief_fact == Ballot::zero(),
                "a plain deployment matches no reconfiguration fact"
            );
        }
        // The matched reconfiguration fact (#278) is a heard one, never
        // above the ballot the belief is bound to; a closed probe's tag is
        // kept only while no probe is open.
        assert!(
            self.belief_fact <= self.belief.since(),
            "a belief is bound at or above the fact it matches"
        );
        if self.belief_fact != Ballot::zero() {
            assert!(
                self.belief_source == BeliefSource::Heard,
                "a matched reconfiguration fact is a heard belief"
            );
        }
        if self.closed_probe.is_some() {
            // A probe closes with its node outside (`fold_probe_answer`),
            // and every move of the belief, a campaign or a fresh probe
            // drops the tag.
            assert!(
                self.role == NodeRole::Follower,
                "a closed probe's tag lives on a follower"
            );
            assert!(
                self.belief_source == BeliefSource::Heard
                    && !self.belief.config().contains(self.config.id),
                "a closed probe's tag lives on a node outside its heard belief"
            );
            assert!(
                self.probe.is_none(),
                "a closed probe's tag never coexists with an open probe"
            );
            assert!(
                self.matchmaking.is_none(),
                "a closed probe's tag never coexists with a campaign"
            );
        }
    }

    /// The per-role state machine: what a leader, a candidate and a follower
    /// may each hold.
    fn assert_role_invariants(&self) {
        match self.role {
            NodeRole::Leader => self.assert_leader_invariants(),
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
                // A live campaign's promise sits at its ballot: `open_campaign`
                // promises the ballot it opens at, and every path that
                // raises the promise past it closes the campaign (a higher
                // `Prepare`, an `Accept` from another leader, a handoff
                // install, a slot learned chosen above it in
                // `mark_chosen`). `start_phase1` relies on it when
                // matchmaking hands over (witness 13099780746496209636).
                assert!(
                    self.acceptor.promised() == self.ballot,
                    "a live campaign's promise sits at its ballot"
                );
                self.assert_candidate_invariants();
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

    /// What a campaign may hold beyond its open phase: on a matchmaker
    /// deployment a node registers only a belief that it heard (root
    /// `AGENTS.md`, "Membership probe"). A campaign whose configuration names
    /// this node raised the retirement fence to its ballot when it opened
    /// (`last_member_ballot`, #1002 (retire fence gaps)): the registration
    /// stays in the matchmakers' histories whether or not the campaign goes
    /// on.
    ///
    /// The round floor is not checked here: it bounds the campaigns that open
    /// later (`open_campaign` asserts it), and a rejoin that opens during a
    /// campaign raises it above the open campaign's round on purpose
    /// (`fold_retired`), while the campaign waits for the matchmakers.
    fn assert_candidate_invariants(&self) {
        if !self.config.has_matchmakers() {
            return;
        }
        assert!(
            self.belief_source == BeliefSource::Heard,
            "a matchmaker deployment campaigns only on a heard belief"
        );
        if let Some(matchmaking) = self.matchmaking.as_ref()
            && matchmaking.config().contains(self.config.id)
        {
            assert!(
                self.last_member_ballot >= self.ballot,
                "a campaign that names this node is under its retirement fence"
            );
        }
    }

    /// What a leader may hold: no campaign, itself as leader, a ballot that
    /// matches its origin, an allocator past the prefix and above every
    /// round, every round at its ballot and in its log, and delegation only
    /// where the deployment has proxies.
    fn assert_leader_invariants(&self) {
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
        // Ballot zero is no one's: every leadership runs at a minted round.
        assert!(
            self.ballot > Ballot::zero(),
            "a leader operates under a minted ballot"
        );
        if self.gc.is_some() {
            assert!(
                self.config.has_matchmakers(),
                "only a matchmaker deployment collects"
            );
        }
        // Whose node id the operating ballot names is exactly what
        // separates the two leadership origins — an elected leader owns
        // its ballot, a handoff leader is exercising a predecessor's.
        match self.leadership_origin {
            LeadershipOrigin::Elected => assert!(
                self.ballot.node == self.config.id,
                "an elected leader's ballot names its own node"
            ),
            LeadershipOrigin::Handoff { from } => {
                // The ballot names whoever *minted* it, and under the
                // one-hop rule (`can_relinquish` requires
                // `LeadershipOrigin::Elected`) the minter is the
                // predecessor itself — always someone else, and pooled.
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
        // The allocator frontier is durable by construction
        // (`record_own_round`): every round this leader opened while
        // its promise allowed is in its own log, so a reboot derives
        // the frontier it had. O(N) structural, always-on by choice.
        // A round opens only while the promise allows its record, and a
        // promise that passes the ballot later leaves the record in place.
        assert!(
            self.proposer
                .rounds()
                .keys()
                .all(|slot| self.acceptor.record(*slot).is_some()),
            "a leader's every open round is recorded in its own log"
        );
        // Phase 2 addresses `C_b` alone (root `AGENTS.md`, "Matchmaking,
        // then Phase 1"): on a matchmaker deployment the belief in force is
        // the configuration the leadership's ballot registered, bound to that
        // ballot. An elected leader adopts its election's configuration at
        // the win, and a handoff successor the configuration the
        // `Relinquish` carried at the inherited ballot.
        if self.config.has_matchmakers() {
            assert!(
                self.belief.since() == self.ballot,
                "a matchmaker leader's configuration is bound to its own ballot"
            );
        }
        // Delegation (#142) is the `None` arm's opt-in: a deployment
        // without proxies delegates nothing, and a delegated round
        // names a proxy the deployment has.
        assert!(
            self.proposer
                .rounds()
                .values()
                .filter_map(Round::proxy)
                .all(|proxy| proxy.is_in(self.belief.config().proxy_count())),
            "a delegated round names a proxy of the deployment"
        );
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
        // The clock of an uncovered inherited fence runs only on a handoff
        // successor: every other leadership resets it.
        if !matches!(self.leadership_origin, LeadershipOrigin::Handoff { .. }) {
            assert!(
                self.handoff_fence_elapsed == 0,
                "only a handoff successor ages an inherited fence"
            );
        }
        // Volatile leadership state exists only on a leader.
        if !self.proposer.rounds().is_empty() {
            assert!(
                self.role == NodeRole::Leader,
                "only a leader holds in-flight accept rounds"
            );
        }
        if self.proposer.fence().is_some() {
            assert!(
                self.role == NodeRole::Leader,
                "only a leader holds a leadership fence"
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
