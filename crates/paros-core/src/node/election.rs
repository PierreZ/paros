//! The node's **campaign wiring**: how the election clock opens a campaign,
//! how a `Promise` reaches the [`Proposer`](crate::proposer::Proposer)'s
//! Phase-1 tally (or its repair probe), how a won Phase 1 becomes a
//! leadership, and how that leadership drains its recovered suffix one
//! bounded page at a time. The component tallies and decides; this module
//! builds the `Prepare`s, moves the role, fixes the allocator and the read
//! fence, and opens the GC campaign on a matchmaker deployment.

use super::matchmaking::Matchmaking;
use super::{
    BTreeMap, Ballot, Command, Control, LeadershipOrigin, Message, NodeId, NodeRole, RawNode, Slot,
};
use crate::matchmaker::MatchRequest;
use crate::membership::AcceptorConfig;
use crate::proposer::{Campaign, PromiseFold, RECOVERY_BATCH, RecoveryPolicy, RecoveryStep};

impl RawNode {
    // ---- election / leadership --------------------------------------------

    /// Election clock fired: campaign for leadership at a fresh ballot with
    /// the acceptor configuration this node believes is the latest.
    ///
    /// On a deployment with matchmakers a node campaigns only when it is a
    /// **member** of that configuration: leadership belongs inside the
    /// acceptor set (a removed node that campaigned would lead a cluster it
    /// is not part of, and a spare that campaigned would register a
    /// configuration nobody asked for). A reconfiguration that removes the
    /// sitting leader is the one deliberate exception, and it runs through
    /// [`RawNode::reconfigure`], not here.
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "trace", skip_all, fields(node = self.config.id.0)))]
    pub(super) fn on_check_leader(&mut self) {
        if self.role == NodeRole::Leader {
            return;
        }
        if self.config.has_matchmakers() && !self.acceptors.contains(self.config.id) {
            self.non_member_campaigns_skipped = self.non_member_campaigns_skipped.saturating_add(1);
            return;
        }
        self.campaign(None);
    }

    /// Open a campaign at a fresh ballot: bump the round, promise it durably,
    /// drop every leadership state, then either register `(b, C_b)` with the
    /// matchmakers (a deployment that names them) or go straight to Phase 1
    /// against the one static configuration (plain Multi-Paxos).
    ///
    /// `target` is `Some` for a reconfiguration ([`RawNode::reconfigure`]):
    /// the configuration to register instead of this node's current belief.
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "trace", skip_all, fields(node = self.config.id.0, reconfiguration = target.is_some())))]
    pub(super) fn campaign(&mut self, target: Option<AcceptorConfig>) {
        let me = self.config.id;
        let base_round = self
            .acceptor
            .promised()
            .round
            .max(self.ballot.round)
            .max(self.round_floor);
        let Some(round) = base_round.checked_add(1) else {
            // The wire/domain round space is exhausted. There is no strictly
            // higher valid ballot to campaign at, so remain a follower rather
            // than wrapping or reusing the maximum round.
            self.become_follower(None);
            return;
        };
        // Leadership state dies whole — a reconfiguring leader abandons its
        // in-flight rounds exactly as a deposed one does (the accepted stall
        // window of #122: the successor ballot's Phase 1 recovers them).
        self.clear_leadership_state();
        self.role = NodeRole::Candidate;
        self.leader = None;
        self.ballot = Ballot { round, node: me };
        self.acceptor
            .set_promise(self.ballot, &mut self.pending_writes);
        // A campaign opens at a ballot this node itself just promised: the
        // fresh round is strictly above every round in the max above, so the
        // promise landed exactly on the campaign ballot.
        assert!(
            self.acceptor.promised() == self.ballot,
            "a candidate promises the ballot it campaigns at"
        );
        // A refused campaign's floor is honoured: the fresh round sits
        // strictly above the highest round that refused this node.
        assert!(
            self.ballot.round > self.round_floor,
            "a campaign opens above the round floor a stale refusal set"
        );
        let reconfiguration = target.is_some();
        let config = target.unwrap_or_else(|| self.acceptors.clone());
        if self.config.has_matchmakers() {
            // The matchmaking phase: register first, prepare only once a
            // matchmaker quorum has answered (see `super::matchmaking`).
            self.matchmaking = Some(Matchmaking::new(
                self.ballot,
                config.clone(),
                reconfiguration,
            ));
            let generation = self.matchmakers.generation;
            let request = if reconfiguration {
                MatchRequest::reconfigure(me, self.ballot, config, generation)
            } else {
                MatchRequest::new(me, self.ballot, config, generation)
            };
            for matchmaker in self.matchmakers.members.clone() {
                self.pending_match_requests
                    .push((matchmaker, request.clone()));
            }
            // Negative space of invariant 1 (#120): nothing Phase-1-shaped
            // left this call — no `Prepare` before a matchmaker quorum.
            assert!(
                !self
                    .pending_messages
                    .iter()
                    .any(|(_, m)| matches!(m, Message::Prepare { .. })),
                "a campaign sends no Prepare before its matchmaker quorum"
            );
            assert!(
                self.proposer.election().is_none(),
                "matchmaking opens before Phase 1"
            );
        } else {
            // Plain Multi-Paxos: `H_b` is implicitly the static configuration.
            self.start_phase1(config.clone(), vec![config]);
        }
    }

    /// The **Phase-1 boundary**: matchmaking (or the plain path) hands over
    /// `config` (`C_b`) and `prior` (`H_b`), and one Phase 1 per ballot opens
    /// over the whole uncommitted log suffix against every prior
    /// configuration.
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "trace", skip_all, fields(node = self.config.id.0, prior = prior.len())))]
    pub(super) fn start_phase1(&mut self, config: AcceptorConfig, prior: Vec<AcceptorConfig>) {
        let me = self.config.id;
        // Precondition stack: Phase 1 opens on a candidate at its own promised
        // ballot, with no other campaign phase open.
        assert!(
            self.role == NodeRole::Candidate,
            "Phase 1 opens on a candidate"
        );
        assert!(
            self.acceptor.promised() == self.ballot,
            "Phase 1 opens at the ballot the candidate promised"
        );
        assert!(
            self.matchmaking.is_none(),
            "Phase 1 opens once matchmaking has closed"
        );
        assert!(self.proposer.election().is_none(), "one Phase 1 per ballot");

        // The campaign's recovery range starts at this node's first *faulty*
        // slot when that sits below the contiguous prefix (Stage 8): a rotted
        // chosen record leaves a hole this node may be the only one able to
        // ask about — the Promise response IS the recovery query, so the
        // node's own Phase 1 covers the hole and the quorum's reports heal it
        // (see the prefix-heal step in `try_become_leader`).
        let from_slot = self
            .acceptor
            .first_faulty()
            .map_or(self.first_unchosen(), |first_faulty| {
                first_faulty.min(self.first_unchosen())
            });
        let wire_config = self.config.has_matchmakers().then(|| config.clone());
        // The candidate is its own first acceptor: its records seed the P2c
        // tally, its faulty entries the tri-state tally, and its promise
        // counts toward every prior configuration that contains it (and
        // toward none when it is in none of them — a fresh member of `C_b`,
        // or a reconfiguring leader that removed itself: the
        // per-configuration tally decides).
        let targets = self.proposer.open_phase1(
            Campaign {
                me,
                ballot: self.ballot,
                config,
                prior,
                from_slot,
            },
            self.acceptor.records(),
            self.acceptor.faulty(),
        );
        let prepare = Message::Prepare {
            config_id: self.config_id,
            from: me,
            ballot: self.ballot,
            from_slot,
            config: wire_config,
        };
        for to in targets {
            self.pending_messages.push((to, prepare.clone()));
        }
        // Proactive catch-up probe. The election clock fires precisely when we have
        // *not* heard a satisfactory leader — the same condition under which we may
        // be silently behind: a stale or absent leader beat never reveals a decided
        // slot past our prefix, so heartbeat-triggered catch-up never fires. Ask
        // every peer to replay our decided-prefix gap directly; any peer that has
        // those slots chosen serves them, healing us even if this election does not
        // win (a won election gap-fills only *accepted* slots it can re-proposes;
        // this learns *chosen* ones outright). Harmless when we are not behind — a
        // peer with nothing past `from_slot` simply sends nothing.
        self.broadcast(&Message::CatchUpRequest {
            from: me,
            from_slot,
        });
        self.try_become_leader();
    }

    /// Candidate: collect a `Promise`, merging the reported accepted suffix
    /// (highest ballot per slot wins) and the tri-state faulty reports. A
    /// **leader** with an open repair probe also lands here: a straggler's
    /// late answer (or a re-queried peer's fresh one) is merged into the probe
    /// and may resolve a blocked slot.
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "trace", skip_all, fields(node = self.config.id.0)))]
    pub(super) fn on_promise(
        &mut self,
        from: NodeId,
        ballot: Ballot,
        from_slot: Slot,
        accepted: BTreeMap<Slot, (Ballot, Command)>,
        faulty: BTreeMap<Slot, Ballot>,
        next_from_slot: Option<Slot>,
    ) {
        // Quorum sets are keyed by NodeId: an id outside the addressable pool
        // must never inflate one (wire hygiene; peers are trusted but a
        // misrouted or misconfigured sender is not a quorum member). Which
        // *configurations* the promise counts toward is the per-configuration
        // tally's business, not this guard's.
        if !self.in_pool(from) {
            return;
        }
        if self.role == NodeRole::Leader {
            self.on_probe_promise(from, ballot, from_slot, &accepted, &faulty, next_from_slot);
            return;
        }
        match self
            .proposer
            .fold_promise(from, ballot, from_slot, accepted, faulty, next_from_slot)
        {
            PromiseFold::Ignored => return,
            PromiseFold::Continue(next) => {
                // A valid page is leader contact for election-timeout purposes;
                // a long suffix must not make the same campaign expire mid-page.
                self.election_elapsed = 0;
                self.request_promise_page(from, ballot, next);
                return;
            }
            PromiseFold::Answered => {}
        }
        self.try_become_leader();
    }

    /// Ask `from` for its next `Promise` page at `ballot`, from `next`.
    fn request_promise_page(&mut self, from: NodeId, ballot: Ballot, next: Slot) {
        let config = self.phase1_wire_config();
        self.pending_messages.push((
            from,
            Message::Prepare {
                config_id: self.config_id,
                from: self.config.id,
                ballot,
                from_slot: next,
                config,
            },
        ));
    }

    /// Merge one straggler `Promise` page into the leader's open repair probe
    /// and resolve any blocked slot the refreshed tally now decides.
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "trace", skip_all, fields(node = self.config.id.0)))]
    fn on_probe_promise(
        &mut self,
        from: NodeId,
        ballot: Ballot,
        from_slot: Slot,
        accepted: &BTreeMap<Slot, (Ballot, Command)>,
        faulty: &BTreeMap<Slot, Ballot>,
        next_from_slot: Option<Slot>,
    ) {
        match self.proposer.fold_probe_promise(
            from,
            ballot,
            from_slot,
            accepted,
            faulty,
            next_from_slot,
        ) {
            PromiseFold::Ignored => return,
            PromiseFold::Continue(next) => {
                self.request_promise_page(from, ballot, next);
                return;
            }
            PromiseFold::Answered => {}
        }
        self.resolve_blocked_repairs();
    }

    /// Decide every blocked slot the current probe tally allows: Case 1
    /// (re-propose the best `have`) or Case 2 (a full Q1 of qualifying answers
    /// with no `have`: decide `Noop`). Closes the probe when nothing stays
    /// blocked.
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "trace", skip_all, fields(node = self.config.id.0)))]
    pub(super) fn resolve_blocked_repairs(&mut self) {
        let decisions = self.proposer.resolve_probe();
        for decision in decisions {
            let slot = decision.slot;
            if self.replica.is_chosen(slot) || slot < self.acceptor.first_slot() {
                continue;
            }
            if decision.from_have {
                self.repair_case1 += 1;
            } else {
                // Case 2 invents a `Noop` from a full Q1 of qualifying `none`.
                // A slot inside this node's own chosen prefix was decided by
                // some Q2, which intersects that Q1 — so at least one answer
                // would have been a `have` or a disqualifying `faulty`. A Q1
                // of `none` there is a broken tally, never a repair.
                assert!(
                    slot >= self.first_unchosen(),
                    "a quorum of none never resolves a slot inside the chosen prefix"
                );
                self.repair_case2 += 1;
            }
            if let Command::User(entry) = &decision.command
                && !self.replica.applied_elsewhere(entry, slot)
            {
                self.replica.track_inflight(entry.client, entry.seq, slot);
            }
            self.start_accept_round(slot, decision.command);
        }
    }

    /// Candidate -> Leader once a promise quorum holds: re-propose every
    /// recovered in-flight slot under the new ballot (gap fill), then stream.
    ///
    /// A campaign whose ballot has fallen **below the node's own promise** is
    /// refused even with a quorum behind it (#67/#88). Mid-election, two paths
    /// raise `max_promised_ballot` without closing the campaign: `mark_chosen`
    /// on a learned `Commit`/`CatchUpResponse`, and `on_install_snapshot` on a
    /// snapshot whose serving peer minted its promise with no quorum at all.
    /// Winning below the own promise breaks "a leader's ballot >= its own
    /// promise": every self-accept is skipped (`start_accept_round`'s
    /// `ballot >= max_promised_ballot` check), so recovered slots reach
    /// the round tally but never `accepted`, `next_slot` — derived from
    /// `accepted` — lands *below* an in-flight slot, and a later `propose`
    /// re-proposes a different command under the same `(slot, ballot)`: two
    /// values can then assemble accept quorums for one slot at n >= 5.
    /// Refusal is a plain non-win: the election stays open and self-heals —
    /// the next election timeout campaigns at `max(max_promised_ballot.round,
    /// ..) + 1` (`on_check_leader`), above the promise that caused the
    /// refusal.
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "trace", skip_all, fields(node = self.config.id.0)))]
    pub(super) fn try_become_leader(&mut self) {
        // The win gate (#121): every prior configuration covered — one
        // predicate, in one place (`Election::covered`) — at a ballot the
        // node's own promise has not moved past.
        if self.role != NodeRole::Candidate || !self.proposer.phase1_won(self.acceptor.promised()) {
            return;
        }
        let me = self.config.id;
        // Close the campaign: the tally becomes the leadership's, and the
        // faulty-reported slots it could not decide (Case 3) go to the repair
        // probe — except those already chosen here, whose value re-replicates
        // through the normal commit/catch-up paths and repairs the faulty
        // copies in place.
        let outcome = self
            .proposer
            .close_phase1(|slot| self.replica.is_chosen(slot));
        // Post-win restatement of the pool half of the win condition (the
        // quorum half is restated by the component, the ballot half in the
        // postcondition block below): every counted promise came from the
        // pool.
        assert!(
            outcome.promised_by.iter().all(|n| self.in_pool(*n)),
            "every counted promise comes from the addressable pool"
        );
        self.role = NodeRole::Leader;
        self.leader = Some(me);
        self.ballot = outcome.ballot;
        // Registration precedes exercise (#120, invariant 5): Phase 2 runs
        // under exactly the configuration this ballot was registered with.
        // The plain path's static configuration stays bound to no ballot.
        if self.config.has_matchmakers() {
            self.acceptors = outcome.config.clone();
            self.acceptors_since = outcome.ballot;
            self.record_membership();
        } else {
            assert!(
                self.acceptors == outcome.config,
                "a plain campaign runs Phase 1 for its static configuration"
            );
        }
        self.heartbeat_elapsed = 0;
        self.election_elapsed = 0;

        // Fix the allocator/read fence from the complete Phase-1 result before
        // starting only its first bounded recovery page. Fresh proposals may
        // then allocate strictly above every inherited slot while the suffix is
        // drained across later Ready batches.
        let highest = self
            .acceptor
            .records()
            .keys()
            .next_back()
            .copied()
            .max(outcome.highest_reported);
        // The campaign range can reach below the contiguous prefix (a faulty
        // chosen slot extends it), so the allocator clamps at the prefix: a
        // below-prefix report never pulls `next_slot` into chosen territory.
        self.next_slot = highest
            .map_or(self.first_unchosen(), |s| Slot(s.0.saturating_add(1)))
            .max(self.first_unchosen());
        // ---- No-op gap fill: the slots the promise quorum reported *nothing* for.
        //
        // Re-proposing `recovered` covers every slot the quorum saw accepted, and
        // `next_slot` now sits one past the highest of them. What that leaves is the
        // dangerous case *between* them: a slot the old leader accepted alone, while
        // a later slot reached the quorum. It is in neither `chosen` nor
        // `recovered`, and `next_slot` jumped clean over it — so no one would ever
        // propose it again. `propose`/`propose_control` only allocate `next_slot`,
        // and a restart recomputes `next_slot` from the accepted log the same way.
        // The hole would be permanent, and it is not a quiet one: the contiguous
        // chosen prefix freezes one below it cluster-wide (`advance_chosen_index`
        // walks contiguously) while higher slots keep being chosen, the fresh-leader
        // read fence sits above it so no read ever confirms again, and commit-replay
        // catch-up cannot heal it — every node's prefix is frozen below the hole, so
        // no peer has anything to replay.
        //
        // Filling it with a [`Control::Noop`] is safe for the ordinary Phase-1
        // reason. Any value already chosen at that slot was accepted by a quorum,
        // which intersects this promise quorum, so at least one Promise would have
        // reported it (an acceptor that truncated the range Nacks instead of
        // under-reporting — see the floor guard in `on_prepare`). Nothing was
        // reported, so nothing is chosen there and the slot is genuinely free.
        // That licence is the recovery's `RecoveryPolicy::Phase1Backed`.
        //
        // Faulty **chosen** slots — holes below the contiguous prefix — are
        // healed from the quorum's merged reports rather than re-proposed: a
        // chosen value's Q2 intersects this promise quorum, so the
        // highest-ballot report at such a slot IS the chosen value (any
        // accepted record above the choosing ballot carries it, P2c). A slot
        // the tally could not clear (a faulty report above the best `have`)
        // stays blocked and resolves through the probe like any other.
        let prefix_heals: Vec<(Slot, Ballot, Command)> = outcome
            .recovered
            .range(..self.first_unchosen())
            .filter(|(slot, _)| !outcome.blocked.contains(slot) && !self.replica.is_chosen(**slot))
            .map(|(slot, (ballot, command))| (*slot, *ballot, command.clone()))
            .collect();
        for (slot, ballot, command) in prefix_heals {
            self.mark_chosen(slot, &command, ballot);
        }

        let recovery_start = self.first_unchosen();
        let recovered: BTreeMap<Slot, Command> = outcome
            .recovered
            .into_iter()
            .map(|(slot, (_ballot, command))| (slot, command))
            .collect();
        self.election_gap_fills = 0;
        self.leadership_origin = LeadershipOrigin::Elected;
        // An election *is* the quorum report that licenses no-op filling.
        self.proposer.open_recovery(
            recovered,
            outcome.blocked,
            recovery_start,
            self.next_slot,
            RecoveryPolicy::Phase1Backed,
        );
        self.pump_leader_recovery();
        // The fresh-leader read fence: nothing decided under an earlier ballot
        // can sit above `next_slot - 1` (the prepare quorum reported it all), so
        // reads wait until the chosen prefix covers that slot. Beat seqs are
        // per-ballot; cross-ballot ack confusion is impossible because an ack
        // must echo the current ballot to count.
        self.read_floor = self.next_slot.0.checked_sub(1).map(Slot);
        self.heartbeat_seq = 0;
        self.read_rounds.clear();
        // CheckQuorum: a fresh leadership starts a fresh ack window (self is
        // always reachable — when it is an acceptor at all).
        self.quorum_elapsed = 0;
        self.quorum_acked_by.clear();
        if self.is_acceptor() {
            self.quorum_acked_by.insert(me);
        }
        // Fresh-leader postconditions (#67/#88): the win condition demanded
        // `e.ballot >= max_promised_ballot`, and nothing in the re-propose or
        // gap-fill loops raises the promise past the leader's own ballot.
        assert!(
            self.ballot >= self.acceptor.promised(),
            "a fresh leader's ballot is at or above its own promise"
        );
        assert!(
            self.proposer.election().is_none(),
            "winning closes the campaign"
        );
        // Every slot below `next_slot` is chosen, re-proposed, or gap-filled,
        // so the allocator never hands out a slot inside the chosen prefix.
        assert!(
            self.next_slot >= self.first_unchosen(),
            "a fresh leader's next slot sits at or past the chosen prefix"
        );
        // The leadership's garbage-collection campaign (#123): decide, once
        // the fence is held by a quorum of `C_b`, that every configuration
        // below this ballot may be forgotten.
        if self.config.has_matchmakers() {
            self.open_gc(&outcome.prior);
        }
    }

    /// Start one bounded page of inherited Phase-2 rounds. The first page is
    /// created on election victory; [`RawNode::advance_recovery`] schedules at
    /// most one more page after the driver finishes the batch just processed.
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "trace", skip_all, fields(node = self.config.id.0)))]
    pub(super) fn pump_leader_recovery(&mut self) {
        if self.role != NodeRole::Leader || self.pending_recovery_batch.is_some() {
            return;
        }
        let mut processed = 0_usize;
        let mut started = 0_usize;
        let mut gap_fills = 0_usize;
        while processed < RECOVERY_BATCH {
            let Some((slot, step)) = self.proposer.recovery_next() else {
                break;
            };
            processed += 1;
            let (command, is_gap) = match step {
                RecoveryStep::Recovered(command) => (command, false),
                RecoveryStep::Fill => (Command::Control(Control::Noop), true),
                // A slot the predecessor did not describe: an inherited
                // recovery skips it (see `RecoveryPolicy::Inherited`).
                RecoveryStep::Undescribed => continue,
            };

            // A previous page can decide and compact slots underneath the
            // continuation (especially in a single-node cluster), so each slot
            // is re-checked at the instant its round starts.
            if slot < self.acceptor.first_slot() || self.replica.is_chosen(slot) {
                continue;
            }
            // A blocked slot (Case 3: wait) is neither re-proposed nor
            // no-op-filled here: the open repair probe owns its resolution.
            if self.proposer.recovery_blocked(slot) {
                continue;
            }
            if is_gap {
                // Only a Phase-1-backed recovery may invent a value: the
                // promise quorum's silence is the licence. A handoff-installed
                // leadership ran no Phase 1 (its policy is `Inherited`), and
                // the fill never reaches into the chosen prefix — the cursor
                // starts at its frontier and a slot the prefix absorbed since
                // is skipped as chosen above.
                assert!(
                    matches!(self.leadership_origin, LeadershipOrigin::Elected),
                    "only an elected leader gap-fills a slot its promise quorum never reported"
                );
                assert!(
                    slot >= self.first_unchosen(),
                    "a no-op gap fill never targets a slot inside the chosen prefix"
                );
                gap_fills += 1;
                self.election_gap_fills = self.election_gap_fills.saturating_add(1);
            }
            if let Command::User(entry) = &command
                && !self.replica.applied_elsewhere(entry, slot)
            {
                self.replica.track_inflight(entry.client, entry.seq, slot);
            }
            self.start_accept_round(slot, command);
            started += 1;
        }

        let remaining = self.proposer.recovery_remaining();
        if remaining == 0 {
            // Closure postconditions live in the component: a recovery only
            // closes once its cursor swept the whole inherited range, leaving
            // nothing at or past the cursor unvisited.
            self.proposer.close_drained_recovery();
        }
        if processed > 0 {
            self.pending_recovery_batch = Some((started, gap_fills, remaining));
        }
    }

    /// A rejection of an in-flight ballot. Step down to Follower and let the
    /// randomized election timeout reschedule us. We do **not** immediately
    /// re-prepare: that (with the randomized timeout) is the dueling-proposer
    /// livelock fix. The reported promise is diagnostic only: retaining an
    /// arbitrary wire round would let one garbage Nack pin every future campaign.
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "trace", skip_all, fields(node = self.config.id.0, from = from.0, round = ballot.round, slot = slot.0)))]
    pub(super) fn on_nack(&mut self, from: NodeId, ballot: Ballot, _promised: Ballot, slot: Slot) {
        if !self.in_pool(from) {
            return;
        }
        if self.proposer.supersedes(ballot, slot) {
            self.become_follower(None);
        }
    }
}
