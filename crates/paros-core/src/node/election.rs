use super::{
    BTreeMap, BTreeSet, Ballot, Command, Control, LEADER_RECOVERY_BATCH, Message, NodeId, NodeRole,
    PROMISE_BATCH, RawNode, Slot,
};

/// Volatile per-ballot Phase-1 state while a Candidate recovers the log suffix.
pub(super) struct Election {
    /// The ballot this election runs under.
    pub(super) ballot: Ballot,
    /// First slot this election recovers (`chosen_index + 1`, or `Slot(0)`).
    pub(super) from_slot: Slot,
    /// Acceptors (incl. self) that have promised `ballot`.
    pub(super) promised_by: BTreeSet<NodeId>,
    /// Highest-ballot accepted command per slot seen across the promise quorum,
    /// for slots `>= from_slot`. Drives gap-fill re-proposal once leader.
    pub(super) recovered: BTreeMap<Slot, (Ballot, Command)>,
    /// The tri-state's third answer, per slot: which acceptor reported its copy
    /// **faulty** (value lost, identity known) at what accepted ballot. A
    /// faulty report is silence toward the none-tally, never denial: it blocks
    /// the no-op gap fill at its slot until [`RawNode::slot_decidable`] finds a
    /// full Q1 of qualifying reports (Stage 8, CTRL restatement R2/R3).
    pub(super) faulty_reports: BTreeMap<Slot, BTreeMap<NodeId, Ballot>>,
    /// Next suffix-page cursor expected from each non-terminal acceptor.
    pub(super) promise_next: BTreeMap<NodeId, Slot>,
}

/// Bounded continuation for a newly elected leader's recovered suffix.
pub(super) struct LeaderRecovery {
    /// Highest-ballot command reported for each retained slot.
    pub(super) recovered: BTreeMap<Slot, Command>,
    /// Slots the Phase-1 tally could not decide (Case 3: wait): neither
    /// re-proposed nor no-op-filled by the pump; the open [`RepairProbe`]
    /// resolves them as stragglers answer.
    pub(super) blocked: BTreeSet<Slot>,
    /// Next slot to recover or fill.
    pub(super) cursor: Slot,
    /// One past the highest slot covered by the Phase-1 quorum.
    pub(super) end: Slot,
}

/// The leader's open **distributed commitment determination** (Stage 8): the
/// faulty slots its winning promise quorum resolved neither as Case 1 (some
/// `have`) nor Case 2 (a full Q1 of qualifying `none`). The leader keeps
/// re-querying the peers that have not answered (their `Promise` pages arrive
/// through the ordinary Phase-1 path, at the leader's own ballot) and decides
/// each blocked slot the moment the tally allows; a probe that stays blocked
/// for a full recovery timeout resigns the leadership (CTRL §4.2).
pub(super) struct RepairProbe {
    /// The leadership ballot the probe queries at.
    pub(super) ballot: Ballot,
    /// First slot the original Phase 1 covered (re-sent `Prepare`s echo it).
    pub(super) from_slot: Slot,
    /// Acceptors (incl. self) whose complete suffix answer has been merged.
    pub(super) answered: BTreeSet<NodeId>,
    /// Faulty reports per still-blocked slot: reporter → accepted ballot.
    pub(super) faulty_reports: BTreeMap<Slot, BTreeMap<NodeId, Ballot>>,
    /// Highest-ballot `have` seen per still-blocked slot.
    pub(super) best_have: BTreeMap<Slot, (Ballot, Command)>,
    /// Slots still undecidable (Case 3: wait).
    pub(super) blocked: BTreeSet<Slot>,
    /// Next suffix-page cursor expected from each non-terminal straggler.
    pub(super) promise_next: BTreeMap<NodeId, Slot>,
}

/// Whether a faulty-reported slot is decidable, and with what.
///
/// The unified CTRL restatement (R2/R3): let `threshold` be the highest
/// reported `have` ballot at the slot (`None` when nothing was reported). A
/// chosen value at or below `threshold` is value-identical to the best `have`
/// (the P2c chain), and a value chosen *above* it would leave a record at
/// ballot `> threshold` on some member of every Q2 — so the slot is decidable
/// the moment a full Q1 of answered acceptors each reported either nothing
/// (`none`) or a record at ballot `<= threshold`: quorum intersection then
/// rules out any hidden chosen value. Decide the best `have` (Case 1) or
/// `Noop` (Case 2, `threshold = None` degenerates to a full Q1 of `none`).
/// Anything less is Case 3: wait.
fn qualifying_answers(
    answered: &BTreeSet<NodeId>,
    reporters: Option<&BTreeMap<NodeId, Ballot>>,
    threshold: Option<Ballot>,
) -> usize {
    let disqualified = reporters.map_or(0, |reporters| {
        reporters
            .iter()
            .filter(|(node, ballot)| answered.contains(node) && Some(**ballot) > threshold)
            .count()
    });
    answered.len().saturating_sub(disqualified)
}

/// A `Promise` page is useful only at the exact requested cursor, carries at
/// most the advertised bound across both tri-state maps, keeps them disjoint,
/// and advances its continuation past everything it reported.
fn promise_page_shape_valid(
    expected: Slot,
    accepted: &BTreeMap<Slot, (Ballot, Command)>,
    faulty: &BTreeMap<Slot, Ballot>,
    from_slot: Slot,
    next_from_slot: Option<Slot>,
) -> bool {
    let len = accepted.len() + faulty.len();
    from_slot == expected
        && len <= PROMISE_BATCH
        && accepted.keys().all(|slot| *slot >= from_slot)
        && faulty
            .keys()
            .all(|slot| *slot >= from_slot && !accepted.contains_key(slot))
        && next_from_slot.is_none_or(|next| {
            len == PROMISE_BATCH
                && next > from_slot
                && accepted.keys().next_back().is_none_or(|last| next > *last)
                && faulty.keys().next_back().is_none_or(|last| next > *last)
        })
}

impl RawNode {
    // ---- election / leadership --------------------------------------------

    /// Election clock fired: become a Candidate and run one Phase 1 (per ballot)
    /// over the whole uncommitted log suffix.
    pub(super) fn on_check_leader(&mut self) {
        if self.role == NodeRole::Leader {
            return;
        }
        let me = self.config.id;
        let base_round = self
            .hard_state
            .max_promised_ballot
            .round
            .max(self.ballot.round);
        let Some(round) = base_round.checked_add(1) else {
            // The wire/domain round space is exhausted. There is no strictly
            // higher valid ballot to campaign at, so remain a follower rather
            // than wrapping or reusing the maximum round.
            self.become_follower(None);
            return;
        };
        self.role = NodeRole::Candidate;
        self.leader = None;
        self.ballot = Ballot { round, node: me };
        self.set_promise(self.ballot);

        let from_slot = self.first_unchosen();
        let recovered: BTreeMap<Slot, (Ballot, Command)> = self
            .accepted
            .range(from_slot..)
            .map(|(s, v)| (*s, v.clone()))
            .collect();
        // Seed the tri-state tally with this node's own faulty entries: a
        // candidate is its own first acceptor, and its rotted copies must block
        // the none-tally exactly like a peer's (never silently count as
        // "nothing accepted here").
        let mut faulty_reports: BTreeMap<Slot, BTreeMap<NodeId, Ballot>> = BTreeMap::new();
        for (slot, ballot) in self.faulty.range(from_slot..) {
            faulty_reports.entry(*slot).or_default().insert(me, *ballot);
        }
        let mut promised_by = BTreeSet::new();
        promised_by.insert(me);
        self.election = Some(Election {
            ballot: self.ballot,
            from_slot,
            promised_by,
            recovered,
            faulty_reports,
            promise_next: BTreeMap::new(),
        });
        self.proposer.clear();
        self.leader_recovery = None;
        self.repair_probe = None;
        self.repair_elapsed = 0;
        self.resend_cursor = None;
        // A campaign opens at a ballot this node itself just promised: the
        // fresh round is strictly above every round in the max above, so the
        // promise landed exactly on the campaign ballot.
        assert!(
            self.hard_state.max_promised_ballot == self.ballot,
            "a candidate promises the ballot it campaigns at"
        );
        self.broadcast(&Message::Prepare {
            config_id: self.hard_state.config_id,
            from: me,
            ballot: self.ballot,
            from_slot,
        });
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
    /// **leader** with an open [`RepairProbe`] also lands here: a straggler's
    /// late answer (or a re-queried peer's fresh one) is merged into the probe
    /// and may resolve a blocked slot.
    pub(super) fn on_promise(
        &mut self,
        from: NodeId,
        ballot: Ballot,
        from_slot: Slot,
        accepted: BTreeMap<Slot, (Ballot, Command)>,
        faulty: BTreeMap<Slot, Ballot>,
        next_from_slot: Option<Slot>,
    ) {
        // Quorum sets are keyed by NodeId: an id outside the configured
        // membership must never inflate one (wire hygiene; peers are trusted
        // but a misrouted or misconfigured sender is not a quorum member).
        if !self.config.peers.contains(&from) {
            return;
        }
        if self.role == NodeRole::Leader {
            self.on_probe_promise(from, ballot, from_slot, &accepted, &faulty, next_from_slot);
            return;
        }
        let mut request_next = None;
        {
            let Some(e) = self.election.as_mut() else {
                return;
            };
            if e.ballot != ballot || e.promised_by.contains(&from) {
                return;
            }
            let expected = e.promise_next.get(&from).copied().unwrap_or(e.from_slot);
            if !promise_page_shape_valid(expected, &accepted, &faulty, from_slot, next_from_slot) {
                return;
            }
            for (slot, (ab, command)) in accepted {
                let supersedes = e.recovered.get(&slot).is_none_or(|(rb, _)| ab > *rb);
                if supersedes {
                    e.recovered.insert(slot, (ab, command));
                }
            }
            for (slot, fb) in faulty {
                e.faulty_reports.entry(slot).or_default().insert(from, fb);
            }
            if let Some(next) = next_from_slot {
                e.promise_next.insert(from, next);
                request_next = Some(next);
                // A valid page is leader contact for election-timeout purposes;
                // a long suffix must not make the same campaign expire mid-page.
                self.election_elapsed = 0;
            } else {
                e.promise_next.remove(&from);
                e.promised_by.insert(from);
            }
        }
        if let Some(next) = request_next {
            self.pending_messages.push((
                from,
                Message::Prepare {
                    config_id: self.hard_state.config_id,
                    from: self.config.id,
                    ballot,
                    from_slot: next,
                },
            ));
            return;
        }
        self.try_become_leader();
    }

    /// Merge one straggler `Promise` page into the leader's open repair probe
    /// and resolve any blocked slot the refreshed tally now decides.
    fn on_probe_promise(
        &mut self,
        from: NodeId,
        ballot: Ballot,
        from_slot: Slot,
        accepted: &BTreeMap<Slot, (Ballot, Command)>,
        faulty: &BTreeMap<Slot, Ballot>,
        next_from_slot: Option<Slot>,
    ) {
        let mut request_next = None;
        {
            let Some(probe) = self.repair_probe.as_mut() else {
                return;
            };
            if probe.ballot != ballot || probe.answered.contains(&from) {
                return;
            }
            let expected = probe.promise_next.get(&from).copied().unwrap_or(probe.from_slot);
            if !promise_page_shape_valid(expected, accepted, faulty, from_slot, next_from_slot) {
                return;
            }
            // Only the still-blocked slots matter: everything else was decided
            // or re-proposed when the election closed.
            for (slot, (ab, command)) in accepted {
                if probe.blocked.contains(slot)
                    && probe.best_have.get(slot).is_none_or(|(rb, _)| ab > rb)
                {
                    probe.best_have.insert(*slot, (*ab, command.clone()));
                }
            }
            for (slot, fb) in faulty {
                if probe.blocked.contains(slot) {
                    probe
                        .faulty_reports
                        .entry(*slot)
                        .or_default()
                        .insert(from, *fb);
                }
            }
            if let Some(next) = next_from_slot {
                probe.promise_next.insert(from, next);
                request_next = Some(next);
            } else {
                probe.promise_next.remove(&from);
                probe.answered.insert(from);
            }
        }
        if let Some(next) = request_next {
            self.pending_messages.push((
                from,
                Message::Prepare {
                    config_id: self.hard_state.config_id,
                    from: self.config.id,
                    ballot,
                    from_slot: next,
                },
            ));
            return;
        }
        self.resolve_blocked_repairs();
    }

    /// Decide every blocked slot the current probe tally allows: Case 1
    /// (re-propose the best `have`) or Case 2 (a full Q1 of qualifying answers
    /// with no `have`: decide `Noop`). Closes the probe when nothing stays
    /// blocked.
    pub(super) fn resolve_blocked_repairs(&mut self) {
        let quorum = self.quorum();
        let mut decisions: Vec<(Slot, Command, bool)> = Vec::new();
        {
            let Some(probe) = self.repair_probe.as_mut() else {
                return;
            };
            for slot in probe.blocked.clone() {
                let have = probe.best_have.get(&slot);
                let threshold = have.map(|(b, _)| *b);
                let qualifying = qualifying_answers(
                    &probe.answered,
                    probe.faulty_reports.get(&slot),
                    threshold,
                );
                if qualifying < quorum {
                    continue;
                }
                let (command, is_have) = have.map_or_else(
                    || (Command::Control(Control::Noop), false),
                    |(_b, command)| (command.clone(), true),
                );
                probe.blocked.remove(&slot);
                probe.best_have.remove(&slot);
                probe.faulty_reports.remove(&slot);
                decisions.push((slot, command, is_have));
            }
            if probe.blocked.is_empty() {
                self.repair_probe = None;
                self.repair_elapsed = 0;
            }
        }
        for (slot, command, is_have) in decisions {
            if self.chosen.contains_key(&slot) || slot < self.first_slot {
                continue;
            }
            if is_have {
                self.repair_case1 += 1;
            } else {
                self.repair_case2 += 1;
            }
            if let Command::User(entry) = &command
                && !self.applied_elsewhere(entry, slot)
            {
                self.inflight.insert((entry.client, entry.seq), slot);
            }
            self.start_accept_round(slot, command);
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
    /// `proposer` but never `accepted`, `next_slot` — derived from `accepted` —
    /// lands *below* an in-flight slot, and a later `propose` re-proposes a
    /// different command under the same `(slot, ballot)`: two values can then
    /// assemble accept quorums for one slot at n >= 5. Refusal is a plain
    /// non-win: the election stays open and self-heals — the next election
    /// timeout campaigns at `max(max_promised_ballot.round, ..) + 1`
    /// (`on_check_leader`), above the promise that caused the refusal.
    pub(super) fn try_become_leader(&mut self) {
        let quorum = self.quorum();
        let won = self.role == NodeRole::Candidate
            && self.election.as_ref().is_some_and(|e| {
                e.promised_by.len() >= quorum && e.ballot >= self.hard_state.max_promised_ballot
            });
        if !won {
            return;
        }
        let me = self.config.id;
        let e = self.election.take().expect("won implies an election");
        self.role = NodeRole::Leader;
        self.leader = Some(me);
        self.ballot = e.ballot;
        self.heartbeat_elapsed = 0;
        self.election_elapsed = 0;
        self.proposer.clear();
        self.resend_cursor = None;

        // Fix the allocator/read fence from the complete Phase-1 result before
        // starting only its first bounded recovery page. Fresh proposals may
        // then allocate strictly above every inherited slot while the suffix is
        // drained across later Ready batches.
        let highest = self
            .accepted
            .keys()
            .chain(e.recovered.keys())
            .chain(e.faulty_reports.keys())
            .max()
            .copied();
        self.next_slot = highest.map_or(self.first_unchosen(), |s| Slot(s.0.saturating_add(1)));
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
        // ---- Faulty-slot tally (Stage 8, CTRL): a slot some quorum member
        // reported *faulty* is fair game for the pump only if the tally already
        // rules out a hidden chosen value (see [`qualifying_answers`]): then a
        // reported `have` re-proposes normally and an all-`none` slot no-op
        // fills normally. Anything else is **blocked** — Case 3: wait — and
        // moves to the repair probe, which keeps querying stragglers.
        let quorum = self.quorum();
        let mut blocked: BTreeSet<Slot> = BTreeSet::new();
        let mut probe_have: BTreeMap<Slot, (Ballot, Command)> = BTreeMap::new();
        let mut probe_faulty: BTreeMap<Slot, BTreeMap<NodeId, Ballot>> = BTreeMap::new();
        for (slot, reporters) in &e.faulty_reports {
            if self.chosen.contains_key(slot) {
                // Already decided: the value re-replicates through the normal
                // commit/catch-up paths, repairing the faulty copies in place.
                continue;
            }
            let have = e.recovered.get(slot);
            let threshold = have.map(|(b, _)| *b);
            let qualifying = qualifying_answers(&e.promised_by, Some(reporters), threshold);
            if qualifying >= quorum {
                continue;
            }
            blocked.insert(*slot);
            if let Some((b, command)) = have {
                probe_have.insert(*slot, (*b, command.clone()));
            }
            probe_faulty.insert(*slot, reporters.clone());
        }
        if blocked.is_empty() {
            self.repair_probe = None;
        } else {
            self.repair_probe = Some(RepairProbe {
                ballot: e.ballot,
                from_slot: e.from_slot,
                answered: e.promised_by.clone(),
                faulty_reports: probe_faulty,
                best_have: probe_have,
                blocked: blocked.clone(),
                promise_next: BTreeMap::new(),
            });
        }
        self.repair_elapsed = 0;

        let recovery_start = self.first_unchosen();
        let recovered: BTreeMap<Slot, Command> = e
            .recovered
            .into_iter()
            .map(|(slot, (_ballot, command))| (slot, command))
            .collect();
        self.election_gap_fills = 0;
        self.leader_recovery = Some(LeaderRecovery {
            recovered,
            blocked,
            cursor: recovery_start,
            end: self.next_slot,
        });
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
        // always reachable).
        self.quorum_elapsed = 0;
        self.quorum_acked_by.clear();
        self.quorum_acked_by.insert(me);
        // Fresh-leader postconditions (#67/#88): the win condition demanded
        // `e.ballot >= max_promised_ballot`, and nothing in the re-propose or
        // gap-fill loops raises the promise past the leader's own ballot.
        assert!(
            self.ballot >= self.hard_state.max_promised_ballot,
            "a fresh leader's ballot is at or above its own promise"
        );
        assert!(self.election.is_none(), "winning closes the campaign");
        // Every slot below `next_slot` is chosen, re-proposed, or gap-filled,
        // so the allocator never hands out a slot inside the chosen prefix.
        assert!(
            self.next_slot >= self.first_unchosen(),
            "a fresh leader's next slot sits at or past the chosen prefix"
        );
    }

    /// Start one bounded page of inherited Phase-2 rounds. The first page is
    /// created on election victory; [`RawNode::advance_recovery`] schedules at
    /// most one more page after the driver finishes the batch just processed.
    pub(super) fn pump_leader_recovery(&mut self) {
        if self.role != NodeRole::Leader || self.pending_recovery_batch.is_some() {
            return;
        }
        let mut processed = 0_usize;
        let mut started = 0_usize;
        let mut gap_fills = 0_usize;
        while processed < LEADER_RECOVERY_BATCH {
            let Some((slot, command, is_gap)) =
                self.leader_recovery.as_mut().and_then(|recovery| {
                    if recovery.cursor >= recovery.end {
                        return None;
                    }
                    let slot = recovery.cursor;
                    recovery.cursor = Slot(recovery.cursor.0.saturating_add(1));
                    let (command, is_gap) = recovery.recovered.remove(&slot).map_or_else(
                        || (Command::Control(Control::Noop), true),
                        |command| (command, false),
                    );
                    Some((slot, command, is_gap))
                })
            else {
                break;
            };
            processed += 1;

            // A previous page can decide and compact slots underneath the
            // continuation (especially in a single-node cluster), so each slot
            // is re-checked at the instant its round starts.
            if slot < self.first_slot || self.chosen.contains_key(&slot) {
                continue;
            }
            // A blocked slot (Case 3: wait) is neither re-proposed nor
            // no-op-filled here: the open repair probe owns its resolution.
            if self
                .leader_recovery
                .as_ref()
                .is_some_and(|recovery| recovery.blocked.contains(&slot))
            {
                continue;
            }
            if is_gap {
                gap_fills += 1;
                self.election_gap_fills = self.election_gap_fills.saturating_add(1);
            }
            if let Command::User(entry) = &command
                && !self.applied_elsewhere(entry, slot)
            {
                self.inflight.insert((entry.client, entry.seq), slot);
            }
            self.start_accept_round(slot, command);
            started += 1;
        }

        let remaining = self.leader_recovery.as_ref().map_or(0, |recovery| {
            usize::try_from(recovery.end.0.saturating_sub(recovery.cursor.0)).unwrap_or(usize::MAX)
        });
        if remaining == 0 {
            self.leader_recovery = None;
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
    pub(super) fn on_nack(&mut self, from: NodeId, ballot: Ballot, _promised: Ballot, slot: Slot) {
        if !self.config.peers.contains(&from) {
            return;
        }
        let superseded = self.election.as_ref().is_some_and(|e| e.ballot == ballot)
            || self.proposer.get(&slot).is_some_and(|p| p.ballot == ballot);
        if superseded {
            self.become_follower(None);
        }
    }
}
