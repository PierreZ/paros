use super::{
    BTreeSet, Ballot, Command, Control, Entry, LEADER_RECOVERY_BATCH, Message, NodeId, NodeRole,
    RawNode, Slot, WriteOp, command_fingerprint,
};

/// Volatile state of one in-flight per-slot Phase-2 (`Accept`) round.
pub(super) struct Proposing {
    /// The ballot this slot is being accepted under.
    pub(super) ballot: Ballot,
    /// The command being accepted for this slot.
    pub(super) command: Command,
    /// Acceptors (incl. self) that have accepted this slot's command at `ballot`.
    pub(super) accepted_by: BTreeSet<NodeId>,
}

impl RawNode {
    // ---- proposer / learner ----------------------------------------------

    /// Leader: collect an `Accepted` for a streamed slot; decide on a quorum.
    pub(super) fn on_accepted(&mut self, from: NodeId, ballot: Ballot, slot: Slot, vhash: u64) {
        // Quorum sets are keyed by NodeId: an id outside the configured
        // membership must never inflate one (wire hygiene; peers are trusted
        // but a misrouted or misconfigured sender is not a quorum member).
        if !self.config.peers.contains(&from) {
            return;
        }
        {
            let Some(p) = self.proposer.get_mut(&slot) else {
                return;
            };
            if p.ballot != ballot || command_fingerprint(&p.command) != vhash {
                return;
            }
            p.accepted_by.insert(from);
        }
        // CheckQuorum: an `Accepted` at our current ballot is leader contact,
        // exactly like a beat ack — a busy leader must not need idle beats to
        // keep its window full.
        if self.role == NodeRole::Leader && ballot == self.ballot {
            self.quorum_acked_by.insert(from);
        }
        self.try_decide(slot);
    }
    /// Learner: a command was chosen elsewhere. Record it; advance the prefix.
    pub(super) fn on_commit(&mut self, ballot: Ballot, slot: Slot, command: &Command) {
        if ballot >= self.ballot {
            self.election_elapsed = 0;
        }
        self.mark_chosen(slot, command, ballot);
    }
    /// Self-accept (if our promise allows) and broadcast `Accept` for `slot`.
    pub(super) fn start_accept_round(&mut self, slot: Slot, command: Command) {
        // Precondition stack (every caller is leader-gated and floor-guarded):
        // only a leader opens a Phase-2 round, and never below the compaction
        // floor — a below-floor slot is already chosen and truncated.
        assert!(
            self.role == NodeRole::Leader,
            "only a leader starts an accept round"
        );
        assert!(
            slot >= self.first_slot,
            "an accept round never starts below the compaction floor"
        );
        // Re-deciding a chosen slot is guarded by the recovery/repair callers;
        // the propose path can only violate it in the acknowledged still-Leader
        // window after a higher-ballot `Commit` passed the allocator (see the
        // role-couplings note in `assert_invariants`), so the check carries the
        // same promise gate.
        if self.ballot >= self.hard_state.max_promised_ballot {
            assert!(
                !self.chosen.contains_key(&slot),
                "an accept round never re-opens a chosen slot"
            );
        }
        let me = self.config.id;
        let ballot = self.ballot;
        let mut accepted_by = BTreeSet::new();
        // Never lower our promise: if a competing higher `Prepare` raised it
        // since we became leader, skip the self-accept (the round relies on
        // peer `Accepted`s and will stall, then we step down on the `Nack`).
        if ballot >= self.hard_state.max_promised_ballot {
            self.set_promise(ballot);
            self.record_accepted(slot, ballot, command.clone());
            accepted_by.insert(me);
        }
        self.proposer.insert(
            slot,
            Proposing {
                ballot,
                command: command.clone(),
                accepted_by,
            },
        );
        self.broadcast(&Message::Accept {
            config_id: self.hard_state.config_id,
            from: me,
            ballot,
            slot,
            command,
        });
        self.try_decide(slot);
    }

    /// If an accept quorum holds for `slot`, the entry is chosen: record it and
    /// `Commit` to the peers.
    pub(super) fn try_decide(&mut self, slot: Slot) {
        let quorum = self.quorum();
        let me = self.config.id;
        let decided = match self.proposer.get(&slot) {
            Some(p) if p.accepted_by.len() >= quorum => Some((p.ballot, p.command.clone())),
            _ => None,
        };
        let Some((ballot, command)) = decided else {
            return;
        };
        self.mark_chosen(slot, &command, ballot);
        self.broadcast(&Message::Commit {
            config_id: self.hard_state.config_id,
            from: me,
            ballot,
            slot,
            command,
        });
        self.proposer.remove(&slot);
    }
    /// Record `(slot, entry)` as chosen: persist, re-point the in-flight dedup
    /// mapping at this slot, and advance the contiguous chosen prefix.
    /// Idempotent.
    ///
    /// **Chosen is not applied.** Two of the three callers hand this
    /// non-contiguous slots — `on_commit` takes whatever the network delivers,
    /// and `try_decide` fires the moment a slot's accept quorum completes while
    /// the leader streams later slots concurrently, so slot 6 routinely decides
    /// before slot 5. So nothing here may record a command as *applied*: the
    /// `applied_seq` bump lives in the contiguous walk
    /// ([`RawNode::advance_chosen_index`]) alongside `pending_committed`, which
    /// is the definition [`RawNode::new`]'s boot rebuild has always used.
    pub(super) fn mark_chosen(&mut self, slot: Slot, command: &Command, ballot: Ballot) {
        // A slot below our floor was chosen and then truncated; do not relearn it
        // (that would re-insert a record below the floor via `record_accepted`).
        if slot < self.first_slot {
            return;
        }
        if self.chosen.contains_key(&slot) {
            // Known value, nothing to relearn — but still re-drive the walk: a
            // snapshot install (or a boot) can leave `chosen_index` *below* a
            // slot already present in `chosen`, and a catch-up replay of that
            // slot is then the only message this node keeps receiving. Skipping
            // the walk here wedged that node in a forever catch-up loop.
            self.advance_chosen_index();
            return;
        }
        // Record the *chosen* value as the authoritative accepted command. Using
        // `insert` (not `or_insert_with`) is load-bearing: a node may hold a stale
        // lower-ballot accept it picked up from a failed earlier ballot, and
        // `chosen` is rebuilt from `accepted` on restart. Keeping the stale entry
        // would resurrect a value the cluster never chose for this slot. A chosen
        // value is durable and safe to record at its choosing ballot.
        self.record_accepted(slot, ballot, command.clone());
        if ballot > self.hard_state.max_promised_ballot {
            self.set_promise(ballot);
        }
        self.chosen.insert(slot, command.clone());
        // Re-point `inflight` at what this slot actually decided. Two halves,
        // and both matter:
        //
        // - Whatever was decided here, this slot can no longer be the landing
        //   place of some *other* in-flight client request, so drop any that
        //   still points at it. Keyed on the *slot*, not on a matching
        //   `Command::User`: a node that booted holding an accepted-but-unchosen
        //   entry at this slot (`RawNode::new` rebuilds `inflight` from exactly
        //   those) keeps a dangling mapping when the slot decides as something
        //   else — a `Noop` filled in by a new leader, say. The client's retry
        //   would then get `ProposeResult::Duplicate(slot)` for a slot whose
        //   commit never acks a proposer (a control command has no client
        //   waiter), and the reply would hang to the client's deadline forever.
        //   Clearing by slot lets the retry take a fresh slot and commit.
        // - Then map the entry this slot *did* decide to it. That is what keeps
        //   the chosen-but-not-yet-applied window safe: `applied_seq` only
        //   learns the command when the contiguous walk applies it, so between
        //   "chosen" and "applied" `inflight` is the only table that knows about
        //   it. A retry in that window must find it here and get
        //   `Duplicate(slot)` — the driver then parks the reply on `slot` and
        //   acks it out of the apply loop, exactly when the write enters the
        //   applied prefix. Miss both tables instead and the retry allocates a
        //   *fresh* slot for a command already chosen: duplicate execution,
        //   strictly worse than the early ack. This insert also covers the node
        //   that learns a slot chosen by `Commit` alone (it never proposed it,
        //   so it never had an `inflight` mapping to keep).
        self.inflight.retain(|_, s| *s != slot);
        if let Command::User(entry) = command
            && !self.applied_elsewhere(entry, slot)
        {
            // An identity already applied at another slot is a #94 duplicate:
            // this slot will suppress to a no-op at apply, so pointing a retry
            // at it would park a reply no commit ever acks. Leaving the table
            // alone lets the retry hit the `applied_seq` fast path instead.
            self.inflight.insert((entry.client, entry.seq), slot);
        }
        // A decision at a probe-blocked slot resolves it (Case 1 arriving
        // through the commit path rather than a straggler's Promise).
        if let Some(probe) = self.repair_probe.as_mut()
            && probe.blocked.remove(&slot)
        {
            probe.best_have.remove(&slot);
            probe.faulty_reports.remove(&slot);
            if probe.blocked.is_empty() {
                self.repair_probe = None;
                self.repair_elapsed = 0;
            }
        }
        // A slot healed *below* the contiguous prefix (an open application
        // repair re-learning a faulty chosen record) never reaches the walk, so
        // its at-most-once ledger fold happens here, min-slot-wins: the ledger
        // may already hold this identity at a *higher* slot recorded while the
        // lower record was unreadable, and the cluster-wide first-slot-wins
        // decision must be restored before the repair pump replays either slot.
        if slot < self.first_unchosen()
            && let Command::User(entry) = command
        {
            let seqs = self.applied_seq.entry(entry.client).or_default();
            match seqs.get(&entry.seq).copied() {
                Some(first) if first < slot => {
                    self.duplicate_slots.insert(slot);
                }
                Some(first) if first > slot => {
                    self.duplicate_slots.insert(first);
                    self.duplicate_slots.remove(&slot);
                    seqs.insert(entry.seq, slot);
                }
                _ => {
                    seqs.insert(entry.seq, slot);
                }
            }
        }
        // The chosen/accepted coupling: a chosen slot always holds its
        // authoritative accepted record, at the same command (`serve_catchup`
        // and election recovery both read one map and trust the other).
        // Checked before the walk below, which may compact this very slot away.
        assert!(
            self.accepted.contains_key(&slot),
            "a chosen slot holds its authoritative accepted record"
        );
        assert!(
            self.accepted.get(&slot).map(|(_, c)| c) == Some(command),
            "a chosen slot's accepted record carries the chosen command"
        );
        self.advance_chosen_index();
    }

    /// Whether `entry`'s `(client, seq)` identity is recorded in the applied
    /// ledger at a slot **other than** `slot` — the #94 duplicate test.
    pub(super) fn applied_elsewhere(&self, entry: &Entry, slot: Slot) -> bool {
        self.applied_seq
            .get(&entry.client)
            .and_then(|m| m.get(&entry.seq))
            .is_some_and(|&first| first != slot)
    }

    /// Walk the contiguous chosen prefix forward, surfacing each newly-applied
    /// `(slot, entry)` for the application in order (no gaps).
    ///
    /// This is also where the **client dedup tables move from "in flight" to
    /// "applied"**, one slot at a time and only in prefix order. `applied_seq`
    /// means exactly what its name says and what [`RawNode::new`]'s boot rebuild
    /// has always meant by it — inside the contiguous chosen prefix — so
    /// [`RawNode::propose`]'s fast path can answer `Chosen` (an immediate
    /// `committed: true` to the client) without lying: the write really is in
    /// the applied prefix by then, and the slot it names really is one the node
    /// applied.
    pub(super) fn advance_chosen_index(&mut self) {
        let mut next = self.first_unchosen();
        let mut advanced = 0_usize;
        // Highest `up_to` from any `Truncate` control command that entered the
        // contiguous chosen prefix this pass. Applied *after* the walk so the
        // mutation `compact` makes to `chosen`/`accepted` cannot disturb the
        // iteration above.
        let mut truncate_up_to: Option<Slot> = None;
        while advanced < LEADER_RECOVERY_BATCH
            && let Some(mut command) = self.chosen.get(&next).cloned()
        {
            // The walk is the *only* writer of `chosen_index`, and it advances
            // exactly one slot per iteration — the contiguity the apply seam
            // and the boot rebuild are built on.
            assert!(
                next == self.first_unchosen(),
                "the chosen prefix advances one slot at a time"
            );
            self.hard_state.chosen_index = Some(next);
            self.pending_writes.push(WriteOp::SetChosenIndex(next));
            if let Command::Control(Control::Truncate { up_to }) = &command {
                truncate_up_to = Some(truncate_up_to.map_or(*up_to, |u| u.max(*up_to)));
            }
            // The slot is applied now, so it is no longer in flight — and only a
            // client entry carries `(client, seq)` dedup state at all (a control
            // command has none; its `Truncate` effect is handled just below).
            // Clearing by slot, the same key `mark_chosen` re-pointed the mapping
            // with, is what makes this the exact hand-off: whatever `inflight`
            // held for this slot leaves as `applied_seq` takes it over.
            self.inflight.retain(|_, s| *s != next);
            if let Command::User(entry) = &command {
                let seqs = self.applied_seq.entry(entry.client).or_default();
                match seqs.get(&entry.seq) {
                    // The #94 duplicate: correct Paxos chose this identity at a
                    // second slot (a retry served across a partition plus the
                    // mandatory P2c re-proposal of the deposed leader's lone
                    // accept). Execute the slot as a no-op. The decision reads
                    // only the replicated ledger, and the walk runs in slot
                    // order on every node, so first-slot-wins is cluster-wide
                    // deterministic — and `RawNode::new` re-derives the same
                    // set from sealed sessions + the retained log on restart.
                    Some(&first) if first != next => {
                        self.duplicate_slots.insert(next);
                        self.duplicates_suppressed += 1;
                        command = Command::Control(Control::Noop);
                    }
                    // First application of this identity: record it at this
                    // slot, and never overwrite it later — the ledger entry IS
                    // the at-most-once claim.
                    _ => {
                        seqs.insert(entry.seq, next);
                    }
                }
            }
            // While an application repair is open, the driver's application sits
            // below this walk's frontier: surfacing new committed entries now
            // would apply them out of order. The repair pump re-emits every
            // decided slot in order from its cursor instead (the walk's other
            // effects — the durable chosen index, the dedup hand-off — proceed
            // unchanged, so consensus keeps advancing while the tail heals).
            if self.app_repair.is_none() {
                self.pending_committed.push((next, command));
            }
            next = Slot(next.0 + 1);
            advanced += 1;
        }
        self.chosen_advance_pending = self.chosen.contains_key(&self.first_unchosen());
        // Apply (lazily, "in the background") the truncation the control command
        // decided: drop the now-safe prefix and raise the floor. `compact` clamps
        // to the chosen index just advanced, is idempotent, and emits a
        // `WriteOp::Truncate` ordered *after* the `SetChosenIndex` writes above, so
        // a durable floor never outruns the durable chosen index.
        if let Some(up_to) = truncate_up_to {
            self.compact(up_to);
        }
        // The walk's exit condition, restated as its postcondition: either it
        // consumed the entire contiguous chosen prefix, or exactly one bounded
        // chunk was released and the post-Ready continuation will resume it.
        assert!(
            advanced == LEADER_RECOVERY_BATCH || !self.chosen.contains_key(&self.first_unchosen()),
            "the walk consumes or bounds the contiguous chosen prefix"
        );
        // An open application repair may now be able to advance (the walk can
        // have made new decided slots visible to its cursor).
        self.pump_app_repair();
        // A read round waiting on the apply condition (`chosen_index >= index`,
        // the fresh-leader fence) resolves exactly here. No-op on a follower.
        self.try_confirm_reads();
    }
}
