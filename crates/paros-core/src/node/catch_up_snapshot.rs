use super::{
    BTreeMap, Ballot, Command, Message, NodeId, RawNode, SessionEntry, Slot, Value, WriteOp,
};

/// Maximum number of decided slots one [`Message::CatchUpResponse`] carries. A
/// lagging peer that needs more re-requests on the next heartbeat, so a large
/// backlog is drained over several rounds rather than one unbounded message.
const CATCHUP_BATCH: usize = 64;

impl RawNode {
    /// Serve a lagging peer's catch-up request by replaying the decided range.
    pub(super) fn on_catchup_request(&mut self, from: NodeId, from_slot: Slot) {
        self.serve_catchup(from, from_slot);
    }

    /// Send `to` the decided `(ballot, entry)` per slot for a bounded range at or
    /// after `from_slot`, up to our own contiguous chosen prefix. A node with
    /// nothing chosen at or above `from_slot` sends nothing. Every entry is chosen —
    /// durable and quorum-decided — so the recipient may learn it directly (the
    /// same safety a `Commit` relies on). Used both to answer a pull
    /// ([`Message::CatchUpRequest`]) and to push a decided prefix to a peer whose
    /// heartbeat `commit` shows it is behind us.
    pub(super) fn serve_catchup(&mut self, to: NodeId, from_slot: Slot) {
        let me = self.config.id;
        let Some(ci) = self.hard_state.chosen_index else {
            return;
        };
        // Below our floor the decided entries have been truncated away, so no
        // contiguous `CatchUpResponse` can replay them. Offer a snapshot instead:
        // record the offer (the driver attaches the opaque application bytes and
        // sends the `InstallSnapshot`), bringing the peer up to our chosen prefix.
        if from_slot < self.first_slot {
            // The offer carries the boundary slot's *choosing* ballot when the
            // log still holds it — a ballot with a real quorum behind it — not
            // this node's own promise, which one quorumless campaigner can
            // mint arbitrarily high; a receiver adopting a minted promise
            // above the live leader's ballot stops acking beats and Nacks its
            // accepts, forcing a spurious election. (If compaction dropped the
            // boundary record, the promise remains the safe upper bound.)
            let choosing = self
                .accepted
                .get(&ci)
                .map_or(self.hard_state.max_promised_ballot, |(b, _)| *b);
            self.pending_snapshot_offers
                .push((to, ci, choosing, self.hard_state.config_id));
            return;
        }
        if from_slot > ci {
            return;
        }
        // Both early exits above bound the served range: it starts at or above
        // our floor (entries below it are truncated) and reaches at most our
        // contiguous chosen prefix (everything served is decided).
        assert!(
            from_slot >= self.first_slot,
            "catch-up is served from at or above the floor"
        );
        assert!(
            from_slot <= ci,
            "catch-up is served from inside the chosen prefix"
        );
        let mut entries: BTreeMap<Slot, (Ballot, Command)> = BTreeMap::new();
        for (slot, command) in self.chosen.range(from_slot..=ci) {
            if entries.len() >= CATCHUP_BATCH {
                break;
            }
            // The choosing ballot is the ballot recorded for this slot in the
            // accepted log (a chosen value is recorded authoritatively there).
            let ballot = self.accepted.get(slot).map_or(self.ballot, |(b, _)| *b);
            entries.insert(*slot, (ballot, command.clone()));
        }
        if entries.is_empty() {
            return;
        }
        self.pending_messages
            .push((to, Message::CatchUpResponse { from: me, entries }));
    }

    /// Learn every decided entry a peer replayed to us. Each is chosen (durable,
    /// quorum-decided), so `mark_chosen` records it authoritatively and advances
    /// the contiguous prefix — filling the hole a missed `Accept`+`Commit` left.
    pub(super) fn on_catchup_response(&mut self, entries: BTreeMap<Slot, (Ballot, Command)>) {
        for (slot, (ballot, command)) in entries {
            self.mark_chosen(slot, &command, ballot);
        }
    }

    /// Install an opaque application snapshot from a peer (below-floor recovery):
    /// jump the chosen prefix to `chosen_index`, adopt `max(promise, ballot)` (the
    /// durable promise never regresses — the safety hinge that keeps a recovered
    /// node from re-voting under a stale ballot), and fully compact the log up to
    /// the snapshot (its state is folded into the opaque bytes). A stale snapshot
    /// that would not advance us is ignored.
    pub(super) fn on_install_snapshot(
        &mut self,
        ballot: Ballot,
        chosen_index: Slot,
        snapshot: Value,
        sessions: Vec<SessionEntry>,
    ) {
        // Never go backward: a snapshot at or below our chosen prefix teaches us
        // nothing and must not lower the floor or re-truncate live slots.
        if self
            .hard_state
            .chosen_index
            .is_some_and(|ci| chosen_index <= ci)
        {
            return;
        }
        // Adopt the choosing ballot. `set_promise` only ever raises the promise
        // (it is a max), so even a far-behind node cannot regress its durable
        // promise here — installing the log does not un-promise a higher ballot.
        if ballot > self.hard_state.max_promised_ballot {
            self.set_promise(ballot);
        }
        // Wire value: saturate rather than overflow on an adversarial u64::MAX.
        let first = Slot(chosen_index.0.saturating_add(1));
        self.hard_state.chosen_index = Some(chosen_index);
        // Fully compact up to the snapshot: everything at or below `chosen_index`
        // is folded into the opaque bytes, so drop the in-memory prefix and raise
        // the floor to `first`.
        self.first_slot = first;
        self.accepted = self.accepted.split_off(&first);
        self.chosen = self.chosen.split_off(&first);
        self.chosen_advance_pending = self.chosen.contains_key(&self.first_unchosen());
        self.proposer.retain(|slot, _| *slot >= first);
        // The prefix jumped without the contiguous walk running, so nothing
        // handed the folded slots' `inflight` entries over. Drop them: a mapping
        // to a slot that no longer exists would answer a retry with
        // `Duplicate(slot)` for a slot whose commit can never ack anyone, and
        // the reply would hang to the client's deadline every time.
        self.inflight.retain(|_, s| *s >= first);
        self.next_slot = self.next_slot.max(first);
        // Adopt the serving peer's session ledger for the folded prefix (#94):
        // those slots' log records will never be walked here, so this transfer
        // is the only way this node learns their `(client, seq) -> slot` facts —
        // both for the dedup fast path and for suppressing a later re-choose of
        // the same identity exactly like every peer does. `or_insert` keeps any
        // record this node already holds; the prefixes agree cluster-wide, so a
        // collision carries the same slot either way.
        for (client, seq, slot) in &sessions {
            self.applied_seq
                .entry(*client)
                .or_default()
                .entry(*seq)
                .or_insert(*slot);
        }
        // Persist the install (opaque bytes + boundary + sealed sessions).
        // Snapshot-xor-entries: this batch surfaces no committed user entries
        // for the folded prefix; the application installs the opaque state via
        // the driver's storage write, and the ledger is sealed beside it.
        self.pending_writes.push(WriteOp::InstallSnapshot {
            chosen_index,
            ballot,
            snapshot,
            sessions,
        });
        // Install postconditions: the floor lands exactly one past the new
        // chosen boundary, and the durable promise absorbed the snapshot's
        // ballot without ever regressing.
        assert!(
            self.first_slot == self.first_unchosen(),
            "a snapshot install raises the floor to its boundary"
        );
        assert!(
            self.hard_state.max_promised_ballot >= ballot,
            "a snapshot install never lowers the promise"
        );
        // Re-drive the contiguous walk: a `Commit` learned out of order may
        // already sit in `chosen` just above the boundary, and without the walk
        // this node would freeze at `chosen_index` forever — catch-up loops
        // (`mark_chosen` returns early for a slot already in `chosen`), and a
        // later leadership here would fence reads above the frozen prefix.
        self.advance_chosen_index();
    }
}
