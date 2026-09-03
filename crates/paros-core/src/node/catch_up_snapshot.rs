use super::{BTreeMap, Ballot, Command, Message, NodeId, RawNode, SessionEntry, Slot, Value};

/// Maximum number of decided slots one [`Message::CatchUpResponse`] carries. A
/// lagging peer that needs more re-requests on the next heartbeat, so a large
/// backlog is drained over several rounds rather than one unbounded message.
const CATCHUP_BATCH: usize = 64;

impl RawNode {
    /// Serve a lagging peer's catch-up request by replaying the decided range.
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "trace", skip_all, fields(node = self.config.id.0, from = from.0, from_slot = from_slot.0)))]
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
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "trace", skip_all, fields(node = self.config.id.0, to = to.0, from_slot = from_slot.0)))]
    pub(super) fn serve_catchup(&mut self, to: NodeId, from_slot: Slot) {
        let me = self.config.id;
        let Some(ci) = self.replica.chosen_index() else {
            return;
        };
        // Below our floor the decided entries have been truncated away, so no
        // contiguous `CatchUpResponse` can replay them. Offer a snapshot instead:
        // record the offer (the driver attaches the opaque application bytes and
        // sends the `InstallSnapshot`), bringing the peer up to our chosen prefix.
        if from_slot < self.acceptor.first_slot() {
            // An open application repair means this node's own applied state
            // does not cover its chosen index yet: it has no snapshot that
            // matches the boundary an offer would advertise. Stay silent — the
            // requester re-asks each beat, and another peer (or this one, once
            // healed) serves it. Same per-slot-attribution shape as the faulty
            // hole above: never serve what you cannot back.
            if self.replica.app_repair().is_some() {
                return;
            }
            // The offer carries the boundary slot's *choosing* ballot when the
            // log still holds it — a ballot with a real quorum behind it — not
            // this node's own promise, which one quorumless campaigner can
            // mint arbitrarily high; a receiver adopting a minted promise
            // above the live leader's ballot stops acking beats and Nacks its
            // accepts, forcing a spurious election. (If compaction dropped the
            // boundary record, the promise remains the safe upper bound.)
            let choosing = self
                .acceptor
                .record(ci)
                .map_or(self.acceptor.promised(), |(b, _)| *b);
            self.pending_snapshot_offers
                .push((to, ci, choosing, self.config_id));
            return;
        }
        if from_slot > ci {
            return;
        }
        // Both early exits above bound the served range: it starts at or above
        // our floor (entries below it are truncated) and reaches at most our
        // contiguous chosen prefix (everything served is decided).
        assert!(
            from_slot >= self.acceptor.first_slot(),
            "catch-up is served from at or above the floor"
        );
        assert!(
            from_slot <= ci,
            "catch-up is served from inside the chosen prefix"
        );
        let mut entries: BTreeMap<Slot, (Ballot, Command)> = BTreeMap::new();
        let mut expected = from_slot;
        for (slot, command) in self.replica.chosen().range(from_slot..=ci) {
            if entries.len() >= CATCHUP_BATCH {
                break;
            }
            // Per-slot attribution (Stage 8): this node serves only what it can
            // read. Its own faulty chosen record leaves a hole in `chosen`; the
            // replay stops *at* the hole rather than skipping it — a response
            // with a silent gap would let the requester's contiguous walk stall
            // on a range this reply claimed to cover. Another peer (or a
            // snapshot) serves past the hole; faulty means silence, not
            // garbage.
            if *slot != expected {
                break;
            }
            expected = Slot(slot.0 + 1);
            // The choosing ballot is the ballot recorded for this slot in the
            // accepted log (a chosen value is recorded authoritatively there).
            let ballot = self.acceptor.record(*slot).map_or(self.ballot, |(b, _)| *b);
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
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "trace", skip_all, fields(node = self.config.id.0, entries = entries.len())))]
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
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "trace", skip_all, fields(node = self.config.id.0)))]
    pub(super) fn on_install_snapshot(
        &mut self,
        ballot: Ballot,
        chosen_index: Slot,
        snapshot: Value,
        mut sessions: Vec<SessionEntry>,
    ) {
        // Wire guard: a boundary at the numeric ceiling has no floor one past
        // itself (`first_unchosen` computes `chosen_index + 1`), and no honest
        // serving peer can have chosen it. An operating condition (a corrupt
        // or adversarial message), so ignore rather than overflow.
        if chosen_index.0 == u64::MAX {
            return;
        }
        // Never go backward: a snapshot at or below our chosen prefix teaches us
        // nothing and must not lower the floor or re-truncate live slots. The
        // one exception (Stage 8): with an **application repair** open, a
        // snapshot at exactly our chosen index is the below-floor heal — the
        // opaque state it installs covers the very prefix whose decided values
        // this node can no longer read, and installing it closes the repair
        // without moving the chosen index anywhere.
        if let Some(ci) = self.replica.chosen_index() {
            if chosen_index < ci {
                return;
            }
            if chosen_index == ci && self.replica.app_repair().is_none() {
                return;
            }
        }
        // Adopt the choosing ballot. `set_promise` only ever raises the promise
        // (it is a max), so even a far-behind node cannot regress its durable
        // promise here — installing the log does not un-promise a higher ballot.
        if ballot > self.acceptor.promised() {
            self.acceptor.set_promise(ballot, &mut self.pending_writes);
        }
        // Wire hygiene: the boundary is the validation line for the ledger the
        // snapshot carries. A session record naming a slot *above*
        // `chosen_index` claims an applied fact the snapshot's state cannot
        // contain; merged, it would let `propose`'s dedup fast path ack a
        // never-applied slot as `Chosen` — a linearizability violation. Drop
        // such records before they reach `applied_seq` or the durable install.
        sessions.retain(|(_, _, slot)| *slot <= chosen_index);
        // Past the validation boundary the fact may be re-asserted.
        assert!(
            sessions.iter().all(|(_, _, slot)| *slot <= chosen_index),
            "a merged session record stays inside the snapshot boundary"
        );
        let old_floor = self.acceptor.first_slot();
        let old_chosen_index = self.replica.chosen_index();
        // The replica jumps its prefix, closes a repair the boundary covers,
        // and adopts the serving peer's session ledger for the folded prefix
        // (#94: those slots' records will never be walked here).
        self.replica.install(chosen_index, &sessions);
        // Fully compact up to the snapshot: everything at or below
        // `chosen_index` is folded into the opaque bytes, so the acceptor drops
        // the in-memory prefix, raises the floor one past the boundary and
        // persists the install (opaque bytes + boundary + sealed sessions).
        // Faulty entries in the folded prefix are healed by it: their decided
        // effects live in the opaque bytes now. Snapshot-xor-entries: this
        // batch surfaces no committed user entries for the folded prefix; the
        // application installs the opaque state via the driver's storage write,
        // and the ledger is sealed beside it.
        let first = self.acceptor.install(
            chosen_index,
            ballot,
            snapshot,
            sessions,
            &mut self.pending_writes,
        );
        // A probe blocked below the boundary is resolved by the fold as well.
        self.proposer.probe_retain_from(first);
        if self.proposer.probe().is_none() {
            self.repair_elapsed = 0;
        }
        self.proposer.retain_rounds_from(first);
        self.next_slot = self.next_slot.max(first);
        // Install postconditions: the floor lands exactly one past the new
        // chosen boundary, and the durable promise absorbed the snapshot's
        // ballot without ever regressing.
        assert!(
            self.acceptor.first_slot() == self.first_unchosen(),
            "a snapshot install raises the floor to its boundary"
        );
        assert!(
            self.acceptor.promised() >= ballot,
            "a snapshot install never lowers the promise"
        );
        // Floor monotonicity, the install-side pair of `compact`'s "the floor
        // strictly rose": the entry guards refuse any snapshot behind our
        // prefix, so the floor an install lands never regresses.
        assert!(
            self.acceptor.first_slot() >= old_floor,
            "a snapshot install never lowers the floor"
        );
        // The durable frontiers only move forward: the entry guards refuse a
        // boundary behind the prefix, so the chosen index lands at or past
        // where it was, and the allocator is carried past the folded prefix.
        assert!(
            Some(chosen_index) >= old_chosen_index,
            "a snapshot install never rewinds the chosen index"
        );
        assert!(
            self.next_slot >= self.acceptor.first_slot(),
            "a snapshot install carries the allocator past the folded prefix"
        );
        // Every open recovery structure now refers only to retained slots:
        // an application repair the fold did not close sits above the
        // boundary, and a probe keeps only blocked slots at or past the floor.
        assert!(
            self.replica
                .app_repair()
                .is_none_or(|cursor| cursor > chosen_index),
            "an application repair surviving a snapshot install sits above its boundary"
        );
        assert!(
            self.proposer
                .probe()
                .is_none_or(|probe| probe.blocked().first().is_none_or(|s| *s >= first)),
            "a repair probe surviving a snapshot install keeps only retained slots"
        );
        // Re-drive the contiguous walk: a `Commit` learned out of order may
        // already sit in `chosen` just above the boundary, and without the walk
        // this node would freeze at `chosen_index` forever — catch-up loops
        // (`mark_chosen` returns early for a slot already in `chosen`), and a
        // later leadership here would fence reads above the frozen prefix.
        self.advance_chosen_index();
    }
}
