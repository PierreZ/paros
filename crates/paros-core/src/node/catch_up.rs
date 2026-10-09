use super::{BTreeMap, Ballot, ColocatedNode, Command, Message, NodeId, Slot};
use crate::journal_state::JournalState;

/// Maximum number of decided slots one [`Message::CatchUpResponse`] carries. A
/// lagging peer that needs more re-requests on the next heartbeat, so a large
/// backlog is drained over several rounds rather than one unbounded message.
const CATCHUP_BATCH: usize = 64;

// A replay page that carries nothing could never heal a laggard.
const _: () = assert!(CATCHUP_BATCH > 0);

impl ColocatedNode {
    /// Serve a lagging peer's catch-up request by replaying the decided range.
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "trace", skip_all, fields(node = self.config.id.0, from = from.0, from_slot = from_slot.0)))]
    pub(super) fn on_catchup_request(&mut self, from: NodeId, from_slot: Slot) {
        let writes = self.pending_writes.len();
        let promised = self.acceptor.promised();
        self.serve_catchup(from, from_slot);
        // Negative space: serving history is a pure reply.
        assert!(
            self.pending_writes.len() == writes,
            "serving catch-up writes nothing"
        );
        assert!(
            self.acceptor.promised() == promised,
            "serving catch-up moves no promise"
        );
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
        // Below our floor the decided entries have been trimmed away, so no
        // contiguous `CatchUpResponse` can replay them. Tell the peer where
        // the retained log starts (#186): it jumps to our trim point, takes
        // the journal state our log folded to below it (#204), and asks again
        // from there. No bytes (paros runs no application) and no ballot (the
        // peer's promise does not move, #180).
        let point = self.acceptor.first_slot();
        if from_slot < point {
            self.send(
                to,
                Message::TrimmedTo {
                    from: me,
                    point,
                    state: self.replica.journal_base(),
                },
            );
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
            // on a range this reply claimed to cover. Another peer (or its
            // trim point) serves past the hole; faulty means silence, not
            // garbage.
            if *slot != expected {
                probe!(
                    reachable,
                    "catch-up: a replay stops at the server's own faulty chosen record"
                );
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
        // A replay page is bounded, contiguous from the requested slot, and
        // decided end to end.
        assert!(entries.len() <= CATCHUP_BATCH, "a catch-up page is bounded");
        assert!(
            entries.keys().next() == Some(&from_slot),
            "a catch-up page starts at the requested slot"
        );
        assert!(
            entries.keys().next_back().is_some_and(|last| *last <= ci),
            "a catch-up page ends inside the chosen prefix"
        );
        assert!(
            entries
                .keys()
                .zip(entries.keys().skip(1))
                .all(|(a, b)| b.0 == a.0 + 1),
            "a catch-up page is contiguous"
        );
        self.send(to, Message::CatchUpResponse { from: me, entries });
    }

    /// Learn every decided entry a peer replayed to us. Each is chosen (durable,
    /// quorum-decided), so `mark_chosen` records it authoritatively and advances
    /// the contiguous prefix — filling the hole a missed `Accept`+`Commit` left.
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "trace", skip_all, fields(node = self.config.id.0, entries = entries.len())))]
    pub(super) fn on_catchup_response(&mut self, entries: BTreeMap<Slot, (Ballot, Command)>) {
        let chosen = self.replica.chosen_index();
        let floor = self.acceptor.first_slot();
        for (slot, (ballot, command)) in entries {
            self.mark_chosen(slot, &command, ballot);
        }
        // Learning history only ever extends it.
        assert!(
            self.replica.chosen_index() >= chosen,
            "a replay never rewinds the prefix"
        );
        assert!(
            self.acceptor.first_slot() >= floor,
            "a replay never lowers the floor"
        );
    }

    /// Jump below a peer's trim point (#186): the peer trimmed the prefix
    /// this node asked for, so everything below `point` is chosen and gone
    /// cluster-wide. Drop what this node still holds below it, move the
    /// chosen index to at least `point - 1`, raise the floor to `point`,
    /// take the journal state the peer's log folded to below it (#204) when
    /// this node's own fold has not reached the point, and let the walk and the
    /// next catch-up continue from there. A point at or below our floor
    /// teaches nothing and is ignored.
    ///
    /// The promise does not move (#180): nothing a trim point says is about
    /// a ballot. A node that jumps keeps the promise it made, and a
    /// candidate campaigning here still collects promises by Phase 1.
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "trace", skip_all, fields(node = self.config.id.0, point = point.0)))]
    pub(super) fn on_trimmed_to(&mut self, point: Slot, state: JournalState) {
        if point <= self.acceptor.first_slot() {
            return;
        }
        let promised = self.acceptor.promised();
        let old_floor = self.acceptor.first_slot();
        let old_chosen_index = self.replica.chosen_index();
        let sealed = self.replica.trim_to(point, state);
        self.acceptor
            .trim_to(point, sealed, &mut self.pending_writes);
        // A probe blocked below the point is resolved by the jump as well.
        self.proposer.probe_retain_from(point);
        self.proposer.retain_rounds_from(point);
        self.proposer.raise_next_slot(point);
        // Postconditions: the floor lands exactly on the point and never
        // regresses, the chosen index covers everything below it and never
        // rewinds, and the promise did not move.
        assert!(
            self.acceptor.first_slot() == point,
            "a trim-point jump raises the floor to the point"
        );
        assert!(
            point > old_floor,
            "a trim-point jump never lowers the floor"
        );
        assert!(
            self.first_unchosen() >= point,
            "a trim-point jump chooses everything below the point"
        );
        assert!(
            Some(Slot(point.0 - 1)) <= self.replica.chosen_index(),
            "a trim-point jump's chosen index covers the point"
        );
        assert!(
            self.replica.chosen_index() >= old_chosen_index,
            "a trim-point jump never rewinds the chosen index"
        );
        assert!(
            self.acceptor.promised() == promised,
            "a trim-point jump never moves the promise"
        );
        assert!(
            self.proposer.next_slot() >= self.acceptor.first_slot(),
            "a trim-point jump carries the allocator past the dropped prefix"
        );
        assert!(
            self.proposer
                .probe()
                .is_none_or(|probe| probe.blocked().first().is_none_or(|s| *s >= point)),
            "a repair probe surviving a trim-point jump keeps only retained slots"
        );
        // Re-drive the contiguous walk: a `Commit` learned out of order may
        // already sit in `chosen` just above the point, and without the walk
        // this node would freeze there — catch-up loops (`mark_chosen`
        // returns early for a slot already in `chosen`).
        self.advance_chosen_index();
    }
}
