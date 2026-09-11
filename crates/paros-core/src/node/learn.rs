//! The node's **learner wiring**: how a chosen value — decided by this
//! node's own tally, by a peer's `Commit`, by a proxy leader's `Commit`
//! (#142), by a catch-up replay or by a handoff's decided tail — reaches the
//! [`Acceptor`](crate::acceptor::Acceptor) (its authoritative record) and
//! the [`Replica`](crate::replica::Replica) (the contiguous prefix), and how
//! the prefix walk's consequences (a decided truncation, an application
//! repair, the reads waiting on the apply condition) are applied. The
//! Phase-2 half — opening a round, fanning it out, folding the votes and
//! deciding — is `node/phase2.rs`.

use super::{Ballot, ColocatedNode, Command, Slot};

impl ColocatedNode {
    /// Learner: a command was chosen elsewhere — by another leader's tally,
    /// or by the proxy leader this node delegated the slot's round to
    /// (#142). Record it; advance the prefix; and close the round this node
    /// still holds open at that `(slot, ballot)`, which is how a delegated
    /// round ends (the proxy folded the votes, this is its verdict) and how
    /// a round taken back from a proxy that decided it after all is retired
    /// without a second decision.
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "trace", skip_all, fields(node = self.config.id.0, round = ballot.round, slot = slot.0)))]
    pub(super) fn on_commit(&mut self, ballot: Ballot, slot: Slot, command: &Command) {
        if ballot >= self.ballot {
            self.election_elapsed = 0;
        }
        self.mark_chosen(slot, command, ballot);
        // `mark_chosen` asserted that a decision at the open round's ballot
        // carries the round's command, so closing it here never drops a
        // round whose command differs from what was chosen.
        if self
            .proposer
            .rounds()
            .get(&slot)
            .is_some_and(|round| round.ballot() == ballot)
        {
            self.proposer.close_round(slot);
        }
    }

    /// Record `(slot, entry)` as chosen: persist the authoritative record,
    /// hand the fact to the replica, resolve a probe blocked on it, and
    /// advance the contiguous chosen prefix. Idempotent.
    ///
    /// **Chosen is not applied.** Two of the three callers hand this
    /// non-contiguous slots — `on_commit` takes whatever the network delivers,
    /// and `try_decide` fires the moment a slot's accept quorum completes while
    /// the leader streams later slots concurrently, so slot 6 routinely decides
    /// before slot 5. Nothing here records a command as *applied*: that is the
    /// replica's contiguous walk ([`ColocatedNode::advance_chosen_index`]).
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "trace", skip_all, fields(node = self.config.id.0, slot = slot.0, round = ballot.round)))]
    pub(super) fn mark_chosen(&mut self, slot: Slot, command: &Command, ballot: Ballot) {
        // A slot below our floor was chosen and then truncated; do not relearn it
        // (that would re-insert a record below the floor via `record_accepted`).
        if slot < self.acceptor.first_slot() {
            return;
        }
        if let Some(known) = self.replica.chosen_at(slot) {
            // Agreement, locally: a slot is chosen once. Relearning it (a
            // duplicated `Commit`, a catch-up replay, a handoff's decided
            // tail) must bring the same value back; a different one is the
            // two-values-for-one-slot violation, caught where it lands.
            assert!(
                known == command,
                "a slot already chosen here is relearned with the same value"
            );
            // Known value, nothing to relearn — but still re-drive the walk: a
            // snapshot install (or a boot) can leave `chosen_index` *below* a
            // slot already present in `chosen`, and a catch-up replay of that
            // slot is then the only message this node keeps receiving. Skipping
            // the walk here wedged that node in a forever catch-up loop.
            self.advance_chosen_index();
            return;
        }
        // A decision at the ballot of this node's own open round must be the
        // round's command: one proposer per ballot (P2b), and a handoff
        // successor re-proposes its inherited rounds verbatim.
        if let Some(round) = self.proposer.rounds().get(&slot)
            && round.ballot() == ballot
        {
            assert!(
                round.command() == command,
                "a decision at the open round's ballot carries the round's command"
            );
        }
        // Adopt the choosing ballot *before* the record lands, so the batch
        // carries the promise ahead of the accept exactly as `on_accept` does:
        // the write-side ordering the boot scan re-asserts ("the durable
        // promise dominates every accepted record"). Recording first left a
        // crash between the two durable ops with a record above the promise;
        // a spare that only ever learns (never prepared, promise still zero)
        // hit it on 1 seed in 2,000 (17196295897912962235) and refused to
        // boot again.
        if ballot > self.acceptor.promised() {
            self.acceptor.set_promise(ballot, &mut self.pending_writes);
        }
        // Record the *chosen* value as the authoritative accepted command. An
        // upsert is load-bearing: a node may hold a stale lower-ballot accept
        // it picked up from a failed earlier ballot, and `chosen` is rebuilt
        // from the accepted log on restart. Keeping the stale entry would
        // resurrect a value the cluster never chose for this slot.
        self.record_accepted(slot, ballot, command.clone());
        self.replica.learn(slot, command);
        // A decision at a probe-blocked slot resolves it (Case 1 arriving
        // through the commit path rather than a straggler's Promise).
        self.proposer.probe_resolved_elsewhere(slot);
        // The chosen/accepted coupling: a chosen slot always holds its
        // authoritative accepted record, at the same command (`serve_catchup`
        // and election recovery both read one map and trust the other).
        // Checked before the walk below, which may compact this very slot away.
        assert!(
            self.acceptor.record(slot).is_some(),
            "a chosen slot holds its authoritative accepted record"
        );
        assert!(
            self.acceptor.record(slot).map(|(_, c)| c) == Some(command),
            "a chosen slot's accepted record carries the chosen command"
        );
        self.advance_chosen_index();
    }

    /// Walk the contiguous chosen prefix forward
    /// ([`crate::replica::Replica::advance`]), then apply what the walk
    /// decided: the truncation a `Truncate` control command ordered (lazily,
    /// *after* the walk so the mutation cannot disturb the iteration, its
    /// [`WriteOp::Truncate`](crate::WriteOp::Truncate) ordered after the
    /// `SetChosenIndex` writes), the application repair that may now
    /// advance, and the read rounds waiting on the apply condition (the
    /// fresh-leader fence).
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "trace", skip_all, fields(node = self.config.id.0)))]
    pub(super) fn advance_chosen_index(&mut self) {
        let acceptor = &self.acceptor;
        let truncate_up_to = self.replica.advance(
            |slot, command| acceptor.record(slot).map(|(_, c)| c) == Some(command),
            &mut self.pending_writes,
        );
        if let Some(up_to) = truncate_up_to {
            self.compact(up_to);
        }
        self.pump_app_repair();
        self.try_confirm_reads();
        self.serve_quorum_reads();
    }
}
