//! The node's **learner wiring**: how a chosen value — decided by this
//! node's own tally, by a peer's `Commit`, by a proxy leader's `Commit`
//! (#142), by a catch-up replay or by a handoff's decided tail — reaches the
//! [`Acceptor`](crate::acceptor::Acceptor) (its authoritative record) and
//! the [`Replica`](crate::replica::Replica) (the contiguous prefix), and how
//! the prefix walk's consequences (a decided truncation, the reads waiting
//! on the apply condition) are applied. The
//! Phase-2 half — opening a round, fanning it out, folding the votes and
//! deciding — is `node/phase2.rs`.

use super::{Ballot, ColocatedNode, Command, NodeRole, Party, Slot};

impl ColocatedNode {
    /// Learner: a command was chosen elsewhere — by another leader's tally,
    /// or by the proxy leader this node delegated the slot's round to
    /// (#142). Record it; advance the prefix; and close the round this node
    /// still holds open at that `(slot, ballot)`, which is how a delegated
    /// round ends (the proxy folded the votes, this is its verdict) and how
    /// a round taken back from a proxy that decided it after all is retired
    /// without a second decision.
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "trace", skip_all, fields(node = self.config.id.0, round = ballot.round, slot = slot.0)))]
    pub(super) fn on_commit(&mut self, from: Party, ballot: Ballot, slot: Slot, command: &Command) {
        // Wire hygiene: a learn raises the durable promise to the choosing
        // ballot (`mark_chosen`), so a `Commit` from outside the pool and the
        // proxy namespace could pin the promise at any ballot and end every
        // later campaign. Only a pooled node or a proxy of the deployment
        // teaches a decision, as only a pooled node's `Nack` moves a ballot.
        if !self.is_party_addressable(from) {
            return;
        }
        if ballot >= self.ballot {
            self.election_elapsed = 0;
        }
        self.mark_chosen(slot, command, ballot);
        // `mark_chosen` asserted that a decision at the open round's ballot
        // carries the round's command, so closing it here never drops a
        // round whose command differs from what was chosen.
        if self.proposer.is_round_open_at(slot, ballot) {
            self.proposer.close_round(slot);
        }
        // A learned slot is chosen here, or compacted away as chosen.
        if slot >= self.acceptor.first_slot() {
            assert!(
                self.replica.is_chosen(slot),
                "a committed slot is learned chosen"
            );
        }
        assert!(
            !self.proposer.is_round_open_at(slot, ballot),
            "a committed round is closed"
        );
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
            // two-values-for-one-slot violation, caught where it lands. The
            // trust rule (decision 30): a correct peer teaches only a value
            // that a quorum chose, and Paxos chooses one value per slot, so a
            // pooled peer cannot send another.
            assert!(
                known == command,
                "a slot already chosen here is relearned with the same value"
            );
            // Known value, nothing to relearn — but still re-drive the walk: a
            // trim-point jump (or a boot) can leave `chosen_index` *below* a
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
        // The acceptor owns the record rule (`Acceptor::learn_chosen`): the
        // promise rises first, a held record of the chosen command at or
        // above the choosing ballot stays, and a faulty entry is repaired at
        // its own ballot when that is higher. A crash between the promise and
        // the record must never leave a record above the promise: the boot
        // re-asserts "the durable promise dominates every accepted record".
        self.acceptor
            .learn_chosen(slot, ballot, command, &mut self.pending_writes);
        // A live campaign's promise sits at its ballot
        // (`assert_invariants`): a candidate that learns a slot chosen
        // above its ballot can no longer accept its own rounds, so its
        // campaign is over, as a higher `Prepare` ends it in `on_prepare`.
        if self.role == NodeRole::Candidate && self.acceptor.promised() != self.ballot {
            probe!(
                reachable,
                "learn: a slot chosen above a campaign's ballot closes it"
            );
            self.become_follower(None);
        }
        assert!(
            self.role != NodeRole::Candidate || self.acceptor.promised() == self.ballot,
            "a learned slot leaves a live campaign's promise at its ballot"
        );
        self.replica.learn(slot, command);
        // A decision at a probe-blocked slot resolves it (Case 1 arriving
        // through the commit path rather than a straggler's Promise).
        self.proposer.probe_resolved_elsewhere(slot);
        assert!(
            self.proposer
                .probe()
                .is_none_or(|p| !p.blocked().contains(&slot)),
            "a slot learned chosen leaves the repair probe"
        );
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
    /// `SetChosenIndex` writes) and the quorum reads waiting for the prefix to
    /// cover their index. A fold stopped at a hole holds the walk
    /// ([`crate::replica::Replica::advance`]).
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "trace", skip_all, fields(node = self.config.id.0)))]
    pub(super) fn advance_chosen_index(&mut self) {
        let chosen = self.replica.chosen_index();
        // A faulty record inside the chosen prefix is a hole in the journal
        // fold: the replica holds the walk until it heals (catch-up pulled
        // by `tick_repair`, a trim-point jump, or this node's own election).
        let acceptor = &self.acceptor;
        let truncate_up_to = self.replica.advance(
            |slot, command| {
                acceptor
                    .record(slot)
                    .filter(|(_, recorded)| recorded == command)
                    .map(|(ballot, _)| *ballot)
            },
            &mut self.pending_writes,
        );
        if let Some(up_to) = truncate_up_to {
            self.compact(up_to);
        }
        self.replica.assert_compacted();
        self.serve_quorum_reads();
        // The walk only advances the prefix, and never past what it holds.
        assert!(
            self.replica.chosen_index() >= chosen,
            "the chosen prefix never retreats"
        );
        assert!(
            self.acceptor.first_slot() <= self.first_unchosen(),
            "a walk's compaction never outruns the chosen prefix"
        );
    }
}
