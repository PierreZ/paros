//! The node's **Phase-2 and learner wiring**: how an `Accepted` reaches the
//! [`Proposer`](crate::proposer::Proposer)'s round tally, how a decision is
//! turned into a `Commit`, and how a chosen value reaches the
//! [`Acceptor`](crate::acceptor::Acceptor) (its authoritative record) and the
//! [`Replica`](crate::replica::Replica) (the contiguous prefix). The
//! components decide; this module builds the messages and keeps the node's
//! probe, allocator and read rounds consistent with what they decided.

use super::{Ballot, ColocatedNode, Command, Message, NodeId, NodeRole, Slot};

impl ColocatedNode {
    // ---- proposer / learner ----------------------------------------------

    /// Leader: collect an `Accepted` for a streamed slot; decide on a quorum.
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "trace", skip_all, fields(node = self.config.id.0, from = from.0, round = ballot.round, slot = slot.0)))]
    pub(super) fn on_accepted(&mut self, from: NodeId, ballot: Ballot, slot: Slot, vhash: u64) {
        // Quorum sets are keyed by NodeId, over the **active configuration**:
        // an acceptor outside the ballot's registered configuration must
        // never inflate an accept quorum (wire hygiene, and #122's "a joining
        // acceptor never inflates a quorum it is not in").
        if !self.acceptors.contains(from) {
            return;
        }
        if !self.proposer.fold_accepted(from, ballot, slot, vhash) {
            return;
        }
        // CheckQuorum: an `Accepted` at our current ballot is leader contact,
        // exactly like a beat ack — a busy leader must not need idle beats to
        // keep its window full.
        if self.role == NodeRole::Leader && ballot == self.ballot {
            self.proposer.credit_authority(from);
        }
        self.try_decide(slot);
    }
    /// Learner: a command was chosen elsewhere. Record it; advance the prefix.
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "trace", skip_all, fields(node = self.config.id.0, round = ballot.round, slot = slot.0)))]
    pub(super) fn on_commit(&mut self, ballot: Ballot, slot: Slot, command: &Command) {
        if ballot >= self.ballot {
            self.election_elapsed = 0;
        }
        self.mark_chosen(slot, command, ballot);
    }
    /// Self-accept (if our promise allows) and broadcast `Accept` for `slot`.
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "trace", skip_all, fields(node = self.config.id.0, slot = slot.0)))]
    pub(super) fn start_accept_round(&mut self, slot: Slot, command: Command) {
        // Precondition stack (every caller is leader-gated and floor-guarded):
        // only a leader opens a Phase-2 round, and never below the compaction
        // floor — a below-floor slot is already chosen and truncated.
        assert!(
            self.role == NodeRole::Leader,
            "only a leader starts an accept round"
        );
        assert!(
            slot >= self.acceptor.first_slot(),
            "an accept round never starts below the compaction floor"
        );
        // Re-deciding a chosen slot is guarded by the recovery/repair callers;
        // the propose path can only violate it in the acknowledged still-Leader
        // window after a higher-ballot `Commit` passed the allocator (see the
        // role-couplings note in `assert_invariants`), so the check carries the
        // same promise gate.
        if self.ballot >= self.acceptor.promised() {
            assert!(
                !self.replica.is_chosen(slot),
                "an accept round never re-opens a chosen slot"
            );
        }
        let me = self.config.id;
        let ballot = self.ballot;
        // Never lower our promise: if a competing higher `Prepare` raised it
        // since we became leader, skip the self-accept (the round relies on
        // peer `Accepted`s and will stall, then we step down on the `Nack`).
        // A leader that is not a member of its own configuration (a
        // reconfiguration that removed it, #122) is a proposer and a learner
        // but not an acceptor: it records nothing and casts no vote.
        let own_vote = if self.is_acceptor() && ballot >= self.acceptor.promised() {
            self.acceptor.set_promise(ballot, &mut self.pending_writes);
            self.record_accepted(slot, ballot, command.clone());
            Some(me)
        } else {
            None
        };
        // One round per slot per leadership (asserted by the component).
        self.proposer
            .open_round(slot, ballot, command.clone(), own_vote);
        // Accepts reach the active configuration only: a removed node is
        // never contacted for a new ballot's Phase 2.
        self.broadcast_acceptors(&Message::Accept {
            config_id: self.config_id,
            reply_to: me,
            leader: me,
            ballot,
            slot,
            command,
        });
        self.try_decide(slot);
    }

    /// If an accept quorum holds for `slot`, the entry is chosen: record it and
    /// `Commit` to the peers.
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "trace", skip_all, fields(node = self.config.id.0, slot = slot.0)))]
    pub(super) fn try_decide(&mut self, slot: Slot) {
        let me = self.config.id;
        // The decision is the proposer's, judged over the active
        // configuration's quorum system; every vote in it came from a
        // configured acceptor (`on_accepted` refuses any other sender, and
        // the component restates it).
        let Some((ballot, command)) = self.proposer.decided(slot, &self.acceptors) else {
            return;
        };
        // Decision provenance: only this leadership's own tally decides, at
        // its own ballot.
        assert!(
            self.role == NodeRole::Leader,
            "only a leader decides from its own accept tally"
        );
        assert!(
            ballot == self.ballot,
            "a decision is counted at the leadership ballot"
        );
        self.mark_chosen(slot, &command, ballot);
        // Post-decision: the slot now carries exactly the decided command
        // (unless the decision arrived after the slot was chosen elsewhere
        // and compacted away — then `mark_chosen` is a no-op below the floor).
        assert!(
            slot < self.acceptor.first_slot() || self.replica.chosen_at(slot) == Some(&command),
            "a decided slot is chosen with the decided command"
        );
        self.broadcast(&Message::Commit {
            config_id: self.config_id,
            from: me,
            ballot,
            slot,
            command,
        });
        self.proposer.close_round(slot);
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
    }
}
