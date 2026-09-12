//! The node's **Phase-2 wiring**: how a leader opens a per-slot round,
//! fans its `Accept` out (or **delegates** it to a proxy leader, #142),
//! folds the `Accepted`s that come back to it, decides on the
//! [`Proposer`](crate::proposer::Proposer)'s quorum, and takes a delegated
//! round back from a proxy that never answered. The component tallies; this
//! module builds the messages and keeps the node's probe, allocator and read
//! rounds consistent with what it decided. The learner half — a chosen
//! value reaching the acceptor's record and the replica's prefix — is
//! `node/learn.rs`.
//!
//! **Delegation, in one place.** A proxy leader
//! ([`crate::proxy_leader::ProxyLeader`]) is this module's fan-out and
//! fold running on another process: the leader hands it the very
//! `Accept` it would have broadcast, with `reply_to` naming the proxy, and
//! learns the decision from the proxy's `Commit` like any learner. What is
//! *never* delegated is Phase-1-shaped work — an election's recovery
//! re-proposals, its gap fills, a repair probe's decisions — so a fresh
//! leadership's recovery depends on no proxy (Compartmentalized Paxos §3.1
//! moves Phase 2b only; frankenpaxos's proxy adopts no round). A handoff
//! successor's inherited rounds *are* re-delegated, with `leader` naming the
//! successor, so `Accept.leader` never goes stale at a proxy. Liveness
//! under a dead proxy is the leader's: a round re-delegated too often
//! without a decision is **taken back** and run colocated
//! ([`ColocatedNode::take_back_delegated`]); the fallback is always today's
//! colocated Phase 2, and safety never rests on it because two fan-outs of
//! one `(slot, ballot, command)` are P2b-idempotent.

use super::{
    Audience, Ballot, ColocatedNode, Command, Delegation, Message, NodeId, NodeRole, Party, Slot,
};
use crate::membership::{ProxyId, QuorumSystem};

impl ColocatedNode {
    /// Leader: collect an `Accepted` for a streamed slot; decide on a quorum.
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "trace", skip_all, fields(node = self.config.id.0, from = from.0, round = ballot.round, slot = slot.0)))]
    pub(super) fn on_accepted(&mut self, from: NodeId, ballot: Ballot, slot: Slot, vhash: u64) {
        // Quorum sets are keyed by NodeId, over the **active configuration**
        // and the round's column: an acceptor outside the ballot's
        // registered configuration must never inflate an accept quorum (wire
        // hygiene, and #122's "a joining acceptor never inflates a quorum it
        // is not in"), and under a grid an acceptor outside the round's
        // column — one a duplicate or a misroute reached — may well have
        // accepted, but its vote is not the column's and does not count.
        // No open round at the slot: nothing to count toward.
        let Some(column) = self.proposer.round_column(slot) else {
            return;
        };
        if !self.acceptors.is_phase2_addressee(from, column) {
            return;
        }
        // A delegated round's votes are the proxy's: the fold refuses them
        // here, so nothing below runs for one (`Rounds::fold_accepted`).
        if !self.proposer.fold_accepted(from, ballot, slot, vhash) {
            return;
        }
        // CheckQuorum: an `Accepted` at our current ballot is leader contact,
        // exactly like a beat ack — a busy leader must not need idle beats to
        // keep its window full. Only a **colocated** round's votes reach this
        // line (the fold above refused a delegated round's), so a leader whose
        // rounds all run through proxies keeps its standing authority on
        // `HeartbeatAck` alone.
        if self.role == NodeRole::Leader && ballot == self.ballot {
            self.proposer.credit_authority(from);
        }
        self.try_decide(slot);
    }

    /// The proxy `slot`'s round goes to under `delegation`: the driver's
    /// explicit choice, or the core's own `ProxyId(slot % proxy_count)`;
    /// `None` runs the round colocated (always the answer on a deployment
    /// without proxies — the `None` arm).
    ///
    /// # Panics
    ///
    /// If an explicit proxy is not one of the deployment's `proxy_count`.
    fn proxy_for(&self, slot: Slot, delegation: Delegation) -> Option<ProxyId> {
        let count = self.config.proxy_count;
        match delegation {
            Delegation::Colocated => None,
            Delegation::Auto => ProxyId::of(slot, count),
            Delegation::To(proxy) => {
                assert!(
                    proxy.is_in(count),
                    "a delegated round names a proxy of the deployment"
                );
                Some(proxy)
            }
        }
    }

    /// Whether this leadership is **settled** enough to delegate a fresh
    /// proposal's round: no election recovery and no repair probe open, so
    /// the rounds a proxy runs are never the ones a fresh leadership's
    /// recovery depends on.
    pub(super) fn may_delegate(&self) -> bool {
        self.proposer.recovery().is_none() && self.proposer.probe().is_none()
    }

    /// Self-accept (if our promise allows) and broadcast `Accept` for `slot`,
    /// addressed to the column the active configuration derives for it, run
    /// colocated or delegated per `delegation`.
    pub(super) fn start_accept_round(
        &mut self,
        slot: Slot,
        command: Command,
        delegation: Delegation,
    ) {
        // The column this slot's Phase 2 is addressed to: a pure function of
        // the slot under the active configuration (a grid's `slot % cols`,
        // nothing under a majority or a flexible split), so a handoff
        // successor re-proposing this slot and a restarted leader's re-send
        // derive the same column without carrying it.
        let column = self.acceptors.column_of(slot);
        self.start_accept_round_in(slot, command, column, delegation);
    }

    /// Self-accept (if our promise allows) and broadcast `Accept` for `slot`
    /// to `column` — the configuration's own column for the slot, or the
    /// one the driver named ([`ColocatedNode::propose_in`]) — or hand the
    /// round to the proxy `delegation` resolves to.
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "trace", skip_all, fields(node = self.config.id.0, slot = slot.0, column = ?column)))]
    pub(super) fn start_accept_round_in(
        &mut self,
        slot: Slot,
        command: Command,
        column: Option<usize>,
        delegation: Delegation,
    ) {
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
        // A named column is one the active grid has; a majority or a
        // flexible split names none. The core derives its own through the
        // membership boundary, and a driver's override is checked against
        // the same boundary — a stray column is a programmer error.
        assert!(
            match self.acceptors.quorum_system() {
                QuorumSystem::Grid { cols, .. } => column.is_none_or(|c| c < cols),
                _ => column.is_none(),
            },
            "an accept round's column is a column of the active configuration"
        );
        let ballot = self.ballot;
        // The allocator is durable by construction: the round is recorded
        // in this node's own log before its `Accept` leaves, whoever folds
        // it and whether or not this node's vote counts (see
        // [`ColocatedNode::record_own_round`]).
        let recorded = self.record_own_round(slot, ballot, &command);
        if let Some(proxy) = self.proxy_for(slot, delegation) {
            // Delegated: the proxy fans out and folds, and this node casts
            // its vote as one acceptor among the proxy's addressees — it
            // tallies nothing here, and the proxy's fan-out reaches it like
            // any other member of the column.
            self.proposer
                .open_delegated_round(slot, ballot, command.clone(), column, proxy);
            self.send_accept(slot, ballot, command, column, Some(proxy));
            return;
        }
        let own_vote = self.own_vote(recorded, column);
        // One round per slot per leadership (asserted by the component).
        self.proposer
            .open_round(slot, ballot, command.clone(), own_vote, column);
        // Accepts reach the active configuration's addressees only: a
        // removed node is never contacted for a new ballot's Phase 2, and a
        // grid's other columns never see this slot.
        self.send_accept(slot, ballot, command, column, None);
        self.try_decide(slot);
    }

    /// Record the round this leader opens at `slot` in its **own** log,
    /// durably, whenever its promise allows; whether it did.
    ///
    /// **The allocator frontier is durable by construction.** A leader's
    /// `next_slot` is rederived from its accepted log at every boot, and the
    /// handoff's replay guard (`on_relinquish` refuses an authority that
    /// would rewind the allocator) rests on that derivation being the
    /// frontier the leader really had. So every round a leader opens leaves
    /// a record at the leader — a colocated one, a delegated one whose votes
    /// a proxy folds, one in a grid column this node is not in, one under a
    /// configuration that removed this node — before its `Accept` leaves:
    /// the record rides the same batch, and persist-before-send seals it.
    /// The record outside the round's column is the "stray copy" the grid
    /// already admits: a member's vote that does not count, and an honest
    /// answer to the next Phase 1 (only this ballot's proposer proposed
    /// there). The proxy model checker found the rule load-bearing (seed
    /// 756 of its first long run): a delegated round left no record, the
    /// handoff successor crashed, rebooted with its frontier rewound, and a
    /// *duplicated* `Relinquish` re-installed the authority and allocated
    /// the slot a second time at the same ballot — two commands at one
    /// `(slot, ballot)`. A grid leader proposing outside its own column
    /// had the same hole before delegation existed.
    ///
    /// Never lower our promise: if a competing higher `Prepare` raised it
    /// since we became leader, nothing is recorded (the round relies on
    /// peers, cannot be chosen — every Phase-2 quorum meets the promise
    /// quorum that deposed us — and we step down on the `Nack`; a replayed
    /// `Relinquish` is then refused by the promise guard instead).
    fn record_own_round(&mut self, slot: Slot, ballot: Ballot, command: &Command) -> bool {
        if ballot < self.acceptor.promised() {
            return false;
        }
        self.acceptor.set_promise(ballot, &mut self.pending_writes);
        self.record_accepted(slot, ballot, command.clone());
        true
    }

    /// This node's own vote on a colocated round it `recorded`: counted
    /// only when it is an addressee of `column`. A leader that is not —
    /// outside its own configuration (a reconfiguration that removed it,
    /// #122), or under a grid outside the slot's column — is a proposer
    /// and a learner but not one of the round's acceptors: its record is a
    /// stray copy and its vote does not count.
    fn own_vote(&self, recorded: bool, column: Option<usize>) -> Option<NodeId> {
        let me = self.config.id;
        (recorded && self.acceptors.is_phase2_addressee(me, column)).then_some(me)
    }

    /// Queue an `Accept` for `slot`: to every Phase-2 addressee of the
    /// active configuration in `column` except this node — the Phase-2
    /// fan-out, whose addressee list comes from the membership boundary
    /// ([`AcceptorConfig::phase2_addressees`](crate::AcceptorConfig::phase2_addressees)),
    /// never from iterating the membership here — or, with `proxy`, to that
    /// proxy alone as the **delegation** (#142): the same message, with
    /// `reply_to` naming the proxy that will fold the `Accepted`s and, on a
    /// matchmaker deployment, the configuration the proxy fans out to. Both
    /// the first send ([`ColocatedNode::start_accept_round`]) and the
    /// re-send ([`ColocatedNode::resend_pending`]) come through here with
    /// the column the round recorded, so the two always agree.
    pub(super) fn send_accept(
        &mut self,
        slot: Slot,
        ballot: Ballot,
        command: Command,
        column: Option<usize>,
        proxy: Option<ProxyId>,
    ) {
        let me = self.config.id;
        let (audience, reply_to, config) = match proxy {
            Some(proxy) => (
                Audience::Proxy(proxy),
                Party::Proxy(proxy),
                self.config
                    .has_matchmakers()
                    .then(|| self.acceptors.clone()),
            ),
            None => (
                Audience::AcceptorsOf {
                    config: self.acceptors.clone(),
                    column,
                },
                Party::Node(me),
                None,
            ),
        };
        self.pending_messages.push((
            audience,
            Message::Accept {
                reply_to,
                leader: me,
                ballot,
                slot,
                command,
                config,
            },
        ));
    }

    /// If an accept quorum holds for `slot`, the entry is chosen: record it and
    /// `Commit` to the peers.
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "trace", skip_all, fields(node = self.config.id.0, slot = slot.0)))]
    pub(super) fn try_decide(&mut self, slot: Slot) {
        let me = self.config.id;
        // The decision is the proposer's, judged over the active
        // configuration's quorum system; every vote in it came from a
        // configured acceptor (`on_accepted` refuses any other sender, and
        // the component restates it). A delegated round never decides here.
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
            from: Party::Node(me),
            ballot,
            slot,
            command,
        });
        self.proposer.close_round(slot);
    }

    /// Take the delegated round at `slot` back and run it colocated: seed
    /// the tally with this node's own vote (its record was made when the
    /// round opened; re-recording is idempotent), fan the `Accept` out to
    /// the round's column with `reply_to` naming this node, and decide if
    /// that already completes a quorum.
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "trace", skip_all, fields(node = self.config.id.0, slot = slot.0)))]
    pub(super) fn take_back(&mut self, slot: Slot) {
        assert!(
            self.role == NodeRole::Leader,
            "only a leader takes a delegated round back"
        );
        let Some(round) = self.proposer.rounds().get(&slot) else {
            return;
        };
        let (ballot, command, column) = (round.ballot(), round.command().clone(), round.column());
        assert!(
            ballot == self.ballot,
            "a leader's in-flight rounds all run at its own ballot"
        );
        let recorded = self.record_own_round(slot, ballot, &command);
        let own_vote = self.own_vote(recorded, column);
        if !self.proposer.take_back_round(slot, own_vote) {
            return;
        }
        self.send_accept(slot, ballot, command, column, None);
        self.try_decide(slot);
    }
}
