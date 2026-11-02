//! The node's reports (the colocated acceptor, leader and learner): votes, commits, elections, applies, reads, GC and recovery.

use std::collections::BTreeSet;

use moonpool_sim::{TimeProvider, assert_always, assert_reachable};
use paros::core::contract::Party;
use paros::core::contract::limits::{PROMISE_BATCH, RECOVERY_BATCH};
use paros::core::journal::{Command, JournalIdentifier};
use paros::core::membership::{AcceptorConfig, ProxyId, QuorumSystem};
use paros::core::node::{HANDOFF_BATCH, Handoff};
use paros::core::{Ballot, Message, NodeId, Slot};
use paros::observe::audit::{Deployment, LogReadAnswer, LogReadReport};
use paros::roles::node::command_hash;

use super::{NodeAudit, judge_floor, recover_floor};
use crate::audit::{READ_FLOOR_SPAN_MS, journal_model, journals, tenants};

impl<T: TimeProvider> NodeAudit<T> {
    pub(super) fn promised(&self, node: NodeId, ballot: Ballot) {
        self.state().observe_promise(node.0, ballot);
    }

    pub(super) fn accepted(
        &self,
        node: NodeId,
        slot: Slot,
        ballot: Ballot,
        promised: Ballot,
        vhash: u64,
    ) {
        let now = self.now_ms();
        let mut st = self.state();
        // A node never persists an accept above the ballot it has promised.
        assert_always!(
            ballot <= promised,
            "a node's accepted ballot never exceeds its promised ballot"
        );
        // The independent half of the same claim: `promised` above is the
        // driver's own word — cross-check the accept against the promise the
        // audit *folded* from durable reports. Sound because a promise raise
        // in the same batch is surfaced before its accepts, and every earlier
        // raise (or the boot report) already fed the fold.
        let folded = st.promised.get(&node.0).copied();
        assert_always!(
            folded.is_some_and(|p| ballot <= p),
            "a node's accepted ballot never exceeds its last reported promise",
            {
                "node" => node.0,
                "slot" => slot.0,
                "round" => ballot.round,
                "folded_round" => folded.map_or(0, |p| p.round)
            }
        );
        // The durable mirror of the on-the-wire per-ballot proposal check
        // lives in the fold (two different commands under one `(slot,
        // ballot)` would be a ratified double-allocation), together with the
        // acceptor tally behind the quorum-decided oracle.
        st.observe_durable_accept(node.0, slot.0, ballot, vhash);
        // An outage's losses (#263): an unrecoverable slot is never accepted
        // again, and a lost copy rewritten is a recovery.
        st.loss_accepted(node.0, slot.0, vhash);
        // The truncated prefix is genuinely gone: nothing below the durable
        // floor is ever written again.
        let floor = st.floor.get(&node.0).copied().unwrap_or_default();
        assert_always!(
            slot.0 >= floor.strictly_before(now),
            "a node never persists an accept below its compaction floor"
        );
        st.persisted.insert((node.0, slot.0), vhash);
        st.rewritten
            .insert((node.0, slot.0), (ballot.round, ballot.node.0));
        // The commit that carried it is no longer in flight for this slot.
        st.dropped_in_flight.remove(&(node.0, slot.0));
        if st.in_flight.get(&(node.0, slot.0)) == Some(&vhash) {
            st.in_flight.remove(&(node.0, slot.0));
        }
    }

    #[tracing::instrument(level = "trace", skip_all, fields(node = node.0, first = first.0))]
    pub(super) fn floor_requested(&self, node: NodeId, first: Slot) {
        let mut st = self.state();
        let requested = st.requested_floor.entry(node.0).or_insert(0);
        *requested = (*requested).max(first.0);
    }

    /// Tell the checker what the commit may land (see
    /// [`AuditWorld::note_in_flight`]): a crash anywhere from here to the
    /// driver's report leaves these accepts durable or not, unreported. An
    /// accept the batch's own floor drops (the core staged a slot and a
    /// truncation past it in one flush) is never written, yet the landed
    /// metainfo makes it count: the floor and chosen index it carries say
    /// the slot was decided, and on a quorum of one that accept was the
    /// decision. Reported by the driver, so a machine's journals, which
    /// run on a bare disk, are judged alike (#454 (control plane meets
    /// chaos): witness 7140828428648307805, a machine killed inside a
    /// commit that overwrote two round-4 accepts at round 5).
    pub(super) fn commit_started(&self, node: NodeId, writes: &[paros::core::WriteOp]) {
        let floor = writes
            .iter()
            .filter_map(|op| match op {
                paros::core::WriteOp::Truncate { first, .. } => Some(*first),
                paros::core::WriteOp::TrimmedTo { point, .. } => Some(*point),
                _ => None,
            })
            .max();
        let mut accepted: Vec<(u64, u64)> = Vec::new();
        let mut dropped: Vec<(u64, Ballot, u64)> = Vec::new();
        for op in writes {
            let (slot, ballot, command) = match op {
                paros::core::WriteOp::Acceptor(
                    paros::core::contract::AcceptorWrite::AppendAccepted {
                        slot,
                        ballot,
                        value,
                    },
                ) => (slot, ballot, value),
                paros::core::WriteOp::Learned {
                    slot,
                    ballot,
                    command,
                } => (slot, ballot, command),
                _ => continue,
            };
            let vhash = paros::roles::node::command_hash(command);
            if floor.is_some_and(|floor| *slot < floor) {
                dropped.push((slot.0, *ballot, vhash));
            } else {
                accepted.push((slot.0, vhash));
            }
        }
        if !dropped.is_empty() {
            self.world.note_dropped_in_flight(node.0, &dropped);
        }
        if !accepted.is_empty() {
            self.world.note_in_flight(node.0, &accepted);
        }
    }

    #[tracing::instrument(level = "trace", skip_all, fields(node = node.0, first = first.0))]
    pub(super) fn truncated(&self, node: NodeId, first: Slot) {
        let now = self.now_ms();
        let mut st = self.state();
        // The core stages `WriteOp::Truncate` only when it raises its floor,
        // batches flush in order, and a report only ever follows a successful
        // fsync — so the *truncated reports themselves* are monotone per
        // node, within and across incarnations. Judged against their own
        // watermark, never the folded floor: a same-batch trim-point jump
        // raises the folded floor higher than a truncate in the batch, and
        // the ground-truth feed likewise forwards raw requests the storage
        // contract treats as no-ops. Equality is an idempotent re-raise.
        let was = st.truncate_watermark.get(&node.0).copied().unwrap_or(0);
        assert_always!(
            first.0 >= was,
            "a compaction floor never regresses",
            { "node" => node.0, "was" => was, "reported" => first.0 }
        );
        st.truncate_watermark.insert(node.0, first.0.max(was));
        // A floor moves only inside the node's walked chosen prefix (#448):
        // the walk staged its `SetChosenIndex` writes ahead of the
        // `Truncate`, and a trim-point landing raised the prefix with the
        // jump, so the floor never passes the slots the node knows decided.
        let decided = st.decided_prefix.get(&node.0).copied().unwrap_or(0);
        assert_always!(
            first.0 <= decided,
            "a compaction floor never passes the quorum-decided frontier",
            { "node" => node.0, "first" => first.0, "decided_prefix" => decided }
        );
        // What the floor drops is judged once the node reported the walk
        // that decided it (the batch reports its floor first): see
        // `judge_floor`.
        st.floor_claims.insert(node.0, (first.0, decided));
        st.floor.entry(node.0).or_default().raise(first.0, now);
        if first.0 > 0 {
            reach_once!(
                st.compacted,
                "a node truncates its log prefix behind the chosen index"
            );
        }
        // Below the *cluster-wide* minimum floor every node has truncated, so
        // the per-slot safety tallies can never be consulted again: reclaim
        // them, keeping the decided vhash per pruned slot as the witness a
        // late `Commit` there is judged against.
        st.prune_below_floor();
    }

    #[tracing::instrument(level = "trace", skip_all, fields(node = node.0, point = point.0))]
    pub(super) fn trimmed_to(&self, node: NodeId, point: Slot) {
        let now = self.now_ms();
        let mut st = self.state();
        st.trim_jumped = true;
        if st.replicas.contains(&node.0) {
            st.replica_jumped = true;
        }
        let landing = point.0.saturating_sub(1);
        // A trim point is a floor some peer holds, and a floor only ever
        // moves inside a walked chosen prefix — so the landing sits inside
        // the cluster's applied frontier, or it names slots nobody chose.
        // The message keeps its pre-#186 wording — the same claim the
        // snapshot install made, and an assertion's slot is its hash.
        assert_always!(
            st.cluster_applied_max.is_some_and(|max| landing <= max),
            "an installed snapshot lands within the cluster's applied frontier",
            {
                "node" => node.0,
                "landing" => landing,
                "cluster_max" => st.cluster_applied_max.unwrap_or(0)
            }
        );
        // The jump raises the durable floor and the chosen index with it:
        // keep the per-incarnation watermarks in step so a later
        // `SetChosenIndex` or `Truncate` report is judged against them.
        let watermark = st.chosen_watermark.entry(node.0).or_insert(0);
        *watermark = (*watermark).max(landing);
        let prefix = st.decided_prefix.entry(node.0).or_insert(0);
        *prefix = (*prefix).max(landing + 1);
        let was = st.truncate_watermark.get(&node.0).copied().unwrap_or(0);
        st.truncate_watermark.insert(node.0, point.0.max(was));
        st.floor.entry(node.0).or_default().raise(point.0, now);
        // The jump moves the walked prefix straight to the landing without
        // walking the slots below it: an admitted forward jump.
        st.landings.entry(node.0).or_default().insert(landing);
        st.observe_applied_index(node.0, landing);
        // The jump may raise the cluster's minimum floor like a truncation
        // does: prune below it, so a late `Commit` there is judged against
        // the pruned slot's decided vhash (witness 15198787338826444353 of
        // PR #551 (operator calls name their journal) at a65d9cc).
        st.prune_below_floor();
    }

    pub(super) fn applied(
        &self,
        node: NodeId,
        slot: Slot,
        vhash: u64,
        command: &Command,
        outcome: Option<&paros::core::journal::Outcome>,
    ) {
        let mut st = self.state();
        if st.replicas.contains(&node.0) {
            st.applied_on_replica = true;
        }
        // The crown jewel: at most one value is ever chosen per slot, cluster-wide.
        if let Some(prev) = st.chosen.insert(slot.0, vhash) {
            assert_always!(prev == vhash, "at most one value is ever chosen for a slot");
        }
        st.chosen_values.insert(vhash);
        // The journal state machine's oracles (#204): every node's verdict
        // at this slot is the same, and the verdicts obey section 6.
        st.journal.applied(node.0, slot.0, command, vhash, outcome);
        judge_floor(&mut st, node.0, slot.0);
        // The quorum-decided oracle's apply leg: a slot applied where the
        // durable-accept tally already decided it must apply the decided
        // value (no apply substitutes a command since #204: the #94
        // suppression that did is gone with the session ledger).
        if let Some(decided_vhash) = st.decided_vhash(slot.0) {
            assert_always!(
                vhash == decided_vhash,
                "an applied value matches the decided value",
                { "node" => node.0, "slot" => slot.0 }
            );
        }
        // Persist-before-send, observed at the apply seam: a slot is applied
        // only once chosen, chosen only on a quorum of `Accepted`s, and each
        // of those left its node after the audit folded the durable accept —
        // so by the time any node applies a slot, the tally has decided it.
        // The end-of-run `decided >= applied` leg is this, per slot. A slot
        // the tally pruned below the acceptors' floor stays decided through
        // its witness: a replica (#144) may still apply it — it was down
        // while the acceptors truncated, and it holds its own log below
        // their floor — and its decision is exactly what the witness keeps
        // (seeds 14697535725710265276, 12166376049160003182 and
        // 2038247294279376366 on the first replica-tier hunt).
        assert_always!(
            st.decided_vhash(slot.0).is_some(),
            "an applied slot was decided by a durable accept quorum before any node applied it",
            { "node" => node.0, "slot" => slot.0 }
        );
        if command.write().is_some() && st.foreign.contains(&vhash) {
            assert_always!(
                !matches!(
                    outcome,
                    Some(paros::core::journal::Outcome::Accepted { .. } | paros::core::journal::Outcome::Duplicate { .. })
                ),
                "journal: a write under another tenant's fence is refused at apply",
                { "node" => node.0, "slot" => slot.0 }
            );
        }
        st.any_chosen = true;
        st.observe_applied_index(node.0, slot.0);
        drop(st);
        // #450: a call a frontend had to deny never reaches a journal.
        if let Some(board) = &self.tenants {
            tenants::lock(board).applied(node, command);
        }
        let takes_slot = !matches!(
            command,
            Command::Control(paros::core::journal::Control::Noop)
        );
        self.journal_applied(node, takes_slot.then_some(vhash));
    }

    pub(super) fn sent(&self, node: NodeId, to: NodeId, msg: &Message) {
        if let Some((journal, board)) = &self.journal {
            let mut board = journals::lock(board);
            assert_always!(
                !board.is_quarantined(node.0, *journal),
                "journal: a quarantined journal sends nothing",
                { "node" => node.0, "journal" => journal.to_string() }
            );
            board.sent(node.0, *journal);
        }
        self.count_sent(msg);
        if let Message::Prepare { ballot, config, .. } = msg {
            self.check_prepare_licence(node, to, *ballot, config.as_ref());
            self.state().observe_prepare_send(node.0, to.0, *ballot);
        }
        if let Message::Relinquish {
            from,
            ballot,
            next_slot,
            ..
        } = msg
        {
            self.state()
                .observe_authority_release(from.0, *ballot, *next_slot);
            return;
        }
        // #95: every broadcast leader beat feeds the zombie-leader streak.
        if let Message::Heartbeat { ballot, .. } = msg {
            self.state().observe_beat(node.0, *ballot);
            return;
        }
        if let Message::Promise {
            ballot, accepted, ..
        } = msg
        {
            assert_always!(
                accepted.len() <= PROMISE_BATCH,
                "a Promise carries at most one bounded suffix chunk",
                { "entries" => accepted.len() }
            );
            // Persist-before-send at the promise seam: the batch that raised
            // the promise flushed and reported it before this send, so a
            // Promise above the folded durable promise left before its fsync.
            let st = self.state();
            let folded = st.promised.get(&node.0).copied();
            assert_always!(
                folded.is_some_and(|p| p >= *ballot),
                "a sent Promise carries a durably promised ballot",
                {
                    "node" => node.0,
                    "round" => ballot.round,
                    "folded_round" => folded.map_or(0, |p| p.round)
                }
            );
        }
        if let Message::Promise { ballot, .. } = msg {
            self.state().observe_promise_send(node.0, *ballot);
        }
        // Persist-before-send at the accept seam: an `Accepted` claims "I hold
        // this durably", so the matching record must already be in this
        // node's folded durable-accept tally (the same-batch write is flushed
        // and reported before the send; a re-answer names an older record
        // that was folded when it was first written or re-read at boot). A
        // `Commit` names a decided slot and carries the decided value.
        self.observe_durable_send(Party::Node(node), msg);
        // The Phase-2 half of P2b, checked *on the wire*, and the two claims
        // around it: *who* may propose under this ballot (authority
        // uniqueness — checked first, since a violation of it explains a
        // violation of the rest), *whom* it addresses and *on what Phase-1
        // licence* (#121, #122), then *what* was proposed.
        if let Message::Accept {
            ballot,
            slot,
            command,
            ..
        } = msg
        {
            let vhash = command_hash(command);
            let mut st = self.state();
            st.observe_authority_use(node.0, *ballot);
            st.observe_accept_send(Party::Node(node), to.0, *ballot);
            st.observe_proposal(Party::Node(node), *ballot, slot.0, vhash);
        }
    }

    pub(super) fn sent_to_proxy(&self, node: NodeId, _proxy: ProxyId, msg: &Message) {
        self.count_sent(msg);
        // Persist-before-send at the accept seam, whoever the vote goes to:
        // an acceptor's `Accepted` to a proxy claims "I hold this durably"
        // exactly as one to a leader does, so the check `sent` runs is run
        // here on the same message (a node never sends a `Commit` to a
        // proxy; the call is the shared seam). Routing Phase 2 through a
        // proxy removes no check — the proxy's later quorum check judges
        // the decision, not the order of each vote and its fsync.
        self.observe_durable_send(Party::Node(node), msg);
        // A delegation is the leader exercising its Phase-2 authority for
        // the slot — the same two claims a colocated `Accept` makes about
        // *who* and *what*; *whom* it addresses is a proxy, which is judged
        // at the proxy's own fan-out.
        if let Message::Accept {
            ballot,
            slot,
            command,
            ..
        } = msg
        {
            let vhash = command_hash(command);
            let mut st = self.state();
            st.observe_authority_use(node.0, *ballot);
            st.observe_proposal(Party::Node(node), *ballot, slot.0, vhash);
        }
    }

    #[tracing::instrument(level = "trace", skip_all, fields(node = node.0, round = won.round))]
    pub(super) fn elected(
        &self,
        node: NodeId,
        won: Ballot,
        promised: Ballot,
        _gap_fills: u64,
        config: &AcceptorConfig,
    ) {
        if let Some(mut board) = self.system_board() {
            let members: Vec<u64> = config.members().iter().map(|m| m.0).collect();
            board.elected_under(&members);
        }
        let now = self.now_ms();
        self.elected_in_chaos();
        let mut st = self.state();
        // Matchmaking invariants 4 and 5 (#120): a leadership on a matchmaker
        // deployment stands on a campaign that closed with a quorum and was
        // never refused, and runs Phase 2 under exactly the configuration
        // some matchmaker durably registered for the ballot. On plain
        // Multi-Paxos the configuration is the bootstrap membership, always.
        if st.matchmaker.has_matchmakers() {
            assert_always!(
                st.matchmaker.phase1_licensed(node.0, won),
                "matchmaking: a refused or unregistered ballot never becomes a leadership",
                { "node" => node.0, "round" => won.round }
            );
            let registered = st.matchmaker.registered_config(won);
            assert_always!(
                registered == Some(config),
                "matchmaking: a leader runs Phase 2 under the configuration registered for its ballot",
                { "node" => node.0, "round" => won.round }
            );
            st.matchmaker.leadership_won(won, config);
        } else {
            assert_always!(
                st.bootstrap.as_ref() == Some(config),
                "plain: a leader on a deployment without matchmakers keeps the bootstrap configuration",
                { "node" => node.0, "round" => won.round }
            );
        }
        // A grid tolerates no member lost for good: each slot is decided by
        // its own column, a column with a dead member never decides again,
        // and the leader's recovery — hence every later reconfiguration —
        // waits on it forever. The harness keeps every identity it gave up
        // for good out of a grid in force (the copy budget parks nobody on
        // a grid seed, the composer asks every column to stay live, the
        // operators' ledger withholds a retirement a registered
        // reconfiguration still names); a leadership under a grid naming a
        // retired identity is one of those promises broken (#198).
        if matches!(config.quorum_system(), QuorumSystem::Grid { .. }) {
            let retired = config
                .members()
                .iter()
                .find(|member| st.retired.contains(&member.0))
                .map(|member| member.0);
            assert_always!(
                retired.is_none(),
                "gc: a leader never runs a grid configuration naming a retired identity",
                {
                    "node" => node.0,
                    "round" => won.round,
                    "retired" => retired.unwrap_or(u64::MAX)
                }
            );
        }
        st.bind_config(won, config);
        if st.bootstrap.as_ref().is_some_and(|b| b != config) {
            st.reconfiguration_completed = true;
        }
        if st.bootstrap.as_ref().is_some_and(|b| {
            b.members()
                .iter()
                .any(|member| !config.members().contains(member))
        }) {
            st.departure_effective = true;
        }
        // The #140 outcome: a leadership genuinely ran under a flexible
        // split (the draw is a `reachable` in `shape::quorum_policy`).
        if matches!(config.quorum_system(), QuorumSystem::Flexible { .. }) {
            st.elected_flexible = true;
        }
        // The #141 outcomes: a leadership ran under a grid (its Phase 1 was
        // covered by a full row); its matchmaking closed with a *different*
        // grid among the prior configurations (Phase 1 needed a row of that
        // one too — a row across a reconfiguration); and the configuration
        // moved between a grid and a majority (a prior of the other kind).
        let is_grid = |c: &AcceptorConfig| matches!(c.quorum_system(), QuorumSystem::Grid { .. });
        if is_grid(config) {
            st.elected_grid = true;
        }
        let prior: Vec<AcceptorConfig> = st.prior_of(won).map(<[_]>::to_vec).unwrap_or_default();
        if prior.iter().any(|c| c != config && is_grid(c)) {
            st.elected_across_grid = true;
        }
        if prior
            .iter()
            .any(|c| c != config && is_grid(c) != is_grid(config))
        {
            st.reconfigured_across_grid_boundary = true;
        }
        if let Some(prev) = st.leader_round.insert(node.0, won.round) {
            assert_always!(
                won.round > prev,
                "a node's leadership ballots strictly increase"
            );
        }
        // Placed at the *instant of victory*: winning means having promised your
        // own campaign ballot and heard nothing higher, so this is an identity
        // there. A tick later the same state is indistinguishable from a sitting
        // leader legitimately learning a higher-ballot commit.
        assert_always!(
            won >= promised,
            "a fresh leader has not promised a ballot above the one it won"
        );
        st.leader_promise_checked = true;
        st.any_leader = true;
        st.leader_rounds.insert(won.round);
        match st.first_leader_round {
            None => st.first_leader_round = Some(won.round),
            Some(r) if r != won.round && st.leader_change_ms.is_none() => {
                st.leader_change_ms = Some(now);
            }
            Some(_) => {}
        }
    }

    pub(super) fn stepped_down(&self, _node: NodeId) {
        // The fired gate is inline at the driver's site; the failover
        // oracle folds the fact.
        self.state().resigned = true;
    }

    #[tracing::instrument(level = "trace", skip_all, fields(node = node.0))]
    pub(super) fn authority_relinquished(&self, node: NodeId, handoff: Handoff) {
        let mut st = self.state();
        // Shape and coverage only. The *bookkeeping* — who holds the authority
        // now — is folded from the `Relinquish` on the wire (see
        // [`NodeAudit::sent`]), because that is the instant with the right
        // causal order: it lands after every message the abdicating batch had
        // already queued, and before any successor can possibly install.
        assert_always!(
            u64::try_from(handoff.decided + handoff.pending).unwrap_or(u64::MAX)
                == handoff.next_slot.0.saturating_sub(handoff.from_slot.0),
            "a relinquished tail exactly tiles the transferred range",
            {
                "node" => node.0,
                "decided" => handoff.decided,
                "pending" => handoff.pending
            }
        );
        assert_always!(
            handoff.decided + handoff.pending <= HANDOFF_BATCH,
            "a relinquished tail stays within one bounded page",
            { "node" => node.0, "slots" => handoff.decided + handoff.pending }
        );
        assert_always!(
            handoff.to != node,
            "an authority is handed to another node, never to its own holder",
            { "node" => node.0 }
        );
        let key = (handoff.ballot.round, handoff.ballot.node.0);
        // The `DPaxos` "at most once" rule, checked on the decision itself: the
        // core demotes in the very call that decides, so it can never decide to
        // relinquish one authority twice.
        assert_always!(
            st.relinquish_calls.insert((node.0, key)),
            "an authority is relinquished at most once by a node",
            { "node" => node.0, "round" => handoff.ballot.round }
        );
        // One hop only: the node that mints a ballot by winning Phase 1 at it is
        // the only one that may hand it on (see `ColocatedNode::can_relinquish`).
        // Without that rule a replayed payload can re-install an authority at a
        // node that already gave it up while its own successor is still
        // exercising it — the hole this sweep found.
        assert_always!(
            handoff.ballot.node == node,
            "only a ballot's own minter relinquishes it",
            { "node" => node.0, "bnode" => handoff.ballot.node.0 }
        );
        reach_once!(
            st.handoff_relinquished,
            "a leader cooperatively relinquishes its authority"
        );
        if handoff.pending > 0 {
            reach_once!(
                st.handoff_carried_tail,
                "a handoff carries accepted-but-unchosen work across"
            );
        }
    }

    #[tracing::instrument(level = "trace", skip_all)]
    pub(super) fn authority_installed(
        &self,
        node: NodeId,
        from: NodeId,
        ballot: Ballot,
        next_slot: Slot,
        _tail: u64,
    ) {
        let mut st = self.state();
        assert_always!(
            from != node,
            "an installed authority came from another node",
            { "node" => node.0 }
        );
        let key = (ballot.round, ballot.node.0);
        let entry = st.authorities.entry(key).or_default();
        assert_always!(
            !entry.retired.contains(&node.0),
            "a node never re-installs an authority it relinquished",
            { "node" => node.0, "round" => ballot.round }
        );
        assert_always!(
            entry.holder.is_none_or(|held| held == node.0),
            "at most one node installs a relinquished authority",
            { "node" => node.0, "holder" => entry.holder.unwrap_or(u64::MAX) }
        );
        assert_always!(
            next_slot.0 >= entry.frontier,
            "an inherited allocator frontier never rewinds",
            {
                "node" => node.0,
                "frontier" => next_slot.0,
                "previous" => entry.frontier
            }
        );
        entry.frontier = next_slot.0;
        entry.holder = Some(node.0);
        st.handoff_installs = st.handoff_installs.saturating_add(1);
        reach_once!(
            st.handoff_installed,
            "a node installs a predecessor's transferred authority"
        );
        if st.handoff_installs >= 2 {
            reach_once!(
                st.handoff_repeated,
                "leadership is handed over more than once in a run"
            );
        }
        st.any_leader = true;
    }

    pub(super) fn handoff_refused(
        &self,
        _node: NodeId,
        target: u64,
        stale: u64,
        shape: u64,
        unfit: u64,
    ) {
        let mut st = self.state();
        if target > 0 {
            reach_once!(
                st.handoff_refused_target,
                "a handoff addressed elsewhere is refused"
            );
        }
        if stale > 0 {
            reach_once!(
                st.handoff_refused_stale,
                "a stale or superseded handoff is refused"
            );
        }
        if shape > 0 {
            reach_once!(
                st.handoff_refused_shape,
                "a malformed handoff tail is refused"
            );
        }
        if unfit > 0 {
            reach_once!(
                st.handoff_refused_unfit,
                "a handoff onto a node needing Phase-1 repair is refused"
            );
        }
    }

    pub(super) fn handoff_fence_expired(&self, _node: NodeId, _count: u64) {
        let mut st = self.state();
        reach_once!(
            st.handoff_fence_expired,
            "an uncovered inherited fence resigns back to an ordinary election"
        );
    }

    pub(super) fn chosen_gap(&self, node: NodeId, hole: Slot, above: Slot) {
        // A gap is perfectly ordinary — pipelining leaves several slots
        // undecided, and a follower that missed one `Commit` holds one until
        // catch-up runs — so nothing is asserted here. A gap that never heals
        // shows up where every liveness failure does: the end-of-run
        // convergence claim, and this record says where the node was stuck.
        self.state().last_gap.insert(node.0, (hole.0, above.0));
    }

    pub(super) fn answered(
        &self,
        node: NodeId,
        slot: Slot,
        command: &Command,
        outcome: &paros::core::journal::Outcome,
    ) {
        // #189, #210: a node acknowledges no write to a journal after
        // folding its tombstone; nor any other call (#449).
        if let (Some(board), Some((journal, _))) = (&self.tenants, &self.journal) {
            if command.write().is_some() {
                tenants::lock(board).acked(node, *journal);
            } else {
                tenants::lock(board).served(node, *journal);
            }
        }
        // #449: a quarantined journal answers no call, as it sends nothing.
        if let Some((journal, board)) = &self.journal {
            assert_always!(
                !journals::lock(board).is_quarantined(node.0, *journal),
                "journal: a quarantined journal answers no call",
                { "node" => node.0, "journal" => journal.to_string() }
            );
        }
        let mut st = self.state();
        st.any_ack_checked = true;
        st.answered_max = st.answered_max.max(Some(slot.0));
        // The verdict answered is the one every node's fold reached there.
        st.journal.answered(node.0, slot.0, outcome);
        // A verdict is answered only from the node's own fold: the slot is
        // inside what it applied. The message predates #204, when it named
        // the ack of a committed write; the property is the same.
        assert_always!(
            st.frontier.get(&node.0).is_some_and(|next| *next > slot.0),
            "a committed write ack names a slot the acking node had already applied",
            { "node" => node.0, "slot" => slot.0 }
        );
        // Decision 1 of #144: the node asked acks, and the slot's reply
        // owner — a replica, a different process by construction — is noted
        // so the gate can prove it applied what it would have answered.
        if let Some(owner) = paros::core::membership::ReplicaId::of(slot, st.replica_count) {
            let owner = crate::roles::replica_node_id(owner).0;
            if owner != node.0 {
                let first = st.acked_by_other.entry(owner).or_insert(slot.0);
                *first = (*first).min(slot.0);
            }
        }
        match outcome {
            paros::core::journal::Outcome::Accepted { .. }
            | paros::core::journal::Outcome::Duplicate { .. } => {
                if st.leader_change_ms.is_some() {
                    st.ack_after_leader_change = true;
                }
                // The retry edge the reply-drop location exists for: a
                // write's verdict was dropped, and a retry was answered from
                // the log.
                if matches!(outcome, paros::core::journal::Outcome::Duplicate { .. })
                    && st.propose_reply_dropped
                {
                    st.dedup_after_dropped_reply = true;
                }
            }
            // Feeds the "chain: compact takes effect" outcome gate.
            paros::core::journal::Outcome::Trimmed(_) => st.compact_ack_accepted = true,
            _ => {}
        }
    }

    pub(super) fn chosen_index(&self, node: NodeId, index: Slot) {
        let mut st = self.state();
        // Within one incarnation the core's chosen index only ever advances
        // (the ordering-chain invariant), and its durable reports arrive in
        // batch order — so a regression here is a driver/storage reordering
        // bug. Across a restart the scalar is flushed *relaxed*, so a crash
        // may legally rewind it (the boot recomputes it from what the disk
        // actually holds); `recovered` therefore resets this watermark to the
        // recovered index instead of asserting continuity across boots.
        let watermark = st.chosen_watermark.entry(node.0).or_insert(0);
        assert_always!(
            index.0 >= *watermark,
            "a chosen index never regresses within a boot",
            { "node" => node.0, "index" => index.0, "watermark" => *watermark }
        );
        *watermark = (*watermark).max(index.0);
        let prefix = st.decided_prefix.entry(node.0).or_insert(0);
        *prefix = (*prefix).max(index.0 + 1);
    }

    pub(super) fn log_read_served(&self, node: NodeId, report: &LogReadReport<'_>) {
        let mut st = self.state();
        let from = report.from.0;
        let next = from + report.records.len() as u64;
        // A page is served from the serving process's journal fold and never
        // past it (the message predates positions: the fold's head is the
        // serving prefix).
        assert_always!(
            report.truncated || report.records.is_empty() || next <= report.state.next_seq.0,
            "journal read: a page never passes the serving prefix",
            {
                "node" => node.0,
                "from" => from,
                "next" => next,
                "next_seq" => report.state.next_seq.0
            }
        );
        // The state it was served from is one the nodes' verdicts reached.
        assert_always!(
            report.state.next_seq.0 <= st.journal.next_seq(),
            "journal read: a served state never passes the applied journal",
            { "node" => node.0, "next_seq" => report.state.next_seq.0 }
        );
        // A node or a replica serves only the fold it reported (#550
        // (replica serves an unreported fold page)): the batch that holds
        // the last position of the served state is one its walk reported,
        // or one its boot or a trim-point jump landed past. Released early,
        // the next apply page folds before its writes and its report; a
        // server behind the cell then serves slots the others reported but
        // it did not, which the journal-wide check above cannot see.
        if let Some(last) = report.state.next_seq.0.checked_sub(1)
            && let Some(slot) = st.journal.slot_holding(last)
        {
            let reported = st.applied_max.get(&node.0).copied();
            assert_always!(
                reported.is_some_and(|max| slot <= max),
                "journal read: a server serves only the fold it reported",
                {
                    "node" => node.0,
                    "slot" => slot,
                    "reported" => reported.map_or(-1, |max| i64::try_from(max).unwrap_or(i64::MAX))
                }
            );
        }
        // Every record is the one accepted at its position.
        for (position, record) in (from..).zip(report.records) {
            if let Some(known) = st.journal.record_at(position) {
                assert_always!(
                    known == journal_model::record_hash(&record.0),
                    "journal read: a record is the one accepted at its position",
                    { "node" => node.0, "position" => position }
                );
            }
        }
        if report.truncated {
            // Truncated only below `first_seq` (the message predates
            // positions: `first_seq` is the trim point).
            assert_always!(
                from < report.state.first_seq.0,
                "journal read: a trim point refuses only reads below it",
                { "node" => node.0, "from" => from, "first_seq" => report.state.first_seq.0 }
            );
            st.journal_read_trimmed = true;
        }
        st.journal_read_woke |= report.answer == LogReadAnswer::Woke && !report.records.is_empty();
        st.journal_read_on_replica |= st.replicas.contains(&node.0);
    }

    pub(super) fn log_read_not_held(&self, node: NodeId, from: paros::core::journal::Seq) {
        let st = self.state();
        // `LogRead::NotHeld` (#448): the record exists in the journal, but
        // in a slot below this node's floor, which rose ahead of its fold
        // (a trim-point jump, or a reboot's refold stopped at a hole). A
        // position the node's floor does not cover is one it should hold.
        if let Some(slot) = st.journal.slot_holding(from.0) {
            let floor = st.floor.get(&node.0).map_or(0, |f| f.now);
            assert_always!(
                slot < floor,
                "journal read: a read is answered not held only below the node's floor",
                { "node" => node.0, "from" => from.0, "slot" => slot, "floor" => floor }
            );
        }
    }

    #[allow(clippy::unused_self, reason = "the `Audit` hook's signature")]
    pub(super) fn journal_refused(
        &self,
        _node: NodeId,
        _journal: JournalIdentifier,
        _call: &'static str,
    ) {
        assert_reachable!("journal: a call naming an unserved journal is refused");
    }

    #[tracing::instrument(level = "trace", skip_all, fields(node = node.0, journal = %journal, seq))]
    pub(super) fn system_folded(
        &self,
        node: NodeId,
        journal: JournalIdentifier,
        seq: u64,
        event: &paros::model::registry::SystemEvent,
    ) {
        match event {
            paros::model::registry::SystemEvent::Registry(event) => {
                if let paros::model::registry::RegistryEvent::TenantDropped { tenant } = event
                    && let Some(board) = &self.tenants
                {
                    tenants::lock(board).tenant_dropped(*tenant);
                }
                if let Some((mut board, plane)) = self.registry_board() {
                    board.folded(node, journal, seq, event, plane);
                }
                if let Some(state) = &self.wipes {
                    crate::machine::registry_folded(state, journal, seq, event);
                }
            }
            paros::model::registry::SystemEvent::Tenant(event) => {
                if let Some(board) = &self.tenants {
                    tenants::lock(board).folded(node, journal, seq, event);
                }
            }
        }
    }

    pub(super) fn checkpoint_folded(
        &self,
        node: NodeId,
        journal: JournalIdentifier,
        seq: u64,
        verified: Option<bool>,
        state: &paros::model::registry::Registry,
    ) {
        // A machine that restored its fold from this checkpoint learns the
        // tenants the cell dropped from it, never from a `TenantDropped`
        // entry below it, and stops their journals (`rehost`). The board
        // learns them the same way, so a dropped tenant's journals owe
        // nothing: witness seed 17310756831146654209 on 3ab2df7, where every
        // follower's read jumped over the drop to the checkpoint's run, two
        // members stopped the tenant's control journal and the third, its
        // store re-opened after a quarantine, never decided a slot.
        if let Some(board) = &self.tenants {
            let mut board = tenants::lock(board);
            for tenant in state.dropped_tenants() {
                board.tenant_dropped(tenant);
            }
        }
        if let Some((mut board, plane)) = self.registry_board() {
            board.checkpoint_folded(node, journal, (seq, verified), state, plane);
        }
        if let Some(run) = &self.wipes {
            crate::machine::registry_restored(run, journal, seq, verified, state);
        }
    }

    pub(super) fn journal_held(&self, _node: NodeId) {
        if let Some((journal, board)) = &self.journal {
            journals::lock(board).held(*journal);
        }
    }

    pub(super) fn journal_started(&self, node: NodeId, journal: JournalIdentifier) {
        if let Some((mut board, plane)) = self.registry_board() {
            board.started(node, plane);
        }
        // #449: a machine starts only a genesis journal or one a tenant
        // created.
        if let Some(board) = &self.tenants {
            tenants::lock(board).started(node, journal);
        }
    }

    pub(super) fn journal_stopped(&self, node: NodeId, journal: JournalIdentifier) {
        assert_reachable!(
            "system: a node stops a journal the directory tombstoned or its retirement ended"
        );
        // A joiner the registry retired leaves the deployment's journal for
        // good (#189): convergence excuses it, exactly like an operator
        // retirement (#123).
        if self.world.main() == Some(journal) {
            self.world.note_left_pool(node.0);
        }
        // A machine reports every stop through its node port, whose world
        // is its home journal's (#447 (model checks for every journal
        // world)): the tenant board keeps the stop for the stopped
        // journal's convergence.
        if let Some(board) = &self.tenants {
            tenants::lock(board).stopped(node, journal);
        }
    }

    pub(super) fn unpooled_message(&self, node: NodeId, _journal: JournalIdentifier, from: NodeId) {
        if let Some(mut board) = self.system_board() {
            board.refused(node, from);
        }
    }

    pub(super) fn pool_admitted(&self, node: NodeId, admitted: NodeId) {
        if let Some(mut board) = self.system_board() {
            board.admitted(node, admitted);
        }
    }

    pub(super) fn quorum_read_opened(&self, node: NodeId, ctx: u64) {
        let now = self.now_ms();
        let mut st = self.state();
        // A token is drawn at random per runtime (`JournalReads`), so it is
        // unique across incarnations too: a dead incarnation's late ack can
        // never confirm a new read. An entry older than `READ_FLOOR_SPAN_MS`
        // is a read long past its TTL that was never served; drop it, keeping
        // the map to the reads that can still be (a dropped entry only skips
        // a check).
        let stale: Vec<(u64, u64)> = st
            .read_floors
            .range((node.0, 0)..=(node.0, u64::MAX))
            .filter(|(_, (_, at))| at.saturating_add(READ_FLOOR_SPAN_MS) < now)
            .map(|(key, _)| *key)
            .collect();
        for key in stale {
            st.read_floors.remove(&key);
        }
        assert_always!(
            !st.read_floors.contains_key(&(node.0, ctx)),
            "quorum read: a node never reuses a read token",
            { "node" => node.0 }
        );
        let floor = st.chosen.keys().next_back().copied();
        st.read_floors.insert((node.0, ctx), (floor, now));
    }

    #[allow(clippy::too_many_arguments, reason = "the `Audit` hook's signature")]
    pub(super) fn quorum_read_served(
        &self,
        node: NodeId,
        ctx: u64,
        row: Option<usize>,
        watermark: Option<Slot>,
        served: Option<Slot>,
        opened: Option<Slot>,
        leader: bool,
    ) {
        let now = self.now_ms();
        let mut st = self.state();
        // The read half of linearizability where the fact arrives (#260):
        // every slot chosen — applied anywhere — before the read opened is in
        // the prefix it serves. Stronger than the client-history search,
        // which needs an acknowledged write before the read: any chosen slot
        // counts here, a no-op or a refused write included.
        if let Some((floor, _)) = st.read_floors.remove(&(node.0, ctx)) {
            assert_always!(
                served.map(|s| s.0) >= floor,
                "quorum read: a read serves every slot chosen before it opened",
                {
                    "node" => node.0,
                    "served" => crate::signed_watermark(served.map(|s| s.0)),
                    "floor" => crate::signed_watermark(floor),
                    "watermark" => crate::signed_watermark(watermark.map(|s| s.0))
                }
            );
        }
        // The replica half of §3.4: a node answers only once its own chosen
        // prefix covers the maximum watermark its row reported — the step
        // that makes every write acked before the read visible to it.
        assert_always!(
            served >= watermark,
            "quorum read: a node serves only once its prefix covers the row's watermark",
            {
                "node" => node.0,
                "served" => crate::signed_watermark(served.map(|s| s.0)),
                "watermark" => crate::signed_watermark(watermark.map(|s| s.0))
            }
        );
        // Both read paths answer from one node's chosen prefix, so the
        // per-boot frontier is one fold over both.
        st.check_read_frontier(node, served);
        st.quorum_read_by_follower |= !leader;
        // The read a local answer would have got wrong: the row knew of a
        // vote past what this node had chosen when the read opened.
        st.quorum_read_past_opened |= watermark > opened;
        st.quorum_read_after_leader_change |= st.leader_change_ms.is_some_and(|t| now > t);
        st.quorum_read_on_row |= row.is_some();
        // §3.4's own shape (#144): the read answered from a replica's state.
        st.quorum_read_on_replica |= st.replicas.contains(&node.0);
        // The same on a matchmaker deployment, where the configuration a
        // read is bound to moves with every registration: a replica must
        // follow it from the beats to have its reads confirmed at all.
        st.quorum_read_on_replica_with_matchmakers |=
            st.replicas.contains(&node.0) && st.matchmaker.has_matchmakers();
    }

    // One boot report, judged leg by leg in order (#496 added the floor).
    #[allow(clippy::too_many_lines)]
    #[tracing::instrument(level = "trace", skip_all)]
    pub(super) fn recovered(
        &self,
        node: NodeId,
        promised: Ballot,
        chosen_index: Option<Slot>,
        floor: Slot,
        deployment: &Deployment,
        accepted: &[(Slot, Ballot, u64)],
    ) {
        let now = self.now_ms();
        if let Some((journal, board)) = &self.journal {
            journals::lock(board).reopened(node.0, *journal);
        }
        let mut st = self.state();
        st.restarted_any |= !st.booted.insert(node.0); // a second boot: it crashed
        // One shared deployment per run: every node's durable configuration
        // names the same bootstrap membership, pool and matchmaker set.
        let bootstrap = st
            .bootstrap
            .get_or_insert_with(|| deployment.bootstrap.clone());
        assert_always!(
            *bootstrap == deployment.bootstrap,
            "every node derives the same bootstrap configuration",
            { "node" => node.0, "members" => deployment.bootstrap.members().len() }
        );
        let pool: BTreeSet<u64> = deployment.pool.iter().map(|n| n.0).collect();
        // A genesis node boots with the genesis pool; a joiner (#189) — a node
        // outside it, admitted by the registry at runtime — boots with the
        // genesis pool plus itself and whoever the registry had admitted. A
        // joiner may boot before every genesis node: its pool is judged once
        // a genesis node names the genesis pool.
        let joiner = crate::roles::is_joiner(node.0);
        let mut judged = Vec::new();
        if joiner && st.pool.is_none() {
            st.early_joiner_pools.push((node.0, pool.clone()));
        } else if st.pool.is_none() {
            st.pool = Some(pool.clone());
            judged = std::mem::take(&mut st.early_joiner_pools);
        } else {
            judged.push((node.0, pool.clone()));
        }
        let known = st.pool.clone().unwrap_or_default();
        for (node, pool) in judged {
            // A node outside the genesis pool joined at runtime: a joiner,
            // or a machine the registry admitted.
            let same = if crate::roles::is_joiner(node) || !known.contains(&node) {
                known.is_subset(&pool) && pool.contains(&node)
            } else {
                known == pool
            };
            assert_always!(
                same,
                "every node derives the same node pool",
                { "node" => node, "pool" => pool.len() }
            );
        }
        st.replica_count = deployment.replica_count;
        st.matchmaker.note_deployment(&deployment.matchmakers);
        st.matchmaker.note_bootstrap(&deployment.bootstrap);
        st.matchmaker.node_booted(node);
        st.observe_promise(node.0, promised);
        // The boot report is the incarnation edge: swap in the faulty
        // classifications staged by *this* boot's scan and drop the previous
        // incarnation's — a stale excuse must not keep explaining divergence
        // forever.
        let fresh = st.faulty_staged.remove(&node.0).unwrap_or_default();
        st.reported_faulty.insert(node.0, fresh);
        // A reboot rewinds the durable chosen index by less than a page
        // (#602 (dark after a full restart)): the journal store writes its
        // metainfo once the index runs `CHOSEN_INDEX_LAG` slots ahead of the
        // durable one, so a booted leader re-proposes about one page, not its
        // whole log. `decided_prefix` is one past the index the last
        // incarnation reported durable.
        let held = st.decided_prefix.get(&node.0).copied().unwrap_or(0);
        let booted = chosen_index.map_or(0, |s| s.0 + 1);
        assert_always!(
            booted + paros::storage::journal::CHOSEN_INDEX_LAG > held,
            "a reboot rewinds the chosen index by less than a page",
            { "node" => node.0, "booted" => booted, "held" => held }
        );
        if held > booted {
            assert_reachable!("a reboot rewinds the chosen index");
        }
        // Fresh incarnation: the durable chosen index legally rewinds across
        // a crash (its writes flush relaxed), so restart the within-boot
        // watermarks from what this boot actually recovered.
        st.chosen_watermark
            .insert(node.0, chosen_index.map_or(0, |s| s.0));
        st.decided_prefix
            .insert(node.0, chosen_index.map_or(0, |s| s.0 + 1));
        st.read_watermark.remove(&node.0);
        // A floor the previous incarnation reported but never walked past
        // dies with it (#448): its walk's verdicts were never reported.
        st.floor_claims.remove(&node.0);
        let boot_floor = st
            .floor
            .get(&node.0)
            .copied()
            .unwrap_or_default()
            .strictly_before(now);
        // What a journal store's last commits had in flight (#264): the only
        // values a boot may recover in place of the reported ones.
        let in_flight = st.take_in_flight(node.0);
        for &(slot, ballot, vhash) in accepted {
            st.judge_recovered_value(node.0, slot.0, vhash, in_flight.get(&slot.0).copied());
            assert_always!(
                slot.0 >= boot_floor,
                "a truncated record is never recovered on boot (the log stays bounded)"
            );
            // Re-fold the durable record into the acceptor tally: a record
            // that became durable through an ambiguous fault leg (flushed,
            // but the driver crashed before reporting it) enters the
            // quorum-decided oracle here; a re-fold of an already-counted
            // record is idempotent.
            st.observe_durable_accept(node.0, slot.0, ballot, vhash);
            st.loss_rewritten_at_boot(node.0, slot.0);
        }
        st.settle_dropped_in_flight(node.0, chosen_index);
        // A boot settles what its in-flight commits landed (#263): an
        // outage's loss is judged again over what the tally now knows.
        st.reevaluate_losses();
        // A chosen index is only ever set once the commits below it were
        // learned, every one of which needed a durable accept quorum — one
        // the tally has folded from live reports, or one this very boot
        // report just re-supplied (an ambiguous fsync can land a decided
        // batch durably with the driver crashing before surfacing it, which
        // is why this check runs *after* the record fold above). The one
        // evidence a fold cannot recover is a torn record whose value rotted:
        // its `(slot, ballot)` identity survives as this boot's faulty
        // report, so those slots extend the admissible frontier. Anything
        // past all three is a fabricated prefix.
        if let Some(ci) = chosen_index {
            let faulty_max = st
                .reported_faulty
                .get(&node.0)
                .and_then(|slots| slots.iter().next_back().copied());
            let frontier = st.decided_max.max(faulty_max);
            assert_always!(
                frontier.is_some_and(|max| ci.0 <= max),
                "a recovered chosen index stays within the cluster's decided frontier",
                {
                    "node" => node.0,
                    "recovered" => ci.0,
                    "frontier" => crate::signed_watermark(frontier)
                }
            );
            // The recovered prefix is walked: with no application to replay
            // (#186), a boot resumes its walk one past the durable chosen
            // index — an admitted forward jump when the previous incarnation
            // made the index durable before reporting the walk over it.
            st.landings.entry(node.0).or_default().insert(ci.0);
            st.observe_applied_index(node.0, ci.0);
        }
        // The #71 explained-divergence form, first leg (Stage 7): a recovered
        // log missing a record this node durably persisted is legal iff a
        // detected-corruption crash explains it. The one honest reaction that
        // drops records without a crash — the truncate-on-mismatch bug class
        // (CTRL Figure 2) — is exactly what this catches: a node that
        // silently truncated on a mismatch reports a recovered log with an
        // unexplained hole. The current floor (not the boot-instant one) is
        // deliberate: a same-instant truncate+reboot only ever *excludes*
        // legally-dropped records, and the divergence this leg hunts never
        // raises the floor. Never weaken for unexplained divergence.
        let reported: BTreeSet<u64> = accepted.iter().map(|&(slot, _, _)| slot.0).collect();
        let floor_now = st.floor.get(&node.0).map_or(0, |f| f.now);
        let missing: Vec<u64> = st
            .persisted
            .range((node.0, 0)..=(node.0, u64::MAX))
            .map(|(&(_, slot), _)| slot)
            .filter(|slot| *slot >= floor_now && !reported.contains(slot))
            .collect();
        for slot in missing {
            let explained = st.corruption_crashed_records.contains(&(node.0, slot))
                || st.corruption_crashed_nodes.contains(&node.0)
                // A floor the node staged and a crash inside its commit
                // left durable unreported: the slot was legally compacted
                // (a floor only moves inside a walked chosen prefix, which
                // the truncation and trim-point oracles judge: `truncated`
                // and `judge_floor`, #448).
                || st
                    .requested_floor
                    .get(&node.0)
                    .is_some_and(|floor| slot < *floor)
                // Stage 8's second explanation: the record was classified
                // recoverable and reported into the tri-state this boot —
                // the peer-recovery path owns it now (#71: explained
                // divergence only, never a blanket weakening).
                || st
                    .reported_faulty
                    .get(&node.0)
                    .is_some_and(|slots| slots.contains(&slot));
            assert_always!(
                explained,
                "storage: a recovered log omits a persisted record only after a detected corruption crash",
                { "node" => node.0, "slot" => slot }
            );
        }
        recover_floor(&mut st, node.0, floor.0, chosen_index, now);
    }

    pub(super) fn call_received(
        &self,
        _node: NodeId,
        _journal: JournalIdentifier,
        call: paros::observe::metrics::CallKind,
    ) {
        if let Some((board, _)) = &self.machines {
            crate::machine::metered_call(board, call);
        }
        if let Some(board) = &self.racked {
            crate::machine::metered_call(board, call);
        }
    }

    pub(super) fn call_answered(
        &self,
        _node: NodeId,
        _journal: JournalIdentifier,
        (call, _): (
            paros::observe::metrics::CallKind,
            paros::observe::metrics::CallOutcome,
        ),
        written: u64,
        read: u64,
    ) {
        if let Some((board, _)) = &self.machines {
            crate::machine::metered_answer(board, call, written, read);
        }
        if let Some(board) = &self.racked {
            crate::machine::metered_answer(board, call, written, read);
        }
    }

    pub(super) fn read_expired(&self, _node: NodeId, early: bool) {
        let mut st = self.state();
        if early {
            // BUGGIFY pairing for the driver's early read expiry. The recovery
            // half is the client's own: "a read is retried across nodes
            // before committing" in the client history fold.
            reach_once!(
                st.read_expired_early,
                "the driver redirects a parked read before its confirmation deadline"
            );
        } else {
            reach_once!(
                st.read_expired_overdue,
                "a parked read outlives its confirmation deadline and is redirected"
            );
        }
    }

    pub(super) fn waiters_cleared(&self, _node: NodeId, _calls: u64) {
        let mut st = self.state();
        reach_once!(
            st.waiters_cleared,
            "a deposed leader clears client replies it still held"
        );
    }

    pub(super) fn election_timeout_set(&self, node: NodeId, ticks: u64) {
        self.state().election_timeouts.insert(node.0, ticks);
    }

    pub(super) fn leader_ack_received(&self, node: NodeId, from: NodeId, ballot: Ballot) {
        self.state().observe_ack_received(node.0, from.0, ballot);
    }

    pub(super) fn ticked(&self, node: NodeId) {
        self.state().observe_tick(node.0);
    }

    pub(super) fn election_backoff(&self, _node: NodeId, _doublings: u32) {
        // A cause, not an outcome: failed campaigns are the swarm's business;
        // what the backoff buys is the convergence claim.
        let mut st = self.state();
        reach_once!(
            st.election_backoff,
            "the driver backs off its election timeout after a failed campaign"
        );
    }

    pub(super) fn waiter_superseded(&self, _node: NodeId, _slot: Slot) {
        let mut st = self.state();
        reach_once!(
            st.waiter_superseded,
            "a parked proposal reply is superseded by a different decided command"
        );
    }

    pub(super) fn quorum_lost(&self, _node: NodeId, _count: u64) {
        self.state().quorum_lost = true;
    }

    pub(super) fn watermark_filled(&self, _node: NodeId, _count: u64) {
        let mut st = self.state();
        reach_once!(
            st.watermark_filled,
            "a leader fills its frontier to a read's vote watermark"
        );
    }

    pub(super) fn faulty_reported(&self, node: NodeId, entries: &[(Slot, Ballot)]) {
        let mut st = self.state();
        let st = &mut *st;
        // Staged, not live: this fires from the boot path *before* the boot's
        // `recovered` report, which swaps the staged set in as this
        // incarnation's classification (and drops the previous boot's).
        let staged = st.faulty_staged.entry(node.0).or_default();
        for &(slot, ballot) in entries {
            staged.insert(slot.0);
            st.faulty_ballots
                .insert((node.0, slot.0), (ballot.round, ballot.node.0));
            st.rewritten.remove(&(node.0, slot.0));
        }
        // A machine's `Batched` journal booted with its last batch lost
        // (#599 (durability mode per journal)).
        if let (Some((journal, _)), Some(tenants), false) =
            (&self.journal, &self.tenants, entries.is_empty())
        {
            tenants::lock(tenants).note_faulty(*journal);
        }
    }

    pub(super) fn repair_progress(
        &self,
        _node: NodeId,
        repaired: u64,
        case1: u64,
        case2: u64,
        step_downs: u64,
    ) {
        let mut st = self.state();
        if repaired > 0 {
            reach_once!(
                st.repaired_seen,
                "a faulty record is repaired in place from the cluster"
            );
        }
        if case1 > 0 {
            reach_once!(
                st.case1_seen,
                "a blocked slot resolves as Case 1 from a straggler's clean copy"
            );
        }
        if case2 > 0 {
            reach_once!(
                st.case2_seen,
                "a blocked slot resolves as Case 2 with a full quorum of none"
            );
        }
        if step_downs > 0 {
            reach_once!(
                st.repair_stepdown_seen,
                "a leader that cannot finish recovery resigns (recovery timeout)"
            );
        }
    }

    pub(super) fn recovery_batch(
        &self,
        _node: NodeId,
        started: u64,
        gap_fills: u64,
        remaining: u64,
        page: u64,
    ) {
        assert_always!(
            started <= page,
            "a leader starts at most one bounded recovery chunk per Ready",
            { "started" => started, "remaining" => remaining, "page" => page }
        );
        assert_always!(
            (1..=RECOVERY_BATCH as u64).contains(&page),
            "a recovery page size lies within its tunable range",
            { "page" => page }
        );
        assert_always!(
            gap_fills <= started,
            "a recovery batch reports only gap fills it actually started",
            { "started" => started, "gap_fills" => gap_fills }
        );
        if gap_fills > 0 {
            let mut st = self.state();
            reach_once!(
                st.gap_filled,
                "a new leader gap-fills a hole its promise quorum never reported"
            );
        }
    }

    pub(super) fn prepare_below_floor(&self, _node: NodeId, _from_slot: Slot, _floor: Slot) {
        let mut st = self.state();
        // Rare (only a lagging node below a compacted peer's floor triggers it),
        // so reachable-only: it must be hit at least once across exploration,
        // not on every seed.
        reach_once!(
            st.prepare_below_floor,
            "a candidate prepares below a peer's compaction floor"
        );
    }
}
