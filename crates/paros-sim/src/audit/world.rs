//! The per-iteration shared checker: [`AuditWorld`], the API every node and
//! workload reaches it through, and the run's final judgement
//! ([`check_run`], [`AuditWorld::check_final_convergence`]).

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use moonpool_sim::{StateHandle, assert_always, assert_reachable, assert_sometimes};

use super::ClientHistory;
use super::state::AuditState;

/// Well-known [`StateHandle`] key under which the single per-iteration
/// [`AuditWorld`] is published (shared by every node and every workload).
const AUDIT_WORLD_KEY: &str = "paros-audit-world";

/// Get-or-create the singleton [`AuditWorld`] for this iteration
/// (`crate::state::published_arc`).
pub(crate) fn audit_world(state: &StateHandle) -> Arc<AuditWorld> {
    audit_world_for(state, crate::shape::frames(state).main)
}

/// `journal`'s own [`AuditWorld`] (#188): every oracle folds one journal's
/// transitions, so safety, the clients' folds, convergence and the storage
/// gates are all keyed by journal without any of them knowing.
pub(crate) fn audit_world_for(state: &StateHandle, journal: paros::JournalKey) -> Arc<AuditWorld> {
    let main = crate::shape::frames(state).main;
    crate::state::published_arc(
        state,
        &crate::state::journal_key(AUDIT_WORLD_KEY, journal),
        || AuditWorld {
            main: Some(main),
            ..AuditWorld::default()
        },
    )
}

/// The per-iteration shared checker.
#[derive(Default)]
pub(crate) struct AuditWorld {
    state: Mutex<AuditState>,
    /// The run's deployment journal ([`crate::shape::Frames::main`]); `None`
    /// for a private checker outside any run.
    main: Option<paros::JournalKey>,
}

impl AuditWorld {
    /// The run's deployment journal, when this checker belongs to a run.
    pub(crate) fn main(&self) -> Option<paros::JournalKey> {
        self.main
    }

    /// A private checker for a run with **no client** at all (the storage
    /// contract suite drives the world-backed storage directly): every
    /// per-transition check still runs, except the "applied command was
    /// proposed" claim, which has no client to be proposed by.
    pub(crate) fn client_free() -> Self {
        let world = Self::default();
        world.lock().client_free = true;
        world
    }

    pub(super) fn lock(&self) -> MutexGuard<'_, AuditState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The workload registered a record it is about to write, hashed with
    /// [`record_hash`](super::journal_model::record_hash). Fed before the
    /// RPC leaves, so a folded record that was never registered is one the
    /// cluster invented.
    pub(crate) fn note_submitted(&self, record_hash: u64) {
        self.lock().submitted.insert(record_hash);
    }

    /// A client sent the write hashing to `vhash` (`paros::command_hash`)
    /// to this world's journal (#188) — what the non-interference oracle
    /// requires of every write a slot of the journal applies.
    pub(crate) fn note_appended(&self, vhash: u64) {
        self.lock().appended.insert(vhash);
    }

    /// A client sent the write hashing to `vhash` to this world's journal
    /// under **another** journal's fence (#247, the cross-tenant attack): it
    /// was appended here, so a slot may hold it, but every verdict on it
    /// must be a refusal.
    pub(crate) fn note_foreign(&self, vhash: u64) {
        let mut st = self.lock();
        st.appended.insert(vhash);
        st.foreign.insert(vhash);
    }

    /// The hash of the record the nodes' verdicts accepted at `position`
    /// (#204) — what a journal read's record there must hash to. `None`
    /// when the audit has not seen it accepted.
    pub(crate) fn record_at(&self, position: u64) -> Option<u64> {
        self.lock().journal.record_at(position)
    }

    /// A client folded the record at position `lsn` (#186: the application
    /// is the client's, fed by `Read`), hashing to `cmd_hash`, reaching
    /// `state`. Strictly increasing per client, one record and one state per
    /// position across every client, and a record traces to a submission.
    /// The message strings are the application check's, which this fold
    /// inherited from the storage layer.
    #[tracing::instrument(level = "trace", skip_all)]
    pub(crate) fn fold_applied(&self, client: u64, lsn: u64, cmd_hash: u64, state: u64) {
        let mut st = self.lock();
        let previous = st.fold_lsn.get(&client).copied();
        assert_always!(
            previous.is_none_or(|previous| lsn > previous),
            "chain: applies are contiguous per node",
            {
                "client" => client,
                "lsn" => lsn,
                "previous" => crate::signed_watermark(previous)
            }
        );
        st.fold_lsn.insert(client, lsn);
        let met = st.fold_state.contains_key(&lsn);
        let prior_command = *st.fold_command.entry(lsn).or_insert(cmd_hash);
        let prior_state = *st.fold_state.entry(lsn).or_insert(state);
        assert_always!(
            prior_command == cmd_hash && prior_state == state,
            "chain: one state per applied index",
            {
                "client" => client,
                "lsn" => lsn,
                "expected_command" => prior_command,
                "observed_command" => cmd_hash,
                "expected_state" => prior_state,
                "observed_state" => state
            }
        );
        st.fold_agreed |= met;
        assert_always!(
            st.client_free || st.submitted.contains(&cmd_hash),
            "chain: applied command was proposed",
            { "client" => client, "lsn" => lsn, "command" => cmd_hash }
        );
    }

    /// The cluster's applied high-water mark so far (`None` before any apply).
    pub(crate) fn cluster_applied_max(&self) -> Option<u64> {
        self.lock().cluster_applied_max
    }

    /// Whether a node outside a ballot's own configuration has answered that
    /// ballot's Phase 1 — the mechanism the departed-straggler corpus case is
    /// named for ("removed is not shut down"), read by that case so it can
    /// assert it actually happened.
    pub(crate) fn removed_member_promised(&self) -> bool {
        self.lock().removed_member_promised
    }

    /// Whether a node outside the configuration in force served a catch-up
    /// with entries: the second way a removed member's copy reaches the
    /// cluster, read by the departed-straggler case beside
    /// [`AuditWorld::removed_member_promised`].
    pub(crate) fn removed_member_served(&self) -> bool {
        self.lock().removed_member_served
    }

    /// A one-line picture of the run for the red path: per-node applied
    /// prefixes, the leader rounds, and the last chosen gap each node reported.
    pub(crate) fn diagnostics(&self) -> String {
        let st = self.lock();
        format!(
            "applied_max={:?} cluster_max={:?} booted={:?} storage_dead={:?} leader_rounds={:?} last_gap={:?} promised={:?} sent={:?} delivery_failures={} edge_rejections={} matchmakers=[{}]",
            st.applied_max,
            st.cluster_applied_max,
            st.booted,
            st.storage_dead,
            st.leader_rounds,
            st.last_gap,
            st.promised,
            st.sent_kinds,
            st.delivery_failures,
            st.edge_rejections,
            st.matchmaker.diagnostics()
        )
    }

    /// Record this run's `sometimes` coverage gates. Called once per workload
    /// from the `check()` phase; repeating it (several client workloads run
    /// concurrently) is idempotent — a `sometimes` slot only accumulates
    /// samples, and an `always` re-check of the same true fact is free.
    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) fn check_gates(&self) {
        let st = self.lock();
        // Liveness reachability: a value does get chosen.
        assert_sometimes!(st.any_chosen, "a value is eventually chosen");
        // The per-ballot proposal check is only as good as the field it reads;
        // saturation has to see it actually compare something.
        assert_sometimes!(
            st.any_proposal_checked,
            "a proposed command is checked against its ballot's other proposals"
        );
        assert_sometimes!(
            st.any_ack_checked,
            "a committed write ack is checked against the acking node's applied prefix"
        );
        // The application check lives on the clients' folds (#186): it is
        // only as good as two folds meeting at one position.
        assert_sometimes!(
            st.fold_agreed,
            "chain: two clients fold the same entry to the same state"
        );
        st.journal.check();
        st.check_protocol_gates();
        st.check_tier_gates();
        st.check_driver_hook_gates();
        st.matchmaker.check_gates();
    }

    /// Merge one client's recorded history into the shared one and run the
    /// client-visible checks over everything merged so far; the
    /// linearizability search runs once, when the last of the journal's
    /// `clients` clients merged. Every client workload calls this from
    /// `check()`; the merged history only grows, so a later caller sees a
    /// superset and the per-operation checks stay sound at every step.
    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) fn check_client_history(&self, history: &ClientHistory, clients: usize) {
        let mut st = self.lock();
        st.lin.merge(history);
        st.check_client_history(clients);
    }

    /// How many Stage-6 write/fsync faults the drivers *detected* (one typed
    /// [`Audit::storage_fault`](paros::Audit::storage_fault) crash decision
    /// each). The workload's `check()` correlates this against the storage
    /// world's injected ground truth.
    pub(crate) fn storage_faults_detected(&self) -> u64 {
        self.lock().storage_faults_detected
    }

    /// How many Stage-7 corruption/metadata detections the drivers surfaced
    /// as typed crash decisions. Correlated 1:1 against the world's
    /// corruption ledger by the workload's `check()`.
    pub(crate) fn corruption_faults_detected(&self) -> u64 {
        self.lock().corruption_crashes
    }

    /// A node was terminally parked by a detected persistent corruption
    /// (detect ⇒ crash, stays down for the run). Convergence excuses exactly
    /// these nodes — and only when the crash decision that explains the
    /// unavailability was actually observed (the asymmetric oracle:
    /// unavailable = pass, unsafe = fail — but *unexplained* unavailable is
    /// still a failure).
    #[tracing::instrument(level = "debug", skip(self), fields(node))]
    pub(crate) fn note_storage_dead(&self, node: u64) {
        self.lock().storage_dead.insert(node);
    }

    /// A boot found its identity retired by the operator (#123) and exited.
    #[tracing::instrument(level = "debug", skip(self), fields(node))]
    pub(crate) fn note_retired_boot(&self, node: u64) {
        self.lock().retired.insert(node);
    }

    /// The operator parked a retired identity for good after an ambiguous
    /// `Retire` (#123): the world never releases it, so it stays down
    /// whether or not it ever boots again — and a node that was already
    /// dead when it was retired may not reboot before the run ends, when
    /// [`note_retired_boot`](Self::note_retired_boot) would have told the
    /// audit. Seed 7375908273228548587 of the moonpool-rpc migration's
    /// 10k hunt: node 0 crashed at 3.9 s, was reconfigured out and named
    /// retirable, the workload's `Retire` found it dead, and the run ended
    /// before its restart — convergence then waited on a node the harness
    /// itself had shut down.
    #[tracing::instrument(level = "debug", skip(self), fields(node))]
    pub(crate) fn note_retired_parked(&self, node: u64) {
        self.lock().retired.insert(node);
    }

    /// A joiner left the pool through the node registry (#189) and stopped
    /// its journals for good: convergence excuses it. Not a GC retirement —
    /// no floor named it, and none needed to: the operators never retire a
    /// joiner a reconfiguration named.
    #[tracing::instrument(level = "debug", skip(self), fields(node))]
    pub(crate) fn note_left_pool(&self, node: u64) {
        self.lock().left_pool.insert(node);
    }

    /// A matchmaker's registry was lost for good (#125).
    #[tracing::instrument(level = "debug", skip(self))]
    pub(crate) fn note_matchmaker_lost(&self) {
        self.lock().matchmaker.lost();
    }

    /// A node booted again after a process-level kill (moonpool attrition on
    /// the main campaign, the script on the corpus) while `parked_peers` other
    /// nodes sat terminally parked. Until this very boot the node was down, so
    /// the two loss kinds — persistent (a parked disk that never comes back)
    /// and transient (a process that does) — overlapped for the whole hold-down.
    /// On a small cluster that overlap is the interesting one: `n = 3` with one
    /// parked node and one killed node has **no quorum** until the killed node
    /// returns, and the run is still required to converge afterwards.
    ///
    /// Recorded here as coverage, never as a verdict: whether a seed draws both
    /// an attrition kill and a parking corruption is the swarm's business.
    /// `quorum` is the live members the configuration floor needs to keep
    /// running — the run's quorum-system policy at that size, handed in
    /// rather than re-derived from the count here.
    #[tracing::instrument(
        level = "debug",
        skip(self),
        fields(node, parked_peers, cluster_size, quorum)
    )]
    pub(crate) fn note_process_restart(
        &self,
        node: u64,
        parked_peers: usize,
        cluster_size: usize,
        quorum: usize,
    ) {
        let mut st = self.lock();
        if parked_peers == 0 {
            return;
        }
        reach_once!(
            st.parked_overlap,
            "storage: a transient process loss overlaps a corruption-parked node"
        );
        // The node reporting is the one that was down; anything else down at
        // the same time only makes the loss deeper, so this is the *at least*
        // side of the count.
        let live_during_hold_down = cluster_size.saturating_sub(parked_peers + 1);
        if live_during_hold_down < quorum {
            reach_once!(
                st.parked_overlap_quorum_returned,
                "storage: quorum returns after a parked node and a transient process loss overlapped"
            );
        }
        tracing::info!(node, parked_peers, cluster_size, "restart_over_parked_peer");
    }

    /// Whether any node has been observed lagging the cluster prefix (one leg
    /// of the #71 compound corruption x partition x lag gate).
    pub(crate) fn lag_observed(&self) -> bool {
        !self.lock().lagged.is_empty()
    }

    /// Ground-truth feed from the storage world (issue #19 C). A record can
    /// become durable through an *ambiguous* fault leg — the flush happened,
    /// but the driver crashed on the reported error before surfacing it — so
    /// the driver's audit stream alone would go stale and the next reboot
    /// would trip the cross-restart checks as false positives. The world owns
    /// the ground truth, so every flush refreshes the **reference data** those
    /// checks compare against: the per-`(node, slot)` persisted value, the
    /// compaction floor, and the admitted trim-point landings. Reference data
    /// only — progress/liveness state (`applied_max`, quiescence clocks) stays
    /// driver-reported, so this observation cannot mask a liveness bug. This
    /// is what keeps recovered-equals-persisted checkable against *actual*
    /// durable state (the #71 weakening is for Stage 7-8, not this).
    pub(crate) fn note_flushed_ground_truth(
        &self,
        node: u64,
        now_ms: u64,
        accepted: &[(u64, u64)],
        floor: Option<u64>,
        landing: Option<u64>,
    ) {
        let mut st = self.lock();
        for &(slot, vhash) in accepted {
            st.persisted.insert((node, slot), vhash);
        }
        if let Some(first) = floor {
            st.floor.entry(node).or_default().raise(first, now_ms);
        }
        if let Some(landing) = landing {
            st.landings.entry(node).or_default().insert(landing);
        }
    }

    /// A fold of the run's end state for the determinism proof: the chosen
    /// log, every node's applied prefix, and the leadership history. Two runs
    /// of one seed must agree on it bit for bit.
    pub(crate) fn digest(&self) -> u64 {
        let st = self.lock();
        let mut h: u64 = 0xcbf2_9ce4_8422_2325;
        let mut fold = |v: u64| {
            for byte in v.to_le_bytes() {
                h ^= u64::from(byte);
                h = h.wrapping_mul(0x0000_0100_0000_01b3);
            }
        };
        for (&slot, &vhash) in &st.chosen {
            fold(slot);
            fold(vhash);
        }
        for (&node, &max) in &st.applied_max {
            fold(node);
            fold(max);
        }
        for &round in &st.leader_rounds {
            fold(round);
        }
        for (&node, &round) in &st.leader_round {
            fold(node);
            fold(round);
        }
        h
    }

    /// The convergence deliverable, judged at the end of the recovery tail
    /// when no future leader change can invalidate a provisional quiescence
    /// decision. It ties the run's four frontiers together:
    ///
    /// ```text
    /// decided frontier == applied frontier == every live node's applied prefix >= every acked slot
    /// ```
    ///
    /// - the **decided frontier** (`decided_max`) is the highest slot a
    ///   majority of the configured cluster durably accepted at one ballot
    ///   for one value — the quorum-decided oracle
    ///   ([`AuditState::decided`]: keyed by `(slot, ballot)`, value-checked
    ///   per key, so it is Paxos "chosen" and never a cross-ballot count),
    ///   fed by durable accepts alone, so it sees a slot that is chosen even
    ///   if no node ever applied it (the blind spot of the apply-fed `chosen`
    ///   map);
    /// - the **applied frontier** (`cluster_max`) is the highest slot any node
    ///   applied; a node's applied prefix is contiguous (`check_no_gaps`), so
    ///   the frontier names a prefix, not a sparse set;
    /// - every node this run brought up that is not terminally parked ends
    ///   exactly on that frontier;
    /// - every slot a client was told was committed is inside it (the
    ///   per-identity presence check lives in the client history fold).
    ///
    /// `decided == applied` is two liveness claims in one. `decided <= applied`
    /// says every quorum-decided slot was eventually applied: a slot durably
    /// accepted by a majority whose proposer never learnt it (lost `Accepted`
    /// acks, a crashed proposer) must still be chosen — by the `Accept`
    /// re-send, by a successor's P2c re-proposal, or as a gap-filled `Noop`
    /// (the applied value's agreement with the decided value is asserted per
    /// apply, so a `Noop` here means the decided command was itself a
    /// control command or a #94-suppressed identity). `decided >= applied`
    /// says nothing was applied without a durable majority behind it — the
    /// persist-before-send ordering seen from the outside. That ordering is
    /// not assumed here; it is asserted at each transition it rests on, and
    /// this end-of-run leg is their corollary: an outgoing `Accepted` names
    /// a durably recorded accept, an outgoing `Commit` names a slot the
    /// tally already decided with that value, and every `applied` report
    /// finds its slot decided (all three in the `sent`/`applied` callbacks
    /// below). The driver folds each accept at its fsync
    /// (`surface_persisted`), before the ack that could count toward a
    /// quorum leaves the node.
    ///
    /// Sparse states are excused where they are legal: a run in which nothing
    /// was ever applied has no frontier (then nothing may have been decided or
    /// acked either), and a parked node is excused from the per-node leg only
    /// when its parking was observed as a corruption crash.
    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) fn check_final_convergence(&self, acked_max: Option<u64>) {
        let mut st = self.lock();
        let Some(cluster_max) = st.applied_max.values().copied().max() else {
            assert_always!(
                acked_max.is_none(),
                "every acked slot is inside the cluster's applied prefix at the end of the tail"
            );
            assert_always!(
                st.decided_max.is_none(),
                "every quorum-decided slot is applied by the end of the tail",
                { "decided_max" => st.decided_max.unwrap_or(0), "cluster_max" => -1_i64 }
            );
            return;
        };
        // The prefix every node must reach covers everything any client was
        // told was committed.
        assert_always!(
            acked_max.is_none_or(|acked| acked <= cluster_max),
            "every acked slot is inside the cluster's applied prefix at the end of the tail",
            { "acked_max" => acked_max.unwrap_or(0), "cluster_max" => cluster_max }
        );
        // The decided frontier and the applied frontier coincide (see above).
        assert_always!(
            st.decided_max.is_none_or(|decided| decided <= cluster_max),
            "every quorum-decided slot is applied by the end of the tail",
            { "decided_max" => st.decided_max.unwrap_or(0), "cluster_max" => cluster_max }
        );
        assert_always!(
            st.decided_max.is_some_and(|decided| decided >= cluster_max),
            "the applied frontier never runs ahead of the quorum-decided frontier",
            {
                "decided_max" => crate::signed_watermark(st.decided_max),
                "cluster_max" => cluster_max
            }
        );
        let cluster: BTreeSet<u64> = st
            .booted
            .iter()
            .copied()
            .chain(st.applied_max.keys().copied())
            .collect();
        // Stage 7's asymmetric availability oracle: a node terminally parked
        // by detect ⇒ crash is excused from convergence — but only when the
        // crash decision explaining its unavailability was actually observed,
        // and only for a minority (the world's dead-node budget, re-asserted
        // in `check_storage_gates`). Unexplained unavailability stays a
        // failure.
        for node in &st.storage_dead {
            assert_always!(
                st.corruption_crashed_nodes.contains(node),
                "storage: a node that stays down is explained by a detected corruption crash",
                { "node" => *node }
            );
        }
        // #123: a retired node stays down only because a leader's effective
        // floor named it retirable (the operator acted on the leader's word).
        for node in &st.retired {
            assert_always!(
                st.matchmaker.retired_by_gc(*node),
                "gc: a node that stays down retired only after an effective floor named it",
                { "node" => *node }
            );
        }
        for node in cluster {
            if st.storage_dead.contains(&node)
                || st.wiped.contains(&node)
                || st.retired.contains(&node)
                || st.left_pool.contains(&node)
            {
                continue;
            }
            let prefix = st.applied_max.get(&node).copied();
            assert_always!(
                prefix == Some(cluster_max),
                "every node converges to the cluster's chosen prefix at the end of the settle tail",
                {
                    "node" => node,
                    "prefix" => crate::signed_watermark(prefix),
                    "cluster_max" => cluster_max,
                    "decided_max" => crate::signed_watermark(st.decided_max)
                }
            );
        }
        // Proof the catch-up path actually healed a hole (not merely that
        // nothing ever broke) — judged against the FINAL cluster maximum, so a
        // transient mid-run match cannot satisfy it.
        let healed = st
            .lagged
            .iter()
            .any(|n| st.applied_max.get(n).copied() == Some(cluster_max))
            && cluster_max > 0;
        if healed {
            reach_once!(st.caught_up, "a lagging node converges via catch-up");
        }
        assert_sometimes!(
            st.caught_up,
            "a lagging node catches up to the cluster's chosen prefix"
        );
    }
}

/// The whole check, in one place: the two perspectives a run is judged from.
///
/// **Client side** — the workload's own history, merged into the shared one:
/// once the journal's last client (of `clients`) merged, the search for a
/// linearization of every attempt against the journal's sequential model
/// (#205), and every acked write inside the journal's positions. **Audit side** — the coverage gates recorded once per run, the
/// storage world's injected⇔detected correlation, and the one liveness claim:
/// every live node ends on the cluster's applied prefix, which covers every
/// acked slot. All of it over `journal`'s own worlds (#188). Returns the
/// run's digest for the determinism proof.
#[tracing::instrument(level = "debug", skip_all)]
pub(crate) fn check_run(
    state: &StateHandle,
    journal: paros::JournalKey,
    history: &ClientHistory,
    clients: usize,
) -> u64 {
    let audit = audit_world_for(state, journal);
    audit.check_client_history(history, clients);
    audit.check_gates();
    crate::world::check_storage_gates(state, journal);
    super::journals::lock(&super::journals::journal_board(state)).check_gates();
    super::system::lock(&super::system::system_board(state)).check_gates();
    // Every slot a call was answered at is inside what every live node
    // walked by the end (#204: answers name positions to the client, slots
    // to the audit).
    let acked_max = audit.lock().answered_max;
    audit.check_final_convergence(acked_max);
    audit.digest()
}
