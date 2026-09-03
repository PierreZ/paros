//! The folded facts of one run and the per-transition checks over them.
//!
//! Every field is one incremental fact the checks need, and nothing else. The
//! trailing block of `AuditState` is a *flag set*, not a state machine: one
//! independent sticky bit per `reachable` gate, each flipped once at its own
//! transition and read once at `check()`.

use std::collections::{BTreeMap, BTreeSet};

use moonpool_sim::{assert_always, assert_reachable, assert_sometimes, assert_sometimes_all};
use paros::{AcceptorConfig, Ballot, HEARTBEAT_TICKS, Slot};

use super::client::{LinHistory, check_disclosed_order, check_sequential_client};
use super::matchmaker::MatchmakerAudit;

/// Consecutive **deposed heartbeats** a leader may broadcast before the checker
/// calls it a zombie (#95). **Never buggified**: an oracle threshold is the
/// judgement a run is measured against, so drawing it per seed does not
/// explore a new state, it changes the verdict on the old one (AGENTS.md,
/// prong 2). A leader whose ballot a promise-majority has moved
/// strictly past can never again assemble any quorum at that ballot — every
/// below-promise beat is ignored unacked — yet without `CheckQuorum` nothing ever
/// demotes it while it is partitioned from the very peers that could tell it.
/// `CheckQuorum` bounds the zombie window to one ack-quorum-less election-timeout
/// window (at most ~10 ticks, one beat per tick); forty beats is ~4x that on
/// the protocol's own clock, immune to wall-time dilation.
/// Ticks of slack past two `CheckQuorum` windows a deposed leader may keep
/// beating: the window it is in when the promise-majority forms may have just
/// started, so it needs that one and the next to notice, plus the tick that
/// runs the check. An oracle threshold: never buggified.
const DEPOSED_TICK_SLACK: u64 = 2;

/// One leader's deposed-heartbeat streak (#95): the ballot it is beating at,
/// the last beat seq counted (one broadcast fans out to n-1 sends, so the seq
/// dedups the fan-out), and how many consecutive beats were sent while a
/// promise-majority sat strictly above the ballot.
#[derive(Clone, Copy, Default)]
pub(super) struct DeposedStreak {
    pub(super) round: u64,
    pub(super) node: u64,
    pub(super) seq: u64,
    /// Whether the last beat at this ballot was deposed (a promise-majority
    /// of its configuration sits strictly above it).
    pub(super) deposed: bool,
    /// Ticks this node has run while `deposed` held.
    pub(super) ticks: u64,
    /// Consecutive ticks with no beat observed at this ballot. A leader beats
    /// every [`paros::HEARTBEAT_TICKS`] ticks, so more than one whole beat
    /// period of silence means it stepped down — but *one* period of silence
    /// does not: the send seam's `drop_outgoing` skips `Audit::sent`, so a
    /// fully dropped beat is invisible here, and a single beatless tick used
    /// to close the streak and hand a zombie leader a fresh budget.
    pub(super) beatless_ticks: u64,
}

/// Who is exercising one logical Phase-2 authority (one ballot), reconstructed
/// **from semantic events only** — the `Accept`s actually put on the wire, and
/// the relinquish/install transitions — never from any node's `role` field.
/// Reading a node's own belief about its leadership would only re-derive the
/// implementation's interpretation; this re-derives the *observable* one.
#[derive(Clone, Debug, Default)]
pub(super) struct Authority {
    /// The single node currently observed exercising this ballot.
    pub(super) holder: Option<u64>,
    /// Nodes that have permanently given this authority up. The `DPaxos` rule:
    /// an authority is relinquished at most once per node, and never exercised
    /// again afterwards.
    pub(super) retired: BTreeSet<u64>,
    /// The highest allocator frontier this authority has been transferred with.
    /// Monotone: a successor that rewound it could propose a *different*
    /// command at a `(slot, ballot)` its predecessor already used.
    pub(super) frontier: u64,
}

/// One node's compaction floor, plus what it was before the most recent raise.
///
/// The truncation checks admit a record at a slot the node compacts away *in
/// the same simulated millisecond*: the accept happened first, in-core, guarded
/// by the core's own floor check, so counting a same-instant compaction against
/// it would be a false positive. Keeping the pre-raise value is how an
/// incremental fold reproduces the "compactions strictly before this event"
/// window the trace-scanning oracle used.
///
/// Load-bearing assumption: [`Floor::strictly_before`] queries arrive in
/// non-decreasing `now_ms` (the sim clock is monotone and every caller stamps
/// its own instant); a query for a *past* instant would see too new a floor.
#[derive(Clone, Copy, Default)]
pub(super) struct Floor {
    pub(super) now: u64,
    pub(super) before_last_raise: u64,
    pub(super) raised_ms: u64,
}

impl Floor {
    /// The floor established strictly before `now_ms`.
    pub(super) fn strictly_before(self, now_ms: u64) -> u64 {
        if self.raised_ms < now_ms {
            self.now
        } else {
            self.before_last_raise
        }
    }

    pub(super) fn raise(&mut self, first: u64, now_ms: u64) {
        // Deliberately lenient about `first <= now`: the ground-truth flush
        // feed passes the *requested* floor through, and the storage contract
        // legally treats a lower request as a no-op (the contract suite
        // exercises exactly that). The no-regression assert lives on the
        // driver-audited truncation report instead, where the core's monotone
        // floor contract genuinely holds.
        if first <= self.now {
            return;
        }
        if self.raised_ms != now_ms {
            self.before_last_raise = self.now;
            self.raised_ms = now_ms;
        }
        self.now = first;
    }
}

/// Every incremental fact the checks need, and nothing else.
///
/// The trailing block is a *flag set*, not a state machine: one independent
/// sticky bit per `reachable` gate, each flipped once at its own transition and
/// read once at `check()`. Folding them into enums would couple gates that have
/// nothing to do with each other, so the bool-count lint is waived here.
#[derive(Default)]
#[allow(clippy::struct_excessive_bools)]
pub(super) struct AuditState {
    // --- Paxos safety -------------------------------------------------------
    /// Cluster-wide: the value chosen for each slot.
    pub(super) chosen: BTreeMap<u64, u64>,
    /// Per node: the last durable promised ballot.
    pub(super) promised: BTreeMap<u64, Ballot>,
    /// `(ballot round, ballot node, slot)` → the command that ballot proposed.
    pub(super) proposed: BTreeMap<(u64, u64, u64), u64>,
    /// `(slot, ballot round, ballot node)` → the command durably accepted.
    pub(super) accepted: BTreeMap<(u64, u64, u64), u64>,
    /// Per `(node, slot)`: the last value the node made durable.
    pub(super) persisted: BTreeMap<(u64, u64), u64>,
    /// The bootstrap acceptor configuration, from the boot reports (one
    /// shared deployment per run). The configuration of every ballot on plain
    /// Multi-Paxos, and of the ballots below the first registration on a
    /// matchmaker deployment.
    pub(super) bootstrap: Option<AcceptorConfig>,
    /// The addressable node pool, from the boot reports.
    pub(super) pool: Option<BTreeSet<u64>>,
    /// Per registered ballot `(round, node)`: the acceptor configuration
    /// bound to it — what that ballot's Phase-2 quorums are counted over.
    /// Bound at the matchmaker's durable registration (before any leader can
    /// exercise the ballot) and re-asserted at the election.
    pub(super) configs: BTreeMap<(u64, u64), AcceptorConfig>,
    /// Per `(node, ballot)`: the prior configurations its matchmaking closed
    /// with (`H_b`), for the cross-configuration Phase-1 oracle.
    pub(super) prior: BTreeMap<(u64, u64, u64), Vec<AcceptorConfig>>,
    /// Per ballot `(round, node)`: every node whose `Promise` for it left the
    /// wire — the Phase-1 answers its leader could possibly have counted
    /// (sends are a superset of receipts, so a quorum the leader claims must
    /// show here first).
    pub(super) promise_senders: BTreeMap<(u64, u64), BTreeSet<u64>>,
    /// `(slot, ballot round, ballot node)` → the nodes holding a durable
    /// accept for it — the acceptor tally behind the quorum-decided oracle.
    /// Fed by both the live accept fold and the boot re-reports (idempotent).
    pub(super) accept_sets: BTreeMap<(u64, u64, u64), BTreeSet<u64>>,
    /// Per slot: the first quorum-decided `(ballot round, ballot node,
    /// vhash)`. Records a decision the moment a majority of the *configured*
    /// cluster holds a durable accept **at one ballot for one value** — the
    /// tally is keyed by `(slot, ballot)`, and two commands under one key are
    /// themselves a violation — so this is Paxos "chosen", not a count of
    /// accepts across ballots (a majority split between `(b5, X)` and
    /// `(b6, Y)` decides nothing until one key alone reaches a quorum). It
    /// is recorded even if no node ever applies the slot, which is exactly
    /// the blind spot the apply-fed `chosen` map has.
    pub(super) decided: BTreeMap<u64, (u64, u64, u64)>,
    /// The highest slot ever quorum-decided — a monotone scalar the
    /// below-floor pruning of `decided` never lowers, so the cross-restart
    /// frontier check stays sound after the whole prefix compacts away.
    pub(super) decided_max: Option<u64>,
    /// Per node: the last durably reported chosen index, reset each boot
    /// (`SetChosenIndex` flushes relaxed, so a crash may legally rewind it
    /// across incarnations — within one it only advances).
    pub(super) chosen_watermark: BTreeMap<u64, u64>,
    /// Per node: the highest confirmed read index served, reset each boot.
    pub(super) read_watermark: BTreeMap<u64, Option<u64>>,

    // --- truncation ---------------------------------------------------------
    pub(super) floor: BTreeMap<u64, Floor>,
    /// Per node: the highest floor its *driver-audited truncations* have
    /// reported — the monotonicity watermark for those reports alone (the
    /// folded [`Floor`] also absorbs installs and ground-truth flushes, which
    /// can legally outrun a reordered stale truncate; see
    /// [`NodeAudit::truncated`]).
    pub(super) truncate_watermark: BTreeMap<u64, u64>,

    // --- applied prefix -----------------------------------------------------
    /// Per node: the next slot expected to be newly applied.
    pub(super) frontier: BTreeMap<u64, u64>,
    /// Per node: every index it jumped to through a snapshot install.
    pub(super) snap_landings: BTreeMap<u64, BTreeSet<u64>>,
    /// Per acceptor: below-floor `Prepare`s it refused (a corpus probe).
    pub(super) below_floor_refusals: BTreeMap<u64, u64>,
    /// Per node: its applied high-water mark (absent = applied nothing).
    pub(super) applied_max: BTreeMap<u64, u64>,
    pub(super) cluster_applied_max: Option<u64>,
    pub(super) lagged: BTreeSet<u64>,
    pub(super) booted: BTreeSet<u64>,
    /// Per node: the last `chosen_gap` it reported, `(hole, above)`. Not
    /// asserted on — a gap is an ordinary transient — but printed on the red
    /// path, where "which node is stuck, and where" is the first question.
    pub(super) last_gap: BTreeMap<u64, (u64, u64)>,

    // --- application state (the Chain-of-Blocks register) --------------------
    /// User command hashes the workload registered before proposing.
    pub(super) submitted: BTreeSet<u64>,
    /// This run has no client (see `AuditWorld::client_free`).
    pub(super) client_free: bool,
    /// Per node: its application's applied count (contiguity frontier).
    pub(super) app_index: BTreeMap<u64, u64>,
    /// Per applied index: the command hash every node must apply there.
    pub(super) app_command: BTreeMap<u64, u64>,
    /// Per applied index: the state hash every node must reach there.
    pub(super) app_state: BTreeMap<u64, u64>,
    pub(super) noop_applied: bool,

    // --- cooperative leader handoff -----------------------------------------
    /// `(ballot round, ballot node)` → who is exercising that logical authority
    /// (see [`Authority`]). The uniqueness oracle's whole state.
    pub(super) authorities: BTreeMap<(u64, u64), Authority>,
    /// Refusal totals folded from [`Audit::handoff_refused`].
    pub(super) handoff_refused: (u64, u64, u64, u64),
    /// `(node, authority)` pairs the core decided to relinquish — the
    /// "at most once" ledger, keyed on the decision rather than the wire (one
    /// decision can be re-transmitted many times).
    pub(super) relinquish_calls: BTreeSet<(u64, (u64, u64))>,
    /// How many authorities have been installed in this run.
    pub(super) handoff_installs: u64,

    // --- leadership ---------------------------------------------------------
    /// Per node: its deposed-heartbeat streak (#95, see [`DeposedStreak`]).
    pub(super) deposed_streaks: BTreeMap<u64, DeposedStreak>,
    /// Per node: the election timeout in force (ticks), from
    /// [`paros::Audit::election_timeout_set`].
    pub(super) election_timeouts: BTreeMap<u64, u64>,
    pub(super) leader_round: BTreeMap<u64, u64>,
    pub(super) leader_rounds: BTreeSet<u64>,
    pub(super) first_leader_round: Option<u64>,
    pub(super) leader_change_ms: Option<u64>,
    /// A committed client ack landed after leadership first changed hands.
    pub(super) ack_after_leader_change: bool,
    /// A node crashed at any durability seam.
    pub(super) crashed_any: bool,

    // --- client history -----------------------------------------------------
    pub(super) lin: LinHistory,

    // --- the matchmaker registry (#119) -------------------------------------
    pub(super) matchmaker: MatchmakerAudit,

    // --- sticky coverage flags ---------------------------------------------
    pub(super) any_chosen: bool,
    pub(super) any_proposal_checked: bool,
    pub(super) any_ack_checked: bool,
    pub(super) config_tagged_protocol_message: bool,
    pub(super) any_leader: bool,
    pub(super) leader_promise_checked: bool,
    pub(super) compacted: bool,
    pub(super) prepare_below_floor: bool,
    pub(super) gap_filled: bool,
    pub(super) snapshot_installed: bool,
    pub(super) snapshot_offered: bool,
    pub(super) snapshot_mid_election: bool,
    pub(super) caught_up: bool,
    /// At-most-once ledger for the oracle: each applied user command's
    /// `(client, seq)` and the single log index it applied at. A second apply
    /// of the same identity at a *different* index is the double-apply the
    /// core review flagged (mandatory P2c re-proposal of a stale suffix after
    /// a healed partition) — every node applies it, so per-index agreement is
    /// blind to it by construction.
    pub(super) applied_identity: BTreeMap<(u64, u64), u64>,
    /// The #94 suppression fired: a re-chosen `(client, seq)` executed as a
    /// no-op. Reachable-only (no `sometimes` counterpart): the interleaving
    /// needs a partition-shaped seed and would starve saturation as a per-run
    /// gate, but when a seed does reach it, the sweep records it.
    pub(super) duplicate_suppressed: bool,
    /// `CheckQuorum` fired (#95): a leader without an ack quorum for a full
    /// election-timeout window demoted itself. The n=2 regime plus attrition
    /// generates it reliably (killing the only peer starves the window).
    pub(super) quorum_lost: bool,
    /// A parked proposal reply was superseded by a different decided command
    /// and answered with a redirect instead of a false commit. Reachable-only:
    /// needs a stale leader learning a foreign decision for a slot it admitted.
    pub(super) waiter_superseded: bool,
    pub(super) multi_slot_applied: bool,
    pub(super) several_slots_applied: bool,
    pub(super) leadership_turnover: bool,
    pub(super) crashed_before_sync: bool,
    pub(super) crashed_after_sync: bool,
    /// Typed Stage-6 write/fsync crash decisions folded in
    /// ([`Audit::storage_fault`] with `Io`/`FsyncFailed`).
    pub(super) storage_faults_detected: u64,
    pub(super) storage_fault_crashed: bool,
    /// Typed Stage-7 corruption/metadata crash decisions folded in.
    pub(super) corruption_crashes: u64,
    pub(super) corruption_crashed: bool,
    /// Explanation state for the recovered-vs-persisted divergence leg (#71,
    /// first leg): the accepted records — and the nodes — whose corruption
    /// crash was actually observed. A boot missing a persisted record is
    /// legal iff explained here (the peer-heal leg arrives in Stage 8).
    pub(super) corruption_crashed_records: BTreeSet<(u64, u64)>,
    pub(super) corruption_crashed_nodes: BTreeSet<u64>,
    /// Nodes terminally parked by detect ⇒ crash (fed by the sim node loop).
    pub(super) storage_dead: BTreeSet<u64>,
    /// Nodes whose disk was wiped at a restart (#124, fed by the sim node
    /// loop): the identity is gone for good and excused from convergence —
    /// the explanation is the harness's own coin, never a driver decision.
    pub(super) wiped: BTreeSet<u64>,
    /// Nodes that shut down on an operator's retirement (#123): reported by
    /// the driver at the instant they exit, or by a boot that found the
    /// identity retired.
    pub(super) retired: BTreeSet<u64>,
    /// Matchmakers whose registry was lost for good (#125).
    pub(super) matchmakers_lost: BTreeSet<u64>,
    pub(super) wiped_any: bool,
    /// A client asked some leader to reconfigure the matchmaker set.
    pub(super) reconfigure_matchmakers_started: bool,
    /// Some leader refused a matchmaker-set reconfiguration request.
    pub(super) reconfigure_matchmakers_refused: bool,
    /// A process-level restart (attrition, or the corpus script) booted while
    /// at least one *other* node sat terminally parked: a transient process
    /// loss overlapped a persistent storage loss.
    pub(super) parked_overlap: bool,
    /// The overlap above cost the cluster its quorum (the parked set plus the
    /// node that was down left fewer live nodes than a majority), and the
    /// restart that reported it is what returned the quorum.
    pub(super) parked_overlap_quorum_returned: bool,
    /// Stage 8: per node, the slots its boot scan classified recoverable and
    /// reported into the tri-state — the second explanation the divergence
    /// and no-gaps checks accept (#71's explained-only rule). Scoped to the
    /// node's **current incarnation**: each boot re-runs the scan and
    /// re-reports what is still faulty, so a stale excuse from a previous
    /// boot must not keep explaining gaps forever.
    pub(super) reported_faulty: BTreeMap<u64, BTreeSet<u64>>,
    /// Faulty reports staged since the node's last boot report: the scan
    /// speaks *before* [`Audit::recovered`] fires, so the swap-in happens
    /// there — the boot report is the incarnation edge.
    pub(super) faulty_staged: BTreeMap<u64, BTreeSet<u64>>,
    /// Repair progress observed (from [`Audit::repair_progress`]): in-place
    /// repairs, straggler Case-1 re-proposals, Case-2 no-op fills, and
    /// recovery-timeout resignations.
    pub(super) repaired_seen: bool,
    pub(super) case1_seen: bool,
    pub(super) case2_seen: bool,
    pub(super) repair_stepdown_seen: bool,
    pub(super) app_repair_seen: bool,
    pub(super) app_repair_below_floor_seen: bool,
    /// #101: decided snapshot points each node has durably recorded — the
    /// per-node custody facts the truncation-coupling check reads.
    pub(super) snap_points: BTreeMap<u64, BTreeSet<u64>>,
    pub(super) snap_recorded_seen: bool,
    pub(super) snap_chunks_reported_seen: bool,
    pub(super) snap_chunk_repaired_seen: bool,
    pub(super) snap_fallback_seen: bool,
    pub(super) snap_restore_seen: bool,
    pub(super) resend_skipped: bool,
    pub(super) resigned: bool,
    /// The `withhold_snap_chunk` hook family: it fired somewhere, the
    /// requesters it was silent toward, and whether one of them still
    /// completed its chunk repair — the recovery path the silence tests.
    pub(super) chunk_withheld: bool,
    pub(super) withheld_from: BTreeSet<u64>,
    pub(super) repaired_after_withhold: bool,
    /// Parked reads redirected: by the deadline, and by the early-expiry hook.
    pub(super) read_expired_overdue: bool,
    pub(super) read_expired_early: bool,
    /// A compaction ack lost at the reply seam (its own gate beside the
    /// redirect family: the compaction client's re-ask loop is a different
    /// recovery path from a blind retry after a lost redirect).
    pub(super) compact_reply_dropped: bool,
    /// Cooperative-handoff coverage: one sticky bit per distinct fact.
    pub(super) handoff_relinquished: bool,
    pub(super) handoff_installed: bool,
    /// The payoff: an installed authority streamed Phase 2 without any Phase 1
    /// of its own — the whole point of the `DPaxos` technique.
    pub(super) handoff_streamed_without_phase1: bool,
    /// A transfer carried unfinished business (an accepted-but-unchosen tail).
    pub(super) handoff_carried_tail: bool,
    /// Leadership was handed over more than once in this run. Distinct
    /// authorities: one authority is handed on at most once (see
    /// `RawNode::can_relinquish`'s *One hop only*).
    pub(super) handoff_repeated: bool,
    /// A refusal path fired: wrong addressee/non-member, stale authority, or a
    /// malformed tail.
    pub(super) handoff_refused_target: bool,
    pub(super) handoff_refused_stale: bool,
    pub(super) handoff_refused_shape: bool,
    pub(super) handoff_refused_unfit: bool,
    /// A handoff-installed leadership resigned on its uncovered inherited
    /// fence — the deliberate fallback to ordinary Phase 1.
    pub(super) handoff_fence_expired: bool,
    /// A relinquishment was lost at the send seam (the availability-only
    /// failure mode a handoff deliberately accepts).
    pub(super) dropped_relinquish: bool,
    pub(super) duplicated_relinquish: bool,
    pub(super) compact_ack_accepted: bool,
    pub(super) compact_ack_refused: bool,
    pub(super) mailbox_dropped: bool,
    pub(super) offer_skipped: bool,
    pub(super) shortest_timeout: bool,
    pub(super) dropped_accept: bool,
    pub(super) dropped_election: bool,
    pub(super) dropped_commit: bool,
    pub(super) dropped_accepted: bool,
    pub(super) dropped_heartbeat: bool,
    pub(super) dropped_repair: bool,
    pub(super) dropped_catchup_request: bool,
    pub(super) dropped_snap_ack: bool,
    pub(super) dropped_snap_chunk_request: bool,
    pub(super) dropped_snap_chunk_response: bool,
    pub(super) dropped_check_leader: bool,
    pub(super) crashed_after_apply: bool,
    pub(super) crashed_before_chunk_sync: bool,
    pub(super) crashed_after_chunk_restore: bool,
    pub(super) crashed_after_boot_replay: bool,
    /// Transport tallies for the failure print: sends per message kind,
    /// failed delivery RPCs, edge rejections.
    pub(super) sent_kinds: BTreeMap<&'static str, u64>,
    pub(super) delivery_failures: u64,
    pub(super) edge_rejections: u64,
    pub(super) delivery_failed: bool,
    pub(super) waiters_cleared: bool,
    pub(super) edge_rejected: bool,
    /// Chunk repairs the store refused after every write returned `Ok`, and
    /// the last point one was refused at — the dynamic context the reachable
    /// gate itself cannot carry (`assert_reachable!` takes only a message).
    pub(super) snap_chunks_rejected: u64,
    pub(super) snap_chunk_rejected_at: Option<u64>,
    pub(super) snap_chunk_rejected: bool,
    /// A matchmaker-plane reply the node loop folded twice, per kind
    /// (`Match`, `GcAck`, `MatchmakerReconfigure`).
    pub(super) reply_duplicated: [bool; 3],
    /// A `Retire` refused because no effective GC floor sat above the target's
    /// membership fence (#123's `not_collected` leg).
    pub(super) retire_not_collected: bool,
    /// A `Retire` refused because the target was the sitting leader.
    pub(super) retire_leader: bool,
    pub(super) redirect_dropped: bool,
    pub(super) duplicated_any: bool,
    pub(super) duplicated_quorum_kind: bool,
    pub(super) duplicated_commit: bool,
    pub(super) duplicated_repair: bool,
    pub(super) duplicated_catchup_request: bool,
    pub(super) duplicated_snap_ack: bool,
    pub(super) duplicated_snap_chunk_request: bool,
    pub(super) duplicated_snap_chunk_response: bool,
    pub(super) reply_dropped: bool,
    pub(super) propose_reply_dropped: bool,
    pub(super) read_reply_dropped: bool,
    pub(super) dedup_after_dropped_reply: bool,
    /// Reconfiguration coverage (#122): a request started / refused, a
    /// non-member declined to campaign, a removed leader resigned.
    pub(super) reconfigure_started: bool,
    pub(super) reconfigure_refused: bool,
    pub(super) non_member_campaign_skipped: bool,
    pub(super) non_member_leader_resigned: bool,
    /// A leadership under a configuration other than the bootstrap one: a
    /// reconfiguration went all the way through matchmaking and the
    /// cross-configuration Phase 1.
    pub(super) reconfiguration_completed: bool,
    pub(super) joined_member_accepted: bool,
    pub(super) removed_member_promised: bool,
    pub(super) cross_config_phase1_checked: bool,
}

impl AuditState {
    /// Bind `config` to `ballot` — once; a second binding must agree (a
    /// configuration is bound to a ballot and never edited).
    pub(super) fn bind_config(&mut self, ballot: Ballot, config: &AcceptorConfig) {
        let key = (ballot.round, ballot.node.0);
        let bound = self.configs.entry(key).or_insert_with(|| config.clone());
        assert_always!(
            *bound == *config,
            "a configuration is bound to a ballot and never edited",
            { "round" => ballot.round, "bnode" => ballot.node.0 }
        );
    }

    /// The acceptor configuration of `ballot`: the one bound to it, else the
    /// bootstrap membership (every ballot on plain Multi-Paxos).
    pub(super) fn config_of(&self, ballot: Ballot) -> Option<&AcceptorConfig> {
        self.configs
            .get(&(ballot.round, ballot.node.0))
            .or(self.bootstrap.as_ref())
    }

    /// Record the prior configurations `node`'s matchmaking for `ballot`
    /// closed with.
    pub(super) fn note_prior(&mut self, node: u64, ballot: Ballot, prior: &[AcceptorConfig]) {
        self.prior
            .insert((node, ballot.round, ballot.node.0), prior.to_vec());
    }

    /// The prior configurations (`H_b`) the owner of `ballot` closed its
    /// matchmaking with, if that phase ran (never on plain Multi-Paxos).
    fn prior_of(&self, ballot: Ballot) -> Option<&[AcceptorConfig]> {
        self.prior
            .get(&(ballot.node.0, ballot.round, ballot.node.0))
            .map(Vec::as_slice)
    }

    /// Fold one `Prepare` leaving `node` for `to` at `ballot` (#122): Phase 1
    /// fans out to the ballot's configuration and its prior configurations,
    /// and to nothing else — a node in neither has nothing to report and no
    /// ballot to learn.
    pub(super) fn observe_prepare_send(&mut self, node: u64, to: u64, ballot: Ballot) {
        let in_config = self
            .config_of(ballot)
            .is_some_and(|c| c.contains(paros::NodeId(to)));
        let prior = self.prior_of(ballot);
        if self.config_of(ballot).is_none() && prior.is_none() {
            return;
        }
        let in_prior = prior.is_some_and(|p| p.iter().any(|c| c.contains(paros::NodeId(to))));
        assert_always!(
            in_config || in_prior,
            "reconfiguration: a Prepare reaches only the ballot's configuration and its prior configurations",
            { "node" => node, "to" => to, "round" => ballot.round }
        );
    }

    /// Fold one `Promise` leaving `node` at `ballot`: the Phase-1 answer the
    /// ballot's leader may count, and — when the node sits outside the
    /// ballot's own configuration — the proof that a removed member keeps
    /// answering Phase 1 for the ballots it took part in.
    pub(super) fn observe_promise_send(&mut self, node: u64, ballot: Ballot) {
        self.promise_senders
            .entry((ballot.round, ballot.node.0))
            .or_default()
            .insert(node);
        if self
            .config_of(ballot)
            .is_some_and(|c| !c.contains(paros::NodeId(node)))
        {
            reach_once!(
                self.removed_member_promised,
                "reconfiguration: a node outside the ballot's configuration answers its Phase 1"
            );
        }
    }

    /// Fold one `Accept` leaving `node` for `to` at `ballot` (#121, #122),
    /// judged on the wire. Two claims: Phase 2 addresses only the ballot's
    /// own acceptors (a removed member is never asked to vote at a ballot it
    /// is not in), and it opens only once **every** prior configuration has
    /// a promise quorum for the ballot — counted per configuration over the
    /// promises that actually left the wire plus the owner's own vote, never
    /// over their union. The union rule would count here and be wrong: it is
    /// exactly what the negative core test refuses.
    pub(super) fn observe_accept_send(&mut self, node: u64, to: u64, ballot: Ballot) {
        let Some(config) = self.config_of(ballot).cloned() else {
            return;
        };
        assert_always!(
            config.contains(paros::NodeId(to)),
            "reconfiguration: an Accept reaches only the ballot's own acceptors",
            { "node" => node, "to" => to, "round" => ballot.round }
        );
        if self
            .bootstrap
            .as_ref()
            .is_some_and(|b| !b.contains(paros::NodeId(to)))
        {
            reach_once!(
                self.joined_member_accepted,
                "reconfiguration: a node outside the bootstrap configuration is asked to accept"
            );
        }
        let Some(prior) = self.prior_of(ballot) else {
            return;
        };
        // The promise quorum the candidate holds: every matchmaker-reported
        // promise sender, plus its own (a candidate promises itself). Judged
        // by each prior configuration's own quorum system — the same
        // predicate the core's Phase-1 completion asks — rather than
        // re-derived here as arithmetic; a sender outside a configuration
        // never counts toward it.
        let mut promised: BTreeSet<paros::NodeId> = self
            .promise_senders
            .get(&(ballot.round, ballot.node.0))
            .map(|s| s.iter().map(|n| paros::NodeId(*n)).collect())
            .unwrap_or_default();
        promised.insert(ballot.node);
        let uncovered = prior
            .iter()
            .filter(|c| !c.has_phase1_quorum(&promised))
            .count();
        assert_always!(
            uncovered == 0,
            "reconfiguration: no Accept leaves before every prior configuration promised a quorum",
            {
                "node" => node,
                "round" => ballot.round,
                "prior" => prior.len(),
                "uncovered" => uncovered
            }
        );
        reach_once!(
            self.cross_config_phase1_checked,
            "reconfiguration: an Accept is checked against every prior configuration's promises"
        );
    }

    /// The protocol-level `sometimes` gates: progress, truncation, snapshot and
    /// the multi-slot log. Their `reachable` counterparts already fired at their
    /// transition instants.
    pub(super) fn check_protocol_gates(&self) {
        let max_applied = self.cluster_applied_max.unwrap_or(0);
        // The log is multi-slot (a stable leader streamed past slot 0).
        assert_sometimes!(max_applied >= 2, "a multi-slot prefix is applied");
        assert_sometimes!(max_applied >= 3, "a stable leader streams several slots");
        assert_sometimes!(
            self.leader_rounds.len() >= 2,
            "leadership turns over and the cluster recovers"
        );
        assert_sometimes!(self.any_leader, "a leader is elected");
        assert_sometimes!(
            self.reconfiguration_completed,
            "reconfiguration: a leader is elected under a reconfigured acceptor set"
        );
        if self.config_tagged_protocol_message {
            assert_reachable!("a protocol message carries a configuration identity");
        }
        // The #67 check reads a promise and a won ballot; saturation has to see
        // it actually compare something.
        assert_sometimes!(
            self.leader_promise_checked,
            "a fresh leader's promise is checked against the ballot it won"
        );
        // The n>=5 shape, whose accept quorums can avoid a two-node pin, is
        // actually visited. Every node boots at start, so the booted set is
        // the drawn topology.
        let n = self.booted.len();
        if n >= 5 {
            assert_reachable!("a run drives a five-node cluster");
        }
        // Compaction actually happens (the workload drives it every run).
        assert_sometimes!(self.compacted, "the log is compacted (truncation happens)");
        // The #101 coupling's other half: compaction implies a decided
        // snapshot point was recorded first, so this saturates wherever the
        // compaction gate does.
        assert_sometimes!(
            self.snap_recorded_seen,
            "storage: a decided snapshot point is recorded"
        );
        assert_sometimes!(
            self.snapshot_installed,
            "a below-floor node recovers via snapshot transfer"
        );
        // The #88 mid-election install window is anchored by the `reach_once!`
        // in [`AuditWorld::snapshot_mid_election`], not demanded per sweep:
        // #101 made whole-blob installs structurally rare (a below-floor node
        // with a clean covering point restores locally, and rotted chunks
        // repair chunk-wise), so the install x live-election coincidence is a
        // leg the swarm is no longer *certain* to visit. Per the assertion
        // doctrine such a leg anchors exploration when hit and never fails
        // coverage (same shape as the block-fault family gate).
        // CheckQuorum (#95) is actually exercised: some seed isolates a leader
        // from its ack quorum long enough that it demotes itself (the n=2
        // regime plus attrition is the reliable generator).
        assert_sometimes!(
            self.quorum_lost,
            "a leader without an ack quorum steps down (CheckQuorum)"
        );
        // The Chain register's campaign gates: a client keeps committing after
        // a leader change, and compaction is asked for AND takes effect.
        assert_sometimes!(
            self.ack_after_leader_change,
            "chain: proposal succeeds after leader change"
        );
        assert_sometimes!(
            self.compact_ack_accepted && self.compacted,
            "chain: compact takes effect"
        );
        // Every way a sitting leader can stop being one — it resigned, it
        // crashed, or it cooperatively handed its authority on — followed by a
        // new leader and a client ack under it.
        assert_sometimes_all!(
            "chain: failover completed",
            [
                (
                    "old leader gone",
                    self.resigned || self.crashed_any || self.handoff_relinquished
                ),
                ("new leader elected", self.leader_rounds.len() >= 2),
                ("client acknowledged", self.ack_after_leader_change),
            ]
        );
    }

    /// The driver's durability seams and rare-but-valid policy decisions are
    /// actually taken on some seeds. Asserts no new safety property; it proves
    /// the hooks are still connected, since perturbations that stopped firing
    /// would leave a sweep looking green while quietly testing less.
    pub(super) fn check_driver_hook_gates(&self) {
        if self.crashed_after_sync {
            assert_reachable!("the driver crashes after sync and before sending a batch");
        }
        if self.crashed_before_sync {
            assert_reachable!("the driver crashes before syncing a staged batch");
        }
        assert_sometimes!(
            self.snapshot_offered,
            "a snapshot offer enters the driver's common outbound path"
        );
        if self.shortest_timeout {
            assert_reachable!("the driver selects the shortest valid election timeout");
        }
        if self.resend_skipped {
            assert_reachable!("the driver skips a pending accept re-send");
        }
        if self.resigned {
            assert_reachable!("the driver voluntarily resigns leadership");
        }
        if self.dropped_accept {
            assert_reachable!("the driver drops one isolated accept at the send seam");
        }
        if self.dropped_election {
            assert_reachable!("the driver drops an election message at the send seam");
        }
        if self.dropped_commit {
            assert_reachable!("the driver drops a commit at the send seam");
        }
        if self.dropped_accepted {
            assert_reachable!("the driver drops an accepted ack at the send seam");
        }
        if self.dropped_heartbeat {
            assert_reachable!("the driver drops a heartbeat at the send seam");
        }
        if self.dropped_repair {
            assert_reachable!("the driver drops a repair message at the send seam");
        }
        if self.crashed_after_apply {
            assert_reachable!(
                "the driver crashes after applying a batch and before its application fsync"
            );
        }
        if self.duplicated_any {
            assert_reachable!("the driver duplicates a message at the send seam");
        }
        if self.duplicated_quorum_kind {
            assert_reachable!("the driver duplicates a quorum-counting message at the send seam");
        }
        if self.duplicated_commit {
            assert_reachable!("the driver duplicates a commit at the send seam");
        }
        if self.duplicated_repair {
            assert_reachable!("the driver duplicates a repair message at the send seam");
        }
        if self.reply_dropped {
            assert_reachable!("a committed client reply is dropped at the reply seam");
        }
        assert_sometimes!(
            self.dedup_after_dropped_reply,
            "a committed proposal ack is lost and the retry takes the dedup path"
        );
        self.check_handoff_gates();
    }

    /// The cooperative-handoff coverage gates.
    ///
    /// Split by *what each one proves* rather than lumped into one "handoff
    /// happened" bit: a campaign that only ever transfers settled leaderships,
    /// or only ever completes them, would saturate a single gate while leaving
    /// the interesting halves of the design — the inherited tail, the refusal
    /// paths, the fallback to Phase 1 — entirely unexercised.
    ///
    /// Only the facts a campaign is *certain* to reach are `sometimes`; the rest
    /// stay `reachable`-only, which creates no slot when unreached and so can
    /// never fail coverage.
    ///
    /// The line is drawn by what a handoff is conditioned on. Relinquishing,
    /// installing, streaming under the inherited ballot and carrying a tail all
    /// follow from a single handoff happening at all, so a campaign that ever
    /// hands leadership over hits every one of them. Everything else needs a
    /// handoff *and* a second rare event — a duplicate or a drop of that exact
    /// message, a superseding election landing inside the window, a second
    /// handoff in the same run, a payload damaged in flight, a successor that
    /// happens to hold faulty records. Gating every seed on a conjunction of
    /// two rare draws is what makes a sweep spend its whole seed budget chasing
    /// one bit, so those are recorded when they happen and never demanded.
    pub(super) fn check_handoff_gates(&self) {
        if self.handoff_relinquished {
            assert_reachable!("a leader cooperatively hands its authority on");
        }
        assert_sometimes!(
            self.handoff_installed,
            "a successor installs a transferred authority"
        );
        assert_sometimes!(
            self.handoff_streamed_without_phase1,
            "a handed-over authority continues Phase 2 without another Phase 1"
        );
        if self.handoff_carried_tail {
            assert_reachable!("a handoff carries accepted-but-unchosen work");
        }
    }

    /// A node's promised ballot is monotonic — it never decreases, including
    /// across a restart (the boot re-reports the recovered promise).
    ///
    /// The cross-restart half is the load-bearing one, and it is the only
    /// oracle a lost *disk* cannot evade: `set_promise`'s in-core assert lives
    /// behind the storage record an amnesiac node no longer has. It was proven
    /// so by mutation — wiping one node's disk after it raised its promise and
    /// letting it rejoin **naively**, as itself, turned this assertion red.
    /// That is CTRL's takedown of Google's `MarkNonVoting`: a node that lost
    /// its promise can accept from an old leader while the new leader still
    /// counts that promise, and a chosen value is overwritten. It is why
    /// `prob_wipe` stays 0 on every campaign — a snapshot restores the log, not
    /// the promise, and restoring redundancy is node replacement (#22's
    /// reconfiguration), never a rejoin.
    pub(super) fn observe_promise(&mut self, node: u64, ballot: Ballot) {
        if let Some(prev) = self.promised.insert(node, ballot) {
            assert_always!(ballot >= prev, "a node's promised ballot never decreases");
        }
    }

    /// Fold one durable accept into the acceptor tally and run the
    /// quorum-decided oracle. Fed by the live accept fold *and* the boot
    /// re-reports (a `BTreeSet` makes the re-fold idempotent), so a value a
    /// majority durably accepted is **decided** here even when no node ever
    /// applies it — the case a buggy leader's later no-op fill would
    /// otherwise hide from the apply-fed `chosen` map. Quorum arithmetic uses
    /// the *configured* cluster size from the boot reports, never the booted
    /// subset (which under-counts while nodes are still coming up).
    pub(super) fn observe_durable_accept(
        &mut self,
        node: u64,
        slot: u64,
        ballot: Ballot,
        vhash: u64,
    ) {
        let key = (slot, ballot.round, ballot.node.0);
        if let Some(prev) = self.accepted.insert(key, vhash) {
            assert_always!(
                prev == vhash,
                "at most one command is ever accepted for one (slot, ballot)"
            );
        }
        // P2, observed on durable state: once a slot is decided at some
        // ballot, every accept at or above that ballot carries the decided
        // value (a proposer above it must have adopted it via P2c).
        if let Some(&(round, bnode, decided_vhash)) = self.decided.get(&slot)
            && (ballot.round, ballot.node.0) >= (round, bnode)
        {
            assert_always!(
                vhash == decided_vhash,
                "an accept at or above a decided ballot carries the decided value",
                {
                    "node" => node,
                    "slot" => slot,
                    "round" => ballot.round,
                    "decided_round" => round
                }
            );
        }
        // The tally is counted over the ballot's *own* configuration — a
        // learner outside it (a spare, a removed member replaying a commit)
        // holds the same bytes but casts no vote (#122).
        let config = self.config_of(ballot).cloned();
        let holders = self.accept_sets.entry(key).or_default();
        holders.insert(node);
        let voters: BTreeSet<paros::NodeId> = holders.iter().map(|n| paros::NodeId(*n)).collect();
        if config
            .as_ref()
            .is_some_and(|c| c.has_phase2_quorum(&voters))
        {
            match self.decided.get(&slot) {
                None => {
                    self.decided
                        .insert(slot, (ballot.round, ballot.node.0, vhash));
                    self.decided_max = Some(self.decided_max.map_or(slot, |m| m.max(slot)));
                }
                // Two quorums (at any two ballots) must agree — the crown
                // jewel judged on durable accepts alone, with no apply in the
                // loop. The first decision wins the recorded ballot.
                Some(&(_, _, decided_vhash)) => {
                    assert_always!(
                        vhash == decided_vhash,
                        "a durable accept quorum never decides two values for a slot",
                        { "node" => node, "slot" => slot, "round" => ballot.round }
                    );
                }
            }
        }
    }

    /// The lowest compaction floor across the cluster: everything below it is
    /// truncated *everywhere*, so the per-slot safety tallies can be pruned.
    pub(super) fn cluster_min_floor(&self) -> u64 {
        self.booted
            .iter()
            .map(|node| self.floor.get(node).map_or(0, |f| f.now))
            .min()
            .unwrap_or(0)
    }

    /// Fold one broadcast leader beat (#95). A leader beating at a ballot that
    /// a **promise-majority** has durably promised strictly past is deposed for
    /// good: an acceptor only acks a beat at or above its promise, so at most a
    /// minority can ever ack this ballot again, and no round it starts can
    /// decide. Zombie-ness is a *bounded-liveness* claim — `CheckQuorum` demotes
    /// a leader that spends a full election-timeout window without an ack
    /// quorum, partition or not — so the streak needs no quiescence gate. It is
    /// measured in **ticks against the node's own election timeout**
    /// ([`Self::observe_tick`]), never in beats against a fixed count: the
    /// window is exactly one timeout long, the timeout is a per-seed knob with
    /// a structural floor (a 10 ms tick raises it to 25–49 ticks), and a
    /// client's `read_index` beats add beats per tick — a fixed budget of 40
    /// beats went red on a seed (18268997339215266796) where node 0 learned it
    /// was deposed only at the end of a 49-tick window, with no protocol fault
    /// anywhere.
    pub(super) fn observe_beat(&mut self, node: u64, ballot: Ballot, seq: u64) {
        // A promise-majority *of the ballot's own configuration*: only its
        // members' promises decide whether the leader can still assemble a
        // quorum at that ballot.
        let Some(config) = self.config_of(ballot).cloned() else {
            return;
        };
        let above: BTreeSet<paros::NodeId> = self
            .promised
            .iter()
            .filter(|(_, p)| **p > ballot)
            .map(|(n, _)| paros::NodeId(*n))
            .collect();
        let outvoted = config.has_phase1_quorum(&above);
        let entry = self.deposed_streaks.entry(node).or_default();
        if entry.round != ballot.round || entry.node != ballot.node.0 {
            *entry = DeposedStreak {
                round: ballot.round,
                node: ballot.node.0,
                seq,
                deposed: false,
                ticks: 0,
                beatless_ticks: 0,
            };
        } else if entry.seq == seq {
            return;
        }
        entry.seq = seq;
        entry.beatless_ticks = 0;
        if outvoted {
            entry.deposed = true;
        } else {
            entry.deposed = false;
            entry.ticks = 0;
        }
    }

    /// One logical tick at `node`: a deposed leader's clock runs, and it must
    /// step down within two `CheckQuorum` windows (see [`Self::observe_beat`]).
    pub(super) fn observe_tick(&mut self, node: u64) {
        let Some(entry) = self.deposed_streaks.get_mut(&node) else {
            return;
        };
        entry.beatless_ticks += 1;
        if entry.beatless_ticks > HEARTBEAT_TICKS {
            // More than one whole beat period without a beat: the leadership
            // is over, so the streak is closed. Exactly one period of silence
            // is tolerated because a beat can be lost at the send seam
            // without the audit ever seeing it.
            self.deposed_streaks.remove(&node);
            return;
        }
        if !entry.deposed {
            return;
        }
        entry.ticks += 1;
        let timeout = self.election_timeouts.get(&node).copied().unwrap_or(0);
        let budget = timeout.saturating_mul(2).saturating_add(DEPOSED_TICK_SLACK);
        let streak = entry.ticks;
        let round = entry.round;
        assert_always!(
            streak <= budget,
            "a leader deposed by a promise-majority stops beating within an election timeout (CheckQuorum)",
            {
                "node" => node,
                "round" => round,
                "streak_ticks" => streak,
                "timeout_ticks" => timeout
            }
        );
    }

    /// Fold one authority actually changing hands, at the **transmit** instant.
    ///
    /// Deliberately not at the core call that decided it: the abdicating batch
    /// may still have `Accept`s queued ahead of this message, and those were
    /// proposed while the node genuinely held the authority. Here the ordering
    /// is exact — every earlier message of the batch has already been reported,
    /// and no successor can install a message it has not yet received.
    ///
    /// Idempotent, because the send seam deliberately duplicates messages: a
    /// re-transmit simply re-applies the same retirement.
    pub(super) fn observe_authority_release(&mut self, from: u64, ballot: Ballot, next_slot: Slot) {
        let entry = self
            .authorities
            .entry((ballot.round, ballot.node.0))
            .or_default();
        assert_always!(
            entry.holder.is_none_or(|held| held == from),
            "only the node exercising an authority relinquishes it",
            { "node" => from, "holder" => entry.holder.unwrap_or(u64::MAX) }
        );
        // The allocator frontier only ever moves forward: a rewind is how one
        // `(slot, ballot)` ends up carrying two different commands.
        assert_always!(
            next_slot.0 >= entry.frontier,
            "a transferred allocator frontier never rewinds",
            {
                "node" => from,
                "frontier" => next_slot.0,
                "previous" => entry.frontier
            }
        );
        entry.frontier = next_slot.0;
        entry.retired.insert(from);
        entry.holder = None;
    }

    /// Fold one observed exercise of a logical authority: `node` put an
    /// `Accept` at `ballot` on the wire.
    ///
    /// This is where **authority uniqueness** — the `DPaxos` handoff's central
    /// safety rule — is checked, and it is checked against what the
    /// cluster can actually observe (a proposal on the wire), never against a
    /// node's own `role` flag. Two nodes exercising one ballot for overlapping
    /// slots is exactly how two different values get chosen for one slot, and
    /// the sibling check in [`NodeAudit::sent`] ("one ballot proposes at most
    /// one command for a slot") is the consequence this exists to prevent
    /// upstream of.
    pub(super) fn observe_authority_use(&mut self, node: u64, ballot: Ballot) {
        let key = (ballot.round, ballot.node.0);
        // A node proposing under a ballot that names *someone else* can only
        // have got there through a handoff: a `Prepare` is honored solely when
        // the ballot names its sender, so no Phase 1 at this ballot is even
        // expressible here. Read straight off the wire, with no bookkeeping to
        // race against.
        let inherited = node != ballot.node.0;
        let entry = self.authorities.entry(key).or_default();
        assert_always!(
            !entry.retired.contains(&node),
            "a relinquished authority is never exercised again",
            { "node" => node, "round" => ballot.round, "bnode" => ballot.node.0 }
        );
        let previous = entry.holder;
        entry.holder = Some(node);
        assert_always!(
            previous.is_none_or(|held| held == node),
            "one physical node at a time exercises a logical Paxos authority",
            {
                "node" => node,
                "previous" => previous.unwrap_or(u64::MAX),
                "round" => ballot.round,
                "bnode" => ballot.node.0
            }
        );
        if inherited {
            // The payoff, observed rather than assumed: this node acquired the
            // ballot from a predecessor and is now streaming Phase 2 under it.
            reach_once!(
                self.handoff_streamed_without_phase1,
                "an inherited authority streams Phase 2 with no Phase 1 of its own"
            );
        }
    }

    /// Fold one applied index into the per-node prefix, the no-gaps frontier and
    /// the cluster high-water mark.
    pub(super) fn observe_applied_index(&mut self, node: u64, idx: u64) {
        self.check_no_gaps(node, idx);
        if idx >= 2 {
            reach_once!(
                self.multi_slot_applied,
                "a multi-slot log prefix is applied"
            );
        }
        if idx >= 3 {
            reach_once!(
                self.several_slots_applied,
                "the chosen prefix advances under a stable leader"
            );
        }
        if self.cluster_applied_max.is_none_or(|m| idx > m) {
            self.cluster_applied_max = Some(idx);
        }
        let prefix = self.applied_max.entry(node).or_insert(0);
        *prefix = (*prefix).max(idx);
        for (&n, &nm) in &self.applied_max {
            if Some(nm) < self.cluster_applied_max {
                self.lagged.insert(n);
            }
        }
        // `caught_up` is judged in `check_final_convergence` against the FINAL
        // cluster maximum: a node that transiently matched a max the cluster
        // immediately moved past is not evidence the catch-up path healed it.
    }

    /// A node's applied (contiguous chosen) prefix advances one slot at a time.
    /// A *replay* of an already-applied slot after a restart is idempotent and
    /// allowed; only a forward skip past the frontier is a real gap, and that is
    /// legal only at the node's compaction floor (a truncated log's boot replay
    /// resumes there) or at a snapshot install.
    pub(super) fn check_no_gaps(&mut self, node: u64, idx: u64) {
        let at_floor = idx == self.floor.get(&node).map_or(0, |f| f.now);
        let at_snapshot = self
            .snap_landings
            .get(&node)
            .is_some_and(|landings| landings.contains(&idx));
        let next_now = self.frontier.get(&node).copied().unwrap_or(0);
        // Stage 8: a boot replay may step over a rotted record whose effect is
        // already durable in the application state — legal only when every
        // skipped slot was reported faulty by this node (the explained jump).
        let over_reported = idx > next_now
            && self
                .reported_faulty
                .get(&node)
                .is_some_and(|slots| (next_now..idx).all(|s| slots.contains(&s)));
        let next = self.frontier.entry(node).or_insert(0);
        if idx == *next {
            *next += 1;
        } else if idx > *next {
            *next = idx + 1;
            assert_always!(
                at_floor || at_snapshot || over_reported,
                "a node's applied prefix advances one slot at a time (a forward jump only at the compaction floor or a snapshot install)",
                { "node" => node, "index" => idx }
            );
        }
    }

    /// The client-visible checks over the merged history (see [`LinHistory`]).
    pub(super) fn check_client_history(&self) {
        let h = &self.lin;
        // A terminal event is only ever recorded for an op that was issued.
        assert_always!(
            h.acked + h.failed <= h.issued,
            "no proposal is acked/failed before it is issued"
        );
        // A committed ack is a promise the command is in the applied log: the
        // audit folded exactly that identity at exactly that slot.
        for (&(client, seq), &slot) in &h.write_slot {
            let applied_at = self.applied_identity.get(&(client, seq)).copied();
            assert_always!(
                applied_at == Some(slot),
                "chain: every acknowledged command was applied",
                {
                    "client" => client,
                    "seq" => seq,
                    "acked_slot" => slot,
                    "applied_at" => applied_at.map_or(-1_i64, |s| i64::try_from(s).unwrap_or(i64::MAX))
                }
            );
        }
        assert_always!(
            h.read_acked + h.read_failed <= h.read_issued,
            "no read is acked/failed before it is issued"
        );
        // With no chaos a proposal does come back — a "sometimes" + "reachable".
        assert_sometimes!(h.acked > 0, "at least one proposal is acknowledged");
        if h.acked > 0 {
            assert_reachable!("a client proposal is acknowledged");
        }
        check_disclosed_order(h);
        // The sequential fast path, per client: every client runs one operation
        // at a time (a primer batch completes before the next op starts), so
        // program order is real-time order within a client even where
        // timestamps tie, and C1-C3 are strictly stronger than L1-L4 there.
        let committed_clients: BTreeSet<u64> = h
            .write_slot
            .keys()
            .chain(h.read_wm.keys())
            .map(|&(c, _)| c)
            .collect();
        for &client in &committed_clients {
            check_sequential_client(client, h);
        }
        h.check_coverage_gates(&committed_clients, self.leader_change_ms);
    }
}
