//! The folded facts of one run and the per-transition checks over them.
//!
//! Every field is one incremental fact the checks need, and nothing else. The
//! trailing block of `AuditState` is a *flag set*, not a state machine: one
//! independent sticky bit per `reachable` gate, each flipped once at its own
//! transition and read once at `check()`.

use std::collections::{BTreeMap, BTreeSet};

use moonpool_sim::{assert_always, assert_reachable, assert_sometimes, assert_sometimes_all};
use paros::{AcceptorConfig, Ballot, HEARTBEAT_TICKS, Party, QuorumSystem, Slot};

use super::client::{LinHistory, check_disclosed_order, check_sequential_client};
use super::matchmaker::MatchmakerAudit;

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
    /// The election timeout (the `CheckQuorum` window, in ticks) the leader
    /// was running under at its last beat. Read at the beat, never at the
    /// tick: a leader never redraws its timeout, but the step-down that ends
    /// the streak draws a fresh *follower* timeout in the same driver tick,
    /// and it is reported (`election_timeout_set`) before that tick's
    /// `ticked` — so a budget read from the live map at the tick would judge
    /// the leader's windows by a timeout it never ran under (a leader whose
    /// timeout was 8 stepped down within 15 ticks — two windows and the
    /// slack — and was measured against the 5 it drew as it stepped down).
    pub(super) timeout: u64,
}

/// One node's `Ready`-batch counter, as the audit can see batch boundaries.
///
/// A `Promise` is built at the step that promised, but its batch's durable
/// writes are reported *before* it leaves — including records the same
/// batch accepted or learned **after** the Promise was built (a `Commit`
/// stepped behind the `Prepare` records a lower-ballot value the page could
/// not have shown). So a record is only held against a Promise when it
/// became durable in an **earlier** batch, which was in memory before any
/// step of this one. The epoch moves strictly between batches: at the first
/// durable report after a send (a drain persists everything before it sends
/// anything), and at every tick (the loop ticks between drains, never inside
/// one). Missing a boundary only makes the check skip more; it never makes
/// it judge a record the Promise could not have seen.
#[derive(Clone, Copy, Default)]
pub(super) struct BatchEpoch {
    pub(super) epoch: u64,
    pub(super) sent_since_write: bool,
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
    /// Per slot pruned from `decided` below the cluster-wide floor: the
    /// vhash the durable-accept quorum decided — the **consensus witness** a
    /// late `Commit` for a compacted slot is judged against. Never the
    /// apply-fed `chosen` map: a #94 re-chosen identity legitimately
    /// applies as a `Noop` everywhere while the command consensus decided,
    /// and therefore the command its `Commit` honestly carries, is the
    /// original user command — the two are distinct facts, and a proxy's
    /// `Commit` for delayed votes trailing the whole cluster's truncation
    /// carries the decided one. One `u64` per pruned slot (`decided`'s
    /// tally, `accept_sets`, is what pruning reclaims).
    pub(super) decided_below_floor: BTreeMap<u64, u64>,
    /// The highest slot ever quorum-decided — a monotone scalar the
    /// below-floor pruning of `decided` never lowers, so the cross-restart
    /// frontier check stays sound after the whole prefix compacts away.
    pub(super) decided_max: Option<u64>,
    /// Per `(node, slot)`: the accepted record the node holds durably *in its
    /// current incarnation* — `(ballot, vhash, batch epoch)`. Unlike
    /// `persisted` it is reset to exactly the read-back at every boot
    /// report (a record lost to a torn tail or a detected corruption is not
    /// something a later Promise can be asked for) and pruned at every
    /// truncation, so it is the ground truth a `Promise` page is judged
    /// against. The epoch says which `Ready` batch made it durable (see
    /// [`BatchEpoch`]).
    pub(super) durable_log: BTreeMap<(u64, u64), (Ballot, u64, u64)>,
    /// Per node: the batch-epoch counter [`Self::durable_log`] stamps with.
    pub(super) batch_epochs: BTreeMap<u64, BatchEpoch>,
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
    /// Nodes whose disk was wiped at a restart (#124): the identity is gone
    /// for good and excused from convergence. Fed by the driver's own
    /// refusal to boot the amnesiac store (`Audit::boot_refused`, #147) —
    /// a library decision the harness only cross-checks against its coin.
    pub(super) wiped: BTreeSet<u64>,
    /// Nodes that shut down on an operator's retirement (#123): reported by
    /// the driver at the instant they exit, or by a boot that found the
    /// identity retired.
    pub(super) retired: BTreeSet<u64>,
    pub(super) wiped_any: bool,
    /// The library refused an amnesiac member (#147).
    pub(super) amnesia_refused: bool,
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
    /// `ColocatedNode::can_relinquish`'s *One hop only*).
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
    pub(super) crashed_after_apply: bool,
    pub(super) crashed_before_chunk_sync: bool,
    pub(super) crashed_after_chunk_restore: bool,
    pub(super) crashed_after_boot_replay: bool,
    /// The three before-fsync seams split out of `BeforeSync`: a first
    /// boot's format marker, a snapshot install, a deferred floor raise.
    pub(super) crashed_before_format_sync: bool,
    pub(super) crashed_before_install_sync: bool,
    pub(super) crashed_before_truncate_sync: bool,
    /// Identities whose first boot crashed at `FormatBeforeSync` and have not
    /// formatted since: the next `store_formatted` for one of them is the
    /// fresh first boot the seam must lead to.
    pub(super) format_interrupted: BTreeSet<u64>,
    /// Every identity whose store was formatted durably — once each, ever: a
    /// second format means the provisioning ledger forgot a durable marker.
    pub(super) formatted: BTreeSet<u64>,
    pub(super) format_rebooted_fresh: bool,
    /// The proxy paths of the send seam's drop and duplicate locations
    /// (#142): a delegation, a proxy's fan-out copy, an `Accepted` to a proxy.
    pub(super) dropped_delegation: bool,
    pub(super) dropped_fan_out: bool,
    pub(super) dropped_accepted_to_proxy: bool,
    pub(super) duplicated_delegation: bool,
    pub(super) duplicated_fan_out: bool,
    pub(super) duplicated_accept: bool,
    pub(super) duplicated_prepare: bool,
    pub(super) duplicated_nack: bool,
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
    /// Flexible-quorum coverage (#140): a leadership ran under a flexible
    /// split, and a slot was decided by an accept set that is not a majority
    /// of its configuration — the two outcomes that prove `q2 < ⌊n/2⌋ + 1`
    /// genuinely ran rather than merely being drawn.
    pub(super) elected_flexible: bool,
    pub(super) decided_below_majority: bool,
    /// Grid coverage (#141): a slot decided on a column, an election covered
    /// by a row, one covered by a row *across* a reconfiguration (a grid in
    /// `H_b`), a read-index round confirmed by a column, a reconfiguration
    /// between a grid and a majority configuration — the outcomes that prove
    /// the grid genuinely ran, and a slot decided on a column other than its
    /// own (the driver's override took effect).
    pub(super) decided_on_column: bool,
    pub(super) decided_off_column: bool,
    pub(super) elected_grid: bool,
    pub(super) elected_across_grid: bool,
    pub(super) read_confirmed_on_column: bool,
    pub(super) reconfigured_across_grid_boundary: bool,
    pub(super) joined_member_accepted: bool,
    pub(super) removed_member_promised: bool,
    pub(super) cross_config_phase1_checked: bool,

    // --- proxy leaders (#142) ---------------------------------------------
    /// Per delegated round `(slot, ballot round, ballot node)`: every leader
    /// a proxy's fan-out named as the hint — more than one means a handoff
    /// successor re-delegated the round and the proxy refreshed the hint.
    pub(super) fanout_leaders: BTreeMap<(u64, u64, u64), BTreeSet<u64>>,
    /// The two outcomes the campaign must reach: a slot decided by a proxy's
    /// `Commit`, and a delegated round taken back by its leader.
    pub(super) decided_through_proxy: bool,
    pub(super) delegation_taken_back: bool,
    /// The causes, recorded when they fire: a proxied round outliving a
    /// handoff, a skipped re-fan-out beat, an unanswered round evicted.
    pub(super) proxied_round_survived_handoff: bool,
    pub(super) proxy_resend_skipped: bool,
    pub(super) proxy_round_expired: bool,
    /// Per proxy, over its current incarnation (reset at every boot): the
    /// highest ballot a delegation it acted on (opened or re-fanned-out)
    /// carried — the leadership it works for.
    pub(super) proxy_works_for: BTreeMap<u64, Ballot>,
    /// A leadership ran under a configuration that no longer names an
    /// identity the library refused as amnesiac (#124, #147): the wiped
    /// member was genuinely moved out by a reconfiguration.
    pub(super) wiped_replaced: bool,
    /// A completed matchmaking was judged against a lower ballot that had
    /// already put an `Accept` on the wire above the campaign's watermark —
    /// the ground-truth `H_b` check was not vacuous.
    pub(super) prior_ground_truth_checked: bool,
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
    pub(super) fn prior_of(&self, ballot: Ballot) -> Option<&[AcceptorConfig]> {
        self.prior
            .get(&(ballot.node.0, ballot.round, ballot.node.0))
            .map(Vec::as_slice)
    }

    /// **`H_b` is complete against ground truth** (#120, #122), judged at the
    /// matchmaking → Phase 1 boundary: every ballot `b'` in
    /// `[watermark, ballot)` that has already put an `Accept` on the wire —
    /// the ballots whose Phase-2 quorums may have chosen something — has its
    /// configuration among the priors the campaign closed with. The
    /// reply-side check (`check_folded_union`) proves the priors are the
    /// union of what the candidate *was told*; this one proves what it was
    /// told is what is *true*.
    ///
    /// Why it must hold: `b'` exercised Phase 2 only after its own
    /// matchmaking closed at a quorum of durable registrations, and any
    /// quorum this campaign folded intersects it at a matchmaker that
    /// either registered `b'` before answering this campaign (so the answer
    /// names it, being at or above the maximum watermark the campaign
    /// filters by) or registered this larger ballot first and refused `b'`
    /// (registration is monotone) — impossible, since `b'` closed. Across a
    /// generation change the freeze quorum intersects `b'`'s registration
    /// quorum the same way (a frozen matchmaker registers nothing), and the
    /// reconstruction carries it. "Above the watermark" is exactly the
    /// protocol's filter: `b' >= watermark`, the maximum GC watermark the
    /// folded replies reported.
    pub(super) fn check_prior_covers_phase2(
        &mut self,
        node: u64,
        ballot: Ballot,
        prior: &[AcceptorConfig],
        watermark: Ballot,
    ) {
        let lo = (watermark.round, watermark.node.0);
        let hi = (ballot.round, ballot.node.0);
        if lo >= hi {
            return;
        }
        let mut judged: u64 = 0;
        let mut uncovered: u64 = 0;
        let mut first_uncovered = (0, 0);
        for key in self.authorities.range(lo..hi).map(|(k, _)| *k) {
            let Some(config) = self.configs.get(&key) else {
                continue;
            };
            judged += 1;
            if !prior.contains(config) {
                if uncovered == 0 {
                    first_uncovered = key;
                }
                uncovered += 1;
            }
        }
        assert_always!(
            uncovered == 0,
            "matchmaking: H_b covers every lower ballot that ran Phase 2 above the watermark",
            {
                "node" => node,
                "round" => ballot.round,
                "watermark_round" => watermark.round,
                "judged" => judged,
                "uncovered" => uncovered,
                "first_uncovered_round" => first_uncovered.0,
                "first_uncovered_bnode" => first_uncovered.1
            }
        );
        if judged > 0 {
            reach_once!(
                self.prior_ground_truth_checked,
                "matchmaking: H_b is checked against a lower ballot that ran Phase 2"
            );
        }
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

    /// The Phase-2 half of P2b, checked **on the wire** whoever sends: a
    /// ballot names its own proposer, so exactly one leadership ever
    /// proposes at it, and two different commands under one `(ballot,
    /// slot)` mean the proposer allocated a slot it already had in flight.
    /// Fed by the leader's colocated `Accept`s, its delegations to a proxy
    /// (#142) and the proxy's fan-outs alike — a proxy carries the leader's
    /// command verbatim, so its fan-out is judged against the very same
    /// record. Reading the send rather than the receive is deliberate — it
    /// indicts the proposer, not the network — and it is the only place the
    /// anomaly is visible: an accept quorum may reject it, leaving no durable
    /// trace at all.
    pub(super) fn observe_proposal(&mut self, from: Party, ballot: Ballot, slot: u64, vhash: u64) {
        self.any_proposal_checked = true;
        if let Some(prev) = self
            .proposed
            .insert((ballot.round, ballot.node.0, slot), vhash)
        {
            assert_always!(
                prev == vhash,
                "one ballot proposes at most one command for a slot",
                { "from" => from.to_string(), "slot" => slot, "round" => ballot.round }
            );
        }
    }

    /// Fold one `Accept` leaving `from` (a leader, or a proxy fanning a
    /// leader's round out) for `to` at `ballot` (#121, #122), judged on the
    /// wire. Two claims: Phase 2 addresses only the ballot's own acceptors
    /// (a removed member is never asked to vote at a ballot it is not in),
    /// and it opens only once **every** prior configuration has a promise
    /// quorum for the ballot — counted per configuration over the promises
    /// that actually left the wire plus the owner's own vote, never over
    /// their union. The union rule would count here and be wrong: it is
    /// exactly what the negative core test refuses.
    pub(super) fn observe_accept_send(&mut self, from: Party, to: u64, ballot: Ballot) {
        let Some(config) = self.config_of(ballot).cloned() else {
            return;
        };
        assert_always!(
            config.contains(paros::NodeId(to)),
            "reconfiguration: an Accept reaches only the ballot's own acceptors",
            { "from" => from.to_string(), "to" => to, "round" => ballot.round }
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
                "from" => from.to_string(),
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
        assert_sometimes!(
            self.elected_flexible,
            "flexible: an election completes under a flexible quorum system"
        );
        assert_sometimes!(
            self.decided_below_majority,
            "flexible: a slot is decided by fewer accepts than a majority"
        );
        assert_sometimes!(
            self.decided_on_column,
            "grid: a slot is decided on a column"
        );
        assert_sometimes!(self.elected_grid, "grid: an election is covered by a row");
        assert_sometimes!(
            self.elected_across_grid,
            "grid: an election is covered by a row across a reconfiguration"
        );
        assert_sometimes!(
            self.read_confirmed_on_column,
            "grid: a read-index round is confirmed by a column"
        );
        assert_sometimes!(
            self.reconfigured_across_grid_boundary,
            "grid: a reconfiguration moves between a grid and a majority"
        );
        // The hook's outcome: a reachable, since whether a seed draws the
        // override is the swarm's business.
        if self.decided_off_column {
            assert_reachable!("grid: a slot is decided on a column other than its own");
        }
        // The proxy outcomes (#142): a seed with proxies delegates every
        // settled proposal, so a sweep decides through a proxy; and the
        // take-back — the leader's liveness under a dead or slow proxy —
        // fires wherever a proxy's `Commit` is late by the budget, which a
        // killed proxy, a lost delegation or the budget's own floor
        // produces.
        assert_sometimes!(
            self.decided_through_proxy,
            "proxy: a slot is decided through a proxy leader"
        );
        assert_sometimes!(
            self.delegation_taken_back,
            "proxy: a leader takes a delegated round back"
        );
        // The #67 check reads a promise and a won ballot; saturation has to see
        // it actually compare something.
        assert_sometimes!(
            self.leader_promise_checked,
            "a fresh leader's promise is checked against the ballot it won"
        );
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

    /// The driver-hook outcomes a sweep must be proven to reach. The hooks'
    /// own firings (a seam crash, a dropped or duplicated message, a skipped
    /// re-send, a resignation) are recorded as `reachable` at the transition
    /// that observes them, in [`super::NodeAudit`]; only the *outcomes* that
    /// need the whole run to judge live here.
    pub(super) fn check_driver_hook_gates(&self) {
        assert_sometimes!(
            self.snapshot_offered,
            "a snapshot offer enters the driver's common outbound path"
        );
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
    /// stay `reachable`-only at their transitions in [`super::NodeAudit`], which
    /// creates no slot when unreached and so can never fail coverage.
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
        assert_sometimes!(
            self.handoff_installed,
            "a successor installs a transferred authority"
        );
        assert_sometimes!(
            self.handoff_streamed_without_phase1,
            "a handed-over authority continues Phase 2 without another Phase 1"
        );
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
                    // The accept set that first made the decision is a Phase-2
                    // quorum of the configuration; under a flexible split (or
                    // a grid column) it may be smaller than a majority of it,
                    // which is the whole point of the variant. Judged through
                    // the boundary's own majority predicate, never a count.
                    if let Some(c) = &config
                        && !QuorumSystem::is_majority(c.members(), &voters)
                    {
                        self.decided_below_majority = true;
                    }
                    // The grid outcomes (#141): the decision was made by a
                    // column, and — when the driver's override took effect —
                    // by a column other than the slot's own. Judged through
                    // the boundary's own column predicates, never a count:
                    // the deciding voters lie in some full column, and not
                    // in the slot's.
                    if let Some(c) = &config
                        && let QuorumSystem::Grid { cols, .. } = c.quorum_system()
                    {
                        self.decided_on_column = true;
                        let own = c.column_of(Slot(slot));
                        let on_own = c.has_phase2_quorum_in(&voters, own);
                        let elsewhere = (0..cols)
                            .filter(|column| Some(*column) != own)
                            .any(|column| c.has_phase2_quorum_in(&voters, Some(column)));
                        if !on_own && elsewhere {
                            self.decided_off_column = true;
                        }
                    }
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
    /// The epoch a durable report from `node` is stamped with: the first
    /// report after a send opens a new batch (see [`BatchEpoch`]).
    fn batch_epoch_for_write(&mut self, node: u64) -> u64 {
        let e = self.batch_epochs.entry(node).or_default();
        if e.sent_since_write {
            e.epoch += 1;
            e.sent_since_write = false;
        }
        e.epoch
    }

    /// `node` put a message on the wire: the next durable report is a new
    /// batch's.
    pub(super) fn note_batch_send(&mut self, node: u64) {
        self.batch_epochs.entry(node).or_default().sent_since_write = true;
    }

    /// A boundary strictly between two of `node`'s batches (a tick, a boot).
    pub(super) fn bump_batch_epoch(&mut self, node: u64) {
        let e = self.batch_epochs.entry(node).or_default();
        e.epoch += 1;
        e.sent_since_write = false;
    }

    /// Fold one durable accepted record of `node` into its current
    /// incarnation's durable log.
    pub(super) fn note_durable_record(&mut self, node: u64, slot: u64, ballot: Ballot, vhash: u64) {
        let epoch = self.batch_epoch_for_write(node);
        self.durable_log
            .insert((node, slot), (ballot, vhash, epoch));
    }

    /// The boot report is the incarnation edge: `node`'s durable log is
    /// exactly what this boot read back — a record a torn tail or a detected
    /// corruption took is not one the node can be asked for — and every
    /// record of it predates every batch this incarnation will run.
    pub(super) fn reset_durable_log(&mut self, node: u64, records: &[(Slot, Ballot, u64)]) {
        // Split this node's records out and drop them (keys sort by node).
        let mut own = self.durable_log.split_off(&(node, 0));
        let later = own.split_off(&(node.saturating_add(1), 0));
        self.durable_log.extend(later);
        let epoch = self.batch_epochs.get(&node).map_or(0, |e| e.epoch);
        for &(slot, ballot, vhash) in records {
            self.durable_log
                .insert((node, slot.0), (ballot, vhash, epoch));
        }
        self.bump_batch_epoch(node);
    }

    /// `node` compacted its log below `first`: those records are gone.
    pub(super) fn drop_durable_log_below(&mut self, node: u64, first: u64) {
        let doomed: Vec<(u64, u64)> = self
            .durable_log
            .range((node, 0)..(node, first))
            .map(|(k, _)| *k)
            .collect();
        for key in doomed {
            self.durable_log.remove(&key);
        }
    }

    /// **A Promise never under-reports** (the acceptor's half of P2c,
    /// judged against the disk rather than the acceptor's own memory): every
    /// record `node` holds durably inside the page's window
    /// `[from_slot, next_from_slot)` appears in the page — readable with the
    /// very ballot and value the disk holds, or as a faulty identity. A
    /// record missing from a Promise is a vote the candidate's P2c selection
    /// never sees, which is how a new leader overwrites a chosen value.
    /// Records the Promise's own batch made durable are skipped (see
    /// [`BatchEpoch`]); below the window's start nothing is judged — a
    /// `Prepare` below the acceptor's floor is refused, so the window always
    /// starts inside the retained log. O(page).
    pub(super) fn check_promise_complete(
        &self,
        node: u64,
        ballot: Ballot,
        from_slot: Slot,
        accepted: &BTreeMap<Slot, (Ballot, paros::Command)>,
        faulty: &BTreeMap<Slot, Ballot>,
        next_from_slot: Option<Slot>,
    ) {
        let epoch = self.batch_epochs.get(&node).map_or(0, |e| e.epoch);
        // Bounded inside this node's keys either way: an unbounded upper
        // end would run on into the next node's records.
        let upper = next_from_slot.map_or(std::ops::Bound::Included((node, u64::MAX)), |s| {
            std::ops::Bound::Excluded((node, s.0))
        });
        let window = (std::ops::Bound::Included((node, from_slot.0)), upper);
        let mut judged: u64 = 0;
        let mut missing: u64 = 0;
        let mut first_missing: Option<u64> = None;
        for (&(_, slot), &(record_ballot, vhash, stamp)) in self.durable_log.range(window) {
            if stamp >= epoch {
                continue;
            }
            judged += 1;
            let reported = accepted
                .get(&Slot(slot))
                .is_some_and(|(b, c)| *b == record_ballot && paros::command_hash(c) == vhash)
                || faulty.contains_key(&Slot(slot));
            if !reported {
                missing += 1;
                first_missing.get_or_insert(slot);
            }
        }
        assert_always!(
            missing == 0,
            "a Promise reports every durable accept in its window",
            {
                "node" => node,
                "round" => ballot.round,
                "from_slot" => from_slot.0,
                "judged" => judged,
                "missing" => missing,
                "first_missing" => first_missing.unwrap_or(u64::MAX)
            }
        );
    }

    pub(super) fn cluster_min_floor(&self) -> u64 {
        self.booted
            .iter()
            .map(|node| self.floor.get(node).map_or(0, |f| f.now))
            .min()
            .unwrap_or(0)
    }

    /// Reclaim the per-slot safety tallies below the cluster-wide floor
    /// (an O(log n) split, on the rare truncation path), keeping one
    /// scalar per pruned slot — the decided vhash — as the consensus
    /// witness a late `Commit` there is judged against
    /// (`decided_below_floor`).
    pub(super) fn prune_below_floor(&mut self) {
        let min_floor = self.cluster_min_floor();
        if min_floor == 0 {
            return;
        }
        let kept = self.decided.split_off(&min_floor);
        let pruned = std::mem::replace(&mut self.decided, kept);
        self.decided_below_floor.extend(
            pruned
                .into_iter()
                .map(|(slot, (_, _, vhash))| (slot, vhash)),
        );
        self.accept_sets = self.accept_sets.split_off(&(min_floor, 0, 0));
    }

    /// The vhash the durable-accept quorum decided for `slot`, wherever the
    /// slot stands against the floor: from the live tally, or from the
    /// witness kept when the tally was pruned.
    pub(super) fn decided_vhash(&self, slot: u64) -> Option<u64> {
        self.decided
            .get(&slot)
            .map(|&(_, _, vhash)| vhash)
            .or_else(|| self.decided_below_floor.get(&slot).copied())
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
        let timeout = self.election_timeouts.get(&node).copied().unwrap_or(0);
        let entry = self.deposed_streaks.entry(node).or_default();
        if entry.round != ballot.round || entry.node != ballot.node.0 {
            *entry = DeposedStreak {
                round: ballot.round,
                node: ballot.node.0,
                seq,
                deposed: false,
                ticks: 0,
                beatless_ticks: 0,
                timeout,
            };
        } else if entry.seq == seq {
            return;
        }
        entry.seq = seq;
        entry.beatless_ticks = 0;
        entry.timeout = timeout;
        if outvoted {
            entry.deposed = true;
        } else {
            entry.deposed = false;
            entry.ticks = 0;
        }
    }

    /// One `HeartbeatAck` reaching `node` at `ballot`: whatever the core
    /// makes of it, an ack at the leader's ballot from a member of its
    /// configuration refills the `CheckQuorum` window, so the deposed
    /// streak's clock restarts here. The ack is legitimate even after the
    /// promise-majority formed: it was sent while the sender's promise still
    /// sat at or below the ballot and merely arrived late — the network's
    /// delay is not bounded by an election window (seed 4279021087318167556:
    /// two acks sent before either promise moved arrived 350–430 ms later
    /// and carried a deposed leader to 13 ticks against a 12-tick budget,
    /// with the protocol behaving exactly as specified).
    pub(super) fn observe_ack_received(&mut self, node: u64, from: u64, ballot: Ballot) {
        let in_config = self
            .config_of(ballot)
            .is_some_and(|c| c.contains(paros::NodeId(from)));
        if !in_config {
            return;
        }
        if let Some(entry) = self.deposed_streaks.get_mut(&node)
            && entry.round == ballot.round
            && entry.node == ballot.node.0
        {
            entry.ticks = 0;
        }
    }

    /// One logical tick at `node`: a deposed leader's clock runs, and it must
    /// step down within two `CheckQuorum` windows of the later of its
    /// deposal and the last ack it received at that ballot (see
    /// [`Self::observe_beat`], [`Self::observe_ack_received`]).
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
        let timeout = entry.timeout;
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

    /// A proxy leader's decision (#142), judged against the disks: the
    /// `Commit` it is about to emit for `slot` at `ballot` must be backed by
    /// a Phase-2 quorum of the ballot's configuration holding a durable
    /// accept of exactly `vhash` **at that ballot** — the tally the proxy
    /// folded is never trusted, only the accepts the acceptors reported
    /// durable before their `Accepted`s left (the proxy model checker's
    /// claim 2, on the real transport). And the value is the one the ballot
    /// proposed for the slot: a proxy carries a command, it never picks one.
    /// Below the cluster-wide compaction floor the per-slot tally is pruned,
    /// so the witness there is the decided vhash the pruning kept
    /// (`decided_below_floor`) — the consensus decision, never the applied
    /// command, which a #94 re-chosen identity turns into a `Noop`.
    pub(super) fn observe_proxy_decision(
        &mut self,
        proxy: u64,
        slot: u64,
        ballot: Ballot,
        vhash: u64,
    ) {
        let key = (slot, ballot.round, ballot.node.0);
        if slot >= self.cluster_min_floor() {
            let voters: BTreeSet<paros::NodeId> = self
                .accept_sets
                .get(&key)
                .map(|holders| holders.iter().map(|n| paros::NodeId(*n)).collect())
                .unwrap_or_default();
            let backed = self
                .config_of(ballot)
                .is_some_and(|c| c.has_phase2_quorum(&voters));
            assert_always!(
                backed,
                "proxy: a Commit a proxy emits is backed by a durable Phase-2 quorum at one ballot",
                {
                    "proxy" => proxy,
                    "slot" => slot,
                    "round" => ballot.round,
                    "bnode" => ballot.node.0,
                    "voters" => voters.len()
                }
            );
        } else {
            // The witness wherever it stands: pruned into
            // `decided_below_floor`, or still in the live tally when the
            // cluster's minimum floor rose without a driver-audited
            // truncation to prune on (an install or a ground-truth flush
            // raises the folded floor too) — hunt seed 5625748798251412727
            // at 13b44b3: slot 4 below a minimum floor of 5 with nothing
            // pruned yet, judged against an empty witness map.
            let decided = self.decided_vhash(slot);
            assert_always!(
                decided == Some(vhash),
                "proxy: a Commit a proxy emits is backed by a durable Phase-2 quorum at one ballot",
                {
                    "proxy" => proxy,
                    "slot" => slot,
                    "round" => ballot.round,
                    "below_floor" => true,
                    "min_floor" => self.cluster_min_floor(),
                    "witness_known" => decided.is_some(),
                    "witness_matches" => decided == Some(vhash),
                    "pruned_slots" => self.decided_below_floor.len()
                }
            );
        }
        let proposed = self
            .proposed
            .get(&(ballot.round, ballot.node.0, slot))
            .copied();
        assert_always!(
            proposed.is_none_or(|p| p == vhash),
            "proxy: a proxy decides the command it was delegated",
            { "proxy" => proxy, "slot" => slot, "round" => ballot.round }
        );
        self.decided_through_proxy = true;
        if self
            .fanout_leaders
            .get(&key)
            .is_some_and(|leaders| leaders.len() > 1)
        {
            reach_once!(
                self.proxied_round_survived_handoff,
                "proxy: a delegated round survives a leader handoff"
            );
        }
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
        // With no chaos a proposal does come back.
        assert_sometimes!(h.acked > 0, "at least one proposal is acknowledged");
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
        h.check_coverage_gates(self.leader_change_ms);
    }
}
