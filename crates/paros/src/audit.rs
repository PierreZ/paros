//! The **audit port**: the driver's provider-generic observation seam.
//!
//! [`Audit`] is the mirror image of [`DriverHooks`](crate::DriverHooks). Hooks
//! *perturb* the driver — they answer "should I take this rare-but-valid
//! alternative?" and the driver's behavior changes with the answer. The audit
//! only *observes*: the driver reports every externally meaningful state
//! transition, typed, at the instant it happens, and nothing it returns (it
//! returns nothing) can influence the run. Deleting every audit call must leave
//! the shipped program bit-identical.
//!
//! Each callback fires **after** the transition it reports is real: a durable
//! write after its fsync, an apply after the application saw it, a send beside
//! the transmit. They sit exactly where the driver's `tracing` events already
//! are — the trace stays for humans and the wasm demo, while correctness
//! checking moves here, where an implementation can fold each transition into
//! O(1) incremental state instead of re-scanning a growing event stream.
//!
//! Production passes [`NoAudit`]; every method defaults to a no-op.

use std::collections::BTreeMap;

use paros_core::{
    AcceptorConfig, Ballot, GcAck, GcStep, Handoff, MatchRefusal, MatchmakerHardState,
    MatchmakerId, MatchmakerPhase, MatchmakerSet, Message, NodeId, PendingBootstrap,
    ReconfigureReply, ReconfigureRequest, ReconfigureResult, ReconfigurerStep, Registration,
    RegistrationKind, Slot,
};

use crate::grpc::EdgeRejection;
use crate::hooks::Seam;
use crate::storage::StorageError;

/// The driver's reaction to a [`StorageError`]. Stage 6 has exactly one honest
/// reaction — crash and re-enter the crash/recovery path — but the decision is
/// typed so Stage 8's protocol-aware choices (mark-faulty, degrade a single
/// record, stay up) slot in as variants the audit can match on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StorageFaultDecision {
    /// Fail-stop: the node crashes rather than run on state it does not
    /// durably have, and recovery is the ordinary crash/restart path.
    Crash,
}

/// A node's durable deployment, as reported at boot by [`Audit::recovered`]:
/// the bootstrap acceptor configuration, the addressable node pool, and the
/// matchmaker set (empty on plain Multi-Paxos).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Deployment {
    /// The bootstrap acceptor configuration (`Config::peers`).
    pub bootstrap: AcceptorConfig,
    /// Every node that may ever be an acceptor (`Config::pool()`).
    pub pool: Vec<NodeId>,
    /// The bootstrap matchmaker set (`Config::matchmakers`).
    pub matchmakers: Vec<MatchmakerId>,
    /// Every matchmaker a matchmaker-set reconfiguration may draw from
    /// (`Config::matchmaker_pool()`).
    pub matchmaker_pool: Vec<MatchmakerId>,
}

/// One `MatchB` page as it leaves a matchmaker: where it starts, the
/// registrations it carries, where the next one starts (`None` when the
/// answer is complete) and the durable watermark it was computed under.
///
/// A borrowed view, never an owned copy: the registry is the driver's own
/// `BTreeMap` and an audit that made it allocate would be a port that
/// changes the shipped program (AGENTS.md, *Audit doctrine*).
pub struct HistoryPage<'a> {
    /// Where this page starts — the request's cursor, floored at the
    /// watermark.
    pub from_ballot: Ballot,
    /// The registrations it carries, in ballot order.
    pub history: &'a BTreeMap<Ballot, Registration>,
    /// Where the next page starts; `None` when the answer is complete.
    pub next_from_ballot: Option<Ballot>,
    /// The durable watermark in force when the page was computed.
    pub gc_watermark: Ballot,
    /// The matchmaker's durable effective configuration
    /// (`MatchmakerHardState::effective`), reported beside the history: GC
    /// drops the record, never the scalar, so a page whose window is empty
    /// can still name the acceptor set in force.
    pub effective: Option<&'a (Ballot, AcceptorConfig)>,
}

/// Provider-generic observation port for [`run_node`](crate::run_node).
///
/// Pure observation: implementations must not influence the driver (that is
/// [`DriverHooks`](crate::DriverHooks)' job) and must not block — a callback
/// runs inline on the node loop.
#[allow(unused_variables)]
pub trait Audit {
    /// This node durably raised its promised ballot (after the fsync).
    fn promised(&self, node: NodeId, ballot: Ballot) {}

    /// This node durably accepted `command` (hashed to `vhash`) at `ballot`
    /// for `slot`. `promised` is the node's promise at the time of the write,
    /// so the never-accept-above-promise invariant is checkable per slot.
    fn accepted(&self, node: NodeId, slot: Slot, ballot: Ballot, promised: Ballot, vhash: u64) {}

    /// This node durably advanced its chosen index.
    fn chosen_index(&self, node: NodeId, index: Slot) {}

    /// This node durably truncated its log prefix; `first` is the new
    /// compaction floor (the first slot still retained).
    fn truncated(&self, node: NodeId, first: Slot) {}

    /// This node installed an opaque application snapshot from a peer, jumping
    /// its chosen prefix to `chosen_index` and adopting `ballot`.
    fn snapshot_installed(&self, node: NodeId, chosen_index: Slot, ballot: Ballot) {}

    /// This node applied the chosen command at `slot` (hashed to `vhash`),
    /// advancing its contiguous applied prefix.
    /// `identity` is the `(client, seq)` dedup key for a user command (`None`
    /// for a control command): the at-most-once oracle keys on it, because two
    /// distinct requests can legitimately share payload bytes.
    fn applied(&self, node: NodeId, slot: Slot, vhash: u64, identity: Option<(u64, u64)>) {}

    /// This node handed `msg` to the transport, addressed to `to`. Reports the
    /// core's outbound decision even when the network later drops it.
    fn sent(&self, node: NodeId, to: NodeId, msg: &Message) {}

    /// This node became leader at `won`, holding `promised` at that instant and
    /// having filled `gap_fills` undecided holes with no-ops, and now runs
    /// Phase 2 over `config` — the configuration `won` was registered with on
    /// a matchmaker deployment, the static membership on plain Multi-Paxos.
    fn elected(
        &self,
        node: NodeId,
        won: Ballot,
        promised: Ballot,
        gap_fills: u64,
        config: &AcceptorConfig,
    ) {
    }

    /// The driver asked this leader to resign, and it did.
    fn stepped_down(&self, node: NodeId) {}

    /// This node **relinquished** the Phase-2 authority of `handoff.ballot` to
    /// a single successor and demoted itself in the same core call. Reported at
    /// the instant the authority changed hands — before the message reaches the
    /// transport, and therefore before any successor can install it, so a
    /// checker sees the two halves of a handoff in causal order.
    ///
    /// This is the semantic "authority released" event the uniqueness oracle
    /// keys on: after it, this node must never again send an `Accept` at that
    /// ballot, whatever its role field happens to say.
    fn authority_relinquished(&self, node: NodeId, handoff: Handoff) {}

    /// This node **installed** a predecessor's transferred authority and is now
    /// exercising Phase 2 under `ballot` — a leadership acquired with *no*
    /// Phase 1, so it is deliberately not reported through
    /// [`Audit::elected`] (whose "leadership ballots strictly increase" reading
    /// is about a node's own campaigns). `next_slot` is the inherited allocator
    /// frontier and `tail` the number of slots between this node's own chosen
    /// prefix and that frontier — the unfinished business it took over.
    fn authority_installed(
        &self,
        node: NodeId,
        from: NodeId,
        ballot: Ballot,
        next_slot: Slot,
        tail: u64,
    ) {
    }

    /// This node **refused** an incoming transfer: `target` counts payloads
    /// addressed elsewhere or naming a non-member, `stale` counts authorities
    /// its own durable promise already dominates (plus allocator rewinds and
    /// re-installs), `shape` counts malformed tails, and `unfit` counts
    /// transfers onto a node that needs Phase-1-shaped repair. Monotone totals
    /// for this incarnation, reported when they change.
    fn handoff_refused(&self, node: NodeId, target: u64, stale: u64, shape: u64, unfit: u64) {}

    /// This node resigned a handoff-installed leadership because its inherited
    /// read fence stayed uncovered — the deliberate fallback to an ordinary
    /// Phase 1. `count` is the monotone total for this incarnation.
    fn handoff_fence_expired(&self, node: NodeId, count: u64) {}

    /// This node holds a chosen slot above its contiguous applied prefix:
    /// `hole` is the first slot missing, `above` the highest chosen slot past
    /// it. Reported once per tick for as long as the gap lasts.
    fn chosen_gap(&self, node: NodeId, hole: Slot, above: Slot) {}

    /// This node answered a client proposal with a *committed* ack naming
    /// `slot`; `applied` is the node's own applied prefix at that instant.
    /// `dedup` marks the fast-path ack of a retry whose request was already
    /// chosen (`ProposeResult::Chosen`), as opposed to the ack-on-commit path.
    #[allow(clippy::fn_params_excessive_bools)]
    fn client_acked(
        &self,
        node: NodeId,
        client: u64,
        seq: u64,
        slot: Slot,
        applied: Option<Slot>,
        dedup: bool,
    ) {
    }

    /// This node answered a client read with a confirmed watermark (`None` is
    /// the empty applied prefix, which is not slot 0).
    fn read_confirmed(&self, node: NodeId, index: Option<Slot>) {}

    /// This node (re)booted, having rebuilt volatile state from durable
    /// storage: its recovered promise, the chosen index it rebuilt
    /// (`None` = an empty chosen prefix), its durable deployment — the
    /// bootstrap acceptor configuration, the addressable node pool, and the
    /// matchmaker set (empty on plain Multi-Paxos) — so a checker can do
    /// quorum arithmetic without guessing the topology, plus every
    /// `(slot, ballot, vhash)` accepted record it read back.
    fn recovered(
        &self,
        node: NodeId,
        promised: Ballot,
        chosen_index: Option<Slot>,
        deployment: &Deployment,
        accepted: &[(Slot, Ballot, u64)],
    ) {
    }

    /// This node crashed at a durability `seam` inside a `Ready` batch.
    fn crashed(&self, node: NodeId, seam: Seam) {}

    /// A [`NodeStorage`](crate::NodeStorage) call surfaced `error` and the
    /// driver decided `decision` — reported at the instant of the decision,
    /// before the crash unwinds. The error carries the fault kind, the record
    /// identity, and the durability outcome as data, so a checker can fold
    /// injected-vs-detected accounting into O(1) state without string parsing.
    fn storage_fault(&self, node: NodeId, error: &StorageError, decision: StorageFaultDecision) {}

    /// This node dropped one outbound message at the send seam (hook-decided
    /// per-message loss, indistinguishable from network loss to the peers).
    fn dropped_at_send(&self, node: NodeId, to: NodeId, msg: &Message) {}

    /// The driver deliberately sent this one outbound message twice
    /// ([`DriverHooks::duplicate_outgoing`](crate::DriverHooks)).
    fn duplicated_at_send(&self, node: NodeId, to: NodeId, msg: &Message) {}

    /// The driver deliberately dropped this one client-facing reply after the
    /// server state advanced ([`DriverHooks::drop_client_reply`](crate::DriverHooks)).
    fn client_reply_dropped(&self, node: NodeId, reply: crate::hooks::Reply) {}

    /// The driver deliberately re-queued this one reply so the node loop
    /// folds it twice
    /// ([`DriverHooks::duplicate_client_reply`](crate::DriverHooks)) — the
    /// mirror of [`Audit::client_reply_dropped`], and the test of every
    /// idempotency claim the matchmaker plane's answers rest on.
    fn client_reply_duplicated(&self, node: NodeId, reply: crate::hooks::Reply) {}

    /// A snapshot install persisted while this node was a live Candidate — the
    /// #88 window (`on_install_snapshot` deliberately does not touch the
    /// election, so the campaign stays open across the install).
    fn snapshot_mid_election(&self, node: NodeId) {}

    /// This node materialized `offers` snapshot transfers into the common
    /// outbound path (reported before the after-sync/before-send seam).
    fn snapshot_offered(&self, node: NodeId, offers: u64) {}

    /// This Ready batch started `started` inherited or gap-fill accept rounds,
    /// including `gap_fills` fresh no-ops; `remaining` slots are deferred.
    fn recovery_batch(&self, node: NodeId, started: u64, gap_fills: u64, remaining: u64) {}

    /// This node deliberately skipped re-sending its pending `Accept`s.
    fn resend_skipped(&self, node: NodeId) {}

    /// This node selected the shortest valid election timeout.
    fn election_timeout_extreme(&self, node: NodeId, ticks: u64) {}

    /// This node now runs with an election timeout of `ticks` (the driver's
    /// randomized draw, re-drawn at every demotion). The `CheckQuorum`
    /// window a leader re-proves its ack quorum in is exactly this long, so
    /// a liveness oracle that bounds a deposed leader's remaining beats must
    /// measure against it, never against a fixed count.
    fn election_timeout_set(&self, node: NodeId, ticks: u64) {}

    /// This node's logical clock ticked (`RawNode::tick`), once per driver
    /// tick. The unit every core timeout is counted in.
    fn ticked(&self, node: NodeId) {}

    /// This node received a `Prepare` below its own compaction floor — the
    /// "campaign against a truncated acceptor" interleaving.
    fn prepare_below_floor(&self, node: NodeId, from_slot: Slot, floor: Slot) {}

    /// This node dropped a parked proposal reply because its slot decided a
    /// *different* command (a stale leader's admission superseded by the
    /// majority's decision); the client was answered with a retry redirect
    /// instead of a false commit.
    fn waiter_superseded(&self, node: NodeId, slot: Slot) {}

    /// This node, as Leader, spent a full election-timeout window without an
    /// ack quorum and demoted itself (`CheckQuorum`, #95). `count` is the number
    /// of such step-downs in the batch (in practice 1).
    fn quorum_lost(&self, node: NodeId, count: u64) {}

    /// This node's apply seam suppressed `count` chosen slots whose
    /// `(client, seq)` identity had already applied at a lower slot — the #94
    /// double-apply, executed as a no-op instead. Reported once per batch with
    /// the number of suppressions the batch performed.
    fn duplicate_suppressed(&self, node: NodeId, count: u64) {}

    /// This node booted with recoverable **faulty entries** (Stage 8): the
    /// scan classified each record's value lost but its identity known, and
    /// the node reports them through the Promise tri-state instead of
    /// crashing. Reported once per boot, before [`Audit::recovered`], so the
    /// divergence checks can key their explained-only rule on it.
    fn faulty_reported(&self, node: NodeId, entries: &[(Slot, Ballot)]) {}

    /// This node opened an **application repair** at boot: the replay could
    /// not walk the whole chosen prefix (`below_floor` says whether the cursor
    /// sits under the compaction floor — the snapshot-recovery path — or at a
    /// faulty/missing chosen record the catch-up heal will re-learn).
    fn app_repair_started(&self, node: NodeId, from: Slot, below_floor: bool) {}

    /// Monotone repair-progress totals for this incarnation, reported when
    /// they change: local faulty records repaired in place, Case-1 straggler
    /// re-proposals, Case-2 straggler no-op fills, recovery-timeout
    /// step-downs (CTRL §4.2), and cumulative repair payload bytes (the CTRL
    /// §5.2 repair-cost metric).
    fn repair_progress(
        &self,
        node: NodeId,
        repaired: u64,
        case1: u64,
        case2: u64,
        step_downs: u64,
        bytes: u64,
    ) {
    }

    /// This node durably recorded the decided snapshot point at `at` — the
    /// applied [`Control::Snap`](paros_core::Control::Snap) marker's slot
    /// (#101). Reported after the recording batch's fsync.
    fn snap_recorded(&self, node: NodeId, at: Slot) {}

    /// This node's boot scan classified `chunks` rotted chunks of its
    /// retained decided snapshot at `at` — value lost, identity known: the
    /// chunk-repair layer pulls them from peers.
    fn snap_chunks_reported(&self, node: NodeId, at: Slot, chunks: u64) {}

    /// This node installed `chunks` repaired chunks of the decided snapshot
    /// at `at`, received from a peer: `bytes` chunk payload against the
    /// point's `blob_bytes` — the CTRL §5.2 chunk-repair cost (a chunk repair
    /// ships chunks, never the blob).
    fn snap_chunk_repaired(
        &self,
        node: NodeId,
        at: Slot,
        chunks: u64,
        bytes: u64,
        blob_bytes: u64,
    ) {
    }

    /// This node's store **refused** a repaired chunk of the decided snapshot
    /// at `at`: every chunk the point still lacked arrived and every write
    /// returned `Ok`, yet the store does not call the point whole. The pull
    /// keeps asking — the point stays incomplete until a custodian serves
    /// bytes the store accepts.
    fn snap_chunk_rejected(&self, node: NodeId, at: Slot) {}

    /// This node answered a chunk request for a point it no longer retains
    /// with its full, more advanced snapshot (`Message::InstallSnapshot`) —
    /// the unchanged whole-blob fallback.
    fn snap_advanced_fallback(&self, node: NodeId, to: NodeId) {}

    /// This node restored its lost application state locally from its own
    /// (chunk-repaired) decided snapshot point at `at`, instead of a
    /// whole-blob transfer.
    fn snap_point_restored(&self, node: NodeId, at: Slot) {}

    /// This node answered a client `Compact` request; `accepted` is the honest
    /// outcome the ack carried (`true` only when the `Truncate` control
    /// proposal was actually admitted — a redirect, a coupling refusal, or a
    /// failed proposal all report `false`).
    fn compact_acked(&self, node: NodeId, accepted: bool) {}

    /// This node held a snapshot chunk `to` asked for, clean, and stayed
    /// silent about it (the `withhold_snap_chunk` hook fired). Reported so a
    /// checker can tie a later chunk repair at the requester to the silence it
    /// had to work around.
    fn snap_chunk_withheld(&self, node: NodeId, to: NodeId) {}

    /// This node answered a parked read with a retry redirect instead of a
    /// confirmation: `early` when the `expire_parked_read_early` hook fired
    /// before the read's confirmation deadline, otherwise the deadline itself
    /// ran out.
    fn read_expired(&self, node: NodeId, early: bool) {}

    /// This node dropped one outbound message at a bounded in-process mailbox
    /// (the lossy per-peer transport handoff): either the enqueue found the
    /// peer queue full, or the delivery task discarded a stale backlog entry
    /// to keep the newest batch. `kind` is the message's stable label.
    /// Deliberately lossy by design (heartbeats/resends repair it); surfaced
    /// so a sweep can see the loss instead of inferring it.
    fn dropped_at_mailbox(&self, node: NodeId, to: NodeId, kind: &'static str) {}

    /// This node skipped materializing a snapshot offer because its applied
    /// application state did not cover the offered boundary (a legitimate
    /// transient under an open application repair); the requester re-asks.
    fn snapshot_offer_skipped(&self, node: NodeId, offered: Slot) {}

    /// One peer-delivery RPC toward `to` failed or timed out; every message
    /// of that batch is lost at the transport. Reported from the delivery
    /// task (the audit handle is cloned into it), so implementations must
    /// stay observation-only here as everywhere.
    fn delivery_failed(&self, node: NodeId, to: NodeId) {}

    /// This node lost its leadership with client replies still parked:
    /// `writes` proposals whose slot may yet commit under the successor (their
    /// clients time out, on purpose) and `reads` that were answered with a
    /// redirect on the spot.
    fn waiters_cleared(&self, node: NodeId, writes: u64, reads: u64) {}

    /// The gRPC edge refused an inbound request before it reached the node
    /// loop — a peer message that decoded from the wire but not into a
    /// `Message`. The refusal happens at the edge; nothing inside the node
    /// changed.
    fn edge_rejected(&self, node: NodeId, kind: EdgeRejection) {}

    // ---- the leader-side matchmaking phase (#120) and reconfiguration (#122) ----

    /// This candidate opened a matchmaking phase for `ballot`, registering
    /// `config` (`C_b`) with every matchmaker; `kind` says whether the
    /// campaign was opened by a reconfiguration request or by the election
    /// clock. Reported at the instant the phase opens, before any request is
    /// sent. Never fires on plain Multi-Paxos.
    fn matchmaking_started(
        &self,
        node: NodeId,
        ballot: Ballot,
        config: &AcceptorConfig,
        kind: RegistrationKind,
        generation: u64,
    ) {
    }

    /// This candidate handed a matchmaking request for `ballot` to the
    /// transport, addressed to `matchmaker` (the first send or a re-send).
    fn match_request_sent(&self, node: NodeId, matchmaker: MatchmakerId, ballot: Ballot) {}

    /// This candidate deliberately skipped re-sending its open matchmaking
    /// request this beat ([`DriverHooks::skip_matchmaking_resend`](crate::DriverHooks)).
    fn matchmaking_resend_skipped(&self, node: NodeId) {}

    /// This candidate's matchmaking quorum named a reconfiguration to a
    /// configuration other than the one its ordinary campaign registered for
    /// `ballot`: the campaign was abandoned and the configuration registered
    /// at `newest` — the effective configuration — adopted as the node's
    /// belief (`RawNode::on_match_reply`, `StaleConfiguration`).
    fn matchmaking_stale_configuration(&self, node: NodeId, ballot: Ballot, newest: Ballot) {}

    /// This candidate's election clock fired while its matchmaking was still
    /// open and re-asked the unanswered matchmakers instead of abandoning the
    /// campaign (`RawNode::tick`). `count` is the monotone total for this
    /// incarnation; the campaign's ballot is unchanged.
    fn matchmaking_timeout(&self, node: NodeId, ballot: Ballot, count: u64) {}

    /// This candidate folded a `Registered` reply from `matchmaker` for
    /// `ballot`; `remaining` registrations are still needed for the quorum.
    ///
    /// `watermark` and `history_hash` name **which** answer was folded (a
    /// matchmaker answers a re-sent request again, from a registry a floor
    /// may have been raised on in between, so the copies differ). Without
    /// them an oracle can only ask whether *some* choice of one copy per
    /// matchmaker explains the campaign's union — a cartesian product over
    /// the copies, superlinear and strictly weaker than the point check the
    /// candidate itself can report.
    fn match_registered_by(
        &self,
        node: NodeId,
        matchmaker: MatchmakerId,
        ballot: Ballot,
        remaining: usize,
        watermark: Ballot,
        history_hash: u64,
    ) {
    }

    /// This candidate folded a `Registered` page from `matchmaker` for
    /// `ballot` that was **not** the last one: the registration does not
    /// count toward the quorum yet, and the next page is asked for from
    /// `next`. `watermark` and `history_hash` name the page, exactly as
    /// [`Audit::match_registered_by`] names the terminal one.
    fn match_paged(
        &self,
        node: NodeId,
        matchmaker: MatchmakerId,
        ballot: Ballot,
        next: Ballot,
        watermark: Ballot,
        history_hash: u64,
    ) {
    }

    /// This candidate's matchmaking quorum closed for `ballot`: `prior` is
    /// `H_b` (the distinct prior configurations Phase 1 must each cover, in
    /// ballot order), `watermark` the maximum GC watermark it was filtered by,
    /// `registered_by` how many matchmakers answered, and `disagreements` how
    /// many ballots two matchmakers reported different configurations for
    /// (always 0 — the union keeps both). Reported at the matchmaking →
    /// Phase 1 boundary, before the first `Prepare` is sent.
    fn matchmaking_completed(
        &self,
        node: NodeId,
        ballot: Ballot,
        prior: &[AcceptorConfig],
        watermark: Ballot,
        registered_by: usize,
        disagreements: u64,
    ) {
    }

    /// A matchmaker refused this candidate's registration for `ballot`: the
    /// campaign was abandoned and the node is a follower again.
    fn matchmaking_refused(
        &self,
        node: NodeId,
        matchmaker: MatchmakerId,
        ballot: Ballot,
        refusal: MatchRefusal,
    ) {
    }

    /// This node declined to campaign because it is not a member of the
    /// configuration it would have registered (a spare, or a removed node).
    /// `count` is the monotone total for this incarnation.
    fn campaign_skipped_non_member(&self, node: NodeId, count: u64) {}

    /// This leader resigned because its own reconfiguration removed it from
    /// the acceptor set and the change is complete; an ordinary election
    /// lands leadership inside the new configuration. `count` is the monotone
    /// total for this incarnation.
    fn non_member_leader_resigned(&self, node: NodeId, count: u64) {}

    /// This node answered a client `Reconfigure` request with `result`
    /// (started at a fresh ballot, refused with a reason, or redirected).
    fn reconfigure_acked(&self, node: NodeId, members: &[NodeId], result: ReconfigureResult) {}

    // ---- garbage collection (#123) ------------------------------------------

    /// This leader handed a garbage-collection request to the transport,
    /// addressed to `matchmaker`, asking `generation`'s registry to raise its
    /// floor to `watermark` (the leader's own ballot) — the first send or a
    /// re-send. Fires only once the forgettability condition held
    /// (`fence` is the election fence a quorum of the configuration in
    /// force reported holding).
    fn gc_request_sent(
        &self,
        node: NodeId,
        matchmaker: MatchmakerId,
        generation: u64,
        watermark: Ballot,
        fence: Option<Slot>,
    ) {
    }

    /// This leader deliberately skipped re-sending its open GC request this
    /// beat ([`DriverHooks::skip_gc_resend`](crate::DriverHooks)).
    fn gc_resend_skipped(&self, node: NodeId) {}

    /// This leader folded `matchmaker`'s GC ack: what it did to the campaign
    /// — one more ack, or the quorum that makes the floor effective and
    /// names the retirable acceptors.
    fn gc_step(&self, node: NodeId, matchmaker: MatchmakerId, ack: &GcAck, step: &GcStep) {}

    // ---- the matchmaker set and its reconfiguration (#125) ------------------

    /// This node adopted `set` as the authoritative matchmaker set (a
    /// refusal naming a chosen successor, a reply from a later generation, or
    /// a handover this node drove to completion).
    fn matchmakers_learned(&self, node: NodeId, set: &MatchmakerSet) {}

    /// This node started a matchmaker-set reconfiguration from `old` toward
    /// `target` (a client `ReconfigureMatchmakers`, or a frozen registry
    /// without a successor that this node finishes on its own).
    fn reconfigurer_started(&self, node: NodeId, old: &MatchmakerSet, target: &[MatchmakerId]) {}

    /// This node answered a client `ReconfigureMatchmakers` request: started
    /// (`refusal` empty) or refused for `refusal`.
    fn reconfigure_matchmakers_acked(&self, node: NodeId, refusal: &'static str) {}

    /// This node's reconfigurer handed `request` to the transport, addressed
    /// to `matchmaker` (the first send or a re-send).
    fn reconfigure_request_sent(
        &self,
        node: NodeId,
        matchmaker: MatchmakerId,
        request: &ReconfigureRequest,
    ) {
    }

    /// This node deliberately skipped re-sending its running handover's
    /// requests this beat ([`DriverHooks::skip_reconfigurer_resend`](crate::DriverHooks)).
    fn reconfigurer_resend_skipped(&self, node: NodeId) {}

    /// This node abandoned a handover whose running phase made no progress
    /// for `reconfigure_timeout_elections` election timeouts (a member that
    /// never answers); the frozen generation stays for the next node that
    /// meets it to finish.
    fn reconfigurer_aborted(&self, node: NodeId) {}

    /// This node's successor decree was preempted and it will wait `ticks`
    /// (a jittered draw) before reopening at a higher ballot.
    fn reconfigurer_backoff(&self, node: NodeId, ticks: u64) {}

    /// This node's reconfigurer folded `reply` from `matchmaker`: what it did
    /// to the handover.
    fn reconfigurer_step(
        &self,
        node: NodeId,
        matchmaker: MatchmakerId,
        reply: &ReconfigureReply,
        step: &ReconfigurerStep,
    ) {
    }

    /// This node told `matchmaker` — a straggler that answered `Inactive`
    /// or from a lower generation — the chosen `successor` it knows.
    fn successor_republished(
        &self,
        node: NodeId,
        matchmaker: MatchmakerId,
        successor: &MatchmakerSet,
    ) {
    }

    /// This node answered an operator `Retire` request: accepted (the node
    /// shuts down for good at its next tick) or refused, with `refusal`
    /// naming the leg that refused it — `"plain"`, `"leader"`, `"member"`,
    /// or `"not_collected"` (the request carried no GC watermark above this
    /// node's membership fence, so nothing proves the cluster is done with
    /// it). Empty when accepted.
    fn retire_acked(&self, node: NodeId, accepted: bool, refusal: &str) {}

    /// This node is shutting down for good, retired by its operator after a
    /// leader's garbage collection named it retirable.
    fn retired(&self, node: NodeId) {}

    // ---- the matchmaker (`run_matchmaker`), a distinct role and namespace ----

    /// This matchmaker (re)booted from its durable registry: the set it is
    /// active or frozen for and its phase, every `(ballot, configuration)` it
    /// read back, and its watermark. Fires on the first boot (an empty
    /// registry) and on every restart.
    fn matchmaker_recovered(
        &self,
        matchmaker: MatchmakerId,
        set: &MatchmakerSet,
        phase: MatchmakerPhase,
        registry: &[(Ballot, Registration)],
        gc_watermark: Ballot,
    ) {
    }

    /// This node's handover closed its freeze: a quorum of `generation`
    /// answered, and `bootstrap` is the reconstruction now on its way to
    /// every proposed member of the successor. Reported on the driver beat
    /// that closes the freeze, never on the ack that completed the quorum
    /// (#125, review finding P5).
    ///
    /// `disagreements` counts the ballots two frozen registries reported
    /// with different registrations: the union keeps one, *durably*, so the
    /// count is what makes "a reconstruction sees one registration per
    /// ballot" a checkable claim rather than a silent narrowing.
    fn reconfigurer_reconstructed(
        &self,
        node: NodeId,
        generation: u64,
        bootstrap: &PendingBootstrap,
        disagreements: u64,
    ) {
    }

    /// This matchmaker durably persisted its generation scalars whole (after
    /// the fsync): a freeze, a successor link, a decree promise or vote, a
    /// pending bootstrap (#125).
    fn matchmaker_scalars_persisted(
        &self,
        matchmaker: MatchmakerId,
        scalars: &MatchmakerHardState,
    ) {
    }

    /// This matchmaker durably activated a successor generation (after the
    /// fsync): `set` is the new set, `gc_watermark` the reconstructed floor,
    /// `effective` the inherited effective configuration (the maximum of the
    /// local and the reconstructed one) and `registry` the reconstructed
    /// registry it now serves from.
    fn matchmaker_activated(
        &self,
        matchmaker: MatchmakerId,
        set: &MatchmakerSet,
        gc_watermark: Ballot,
        effective: Option<&(Ballot, AcceptorConfig)>,
        registry: &[(Ballot, Registration)],
    ) {
    }

    /// This matchmaker is answering a reconfiguration `request` with `reply`.
    /// Reported at the instant the reply leaves — after the batch's fsync
    /// and its durable reports.
    fn matchmaker_reconfigure_replied(
        &self,
        matchmaker: MatchmakerId,
        request: &ReconfigureRequest,
        reply: &ReconfigureReply,
    ) {
    }

    /// This matchmaker is answering a GC request with `ack` (applied, or
    /// refused for a generation it is not active for). Reported at the
    /// instant the ack leaves, after the raise's fsync and its report.
    fn matchmaker_gc_replied(&self, matchmaker: MatchmakerId, ack: &GcAck) {}

    /// This matchmaker durably registered `config` under `ballot` (after the
    /// fsync).
    fn match_registered(
        &self,
        matchmaker: MatchmakerId,
        ballot: Ballot,
        registration: &Registration,
    ) {
    }

    /// This matchmaker durably raised its GC watermark (after the fsync),
    /// dropping every registration below it.
    fn gc_watermark_raised(&self, matchmaker: MatchmakerId, watermark: Ballot) {}

    /// This matchmaker is answering `to`'s request for `ballot` with a
    /// registration: `history` is every `(ballot, registration)` the reply
    /// names, `generation` the matchmaker set the reply speaks for, and
    /// `gc_watermark` the floor it reports. Reported at the instant
    /// the reply leaves — after the registration's fsync and its
    /// [`Audit::match_registered`] report, which is what lets a checker judge
    /// persist-before-reply.
    fn match_replied(
        &self,
        matchmaker: MatchmakerId,
        to: NodeId,
        ballot: Ballot,
        generation: u64,
        page: &HistoryPage<'_>,
    ) {
    }

    /// This matchmaker refused `to`'s request for `ballot`; nothing was
    /// written. Reported at the instant the refusal leaves.
    fn match_refused(
        &self,
        matchmaker: MatchmakerId,
        to: NodeId,
        ballot: Ballot,
        refusal: MatchRefusal,
    ) {
    }

    /// This matchmaker crashed at a durability `seam` inside one batch.
    fn matchmaker_crashed(&self, matchmaker: MatchmakerId, seam: Seam) {}

    /// The driver deliberately dropped one matchmaker reply after its write
    /// was durable ([`DriverHooks::drop_client_reply`] with
    /// [`Reply::Match`](crate::Reply::Match), [`Reply::GcAck`](crate::Reply::GcAck)
    /// or [`Reply::MatchmakerReconfigure`](crate::Reply::MatchmakerReconfigure)).
    fn match_reply_dropped(&self, matchmaker: MatchmakerId, reply: crate::hooks::Reply) {}

    /// A [`MatchmakerStorage`](crate::MatchmakerStorage) call surfaced `error`
    /// and the driver decided `decision` (see [`Audit::storage_fault`]).
    fn matchmaker_storage_fault(
        &self,
        matchmaker: MatchmakerId,
        error: &StorageError,
        decision: StorageFaultDecision,
    ) {
    }
}

/// Inert production audit: every observation is dropped.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoAudit;

impl Audit for NoAudit {}
