//! The [`RawNode`] handle: the sans-IO Multi-Paxos state machine and the
//! `step`/`tick`/`ready`/`advance` contract.

mod acceptor;
mod catch_up_snapshot;
mod decide_apply;
mod election;
mod helpers;
mod reads;
mod replication;

use std::collections::{BTreeMap, BTreeSet};

use self::decide_apply::Proposing;
use self::election::{Election, LeaderRecovery, RepairProbe};
use self::reads::{READ_ROUND_TTL_TICKS, ReadRound};
use self::replication::HEARTBEAT_TICKS;
use crate::message::Message;
use crate::ready::Ready;
use crate::state::{Config, HardState};
use crate::storage::Storage;
use crate::types::{
    Ballot, ClientId, ClientSeq, Command, ConfigId, Control, Entry, NodeId, SessionEntry, Slot,
    Value, command_fingerprint,
};
use crate::write::WriteOp;

/// Maximum accepted records carried by one [`Message::Promise`] page.
pub const PROMISE_BATCH: usize = 64;

/// Maximum recovered or gap-fill Phase-2 rounds started in one recovery pump.
pub const LEADER_RECOVERY_BATCH: usize = 64;

/// Election timeouts a leader's blocked repair probe may stay open before the
/// leader resigns (CTRL §4.2): a leader that cannot finish recovery — e.g.
/// partitioned from the only holder of a faulty slot's value — steps down so
/// another node can try. Multiplies the driver-supplied randomized election
/// timeout, so the effective window inherits its per-seed jitter.
pub const REPAIR_TIMEOUT_ELECTIONS: u64 = 3;

/// This node's role in the cluster. A read-only view for drivers / oracles.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum NodeRole {
    /// Following a (believed) leader; resets its election clock on leader traffic.
    #[default]
    Follower,
    /// Ran out of election timeout, bumped its ballot, gathering a Phase-1 quorum.
    Candidate,
    /// Holds a Phase-1 quorum for its ballot; streams Phase-2 `Accept`s per slot.
    Leader,
}

/// The outcome of [`RawNode::propose`], telling the driver how to answer the
/// client. The driver acks on commit: it holds the reply for `Accepted`/
/// `Duplicate` until that slot commits, redirects on `NotLeader`, and acks
/// immediately on `Chosen`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProposeResult {
    /// This node is not the leader; the client should retry the hinted node
    /// (`None` if leadership is currently unknown).
    NotLeader(Option<NodeId>),
    /// Newly admitted at this slot; ack when the slot commits.
    Accepted(Slot),
    /// A retry already in flight at this slot; ack when the slot commits.
    Duplicate(Slot),
    /// Already **applied** (inside this node's contiguous chosen prefix); the
    /// driver acks immediately (idempotent), reporting this slot.
    ///
    /// The slot is the one this client's *highest applied* command landed at —
    /// this command's own slot for the ordinary retry (a sequential client
    /// cannot be past the seq it is still retrying), and a slot at or above it
    /// otherwise. Either way it is inside the applied prefix, which is exactly
    /// what the immediate ack claims: the write is in the register the project
    /// defines. An ack naming a slot the node has not applied would be a
    /// linearizability violation, so this carries a slot rather than nothing —
    /// it makes the fast path checkable by the simulation's oracles instead of
    /// exempt from them.
    Chosen(Slot),
}

/// The outcome of [`RawNode::read_index`], telling the driver how to answer the
/// reading client: redirect on `NotLeader`, or park the reply and wait for the
/// matching [`ReadState`] to surface via [`Ready::read_states`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReadIndexResult {
    /// This node is not the leader; the client should retry the hinted node
    /// (`None` if leadership is currently unknown).
    NotLeader(Option<NodeId>),
    /// The round is running; a [`ReadState`] with this call's `ctx` surfaces via
    /// [`Ready::read_states`] once confirmed (possibly in the very next batch).
    /// A round that cannot confirm (leadership lost, acks lost) surfaces
    /// nothing — the driver owns the client-facing timeout.
    Pending,
}

/// A confirmed read-index round, surfaced via [`Ready::read_states`]: at the
/// moment the round began this node was leader (a heartbeat-ack quorum at its
/// ballot proved it afterwards) and `index` was covered by the applied prefix
/// by confirmation time — the linearization point a read at `ctx` observes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReadState {
    /// The driver-supplied correlation token from [`RawNode::read_index`].
    pub ctx: u64,
    /// The read index: the applied watermark the read observes (`None` = empty
    /// prefix). The node's chosen index is at or past it on confirmation.
    pub index: Option<Slot>,
}

/// The pure, synchronous, single-threaded Multi-Paxos state machine.
///
/// No I/O, no clock, no randomness. Inputs arrive via [`RawNode::step`] (peer
/// messages and tick-injected self-events), [`RawNode::tick`] (logical time),
/// and [`RawNode::propose`] (a client value). Output is drained via
/// [`RawNode::ready`] and acknowledged via [`Ready::advance`].
///
/// Stage 3 is **Multi-Paxos**: a per-slot replicated log with a stable leader.
/// A node times out (randomized election timeout supplied by the driver),
/// becomes a Candidate, runs **one** Phase 1 for its ballot over the whole log
/// suffix, and on a promise quorum becomes Leader: it re-proposes recovered
/// in-flight slots (gap fill) and then streams Phase-2 `Accept`s for fresh
/// client values. Heartbeats hold leadership; a `Nack` or a higher ballot makes
/// a node step down (the dueling-proposer livelock fix). Client requests are
/// deduplicated by `(ClientId, ClientSeq)` for at-most-once execution.
pub struct RawNode {
    /// This node's static identity and membership.
    config: Config,
    /// The must-be-durable scalars (promised ballot + chosen index), surfaced for
    /// persistence via [`Ready`].
    hard_state: HardState,
    /// The per-slot accepted log: the working copy the protocol reads (range
    /// scans for Phase-1 recovery / Phase-2 gap-fill). Volatile — rebuilt on boot
    /// from the durable log (see [`RawNode::new`]); durably persisted one record
    /// at a time via [`WriteOp::AppendAccepted`], never as a blob.
    accepted: BTreeMap<Slot, (Ballot, Command)>,
    /// The compaction floor: the first slot still retained. Everything below it
    /// has been truncated away (see [`RawNode::compact`]). Rebuilt on boot from
    /// [`Storage::first_slot`]. Always `<= first_unchosen()`: only chosen slots
    /// are ever dropped.
    first_slot: Slot,
    /// Recoverable **faulty entries** (Stage 8, CTRL): retained slots whose
    /// accepted value was lost to storage corruption but whose identity
    /// `(slot, accepted_ballot)` survived the boot scan
    /// ([`Storage::faulty_entries`]). Reported as the Promise tri-state's third
    /// answer (never as "nothing accepted here"), never served by catch-up, and
    /// repaired in place: a fresh `Accept` at or above the promise, a learned
    /// chosen value, or a snapshot install past the slot each clear the entry
    /// (fill or replace-with-proven-identical — repair never deletes promised-
    /// or accepted-ballot state). Disjoint from `accepted` at all times.
    faulty: BTreeMap<Slot, Ballot>,
    /// The **application repair cursor** (Stage 8): the first slot whose
    /// decided command the driver's application still needs, when the boot
    /// replay could not walk the whole chosen prefix (a faulty chosen record
    /// blocked it, or the application snapshot was lost below the compaction
    /// floor). While set, [`RawNode::advance_chosen_index`] defers surfacing
    /// committed entries to [`RawNode::pump_app_repair`], which re-emits them
    /// **in slot order from this cursor** so the application never applies out
    /// of order; the node pulls the missing range via catch-up (or a snapshot,
    /// when the cursor sits below the floor) each tick. `None` = fully healed.
    app_repair: Option<Slot>,

    // ---- pending output buckets: filled by the protocol logic, drained by
    // ---- `ready`, cleared by `advance`.
    /// Semantic durable write deltas produced this batch, in apply order.
    pending_writes: Vec<WriteOp>,
    pending_messages: Vec<(NodeId, Message)>,
    pending_committed: Vec<(Slot, Command)>,
    /// Snapshot offers to serve this batch:
    /// `(to, chosen_index, ballot, config_id)`. The core decides *who* needs a
    /// snapshot and *up to where* (a below-floor catch-up request), but holds no
    /// application state, so the driver attaches the opaque snapshot bytes (from
    /// storage) and sends the [`Message::InstallSnapshot`].
    pending_snapshot_offers: Vec<(NodeId, Slot, Ballot, ConfigId)>,
    /// Read-index rounds confirmed this batch, drained via
    /// [`Ready::read_states`] after the batch's committed entries are applied.
    pending_read_states: Vec<ReadState>,
    /// `(started, gap_fills, remaining)` for this Ready's recovery chunk.
    pending_recovery_batch: Option<(usize, usize, usize)>,

    /// Logical clock, advanced by [`RawNode::tick`].
    tick_count: u64,

    // ---- leadership / election (all volatile) ----
    /// Current role.
    role: NodeRole,
    /// The node we currently believe is leader (`None` = unknown / electing).
    leader: Option<NodeId>,
    /// The ballot this node operates under as Candidate/Leader (and the highest
    /// leader ballot it has adopted as a Follower).
    ballot: Ballot,
    /// Ticks since the last leader contact (reset on `Prepare`/`Accept`/
    /// `Heartbeat`/`Commit` at a ballot `>=` ours, and on becoming Leader).
    election_elapsed: u64,
    /// Driver-supplied randomized election timeout, in ticks. `0` disables the
    /// election clock (the sentinel until the driver seeds one).
    election_timeout: u64,
    /// Set when the election clock resets (fired or stepped down); the driver
    /// reads it to feed a fresh randomized `election_timeout`. Jitter is drawn in
    /// the driver, never here (the core stays zero-dep).
    needs_election_timeout: bool,
    /// Ticks since the leader last beat. Leader-only.
    heartbeat_elapsed: u64,
    /// Fixed heartbeat interval in ticks (not randomized).
    heartbeat_timeout: u64,
    /// Monotone per-ballot beat sequence, bumped at each broadcast
    /// ([`RawNode::broadcast_heartbeat`]); reset on winning an election. Acks
    /// echo it, so a read round knows which beats prove leadership *after* it
    /// began.
    heartbeat_seq: u64,
    /// `CheckQuorum` (#95): ticks since the leader last proved it can reach an
    /// ack quorum. Leader-only, reset when the window closes with a quorum.
    quorum_elapsed: u64,
    /// `CheckQuorum`: the distinct peers (incl. self) whose ballot-matching
    /// `HeartbeatAck` or `Accepted` arrived inside the current window.
    quorum_acked_by: BTreeSet<NodeId>,
    /// Monotone count of `CheckQuorum` step-downs this incarnation, for the
    /// driver's audit report (mirrors `duplicates_suppressed`).
    quorum_lost_step_downs: u64,

    // ---- linearizable reads (all volatile: a read round carries no durable
    // ---- obligation — losing one merely fails an RPC whose reply promise dies
    // ---- with the process, and the client retries) ----
    /// The fresh-leader read fence: the highest slot the winning prepare quorum
    /// reported (`next_slot - 1` at election). Everything a previous leader may
    /// have acked sits at or below it (quorum intersection + the Prepare floor
    /// guard), so no read round confirms until the chosen prefix covers it —
    /// Raft's "no-op at term start" problem, solved by waiting instead.
    read_floor: Option<Slot>,
    /// In-flight read-index rounds, in creation order (leader only).
    read_rounds: Vec<ReadRound>,

    // ---- proposer (multi-decree) ----
    /// Per-slot in-flight Phase-2 rounds, keyed by slot. The leader streams these.
    proposer: BTreeMap<Slot, Proposing>,
    /// Phase-1 (per-ballot) recovery state while a Candidate. `None` once Leader.
    election: Option<Election>,
    /// The leader's open **distributed commitment determination** (Stage 8,
    /// CTRL): faulty slots the winning quorum could resolve neither as Case 1
    /// (some `have`) nor Case 2 (a full Q1 of `none`). The leader keeps
    /// querying stragglers at its ballot until each blocked slot resolves, and
    /// resigns after a recovery timeout so another node can try (CTRL §4.2).
    /// Leader-only, volatile.
    repair_probe: Option<RepairProbe>,
    /// Ticks the current `repair_probe` has been open. Reset when the probe
    /// closes; drives the recovery-timeout resignation.
    repair_elapsed: u64,
    /// Monotone count of recovery-timeout step-downs this incarnation (a leader
    /// resigning because it could not finish repairing its blocked slots).
    repair_step_downs: u64,
    /// Monotone count of local faulty records repaired in place this
    /// incarnation (a fresh accept, a learned chosen value, or a snapshot
    /// install clearing a `faulty` entry).
    faulty_repaired: u64,
    /// Cumulative payload bytes shipped into local repairs (the CTRL §5.2
    /// repair-cost metric: a protocol-aware repair moves one entry, not the
    /// log).
    repair_bytes: u64,
    /// Monotone count of blocked slots resolved as Case 1 (re-proposed from a
    /// straggler's `have`) after the election closed.
    repair_case1: u64,
    /// Monotone count of blocked slots resolved as Case 2 (a full Q1 of `none`
    /// assembled from stragglers; decided `Noop`).
    repair_case2: u64,
    /// Remaining bounded recovery work for the current leadership.
    leader_recovery: Option<LeaderRecovery>,
    /// The bounded chosen-prefix walk stopped with another contiguous slot ready.
    chosen_advance_pending: bool,
    /// Fair cursor for bounded pending-Accept re-sends.
    resend_cursor: Option<Slot>,
    /// Next slot the leader allocates to a fresh client proposal.
    next_slot: Slot,
    /// How many undecided holes this node filled with a [`Control::Noop`] when it
    /// won its *current* leadership (0 until it wins one, and re-set at each
    /// election). Purely observational: the driver reads it on the transition to
    /// Leader and surfaces it, so the simulation can prove the gap-fill path is
    /// genuinely reached rather than merely present.
    election_gap_fills: u64,

    // ---- learner / dedup ----
    /// Commands this node has learned are chosen, per slot. Volatile.
    chosen: BTreeMap<Slot, Command>,
    /// Highest applied `ClientSeq` per client (for at-most-once dedup), with the
    /// slot that command landed at so the dedup fast path can *name* the slot it
    /// acks (see [`ProposeResult::Chosen`]). Rebuilt from `HardState` on
    /// construction.
    ///
    /// Written **only** from the contiguous walk in
    /// [`RawNode::advance_chosen_index`] (and the equivalent boot rebuild), so
    /// an entry here always means "inside this node's applied prefix" — never
    /// merely "chosen somewhere above the prefix". That is the whole difference
    /// between an honest immediate ack and one the client cannot trust.
    ///
    /// Per client it maps **each executed seq** to its slot, not merely the
    /// latest `(seq, slot)`: a `seq <= latest` shortcut would assume a client's
    /// seqs execute in order, and they do not — an early seq can die without
    /// ever entering the log (a `NotLeader` window, a lost round the gap fill
    /// paved over) while a later seq applies, and the shortcut then acked the
    /// dead command as committed, at another command's slot (the network-axis
    /// seeds 2791878389799639169 / 8872503201755490526). An exact-seq hit is
    /// the only honest `Chosen`; a miss falls through and executes the retry
    /// for real. Volatile and rebuilt from the remaining log on boot, so the
    /// truncation-across-restart window (see `propose`) is unchanged in kind.
    applied_seq: BTreeMap<ClientId, BTreeMap<ClientSeq, Slot>>,
    /// Client requests mapped to the slot they will land in, so a retry dedups
    /// against that slot instead of allocating a second one. Covers the whole
    /// span before the command is applied: proposed at a slot
    /// ([`RawNode::propose`]), recovered into one by a new leader, or already
    /// *chosen* at one but not yet in the contiguous prefix
    /// ([`RawNode::mark_chosen`]). The contiguous walk hands each entry over to
    /// `applied_seq` when its slot applies. Rebuilt from `HardState` on
    /// construction.
    inflight: BTreeMap<(ClientId, ClientSeq), Slot>,
    /// Chosen slots whose `Command::User` identity had **already applied at a
    /// lower slot** when the contiguous walk reached them — the double-apply
    /// #94 makes reachable: correct Paxos can choose one `(client, seq)` at two
    /// slots (a client retry across a partition lands on the majority while the
    /// deposed leader's lone accept survives above the cluster prefix, and a
    /// later election's mandatory P2c re-proposal decides it again). The apply
    /// seam surfaces these slots as a [`Control::Noop`] instead of executing
    /// the duplicate; membership is derived purely from the replicated ledger
    /// (walk order + sealed sessions), so every node — and every restart
    /// ([`RawNode::new`] re-derives this set) — makes the identical decision.
    duplicate_slots: BTreeSet<Slot>,
    /// Monotone count of duplicate suppressions performed by the contiguous
    /// walk this incarnation (boot-rebuild detections excluded). Observability
    /// only: the driver reads the delta after each batch and reports it through
    /// its audit port, so the simulation can prove the at-most-once suppression
    /// path is genuinely reached.
    duplicates_suppressed: u64,
}

impl RawNode {
    /// Construct from a read-only [`Storage`] by reading durable state back in.
    /// Bootstrap and restart share this path. The volatile dedup tables
    /// (`applied_seq`, `inflight`) and the `chosen` map are rebuilt from the
    /// durable `accepted` log and `chosen_index`.
    ///
    /// # Panics
    ///
    /// If the configuration is malformed (membership not sorted/deduplicated,
    /// or missing this node's own id) or the durable state violates the write
    /// ordering contract (a floor past the chosen prefix). A broken invariant
    /// here means corrupted storage or a broken storage implementation;
    /// crashing beats running on it.
    #[allow(clippy::too_many_lines)]
    pub fn new<S: Storage>(storage: &S) -> Self {
        let (hard_state, config) = storage.initial_state();
        // Config shape: quorum arithmetic and broadcast both assume a strictly
        // sorted, deduplicated membership that includes this node. A duplicated
        // peer silently inflates the quorum; a missing self silently deflates it.
        assert!(
            !config.peers.is_empty(),
            "membership includes at least self"
        );
        assert!(
            config.peers.windows(2).all(|w| w[0] < w[1]),
            "membership is sorted and deduplicated"
        );
        assert!(
            config.peers.binary_search(&config.id).is_ok(),
            "membership includes this node's own id"
        );
        let ballot = hard_state.max_promised_ballot;

        // Rebuild the working accepted log by scanning the durable per-slot log
        // (first_slot..=last_slot); gaps read back as `None` and are skipped.
        let mut accepted: BTreeMap<Slot, (Ballot, Command)> = BTreeMap::new();
        let first_slot = storage.first_slot();
        let (first, last) = (first_slot.0, storage.last_slot().0);
        for s in first..=last {
            if let Some(record) = storage.accepted(Slot(s)) {
                accepted.insert(Slot(s), record);
            }
        }

        let mut chosen = BTreeMap::new();
        // The at-most-once ledger starts from the durable **sealed** records —
        // the `(client, seq) -> slot` facts whose log records truncation (or a
        // snapshot install) already dropped — and the walk over the retained log
        // below layers on top with first-slot-wins semantics. Sealed slots are
        // always below the compaction floor, so the two sources never disagree;
        // seeding sealed first is what keeps a restarted (or snapshot-recovered)
        // node's duplicate-suppression decisions identical to a node that held
        // the whole log in memory (#94).
        let mut applied_seq: BTreeMap<ClientId, BTreeMap<ClientSeq, Slot>> = BTreeMap::new();
        for (client, seq, slot) in storage.sealed_sessions() {
            applied_seq.entry(client).or_default().insert(seq, slot);
        }
        let mut inflight = BTreeMap::new();
        let mut duplicate_slots = BTreeSet::new();
        for (slot, (_b, command)) in &accepted {
            let is_chosen = hard_state.chosen_index.is_some_and(|ci| *slot <= ci);
            if is_chosen {
                chosen.insert(*slot, command.clone());
                // Only client entries carry a `(client, seq)` dedup key; a control
                // command never dedups. Every executed seq is recorded, not just
                // the latest per client (see the `applied_seq` field doc) — and
                // only at its **first** (lowest) slot: a second chosen slot for
                // the same identity is the #94 duplicate, re-derived here exactly
                // as the live walk derived it, so the boot replay suppresses the
                // same slots the pre-restart apply did.
                if let Command::User(entry) = command {
                    let seqs = applied_seq.entry(entry.client).or_default();
                    match seqs.get(&entry.seq) {
                        Some(&first) if first != *slot => {
                            duplicate_slots.insert(*slot);
                        }
                        _ => {
                            seqs.insert(entry.seq, *slot);
                        }
                    }
                }
            } else if let Command::User(entry) = command {
                inflight.insert((entry.client, entry.seq), *slot);
            }
        }
        // The boot scan's recoverable faulty entries (Stage 8): value lost,
        // identity known. They are *records this node accepted*, so they bound
        // `next_slot` exactly like readable records; they are simply unreadable
        // and reported as `faulty` instead of `have`. Nothing below the floor is
        // retained, and a slot never appears in both maps.
        let mut faulty: BTreeMap<Slot, Ballot> = BTreeMap::new();
        for (slot, ballot) in storage.faulty_entries() {
            if slot < first_slot {
                continue;
            }
            assert!(
                !accepted.contains_key(&slot),
                "a faulty entry is never also a readable accepted record"
            );
            faulty.insert(slot, ballot);
        }

        // Next free slot: one past the highest accepted entry (readable or
        // faulty), or (when the log is empty, e.g. fully truncated) one past
        // the durable chosen index.
        let first_unchosen = hard_state.chosen_index.map_or(Slot(0), |ci| Slot(ci.0 + 1));
        let next_slot = accepted
            .keys()
            .chain(faulty.keys())
            .max()
            .map_or(first_unchosen, |s| Slot(s.0 + 1))
            .max(first_unchosen);
        // Trust-boundary re-assertion of the durable write ordering: a flushed
        // floor never outruns the flushed chosen index (only chosen slots are
        // truncated), and a durable chosen index implies its accept was flushed
        // in the same or an earlier sync, so the rebuilt `next_slot` sits at or
        // past the first unchosen slot.
        assert!(
            first_slot <= first_unchosen,
            "the durable floor never outruns the durable chosen index"
        );
        assert!(
            next_slot >= first_unchosen,
            "the rebuilt next slot never falls inside the chosen prefix"
        );

        let node = Self {
            config,
            hard_state,
            accepted,
            first_slot,
            faulty,
            app_repair: None,
            pending_writes: Vec::new(),
            pending_messages: Vec::new(),
            pending_committed: Vec::new(),
            pending_snapshot_offers: Vec::new(),
            pending_read_states: Vec::new(),
            pending_recovery_batch: None,
            tick_count: 0,
            role: NodeRole::Follower,
            leader: None,
            ballot,
            election_elapsed: 0,
            election_timeout: 0,
            needs_election_timeout: true,
            heartbeat_elapsed: 0,
            heartbeat_timeout: HEARTBEAT_TICKS,
            heartbeat_seq: 0,
            quorum_elapsed: 0,
            quorum_acked_by: BTreeSet::new(),
            quorum_lost_step_downs: 0,
            read_floor: None,
            read_rounds: Vec::new(),
            proposer: BTreeMap::new(),
            election: None,
            repair_probe: None,
            repair_elapsed: 0,
            repair_step_downs: 0,
            faulty_repaired: 0,
            repair_bytes: 0,
            repair_case1: 0,
            repair_case2: 0,
            leader_recovery: None,
            chosen_advance_pending: false,
            resend_cursor: None,
            next_slot,
            election_gap_fills: 0,
            chosen,
            applied_seq,
            inflight,
            duplicate_slots,
            duplicates_suppressed: 0,
        };
        node.assert_invariants();
        node
    }

    /// Assert every cross-field invariant of the node's volatile state. All
    /// checks are O(1) or O(log n) (min-key probes), so this runs
    /// unconditionally — TigerBeetle-style — at boot and at the exit of every
    /// public mutating entry point.
    fn assert_invariants(&self) {
        // Ordering chain: only chosen slots are ever dropped, so the compaction
        // floor never passes the first unchosen slot.
        assert!(
            self.first_slot <= self.first_unchosen(),
            "the compaction floor never outruns the chosen prefix"
        );
        // A chosen first-unchosen slot is legal only as the explicit bounded
        // continuation left by `advance_chosen_index`.
        assert!(
            self.chosen.contains_key(&self.first_unchosen()) == self.chosen_advance_pending,
            "a chosen first-unchosen slot has a deferred prefix continuation"
        );
        // Floor bounds: nothing below the floor survives in any slot map.
        assert!(
            self.accepted
                .keys()
                .next()
                .is_none_or(|s| *s >= self.first_slot),
            "no accepted record survives below the compaction floor"
        );
        assert!(
            self.faulty
                .keys()
                .next()
                .is_none_or(|s| *s >= self.first_slot),
            "no faulty entry survives below the compaction floor"
        );
        // The tri-state is a partition: a slot is readable, faulty, or absent —
        // never two at once (O(N∩) structural check, so debug-only).
        debug_assert!(
            self.faulty.keys().all(|s| !self.accepted.contains_key(s)),
            "the faulty set stays disjoint from the accepted log"
        );
        // The application repair cursor only ever points inside the chosen
        // prefix (there is nothing decided to re-emit past it).
        assert!(
            self.app_repair.is_none_or(|s| s < self.first_unchosen()),
            "the application repair cursor stays inside the chosen prefix"
        );
        assert!(
            self.chosen
                .keys()
                .next()
                .is_none_or(|s| *s >= self.first_slot),
            "no chosen record survives below the compaction floor"
        );
        assert!(
            self.proposer
                .keys()
                .next()
                .is_none_or(|s| *s >= self.first_slot),
            "no in-flight round survives below the compaction floor"
        );
        // Role couplings. Note "leader ballot >= own promise" is deliberately
        // NOT a global invariant: a still-Leader node can learn a higher-ballot
        // `Commit` (raising its promise via `mark_chosen`) before any deposing
        // message arrives — `start_accept_round`'s self-accept guard is the
        // designed defense. It holds only for a *fresh* leader
        // (see `try_become_leader`).
        match self.role {
            NodeRole::Leader => {
                assert!(self.election.is_none(), "a leader has no open campaign");
                assert!(
                    self.leader == Some(self.config.id),
                    "a leader knows itself as leader"
                );
            }
            NodeRole::Candidate => {
                assert!(
                    self.election
                        .as_ref()
                        .is_some_and(|e| e.ballot == self.ballot),
                    "a candidate's campaign runs at its own operating ballot"
                );
            }
            NodeRole::Follower => {
                assert!(self.election.is_none(), "a follower has no open campaign");
            }
        }
        // Volatile leadership state exists only on a leader.
        if !self.proposer.is_empty() {
            assert!(
                self.role == NodeRole::Leader,
                "only a leader holds in-flight accept rounds"
            );
        }
        if !self.read_rounds.is_empty() {
            assert!(
                self.role == NodeRole::Leader,
                "only a leader holds pending read rounds"
            );
        }
        if self.leader_recovery.is_some() {
            assert!(
                self.role == NodeRole::Leader,
                "only a leader holds deferred recovery work"
            );
        }
        if self.repair_probe.is_some() {
            assert!(
                self.role == NodeRole::Leader,
                "only a leader holds an open repair probe"
            );
        }
    }

    /// The single input entry point: every stimulus is a [`Message`], routed by
    /// variant and role. Tick-injected self-events (`CheckLeader`/`Heartbeat`)
    /// enter here too.
    ///
    /// # Panics
    ///
    /// Panics if a ballot-bearing protocol message names a configuration other
    /// than this node's durable configuration, or if processing exposes a
    /// broken internal invariant.
    pub fn step(&mut self, msg: Message) {
        assert!(
            msg.config_id()
                .is_none_or(|config_id| config_id == self.hard_state.config_id),
            "a protocol message matches the local durable configuration"
        );
        match msg {
            Message::Prepare {
                from,
                ballot,
                from_slot,
                ..
            } => self.on_prepare(from, ballot, from_slot),
            Message::Promise {
                from,
                ballot,
                from_slot,
                accepted,
                faulty,
                next_from_slot,
                ..
            } => self.on_promise(from, ballot, from_slot, accepted, faulty, next_from_slot),
            Message::Accept {
                from,
                ballot,
                slot,
                command,
                ..
            } => self.on_accept(from, ballot, slot, command),
            Message::Accepted {
                from,
                ballot,
                slot,
                vhash,
                ..
            } => self.on_accepted(from, ballot, slot, vhash),
            Message::Nack {
                from,
                ballot,
                promised,
                slot,
                ..
            } => self.on_nack(from, ballot, promised, slot),
            Message::Commit {
                ballot,
                slot,
                command,
                ..
            } => self.on_commit(ballot, slot, &command),
            Message::CatchUpRequest { from, from_slot } => self.on_catchup_request(from, from_slot),
            Message::CatchUpResponse { entries, .. } => self.on_catchup_response(entries),
            Message::InstallSnapshot {
                ballot,
                chosen_index,
                snapshot,
                sessions,
                ..
            } => self.on_install_snapshot(ballot, chosen_index, snapshot, sessions),
            Message::CheckLeader { .. } => self.on_check_leader(),
            Message::Heartbeat {
                from,
                ballot,
                commit,
                seq,
                ..
            } => self.on_heartbeat(from, ballot, commit, seq),
            Message::HeartbeatAck {
                from, ballot, seq, ..
            } => {
                self.on_heartbeat_ack(from, ballot, seq);
            }
            // Driver-terminal snapshot-repair traffic (CTRL §3.5): the
            // driver's repair layer owns these end to end and normally
            // intercepts them before `step`. Consensus state never depends on
            // snapshot custody, so a message that does reach the core is
            // deliberately ignored rather than an error.
            Message::SnapAck { .. }
            | Message::SnapChunkRequest { .. }
            | Message::SnapChunkResponse { .. } => {}
        }
        self.assert_invariants();
    }

    /// Client entry point: try to get `value` chosen, deduplicated by
    /// `(client, seq)`. Only the leader admits proposals; a non-leader returns
    /// [`ProposeResult::NotLeader`] with a redirect hint.
    pub fn propose(&mut self, client: ClientId, seq: ClientSeq, value: Value) -> ProposeResult {
        if self.role != NodeRole::Leader {
            return ProposeResult::NotLeader(self.leader);
        }
        // The applied ledger outranks the in-flight table: once an identity has
        // applied, the honest answer is `Chosen` at its first slot, even while a
        // #94 duplicate of it sits chosen-but-unapplied at a later slot (that
        // slot will suppress to a no-op at apply, so a reply parked on it would
        // hang to the client's deadline). While an application repair is open,
        // the ledger's slots are chosen but not yet re-applied here, so the
        // immediate `Chosen` ack would name a slot outside the applied prefix —
        // fall through to the honest slower paths instead.
        if self.app_repair.is_none()
            && let Some(&at) = self.applied_seq.get(&client).and_then(|m| m.get(&seq))
        {
            return ProposeResult::Chosen(at);
        }
        if let Some(&slot) = self.inflight.get(&(client, seq)) {
            return ProposeResult::Duplicate(slot);
        }
        let slot = self.next_slot;
        self.next_slot = Slot(slot.0 + 1);
        let entry = Entry { client, seq, value };
        self.inflight.insert((client, seq), slot);
        self.start_accept_round(slot, Command::User(entry));
        self.assert_invariants();
        ProposeResult::Accepted(slot)
    }

    /// Leader entry point for a **control command**: get `control` chosen into the
    /// next log slot by ordinary Paxos. Only the leader admits it; a non-leader
    /// returns [`ProposeResult::NotLeader`] with a redirect hint.
    ///
    /// A control command carries no `(client, seq)` and so is never deduplicated;
    /// each proposal takes a fresh slot. It is a normal Phase-2 round from the
    /// acceptors' point of view (they store it opaquely, exactly like a client
    /// entry). Its *effect* — for [`Control::Truncate`], dropping the log prefix —
    /// is applied lazily by every node when the slot enters its contiguous chosen
    /// prefix (see [`RawNode::advance_chosen_index`]).
    pub fn propose_control(&mut self, control: Control) -> ProposeResult {
        if self.role != NodeRole::Leader {
            return ProposeResult::NotLeader(self.leader);
        }
        let slot = self.next_slot;
        self.next_slot = Slot(slot.0 + 1);
        self.start_accept_round(slot, Command::Control(control));
        self.assert_invariants();
        ProposeResult::Accepted(slot)
    }

    /// Leader entry point for a **decided snapshot point** (#101, CTRL §3.5):
    /// propose a [`Control::Snap`] marker into the next slot, with `at_index`
    /// bound to exactly that slot **by construction** — Paxos never moves an
    /// accepted command between slots, so a decided marker always describes
    /// its own position. A non-leader returns [`ProposeResult::NotLeader`].
    pub fn propose_snap_marker(&mut self) -> ProposeResult {
        if self.role != NodeRole::Leader {
            return ProposeResult::NotLeader(self.leader);
        }
        let slot = self.next_slot;
        self.next_slot = Slot(slot.0 + 1);
        self.start_accept_round(slot, Command::Control(Control::Snap { at_index: slot }));
        self.assert_invariants();
        ProposeResult::Accepted(slot)
    }

    /// Leader entry point for a **linearizable read**: capture the current
    /// applied watermark as the read index and start a heartbeat-ack quorum
    /// round to confirm this node is still leader — no log write. The confirmed
    /// round surfaces as a [`ReadState`] carrying `ctx` via
    /// [`Ready::read_states`], once a quorum has acked a beat broadcast at or
    /// after this call **and** the chosen prefix covers the captured index
    /// (the fresh-leader fence, see the `read_floor` field). A non-leader
    /// returns [`ReadIndexResult::NotLeader`] with a redirect hint.
    ///
    /// # Panics
    ///
    /// If an internal invariant is broken (a programmer error, never an
    /// operating condition): read rounds must confirm in creation order.
    pub fn read_index(&mut self, ctx: u64) -> ReadIndexResult {
        if self.role != NodeRole::Leader {
            return ReadIndexResult::NotLeader(self.leader);
        }
        // The fence dominates: a fresh leader must not serve below the highest
        // slot its prepare quorum reported, even while its own chosen prefix
        // still lags the recovered suffix.
        let index = self.hard_state.chosen_index.max(self.read_floor);
        // Beat immediately (rather than waiting for the next tick) so the
        // round's confirmation costs one network round trip, not a tick.
        self.broadcast_heartbeat();
        let mut acked_by = BTreeSet::new();
        // `QuorumSystem::Majority` is the load-bearing reason the leader may
        // count its own real acceptor vote unconditionally: a stale leader can
        // collect at most `q - 1` peer acks once an intersecting majority has
        // promised higher. A future asymmetric quorum system must replace this
        // cardinality check and explicit own vote with read-quorum membership.
        acked_by.insert(self.config.id);
        self.read_rounds.push(ReadRound {
            ctx,
            index,
            required_seq: self.heartbeat_seq,
            acked_by,
            created_tick: self.tick_count,
        });
        // `try_confirm_reads` front-scans on the premise that creation order is
        // monotone in both the index and the required beat; pin it at the only
        // place a round is created (O(1): the last two entries).
        if let [.., prev, last] = self.read_rounds.as_slice() {
            assert!(
                prev.index <= last.index,
                "read rounds are created with monotone indexes"
            );
            assert!(
                prev.required_seq <= last.required_seq,
                "read rounds are created with monotone required beats"
            );
        }
        // A single-node cluster is its own quorum: confirm in this same batch.
        self.try_confirm_reads();
        self.assert_invariants();
        ReadIndexResult::Pending
    }

    /// Application-driven log compaction: drop every retained slot at or below
    /// `up_to`, raising the truncation floor. Returns the new floor (the first
    /// slot still retained).
    ///
    /// `up_to` is the last slot the application permits dropping (inclusive); the
    /// floor stored in the log is the first slot *retained*. The request is
    /// clamped to the contiguous chosen prefix (`up_to.min(chosen_index)`): a slot
    /// that is not yet chosen is never dropped, so nothing undecided is lost.
    /// Clamping makes the call safe to over-request ("drop as much as you may, up
    /// to N") and idempotent (a floor never moves backward). With nothing chosen,
    /// or when the floor would not rise, it is a no-op that emits no
    /// [`WriteOp::Truncate`].
    ///
    /// Truncation does **not** shrink the at-most-once dedup window: the ledger
    /// records whose slots this call drops are *sealed* — emitted on the
    /// [`WriteOp::Truncate`] for durable persistence and read back through
    /// [`Storage::sealed_sessions`] on the next boot — so a restart recognizes a
    /// truncated `(client, seq)` exactly like a node that never restarted, and
    /// the #94 duplicate-suppression decision stays cluster-consistent. (The
    /// sealed ledger grows with distinct client identities for the lifetime of
    /// the cluster; bounding it needs a client-session expiry policy, which is
    /// out of scope here.)
    ///
    /// # Panics
    ///
    /// If an internal invariant is broken (a programmer error, never an
    /// operating condition): the floor must rise monotonically and stay
    /// clamped inside the chosen prefix.
    pub fn compact(&mut self, up_to: Slot) -> Slot {
        let Some(ci) = self.hard_state.chosen_index else {
            return self.first_slot;
        };
        let mut highest_drop = up_to.min(ci);
        // An open application repair pins the floor: truncating at or past the
        // repair cursor would drop the very records the catch-up heal is about
        // to re-emit, converting a one-slot repair into a snapshot transfer (or
        // an unrecoverable wait). The floor resumes rising once the repair
        // closes; a decided `Truncate` is idempotent over-asking by design.
        if let Some(pending) = self.app_repair {
            let Some(cap) = pending.0.checked_sub(1) else {
                return self.first_slot;
            };
            highest_drop = highest_drop.min(Slot(cap));
        }
        let first = Slot(highest_drop.0 + 1).max(self.first_slot);
        if first <= self.first_slot {
            return self.first_slot;
        }
        let old_floor = self.first_slot;
        // Seal from the *ledger*, not from the dropped `chosen` range: a
        // duplicate slot's chosen command is a `User` entry whose ledger record
        // points at its first slot, and sealing the duplicate's own slot would
        // corrupt the ledger. Only the delta is sealed — records below the old
        // floor were sealed by the truncation (or install) that dropped them.
        let sealed: Vec<SessionEntry> = self
            .applied_seq
            .iter()
            .flat_map(|(client, seqs)| {
                seqs.iter()
                    .filter(|entry| *entry.1 >= self.first_slot && *entry.1 < first)
                    .map(|(&seq, &slot)| (*client, seq, slot))
            })
            .collect();
        self.accepted = self.accepted.split_off(&first);
        self.chosen = self.chosen.split_off(&first);
        // A faulty entry below the floor is superseded by the compacted state
        // (only chosen slots are dropped, and truncation is decided over the
        // applied prefix): custodianship moved into the application snapshot.
        self.faulty = self.faulty.split_off(&first);
        self.chosen_advance_pending = self.chosen.contains_key(&self.first_unchosen());
        self.proposer.retain(|slot, _| *slot >= first);
        self.first_slot = first;
        self.pending_writes
            .push(WriteOp::Truncate { first, sealed });
        // Postconditions: the floor strictly rose (the no-op path returned
        // above) and stayed clamped inside the chosen prefix.
        assert!(self.first_slot > old_floor, "compaction raised the floor");
        assert!(
            self.first_slot <= self.first_unchosen(),
            "compaction never drops an undecided slot"
        );
        self.assert_invariants();
        self.first_slot
    }

    /// Advance logical time by one tick, synthesizing `CheckLeader`/`Heartbeat`
    /// self-events when the election / heartbeat counters cross their thresholds.
    ///
    /// Re-sending a leader's still-pending `Accept`s is deliberately *not* part of
    /// this: it is a separate decision on the same cadence, so the driver can skip
    /// it (see [`RawNode::resend_pending`]).
    pub fn tick(&mut self) {
        self.tick_count += 1;
        let me = self.config.id;
        if self.role == NodeRole::Leader {
            self.heartbeat_elapsed += 1;
            if self.heartbeat_elapsed >= self.heartbeat_timeout {
                self.heartbeat_elapsed = 0;
                self.step(Message::Heartbeat {
                    config_id: self.hard_state.config_id,
                    from: me,
                    ballot: self.ballot,
                    commit: self.hard_state.chosen_index,
                    seq: 0,
                });
            }
            // GC read rounds that outlived their TTL (lost acks, an unreachable
            // quorum). No re-broadcast logic is needed for the live ones: every
            // leader tick already broadcasts a fresh, higher-seq beat whose acks
            // confirm all older pending rounds.
            let now = self.tick_count;
            self.read_rounds
                .retain(|r| now.saturating_sub(r.created_tick) <= READ_ROUND_TTL_TICKS);
            // CheckQuorum (#95): a leader must re-prove, once per election
            // timeout, that an ack quorum can still reach it. Without this, an
            // idle leader cut off from its quorum stays Leader forever — its
            // election clock is frozen (the branch below runs only for
            // non-leaders), below-promise beats are ignored unacked rather than
            // Nacked, and an idle leader emits no `Accept`s whose Nack could
            // demote it — while it keeps admitting proposals into a stale
            // suffix for the whole partition, feeding #94's double-apply. The
            // window is the same length as the election timeout (etcd-raft's
            // CheckQuorum), so a demoted leader's peers are already eligible to
            // campaign by the time it steps down. Every beat is acked by every
            // reachable follower each tick, so a healthy leader trivially
            // refills the window.
            if self.election_timeout != 0 {
                self.quorum_elapsed += 1;
                if self.quorum_elapsed >= self.election_timeout {
                    if self.quorum_acked_by.len() >= self.quorum() {
                        self.quorum_elapsed = 0;
                        self.quorum_acked_by.clear();
                        self.quorum_acked_by.insert(me);
                    } else {
                        self.quorum_lost_step_downs += 1;
                        self.become_follower(None);
                    }
                }
            }
        } else {
            self.election_elapsed += 1;
            if self.election_timeout != 0 && self.election_elapsed >= self.election_timeout {
                self.election_elapsed = 0;
                self.needs_election_timeout = true;
                self.step(Message::CheckLeader { from: me });
            }
        }
        self.tick_repair();
        self.assert_invariants();
    }

    /// Per-tick repair upkeep (Stage 8): drive the leader's open repair probe
    /// (straggler re-query + the CTRL §4.2 recovery-timeout resignation) and
    /// pull the application repair range from peers.
    fn tick_repair(&mut self) {
        // The leader's blocked-slot probe: re-send `Prepare` at our ballot to
        // every peer that has not yet answered its full suffix, once per tick
        // (the heartbeat cadence — a straggler that was down or partitioned
        // when the campaign's Prepare went out only ever answers a re-send). A
        // probe that stays blocked for a full recovery timeout resigns: another
        // node — possibly one holding the missing copy — gets to try.
        if self.role == NodeRole::Leader && self.repair_probe.is_some() {
            self.repair_elapsed += 1;
            let timeout = self
                .election_timeout
                .saturating_mul(REPAIR_TIMEOUT_ELECTIONS);
            if self.election_timeout != 0 && self.repair_elapsed >= timeout {
                self.repair_step_downs += 1;
                self.become_follower(None);
            } else {
                let (ballot, from_slot, unanswered) = {
                    let probe = self.repair_probe.as_ref().expect("checked above");
                    let unanswered: Vec<NodeId> = self
                        .config
                        .peers
                        .iter()
                        .copied()
                        .filter(|p| *p != self.config.id && !probe.answered.contains(p))
                        .collect();
                    (probe.ballot, probe.from_slot, unanswered)
                };
                for to in unanswered {
                    self.pending_messages.push((
                        to,
                        Message::Prepare {
                            config_id: self.hard_state.config_id,
                            from: self.config.id,
                            ballot,
                            from_slot,
                        },
                    ));
                }
            }
        }
        // The application repair pull: ask every peer for the decided range
        // from the cursor. A peer that still holds the slots serves a
        // catch-up replay; one that truncated past them offers a snapshot.
        // Once per tick — the same cadence heartbeat-driven catch-up uses.
        if let Some(from_slot) = self.app_repair {
            self.broadcast(&Message::CatchUpRequest {
                from: self.config.id,
                from_slot,
            });
        } else if let Some(first_faulty) = self
            .faulty
            .keys()
            .next()
            .copied()
            .filter(|slot| *slot < self.first_unchosen())
        {
            // A faulty **chosen** record whose effect the application already
            // holds still leaves a hole in the servable log (catch-up replay
            // stops at it — per-slot attribution). Pull the decided range from
            // peers so the record itself heals; a peer that has it chosen
            // serves it, and this node's own next election covers it either
            // way (the campaign range starts at the first faulty slot).
            self.broadcast(&Message::CatchUpRequest {
                from: self.config.id,
                from_slot: first_faulty,
            });
        }
    }

    /// Re-broadcast a fair bounded page of this leader's in-flight `Accept`
    /// rounds. A no-op on a node that is not the leader, and on a leader with
    /// nothing pending.
    ///
    /// **The driver is expected to call this on each heartbeat beat**, right after
    /// [`RawNode::tick`] — that is what lets a peer that lost the original
    /// `Accept` (or was down when it went out) catch up without waiting for an
    /// election.
    ///
    /// **Skipping a call is always safe.** Re-sending is pure optimization:
    /// nothing in Paxos safety depends on it, because the round's first broadcast
    /// already went out and a round that never gathers a quorum is simply
    /// *undecided*, which is a state the protocol is built to survive. A skipped
    /// round stalls until a later call re-sends it, or until an election recovers
    /// it. That is precisely why this is a *method* rather than something `tick`
    /// does implicitly: the decision to skip is the one whose rare omission makes
    /// the #54 election hole reachable — an undecided slot sitting *below* a
    /// decided one, which no environmental fault can produce on its own (a
    /// partition takes slots away in contiguous runs, never one here and one
    /// there) and which the `Control::Noop` gap fill exists to close. The
    /// deterministic simulation drives exactly that by skipping calls; production
    /// never skips.
    pub fn resend_pending(&mut self) {
        if self.role != NodeRole::Leader {
            return;
        }
        let me = self.config.id;
        let start = self.resend_cursor.unwrap_or(self.first_slot);
        let mut pending: Vec<(Slot, Ballot, Command)> = self
            .proposer
            .range(start..)
            .take(LEADER_RECOVERY_BATCH)
            .map(|(s, p)| (*s, p.ballot, p.command.clone()))
            .collect();
        if pending.len() < LEADER_RECOVERY_BATCH {
            let remaining = LEADER_RECOVERY_BATCH - pending.len();
            pending.extend(
                self.proposer
                    .range(..start)
                    .take(remaining)
                    .map(|(s, p)| (*s, p.ballot, p.command.clone())),
            );
        }
        self.resend_cursor = pending
            .last()
            .and_then(|(slot, _, _)| slot.0.checked_add(1).map(Slot));
        for (slot, ballot, command) in pending {
            self.broadcast(&Message::Accept {
                config_id: self.hard_state.config_id,
                from: me,
                ballot,
                slot,
                command,
            });
        }
    }

    /// Advance the next bounded page of deferred chosen-prefix application or
    /// inherited leader-recovery rounds after the caller has fully processed
    /// the previous [`Ready`] batch. A no-op when neither continuation exists.
    ///
    /// Drivers call this after persistence, sends, and application complete;
    /// keeping it separate from [`Ready::advance`](crate::Ready::advance)
    /// prevents a single-node recovery from advancing the in-memory chosen
    /// prefix ahead of the batch the driver is still persisting.
    ///
    /// # Panics
    ///
    /// If an internal role/recovery invariant is broken.
    pub fn advance_recovery(&mut self) {
        let writes_before = self.pending_writes.len();
        self.advance_chosen_index();
        if self.pending_writes.len() != writes_before {
            self.assert_invariants();
            return;
        }
        self.pump_leader_recovery();
        self.assert_invariants();
    }

    /// Whether this leader has Phase-2 rounds whose `Accept`s can be re-sent.
    /// Drivers use this to avoid consulting optional policy hooks when skipping
    /// a re-send would have no observable effect.
    #[must_use]
    pub fn has_pending_accepts(&self) -> bool {
        self.role == NodeRole::Leader && !self.proposer.is_empty()
    }

    /// Voluntarily resign the leadership: Leader → Follower, keeping every
    /// durable commitment (the promised ballot and the accepted log are
    /// untouched) and dropping only the volatile leadership state — the in-flight
    /// Phase-2 `proposer` map and any unconfirmed read-index rounds. A no-op on a
    /// node that is not the leader.
    ///
    /// A legitimate operational primitive: a node may want to hand leadership on
    /// before a planned restart or a rebalance (etcd-raft exposes the same idea as
    /// leadership transfer), and stepping down is always sound — Paxos never
    /// requires a leader to *stay* one. The slots the resigning leader was still
    /// re-proposing are simply undecided; the next leader recovers them from its
    /// promise quorum, or fills the ones the quorum never saw with a
    /// [`Control::Noop`].
    ///
    /// In the deterministic simulation this is the decision that makes an
    /// undecided slot **permanent**: the hole a skipped
    /// [`resend_pending`](RawNode::resend_pending) leaves behind heals for as long
    /// as its holder keeps re-proposing it, and stops healing the moment that node
    /// stops being leader (#54) — and the leadership churn it creates is what
    /// #67's arc needs.
    pub fn step_down(&mut self) {
        if self.role != NodeRole::Leader {
            return;
        }
        self.become_follower(None);
    }

    /// Open an **application repair** (Stage 8): the driver's boot replay could
    /// not walk the whole chosen prefix — a faulty chosen record blocked it, or
    /// the application snapshot was lost below the compaction floor — and the
    /// application's durable prefix stops just below `from`. The core re-emits
    /// every decided command from `from` onward, in slot order, through the
    /// ordinary [`Ready::committed`](crate::Ready::committed) seam as the
    /// missing values arrive (commit-replay catch-up, or a snapshot install
    /// when `from` sits below the floor), and pulls them from peers each tick.
    ///
    /// A no-op when nothing is chosen or `from` already covers the prefix.
    ///
    /// # Panics
    ///
    /// If `from` lies past the contiguous chosen prefix — the driver may only
    /// name a slot the durable chosen index already covers.
    pub fn open_app_repair(&mut self, from: Slot) {
        assert!(
            from <= self.first_unchosen(),
            "an application repair starts inside the chosen prefix"
        );
        if from >= self.first_unchosen() {
            return;
        }
        self.app_repair = Some(from);
        self.pump_app_repair();
        self.assert_invariants();
    }

    /// Re-emit the next run of decided commands the open application repair can
    /// serve: from the cursor, while each slot's value is present (readable in
    /// `chosen`), bounded per batch. Stops at the floor (only a snapshot can
    /// heal below it) or at the first still-missing value (catch-up will bring
    /// it). Closes the repair when the cursor reaches the contiguous prefix
    /// walk's frontier.
    pub(crate) fn pump_app_repair(&mut self) {
        let Some(mut cursor) = self.app_repair else {
            return;
        };
        let end = self.first_unchosen();
        let mut emitted = 0_usize;
        while cursor < end && emitted < LEADER_RECOVERY_BATCH {
            if cursor < self.first_slot {
                // Below the floor the decided values are gone locally; only an
                // InstallSnapshot can close this repair.
                break;
            }
            let Some(command) = self.chosen.get(&cursor).cloned() else {
                break;
            };
            // The apply seam's duplicate suppression is re-derived exactly as
            // the contiguous walk derives it (the ledger already holds these
            // decisions from the boot rebuild plus the heal-time patches).
            let command = if self.duplicate_slots.contains(&cursor) {
                Command::Control(Control::Noop)
            } else {
                command
            };
            self.pending_committed.push((cursor, command));
            cursor = Slot(cursor.0 + 1);
            emitted += 1;
        }
        self.app_repair = if cursor >= end { None } else { Some(cursor) };
    }

    /// The driver supplies a randomized election timeout (in ticks, jitter drawn
    /// from its `RandomProvider`). Clears the [`RawNode::needs_election_timeout`]
    /// flag.
    pub fn set_election_timeout(&mut self, ticks: u64) {
        self.election_timeout = ticks;
        self.needs_election_timeout = false;
    }

    /// Borrow the node to drain one batch of work. The returned [`Ready`] holds
    /// the unique `&mut` borrow, so a second `ready()` before [`Ready::advance`]
    /// is a **compile error**.
    pub fn ready(&mut self) -> Ready<'_> {
        Ready::new(self)
    }

    // ---- accessors --------------------------------------------------------

    /// This node's configuration (identity + membership).
    #[must_use]
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// The current durable scalars (promised ballot + chosen index).
    #[must_use]
    pub fn hard_state(&self) -> &HardState {
        &self.hard_state
    }

    /// The working per-slot accepted log (rebuilt from durable storage on boot).
    /// A read view for drivers/oracles; writes go through [`Ready`] deltas.
    #[must_use]
    pub fn accepted(&self) -> &BTreeMap<Slot, (Ballot, Command)> {
        &self.accepted
    }

    /// The compaction floor: the first slot still retained. Slots below it have
    /// been truncated away (see [`RawNode::compact`]).
    #[must_use]
    pub fn first_slot(&self) -> Slot {
        self.first_slot
    }

    /// The number of ticks observed so far.
    #[must_use]
    pub fn tick_count(&self) -> u64 {
        self.tick_count
    }

    /// This node's current role.
    #[must_use]
    pub fn role(&self) -> NodeRole {
        self.role
    }

    /// The node this one believes is leader, if any.
    #[must_use]
    pub fn leader(&self) -> Option<NodeId> {
        self.leader
    }

    /// Whether this node is currently the leader.
    #[must_use]
    pub fn is_leader(&self) -> bool {
        self.role == NodeRole::Leader
    }

    /// This node's current operating ballot.
    #[must_use]
    pub fn ballot(&self) -> Ballot {
        self.ballot
    }

    /// Whether the driver should feed a fresh randomized election timeout (the
    /// election clock just reset).
    #[must_use]
    pub fn needs_election_timeout(&self) -> bool {
        self.needs_election_timeout
    }

    /// How many undecided holes this node filled with a [`Control::Noop`] when it
    /// won its current leadership — 0 on a node that has never led, and re-set at
    /// each election it wins. A read-only observability counter: the driver reads
    /// it on the transition to Leader so a simulation can prove the gap-fill path
    /// is genuinely reached.
    #[must_use]
    pub fn election_gap_fills(&self) -> u64 {
        self.election_gap_fills
    }

    /// The full at-most-once session ledger: every `(client, seq) -> slot`
    /// record in this node's applied prefix, flattened. The driver attaches it
    /// to each [`Message::InstallSnapshot`] it serves (paros-owned metadata
    /// beside the opaque application bytes), so a snapshot-recovered peer makes
    /// the same #94 duplicate-suppression decisions as everyone else.
    #[must_use]
    pub fn session_ledger(&self) -> Vec<SessionEntry> {
        self.applied_seq
            .iter()
            .flat_map(|(client, seqs)| seqs.iter().map(|(&seq, &slot)| (*client, seq, slot)))
            .collect()
    }

    /// Chosen slots the apply seam executes as a no-op because their
    /// `Command::User` identity already applied at a lower slot (see the field
    /// doc). The driver's boot replay consults this so a restart re-applies the
    /// retained prefix with the identical suppression decisions.
    #[must_use]
    pub fn duplicate_slots(&self) -> &BTreeSet<Slot> {
        &self.duplicate_slots
    }

    /// Monotone count of #94 duplicate suppressions the contiguous walk
    /// performed this incarnation. The driver reads the delta per batch and
    /// reports it through its audit port.
    #[must_use]
    pub fn duplicates_suppressed(&self) -> u64 {
        self.duplicates_suppressed
    }

    /// Monotone count of `CheckQuorum` step-downs (#95) this incarnation: the
    /// times this node, as Leader, spent a full election-timeout window without
    /// hearing an ack quorum and demoted itself. The driver reads the delta per
    /// batch and reports it through its audit port.
    #[must_use]
    pub fn quorum_lost_step_downs(&self) -> u64 {
        self.quorum_lost_step_downs
    }

    /// The recoverable faulty entries this node still holds (Stage 8): value
    /// lost, identity known, reported in the Promise tri-state and repaired in
    /// place. A read view for drivers/oracles.
    #[must_use]
    pub fn faulty_entries(&self) -> &BTreeMap<Slot, Ballot> {
        &self.faulty
    }

    /// The open application repair cursor, if any (see
    /// [`RawNode::open_app_repair`]).
    #[must_use]
    pub fn app_repair(&self) -> Option<Slot> {
        self.app_repair
    }

    /// How many blocked slots the leader's open repair probe still holds (0
    /// when no probe is open): faulty slots the promise quorum resolved neither
    /// as Case 1 (`have`) nor Case 2 (a full Q1 of `none`), still waiting on
    /// stragglers.
    #[must_use]
    pub fn blocked_repairs(&self) -> usize {
        self.repair_probe.as_ref().map_or(0, |p| p.blocked.len())
    }

    /// Monotone repair counters this incarnation, for the driver's audit
    /// report: `(faulty records repaired in place, Case-1 straggler
    /// re-proposals, Case-2 straggler no-op fills, recovery-timeout
    /// step-downs, repair payload bytes)`.
    #[must_use]
    pub fn repair_counters(&self) -> (u64, u64, u64, u64, u64) {
        (
            self.faulty_repaired,
            self.repair_case1,
            self.repair_case2,
            self.repair_step_downs,
            self.repair_bytes,
        )
    }

    /// The **chosen gap**, if this node holds one: `(hole, highest)` where `hole`
    /// is the first slot missing from the contiguous chosen prefix and `highest`
    /// is the highest slot above it this node already knows is chosen. `None` when
    /// nothing is chosen past the prefix — the healthy steady state.
    ///
    /// A read-only observability accessor: the core cannot trace, and the gap is
    /// invisible from outside because [`Ready::committed`](crate::Ready::committed)
    /// only ever surfaces the *contiguous* prefix. A gap is a normal transient
    /// (pipelining, a follower that missed one `Commit`); a gap that **survives
    /// quiescence** is the wedge this exists to make observable — the chosen index
    /// frozen at `hole - 1` cluster-wide while higher slots keep being chosen.
    #[must_use]
    pub fn chosen_gap(&self) -> Option<(Slot, Slot)> {
        // An open application repair is the same shape of stall, routed through
        // the same seam (Stage 8): decided slots sit above a prefix the
        // application cannot advance past. `hole` is the repair cursor; the
        // highest decided slot above it is at least the chosen index.
        if let Some(hole) = self.app_repair {
            let highest = self
                .chosen
                .range(hole..)
                .next_back()
                .map_or(hole, |(s, _)| *s)
                .max(self.hard_state.chosen_index.unwrap_or(hole));
            return Some((hole, highest));
        }
        let hole = self.first_unchosen();
        // `hole` may itself be chosen while the bounded prefix walk is pending;
        // the driver drains that continuation before quiescence.
        let highest = *self.chosen.range(hole..).next_back()?.0;
        Some((hole, highest))
    }

    // ---- crate-internal accessors used by `Ready` (not public API) ----

    pub(crate) fn pending_writes(&self) -> &[WriteOp] {
        &self.pending_writes
    }

    pub(crate) fn pending_messages(&self) -> &[(NodeId, Message)] {
        &self.pending_messages
    }

    pub(crate) fn pending_committed(&self) -> &[(Slot, Command)] {
        &self.pending_committed
    }

    pub(crate) fn pending_snapshot_offers(&self) -> &[(NodeId, Slot, Ballot, ConfigId)] {
        &self.pending_snapshot_offers
    }

    pub(crate) fn pending_read_states(&self) -> &[ReadState] {
        &self.pending_read_states
    }

    pub(crate) fn pending_recovery_batch(&self) -> Option<(usize, usize, usize)> {
        self.pending_recovery_batch
    }

    pub(crate) fn clear_pending(&mut self) {
        self.pending_writes.clear();
        self.pending_messages.clear();
        self.pending_committed.clear();
        self.pending_snapshot_offers.clear();
        self.pending_read_states.clear();
        self.pending_recovery_batch = None;
    }
}

#[cfg(test)]
mod tests;
