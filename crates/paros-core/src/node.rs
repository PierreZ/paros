//! The [`RawNode`] handle: the sans-IO Multi-Paxos state machine and the
//! `step`/`tick`/`ready`/`advance` contract.

use std::collections::{BTreeMap, BTreeSet};

use crate::message::Message;
use crate::ready::Ready;
use crate::state::{Config, HardState};
use crate::storage::Storage;
use crate::types::{
    Ballot, ClientId, ClientSeq, Command, Control, Entry, NodeId, SessionEntry, Slot, Value,
};
use crate::write::WriteOp;

/// Leader heartbeat interval, in ticks. The driver always supplies an election
/// timeout far larger than this (`>= 2 * HEARTBEAT_TICKS`), so a live leader
/// always beats before any follower's election clock fires.
const HEARTBEAT_TICKS: u64 = 1;

/// Maximum number of decided slots one [`Message::CatchUpResponse`] carries. A
/// lagging peer that needs more re-requests on the next heartbeat, so a large
/// backlog is drained over several rounds rather than one unbounded message.
const CATCHUP_BATCH: usize = 64;

/// Ticks a pending read-index round may wait for its ack quorum before the
/// leader garbage-collects it (lost acks, an unreachable quorum). Dropped
/// silently: the round carries no durable obligation, and the driver owns the
/// client reply (its retry sweep answers first, well inside this window).
const READ_ROUND_TTL_TICKS: u64 = 20;

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

/// Volatile state of one in-flight read-index round (leader only).
struct ReadRound {
    /// The driver-supplied correlation token.
    ctx: u64,
    /// The captured read index: `max(chosen_index, read_floor)` at capture time.
    index: Option<Slot>,
    /// The beat sequence an ack must answer (at or after) to credit this round:
    /// the heartbeat broadcast when the round began. Later beats' acks count
    /// too, so one ack can confirm every older pending round.
    required_seq: u64,
    /// Peers (incl. self) that acked a qualifying beat at the round's ballot.
    acked_by: BTreeSet<NodeId>,
    /// Tick the round was created on, for TTL garbage collection.
    created_tick: u64,
}

/// Volatile state of one in-flight per-slot Phase-2 (`Accept`) round.
struct Proposing {
    /// The ballot this slot is being accepted under.
    ballot: Ballot,
    /// The command being accepted for this slot.
    command: Command,
    /// Acceptors (incl. self) that have accepted this slot's command at `ballot`.
    accepted_by: BTreeSet<NodeId>,
}

/// Volatile per-ballot Phase-1 state while a Candidate recovers the log suffix.
struct Election {
    /// The ballot this election runs under.
    ballot: Ballot,
    /// First slot this election recovers (`chosen_index + 1`, or `Slot(0)`).
    from_slot: Slot,
    /// Acceptors (incl. self) that have promised `ballot`.
    promised_by: BTreeSet<NodeId>,
    /// Highest-ballot accepted command per slot seen across the promise quorum,
    /// for slots `>= from_slot`. Drives gap-fill re-proposal once leader.
    recovered: BTreeMap<Slot, (Ballot, Command)>,
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

    // ---- pending output buckets: filled by the protocol logic, drained by
    // ---- `ready`, cleared by `advance`.
    /// Semantic durable write deltas produced this batch, in apply order.
    pending_writes: Vec<WriteOp>,
    pending_messages: Vec<(NodeId, Message)>,
    pending_committed: Vec<(Slot, Command)>,
    /// Snapshot offers to serve this batch: `(to, chosen_index, ballot)`. The core
    /// decides *who* needs a snapshot and *up to where* (a below-floor catch-up
    /// request), but holds no application state, so the driver attaches the opaque
    /// snapshot bytes (from storage) and sends the [`Message::InstallSnapshot`].
    pending_snapshot_offers: Vec<(NodeId, Slot, Ballot)>,
    /// Read-index rounds confirmed this batch, drained via
    /// [`Ready::read_states`] after the batch's committed entries are applied.
    pending_read_states: Vec<ReadState>,

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
    /// The highest round some acceptor has reported already promised, via a
    /// [`Message::Nack`] matching an in-flight campaign or accept round, even
    /// when we never adopted that ballot ourselves. Floors the round
    /// [`RawNode::on_check_leader`] picks for the next campaign, so a candidate
    /// facing a much higher remote promise converges in one hop instead of
    /// climbing one round per election timeout. Monotonically non-decreasing.
    known_promised_round: u64,
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
    pub fn new<S: Storage>(storage: &S) -> Self {
        let (hard_state, config) = storage.initial_state();
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
        // Next free slot: one past the highest accepted entry, or (when the log
        // is empty, e.g. fully truncated) one past the durable chosen index.
        let first_unchosen = hard_state.chosen_index.map_or(Slot(0), |ci| Slot(ci.0 + 1));
        let next_slot = accepted
            .keys()
            .next_back()
            .map_or(first_unchosen, |s| Slot(s.0 + 1));

        Self {
            config,
            hard_state,
            accepted,
            first_slot,
            pending_writes: Vec::new(),
            pending_messages: Vec::new(),
            pending_committed: Vec::new(),
            pending_snapshot_offers: Vec::new(),
            pending_read_states: Vec::new(),
            tick_count: 0,
            role: NodeRole::Follower,
            leader: None,
            ballot,
            known_promised_round: 0,
            election_elapsed: 0,
            election_timeout: 0,
            needs_election_timeout: true,
            heartbeat_elapsed: 0,
            heartbeat_timeout: HEARTBEAT_TICKS,
            heartbeat_seq: 0,
            read_floor: None,
            read_rounds: Vec::new(),
            proposer: BTreeMap::new(),
            election: None,
            next_slot,
            election_gap_fills: 0,
            chosen,
            applied_seq,
            inflight,
            duplicate_slots,
            duplicates_suppressed: 0,
        }
    }

    /// The single input entry point: every stimulus is a [`Message`], routed by
    /// variant and role. Tick-injected self-events (`CheckLeader`/`Heartbeat`)
    /// enter here too.
    pub fn step(&mut self, msg: Message) {
        match msg {
            Message::Prepare {
                from,
                ballot,
                from_slot,
            } => self.on_prepare(from, ballot, from_slot),
            Message::Promise {
                from,
                ballot,
                from_slot,
                accepted,
            } => self.on_promise(from, ballot, from_slot, accepted),
            Message::Accept {
                from,
                ballot,
                slot,
                command,
            } => self.on_accept(from, ballot, slot, command),
            Message::Accepted { from, ballot, slot } => self.on_accepted(from, ballot, slot),
            Message::Nack {
                ballot,
                promised,
                slot,
                ..
            } => self.on_nack(ballot, promised, slot),
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
            } => self.on_heartbeat(from, ballot, commit, seq),
            Message::HeartbeatAck { from, ballot, seq } => {
                self.on_heartbeat_ack(from, ballot, seq);
            }
        }
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
        // hang to the client's deadline).
        if let Some(&at) = self.applied_seq.get(&client).and_then(|m| m.get(&seq)) {
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
        acked_by.insert(self.config.id);
        self.read_rounds.push(ReadRound {
            ctx,
            index,
            required_seq: self.heartbeat_seq,
            acked_by,
            created_tick: self.tick_count,
        });
        // A single-node cluster is its own quorum: confirm in this same batch.
        self.try_confirm_reads();
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
    pub fn compact(&mut self, up_to: Slot) -> Slot {
        let Some(ci) = self.hard_state.chosen_index else {
            return self.first_slot;
        };
        let highest_drop = up_to.min(ci);
        let first = Slot(highest_drop.0 + 1).max(self.first_slot);
        if first <= self.first_slot {
            return self.first_slot;
        }
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
        self.proposer.retain(|slot, _| *slot >= first);
        self.first_slot = first;
        self.pending_writes.push(WriteOp::Truncate { first, sealed });
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
        } else {
            self.election_elapsed += 1;
            if self.election_timeout != 0 && self.election_elapsed >= self.election_timeout {
                self.election_elapsed = 0;
                self.needs_election_timeout = true;
                self.step(Message::CheckLeader { from: me });
            }
        }
    }

    /// Re-broadcast the `Accept` for every Phase-2 round this leader still has in
    /// flight. A no-op on a node that is not the leader, and on a leader with
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
        let pending: Vec<(Slot, Ballot, Command)> = self
            .proposer
            .iter()
            .map(|(s, p)| (*s, p.ballot, p.command.clone()))
            .collect();
        for (slot, ballot, command) in pending {
            self.broadcast(&Message::Accept {
                from: me,
                ballot,
                slot,
                command,
            });
        }
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

    // ---- election / leadership --------------------------------------------

    /// Election clock fired: become a Candidate and run one Phase 1 (per ballot)
    /// over the whole uncommitted log suffix.
    fn on_check_leader(&mut self) {
        if self.role == NodeRole::Leader {
            return;
        }
        let me = self.config.id;
        let round = self
            .hard_state
            .max_promised_ballot
            .round
            .max(self.ballot.round)
            .max(self.known_promised_round)
            + 1;
        self.role = NodeRole::Candidate;
        self.leader = None;
        self.ballot = Ballot { round, node: me };
        self.set_promise(self.ballot);

        let from_slot = self.first_unchosen();
        let recovered: BTreeMap<Slot, (Ballot, Command)> = self
            .accepted
            .range(from_slot..)
            .map(|(s, v)| (*s, v.clone()))
            .collect();
        let mut promised_by = BTreeSet::new();
        promised_by.insert(me);
        self.election = Some(Election {
            ballot: self.ballot,
            from_slot,
            promised_by,
            recovered,
        });
        self.proposer.clear();
        self.broadcast(&Message::Prepare {
            from: me,
            ballot: self.ballot,
            from_slot,
        });
        // Proactive catch-up probe. The election clock fires precisely when we have
        // *not* heard a satisfactory leader — the same condition under which we may
        // be silently behind: a stale or absent leader beat never reveals a decided
        // slot past our prefix, so heartbeat-triggered catch-up never fires. Ask
        // every peer to replay our decided-prefix gap directly; any peer that has
        // those slots chosen serves them, healing us even if this election does not
        // win (a won election gap-fills only *accepted* slots it can re-proposes;
        // this learns *chosen* ones outright). Harmless when we are not behind — a
        // peer with nothing past `from_slot` simply sends nothing.
        self.broadcast(&Message::CatchUpRequest {
            from: me,
            from_slot,
        });
        self.try_become_leader();
    }

    /// Candidate: collect a `Promise`, merging the reported accepted suffix
    /// (highest ballot per slot wins).
    fn on_promise(
        &mut self,
        from: NodeId,
        ballot: Ballot,
        from_slot: Slot,
        accepted: BTreeMap<Slot, (Ballot, Command)>,
    ) {
        // Quorum sets are keyed by NodeId: an id outside the configured
        // membership must never inflate one (wire hygiene; peers are trusted
        // but a misrouted or misconfigured sender is not a quorum member).
        if !self.config.peers.contains(&from) {
            return;
        }
        {
            let Some(e) = self.election.as_mut() else {
                return;
            };
            if e.ballot != ballot || e.from_slot != from_slot {
                return;
            }
            e.promised_by.insert(from);
            for (slot, (ab, command)) in accepted {
                let supersedes = e.recovered.get(&slot).is_none_or(|(rb, _)| ab > *rb);
                if supersedes {
                    e.recovered.insert(slot, (ab, command));
                }
            }
        }
        self.try_become_leader();
    }

    /// Candidate -> Leader once a promise quorum holds: re-propose every
    /// recovered in-flight slot under the new ballot (gap fill), then stream.
    ///
    /// A campaign whose ballot has fallen **below the node's own promise** is
    /// refused even with a quorum behind it (#67/#88). Mid-election, two paths
    /// raise `max_promised_ballot` without closing the campaign: `mark_chosen`
    /// on a learned `Commit`/`CatchUpResponse`, and `on_install_snapshot` on a
    /// snapshot whose serving peer minted its promise with no quorum at all.
    /// Winning below the own promise breaks "a leader's ballot >= its own
    /// promise": every self-accept is skipped (`start_accept_round`'s
    /// `ballot >= max_promised_ballot` check), so recovered slots reach
    /// `proposer` but never `accepted`, `next_slot` — derived from `accepted` —
    /// lands *below* an in-flight slot, and a later `propose` re-proposes a
    /// different command under the same `(slot, ballot)`: two values can then
    /// assemble accept quorums for one slot at n >= 5. Refusal is a plain
    /// non-win: the election stays open and self-heals — the next election
    /// timeout campaigns at `max(max_promised_ballot.round, ..) + 1`
    /// (`on_check_leader`), above the promise that caused the refusal.
    fn try_become_leader(&mut self) {
        let quorum = self.quorum();
        let won = self.role == NodeRole::Candidate
            && self.election.as_ref().is_some_and(|e| {
                e.promised_by.len() >= quorum && e.ballot >= self.hard_state.max_promised_ballot
            });
        if !won {
            return;
        }
        let me = self.config.id;
        let e = self.election.take().expect("won implies an election");
        self.role = NodeRole::Leader;
        self.leader = Some(me);
        self.ballot = e.ballot;
        self.heartbeat_elapsed = 0;
        self.election_elapsed = 0;
        self.proposer.clear();

        for (slot, (_old, command)) in e.recovered {
            // A slot below our floor is chosen (only chosen slots are truncated),
            // so it needs no re-proposal.
            if slot < self.first_slot || self.chosen.contains_key(&slot) {
                continue;
            }
            if let Command::User(entry) = &command
                && !self.applied_elsewhere(entry, slot)
            {
                // Same guard as `mark_chosen`: a recovered identity that already
                // applied at another slot is still re-proposed (P2c is
                // mandatory — it may already be chosen here), but a retry must
                // find the applied fast path, not park on the doomed slot.
                self.inflight.insert((entry.client, entry.seq), slot);
            }
            self.start_accept_round(slot, command);
        }
        self.next_slot = self
            .accepted
            .keys()
            .next_back()
            .map_or(self.first_unchosen(), |s| Slot(s.0 + 1));
        // ---- No-op gap fill: the slots the promise quorum reported *nothing* for.
        //
        // Re-proposing `recovered` covers every slot the quorum saw accepted, and
        // `next_slot` now sits one past the highest of them. What that leaves is the
        // dangerous case *between* them: a slot the old leader accepted alone, while
        // a later slot reached the quorum. It is in neither `chosen` nor
        // `recovered`, and `next_slot` jumped clean over it — so no one would ever
        // propose it again. `propose`/`propose_control` only allocate `next_slot`,
        // and a restart recomputes `next_slot` from the accepted log the same way.
        // The hole would be permanent, and it is not a quiet one: the contiguous
        // chosen prefix freezes one below it cluster-wide (`advance_chosen_index`
        // walks contiguously) while higher slots keep being chosen, the fresh-leader
        // read fence sits above it so no read ever confirms again, and commit-replay
        // catch-up cannot heal it — every node's prefix is frozen below the hole, so
        // no peer has anything to replay.
        //
        // Filling it with a [`Control::Noop`] is safe for the ordinary Phase-1
        // reason. Any value already chosen at that slot was accepted by a quorum,
        // which intersects this promise quorum, so at least one Promise would have
        // reported it (an acceptor that truncated the range Nacks instead of
        // under-reporting — see the floor guard in `on_prepare`). Nothing was
        // reported, so nothing is chosen there and the slot is genuinely free.
        let fill: Vec<Slot> = (self.first_unchosen().0..self.next_slot.0)
            .map(Slot)
            .filter(|s| !self.proposer.contains_key(s))
            .collect();
        let mut filled = 0_u64;
        for slot in fill {
            // Re-checked per slot, not once up front: a fill can decide in this same
            // loop (a single-node cluster is its own quorum), advancing the chosen
            // prefix underneath it. Starting a round on an already-chosen slot would
            // overwrite its authoritative accepted record with the no-op.
            if slot < self.first_slot || self.chosen.contains_key(&slot) {
                continue;
            }
            self.start_accept_round(slot, Command::Control(Control::Noop));
            filled += 1;
        }
        self.election_gap_fills = filled;
        // The fresh-leader read fence: nothing decided under an earlier ballot
        // can sit above `next_slot - 1` (the prepare quorum reported it all), so
        // reads wait until the chosen prefix covers that slot. Beat seqs are
        // per-ballot; cross-ballot ack confusion is impossible because an ack
        // must echo the current ballot to count.
        self.read_floor = self.next_slot.0.checked_sub(1).map(Slot);
        self.heartbeat_seq = 0;
        self.read_rounds.clear();
    }

    // ---- acceptor ---------------------------------------------------------

    /// Acceptor: a candidate prepares `ballot` for every slot `>= from_slot`.
    /// Promote and reply `Promise` (carrying the accepted suffix) if strictly
    /// higher than our promise; otherwise `Nack`.
    fn on_prepare(&mut self, from: NodeId, ballot: Ballot, from_slot: Slot) {
        let me = self.config.id;
        // Floor guard: a Prepare whose `from_slot` is below our compaction floor
        // cannot be promised. We truncated the accepted entries for
        // `[from_slot, first_slot)`, so our Promise could not report them, and the
        // candidate would treat those already-chosen slots as free and re-propose
        // a different value: two values chosen for one slot. Nack *without* raising
        // our promise (so a blind laggard cannot ratchet our promise up and depose
        // a healthy leader); those slots are chosen, and the candidate must recover
        // them out of band.
        if from_slot < self.first_slot {
            self.pending_messages.push((
                from,
                Message::Nack {
                    from: me,
                    ballot,
                    promised: self.hard_state.max_promised_ballot,
                    slot: from_slot,
                },
            ));
            return;
        }
        if ballot > self.hard_state.max_promised_ballot {
            if ballot.node != me && self.role != NodeRole::Follower {
                self.become_follower(None);
            }
            self.election_elapsed = 0;
            self.set_promise(ballot);
            if ballot > self.ballot {
                self.ballot = ballot;
            }
            let accepted: BTreeMap<Slot, (Ballot, Command)> = self
                .accepted
                .range(from_slot..)
                .map(|(s, v)| (*s, v.clone()))
                .collect();
            self.pending_messages.push((
                from,
                Message::Promise {
                    from: me,
                    ballot,
                    from_slot,
                    accepted,
                },
            ));
        } else {
            self.pending_messages.push((
                from,
                Message::Nack {
                    from: me,
                    ballot,
                    promised: self.hard_state.max_promised_ballot,
                    slot: from_slot,
                },
            ));
        }
    }

    /// Acceptor: a leader asks us to accept `entry` for `slot` at `ballot`.
    /// Accept (and persist) if we have not promised a higher ballot; else `Nack`.
    fn on_accept(&mut self, from: NodeId, ballot: Ballot, slot: Slot, command: Command) {
        // Floor guard: a slot below our floor is already chosen (only chosen slots
        // are ever truncated). Ignore the Accept rather than Nack: the slot is
        // decided, so re-accepting a different value there would break agreement,
        // and a Nack would needlessly depose a leader that can still assemble a
        // quorum on live slots. Heartbeat commit reconciliation heals any real gap.
        if slot < self.first_slot {
            return;
        }
        let me = self.config.id;
        if ballot >= self.hard_state.max_promised_ballot {
            if ballot.node != me && self.role != NodeRole::Follower {
                self.become_follower(Some(ballot.node));
            } else {
                self.leader = Some(ballot.node);
                self.election_elapsed = 0;
            }
            if ballot > self.ballot {
                self.ballot = ballot;
            }
            self.set_promise(ballot);
            self.record_accepted(slot, ballot, command);
            self.pending_messages.push((
                from,
                Message::Accepted {
                    from: me,
                    ballot,
                    slot,
                },
            ));
        } else {
            self.pending_messages.push((
                from,
                Message::Nack {
                    from: me,
                    ballot,
                    promised: self.hard_state.max_promised_ballot,
                    slot,
                },
            ));
        }
    }

    // ---- proposer / learner ----------------------------------------------

    /// Leader: collect an `Accepted` for a streamed slot; decide on a quorum.
    fn on_accepted(&mut self, from: NodeId, ballot: Ballot, slot: Slot) {
        // Quorum sets are keyed by NodeId: an id outside the configured
        // membership must never inflate one (wire hygiene; peers are trusted
        // but a misrouted or misconfigured sender is not a quorum member).
        if !self.config.peers.contains(&from) {
            return;
        }
        {
            let Some(p) = self.proposer.get_mut(&slot) else {
                return;
            };
            if p.ballot != ballot {
                return;
            }
            p.accepted_by.insert(from);
        }
        self.try_decide(slot);
    }

    /// A rejection of an in-flight ballot. Step down to Follower and let the
    /// randomized election timeout reschedule us. We do **not** immediately
    /// re-prepare: that (with the randomized timeout) is the dueling-proposer
    /// livelock fix. We do, however, remember the acceptor's reported `promised`
    /// ballot so the *next* campaign starts past it instead of climbing one round
    /// per timeout.
    fn on_nack(&mut self, ballot: Ballot, promised: Ballot, slot: Slot) {
        let superseded = self.election.as_ref().is_some_and(|e| e.ballot == ballot)
            || self.proposer.get(&slot).is_some_and(|p| p.ballot == ballot);
        if superseded {
            self.become_follower(None);
            if promised.round > self.known_promised_round {
                self.known_promised_round = promised.round;
            }
        }
    }

    /// Learner: a command was chosen elsewhere. Record it; advance the prefix.
    fn on_commit(&mut self, ballot: Ballot, slot: Slot, command: &Command) {
        if ballot >= self.ballot {
            self.election_elapsed = 0;
        }
        self.mark_chosen(slot, command, ballot);
    }

    /// Broadcast one leader beat at a fresh, monotonically increasing
    /// per-ballot sequence number. Both the tick self-trigger and
    /// [`RawNode::read_index`] beat through here, so every broadcast beat
    /// carries a seq an ack can be matched against.
    fn broadcast_heartbeat(&mut self) {
        self.heartbeat_seq += 1;
        self.broadcast(&Message::Heartbeat {
            from: self.config.id,
            ballot: self.ballot,
            commit: self.hard_state.chosen_index,
            seq: self.heartbeat_seq,
        });
    }

    /// Leader self-beat or a follower receiving a peer beat. The self event's
    /// `seq` is ignored (the real seq is assigned at broadcast).
    fn on_heartbeat(&mut self, from: NodeId, ballot: Ballot, commit: Option<Slot>, seq: u64) {
        let me = self.config.id;
        if from == me {
            // Leader self-trigger: broadcast the beat. Re-sending the un-acked
            // `Accept`s is a *separate* decision the driver makes on the same
            // cadence — see [`RawNode::resend_pending`].
            self.broadcast_heartbeat();
            return;
        }
        // Follower receiving the leader's beat: adopt its ballot / leadership only
        // if it is at or above our promise.
        if ballot >= self.hard_state.max_promised_ballot {
            if self.role == NodeRole::Follower {
                self.leader = Some(from);
                self.election_elapsed = 0;
            } else {
                self.become_follower(Some(from));
            }
            if ballot > self.ballot {
                self.ballot = ballot;
            }
            // Ack the beat, echoing `(ballot, seq)`: the leader counts these
            // toward read-index confirmation quorums. Below-promise beats fall
            // through unacked, so a deposed leader's read rounds starve instead
            // of confirming. No durable write precedes the ack — it claims only
            // "my promise is at or below `ballot` right now", which the guard
            // above just checked and promise monotonicity preserves.
            self.pending_messages.push((
                from,
                Message::HeartbeatAck {
                    from: me,
                    ballot,
                    seq,
                },
            ));
        }
        // Commit-replay catch-up reconciles the sender's advertised contiguous
        // chosen prefix (`commit`) against ours, in **both** directions. It is
        // deliberately **not** gated on `ballot >= promise`: catch-up learns
        // *immutable chosen history*, not leadership, and a value either side has
        // decided is quorum-committed and safe to learn. The per-beat cadence
        // rate-limits (and self-heals a lost message); it stops once the prefixes
        // agree.
        //
        // Both sides are `Option<Slot>` and compare directly: `None` (nothing
        // chosen) orders below `Some(Slot(0))` (slot 0 chosen), which is the whole
        // point — those two states are genuinely different, and a wire encoding
        // that folded them together left a follower missing exactly slot 0 with no
        // way to notice (#56).
        let ci = self.hard_state.chosen_index;
        if commit > ci {
            // We are behind: a `Commit` (and its `Accept`) for a decided slot never
            // reached us — the leader only re-sends `Accept`s for still-*pending*
            // slots, so that hole would be permanent. Pull the decided range from
            // our first unchosen slot.
            let from_slot = self.first_unchosen();
            self.pending_messages.push((
                from,
                Message::CatchUpRequest {
                    from: me,
                    from_slot,
                },
            ));
        } else if commit < ci {
            // We are ahead of the sender: push what it is missing. This is what
            // heals a leader that lost its (relaxed, non-fsync'd) chosen index to a
            // crash — it beats a stale low `commit`, so no follower would ever pull;
            // a follower that *does* know the slot is decided replays it to the
            // leader, which then advertises the true prefix and the genuinely-behind
            // nodes pull. A sender with nothing chosen needs the replay from the
            // very first slot.
            // Serve from one PAST the sender's contiguous chosen index: it
            // already holds everything at and below `commit`. Serving from
            // `commit` itself wasted one batch entry — and at the floor
            // boundary it converted a one-slot-behind peer into a snapshot
            // install (`commit == first_slot - 1` tripped the below-floor
            // branch for a replay we can serve normally).
            self.serve_catchup(from, commit.map_or(Slot(0), |c| Slot(c.0 + 1)));
        }
    }

    /// Leader: a peer answered a beat at `(ballot, seq)`. Credit every read
    /// round the ack qualifies for: same ballot as ours, and a seq at or after
    /// the round's required beat — an ack to an *earlier* beat proves nothing
    /// about leadership after the round began, so it never counts. Stale or
    /// cross-ballot acks are dropped whole.
    fn on_heartbeat_ack(&mut self, from: NodeId, ballot: Ballot, seq: u64) {
        // Quorum sets are keyed by NodeId: an id outside the configured
        // membership must never inflate one (wire hygiene; peers are trusted
        // but a misrouted or misconfigured sender is not a quorum member).
        if !self.config.peers.contains(&from) {
            return;
        }
        if self.role != NodeRole::Leader || ballot != self.ballot {
            return;
        }
        for round in &mut self.read_rounds {
            if seq >= round.required_seq {
                round.acked_by.insert(from);
            }
        }
        self.try_confirm_reads();
    }

    /// Confirm the eligible prefix of pending read rounds, in creation order: a
    /// round resolves once a quorum (incl. self) acked a qualifying beat AND the
    /// chosen prefix covers the round's index (the fresh-leader fence resolves
    /// here, via [`RawNode::advance_chosen_index`]). Confirmability is monotone
    /// in creation order — a later round's index and required seq are both at or
    /// above an earlier one's — so scanning the front suffices.
    fn try_confirm_reads(&mut self) {
        if self.role != NodeRole::Leader {
            return;
        }
        let quorum = self.quorum();
        while let Some(round) = self.read_rounds.first() {
            let confirmed =
                round.acked_by.len() >= quorum && self.hard_state.chosen_index >= round.index;
            if !confirmed {
                break;
            }
            let round = self.read_rounds.remove(0);
            self.pending_read_states.push(ReadState {
                ctx: round.ctx,
                index: round.index,
            });
        }
    }

    /// Serve a lagging peer's catch-up request by replaying the decided range.
    fn on_catchup_request(&mut self, from: NodeId, from_slot: Slot) {
        self.serve_catchup(from, from_slot);
    }

    /// Send `to` the decided `(ballot, entry)` per slot for a bounded range at or
    /// after `from_slot`, up to our own contiguous chosen prefix. A node with
    /// nothing chosen at or above `from_slot` sends nothing. Every entry is chosen —
    /// durable and quorum-decided — so the recipient may learn it directly (the
    /// same safety a `Commit` relies on). Used both to answer a pull
    /// ([`Message::CatchUpRequest`]) and to push a decided prefix to a peer whose
    /// heartbeat `commit` shows it is behind us.
    fn serve_catchup(&mut self, to: NodeId, from_slot: Slot) {
        let me = self.config.id;
        let Some(ci) = self.hard_state.chosen_index else {
            return;
        };
        // Below our floor the decided entries have been truncated away, so no
        // contiguous `CatchUpResponse` can replay them. Offer a snapshot instead:
        // record the offer (the driver attaches the opaque application bytes and
        // sends the `InstallSnapshot`), bringing the peer up to our chosen prefix.
        if from_slot < self.first_slot {
            // The offer carries the boundary slot's *choosing* ballot when the
            // log still holds it — a ballot with a real quorum behind it — not
            // this node's own promise, which one quorumless campaigner can
            // mint arbitrarily high; a receiver adopting a minted promise
            // above the live leader's ballot stops acking beats and Nacks its
            // accepts, forcing a spurious election. (If compaction dropped the
            // boundary record, the promise remains the safe upper bound.)
            let choosing = self
                .accepted
                .get(&ci)
                .map_or(self.hard_state.max_promised_ballot, |(b, _)| *b);
            self.pending_snapshot_offers.push((to, ci, choosing));
            return;
        }
        if from_slot > ci {
            return;
        }
        let mut entries: BTreeMap<Slot, (Ballot, Command)> = BTreeMap::new();
        for (slot, command) in self.chosen.range(from_slot..=ci) {
            if entries.len() >= CATCHUP_BATCH {
                break;
            }
            // The choosing ballot is the ballot recorded for this slot in the
            // accepted log (a chosen value is recorded authoritatively there).
            let ballot = self.accepted.get(slot).map_or(self.ballot, |(b, _)| *b);
            entries.insert(*slot, (ballot, command.clone()));
        }
        if entries.is_empty() {
            return;
        }
        self.pending_messages
            .push((to, Message::CatchUpResponse { from: me, entries }));
    }

    /// Learn every decided entry a peer replayed to us. Each is chosen (durable,
    /// quorum-decided), so `mark_chosen` records it authoritatively and advances
    /// the contiguous prefix — filling the hole a missed `Accept`+`Commit` left.
    fn on_catchup_response(&mut self, entries: BTreeMap<Slot, (Ballot, Command)>) {
        for (slot, (ballot, command)) in entries {
            self.mark_chosen(slot, &command, ballot);
        }
    }

    /// Install an opaque application snapshot from a peer (below-floor recovery):
    /// jump the chosen prefix to `chosen_index`, adopt `max(promise, ballot)` (the
    /// durable promise never regresses — the safety hinge that keeps a recovered
    /// node from re-voting under a stale ballot), and fully compact the log up to
    /// the snapshot (its state is folded into the opaque bytes). A stale snapshot
    /// that would not advance us is ignored.
    fn on_install_snapshot(
        &mut self,
        ballot: Ballot,
        chosen_index: Slot,
        snapshot: Value,
        sessions: Vec<SessionEntry>,
    ) {
        // Never go backward: a snapshot at or below our chosen prefix teaches us
        // nothing and must not lower the floor or re-truncate live slots.
        if self
            .hard_state
            .chosen_index
            .is_some_and(|ci| chosen_index <= ci)
        {
            return;
        }
        // Adopt the choosing ballot. `set_promise` only ever raises the promise
        // (it is a max), so even a far-behind node cannot regress its durable
        // promise here — installing the log does not un-promise a higher ballot.
        if ballot > self.hard_state.max_promised_ballot {
            self.set_promise(ballot);
        }
        // Wire value: saturate rather than overflow on an adversarial u64::MAX.
        let first = Slot(chosen_index.0.saturating_add(1));
        self.hard_state.chosen_index = Some(chosen_index);
        // Fully compact up to the snapshot: everything at or below `chosen_index`
        // is folded into the opaque bytes, so drop the in-memory prefix and raise
        // the floor to `first`.
        self.first_slot = first;
        self.accepted = self.accepted.split_off(&first);
        self.chosen = self.chosen.split_off(&first);
        self.proposer.retain(|slot, _| *slot >= first);
        // The prefix jumped without the contiguous walk running, so nothing
        // handed the folded slots' `inflight` entries over. Drop them: a mapping
        // to a slot that no longer exists would answer a retry with
        // `Duplicate(slot)` for a slot whose commit can never ack anyone, and
        // the reply would hang to the client's deadline every time.
        self.inflight.retain(|_, s| *s >= first);
        self.next_slot = self.next_slot.max(first);
        // Adopt the serving peer's session ledger for the folded prefix (#94):
        // those slots' log records will never be walked here, so this transfer
        // is the only way this node learns their `(client, seq) -> slot` facts —
        // both for the dedup fast path and for suppressing a later re-choose of
        // the same identity exactly like every peer does. `or_insert` keeps any
        // record this node already holds; the prefixes agree cluster-wide, so a
        // collision carries the same slot either way.
        for (client, seq, slot) in &sessions {
            self.applied_seq
                .entry(*client)
                .or_default()
                .entry(*seq)
                .or_insert(*slot);
        }
        // Persist the install (opaque bytes + boundary + sealed sessions).
        // Snapshot-xor-entries: this batch surfaces no committed user entries
        // for the folded prefix; the application installs the opaque state via
        // the driver's storage write, and the ledger is sealed beside it.
        self.pending_writes.push(WriteOp::InstallSnapshot {
            chosen_index,
            ballot,
            snapshot,
            sessions,
        });
        // Re-drive the contiguous walk: a `Commit` learned out of order may
        // already sit in `chosen` just above the boundary, and without the walk
        // this node would freeze at `chosen_index` forever — catch-up loops
        // (`mark_chosen` returns early for a slot already in `chosen`), and a
        // later leadership here would fence reads above the frozen prefix.
        self.advance_chosen_index();
    }

    /// Self-accept (if our promise allows) and broadcast `Accept` for `slot`.
    fn start_accept_round(&mut self, slot: Slot, command: Command) {
        let me = self.config.id;
        let ballot = self.ballot;
        let mut accepted_by = BTreeSet::new();
        // Never lower our promise: if a competing higher `Prepare` raised it
        // since we became leader, skip the self-accept (the round relies on
        // peer `Accepted`s and will stall, then we step down on the `Nack`).
        if ballot >= self.hard_state.max_promised_ballot {
            self.set_promise(ballot);
            self.record_accepted(slot, ballot, command.clone());
            accepted_by.insert(me);
        }
        self.proposer.insert(
            slot,
            Proposing {
                ballot,
                command: command.clone(),
                accepted_by,
            },
        );
        self.broadcast(&Message::Accept {
            from: me,
            ballot,
            slot,
            command,
        });
        self.try_decide(slot);
    }

    /// If an accept quorum holds for `slot`, the entry is chosen: record it and
    /// `Commit` to the peers.
    fn try_decide(&mut self, slot: Slot) {
        let quorum = self.quorum();
        let me = self.config.id;
        let decided = match self.proposer.get(&slot) {
            Some(p) if p.accepted_by.len() >= quorum => Some((p.ballot, p.command.clone())),
            _ => None,
        };
        let Some((ballot, command)) = decided else {
            return;
        };
        self.mark_chosen(slot, &command, ballot);
        self.broadcast(&Message::Commit {
            from: me,
            ballot,
            slot,
            command,
        });
        self.proposer.remove(&slot);
    }

    // ---- helpers ----------------------------------------------------------

    /// Quorum size of the cluster, per the configured [`crate::QuorumSystem`]
    /// (membership includes self).
    fn quorum(&self) -> usize {
        self.config
            .quorum_system
            .quorum_size(self.config.peers.len())
    }

    /// Raise (or re-affirm) the promised ballot to `ballot`, recording a
    /// [`WriteOp::SetPromise`] delta only when it actually changes. Callers that
    /// must never lower the promise guard with `ballot >` first.
    fn set_promise(&mut self, ballot: Ballot) {
        if self.hard_state.max_promised_ballot != ballot {
            self.hard_state.max_promised_ballot = ballot;
            self.pending_writes.push(WriteOp::SetPromise(ballot));
        }
    }

    /// Record `(ballot, command)` as accepted for `slot` in the working log and
    /// queue the matching [`WriteOp::AppendAccepted`] delta. An upsert-by-slot:
    /// a higher-ballot re-accept, or a chosen value overwriting a stale accept.
    fn record_accepted(&mut self, slot: Slot, ballot: Ballot, command: Command) {
        debug_assert!(
            slot >= self.first_slot,
            "never record an accept below the compaction floor"
        );
        self.accepted.insert(slot, (ballot, command.clone()));
        self.pending_writes.push(WriteOp::AppendAccepted {
            slot,
            ballot,
            command,
        });
    }

    /// Queue `msg` to every member except this node.
    fn broadcast(&mut self, msg: &Message) {
        let me = self.config.id;
        let targets: Vec<NodeId> = self
            .config
            .peers
            .iter()
            .copied()
            .filter(|&p| p != me)
            .collect();
        for to in targets {
            self.pending_messages.push((to, msg.clone()));
        }
    }

    /// Step down to Follower, abandoning any campaign or in-flight rounds, and
    /// ask the driver for a fresh randomized election timeout.
    fn become_follower(&mut self, leader: Option<NodeId>) {
        self.role = NodeRole::Follower;
        self.leader = leader;
        self.election = None;
        self.proposer.clear();
        // Unconfirmed read rounds die with the leadership; already-confirmed
        // `pending_read_states` stay — they were valid at their linearization
        // point and the driver drains them this same batch.
        self.read_rounds.clear();
        self.election_elapsed = 0;
        self.needs_election_timeout = true;
    }

    /// First slot not in the contiguous chosen prefix.
    fn first_unchosen(&self) -> Slot {
        match self.hard_state.chosen_index {
            Some(s) => Slot(s.0 + 1),
            None => Slot(0),
        }
    }

    /// Record `(slot, entry)` as chosen: persist, re-point the in-flight dedup
    /// mapping at this slot, and advance the contiguous chosen prefix.
    /// Idempotent.
    ///
    /// **Chosen is not applied.** Two of the three callers hand this
    /// non-contiguous slots — `on_commit` takes whatever the network delivers,
    /// and `try_decide` fires the moment a slot's accept quorum completes while
    /// the leader streams later slots concurrently, so slot 6 routinely decides
    /// before slot 5. So nothing here may record a command as *applied*: the
    /// `applied_seq` bump lives in the contiguous walk
    /// ([`RawNode::advance_chosen_index`]) alongside `pending_committed`, which
    /// is the definition [`RawNode::new`]'s boot rebuild has always used.
    fn mark_chosen(&mut self, slot: Slot, command: &Command, ballot: Ballot) {
        // A slot below our floor was chosen and then truncated; do not relearn it
        // (that would re-insert a record below the floor via `record_accepted`).
        if slot < self.first_slot {
            return;
        }
        if self.chosen.contains_key(&slot) {
            // Known value, nothing to relearn — but still re-drive the walk: a
            // snapshot install (or a boot) can leave `chosen_index` *below* a
            // slot already present in `chosen`, and a catch-up replay of that
            // slot is then the only message this node keeps receiving. Skipping
            // the walk here wedged that node in a forever catch-up loop.
            self.advance_chosen_index();
            return;
        }
        // Record the *chosen* value as the authoritative accepted command. Using
        // `insert` (not `or_insert_with`) is load-bearing: a node may hold a stale
        // lower-ballot accept it picked up from a failed earlier ballot, and
        // `chosen` is rebuilt from `accepted` on restart. Keeping the stale entry
        // would resurrect a value the cluster never chose for this slot. A chosen
        // value is durable and safe to record at its choosing ballot.
        self.record_accepted(slot, ballot, command.clone());
        if ballot > self.hard_state.max_promised_ballot {
            self.set_promise(ballot);
        }
        self.chosen.insert(slot, command.clone());
        // Re-point `inflight` at what this slot actually decided. Two halves,
        // and both matter:
        //
        // - Whatever was decided here, this slot can no longer be the landing
        //   place of some *other* in-flight client request, so drop any that
        //   still points at it. Keyed on the *slot*, not on a matching
        //   `Command::User`: a node that booted holding an accepted-but-unchosen
        //   entry at this slot (`RawNode::new` rebuilds `inflight` from exactly
        //   those) keeps a dangling mapping when the slot decides as something
        //   else — a `Noop` filled in by a new leader, say. The client's retry
        //   would then get `ProposeResult::Duplicate(slot)` for a slot whose
        //   commit never acks a proposer (a control command has no client
        //   waiter), and the reply would hang to the client's deadline forever.
        //   Clearing by slot lets the retry take a fresh slot and commit.
        // - Then map the entry this slot *did* decide to it. That is what keeps
        //   the chosen-but-not-yet-applied window safe: `applied_seq` only
        //   learns the command when the contiguous walk applies it, so between
        //   "chosen" and "applied" `inflight` is the only table that knows about
        //   it. A retry in that window must find it here and get
        //   `Duplicate(slot)` — the driver then parks the reply on `slot` and
        //   acks it out of the apply loop, exactly when the write enters the
        //   applied prefix. Miss both tables instead and the retry allocates a
        //   *fresh* slot for a command already chosen: duplicate execution,
        //   strictly worse than the early ack. This insert also covers the node
        //   that learns a slot chosen by `Commit` alone (it never proposed it,
        //   so it never had an `inflight` mapping to keep).
        self.inflight.retain(|_, s| *s != slot);
        if let Command::User(entry) = command
            && !self.applied_elsewhere(entry, slot)
        {
            // An identity already applied at another slot is a #94 duplicate:
            // this slot will suppress to a no-op at apply, so pointing a retry
            // at it would park a reply no commit ever acks. Leaving the table
            // alone lets the retry hit the `applied_seq` fast path instead.
            self.inflight.insert((entry.client, entry.seq), slot);
        }
        self.advance_chosen_index();
    }

    /// Whether `entry`'s `(client, seq)` identity is recorded in the applied
    /// ledger at a slot **other than** `slot` — the #94 duplicate test.
    fn applied_elsewhere(&self, entry: &Entry, slot: Slot) -> bool {
        self.applied_seq
            .get(&entry.client)
            .and_then(|m| m.get(&entry.seq))
            .is_some_and(|&first| first != slot)
    }

    /// Walk the contiguous chosen prefix forward, surfacing each newly-applied
    /// `(slot, entry)` for the application in order (no gaps).
    ///
    /// This is also where the **client dedup tables move from "in flight" to
    /// "applied"**, one slot at a time and only in prefix order. `applied_seq`
    /// means exactly what its name says and what [`RawNode::new`]'s boot rebuild
    /// has always meant by it — inside the contiguous chosen prefix — so
    /// [`RawNode::propose`]'s fast path can answer `Chosen` (an immediate
    /// `committed: true` to the client) without lying: the write really is in
    /// the applied prefix by then, and the slot it names really is one the node
    /// applied.
    fn advance_chosen_index(&mut self) {
        let mut next = self.first_unchosen();
        // Highest `up_to` from any `Truncate` control command that entered the
        // contiguous chosen prefix this pass. Applied *after* the walk so the
        // mutation `compact` makes to `chosen`/`accepted` cannot disturb the
        // iteration above.
        let mut truncate_up_to: Option<Slot> = None;
        while let Some(mut command) = self.chosen.get(&next).cloned() {
            self.hard_state.chosen_index = Some(next);
            self.pending_writes.push(WriteOp::SetChosenIndex(next));
            if let Command::Control(Control::Truncate { up_to }) = &command {
                truncate_up_to = Some(truncate_up_to.map_or(*up_to, |u| u.max(*up_to)));
            }
            // The slot is applied now, so it is no longer in flight — and only a
            // client entry carries `(client, seq)` dedup state at all (a control
            // command has none; its `Truncate` effect is handled just below).
            // Clearing by slot, the same key `mark_chosen` re-pointed the mapping
            // with, is what makes this the exact hand-off: whatever `inflight`
            // held for this slot leaves as `applied_seq` takes it over.
            self.inflight.retain(|_, s| *s != next);
            if let Command::User(entry) = &command {
                let seqs = self.applied_seq.entry(entry.client).or_default();
                match seqs.get(&entry.seq) {
                    // The #94 duplicate: correct Paxos chose this identity at a
                    // second slot (a retry served across a partition plus the
                    // mandatory P2c re-proposal of the deposed leader's lone
                    // accept). Execute the slot as a no-op. The decision reads
                    // only the replicated ledger, and the walk runs in slot
                    // order on every node, so first-slot-wins is cluster-wide
                    // deterministic — and `RawNode::new` re-derives the same
                    // set from sealed sessions + the retained log on restart.
                    Some(&first) if first != next => {
                        self.duplicate_slots.insert(next);
                        self.duplicates_suppressed += 1;
                        command = Command::Control(Control::Noop);
                    }
                    // First application of this identity: record it at this
                    // slot, and never overwrite it later — the ledger entry IS
                    // the at-most-once claim.
                    _ => {
                        seqs.insert(entry.seq, next);
                    }
                }
            }
            self.pending_committed.push((next, command));
            next = Slot(next.0 + 1);
        }
        // Apply (lazily, "in the background") the truncation the control command
        // decided: drop the now-safe prefix and raise the floor. `compact` clamps
        // to the chosen index just advanced, is idempotent, and emits a
        // `WriteOp::Truncate` ordered *after* the `SetChosenIndex` writes above, so
        // a durable floor never outruns the durable chosen index.
        if let Some(up_to) = truncate_up_to {
            self.compact(up_to);
        }
        // A read round waiting on the apply condition (`chosen_index >= index`,
        // the fresh-leader fence) resolves exactly here. No-op on a follower.
        self.try_confirm_reads();
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
        let hole = self.first_unchosen();
        // `hole` itself is never in `chosen`: `advance_chosen_index` runs after
        // every `mark_chosen`, so anything at or above it is strictly above.
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

    pub(crate) fn pending_snapshot_offers(&self) -> &[(NodeId, Slot, Ballot)] {
        &self.pending_snapshot_offers
    }

    pub(crate) fn pending_read_states(&self) -> &[ReadState] {
        &self.pending_read_states
    }

    pub(crate) fn clear_pending(&mut self) {
        self.pending_writes.clear();
        self.pending_messages.clear();
        self.pending_committed.clear();
        self.pending_snapshot_offers.clear();
        self.pending_read_states.clear();
    }
}

#[cfg(test)]
mod tests;
