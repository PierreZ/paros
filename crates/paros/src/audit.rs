//! The **audit port**: the driver's provider-generic observation seam.
//!
//! [`Audit`] is the mirror image of the driver's inline BUGGIFY sites. A site
//! *perturbs* the driver — it answers "should I take this rare-but-valid
//! alternative?" and the driver's behavior changes with the answer. The audit
//! only *observes*: the driver reports every externally meaningful state
//! transition, typed, at the instant it happens, and nothing it returns (it
//! returns nothing) can influence the run. Deleting every audit call must leave
//! the shipped program bit-identical.
//!
//! Each callback fires **after** the transition it reports is real: a durable
//! write after its fsync, a walked slot after the prefix moved over it, a send beside
//! the transmit. They sit exactly where the driver's `tracing` events already
//! are — the trace stays for humans, while correctness checking lives here,
//! where an implementation can fold each transition into O(1) incremental
//! state instead of re-scanning a growing event stream.
//!
//! Production passes [`NoAudit`]; every method defaults to a no-op.

use std::collections::BTreeMap;
use std::sync::Arc;

use paros_core::{
    AcceptorConfig, Ballot, Command, GcAck, GcStep, Handoff, JournalIdentifier, JournalView,
    LogRead, MatchRefusal, MatchmakerHardState, MatchmakerId, MatchmakerPhase, MatchmakerSet,
    Message, NodeId, Outcome, Party, PendingBootstrap, ProxyId, ReconfigureReply,
    ReconfigureRequest, ReconfigureResult, ReconfigurerStep, Registration, RegistrationKind, Seq,
    Slot, Value,
};

use crate::Address;
use crate::client::CallObserver;
use crate::driver::BootRefusal;
use crate::machine::{CellPlan, MachineRecord};
use crate::rpc::{EdgeRejection, MatchmakersRefusal, RetireRefusal};
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

/// What a delegated `Accept` did when it reached a proxy leader (#142), as
/// reported by [`Audit::proxy_delegated`]: the proxy's own counters, read at
/// the step, say which.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DelegationOutcome {
    /// A fresh round was opened and fanned out.
    Opened,
    /// A re-delegation of an open round: re-fanned-out (P2b-idempotent),
    /// the leader hint refreshed.
    Refanned,
    /// Ignored: a round this proxy already decided at that ballot, a ballot
    /// below the one it works for, or a delegation naming another party.
    Ignored,
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
    /// How many replicas the deployment runs (`Config::replica_count`, #144):
    /// the modulus of `Config::reply_owner`, zero on the plain deployment.
    pub replica_count: usize,
}

/// How a journal `Read` (#204) was answered, as reported by
/// [`Audit::log_read_served`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogReadAnswer {
    /// Served as soon as its quorum read confirmed.
    Immediate,
    /// Confirmed at the tail, then woken by a newly folded write (or by a
    /// truncation that overtook it).
    Woke,
    /// Confirmed at the tail and answered empty when its wait ran out.
    Expired,
}

/// One journal `Read` answer (#204), as it leaves a node or a replica. A
/// borrowed view of the page (see [`HistoryPage`] for why never a copy).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogReadReport<'a> {
    /// Where the read started (its `from_seq`).
    pub from: Seq,
    /// Set when the read started below `first_seq`: no records were
    /// answered, only the state.
    pub truncated: bool,
    /// The records, dense from `from`.
    pub records: &'a [Value],
    /// The journal state the page was served from.
    pub state: JournalView,
    /// How it was answered.
    pub answer: LogReadAnswer,
}

impl<'a> LogReadReport<'a> {
    /// The report of the core page `read` answering a read from `from`;
    /// `None` for [`LogRead::NotHeld`], which serves nothing (the read is
    /// answered unserved).
    #[must_use]
    pub fn of(read: &'a LogRead, from: Seq, answer: LogReadAnswer) -> Option<Self> {
        match read {
            LogRead::Truncated(state) => Some(Self {
                from,
                truncated: true,
                records: &[],
                state: *state,
                answer,
            }),
            LogRead::Page(page) => Some(Self {
                from,
                truncated: false,
                records: &page.records,
                state: page.state,
                answer,
            }),
            LogRead::NotHeld => None,
        }
    }
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
/// the inline BUGGIFY sites' job) and must not block — a callback
/// runs inline on the node loop.
#[allow(unused_variables)]
pub trait Audit {
    /// The observer of the library calls this node's own client makes (the
    /// cell coordinator's, #240), so a harness checks them with the rest of
    /// each journal's history. Asked once, when the client is built; `None`
    /// (production) observes nothing.
    fn call_observer(&self) -> Option<Arc<dyn CallObserver>> {
        None
    }

    /// This node durably raised its promised ballot (after the fsync).
    fn promised(&self, node: NodeId, ballot: Ballot) {}

    /// This node durably accepted `command` (hashed to `vhash`) at `ballot`
    /// for `slot`. `promised` is the node's promise at the time of the write,
    /// so the never-accept-above-promise invariant is checkable per slot.
    fn accepted(&self, node: NodeId, slot: Slot, ballot: Ballot, promised: Ballot, vhash: u64) {}

    /// This node durably advanced its chosen index.
    fn chosen_index(&self, node: NodeId, index: Slot) {}

    /// This node is about to raise its compaction floor to `first` (a
    /// `Truncate` or a trim-point jump, staged before its sync). A store
    /// whose commit has an unknown outcome can make the floor durable even
    /// when the sync never returns (a crash inside the commit), so a
    /// recovered log may lack records below a floor that was only
    /// requested, never reported [`truncated`](Audit::truncated).
    fn floor_requested(&self, node: NodeId, first: Slot) {}

    /// This node durably truncated its log prefix; `first` is the new
    /// compaction floor (the first slot still retained).
    fn truncated(&self, node: NodeId, first: Slot) {}

    /// This node (or replica) durably jumped below a peer's trim point
    /// (#186, `TrimmedTo`): its floor is `point` and its chosen prefix
    /// covers everything below it, without the walk having moved over those
    /// slots. Its promise did not move.
    fn trimmed_to(&self, node: NodeId, point: Slot) {}

    /// This node (or replica) answered a journal `Read` (#204) — reported
    /// once, as the answer is handed to the reply seam (a reply the seam
    /// then drops was still served).
    fn log_read_served(&self, node: NodeId, report: &LogReadReport<'_>) {}

    /// This node refused a client call naming a journal it does not serve
    /// (`0`, or any other id than its own, #185). `call` names the RPC.
    fn journal_refused(&self, node: NodeId, journal: JournalIdentifier, call: &'static str) {}

    /// This node folded the system-journal record at position `seq` of
    /// `journal` (#189: the directory or the node registry) into `event` —
    /// reported once per position, in order, at the instant the fold moves.
    fn system_folded(
        &self,
        node: NodeId,
        journal: JournalIdentifier,
        seq: u64,
        event: &crate::system::SystemEvent,
    ) {
    }

    /// This node folded a checkpoint record (#230) at position `seq` of
    /// system journal `journal`: `verified` is `Some(equal)` when its fold
    /// already held every position below and compared its own state with
    /// the checkpoint's, `None` when it restored from it. `state` is the
    /// registry the fold holds right after it (#247: what an oracle that
    /// models the registry resumes from across a truncation).
    fn checkpoint_folded(
        &self,
        node: NodeId,
        journal: JournalIdentifier,
        seq: u64,
        verified: Option<bool>,
        state: &crate::system::Registry,
    ) {
    }

    /// This node started serving `journal`, a journal the directory created
    /// naming it (#189).
    fn journal_started(&self, node: NodeId, journal: JournalIdentifier) {}

    /// This node held this port's journal for one beat
    /// (`crate::scenario::HOLD_JOURNAL`, #188): it skipped the journal's
    /// tick, and it drops the journal's inbound peer messages while the
    /// hold lasts. The non-interference oracle keys on it: the journal's
    /// siblings must keep committing.
    fn journal_held(&self, node: NodeId) {}

    /// This node stopped serving `journal` for good (#189): its tombstone was
    /// folded, or this node's own retirement was.
    fn journal_stopped(&self, node: NodeId, journal: JournalIdentifier) {}

    /// This node refused a peer message for `journal` from `from`, a node its
    /// registry fold does not have in the pool yet (#189). A liveness cost
    /// until the fold catches up, never a safety one.
    fn unpooled_message(&self, node: NodeId, journal: JournalIdentifier, from: NodeId) {}

    /// This node's registry fold admitted `admitted` to the pool (#189): its
    /// messages are accepted from now on.
    fn pool_admitted(&self, node: NodeId, admitted: NodeId) {}

    /// This node applied the chosen `command` at `slot` (hashed to `vhash`),
    /// advancing its contiguous applied prefix, and the journal state
    /// machine judged it `outcome` (#204; `None` for a `Noop`). The journal
    /// oracles key on it: one outcome per slot on every node, dense
    /// positions, one owner per generation.
    fn applied(
        &self,
        node: NodeId,
        slot: Slot,
        vhash: u64,
        command: &Command,
        outcome: Option<&Outcome>,
    ) {
    }

    /// This node handed `msg` to the transport, addressed to `to`. Reports the
    /// core's outbound decision even when the network later drops it.
    fn sent(&self, node: NodeId, to: NodeId, msg: &Message) {}

    /// This node handed `msg` to the transport, addressed to the proxy
    /// leader `proxy` (#142) — **every** node-to-proxy message, which is one
    /// of two things. A leader's delegated `Accept`: the first delegation
    /// of a round, a re-send's re-delegation, or a handoff successor's
    /// re-delegation with `leader` naming itself — the leader's exercise of
    /// its Phase-2 authority for that slot, exactly as a colocated `Accept`
    /// send is. Or an acceptor's reply to a delegated round — the
    /// `Accepted` or `Nack` it sends to the `reply_to` party instead of the
    /// leader — which makes exactly the claim it makes on the way to a
    /// leader (an `Accepted` says "I hold this durably"), so the
    /// persist-before-send checks [`Audit::sent`] runs on it apply here
    /// unchanged: routing Phase 2 through a proxy removes no check.
    fn sent_to_proxy(&self, node: NodeId, proxy: ProxyId, msg: &Message) {}

    /// The proxy leader `proxy` handed `msg` to the transport, addressed to
    /// `to`: the `Accept` it fans out to a column (`reply_to` naming the
    /// proxy, `leader` the node whose authority it carries), the `Commit` it
    /// emits to every learner, or a `Nack` it relays to the delegating
    /// leader.
    fn proxy_sent(&self, proxy: ProxyId, to: NodeId, msg: &Message) {}

    /// The proxy leader `proxy` **evicted** its open round for `slot` on the
    /// driver's beat (`ProxyLeader::expire_stale`, #142): re-fanned-out
    /// `DriverTunables::proxy_round_resends` times without an answer. Not a
    /// decision — nothing was emitted and the slot is not remembered as
    /// done; a later delegation reopens it.
    fn proxy_round_expired(&self, proxy: ProxyId, slot: Slot) {}

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

    /// This leader resigned on its own (an inline BUGGIFY site in the
    /// driver), and it did.
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
    /// fence stayed uncovered — the deliberate fallback to an ordinary
    /// Phase 1. `count` is the monotone total for this incarnation.
    fn handoff_fence_expired(&self, node: NodeId, count: u64) {}

    /// This node holds a chosen slot above its contiguous applied prefix:
    /// `hole` is the first slot missing, `above` the highest chosen slot past
    /// it. Reported once per tick for as long as the gap lasts.
    fn chosen_gap(&self, node: NodeId, hole: Slot, above: Slot) {}

    /// This node answered a client call — a `Write`, a `SetLeader` or a
    /// `Truncate` (#204) — whose `command` it proposed at `slot`, with the
    /// verdict the journal state machine gave there (`outcome`), once the
    /// slot applied. A call whose slot decided another command, or was
    /// trimmed before it applied here, is [`Audit::waiter_superseded`]
    /// instead: it gets no verdict.
    fn answered(&self, node: NodeId, slot: Slot, command: &Command, outcome: &Outcome) {}

    /// This node opened the **quorum read** `ctx` (#143, #260): a token
    /// unique within this incarnation, named again by
    /// [`Audit::quorum_read_served`] if the read completes. Reported as the
    /// core opens it, so whatever was chosen before this instant is what the
    /// read must serve.
    fn quorum_read_opened(&self, node: NodeId, ctx: u64) {}

    /// This node served a **quorum read** (#143) — the confirmation every
    /// journal `Read` waits on (#204): its row answered whole at
    /// `watermark` (the maximum vote watermark, `None` when nobody in the
    /// row had voted), and the page was served from `served`, the last slot
    /// of this node's journal fold at serve time — at or past the watermark.
    /// `opened` is the same when the read opened (what an unconfirmed local
    /// read would have answered), `row` the grid row the
    /// read asked (`None`: the whole configuration, under a majority or a
    /// flexible split), `leader` whether this node led when it served, and
    /// `ctx` the token [`Audit::quorum_read_opened`] reported.
    #[allow(clippy::too_many_arguments)]
    fn quorum_read_served(
        &self,
        node: NodeId,
        ctx: u64,
        row: Option<usize>,
        watermark: Option<Slot>,
        served: Option<Slot>,
        opened: Option<Slot>,
        leader: bool,
    ) {
    }

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

    /// A machine advertising `addr` read its record at boot (#246): `None` for an
    /// empty disk, which it formats next. Reported before anything is
    /// written, on the machine's own audit.
    fn machine_booted(&self, addr: &Address, record: Option<&MachineRecord>) {}

    /// A machine rewrote its record durably (after the rename and the
    /// directory syncs): its identity, or a step of the cell decree — a
    /// raised promise, or the vote that forms it.
    fn machine_recorded(&self, record: &MachineRecord) {}

    /// A machine advertising `addr` is about to format the stores of `plan` as
    /// member `node` (the step before its vote); `leftovers` when the disk
    /// already holds journals an unvoted attempt left.
    fn cell_formatting(&self, addr: &Address, node: NodeId, plan: &CellPlan, leftovers: bool) {}

    /// The driver refused to boot this identity (#147): the operator's
    /// [`BootKind`](crate::BootKind) claim and the store's format marker
    /// disagree. Reported at the instant of the decision, before the
    /// [`RunError::Refused`](crate::RunError::Refused) exit; nothing was
    /// written and no message left.
    fn boot_refused(&self, node: NodeId, refusal: BootRefusal) {}

    /// A [`LogStorage`](crate::LogStorage) call surfaced `error` and the
    /// driver decided `decision` — reported at the instant of the decision,
    /// before the crash unwinds. The error carries the fault kind, the record
    /// identity, and the durability outcome as data, so a checker can fold
    /// injected-vs-detected accounting into O(1) state without string parsing.
    fn storage_fault(&self, node: NodeId, error: &StorageError, decision: StorageFaultDecision) {}

    /// A storage fault ended this journal's incarnation on `node` (#188):
    /// the journal is quarantined — it sends nothing and answers nothing —
    /// while the node serves its other journals, until the driver re-opens
    /// it from its store (the next [`Audit::recovered`] boot report) or the
    /// store refuses to open for good. Reported to the quarantined
    /// journal's own audit port.
    fn journal_quarantined(&self, node: NodeId) {}

    /// `from` (a node, or a proxy leader) dropped one outbound message at
    /// the send seam (an inline BUGGIFY location per kind family, #318:
    /// per-message loss, indistinguishable from network loss to the peers).
    fn dropped_at_send(&self, from: Party, to: Party, msg: &Message) {}

    /// The driver deliberately sent this one outbound message twice (an
    /// inline BUGGIFY location per kind family at the send seam, #318).
    fn duplicated_at_send(&self, from: Party, to: Party, msg: &Message) {}

    /// The driver deliberately dropped this one client-facing reply after the
    /// server state advanced (the reply seam's inline location for its kind,
    /// #318, or for a write the lost-verdict scenario's named location,
    /// [`LOSE_VERDICTS`](crate::scenario::LOSE_VERDICTS)).
    fn client_reply_dropped(&self, node: NodeId, reply: crate::Reply) {}

    /// This Ready batch started `started` inherited or gap-fill accept rounds,
    /// including `gap_fills` fresh no-ops; `remaining` slots are deferred.
    /// `page` is the node's recovery page size, the bound `started` stays
    /// under ([`crate::DriverTunables::recovery_page`], #330).
    fn recovery_batch(
        &self,
        node: NodeId,
        started: u64,
        gap_fills: u64,
        remaining: u64,
        page: u64,
    ) {
    }

    /// This node's election timeout base was doubled `doublings` times: its
    /// previous campaigns expired with no leader known (the election
    /// backoff, [`crate::DriverTunables::election_backoff_doublings`]).
    fn election_backoff(&self, node: NodeId, doublings: u32) {}

    /// This node now runs with an election timeout of `ticks` (the driver's
    /// randomized draw, re-drawn at every demotion). The `CheckQuorum`
    /// window a leader re-proves its ack quorum in is exactly this long, so
    /// a liveness oracle that bounds a deposed leader's remaining beats must
    /// measure against it, never against a fixed count.
    fn election_timeout_set(&self, node: NodeId, ticks: u64) {}

    /// This node's logical clock ticked (`ColocatedNode::tick`), once per driver
    /// tick. The unit every core timeout is counted in.
    fn ticked(&self, node: NodeId) {}

    /// This node received a `HeartbeatAck` from `from` echoing `ballot`,
    /// reported at the inbox before the core folds it — whether or
    /// not the core counts it (a stale ballot's ack moves nothing). An ack
    /// that reaches a leader refills its `CheckQuorum` window, and an ack in
    /// flight can be older than a window: the deposed-leader oracle measures
    /// from the last ack received, never from the promise-majority alone.
    fn heartbeat_ack_received(&self, node: NodeId, from: NodeId, ballot: Ballot) {}

    /// This node received a `Prepare` below its own compaction floor — the
    /// "campaign against a truncated acceptor" interleaving.
    fn prepare_below_floor(&self, node: NodeId, from_slot: Slot, floor: Slot) {}

    /// This node dropped a parked call's verdict because its slot decided a
    /// *different* command (a stale leader's admission superseded by the
    /// majority's decision), or was trimmed below this node's floor before
    /// it applied here; the client was answered with no verdict — an
    /// ambiguous outcome — instead of a false one.
    fn waiter_superseded(&self, node: NodeId, slot: Slot) {}

    /// This node, as Leader, spent a full election-timeout window without an
    /// ack quorum and demoted itself (`CheckQuorum`, #95). `count` is the number
    /// of such step-downs in the batch (in practice 1).
    fn quorum_lost(&self, node: NodeId, count: u64) {}

    /// This node, as a settled leader, filled `count` slots with a `Noop` up
    /// to a vote watermark a pre-read reported past its allocator frontier
    /// (#204, `ColocatedNode::watermark_fills`).
    fn watermark_filled(&self, node: NodeId, count: u64) {}

    /// This node booted with recoverable **faulty entries** (Stage 8): the
    /// scan classified each record's value lost but its identity known, and
    /// the node reports them through the Promise tri-state instead of
    /// crashing. Reported once per boot, before [`Audit::recovered`], so the
    /// divergence checks can key their explained-only rule on it.
    fn faulty_reported(&self, node: NodeId, entries: &[(Slot, Ballot)]) {}

    /// Monotone repair-progress totals for this incarnation, reported when
    /// they change: local faulty records repaired in place, Case-1 straggler
    /// re-proposals, Case-2 straggler no-op fills, and recovery-timeout
    /// step-downs (CTRL §4.2).
    fn repair_progress(
        &self,
        node: NodeId,
        repaired: u64,
        case1: u64,
        case2: u64,
        step_downs: u64,
    ) {
    }

    /// This node answered a journal `Read` `served: false` because its
    /// quorum read did not confirm: `early` when the driver's inline
    /// early-expiry location fired before the read's confirmation deadline,
    /// otherwise the deadline itself ran out.
    fn read_expired(&self, node: NodeId, early: bool) {}

    /// `from` (a node, or a proxy leader) dropped one outbound message at a
    /// bounded in-process mailbox (the lossy per-peer transport handoff):
    /// either the enqueue found the peer queue full, or the delivery task
    /// discarded a stale backlog entry to keep the newest batch. `kind` is
    /// the message's stable label. Deliberately lossy by design
    /// (heartbeats/resends repair it); surfaced so a sweep can see the loss
    /// instead of inferring it.
    fn dropped_at_mailbox(&self, from: Party, to: Party, kind: &'static str) {}

    /// One peer-delivery RPC toward `to` failed or timed out, so the batch
    /// never (provably) entered the peer's inbox: the connection was down, or
    /// the peer's bounded inbox stayed full for the whole `delivery_timeout`.
    /// The peer acknowledges a batch as soon as its messages are *enqueued*,
    /// never after it has stepped them, so a slow peer loop is not a failed
    /// delivery — only an unreachable or saturated peer is. A timed-out batch
    /// may still have been partly enqueued; the protocol's heartbeats and
    /// resends repair whichever messages were lost. Reported from the
    /// delivery task (the audit handle is cloned into it), so implementations
    /// must stay observation-only here as everywhere.
    fn delivery_failed(&self, from: Party, to: Party) {}

    // ---- proxy leaders (#142): the leader's side and `run_proxy` -------------

    /// This leader **took back** the round at `slot` it had delegated to
    /// `proxy` — re-delegated `after_resends` times without the proxy's
    /// `Commit` — and now runs it colocated
    /// ([`paros_core::ColocatedNode::take_back_delegated`]). Reported once
    /// per round taken back, on the beat that took it.
    fn delegation_taken_back(&self, node: NodeId, slot: Slot, proxy: ProxyId) {}

    /// The replica `replica` (#144, a learner outside the pool that is not
    /// an acceptor) (re)booted from its durable chosen log: the chosen
    /// index it rebuilt (`None` = an empty prefix) and its compaction floor.
    /// Reported as it boots, before it learns anything new.
    fn replica_booted(&self, replica: NodeId, chosen_index: Option<Slot>, floor: Slot) {}

    /// The proxy leader `proxy` (re)booted, empty, over the bootstrap
    /// configuration `acceptors`. A proxy holds nothing durable, so every
    /// boot is a first boot; a rebooted proxy relearns its rounds from the
    /// leader's re-delegations.
    fn proxy_booted(&self, proxy: ProxyId, acceptors: &AcceptorConfig) {}

    /// The proxy leader `proxy` stepped a delegated `Accept` from `leader`
    /// for `slot` at `ballot` (the command hashed to `vhash`), and `outcome`
    /// says what it did with it. Reported at the step, before the batch it
    /// produced is drained.
    fn proxy_delegated(
        &self,
        proxy: ProxyId,
        leader: NodeId,
        slot: Slot,
        ballot: Ballot,
        vhash: u64,
        outcome: DelegationOutcome,
    ) {
    }

    /// The proxy leader `proxy` closed `count` open rounds of a lower ballot
    /// because a delegation at a higher one arrived: the leadership they
    /// belonged to is superseded and no `Commit` will ever close them here.
    fn proxy_rounds_superseded(&self, proxy: ProxyId, count: u64) {}

    /// The proxy leader `proxy` fanned the `Accept` for `slot` at `ballot`
    /// (carrying `leader` as the hint and the command hashed to `vhash`) out
    /// to `addressees` acceptors of `column`. Once per fan-out (the first,
    /// and every re-fan-out a re-delegation or the proxy's beat produces),
    /// reported as the batch is drained, before its messages leave.
    #[allow(clippy::too_many_arguments)]
    fn proxy_fanned_out(
        &self,
        proxy: ProxyId,
        leader: NodeId,
        slot: Slot,
        ballot: Ballot,
        vhash: u64,
        column: Option<usize>,
        addressees: usize,
    ) {
    }

    /// The proxy leader `proxy` **decided** `slot` at `ballot` (the command
    /// hashed to `vhash`) on a Phase-2 quorum of the round's column and is
    /// emitting the `Commit`. Reported as the batch is drained, before the
    /// `Commit` leaves — the instant an oracle judges the decision against
    /// the durable accepts it has already folded.
    fn proxy_decided(&self, proxy: ProxyId, slot: Slot, ballot: Ballot, vhash: u64) {}

    /// The proxy leader `proxy` relayed an acceptor's `Nack` for `slot` at
    /// `ballot` to `leader`, the node that delegated the round, and closed
    /// the round.
    fn proxy_nack_relayed(&self, proxy: ProxyId, leader: NodeId, slot: Slot, ballot: Ballot) {}

    /// This node lost its leadership with `calls` journal calls still
    /// parked, whose slots may yet decide under the successor: their clients
    /// time out, on purpose (an ambiguous outcome).
    fn waiters_cleared(&self, node: NodeId, calls: u64) {}

    /// The RPC edge of `at` (a node, a proxy leader or a replica) refused an inbound
    /// request before it reached the loop — a peer message that decoded from
    /// the wire but not into a `Message`. The refusal happens at the edge;
    /// nothing inside the process changed.
    fn edge_rejected(&self, at: Party, kind: EdgeRejection) {}

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

    /// This node opened a membership probe tagged `ballot` (#173): its
    /// belief is only the bootstrap default `believed`, which does not name
    /// it, so its election clock asks the matchmakers which configuration is
    /// in force instead of skipping the campaign. Reported at the instant the
    /// probe opens, before any request is sent. Never fires on plain
    /// Multi-Paxos. `generation` is the matchmaker set the probe asks.
    fn membership_probe_opened(
        &self,
        node: NodeId,
        ballot: Ballot,
        believed: &AcceptorConfig,
        generation: u64,
    ) {
    }

    /// This node handed its membership probe's request for `ballot` to the
    /// transport, addressed to `matchmaker` (the first send or a re-send).
    fn membership_probe_sent(&self, node: NodeId, matchmaker: MatchmakerId, ballot: Ballot) {}

    /// A matchmaker quorum answered this node's membership probe for
    /// `ballot` and it closed: `effective` is the ballot the node's belief is
    /// bound to (the effective configuration it adopted, or a newer one it
    /// already held; `None`: the bootstrap stands, now heard), and `member`
    /// whether the belief names
    /// the node — in which case a campaign opened in the same step.
    fn membership_probe_closed(
        &self,
        node: NodeId,
        ballot: Ballot,
        effective: Option<Ballot>,
        member: bool,
    ) {
    }

    /// A matchmaker answered this node's membership probe for `ballot` after
    /// it had closed with the node outside, naming a newer reconfiguration
    /// (#278): `effective` is the ballot the belief is now bound to, and
    /// `member` whether it names the node — a campaign opened in the same
    /// step.
    fn membership_probe_late(
        &self,
        node: NodeId,
        ballot: Ballot,
        effective: Option<Ballot>,
        member: bool,
    ) {
    }

    /// This candidate's matchmaking quorum named a reconfiguration to a
    /// configuration other than the one its ordinary campaign registered for
    /// `ballot`: the campaign was abandoned and the configuration registered
    /// at `newest` — the effective configuration — adopted as the node's
    /// belief (`ColocatedNode::on_match_reply`, `StaleConfiguration`).
    fn matchmaking_stale_configuration(&self, node: NodeId, ballot: Ballot, newest: Ballot) {}

    /// This candidate's election clock fired while its matchmaking was still
    /// open and re-asked the unanswered matchmakers instead of abandoning the
    /// campaign (`ColocatedNode::tick`). `count` is the monotone total for this
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
    /// (`refusal` `None`) or refused for `refusal`.
    fn reconfigure_matchmakers_acked(&self, node: NodeId, refusal: Option<MatchmakersRefusal>) {}

    /// This node's reconfigurer handed `request` to the transport, addressed
    /// to `matchmaker` (the first send or a re-send).
    fn reconfigure_request_sent(
        &self,
        node: NodeId,
        matchmaker: MatchmakerId,
        request: &ReconfigureRequest,
    ) {
    }

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

    /// This node answered an operator `Retire` request: accepted
    /// (`refusal` `None`: the node shuts down for good at its next tick) or
    /// refused, with `refusal` naming the leg that refused it
    /// ([`RetireRefusal`]; `Stale` is #165's freshness leg).
    fn retire_acked(&self, node: NodeId, refusal: Option<RetireRefusal>) {}

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
        registry: &BTreeMap<Ballot, Registration>,
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
        registry: &BTreeMap<Ballot, Registration>,
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

    /// This matchmaker is answering `to`'s membership probe for `ballot`
    /// (#173) with the effective configuration it durably holds; nothing was
    /// registered or written. Reported at the instant the answer leaves.
    fn match_probed(
        &self,
        matchmaker: MatchmakerId,
        to: NodeId,
        ballot: Ballot,
        generation: u64,
        effective: Option<&(Ballot, AcceptorConfig)>,
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

    /// The matchmaker driver refused to boot this matchmaker (#183): the
    /// operator's [`BootKind`](crate::BootKind) claim and the registry's
    /// format marker disagree. The [`Audit::boot_refused`] twin, reported
    /// at the instant of the decision, before the
    /// [`RunError::Refused`](crate::RunError::Refused) exit; nothing was
    /// written and no reply left.
    fn matchmaker_boot_refused(&self, matchmaker: MatchmakerId, refusal: BootRefusal) {}

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
