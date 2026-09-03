//! The driver's observability vocabulary: the `EV_*` tracing event names the
//! oracles match on, and the small pure helpers that turn a domain value into
//! the stable field a trace carries (value/command hashes, message labels, the
//! ballot-carrying route triple).

use paros_core::{Ballot, Command, ConfigId, Control, Message, NodeId, Registration, Slot};

use crate::grpc::internal;

/// Tracing event name for a node logical-clock tick. Emitters use the string
/// literal (tracing requires one); readers (oracles) match on this constant.
pub const EV_NODE_TICK: &str = "node_tick";

/// Tracing event: this node raised its durable promised ballot. Carries `node`
/// (id) and the promised ballot (`pround`/`pbnode`). The safety oracle reads it
/// for the monotonic-promise invariant. (Per-slot accepted state is surfaced
/// separately by [`EV_PERSIST`], so never-accept-below-promise is checked across
/// the whole log, not just slot 0.)
pub const EV_NODE_STATE: &str = "node_state";

/// Tracing event: this node durably persisted an accepted `(ballot, entry)` for a
/// slot (a `WriteOp::AppendAccepted`). Carries `node`, `slot`, the node's current
/// promised ballot (`pround`/`pbnode`), the accepted ballot (`around`/`abnode`),
/// and the value hash (`vhash`). The safety oracle reads it for the
/// never-accept-below-promise invariant per slot; the recovery oracle reads it to
/// check a pre-crash accepted `(slot -> value)` is stable across a restart.
pub const EV_PERSIST: &str = "persist";

/// Tracing event: on (re)boot, this node recovered an accepted record from
/// durable storage. Carries `node`, `slot`, the accepted ballot (`around`/
/// `abnode`), and the value hash (`vhash`). The recovery oracle reads it to check
/// a restart never changes a pre-crash accepted `(slot -> value)`.
pub const EV_RECOVERED: &str = "recovered";

/// Tracing event: this node started an incarnation, having rebuilt its volatile
/// state from durable storage. Carries `node`. Fires on the initial boot and on
/// every restart (a seam-crash recovery re-run or an attrition process kill), so
/// it is the reliable "this node came (back) up" marker. Purely observational: no
/// oracle asserts on it; the recovery recorder derives per-node *restarts* from it
/// (every `booted` after the first). The crash/restart animation reads it.
pub const EV_BOOTED: &str = "booted";

/// Tracing event: this node crashed at a durability seam inside a `Ready` batch
/// or the chunk-repair pipeline (a `buggify`-injected [`Seam`] crash). Carries
/// `node` and `seam` (`"before_sync"` — the whole un-synced batch is lost —
/// `"after_sync_before_send"` — the writes are durable but the batch's messages
/// never left — `"after_apply_before_sync"`, `"before_chunk_sync"`, or
/// `"after_chunk_restore_before_sync"`). After-sync events also carry `snapshot_offers`, the number of
/// snapshot transfers dropped with the batch. Provider-generic but inert in production, where
/// [`NoHooks`](crate::NoHooks) never fires. Purely observational; the crash
/// animation reads it to mark the persist/send seam a node died on.
pub const EV_CRASHED: &str = "crashed";

/// Tracing event: the driver deliberately skipped re-sending one or more
/// pending `Accept`s on this beat.
pub const EV_RESEND_SKIPPED: &str = "accept_resend_skipped";

/// Tracing event: the driver deliberately asked the current leader to resign.
pub const EV_LEADERSHIP_RESIGNED: &str = "leadership_resigned";

/// Tracing event: this leader cooperatively relinquished its Phase-2 authority to
/// a named successor and demoted itself in the same core call (`DPaxos` leader
/// handoff). Fields: `node`, `to`, `round`/`bnode` (the transferred authority),
/// `next_slot`, `decided`, `pending`.
pub const EV_AUTHORITY_RELINQUISHED: &str = "authority_relinquished";

/// Tracing event: this node installed a predecessor's transferred authority and
/// continues Phase 2 under it with **no** Phase 1 of its own. Fields: `node`,
/// `from`, `round`/`bnode`, `next_slot`, `tail`.
pub const EV_AUTHORITY_INSTALLED: &str = "authority_installed";

/// Tracing event: this node refused an incoming transfer. Fields: `node` plus the
/// monotone per-reason totals `target`, `stale`, `shape`, `unfit`.
pub const EV_HANDOFF_REFUSED: &str = "handoff_refused";

/// Tracing event: a handoff-installed leadership resigned because its inherited
/// read fence stayed uncovered — the deliberate fallback to ordinary Phase 1.
/// Fields: `node`, `count`.
pub const EV_HANDOFF_FENCE_EXPIRED: &str = "handoff_fence_expired";

/// Tracing event: a snapshot install persisted while this node was a live
/// Candidate (`role == Candidate`, election open). This is the #88 window —
/// `on_install_snapshot` may raise the candidate's promise above the ballot it
/// is campaigning at — surfaced so the sweep can prove the interleaving is
/// actually visited. Carries `node`.
pub const EV_SNAPSHOT_MID_ELECTION: &str = "snapshot_mid_election";

/// Tracing event: the driver deliberately dropped one outbound protocol message
/// at the send seam (after durability, before the transport). Carries `node`,
/// `to`, `kind`, and for an `Accept` the `slot`. Indistinguishable from network
/// loss to the peers; emitted so the sweep can prove the per-message-loss
/// BUGGIFY location is active and so a trace shows why a message never arrived.
pub const EV_SEND_DROPPED: &str = "msg_dropped_at_send";

/// Tracing event: the driver deliberately sent one outbound protocol message
/// twice at the send seam. Carries `node`, `to`, `kind`. Retransmission is
/// legal transport behavior; the sweep uses it to prove set-based quorum
/// counting tolerates duplicates.
pub const EV_SEND_DUPLICATED: &str = "msg_duplicated_at_send";

/// Tracing event: the driver deliberately dropped one client-facing reply
/// after the server state advanced. Carries `node` and `reply`
/// (`propose`/`propose_dedup`/`read`). The client's retry takes the
/// `(client, seq)` dedup path, which is the edge this exists to exercise.
pub const EV_CLIENT_REPLY_DROPPED: &str = "client_reply_dropped";

/// Tracing event: the driver selected the shortest valid election timeout.
/// Carries `node` and `ticks`. The driver-hook oracle uses it to prove the
/// timeout-jitter BUGGIFY location is active.
pub const EV_ELECTION_TIMEOUT_EXTREME: &str = "election_timeout_extreme";

/// Tracing event: a [`NodeStorage`] call failed and the driver took its
/// deliberate crash decision (see [`RunError::Storage`]). Carries `node`, the
/// human-readable `error` (the typed [`StorageError`] travels on the
/// [`Audit::storage_fault`] callback), and `decision` (`"crash"` — Stage 6's
/// only reaction). Emitted at the instant of the decision, before the crash
/// unwinds the incarnation.
pub const EV_STORAGE_FAULT: &str = "storage_fault";

/// Tracing event: this node flushed a `Ready` batch's durable writes. Carries
/// `node`, `sync` (whether the batch required an fsync-before-send —
/// [`MustSync::Sync`] — or a relaxed write), and `writes` (op count). Emitted once
/// per non-empty batch, right after the flush. Purely observational; it is the
/// "was this batch fsync'd?" marker the persist/send-seam animation renders.
pub const EV_SYNCED: &str = "synced";

/// Tracing event: this node applied a chosen value. Carries `node`, `slot`, and
/// the value hash (`vhash`). The safety oracle reads it for the
/// at-most-one-value-chosen invariant.
pub const EV_CHOSEN: &str = "value_chosen";

/// Tracing event: this node sent a protocol message. Carries `node` (sender),
/// `to` (destination), and `kind`; for the six ballot-carrying Paxos kinds it
/// also carries the ballot (`bround`/`bnode`) and `slot`. An `accept` additionally
/// carries the proposed command's `vhash` — the only message that proposes a
/// value, and so the only one whose hash the safety oracle needs to check that one
/// ballot proposes at most one command per slot. The wasm demo pairs it with
/// [`EV_MSG_RECV`] to draw the protocol timeline.
pub const EV_MSG_SENT: &str = "msg_sent";

/// Tracing event: this node received a protocol message (the mirror of
/// [`EV_MSG_SENT`]). Carries `node` (receiver), `from` (sender), and `kind`; for
/// the six ballot-carrying Paxos kinds it also carries `bround`/`bnode`/`slot`. A
/// sent message with no matching receive is one the network dropped.
pub const EV_MSG_RECV: &str = "msg_received";

/// Tracing event: this node became leader. Carries `node`, the won ballot
/// (`round`/`bnode`) and the promise it held at the instant of victory
/// (`pround`/`pbnode`). The leadership oracle asserts per-node ballot
/// monotonicity, and — the #67 check — that a fresh leader's promise never sits
/// above the ballot it just won.
pub const EV_LEADER: &str = "leader_elected";

/// Tracing event: this node advanced its applied (contiguous chosen) prefix.
/// Carries `node`, `slot` (the slot just applied), and `applied_index` (the new
/// high-water mark). The no-gaps oracle asserts the prefix grows by one without
/// skipping.
pub const EV_APPLIED: &str = "log_applied";

/// Tracing event: this node durably truncated its log prefix (a
/// `WriteOp::Truncate`). Carries `node` and `first` (the new compaction floor:
/// the first slot still retained). Emitted only after the fsync, like
/// [`EV_PERSIST`], so it never claims a truncation a `BeforeSync` crash discards.
/// The truncation oracle reads it to check the log stays bounded and nothing
/// below the floor is ever persisted or recovered again.
pub const EV_COMPACTED: &str = "compacted";

/// Tracing event: this node installed an opaque application snapshot from a peer
/// (a `WriteOp::InstallSnapshot`), jumping its chosen prefix. Carries `node`,
/// `chosen_index` (the commit index the snapshot brought it up to), and `first`
/// (the new compaction floor). Emitted only after the install is fsync'd. The
/// snapshot oracle reads it to confirm a below-floor node recovered, and the
/// no-gaps oracle reads it to admit the applied-index jump the install performs.
pub const EV_SNAPSHOT_INSTALLED: &str = "snapshot_installed";

/// Tracing event: the driver materialized one or more opaque snapshot offers as
/// outbound protocol messages. Carries `node` and `snapshot_offers`. Emitted
/// before the after-sync-before-send seam, so the driver-hook oracle can prove
/// snapshot transfers use the common outbound path.
pub const EV_SNAPSHOT_OFFERED: &str = "snapshot_offered";

/// Tracing event: this node, on winning an election, filled at least one undecided
/// hole in the recovered suffix with a [`Control::Noop`]. Carries `node`, `round`
/// (the ballot round it now leads at) and `gaps` (how many slots it filled). The
/// gap-fill oracle reads it as the reachability gate proving the fill path is
/// genuinely exercised, not merely present.
pub const EV_GAP_FILLED: &str = "election_gap_filled";

/// Tracing event: this node holds a **chosen gap** — a slot it knows is chosen
/// sitting above its contiguous applied prefix (see [`RawNode::chosen_gap`]).
/// Carries `node`, `hole` (the first slot missing from the prefix) and `above`
/// (the highest chosen slot past it). Emitted once per tick while the gap exists,
/// so its *persistence* is what the trace records, not a single instant. A gap is
/// an ordinary transient (pipelining, a missed `Commit`); one that outlives
/// quiescence is a wedged cluster, which is what the gap-fill oracle asserts
/// against. Purely observational.
pub const EV_CHOSEN_GAP: &str = "chosen_gap";

/// Tracing event: this node received a `Prepare` whose `from_slot` is below its
/// own compaction floor. Carries `node`, `from_slot`, and `floor`. Purely
/// observational: it marks that the dangerous "campaign against a truncated
/// acceptor" interleaving was reached, so the sweep can assert it stays reachable
/// once the acceptor floor guard is in place.
pub const EV_PREPARE_BELOW_FLOOR: &str = "prepare_below_floor";

/// Tracing event: this node's apply seam executed a chosen slot as a no-op
/// because its `(client, seq)` identity had already applied at a lower slot —
/// the #94 double-apply, suppressed. Carries `node` and `count` (suppressions in
/// the batch). Rare and mechanism-specific: it is the only outside evidence the
/// at-most-once suppression path ran.
pub const EV_DUPLICATE_SUPPRESSED: &str = "duplicate_suppressed";

/// Tracing event: this node, as Leader, spent a full election-timeout window
/// without hearing an ack quorum and demoted itself (`CheckQuorum`, #95). Carries
/// `node` and `count` (step-downs in the batch — in practice 1). The zombie
/// leader this bounds is the feeder of #94's stale-suffix interleaving.
pub const EV_QUORUM_LOST: &str = "leader_quorum_lost";

/// Tracing event: a client proposal was answered by the **dedup fast path** —
/// the `(client, seq)` was already applied here, so the reply fired immediately
/// instead of being parked on a slot ([`ProposeResult::Chosen`]). Carries `node`
/// and the `slot` the ack names. Purely observational: this is the one committed
/// ack that does not come out of the apply loop, so the sweep needs evidence it
/// is genuinely reached (and the ack oracle needs it named, not hidden).
pub const EV_PROPOSE_DEDUP_ACK: &str = "propose_dedup_ack";

/// A stable `u64` digest of a value's bytes (FNV-1a), emitted on observability
/// events so the safety oracle can compare chosen values by equality without
/// carrying the raw payload through the trace.
fn value_hash(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in bytes {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

/// The value hash for a decided [`Command`], for observability. A client entry
/// hashes its opaque value bytes; a control command hashes a stable, distinct
/// encoding of its metadata, so every node agrees on the per-slot hash the safety
/// oracle compares (a control command decided for a slot is the same on all
/// nodes).
///
/// Public so an [`Audit`] implementation can hash a `Command` it observes on the
/// wire ([`Audit::sent`]) with the *same* function the driver uses for the
/// durable-write and apply callbacks.
#[must_use]
pub fn command_hash(command: &Command) -> u64 {
    match command {
        Command::User(entry) => value_hash(&entry.value.0),
        Command::Control(Control::Truncate { up_to }) => {
            let mut bytes = vec![0xff_u8];
            bytes.extend_from_slice(&up_to.0.to_le_bytes());
            value_hash(&bytes)
        }
        // A distinct one-byte tag: no `Truncate` encoding can collide with it (they
        // are nine bytes and start `0xff`), and every node hashes the same no-op to
        // the same digest, so per-slot prefix agreement stays checkable.
        Command::Control(Control::Noop) => value_hash(&[0xfe_u8]),
        // Nine bytes starting 0xfd: disjoint from both encodings above.
        Command::Control(Control::Snap { at_index }) => {
            let mut bytes = vec![0xfd_u8];
            bytes.extend_from_slice(&at_index.0.to_le_bytes());
            value_hash(&bytes)
        }
    }
}

/// A stable `u64` digest of a matchmaking history page (FNV-1a over each
/// registration's ballot, kind and membership), so the audit can name *which*
/// answer a candidate folded without the reply's bytes travelling through the
/// port. Order-sensitive, which is what the page contract wants: two pages
/// with the same registrations in a different order are different answers.
pub fn registration_history_hash<'a, I>(history: I) -> u64
where
    I: IntoIterator<Item = (&'a Ballot, &'a Registration)>,
{
    let mut bytes: Vec<u8> = Vec::new();
    for (ballot, registration) in history {
        bytes.extend_from_slice(&ballot.round.to_le_bytes());
        bytes.extend_from_slice(&ballot.node.0.to_le_bytes());
        bytes.push(u8::from(registration.kind.is_reconfiguration()));
        for member in registration.config.members() {
            bytes.extend_from_slice(&member.0.to_le_bytes());
        }
        // A separator, so two adjacent memberships cannot be re-cut into
        // the same byte string.
        bytes.push(0xff);
    }
    value_hash(&bytes)
}

/// A short, stable label for a [`Message`] variant, for observability: the `kind`
/// field on the `msg_sent` / `msg_received` events.
pub(crate) fn message_kind(m: &Message) -> &'static str {
    match m {
        Message::Prepare { .. } => "prepare",
        Message::Promise { .. } => "promise",
        Message::Accept { .. } => "accept",
        Message::Accepted { .. } => "accepted",
        Message::Nack { .. } => "nack",
        Message::Commit { .. } => "commit",
        Message::CatchUpRequest { .. } => "catchup_request",
        Message::CatchUpResponse { .. } => "catchup_response",
        Message::InstallSnapshot { .. } => "install_snapshot",
        Message::CheckLeader { .. } => "check_leader",
        Message::Heartbeat { .. } => "heartbeat",
        Message::HeartbeatAck { .. } => "heartbeat_ack",
        Message::SnapAck { .. } => "snap_ack",
        Message::SnapChunkRequest { .. } => "snap_chunk_request",
        Message::SnapChunkResponse { .. } => "snap_chunk_response",
        Message::Relinquish { .. } => "relinquish",
        _ => "unknown",
    }
}

/// The `(sender, ballot, slot)` triple a ballot-carrying Paxos message routes on,
/// for observability. Every ballot-carrying kind returns `Some`, `Heartbeat`
/// included — its "slot" is the commit watermark it advertises, which is
/// `None` on a leader that has chosen nothing (an empty prefix is not slot 0;
/// see [`paros_core::Message::Heartbeat`]). The kinds with no ballot at all
/// (`CheckLeader`, the catch-up pair) return `None` outright.
pub(crate) fn message_route(m: &Message) -> Option<(NodeId, ConfigId, Ballot, Option<Slot>)> {
    match m {
        // Phase 1 is per-ballot: report `from_slot` as the slot for the timeline.
        Message::Prepare {
            config_id,
            from,
            ballot,
            from_slot,
            ..
        }
        | Message::Promise {
            config_id,
            from,
            ballot,
            from_slot,
            ..
        } => Some((*from, *config_id, *ballot, Some(*from_slot))),
        Message::Accept {
            config_id,
            from,
            ballot,
            slot,
            ..
        }
        | Message::Accepted {
            config_id,
            from,
            ballot,
            slot,
            ..
        }
        | Message::Nack {
            config_id,
            from,
            ballot,
            slot,
            ..
        }
        | Message::Commit {
            config_id,
            from,
            ballot,
            slot,
            ..
        } => Some((*from, *config_id, *ballot, Some(*slot))),
        Message::Heartbeat {
            config_id,
            from,
            ballot,
            commit,
            ..
        } => Some((*from, *config_id, *ballot, *commit)),
        Message::InstallSnapshot {
            config_id,
            from,
            ballot,
            chosen_index,
            ..
        } => Some((*from, *config_id, *ballot, Some(*chosen_index))),
        // A handoff's "slot" is the allocator frontier it transfers — the
        // field that carries its meaning on a timeline.
        Message::Relinquish {
            config_id,
            from,
            ballot,
            next_slot,
            ..
        } => Some((*from, *config_id, *ballot, Some(*next_slot))),
        _ => None,
    }
}

/// A short, stable label for an encoded [`internal::ConsensusMessage`], for
/// the mailbox-drop audit report (mirrors [`message_kind`], which needs the
/// decoded domain [`Message`] the delivery task no longer has).
pub(crate) fn proto_message_kind(m: &internal::ConsensusMessage) -> &'static str {
    use internal::consensus_message::Kind;
    match &m.kind {
        Some(Kind::Prepare(_)) => "prepare",
        Some(Kind::Promise(_)) => "promise",
        Some(Kind::Accept(_)) => "accept",
        Some(Kind::Accepted(_)) => "accepted",
        Some(Kind::Nack(_)) => "nack",
        Some(Kind::Commit(_)) => "commit",
        Some(Kind::CatchUpRequest(_)) => "catchup_request",
        Some(Kind::CatchUpResponse(_)) => "catchup_response",
        Some(Kind::InstallSnapshot(_)) => "install_snapshot",
        Some(Kind::CheckLeader(_)) => "check_leader",
        Some(Kind::Heartbeat(_)) => "heartbeat",
        Some(Kind::HeartbeatAck(_)) => "heartbeat_ack",
        Some(Kind::SnapAck(_)) => "snap_ack",
        Some(Kind::Relinquish(_)) => "relinquish",
        Some(Kind::SnapChunkRequest(_)) => "snap_chunk_request",
        Some(Kind::SnapChunkResponse(_)) => "snap_chunk_response",
        None => "unknown",
    }
}
