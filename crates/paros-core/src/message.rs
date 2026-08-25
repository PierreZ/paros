//! The protocol message enum. Pure in-memory data — the core never serializes
//! it. The driver decodes inbound bytes into a [`Message`] before
//! [`crate::RawNode::step`], and encodes [`crate::Ready::messages`] after
//! draining a batch.

use std::collections::BTreeMap;

use crate::types::{Ballot, Command, ConfigId, NodeId, SessionEntry, Slot, Value};

/// Every protocol stimulus the core understands. Peer RPCs and tick-injected
/// self-events all enter through the single [`crate::RawNode::step`] router.
///
/// `#[non_exhaustive]` so later stages can add variants (e.g. snapshot transfer,
/// reconfiguration) without a breaking change.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
pub enum Message {
    // ---- Phase 1 (prepare / promise), per ballot, covering a whole log suffix ----
    /// Proposer → acceptors: "promise not to accept anything below `ballot`, for
    /// every slot at or after `from_slot`." One Phase 1 per ballot covers the
    /// whole log suffix (the stable-leader optimization).
    Prepare {
        /// Durable cluster configuration identity.
        #[cfg_attr(feature = "serde", serde(default))]
        config_id: ConfigId,
        /// Sender.
        from: NodeId,
        /// The ballot being prepared.
        ballot: Ballot,
        /// First slot this prepare covers (the candidate's `chosen_index + 1`).
        from_slot: Slot,
    },
    /// Acceptor → proposer: a promise covering every slot at or after `from_slot`,
    /// reporting all previously accepted `(ballot, entry)` in that suffix so the
    /// new leader can re-propose in-flight values (gap fill).
    ///
    /// An acceptor whose compaction floor is above `from_slot` answers a `Nack`
    /// instead: it truncated the accepted entries for `[from_slot, first_slot)`, so
    /// a Promise could not report them, and the candidate would treat those
    /// already-chosen slots as free. A candidate that far behind must recover the
    /// compacted prefix out of band.
    Promise {
        /// Durable cluster configuration identity.
        #[cfg_attr(feature = "serde", serde(default))]
        config_id: ConfigId,
        /// Sender.
        from: NodeId,
        /// The ballot promised.
        ballot: Ballot,
        /// First slot this promise covers (echoes the prepare's `from_slot`).
        from_slot: Slot,
        /// All accepted commands for slots `>= from_slot`. Empty if none.
        accepted: BTreeMap<Slot, (Ballot, Command)>,
        /// Cursor for the next bounded suffix page. `None` marks the terminal
        /// page; only then may the candidate count this acceptor in its Phase-1
        /// quorum.
        next_from_slot: Option<Slot>,
    },

    // ---- Phase 2 (accept / accepted / nack) ----
    /// Proposer → acceptors: "accept `command` for `slot` at `ballot`."
    Accept {
        /// Durable cluster configuration identity.
        #[cfg_attr(feature = "serde", serde(default))]
        config_id: ConfigId,
        /// Sender.
        from: NodeId,
        /// The ballot under which the command is proposed.
        ballot: Ballot,
        /// The target slot.
        slot: Slot,
        /// The proposed command (an opaque client entry or a control command).
        command: Command,
    },
    /// Acceptor → proposer: durably accepted the proposal for `slot` at `ballot`.
    Accepted {
        /// Durable cluster configuration identity.
        #[cfg_attr(feature = "serde", serde(default))]
        config_id: ConfigId,
        /// Sender.
        from: NodeId,
        /// The accepted ballot.
        ballot: Ballot,
        /// The accepted slot.
        slot: Slot,
        /// Fingerprint of the complete command accepted at `(ballot, slot)`.
        vhash: u64,
    },
    /// Acceptor → proposer: rejection of a `Prepare` or `Accept`.
    Nack {
        /// Durable cluster configuration identity.
        #[cfg_attr(feature = "serde", serde(default))]
        config_id: ConfigId,
        /// Sender.
        from: NodeId,
        /// The rejected ballot, echoed from the `Prepare`/`Accept` that was
        /// refused (matches the proposer's in-flight campaign or accept round).
        ballot: Ballot,
        /// The acceptor's current `max_promised_ballot`, for diagnostics. The
        /// receiver deliberately does not retain this untrusted wire value as a
        /// future campaign-round hint.
        promised: Ballot,
        /// The contested slot.
        slot: Slot,
    },

    // ---- Learning ----
    /// Any → any: `command` is chosen for `slot` (decided at `ballot`).
    Commit {
        /// Durable cluster configuration identity.
        #[cfg_attr(feature = "serde", serde(default))]
        config_id: ConfigId,
        /// Sender.
        from: NodeId,
        /// The ballot at which the command was chosen.
        ballot: Ballot,
        /// The chosen slot.
        slot: Slot,
        /// The chosen command (an opaque client entry or a control command).
        command: Command,
    },

    // ---- Catch-up (commit replay) ----
    /// Lagging node → an up-to-date peer: "I am behind; send me every decided slot
    /// at or after `from_slot`." A follower emits this when a `Heartbeat.commit`
    /// (or a `Commit` it received out of order) reveals decided slots beyond its
    /// own contiguous chosen prefix — the hole a missed `Accept`+`Commit` pair
    /// leaves that no re-send would otherwise fill.
    CatchUpRequest {
        /// Sender (where the response is addressed).
        from: NodeId,
        /// First slot the requester still needs (its `chosen_index + 1`).
        from_slot: Slot,
    },
    /// An up-to-date peer → the lagging requester: the decided `(ballot, entry)`
    /// per slot for a bounded range at or after the request's `from_slot`. Every
    /// entry is already **chosen** on the server (quorum-decided, durable), so the
    /// requester may learn it directly — the same safety `Commit` relies on. The
    /// choosing `ballot` is carried so the learner records it authoritatively
    /// (mirroring [`Message::Promise`]'s `accepted`).
    CatchUpResponse {
        /// Sender (the serving peer).
        from: NodeId,
        /// Decided commands by slot, contiguous from the request's `from_slot`.
        entries: BTreeMap<Slot, (Ballot, Command)>,
    },

    // ---- Snapshot transfer (below-floor recovery) ----
    /// An up-to-date peer → a requester whose needed prefix sits **below the
    /// server's compaction floor** (it was truncated, so no [`CatchUpResponse`]
    /// could replay it). Carries an **opaque application snapshot** at
    /// `chosen_index` (the core never interprets `snapshot`; the application
    /// produced it). The requester jumps its chosen prefix to `chosen_index`,
    /// adopts `max(promise, ballot)` (so its durable promise never regresses —
    /// the safety hinge), and truncates to a fully-compacted log above it.
    ///
    /// This is a recovery accelerator, not log bounding: it exists precisely for a
    /// node that was down while the cluster advanced and truncated past it, so
    /// commit-replay catch-up can no longer heal it.
    InstallSnapshot {
        /// Durable cluster configuration identity.
        #[cfg_attr(feature = "serde", serde(default))]
        config_id: ConfigId,
        /// Sender (the serving peer).
        from: NodeId,
        /// The ballot the requester adopts (`>=` every ballot the snapshot's
        /// prefix was chosen under); it takes `max(promise, ballot)`.
        ballot: Ballot,
        /// The chosen index the snapshot brings the requester up to. Everything at
        /// or below it is decided and folded into `snapshot`.
        chosen_index: Slot,
        /// Opaque application snapshot bytes at `chosen_index`. Paros never
        /// interprets them; the application owns their meaning.
        snapshot: Value,
        /// The serving peer's at-most-once session ledger — every
        /// `(client, seq) -> slot` fact in its applied prefix — carried as
        /// **paros-owned metadata beside the opaque bytes** (#94). The folded
        /// prefix's log records never reach the receiver, so without this the
        /// receiver's walk-derived ledger would silently miss them, and its
        /// duplicate-suppression decision at the apply seam would diverge from
        /// every peer's: a mandatory P2c re-proposal of an already-applied
        /// identity would apply for real here and as a no-op elsewhere.
        sessions: Vec<SessionEntry>,
    },

    // ---- Tick-injected self-events (synthesized by `tick`, routed via `step`) ----
    /// "Have I heard from a leader recently?" — drives leader election / a
    /// ballot bump when it fires.
    CheckLeader {
        /// The node checking on itself.
        from: NodeId,
    },
    /// Leader → peers (and a leader self-trigger from `tick`): a liveness beat
    /// carrying the leader's commit index so followers advance their chosen
    /// prefix; also the trigger to re-send un-acked `Accept`s.
    Heartbeat {
        /// Durable cluster configuration identity.
        #[cfg_attr(feature = "serde", serde(default))]
        config_id: ConfigId,
        /// The leader heartbeating.
        from: NodeId,
        /// The leader's current ballot (lets a follower adopt or refuse it).
        ballot: Ballot,
        /// The leader's highest contiguous chosen slot, or `None` when it has
        /// chosen nothing at all. The `Option` is load-bearing: `Slot(0)` is a
        /// real log position, so it cannot double as "no log position". Encoding
        /// the empty prefix as a bare `Slot(0)` made a leader that had chosen its
        /// *first* slot indistinguishable on the wire from a leader with nothing,
        /// and a follower missing exactly that slot read the beat as "no lag" and
        /// never pulled (#56).
        commit: Option<Slot>,
        /// Monotone per-ballot beat sequence number, assigned at broadcast
        /// (`0` on the tick-injected self event, which never leaves the node).
        /// Echoed by [`Message::HeartbeatAck`] so the leader can tell which
        /// beat an ack answers — the freshness a read-index round counts.
        seq: u64,
    },

    /// Follower → leader: acknowledges a [`Message::Heartbeat`] whose ballot the
    /// follower accepts (its promise is at or below it), echoing `(ballot, seq)`.
    /// A quorum of acks at the leader's current ballot, for beats broadcast at or
    /// after a read-index round began, proves the node was still leader after the
    /// read was captured — the no-log-write leadership confirmation linearizable
    /// reads need. Carries no durable obligation: the ack claims only "my promise
    /// is at or below `ballot` right now".
    HeartbeatAck {
        /// Durable cluster configuration identity.
        #[cfg_attr(feature = "serde", serde(default))]
        config_id: ConfigId,
        /// The acknowledging follower.
        from: NodeId,
        /// The heartbeat's ballot, echoed.
        ballot: Ballot,
        /// The heartbeat's beat sequence number, echoed.
        seq: u64,
    },
}

impl Message {
    /// Return the durable configuration identity carried by a ballot-bearing
    /// protocol message. Local triggers and configuration-neutral catch-up
    /// requests/responses carry no identity.
    #[must_use]
    pub fn config_id(&self) -> Option<ConfigId> {
        match self {
            Self::Prepare { config_id, .. }
            | Self::Promise { config_id, .. }
            | Self::Accept { config_id, .. }
            | Self::Accepted { config_id, .. }
            | Self::Nack { config_id, .. }
            | Self::Commit { config_id, .. }
            | Self::InstallSnapshot { config_id, .. }
            | Self::Heartbeat { config_id, .. }
            | Self::HeartbeatAck { config_id, .. } => Some(*config_id),
            Self::CatchUpRequest { .. }
            | Self::CatchUpResponse { .. }
            | Self::CheckLeader { .. } => None,
        }
    }
}
