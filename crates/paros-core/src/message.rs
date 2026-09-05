//! The protocol message enum. Pure in-memory data — the core never serializes
//! it. The driver decodes inbound bytes into a [`Message`] before
//! [`crate::ColocatedNode::step`], and encodes [`crate::Ready::messages`] after
//! draining a batch.

use std::collections::BTreeMap;

use crate::membership::AcceptorConfig;
use crate::types::{Ballot, Command, NodeId, SessionEntry, Slot, Value};

/// **Who a message is addressed to**, in the protocol's own terms rather
/// than in addresses.
///
/// The core decides *audiences*; the driver holds the deployment map that
/// turns one into a list of nodes ([`Audience::resolve`]). That split is what
/// keeps the batch small — a heartbeat to a six-node pool is one entry, not
/// six clones of the same bytes — and it is what a compartmentalized
/// deployment needs, where "the acceptors of this configuration" is a column
/// of a grid rather than a membership the sender enumerates.
///
/// There is no "the proposer of ballot `b`" audience: since `Prepare` and
/// `Accept` carry an explicit `reply_to`, a reply is addressed to the address
/// the request named ([`Audience::Node`]), which is exactly what lets a
/// proxied request be answered without the acceptor knowing who the leader
/// is.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Audience {
    /// One node, by id: every reply, every targeted request, the single
    /// successor of a handoff.
    Node(NodeId),
    /// The Phase-2 addressees of `config`
    /// ([`AcceptorConfig::phase2_addressees`]) — an `Accept`'s fan-out. A
    /// removed node is never contacted for a new ballot's accepts, and a
    /// grid or compartmentalized deployment addresses one column here.
    AcceptorsOf(AcceptorConfig),
    /// Every node of the pool — the learner fan-out (commits, beats,
    /// catch-up), which reaches spares and removed members too so every
    /// replica keeps the chosen log.
    Learners,
}

impl Audience {
    /// The nodes this audience names, given the deployment's `pool` and the
    /// sender's own id (which is never addressed: a node does not send to
    /// itself). In pool / membership order, so a batch's sends keep the order
    /// the core queued them in.
    #[must_use]
    pub fn resolve(&self, pool: &[NodeId], me: NodeId) -> Vec<NodeId> {
        match self {
            Audience::Node(to) => vec![*to],
            Audience::AcceptorsOf(config) => config
                .phase2_addressees()
                .iter()
                .copied()
                .filter(|p| *p != me)
                .collect(),
            Audience::Learners => pool.iter().copied().filter(|p| *p != me).collect(),
        }
    }
}

/// Every protocol stimulus the core understands. Peer RPCs and tick-injected
/// self-events all enter through the single [`crate::ColocatedNode::step`] router.
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
        /// Where the `Promise` (or `Nack`) is addressed. The **reply address**
        /// alone: it says nothing about who owns the ballot.
        reply_to: NodeId,
        /// The ballot being prepared. Its [`Ballot::node`](crate::Ballot::node)
        /// is the candidate running this campaign, and there is deliberately
        /// no separate `leader` field beside it: Phase 1 is always run by the
        /// ballot's owner — Compartmentalized Paxos's proxy leaders take over
        /// Phase 2 only (§3.1 of the paper) — so the owner is fully determined
        /// by the ballot and an acceptor checks ownership against it.
        ballot: Ballot,
        /// First slot this prepare covers (the candidate's `chosen_index + 1`).
        from_slot: Slot,
        /// The acceptor configuration the candidate registered for `ballot`
        /// (`C_b`), so every acceptor it reaches — the members of every prior
        /// configuration and of `C_b` itself — learns the latest configuration
        /// and can register it on its own next campaign. **`None` on plain
        /// Multi-Paxos**, whose `Prepare` is exactly today's; a plain node
        /// ignores the field.
        #[cfg_attr(feature = "serde", serde(default))]
        config: Option<AcceptorConfig>,
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
        /// Sender.
        from: NodeId,
        /// The ballot promised.
        ballot: Ballot,
        /// First slot this promise covers (echoes the prepare's `from_slot`).
        from_slot: Slot,
        /// All accepted commands for slots `>= from_slot`. Empty if none.
        accepted: BTreeMap<Slot, (Ballot, Command)>,
        /// The **tri-state's third answer** (Stage 8, CTRL): slots in this page's
        /// range whose accepted value this acceptor *lost* to storage corruption
        /// but whose identity `(slot, accepted_ballot)` survived. `faulty` means
        /// silence toward the none-tally, never denial: the candidate must not
        /// treat these slots as "nothing accepted here" (a unanimous-`none`
        /// no-op fill over a misreported faulty copy is the CTRL Figure-2 bug
        /// class), and must not count this acceptor toward the full-Q1-of-`none`
        /// threshold at these slots. Disjoint from `accepted` by construction.
        #[cfg_attr(feature = "serde", serde(default))]
        faulty: BTreeMap<Slot, Ballot>,
        /// Cursor for the next bounded suffix page. `None` marks the terminal
        /// page; only then may the candidate count this acceptor in its Phase-1
        /// quorum.
        next_from_slot: Option<Slot>,
    },

    // ---- Phase 2 (accept / accepted / nack) ----
    /// Proposer → acceptors: "accept `command` for `slot` at `ballot`."
    Accept {
        /// Where the `Accepted` (or `Nack`) is addressed. The **reply address**
        /// alone.
        reply_to: NodeId,
        /// The node exercising `ballot`'s Phase-2 authority — the **leader
        /// hint** an acceptor adopts and a client is redirected to. It is
        /// deliberately not [`Ballot::node`](crate::Ballot::node): after a
        /// cooperative handoff the ballot keeps naming the node that won it
        /// while a different node drives Phase 2 (so `leader != ballot.node`
        /// already happens). It is also deliberately not `reply_to`: a
        /// compartmentalized deployment's proxy leaders run Phase 2 on the
        /// leader's behalf and collect the `Accepted`s themselves, so the
        /// reply address names the proxy while this field still names the
        /// leader an acceptor adopts — proxy leaders are the reason the two
        /// are separate fields. Today they are equal on every deployment.
        leader: NodeId,
        /// The ballot under which the command is proposed.
        ballot: Ballot,
        /// The target slot.
        slot: Slot,
        /// The proposed command (an opaque client entry or a control command).
        command: Command,
    },
    /// Acceptor → proposer: durably accepted the proposal for `slot` at `ballot`.
    Accepted {
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
        /// Sender.
        from: NodeId,
        /// The rejected ballot, echoed from the `Prepare`/`Accept` that was
        /// refused (matches the proposer's in-flight campaign or accept round).
        /// The winning promise deliberately does not travel with it: an
        /// untrusted wire value must never select a future campaign round.
        ballot: Ballot,
        /// The contested slot.
        slot: Slot,
    },

    // ---- Learning ----
    /// Any → any: `command` is chosen for `slot` (decided at `ballot`).
    Commit {
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
    /// server's compaction floor** (it was truncated, so no [`CatchUpResponse`](Message::CatchUpResponse)
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

    // ---- Snapshot-point repair (driver-terminal; CTRL §3.5 chunk repair) ----
    /// Follower → leader: "I have durably recorded the decided snapshot at
    /// `at_index`." The leader tallies these for the `Truncate`-coupling rule
    /// (truncation is proposed only once a quorum has snapshotted at the
    /// index). **Driver-terminal**: the driver's snapshot-repair layer owns
    /// it end to end; [`crate::ColocatedNode::step`] ignores it — consensus state
    /// never depends on snapshot custody.
    SnapAck {
        /// The acknowledging node.
        from: NodeId,
        /// The decided snapshot point recorded (the `Snap` marker's slot).
        at_index: Slot,
    },
    /// A node with rotted chunks of its decided snapshot → its peers: "send me
    /// chunks `chunks` of the snapshot at `at_index`." Byte-wise snapshot
    /// identity (the `Snap` marker) is what makes the answer verifiable.
    /// Driver-terminal, like [`Message::SnapAck`].
    SnapChunkRequest {
        /// The requesting node.
        from: NodeId,
        /// The decided snapshot point whose chunks are needed.
        at_index: Slot,
        /// The chunk indexes needed (the driver's fixed chunk size).
        chunks: Vec<u32>,
    },
    /// A peer holding the identical decided snapshot → the requester: the
    /// requested chunks' bytes. A peer *lacking* the snapshot stays silent
    /// (absence answers nothing — CTRL Figure 6 Box B); a peer holding only a
    /// more advanced snapshot answers with a whole-blob
    /// [`Message::InstallSnapshot`] instead (the unchanged fallback).
    /// Driver-terminal, like [`Message::SnapAck`].
    SnapChunkResponse {
        /// The serving peer.
        from: NodeId,
        /// The decided snapshot point the chunks belong to.
        at_index: Slot,
        /// `(chunk index, chunk bytes)` for each chunk this peer holds clean.
        chunks: Vec<(u32, Value)>,
    },

    // ---- Cooperative leader handoff (DPaxos "Leader Handoff") ----
    /// Outgoing leader → **one** successor: "I permanently give up the
    /// Phase-2 authority of `ballot` for every slot at or after `next_slot`,
    /// together with the unfinished business below it; you may continue
    /// Phase 2 under `ballot` without running another Phase 1."
    ///
    /// This is the cooperative counterpart of an election. An election
    /// *destroys* the sitting leader's authority and makes the successor
    /// rediscover the log through Phase 1; a handoff *moves* the existing
    /// logical authority to another physical node, which is why the ballot
    /// carried here keeps naming the **relinquishing** node
    /// ([`Ballot::node`](crate::Ballot::node) is the ballot's owner, not the
    /// sender of a given message).
    ///
    /// # The safety rule
    ///
    /// An authority is relinquished **at most once** and never exercised
    /// again by the node that gave it up. In paros that rule needs no durable
    /// fence: leadership is entirely volatile state
    /// ([`ColocatedNode::new`](crate::ColocatedNode::new) always boots a Follower, and
    /// `on_check_leader` only ever campaigns at a strictly higher round), so a
    /// crash is itself an abdication — and
    /// [`ColocatedNode::relinquish_to`](crate::ColocatedNode::relinquish_to) abdicates
    /// *synchronously, in the same call that queues this message*, before it
    /// can possibly reach the transport. See that method's `# Safety` section
    /// for the full argument.
    ///
    /// # Failure is an availability problem, never a safety one
    ///
    /// This message is fire-and-forget: no ack, no retry, no two-phase
    /// commit. If it is lost, the old leader has already stopped and the new
    /// one never started, so the cluster simply has no leader until an
    /// ordinary Phase 1 elects one. That is the intended trade.
    Relinquish {
        /// The node giving the authority up. Always `ballot.node`: only the
        /// node that minted a ballot may hand it on (one hop —
        /// `ColocatedNode::can_relinquish` requires `LeadershipOrigin::Elected`),
        /// so a successor never relinquishes what it inherited and every
        /// `Relinquish` on the wire is sent by its ballot's minter.
        from: NodeId,
        /// The **single intended successor**. A receiver whose own id differs
        /// ignores the message whole: authority uniqueness must not depend on
        /// the transport delivering to exactly one address, so the intended
        /// target travels *inside* the payload where a duplicate, a misroute,
        /// or a replay cannot change it.
        to: NodeId,
        /// The logical Phase-2 authority being transferred.
        ballot: Ballot,
        /// First slot the transferred tail describes: the relinquishing
        /// leader's own first unchosen slot.
        from_slot: Slot,
        /// The **allocator frontier**: the successor must allocate fresh
        /// proposals at or above this slot, exactly as the relinquishing
        /// leader would have. This is the field that makes authority
        /// uniqueness structural — two nodes can only ever propose different
        /// commands at one `(slot, ballot)` if the successor rewinds the
        /// allocator, and it never does.
        next_slot: Slot,
        /// Slots in `[from_slot, next_slot)` the relinquishing leader knows
        /// are **chosen**, with the ballot each was decided under. Exactly the
        /// claim a [`Message::Commit`] or a
        /// [`Message::CatchUpResponse`] makes, batched.
        decided: BTreeMap<Slot, (Ballot, Command)>,
        /// Slots in `[from_slot, next_slot)` with an **open Phase-2 round at
        /// `ballot`**: the accepted-but-unchosen work the successor inherits
        /// and re-proposes verbatim under the same ballot (re-proposing an
        /// identical command at an identical `(slot, ballot)` is a no-op for
        /// P2b, and it is what keeps the contiguous chosen prefix from
        /// freezing at the first inherited hole).
        ///
        /// Every command here runs at `ballot` by construction — a leader's
        /// in-flight rounds all run at its own ballot — so no per-slot ballot
        /// is carried. Together with `decided` this **exactly tiles**
        /// `[from_slot, next_slot)`; a payload that does not is rejected
        /// whole, and the cluster falls back to an ordinary election.
        pending: BTreeMap<Slot, Command>,
        /// The acceptor configuration `ballot` was registered with — the
        /// authority's Phase-2 membership, transferred verbatim so the
        /// successor counts its quorums over exactly the registered
        /// configuration. **`None` on plain Multi-Paxos**; a matchmaker
        /// deployment refuses a transfer that carries none.
        #[cfg_attr(feature = "serde", serde(default))]
        config: Option<AcceptorConfig>,
    },

    // ---- Liveness ----
    /// Leader → peers: a liveness beat carrying the leader's commit index so
    /// followers advance their chosen prefix. Broadcast on the leader's tick
    /// cadence (and by a read round), never received by its sender.
    Heartbeat {
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
        /// The configuration the leader's ballot runs with, so a follower that
        /// missed the `Prepare` (down or partitioned through the election)
        /// still learns the latest configuration from ordinary beats.
        /// **`None` on plain Multi-Paxos**; a plain node ignores the field.
        #[cfg_attr(feature = "serde", serde(default))]
        config: Option<AcceptorConfig>,
    },

    /// Follower → leader: acknowledges a [`Message::Heartbeat`] whose ballot the
    /// follower accepts (its promise is at or below it), echoing `(ballot, seq)`.
    /// A quorum of acks at the leader's current ballot, for beats broadcast at or
    /// after a read-index round began, proves the node was still leader after the
    /// read was captured — the no-log-write leadership confirmation linearizable
    /// reads need. Carries no durable obligation: the ack claims only "my promise
    /// is at or below `ballot` right now".
    HeartbeatAck {
        /// The acknowledging follower.
        from: NodeId,
        /// The heartbeat's ballot, echoed.
        ballot: Ballot,
        /// The heartbeat's beat sequence number, echoed.
        seq: u64,
        /// The follower's contiguous chosen index, on a matchmaker
        /// deployment: what the leader's garbage collection (#123) counts
        /// toward "a Phase-2 quorum holds the prefix below my fence". Absent
        /// on plain Multi-Paxos, whose acks are byte-for-byte today's.
        #[cfg_attr(feature = "serde", serde(default))]
        chosen: Option<Slot>,
    },
}
