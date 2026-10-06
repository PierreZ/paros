//! The protocol message enum. Pure in-memory data — the core never serializes
//! it. The driver decodes inbound bytes into a [`Message`] before
//! [`crate::ColocatedNode::step`], and encodes [`crate::Ready::messages`] after
//! draining a batch.

use std::collections::BTreeMap;
use std::fmt;

use crate::journal_state::JournalState;
use crate::membership::{AcceptorConfig, ProxyId};
use crate::types::{Ballot, Command, NodeId, Slot};

/// A **party** to the Phase-2 exchange: the address an `Accept` asks its
/// `Accepted` sent to, and the sender a `Commit` names. A node, or a proxy
/// leader (#142) — two identity namespaces, because a proxy is not an
/// acceptor and never has a [`NodeId`]. Every other message keeps naming
/// nodes: Phase 1 is never proxied, a learner is always a node, and a
/// proxy holds nothing a catch-up could serve.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Party {
    /// A node of the pool.
    Node(NodeId),
    /// A proxy leader of the deployment.
    Proxy(ProxyId),
}

impl Party {
    /// The audience a reply to this party is addressed to.
    #[must_use]
    pub fn audience(self) -> Audience {
        match self {
            Party::Node(node) => Audience::Node(node),
            Party::Proxy(proxy) => Audience::Proxy(proxy),
        }
    }

    /// The node this party is, if it is one.
    #[must_use]
    pub fn node(self) -> Option<NodeId> {
        match self {
            Party::Node(node) => Some(node),
            Party::Proxy(_) => None,
        }
    }
}

impl fmt::Display for Party {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Party::Node(node) => write!(f, "node:{}", node.0),
            Party::Proxy(proxy) => write!(f, "proxy:{}", proxy.0),
        }
    }
}

/// **Who a message is addressed to**, in the protocol's own terms rather
/// than in addresses.
///
/// The core decides *audiences*; the driver holds the deployment map that
/// turns one into a list of nodes ([`Audience::resolve`]). That split is what
/// keeps the batch small — a heartbeat to a six-node pool is one entry, not
/// six clones of the same bytes — and it is what a compartmentalized
/// deployment needs, where "the acceptors of this configuration" is a column
/// of a grid rather than a membership the sender enumerates, and "the proxy
/// this slot is delegated to" is an identity outside the pool altogether.
///
/// There is no "the proposer of ballot `b`" audience: since `Prepare` and
/// `Accept` carry an explicit `reply_to`, a reply is addressed to the party
/// the request named ([`Audience::Node`], or [`Audience::Proxy`] for a
/// delegated round), which is exactly what lets a proxied request be
/// answered without the acceptor knowing who the leader is.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Audience {
    /// One node, by id: every reply, every targeted request, the single
    /// successor of a handoff.
    Node(NodeId),
    /// One proxy leader, by id (#142): the delegated `Accept` a leader hands
    /// it, and the `Accepted` or `Nack` an acceptor answers it with. The
    /// variant exists because a proxy has its own identity namespace, so
    /// [`Audience::Node`] cannot name it; the driver's deployment map
    /// resolves the id to a process exactly as it resolves
    /// [`Audience::Learners`] from the pool.
    Proxy(ProxyId),
    /// The Phase-2 addressees of `config` in `column`
    /// ([`AcceptorConfig::phase2_addressees`]) — an `Accept`'s fan-out. A
    /// removed node is never contacted for a new ballot's accepts. The
    /// column is the one the core resolved for the round
    /// ([`AcceptorConfig::column_of`], `None` under a majority or a flexible
    /// split), carried here so the driver's deployment map never re-derives
    /// it: a grid addresses that one column and nothing else.
    AcceptorsOf {
        /// The configuration the round runs under.
        config: AcceptorConfig,
        /// The column the round was opened against.
        column: Option<usize>,
    },
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
    ///
    /// A proxy audience names **no node**: a proxy lives in its own
    /// namespace, and the driver's deployment map resolves it through
    /// [`Audience::proxy`] instead.
    #[must_use]
    pub fn resolve(&self, pool: &[NodeId], me: NodeId) -> Vec<NodeId> {
        self.resolve_excluding(pool, Some(me))
    }

    /// [`Audience::resolve`] as a **proxy leader** sends: a proxy is
    /// nobody's peer, so nothing is filtered out — an `Accept` it fans out
    /// reaches every addressee of the column, the leader included when it
    /// sits in it, and a `Commit` reaches the whole pool.
    #[must_use]
    pub fn resolve_from_proxy(&self, pool: &[NodeId]) -> Vec<NodeId> {
        self.resolve_excluding(pool, None)
    }

    fn resolve_excluding(&self, pool: &[NodeId], me: Option<NodeId>) -> Vec<NodeId> {
        let resolved = self.resolve_unfiltered(pool, me);
        // A proxy audience names no node, and a broadcast never loops back.
        if self.proxy().is_some() {
            assert!(resolved.is_empty(), "a proxy audience resolves to no node");
        }
        if let (Some(me), false) = (me, matches!(self, Audience::Node(_))) {
            assert!(
                !resolved.contains(&me),
                "a fan-out never addresses its sender"
            );
        }
        resolved
    }

    /// [`Audience::resolve_excluding`]'s answer, before its postconditions.
    fn resolve_unfiltered(&self, pool: &[NodeId], me: Option<NodeId>) -> Vec<NodeId> {
        match self {
            Audience::Node(to) => vec![*to],
            Audience::Proxy(_) => Vec::new(),
            Audience::AcceptorsOf { config, column } => config
                .phase2_addressees(*column)
                .into_iter()
                .filter(|p| Some(*p) != me)
                .collect(),
            Audience::Learners => pool.iter().copied().filter(|p| Some(*p) != me).collect(),
        }
    }

    /// The proxy this audience names, if it is a proxy audience.
    #[must_use]
    pub fn proxy(&self) -> Option<ProxyId> {
        match self {
            Audience::Proxy(proxy) => Some(*proxy),
            Audience::Node(_) | Audience::AcceptorsOf { .. } | Audience::Learners => None,
        }
    }
}

/// Every protocol stimulus the core understands. Peer RPCs and tick-injected
/// self-events all enter through the single [`crate::ColocatedNode::step`] router.
///
/// `#[non_exhaustive]` so later stages can add variants (e.g. a trim point,
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
    /// Proposer → acceptors, or leader → proxy leader: "accept `command` for
    /// `slot` at `ballot`."
    ///
    /// The same message plays two parts (#142). Addressed to the acceptors
    /// of a column it is the Phase-2 request itself; addressed to a proxy
    /// ([`Audience::Proxy`], `reply_to` naming that proxy) it is the
    /// **delegation** — the proxy fans exactly this message out to the
    /// column, folds the `Accepted`s the acceptors send to `reply_to`, and
    /// emits the `Commit`. A duplicate at a proxy's open round re-fans-out
    /// (P2b-idempotent); the leader re-delegates on every re-send, and a
    /// handoff successor re-delegates every inherited round with `leader`
    /// naming itself.
    Accept {
        /// Where the `Accepted` (or `Nack`) is addressed. The **reply
        /// party** alone: the leader on a colocated round, the proxy on a
        /// delegated one.
        reply_to: Party,
        /// The node exercising `ballot`'s Phase-2 authority — the **leader
        /// hint** an acceptor adopts and a client is redirected to. It is
        /// deliberately not [`Ballot::node`](crate::Ballot::node): after a
        /// cooperative handoff the ballot keeps naming the node that won it
        /// while a different node drives Phase 2 (so `leader != ballot.node`
        /// already happens). It is also deliberately not `reply_to`: on a
        /// delegated round the reply party is the proxy collecting the
        /// `Accepted`s while this field still names the leader an acceptor
        /// adopts — proxy leaders are the reason the two are separate
        /// fields. On a deployment without proxies `reply_to` is
        /// `Party::Node(leader)` on every `Accept`.
        leader: NodeId,
        /// The ballot under which the command is proposed.
        ballot: Ballot,
        /// The target slot.
        slot: Slot,
        /// The proposed command (an opaque client entry or a control command).
        command: Command,
        /// The acceptor configuration `ballot` was registered with (`C_b`),
        /// on the **delegation** a matchmaker deployment's leader hands a
        /// proxy: the proxy fans out to `C_b`'s addressees and judges the
        /// decision over it, and it has no other way to learn a
        /// configuration (it takes part in no Phase 1 and hears no beat).
        /// **`None` on plain Multi-Paxos** and on every acceptor-bound
        /// `Accept`, whose wire is unchanged; an acceptor ignores it.
        #[cfg_attr(feature = "serde", serde(default))]
        config: Option<AcceptorConfig>,
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
    /// Any → any: `command` is chosen for `slot` (decided at `ballot`). The
    /// leader's decision on its own tally, or a proxy leader's on the
    /// delegated round it folded (#142) — a learner treats the two alike,
    /// and a leader that delegated the round closes it on this message.
    Commit {
        /// Sender: the deciding leader, or the proxy that folded the round.
        from: Party,
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

    // ---- Below the trim point (#186) ----
    /// An up-to-date peer → a requester whose needed prefix sits **below the
    /// server's trim point** (it was trimmed, so no
    /// [`CatchUpResponse`](Message::CatchUpResponse) can replay it): "the log
    /// below `point` is gone here." The requester jumps its chosen index to
    /// `point - 1` and its floor to `point`, drops what it held below, seals
    /// the ledger it is handed, and catches up from `point` like any laggard.
    ///
    /// It carries **no bytes and no ballot**, and the requester's promise does
    /// not move (#180's rule): everything below the trim point is chosen, the
    /// application that folded it lives in the client, and a trim point is
    /// replicated by consensus (a decided `Truncate`), so the only facts a
    /// laggard needs are where the retained log starts and the journal
    /// state sealed over what it will never walk.
    TrimmedTo {
        /// Sender (the serving peer).
        from: NodeId,
        /// The serving peer's trim point: its first retained slot. Everything
        /// below it is chosen.
        point: Slot,
        /// The journal state the serving peer's log folded to below `point`
        /// (#204): those slots never reach the requester, so without this its
        /// journal fold would restart from nothing and judge every later write
        /// against the wrong writer and the wrong next position.
        state: JournalState,
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
    /// cadence, never received by its sender.
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
        /// The configuration the leader's ballot runs with, so a follower that
        /// missed the `Prepare` (down or partitioned through the election)
        /// still learns the latest configuration from ordinary beats.
        /// **`None` on plain Multi-Paxos**; a plain node ignores the field.
        #[cfg_attr(feature = "serde", serde(default))]
        config: Option<AcceptorConfig>,
    },

    // ---- Leaderless reads (Paxos Quorum Reads, #143) ----
    /// Any node → a **Phase-1 quorum** of acceptors (a row of a grid; the
    /// whole membership under a majority or a flexible split): "what is the
    /// highest slot you have voted in?" — the first half of a
    /// **quorum read** (Compartmentalized Paxos §3.4). Fire-and-forget, never
    /// re-sent: a read whose row does not answer within its TTL is dropped
    /// silently and the driver's client-facing retry asks again. Carries no
    /// ballot and no configuration: the reader tallies the answers over the
    /// configuration *it* believes in force, and abandons the read if an
    /// answer names a newer one.
    PreRead {
        /// Where the `PreReadAck` is addressed. The reply address alone.
        reply_to: NodeId,
        /// The reader's correlation token, echoed by the ack.
        ctx: u64,
    },
    /// Acceptor → the reader: its **vote watermark**
    /// ([`crate::acceptor::Acceptor::vote_watermark`]) — the highest slot
    /// it has voted in, `None` on a log that never voted. No durable
    /// obligation: the ack claims "I have voted this high", a fact the
    /// durable log already holds, and the reader waits until its replica has
    /// applied the maximum over its quorum before serving.
    PreReadAck {
        /// The answering acceptor.
        from: NodeId,
        /// The read's correlation token, echoed.
        ctx: u64,
        /// The acceptor's vote watermark.
        watermark: Option<Slot>,
        /// The ballot of the acceptor configuration the answering node
        /// believes in force, on a matchmaker deployment — the reader
        /// abandons a read whose row names a configuration newer than the
        /// one it was opened against (its row may not intersect the
        /// successor's columns). `None` on plain Multi-Paxos, whose
        /// configuration never moves.
        #[cfg_attr(feature = "serde", serde(default))]
        config_since: Option<Ballot>,
    },

    /// Follower → leader: acknowledges a [`Message::Heartbeat`] whose ballot the
    /// follower accepts (its promise is at or below it), echoing its ballot.
    /// The acks at the leader's current ballot refill its `CheckQuorum` window
    /// and, on a matchmaker deployment, feed its GC fence. Carries no durable
    /// obligation: the ack claims only "my promise is at or below `ballot`
    /// right now".
    HeartbeatAck {
        /// The acknowledging follower.
        from: NodeId,
        /// The heartbeat's ballot, echoed.
        ballot: Ballot,
        /// The follower's contiguous chosen index, on a matchmaker
        /// deployment: what the leader's garbage collection (#123) counts
        /// toward "a Phase-2 quorum holds the prefix below my fence". Absent
        /// on plain Multi-Paxos, whose acks are byte-for-byte today's.
        #[cfg_attr(feature = "serde", serde(default))]
        chosen: Option<Slot>,
    },
}
