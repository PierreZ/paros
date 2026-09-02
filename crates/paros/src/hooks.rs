//! Driver fault-injection hooks.
//!
//! `drain_ready` runs synchronously (no `.await`), so process-granularity chaos
//! (moonpool's attrition) can only crash a node *between* batches — never at the
//! persist/send seam within one. [`DriverHooks`] also exposes the driver's
//! optional policy decisions: delaying an `Accept` re-send, resigning
//! leadership, choosing the shortest valid election timeout, the peer mailbox's
//! choices (overtake the queue, evict across kinds, and — armed at enqueue,
//! applied at the drain — hold a batch or reverse it), skipping a snapshot
//! offer, and stretching a tick.
//! Production passes [`NoHooks`], whose defaults never perturb the driver.
//!
//! **Every hook is consulted from the driver's node loop, never from a spawned
//! task.** A hook answer is a randomness draw in simulation, and the node loop
//! is where the simulation steps deterministically; a draw taken inside a
//! detached task can outlive its simulation and shift the next run's stream.
//! `PeerMailbox` in `crate::driver` carries the CI failure that established
//! this.

use paros_core::{Message, NodeId, Slot};

/// A durability seam within one `Ready` batch where a crash can be injected.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Seam {
    /// After the batch's durable writes are staged but **before** the fsync. A
    /// crash here loses the whole un-synced batch — and no message was sent yet
    /// (sends come after the fsync), so it is a clean "the step never happened".
    BeforeSync,
    /// After the batch is fsync-durable but **before** its messages are sent
    /// (this subsumes the after-accept-before-`Accepted`-reply seam). A crash
    /// here keeps the durable writes but drops the batch's outbound messages;
    /// the peers must recover from the restarted node re-deriving them.
    AfterSyncBeforeSend,
    /// After the batch's committed entries are applied to the application state
    /// but **before** the application fsync. A crash here lands a node whose
    /// consensus prefix is durable while its application prefix is behind —
    /// the state the boot replay's idempotent re-apply exists to heal, and the
    /// only durability seam process-level attrition cannot reach.
    AfterApplyBeforeSync,
    /// Inside the chunk-repair pipeline: repaired snapshot chunks from a peer
    /// are written but **before** their fsync. A crash here may lose any
    /// staged, un-synced installs (a store with atomic per-chunk replace keeps
    /// them); either way the reboot's scan re-derives the truth — still-faulty
    /// chunks re-arm the per-tick pull, and a whole-but-unrestored point falls
    /// back to the ordinary below-floor recovery path.
    BeforeChunkSync,
    /// Inside the chunk-repair pipeline: the now-whole snapshot point has been
    /// restored into the application (staged) but **before** the restore's
    /// fsync. A crash here loses the staged restore while keeping the durable,
    /// fully clean chunks; the reboot lands below the floor again and recovers
    /// through the ordinary peer `InstallSnapshot` path instead.
    AfterChunkRestoreBeforeSync,
    /// At boot, after the replay re-applied the committed prefix the previous
    /// incarnation had persisted but not yet applied, and **before** that
    /// application fsync. A crash here repeats the boot replay from the same
    /// durable state: the idempotent re-apply must converge on the second
    /// attempt exactly as on the first, and nothing the first attempt staged
    /// may leak into what the second one reads.
    AfterBootReplayBeforeSync,
}

/// What a cooperative leader handoff would transfer right now, handed to
/// [`DriverHooks::initiate_handoff`] so a simulation can bias the decision
/// toward the states that are actually interesting to explore rather than
/// firing uniformly. Pure read-only context: the driver computes it from the
/// core's public accessors and nothing here changes with the answer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct HandoffContext {
    /// Slots between this leader's contiguous chosen prefix and its allocator
    /// frontier — the unfinished business a transfer must carry across
    /// (accepted-but-unchosen rounds, plus anything decided above the prefix).
    /// `0` on a fully settled leader.
    pub tail: usize,
    /// The allocator frontier the successor would inherit.
    pub next_slot: Slot,
    /// Whether the transfer is "clean": nothing unfinished below the frontier.
    pub settled: bool,
    /// Whether this leader is itself holding a chosen slot above its applied
    /// prefix — a hole ordinary replication is still healing.
    pub healing: bool,
    /// How many successors are eligible.
    pub candidates: usize,
}

/// A client-facing reply the driver is about to send.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Reply {
    /// The ack-on-commit `ProposeAck` (the slot just became durable + applied).
    Propose,
    /// The dedup fast-path `ProposeAck` (a retry of an already-chosen request).
    ProposeDedup,
    /// A confirmed `ReadAck`.
    Read,
    /// A `ProposeAck` redirect from a non-leader (`committed: false`).
    ProposeRedirect,
    /// A `ReadAck` redirect from a non-leader (`committed: false`).
    ReadRedirect,
    /// A `CompactAck` (accepted or refused).
    Compact,
}

/// Optional driver-level fault and policy hooks.
///
/// Each method corresponds to one independent `BUGGIFY` location in simulation.
/// The default implementation is production behavior: never crash, always
/// re-send pending accepts, retain leadership, and use normal randomized
/// election timeouts.
pub trait DriverHooks {
    /// Whether to simulate a crash at `seam` right now.
    fn crash_at(&self, _seam: Seam) -> bool {
        false
    }

    /// Whether to skip a re-send that has pending `Accept`s to send.
    fn skip_accept_resend(&self) -> bool {
        false
    }

    /// Whether the current leader should voluntarily step down.
    fn resign_leadership(&self) -> bool {
        false
    }

    /// Whether this leader should **cooperatively hand its Phase-2 authority
    /// on** right now (`paros_core::RawNode::relinquish_to`), instead of
    /// keeping it until an election takes it away.
    ///
    /// Consulted only when the core reports the leadership is in a
    /// handoff-eligible state and at least one successor exists, so a `true`
    /// here always has an observable effect. Answering `false` is always safe:
    /// a handoff is an *optimization* — it saves the successor a Phase 1 — and
    /// never a requirement, exactly like
    /// [`DriverHooks::skip_accept_resend`]'s re-send.
    ///
    /// `ctx` describes what the transfer would carry, so a simulation can bias
    /// toward the adversarial shapes (a non-empty accepted-but-unchosen tail, a
    /// leader whose own prefix has not caught up) instead of drawing uniformly.
    fn initiate_handoff(&self, _ctx: HandoffContext) -> bool {
        false
    }

    /// Which of `candidates` should receive the authority, when a handoff is
    /// going ahead. `None` (the default) leaves the choice to the driver's own
    /// randomized pick. A returned id that is not in `candidates` is ignored.
    ///
    /// Every candidate is equally valid — the successor validates the transfer
    /// against its own durable promise and falls back to an ordinary election
    /// if it cannot use it — so this only steers *which* valid state the run
    /// explores.
    fn handoff_target(&self, _candidates: &[NodeId]) -> Option<NodeId> {
        None
    }

    /// Whether the next election timeout should use the shortest valid value.
    fn shortest_election_timeout(&self) -> bool {
        false
    }

    /// Whether the next election timeout should use the **longest** valid
    /// value — the other jitter extreme. Consulted only when
    /// [`DriverHooks::shortest_election_timeout`] did not fire, so the two
    /// extremes stay independent locations and never both apply to one draw.
    fn longest_election_timeout(&self) -> bool {
        false
    }

    /// Whether to skip this beat's snapshot-custody advertisement
    /// (`SnapAck` toward the leader). Always safe: the advertisement is
    /// re-sent every tick, so a skipped beat only delays the leader's
    /// truncation-coupling tally.
    fn skip_snap_advertisement(&self) -> bool {
        false
    }

    /// Whether to skip this beat's chunk-repair pull (`SnapChunkRequest`
    /// toward the peers). Always safe: the pull is re-issued every tick while
    /// rotted chunks remain, so a skipped beat only delays the repair.
    fn skip_chunk_pull(&self) -> bool {
        false
    }

    /// Whether to drop this one outbound protocol message after it is durable
    /// but before it reaches the transport. Always safe: the network could lose
    /// the same message, and every protocol path already tolerates that loss
    /// (`resend_pending` re-derives what still matters). Unlike moonpool's
    /// connection-level faults, this reaches *per-message* loss — e.g. one
    /// isolated `Accept` for an earlier slot vanishing while later slots land,
    /// the interleaving behind a stranded chosen-gap wedge.
    fn drop_outgoing(&self, _to: NodeId, _msg: &Message) -> bool {
        false
    }

    /// Whether to send this one outbound protocol message **twice**. Always
    /// safe: retransmission is legal transport behavior on any reconnecting
    /// link, and every quorum in the core is set-based, so a duplicate must be
    /// harmless — this location exists to keep it that way (a quorum counter
    /// "optimized" into an integer would let a duplicated `Accepted` fabricate
    /// a quorum from a sub-quorum). Moonpool has no message-duplication fault.
    fn duplicate_outgoing(&self, _to: NodeId, _msg: &Message) -> bool {
        false
    }

    /// Whether this outbound message should **overtake** everything already
    /// queued in its peer mailbox — enqueued at the front instead of the back.
    /// Always safe: the peer transport never promised ordering (a reconnect,
    /// a retried RPC or the network itself reorders), and every protocol path
    /// is built for it — but the unary batch RPC normally preserves the order
    /// a node enqueued in, so within one peer stream this interleaving is
    /// otherwise unreachable. Consulted only when the mailbox is non-empty
    /// (overtaking an empty queue changes nothing).
    fn overtake_in_mailbox(&self, _to: NodeId, _msg: &Message) -> bool {
        false
    }

    /// Whether a **full** peer mailbox should make room for this message by
    /// evicting its oldest queued message of *any* kind, instead of the
    /// default oldest-of-the-same-kind victim. Always safe: the mailbox is
    /// lossy by contract, so any queued message may be lost, and the
    /// per-kind default is a liveness policy (it stops one class from
    /// crowding another out on a slow link), not a safety one. Consulted only
    /// on overflow, so a `true` always evicts something the default would have
    /// kept — the occasional cross-kind pressure the liveness argument has to
    /// survive.
    fn evict_across_kinds(&self, _to: NodeId, _msg: &Message) -> bool {
        false
    }

    /// Whether the peer-delivery task should **hold** its next drained batch
    /// for one tick before putting it on the wire. Always safe: the transport
    /// is allowed to take arbitrarily long (a reconnect, a congested link, a
    /// stalled h2 window all do exactly this), and the mailbox is lossy by
    /// contract, so a delayed batch is weaker than a dropped one. What it
    /// reaches is the *concurrency window* the other mailbox hooks cannot:
    /// while the drain is parked the mailbox keeps filling, so the backlog
    /// crosses the shed threshold and the batcher's keep-newest path runs
    /// against a genuinely stale head instead of a one-message queue.
    ///
    /// Consulted at **enqueue** time, on a mailbox that already holds
    /// something, and applied by the delivery task — this message's arrival is
    /// what makes the next batch worth holding. The split is a determinism
    /// requirement rather than a convenience: see `PeerMailbox` in
    /// `paros::driver`.
    fn hold_peer_delivery(&self, _to: NodeId) -> bool {
        false
    }

    /// Whether the next delivery batch should be handed to the peer in
    /// **reverse** enqueue order. Always safe, and for the same reason
    /// [`DriverHooks::overtake_in_mailbox`] is: the peer transport never
    /// promised ordering. It is the drain-side half of that location — overtake
    /// reorders one message against a queue, this reorders a whole batch at
    /// once, which is the shape a retried RPC or a re-established stream
    /// produces.
    ///
    /// Consulted at **enqueue** time, once this message makes a reorderable
    /// (two-message) batch possible, and applied by the delivery task only
    /// when the batch it drains really does hold more than one message.
    fn reverse_delivery_batch(&self, _to: NodeId) -> bool {
        false
    }

    /// Whether to skip sending this snapshot offer. Always safe: the offer is
    /// an *answer* to a below-floor peer's `CatchUpRequest`, which that peer
    /// re-issues every tick until it is served, and any other custodian may
    /// serve it instead. This is the driver's own mismatch-skip path (an offer
    /// whose application prefix has fallen behind is dropped, never sent wrong)
    /// taken spuriously, so the recovery path that must tolerate an unserved
    /// beat is exercised without needing an application repair to be open.
    fn skip_snapshot_offer(&self, _to: NodeId) -> bool {
        false
    }

    /// Whether the next tick should wait **twice** the normal interval. Always
    /// safe: the tick interval is a pacing choice, not a protocol bound — every
    /// timeout the core owns is counted in ticks, so a node that ticks at half
    /// speed is exactly a slow node, which the cluster must already tolerate
    /// (moonpool's clock skew produces the same relative drift). Consulted once
    /// per tick; a simulation is expected to stop stretching after its chaos
    /// window so the recovery tail runs at the honest cadence.
    fn stretch_tick_interval(&self) -> bool {
        false
    }

    /// Whether to drop this one client-facing reply after the server state has
    /// advanced. Always safe: the client-facing RPC response can be lost in
    /// production at any time, and the whole ack contract is built for it —
    /// "committed" is re-derivable by a retry through the `(client, seq)`
    /// dedup path. Deterministically produces "committed, applied, and the
    /// client does not know", the precondition of the dedup-window edges.
    fn drop_client_reply(&self, _reply: Reply) -> bool {
        false
    }

    /// Whether to stay silent about one chunk a peer asked for, even though
    /// this node holds it clean. Always safe: the chunk-repair protocol is
    /// built on silence — a peer answers what it holds and says nothing about
    /// what it lacks — and the requester re-asks every tick for whatever is
    /// still missing, from every peer. This reaches the partial-answer
    /// shapes (a point repaired from two custodians, a pull that takes several
    /// beats) without needing the custodians' own rot to line up.
    fn withhold_snap_chunk(&self, _to: NodeId) -> bool {
        false
    }

    /// Whether to answer a parked read with a retry redirect **now**, before
    /// its confirmation deadline. Always safe: the redirect is the same reply
    /// the deadline produces, and a client is built to retry it; a late core
    /// confirmation finds the ctx gone and is ignored, exactly as after the
    /// deadline. Consulted once per tick while reads are parked, so it
    /// reaches the "redirected while the confirmation was in flight" edge the
    /// deadline only reaches under a lost ack.
    fn expire_parked_read_early(&self) -> bool {
        false
    }
}

/// Inert production hooks.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoHooks;

impl DriverHooks for NoHooks {}
