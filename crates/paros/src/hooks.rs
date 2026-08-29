//! Driver fault-injection hooks.
//!
//! `drain_ready` runs synchronously (no `.await`), so process-granularity chaos
//! (moonpool's attrition) can only crash a node *between* batches — never at the
//! persist/send seam within one. [`DriverHooks`] also exposes the driver's
//! optional policy decisions: delaying an `Accept` re-send, resigning
//! leadership, and choosing the shortest valid election timeout. Production
//! passes [`NoHooks`], whose defaults never perturb the driver.

use paros_core::{Message, NodeId};

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

    /// Whether to drop this one client-facing reply after the server state has
    /// advanced. Always safe: the client-facing RPC response can be lost in
    /// production at any time, and the whole ack contract is built for it —
    /// "committed" is re-derivable by a retry through the `(client, seq)`
    /// dedup path. Deterministically produces "committed, applied, and the
    /// client does not know", the precondition of the dedup-window edges.
    fn drop_client_reply(&self, _reply: Reply) -> bool {
        false
    }
}

/// Inert production hooks.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoHooks;

impl DriverHooks for NoHooks {}
