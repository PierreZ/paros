//! Driver fault-injection hooks: the three per-seed latches left (#318 E).
//!
//! Every per-call choice of the driver is an inline BUGGIFY site at the line
//! that makes it (#294, #318): the re-sends, the handover abandon, the leader
//! handoff, the send seam's drops and duplicates, the reply seam's drops and
//! duplicates, the grid and proxy picks, the read expiry, the tick stretch
//! and the peer mailbox's four choices. The durability moments are inline
//! `hint!`s. What is left here are the decisions fixed once per seed and
//! coupled to a simulation scenario (`withhold_gc_requests`,
//! `hold_journal`, the lost-verdict reply drop): they move inline once
//! moonpool can force a location's activation per seed. Production passes
//! [`NoHooks`], whose defaults never perturb the driver.
//!
//! **Every hook is consulted from the driver's node loop, never from a spawned
//! task.** A hook answer can be a randomness draw in simulation, and the node
//! loop is where the simulation steps deterministically; a draw taken inside
//! a detached task can outlive its simulation and shift the next run's
//! stream. `PeerMailbox` in `crate::driver` carries the CI failure that
//! established this.

use paros_core::JournalIdentifier;

/// A client-facing reply the driver is about to send.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Reply {
    /// A `WriteAck` with the journal state machine's verdict (#204): the
    /// deciding slot applied. Dropping it makes the client's retry meet the
    /// write already in the log — the idempotent `Duplicate` path.
    Write,
    /// A `SetLeaderAck` with the compare-and-swap's verdict (#204).
    SetLeader,
    /// A `TruncateAck` with the applied truncation (#204).
    Truncate,
    /// A call answered with no verdict (#204): a redirect from a non-leader,
    /// or a call whose slot decided another command. Ambiguous to the
    /// client, which retries.
    Redirect,
    /// A `ReadAck` answered `served: false`: the read's quorum read did not
    /// confirm in time.
    ReadUnserved,
    /// A `ReconfigureAck` (started, refused, or redirected). Dropping it
    /// after a reconfiguration started makes the client's retry meet the
    /// change already under way.
    Reconfigure,
    /// A matchmaker's `MatchReply` (registered or refused). Dropping it after
    /// the registration is durable is what makes the requester's retry the
    /// same request again — the idempotent re-answer path.
    Match,
    /// A matchmaker's `GarbageCollectAck` (#123). Dropping it after the
    /// floor is durable makes the leader re-ask a floor already in force
    /// (the idempotent `Unchanged` path) and stretches the retirement
    /// window.
    GcAck,
    /// A matchmaker's reconfiguration reply (#125: a `StopAck`, a bootstrap
    /// ack, a decree promise or vote, a `Learned`). Dropping it after its
    /// write is durable is what makes the reconfigurer's re-send meet the
    /// idempotent stop, the keyed bootstrap, and the durable vote.
    MatchmakerReconfigure,
    /// A `ReconfigureMatchmakersAck`. Dropping it after a handover started
    /// makes the client's retry meet `busy`, or a later generation.
    ReconfigureMatchmakers,
    /// A `RetireAck`. Dropping it after the node accepted its retirement
    /// leaves the operator to re-ask a node that is already gone.
    Retire,
    /// A journal `ReadAck` (#204): a page, a truncation, or an empty
    /// long-poll answer. Dropping it is a lost read the client re-asks.
    LogRead,
}

impl Reply {
    /// The stable `reply` field a dropped or duplicated reply of this kind is
    /// traced with.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Reply::LogRead => "log_read",
            Reply::Write => "write",
            Reply::SetLeader => "set_leader",
            Reply::Truncate => "truncate",
            Reply::Redirect => "redirect",
            Reply::ReadUnserved => "read_unserved",
            Reply::Reconfigure => "reconfigure",
            Reply::Match => "match",
            Reply::GcAck => "gc_ack",
            Reply::MatchmakerReconfigure => "matchmaker_reconfigure",
            Reply::ReconfigureMatchmakers => "reconfigure_matchmakers",
            Reply::Retire => "retire",
        }
    }
}

/// The driver's per-seed latches (see the module doc).
///
/// Each method is one per-seed decision in simulation. The default
/// implementation is production behavior.
pub trait DriverHooks {
    /// Whether to withhold every garbage-collection request (#123) this
    /// node would send — the first send of a request and its re-sends alike.
    /// Consulted on the node loop whenever a batch carries GC requests or a
    /// re-send is due. Always safe: GC is optional work, and a floor that
    /// never becomes effective costs only the retirements it would have
    /// licensed. Production answers `false`; the simulation withholds them
    /// for the chaos window on some seeds, keeping a prior configuration
    /// answerable (the departed straggler, #124, #263).
    fn withhold_gc_requests(&self) -> bool {
        false
    }

    /// Whether `journal` sits out this beat on this node (#188): its tick is
    /// skipped and the peer messages that arrive for it are dropped. Consulted
    /// on the node loop only when the node runs more than one journal — once
    /// per journal per beat and once per inbound message — so a held journal
    /// is a slow, partitioned journal, and its siblings on the same node must
    /// not notice (the non-interference claim). Always safe: a slow node and
    /// a lossy network are both within the model.
    fn hold_journal(&self, _journal: JournalIdentifier) -> bool {
        false
    }

    /// Whether to drop this one client-facing reply on top of the reply
    /// seam's own per-kind locations (`crate::driver::reply`, #294). It is
    /// left only for the lost-verdict scenario, a per-seed latch that drops
    /// a write's verdict on every node at one rate; it goes with the other
    /// latches (#318 E). Always safe: the client-facing RPC response can be
    /// lost in production at any time.
    fn drop_client_reply(&self, _reply: Reply) -> bool {
        false
    }
}

/// Inert production hooks.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoHooks;

impl DriverHooks for NoHooks {}
