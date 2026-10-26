//! The kinds of client-facing reply a driver sends, as a dropped or
//! duplicated reply is reported to the audit.

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
