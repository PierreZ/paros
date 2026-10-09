//! The client-reply seam: the one place a driver hands an answer back to the
//! reply handle a driver loop is holding. Each reply kind is its own inline
//! BUGGIFY location ([`drop_reply`]), drawn exactly once per reply after the
//! server state advanced, and a drop is reported the instant it happens. Its
//! mirror is the matchmaker-plane duplicate ([`maybe_duplicate`]). Both are
//! inert in production and silent in the recovery tail (#294).

use paros_core::{MatchmakerId, NodeId};
use tokio::sync::mpsc;

use crate::audit::Audit;
use crate::rpc::ReplySender;

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

/// Whether to drop this one reply of `kind` after the server state advanced.
/// One location per kind, one macro line per arm.
///
/// Always safe: the client-facing RPC response can be lost in production at
/// any time, and the ack contract is built for it. "Committed" is
/// re-derivable by a retry through the `(client, seq)` dedup path. This
/// makes "committed, applied, and the client does not know" likely: the
/// precondition of the dedup-window edges.
fn drop_reply(kind: Reply) -> bool {
    match kind {
        // A retried write meets its own verdict in the log (#204: a
        // `Duplicate`), the edge a lost verdict lives on.
        Reply::Write => moonpool_buggify::buggify_fault_with_prob!(0.10),
        // A lost claim: the owner does not know it won, and its next write
        // names its old generation. It is refused, naming itself as the
        // writer, which it adopts.
        Reply::SetLeader => moonpool_buggify::buggify_fault_with_prob!(0.10),
        // A lost journal read: the client re-asks, and a read waiting at the
        // tail costs it its deadline first.
        Reply::LogRead => moonpool_buggify::buggify_fault_with_prob!(0.10),
        // A lost redirect costs the client its whole request deadline before
        // it retries blind, so the retarget policies meet a stale hint under
        // time pressure.
        Reply::Redirect => moonpool_buggify::buggify_fault_with_prob!(0.10),
        Reply::ReadUnserved => moonpool_buggify::buggify_fault_with_prob!(0.10),
        // A lost truncation ack is the one ambiguity the truncation client's
        // re-ask loop must absorb.
        Reply::Truncate => moonpool_buggify::buggify_fault_with_prob!(0.10),
        // A lost matchmaker reply after the registration is durable: the
        // requester's retry is the same request again, the idempotent
        // re-answer path.
        Reply::Match => moonpool_buggify::buggify_fault_with_prob!(0.20),
        // A lost reconfiguration ack: the client re-asks and meets the change
        // already under way (refused `not_leader`, then `unchanged`).
        Reply::Reconfigure => moonpool_buggify::buggify_fault_with_prob!(0.10),
        // A lost GC ack after the floor is durable: the leader re-asks a
        // floor already in force (the idempotent `Unchanged` answer).
        Reply::GcAck => moonpool_buggify::buggify_fault_with_prob!(0.20),
        // A lost handover reply after its write is durable: the
        // reconfigurer's re-send meets the idempotent stop, the keyed
        // bootstrap, the durable vote.
        Reply::MatchmakerReconfigure => moonpool_buggify::buggify_fault_with_prob!(0.20),
        // A lost matchmaker-reconfiguration ack: the client re-asks and meets
        // `busy`, or a later generation.
        Reply::ReconfigureMatchmakers => moonpool_buggify::buggify_fault_with_prob!(0.10),
        // A lost retirement ack: the operator re-asks a node already gone.
        Reply::Retire => moonpool_buggify::buggify_fault_with_prob!(0.10),
    }
}

/// Answer one client-facing reply from a driver, or drop it at the reply
/// seam, reported through [`Audit::client_reply_dropped`].
///
/// The drop is the kind's own location ([`drop_reply`]), or, for a write,
/// the lost-verdict scenario's named location
/// ([`LOSE_VERDICTS`](crate::scenario::LOSE_VERDICTS), #318 E).
pub(crate) fn answer<T, A: Audit>(
    audit: &A,
    node: NodeId,
    kind: Reply,
    waiter: ReplySender<T>,
    ack: T,
) {
    if (kind == Reply::Write && crate::scenario::lose_verdict()) || drop_reply(kind) {
        audit.client_reply_dropped(node, kind);
        tracing::info!(node = node.0, reply = kind.label(), "client_reply_dropped");
    } else {
        let _ = waiter.send(ack);
    }
}

/// The matchmaker driver's twin of [`answer`]: the same locations, with the
/// fired gate inline, one per family.
pub(crate) fn match_answer<T>(
    matchmaker: MatchmakerId,
    kind: Reply,
    waiter: ReplySender<T>,
    ack: T,
) {
    if !drop_reply(kind) {
        let _ = waiter.send(ack);
        return;
    }
    // BUGGIFY pairing: a matchmaker reply is genuinely dropped. The recovery
    // halves are the GC, handover and re-answer gates.
    match kind {
        Reply::GcAck => moonpool_assertions::reachable!(
            "gc: a garbage-collection ack is dropped at the reply seam"
        ),
        Reply::MatchmakerReconfigure => moonpool_assertions::reachable!(
            "generation: a handover reply is dropped at the reply seam"
        ),
        _ => moonpool_assertions::reachable!("matchmaker: a reply is dropped at the reply seam"),
    }
    tracing::info!(
        matchmaker = matchmaker.0,
        reply = kind.label(),
        "match_reply_dropped"
    );
}

/// Whether to fold this one matchmaker-plane reply twice. One location per
/// kind, one macro line per arm, at the drop twins' rates.
///
/// Always safe: a duplicate is what the sender's own re-send produces once
/// its first answer was merely slow, so every reply the node loop folds must
/// already be idempotent. Only the replies that reach the node loop through
/// a channel can be duplicated: a unary RPC reply is delivered exactly once,
/// and the client's retry is the duplicate that path has to survive.
fn duplicate_reply(kind: Reply) -> bool {
    match kind {
        // Folded twice into the open matchmaking phase: a matchmaker already
        // counted must not re-open the registration quorum, and a refusal
        // must not be applied twice to the round floor.
        Reply::Match => moonpool_buggify::buggify_fault_with_prob!(0.20),
        // Folded twice into the collector: an ack already counted must not
        // re-close the floor, nor re-name the retirable acceptors.
        Reply::GcAck => moonpool_buggify::buggify_fault_with_prob!(0.20),
        // Folded twice into the reconfigurer: a repeated `StopAck`, bootstrap
        // ack, decree promise or vote must move no tally, and a repeated
        // `Learned` must not re-publish.
        Reply::MatchmakerReconfigure => moonpool_buggify::buggify_fault_with_prob!(0.20),
        _ => false,
    }
}

/// The duplicate seam of the matchmaker plane: re-queue `reply` so the node
/// loop folds it a second time, through the identical arm, interleaved with
/// whatever else arrives. This makes the idempotency every folded answer
/// claims likely instead of lucky. Drawn on the node loop; the bounded
/// channel caps the copies whatever the coin says, and only a copy that was
/// actually queued fires the gate.
pub(crate) fn maybe_duplicate<T: Clone>(
    node: NodeId,
    kind: Reply,
    inbox: &mpsc::Sender<T>,
    reply: &T,
) {
    if !duplicate_reply(kind) || inbox.try_send(reply.clone()).is_err() {
        return;
    }
    // BUGGIFY pairing: the copy is genuinely queued. The recovery half is
    // every idempotency `always` check the fold meets.
    match kind {
        Reply::Match => {
            moonpool_assertions::reachable!("a matchmaker's registration reply is folded twice");
        }
        Reply::GcAck => moonpool_assertions::reachable!("a matchmaker's GC ack is folded twice"),
        Reply::MatchmakerReconfigure => {
            moonpool_assertions::reachable!("a matchmaker's handover reply is folded twice");
        }
        _ => unreachable!("only the matchmaker plane's replies are duplicated"),
    }
    tracing::info!(
        node = node.0,
        reply = kind.label(),
        "client_reply_duplicated"
    );
}
