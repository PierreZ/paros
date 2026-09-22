//! The client-reply seam: the one place a driver hands an answer back to the
//! oneshot a tonic handler is holding, consulting the reply-drop hook exactly
//! once per reply ([`DriverHooks::drop_client_reply`]) and reporting a drop
//! the instant it happens — and its mirror, the matchmaker-plane duplicate
//! ([`DriverHooks::duplicate_client_reply`]).

use paros_core::{MatchmakerId, NodeId};
use tokio::sync::mpsc;

use crate::audit::Audit;
use crate::grpc::ReplySender;
use crate::hooks::{DriverHooks, Reply};

/// Consult the reply-drop hook exactly once, after the server state advanced,
/// and either send `ack` to the held oneshot or report the drop through
/// `report_drop`, where its trace is emitted.
fn answer_or_drop<T, H: DriverHooks>(
    hooks: &H,
    kind: Reply,
    waiter: ReplySender<T>,
    ack: T,
    report_drop: impl FnOnce(),
) {
    if hooks.drop_client_reply(kind) {
        report_drop();
    } else {
        let _ = waiter.send(ack);
    }
}

/// Answer one client-facing reply from the node driver, or drop it at the
/// reply seam, reported through [`Audit::client_reply_dropped`].
pub(crate) fn answer<T, H: DriverHooks, A: Audit>(
    hooks: &H,
    audit: &A,
    node: NodeId,
    kind: Reply,
    waiter: ReplySender<T>,
    ack: T,
) {
    answer_or_drop(hooks, kind, waiter, ack, || {
        audit.client_reply_dropped(node, kind);
        tracing::info!(node = node.0, reply = kind.label(), "client_reply_dropped");
    });
}

/// The matchmaker driver's twin of [`answer`]: the same seam, reported
/// through [`Audit::match_reply_dropped`].
pub(crate) fn match_answer<T, H: DriverHooks, A: Audit>(
    hooks: &H,
    audit: &A,
    matchmaker: MatchmakerId,
    kind: Reply,
    waiter: ReplySender<T>,
    ack: T,
) {
    answer_or_drop(hooks, kind, waiter, ack, || {
        audit.match_reply_dropped(matchmaker, kind);
        tracing::info!(
            matchmaker = matchmaker.0,
            reply = kind.label(),
            "match_reply_dropped"
        );
    });
}

/// The duplicate seam of the matchmaker plane: re-queue `reply` so the node
/// loop folds it a second time, through the identical arm, interleaved with
/// whatever else arrives — the idempotency every folded answer claims, made
/// likely instead of lucky. Decided on the loop, per the hooks rule; the
/// bounded channel caps the copies whatever the coin says, and only a copy
/// that was actually queued is reported.
pub(crate) fn maybe_duplicate<T: Clone, H: DriverHooks, A: Audit>(
    hooks: &H,
    audit: &A,
    node: NodeId,
    kind: Reply,
    inbox: &mpsc::Sender<T>,
    reply: &T,
) {
    if hooks.duplicate_client_reply(kind) && inbox.try_send(reply.clone()).is_ok() {
        audit.client_reply_duplicated(node, kind);
        tracing::info!(
            node = node.0,
            reply = kind.label(),
            "client_reply_duplicated"
        );
    }
}
