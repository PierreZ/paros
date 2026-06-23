//! The protocol message enum. Pure in-memory data — the core never serializes
//! it. The driver decodes inbound bytes into a [`Message`] before
//! [`crate::RawNode::step`], and encodes [`crate::Ready::messages`] after
//! draining a batch.

use crate::types::{Ballot, NodeId, Slot, Value};

/// Every protocol stimulus the core understands. Peer RPCs and tick-injected
/// self-events all enter through the single [`crate::RawNode::step`] router.
///
/// `#[non_exhaustive]` so later stages can add variants (e.g. snapshot transfer,
/// reconfiguration) without a breaking change.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Message {
    // ---- Phase 1 (prepare / promise) ----
    /// Proposer → acceptors: "promise not to accept anything below `ballot`."
    Prepare {
        /// Sender.
        from: NodeId,
        /// The ballot being prepared.
        ballot: Ballot,
        /// The slot being prepared.
        slot: Slot,
    },
    /// Acceptor → proposer: a promise, optionally reporting a previously
    /// accepted `(ballot, value)` for `slot` so the proposer can re-propose it.
    Promise {
        /// Sender.
        from: NodeId,
        /// The ballot promised.
        ballot: Ballot,
        /// The slot promised.
        slot: Slot,
        /// Any value already accepted for `slot`, with the ballot it was
        /// accepted at.
        accepted: Option<(Ballot, Value)>,
    },

    // ---- Phase 2 (accept / accepted / nack) ----
    /// Proposer → acceptors: "accept `value` for `slot` at `ballot`."
    Accept {
        /// Sender.
        from: NodeId,
        /// The ballot under which the value is proposed.
        ballot: Ballot,
        /// The target slot.
        slot: Slot,
        /// The proposed value.
        value: Value,
    },
    /// Acceptor → proposer: durably accepted the proposal for `slot` at `ballot`.
    Accepted {
        /// Sender.
        from: NodeId,
        /// The accepted ballot.
        ballot: Ballot,
        /// The accepted slot.
        slot: Slot,
    },
    /// Acceptor → proposer: rejection, reporting the higher `ballot` it has
    /// already promised so the proposer can catch up.
    Nack {
        /// Sender.
        from: NodeId,
        /// The higher ballot already promised by the acceptor.
        ballot: Ballot,
        /// The contested slot.
        slot: Slot,
    },

    // ---- Learning ----
    /// Any → any: `value` is chosen for `slot` (decided at `ballot`).
    Commit {
        /// Sender.
        from: NodeId,
        /// The ballot at which the value was chosen.
        ballot: Ballot,
        /// The chosen slot.
        slot: Slot,
        /// The chosen value.
        value: Value,
    },

    // ---- Tick-injected self-events (synthesized by `tick`, routed via `step`) ----
    /// "Have I heard from a leader recently?" — drives leader election / a
    /// ballot bump when it fires.
    CheckLeader {
        /// The node checking on itself.
        from: NodeId,
    },
    /// Leader → self: time to broadcast heartbeats / re-send un-acked `Accept`s.
    Heartbeat {
        /// The leader heartbeating.
        from: NodeId,
    },
}
