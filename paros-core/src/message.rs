//! The protocol message enum. Pure in-memory data — the core never serializes
//! it. The driver decodes inbound bytes into a [`Message`] before
//! [`crate::RawNode::step`], and encodes [`crate::Ready::messages`] after
//! draining a batch.

use std::collections::BTreeMap;

use crate::types::{Ballot, Entry, NodeId, Slot};

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
    Promise {
        /// Sender.
        from: NodeId,
        /// The ballot promised.
        ballot: Ballot,
        /// First slot this promise covers (echoes the prepare's `from_slot`).
        from_slot: Slot,
        /// All accepted entries for slots `>= from_slot`. Empty if none.
        accepted: BTreeMap<Slot, (Ballot, Entry)>,
    },

    // ---- Phase 2 (accept / accepted / nack) ----
    /// Proposer → acceptors: "accept `entry` for `slot` at `ballot`."
    Accept {
        /// Sender.
        from: NodeId,
        /// The ballot under which the entry is proposed.
        ballot: Ballot,
        /// The target slot.
        slot: Slot,
        /// The proposed entry (value plus its client tag).
        entry: Entry,
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
    /// Any → any: `entry` is chosen for `slot` (decided at `ballot`).
    Commit {
        /// Sender.
        from: NodeId,
        /// The ballot at which the entry was chosen.
        ballot: Ballot,
        /// The chosen slot.
        slot: Slot,
        /// The chosen entry.
        entry: Entry,
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
        /// The leader heartbeating.
        from: NodeId,
        /// The leader's current ballot (lets a follower adopt or refuse it).
        ballot: Ballot,
        /// The leader's highest contiguous chosen slot.
        commit: Slot,
    },
}
