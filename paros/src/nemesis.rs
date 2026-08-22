//! Per-message send filtering: the hook the driver consults for *every* outbound
//! protocol message, where a fault injector can target one **message class**.
//!
//! Network-level chaos (moonpool's swarm partitions) is blunt: it cuts a link and
//! everything on it dies together. A consensus implementation's recovery
//! mechanisms are per-*variant*, though — starving only `Commit`s leaves catch-up
//! as the sole way a follower can heal, starving only `HeartbeatAck`s toward the
//! leader is the only way to reach the read-index round's TTL sweep, and dropping
//! only `Accepted` from one acceptor probes the quorum edge. An IP-level partition
//! cannot express any of those.
//!
//! The per-message send point lives in the *driver*, in `drain_ready`, where the
//! message is still a fully typed [`Message`] — so the hook has to live there too,
//! provider-generic, exactly like [`CrashSeam`](crate::CrashSeam). Production
//! ships [`SendAll`], which returns [`SendVerdict::Send`] for everything, so the
//! hook compiles away to a branch that is never taken and the filter is inert
//! outside the deterministic simulation.

use paros_core::{Message, NodeId};

/// What the driver should do with one outbound message.
///
/// Every variant is a *link-level* fault: the sender's own state machine has
/// already run and its durable writes are already persisted, so none of these
/// can make the core observe an impossible history — they only decide what the
/// peers get to see, which is precisely what a real network is free to do.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum SendVerdict {
    /// Send it once, unchanged. The production behavior.
    #[default]
    Send,
    /// Drop it: the message never leaves this node. Indistinguishable, to the
    /// peer, from a network drop — but selected by variant and destination.
    Drop,
    /// Send it twice. Paxos messages are idempotent, so a duplicate must be
    /// harmless; this is the assertion that they really are.
    Duplicate,
    /// Park it until the node's next logical tick, then send it. A bounded delay
    /// applied to one class reorders that class against every other one — the
    /// cheapest way to reach interleavings like "slot N+1 decides before slot N".
    Defer,
}

/// The driver's per-message send hook. Called once for every message a
/// [`paros_core::Ready`] batch addresses (and for every snapshot offer it
/// serves), *after* the batch's durable writes are on disk and immediately
/// before the message would go on the wire.
///
/// `to` is the destination the core addressed, so a filter can be directional —
/// the one-way partition shape that the hardest consensus bugs need.
pub trait SendFilter {
    /// Decide what happens to `msg` on its way to `to`.
    fn on_send(&self, to: NodeId, msg: &Message) -> SendVerdict;
}

/// The production send filter: every message goes out once, unchanged. The
/// driver ships this, so the hook is inert outside the deterministic simulation.
#[derive(Clone, Copy, Debug, Default)]
pub struct SendAll;

impl SendFilter for SendAll {
    fn on_send(&self, _to: NodeId, _msg: &Message) -> SendVerdict {
        SendVerdict::Send
    }
}

impl SendVerdict {
    /// A short, stable label for the fault timeline (the `action` field on the
    /// driver's `msg_filtered` event).
    #[must_use]
    pub(crate) fn label(self) -> &'static str {
        match self {
            SendVerdict::Send => "send",
            SendVerdict::Drop => "drop",
            SendVerdict::Duplicate => "duplicate",
            SendVerdict::Defer => "defer",
        }
    }
}
