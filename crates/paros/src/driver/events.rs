//! The driver's observability helpers: the small pure functions that turn a
//! domain value into the stable field a trace or an [`Audit`](crate::Audit)
//! callback carries (value/command hashes, message labels, the
//! ballot-carrying route triple). The tracing event names themselves are
//! string literals at their emit sites, for humans only: nothing reads the
//! trace back (correctness lives in the audit).

use paros_core::{Ballot, Command, Control, Message, NodeId, Registration, Slot};

use crate::grpc::internal;

/// A stable `u64` digest of a value's bytes (FNV-1a), emitted on observability
/// events so an observer can compare chosen values by equality without
/// carrying the raw payload through the trace.
fn value_hash(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in bytes {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

/// The value hash for a decided [`Command`], for observability. A client entry
/// hashes its opaque value bytes; a control command hashes a stable, distinct
/// encoding of its metadata, so every node agrees on the per-slot hash the audit
/// compares (a control command decided for a slot is the same on all nodes).
///
/// Public so an [`Audit`](crate::Audit) implementation can hash a `Command` it observes on the
/// wire ([`Audit::sent`](crate::Audit::sent)) with the *same* function the driver uses for the
/// durable-write and apply callbacks.
#[must_use]
pub fn command_hash(command: &Command) -> u64 {
    match command {
        Command::User(entry) => value_hash(&entry.value.0),
        Command::Control(Control::Truncate { up_to }) => {
            let mut bytes = vec![0xff_u8];
            bytes.extend_from_slice(&up_to.0.to_le_bytes());
            value_hash(&bytes)
        }
        // A distinct one-byte tag: no `Truncate` encoding can collide with it (they
        // are nine bytes and start `0xff`), and every node hashes the same no-op to
        // the same digest, so per-slot prefix agreement stays checkable.
        Command::Control(Control::Noop) => value_hash(&[0xfe_u8]),
        // Nine bytes starting 0xfd: disjoint from both encodings above.
        Command::Control(Control::Snap { at_index }) => {
            let mut bytes = vec![0xfd_u8];
            bytes.extend_from_slice(&at_index.0.to_le_bytes());
            value_hash(&bytes)
        }
    }
}

/// A stable `u64` digest of a matchmaking history page (FNV-1a over each
/// registration's ballot, kind and membership), so the audit can name *which*
/// answer a candidate folded without the reply's bytes travelling through the
/// port. Order-sensitive, which is what the page contract wants: two pages
/// with the same registrations in a different order are different answers.
pub fn registration_history_hash<'a, I>(history: I) -> u64
where
    I: IntoIterator<Item = (&'a Ballot, &'a Registration)>,
{
    let mut bytes: Vec<u8> = Vec::new();
    for (ballot, registration) in history {
        bytes.extend_from_slice(&ballot.round.to_le_bytes());
        bytes.extend_from_slice(&ballot.node.0.to_le_bytes());
        bytes.push(u8::from(registration.kind.is_reconfiguration()));
        for member in registration.config.members() {
            bytes.extend_from_slice(&member.0.to_le_bytes());
        }
        // A separator, so two adjacent memberships cannot be re-cut into
        // the same byte string.
        bytes.push(0xff);
    }
    value_hash(&bytes)
}

/// A short, stable label for a [`Message`] variant, for observability: the `kind`
/// field on the `msg_sent` / `msg_received` events.
pub(crate) fn message_kind(m: &Message) -> &'static str {
    match m {
        Message::Prepare { .. } => "prepare",
        Message::Promise { .. } => "promise",
        Message::Accept { .. } => "accept",
        Message::Accepted { .. } => "accepted",
        Message::Nack { .. } => "nack",
        Message::Commit { .. } => "commit",
        Message::CatchUpRequest { .. } => "catchup_request",
        Message::CatchUpResponse { .. } => "catchup_response",
        Message::InstallSnapshot { .. } => "install_snapshot",
        Message::Heartbeat { .. } => "heartbeat",
        Message::HeartbeatAck { .. } => "heartbeat_ack",
        Message::SnapAck { .. } => "snap_ack",
        Message::SnapChunkRequest { .. } => "snap_chunk_request",
        Message::SnapChunkResponse { .. } => "snap_chunk_response",
        Message::Relinquish { .. } => "relinquish",
        _ => "unknown",
    }
}

/// The `(sender, ballot, slot)` triple a ballot-carrying Paxos message routes on,
/// for observability. Every ballot-carrying kind returns `Some`, `Heartbeat`
/// included — its "slot" is the commit watermark it advertises, which is
/// `None` on a leader that has chosen nothing (an empty prefix is not slot 0;
/// see [`paros_core::Message::Heartbeat`]). The kinds with no ballot at all
/// (the catch-up pair) return `None` outright.
pub(crate) fn message_route(m: &Message) -> Option<(NodeId, Ballot, Option<Slot>)> {
    match m {
        // Phase 1 is per-ballot: report `from_slot` as the slot for the timeline.
        Message::Prepare {
            reply_to: from,
            ballot,
            from_slot,
            ..
        }
        | Message::Promise {
            from,
            ballot,
            from_slot,
            ..
        } => Some((*from, *ballot, Some(*from_slot))),
        Message::Accept {
            reply_to: from,
            ballot,
            slot,
            ..
        }
        | Message::Accepted {
            from, ballot, slot, ..
        }
        | Message::Nack {
            from, ballot, slot, ..
        }
        | Message::Commit {
            from, ballot, slot, ..
        } => Some((*from, *ballot, Some(*slot))),
        Message::Heartbeat {
            from,
            ballot,
            commit,
            ..
        } => Some((*from, *ballot, *commit)),
        Message::InstallSnapshot {
            from,
            ballot,
            chosen_index,
            ..
        } => Some((*from, *ballot, Some(*chosen_index))),
        // A handoff's "slot" is the allocator frontier it transfers — the
        // field that carries its meaning on a timeline.
        Message::Relinquish {
            from,
            ballot,
            next_slot,
            ..
        } => Some((*from, *ballot, Some(*next_slot))),
        _ => None,
    }
}

/// A short, stable label for an encoded [`internal::ConsensusMessage`], for
/// the mailbox-drop audit report (mirrors [`message_kind`], which needs the
/// decoded domain [`Message`] the delivery task no longer has).
pub(crate) fn proto_message_kind(m: &internal::ConsensusMessage) -> &'static str {
    use internal::consensus_message::Kind;
    match &m.kind {
        Some(Kind::Prepare(_)) => "prepare",
        Some(Kind::Promise(_)) => "promise",
        Some(Kind::Accept(_)) => "accept",
        Some(Kind::Accepted(_)) => "accepted",
        Some(Kind::Nack(_)) => "nack",
        Some(Kind::Commit(_)) => "commit",
        Some(Kind::CatchUpRequest(_)) => "catchup_request",
        Some(Kind::CatchUpResponse(_)) => "catchup_response",
        Some(Kind::InstallSnapshot(_)) => "install_snapshot",
        Some(Kind::Heartbeat(_)) => "heartbeat",
        Some(Kind::HeartbeatAck(_)) => "heartbeat_ack",
        Some(Kind::SnapAck(_)) => "snap_ack",
        Some(Kind::Relinquish(_)) => "relinquish",
        Some(Kind::SnapChunkRequest(_)) => "snap_chunk_request",
        Some(Kind::SnapChunkResponse(_)) => "snap_chunk_response",
        None => "unknown",
    }
}
