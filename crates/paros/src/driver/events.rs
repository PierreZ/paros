//! The driver's observability helpers: the small pure functions that turn a
//! domain value into the stable field a trace or an [`Audit`](crate::Audit)
//! callback carries (value/command hashes, message labels, the
//! ballot-carrying route triple). The tracing event names themselves are
//! string literals at their emit sites, for humans only: nothing reads the
//! trace back (correctness lives in the audit).

use paros_core::{
    AcceptorConfig, Ballot, Command, Control, Message, Party, ReconfigureRefusal, ReconfigureReply,
    ReconfigureRequest, ReconfigureResult, Registration, Slot,
};

use crate::rpc::internal;

/// A stable digest of an acceptor configuration (FNV-1a over the sorted
/// membership and the quorum system), emitted as a trace field so a human
/// reading the trace can compare configurations by equality without printing
/// them. The audit callbacks carry the configuration itself.
#[must_use]
pub(crate) fn config_hash(config: &AcceptorConfig) -> u64 {
    // The same byte sequence, tag for tag, the digest has always folded:
    // the membership length, each member, the quorum-system tag, its sizes.
    let mut bytes: Vec<u8> = Vec::new();
    bytes.extend_from_slice(&(config.members().len() as u64).to_le_bytes());
    for member in config.members() {
        bytes.extend_from_slice(&member.0.to_le_bytes());
    }
    match config.quorum_system() {
        paros_core::QuorumSystem::Majority => bytes.push(0_u8),
        paros_core::QuorumSystem::Flexible { q1, q2 } => {
            bytes.push(1_u8);
            bytes.extend_from_slice(&(q1 as u64).to_le_bytes());
            bytes.extend_from_slice(&(q2 as u64).to_le_bytes());
        }
        paros_core::QuorumSystem::Grid { rows, cols } => {
            bytes.push(2_u8);
            bytes.extend_from_slice(&(rows as u64).to_le_bytes());
            bytes.extend_from_slice(&(cols as u64).to_le_bytes());
        }
    }
    // The encoding is the length word, one word per member and a tag that
    // carries either nothing or two size words: nothing else is folded.
    let header = 8 * (1 + config.members().len());
    assert!(
        bytes.len() > header,
        "a configuration digest folds its quorum tag"
    );
    assert!(
        bytes.len() == header + 1 || bytes.len() == header + 17,
        "a quorum tag carries nothing or two size words"
    );
    value_hash(&bytes)
}

/// A stable `u64` digest of a value's bytes (FNV-1a), emitted on observability
/// events so an observer can compare chosen values by equality without
/// carrying the raw payload through the trace.
pub(crate) fn value_hash(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in bytes {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    if bytes.is_empty() {
        assert!(
            h == 0xcbf2_9ce4_8422_2325,
            "an empty value hashes to the offset basis"
        );
    }
    h
}

/// The value hash for a decided [`Command`], for observability. A client write
/// hashes its writer, position and records; a control command hashes a stable, distinct
/// encoding of its metadata, so every node agrees on the per-slot hash the audit
/// compares (a control command decided for a slot is the same on all nodes).
///
/// Public so an [`Audit`](crate::Audit) implementation can hash a `Command` it observes on the
/// wire ([`Audit::sent`](crate::Audit::sent)) with the *same* function the driver uses for the
/// durable-write and apply callbacks.
///
/// # Panics
///
/// If an assertion on its own invariants, preconditions or postconditions
/// fails: a programmer error, never an operating condition.
#[must_use]
pub fn command_hash(command: &Command) -> u64 {
    match command {
        Command::Write(entry) => {
            let mut bytes = Vec::new();
            bytes.extend_from_slice(&entry.leader.0.to_le_bytes());
            bytes.extend_from_slice(&entry.seq.0.to_le_bytes());
            for record in &entry.records {
                bytes.extend_from_slice(&(record.0.len() as u64).to_le_bytes());
                bytes.extend_from_slice(&record.0);
            }
            value_hash(&bytes)
        }
        Command::Control(Control::Truncate { leader, up_to }) => {
            let mut bytes = vec![0xff_u8];
            bytes.extend_from_slice(&leader.0.to_le_bytes());
            bytes.extend_from_slice(&up_to.0.to_le_bytes());
            // The no-collision argument below rests on this exact shape.
            assert!(bytes.len() == 25, "a truncate encodes to twenty-five bytes");
            assert!(bytes[0] == 0xff, "a truncate encoding starts with its tag");
            value_hash(&bytes)
        }
        // A distinct one-byte tag: no `Truncate` encoding can collide with it (they
        // are twenty-five bytes and start `0xff`), and every node hashes the same no-op to
        // the same digest, so per-slot prefix agreement stays checkable.
        Command::Control(Control::Noop) => value_hash(&[0xfe_u8]),
        Command::Control(Control::SetLeader { new, old }) => {
            let mut bytes = vec![0xfd_u8];
            bytes.extend_from_slice(&new.0.to_le_bytes());
            // `None` encodes as the unset uuid, which no `new` can be.
            bytes.extend_from_slice(&old.map_or(0, |old| old.0).to_le_bytes());
            assert!(
                bytes.len() == 33,
                "a set-leader encodes to thirty-three bytes"
            );
            assert!(
                bytes[0] == 0xfd,
                "a set-leader encoding starts with its tag"
            );
            value_hash(&bytes)
        }
    }
}

/// A stable `u64` digest of a matchmaking history page (FNV-1a over each
/// registration's ballot, kind and membership), so the audit can name *which*
/// answer a candidate folded without the reply's bytes travelling through the
/// port. Order-sensitive, which is what the page contract wants: two pages
/// with the same registrations in a different order are different answers.
///
/// # Panics
///
/// If an assertion on its own invariants, preconditions or postconditions
/// fails: a programmer error, never an operating condition.
pub fn registration_history_hash<'a, I>(history: I) -> u64
where
    I: IntoIterator<Item = (&'a Ballot, &'a Registration)>,
{
    let mut bytes: Vec<u8> = Vec::new();
    let mut registrations = 0_usize;
    for (ballot, registration) in history {
        registrations += 1;
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
    // Every registration folds at least its ballot, kind and separator.
    assert!(
        bytes.len() >= registrations * 18,
        "each registration folds its header"
    );
    if registrations == 0 {
        assert!(bytes.is_empty(), "an empty page folds nothing");
    }
    value_hash(&bytes)
}

/// The trace label of one matchmaker-set reconfiguration request (#125).
#[must_use]
pub(crate) fn reconfigure_kind(request: &ReconfigureRequest) -> &'static str {
    match request {
        ReconfigureRequest::Stop { .. } => "stop",
        ReconfigureRequest::Bootstrap { .. } => "bootstrap",
        ReconfigureRequest::DecreePrepare { .. } => "decree_prepare",
        ReconfigureRequest::DecreeAccept { .. } => "decree_accept",
        ReconfigureRequest::Chosen { .. } => "chosen",
    }
}

/// The trace label of one matchmaker-set reconfiguration reply (#125).
#[must_use]
pub(crate) fn reconfigure_reply_kind(reply: &ReconfigureReply) -> &'static str {
    match reply {
        ReconfigureReply::Stopped { .. } => "stopped",
        ReconfigureReply::Bootstrapped { .. } => "bootstrapped",
        ReconfigureReply::Promised { .. } => "promised",
        ReconfigureReply::Accepted { .. } => "accepted",
        ReconfigureReply::Nacked { .. } => "nacked",
        ReconfigureReply::Learned { .. } => "learned",
        ReconfigureReply::Refused { .. } => "refused",
    }
}

/// What an acceptor-set reconfiguration request (#122) answers the client
/// with: `(accepted, the stable refusal label — empty when accepted, the
/// round the reconfiguration campaigns at)`.
#[must_use]
pub(crate) fn reconfigure_outcome(result: ReconfigureResult) -> (bool, &'static str, Option<u64>) {
    let outcome = reconfigure_outcome_unchecked(result);
    // Accepted, label-free and carrying a round are one fact, said three ways.
    assert!(
        outcome.0 == outcome.1.is_empty(),
        "only a refusal carries a label"
    );
    assert!(
        outcome.0 == outcome.2.is_some(),
        "only a started reconfiguration has a round"
    );
    outcome
}

fn reconfigure_outcome_unchecked(result: ReconfigureResult) -> (bool, &'static str, Option<u64>) {
    match result {
        ReconfigureResult::Started(ballot) => (true, "", Some(ballot.round)),
        ReconfigureResult::NotLeader(_) => (false, "not_leader", None),
        ReconfigureResult::Refused(ReconfigureRefusal::NoMatchmakers) => {
            (false, "no_matchmakers", None)
        }
        ReconfigureResult::Refused(ReconfigureRefusal::Unchanged) => (false, "unchanged", None),
        ReconfigureResult::Refused(ReconfigureRefusal::UnknownMember) => {
            (false, "unknown_member", None)
        }
        ReconfigureResult::Refused(ReconfigureRefusal::Malformed) => (false, "malformed", None),
        ReconfigureResult::Refused(ReconfigureRefusal::Unsettled) => (false, "unsettled", None),
        ReconfigureResult::Refused(ReconfigureRefusal::RoundExhausted) => {
            (false, "round_exhausted", None)
        }
    }
}

/// A short, stable label for a [`Message`] variant, for observability: the `kind`
/// field on the `msg_sent` / `msg_received` events. Public so an [`Audit`]
/// implementation can tally by the same labels the driver traces with.
///
/// [`Audit`]: crate::Audit
///
/// # Panics
///
/// If an assertion on its own invariants, preconditions or postconditions
/// fails: a programmer error, never an operating condition.
#[must_use]
pub fn message_kind(m: &Message) -> &'static str {
    let kind = message_kind_unchecked(m);
    // Every labelled kind names its origin; the pair below must agree.
    if kind != "unknown" {
        assert!(
            message_sender(m).is_some(),
            "a labelled message names its sender"
        );
    }
    kind
}

fn message_kind_unchecked(m: &Message) -> &'static str {
    match m {
        Message::Prepare { .. } => "prepare",
        Message::Promise { .. } => "promise",
        Message::Accept { .. } => "accept",
        Message::Accepted { .. } => "accepted",
        Message::Nack { .. } => "nack",
        Message::Commit { .. } => "commit",
        Message::CatchUpRequest { .. } => "catchup_request",
        Message::CatchUpResponse { .. } => "catchup_response",
        Message::TrimmedTo { .. } => "trimmed_to",
        Message::Heartbeat { .. } => "heartbeat",
        Message::HeartbeatAck { .. } => "heartbeat_ack",
        Message::Relinquish { .. } => "relinquish",
        Message::PreRead { .. } => "pre_read",
        Message::PreReadAck { .. } => "pre_read_ack",
        _ => "unknown",
    }
}

/// The `(sender, ballot, slot)` triple a ballot-carrying Paxos message routes on,
/// for observability. Every ballot-carrying kind returns `Some`, `Heartbeat`
/// included — its "slot" is the commit watermark it advertises, which is
/// `None` on a leader that has chosen nothing (an empty prefix is not slot 0;
/// see [`paros_core::Message::Heartbeat`]). The kinds with no ballot at all
/// (the catch-up pair) return `None` outright.
pub(crate) fn message_route(m: &Message) -> Option<(Party, Ballot, Option<Slot>)> {
    let route = message_route_unchecked(m);
    // The route's party and the sender are read from the same field: a
    // routed message's origin is its sender, never a second party.
    if let Some((party, _, _)) = route {
        assert!(message_sender(m) == Some(party), "a route names the sender");
        assert!(message_kind(m) != "unknown", "a routed message has a label");
    }
    route
}

fn message_route_unchecked(m: &Message) -> Option<(Party, Ballot, Option<Slot>)> {
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
        } => Some((Party::Node(*from), *ballot, Some(*from_slot))),
        // The two Phase-2 kinds whose party may be a proxy leader (#142).
        Message::Accept {
            reply_to: from,
            ballot,
            slot,
            ..
        }
        | Message::Commit {
            from, ballot, slot, ..
        } => Some((*from, *ballot, Some(*slot))),
        Message::Accepted {
            from, ballot, slot, ..
        }
        | Message::Nack {
            from, ballot, slot, ..
        } => Some((Party::Node(*from), *ballot, Some(*slot))),
        Message::Heartbeat {
            from,
            ballot,
            commit,
            ..
        } => Some((Party::Node(*from), *ballot, *commit)),
        // A handoff's "slot" is the allocator frontier it transfers — the
        // field that carries its meaning on a timeline.
        Message::Relinquish {
            from,
            ballot,
            next_slot,
            ..
        } => Some((Party::Node(*from), *ballot, Some(*next_slot))),
        _ => None,
    }
}

/// Who sent `m`: the party every consensus message names as its origin
/// (`from`, or `reply_to` for the kinds that ask for an answer). The
/// system journals' pool filter (#189) reads it to refuse a node the
/// registry has not admitted yet.
pub(crate) fn message_sender(m: &Message) -> Option<Party> {
    match m {
        Message::Prepare { reply_to, .. } | Message::PreRead { reply_to, .. } => {
            Some(Party::Node(*reply_to))
        }
        Message::Accept { reply_to, .. } => Some(*reply_to),
        Message::Commit { from, .. } => Some(*from),
        Message::Promise { from, .. }
        | Message::Accepted { from, .. }
        | Message::Nack { from, .. }
        | Message::CatchUpRequest { from, .. }
        | Message::CatchUpResponse { from, .. }
        | Message::TrimmedTo { from, .. }
        | Message::Relinquish { from, .. }
        | Message::Heartbeat { from, .. }
        | Message::PreReadAck { from, .. }
        | Message::HeartbeatAck { from, .. } => Some(Party::Node(*from)),
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
        Some(Kind::TrimmedTo(_)) => "trimmed_to",
        Some(Kind::Heartbeat(_)) => "heartbeat",
        Some(Kind::HeartbeatAck(_)) => "heartbeat_ack",
        Some(Kind::Relinquish(_)) => "relinquish",
        Some(Kind::PreRead(_)) => "pre_read",
        Some(Kind::PreReadAck(_)) => "pre_read_ack",
        None => "unknown",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use paros_core::{NodeId, QuorumSystem};

    #[test]
    fn config_hash_distinguishes_membership_and_is_order_independent() {
        let a = AcceptorConfig::new(
            vec![NodeId(0), NodeId(1), NodeId(2)],
            QuorumSystem::Majority,
        );
        let b = AcceptorConfig::new(
            vec![NodeId(2), NodeId(1), NodeId(0)],
            QuorumSystem::Majority,
        );
        let c = AcceptorConfig::new(vec![NodeId(0), NodeId(1)], QuorumSystem::Majority);
        assert_eq!(config_hash(&a), config_hash(&b));
        assert_ne!(config_hash(&a), config_hash(&c));
    }
}
