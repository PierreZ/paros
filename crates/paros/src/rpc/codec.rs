//! The shared scalar codecs both wire contracts use: ballots, parties, the
//! quorum system, acceptor configurations, commands and keyed maps.

use std::collections::{BTreeMap, BTreeSet};

use paros_core::{
    AcceptorConfig, Ballot, Command, Control, Entry, JournalState, JournalView, LeaderUuid, NodeId,
    Party, ProxyId, QuorumSystem, Seq, Value, WriterMode,
};

use super::{InspectReply, Reconfigure, common, internal};

pub(super) fn ballot_to_proto(ballot: Ballot) -> common::Ballot {
    common::Ballot {
        round: ballot.round,
        node: ballot.node.0,
    }
}

/// A ballot the wire carries unconditionally (a oneof arm, a repeated or an
/// already-unwrapped field) decodes infallibly.
impl From<common::Ballot> for Ballot {
    fn from(ballot: common::Ballot) -> Self {
        Ballot {
            round: ballot.round,
            node: NodeId(ballot.node),
        }
    }
}

pub(super) fn ballot_from_proto(ballot: Option<common::Ballot>) -> Result<Ballot, &'static str> {
    Ok(ballot.ok_or("missing ballot")?.into())
}

/// The wire form of a [`Party`] (#142): the node field, zero for a proxy, and
/// the optional proxy field — absent for a node, so a colocated round's
/// `Accept` and `Commit` are byte-for-byte the plain deployment's.
pub(super) fn party_to_proto(party: Party) -> (u64, Option<u64>) {
    match party {
        Party::Node(node) => (node.0, None),
        Party::Proxy(proxy) => (0, Some(proxy.0)),
    }
}

/// Decode a [`Party`] from its two wire fields: the proxy field names a proxy
/// leader when set, else the node field names a node.
pub(super) fn party_from_proto(node: u64, proxy: Option<u64>) -> Party {
    match proxy {
        Some(proxy) => Party::Proxy(ProxyId(proxy)),
        None => Party::Node(NodeId(node)),
    }
}

/// The wire form of a quorum system: the discriminant plus the scalars its
/// variant carries — `(phase1_quorum, phase2_quorum)` for a flexible split,
/// `(rows, cols)` for a grid, all zero under a majority so a plain
/// deployment's encoding is unchanged (proto3 omits default-valued fields).
/// Shared by every message that carries a configuration or names one
/// (`AcceptorConfig` inside every protocol message, the `Reconfigure`
/// request, the `Inspect` reply), so the encodings cannot drift. Public so
/// an operator or a client composing a `Reconfigure` fills the same fields
/// the library reads.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WireQuorumSystem {
    /// The `paros.common.v1.QuorumSystem` discriminant.
    pub quorum_system: i32,
    /// `q1` under a flexible split, else zero.
    pub phase1_quorum: u64,
    /// `q2` under a flexible split, else zero.
    pub phase2_quorum: u64,
    /// The grid's rows, else zero.
    pub rows: u64,
    /// The grid's columns, else zero.
    pub cols: u64,
}

impl WireQuorumSystem {
    /// The five wire fields in declaration order — `(quorum_system,
    /// phase1_quorum, phase2_quorum, rows, cols)` — for spreading into a
    /// message that carries them inline.
    #[must_use]
    pub fn into_parts(self) -> (i32, u64, u64, u64, u64) {
        (
            self.quorum_system,
            self.phase1_quorum,
            self.phase2_quorum,
            self.rows,
            self.cols,
        )
    }
}

/// Every message that carries the five quorum-system fields inline reads
/// them the same way.
macro_rules! wire_quorum_system_from {
    ($($source:ty),+ $(,)?) => {$(
        impl From<&$source> for WireQuorumSystem {
            fn from(source: &$source) -> Self {
                Self {
                    quorum_system: source.quorum_system,
                    phase1_quorum: source.phase1_quorum,
                    phase2_quorum: source.phase2_quorum,
                    rows: source.rows,
                    cols: source.cols,
                }
            }
        }
    )+};
}

wire_quorum_system_from!(common::AcceptorConfig, Reconfigure, InspectReply);

/// Encode a quorum system for the wire.
#[must_use]
pub fn quorum_system_to_proto(quorum_system: QuorumSystem) -> WireQuorumSystem {
    let size = |n: usize| u64::try_from(n).unwrap_or(u64::MAX);
    let (kind, phase1_quorum, phase2_quorum, rows, cols) = match quorum_system {
        QuorumSystem::Majority => (common::QuorumSystem::Majority, 0, 0, 0, 0),
        QuorumSystem::Flexible { q1, q2 } => {
            (common::QuorumSystem::Flexible, size(q1), size(q2), 0, 0)
        }
        QuorumSystem::Grid { rows, cols } => {
            (common::QuorumSystem::Grid, 0, 0, size(rows), size(cols))
        }
    };
    WireQuorumSystem {
        quorum_system: kind.into(),
        phase1_quorum,
        phase2_quorum,
        rows,
        cols,
    }
}

/// Decode a wire quorum system. Only the discriminant and the sizes are
/// checked here; whether the membership admits it is the caller's question
/// (`QuorumSystem::admits`), asked once the membership is known.
///
/// # Errors
///
/// An unknown discriminant, or a size that does not fit a `usize`.
pub fn quorum_system_from_proto(wire: &WireQuorumSystem) -> Result<QuorumSystem, &'static str> {
    match common::QuorumSystem::try_from(wire.quorum_system) {
        Ok(common::QuorumSystem::Majority) => Ok(QuorumSystem::Majority),
        Ok(common::QuorumSystem::Flexible) => Ok(QuorumSystem::Flexible {
            q1: usize::try_from(wire.phase1_quorum).map_err(|_| "phase-1 quorum out of range")?,
            q2: usize::try_from(wire.phase2_quorum).map_err(|_| "phase-2 quorum out of range")?,
        }),
        Ok(common::QuorumSystem::Grid) => Ok(QuorumSystem::Grid {
            rows: usize::try_from(wire.rows).map_err(|_| "grid rows out of range")?,
            cols: usize::try_from(wire.cols).map_err(|_| "grid cols out of range")?,
        }),
        Err(_) => Err("unknown quorum system"),
    }
}

pub(crate) fn config_to_proto(config: &AcceptorConfig) -> common::AcceptorConfig {
    let (quorum_system, phase1_quorum, phase2_quorum, rows, cols) =
        quorum_system_to_proto(config.quorum_system()).into_parts();
    common::AcceptorConfig {
        members: config.members().iter().map(|n| n.0).collect(),
        quorum_system,
        phase1_quorum,
        phase2_quorum,
        rows,
        cols,
    }
}

/// Decode a wire configuration, **refusing** what `AcceptorConfig::new`
/// would panic on: the constructor asserts well-formedness because a
/// malformed configuration is a programmer error inside the process, but on
/// the wire it is external input, so it is validated here first
/// (`QuorumSystem::admits` over the deduplicated membership) and answered
/// with an error, never a crash.
pub(crate) fn config_from_proto(
    config: Option<common::AcceptorConfig>,
) -> Result<Option<AcceptorConfig>, &'static str> {
    let Some(config) = config else {
        return Ok(None);
    };
    let quorum_system = quorum_system_from_proto(&WireQuorumSystem::from(&config))?;
    if config.members.is_empty() {
        return Err("empty acceptor configuration");
    }
    let members: BTreeSet<NodeId> = config.members.into_iter().map(NodeId).collect();
    if !quorum_system.admits(members.len()) {
        return Err("acceptor configuration does not admit its quorum system");
    }
    Ok(Some(AcceptorConfig::new(
        members.into_iter().collect(),
        quorum_system,
    )))
}

pub(super) fn command_to_proto(command: &Command) -> internal::Command {
    let kind = match command {
        Command::Write(entry) => internal::command::Kind::Write(internal::WriteEntry {
            leader: Some(leader_uuid_to_proto(entry.leader)),
            seq: entry.seq.0,
            records: entry.records.iter().map(|r| r.0.clone()).collect(),
        }),
        Command::Control(control) => {
            let kind = match control {
                Control::Truncate { leader, up_to } => {
                    internal::control_command::Kind::Truncate(internal::Truncate {
                        up_to: up_to.0,
                        leader: Some(leader_uuid_to_proto(*leader)),
                    })
                }
                Control::Noop => internal::control_command::Kind::Noop(internal::Noop {}),
                Control::SetLeader { new, old } => {
                    internal::control_command::Kind::SetLeader(internal::SetLeader {
                        new: Some(leader_uuid_to_proto(*new)),
                        old: old.map(leader_uuid_to_proto),
                    })
                }
            };
            internal::command::Kind::Control(internal::ControlCommand { kind: Some(kind) })
        }
    };
    internal::Command { kind: Some(kind) }
}

pub(super) fn command_from_proto(
    command: Option<internal::Command>,
) -> Result<Command, &'static str> {
    match command
        .ok_or("missing command")?
        .kind
        .ok_or("missing command kind")?
    {
        internal::command::Kind::Write(entry) => Ok(Command::Write(Entry {
            leader: leader_uuid_from_proto(entry.leader),
            seq: Seq(entry.seq),
            records: entry.records.into_iter().map(Value).collect(),
        })),
        internal::command::Kind::Control(control) => {
            let control = match control.kind.ok_or("missing control command kind")? {
                internal::control_command::Kind::Truncate(truncate) => Control::Truncate {
                    leader: leader_uuid_from_proto(truncate.leader),
                    up_to: Seq(truncate.up_to),
                },
                internal::control_command::Kind::Noop(_) => Control::Noop,
                internal::control_command::Kind::SetLeader(set) => Control::SetLeader {
                    new: leader_uuid_from_proto(set.new),
                    old: leader_from_proto(set.old),
                },
            };
            Ok(Command::Control(control))
        }
    }
}

/// The wire form of a leader uuid (#241): its high and low halves.
#[must_use]
pub fn leader_uuid_to_proto(uuid: LeaderUuid) -> common::LeaderUuid {
    let half = |bits: u128| u64::try_from(bits & u128::from(u64::MAX)).unwrap_or_default();
    common::LeaderUuid {
        hi: half(uuid.0 >> 64),
        lo: half(uuid.0),
    }
}

/// Decode a leader uuid; a missing one is the unset uuid, which never leads
/// (a fence naming it is refused at apply, never at decode).
#[must_use]
pub fn leader_uuid_from_proto(uuid: Option<common::LeaderUuid>) -> LeaderUuid {
    uuid.map_or(LeaderUuid(0), |uuid| {
        LeaderUuid((u128::from(uuid.hi) << 64) | u128::from(uuid.lo))
    })
}

/// Decode an optional leader: absent or all zero is none.
#[must_use]
pub fn leader_from_proto(uuid: Option<common::LeaderUuid>) -> Option<LeaderUuid> {
    Some(leader_uuid_from_proto(uuid)).filter(|uuid| uuid.is_set())
}

/// The wire form of a journal's control state (#204, #241) between nodes: a
/// `TrimmedTo`'s sealed state, a heartbeat's fold. A client is answered a
/// [`journal_view_to_proto`], never the term.
#[must_use]
pub fn journal_state_to_proto(state: JournalState) -> common::JournalState {
    common::JournalState {
        leader: state.leader.map(leader_uuid_to_proto),
        term: state.term,
        next_seq: state.next_seq.0,
        first_seq: state.first_seq.0,
    }
}

/// Decode a journal state off the wire. A missing one is the journal's
/// birth; an ill-formed one (a leader without a term, a first position past
/// the next) is refused.
///
/// # Errors
///
/// A state that breaks its own ordering.
pub fn journal_state_from_proto(
    state: Option<common::JournalState>,
) -> Result<JournalState, &'static str> {
    let Some(state) = state else {
        return Ok(JournalState::default());
    };
    let decoded = JournalState {
        leader: leader_from_proto(state.leader),
        term: state.term,
        next_seq: Seq(state.next_seq),
        first_seq: Seq(state.first_seq),
    };
    if decoded.first_seq > decoded.next_seq || decoded.leader.is_some() != (decoded.term > 0) {
        return Err("ill-formed journal state");
    }
    Ok(decoded)
}

/// A journal's writer mode on the wire (#241).
#[must_use]
pub fn writer_mode_to_proto(mode: WriterMode) -> common::WriterMode {
    match mode {
        WriterMode::Single => common::WriterMode::Single,
        WriterMode::Multi => common::WriterMode::Multi,
    }
}

/// A journal's writer mode off the wire (#241). An unknown value is
/// refused, never read as single-writer.
///
/// # Errors
///
/// The value names no writer mode.
pub fn writer_mode_from_proto(mode: i32) -> Result<WriterMode, &'static str> {
    match common::WriterMode::try_from(mode) {
        Ok(common::WriterMode::Single) => Ok(WriterMode::Single),
        Ok(common::WriterMode::Multi) => Ok(WriterMode::Multi),
        Err(_) => Err("an unknown writer mode"),
    }
}

/// The wire form of what a client learns of a journal (#241): every verdict
/// names the view it was judged against.
#[must_use]
pub fn journal_view_to_proto(view: JournalView) -> common::JournalView {
    common::JournalView {
        leader: view.leader.map(leader_uuid_to_proto),
        next_seq: view.next_seq.0,
        first_seq: view.first_seq.0,
    }
}

/// Decode a journal view off the wire. A missing one is the journal's birth;
/// a first position past the next is refused.
///
/// # Errors
///
/// A view that breaks its own ordering.
pub fn journal_view_from_proto(
    view: Option<common::JournalView>,
) -> Result<JournalView, &'static str> {
    let Some(view) = view else {
        return Ok(JournalView::default());
    };
    let decoded = JournalView {
        leader: leader_from_proto(view.leader),
        next_seq: Seq(view.next_seq),
        first_seq: Seq(view.first_seq),
    };
    if decoded.first_seq > decoded.next_seq {
        return Err("ill-formed journal view");
    }
    Ok(decoded)
}

/// Collect decoded `(key, value)` entries into a map, refusing the first
/// malformed entry and any key that repeats (`duplicate` names that error).
pub(super) fn unique_map<K: Ord, V>(
    entries: impl IntoIterator<Item = Result<(K, V), &'static str>>,
    duplicate: &'static str,
) -> Result<BTreeMap<K, V>, &'static str> {
    let mut decoded = BTreeMap::new();
    for entry in entries {
        let (key, value) = entry?;
        if decoded.insert(key, value).is_some() {
            return Err(duplicate);
        }
    }
    Ok(decoded)
}
