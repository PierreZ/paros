//! The shared scalar codecs both wire contracts use: ballots, parties, the
//! quorum system, acceptor configurations, commands and keyed maps.

use std::collections::{BTreeMap, BTreeSet};

use paros_core::{
    AcceptorConfig, Ballot, ClientId, ClientSeq, Command, Control, Entry, NodeId, Party, ProxyId,
    QuorumSystem, Slot, Value,
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

pub(super) fn config_to_proto(config: &AcceptorConfig) -> common::AcceptorConfig {
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
pub(super) fn config_from_proto(
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
        Command::User(entry) => internal::command::Kind::User(internal::UserEntry {
            client: entry.client.0,
            seq: entry.seq.0,
            value: entry.value.0.clone(),
        }),
        Command::Control(control) => {
            let kind = match control {
                Control::Truncate { up_to } => {
                    internal::control_command::Kind::Truncate(internal::Truncate { up_to: up_to.0 })
                }
                Control::Noop => internal::control_command::Kind::Noop(internal::Noop {}),
                Control::Snap { at_index } => {
                    internal::control_command::Kind::Snap(internal::Snap {
                        at_index: at_index.0,
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
        internal::command::Kind::User(entry) => Ok(Command::User(Entry {
            client: ClientId(entry.client),
            seq: ClientSeq(entry.seq),
            value: Value(entry.value),
        })),
        internal::command::Kind::Control(control) => {
            let control = match control.kind.ok_or("missing control command kind")? {
                internal::control_command::Kind::Truncate(truncate) => Control::Truncate {
                    up_to: Slot(truncate.up_to),
                },
                internal::control_command::Kind::Noop(_) => Control::Noop,
                internal::control_command::Kind::Snap(snap) => Control::Snap {
                    at_index: Slot(snap.at_index),
                },
            };
            Ok(Command::Control(control))
        }
    }
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
