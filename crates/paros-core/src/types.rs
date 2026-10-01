//! Core domain types for Multi-Paxos. Pure data, no logic.

use core::cmp::Ordering;

/// Stable identity of a node in the cluster.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct NodeId(pub u64);

/// A replicated-log slot index. Multi-Paxos chooses one [`Value`] per slot.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Slot(pub u64);

/// The identity of one **journal** (#184, M6): an independent ordered log
/// with its own ballots, its own chosen prefix and its own store. Every
/// client call names one, and a process serves a static list of them.
///
/// `0` means *unset* and is never served ([`JournalId::is_set`]): a request
/// that names no journal is refused at the wire, never routed to a default.
/// `1..=127` are reserved for system journals
/// ([`JournalId::FIRST_USER`]` - 1` and below); user journals start at
/// [`JournalId::FIRST_USER`]. The core never makes a protocol decision on
/// the id — a [`crate::ColocatedNode`] carries it in its [`crate::Config`]
/// for assertions and tracing only; routing a message to its journal is
/// the driver's envelope.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct JournalId(pub u64);

impl JournalId {
    /// The unset id: refused wherever a journal must be named.
    pub const UNSET: Self = Self(0);
    /// The first id a user journal may take (`1..=127` are system journals).
    pub const FIRST_USER: Self = Self(128);

    /// Whether this id names a journal at all (`0` does not).
    #[must_use]
    pub const fn is_set(self) -> bool {
        self.0 != 0
    }

    /// Whether this id is a user journal's (`>= 128`).
    #[must_use]
    pub const fn is_user(self) -> bool {
        self.0 >= Self::FIRST_USER.0
    }
}

impl Default for JournalId {
    /// The one user journal of a single-journal deployment
    /// ([`JournalId::FIRST_USER`]) — never [`JournalId::UNSET`], so a
    /// defaulted [`crate::Config`] serves a journal a client can name.
    fn default() -> Self {
        Self::FIRST_USER
    }
}

/// The identity of a journal **client**: a writer that may own a journal
/// (#204, the `owner` of a [`Entry`] and of a [`Control::SetLeader`]) or a
/// reader. Opaque to paros; the journal state machine only compares it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ClientId(pub u64);

/// A journal's **writer generation** (#204): which client may write, bumped
/// by one at every successful [`Control::SetLeader`]. `0` is the journal's
/// birth, owned by nobody. Invisible to Paxos — the ballot says which
/// *machine* runs a journal's leader, the generation which *client* may
/// write, and the two never meet.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Generation(pub u64);

/// A **position** in a journal (#204): the dense index of an accepted
/// record, assigned at apply. A batch of `n` records accepted at `seq`
/// occupies `[seq, seq + n)`, in one Paxos slot; a refused write, a `Noop`,
/// a control command and a generation change consume a slot and no `Seq`,
/// so a reader never sees a hole. Slots stay internal.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Seq(pub u64);

/// An opaque value proposed into / chosen for a slot. The core never interprets
/// the bytes; the application owns their meaning.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Value(pub Vec<u8>);

/// A journal **`Write`** as it is decided into one slot (#204): the batch
/// `records`, to be accepted at position `seq` iff `(generation, owner)` is
/// the journal's current writer and `seq` its next position — judged at
/// apply, in slot order, by the journal state machine
/// ([`crate::journal_state::JournalState::apply`]), never at propose time.
/// The records are opaque bytes the core counts and slices (a read may start
/// inside a batch) but never interprets.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Entry {
    /// The writer generation the client wrote under.
    pub generation: Generation,
    /// The client that wrote it.
    pub owner: ClientId,
    /// The position the batch's first record asks for.
    pub seq: Seq,
    /// The records, in order. Accepted or refused whole.
    pub records: Vec<Value>,
}

impl Entry {
    /// How many positions the batch occupies once accepted.
    #[must_use]
    pub fn count(&self) -> u64 {
        self.records.len() as u64
    }
}

/// A paros-interpreted **control command**: journal metadata decided into a
/// log slot by ordinary consensus, rather than a client's records.
///
/// A control command *is* interpreted — by the replica/apply path only, when
/// the slot it occupies enters the contiguous chosen prefix. The
/// acceptor/consensus paths (`Prepare`/`Accept`/`Promise`/catch-up) treat a
/// whole [`Command`] opaquely, exactly as Compartmentalized Paxos treats a
/// `Noop`, so deciding journal metadata does not leak into the vote
/// machinery.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Control {
    /// Truncate the journal (#204, `Truncate(up_to_seq)`): drop every record
    /// below `up_to`. Monotone, clamped to the journal's next position. Every
    /// node applies it when its contiguous walk reaches this slot, and drops
    /// the log slots whose records all lie below the new first position —
    /// forwarded by normal replication + catch-up.
    Truncate {
        /// The first position the client still needs (every record below it
        /// may go).
        up_to: Seq,
    },
    /// Change the journal's writer (#204, `SetLeader(expected_gen,
    /// new_owner)`): a pure compare-and-swap, judged at apply — it succeeds
    /// iff `expected` is the current generation, and then the journal's
    /// generation becomes `expected + 1` and its owner `owner`. No lease and
    /// no clock.
    SetLeader {
        /// The generation the caller believes current.
        expected: Generation,
        /// The client that should own the journal from the next generation.
        owner: ClientId,
    },
    /// A **no-op**: decides the slot without doing anything at apply time.
    ///
    /// The gap filler. A new leader re-proposes every slot its promise quorum
    /// reported accepted, but a slot the quorum reported *nothing* for — one the
    /// old leader accepted alone, below a later slot that did reach the quorum —
    /// would otherwise never be proposed by anyone again, freezing the contiguous
    /// chosen prefix one below it forever (see
    /// [`Replica::chosen_gap`](crate::replica::Replica::chosen_gap)). Deciding a `Noop`
    /// there is safe for the ordinary Phase-1 reason: quorum intersection
    /// guarantees a value already chosen at that slot would have been reported, so
    /// the slot is genuinely free.
    ///
    /// It is an entry like any other — persisted, replicated, truncatable — and
    /// consumes a slot and no position. Applying it advances the applied prefix
    /// and nothing else.
    Noop,
}

/// What a single log slot decides: a client's [`Entry`] (a journal `Write`)
/// or a paros-interpreted [`Control`] command.
///
/// This is the per-slot value the whole protocol carries (in `Accept`,
/// `Promise`, `Commit`, catch-up, and the durable accepted log). Only the
/// replica/apply path distinguishes the two variants; every acceptor/consensus
/// path stores and relays a `Command` without inspecting it.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Command {
    /// A client's `Write` (the core never interprets its records).
    Write(Entry),
    /// A paros-interpreted control command (interpreted only at apply time).
    Control(Control),
}

/// A consensus value that can name itself in one word.
///
/// Phase 2 carries the fingerprint, not the value: an `Accepted` reports
/// *which* value the acceptor took, so a leader never credits an ack for a
/// different value at the same `(slot, ballot)`. That is the only thing the
/// [`crate::proposer::Proposer`] role ever needs to know about a value, which
/// is why it is a trait rather than a `Command`-shaped function call: a
/// deployment over some other value type implements this and reuses the role
/// unchanged.
pub trait Fingerprint {
    /// A stable identity for this value. Two values that compare equal must
    /// fingerprint equal; distinct values should collide only by accident.
    fn fingerprint(&self) -> u64;
}

impl Fingerprint for Command {
    fn fingerprint(&self) -> u64 {
        command_fingerprint(self)
    }
}

/// A stable fingerprint of the complete consensus value identity.
///
/// Unlike application-level value hashes, this includes the command variant,
/// the writer's generation and identity, the position, every record's length
/// and bytes, and control metadata. It
/// is carried by [`crate::Message::Accepted`] so a leader never credits an ack
/// for a different command at the same `(slot, ballot)`.
#[must_use]
pub fn command_fingerprint(command: &Command) -> u64 {
    match command {
        Command::Write(entry) => {
            let hash = fnv1a(FNV_OFFSET, &[0]);
            let hash = fnv1a(hash, &entry.generation.0.to_le_bytes());
            let hash = fnv1a(hash, &entry.owner.0.to_le_bytes());
            let mut hash = fnv1a(hash, &entry.seq.0.to_le_bytes());
            hash = fnv1a(hash, &entry.count().to_le_bytes());
            for record in &entry.records {
                hash = fnv1a(hash, &(record.0.len() as u64).to_le_bytes());
                hash = fnv1a(hash, &record.0);
            }
            hash
        }
        Command::Control(Control::Truncate { up_to }) => {
            let hash = fnv1a(FNV_OFFSET, &[1]);
            fnv1a(hash, &up_to.0.to_le_bytes())
        }
        Command::Control(Control::Noop) => fnv1a(FNV_OFFSET, &[2]),
        Command::Control(Control::SetLeader { expected, owner }) => {
            let hash = fnv1a(FNV_OFFSET, &[3]);
            let hash = fnv1a(hash, &expected.0.to_le_bytes());
            fnv1a(hash, &owner.0.to_le_bytes())
        }
    }
}

/// The FNV-1a offset basis every paros fingerprint folds from.
pub(crate) const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;

/// Folds `bytes` into an FNV-1a `hash`: the one mixing step behind
/// [`command_fingerprint`] and a matchmaker set's decree identity.
pub(crate) fn fnv1a(mut hash: u64, bytes: &[u8]) -> u64 {
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    for &byte in bytes {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}

impl Command {
    /// The client [`Entry`] if this is a [`Command::Write`], else `None`.
    #[must_use]
    pub fn write(&self) -> Option<&Entry> {
        match self {
            Command::Write(entry) => Some(entry),
            Command::Control(_) => None,
        }
    }
}

/// A Paxos ballot (a.k.a. proposal / round number), forming a **total order**.
///
/// Ordering is keyed on `(round, node)`: a strictly higher `round` always wins;
/// equal rounds are broken deterministically by [`NodeId`]. This total order is
/// the backbone of Paxos safety — every two ballots are comparable, so an
/// acceptor can always decide whether an incoming ballot is `>=` the one it has
/// promised.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Ballot {
    /// The round number. Higher rounds dominate.
    pub round: u64,
    /// The proposer's identity, used only to break ties between equal rounds.
    pub node: NodeId,
}

impl Ballot {
    /// The smallest possible ballot (round 0 from node 0). Doubles as the
    /// "nothing promised / nothing accepted yet" sentinel in [`crate::HardState`].
    #[must_use]
    pub const fn zero() -> Self {
        Self {
            round: 0,
            node: NodeId(0),
        }
    }
}

impl Ord for Ballot {
    fn cmp(&self, other: &Self) -> Ordering {
        // Higher round wins; ties broken by NodeId. Written out (rather than
        // derived) so the total-order contract is local to this impl and
        // survives any future field reordering.
        self.round
            .cmp(&other.round)
            .then_with(|| self.node.cmp(&other.node))
    }
}

impl PartialOrd for Ballot {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
mod tests {
    use super::{Ballot, NodeId};

    fn ballot(round: u64, node: u64) -> Ballot {
        Ballot {
            round,
            node: NodeId(node),
        }
    }

    #[test]
    fn higher_round_dominates_regardless_of_node() {
        assert!(ballot(2, 0) > ballot(1, 9));
    }

    #[test]
    fn equal_round_is_broken_by_node_id() {
        assert!(ballot(1, 2) > ballot(1, 1));
        assert_eq!(ballot(1, 1), ballot(1, 1));
    }

    #[test]
    fn zero_is_the_minimum_and_equals_default() {
        assert!(Ballot::zero() < ballot(0, 1));
        assert_eq!(Ballot::zero(), Ballot::default());
    }
}
