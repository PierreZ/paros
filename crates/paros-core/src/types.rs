//! Core domain types for Multi-Paxos. Pure data, no logic.

use core::cmp::Ordering;

/// Stable identity of a node in the cluster. An id has no default: it is
/// minted (random, at format, in a deployment) or named, never assumed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct NodeId(pub u64);

/// A replicated-log slot index. Multi-Paxos chooses one [`Value`] per slot.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Slot(pub u64);

/// The identity of one **tenant** (#226, #235): the owner of a set of
/// journals, and the first half of the identifier every peer and client message
/// carries ([`JournalIdentifier`]).
///
/// Random, drawn by whoever creates the tenant and checked where it is
/// recorded — the fleet directory for every tenant, the system ones included
/// (`docs/architecture.md` §3.8). **No id is fixed**: there is no well-known
/// tenant and no reserved range, and `0` means *unset* and is never
/// served. An id has **no default**: it is drawn or read, never assumed. The core never makes a protocol decision on the id.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct TenantId(pub u64);

impl TenantId {
    /// The unset id: refused wherever a tenant must be named.
    pub const UNSET: Self = Self(0);

    /// Whether this id names a tenant at all (`0` does not).
    #[must_use]
    pub const fn is_set(self) -> bool {
        self.0 != 0
    }
}

/// The identity of one **journal** inside its tenant (#184, #235): an
/// independent ordered log with its own ballots, its own chosen prefix and
/// its own store. A journal is named by its [`JournalIdentifier`], the pair
/// `(TenantId, JournalId)`: a journal id is unique only within its tenant.
///
/// Random, drawn by the journal's creator and checked at apply by the
/// tenant's control journal — whose own id is random too, recorded where
/// the tenant is (§3.8). **No id is fixed**, and none has a default: `0`
/// means *unset* and is never served ([`JournalId::is_set`]); a request that
/// names no journal is refused at the wire. The core never makes a
/// protocol decision on the id — a [`crate::ColocatedNode`] carries it in
/// its [`crate::Config`] for assertions and tracing only; routing a message
/// to its journal is the driver's envelope.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct JournalId(pub u64);

impl JournalId {
    /// The unset id: refused wherever a journal must be named.
    pub const UNSET: Self = Self(0);

    /// Whether this id names a journal at all (`0` does not).
    #[must_use]
    pub const fn is_set(self) -> bool {
        self.0 != 0
    }
}

/// The **identifier** of every peer and client message (#226, #235): the tenant
/// and the journal inside it. Uniqueness is only ever needed where it can be
/// checked — a tenant id by the fleet directory, a journal id by its tenant's control
/// journal — so a journal is only ever named by the pair. An identifier has no
/// default: it is always drawn or read, never assumed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct JournalIdentifier {
    /// The tenant that owns the journal.
    pub tenant: TenantId,
    /// The journal, unique within its tenant.
    pub journal: JournalId,
}

impl JournalIdentifier {
    /// The unset identifier: both halves `0`. Never served, and never read as a
    /// default: a request that names it is refused (#243).
    pub const UNSET: Self = Self::new(TenantId::UNSET, JournalId::UNSET);

    /// The journal `journal` of tenant `tenant`.
    #[must_use]
    pub const fn new(tenant: TenantId, journal: JournalId) -> Self {
        Self { tenant, journal }
    }

    /// Whether both halves are set: an identifier with an unset half names
    /// nothing and is refused.
    #[must_use]
    pub const fn is_set(self) -> bool {
        self.tenant.is_set() && self.journal.is_set()
    }
}

// No id has a default and `0` is the only value with a meaning: unset.
const _: () = assert!(!TenantId::UNSET.is_set());
const _: () = assert!(!JournalId::UNSET.is_set());
const _: () = assert!(!JournalIdentifier::UNSET.is_set());
const _: () = assert!(JournalIdentifier::new(TenantId(1), JournalId(1)).is_set());

impl core::fmt::Display for JournalIdentifier {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{}/{}", self.tenant.0, self.journal.0)
    }
}

impl core::str::FromStr for JournalIdentifier {
    type Err = &'static str;

    /// `<tenant>/<journal>`, the form [`JournalIdentifier`]'s `Display` renders.
    /// Both halves must be set: there is no default tenant.
    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let (tenant, journal) = text
            .split_once('/')
            .ok_or("a journal identifier is <tenant>/<journal>")?;
        let tenant = TenantId(tenant.parse().map_err(|_| "a tenant id is a u64")?);
        let journal = JournalId(journal.parse().map_err(|_| "a journal id is a u64")?);
        let identifier = Self::new(tenant, journal);
        if identifier.is_set() {
            Ok(identifier)
        } else {
            Err("a journal identifier names a tenant and a journal, both non-zero")
        }
    }
}

/// A single-writer journal's **leader uuid** (#241, `docs/architecture.md`
/// §2.3): the one fence a `Write` and a `Truncate` are judged against. A
/// 128-bit random value the leader draws for one leadership term, never per
/// process, so a process that wins again fences its own older in-flight
/// writes. Not a secret. Like every id it has no default: `0` is unset and
/// never a leader (§3.8). Invisible to Paxos — the ballot says which
/// *machine* runs a journal's consensus leader, the leader uuid which
/// *client* may write, and the two never meet.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct LeaderUuid(pub u128);

impl LeaderUuid {
    /// Whether this names a leader: `0` is unset.
    #[must_use]
    pub const fn is_set(self) -> bool {
        self.0 != 0
    }
}

impl core::fmt::Display for LeaderUuid {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{:032x}", self.0)
    }
}

/// A **position** in a journal (#204): the dense index of an accepted
/// record, assigned at apply. A batch of `n` records accepted at `seq`
/// occupies `[seq, seq + n)`, in one Paxos slot; a refused write, a `Noop`,
/// a control command and a leadership change consume a slot and no `Seq`,
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
/// `records`, to be accepted at position `seq` iff `leader` is the
/// journal's current leader uuid and `seq` its next position — judged at
/// apply, in slot order, by the journal state machine
/// ([`crate::journal_state::JournalState::apply`]). That apply-time
/// judgement is the safety rule: a propose-time refusal from the leader's
/// own fold is only an optimisation (`docs/architecture.md` §2.2), and the
/// apply decides every slot whatever was or was not checked before it.
/// The records are opaque bytes the core counts and slices (a read may start
/// inside a batch) but never interprets.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Entry {
    /// The leader uuid the client wrote under.
    pub leader: LeaderUuid,
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
    /// Truncate the journal (#228, `Truncate(leader_uuid, up_to_seq)`): drop
    /// every record below `up_to`. Fenced like a `Write`: it applies iff
    /// `leader` is the journal's current leader uuid, judged at apply
    /// in slot order, and is otherwise refused in place. Monotone, clamped to
    /// the journal's next position. Every node applies an accepted one when
    /// its contiguous walk reaches this slot, and drops the log slots whose
    /// records all lie below the new first position — forwarded by normal
    /// replication + catch-up.
    Truncate {
        /// The leader uuid the caller truncates under.
        leader: LeaderUuid,
        /// The first position the client still needs (every record below it
        /// may go).
        up_to: Seq,
    },
    /// Change the journal's leader (#241, `SetLeader(new_uuid, old_uuid)`):
    /// a pure compare-and-set, judged at apply — it succeeds iff `old` is the
    /// current leader uuid (`None` on a journal that never had one) and `new`
    /// is set and not the current leader, and then `new` leads from the next
    /// term. A uuid that led before wins again: the journal trusts its
    /// clients to draw fresh ones (`docs/architecture.md` §2.3). No lease and
    /// no clock.
    SetLeader {
        /// The leader uuid that should lead from the next term.
        new: LeaderUuid,
        /// The leader uuid the caller believes current, `None` for none.
        old: Option<LeaderUuid>,
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
/// the leader uuid, the position, every record's length
/// and bytes, and control metadata. It
/// is carried by [`crate::Message::Accepted`] so a leader never credits an ack
/// for a different command at the same `(slot, ballot)`.
#[must_use]
pub fn command_fingerprint(command: &Command) -> u64 {
    match command {
        Command::Write(entry) => {
            let hash = fnv1a(FNV_OFFSET, &[0]);
            let hash = fnv1a(hash, &entry.leader.0.to_le_bytes());
            let mut hash = fnv1a(hash, &entry.seq.0.to_le_bytes());
            hash = fnv1a(hash, &entry.count().to_le_bytes());
            for record in &entry.records {
                hash = fnv1a(hash, &(record.0.len() as u64).to_le_bytes());
                hash = fnv1a(hash, &record.0);
            }
            hash
        }
        Command::Control(Control::Truncate { leader, up_to }) => {
            let hash = fnv1a(FNV_OFFSET, &[1]);
            let hash = fnv1a(hash, &leader.0.to_le_bytes());
            fnv1a(hash, &up_to.0.to_le_bytes())
        }
        Command::Control(Control::Noop) => fnv1a(FNV_OFFSET, &[2]),
        Command::Control(Control::SetLeader { new, old }) => {
            let hash = fnv1a(FNV_OFFSET, &[3]);
            let hash = fnv1a(hash, &new.0.to_le_bytes());
            // `None` folds as the unset uuid, which no `new` can be.
            fnv1a(hash, &old.map_or(0, |old| old.0).to_le_bytes())
        }
    }
}

/// The FNV-1a offset basis every paros fingerprint folds from.
pub(crate) const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;

// A zero basis would fingerprint every all-zero prefix to zero.
const _: () = assert!(FNV_OFFSET != 0);

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
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
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

// The sentinel is the order's minimum: round zero, node zero.
const _: () = assert!(Ballot::zero().round == 0);
const _: () = assert!(Ballot::zero().node.0 == 0);

impl Default for Ballot {
    /// [`Ballot::zero`]: the smallest ballot, the sentinel of nothing
    /// promised — an order's minimum, not an identity.
    fn default() -> Self {
        Self::zero()
    }
}

impl Ord for Ballot {
    fn cmp(&self, other: &Self) -> Ordering {
        // Higher round wins; ties broken by NodeId. Written out (rather than
        // derived) so the total-order contract is local to this impl and
        // survives any future field reordering.
        let order = self
            .round
            .cmp(&other.round)
            .then_with(|| self.node.cmp(&other.node));
        // The total order agrees with equality: two ballots tie exactly when
        // they are the same ballot.
        assert!(
            (order == Ordering::Equal) == (self == other),
            "ballot order agrees with ballot equality"
        );
        order
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
