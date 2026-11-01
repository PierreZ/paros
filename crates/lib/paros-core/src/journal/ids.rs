//! The identifiers of a journal: tenant, journal, leader uuid, position.

/// The identity of one **tenant** (#226, #235): the owner of a set of
/// journals, and the first half of the identifier every peer and client message
/// carries ([`JournalIdentifier`]).
///
/// Random, drawn by whoever creates the tenant and checked where it is
/// recorded — the fleet directory for every tenant, the system ones included
/// (`docs/architecture/03-8-identifiers-checkpoints-recovery-security.md` §3.8). **No id is fixed**: there is no well-known
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
const _: () = assert!(!LeaderUuid::UNSET.is_set());
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

/// A single-writer journal's **leader uuid** (#241, `docs/architecture/02-data-plane.md`
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
    /// No leader: what a multi-writer call carries (#241), and what never
    /// leads a single-writer journal.
    pub const UNSET: Self = Self(0);

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

#[cfg(test)]
mod tests;
