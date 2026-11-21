use std::collections::BTreeSet;

#[cfg(doc)]
use super::MatchmakerSet;
use super::quorum_system::counted;
use super::{FailureDomain, MAX_ROLE_COUNT, QuorumSystem, ReplicaId, ShapeError};
use crate::types::{NodeId, Slot};

/// An acceptor configuration as registered with a matchmaker: a membership
/// plus the quorum system in force over it — [`crate::Config`] minus the
/// per-node `id`. The core never interprets it beyond storing and reporting it;
/// the leader-side matchmaking phase is what runs Phase 1 against it.
///
/// Generic over the **acceptor identity**, defaulting to [`NodeId`]: the
/// acceptor pool of a paros cluster is named by node ids, and the one other
/// deployment in the core — the matchmaker-set handover's single decree,
/// whose acceptors are the matchmakers of `M_g` — is an
/// `AcceptorConfig<MatchmakerId>`. Nothing here depends on what an identity
/// *is*, only that it sorts.
///
/// **Every field is private and [`AcceptorConfig::new`] (or its labeled
/// form, [`AcceptorConfig::zoned`]) is the only way to
/// build one**, deserialisation included (see `SerdeAcceptorConfig`). The
/// membership is a sorted, deduplicated [`Vec`] that
/// [`AcceptorConfig::contains`] and every quorum predicate binary-search:
/// an unsorted or duplicated vector would not fail, it would make a quorum
/// tally *silently miscount*, which is the one failure mode a consensus
/// membership must not have. Only `new` normalizes, so only `new` may
/// construct.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(
    feature = "serde",
    serde(
        try_from = "SerdeAcceptorConfig<Id>",
        bound(
            serialize = "Id: serde::Serialize",
            deserialize = "Id: Copy + Ord + serde::Deserialize<'de>"
        )
    )
)]
pub struct AcceptorConfig<Id = NodeId> {
    /// The full membership, sorted and deduplicated (a [`Vec`] keeps iteration
    /// deterministic without a map).
    members: Vec<Id>,
    /// The quorum system election and decide consult over `members`.
    quorum_system: QuorumSystem,
    /// Each member's failure domain, parallel to `members` (#215 (zones)):
    /// empty is the plain arm, an unlabeled configuration. No tally reads
    /// it; [`AcceptorConfig::zone_rule`] does.
    domains: Vec<FailureDomain>,
    /// How many **proxy leaders** this configuration's leader delegates
    /// Phase 2 to (#142, Compartmentalized Paxos §3.1): `ProxyId(0..proxies)`.
    /// Zero runs every Phase 2 on the leader, the plain arm. Bound to the
    /// ballot like the membership (#754 (counts off the format marker)), so
    /// a change of the count is a round change. An acceptor reads it only
    /// for wire checks: what is chosen never depends on it.
    proxies: usize,
    /// How many **replicas** learn this configuration's log beside its
    /// acceptors (#144, Compartmentalized Paxos §3.3):
    /// `ReplicaId(0..replicas)`. Zero is the plain arm. Bound to the ballot
    /// like `proxies` (#754).
    replicas: usize,
}

/// The wire shape [`AcceptorConfig`] deserialises through, so a serialized
/// configuration is normalized by [`AcceptorConfig::new`] exactly like a
/// constructed one and no path can produce an unsorted membership.
#[cfg(feature = "serde")]
#[derive(serde::Deserialize)]
struct SerdeAcceptorConfig<Id> {
    members: Vec<Id>,
    quorum_system: QuorumSystem,
    #[serde(default)]
    domains: Vec<FailureDomain>,
    #[serde(default)]
    proxies: usize,
    #[serde(default)]
    replicas: usize,
}

#[cfg(feature = "serde")]
impl<Id: Copy + Ord> TryFrom<SerdeAcceptorConfig<Id>> for AcceptorConfig<Id> {
    type Error = ShapeError;

    /// A configuration off the wire is an input: a malformed one is refused
    /// with a decode error, never a panic.
    fn try_from(wire: SerdeAcceptorConfig<Id>) -> Result<Self, ShapeError> {
        let config = if wire.domains.is_empty() {
            Self::try_new(wire.members, wire.quorum_system)?
        } else {
            if wire.domains.len() != wire.members.len() {
                return Err(ShapeError::UnlabeledMember);
            }
            Self::try_zoned(
                wire.members.into_iter().zip(wire.domains).collect(),
                wire.quorum_system,
            )?
        };
        config.try_with_counts(wire.proxies, wire.replicas)
    }
}

impl<Id: Copy + Ord> AcceptorConfig<Id> {
    /// A configuration over `members` (sorted and deduplicated here) under
    /// `quorum_system`: [`AcceptorConfig::try_new`] for a value the program
    /// builds itself.
    ///
    /// # Panics
    ///
    /// If [`AcceptorConfig::try_new`] refuses it: a membership with no
    /// acceptor, or a quorum system the membership does not admit
    /// ([`AcceptorConfig::is_well_formed`]). The panic text is the
    /// [`ShapeError`]'s rule.
    #[must_use]
    pub fn new(members: Vec<Id>, quorum_system: QuorumSystem) -> Self {
        Self::try_new(members, quorum_system).unwrap_or_else(|e| panic!("{e}"))
    }

    /// A configuration over `members` (sorted and deduplicated here) under
    /// `quorum_system`, or the rule it breaks. The form for an input: a
    /// configuration from the wire or from an operator.
    ///
    /// # Errors
    ///
    /// [`ShapeError::NoMember`] if `members` is empty: no quorum can form.
    /// [`ShapeError::QuorumNotAdmitted`] if the quorum system does not admit
    /// the membership ([`QuorumSystem::admits`]).
    ///
    /// # Panics
    ///
    /// If a built configuration breaks a postcondition (a programmer error).
    pub fn try_new(mut members: Vec<Id>, quorum_system: QuorumSystem) -> Result<Self, ShapeError> {
        members.sort_unstable();
        members.dedup();
        Self::build(members, Vec::new(), quorum_system)
    }

    /// A **labeled** configuration (#215 (zones)): each member with its
    /// failure domain, sorted by member and deduplicated here, under
    /// `quorum_system`. The labels are bound to the configuration like the
    /// membership: two configurations that differ only in a label are two
    /// configurations. [`AcceptorConfig::try_zoned`] for a value the program
    /// builds itself.
    ///
    /// # Panics
    ///
    /// If [`AcceptorConfig::try_zoned`] refuses it. The panic text is the
    /// [`ShapeError`]'s rule.
    #[must_use]
    pub fn zoned(members: Vec<(Id, FailureDomain)>, quorum_system: QuorumSystem) -> Self {
        Self::try_zoned(members, quorum_system).unwrap_or_else(|e| panic!("{e}"))
    }

    /// The labeled form of [`AcceptorConfig::try_new`].
    ///
    /// # Errors
    ///
    /// As [`AcceptorConfig::try_new`]; also [`ShapeError::TwoDomains`] if one
    /// member is named twice with two domains, and
    /// [`ShapeError::DomainTooLong`] if a domain is not well formed
    /// ([`FailureDomain::is_well_formed`]).
    ///
    /// # Panics
    ///
    /// If a built configuration breaks a postcondition (a programmer error).
    pub fn try_zoned(
        mut members: Vec<(Id, FailureDomain)>,
        quorum_system: QuorumSystem,
    ) -> Result<Self, ShapeError> {
        members.sort_by_key(|a| a.0);
        if members
            .windows(2)
            .any(|pair| pair[0].0 == pair[1].0 && pair[0].1 != pair[1].1)
        {
            return Err(ShapeError::TwoDomains);
        }
        members.dedup_by(|a, b| a.0 == b.0);
        let (members, domains): (Vec<Id>, Vec<FailureDomain>) = members.into_iter().unzip();
        if !domains.iter().all(FailureDomain::is_well_formed) {
            return Err(ShapeError::DomainTooLong);
        }
        let config = Self::build(members, domains, quorum_system)?;
        assert!(config.is_zoned(), "a labeled configuration stays labeled");
        Ok(config)
    }

    /// The one place a configuration is assembled, from a sorted and
    /// deduplicated membership and its labels (empty, or one per member).
    fn build(
        members: Vec<Id>,
        domains: Vec<FailureDomain>,
        quorum_system: QuorumSystem,
    ) -> Result<Self, ShapeError> {
        if members.is_empty() {
            return Err(ShapeError::NoMember);
        }
        assert!(
            domains.is_empty() || domains.len() == members.len(),
            "a configuration labels every member or none"
        );
        let config = Self {
            members,
            quorum_system,
            domains,
            proxies: 0,
            replicas: 0,
        };
        if !config.is_well_formed() {
            return Err(ShapeError::QuorumNotAdmitted);
        }
        // Paxos safety, restated once at the only constructor: every Phase-1
        // quorum meets every Phase-2 quorum, and both fit the membership.
        let n = config.members.len();
        assert!(
            config.quorum_system.cross_intersects(n),
            "an acceptor configuration's quorums cross-intersect"
        );
        assert!(
            config.quorum_system.phase1_quorum_size(n) <= n,
            "a Phase-1 quorum fits the membership"
        );
        assert!(
            config.quorum_system.phase2_quorum_size(n) <= n,
            "a Phase-2 quorum fits the membership"
        );
        Ok(config)
    }

    /// Whether this configuration can be run at all: at least one acceptor, a
    /// membership that is sorted and deduplicated, and a quorum system the
    /// membership admits ([`QuorumSystem::admits`]: each phase's quorum
    /// between one acceptor and the whole membership, and the two phases'
    /// quorums always intersecting, [`QuorumSystem::cross_intersects`] —
    /// `2q > n` for a majority, and for a grid a layout that tiles the membership, `rows * cols == n`). This
    /// is the invariant
    /// [`AcceptorConfig::new`] establishes — asserted there, once, since it
    /// is the only constructor (deserialisation included) — and every quorum
    /// predicate relies on it without re-checking; it is public so a reader
    /// can see exactly what a constructed configuration guarantees.
    ///
    /// The ordering clause is not cosmetic and matches
    /// [`MatchmakerSet::is_well_formed`]: [`AcceptorConfig::contains`]
    /// binary-searches the membership, so an unsorted or duplicated vector —
    /// which only [`AcceptorConfig::new`] normalizes — would make a quorum
    /// tally *silently* miscount rather than fail.
    #[must_use]
    pub fn is_well_formed(&self) -> bool {
        let n = self.members.len();
        self.quorum_system.admits(n)
            && self.members.windows(2).all(|w| w[0] < w[1])
            && (self.domains.is_empty() || self.domains.len() == n)
    }

    /// Whether `voters` hold a **Phase-1** quorum of this configuration —
    /// the promises an election or a CTRL repair probe must gather before it
    /// concludes anything about what an earlier ballot could have chosen. A
    /// voter outside the membership never counts.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn has_phase1_quorum(&self, voters: &BTreeSet<Id>) -> bool {
        let quorum = self.quorum_system.is_phase1_quorum(&self.members, voters);
        if quorum {
            assert!(
                counted(&self.members, voters)
                    >= self.quorum_system.phase1_quorum_size(self.members.len()),
                "a Phase-1 quorum counts at least the Phase-1 quorum size"
            );
        }
        quorum
    }

    /// Whether `voters` hold a **Phase-1** quorum of this configuration in
    /// `row` — [`QuorumSystem::is_phase1_quorum_in`] over it: the form a
    /// quorum read asks, by the row it was addressed to.
    ///
    /// # Panics
    ///
    /// If a row is named under a majority.
    #[must_use]
    pub fn has_phase1_quorum_in(&self, voters: &BTreeSet<Id>, row: Option<usize>) -> bool {
        let quorum = self
            .quorum_system
            .is_phase1_quorum_in(&self.members, voters, row);
        if quorum {
            assert!(
                counted(&self.members, voters)
                    >= self.quorum_system.phase1_quorum_size(self.members.len()),
                "a Phase-1 quorum counts at least the Phase-1 quorum size"
            );
        }
        quorum
    }

    /// The row a quorum read with token `ctx` is addressed to under this
    /// configuration — [`QuorumSystem::row_of`]: `ctx % rows` on a grid,
    /// `None` otherwise.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn row_of(&self, ctx: u64) -> Option<usize> {
        let row = self.quorum_system.row_of(ctx);
        assert!(
            self.admits_read_row(row),
            "a derived row is one of this configuration's"
        );
        row
    }

    /// The row a quorum read with token `ctx` goes to when its reader names
    /// `chosen` — [`QuorumSystem::read_row`].
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn read_row(&self, ctx: u64, chosen: Option<usize>) -> Option<usize> {
        let row = self.quorum_system.read_row(ctx, chosen);
        assert!(
            self.admits_read_row(row),
            "a read runs on one of this configuration's rows"
        );
        row
    }

    /// Whether `row` names a read row of this configuration —
    /// [`QuorumSystem::admits_read_row`].
    #[must_use]
    pub fn admits_read_row(&self, row: Option<usize>) -> bool {
        self.quorum_system.admits_read_row(row)
    }

    /// Whether `node` is an acceptor a Phase-1 message in `row` is addressed
    /// to — [`QuorumSystem::is_phase1_addressee`] over this membership. With
    /// no row, exactly [`AcceptorConfig::contains`].
    ///
    /// # Panics
    ///
    /// If a row is named under a majority.
    #[must_use]
    pub fn is_phase1_addressee(&self, node: Id, row: Option<usize>) -> bool {
        let addressee = self
            .quorum_system
            .is_phase1_addressee(&self.members, &node, row);
        // Only a member is ever addressed.
        if addressee {
            assert!(self.contains(node), "a Phase-1 addressee is a member");
        }
        addressee
    }

    /// The acceptors a Phase-1 message in `row` addresses, out of this
    /// membership — [`QuorumSystem::phase1_addressees`] over it.
    ///
    /// # Panics
    ///
    /// If a row is named under a majority.
    #[must_use]
    pub fn phase1_addressees(&self, row: Option<usize>) -> Vec<Id> {
        let addressees = self.quorum_system.phase1_addressees(&self.members, row);
        assert!(
            addressees.iter().all(|a| self.is_phase1_addressee(*a, row)),
            "a Phase-1 fan-out addresses exactly the row's members"
        );
        addressees
    }

    /// Whether `voters` hold a **Phase-2** quorum of this configuration — the
    /// accepts that choose a value, and every claim that rests on one: a
    /// leader's standing authority (`CheckQuorum`) and the GC fence's
    /// custody claim. A voter outside the membership never
    /// counts. Under a grid, *any* full column; a round addressed to one
    /// column is judged by [`AcceptorConfig::has_phase2_quorum_in`].
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn has_phase2_quorum(&self, voters: &BTreeSet<Id>) -> bool {
        let quorum = self.quorum_system.is_phase2_quorum(&self.members, voters);
        if quorum {
            assert!(
                counted(&self.members, voters)
                    >= self.quorum_system.phase2_quorum_size(self.members.len()),
                "a Phase-2 quorum counts at least the Phase-2 quorum size"
            );
        }
        quorum
    }

    /// Whether `voters` hold a **Phase-2** quorum of this configuration in
    /// `column` — [`QuorumSystem::is_phase2_quorum_in`] over it: the form a
    /// decision asks, by the column its round was addressed to.
    ///
    /// # Panics
    ///
    /// If a column is named under a majority.
    #[must_use]
    pub fn has_phase2_quorum_in(&self, voters: &BTreeSet<Id>, column: Option<usize>) -> bool {
        let quorum = self
            .quorum_system
            .is_phase2_quorum_in(&self.members, voters, column);
        if quorum {
            assert!(
                counted(&self.members, voters)
                    >= self.quorum_system.phase2_quorum_size(self.members.len()),
                "a Phase-2 quorum counts at least the Phase-2 quorum size"
            );
        }
        quorum
    }

    /// The column `slot`'s Phase 2 is addressed to under this configuration
    /// — [`QuorumSystem::column_of`]: `slot % cols` on a grid, `None`
    /// otherwise.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn column_of(&self, slot: Slot) -> Option<usize> {
        let column = self.quorum_system.column_of(slot);
        // A slot's column is one of this configuration's, and it holds a
        // whole Phase-2 quorum.
        if let Some(column) = column {
            assert!(
                !self.phase2_addressees(Some(column)).is_empty(),
                "a slot's column has members"
            );
        }
        column
    }

    /// Whether `node` is an acceptor a Phase-2 message in `column` is
    /// addressed to — [`QuorumSystem::is_phase2_addressee`] over this
    /// membership. With no column, exactly [`AcceptorConfig::contains`].
    ///
    /// # Panics
    ///
    /// If a column is named under a majority.
    #[must_use]
    pub fn is_phase2_addressee(&self, node: Id, column: Option<usize>) -> bool {
        let addressee = self
            .quorum_system
            .is_phase2_addressee(&self.members, &node, column);
        if addressee {
            assert!(self.contains(node), "a Phase-2 addressee is a member");
        }
        addressee
    }

    /// The acceptors a Phase-2 message in `column` addresses, out of this
    /// membership — [`QuorumSystem::phase2_addressees`] over it.
    ///
    /// # Panics
    ///
    /// If a column is named under a majority.
    #[must_use]
    pub fn phase2_addressees(&self, column: Option<usize>) -> Vec<Id> {
        let addressees = self.quorum_system.phase2_addressees(&self.members, column);
        assert!(
            addressees
                .iter()
                .all(|a| self.is_phase2_addressee(*a, column)),
            "a Phase-2 fan-out addresses exactly the column's members"
        );
        addressees
    }

    /// The membership, sorted and deduplicated.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn members(&self) -> &[Id] {
        // The membership every predicate binary-searches: non-empty, sorted
        // and deduplicated (`AcceptorConfig::new`).
        assert!(
            !self.members.is_empty(),
            "a configuration names an acceptor"
        );
        &self.members
    }

    /// Each member's failure domain, in the order of
    /// [`AcceptorConfig::members`]; empty for an unlabeled configuration
    /// (#215 (zones)).
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn domains(&self) -> &[FailureDomain] {
        assert!(
            self.domains.is_empty() || self.domains.len() == self.members.len(),
            "a configuration labels every member or none"
        );
        &self.domains
    }

    /// This configuration with `proxies` proxy leaders and `replicas`
    /// replicas, every other field unchanged (#754 (counts off the format
    /// marker)). Two configurations that differ only in a count are two
    /// configurations, so a count change is a round change.
    ///
    /// # Panics
    ///
    /// If a count is above [`MAX_ROLE_COUNT`].
    #[must_use]
    pub fn with_counts(self, proxies: usize, replicas: usize) -> Self {
        self.try_with_counts(proxies, replicas)
            .unwrap_or_else(|e| panic!("{e}"))
    }

    /// [`AcceptorConfig::with_counts`] for an input: the counts of a
    /// configuration from the wire or from an operator.
    ///
    /// # Errors
    ///
    /// [`ShapeError::RoleCountOverCap`] if a count is above
    /// [`MAX_ROLE_COUNT`].
    ///
    /// # Panics
    ///
    /// If the counted configuration is not well formed (a programmer error:
    /// the counts do not touch the membership or the quorum system).
    pub fn try_with_counts(mut self, proxies: usize, replicas: usize) -> Result<Self, ShapeError> {
        if proxies > MAX_ROLE_COUNT || replicas > MAX_ROLE_COUNT {
            return Err(ShapeError::RoleCountOverCap);
        }
        self.proxies = proxies;
        self.replicas = replicas;
        assert!(
            self.is_well_formed(),
            "naming the counts keeps a configuration well formed"
        );
        Ok(self)
    }

    /// How many proxy leaders this configuration's leader delegates to
    /// (`ProxyId(0..proxy_count)`); zero runs Phase 2 colocated.
    #[must_use]
    pub fn proxy_count(&self) -> usize {
        self.proxies
    }

    /// How many replicas learn this configuration's log
    /// (`ReplicaId(0..replica_count)`); zero is none.
    #[must_use]
    pub fn replica_count(&self) -> usize {
        self.replicas
    }

    /// Whether this configuration delegates Phase 2 to proxy leaders.
    #[must_use]
    pub fn has_proxies(&self) -> bool {
        self.proxies > 0
    }

    /// Whether this configuration runs a replica tier.
    #[must_use]
    pub fn has_replicas(&self) -> bool {
        self.replicas > 0
    }

    /// The replica that owns `slot`'s client reply under this configuration
    /// (Compartmentalized Paxos §3.3): `ReplicaId(slot % replica_count)`,
    /// [`ReplicaId::of`]. `None` without a replica tier.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn reply_owner(&self, slot: Slot) -> Option<ReplicaId> {
        let owner = ReplicaId::of(slot, self.replicas);
        assert!(
            owner.is_some() == self.has_replicas(),
            "a slot has a reply owner iff replicas run"
        );
        owner
    }

    /// Whether this configuration carries failure domains.
    #[must_use]
    pub fn is_zoned(&self) -> bool {
        !self.domains.is_empty()
    }

    /// The failure domain of `node`: `None` when it is no member or the
    /// configuration is unlabeled.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn domain_of(&self, node: Id) -> Option<&FailureDomain> {
        let index = self.members.binary_search(&node).ok()?;
        let domain = self.domains.get(index);
        assert!(
            domain.is_some() == self.is_zoned(),
            "a member of a labeled configuration has a domain"
        );
        domain
    }

    /// The quorum system this configuration's tallies are judged under.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn quorum_system(&self) -> QuorumSystem {
        assert!(
            self.quorum_system.admits(self.members.len()),
            "a configuration admits its quorum system"
        );
        self.quorum_system
    }

    /// Whether `node` is a member of this configuration.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn contains(&self, node: Id) -> bool {
        let member = self.members.binary_search(&node).is_ok();
        // The binary search is only sound over the sorted membership the
        // constructor built; restated where it is relied on.
        assert!(
            self.members.windows(2).all(|w| w[0] < w[1]),
            "a configuration's membership is sorted and deduplicated"
        );
        member
    }

    /// Whether every member is in `pool` (sorted and deduplicated): the
    /// wire-hygiene question every deployment asks of a configuration it
    /// learns, registers or is asked to run — one naming a node outside the
    /// pool is not one it can address.
    #[must_use]
    ///
    /// # Panics
    ///
    /// If `pool` is not sorted and deduplicated (a programmer error: every
    /// pool is normalized where it is built).
    pub fn is_drawn_from(&self, pool: &[Id]) -> bool {
        assert!(
            pool.windows(2).all(|w| w[0] < w[1]),
            "a pool is sorted and deduplicated"
        );
        self.members.iter().all(|m| pool.binary_search(m).is_ok())
    }
}

#[cfg(test)]
mod tests;
