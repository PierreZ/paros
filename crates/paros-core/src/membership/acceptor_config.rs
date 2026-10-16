use std::collections::BTreeSet;

#[cfg(doc)]
use super::MatchmakerSet;
use super::quorum_system::counted;
use super::{FailureDomain, QuorumSystem};
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
        from = "SerdeAcceptorConfig<Id>",
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
    /// The cell that owns the journal this configuration runs (#215
    /// (zones), #232 (multiple cells), `docs/architecture.md` §3.7): `0`
    /// is none, the plain arm. The core never decides on it; it is bound to
    /// the ballot like the rest, so the effective configuration names the
    /// owning cell.
    cell: u64,
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
    cell: u64,
}

#[cfg(feature = "serde")]
impl<Id: Copy + Ord> From<SerdeAcceptorConfig<Id>> for AcceptorConfig<Id> {
    fn from(wire: SerdeAcceptorConfig<Id>) -> Self {
        let config = if wire.domains.is_empty() {
            Self::new(wire.members, wire.quorum_system)
        } else {
            Self::zoned(
                wire.members.into_iter().zip(wire.domains).collect(),
                wire.quorum_system,
            )
        };
        config.with_cell(wire.cell)
    }
}

impl<Id: Copy + Ord> AcceptorConfig<Id> {
    /// A configuration over `members` (sorted and deduplicated here) under
    /// `quorum_system`.
    ///
    /// # Panics
    ///
    /// If `members` is empty: a configuration with no acceptor can never form
    /// a quorum, so registering one is a programmer error. Also if the
    /// normalized configuration is not well formed
    /// ([`AcceptorConfig::is_well_formed`]) — the cross-intersection claim
    /// every quorum tally rests on is asserted here, once, at the only
    /// construction site, never per tally.
    #[must_use]
    pub fn new(mut members: Vec<Id>, quorum_system: QuorumSystem) -> Self {
        members.sort_unstable();
        members.dedup();
        Self::build(members, Vec::new(), quorum_system)
    }

    /// A **labeled** configuration (#215 (zones)): each member with its
    /// failure domain, sorted by member and deduplicated here, under
    /// `quorum_system`. The labels are bound to the configuration like the
    /// membership: two configurations that differ only in a label are two
    /// configurations.
    ///
    /// # Panics
    ///
    /// As [`AcceptorConfig::new`]; also if one member is named twice with
    /// two domains, or a domain is not well formed
    /// ([`FailureDomain::is_well_formed`]).
    #[must_use]
    pub fn zoned(mut members: Vec<(Id, FailureDomain)>, quorum_system: QuorumSystem) -> Self {
        members.sort_by_key(|a| a.0);
        for pair in members.windows(2) {
            if pair[0].0 == pair[1].0 {
                assert!(
                    pair[0].1 == pair[1].1,
                    "a member of a configuration has one failure domain"
                );
            }
        }
        members.dedup_by(|a, b| a.0 == b.0);
        let (members, domains): (Vec<Id>, Vec<FailureDomain>) = members.into_iter().unzip();
        assert!(
            domains.iter().all(FailureDomain::is_well_formed),
            "a configuration's failure domains are short labels"
        );
        Self::build(members, domains, quorum_system)
    }

    /// The one place a configuration is assembled, from a sorted and
    /// deduplicated membership and its labels (empty, or one per member).
    fn build(members: Vec<Id>, domains: Vec<FailureDomain>, quorum_system: QuorumSystem) -> Self {
        assert!(
            !members.is_empty(),
            "an acceptor configuration names at least one acceptor"
        );
        assert!(
            domains.is_empty() || domains.len() == members.len(),
            "a configuration labels every member or none"
        );
        let config = Self {
            members,
            quorum_system,
            domains,
            cell: 0,
        };
        assert!(
            config.is_well_formed(),
            "an acceptor configuration admits its quorum system"
        );
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
        config
    }

    /// Whether this configuration can be run at all: at least one acceptor, a
    /// membership that is sorted and deduplicated, and a quorum system the
    /// membership admits ([`QuorumSystem::admits`]: each phase's quorum
    /// between one acceptor and the whole membership, and the two phases'
    /// quorums always intersecting, [`QuorumSystem::cross_intersects`] —
    /// `2q > n` for a majority, `q1 + q2 > n` for a flexible split, and for
    /// a grid a layout that tiles the membership, `rows * cols == n`). This
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
    /// If a row is named under a majority or a flexible split.
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
    /// If a row is named under a majority or a flexible split.
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
    /// If a row is named under a majority or a flexible split.
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
    /// If a column is named under a majority or a flexible split.
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
    /// If a column is named under a majority or a flexible split.
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
    /// If a column is named under a majority or a flexible split.
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

    /// This configuration owned by cell `cell` (`0`: none), every other
    /// field unchanged (#215 (zones), `docs/architecture.md` §3.7).
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn with_cell(mut self, cell: u64) -> Self {
        self.cell = cell;
        assert!(
            self.is_well_formed(),
            "naming a cell keeps a configuration well formed"
        );
        self
    }

    /// The cell that owns the journal this configuration runs; `0` for
    /// none.
    #[must_use]
    pub fn cell(&self) -> u64 {
        self.cell
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
mod tests {
    use std::collections::BTreeSet;

    use super::{AcceptorConfig, QuorumSystem};
    use crate::types::{NodeId, Slot};

    fn nodes(ids: impl IntoIterator<Item = u64>) -> Vec<NodeId> {
        ids.into_iter().map(NodeId).collect()
    }

    fn voters(ids: impl IntoIterator<Item = u64>) -> BTreeSet<NodeId> {
        ids.into_iter().map(NodeId).collect()
    }

    /// acceptors the two phase predicates differ for the first time — two
    /// accepts choose, two promises do not elect — while a majority would
    /// have taken three of either.
    #[test]
    fn flexible_predicates_differ_by_phase() {
        let flexible = AcceptorConfig::new(nodes(1..=4), QuorumSystem::Flexible { q1: 3, q2: 2 });
        let majority = AcceptorConfig::new(nodes(1..=4), QuorumSystem::Majority);
        let two = voters([1, 2]);
        let three = voters([1, 2, 3]);
        assert!(flexible.has_phase2_quorum(&two));
        assert!(!flexible.has_phase1_quorum(&two));
        assert!(flexible.has_phase1_quorum(&three));
        assert!(!majority.has_phase2_quorum(&two));
        assert!(!majority.has_phase1_quorum(&two));
        assert!(majority.has_phase1_quorum(&three));
        assert!(majority.has_phase2_quorum(&three));
        // Two Phase-2 quorums need not intersect under the flexible split;
        // every Phase-1 quorum still meets every Phase-2 quorum.
        assert!(flexible.has_phase2_quorum(&voters([3, 4])));
        assert_eq!(flexible.quorum_system().phase1_quorum_size(4), 3);
        assert_eq!(flexible.quorum_system().phase2_quorum_size(4), 2);
        assert_eq!(flexible.phase2_addressees(None), flexible.members());
        assert_eq!(flexible.column_of(Slot(7)), None);
    }

    /// The `2 × 3` grid of Compartmentalized Paxos §3.2 over six acceptors:
    /// row `{1, 2, 3}` and `{4, 5, 6}`, columns `{1, 4}`, `{2, 5}`, `{3, 6}`.
    fn grid() -> AcceptorConfig {
        AcceptorConfig::new(nodes(1..=6), QuorumSystem::grid(2, 3))
    }

    /// The point of the variant: a full row is a Phase-1 quorum and a full
    /// column a Phase-2 quorum — set membership, not a count.
    #[test]
    fn a_row_elects_and_a_column_decides() {
        let grid = grid();
        assert!(grid.has_phase1_quorum(&voters([1, 2, 3])));
        assert!(grid.has_phase1_quorum(&voters([4, 5, 6])));
        assert!(grid.has_phase1_quorum(&voters([4, 5, 6, 1])));
        assert!(grid.has_phase2_quorum(&voters([1, 4])));
        assert!(grid.has_phase2_quorum(&voters([3, 6])));
        assert!(grid.has_phase2_quorum_in(&voters([2, 5]), Some(1)));
        assert_eq!(grid.quorum_system().phase1_quorum_size(6), 3);
        assert_eq!(grid.quorum_system().phase2_quorum_size(6), 2);
        assert!(grid.quorum_system().cross_intersects(6));
    }

    /// The negative space: a row is not a Phase-2 quorum, a column not a
    /// Phase-1 quorum, and a majority-sized set that is neither is nothing.
    #[test]
    fn a_row_does_not_decide_and_a_column_does_not_elect() {
        let grid = grid();
        assert!(!grid.has_phase2_quorum(&voters([1, 2, 3])));
        assert!(!grid.has_phase1_quorum(&voters([1, 4])));
        // Four of six, a majority anywhere else, but no full row.
        assert!(!grid.has_phase1_quorum(&voters([1, 2, 4, 5])));
        // Three of six with a member of every column and no full column.
        assert!(!grid.has_phase2_quorum(&voters([1, 2, 6])));
        // Any four of six do contain a full column (a two-node complement
        // can break at most two of three columns): the grid's Phase-2
        // quorums are small, and that is the point.
        assert!(grid.has_phase2_quorum(&voters([1, 2, 5, 6])));
        // A full column that is not the round's column does not decide the
        // round; the round's own column does.
        assert!(!grid.has_phase2_quorum_in(&voters([1, 4]), Some(1)));
        assert!(grid.has_phase2_quorum_in(&voters([1, 4]), Some(0)));
        // A column index past the grid is no column.
        assert!(!grid.has_phase2_quorum_in(&voters([1, 2, 3, 4, 5, 6]), Some(3)));
        // Strangers never count.
        assert!(!grid.has_phase1_quorum(&voters([1, 2, 9])));
    }

    /// Column addressing: `slot % cols` is the column, the addressees are
    /// exactly that column in membership order, and an acceptor outside it
    /// is not an addressee even though it is a member.
    #[test]
    fn a_slot_is_addressed_to_its_column() {
        let grid = grid();
        assert_eq!(grid.column_of(Slot(0)), Some(0));
        assert_eq!(grid.column_of(Slot(1)), Some(1));
        assert_eq!(grid.column_of(Slot(2)), Some(2));
        assert_eq!(grid.column_of(Slot(3)), Some(0));
        assert_eq!(grid.column_of(Slot(u64::MAX)), Some(0));
        assert_eq!(grid.phase2_addressees(Some(0)), nodes([1, 4]));
        assert_eq!(grid.phase2_addressees(Some(2)), nodes([3, 6]));
        assert_eq!(grid.phase2_addressees(None), nodes(1..=6));
        assert!(grid.is_phase2_addressee(NodeId(4), Some(0)));
        assert!(!grid.is_phase2_addressee(NodeId(5), Some(0)));
        assert!(grid.is_phase2_addressee(NodeId(5), None));
        assert!(!grid.is_phase2_addressee(NodeId(9), None));
        let majority = AcceptorConfig::new(nodes(1..=3), QuorumSystem::Majority);
        assert!(majority.is_phase2_addressee(NodeId(2), None));
        assert!(!majority.is_phase2_addressee(NodeId(9), None));
    }

    /// The read side of the grid (#143): a row is addressed by `ctx % rows`,
    /// judged by set membership, and a row that is *not* a column still
    /// completes a Phase-1 quorum — while a column never does.
    #[test]
    fn a_grid_row_is_addressed_by_the_read_token_and_judged_as_a_row() {
        let grid = QuorumSystem::grid(2, 3);
        let config = AcceptorConfig::new(nodes(1..=6), grid);
        assert_eq!(config.row_of(0), Some(0));
        assert_eq!(config.row_of(1), Some(1));
        assert_eq!(config.row_of(7), Some(1));
        assert_eq!(config.phase1_addressees(Some(0)), nodes([1, 2, 3]));
        assert_eq!(config.phase1_addressees(Some(1)), nodes([4, 5, 6]));
        assert_eq!(config.phase1_addressees(None), nodes(1..=6));
        assert!(config.is_phase1_addressee(NodeId(5), Some(1)));
        assert!(!config.is_phase1_addressee(NodeId(5), Some(0)));
        assert!(config.is_phase1_addressee(NodeId(5), None));
        assert!(!config.is_phase1_addressee(NodeId(9), None));
        // Row 1 is whole: a Phase-1 quorum in row 1, not in row 0.
        assert!(config.has_phase1_quorum_in(&voters([4, 5, 6]), Some(1)));
        assert!(!config.has_phase1_quorum_in(&voters([4, 5, 6]), Some(0)));
        assert!(config.has_phase1_quorum_in(&voters([4, 5, 6]), None));
        // Negative space: a full column is not a row, and a row is not a
        // column — the two phases' quorums differ by shape, not by count.
        assert!(!config.has_phase1_quorum_in(&voters([1, 4]), Some(0)));
        assert!(!config.has_phase1_quorum_in(&voters([1, 4]), None));
        assert!(!config.has_phase2_quorum(&voters([4, 5, 6])));
        // A row index past the grid is never a quorum.
        assert!(!config.has_phase1_quorum_in(&voters(1..=6), Some(2)));
        // A majority names no row: the whole membership is the row.
        let majority = AcceptorConfig::new(nodes(0..3), QuorumSystem::Majority);
        assert_eq!(majority.row_of(5), None);
        assert_eq!(majority.phase1_addressees(None), nodes(0..3));
        assert!(majority.has_phase1_quorum_in(&voters([0, 1]), None));
    }

    #[test]
    #[should_panic(expected = "only a grid names a row")]
    fn naming_a_row_under_a_majority_is_a_programmer_error() {
        let majority = AcceptorConfig::new(nodes(0..3), QuorumSystem::Majority);
        let _ = majority.has_phase1_quorum_in(&voters([0, 1]), Some(0));
    }

    #[test]
    #[should_panic(expected = "an acceptor configuration admits its quorum system")]
    fn a_grid_that_does_not_tile_is_unconstructible() {
        let _ = AcceptorConfig::new(nodes(1..=5), QuorumSystem::grid(2, 3));
    }

    /// A column is a grid notion: naming one under a majority is a
    /// programmer error, asserted.
    #[test]
    #[should_panic(expected = "only a grid names a column for a Phase-2 quorum")]
    fn a_majority_refuses_a_column() {
        let majority = AcceptorConfig::new(nodes(1..=3), QuorumSystem::Majority);
        let _ = majority.has_phase2_quorum_in(&voters([1, 2]), Some(0));
    }

    /// A voter outside the membership never counts, under either system.
    #[test]
    fn strangers_never_count() {
        let flexible = AcceptorConfig::new(nodes(1..=4), QuorumSystem::Flexible { q1: 3, q2: 2 });
        assert!(!flexible.has_phase2_quorum(&voters([1, 9])));
        assert!(!flexible.has_phase1_quorum(&voters([1, 2, 9])));
        let majority = AcceptorConfig::new(nodes(1..=3), QuorumSystem::Majority);
        assert!(!majority.has_phase2_quorum(&voters([1, 9])));
    }

    /// The constructor is the only site, and it refuses a split whose
    /// phases do not cross-intersect.
    #[test]
    #[should_panic(expected = "an acceptor configuration admits its quorum system")]
    fn flexible_without_cross_intersection_is_unconstructible() {
        let _ = AcceptorConfig::new(nodes(1..=4), QuorumSystem::Flexible { q1: 2, q2: 2 });
    }

    #[test]
    #[should_panic(expected = "an acceptor configuration admits its quorum system")]
    fn flexible_larger_than_the_membership_is_unconstructible() {
        let _ = AcceptorConfig::new(nodes(1..=3), QuorumSystem::Flexible { q1: 4, q2: 1 });
    }

    /// Normalization happens before the arm: duplicates collapse, so a
    /// split that admits the *deduplicated* size is what counts.
    #[test]
    fn well_formedness_is_judged_over_the_deduplicated_membership() {
        let config =
            AcceptorConfig::new(nodes([2, 1, 2, 1]), QuorumSystem::Flexible { q1: 2, q2: 1 });
        assert_eq!(config.members(), nodes([1, 2]));
        assert!(config.is_well_formed());
    }
}
