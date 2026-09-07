//! **Membership**: the data every quorum question is asked over — an
//! acceptor configuration, a matchmaker set, and the quorum system that says
//! which subsets of a membership count.
//!
//! This is the boundary the rest of the core reasons through and never
//! around: the proposer, the read rounds, `CheckQuorum` and the GC fence all
//! ask [`AcceptorConfig::has_phase1_quorum`] or
//! [`AcceptorConfig::has_phase2_quorum`], which ask
//! [`QuorumSystem::is_phase1_quorum`] / [`QuorumSystem::is_phase2_quorum`];
//! no tally compares a count against a threshold on its own. Three quorum
//! systems exist: [`QuorumSystem::Majority`], the default, where the two
//! predicates coincide; [`QuorumSystem::Flexible`], where they differ for
//! the first time — Flexible Paxos's simple quorums, `|Q1| = q1` and
//! `|Q2| = q2` with only `q1 + q2 > n` required; and [`QuorumSystem::Grid`],
//! the first that is not a cardinality at all — the membership laid out in
//! rows and columns, a Phase-1 quorum being any full row and a Phase-2 quorum
//! any full column, answered by set membership. The compartmentalized
//! deployments are further variants of [`QuorumSystem`] and new data in a
//! configuration — never a rewrite of the tallies. The predicates are
//! **phase-split** precisely so such a variant is expressible: Paxos safety
//! needs every Phase-1 quorum to intersect every Phase-2 quorum (`q1 + q2 >
//! n`, see [`QuorumSystem::cross_intersects`]), not each phase's quorums to
//! intersect each other, and a system that exploits the difference cannot be
//! written against one un-tagged predicate. The majority is the plain
//! deployment's system and stays the default: a flexible or grid system is
//! opt-in configuration data, chosen per configuration and carried with it.
//!
//! **Addressing crosses the same boundary.** A grid does not only *judge*
//! Phase 2 by column, it *addresses* it by column: every slot's `Accept`
//! goes to one column ([`QuorumSystem::column_of`] — `slot % cols`, a pure
//! function of the slot so every incarnation of a leadership derives the
//! same column without carrying it), and the round is decided by that
//! column alone ([`QuorumSystem::is_phase2_quorum_in`]). That is
//! Compartmentalized Paxos's §3.2: every acceptor sees `1 / cols` of the
//! commands. A majority or a flexible split names no column and addresses
//! the whole membership. The claims that rest on *any* Phase-2 quorum — a
//! leader's standing authority, a read's confirmation, the GC fence — ask
//! the column-less predicate and are satisfied by any full column.
//!
//! Matchmaker quorums are deliberately **not** parameterized
//! ([`MatchmakerSet::has_quorum`] is a majority by construction): the
//! generation handover's safety argument is made under the majority model
//! alone. They still ask the same predicate — a matchmaker tally is not a
//! count either.

use std::collections::BTreeSet;

use crate::types::{Fingerprint, NodeId, Slot};

/// The quorum system a configuration uses: which sets of acceptors count as a
/// quorum for Phase 1 (election) and Phase 2 (decide).
///
/// Carried as a *value* in [`crate::Config`] so that a reconfiguration is a
/// *data* change — a different quorum system per configuration — rather than
/// a rewrite of the election/decide logic. Paxos safety rests on every
/// Phase-1 quorum intersecting every Phase-2 quorum
/// ([`QuorumSystem::cross_intersects`]); a simple majority satisfies that
/// trivially, and a flexible split satisfies it by construction
/// ([`AcceptorConfig::new`] asserts it once).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum QuorumSystem {
    /// A simple majority of the membership: any `⌊n/2⌋ + 1` acceptors. Every two
    /// majorities intersect, so Phase-1 and Phase-2 quorums always share an
    /// acceptor. The plain deployment's system and the default.
    #[default]
    Majority,
    /// Flexible Paxos's **simple quorums** (Howard, Malkhi, Spiegelman, §4):
    /// any `q1` acceptors form a Phase-1 quorum and any `q2` a Phase-2
    /// quorum, with only the cross-phase intersection `q1 + q2 > n` required
    /// (§3: `∀ Q1, ∀ Q2 : Q1 ∩ Q2 ≠ ∅`, and nothing within a phase). Two
    /// Phase-2 quorums need not intersect — an even cluster may decide on
    /// `n/2` accepts — and neither need two Phase-1 quorums. The trade is
    /// the paper's: a smaller `q2` makes the steady state (Phase 2, one round
    /// per command) cheaper and tolerant of `q2 - 1` failures at any time,
    /// paid for by a larger `q1` at the next election. `Flexible { q1: n/2 +
    /// 1, q2: n/2 + 1 }` is the majority again; `q2 = 1` is "any single
    /// acceptor learns a value in one hop, but recovery needs everyone up".
    ///
    /// Well-formed over `n` members when `1 <= q1 <= n`, `1 <= q2 <= n` and
    /// `q1 + q2 > n` ([`QuorumSystem::admits`]); a configuration that does
    /// not admit its system cannot be constructed.
    Flexible {
        /// The Phase-1 quorum size: promises an election needs.
        q1: usize,
        /// The Phase-2 quorum size: accepts that choose a value.
        q2: usize,
    },
    /// An **acceptor grid** (Compartmentalized Paxos §3.2, *Compartmentalization
    /// 2*; Flexible Paxos §4, grid quorums): the sorted membership laid out
    /// row-major in `rows × cols`, member `i` at `(i / cols, i % cols)`, so
    /// the layout is a pure function of the configuration and every node
    /// derives the same one. **A Phase-1 quorum is any full row; a Phase-2
    /// quorum is any full column.** Every row meets every column in exactly
    /// one cell, so the cross-phase intersection holds by construction — no
    /// arithmetic to check ([`QuorumSystem::cross_intersects`] answers `true`).
    ///
    /// What the grid buys: each slot's Phase 2 is addressed to **one column**
    /// ([`QuorumSystem::column_of`], `slot % cols`), so every acceptor sees
    /// `1 / cols` of the commands and the acceptor tier's throughput scales
    /// with `cols`. What it costs: a Phase-1 quorum is a *whole* row (`cols`
    /// specific acceptors, not any `cols`), and a Phase-2 quorum a whole
    /// column, so failure tolerance is about *which* acceptors fail — one
    /// dead acceptor per row leaves no full row for the next election, one
    /// per column no full column for its slots. The `1 × n` and `n × 1`
    /// grids are Flexible's `|Q1| = 1, |Q2| = n` thought experiment and its
    /// mirror: well formed here, and a deployment's own business.
    ///
    /// Well-formed over `n` members when `rows >= 1`, `cols >= 1` and
    /// `rows * cols == n` ([`QuorumSystem::admits`]); a configuration that
    /// does not admit its grid cannot be constructed.
    Grid {
        /// Rows of the grid: how many Phase-1 quorums there are.
        rows: usize,
        /// Columns of the grid: how many Phase-2 quorums there are, and the
        /// modulus a slot's column is drawn from.
        cols: usize,
    },
}

/// The size of a majority over `members`: `⌊n/2⌋ + 1`. The one place the
/// majority is spelled out as arithmetic; every majority in the crate — the
/// acceptor-side default and the matchmaker quorum — derives from here.
fn majority_of(members: usize) -> usize {
    members / 2 + 1
}

/// How many of `voters` are in the sorted membership `members` — the count
/// every cardinality quorum compares. A voter outside `members` never counts.
fn counted<I: Ord>(members: &[I], voters: &BTreeSet<I>) -> usize {
    voters
        .iter()
        .filter(|v| members.binary_search(v).is_ok())
        .count()
}

/// The members of row `row` of a `cols`-wide grid laid row-major over
/// `members`: the contiguous run `members[row * cols .. (row + 1) * cols]`.
fn grid_row<I>(members: &[I], cols: usize, row: usize) -> &[I] {
    let start = row.saturating_mul(cols).min(members.len());
    let end = start.saturating_add(cols).min(members.len());
    &members[start..end]
}

/// The members of column `column` of a `cols`-wide grid laid row-major over
/// `members`: every `cols`-th member from `members[column]` on.
fn grid_column<I>(members: &[I], cols: usize, column: usize) -> impl Iterator<Item = &I> {
    members.iter().skip(column).step_by(cols.max(1))
}

/// Whether every member of `cell` voted.
fn all_voted<'a, I: Ord + 'a>(cell: impl IntoIterator<Item = &'a I>, voters: &BTreeSet<I>) -> bool {
    cell.into_iter().all(|m| voters.contains(m))
}

impl QuorumSystem {
    /// The number of acceptors a **Phase-1** quorum over a membership of
    /// `members` takes. Kept, with [`QuorumSystem::phase2_quorum_size`], for
    /// the one thing a predicate cannot report — how many more answers a
    /// pending tally still waits for; whether a tally *holds* is always
    /// [`QuorumSystem::is_phase1_quorum`].
    #[must_use]
    pub fn phase1_quorum_size(self, members: usize) -> usize {
        match self {
            QuorumSystem::Majority => majority_of(members),
            QuorumSystem::Flexible { q1, .. } => q1,
            // A row: `cols` acceptors wide.
            QuorumSystem::Grid { cols, .. } => cols,
        }
    }

    /// The number of acceptors a **Phase-2** quorum over a membership of
    /// `members` takes. See [`QuorumSystem::phase1_quorum_size`].
    #[must_use]
    pub fn phase2_quorum_size(self, members: usize) -> usize {
        match self {
            QuorumSystem::Majority => majority_of(members),
            QuorumSystem::Flexible { q2, .. } => q2,
            // A column: `rows` acceptors tall.
            QuorumSystem::Grid { rows, .. } => rows,
        }
    }

    /// Whether every Phase-1 quorum of a membership of `members` intersects
    /// every Phase-2 quorum of it — the one fact Paxos safety rests on. For
    /// the cardinality systems it is arithmetic, `q1 + q2 > n` (Flexible
    /// Paxos §4): [`QuorumSystem::Majority`] takes the same `q` in both
    /// phases, so it reduces to the familiar `2q > n`;
    /// [`QuorumSystem::Flexible`] answers `q1 + q2 > n` and is free to let
    /// one phase's quorums *not* intersect each other (the paper's whole
    /// point — an even cluster with `|Q2| = n/2`), which the old
    /// self-intersection assert forbade outright. For [`QuorumSystem::Grid`]
    /// it is **geometry, true by construction**: a row and a column of the
    /// same grid always share exactly one cell, so a full row always meets a
    /// full column — whatever `rows` and `cols` are.
    #[must_use]
    pub fn cross_intersects(self, members: usize) -> bool {
        match self {
            QuorumSystem::Majority | QuorumSystem::Flexible { .. } => {
                self.phase1_quorum_size(members)
                    .saturating_add(self.phase2_quorum_size(members))
                    > members
            }
            QuorumSystem::Grid { .. } => true,
        }
    }

    /// Whether this quorum system can be run over a membership of `members`
    /// at all: each phase's quorum is at least one acceptor and at most the
    /// membership, and the two cross-intersect. This is the
    /// well-formedness arm [`AcceptorConfig::new`] asserts once; it is public
    /// so a wire boundary can *refuse* a malformed configuration before
    /// constructing one, where the constructor would panic.
    ///
    /// A majority admits every non-empty membership. A flexible split admits
    /// `n` when `1 <= q1 <= n`, `1 <= q2 <= n` and `q1 + q2 > n`. A grid
    /// admits `n` when `rows >= 1`, `cols >= 1` and `rows * cols == n` — the
    /// layout must tile the membership exactly, or some row or column would
    /// be short and the set-membership predicates would answer for a cell
    /// that does not exist.
    #[must_use]
    pub fn admits(self, members: usize) -> bool {
        match self {
            QuorumSystem::Majority | QuorumSystem::Flexible { .. } => {
                let q1 = self.phase1_quorum_size(members);
                let q2 = self.phase2_quorum_size(members);
                members >= 1
                    && (1..=members).contains(&q1)
                    && (1..=members).contains(&q2)
                    && self.cross_intersects(members)
            }
            QuorumSystem::Grid { rows, cols } => {
                rows >= 1 && cols >= 1 && rows.checked_mul(cols) == Some(members)
            }
        }
    }

    /// The **column** a slot's Phase 2 is addressed to under this quorum
    /// system: `slot % cols` on a [`QuorumSystem::Grid`], `None` — the whole
    /// membership — under a majority or a flexible split, which name no
    /// column.
    ///
    /// A **pure function of the slot**, deliberately: the core has no RNG,
    /// and the column must be derivable by every incarnation of a leadership
    /// without carrying it — a handoff successor re-proposing an inherited
    /// round and a restarted leader's re-send both land on the same column
    /// the round was opened against. (frankenpaxos draws
    /// `grid.randomWriteQuorum()`; paros replaces the draw by the modulus,
    /// and a driver that wants to perturb the choice does so through its own
    /// hook, never inside the core.) Consecutive slots walk the columns
    /// round-robin, so a leader streaming commands spreads them evenly:
    /// Compartmentalized Paxos's `1 / w` per acceptor.
    #[must_use]
    pub fn column_of(self, slot: Slot) -> Option<usize> {
        match self {
            QuorumSystem::Majority | QuorumSystem::Flexible { .. } => None,
            QuorumSystem::Grid { cols, .. } => {
                let cols = u64::try_from(cols).unwrap_or(u64::MAX).max(1);
                // `slot % cols < cols`, and `cols` came from a `usize`, so the
                // remainder always converts back.
                Some(usize::try_from(slot.0 % cols).unwrap_or(0))
            }
        }
    }

    /// Whether `voters` form a **majority** of the sorted membership
    /// `members`. The body of [`QuorumSystem::Majority`]'s two predicates and
    /// of every matchmaker-side tally ([`MatchmakerSet::has_quorum`]) — and
    /// deliberately *not* a method on a quorum system: it matches on no
    /// variant, so it can never quietly answer for a system that is not a
    /// majority. A tally that wants a quorum of a configuration asks the
    /// phase-tagged predicate; a voter outside `members` never counts.
    ///
    /// Generic over the identity so the matchmaker namespace
    /// ([`MatchmakerId`]) and the decree kernel's own acceptor type ask the
    /// same predicate as the acceptor pool: the body is a `binary_search`
    /// over a sorted membership and a count, and neither depends on what an
    /// identity *is*.
    #[must_use]
    pub fn is_majority<I: Ord>(members: &[I], voters: &BTreeSet<I>) -> bool {
        counted(members, voters) >= majority_of(members.len())
    }

    /// Whether `voters` form a **Phase-1** quorum over `members`: the
    /// promises an election (or a CTRL repair probe) must hold before it may
    /// conclude anything about what an earlier ballot could have chosen.
    /// Identical to [`QuorumSystem::is_phase2_quorum`] under
    /// [`QuorumSystem::Majority`]; [`QuorumSystem::Flexible`] counts against
    /// `q1` here and `q2` there. A voter outside `members` never counts.
    #[must_use]
    pub fn is_phase1_quorum<I: Ord>(self, members: &[I], voters: &BTreeSet<I>) -> bool {
        match self {
            QuorumSystem::Majority => Self::is_majority(members, voters),
            QuorumSystem::Flexible { q1, .. } => counted(members, voters) >= q1,
            // Set membership, not a count: some row lies wholly in `voters`.
            QuorumSystem::Grid { rows, cols } => {
                (0..rows).any(|row| all_voted(grid_row(members, cols, row), voters))
            }
        }
    }

    /// Whether `voters` form a **Phase-2** quorum over `members`: the accepts
    /// that choose a value, and every claim that rests on one — a leader's
    /// standing authority (`CheckQuorum`), a read's confirmation, the GC
    /// fence's custody claim. A voter outside `members` never counts. Under a
    /// grid this is *any* full column — the column-less form the standing
    /// claims ask; a round that was addressed to one column is judged by
    /// [`QuorumSystem::is_phase2_quorum_in`] with that column.
    #[must_use]
    pub fn is_phase2_quorum<I: Ord>(self, members: &[I], voters: &BTreeSet<I>) -> bool {
        self.is_phase2_quorum_in(members, voters, None)
    }

    /// Whether `voters` form a **Phase-2** quorum over `members` **in
    /// `column`**: the form a decision asks, judged by the column the round
    /// was addressed to. `None` names no column — the whole membership under
    /// a majority or a flexible split, *any* full column under a grid.
    ///
    /// A vote from outside the column never counts here, even from a
    /// configured acceptor: an acceptor that received a misrouted or
    /// duplicated `Accept` may well have accepted it (acceptor guards are
    /// pool-based, never configuration-based), but the round's quorum is its
    /// column, and only a full column proves the value chosen. The caller
    /// keeps such a vote out of the tally
    /// ([`QuorumSystem::is_phase2_addressee`]); the decision restates it.
    ///
    /// # Panics
    ///
    /// If a column is named under a majority or a flexible split: neither
    /// has columns, so a caller that names one holds a round opened against
    /// a different quorum system than the one it judges by — a programmer
    /// error, never wire input (the column is derived by
    /// [`QuorumSystem::column_of`] from the same configuration).
    #[must_use]
    pub fn is_phase2_quorum_in<I: Ord>(
        self,
        members: &[I],
        voters: &BTreeSet<I>,
        column: Option<usize>,
    ) -> bool {
        match self {
            QuorumSystem::Majority => {
                assert!(
                    column.is_none(),
                    "only a grid names a column for a Phase-2 quorum"
                );
                Self::is_majority(members, voters)
            }
            QuorumSystem::Flexible { q2, .. } => {
                assert!(
                    column.is_none(),
                    "only a grid names a column for a Phase-2 quorum"
                );
                counted(members, voters) >= q2
            }
            QuorumSystem::Grid { cols, .. } => match column {
                Some(column) => {
                    column < cols && all_voted(grid_column(members, cols, column), voters)
                }
                None => (0..cols).any(|c| all_voted(grid_column(members, cols, c), voters)),
            },
        }
    }

    /// Whether `node` is one of the acceptors a Phase-2 message in `column`
    /// is addressed to — the guard a round's tally applies to an `Accepted`
    /// before counting it. A member of the configuration under a majority or
    /// a flexible split (`column` is `None` there); a member of exactly that
    /// column under a grid. A node outside `members` is never an addressee.
    ///
    /// # Panics
    ///
    /// If a column is named under a majority or a flexible split (see
    /// [`QuorumSystem::is_phase2_quorum_in`]).
    #[must_use]
    pub fn is_phase2_addressee<I: Ord>(
        self,
        members: &[I],
        node: &I,
        column: Option<usize>,
    ) -> bool {
        let Ok(position) = members.binary_search(node) else {
            return false;
        };
        match self {
            QuorumSystem::Majority | QuorumSystem::Flexible { .. } => {
                assert!(
                    column.is_none(),
                    "only a grid names a column for a Phase-2 addressee"
                );
                true
            }
            QuorumSystem::Grid { cols, .. } => {
                column.is_none_or(|column| cols >= 1 && position % cols == column)
            }
        }
    }

    /// The acceptors a Phase-2 message in `column` is addressed to, out of
    /// `members`, in membership order.
    ///
    /// A majority addresses the whole membership: any subset large enough may
    /// answer. So does a flexible split — any `q2` of them decide, so every
    /// one of them is asked (the paper's `2|Q2|` message saving, addressing
    /// only `q2` and retrying on the rest, is a latency trade paros does not
    /// take). A grid addresses **one column** — the one
    /// [`QuorumSystem::column_of`] derived for the slot — and that is the
    /// whole of Compartmentalized Paxos's acceptor-side change: the caller
    /// ([`crate::ColocatedNode`]'s Phase-2 fan-out) asks the boundary instead
    /// of iterating the membership itself. `None` under a grid addresses
    /// every column.
    ///
    /// # Panics
    ///
    /// If a column is named under a majority or a flexible split (see
    /// [`QuorumSystem::is_phase2_quorum_in`]).
    #[must_use]
    pub fn phase2_addressees<I: Copy>(self, members: &[I], column: Option<usize>) -> Vec<I> {
        match self {
            QuorumSystem::Majority | QuorumSystem::Flexible { .. } => {
                assert!(
                    column.is_none(),
                    "only a grid names a column for a Phase-2 fan-out"
                );
                members.to_vec()
            }
            QuorumSystem::Grid { cols, .. } => match column {
                Some(column) => grid_column(members, cols, column).copied().collect(),
                None => members.to_vec(),
            },
        }
    }
}

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
/// **Both fields are private and [`AcceptorConfig::new`] is the only way to
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
}

/// The wire shape [`AcceptorConfig`] deserialises through, so a serialized
/// configuration is normalized by [`AcceptorConfig::new`] exactly like a
/// constructed one and no path can produce an unsorted membership.
#[cfg(feature = "serde")]
#[derive(serde::Deserialize)]
struct SerdeAcceptorConfig<Id> {
    members: Vec<Id>,
    quorum_system: QuorumSystem,
}

#[cfg(feature = "serde")]
impl<Id: Copy + Ord> From<SerdeAcceptorConfig<Id>> for AcceptorConfig<Id> {
    fn from(wire: SerdeAcceptorConfig<Id>) -> Self {
        Self::new(wire.members, wire.quorum_system)
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
        assert!(
            !members.is_empty(),
            "an acceptor configuration names at least one acceptor"
        );
        let config = Self {
            members,
            quorum_system,
        };
        assert!(
            config.is_well_formed(),
            "an acceptor configuration admits its quorum system"
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
        self.quorum_system.admits(n) && self.members.windows(2).all(|w| w[0] < w[1])
    }

    /// Whether `voters` hold a **Phase-1** quorum of this configuration —
    /// the promises an election or a CTRL repair probe must gather before it
    /// concludes anything about what an earlier ballot could have chosen. A
    /// voter outside the membership never counts.
    #[must_use]
    pub fn has_phase1_quorum(&self, voters: &BTreeSet<Id>) -> bool {
        self.quorum_system.is_phase1_quorum(&self.members, voters)
    }

    /// Whether `voters` hold a **Phase-2** quorum of this configuration — the
    /// accepts that choose a value, and every claim that rests on one: a
    /// leader's standing authority (`CheckQuorum`), a read's confirmation,
    /// the GC fence's custody claim. A voter outside the membership never
    /// counts. Under a grid, *any* full column; a round addressed to one
    /// column is judged by [`AcceptorConfig::has_phase2_quorum_in`].
    #[must_use]
    pub fn has_phase2_quorum(&self, voters: &BTreeSet<Id>) -> bool {
        self.quorum_system.is_phase2_quorum(&self.members, voters)
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
        self.quorum_system
            .is_phase2_quorum_in(&self.members, voters, column)
    }

    /// The column `slot`'s Phase 2 is addressed to under this configuration
    /// — [`QuorumSystem::column_of`]: `slot % cols` on a grid, `None`
    /// otherwise.
    #[must_use]
    pub fn column_of(&self, slot: Slot) -> Option<usize> {
        self.quorum_system.column_of(slot)
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
        self.quorum_system
            .is_phase2_addressee(&self.members, &node, column)
    }

    /// The acceptors a Phase-2 message in `column` addresses, out of this
    /// membership — [`QuorumSystem::phase2_addressees`] over it.
    ///
    /// # Panics
    ///
    /// If a column is named under a majority or a flexible split.
    #[must_use]
    pub fn phase2_addressees(&self, column: Option<usize>) -> Vec<Id> {
        self.quorum_system.phase2_addressees(&self.members, column)
    }

    /// The membership, sorted and deduplicated.
    #[must_use]
    pub fn members(&self) -> &[Id] {
        &self.members
    }

    /// The quorum system this configuration's tallies are judged under.
    #[must_use]
    pub fn quorum_system(&self) -> QuorumSystem {
        self.quorum_system
    }

    /// Whether `node` is a member of this configuration.
    #[must_use]
    pub fn contains(&self, node: Id) -> bool {
        self.members.binary_search(&node).is_ok()
    }
}

/// Stable identity of a matchmaker within the matchmaker pool. A distinct
/// namespace from [`NodeId`]: a matchmaker is not an acceptor.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct MatchmakerId(pub u64);

impl Fingerprint for Vec<MatchmakerId> {
    /// The identity a matchmaker set carries through Phase 2: an FNV-1a fold
    /// over the members, in their sorted order. The value a decree chooses is
    /// small and always normalized, so its identity is its content.
    fn fingerprint(&self) -> u64 {
        const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
        const PRIME: u64 = 0x0000_0100_0000_01b3;
        let mut hash = OFFSET;
        for member in self {
            for byte in member.0.to_le_bytes() {
                hash ^= u64::from(byte);
                hash = hash.wrapping_mul(PRIME);
            }
        }
        hash
    }
}

/// A matchmaker-set **generation** (#125): which matchmaker set is
/// authoritative. Distinct from a Paxos ballot (consensus leadership, and the
/// acceptor configuration bound to it). Generation 0 is the bootstrap set.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct MatchmakerGeneration(pub u64);

impl MatchmakerGeneration {
    /// The next generation.
    #[must_use]
    pub fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }
}

/// A matchmaker set bound to its generation: the value the successor decree
/// chooses, and what every matchmaking message is fenced by.
///
/// **The membership is private and [`MatchmakerSet::new`] is the only way to
/// build one**, deserialisation included (see `SerdeMatchmakerSet`), for the
/// reason [`AcceptorConfig`] gives: [`MatchmakerSet::contains`] and every
/// quorum tally binary-search the membership, so an unsorted, duplicated or
/// empty vector would not fail, it would make a tally *silently miscount*.
/// Only `new` normalizes and asserts [`MatchmakerSet::is_well_formed`], so
/// only `new` may construct — and a deployment with no matchmakers holds no
/// `MatchmakerSet` at all (`ColocatedNode::matchmaker_set` is `None` there)
/// rather than an empty one.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(from = "SerdeMatchmakerSet"))]
pub struct MatchmakerSet {
    /// The generation this set is authoritative for.
    pub generation: MatchmakerGeneration,
    /// The members, sorted and deduplicated.
    members: Vec<MatchmakerId>,
}

/// The wire shape [`MatchmakerSet`] deserialises through, so a serialized set
/// is normalized and checked by [`MatchmakerSet::new`] exactly like a
/// constructed one.
#[cfg(feature = "serde")]
#[derive(serde::Deserialize)]
struct SerdeMatchmakerSet {
    generation: MatchmakerGeneration,
    members: Vec<MatchmakerId>,
}

#[cfg(feature = "serde")]
impl From<SerdeMatchmakerSet> for MatchmakerSet {
    fn from(wire: SerdeMatchmakerSet) -> Self {
        Self::new(wire.generation, wire.members)
    }
}

impl MatchmakerSet {
    /// A set of `members` (sorted and deduplicated here) for `generation`.
    ///
    /// # Panics
    ///
    /// If `members` is empty: a set with no matchmaker can never form a
    /// quorum, so a generation naming one is a programmer error. Also if the
    /// normalized set is not well formed ([`MatchmakerSet::is_well_formed`])
    /// — asserted here, once, at the only construction site, never per
    /// tally.
    #[must_use]
    pub fn new(generation: MatchmakerGeneration, mut members: Vec<MatchmakerId>) -> Self {
        members.sort_unstable();
        members.dedup();
        assert!(
            !members.is_empty(),
            "a matchmaker set names at least one matchmaker"
        );
        let set = Self {
            generation,
            members,
        };
        assert!(
            set.is_well_formed(),
            "a matchmaker set admits the matchmaker quorum system"
        );
        set
    }

    /// The members, sorted and deduplicated.
    #[must_use]
    pub fn members(&self) -> &[MatchmakerId] {
        &self.members
    }

    /// The size of a matchmaker quorum over this set: a majority. Kept for
    /// the one thing a predicate cannot answer — how many more acks a
    /// pending tally still waits for (`remaining:`). Whether a tally *holds*
    /// is always [`MatchmakerSet::has_quorum`].
    ///
    /// **Majority quorums only.** Matchmaker Paxos generalizes matchmaker
    /// quorums to arbitrary quorum systems; paros deliberately does not. Every
    /// matchmaker-side quorum — registration, GC ack, freeze, the successor
    /// decree over `M_g` (whose `Decree` builds the same majority
    /// from the set it replaces) and publication — is this rule,
    /// and the generation handover's safety argument (quorum intersection
    /// between the freeze quorum and every completed registration, Appendix
    /// B, and between the decree's two phases) is made only under it. A
    /// flexible matchmaker quorum system would have to replace this method
    /// *and* the decree kernel together, never one without the other.
    ///
    /// # Panics
    ///
    /// If the majority does not self-intersect over the membership (a
    /// programmer error: the arithmetic guarantees it).
    #[must_use]
    pub fn quorum_size(&self) -> usize {
        let quorum = majority_of(self.members.len());
        // Postcondition: self-intersecting over the membership.
        assert!(
            quorum * 2 > self.members.len(),
            "a matchmaker quorum is a majority"
        );
        quorum
    }

    /// Whether `voters` hold a matchmaker quorum of this set — the only way
    /// a matchmaker-side tally is ever judged. Routes to
    /// [`QuorumSystem::is_majority`], the one quorum model paros supports for
    /// matchmakers (see [`MatchmakerSet::quorum_size`]); a voter outside the
    /// set never counts.
    #[must_use]
    pub fn has_quorum(&self, voters: &BTreeSet<MatchmakerId>) -> bool {
        QuorumSystem::is_majority(&self.members, voters)
    }

    /// Whether `id` is a member.
    #[must_use]
    pub fn contains(&self, id: MatchmakerId) -> bool {
        self.members.binary_search(&id).is_ok()
    }

    /// Whether this set can serve as a matchmaker configuration at all: it
    /// names at least one matchmaker, sorted and deduplicated, and admits the
    /// quorum system every matchmaker-side quorum is drawn from (majority:
    /// any two quorums intersect, `2q > n`). **A chosen `MatchmakerSet` must
    /// itself admit the required quorum system**, and since
    /// [`MatchmakerSet::new`] is the only constructor and asserts this, every
    /// set that exists — the one a `start` targets, the one a `Bootstrap` or
    /// `Chosen` carries, the one a `finish` proposes from the members that
    /// answered the freeze — does. Under the majority system every non-empty
    /// set qualifies; the check is the explicit invariant a flexible
    /// matchmaker quorum system would have to satisfy too.
    #[must_use]
    pub fn is_well_formed(&self) -> bool {
        let n = self.members.len();
        if n == 0 {
            return false;
        }
        QuorumSystem::Majority.cross_intersects(n) && self.members.windows(2).all(|w| w[0] < w[1])
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::{AcceptorConfig, MatchmakerGeneration, MatchmakerId, MatchmakerSet, QuorumSystem};
    use crate::types::{NodeId, Slot};

    fn nodes(ids: impl IntoIterator<Item = u64>) -> Vec<NodeId> {
        ids.into_iter().map(NodeId).collect()
    }

    fn voters(ids: impl IntoIterator<Item = u64>) -> BTreeSet<NodeId> {
        ids.into_iter().map(NodeId).collect()
    }

    /// The point of the variant: under `Flexible { q1: 3, q2: 2 }` over four
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
        AcceptorConfig::new(nodes(1..=6), QuorumSystem::Grid { rows: 2, cols: 3 })
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

    /// The well-formedness arm: the layout must tile the membership.
    #[test]
    fn a_grid_admits_exactly_what_it_tiles() {
        let g = |rows, cols| QuorumSystem::Grid { rows, cols };
        assert!(g(2, 3).admits(6));
        assert!(g(3, 2).admits(6));
        assert!(g(1, 6).admits(6), "1 × n is Flexible's |Q1| = 1 experiment");
        assert!(g(6, 1).admits(6), "n × 1 is its mirror");
        assert!(g(1, 1).admits(1));
        assert!(!g(2, 3).admits(5));
        assert!(!g(2, 3).admits(7));
        assert!(!g(0, 6).admits(6));
        assert!(!g(6, 0).admits(6));
        assert!(!g(usize::MAX, 2).admits(6), "no overflow");
    }

    #[test]
    #[should_panic(expected = "an acceptor configuration admits its quorum system")]
    fn a_grid_that_does_not_tile_is_unconstructible() {
        let _ = AcceptorConfig::new(nodes(1..=5), QuorumSystem::Grid { rows: 2, cols: 3 });
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

    /// The well-formedness arm, spelled out: `1 <= q1 <= n`, `1 <= q2 <= n`,
    /// `q1 + q2 > n`. A majority admits every non-empty membership.
    #[test]
    fn admits_is_the_flexible_arm() {
        for n in 1..=7 {
            assert!(QuorumSystem::Majority.admits(n));
            assert!(QuorumSystem::Majority.cross_intersects(n));
        }
        assert!(!QuorumSystem::Majority.admits(0));
        let f = |q1, q2| QuorumSystem::Flexible { q1, q2 };
        assert!(f(3, 2).admits(4));
        assert!(f(2, 3).admits(4));
        assert!(f(4, 1).admits(4));
        assert!(f(1, 4).admits(4));
        assert!(f(3, 3).admits(4));
        assert!(!f(2, 2).admits(4), "q1 + q2 = n does not intersect");
        assert!(!f(0, 4).admits(4), "an empty Phase-1 quorum");
        assert!(!f(4, 0).admits(4), "an empty Phase-2 quorum");
        assert!(!f(5, 1).admits(4), "a quorum larger than the membership");
        assert!(!f(usize::MAX, usize::MAX).admits(4), "no overflow");
        assert!(!f(1, 1).admits(0));
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

    /// Matchmaker quorums are majorities only, whatever the acceptor side
    /// runs: `MatchmakerSet` holds no quorum system at all.
    #[test]
    fn matchmaker_quorums_stay_majorities() {
        let set = MatchmakerSet::new(MatchmakerGeneration(0), (1..=4).map(MatchmakerId).collect());
        assert_eq!(set.quorum_size(), 3);
        let two: BTreeSet<MatchmakerId> = [1, 2].into_iter().map(MatchmakerId).collect();
        let three: BTreeSet<MatchmakerId> = [1, 2, 3].into_iter().map(MatchmakerId).collect();
        assert!(!set.has_quorum(&two));
        assert!(set.has_quorum(&three));
    }
}
