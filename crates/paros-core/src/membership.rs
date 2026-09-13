//! **Membership**: the data every quorum question is asked over — an
//! acceptor configuration, a matchmaker set, and the quorum system that says
//! which subsets of a membership count.
//!
//! This is the boundary the rest of the core reasons through and never
//! around: the proposer, the read rounds, `CheckQuorum` and the GC fence all
//! ask [`AcceptorConfig::has_phase1_quorum`] or
//! [`AcceptorConfig::has_phase2_quorum`], which ask
//! [`Quorums::is_phase1_quorum`] / [`Quorums::is_phase2_quorum`];
//! no tally compares a count against a threshold on its own. That surface is
//! the [`Quorums`] trait — the *flavor* of Paxos a deployment runs, stated as
//! the one law every implementation must satisfy (every Phase-1 quorum meets
//! every Phase-2 quorum, [`Quorums::cross_intersects`]) — and
//! [`QuorumSystem`] is its wire-borne implementation. Three quorum
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
//! n`, see [`Quorums::cross_intersects`]), not each phase's quorums to
//! intersect each other, and a system that exploits the difference cannot be
//! written against one un-tagged predicate. The majority is the plain
//! deployment's system and stays the default: a flexible or grid system is
//! opt-in configuration data, chosen per configuration and carried with it.
//!
//! **Addressing crosses the same boundary.** A grid does not only *judge*
//! Phase 2 by column, it *addresses* it by column: every slot's `Accept`
//! goes to one column ([`Quorums::column_of`] — `slot % cols`, a pure
//! function of the slot so every incarnation of a leadership derives the
//! same column without carrying it), and the round is decided by that
//! column alone ([`Quorums::is_phase2_quorum_in`]). That is
//! Compartmentalized Paxos's §3.2: every acceptor sees `1 / cols` of the
//! commands. A majority or a flexible split names no column and addresses
//! the whole membership. The claims that rest on *any* Phase-2 quorum — a
//! leader's standing authority, a read's confirmation, the GC fence — ask
//! the column-less predicate and are satisfied by any full column. The read
//! side mirrors it (#143): a quorum read is addressed to **one row**
//! ([`Quorums::row_of`] — `ctx % rows`, [`Quorums::phase1_addressees`])
//! and judged by that row alone ([`Quorums::is_phase1_quorum_in`]); an
//! election's `Prepare` still goes to the whole membership and any full row
//! completes it.
//!
//! Matchmaker quorums are deliberately **not** parameterized
//! ([`MatchmakerSet::has_quorum`] is a majority by construction): the
//! generation handover's safety argument is made under the majority model
//! alone. They still ask the same predicate — a matchmaker tally is not a
//! count either.

use std::collections::BTreeSet;

use crate::types::{Fingerprint, NodeId, Slot};

/// The quorum system a configuration uses: which sets of acceptors count as a
/// quorum for Phase 1 (election) and Phase 2 (decide). **One implementation
/// of [`Quorums`]** — the one the wire carries, since an [`AcceptorConfig`]
/// rides inside messages and holds this enum; every predicate and every
/// addressing question it answers is the trait's, and the law it satisfies
/// is stated there once.
///
/// Carried as a *value* in [`crate::Config`] so that a reconfiguration is a
/// *data* change — a different quorum system per configuration — rather than
/// a rewrite of the election/decide logic. Paxos safety rests on every
/// Phase-1 quorum intersecting every Phase-2 quorum
/// ([`Quorums::cross_intersects`]); a simple majority satisfies that
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
    /// `q1 + q2 > n` ([`Quorums::admits`]); a configuration that does
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
    /// arithmetic to check ([`Quorums::cross_intersects`] answers `true`).
    ///
    /// What the grid buys: each slot's Phase 2 is addressed to **one column**
    /// ([`Quorums::column_of`], `slot % cols`), so every acceptor sees
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
    /// `rows * cols == n` ([`Quorums::admits`]); a configuration that
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

/// The membership size past which [`Quorums::cross_intersects`]'s default
/// brute force gives up: `2^n` subsets, each judged by both predicates,
/// is cheap to sixteen members and past a lesson's reach beyond.
pub const BRUTE_FORCE_MEMBERS: usize = 16;

/// The **quorum system** a configuration runs: which subsets of a
/// membership count as a Phase-1 quorum (an election, a repair probe, a
/// quorum read) and which as a Phase-2 quorum (a decision, and every claim
/// that rests on one), and which members each phase's message is addressed
/// to. This is the one surface the roles reach a quorum system through —
/// [`AcceptorConfig`] forwards every one of its quorum questions here and
/// nothing else in the crate asks another way — so a *flavor* of Paxos is
/// an implementation of this trait handed to the same roles, and the law it
/// has to satisfy is stated once, here.
///
/// # The law
///
/// **Every Phase-1 quorum meets every Phase-2 quorum.** That is the whole
/// of what Paxos safety asks of a quorum system (Flexible Paxos §3: `∀ Q1,
/// ∀ Q2 : Q1 ∩ Q2 ≠ ∅`, and nothing within a phase): a value chosen by a
/// Phase-2 quorum at one ballot is reported to every later ballot's Phase-1
/// quorum by the member they share, which is what lets the proposer's P2c
/// rule adopt it. [`Quorums::cross_intersects`] is the check;
/// [`Quorums::admits`] is the well-formedness arm [`AcceptorConfig::new`]
/// asserts once, at the only construction site, so no tally re-checks it.
/// Two Phase-1 quorums need not meet, and neither need two Phase-2 quorums
/// — the freedom [`QuorumSystem::Flexible`] spends.
///
/// # The contract between the methods
///
/// - The `_in` predicates and the addressees agree: `is_phase2_quorum_in(m,
///   v, Some(c))` counts exactly the members `phase2_addressees(m, Some(c))`
///   names, and a member outside them is never an addressee
///   (`is_phase2_addressee`); the Phase-1 trio says the same of a row.
/// - The column-less and row-less forms are the disjunction: `None` names no
///   column, and *any* column (row) satisfies it.
/// - Addressing is a pure function of the slot (`column_of`) and of the read
///   token (`row_of`): the core draws nothing, and every incarnation of a
///   leadership derives the same column without carrying it.
/// - `column_of` answers `Some` exactly when `column_count` does, and every
///   column it names is below the count.
/// - The predicates are **monotone**: a superset of a quorum is a quorum
///   (a quorum system is an upward-closed family). The default
///   `cross_intersects` relies on it.
///
/// # Implementing it
///
/// A system that names no rows and no columns — a majority, a flexible
/// split, a reader's own two-site rule — implements the two sizes, the two
/// `_in` predicates and `admits`, and keeps every default: the addressing
/// methods default to the whole membership, `column_of` / `row_of` /
/// `column_count` to `None`, and `cross_intersects` to a brute force over
/// every subset. A system that addresses a column or a row overrides the
/// whole addressing family together, exactly as [`QuorumSystem::Grid`] does.
///
/// [`QuorumSystem`] is the one implementation the wire carries: an
/// [`AcceptorConfig`] rides in `Message::Accept` and in every
/// `Option<AcceptorConfig>` a message names, and it holds the enum, not a
/// generic. A reader's own implementation drives the roles by hand — as the
/// crate's examples do — and never goes on the wire.
pub trait Quorums {
    /// The number of acceptors a **Phase-1** quorum over a membership of
    /// `members` takes. Kept, with [`Quorums::phase2_quorum_size`], for
    /// the one thing a predicate cannot report — how many more answers a
    /// pending tally still waits for; whether a tally *holds* is always
    /// [`Quorums::is_phase1_quorum`].
    fn phase1_quorum_size(&self, members: usize) -> usize;

    /// The number of acceptors a **Phase-2** quorum over a membership of
    /// `members` takes. See [`Quorums::phase1_quorum_size`].
    fn phase2_quorum_size(&self, members: usize) -> usize;

    /// Whether this quorum system can be run over a membership of `members`
    /// at all: each phase's quorum is at least one acceptor and at most the
    /// membership, and the two cross-intersect. This is the
    /// well-formedness arm [`AcceptorConfig::new`] asserts once; it is public
    /// so a wire boundary can *refuse* a malformed configuration before
    /// constructing one, where the constructor would panic.
    fn admits(&self, members: usize) -> bool;

    /// Whether every Phase-1 quorum of a membership of `members` intersects
    /// every Phase-2 quorum of it — the one fact Paxos safety rests on (the
    /// law above). The default proves it by **brute force**: every subset
    /// of `members` that [`Quorums::is_phase1_quorum`] accepts must have a
    /// complement that [`Quorums::is_phase2_quorum`] refuses (with monotone
    /// predicates, a Phase-2 quorum disjoint from `Q1` would be a subset of
    /// the complement, and so would the complement itself be a quorum). It
    /// is `O(2^n · n)`: exact to [`BRUTE_FORCE_MEMBERS`] members and
    /// **`false` beyond** — an unproven law is a refused configuration, so a
    /// system for a larger deployment overrides it with its own argument, as
    /// [`QuorumSystem`] does with arithmetic and geometry (and a test pins
    /// that the two agree).
    fn cross_intersects(&self, members: usize) -> bool {
        if members > BRUTE_FORCE_MEMBERS {
            return false;
        }
        let all: Vec<usize> = (0..members).collect();
        let every = 1u64 << members;
        (0..every).all(|mask| {
            let voters: BTreeSet<usize> =
                all.iter().copied().filter(|m| mask >> m & 1 == 1).collect();
            if !self.is_phase1_quorum(&all, &voters) {
                return true;
            }
            let rest: BTreeSet<usize> =
                all.iter().copied().filter(|m| mask >> m & 1 == 0).collect();
            !self.is_phase2_quorum(&all, &rest)
        })
    }

    /// Whether `voters` form a **Phase-1** quorum over `members`: the
    /// promises an election (or a CTRL repair probe) must hold before it may
    /// conclude anything about what an earlier ballot could have chosen. A
    /// voter outside `members` never counts. The row-less form an election
    /// asks — [`Quorums::is_phase1_quorum_in`] with no row, *any* full row
    /// under a grid; a quorum read addressed to one row is judged by that
    /// row.
    fn is_phase1_quorum<I: Ord>(&self, members: &[I], voters: &BTreeSet<I>) -> bool {
        self.is_phase1_quorum_in(members, voters, None)
    }

    /// Whether `voters` form a **Phase-1** quorum over `members` **in
    /// `row`**: the form a quorum read asks (#143), judged by the row it
    /// was addressed to. `None` names no row — the whole membership under a
    /// system without rows, *any* full row under a grid.
    ///
    /// # Panics
    ///
    /// An implementation without rows panics on a named row: a caller that
    /// names one holds a read opened against a different quorum system than
    /// the one it judges by — a programmer error, never wire input (the row
    /// is derived by [`Quorums::row_of`] from the same configuration).
    fn is_phase1_quorum_in<I: Ord>(
        &self,
        members: &[I],
        voters: &BTreeSet<I>,
        row: Option<usize>,
    ) -> bool;

    /// Whether `voters` form a **Phase-2** quorum over `members`: the accepts
    /// that choose a value, and every claim that rests on one — a leader's
    /// standing authority (`CheckQuorum`), a read's confirmation, the GC
    /// fence's custody claim. A voter outside `members` never counts. The
    /// column-less form the standing claims ask —
    /// [`Quorums::is_phase2_quorum_in`] with no column, *any* full column
    /// under a grid; a round that was addressed to one column is judged by
    /// that column.
    fn is_phase2_quorum<I: Ord>(&self, members: &[I], voters: &BTreeSet<I>) -> bool {
        self.is_phase2_quorum_in(members, voters, None)
    }

    /// Whether `voters` form a **Phase-2** quorum over `members` **in
    /// `column`**: the form a decision asks, judged by the column the round
    /// was addressed to. `None` names no column — the whole membership under
    /// a system without columns, *any* full column under a grid.
    ///
    /// A vote from outside the column never counts here, even from a
    /// configured acceptor: an acceptor that received a misrouted or
    /// duplicated `Accept` may well have accepted it (acceptor guards are
    /// pool-based, never configuration-based), but the round's quorum is its
    /// column, and only a full column proves the value chosen. The caller
    /// keeps such a vote out of the tally
    /// ([`Quorums::is_phase2_addressee`]); the decision restates it.
    ///
    /// # Panics
    ///
    /// An implementation without columns panics on a named column: a caller
    /// that names one holds a round opened against a different quorum
    /// system than the one it judges by — a programmer error, never wire
    /// input (the column is derived by [`Quorums::column_of`] from the same
    /// configuration).
    fn is_phase2_quorum_in<I: Ord>(
        &self,
        members: &[I],
        voters: &BTreeSet<I>,
        column: Option<usize>,
    ) -> bool;

    /// Whether `node` is one of the acceptors a Phase-1 message in `row` is
    /// addressed to — the guard a quorum read's tally applies to a
    /// `PreReadAck` before counting it. A node outside `members` is never an
    /// addressee. The default is the system without rows: every member, and
    /// `row` must be `None`.
    ///
    /// # Panics
    ///
    /// If a row is named under a system without rows (see
    /// [`Quorums::is_phase1_quorum_in`]).
    fn is_phase1_addressee<I: Ord>(&self, members: &[I], node: &I, row: Option<usize>) -> bool {
        assert!(
            row.is_none(),
            "only a grid names a row for a Phase-1 addressee"
        );
        members.binary_search(node).is_ok()
    }

    /// The acceptors a Phase-1 message in `row` is addressed to, out of
    /// `members`, in membership order — the read-side twin of
    /// [`Quorums::phase2_addressees`]. The default is the system without
    /// rows: the whole membership, and `row` must be `None`. Under a grid,
    /// `None` addresses every row: an election's `Prepare` goes to the
    /// whole membership, since which row will be whole is not known ahead.
    ///
    /// # Panics
    ///
    /// If a row is named under a system without rows (see
    /// [`Quorums::is_phase1_quorum_in`]).
    fn phase1_addressees<I: Copy>(&self, members: &[I], row: Option<usize>) -> Vec<I> {
        assert!(
            row.is_none(),
            "only a grid names a row for a Phase-1 fan-out"
        );
        members.to_vec()
    }

    /// Whether `node` is one of the acceptors a Phase-2 message in `column`
    /// is addressed to — the guard a round's tally applies to an `Accepted`
    /// before counting it. A node outside `members` is never an addressee.
    /// The default is the system without columns: every member, and
    /// `column` must be `None`.
    ///
    /// # Panics
    ///
    /// If a column is named under a system without columns (see
    /// [`Quorums::is_phase2_quorum_in`]).
    fn is_phase2_addressee<I: Ord>(&self, members: &[I], node: &I, column: Option<usize>) -> bool {
        assert!(
            column.is_none(),
            "only a grid names a column for a Phase-2 addressee"
        );
        members.binary_search(node).is_ok()
    }

    /// The acceptors a Phase-2 message in `column` is addressed to, out of
    /// `members`, in membership order. The default is the system without
    /// columns: the whole membership — any subset large enough may answer —
    /// and `column` must be `None`. Under a grid, `None` addresses every
    /// column.
    ///
    /// # Panics
    ///
    /// If a column is named under a system without columns (see
    /// [`Quorums::is_phase2_quorum_in`]).
    fn phase2_addressees<I: Copy>(&self, members: &[I], column: Option<usize>) -> Vec<I> {
        assert!(
            column.is_none(),
            "only a grid names a column for a Phase-2 fan-out"
        );
        members.to_vec()
    }

    /// The **column** a slot's Phase 2 is addressed to under this quorum
    /// system, or `None` — the whole membership — under a system that names
    /// no column (the default). A **pure function of the slot**,
    /// deliberately: the core has no RNG, and the column must be derivable
    /// by every incarnation of a leadership without carrying it. `Some`
    /// exactly when [`Quorums::column_count`] is, and always below it.
    fn column_of(&self, slot: Slot) -> Option<usize> {
        let _ = slot;
        None
    }

    /// The **row** a quorum read is addressed to under this quorum system
    /// (#143), or `None` — the whole membership — under a system that
    /// names no row (the default). The Phase-1 twin of
    /// [`Quorums::column_of`]: a pure function of the read's token, so the
    /// core draws nothing.
    fn row_of(&self, ctx: u64) -> Option<usize> {
        let _ = ctx;
        None
    }

    /// How many columns this quorum system addresses Phase 2 to — the
    /// modulus [`Quorums::column_of`] draws from — or `None` under a system
    /// that names no column (the default). What a caller checks a column
    /// against without knowing which implementation it holds: a named
    /// column is one below this count, and none at all where there is no
    /// count.
    fn column_count(&self) -> Option<usize> {
        None
    }
}

impl QuorumSystem {
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
}

/// The wire's implementation of [`Quorums`]: the three systems the enum
/// names, each answering the trait's questions its own way — a count for
/// [`QuorumSystem::Majority`] and [`QuorumSystem::Flexible`], set
/// membership over rows and columns for [`QuorumSystem::Grid`]. The law is
/// arithmetic for the first two and geometry for the third, and the
/// override of [`Quorums::cross_intersects`] says so; a test pins it to the
/// trait's brute force.
impl Quorums for QuorumSystem {
    fn phase1_quorum_size(&self, members: usize) -> usize {
        match *self {
            QuorumSystem::Majority => majority_of(members),
            QuorumSystem::Flexible { q1, .. } => q1,
            // A row: `cols` acceptors wide.
            QuorumSystem::Grid { cols, .. } => cols,
        }
    }

    fn phase2_quorum_size(&self, members: usize) -> usize {
        match *self {
            QuorumSystem::Majority => majority_of(members),
            QuorumSystem::Flexible { q2, .. } => q2,
            // A column: `rows` acceptors tall.
            QuorumSystem::Grid { rows, .. } => rows,
        }
    }

    /// For the cardinality systems it is arithmetic, `q1 + q2 > n` (Flexible
    /// Paxos §4): [`QuorumSystem::Majority`] takes the same `q` in both
    /// phases, so it reduces to the familiar `2q > n`;
    /// [`QuorumSystem::Flexible`] answers `q1 + q2 > n` and is free to let
    /// one phase's quorums *not* intersect each other (the paper's whole
    /// point — an even cluster with `|Q2| = n/2`), which the old
    /// self-intersection assert forbade outright. For [`QuorumSystem::Grid`]
    /// it is **geometry, true by construction**: a row and a column of the
    /// same grid always share exactly one cell, so a full row always meets a
    /// full column — whatever `rows` and `cols` are.
    fn cross_intersects(&self, members: usize) -> bool {
        match *self {
            QuorumSystem::Majority | QuorumSystem::Flexible { .. } => {
                self.phase1_quorum_size(members)
                    .saturating_add(self.phase2_quorum_size(members))
                    > members
            }
            QuorumSystem::Grid { .. } => true,
        }
    }

    /// A majority admits every non-empty membership. A flexible split admits
    /// `n` when `1 <= q1 <= n`, `1 <= q2 <= n` and `q1 + q2 > n`. A grid
    /// admits `n` when `rows >= 1`, `cols >= 1` and `rows * cols == n` — the
    /// layout must tile the membership exactly, or some row or column would
    /// be short and the set-membership predicates would answer for a cell
    /// that does not exist.
    fn admits(&self, members: usize) -> bool {
        match *self {
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

    /// `slot % cols` on a [`QuorumSystem::Grid`], `None` — the whole
    /// membership — under a majority or a flexible split, which name no
    /// column. (frankenpaxos draws `grid.randomWriteQuorum()`; paros
    /// replaces the draw by the modulus, and a driver that wants to perturb
    /// the choice does so through its own hook, never inside the core.)
    /// Consecutive slots walk the columns round-robin, so a leader streaming
    /// commands spreads them evenly: Compartmentalized Paxos's `1 / w` per
    /// acceptor.
    fn column_of(&self, slot: Slot) -> Option<usize> {
        match *self {
            QuorumSystem::Majority | QuorumSystem::Flexible { .. } => None,
            QuorumSystem::Grid { cols, .. } => {
                let cols = u64::try_from(cols).unwrap_or(u64::MAX).max(1);
                // `slot % cols < cols`, and `cols` came from a `usize`, so the
                // remainder always converts back.
                Some(usize::try_from(slot.0 % cols).unwrap_or(0))
            }
        }
    }

    /// `ctx % rows` on a [`QuorumSystem::Grid`], `None` — the whole
    /// membership — under a majority or a flexible split, which name no
    /// row. Compartmentalized Paxos §3.4 sends `PreRead` to *a* read quorum;
    /// which one is the reader's choice, and spreading reads over the rows
    /// is what lets the read load scale with the number of rows.
    fn row_of(&self, ctx: u64) -> Option<usize> {
        match *self {
            QuorumSystem::Majority | QuorumSystem::Flexible { .. } => None,
            QuorumSystem::Grid { rows, .. } => {
                let rows = u64::try_from(rows).unwrap_or(u64::MAX).max(1);
                // `ctx % rows < rows`, and `rows` came from a `usize`.
                Some(usize::try_from(ctx % rows).unwrap_or(0))
            }
        }
    }

    /// `cols` on a [`QuorumSystem::Grid`], `None` under a majority or a
    /// flexible split.
    fn column_count(&self) -> Option<usize> {
        match *self {
            QuorumSystem::Majority | QuorumSystem::Flexible { .. } => None,
            QuorumSystem::Grid { cols, .. } => Some(cols),
        }
    }

    /// Identical to [`Quorums::is_phase2_quorum`] under
    /// [`QuorumSystem::Majority`]; [`QuorumSystem::Flexible`] counts against
    /// `q1` here and `q2` there; a [`QuorumSystem::Grid`] answers by set
    /// membership — the named row, or some row, lies wholly in `voters`.
    fn is_phase1_quorum_in<I: Ord>(
        &self,
        members: &[I],
        voters: &BTreeSet<I>,
        row: Option<usize>,
    ) -> bool {
        match *self {
            QuorumSystem::Majority => {
                assert!(
                    row.is_none(),
                    "only a grid names a row for a Phase-1 quorum"
                );
                Self::is_majority(members, voters)
            }
            QuorumSystem::Flexible { q1, .. } => {
                assert!(
                    row.is_none(),
                    "only a grid names a row for a Phase-1 quorum"
                );
                counted(members, voters) >= q1
            }
            // Set membership, not a count: the named row — or some row —
            // lies wholly in `voters`.
            QuorumSystem::Grid { rows, cols } => match row {
                Some(row) => row < rows && all_voted(grid_row(members, cols, row), voters),
                None => (0..rows).any(|row| all_voted(grid_row(members, cols, row), voters)),
            },
        }
    }

    /// A member of the configuration under a majority or a flexible split
    /// (`row` is `None` there); a member of exactly that row under a grid.
    fn is_phase1_addressee<I: Ord>(&self, members: &[I], node: &I, row: Option<usize>) -> bool {
        let Ok(position) = members.binary_search(node) else {
            return false;
        };
        match *self {
            QuorumSystem::Majority | QuorumSystem::Flexible { .. } => {
                assert!(
                    row.is_none(),
                    "only a grid names a row for a Phase-1 addressee"
                );
                true
            }
            QuorumSystem::Grid { cols, .. } => {
                row.is_none_or(|row| cols >= 1 && position / cols == row)
            }
        }
    }

    /// A majority or a flexible split addresses the whole membership; a grid
    /// addresses **one row** — the one [`Quorums::row_of`] derived for the
    /// read — so every acceptor answers `1 / rows` of the reads
    /// (Compartmentalized Paxos §3.4). `None` under a grid addresses every
    /// row.
    fn phase1_addressees<I: Copy>(&self, members: &[I], row: Option<usize>) -> Vec<I> {
        match *self {
            QuorumSystem::Majority | QuorumSystem::Flexible { .. } => {
                assert!(
                    row.is_none(),
                    "only a grid names a row for a Phase-1 fan-out"
                );
                members.to_vec()
            }
            QuorumSystem::Grid { cols, .. } => match row {
                Some(row) => grid_row(members, cols, row).to_vec(),
                None => members.to_vec(),
            },
        }
    }

    /// A count against `q` (a majority) or `q2` (a flexible split); set
    /// membership over the named column — or some column — under a grid.
    fn is_phase2_quorum_in<I: Ord>(
        &self,
        members: &[I],
        voters: &BTreeSet<I>,
        column: Option<usize>,
    ) -> bool {
        match *self {
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

    /// A member of the configuration under a majority or a flexible split
    /// (`column` is `None` there); a member of exactly that column under a
    /// grid.
    fn is_phase2_addressee<I: Ord>(&self, members: &[I], node: &I, column: Option<usize>) -> bool {
        let Ok(position) = members.binary_search(node) else {
            return false;
        };
        match *self {
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

    /// A majority addresses the whole membership: any subset large enough may
    /// answer. So does a flexible split — any `q2` of them decide, so every
    /// one of them is asked (the paper's `2|Q2|` message saving, addressing
    /// only `q2` and retrying on the rest, is a latency trade paros does not
    /// take). A grid addresses **one column** — the one
    /// [`Quorums::column_of`] derived for the slot — and that is the
    /// whole of Compartmentalized Paxos's acceptor-side change: the caller
    /// ([`crate::ColocatedNode`]'s Phase-2 fan-out) asks the boundary instead
    /// of iterating the membership itself. `None` under a grid addresses
    /// every column.
    fn phase2_addressees<I: Copy>(&self, members: &[I], column: Option<usize>) -> Vec<I> {
        match *self {
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
    /// membership admits ([`Quorums::admits`]: each phase's quorum
    /// between one acceptor and the whole membership, and the two phases'
    /// quorums always intersecting, [`Quorums::cross_intersects`] —
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

    /// Whether `voters` hold a **Phase-1** quorum of this configuration in
    /// `row` — [`Quorums::is_phase1_quorum_in`] over it: the form a
    /// quorum read asks, by the row it was addressed to.
    ///
    /// # Panics
    ///
    /// If a row is named under a majority or a flexible split.
    #[must_use]
    pub fn has_phase1_quorum_in(&self, voters: &BTreeSet<Id>, row: Option<usize>) -> bool {
        self.quorum_system
            .is_phase1_quorum_in(&self.members, voters, row)
    }

    /// The row a quorum read with token `ctx` is addressed to under this
    /// configuration — [`Quorums::row_of`]: `ctx % rows` on a grid,
    /// `None` otherwise.
    #[must_use]
    pub fn row_of(&self, ctx: u64) -> Option<usize> {
        self.quorum_system.row_of(ctx)
    }

    /// Whether `node` is an acceptor a Phase-1 message in `row` is addressed
    /// to — [`Quorums::is_phase1_addressee`] over this membership. With
    /// no row, exactly [`AcceptorConfig::contains`].
    ///
    /// # Panics
    ///
    /// If a row is named under a majority or a flexible split.
    #[must_use]
    pub fn is_phase1_addressee(&self, node: Id, row: Option<usize>) -> bool {
        self.quorum_system
            .is_phase1_addressee(&self.members, &node, row)
    }

    /// The acceptors a Phase-1 message in `row` addresses, out of this
    /// membership — [`Quorums::phase1_addressees`] over it.
    ///
    /// # Panics
    ///
    /// If a row is named under a majority or a flexible split.
    #[must_use]
    pub fn phase1_addressees(&self, row: Option<usize>) -> Vec<Id> {
        self.quorum_system.phase1_addressees(&self.members, row)
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
    /// `column` — [`Quorums::is_phase2_quorum_in`] over it: the form a
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
    /// — [`Quorums::column_of`]: `slot % cols` on a grid, `None`
    /// otherwise.
    #[must_use]
    pub fn column_of(&self, slot: Slot) -> Option<usize> {
        self.quorum_system.column_of(slot)
    }

    /// Whether `node` is an acceptor a Phase-2 message in `column` is
    /// addressed to — [`Quorums::is_phase2_addressee`] over this
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
    /// membership — [`Quorums::phase2_addressees`] over it.
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

/// Stable identity of a **proxy leader** within the deployment's proxy set
/// (#142, Compartmentalized Paxos §3.1). A distinct namespace from
/// [`NodeId`], exactly as [`MatchmakerId`] is and for the same reason: a
/// proxy is not an acceptor. It is never in `Config::nodes`, never drawn by a
/// reconfiguration, never counted by retirement, GC, the pool guards or a
/// dead-node budget; an operator scales proxies by starting processes, never
/// by reconfiguring the acceptor set.
///
/// The core holds only the proxy **count** (`Config::proxy_count`) and
/// derives a slot's proxy as `ProxyId(slot % proxy_count)`
/// ([`ProxyId::of`]): a pure function of the slot, so a handoff successor
/// re-delegating an inherited round and a restarted leader's re-send name
/// the same proxy without carrying it. The driver's deployment map resolves
/// the id to a process, as it resolves `Learners` from the pool.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ProxyId(pub u64);

impl ProxyId {
    /// The proxy `slot`'s Phase 2 is delegated to under a deployment of
    /// `proxy_count` proxies: `slot % proxy_count`. `None` when the count is
    /// zero — the plain deployment, whose Phase 2 stays colocated.
    #[must_use]
    pub fn of(slot: Slot, proxy_count: usize) -> Option<Self> {
        if proxy_count == 0 {
            return None;
        }
        let count = u64::try_from(proxy_count).unwrap_or(u64::MAX);
        Some(Self(slot.0 % count))
    }

    /// Whether this id names a proxy of a deployment of `proxy_count`.
    #[must_use]
    pub fn is_in(self, proxy_count: usize) -> bool {
        u64::try_from(proxy_count).is_ok_and(|count| self.0 < count)
    }
}

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
        Self::admits(&self.members) && self.members.windows(2).all(|w| w[0] < w[1])
    }

    /// Whether `members`, once normalized, would make a well-formed set —
    /// the rule [`MatchmakerSet::new`] asserts, asked *before* constructing:
    /// at least one distinct matchmaker, and the majority system every
    /// matchmaker-side quorum is drawn from cross-intersecting over them
    /// (`2q > n`, true of every non-empty set). Public so a deployment's
    /// proof obligations ([`crate::Config::check`]) can restate the rule by
    /// calling it rather than duplicating it.
    #[must_use]
    pub fn admits(members: &[MatchmakerId]) -> bool {
        let distinct = members.iter().collect::<BTreeSet<_>>().len();
        distinct >= 1 && QuorumSystem::Majority.cross_intersects(distinct)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::{
        AcceptorConfig, MatchmakerGeneration, MatchmakerId, MatchmakerSet, QuorumSystem, Quorums,
    };
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

    /// The read side of the grid (#143): a row is addressed by `ctx % rows`,
    /// judged by set membership, and a row that is *not* a column still
    /// completes a Phase-1 quorum — while a column never does.
    #[test]
    fn a_grid_row_is_addressed_by_the_read_token_and_judged_as_a_row() {
        let grid = QuorumSystem::Grid { rows: 2, cols: 3 };
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

    /// A [`Quorums`] that forwards everything to a [`QuorumSystem`] except
    /// [`Quorums::cross_intersects`], which it leaves to the trait's brute
    /// force — so the enum's arithmetic and geometry can be pinned to it.
    struct BruteForce(QuorumSystem);

    impl Quorums for BruteForce {
        fn phase1_quorum_size(&self, members: usize) -> usize {
            self.0.phase1_quorum_size(members)
        }
        fn phase2_quorum_size(&self, members: usize) -> usize {
            self.0.phase2_quorum_size(members)
        }
        fn admits(&self, members: usize) -> bool {
            self.0.admits(members)
        }
        fn is_phase1_quorum_in<I: Ord>(
            &self,
            members: &[I],
            voters: &BTreeSet<I>,
            row: Option<usize>,
        ) -> bool {
            self.0.is_phase1_quorum_in(members, voters, row)
        }
        fn is_phase2_quorum_in<I: Ord>(
            &self,
            members: &[I],
            voters: &BTreeSet<I>,
            column: Option<usize>,
        ) -> bool {
            self.0.is_phase2_quorum_in(members, voters, column)
        }
    }

    /// The trait's default `cross_intersects` is trustworthy because the
    /// enum's hand-written override agrees with it: over every membership up
    /// to eight, for the majority, every `Flexible { q1, q2 }` with `1 <= q1,
    /// q2 <= n` (well formed or not — both must refuse `q1 + q2 <= n`) and
    /// every grid that tiles `n`, the brute force over all `2^n` subsets and
    /// the arithmetic or geometry answer the same.
    #[test]
    fn the_brute_force_agrees_with_arithmetic_and_geometry() {
        for n in 0..=8 {
            let mut systems = vec![QuorumSystem::Majority];
            for q1 in 1..=n {
                for q2 in 1..=n {
                    systems.push(QuorumSystem::Flexible { q1, q2 });
                }
            }
            for rows in 1..=n {
                if n % rows == 0 {
                    systems.push(QuorumSystem::Grid {
                        rows,
                        cols: n / rows,
                    });
                }
            }
            for system in systems {
                assert_eq!(
                    BruteForce(system).cross_intersects(n),
                    system.cross_intersects(n),
                    "{system:?} over {n} members"
                );
            }
        }
        // Past the brute force's reach the default refuses — an unproven
        // law — where the override still proves it by arithmetic.
        let past = super::BRUTE_FORCE_MEMBERS + 1;
        assert!(!BruteForce(QuorumSystem::Majority).cross_intersects(past));
        assert!(QuorumSystem::Majority.cross_intersects(past));
    }

    /// The deliberately wrong system: `q1 = q2 = n / 2` over an even
    /// membership, so `q1 + q2 == n` — two halves that never meet. A reader's
    /// own implementation, written against the trait alone.
    struct HalfAndHalf;

    impl Quorums for HalfAndHalf {
        fn phase1_quorum_size(&self, members: usize) -> usize {
            members / 2
        }
        fn phase2_quorum_size(&self, members: usize) -> usize {
            members / 2
        }
        /// The cardinality rule, as the enum states it, over the brute
        /// force: this is the arm the constructor asserts.
        fn admits(&self, members: usize) -> bool {
            let q1 = self.phase1_quorum_size(members);
            let q2 = self.phase2_quorum_size(members);
            (1..=members).contains(&q1)
                && (1..=members).contains(&q2)
                && self.cross_intersects(members)
        }
        fn is_phase1_quorum_in<I: Ord>(
            &self,
            members: &[I],
            voters: &BTreeSet<I>,
            row: Option<usize>,
        ) -> bool {
            assert!(row.is_none(), "two halves name no row");
            super::counted(members, voters) >= self.phase1_quorum_size(members.len())
        }
        fn is_phase2_quorum_in<I: Ord>(
            &self,
            members: &[I],
            voters: &BTreeSet<I>,
            column: Option<usize>,
        ) -> bool {
            assert!(column.is_none(), "two halves name no column");
            super::counted(members, voters) >= self.phase2_quorum_size(members.len())
        }
    }

    /// What [`AcceptorConfig::new`] asserts, restated over any [`Quorums`]:
    /// the membership is sorted and deduplicated and admits its system. The
    /// constructor itself takes only the wire's enum, so a reader's system
    /// is judged by the same arm here.
    fn acceptor_config_would_accept<Q: Quorums>(members: &[NodeId], system: &Q) -> bool {
        !members.is_empty()
            && members.windows(2).all(|w| w[0] < w[1])
            && system.admits(members.len())
    }

    /// The law bites: two halves fail the brute force, and the constructor's
    /// arm refuses the configuration with it — while the witness is plain to
    /// see, `{1, 2}` elects and `{3, 4}` decides, sharing nothing.
    #[test]
    fn two_halves_never_meet_and_are_refused() {
        let members = nodes(1..=4);
        assert!(HalfAndHalf.is_phase1_quorum(&members, &voters([1, 2])));
        assert!(HalfAndHalf.is_phase2_quorum(&members, &voters([3, 4])));
        assert!(!HalfAndHalf.cross_intersects(4));
        assert!(!HalfAndHalf.admits(4));
        assert!(!acceptor_config_would_accept(&members, &HalfAndHalf));
        // The same arm accepts the wire's systems, so it is the arm and not
        // the helper that refuses.
        assert!(acceptor_config_would_accept(
            &members,
            &QuorumSystem::Majority
        ));
        assert!(acceptor_config_would_accept(
            &members,
            &QuorumSystem::Flexible { q1: 3, q2: 2 }
        ));
        // And the defaults a system without rows or columns keeps: the
        // whole membership is the addressee set, and no column is named.
        assert_eq!(HalfAndHalf.phase2_addressees(&members, None), members);
        assert!(HalfAndHalf.is_phase1_addressee(&members, &NodeId(3), None));
        assert!(!HalfAndHalf.is_phase1_addressee(&members, &NodeId(9), None));
        assert_eq!(HalfAndHalf.column_of(Slot(7)), None);
        assert_eq!(HalfAndHalf.column_count(), None);
        assert_eq!(HalfAndHalf.row_of(7), None);
    }
}
