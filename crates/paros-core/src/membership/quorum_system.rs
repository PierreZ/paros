use std::collections::BTreeSet;

#[cfg(doc)]
use super::{AcceptorConfig, MatchmakerId, MatchmakerSet};
use crate::types::Slot;

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
pub(super) fn majority_of(members: usize) -> usize {
    let majority = members / 2 + 1;
    // Two majorities always intersect, and a non-empty membership can
    // assemble one.
    assert!(
        majority.saturating_mul(2) > members,
        "a majority self-intersects"
    );
    if members > 0 {
        assert!(majority <= members, "a majority fits inside its membership");
    }
    majority
}

/// How many of `voters` are in the sorted membership `members` — the count
/// every cardinality quorum compares. A voter outside `members` never counts.
pub(super) fn counted<I: Ord>(members: &[I], voters: &BTreeSet<I>) -> usize {
    // The binary search below is only sound over a sorted membership.
    assert!(
        members.windows(2).all(|w| w[0] < w[1]),
        "a counted membership is sorted and deduplicated"
    );
    let count = voters
        .iter()
        .filter(|v| members.binary_search(v).is_ok())
        .count();
    // Strangers never count: at most every member voted.
    assert!(count <= members.len(), "only members count toward a quorum");
    count
}

/// The members of row `row` of a `cols`-wide grid laid row-major over
/// `members`: the contiguous run `members[row * cols .. (row + 1) * cols]`.
pub(super) fn grid_row<I>(members: &[I], cols: usize, row: usize) -> &[I] {
    let start = row.saturating_mul(cols).min(members.len());
    let end = start.saturating_add(cols).min(members.len());
    assert!(start <= end, "a grid row is a forward range");
    assert!(end - start <= cols, "a grid row is at most one row wide");
    &members[start..end]
}

/// The members of column `column` of a `cols`-wide grid laid row-major over
/// `members`: every `cols`-th member from `members[column]` on.
pub(super) fn grid_column<I>(
    members: &[I],
    cols: usize,
    column: usize,
) -> impl Iterator<Item = &I> {
    assert!(cols >= 1, "a grid has at least one column");
    members.iter().skip(column).step_by(cols)
}

/// `value % modulus` as an index below `modulus` — the grid's pure
/// addressing arithmetic (a slot's column, a read token's row). A zero
/// modulus is treated as one. The remainder is below `modulus`, which came
/// from a `usize`, so it always converts back.
pub(super) fn residue(value: u64, modulus: usize) -> usize {
    let modulus = u64::try_from(modulus).unwrap_or(u64::MAX).max(1);
    let residue = value % modulus;
    assert!(residue < modulus, "a residue lies below its modulus");
    usize::try_from(residue).unwrap_or(0)
}

/// The rank owning `slot` among `count` peers of one tier (a proxy leader,
/// a replica): `slot % count`, `None` when the tier is absent (`count` 0).
pub(super) fn slot_rank(slot: Slot, count: usize) -> Option<u64> {
    let rank = (count != 0).then(|| slot.0 % u64::try_from(count).unwrap_or(u64::MAX));
    if let Some(rank) = rank {
        assert!(
            rank_is_in(rank, count),
            "a slot's rank lies inside the count"
        );
    }
    rank
}

/// Whether `rank` names one of `count` peers of a tier.
pub(super) fn rank_is_in(rank: u64, count: usize) -> bool {
    let inside = u64::try_from(count).is_ok_and(|count| rank < count);
    // Nothing ranks inside an empty deployment.
    if count == 0 {
        assert!(!inside, "no rank lies inside an empty deployment");
    }
    inside
}

/// Whether every member of `cell` voted.
pub(super) fn all_voted<'a, I: Ord + 'a>(
    cell: impl IntoIterator<Item = &'a I>,
    voters: &BTreeSet<I>,
) -> bool {
    cell.into_iter().all(|m| voters.contains(m))
}

impl QuorumSystem {
    /// The number of acceptors a **Phase-1** quorum over a membership of
    /// `members` takes. Kept, with [`QuorumSystem::phase2_quorum_size`], for
    /// the one thing a predicate cannot report — how many more answers a
    /// pending tally still waits for; whether a tally *holds* is always
    /// [`QuorumSystem::is_phase1_quorum`].
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn phase1_quorum_size(self, members: usize) -> usize {
        let size = match self {
            QuorumSystem::Majority => majority_of(members),
            QuorumSystem::Flexible { q1, .. } => q1,
            // A row: `cols` acceptors wide.
            QuorumSystem::Grid { cols, .. } => cols,
        };
        if let QuorumSystem::Grid { rows, cols } = self
            && rows.checked_mul(cols) == Some(members)
        {
            assert!(size <= members, "a full row fits inside a tiled membership");
        }
        size
    }

    /// The number of acceptors a **Phase-2** quorum over a membership of
    /// `members` takes. See [`QuorumSystem::phase1_quorum_size`].
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn phase2_quorum_size(self, members: usize) -> usize {
        let size = match self {
            QuorumSystem::Majority => majority_of(members),
            QuorumSystem::Flexible { q2, .. } => q2,
            // A column: `rows` acceptors tall.
            QuorumSystem::Grid { rows, .. } => rows,
        };
        if let QuorumSystem::Grid { rows, cols } = self
            && rows.checked_mul(cols) == Some(members)
        {
            assert!(
                size <= members,
                "a full column fits inside a tiled membership"
            );
        }
        size
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
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn cross_intersects(self, members: usize) -> bool {
        let intersects = match self {
            QuorumSystem::Majority | QuorumSystem::Flexible { .. } => {
                self.phase1_quorum_size(members)
                    .saturating_add(self.phase2_quorum_size(members))
                    > members
            }
            QuorumSystem::Grid { .. } => true,
        };
        // Two majorities always meet; a row always meets a column.
        if matches!(self, QuorumSystem::Majority | QuorumSystem::Grid { .. }) {
            assert!(
                intersects,
                "a majority or a grid cross-intersects by construction"
            );
        }
        intersects
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
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn admits(self, members: usize) -> bool {
        let admits = match self {
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
        };
        // Admission is exactly what Paxos safety needs: every admitted system
        // cross-intersects, over a non-empty membership.
        if admits {
            assert!(
                self.cross_intersects(members),
                "an admitted system cross-intersects"
            );
            assert!(members >= 1, "an admitted system has a member");
        }
        admits
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
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn column_of(self, slot: Slot) -> Option<usize> {
        let column = match self {
            QuorumSystem::Majority | QuorumSystem::Flexible { .. } => None,
            QuorumSystem::Grid { cols, .. } => Some(residue(slot.0, cols)),
        };
        if let (QuorumSystem::Grid { cols, .. }, Some(column)) = (self, column) {
            assert!(column < cols.max(1), "a slot's column lies inside the grid");
        } else {
            assert!(column.is_none(), "only a grid names a column");
        }
        column
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
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn is_majority<I: Ord>(members: &[I], voters: &BTreeSet<I>) -> bool {
        let majority = counted(members, voters) >= majority_of(members.len());
        // A majority of a non-empty membership is somebody.
        if majority && !members.is_empty() {
            assert!(!voters.is_empty(), "a majority holds a vote");
        }
        majority
    }

    /// The **row** a quorum read is addressed to under this quorum system
    /// (#143): `ctx % rows` on a [`QuorumSystem::Grid`], `None` — the whole
    /// membership — under a majority or a flexible split, which name no row.
    /// The Phase-1 twin of [`QuorumSystem::column_of`]: a pure function of
    /// the read's token, so the core draws nothing (a driver that wants to
    /// perturb the choice does so through its own hook, never here).
    /// Compartmentalized Paxos §3.4 sends `PreRead` to *a* read quorum; which
    /// one is the reader's choice, and spreading reads over the rows is what
    /// lets the read load scale with the number of rows.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn row_of(self, ctx: u64) -> Option<usize> {
        let row = match self {
            QuorumSystem::Majority | QuorumSystem::Flexible { .. } => None,
            QuorumSystem::Grid { rows, .. } => Some(residue(ctx, rows)),
        };
        if let (QuorumSystem::Grid { rows, .. }, Some(row)) = (self, row) {
            assert!(row < rows.max(1), "a read's row lies inside the grid");
        } else {
            assert!(row.is_none(), "only a grid names a row");
        }
        row
    }

    /// The row a quorum read with token `ctx` goes to when its reader
    /// **names** one (`chosen`, a driver's override): `chosen` itself when it
    /// is a row of this system ([`QuorumSystem::admits_read_row`]), otherwise
    /// the pure default [`QuorumSystem::row_of`]. The Phase-1 twin of a
    /// round's named column: every row is a Phase-1 quorum, so the choice is
    /// always safe, and a choice the system cannot honour (a row under a
    /// majority, one past a grid's last) falls back rather than failing.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn read_row(self, ctx: u64, chosen: Option<usize>) -> Option<usize> {
        let row = match chosen {
            Some(row) if self.admits_read_row(Some(row)) => Some(row),
            _ => self.row_of(ctx),
        };
        // Whatever the caller chose, the row a read runs on is admitted.
        if let QuorumSystem::Grid { rows, .. } = self
            && rows >= 1
        {
            assert!(self.admits_read_row(row), "a read runs on an admitted row");
        }
        row
    }

    /// Whether `row` names a read row of this system: `None` under a
    /// majority or a flexible split (the whole membership), and on a grid
    /// either `None` or a row below `rows`.
    #[must_use]
    pub fn admits_read_row(self, row: Option<usize>) -> bool {
        match (self, row) {
            (_, None) => true,
            (QuorumSystem::Majority | QuorumSystem::Flexible { .. }, Some(_)) => false,
            (QuorumSystem::Grid { rows, .. }, Some(row)) => row < rows,
        }
    }

    /// Whether `voters` form a **Phase-1** quorum over `members`: the
    /// promises an election (or a CTRL repair probe) must hold before it may
    /// conclude anything about what an earlier ballot could have chosen.
    /// Identical to [`QuorumSystem::is_phase2_quorum`] under
    /// [`QuorumSystem::Majority`]; [`QuorumSystem::Flexible`] counts against
    /// `q1` here and `q2` there. A voter outside `members` never counts.
    /// Under a grid this is *any* full row — the row-less form an election
    /// asks; a quorum read addressed to one row is judged by
    /// [`QuorumSystem::is_phase1_quorum_in`] with that row.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn is_phase1_quorum<I: Ord>(self, members: &[I], voters: &BTreeSet<I>) -> bool {
        let quorum = self.is_phase1_quorum_in(members, voters, None);
        // Under a majority the two phases ask the same question.
        if self == QuorumSystem::Majority {
            assert!(
                quorum == Self::is_majority(members, voters),
                "a majority's Phase-1 quorum is a majority"
            );
        }
        quorum
    }

    /// Whether `voters` form a **Phase-1** quorum over `members` **in
    /// `row`**: the form a quorum read asks (#143), judged by the row it
    /// was addressed to. `None` names no row — the whole membership under a
    /// majority or a flexible split, *any* full row under a grid.
    ///
    /// # Panics
    ///
    /// If a row is named under a majority or a flexible split: neither has
    /// rows, so a caller that names one holds a read opened against a
    /// different quorum system than the one it judges by — a programmer
    /// error, never wire input (the row is derived by
    /// [`QuorumSystem::row_of`] from the same configuration).
    #[must_use]
    pub fn is_phase1_quorum_in<I: Ord>(
        self,
        members: &[I],
        voters: &BTreeSet<I>,
        row: Option<usize>,
    ) -> bool {
        let quorum = match self {
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
        };
        // A quorum is never assembled out of strangers alone.
        if quorum && !members.is_empty() {
            assert!(
                counted(members, voters) >= 1,
                "a Phase-1 quorum holds a member's vote"
            );
        }
        quorum
    }

    /// Whether `node` is one of the acceptors a Phase-1 message in `row` is
    /// addressed to — the guard a quorum read's tally applies to a
    /// `PreReadAck` before counting it. A member of the configuration under
    /// a majority or a flexible split (`row` is `None` there); a member of
    /// exactly that row under a grid. A node outside `members` is never an
    /// addressee.
    ///
    /// # Panics
    ///
    /// If a row is named under a majority or a flexible split (see
    /// [`QuorumSystem::is_phase1_quorum_in`]).
    #[must_use]
    pub fn is_phase1_addressee<I: Ord>(self, members: &[I], node: &I, row: Option<usize>) -> bool {
        let Ok(position) = members.binary_search(node) else {
            return false;
        };
        match self {
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

    /// The acceptors a Phase-1 message in `row` is addressed to, out of
    /// `members`, in membership order — the read-side twin of
    /// [`QuorumSystem::phase2_addressees`]. A majority or a flexible split
    /// addresses the whole membership; a grid addresses **one row** — the
    /// one [`QuorumSystem::row_of`] derived for the read — so every acceptor
    /// answers `1 / rows` of the reads (Compartmentalized Paxos §3.4). `None`
    /// under a grid addresses every row: an election's `Prepare` goes to the
    /// whole membership, since which row will be whole is not known ahead.
    ///
    /// # Panics
    ///
    /// If a row is named under a majority or a flexible split (see
    /// [`QuorumSystem::is_phase1_quorum_in`]).
    #[must_use]
    pub fn phase1_addressees<I: Copy>(self, members: &[I], row: Option<usize>) -> Vec<I> {
        let addressees = match self {
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
        };
        assert!(
            addressees.len() <= members.len(),
            "a fan-out addresses only members"
        );
        if row.is_none() {
            assert!(
                addressees.len() == members.len(),
                "an unnamed row addresses everyone"
            );
        }
        addressees
    }

    /// Whether `voters` form a **Phase-2** quorum over `members`: the accepts
    /// that choose a value, and every claim that rests on one — a leader's
    /// standing authority (`CheckQuorum`) and the GC fence's custody claim. A voter outside `members` never counts. Under a
    /// grid this is *any* full column — the column-less form the standing
    /// claims ask; a round that was addressed to one column is judged by
    /// [`QuorumSystem::is_phase2_quorum_in`] with that column.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn is_phase2_quorum<I: Ord>(self, members: &[I], voters: &BTreeSet<I>) -> bool {
        let quorum = self.is_phase2_quorum_in(members, voters, None);
        if self == QuorumSystem::Majority {
            assert!(
                quorum == Self::is_majority(members, voters),
                "a majority's Phase-2 quorum is a majority"
            );
        }
        quorum
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
        let quorum = match self {
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
        };
        // A quorum is never assembled out of strangers alone.
        if quorum && !members.is_empty() {
            assert!(
                counted(members, voters) >= 1,
                "a Phase-2 quorum holds a member's vote"
            );
        }
        quorum
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
        let addressees: Vec<I> = match self {
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
        };
        assert!(
            addressees.len() <= members.len(),
            "a fan-out addresses only members"
        );
        if column.is_none() {
            assert!(
                addressees.len() == members.len(),
                "an unnamed column addresses everyone"
            );
        }
        addressees
    }
}

#[cfg(test)]
mod tests {
    use super::QuorumSystem;

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
}
