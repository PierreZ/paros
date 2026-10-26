//! The quorum system a seed draws: grids, zones, the bootstrap configuration and the matchmaker floor.

use std::sync::PoisonError;

use moonpool_sim::{StateHandle, assert_reachable, buggify_knob};
use paros::core::{FailureDomain, NodeId, QuorumSystem};

use crate::shape::registry;
use crate::shape::scenarios::{departed_straggler, slow_link, stalled_proxy};

/// The run's **quorum-system policy** (#140, #141): how every configuration
/// the run puts in force is judged, drawn once per seed and applied to each
/// configuration's own size. Protocol data like the bootstrap ranks — a
/// majority is the plain deployment's system and the default; a flexible
/// split and an acceptor grid are the opt-ins the swarm turns on per seed,
/// so one campaign proves the library under all three.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum QuorumPolicy {
    /// A majority in both phases.
    Majority,
    /// Flexible Paxos's simple quorums with `q2` accepts deciding and
    /// `q1 = n - q2 + 1` promises electing — the tightest split that still
    /// cross-intersects. `q2` is clamped to `1..=n/2` per configuration, so a
    /// successor of a different size runs the same *kind* of split at its
    /// own dimensions.
    Flexible {
        /// The Phase-2 quorum size drawn at the pool, before clamping.
        q2: usize,
    },
    /// Compartmentalized Paxos's acceptor grid (#141): rows elect, columns
    /// decide, and every slot's `Accept` goes to one column. The layout is
    /// drawn at the pool; a configuration of another size runs the grid
    /// layout closest to it that tiles it ([`grid_layouts`]), or a majority
    /// when no layout does — a successor on a grid seed is grid-shaped or
    /// switches system, never malformed.
    Grid {
        /// The rows drawn at the pool.
        rows: usize,
        /// The columns drawn at the pool.
        cols: usize,
        /// How many acceptors of a column may miss a slot's Phase 2 (#532
        /// (acceptor partitions)): a column decides on `rows - slack`
        /// accepts, at least one. Zero is the full-column grid; `1` on a
        /// three-row grid is the acceptor partitions `parosctl` offers.
        slack: usize,
    },
}

/// Every grid layout of `n` acceptors the harness may run: `rows × cols ==
/// n` with **both `rows >= 2` and `cols >= 2`** — the knob's floor. A
/// `1 × n` grid is Flexible Paxos's `|Q1| = 1, |Q2| = n` thought experiment
/// and an `n × 1` grid its mirror: valid configurations on paper and
/// permanent partitions under attrition (one dead acceptor stops Phase 2,
/// or Phase 1, for the rest of the run), which is the defeat of eventual
/// synchrony the knob doctrine forbids. In row-major order of `rows`, so the
/// list is a pure function of `n` every process derives alike. Empty when
/// `n` is prime or below four.
pub(crate) fn grid_layouts(n: usize) -> Vec<(usize, usize)> {
    (2..n)
        .filter(|rows| n.is_multiple_of(*rows) && n / rows >= 2)
        .map(|rows| (rows, n / rows))
        .collect()
}

impl QuorumPolicy {
    /// The quorum system a configuration of `n` acceptors runs under this
    /// policy. **Floor:** `q1 + q2 = n + 1 > n` by construction and
    /// `q2 >= 1`; the extreme `q2 = 1` is Flexible Paxos's "any single
    /// acceptor learns a value in one hop", a valid configuration because
    /// the fault window closes before the recovery tail does and the copy
    /// budget ([`StorageWorld::set_budget`]) is sized over the split's
    /// Phase-1 quorum. `n = 1` and `n = 2` admit only `q2 = 1`. A grid
    /// policy lays the drawn layout over `n` when it tiles it, else the
    /// layout of `n` with the same row count, else the first that tiles it,
    /// else a majority (see [`grid_layouts`] for the floor).
    ///
    /// [`StorageWorld::set_budget`]: crate::world::StorageWorld::set_budget
    pub(crate) fn system(self, n: usize) -> QuorumSystem {
        match self {
            QuorumPolicy::Majority => QuorumSystem::Majority,
            QuorumPolicy::Flexible { q2 } => {
                let q2 = q2.clamp(1, (n / 2).max(1));
                QuorumSystem::Flexible {
                    q1: n.saturating_sub(q2).saturating_add(1),
                    q2,
                }
            }
            QuorumPolicy::Grid { rows, cols, slack } => {
                let layouts = grid_layouts(n);
                layouts
                    .iter()
                    .find(|(r, c)| *r == rows && *c == cols)
                    .or_else(|| layouts.iter().find(|(r, _)| *r == rows))
                    .or_else(|| layouts.first())
                    .map_or(QuorumSystem::Majority, |(rows, cols)| QuorumSystem::Grid {
                        rows: *rows,
                        cols: *cols,
                        acks: rows.saturating_sub(slack).max(1),
                    })
            }
        }
    }

    /// The copies of a record a configuration of `n` may lose under this
    /// policy and stay winnable: `n` minus the larger of the two phase
    /// quorums, read off the membership boundary, never re-derived from a
    /// count here — `⌊(n-1)/2⌋` under a majority, `q2 - 1` under a flexible
    /// split (`n - q1`: a faulty slot is decidable only once a full Phase-1
    /// quorum of clean answers holds, CTRL R2/R3, and a value is choosable
    /// only while a Phase-2 quorum is live, so both quorums must keep clean
    /// members). Under a grid it is **zero**, whatever the layout: the
    /// record-recoverability bound would be `⌊(m-1)/2⌋` over `m = min(rows,
    /// cols)` (that many losses break strictly fewer rows and columns than
    /// the grid has), but the same budget bounds the *permanent* losses —
    /// a corruption park, a wipe — and every slot's Phase 2 is addressed to
    /// one column, so one acceptor dead for good freezes its column's
    /// slots for the rest of the run: an unwinnable run, which no budget
    /// may permit. For the grids the pool range admits (`2 × 2`, `2 × 3`,
    /// `3 × 2`) the two bounds coincide anyway.
    pub(crate) fn tolerated_loss(self, n: usize) -> usize {
        match self.system(n) {
            QuorumSystem::Grid { .. } => 0,
            system => n.saturating_sub(
                system
                    .phase1_quorum_size(n)
                    .max(system.phase2_quorum_size(n)),
            ),
        }
    }

    /// The clean live copies a record must keep at the run's configuration
    /// floor `floor` so that **every** configuration the run may put in
    /// force — every size in `floor..=pool` — stays winnable: the floor
    /// minus the smallest loss any of those sizes tolerates
    /// ([`QuorumPolicy::tolerated_loss`]). Under a majority or a flexible
    /// split the tolerated loss only grows with `n` (the split fixes `q2`),
    /// so this is the floor configuration's own `max(q1, q2)` — `⌊n/2⌋ + 1`
    /// under a majority, `q1` under the split. Under a grid it is not
    /// monotone (a three-member successor is a majority tolerating one
    /// loss, a four-member one a `2 × 2` grid tolerating none), so the
    /// minimum over the range is what the budget keeps; on every grid seed
    /// that is the whole floor — no storage-fault injection and no park,
    /// the cost the grid pays for its `1 / cols`.
    pub(crate) fn clean_copies(self, floor: usize, pool: usize) -> usize {
        let loss = (floor..=pool.max(floor))
            .map(|n| self.tolerated_loss(n))
            .min()
            .unwrap_or(0);
        floor.saturating_sub(loss)
    }
}

/// Whether the run draws the **wide-grid scenario** (#533 (move a stuck
/// round)): drawn once per seed, its own BUGGIFY location. A stuck grid
/// round skips a column only on a grid of three columns or more, which the
/// pool tiles only at six acceptors (`2 × 3`), and only once the grid knob
/// and the layout pick both line up: the gate "grid: the driver skips a
/// column when it moves a round" fired 57 times in 500 hunt seeds and never
/// in the CI sweep's 400. On a scenario seed `quorum_policy` runs the
/// widest grid of three columns or more the pool tiles, with full columns,
/// and every node moves a stuck round after 1 to 3 re-sends (the
/// `partition_move_resends` extreme). Each ingredient keeps its own coin on the other seeds.
/// Rare-but-valid: both are configurations an operator may choose.
#[tracing::instrument(level = "debug", skip_all)]
pub(crate) fn wide_grid(state: &StateHandle) -> bool {
    let registry = registry(state);
    let mut guard = registry.lock().unwrap_or_else(PoisonError::into_inner);
    *guard.wide_grid.get_or_insert_with(|| {
        let wide = moonpool_sim::buggify_with_prob!(0.5);
        if wide {
            assert_reachable!("a run draws the wide-grid scenario");
        }
        wide
    })
}

/// How many zones label the acceptor pool (#215 (zones)): drawn once per
/// seed, 0 (an unlabeled pool, the plain arm) on two seeds in three, else
/// 2 or 3. On a labeled seed acceptor `n` stands in zone `zone-<n % zones>`
/// ([`acceptor_domain`]): the bootstrap configuration and every
/// configuration the composer proposes carry those labels, so a labeled
/// configuration crosses the wire, the matchmakers and the stores like an
/// unlabeled one. No tally reads a label, so the coin changes no outcome.
#[tracing::instrument(level = "debug", skip_all)]
pub(crate) fn acceptor_zones(state: &StateHandle) -> u64 {
    let registry = registry(state);
    let mut guard = registry.lock().unwrap_or_else(PoisonError::into_inner);
    *guard.acceptor_zones.get_or_insert_with(|| {
        if moonpool_sim::sim_random_range(0_u32..3_u32) == 0 {
            assert_reachable!("zones: a run labels its acceptor pool");
            moonpool_sim::sim_random_range(2_u64..4_u64)
        } else {
            0
        }
    })
}

/// The failure domain of acceptor `node` on a labeled seed (#215 (zones)):
/// `zone-<node % zones>`; `None` on an unlabeled seed.
pub(crate) fn acceptor_domain(state: &StateHandle, node: NodeId) -> Option<FailureDomain> {
    let zones = acceptor_zones(state);
    (zones > 0).then(|| FailureDomain::new("", format!("zone-{}", node.0 % zones)))
}

/// The failure domains of `peers`, in order, on a labeled seed; empty on an
/// unlabeled one: a [`paros::core::Config`]'s `peer_domains`.
pub(crate) fn peer_domains(state: &StateHandle, peers: &[NodeId]) -> Vec<FailureDomain> {
    peers
        .iter()
        .filter_map(|node| acceptor_domain(state, *node))
        .collect()
}

/// `members` under `system`, labeled with their failure domains on a
/// labeled seed (#215 (zones)): the one way the harness composes an
/// acceptor configuration.
pub(crate) fn acceptor_config(
    state: &StateHandle,
    members: Vec<NodeId>,
    system: QuorumSystem,
) -> paros::core::AcceptorConfig {
    if acceptor_zones(state) == 0 {
        return paros::core::AcceptorConfig::new(members, system);
    }
    let labeled = members
        .into_iter()
        .map(|node| {
            let domain = acceptor_domain(state, node).expect("a labeled seed labels every node");
            (node, domain)
        })
        .collect();
    paros::core::AcceptorConfig::zoned(labeled, system)
}

/// The run's **quorum-system policy** (#140, #141), drawn once per seed by
/// whichever process or workload asks first and handed back unchanged to
/// every later caller (an attrition restart must boot the same node under
/// the same system, and every client must compose successors under it).
///
/// The default is the majority, the plain deployment's system. A seed
/// may instead draw a **flexible split**: one `buggify_knob!` location
/// for `q2` over the pool (default the majority, extreme `1..=pool/2`), with
/// `q1 = n - q2 + 1` derived per configuration ([`QuorumPolicy::system`]);
/// or, on a pool whose size tiles a grid, an **acceptor grid**: its own
/// `buggify_knob!` location over the layouts of the pool ([`grid_layouts`],
/// floor `rows >= 2` and `cols >= 2`; default no grid), so a seed can be
/// extreme in one system and never the other. Both are opt-in configuration
/// data on the core side, so a majority seed is byte-identical to a run
/// before the policy existed.
#[tracing::instrument(level = "debug", skip(state), fields(pool))]
pub(crate) fn quorum_policy(state: &StateHandle, pool: usize) -> QuorumPolicy {
    let stalled = stalled_proxy(state);
    let slow = slow_link(state);
    let wide = wide_grid(state);
    let registry = registry(state);
    let mut guard = registry.lock().unwrap_or_else(PoisonError::into_inner);
    *guard.quorum.get_or_insert_with(|| {
        let majority = pool / 2 + 1;
        if pool < 2 {
            return QuorumPolicy::Majority;
        }
        // The stalled-proxy scenario (#341) runs a grid wherever the pool
        // tiles one, so its take-back runs on a column.
        let layouts = grid_layouts(pool);
        if stalled && !layouts.is_empty() {
            let pick = moonpool_sim::sim_random_range(0..layouts.len());
            let (rows, cols) = layouts[pick];
            assert_reachable!("a run draws an acceptor grid");
            return QuorumPolicy::Grid {
                rows,
                cols,
                slack: grid_slack(),
            };
        }
        // The wide-grid scenario (#533 (move a stuck round)) runs the
        // widest grid of three columns or more, so a stuck round may skip
        // a column. Never on a slow-link seed, which reads from every
        // acceptor.
        let widest = layouts
            .iter()
            .filter(|(_, cols)| *cols >= 3)
            .max_by_key(|(_, cols)| *cols);
        if wide
            && !slow
            && let Some(&(rows, cols)) = widest
        {
            assert_reachable!("a run draws an acceptor grid");
            // Full columns: a column with one acceptor down holds its
            // rounds until they move.
            return QuorumPolicy::Grid {
                rows,
                cols,
                slack: 0,
            };
        }
        let q2 = buggify_knob!(majority, 1_usize..(pool / 2 + 1));
        // A slow-link seed reads from every acceptor: `q1 = n` waits on the
        // slowest link (#386).
        let q2 = if slow { 1 } else { q2 };
        if q2 != majority {
            // BUGGIFY pairing: a seed genuinely runs a flexible split (a
            // cause, never a `sometimes`; the outcomes — a slot decided by
            // fewer accepts than a majority, an election completed under the
            // split — are the audit's gates).
            assert_reachable!("a run draws a flexible quorum system");
            return QuorumPolicy::Flexible { q2 };
        }
        // The grid knob, its own location: index 0 is "no grid", `k` the
        // `k`-th layout of the pool. A pool that tiles no grid (three or
        // five nodes) draws nothing here and stays a majority.
        if layouts.is_empty() {
            return QuorumPolicy::Majority;
        }
        let pick = buggify_knob!(0_usize, 1_usize..layouts.len() + 1);
        let Some((rows, cols)) = pick.checked_sub(1).and_then(|k| layouts.get(k).copied()) else {
            return QuorumPolicy::Majority;
        };
        // BUGGIFY pairing: a seed genuinely runs an acceptor grid (a cause;
        // the outcomes — a slot decided on a column, an election covered by
        // a row — are the audit's gates).
        assert_reachable!("a run draws an acceptor grid");
        QuorumPolicy::Grid {
            rows,
            cols,
            slack: grid_slack(),
        }
    })
}

/// A grid seed's column slack (#532 (acceptor partitions)): its own
/// `buggify_knob!` location, default 0 (the full-column grid), extreme 1–2
/// (a column decides on fewer than all its acceptors; on a three-row grid
/// slack 1 is a majority of the partition). **Floor:** a column decides on
/// at least one accept, clamped in [`QuorumPolicy::system`]; a Phase 1
/// then needs every acceptor of every column, a valid configuration
/// because the fault window closes before the recovery tail does.
pub(crate) fn grid_slack() -> usize {
    let slack = buggify_knob!(0_usize, 1_usize..3);
    if slack > 0 {
        // BUGGIFY pairing: a seed genuinely runs columns that decide on
        // fewer than all their acceptors.
        assert_reachable!("a run draws acceptor partitions");
    }
    slack
}

/// The smallest acceptor configuration a matchmaker seed ever puts in force
/// — the bootstrap never draws below it and no reconfiguration shrinks below
/// it. Not a tunable: it is the size the storage world's copy budget is
/// computed over on such a seed (see [`config_floor`]). Three is the smallest
/// set where one loss keeps a quorum (the dead-node budget's own floor).
pub(crate) const MIN_BOOTSTRAP: usize = 3;

/// The **configuration floor** of a run, and the size the storage world's
/// copy budget (`StorageWorld::set_budget`) is computed over. On a plain
/// deployment the membership is fixed at the whole pool, so the budget is
/// sized by it, exactly as before matchmakers existed. On a matchmaker
/// deployment the acceptor set may shrink as far as [`MIN_BOOTSTRAP`], and the
/// budget is sized by *that*: a budget that keeps the clean copies the
/// policy demands over every size the run may put in force
/// ([`QuorumPolicy::clean_copies`] takes the floor *and* the pool: the
/// tolerated loss `n - max(q1, q2)` only grows with `n` under a policy that
/// fixes `q2`, but a grid's `⌊(min(rows, cols) - 1)/2⌋` does not, so the
/// smallest loss over the range is what the budget keeps), so it stays
/// conservative through every reconfiguration at the cost of fewer
/// storage-fault injections on the larger matchmaker seeds — and, under a
/// flexible split at a three-node floor (`q2 = 1`, `q1 = 3`) or on any
/// grid seed, of none at all.
pub(crate) fn config_floor(pool: usize, has_matchmakers: bool) -> usize {
    if has_matchmakers {
        MIN_BOOTSTRAP.min(pool)
    } else {
        pool
    }
}

/// The run's **bootstrap acceptor ranks** — membership as protocol data,
/// drawn once per seed by whichever process or workload asks first and
/// handed back unchanged to every later caller (an attrition restart must
/// boot the same node into the same bootstrap configuration).
///
/// The default is the whole pool: every node an acceptor, the plain
/// Multi-Paxos deployment and the shape of every existing axis. A seed that
/// deploys matchmakers may instead draw a **subset** (a run with
/// `has_matchmakers == false` never draws — a plain deployment's membership
/// must include every node, per `paros::core::Config::peers`), leaving the other
/// nodes as *spares*: addressable pool members outside every configuration
/// until a `Reconfigure` pulls them in. Two knob locations, each its own
/// per-seed activation: the subset *size* (floor [`MIN_BOOTSTRAP`], ceiling
/// the pool) and the *rotation* that decides which ranks are the spares, so
/// a seed can bootstrap on `{2, 3, 4}` of a five-node pool and leave
/// `{0, 1}` — the lowest ranks, the ones every "first node" heuristic would
/// pick — outside.
#[tracing::instrument(level = "debug", skip(state), fields(pool, has_matchmakers))]
pub(crate) fn bootstrap_ranks(state: &StateHandle, pool: usize, has_matchmakers: bool) -> Vec<u64> {
    let scenario = departed_straggler(state);
    let registry = registry(state);
    let mut guard = registry.lock().unwrap_or_else(PoisonError::into_inner);
    guard
        .bootstrap
        .get_or_insert_with(|| {
            let all: Vec<u64> = (0..pool)
                .map(|i| u64::try_from(i).unwrap_or(u64::MAX))
                .collect();
            if !(has_matchmakers && pool > MIN_BOOTSTRAP) {
                return all;
            }
            // The departed-straggler scenario bootstraps at the floor,
            // leaving every other node a spare: the owner's removal right
            // after its claim is a rotation onto them, as whole as they
            // allow, so the successor is mostly members that never held
            // a departed member's slot (#267).
            let size = if scenario {
                MIN_BOOTSTRAP
            } else {
                buggify_knob!(pool, MIN_BOOTSTRAP..pool)
            };
            if size == pool {
                return all;
            }
            // BUGGIFY pairing: a seed genuinely bootstraps on a subset.
            assert_reachable!("a run bootstraps on a subset of the node pool, leaving spares");
            let rotation = buggify_knob!(0_usize, 1_usize..pool);
            if rotation != 0 {
                // BUGGIFY pairing: the spares are not simply the highest ranks.
                assert_reachable!("a run's spares are drawn from the low ranks");
            }
            let mut ranks: Vec<u64> = (0..size)
                .map(|i| u64::try_from((rotation + i) % pool).unwrap_or(u64::MAX))
                .collect();
            ranks.sort_unstable();
            ranks
        })
        .clone()
}

/// The run's **bootstrap matchmaker ranks** (#125): generation 0's set, drawn
/// once per seed by whichever process or workload asks first. The default is
/// the whole matchmaker pool; a seed with two or more matchmakers
/// may draw a **subset** (any size from one up — a one- or two-member set is
/// a valid registry that tolerates no loss), leaving the rest as matchmaker
/// *spares* a `ReconfigureMatchmakers` pulls in. Two knob locations, as for
/// the acceptors: the subset size and the rotation that picks the spares.
#[tracing::instrument(level = "debug", skip(state), fields(pool))]
pub(crate) fn matchmaker_bootstrap_ranks(state: &StateHandle, pool: usize) -> Vec<u64> {
    let registry = registry(state);
    let mut guard = registry.lock().unwrap_or_else(PoisonError::into_inner);
    guard
        .matchmaker_bootstrap
        .get_or_insert_with(|| {
            let all: Vec<u64> = (0..pool)
                .map(|i| u64::try_from(i).unwrap_or(u64::MAX))
                .collect();
            if pool < 2 {
                return all;
            }
            let size = buggify_knob!(pool, 1_usize..pool);
            if size == pool {
                return all;
            }
            // BUGGIFY pairing: a seed genuinely bootstraps its matchmakers on
            // a subset, leaving matchmaker spares.
            assert_reachable!(
                "a run bootstraps on a subset of the matchmaker pool, leaving spares"
            );
            let rotation = buggify_knob!(0_usize, 1_usize..pool);
            if rotation != 0 {
                // BUGGIFY pairing: the matchmaker spares are not simply the
                // highest ranks.
                assert_reachable!("a run's matchmaker spares are drawn from the low ranks");
            }
            let mut ranks: Vec<u64> = (0..size)
                .map(|i| u64::try_from((rotation + i) % pool).unwrap_or(u64::MAX))
                .collect();
            ranks.sort_unstable();
            ranks
        })
        .clone()
}

/// The smallest bootstrap matchmaker set that can lose one member and keep a
/// quorum — and therefore the smallest set the world will ever take a
/// matchmaker from ([`StorageWorld::wipe_matchmaker`]) and the ceiling of
/// [`matchmaker_floor`]. Not a tunable: it is what the matchmaker-loss budget
/// is computed over.
///
/// [`StorageWorld::wipe_matchmaker`]: crate::world::StorageWorld::wipe_matchmaker
pub(crate) const MATCHMAKER_LOSS_FLOOR: usize = 3;

/// The smallest matchmaker set a run may put in force (#125):
/// [`MATCHMAKER_LOSS_FLOOR`] where the bootstrap set has that many members or
/// more, else the bootstrap size itself.
pub(crate) fn matchmaker_floor(bootstrap: usize) -> usize {
    bootstrap.min(MATCHMAKER_LOSS_FLOOR)
}
