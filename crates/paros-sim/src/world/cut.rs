//! The budget of a power loss **inside** a journal commit (#176, #294).
//!
//! The shipped stores name the moments of a commit with `hint!`:
//! `moonpool-journal` inside one commit (entries written, records written,
//! metainfo stale), and `paros::journal` between the commits of one sync.
//! The seed's attrition regime decides whether the process dies there. The
//! harness only keeps the budget the simulator cannot see, through
//! moonpool's [`HintVeto`]: a `Batched` commit cut partway may leave its
//! last batch ambiguous, so its entries come back faulty, a lost copy of
//! every slot it wrote. The cut acceptors are budgeted like rot
//! ([`StorageWorld::permit_power_cut`]), and an ambiguous registration is
//! the run's one matchmaker loss
//! ([`StorageWorld::permit_matchmaker_power_cut`]).
//!
//! A store registers each commit that writes while it is in flight
//! ([`InFlight`]). A hint kills the whole process, so the veto permits a
//! kill only if every commit the process has in flight fits its budget,
//! and then spends each budget: a yes is a kill.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError, Weak};

use moonpool_sim::{HintVeto, StateHandle, assert_reachable};

use super::StorageWorld;

/// The [`StateHandle`] key of the run's [`Commits`].
const COMMITS_KEY: &str = "paros-commits-in-flight";

/// Whose store a commit belongs to: each one's cut is its own reachable.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Owner {
    /// An acceptor's or a replica's `JournalStorage`.
    Node,
    /// A matchmaker's `JournalMatchmakerStorage`.
    Matchmaker,
}

/// What a cut of one commit spends.
#[derive(Clone, Debug)]
pub(crate) enum Budget {
    /// Nothing: an `Ordered` commit is torn or whole, never ambiguous.
    Free,
    /// An acceptor's `Batched` commit writing `slots`: at most `tolerated`
    /// distinct cut acceptors per journal, and a lost copy of each slot
    /// inside the per-record budget (#331).
    Node { tolerated: usize, slots: Vec<u64> },
    /// A matchmaker's commit: on a `Batched` registry, over a bootstrap set
    /// of `bootstrap` members (`None` on an `Ordered` one, spending
    /// nothing); `held` is whether the registry held a registration, which
    /// its next boot then judges.
    Matchmaker {
        bootstrap: Option<usize>,
        held: bool,
    },
}

/// One commit in flight.
struct Commit {
    ip: String,
    owner: Owner,
    budget: Budget,
    world: Weak<Mutex<StorageWorld>>,
}

/// The run's commits in flight, by registration number.
#[derive(Default)]
pub(crate) struct Commits {
    next: u64,
    open: BTreeMap<u64, Commit>,
}

/// The run's [`Commits`], publishing the veto that reads them on the first
/// ask.
fn commits(state: &StateHandle) -> Arc<Mutex<Commits>> {
    if let Some(commits) = state.get::<Arc<Mutex<Commits>>>(COMMITS_KEY) {
        return commits;
    }
    let commits = crate::state::published(state, COMMITS_KEY, Commits::default);
    let veto = Arc::clone(&commits);
    HintVeto::new(move |ip, _label| {
        veto.lock()
            .unwrap_or_else(PoisonError::into_inner)
            .permit_kill(ip)
    })
    .publish(state);
    commits
}

impl Commits {
    /// Whether the process at `ip` may die now, spending the budget of every
    /// commit it has in flight if so.
    fn permit_kill(&self, ip: &str) -> bool {
        let open: Vec<(&Commit, Arc<Mutex<StorageWorld>>)> = self
            .open
            .values()
            .filter(|commit| commit.ip == ip)
            .filter_map(|commit| commit.world.upgrade().map(|world| (commit, world)))
            .collect();
        let fits = open.iter().all(|(commit, world)| {
            let world = world.lock().unwrap_or_else(PoisonError::into_inner);
            match &commit.budget {
                Budget::Free => true,
                Budget::Node { tolerated, slots } => world.may_cut_node(ip, *tolerated, slots),
                Budget::Matchmaker { bootstrap, .. } => {
                    bootstrap.is_none_or(|bootstrap| world.may_cut_matchmaker(ip, bootstrap))
                }
            }
        });
        if !fits {
            assert_reachable!("journal store: the copy budget refuses a cut mid-commit");
            return false;
        }
        for (commit, world) in &open {
            let mut world = world.lock().unwrap_or_else(PoisonError::into_inner);
            match &commit.budget {
                Budget::Free => {}
                Budget::Node { tolerated, slots } => {
                    let spent = world.permit_power_cut(ip, *tolerated, slots);
                    assert!(spent, "a cut that fits the budget spends it");
                }
                Budget::Matchmaker { bootstrap, held } => {
                    if let Some(bootstrap) = *bootstrap {
                        let spent = world.permit_matchmaker_power_cut(ip, bootstrap);
                        assert!(spent, "a cut that fits the budget spends it");
                    }
                    if *held {
                        world.note_registry_cut(ip);
                    }
                }
            }
            // Paired with the recovery gates the journal reports at the
            // next boot.
            match commit.owner {
                Owner::Node => {
                    assert_reachable!("journal store: a node loses power mid-commit");
                }
                Owner::Matchmaker => {
                    assert_reachable!("journal store: a matchmaker loses power mid-commit");
                }
            }
        }
        true
    }
}

/// A commit registered in flight until this guard drops: when the sync
/// returns, or when a kill drops the task that awaited it.
pub(crate) struct InFlight {
    commits: Arc<Mutex<Commits>>,
    id: u64,
}

impl InFlight {
    /// Register a commit of `owner`'s store at `ip`, judged against
    /// `world`'s budget.
    pub(crate) fn open(
        state: &StateHandle,
        ip: &str,
        owner: Owner,
        budget: Budget,
        world: Weak<Mutex<StorageWorld>>,
    ) -> Self {
        let commits = commits(state);
        let id = {
            let mut guard = commits.lock().unwrap_or_else(PoisonError::into_inner);
            let id = guard.next;
            guard.next += 1;
            guard.open.insert(
                id,
                Commit {
                    ip: ip.to_string(),
                    owner,
                    budget,
                    world,
                },
            );
            id
        };
        Self { commits, id }
    }
}

impl Drop for InFlight {
    fn drop(&mut self) {
        self.commits
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .open
            .remove(&self.id);
    }
}
