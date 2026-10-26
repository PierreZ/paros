//! The budget of a power loss **inside** a journal commit (#176, #294).
//!
//! The shipped stores name the moments of a commit with `hint!`:
//! `moonpool-journal` inside one commit (entries written, records written,
//! metainfo stale), and `paros::storage::journal` between the commits of one sync.
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
//! and then spends each budget: a yes is a kill. Moonpool asks the same
//! veto before an attrition regime's timed reboot (moonpool#332): a timed
//! reboot lands inside a commit too, and under whole-run chaos one tore a
//! matchmaker's one-sync registry commit outside its budget (#454 (control
//! plane meets chaos), witness seed 6698767524067190541).
//!
//! A machine's journals run on a bare disk, with no store to register
//! through (#294). A created journal stores `Batched` when its create asked
//! for it (#599 (durability mode per journal)), so the driver's own report
//! of each commit registers it ([`open_machine_commit`] from
//! `Audit::commit_started`, [`close_machine_commit`] from
//! `Audit::commit_ended`), under the journal's budget: at most as many
//! distinct machines cut inside its commits over the run as copies of one
//! slot its configurations may lose (the tenant board's `cut_budget`). A
//! permitted kill closes every commit the dead process had in flight.
//!
//! A correlated outage kills every acceptor and proxy leader at one instant
//! (#332): moonpool's `Chaos::Outage` asks the published [`OutageVeto`],
//! and the harness's own outages ask [`outage_permitted`] before they
//! strike. An outage plans its own losses (`super::outage::plan_losses`)
//! and the audit excuses only those, so it waits until no victim has a
//! commit in flight whose cut would spend a budget. Each budget is
//! judged one process at a time, so a set of cuts is never judged
//! together. A commit a victim opens after the yes, before its kill one
//! tick later, has no completed write yet, so the kill leaves it absent,
//! never ambiguous.
//!
//! A failed sync does not end its commit: on moonpool's disk the batch
//! stays in the file image, volatile, until a later sync makes it durable.
//! A kill can still tear it into an ambiguous last batch. So a store whose
//! sync failed parks its commit ([`InFlight::park`]) until the store opens
//! or syncs again ([`release_parked`]), and the veto keeps judging it.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, PoisonError, Weak};

use paros::core::JournalIdentifier;

use moonpool_sim::{HintVeto, OutageVeto, StateHandle, assert_always, assert_reachable};

use super::StorageWorld;
use super::scenarios::thin_sets::ThinSets;

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
    /// its next boot then judges. `thin` names the matchmaker among the
    /// run's thin sets, read when the kill is decided (#496): a member of a
    /// set that cannot spare it is never cut.
    Matchmaker {
        bootstrap: Option<usize>,
        held: bool,
        thin: (ThinSets, u64),
    },
}

impl Budget {
    /// Whether a cut of this commit spends a budget.
    fn spends(&self) -> bool {
        match self {
            Budget::Free => false,
            Budget::Node { .. } => true,
            Budget::Matchmaker {
                bootstrap, held, ..
            } => bootstrap.is_some() || *held,
        }
    }
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
    /// A machine's commit in flight of a `Batched` journal a tenant created
    /// (#599 (durability mode per journal)), by `(ip, journal)`: the copies
    /// of a slot the journal may lose.
    machine: BTreeMap<(String, JournalIdentifier), usize>,
    /// Per `Batched` journal, the machines a kill cut inside its commits.
    machine_cuts: BTreeMap<JournalIdentifier, BTreeSet<String>>,
    /// The commits a failed sync left in the file image, by store key: in
    /// flight until that store opens or syncs again.
    parked: BTreeMap<String, u64>,
}

/// The run's [`Commits`], publishing the veto that reads them on the first
/// ask.
fn commits(state: &StateHandle) -> Arc<Mutex<Commits>> {
    if let Some(commits) = state.get::<Arc<Mutex<Commits>>>(COMMITS_KEY) {
        return commits;
    }
    let commits = crate::state::published(state, COMMITS_KEY, Commits::default);
    let veto = Arc::clone(&commits);
    let run = state.clone();
    HintVeto::new(move |ip, _label| {
        let mut commits = veto.lock().unwrap_or_else(PoisonError::into_inner);
        !commits.holds_machine(&run, ip) && commits.permit_kill(ip)
    })
    .publish(state);
    let veto = Arc::clone(&commits);
    OutageVeto::new(move |victims| {
        veto.lock()
            .unwrap_or_else(PoisonError::into_inner)
            .permit_outage(victims)
    })
    .publish(state);
    commits
}

/// Whether an outage over `victims` may strike now (see the module doc).
/// It spends nothing: a permitted outage cuts no budgeted commit.
pub(crate) fn outage_permitted(state: &StateHandle, victims: &[String]) -> bool {
    state
        .get::<Arc<Mutex<Commits>>>(COMMITS_KEY)
        .is_none_or(|commits| {
            commits
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .permit_outage(victims)
        })
}

/// Assert that an outage striking `victims` now cuts no budgeted commit:
/// the pair of [`outage_permitted`], checked where the kills are decided.
pub(crate) fn assert_outage_permitted(state: &StateHandle, victims: &[String]) {
    assert_always!(
        outage_permitted(state, victims),
        "outage: no victim has a budgeted commit in flight"
    );
}

impl Commits {
    /// Whether every one of `victims` may die now with no budget spent: no
    /// victim has a commit in flight whose cut a budget would have to pay.
    fn permit_outage(&self, victims: &[String]) -> bool {
        let blocked = self
            .open
            .values()
            .filter(|commit| victims.contains(&commit.ip))
            .filter(|commit| commit.world.strong_count() > 0)
            .any(|commit| commit.budget.spends())
            || self.machine.keys().any(|(ip, _)| victims.contains(ip));
        if blocked {
            assert_reachable!("outage: a commit in flight holds an outage back");
        }
        !blocked
    }

    /// Whether a kill of the process at `ip` waits, on a batched-journals
    /// seed (#599 (durability mode per journal), `shape::batched_journals`):
    /// a machine dies there only inside a commit of a `Batched` journal
    /// that may lose a copy. The machines' kills are kept for the moment
    /// the scenario exists for. A cell whose machines die every few seconds
    /// never creates a tenant in the chaos window: over 120 hunt seeds,
    /// every one of the 25 batched-journals seeds whose machines were
    /// killed created no journal before the tail, so no kill ever met a
    /// batched commit. Held, the cell forms, creates and appends, and the
    /// attrition regime strikes at the journal commit's own `hint!`s (or a
    /// timed reboot lands inside the commit). It holds nothing on any other
    /// seed or process.
    fn holds_machine(&self, state: &StateHandle, ip: &str) -> bool {
        if !crate::shape::batched_journals_drawn(state)
            || self
                .machine
                .iter()
                .any(|((at, _), tolerated)| at == ip && *tolerated > 0)
            || !crate::machine::is_machine_ip(state, ip)
        {
            return false;
        }
        assert_reachable!("journal store: a machine kill waits for a batched commit");
        true
    }

    /// Whether the process at `ip` may die now, spending the budget of every
    /// commit it has in flight if so.
    fn permit_kill(&mut self, ip: &str) -> bool {
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
                Budget::Matchmaker {
                    bootstrap,
                    thin: (thin, matchmaker),
                    ..
                } => bootstrap.is_none_or(|bootstrap| {
                    let spared = thin.spares(*matchmaker);
                    if !spared {
                        assert_reachable!("matchmaker: a thin set holds a matchmaker loss back");
                    }
                    spared && world.may_cut_matchmaker(ip, bootstrap)
                }),
            }
        });
        let machine: Vec<(JournalIdentifier, usize)> = self
            .machine
            .iter()
            .filter(|((at, _), _)| at == ip)
            .map(|((_, journal), tolerated)| (*journal, *tolerated))
            .collect();
        // The machines cut with this one fit the journal's budget, as it
        // stands now (a configure may have lowered it).
        let machine_fits = machine.iter().all(|(journal, tolerated)| {
            let cut = self.machine_cuts.get(journal);
            let counted = cut.map_or(0, BTreeSet::len);
            let fresh = usize::from(!cut.is_some_and(|cut| cut.contains(ip)));
            counted + fresh <= *tolerated
        });
        if !fits || !machine_fits {
            assert_reachable!("journal store: the copy budget refuses a cut mid-commit");
            return false;
        }
        for (journal, tolerated) in machine {
            let cut = self.machine_cuts.entry(journal).or_default();
            cut.insert(ip.to_string());
            assert_always!(
                cut.len() <= tolerated,
                "journal store: a batched journal's cut machines fit its loss budget"
            );
            // Paired with the faulty boot the journal then reports
            // (`TenantBoard::note_faulty`).
            assert_reachable!("journal store: a machine loses power inside a batched commit");
        }
        // The process dies: none of its commits is in flight any more.
        self.machine.retain(|(at, _), _| at != ip);
        for (commit, world) in &open {
            let mut world = world.lock().unwrap_or_else(PoisonError::into_inner);
            match &commit.budget {
                Budget::Free => {}
                Budget::Node { tolerated, slots } => {
                    let spent = world.permit_power_cut(ip, *tolerated, slots);
                    assert!(spent, "a cut that fits the budget spends it");
                }
                Budget::Matchmaker {
                    bootstrap, held, ..
                } => {
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

/// The id of a guard whose commit is parked: its drop removes nothing.
const PARKED: u64 = u64::MAX;

/// A commit registered in flight until this guard drops: when the sync
/// returns, or when a kill drops the task that awaited it. A failed sync
/// parks it instead ([`InFlight::park`]).
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

    /// Keep this commit in flight past its guard, under the store `key`:
    /// its sync failed, so its batch stays volatile in the file image (see
    /// the module doc). It replaces the store's earlier parked commit.
    pub(crate) fn park(mut self, key: &str) {
        let mut guard = self.commits.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(earlier) = guard.parked.insert(key.to_string(), self.id) {
            guard.open.remove(&earlier);
        }
        drop(guard);
        assert_reachable!("journal store: a failed registry sync keeps its commit in flight");
        self.id = PARKED;
    }
}

/// End the commit the store `key` parked, if any: the store opened or
/// synced again, so what the batch held is durable or gone.
pub(crate) fn release_parked(state: &StateHandle, key: &str) {
    if let Some(commits) = state.get::<Arc<Mutex<Commits>>>(COMMITS_KEY) {
        let mut guard = commits.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(id) = guard.parked.remove(key) {
            guard.open.remove(&id);
        }
    }
}

/// Register `ip`'s commit of `journal` in flight (#599 (durability mode per
/// journal)): a `Batched` journal a tenant created, whose slots may each
/// lose `tolerated` copies. A machine commits one batch of a journal at a
/// time, so a registration left by a kill the veto never saw (a scenario's
/// crash) is replaced here.
pub(crate) fn open_machine_commit(
    state: &StateHandle,
    ip: &str,
    journal: JournalIdentifier,
    tolerated: usize,
) {
    let commits = commits(state);
    let mut guard = commits.lock().unwrap_or_else(PoisonError::into_inner);
    guard.machine.insert((ip.to_string(), journal), tolerated);
}

/// `ip`'s commit of `journal` returned: nothing of it is in flight (#599).
pub(crate) fn close_machine_commit(state: &StateHandle, ip: &str, journal: JournalIdentifier) {
    if let Some(commits) = state.get::<Arc<Mutex<Commits>>>(COMMITS_KEY) {
        commits
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .machine
            .remove(&(ip.to_string(), journal));
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
