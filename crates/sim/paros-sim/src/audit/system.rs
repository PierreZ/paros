//! The node registry's oracles (#189): what every node's folds of a
//! registry owe each other. A registry is the harness tenant's control
//! journal on the acceptors (#235), or a cell control journal on the
//! machines (#650): each registry journal has its own booking model. A
//! tenant control journal's oracles are the machines' (`super::tenants`,
//! #210).
//!
//! The registry's own [`AuditWorld`](super::AuditWorld) already judges its
//! protocol safety, so what lives here is only the *meaning* of its entries,
//! on one board every node's audit port reports to:
//!
//! - **agreement** — every node that folded LSN `l` of the registry folded
//!   it to the same event (the registry's fold is one sequence at every
//!   node, only lagging);
//! - **checkpoints** (#230) — a node whose fold held the registry's whole
//!   prefix finds every checkpoint equal to its own state (folding from a
//!   checkpoint yields what folding the full history does);
//! - **classes and capacity** (#211) — a `stateless` machine never starts a
//!   journal, a booking takes a slot of its node's own class, and a node is
//!   never booked past its capacity, and a booking id is never booked
//!   twice, a released one and one across a checkpoint included (judged on
//!   the registry's events in position order; a truncation is crossed at
//!   the checkpoint a restoring node meets, which the model equals —
//!   bookings and spent ids — wherever it reaches one whole, #247);
//! - **cached folds** (#650) — a machine's durable cached registry fold
//!   holds, for every machine the registry holds at the cache's position,
//!   the address the registry holds for it there, and no retired machine.
//!
//! The gates are outcomes the run must be proven to reach: a joiner that
//! learned the registry before any node had it in its pool, and a joiner's
//! message refused by a node that had not folded its registration and
//! accepted once it had. They are the harness registry's: a machine's port
//! reports only its folds, its checkpoints, its record and its cache
//! ([`Plane::Machine`]).

use std::collections::{BTreeMap, BTreeSet};
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use moonpool_sim::{StateHandle, assert_always, assert_reachable, assert_sometimes};
use paros::core::NodeId;
use paros::core::journal::JournalIdentifier;
use paros::machine::CachedRegistry;
use paros::model::address::Address;
use paros::model::registry::{Class, Registry, RegistryEvent, RegistryRefusal};

const SYSTEM_BOARD_KEY: &str = "paros-system-board";

/// The run's system-journal facts, shared by every node's audit port.
#[derive(Default)]
#[allow(clippy::struct_excessive_bools)] // sticky, independent gate facts
pub(crate) struct SystemBoard {
    /// The run runs the system journals.
    armed: bool,
    /// The run deploys joiners.
    joiners: bool,
    /// A joiner joins the default journal as a spare a reconfiguration may
    /// pull in (a seed with matchmakers, no proxies, no replicas).
    spares: bool,
    /// The genesis pool: a node outside it is a joiner.
    genesis_pool: BTreeSet<u64>,
    /// Per `(journal, lsn)`: the digest of the event the first node to fold
    /// it folded it to.
    folded: BTreeMap<(JournalIdentifier, u64), u64>,
    /// Joiners some node has admitted to its pool.
    admitted_anywhere: BTreeSet<u64>,
    /// `(node, from)`: `node` refused a message from `from`, not yet in its
    /// pool.
    refused: BTreeSet<(u64, u64)>,
    /// A joiner folded a system entry before any node had it in its pool.
    learned_before_pool: bool,
    /// A node refused a joiner's message and later admitted it.
    refused_then_admitted: bool,
    /// A joiner started a journal (a spare's).
    joiner_started: bool,
    /// A leadership ran under a configuration naming a joiner: a node the
    /// registry admitted at runtime joined a journal through `Reconfigure`.
    joined_through_reconfigure: bool,
    /// Each machine's class and capacity, as the role map drew them (#211)
    /// for a joiner and the layout for a machine (#650): never the
    /// registry's.
    machines: BTreeMap<u64, (Class, u64)>,
    /// Each registry journal's booking model, keyed by the journal (#650):
    /// the harness registry, and every cell control journal the machines
    /// fold.
    models: BTreeMap<JournalIdentifier, RegistryModel>,
    /// The registry journal each machine folds (#650): where its cache is
    /// judged.
    registry_of: BTreeMap<u64, JournalIdentifier>,
    /// An owner truncated the registry to a checkpoint (#230).
    truncated: bool,
    /// The booking model crossed a truncation at a restored checkpoint.
    resumed_at_checkpoint: bool,
    /// A checkpoint held a live booking (#211).
    booking_crossed: bool,
    /// A cached registry fold was judged at its position (#650).
    cache_judged: bool,
    /// A fold — a node's or a client's — restarted from a checkpoint.
    restarted: bool,
}

/// Which registry a port reports to the board (#650).
#[derive(Clone, Copy, Debug)]
pub(crate) enum Plane {
    /// The harness registry on the acceptors and joiners: every callback.
    Harness,
    /// A machine's cell control journal: the fold oracles only (folds,
    /// checkpoints, a started journal, the record and the cache), with the
    /// machine's class and capacity as the layout drew them. The joiner
    /// gates stay the harness registry's.
    Machine(Class, u64),
}

impl Plane {
    fn harness(self) -> bool {
        matches!(self, Self::Harness)
    }
}

/// One registry journal's model (#211, #650): its events in position
/// order, as first folded anywhere, from position 0.
struct RegistryModel {
    /// The next position the model expects. A truncation that took
    /// positions before any node folded them is crossed at the checkpoint a
    /// restoring node meets there (#247): the model resumes from the
    /// bookings that checkpoint holds. `None` only once a position was first
    /// seen above a gap no checkpoint healed (an oracle has fired).
    next: Option<u64>,
    /// The live bookings the model knows, to their node.
    bookings: BTreeMap<u64, u64>,
    /// Every booking id the model saw booked (#211): never booked again.
    spent: BTreeSet<u64>,
    /// Each node's fold: the position after the last one it reported (its
    /// latest incarnation's, as folds report in order), for the recovery
    /// tail's liveness claim (#247).
    at: BTreeMap<u64, u64>,
    /// The bookings and spent ids of each checkpoint a whole fold verified
    /// at or past the model's next position, to compare the model with
    /// when it gets there.
    checkpoints: BTreeMap<u64, (BTreeMap<u64, u64>, BTreeSet<u64>)>,
    /// Each registered machine's address changes, in position order: the
    /// position that changed it, and its address after it (`None` once
    /// retired), for the cached folds (#650).
    addresses: BTreeMap<u64, Vec<(u64, Option<String>)>>,
    /// The first position the model knows every address at: a resume at a
    /// checkpoint forgets the positions below it.
    addresses_from: u64,
    /// Caches written at a position the model has not reached yet.
    pending: Vec<CachedRegistry>,
}

impl Default for RegistryModel {
    fn default() -> Self {
        Self {
            next: Some(0),
            bookings: BTreeMap::new(),
            spent: BTreeSet::new(),
            at: BTreeMap::new(),
            checkpoints: BTreeMap::new(),
            addresses: BTreeMap::new(),
            addresses_from: 0,
            pending: Vec::new(),
        }
    }
}

impl RegistryModel {
    /// `node`'s address at the cache position `position` (the registry
    /// below it): `Some(None)` once retired, `None` while unregistered.
    fn address_at(&self, node: u64, position: u64) -> Option<&Option<String>> {
        self.addresses
            .get(&node)?
            .iter()
            .rev()
            .find(|(at, _)| *at < position)
            .map(|(_, addr)| addr)
    }

    /// Note `node`'s address after position `lsn`.
    fn readdress(&mut self, node: u64, lsn: u64, addr: Option<&str>) {
        let changes = self.addresses.entry(node).or_default();
        assert_always!(
            changes.last().is_none_or(|(at, _)| *at <= lsn),
            "registry: the address model folds in position order",
            { "node" => node, "lsn" => lsn }
        );
        changes.push((lsn, addr.map(str::to_string)));
    }

    /// Judge every pending cache the model has reached.
    fn judge_reached(&mut self) -> bool {
        let Some(next) = self.next else {
            self.pending.clear();
            return false;
        };
        let (reached, waiting): (Vec<_>, Vec<_>) = std::mem::take(&mut self.pending)
            .into_iter()
            .partition(|c| c.position <= next);
        self.pending = waiting;
        let mut judged = false;
        for cache in reached {
            judged |= self.judge(&cache);
        }
        judged
    }

    /// A cache at a position the model reached (#650): every machine it
    /// lists that the registry holds there is at the registry's address,
    /// and none is retired. A founding member the registry does not hold
    /// is at an address only the plan or an older book knows, and the
    /// cache's own machine where it answered last (#505 (stranded admitted
    /// machine)): neither is judged. Whether the cache was judged.
    fn judge(&self, cache: &CachedRegistry) -> bool {
        if cache.position < self.addresses_from {
            return false;
        }
        for (id, addr) in &cache.machines {
            if *id == cache.node {
                continue;
            }
            let Some(held) = self.address_at(id.0, cache.position) else {
                continue;
            };
            assert_always!(
                held.is_some(),
                "registry: a cached registry fold lists no retired machine",
                { "node" => cache.node.0, "machine" => id.0, "position" => cache.position }
            );
            if let Some(held) = held {
                assert_always!(
                    Address::parse(held).ok().as_ref() == Some(addr),
                    "registry: a cached registry fold holds the registry's address at its position",
                    { "node" => cache.node.0, "machine" => id.0, "position" => cache.position }
                );
            }
        }
        true
    }
}

/// The run's [`SystemBoard`] (`crate::state::published`).
pub(crate) fn system_board(state: &StateHandle) -> Arc<Mutex<SystemBoard>> {
    crate::state::published(state, SYSTEM_BOARD_KEY, SystemBoard::default)
}

/// Lock the board.
pub(crate) fn lock(board: &Mutex<SystemBoard>) -> MutexGuard<'_, SystemBoard> {
    board.lock().unwrap_or_else(PoisonError::into_inner)
}

/// A stable digest of one folded event (its `Debug` rendering is a pure
/// function of the event).
pub(super) fn digest(event: &impl std::fmt::Debug) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    format!("{event:?}").hash(&mut hasher);
    hasher.finish()
}

impl SystemBoard {
    /// Record the run's genesis (idempotent: every node arms the same).
    pub(crate) fn arm(
        &mut self,
        pool: impl IntoIterator<Item = u64>,
        joiners: bool,
        spares: bool,
        machines: impl IntoIterator<Item = (u64, (Class, u64))>,
    ) {
        // The machines' entries (#650) stay: their ids are minted, never a
        // joiner's.
        self.machines.extend(machines);
        self.armed = true;
        self.joiners = joiners;
        self.spares = spares;
        self.genesis_pool = pool.into_iter().collect();
    }

    /// Machine `node` booted as its layout drew it (#650): the booking
    /// model judges its bookings by this class and capacity.
    pub(crate) fn machine(&mut self, node: NodeId, class: Class, capacity: u64) {
        let before = self.machines.insert(node.0, (class, capacity));
        assert_always!(
            before.is_none_or(|b| b == (class, capacity)),
            "registry: a machine keeps the class and capacity its layout drew",
            { "node" => node.0 }
        );
    }

    /// `node` folded LSN `lsn` of the registry `journal` to `event`.
    #[tracing::instrument(level = "trace", skip(self, event), fields(node = node.0, journal = %journal, lsn))]
    pub(crate) fn folded(
        &mut self,
        node: NodeId,
        journal: JournalIdentifier,
        lsn: u64,
        event: &RegistryEvent,
        plane: Plane,
    ) {
        let digest = digest(event);
        let known = *self.folded.entry((journal, lsn)).or_insert(digest);
        assert_always!(
            known == digest,
            "system: every node folds the registry to the same event at every lsn",
            { "node" => node.0, "lsn" => lsn }
        );
        if plane.harness() {
            if !self.genesis_pool.contains(&node.0) && !self.admitted_anywhere.contains(&node.0) {
                if !self.learned_before_pool {
                    assert_reachable!(
                        "system: a joiner folds a system entry before it is in any pool"
                    );
                }
                self.learned_before_pool = true;
            }
        } else {
            let before = self.registry_of.insert(node.0, journal);
            assert_always!(
                before.is_none_or(|b| b == journal),
                "registry: a machine folds one cell's registry",
                { "node" => node.0 }
            );
        }
        let mut model = self.models.remove(&journal).unwrap_or_default();
        model.at.insert(node.0, lsn + 1);
        if known == digest {
            self.model_registry(&mut model, lsn, event);
        }
        self.models.insert(journal, model);
    }

    /// Advance `model` by the event first folded at `lsn` (#211).
    fn model_registry(&mut self, model: &mut RegistryModel, lsn: u64, event: &RegistryEvent) {
        let Some(next) = model.next else {
            return;
        };
        if lsn < next {
            return;
        }
        if lsn > next {
            // A whole fold reports nothing above a gap: the first position a
            // node folds past the model's is the checkpoint it restored from,
            // which `checkpoint_folded` has already resumed the model at. An
            // unreadable record above a gap is reported as `Malformed` and
            // changes no fold.
            if matches!(event, RegistryEvent::Refused(RegistryRefusal::Malformed)) {
                return;
            }
            assert_always!(
                false,
                "registry: the booking model meets no gap a checkpoint does not heal",
                { "lsn" => lsn, "next" => next }
            );
            model.next = None;
            model.judge_reached();
            return;
        }
        model.next = Some(lsn + 1);
        let reached = model.checkpoints.remove(&lsn);
        model.checkpoints.retain(|seq, _| *seq > lsn);
        if let (RegistryEvent::Checkpoint { .. }, Some((held, spent))) = (event, reached) {
            // The model folded every position below the checkpoint: it holds
            // exactly the checkpoint's bookings and spent ids.
            assert_always!(
                held == model.bookings,
                "registry: the booking model equals every checkpoint it reaches",
                { "lsn" => lsn, "model" => model.bookings.len(), "checkpoint" => held.len() }
            );
            assert_always!(
                spent == model.spent,
                "registry: the spent booking ids equal every checkpoint the model reaches",
                { "lsn" => lsn, "model" => model.spent.len(), "checkpoint" => spent.len() }
            );
        }
        match event {
            RegistryEvent::Registered { id, addr, .. }
            | RegistryEvent::Reregistered { id, addr, .. } => {
                model.readdress(id.0, lsn, Some(addr));
            }
            RegistryEvent::Booked {
                booking,
                node,
                role,
            } => self.model_booking(model, lsn, *booking, *node, *role),
            // A grant books its slots in order (#476), each judged alike.
            RegistryEvent::Granted { slots, .. } => {
                for slot in slots {
                    self.model_booking(model, lsn, slot.booking, slot.node, slot.role);
                }
            }
            RegistryEvent::Released { booking, .. } => {
                model.bookings.remove(booking);
            }
            RegistryEvent::Retired { id } => {
                model.bookings.retain(|_, node| *node != id.0);
                model.readdress(id.0, lsn, None);
            }
            RegistryEvent::Refused(RegistryRefusal::NoCapacity { .. }) => {
                assert_reachable!("registry: a booking is refused at apply for want of capacity");
            }
            RegistryEvent::Refused(RegistryRefusal::WrongClass { .. }) => {
                assert_reachable!("registry: a booking of the other class is refused at apply");
            }
            RegistryEvent::Refused(RegistryRefusal::ClassChanged { .. }) => {
                assert_reachable!("registry: a re-registration under another class is refused");
            }
            RegistryEvent::Refused(RegistryRefusal::BookingTaken { booking })
                if !model.bookings.contains_key(booking) =>
            {
                assert_reachable!("registry: a released booking id is refused at apply");
            }
            RegistryEvent::Refused(RegistryRefusal::AlreadyBooked { .. }) => {
                assert_reachable!("registry: a second booking of one role instance is refused");
            }
            _ => {}
        }
        if let RegistryEvent::Reregistered { .. } = event {
            assert_reachable!("registry: a registered node registers again");
        }
        self.cache_judged(model.judge_reached());
    }

    /// The model's half of one booking (#211): `booking` of `node` for
    /// `role`, first folded at `lsn`.
    fn model_booking(
        &self,
        model: &mut RegistryModel,
        lsn: u64,
        booking: u64,
        node: NodeId,
        role: paros::model::registry::Role,
    ) {
        let machine = self.machines.get(&node.0).copied();
        assert_always!(
            machine.is_none_or(|(drawn, _)| drawn == role.class()),
            "registry: a booking takes a slot of the node's own class",
            { "node" => node.0, "lsn" => lsn }
        );
        assert_always!(
            model.spent.insert(booking),
            "registry: a booking id is booked at most once across checkpoints",
            { "node" => node.0, "lsn" => lsn }
        );
        let held = model.bookings.values().filter(|n| **n == node.0).count() as u64;
        assert_always!(
            machine.is_none_or(|(_, capacity)| held < capacity),
            "registry: a node is never booked past its capacity",
            { "node" => node.0, "lsn" => lsn, "held" => held }
        );
        let before = model.bookings.insert(booking, node.0);
        assert_always!(
            before.is_none(),
            "registry: a live booking id is never booked again",
            { "node" => node.0, "lsn" => lsn, "held_by" => before.unwrap_or_default() }
        );
    }

    /// `node` folded a checkpoint of the registry `journal` at `seq`
    /// (#230): `verified` as [`paros::observe::audit::Audit::checkpoint_folded`] reports it.
    /// `state` is the registry the node's fold holds right after it: the
    /// booking model is checked against it where the model stands at the
    /// checkpoint, and resumes from it where a truncation took positions no
    /// node folded (#247).
    pub(crate) fn checkpoint_folded(
        &mut self,
        node: NodeId,
        journal: JournalIdentifier,
        (seq, verified): (u64, Option<bool>),
        state: &Registry,
        plane: Plane,
    ) {
        assert_always!(
            verified != Some(false),
            "checkpoint: a checkpoint is the state its whole prefix folds to",
            { "node" => node.0, "seq" => seq }
        );
        if verified.is_none() && plane.harness() {
            self.reader_restarted();
        }
        let held: BTreeMap<u64, u64> = state.bookings().map(|(id, b)| (id, b.node.0)).collect();
        let spent: BTreeSet<u64> = state.spent().collect();
        if !held.is_empty() && !self.booking_crossed {
            assert_reachable!("registry: a live booking crosses a checkpoint");
            self.booking_crossed = true;
        }
        if !plane.harness() {
            // A grant's slots are live bookings a machine's fold carries
            // across the checkpoint (#476, #650).
            assert_sometimes!(
                state.grants().any(|(_, grant)| grant.granted()),
                "registry: a machine's fold crosses a granted booking at a checkpoint"
            );
        }
        let mut model = self.models.remove(&journal).unwrap_or_default();
        model.at.insert(node.0, seq + 1);
        match model.next {
            Some(next) if next == seq || (next < seq && verified.is_some()) => {
                // The model reaches this checkpoint through the positions
                // below it (a whole fold folded them), and is compared there
                // (`model_registry`).
                model.checkpoints.entry(seq).or_insert((held, spent));
            }
            Some(next) if next < seq => {
                // A truncation took `next..seq` before any node folded them:
                // resume from the restored checkpoint.
                if !self.resumed_at_checkpoint {
                    assert_reachable!("registry: the booking model resumes at a checkpoint");
                }
                self.resumed_at_checkpoint = true;
                model.bookings = held;
                model.spent = spent;
                model.checkpoints.retain(|at, _| *at > seq);
                model.next = Some(seq + 1);
                // The addresses resume at the checkpoint too: a cache below
                // it is not judged.
                model.addresses.clear();
                for (id, registered) in state.nodes() {
                    let addr = (registered.standing
                        != paros::model::registry::NodeStanding::Retired)
                        .then_some(registered.addr.as_str());
                    model.readdress(id.0, seq, addr);
                }
                model.addresses_from = seq + 1;
                self.cache_judged(model.judge_reached());
            }
            _ => {}
        }
        self.models.insert(journal, model);
    }

    /// Machine `node` wrote `cache` durably (#650): judged against the
    /// registry it folds at the cache's position, now or once the model
    /// reaches it.
    pub(crate) fn cached(&mut self, node: NodeId, cache: &CachedRegistry) {
        let Some(journal) = self.registry_of.get(&node.0).copied() else {
            return;
        };
        let Some(mut model) = self.models.remove(&journal) else {
            return;
        };
        model.pending.push(cache.clone());
        let judged = model.judge_reached();
        self.models.insert(journal, model);
        self.cache_judged(judged);
    }

    /// A cache was judged, when `judged` (#650).
    fn cache_judged(&mut self, judged: bool) {
        if judged && !self.cache_judged {
            assert_reachable!("registry: a cached registry fold is judged at its position");
            self.cache_judged = true;
        }
    }

    /// The nodes of `nodes` whose fold of the registry `journal` has not
    /// reached `tail` (#247).
    pub(crate) fn registry_lagging(
        &self,
        journal: JournalIdentifier,
        nodes: &[u64],
        tail: u64,
    ) -> Vec<u64> {
        let at = self.models.get(&journal).map(|model| &model.at);
        nodes
            .iter()
            .copied()
            .filter(|node| at.and_then(|at| at.get(node)).copied().unwrap_or(0) < tail)
            .collect()
    }

    /// The sum of `nodes`' registry fold positions in `journal`: it grows
    /// while any of them still folds.
    pub(crate) fn registry_progress(&self, journal: JournalIdentifier, nodes: &[u64]) -> u64 {
        let at = self.models.get(&journal).map(|model| &model.at);
        nodes
            .iter()
            .map(|node| at.and_then(|at| at.get(node)).copied().unwrap_or(0))
            .sum()
    }

    /// A fold restarted from a checkpoint (#230).
    pub(crate) fn reader_restarted(&mut self) {
        self.restarted = true;
    }

    /// An owner truncated the registry to its checkpoint (#230).
    pub(crate) fn truncated_to_checkpoint(&mut self) {
        self.truncated = true;
    }

    /// `node` started a journal (a spare's, or one placed on a machine,
    /// #650): never a `stateless` machine (#211).
    pub(crate) fn started(&mut self, node: NodeId, plane: Plane) {
        assert_always!(
            self.machines
                .get(&node.0)
                .is_none_or(|(class, _)| *class == Class::Storage),
            "registry: a stateless machine never serves a journal",
            { "node" => node.0 }
        );
        if plane.harness() && !self.genesis_pool.contains(&node.0) {
            if !self.joiner_started {
                assert_reachable!("system: a joiner serves a journal the directory created");
            }
            self.joiner_started = true;
        }
    }

    /// A leader was elected under `members`: a joiner among them joined its
    /// journal's configuration through `Reconfigure` (a genesis pool is all
    /// a bootstrap names).
    pub(crate) fn elected_under(&mut self, members: &[u64]) {
        if members.iter().any(|m| !self.genesis_pool.contains(m)) {
            self.joined_through_reconfigure = true;
        }
    }

    /// `node` refused a message from `from`, not in its pool yet. Never a
    /// genesis node's (#247, static stability): the pool check admits the
    /// genesis pool whatever the registry fold says, so no tenant journal's
    /// traffic between genesis nodes waits on its parent.
    pub(crate) fn refused(&mut self, node: NodeId, from: NodeId) {
        assert_always!(
            !self.genesis_pool.contains(&from.0),
            "static: no genesis node's message waits on the registry fold",
            { "node" => node.0, "from" => from.0 }
        );
        if self.refused.insert((node.0, from.0)) && self.refused.len() == 1 {
            assert_reachable!("system: a node refuses a message from a node not yet in its pool");
        }
    }

    /// `node`'s registry fold admitted `admitted` to its pool.
    pub(crate) fn admitted(&mut self, node: NodeId, admitted: NodeId) {
        self.admitted_anywhere.insert(admitted.0);
        if self.refused.contains(&(node.0, admitted.0)) {
            self.refused_then_admitted = true;
        }
    }

    /// The outcome gates, once per run, on a run that ran the system
    /// journals.
    pub(crate) fn check_gates(&self) {
        if !self.armed {
            return;
        }
        if self.truncated {
            assert_sometimes!(
                self.restarted,
                "checkpoint: a fold restarts from the registry's checkpoint"
            );
        }
        if !self.joiners {
            return;
        }
        assert_sometimes!(
            self.learned_before_pool,
            "system: a new node learns journals 1 and 2 from a seed before it is in any pool"
        );
        assert_sometimes!(
            self.refused_then_admitted,
            "system: a message from a not-yet-folded node is refused and later accepted"
        );
        if self.spares {
            assert_sometimes!(
                self.joined_through_reconfigure,
                "system: a node registered at runtime joins a journal's configuration through Reconfigure"
            );
        }
    }
}
