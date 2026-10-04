//! The system journals' oracles (#189): what every node's folds of the
//! directory (the user tenant's control journal) and the node registry (the
//! cell tenant's control journal, #235) owe each other,
//! and what a created or deleted journal owes its clients.
//!
//! Each journal's own [`AuditWorld`](super::AuditWorld) already judges its
//! protocol safety — the system journals and every created journal get one
//! like any other — so what lives here is only the *meaning* of their
//! entries, on one board every node's audit port reports to:
//!
//! - **agreement** — every node that folded LSN `l` of a system journal
//!   folded it to the same event (the directory's and the registry's folds
//!   are one sequence at every node, only lagging);
//! - **allocation** — a created journal's id is the one its creator drew
//!   (#235), in the user range, never a genesis journal's and never one
//!   created before;
//! - **tombstones** — a node that folded a journal's tombstone acknowledges
//!   no append to it afterwards;
//! - **checkpoints** (#230) — a node whose fold held the registry's whole
//!   prefix finds every checkpoint equal to its own state (folding from a
//!   checkpoint yields what folding the full history does);
//! - **meta** (#229) — every node folds meta alike too; a registered
//!   tenant takes a drawn user id, never one registered before, and meta
//!   refuses an id as taken only when it was;
//! - **classes and capacity** (#211) — a `stateless` machine never starts a
//!   journal, a booking takes a slot of its node's own class, and a node is
//!   never booked past its capacity (judged on the registry's events in
//!   position order, while the board has seen every position).
//!
//! The gates are outcomes the run must be proven to reach: a name race
//! decided by slot order, a joiner that learned the system journals before
//! any node had it in its pool, and a joiner's message refused by a node
//! that had not folded its registration and accepted once it had.

use std::collections::{BTreeMap, BTreeSet};
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use moonpool_sim::{StateHandle, assert_always, assert_reachable, assert_sometimes};
use paros::system::{
    Class, DirectoryEvent, DirectoryRefusal, META, MetaEvent, MetaRefusal, REGISTRY, RegistryEvent,
    RegistryRefusal, SystemEvent,
};
use paros::{JournalKey, NodeId, TenantId};

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
    /// The genesis journals: frames the directory never allocates.
    genesis: BTreeSet<JournalKey>,
    /// The genesis pool: a node outside it is a joiner.
    genesis_pool: BTreeSet<u64>,
    /// Per `(journal, lsn)`: the digest of the event the first node to fold
    /// it folded it to.
    folded: BTreeMap<(JournalKey, u64), u64>,
    /// Every journal a tenant's directory created (#210: keyed by its
    /// tenant), with the LSN that created it.
    created: BTreeMap<JournalKey, u64>,
    /// Every tenant the registry hosted (#210): its control journal is one
    /// a node may serve.
    hosted: BTreeSet<TenantId>,
    /// Per tenant directory, as `registry_next` is the registry's: the next
    /// position the board expects, `None` once one was first seen out of
    /// order (a checkpoint restore hid the positions before it).
    directory_next: BTreeMap<TenantId, Option<u64>>,
    /// Every tenant id meta registered, with the LSN that registered it.
    tenants: BTreeMap<TenantId, u64>,
    /// Meta's events in position order, as first folded anywhere: the next
    /// position the model expects, `None` once a position was first seen
    /// out of order (a truncation took the ones before it first, so the
    /// model no longer knows every registered id).
    meta_next: Option<u64>,
    /// `(node, journal)`: the node folded the journal's tombstone.
    tombstoned: BTreeSet<(u64, JournalKey)>,
    /// Joiners some node has admitted to its pool.
    admitted_anywhere: BTreeSet<u64>,
    /// `(node, from)`: `node` refused a message from `from`, not yet in its
    /// pool.
    refused: BTreeSet<(u64, u64)>,
    /// A name race was decided by slot order.
    name_race: bool,
    /// A create naming an id the directory already created was refused.
    id_taken: bool,
    /// A joiner folded a system entry before any node had it in its pool.
    learned_before_pool: bool,
    /// A node refused a joiner's message and later admitted it.
    refused_then_admitted: bool,
    /// A joiner started a journal the directory created naming it.
    joiner_started: bool,
    /// A leadership ran under a configuration naming a joiner: a node the
    /// registry admitted at runtime joined a journal through `Reconfigure`.
    joined_through_reconfigure: bool,
    /// Each joiner's class and capacity, as the role map drew them (#211).
    machines: BTreeMap<u64, (Class, u64)>,
    /// The registry's events in position order, as first folded anywhere:
    /// the next position the model expects, `None` once a position was
    /// first seen out of order (no node folded the ones before it, so the
    /// model no longer knows the bookings).
    registry_next: Option<u64>,
    /// The live bookings the model knows, to their node.
    bookings: BTreeMap<u64, u64>,
    /// An owner truncated the registry to a checkpoint (#230).
    truncated: bool,
    /// A fold — a node's or a client's — restarted from a checkpoint.
    restarted: bool,
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
fn digest(event: &SystemEvent) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    format!("{event:?}").hash(&mut hasher);
    hasher.finish()
}

impl SystemBoard {
    /// Record the run's genesis (idempotent: every node arms the same).
    pub(crate) fn arm(
        &mut self,
        genesis: impl IntoIterator<Item = JournalKey>,
        pool: impl IntoIterator<Item = u64>,
        joiners: bool,
        spares: bool,
        machines: impl IntoIterator<Item = (u64, (Class, u64))>,
    ) {
        self.machines = machines.into_iter().collect();
        if !self.armed {
            self.registry_next = Some(0);
            self.meta_next = Some(0);
        }
        self.armed = true;
        self.joiners = joiners;
        self.spares = spares;
        self.genesis = genesis.into_iter().collect();
        self.genesis_pool = pool.into_iter().collect();
    }

    /// `node` folded LSN `lsn` of system journal `journal` to `event`.
    #[tracing::instrument(level = "trace", skip(self, event), fields(node = node.0, journal = %journal, lsn))]
    pub(crate) fn folded(
        &mut self,
        node: NodeId,
        journal: JournalKey,
        lsn: u64,
        event: &SystemEvent,
    ) {
        let digest = digest(event);
        let known = *self.folded.entry((journal, lsn)).or_insert(digest);
        if journal == META {
            assert_always!(
                known == digest,
                "system: every node folds meta to the same event at every lsn",
                { "node" => node.0, "lsn" => lsn }
            );
        } else if journal == REGISTRY {
            assert_always!(
                known == digest,
                "system: every node folds the registry to the same event at every lsn",
                { "node" => node.0, "lsn" => lsn }
            );
        } else {
            // Every tenant's directory (#210), the genesis one included.
            assert_always!(
                known == digest,
                "system: every node folds the directory to the same event at every lsn",
                { "node" => node.0, "lsn" => lsn }
            );
        }
        if !self.genesis_pool.contains(&node.0) && !self.admitted_anywhere.contains(&node.0) {
            if !self.learned_before_pool {
                assert_reachable!("system: a joiner folds a system entry before it is in any pool");
            }
            self.learned_before_pool = true;
        }
        if let SystemEvent::Registry(event) = event
            && known == digest
        {
            self.model_registry(lsn, event);
        }
        if let SystemEvent::Meta(event) = event
            && known == digest
        {
            self.model_meta(lsn, event);
        }
        if let SystemEvent::Directory(_) = event {
            let next = self.directory_next.entry(journal.tenant).or_insert(Some(0));
            *next = match *next {
                Some(expected) if lsn == expected => Some(lsn + 1),
                Some(expected) if lsn < expected => Some(expected),
                _ => None,
            };
        }
        match event {
            SystemEvent::Directory(DirectoryEvent::Created { id, .. }) => {
                let key = JournalKey::new(journal.tenant, *id);
                let at = *self.created.entry(key).or_insert(lsn);
                assert_always!(
                    id.is_user() && at == lsn && !self.genesis.contains(&key),
                    "system: a created journal takes a drawn user id, never reused",
                    { "id" => id.0, "lsn" => lsn, "first" => at }
                );
            }
            SystemEvent::Registry(RegistryEvent::TenantHosted { tenant, .. }) => {
                self.hosted.insert(*tenant);
            }
            SystemEvent::Directory(DirectoryEvent::Deleted { id }) => {
                self.tombstoned
                    .insert((node.0, JournalKey::new(journal.tenant, *id)));
            }
            SystemEvent::Directory(DirectoryEvent::Refused(DirectoryRefusal::NameTaken {
                winner,
            })) if self
                .created
                .get(&JournalKey::new(journal.tenant, *winner))
                .is_some_and(|at| *at < lsn) =>
            {
                self.name_race = true;
            }
            SystemEvent::Directory(DirectoryEvent::Refused(DirectoryRefusal::IdTaken { id })) => {
                assert_always!(
                    self.created
                        .get(&JournalKey::new(journal.tenant, *id))
                        .is_some_and(|at| *at < lsn),
                    "system: a create is refused as taken only for an id created before",
                    { "id" => id.0, "lsn" => lsn }
                );
                self.id_taken = true;
            }
            _ => {}
        }
    }

    /// Advance the meta model by the event first folded at `lsn` (#229):
    /// judged only while the board has seen every position, since a
    /// registration a truncation took before any node folded it is one the
    /// model cannot know.
    fn model_meta(&mut self, lsn: u64, event: &MetaEvent) {
        let Some(next) = self.meta_next else {
            return;
        };
        if lsn < next {
            return;
        }
        if lsn > next {
            self.meta_next = None;
            return;
        }
        self.meta_next = Some(lsn + 1);
        match event {
            MetaEvent::TenantRegistered { tenant, .. } => {
                let known = self.tenants.insert(*tenant, lsn);
                assert_always!(
                    tenant.is_user() && known.is_none(),
                    "fleet: a registered tenant takes a drawn user id, never reused",
                    { "tenant" => tenant.0, "lsn" => lsn }
                );
            }
            MetaEvent::Refused(MetaRefusal::IdTaken { tenant }) => {
                assert_always!(
                    self.tenants.contains_key(tenant),
                    "fleet: meta refuses a tenant id as taken only when it was registered",
                    { "tenant" => tenant.0, "lsn" => lsn }
                );
                assert_reachable!("fleet: meta refuses a tenant id registered before");
            }
            _ => {}
        }
    }

    /// Advance the registry model by the event first folded at `lsn` (#211).
    fn model_registry(&mut self, lsn: u64, event: &RegistryEvent) {
        let Some(next) = self.registry_next else {
            return;
        };
        if lsn < next {
            return;
        }
        if lsn > next {
            // No node folded the positions in between (a truncation took
            // them first): the model stops here.
            self.registry_next = None;
            return;
        }
        self.registry_next = Some(lsn + 1);
        match event {
            RegistryEvent::Booked {
                booking,
                node,
                class,
            } => {
                let machine = self.machines.get(&node.0).copied();
                assert_always!(
                    machine.is_none_or(|(drawn, _)| drawn == *class),
                    "registry: a booking takes a slot of the node's own class",
                    { "node" => node.0, "lsn" => lsn }
                );
                let held = self.bookings.values().filter(|n| **n == node.0).count() as u64;
                assert_always!(
                    machine.is_none_or(|(_, capacity)| held < capacity),
                    "registry: a node is never booked past its capacity",
                    { "node" => node.0, "lsn" => lsn, "held" => held }
                );
                self.bookings.insert(*booking, node.0);
            }
            RegistryEvent::Released { booking, .. } => {
                self.bookings.remove(booking);
            }
            RegistryEvent::Retired { id } => {
                self.bookings.retain(|_, node| *node != id.0);
            }
            RegistryEvent::Reregistered { .. } => {
                assert_reachable!("registry: a registered node registers again");
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
            _ => {}
        }
    }

    /// `node` folded a checkpoint of `journal` at `seq` (#230): `verified` as
    /// [`paros::Audit::checkpoint_folded`] reports it.
    pub(crate) fn checkpoint_folded(
        &mut self,
        node: NodeId,
        journal: JournalKey,
        seq: u64,
        verified: Option<bool>,
    ) {
        assert_always!(
            verified != Some(false),
            "checkpoint: a checkpoint is the state its whole prefix folds to",
            { "node" => node.0, "seq" => seq }
        );
        // The restart gate is the registry's (its messages say so); meta's
        // checkpoints are judged by the same oracle above.
        if verified.is_none() && journal == paros::system::REGISTRY {
            self.reader_restarted();
        }
    }

    /// A fold restarted from a checkpoint (#230).
    pub(crate) fn reader_restarted(&mut self) {
        self.restarted = true;
    }

    /// An owner truncated the registry to its checkpoint (#230).
    pub(crate) fn truncated_to_checkpoint(&mut self) {
        self.truncated = true;
    }

    /// `node` acknowledged an append to `journal`: never after it folded the
    /// journal's tombstone.
    pub(crate) fn acked(&self, node: NodeId, journal: JournalKey) {
        assert_always!(
            !self.tombstoned.contains(&(node.0, journal)),
            "system: a node acknowledges no append to a journal after folding its tombstone",
            { "node" => node.0, "journal" => journal.to_string() }
        );
    }

    /// `node` started `journal`, a journal the directory created naming it
    /// (or a spare's): never a `stateless` machine (#211), and only a
    /// journal its own tenant created, a genesis one, or the control
    /// journal of a tenant the cell hosts (#210).
    pub(crate) fn started(&mut self, node: NodeId, journal: JournalKey) {
        // Judged only on what the board has seen whole: a journal whose
        // creation (or hosting) a checkpoint restore hid is not known here.
        let control = journal == JournalKey::control(journal.tenant);
        let known = if control {
            self.registry_next.is_none() || self.hosted.contains(&journal.tenant)
        } else {
            self.created.contains_key(&journal)
                || !matches!(self.directory_next.get(&journal.tenant), Some(Some(_)))
        };
        assert_always!(
            self.genesis.contains(&journal) || known,
            "tenant: a node serves only a journal its own tenant created",
            { "node" => node.0, "journal" => journal.to_string() }
        );
        if journal == JournalKey::control(journal.tenant) && self.hosted.contains(&journal.tenant) {
            assert_reachable!("tenant: a node starts the control journal of a hosted tenant");
        }
        assert_always!(
            self.machines
                .get(&node.0)
                .is_none_or(|(class, _)| *class == Class::Storage),
            "registry: a stateless machine never serves a journal",
            { "node" => node.0 }
        );
        if !self.genesis_pool.contains(&node.0) {
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

    /// `node` refused a message from `from`, not in its pool yet.
    pub(crate) fn refused(&mut self, node: NodeId, from: NodeId) {
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
        assert_sometimes!(
            self.name_race,
            "system: a name race is decided by slot order"
        );
        assert_sometimes!(
            self.id_taken,
            "system: a create naming a taken id is refused"
        );
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
