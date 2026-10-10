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
//! - **classes and capacity** (#211) — a `stateless` machine never starts a
//!   journal, a booking takes a slot of its node's own class, and a node is
//!   never booked past its capacity, and a live booking id is never booked
//!   again (judged on the registry's events in position order; a
//!   truncation is crossed at the checkpoint a restoring node meets, which
//!   the model equals wherever it reaches one whole, #247).
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
    Class, DirectoryEvent, DirectoryRefusal, Registry, RegistryEvent, RegistryRefusal, SystemEvent,
};
use paros::{JournalId, JournalIdentifier, NodeId};

const SYSTEM_BOARD_KEY: &str = "paros-system-board";

/// No node reported some position of the directory yet.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Unreported;

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
    /// The genesis journals: identifiers the directory never allocates.
    genesis: BTreeSet<JournalIdentifier>,
    /// The genesis pool: a node outside it is a joiner.
    genesis_pool: BTreeSet<u64>,
    /// Per `(journal, lsn)`: the digest of the event the first node to fold
    /// it folded it to.
    folded: BTreeMap<(JournalIdentifier, u64), u64>,
    /// Every id the directory created, with the LSN that created it.
    created: BTreeMap<JournalId, u64>,
    /// `(node, journal)`: the node folded the journal's tombstone.
    tombstoned: BTreeSet<(u64, JournalIdentifier)>,
    /// Joiners some node has admitted to its pool.
    admitted_anywhere: BTreeSet<u64>,
    /// `(node, from)`: `node` refused a message from `from`, not yet in its
    /// pool.
    refused: BTreeSet<(u64, u64)>,
    /// A name race was decided by slot order.
    name_race: bool,
    /// The directory's events, as first folded anywhere, by position: what
    /// a name resolved at a position must agree with (#239).
    directory_events: BTreeMap<u64, DirectoryEvent>,
    /// Each created journal's name.
    created_names: BTreeMap<JournalId, Vec<u8>>,
    /// Names a completed delete freed.
    freed_names: BTreeSet<Vec<u8>>,
    /// A name was created again after a completed delete freed it (#239).
    name_reused: bool,
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
    /// the next position the model expects. A truncation that took
    /// positions before any node folded them is crossed at the checkpoint a
    /// restoring node meets there (#247): the model resumes from the
    /// bookings that checkpoint holds. `None` only once a position was first
    /// seen above a gap no checkpoint healed (an oracle has fired).
    registry_next: Option<u64>,
    /// The live bookings the model knows, to their node.
    bookings: BTreeMap<u64, u64>,
    /// An owner truncated the registry to a checkpoint (#230).
    truncated: bool,
    /// The booking model crossed a truncation at a restored checkpoint.
    resumed_at_checkpoint: bool,
    /// Each node's registry fold: the position after the last one it
    /// reported (its latest incarnation's, as folds report in order), for
    /// the recovery tail's liveness claim (#247).
    registry_at: BTreeMap<u64, u64>,
    /// The bookings of each checkpoint a whole fold verified at or past
    /// the model's next position, to compare the model with when it gets
    /// there.
    checkpoints: BTreeMap<u64, BTreeMap<u64, u64>>,
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
        genesis: impl IntoIterator<Item = JournalIdentifier>,
        pool: impl IntoIterator<Item = u64>,
        joiners: bool,
        spares: bool,
        machines: impl IntoIterator<Item = (u64, (Class, u64))>,
    ) {
        self.machines = machines.into_iter().collect();
        if !self.armed {
            self.registry_next = Some(0);
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
        journal: JournalIdentifier,
        lsn: u64,
        event: &SystemEvent,
    ) {
        let digest = digest(event);
        let known = *self.folded.entry((journal, lsn)).or_insert(digest);
        if let SystemEvent::Directory(_) = event {
            assert_always!(
                known == digest,
                "system: every node folds the directory to the same event at every lsn",
                { "node" => node.0, "lsn" => lsn }
            );
        } else {
            assert_always!(
                known == digest,
                "system: every node folds the registry to the same event at every lsn",
                { "node" => node.0, "lsn" => lsn }
            );
        }
        if !self.genesis_pool.contains(&node.0) && !self.admitted_anywhere.contains(&node.0) {
            if !self.learned_before_pool {
                assert_reachable!("system: a joiner folds a system entry before it is in any pool");
            }
            self.learned_before_pool = true;
        }
        if let SystemEvent::Registry(event) = event {
            self.registry_at.insert(node.0, lsn + 1);
            if known == digest {
                self.model_registry(lsn, event);
            }
        }
        if let SystemEvent::Directory(event) = event {
            self.directory_events
                .entry(lsn)
                .or_insert_with(|| event.clone());
        }
        match event {
            SystemEvent::Directory(DirectoryEvent::Created { id, name, .. }) => {
                if self.created_names.insert(*id, name.clone()).is_none()
                    && self.freed_names.contains(name)
                {
                    assert_reachable!("names: a journal name is reused after a completed delete");
                    self.name_reused = true;
                }
                let at = *self.created.entry(*id).or_insert(lsn);
                assert_always!(
                    id.is_set()
                        && at == lsn
                        && !self.genesis.contains(&JournalIdentifier::new(journal.tenant, *id)),
                    "system: a created journal takes a drawn user id, never reused",
                    { "id" => id.0, "lsn" => lsn, "first" => at }
                );
            }
            SystemEvent::Directory(DirectoryEvent::Deleted { id }) => {
                if let Some(name) = self.created_names.get(id) {
                    self.freed_names.insert(name.clone());
                }
                self.tombstoned
                    .insert((node.0, JournalIdentifier::new(journal.tenant, *id)));
            }
            SystemEvent::Directory(DirectoryEvent::Refused(DirectoryRefusal::NameTaken {
                winner,
            })) if self.created.get(winner).is_some_and(|at| *at < lsn) => {
                self.name_race = true;
            }
            SystemEvent::Directory(DirectoryEvent::Refused(DirectoryRefusal::IdTaken { id })) => {
                assert_always!(
                    self.created.get(id).is_some_and(|at| *at < lsn),
                    "system: a create is refused as taken only for an id created before",
                    { "id" => id.0, "lsn" => lsn }
                );
                self.id_taken = true;
            }
            _ => {}
        }
    }

    /// The live journal named `name` after the directory's positions below
    /// `at`, as the nodes folded them (#239): `Err` while some position
    /// below `at` was not reported yet.
    pub(crate) fn named_at(&self, name: &[u8], at: u64) -> Result<Option<JournalId>, Unreported> {
        let mut holder = None;
        for lsn in 0..at {
            match self.directory_events.get(&lsn).ok_or(Unreported)? {
                DirectoryEvent::Created {
                    id, name: created, ..
                } if created == name => {
                    holder = Some(*id);
                }
                DirectoryEvent::Deleted { id } if holder == Some(*id) => holder = None,
                _ => {}
            }
        }
        Ok(holder)
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
            self.registry_next = None;
            return;
        }
        self.registry_next = Some(lsn + 1);
        let reached = self.checkpoints.remove(&lsn);
        self.checkpoints.retain(|seq, _| *seq > lsn);
        if let (RegistryEvent::Checkpoint { .. }, Some(held)) = (event, reached) {
            // The model folded every position below the checkpoint: it holds
            // exactly the checkpoint's bookings.
            assert_always!(
                held == self.bookings,
                "registry: the booking model equals every checkpoint it reaches",
                { "lsn" => lsn, "model" => self.bookings.len(), "checkpoint" => held.len() }
            );
        }
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
                let before = self.bookings.insert(*booking, node.0);
                assert_always!(
                    before.is_none(),
                    "registry: a live booking id is never booked again",
                    { "node" => node.0, "lsn" => lsn, "held_by" => before.unwrap_or_default() }
                );
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

    /// `node` folded a registry checkpoint at `seq` (#230): `verified` as
    /// [`paros::Audit::checkpoint_folded`] reports it.
    /// `state` is the registry the node's fold holds right after it: the
    /// booking model is checked against it where the model stands at the
    /// checkpoint, and resumes from it where a truncation took positions no
    /// node folded (#247).
    pub(crate) fn checkpoint_folded(
        &mut self,
        node: NodeId,
        seq: u64,
        verified: Option<bool>,
        state: &Registry,
    ) {
        assert_always!(
            verified != Some(false),
            "checkpoint: a checkpoint is the state its whole prefix folds to",
            { "node" => node.0, "seq" => seq }
        );
        if verified.is_none() {
            self.reader_restarted();
        }
        self.registry_at.insert(node.0, seq + 1);
        let held: BTreeMap<u64, u64> = state.bookings().map(|(id, b)| (id, b.node.0)).collect();
        match self.registry_next {
            Some(next) if next == seq || (next < seq && verified.is_some()) => {
                // The model reaches this checkpoint through the positions
                // below it (a whole fold folded them), and is compared there
                // (`model_registry`).
                self.checkpoints.entry(seq).or_insert(held);
            }
            Some(next) if next < seq => {
                // A truncation took `next..seq` before any node folded them:
                // resume from the restored checkpoint.
                if !self.resumed_at_checkpoint {
                    assert_reachable!("registry: the booking model resumes at a checkpoint");
                }
                self.resumed_at_checkpoint = true;
                self.bookings = held;
                self.checkpoints.retain(|at, _| *at > seq);
                self.registry_next = Some(seq + 1);
            }
            _ => {}
        }
    }

    /// The nodes of `nodes` whose registry fold has not reached `tail` (#247).
    pub(crate) fn registry_lagging(&self, nodes: &[u64], tail: u64) -> Vec<u64> {
        nodes
            .iter()
            .copied()
            .filter(|node| self.registry_at.get(node).copied().unwrap_or(0) < tail)
            .collect()
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
    pub(crate) fn acked(&self, node: NodeId, journal: JournalIdentifier) {
        assert_always!(
            !self.tombstoned.contains(&(node.0, journal)),
            "system: a node acknowledges no append to a journal after folding its tombstone",
            { "node" => node.0, "journal" => journal.to_string() }
        );
    }

    /// `node` started `journal`, a journal the directory created naming it
    /// (or a spare's): never a `stateless` machine (#211).
    pub(crate) fn started(&mut self, node: NodeId) {
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
        assert_sometimes!(
            self.name_race,
            "system: a name race is decided by slot order"
        );
        assert_sometimes!(
            self.id_taken,
            "system: a create naming a taken id is refused"
        );
        assert_sometimes!(
            self.name_reused,
            "names: a journal name is created again after a completed delete"
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
