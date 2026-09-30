//! The system journals' oracles (#189): what every node's folds of the
//! directory (journal 1) and the node registry (journal 2) owe each other,
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
//! - **allocation** — a created journal's id is `128 + its LSN`, never a
//!   genesis journal's and never one created before;
//! - **tombstones** — a node that folded a journal's tombstone acknowledges
//!   no append to it afterwards.
//!
//! The gates are outcomes the run must be proven to reach: a name race
//! decided by slot order, a joiner that learned the system journals before
//! any node had it in its pool, and a joiner's message refused by a node
//! that had not folded its registration and accepted once it had.

use std::collections::{BTreeMap, BTreeSet};
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use moonpool_sim::{StateHandle, assert_always, assert_reachable, assert_sometimes};
use paros::system::{DIRECTORY, DirectoryEvent, DirectoryRefusal, SystemEvent};
use paros::{JournalId, NodeId};

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
    /// The genesis journals: ids the directory never allocates.
    genesis: BTreeSet<JournalId>,
    /// The genesis pool: a node outside it is a joiner.
    genesis_pool: BTreeSet<u64>,
    /// Per `(journal, lsn)`: the digest of the event the first node to fold
    /// it folded it to.
    folded: BTreeMap<(u64, u64), u64>,
    /// Every id the directory created, with the LSN that created it.
    created: BTreeMap<JournalId, u64>,
    /// `(node, journal)`: the node folded the journal's tombstone.
    tombstoned: BTreeSet<(u64, u64)>,
    /// Joiners some node has admitted to its pool.
    admitted_anywhere: BTreeSet<u64>,
    /// `(node, from)`: `node` refused a message from `from`, not yet in its
    /// pool.
    refused: BTreeSet<(u64, u64)>,
    /// A name race was decided by slot order.
    name_race: bool,
    /// A joiner folded a system entry before any node had it in its pool.
    learned_before_pool: bool,
    /// A node refused a joiner's message and later admitted it.
    refused_then_admitted: bool,
    /// A joiner started a journal the directory created naming it.
    joiner_started: bool,
    /// A leadership ran under a configuration naming a joiner: a node the
    /// registry admitted at runtime joined a journal through `Reconfigure`.
    joined_through_reconfigure: bool,
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
        genesis: impl IntoIterator<Item = JournalId>,
        pool: impl IntoIterator<Item = u64>,
        joiners: bool,
        spares: bool,
    ) {
        self.armed = true;
        self.joiners = joiners;
        self.spares = spares;
        self.genesis = genesis.into_iter().collect();
        self.genesis_pool = pool.into_iter().collect();
    }

    /// `node` folded LSN `lsn` of system journal `journal` to `event`.
    #[tracing::instrument(level = "trace", skip(self, event), fields(node = node.0, journal = journal.0, lsn))]
    pub(crate) fn folded(
        &mut self,
        node: NodeId,
        journal: JournalId,
        lsn: u64,
        event: &SystemEvent,
    ) {
        let digest = digest(event);
        let known = *self.folded.entry((journal.0, lsn)).or_insert(digest);
        if journal == DIRECTORY {
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
        match event {
            SystemEvent::Directory(DirectoryEvent::Created { id, .. }) => {
                let at = *self.created.entry(*id).or_insert(lsn);
                assert_always!(
                    id.0 == JournalId::FIRST_USER.0 + lsn
                        && at == lsn
                        && !self.genesis.contains(id),
                    "system: a created journal's id is 128 plus its lsn and never reused",
                    { "id" => id.0, "lsn" => lsn, "first" => at }
                );
            }
            SystemEvent::Directory(DirectoryEvent::Deleted { id }) => {
                self.tombstoned.insert((node.0, id.0));
            }
            SystemEvent::Directory(DirectoryEvent::Refused(DirectoryRefusal::NameTaken {
                winner,
            })) if winner.0 < JournalId::FIRST_USER.0 + lsn => {
                self.name_race = true;
            }
            _ => {}
        }
    }

    /// `node` acknowledged an append to `journal`: never after it folded the
    /// journal's tombstone.
    pub(crate) fn acked(&self, node: NodeId, journal: JournalId) {
        assert_always!(
            !self.tombstoned.contains(&(node.0, journal.0)),
            "system: a node acknowledges no append to a journal after folding its tombstone",
            { "node" => node.0, "journal" => journal.0 }
        );
    }

    /// `node` started `journal`, a journal the directory created naming it.
    pub(crate) fn started(&mut self, node: NodeId) {
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
