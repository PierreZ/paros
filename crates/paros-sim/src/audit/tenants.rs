//! The tenant control journals' oracles (#210): what every machine's folds of
//! a tenant's control journal owe each other, and what a created or deleted
//! journal owes its clients.
//!
//! Each tenant control journal's own [`AuditWorld`](super::AuditWorld)
//! judges its protocol safety, like every journal's; what lives here is the
//! *meaning* of its entries, on one board every machine's audit port
//! reports to:
//!
//! - **agreement** — every machine that folded LSN `l` of a tenant control
//!   journal folded it to the same event;
//! - **allocation** — a created journal lives in the tenant whose control
//!   journal created it, its id is set and never the id of a journal created
//!   before (a tombstone keeps its id), and `IdTaken` names only an id the
//!   tenant used before;
//! - **one outcome per request** — a request id the fold answered once is
//!   answered the same way every time after (a retry acts once);
//! - **tombstones** — a machine that folded a journal's tombstone
//!   acknowledges no append to it afterwards.
//!
//! The gates are outcomes the run must reach once it created a journal: a
//! name taken by a live journal, and an id redrawn after `IdTaken`. A retry
//! is answered from the recorded outcome without a write, so a `Repeated`
//! fold is rare: a reachable, never a gate.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use moonpool_sim::{StateHandle, assert_always, assert_sometimes};
use paros::tenant::{RequestOutcome, TenantEvent, TenantRefusal};
use paros::{JournalIdentifier, NodeId, WriterMode};

const TENANT_BOARD_KEY: &str = "paros-tenant-board";

/// The run's tenant-control facts, shared by every machine's audit port.
#[derive(Default)]
pub(crate) struct TenantBoard {
    /// Per `(control journal, lsn)`: the digest of the event the first
    /// machine to fold it folded it to.
    folded: BTreeMap<(JournalIdentifier, u64), u64>,
    /// Every journal a tenant created, with the control journal's LSN that
    /// created it.
    created: BTreeMap<JournalIdentifier, u64>,
    /// Every created journal's writer mode (#241).
    modes: BTreeMap<JournalIdentifier, WriterMode>,
    /// Every request's first outcome, by control journal and request id.
    outcomes: BTreeMap<(JournalIdentifier, u64), RequestOutcome>,
    /// `(machine, journal)`: the machine folded the journal's tombstone.
    tombstoned: BTreeSet<(u64, JournalIdentifier)>,
    /// A tenant created a journal.
    armed: bool,
    /// A create was answered `name_taken` by a journal created before.
    name_taken: bool,
    /// A create naming an id the tenant used before was refused.
    id_taken: bool,
}

/// The run's [`TenantBoard`] (`crate::state::published`).
pub(crate) fn tenant_board(state: &StateHandle) -> Arc<Mutex<TenantBoard>> {
    crate::state::published(state, TENANT_BOARD_KEY, TenantBoard::default)
}

/// Lock the board.
pub(crate) fn lock(board: &Mutex<TenantBoard>) -> MutexGuard<'_, TenantBoard> {
    board.lock().unwrap_or_else(PoisonError::into_inner)
}

impl TenantBoard {
    /// `node` folded LSN `lsn` of tenant control journal `control` to
    /// `event`.
    #[tracing::instrument(level = "trace", skip(self, event), fields(node = node.0, journal = %control, lsn))]
    pub(crate) fn folded(
        &mut self,
        node: NodeId,
        control: JournalIdentifier,
        lsn: u64,
        event: &TenantEvent,
    ) {
        let digest = super::system::digest(event);
        let known = *self.folded.entry((control, lsn)).or_insert(digest);
        assert_always!(
            known == digest,
            "tenant: every machine folds a tenant control journal alike at every lsn",
            { "node" => node.0, "lsn" => lsn }
        );
        match event {
            TenantEvent::Created {
                request,
                id,
                writer,
                ..
            } => {
                self.armed = true;
                let journal = JournalIdentifier::new(control.tenant, *id);
                self.modes.entry(journal).or_insert(*writer);
                let at = *self.created.entry(journal).or_insert(lsn);
                assert_always!(
                    id.is_set() && *id != control.journal && at == lsn,
                    "tenant: a created journal takes a set id never used before",
                    { "lsn" => lsn, "first" => at }
                );
                self.answered(control, *request, RequestOutcome::Created(*id), lsn);
            }
            TenantEvent::Deleted { request, id } => {
                let journal = JournalIdentifier::new(control.tenant, *id);
                assert_always!(
                    self.created.get(&journal).is_some_and(|at| *at < lsn),
                    "tenant: a deleted journal is one the tenant created before",
                    { "lsn" => lsn }
                );
                self.tombstoned.insert((node.0, journal));
                self.answered(control, *request, RequestOutcome::Deleted(*id), lsn);
            }
            TenantEvent::Answered { request, outcome } => {
                if let RequestOutcome::NameTaken(winner) = outcome {
                    let winner = JournalIdentifier::new(control.tenant, *winner);
                    assert_always!(
                        self.created.get(&winner).is_some_and(|at| *at < lsn),
                        "tenant: a name is taken only by a journal created before",
                        { "lsn" => lsn }
                    );
                    self.name_taken = true;
                }
                self.answered(control, *request, *outcome, lsn);
            }
            TenantEvent::Repeated { request, outcome } => {
                let first = self.outcomes.get(&(control, *request)).copied();
                assert_always!(
                    first.is_none_or(|first| first == *outcome),
                    "tenant: a repeated request reads back its first outcome",
                    { "lsn" => lsn }
                );
            }
            TenantEvent::Refused(TenantRefusal::IdTaken { id }) => {
                let journal = JournalIdentifier::new(control.tenant, *id);
                assert_always!(
                    *id == control.journal
                        || self.created.get(&journal).is_some_and(|at| *at < lsn),
                    "tenant: a create is refused as taken only for an id used before",
                    { "lsn" => lsn }
                );
                self.id_taken = true;
            }
            TenantEvent::Described
            | TenantEvent::Checkpoint { .. }
            | TenantEvent::Refused(TenantRefusal::Malformed | TenantRefusal::Redescribed) => {}
        }
    }

    /// Request `request` of `control` folded to `outcome` at `lsn`: a
    /// request is answered once (a later answer is a `Repeated`).
    fn answered(
        &mut self,
        control: JournalIdentifier,
        request: u64,
        outcome: RequestOutcome,
        lsn: u64,
    ) {
        let first = *self.outcomes.entry((control, request)).or_insert(outcome);
        assert_always!(
            first == outcome,
            "tenant: a request folds to one outcome",
            { "lsn" => lsn }
        );
    }

    /// The writer mode `journal` was created in, when a tenant created it.
    pub(crate) fn mode(&self, journal: JournalIdentifier) -> Option<WriterMode> {
        self.modes.get(&journal).copied()
    }

    /// Every tenant control journal a machine folded, in identifier order.
    pub(crate) fn controls(&self) -> BTreeSet<JournalIdentifier> {
        self.folded.keys().map(|(journal, _)| *journal).collect()
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

    /// The outcome gates, once per run, on a run that created a journal.
    pub(crate) fn check_gates(&self) {
        if !self.armed {
            return;
        }
        assert_sometimes!(
            self.name_taken,
            "tenant: a create is answered name_taken by a live journal"
        );
        assert_sometimes!(
            self.id_taken,
            "tenant: a create naming a used id is refused and redrawn"
        );
    }
}
