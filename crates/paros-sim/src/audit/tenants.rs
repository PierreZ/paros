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
//!   acknowledges no append to it afterwards, and answers no other call of
//!   it either (#449); a journal is deleted once;
//! - **starts** — a machine starts only a tenant's control journal or a
//!   journal the tenant created (#449);
//! - **denials** — a call a frontend must deny never reaches a journal: no
//!   node applies its payload or its leader uuid (#450);
//! - **store durability** — a created journal keeps the store durability
//!   its create recorded (#599 (durability mode per journal)); the board
//!   holds it, with the copies a power loss may cost each slot, for the
//!   machines' cut budget (`crate::world::cut`).
//!
//! The gates are outcomes the run must reach once it created a journal: a
//! name taken by a live journal, and an id redrawn after `IdTaken`. A retry
//! is answered from the recorded outcome without a write, so a `Repeated`
//! fold is rare: a reachable, never a gate.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use moonpool_sim::{StateHandle, assert_always, assert_reachable, assert_sometimes};
use paros::model::tenant::{RequestOutcome, StoreDurability, TenantEvent, TenantRefusal};
use paros::{
    AcceptorConfig, JournalId, JournalIdentifier, NodeId, QuorumSystem, TenantId, WriterMode,
};

const TENANT_BOARD_KEY: &str = "paros-tenant-board";

/// No machine reported some position of a tenant control journal yet.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Unreported;

/// What a position of a tenant control journal did to its names (#239).
#[derive(Clone, Debug, PartialEq, Eq)]
enum Naming {
    /// A journal took this name.
    Created(JournalId, Vec<u8>),
    /// A journal was tombstoned: its name is free.
    Deleted(JournalId),
    /// No name changed.
    Other,
}

/// The run's tenant-control facts, shared by every machine's audit port.
/// Its flags are independent gates, each fired once.
#[allow(clippy::struct_excessive_bools)]
#[derive(Default)]
pub(crate) struct TenantBoard {
    /// Per `(control journal, lsn)`: the digest of the event the first
    /// machine to fold it folded it to.
    folded: BTreeMap<(JournalIdentifier, u64), u64>,
    /// Every journal a tenant created, with the control journal's LSN that
    /// created it.
    created: BTreeMap<JournalIdentifier, u64>,
    /// Every created journal's acceptor configuration at its create (#362
    /// (background scrubbing): the cold-rot scenario rots only a journal
    /// that keeps a clean copy on a quorum).
    configs: BTreeMap<JournalIdentifier, paros::AcceptorConfig>,
    /// Every created journal's writer mode (#241).
    modes: BTreeMap<JournalIdentifier, WriterMode>,
    /// Every created journal's store durability (#599 (durability mode per
    /// journal)).
    stores: BTreeMap<JournalIdentifier, StoreDurability>,
    /// Per created journal: the fewest copies of a slot its configurations
    /// may lose and still repair it, over its create and every configure
    /// (#599): the machines' cut budget.
    tolerated: BTreeMap<JournalIdentifier, usize>,
    /// Batched journals a machine booted with faulty entries: a lost last
    /// batch (#599).
    batch_lost: BTreeSet<JournalIdentifier>,
    /// Every request's first outcome, by control journal and request id.
    outcomes: BTreeMap<(JournalIdentifier, u64), RequestOutcome>,
    /// `(machine, journal)`: the machine folded the journal's tombstone.
    tombstoned: BTreeSet<(u64, JournalIdentifier)>,
    /// The LSN each journal's tombstone folded at. A node that re-folds
    /// after a reboot meets a tag request below a tombstone it folded
    /// before, so a tag is judged by LSN, not by what a node folded.
    deleted: BTreeMap<JournalIdentifier, u64>,
    /// A tenant created a journal.
    armed: bool,
    /// A create was answered `name_taken` by a journal created before.
    name_taken: bool,
    /// A create naming an id the tenant used before was refused.
    id_taken: bool,
    /// Per `(control journal, lsn)`: what the position did to the names,
    /// as first folded anywhere. A name resolved at a position must agree
    /// with it (#239).
    naming: BTreeMap<(JournalIdentifier, u64), Naming>,
    /// Names a completed delete freed, by control journal.
    freed: BTreeSet<(JournalIdentifier, Vec<u8>)>,
    /// A name was created again after a completed delete freed it (#239).
    name_reused: bool,
    /// `(machine, journal)`: the machine stopped serving the journal for
    /// good, a delete or its tenant's drop (#447 (model checks for every
    /// journal world)).
    stopped: BTreeSet<(u64, JournalIdentifier)>,
    /// Tenants a machine folded the cell's drop of: every machine that folds
    /// it stops each journal of the tenant, a lagging one included, so none
    /// owes convergence (#529 (sets oracles after PR #515)).
    dropped: BTreeSet<TenantId>,
    /// The payloads of the writes a frontend must deny (#450): no node
    /// ever applies one.
    denied_payloads: BTreeSet<Vec<u8>>,
    /// The leader uuids of the claims a frontend must deny (#450): no
    /// node ever applies a `SetLeader` to one.
    denied_leaders: BTreeSet<u128>,
}

/// The run's [`TenantBoard`] (`crate::state::published`).
pub(crate) fn tenant_board(state: &StateHandle) -> Arc<Mutex<TenantBoard>> {
    crate::state::published(state, TENANT_BOARD_KEY, TenantBoard::default)
}

/// Lock the board.
pub(crate) fn lock(board: &Mutex<TenantBoard>) -> MutexGuard<'_, TenantBoard> {
    board.lock().unwrap_or_else(PoisonError::into_inner)
}

/// #215 (zones): a created journal's placement was placed under the zone
/// rule where the rule binds, so it survives any one zone. The rule binds
/// where a partition survives a failure (`F >= 1`: three acceptors per
/// partition or more) and the placement spans three zones or more
/// (`paros::model::placement::zone_rule_binds`).
fn zone_spread(config: &paros::AcceptorConfig, lsn: u64) {
    let spread = config.distinct_domains().len();
    let per_partition = match config.quorum_system() {
        paros::QuorumSystem::Grid { rows, .. } => rows,
        _ => config.members().len(),
    };
    let binds = per_partition >= 3 && spread >= 3;
    assert_always!(
        !binds || config.zone_rule().holds(),
        "zones: a placed journal that spans three zones survives any one",
        { "lsn" => lsn, "zones" => spread, "per_partition" => per_partition }
    );
    if binds {
        assert_reachable!("zones: a journal is placed over three zones");
    }
}

/// The copies of one slot `config` may lose and still repair it (#599
/// (durability mode per journal)): a lost copy is a faulty vote, and CTRL
/// decides the slot only while every Phase-1 quorum keeps a clean copy and
/// a Phase-1 quorum still answers. A majority of `n` loses `⌊(n-1)/2⌋`; a
/// column of `rows` deciding on `acks` loses `min(acks - 1, rows - acks)`.
fn tolerated_loss(config: &AcceptorConfig) -> usize {
    let n = config.members().len();
    match config.quorum_system() {
        QuorumSystem::Majority => n.saturating_sub(1) / 2,
        QuorumSystem::Flexible { q1, q2 } => n.saturating_sub(q1).min(q2.saturating_sub(1)),
        QuorumSystem::Grid { rows, acks, .. } => {
            acks.saturating_sub(1).min(rows.saturating_sub(acks))
        }
    }
}

impl TenantBoard {
    /// The acceptor configuration a tenant created `journal` with, while it
    /// is not deleted (#362 (background scrubbing): the journals the
    /// cold-rot scenario may rot).
    pub(crate) fn live_config(&self, journal: JournalIdentifier) -> Option<&paros::AcceptorConfig> {
        if self.deleted.contains_key(&journal) {
            return None;
        }
        self.configs.get(&journal)
    }

    /// #575 (key:value tags): a tag request names a live journal the
    /// tenant created before; a tag changes no outcome.
    fn tagged(&mut self, control: JournalIdentifier, request: u64, id: JournalId, lsn: u64) {
        let journal = JournalIdentifier::new(control.tenant, id);
        assert_always!(
            self.created.get(&journal).is_some_and(|at| *at < lsn)
                && self.deleted.get(&journal).is_none_or(|at| *at > lsn),
            "tenant: a tagged journal is a live one the tenant created before",
            { "lsn" => lsn }
        );
        self.answered(control, request, RequestOutcome::Tagged(id), lsn);
    }

    /// `node` folded LSN `lsn` of `control`, the delete `request` of
    /// journal `id`: a journal the tenant created before, deleted once.
    fn deleted(
        &mut self,
        node: NodeId,
        control: JournalIdentifier,
        (request, id): (u64, JournalId),
        lsn: u64,
    ) {
        let journal = JournalIdentifier::new(control.tenant, id);
        assert_always!(
            self.created.get(&journal).is_some_and(|at| *at < lsn),
            "tenant: a deleted journal is one the tenant created before",
            { "lsn" => lsn }
        );
        self.tombstoned.insert((node.0, journal));
        let first = *self.deleted.entry(journal).or_insert(lsn);
        assert_always!(
            first == lsn,
            "tenant: a journal is deleted once",
            { "lsn" => lsn, "first" => first }
        );
        self.answered(control, request, RequestOutcome::Deleted(id), lsn);
    }

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
        self.name(control, lsn, event);
        match event {
            TenantEvent::Created {
                request,
                id,
                writer,
                config,
                store,
                ..
            } => {
                zone_spread(config, lsn);
                self.armed = true;
                let journal = JournalIdentifier::new(control.tenant, *id);
                self.modes.entry(journal).or_insert(*writer);
                self.configs
                    .entry(journal)
                    .or_insert_with(|| config.clone());
                self.created_store(journal, (*store, config), lsn);
                let at = *self.created.entry(journal).or_insert(lsn);
                assert_always!(
                    id.is_set() && *id != control.journal && at == lsn,
                    "tenant: a created journal takes a set id never used before",
                    { "lsn" => lsn, "first" => at }
                );
                self.answered(control, *request, RequestOutcome::Created(*id), lsn);
            }
            TenantEvent::Deleted { request, id } => {
                self.deleted(node, control, (*request, *id), lsn);
            }
            TenantEvent::Tagged { request, id } => self.tagged(control, *request, *id, lsn),
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
            TenantEvent::Configured {
                request,
                id,
                desired,
            } => self.configured(control, (*request, *id, desired), lsn),
            TenantEvent::Placed { id, config } => {
                let journal = JournalIdentifier::new(control.tenant, *id);
                self.tolerate(journal, tolerated_loss(config));
                assert_always!(
                    self.created.get(&journal).is_some_and(|at| *at < lsn),
                    "tenant: a placement is recorded only for a journal created before",
                    { "lsn" => lsn }
                );
            }
            TenantEvent::Scrubbed(progress) => scrubbed(lsn, progress),
            TenantEvent::Described
            | TenantEvent::Checkpoint { .. }
            | TenantEvent::Refused(
                TenantRefusal::Malformed
                | TenantRefusal::Redescribed
                | TenantRefusal::NotPlaceable { .. }
                | TenantRefusal::ScrubBehind,
            ) => {}
        }
    }

    /// Record what position `lsn` of `control` did to its names, the first
    /// time a machine folds it.
    fn name(&mut self, control: JournalIdentifier, lsn: u64, event: &TenantEvent) {
        if self.naming.contains_key(&(control, lsn)) {
            return;
        }
        let naming = match event {
            TenantEvent::Created { id, name, .. } => {
                if self.freed.contains(&(control, name.clone())) {
                    assert_reachable!("names: a journal name is reused after a completed delete");
                    self.name_reused = true;
                }
                Naming::Created(*id, name.clone())
            }
            TenantEvent::Deleted { id, .. } => {
                let freed = self.naming.values().find_map(|naming| match naming {
                    Naming::Created(created, name) if created == id => Some(name.clone()),
                    _ => None,
                });
                if let Some(name) = freed {
                    self.freed.insert((control, name));
                }
                Naming::Deleted(*id)
            }
            _ => Naming::Other,
        };
        self.naming.insert((control, lsn), naming);
    }

    /// The live journal named `name` after the positions of `control` below
    /// `at`, as the machines folded them (#239): `Err` while some position
    /// below `at` was not reported yet.
    pub(crate) fn named_at(
        &self,
        control: JournalIdentifier,
        name: &[u8],
        at: u64,
    ) -> Result<Option<JournalId>, Unreported> {
        let mut holder = None;
        for lsn in 0..at {
            match self.naming.get(&(control, lsn)).ok_or(Unreported)? {
                Naming::Created(id, created) if created == name => holder = Some(*id),
                Naming::Deleted(id) if holder == Some(*id) => holder = None,
                _ => {}
            }
        }
        Ok(holder)
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

    /// A configure of `control`'s tenant folded at `lsn`: of journal `id`,
    /// or of the tenant default (`None`).
    fn configured(
        &mut self,
        control: JournalIdentifier,
        (request, id, desired): (u64, Option<JournalId>, &paros::model::tenant::Desired),
        lsn: u64,
    ) {
        // A journal's mode changes only for a journal its tenant created
        // before (#214 (tenant modes)).
        if let Some(id) = id {
            let journal = JournalIdentifier::new(control.tenant, id);
            // Its new placement survives `F` failures per partition (#599:
            // the cut budget takes the smallest of its life).
            let failures = desired.durability.failures_per_partition;
            self.tolerate(journal, usize::try_from(failures).unwrap_or(0));
            assert_always!(
                self.created.get(&journal).is_some_and(|at| *at < lsn),
                "tenant: a configured journal is one the tenant created before",
                { "lsn" => lsn }
            );
            assert_reachable!("tenant: a journal's desired mode is configured");
        } else {
            assert_reachable!("tenant: a tenant's default mode is configured");
        }
        self.answered(control, request, RequestOutcome::Configured(id), lsn);
    }

    /// A create of `journal` folded with `store` on `config` (#599): every
    /// fold of it names the same mode, and its placement sets the cut
    /// budget.
    fn created_store(
        &mut self,
        journal: JournalIdentifier,
        (store, config): (StoreDurability, &AcceptorConfig),
        lsn: u64,
    ) {
        let held = *self.stores.entry(journal).or_insert(store);
        assert_always!(
            held == store,
            "tenant: a created journal keeps the store durability of its create",
            { "lsn" => lsn }
        );
        self.tolerate(journal, tolerated_loss(config));
    }

    /// `journal`'s cut budget falls to `loss` when that is smaller (#599).
    fn tolerate(&mut self, journal: JournalIdentifier, loss: usize) {
        let held = self.tolerated.entry(journal).or_insert(loss);
        *held = (*held).min(loss);
    }

    /// The store durability `journal` was created with, when a tenant
    /// created it (#599 (durability mode per journal)).
    pub(crate) fn store(&self, journal: JournalIdentifier) -> Option<StoreDurability> {
        self.stores.get(&journal).copied()
    }

    /// The distinct machines a power loss may cut inside a commit of
    /// `journal`, a `Batched` journal a tenant created: the fewest copies
    /// of a slot its configurations may lose (#599). `None` for a journal
    /// that stores ordered or that no tenant created: its cut costs no copy.
    pub(crate) fn cut_budget(&self, journal: JournalIdentifier) -> Option<usize> {
        (self.store(journal)? == StoreDurability::Batched)
            .then(|| self.tolerated.get(&journal).copied().unwrap_or(0))
    }

    /// A machine booted `journal` with faulty entries: on a `Batched`
    /// journal, its last batch came back ambiguous (#599).
    pub(crate) fn note_faulty(&mut self, journal: JournalIdentifier) {
        if self.store(journal) == Some(StoreDurability::Batched) {
            assert_reachable!("journal store: a batched journal boots with its last batch lost");
            self.batch_lost.insert(journal);
        }
    }

    /// Whether a machine booted `journal`, a `Batched` journal, with its
    /// last batch lost (#599).
    pub(crate) fn batch_lost(&self, journal: JournalIdentifier) -> bool {
        self.batch_lost.contains(&journal)
    }

    /// The writer mode `journal` was created in, when a tenant created it.
    pub(crate) fn mode(&self, journal: JournalIdentifier) -> Option<WriterMode> {
        self.modes.get(&journal).copied()
    }

    /// Every journal a tenant created, with the writer mode it was created
    /// in.
    pub(crate) fn created_modes(&self) -> BTreeMap<JournalIdentifier, WriterMode> {
        self.modes.clone()
    }

    /// `node` stopped serving `journal` for good.
    pub(crate) fn stopped(&mut self, node: NodeId, journal: JournalIdentifier) {
        self.stopped.insert((node.0, journal));
    }

    /// Every machine that stopped serving `journal` for good.
    pub(crate) fn stopped_on(&self, journal: JournalIdentifier) -> BTreeSet<u64> {
        self.stopped
            .iter()
            .filter(|(_, j)| *j == journal)
            .map(|(node, _)| *node)
            .collect()
    }

    /// A machine folded the cell's drop of `tenant`.
    pub(crate) fn tenant_dropped(&mut self, tenant: TenantId) {
        self.dropped.insert(tenant);
    }

    /// Whether a machine folded the cell's drop of `tenant`.
    pub(crate) fn dropped(&self, tenant: TenantId) -> bool {
        self.dropped.contains(&tenant)
    }

    /// Whether any machine folded `journal`'s tombstone: its nodes stop it.
    pub(crate) fn tombstoned(&self, journal: JournalIdentifier) -> bool {
        self.tombstoned.iter().any(|(_, j)| *j == journal)
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

    /// `node` answered a `SetLeader` or a `Truncate` of `journal` (#449):
    /// never after it folded the journal's tombstone.
    pub(crate) fn served(&self, node: NodeId, journal: JournalIdentifier) {
        assert_always!(
            !self.tombstoned.contains(&(node.0, journal)),
            "system: a node serves no call of a journal after folding its tombstone",
            { "node" => node.0, "journal" => journal.to_string() }
        );
    }

    /// `node` started serving `journal` (#449): a tenant's control journal,
    /// or a journal its tenant created, which the node folded before it
    /// started it. A journal of a tenant whose control journal no machine
    /// folded yet is not judged: the board does not know the tenant.
    pub(crate) fn started(&self, node: NodeId, journal: JournalIdentifier) {
        let controls = self.controls();
        let Some(control) = controls.iter().find(|c| c.tenant == journal.tenant) else {
            return;
        };
        if *control == journal {
            return;
        }
        assert_always!(
            self.created.contains_key(&journal),
            "system: a node starts only a genesis journal or one a tenant created",
            { "node" => node.0, "journal" => journal.to_string() }
        );
        assert_reachable!("system: a node starts a journal its tenant created");
    }

    /// A frontend must deny a write carrying `payload`, or a claim under
    /// `leader` (#450): noted before the call is sent.
    pub(crate) fn must_deny(&mut self, payload: Option<Vec<u8>>, leader: Option<u128>) {
        self.denied_payloads.extend(payload);
        self.denied_leaders.extend(leader);
    }

    /// `node` applied `command` (#450): never a call a frontend had to deny.
    pub(crate) fn applied(&self, node: NodeId, command: &paros::Command) {
        if self.denied_payloads.is_empty() && self.denied_leaders.is_empty() {
            return;
        }
        let denied = match command {
            paros::Command::Write(entry) => entry
                .records
                .iter()
                .any(|record| self.denied_payloads.contains(&record.0)),
            paros::Command::Control(paros::Control::SetLeader { new, .. }) => {
                self.denied_leaders.contains(&new.0)
            }
            paros::Command::Control(_) => false,
        };
        assert_always!(
            !denied,
            "frontend: a denied call never changes a journal",
            { "node" => node.0 }
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
        assert_sometimes!(
            self.name_reused,
            "names: a journal name is created again after a completed delete"
        );
    }
}

/// A scrub record folded at `lsn` (#362 (background scrubbing)). The
/// scrubber compares the copies of one slot under one ballot: Paxos accepts
/// one value per slot per ballot, so they never differ.
fn scrubbed(lsn: u64, progress: &paros::model::tenant::ScrubProgress) {
    assert_always!(
        progress.mismatched_total == 0,
        "scrub: the copies of a slot under one ballot hold one value",
        { "lsn" => lsn, "mismatched" => progress.mismatched_total }
    );
}
