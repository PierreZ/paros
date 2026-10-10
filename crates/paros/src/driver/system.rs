//! The control journals' follower (#189, #210): how a node learns the cell
//! control journal (the registry: the cell tenant's control journal, #235)
//! and the control journal of every tenant its cell hosts, by reading them.
//!
//! A node that follows them is handed a [`ControlPlan`]: the **seeds** (the
//! nodes that serve the cell control journal), the founding members the
//! registry folds start from, and the spares a registered node joins. Every
//! node folds each journal with the same pure folds a client reads back
//! through ([`crate::system`], [`crate::tenant`]): a node reads its own
//! chosen prefix after each tick when it serves the journal, and otherwise
//! keeps one long-polling `Read` per journal open against a seed. The public
//! `Read` *is* how a seed serves a node outside the pool: no peer message
//! from outside the pool is ever needed, so a seed's peer lanes stay
//! pool-only.
//!
//! The folds drive the node (`run_journals` applies each event), the four
//! levels of `docs/architecture.md` §3.1:
//!
//! - a tenant the cell hosts (`HostTenant`, #210) brings its control journal:
//!   every founding member serves it (the placement until #212), and every
//!   node follows it; a tenant the cell drops stops with all its journals,
//!   for good;
//! - a journal a tenant's control journal creates naming this node starts
//!   here, inside that tenant, and a tombstoned one stops here for good (a
//!   call naming it is then refused as unknown);
//! - a node the registry admits gets a peer lane (its registered address),
//!   and a peer message from a node the fold does not have in the pool is
//!   refused before the core sees it — a liveness cost until the fold
//!   catches up, never a safety one (a configuration still binds its
//!   membership to a ballot);
//! - this node's own retirement stops every user journal it serves.
//!
//! The follow is volatile: every incarnation folds every journal again from
//! position 0, so a restart re-derives exactly what it knew. Both folds are
//! [`Folder`]s: a read below the floor jumps to it, the checkpoint there is
//! restored (a fold that held every position below verifies it instead),
//! and the driver re-applies the whole restored state (§3.9, #230). The
//! remote reads run on detached tasks that consult no hook and draw no
//! randomness; which seed a read goes to is chosen on the loop,
//! round-robin, and the answer comes back through an inbox.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use moonpool_core::{
    Detach, Providers, SimulationError, SimulationResult, TaskProvider, TimeProvider,
};
use moonpool_rpc::RpcHandle;
use paros_core::{JournalIdentifier, LogPage, LogRead, NodeId, Party, Seq, TenantId};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::client::checkpoint::{Folded, Folder};
use crate::rpc::{NodeClient, Read, ReadAck};
use crate::system::{Registry, RegistryEvent, SystemEvent, registry_event};
use crate::tenant::{TenantControl, TenantEvent, tenant_event};

/// A checkpoint the registry fold met: its position, how it was folded
/// (`Some(equal)` verified, `None` restored) and the registry the fold held
/// right after it, see [`crate::Audit::checkpoint_folded`].
pub(crate) type FoldedCheckpoint = (u64, Option<bool>, Registry);

use super::config::DriverTunables;
use super::journals::Journals;
use super::transport::peer_target;

/// The records one follow read asks for: enough that a control journal's
/// history arrives in a few pages (the server's byte budget keeps the page
/// far below the frame limit).
const FOLLOW_READ_RECORDS: u64 = 256;

// A follow read that asks for no record never moves its cursor.
const _: () = assert!(FOLLOW_READ_RECORDS > 0);

/// The record bytes one local follow read takes: the default `Read` page's
/// budget. Local, so no frame bounds it; a lone larger record still comes.
const FOLLOW_READ_BYTES: usize = 64 * 1024;

/// What a node that follows the **control journals** (#189, #210) is told.
/// `None` is the static deployment of #188: no registry, no tenant, a pool
/// fixed at boot.
#[derive(Clone, Debug)]
pub struct ControlPlan {
    /// This node's identity (a joiner may serve no journal at boot).
    pub self_id: NodeId,
    /// This machine's class (#211, its `MachineFacts::class`): a
    /// `stateless` machine never serves a journal — neither one a tenant
    /// creates naming it nor a spare's.
    pub class: crate::system::Class,
    /// The nodes that serve the cell control journal, with their addresses:
    /// a node that does not serve a control journal follows it from here.
    pub seeds: Vec<(NodeId, String)>,
    /// The cell control journal's identifier: the registry (#235). No
    /// identifier is fixed (`docs/architecture.md` §3.8): the deployment
    /// drew it.
    pub cell: JournalIdentifier,
    /// The cell's founding members: always in the pool, never retired
    /// through the registry, and the members of every tenant control journal
    /// the cell hosts until placement (#212).
    pub founders: Vec<NodeId>,
    /// The journals a node outside the founding members joins **as a
    /// spare** once the registry admits it: each a configuration template
    /// (its journal, bootstrap membership, quorum system, matchmakers) whose
    /// identity and pool the driver fills in — the node's own id, and the
    /// pool the registry has admitted. A reconfiguration may then name the
    /// node. Empty where no journal can reconfigure (a deployment without
    /// matchmakers never names a new member).
    pub spares: Vec<paros_core::Config>,
}

/// One remote follow read's answer, back on the loop.
pub(crate) struct Followed {
    journal: JournalIdentifier,
    reply: ReadAck,
}

/// The node's follow of the control journals, and their folds.
pub(crate) struct ControlFollower<P: Providers> {
    self_id: NodeId,
    class: crate::system::Class,
    seeds: Vec<NodeClient<P>>,
    /// Senders every node accepts whatever the registry says: the founding
    /// members and the deployment's static address book (replicas included).
    fixed: BTreeSet<NodeId>,
    founders: Vec<NodeId>,
    registry: Folder<Registry>,
    /// The registry's identifier.
    registry_key: JournalIdentifier,
    /// The control journal of every tenant the cell hosts, by tenant.
    tenants: BTreeMap<TenantId, Folder<TenantControl>>,
    /// The tenants the cell dropped: every journal of theirs is unknown here.
    dropped: BTreeSet<TenantId>,
    /// Checkpoints the registry fold met since the driver last took them:
    /// `(seq, verified, state)`, see [`crate::Audit::checkpoint_folded`].
    checkpoints: Vec<FoldedCheckpoint>,
    /// Control journals with a remote read in flight.
    outstanding: BTreeSet<JournalIdentifier>,
    next_seed: usize,
    /// Journals a tenant deleted.
    tombstones: BTreeSet<JournalIdentifier>,
    spares: Vec<paros_core::Config>,
    replies: mpsc::Sender<Followed>,
    timeout: Duration,
    shutdown: CancellationToken,
}

impl<P: Providers> ControlFollower<P> {
    /// A follower for `plan`, reading remotely through `rpc`; `fixed` is the
    /// static address book. Returns the inbox remote answers arrive on.
    ///
    /// # Errors
    ///
    /// A seed address that does not parse.
    pub(crate) fn new(
        plan: &ControlPlan,
        rpc: &RpcHandle<P>,
        names: &crate::Names,
        fixed: impl IntoIterator<Item = NodeId>,
        tunables: &DriverTunables,
        shutdown: CancellationToken,
    ) -> SimulationResult<(Self, mpsc::Receiver<Followed>)> {
        let seeds = plan
            .seeds
            .iter()
            .map(|(_, addr)| Ok(NodeClient::named(rpc, names.clone(), peer_target(addr)?)))
            .collect::<SimulationResult<Vec<_>>>()?;
        if seeds.is_empty() {
            return Err(SimulationError::InvalidState(
                "a control plan names at least one seed".into(),
            ));
        }
        let (replies, inbox) = mpsc::channel(4);
        // A tail wait answers empty after at most `max_wait_ms` (#241): the
        // follow's deadline covers it and one delivery either way.
        let poll = super::log_reads::ReadLimits::of(tunables).longest_wait();
        let timeout = poll + tunables.delivery_timeout.saturating_mul(2);
        let mut fixed: BTreeSet<NodeId> = fixed.into_iter().collect();
        fixed.extend(plan.founders.iter().copied());
        assert!(
            plan.founders.iter().all(|n| fixed.contains(n)),
            "the founding members are always admitted"
        );
        Ok((
            Self {
                self_id: plan.self_id,
                class: plan.class,
                seeds,
                fixed,
                founders: plan.founders.clone(),
                registry: Folder::new(Registry::new(plan.founders.iter().copied())),
                registry_key: plan.cell,
                tenants: BTreeMap::new(),
                dropped: BTreeSet::new(),
                checkpoints: Vec::new(),
                outstanding: BTreeSet::new(),
                next_seed: 0,
                tombstones: BTreeSet::new(),
                spares: plan.spares.clone(),
                replies,
                timeout,
                shutdown,
            },
            inbox,
        ))
    }

    /// This node's identity.
    pub(crate) fn self_id(&self) -> NodeId {
        self.self_id
    }

    /// The founding members: the members of every tenant control journal
    /// until placement (#212).
    pub(crate) fn founders(&self) -> &[NodeId] {
        &self.founders
    }

    /// Whether a peer message from `from` is accepted: a proxy leader (no
    /// pool names one), a node of the static address book, or a node the
    /// registry fold has in the pool.
    pub(crate) fn admits(&self, from: Party) -> bool {
        match from {
            Party::Proxy(_) => true,
            Party::Node(node) => self.fixed.contains(&node) || self.registry.state().contains(node),
        }
    }

    /// The pool the registry fold has admitted: the founding members and
    /// every registered node not retired.
    pub(crate) fn pool(&self) -> Vec<NodeId> {
        self.registry.state().pool()
    }

    /// The registry as folded so far.
    pub(crate) fn registry(&self) -> &Registry {
        self.registry.state()
    }

    /// The control journal of `tenant` as folded so far, while the cell
    /// hosts it.
    pub(crate) fn tenant(&self, tenant: TenantId) -> Option<&TenantControl> {
        self.tenants.get(&tenant).map(Folder::state)
    }

    /// Whether this machine takes acceptor work (#211): a `storage` one. A
    /// `stateless` machine never serves a journal.
    pub(crate) fn takes_storage_work(&self) -> bool {
        self.class == crate::system::Class::Storage
    }

    /// The checkpoints the registry fold met since the last call.
    pub(crate) fn take_checkpoints(&mut self) -> Vec<FoldedCheckpoint> {
        let taken = std::mem::take(&mut self.checkpoints);
        assert!(
            taken.windows(2).all(|w| w[0].0 < w[1].0),
            "checkpoints are reported in position order"
        );
        taken
    }

    /// The journals this node joins as a spare once registered.
    pub(crate) fn spares(&self) -> &[paros_core::Config] {
        &self.spares
    }

    /// Whether `journal` is gone for good: a tenant deleted it, or the cell
    /// dropped its tenant.
    pub(crate) fn is_tombstoned(&self, journal: JournalIdentifier) -> bool {
        let tombstoned =
            self.tombstones.contains(&journal) || self.dropped.contains(&journal.tenant);
        // The cell control journal is never tombstoned: its tenant is the
        // cell's own, which the cell never drops.
        if tombstoned {
            assert!(
                journal != self.registry_key,
                "the cell control journal is never tombstoned"
            );
            assert!(
                journal.tenant != self.registry_key.tenant,
                "the cell tenant is never dropped"
            );
        }
        tombstoned
    }

    /// Fold one page of `journal` read from this node's own journal fold.
    pub(crate) fn fold_local(
        &mut self,
        journal: JournalIdentifier,
        page: &LogPage,
    ) -> Vec<(u64, SystemEvent)> {
        // The local follow reads exactly where its cursor stands.
        assert!(
            page.from.0 == self.cursor(journal),
            "a local page starts at the follow cursor"
        );
        let records: Vec<Vec<u8>> = page.records.iter().map(|r| r.0.clone()).collect();
        self.fold(journal, page.from.0, records)
    }

    /// The registry's identifier.
    pub(crate) fn registry_key(&self) -> JournalIdentifier {
        self.registry_key
    }

    /// The control journal of `tenant`, while the cell hosts it.
    fn tenant_key(&self, tenant: TenantId) -> Option<JournalIdentifier> {
        self.tenants
            .get(&tenant)
            .map(|fold| JournalIdentifier::new(tenant, fold.state().control()))
    }

    /// Every journal this node follows: the registry, then each hosted
    /// tenant's control journal.
    pub(crate) fn followed(&self) -> Vec<JournalIdentifier> {
        std::iter::once(self.registry_key)
            .chain(self.tenants.keys().filter_map(|t| self.tenant_key(*t)))
            .collect()
    }

    /// Whether `journal` is a control journal this node follows.
    pub(crate) fn follows(&self, journal: JournalIdentifier) -> bool {
        journal == self.registry_key || self.tenant_key(journal.tenant) == Some(journal)
    }

    /// Where the next read of `journal` starts.
    pub(crate) fn cursor(&self, journal: JournalIdentifier) -> u64 {
        assert!(
            self.follows(journal),
            "a follow cursor names a followed control journal"
        );
        if journal == self.registry_key {
            return self.registry.next_seq();
        }
        self.tenants
            .get(&journal.tenant)
            .map_or(0, Folder::next_seq)
    }

    /// `journal`'s positions below `floor` are gone (a read answered
    /// `truncated`): its fold jumps there, to restore from the checkpoint at
    /// the floor.
    pub(crate) fn jump(&mut self, journal: JournalIdentifier, floor: u64) {
        let before = self.cursor(journal);
        if journal == self.registry_key {
            self.registry.jump(floor);
        } else if let Some(fold) = self.tenants.get_mut(&journal.tenant) {
            fold.jump(floor);
        }
        assert!(
            self.cursor(journal) >= floor,
            "a jump lands at or past the floor"
        );
        assert!(
            self.cursor(journal) >= before,
            "a follow cursor never moves back"
        );
    }

    /// Fold one remote answer.
    pub(crate) fn fold_remote(&mut self, followed: Followed) -> Vec<(u64, SystemEvent)> {
        let Followed { journal, reply } = followed;
        // Every answer closes the one read `poll_remote` opened for it.
        let was_outstanding = self.outstanding.remove(&journal);
        assert!(was_outstanding, "a follow answer closes an open read");
        // A tenant dropped while its read was in flight is followed no more.
        if !self.follows(journal) || reply.unknown_journal || !reply.served {
            return Vec::new();
        }
        if reply.truncated {
            let floor = reply.state.as_ref().map_or(0, |state| state.first_seq);
            self.jump(journal, floor);
            return Vec::new();
        }
        self.fold(journal, reply.from_seq, reply.records)
    }

    /// The cell hosts `tenant` (its control journal `control`): follow it.
    fn host(&mut self, tenant: TenantId, control: paros_core::JournalId) {
        if self.dropped.contains(&tenant) {
            return;
        }
        self.tenants
            .entry(tenant)
            .or_insert_with(|| Folder::new(TenantControl::new(tenant, control)));
    }

    /// The cell dropped `tenant`: follow it no more, and every journal of
    /// it is unknown here from now on.
    fn drop_tenant(&mut self, tenant: TenantId) {
        self.tenants.remove(&tenant);
        self.dropped.insert(tenant);
    }

    /// The registry's hosted tenants changed wholesale (a checkpoint): follow
    /// exactly the hosted ones, and drop the dropped ones.
    fn rehost(&mut self) {
        let registry = self.registry.state().clone();
        for tenant in registry.hosted() {
            if let Some(hosted) = registry.hosted_tenant(tenant) {
                self.host(tenant, hosted.control);
            }
        }
        let dropped: Vec<TenantId> = self
            .tenants
            .keys()
            .copied()
            .filter(|t| registry.dropped(*t))
            .collect();
        for tenant in dropped {
            self.drop_tenant(tenant);
        }
        for tenant in self.tenants.keys() {
            assert!(registry.hosts(*tenant), "a followed tenant is hosted");
        }
    }

    /// Fold `records` of `journal` (dense from position `from`). A record
    /// the fold has already seen is skipped: pages may overlap.
    fn fold(
        &mut self,
        journal: JournalIdentifier,
        from: u64,
        records: Vec<Vec<u8>>,
    ) -> Vec<(u64, SystemEvent)> {
        let mut events = Vec::new();
        let next = from + records.len() as u64;
        for (seq, record) in (from..).zip(records) {
            if !self.follows(journal) {
                break;
            }
            let event = if journal == self.registry_key {
                let Some(event) = self.fold_registry(seq, &record) else {
                    continue;
                };
                match &event {
                    RegistryEvent::TenantHosted { tenant, control } => {
                        self.host(*tenant, *control);
                    }
                    RegistryEvent::TenantDropped { tenant } => self.drop_tenant(*tenant),
                    RegistryEvent::Checkpoint { .. } => self.rehost(),
                    _ => {}
                }
                SystemEvent::Registry(event)
            } else {
                let Some(fold) = self.tenants.get_mut(&journal.tenant) else {
                    break;
                };
                if seq < fold.next_seq() {
                    continue;
                }
                let Some(folded) = fold.fold(seq, &record) else {
                    continue;
                };
                let Some(event) = tenant_event(folded) else {
                    continue;
                };
                if let TenantEvent::Deleted { id, .. } = &event {
                    self.tombstones
                        .insert(JournalIdentifier::new(journal.tenant, *id));
                }
                SystemEvent::Tenant(event)
            };
            events.push((seq, event));
        }
        // Events surface in position order, from inside the folded page.
        assert!(
            events.windows(2).all(|w| w[0].0 < w[1].0),
            "folded events surface in position order"
        );
        assert!(
            events.iter().all(|(seq, _)| *seq >= from && *seq < next),
            "a folded event lies inside its page"
        );
        events
    }

    /// Fold one registry record, keeping every checkpoint it meets for the
    /// audit.
    fn fold_registry(&mut self, seq: u64, record: &[u8]) -> Option<RegistryEvent> {
        let folded = match self.registry.fold(seq, record)? {
            Folded::Checkpoint {
                covers_up_to,
                verified,
            } => {
                // The fold holds the checkpoint's state, whole, and stands
                // just past it: what the audit is handed is the registry at
                // `seq` (#247).
                assert!(
                    self.registry.is_whole(),
                    "a folded checkpoint leaves the fold whole"
                );
                assert!(
                    self.registry.next_seq() == seq + 1,
                    "a folded checkpoint leaves the fold just past it"
                );
                self.checkpoints
                    .push((seq, verified, self.registry.state().clone()));
                Folded::Checkpoint {
                    covers_up_to,
                    verified,
                }
            }
            other => other,
        };
        registry_event(folded)
    }

    /// Open a remote follow read of every followed journal `local` says
    /// this node does not run and that has none in flight, each to the next
    /// seed in turn.
    pub(crate) fn poll_remote(&mut self, providers: &P, local: impl Fn(JournalIdentifier) -> bool) {
        for journal in self.followed() {
            if local(journal) || self.outstanding.contains(&journal) {
                continue;
            }
            let client = self.seeds[self.next_seed % self.seeds.len()].clone();
            self.next_seed = self.next_seed.wrapping_add(1);
            self.outstanding.insert(journal);
            let request = Read {
                journal: journal.journal.0,
                tenant: journal.tenant.0,
                from_seq: self.cursor(journal),
                limit: FOLLOW_READ_RECORDS,
                wait_ms: 0,
            };
            let sink = self.replies.clone();
            let time = providers.time().clone();
            let timeout = self.timeout;
            let shutdown = self.shutdown.clone();
            let self_id = self.self_id.0;
            providers
                .task()
                .spawn_task("paros-system-follow", async move {
                    let answer = moonpool_core::select! {
                        biased;
                        () = shutdown.cancelled() => return,
                        result = time.timeout(timeout, client.read(&request)) => result,
                    };
                    let reply = match answer {
                        Ok(Ok(reply)) => reply,
                        Ok(Err(error)) => {
                            tracing::debug!(node = self_id, journal = %journal, %error, "system_follow_failed");
                            ReadAck { unknown_journal: true, ..ReadAck::default() }
                        }
                        Err(_) => {
                            tracing::debug!(node = self_id, journal = %journal, "system_follow_timed_out");
                            ReadAck { unknown_journal: true, ..ReadAck::default() }
                        }
                    };
                    // A failed read comes back too, so the journal's next
                    // read opens on the next tick.
                    let _ = sink.send(Followed { journal, reply }).await;
                })
                .detach();
        }
        // Every followed journal this node does not run has a read in flight.
        assert!(
            self.followed()
                .iter()
                .all(|journal| local(*journal) || self.outstanding.contains(journal)),
            "every remote control journal is followed"
        );
    }
}

impl Followed {
    /// The control journal the answer is for.
    pub(crate) fn journal(&self) -> JournalIdentifier {
        self.journal
    }
}

/// Fold every followed journal this node runs from its own journal fold,
/// page by page, to the end: `(journal, events)` for each that moved, the
/// registry first (a tenant it hosts is folded in the same pass). A local
/// read needs no confirmation: it is this node's own fold, and the driver
/// only acts on what it folded.
pub(crate) fn follow_local<P: Providers, S, A>(
    follower: &mut ControlFollower<P>,
    journals: &Journals<S, A>,
) -> Vec<(JournalIdentifier, Vec<(u64, SystemEvent)>)> {
    let page_records = usize::try_from(FOLLOW_READ_RECORDS).unwrap_or(usize::MAX);
    let mut moved = Vec::new();
    for journal in follower.followed() {
        let Some(rt) = journals.live.get(&journal) else {
            continue;
        };
        let mut events = Vec::new();
        // A tenant the registry dropped earlier in this pass is followed no
        // more.
        while follower.follows(journal) {
            let from = follower.cursor(journal);
            match rt.node.read_log(Seq(from), page_records, FOLLOW_READ_BYTES) {
                LogRead::Page(page) if page.next().0 > from => {
                    events.extend(follower.fold_local(journal, &page));
                }
                LogRead::Truncated(state) if state.first_seq.0 > from => {
                    follower.jump(journal, state.first_seq.0);
                    if follower.cursor(journal) <= from {
                        break;
                    }
                }
                _ => break,
            }
        }
        if !events.is_empty() {
            moved.push((journal, events));
        }
    }
    assert!(
        moved.iter().all(|(_, events)| !events.is_empty()),
        "only a journal whose fold moved is reported"
    );
    moved
}
