//! The control journals' follower (#189, #210): how a node learns the cell
//! control journal (the registry: the cell tenant's control journal, #235)
//! and the control journal of every tenant its cell hosts, by reading them.
//!
//! A node that follows them is handed a [`ControlPlan`]: the **seeds** (the
//! nodes that serve the cell control journal), the initial members the
//! registry folds are seeded with (#685 (no founders)), and the spares a
//! registered node joins. Every
//! node folds each journal with the same pure folds a client reads back
//! through ([`paros_model::registry`], [`paros_model::tenant`]): a node reads its own
//! chosen prefix after each tick when it serves the journal, and otherwise
//! keeps one long-polling `Read` per journal open against a seed. The public
//! `Read` *is* how a seed serves a node outside the pool: no peer message
//! from outside the pool is ever needed, so a seed's peer lanes stay
//! pool-only.
//!
//! The folds drive the node (`run_journals` applies each event), the four
//! levels of `docs/architecture/03-1-cell-tenant-and-bootstrap.md` §3.1:
//!
//! - a tenant the cell hosts (`HostTenant`, #210) brings its control journal:
//!   the machines it was born on serve it, any other once a slot of it is
//!   booked there, and every node follows it; a tenant the cell drops stops
//!   with all its journals, for good;
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
    Detach, LocalTaskProvider, LocalTimeProvider, SimulationError, SimulationResult,
};
use moonpool_rpc::RpcHandle;
use paros_core::NodeId;
use paros_core::contract::Party;
use paros_core::journal::{JournalIdentifier, Seq, TenantId};
use paros_core::replica::{LogPage, LogRead};
use paros_net::runtime::ParosProviders;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use paros_model::checkpoint::{Folded, Folder};
use paros_model::registry::{Registry, RegistryEvent, SystemEvent, registry_event};
use paros_model::tenant::{TenantControl, TenantEvent, tenant_event};
use paros_net::NodeClient;
use paros_wire::{Read, ReadAck};

/// A checkpoint the registry fold met: its position, how it was folded
/// (`Some(equal)` verified, `None` restored) and the registry the fold held
/// right after it, see [`paros_observe::audit::Audit::checkpoint_folded`].
pub(crate) type FoldedCheckpoint = (u64, Option<bool>, Registry);

use super::journals::Journals;
use super::transport::peer_target;
use paros_tunables::DriverTunables;

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
    pub class: paros_model::registry::Class,
    /// The nodes that serve the cell control journal, with their addresses:
    /// a node that does not serve a control journal follows it from here.
    pub seeds: Vec<(NodeId, String)>,
    /// The cell control journal's identifier: the registry (#235). No
    /// identifier is fixed (`docs/architecture/03-8-identifiers-checkpoints-recovery-security.md` §3.8): the deployment
    /// drew it.
    pub cell: JournalIdentifier,
    /// The cell's initial members (#685 (no founders)): the members `init`
    /// wrote into the plan journals' first configuration, which the
    /// registry fold is seeded with. No rule reads them after that: the
    /// fold says who is in the pool.
    pub initial_members: Vec<NodeId>,
    /// The matchmaker set every journal the control journals create here
    /// is born with (#190): on a machine, the initial members, which host
    /// every tenant's set until placement (#212). Empty on a deployment
    /// without matchmakers.
    pub matchmakers: Vec<paros_core::membership::MatchmakerId>,
    /// The journals a node outside the initial members joins **as a
    /// spare** once the registry admits it: each a configuration template
    /// (its journal, bootstrap membership, quorum system, matchmakers) whose
    /// identity and pool the driver fills in — the node's own id, and the
    /// pool the registry has admitted. A reconfiguration may then name the
    /// node. Empty where no journal can reconfigure (a deployment without
    /// matchmakers never names a new member).
    pub spares: Vec<paros_core::Config>,
    /// The machines this machine's durable cached registry fold names
    /// (#211), a boot hint: every journal admits their matchmakers to its
    /// matchmaker pool from boot, before the fold reaches their
    /// registrations (#685 (special tenants are tenants)). A cell control
    /// journal re-opened at generation 0 learns the set in force from a
    /// refusal, and that set may name admitted machines: the registry it
    /// would learn them from waits for that set. The node also admits
    /// their peer messages and opens a lane to each from boot
    /// (`ControlFollower::admits`). A name only widens what a journal can
    /// reach, never what it believes. Empty off a machine.
    pub cached: Vec<NodeId>,
    /// The spare templates of the cell plan's journals (the cell control
    /// journal, the election journal and the universe tenant's control
    /// journal) on a machine outside the initial members (#685 (special
    /// tenants are tenants) step 4,
    /// `paros_model::machine::spare_journal_config`): a booking of one
    /// here starts it as a spare, and a release of that booking stops it.
    /// Empty on an initial member, which serves them from its plan.
    pub planned: Vec<paros_core::Config>,
}

/// One remote follow read's answer, back on the loop.
pub(crate) struct Followed {
    journal: JournalIdentifier,
    reply: ReadAck,
}

/// The node's follow of the control journals, and their folds.
pub(crate) struct ControlFollower<P: ParosProviders> {
    self_id: NodeId,
    class: paros_model::registry::Class,
    /// The nodes a remote read goes to, in turn, by id: each dialed where
    /// the node last learned it is (#505 (stranded admitted machine)).
    seeds: Vec<(NodeId, NodeClient<P>)>,
    /// What a moved seed is dialed again through.
    rpc: RpcHandle<P>,
    names: paros_model::address::Names,
    /// The position of the newest book the node adopted from outside its
    /// fold (#505 (stranded admitted machine)): a registration the fold
    /// meets below it is older than that book and moves no seed.
    adopted: u64,
    /// Senders every node accepts whatever the registry says: the
    /// deployment's static address book (replicas included).
    fixed: BTreeSet<NodeId>,
    /// The initial members (`ControlPlan::initial_members`): the registry
    /// fold's seed, and the cell control journal's first configuration.
    initial_members: Vec<NodeId>,
    /// The matchmaker set a created journal is born with (#190).
    matchmakers: Vec<paros_core::membership::MatchmakerId>,
    registry: Folder<Registry>,
    /// The registry's identifier.
    registry_key: JournalIdentifier,
    /// The control journal of every tenant the cell hosts, by tenant.
    tenants: BTreeMap<TenantId, Folder<TenantControl>>,
    /// The tenants the cell dropped: every journal of theirs is unknown here.
    dropped: BTreeSet<TenantId>,
    /// Checkpoints the registry fold met since the driver last took them:
    /// `(seq, verified, state)`, see [`paros_observe::audit::Audit::checkpoint_folded`].
    checkpoints: Vec<FoldedCheckpoint>,
    /// Control journals with a remote read in flight.
    outstanding: BTreeSet<JournalIdentifier>,
    /// Each followed journal's next seed, in turn: a journal's next read asks
    /// the seed after the one its last read asked (#607 (set left on its old
    /// machines)). One counter shared by every journal pinned each journal to
    /// one seed whenever the remote journals were a multiple of the seeds
    /// (witness 16710777052220853539 on a04c94d: three remote journals over
    /// three seeds, an admitted machine read the registry only from an initial
    /// member that stayed down, never folded its matchmaker booking and dropped
    /// every handover `Bootstrap` for the set, red on "sets: every tenant's set
    /// in force is its booked machines after chaos").
    seed_turns: BTreeMap<JournalIdentifier, usize>,
    /// Journals a tenant deleted.
    tombstones: BTreeSet<JournalIdentifier>,
    spares: Vec<paros_core::Config>,
    /// The machines the plan's cached registry fold names (`ControlPlan::cached`).
    cached: Vec<NodeId>,
    /// The plan journals' spare templates (`ControlPlan::planned`).
    planned: Vec<paros_core::Config>,
    replies: mpsc::Sender<Followed>,
    timeout: Duration,
    shutdown: CancellationToken,
}

impl<P: ParosProviders> ControlFollower<P> {
    /// A follower for `plan`, reading remotely through `rpc`; `fixed` is the
    /// static address book. Returns the inbox remote answers arrive on.
    ///
    /// # Errors
    ///
    /// A seed address that does not parse.
    pub(crate) fn new(
        plan: &ControlPlan,
        rpc: &RpcHandle<P>,
        names: &paros_model::address::Names,
        fixed: impl IntoIterator<Item = NodeId>,
        tunables: &DriverTunables,
        shutdown: CancellationToken,
    ) -> SimulationResult<(Self, mpsc::Receiver<Followed>)> {
        if plan.seeds.is_empty() {
            return Err(SimulationError::InvalidState(
                "a control plan names at least one seed".into(),
            ));
        }
        // A node never reads itself remotely: a `Read` is served from the
        // serving node's own fold, which this node's local follow reads
        // already. An initial member's plan names every initial member, so
        // the only initial member of a cell keeps no seed until it adopts
        // a book. Witness: seed 9998526043558789590 on PR #895
        // (fix-main/acceptor-follow): the one initial member, with no
        // leader at boot, asked only itself for the registry; those reads
        // could never move its fold.
        let seeds = plan
            .seeds
            .iter()
            .filter(|(id, _)| *id != plan.self_id)
            .map(|(id, addr)| {
                Ok((
                    *id,
                    NodeClient::named(rpc, names.clone(), peer_target(addr)?),
                ))
            })
            .collect::<SimulationResult<Vec<_>>>()?;
        assert!(
            seeds.iter().all(|(seed, _)| *seed != plan.self_id),
            "a node is never its own seed"
        );
        assert!(
            !seeds.is_empty() || plan.seeds.iter().all(|(id, _)| *id == plan.self_id),
            "only a plan that names this node alone leaves no seed"
        );
        let (replies, inbox) = mpsc::channel(4);
        // A tail wait answers empty after at most `max_wait_ms` (#241): the
        // follow's deadline covers it and one delivery either way.
        let poll = super::log_reads::ReadLimits::of(tunables).longest_wait();
        let timeout = poll + tunables.delivery_timeout.saturating_mul(2);
        let fixed: BTreeSet<NodeId> = fixed.into_iter().collect();
        // The initial members are in the pool by the fold's seed alone, as
        // any machine the registry holds is (#685 (no founders)): a retired
        // one is refused like any other.
        let registry = Folder::new(Registry::new(plan.initial_members.iter().copied()));
        assert!(
            plan.initial_members
                .iter()
                .all(|n| registry.state().contains(*n)),
            "the seeded fold holds every initial member"
        );
        Ok((
            Self {
                self_id: plan.self_id,
                class: plan.class,
                seeds,
                rpc: rpc.clone(),
                names: names.clone(),
                adopted: 0,
                fixed,
                initial_members: plan.initial_members.clone(),
                matchmakers: plan.matchmakers.clone(),
                registry,
                registry_key: plan.cell,
                tenants: BTreeMap::new(),
                dropped: BTreeSet::new(),
                checkpoints: Vec::new(),
                outstanding: BTreeSet::new(),
                seed_turns: BTreeMap::new(),
                tombstones: BTreeSet::new(),
                spares: plan.spares.clone(),
                cached: plan.cached.clone(),
                planned: plan.planned.clone(),
                replies,
                timeout,
                shutdown,
            },
            inbox,
        ))
    }

    /// Dial seed `id` at `addr` from its next read on: a seed that moved
    /// (#349), which a node that serves no control journal learns from its
    /// registry fold or from a book its cell coordinator handed it (#505
    /// (stranded admitted machine)). A node that is no seed is ignored.
    /// Whether the seed moved.
    pub(crate) fn reseed(&mut self, id: NodeId, addr: &paros_model::address::Address) -> bool {
        assert!(id != self.self_id, "a node is never its own seed");
        let count = self.seeds.len();
        let Some((_, client)) = self
            .seeds
            .iter_mut()
            .find(|(seed, client)| *seed == id && client.address() != addr)
        else {
            return false;
        };
        *client = NodeClient::named(&self.rpc, self.names.clone(), addr.clone());
        assert_eq!(self.seeds.len(), count, "a moved seed stays one seed");
        assert!(
            self.seeds
                .iter()
                .any(|(seed, c)| *seed == id && c.address() == addr),
            "a moved seed is dialed at its new address"
        );
        moonpool_assertions::reachable!("driver: a control follower dials a moved seed");
        true
    }

    /// Adopt `book`, the cell's machines at registry position `position`,
    /// handed from outside the fold (#505 (stranded admitted machine)): an
    /// admitted machine whose every seed moved, or was never a seed, learns
    /// where they are from its cell coordinator. A machine `book` names
    /// that serves the cell control journal by the fold ([`Self::holds_registry`],
    /// #685 (no founders)) becomes a seed, and every machine it names when
    /// it names none of those: a machine admitted while one initial member
    /// alone had registered starts with that one seed, and when it is wiped
    /// the fold never reads past it (replay-main 1952391620211990956 on
    /// ec801f5: the admitted machine never booked its tenant's matchmaker
    /// set, every handover onto it froze the set, and a journal it held
    /// never elected a leader). Each seed `book` names is dialed there from
    /// its next read on, unless the node adopted a newer book already. The
    /// seeds that moved, at their new addresses.
    pub(crate) fn adopt(
        &mut self,
        position: u64,
        book: &[(NodeId, paros_model::address::Address)],
    ) -> Vec<(NodeId, paros_model::address::Address)> {
        if position <= self.adopted {
            return Vec::new();
        }
        self.adopted = position;
        let me = self.self_id;
        let count = self.seeds.len();
        let serving = book
            .iter()
            .any(|(id, _)| *id != me && self.holds_registry(*id));
        for (id, addr) in book {
            if *id != me
                && (!serving || self.holds_registry(*id))
                && !self.seeds.iter().any(|(seed, _)| seed == id)
            {
                self.seeds.push((
                    *id,
                    NodeClient::named(&self.rpc, self.names.clone(), addr.clone()),
                ));
            }
        }
        assert!(
            self.seeds.len() >= count,
            "an adopted book never drops a seed"
        );
        assert!(
            self.seeds.iter().all(|(seed, _)| *seed != me),
            "a node is never its own seed"
        );
        let seeds: Vec<NodeId> = self.seeds.iter().map(|(id, _)| *id).collect();
        let moved: Vec<(NodeId, paros_model::address::Address)> = book
            .iter()
            .filter(|(id, _)| *id != me && seeds.contains(id))
            .filter(|(id, addr)| self.reseed(*id, addr))
            .cloned()
            .collect();
        assert!(
            moved.len() <= seeds.len(),
            "a book moves each seed at most once"
        );
        assert_eq!(self.adopted, position, "the adopted position is the book's");
        moved
    }

    /// The seeds a remote read goes to, by id, in turn.
    #[cfg(test)]
    pub(crate) fn seed_ids(&self) -> Vec<NodeId> {
        self.seeds.iter().map(|(id, _)| *id).collect()
    }

    /// The control journals with a remote read in flight.
    #[cfg(test)]
    pub(crate) fn reads_in_flight(&self) -> usize {
        self.outstanding.len()
    }

    /// This node's identity.
    pub(crate) fn self_id(&self) -> NodeId {
        self.self_id
    }

    /// The matchmaker set a journal of `tenant` is born with (#190): a
    /// hosted tenant's generation 0 is the machines it was born on (#685
    /// (no founders)); a plan tenant's is the plan's. Empty on a deployment
    /// without matchmakers.
    pub(crate) fn matchmakers_of(
        &self,
        tenant: TenantId,
    ) -> Vec<paros_core::membership::MatchmakerId> {
        if self.matchmakers.is_empty() {
            return Vec::new();
        }
        match self.registry().hosted_tenant(tenant) {
            Some(hosted) => {
                assert!(
                    !hosted.members.is_empty(),
                    "a hosted tenant is born on machines"
                );
                hosted
                    .members
                    .iter()
                    .map(|id| paros_core::membership::MatchmakerId(id.0))
                    .collect()
            }
            None => self.matchmakers.clone(),
        }
    }

    /// The generation 0 of every hosted tenant's set: the machines it was
    /// born on (#685 (no founders)).
    pub(crate) fn hosted_bootstraps(&self) -> BTreeMap<TenantId, Vec<NodeId>> {
        let registry = self.registry();
        registry
            .hosted()
            .filter_map(|tenant| {
                registry
                    .hosted_tenant(tenant)
                    .map(|hosted| (tenant, hosted.members.clone()))
            })
            .collect()
    }

    /// The initial members: the registry fold's seed (#685 (no founders)).
    pub(crate) fn initial_members(&self) -> &[NodeId] {
        &self.initial_members
    }

    /// Whether `id` serves the cell control journal as this node's fold
    /// knows it: it holds one of the journal's acceptor slots, or, while
    /// none is booked, it is of the journal's first configuration (the
    /// initial members, #685 (no founders)). The same rule leads the cell
    /// coordinator's followed book.
    pub(crate) fn holds_registry(&self, id: NodeId) -> bool {
        let holders = paros_model::machine::control_members(self.registry(), self.registry_key);
        let holds = if holders.is_empty() {
            self.initial_members.contains(&id)
        } else {
            holders.contains(&id)
        };
        assert!(
            !holds || !holders.is_empty() || !self.initial_members.is_empty(),
            "a holder of the registry is a holder or an initial member"
        );
        holds
    }

    /// Whether a peer message from `from` is accepted: a proxy leader (no
    /// pool names one), a node of the static address book, a node the
    /// registry fold has in the pool, or a machine the durable cached
    /// registry fold names (`ControlPlan::cached`).
    ///
    /// The cached machines pass from boot, before the fold reaches their
    /// registrations: the fold may wait on them. A cell's only initial
    /// member that restarts folds the registry only from a leader it
    /// follows, and it reads no seed. When the cell control journal's other
    /// members are admitted machines, no leader is elected without its
    /// promise, so a refusal of their messages waits on itself. Witness
    /// (cite only): replay-main 10421740740893916363 on 8dad9e7, an initial
    /// member restarted with its fold at position 0 while both admitted
    /// members had moved to new names no one registered. It refused their
    /// every `Prepare` for the 400 s tail, and the cell had no leader (red on
    /// "election: the election settles on one renewing leader after chaos"
    /// and five more; green with this rule). A cached name widens who may
    /// speak, never what the node believes, as it does for the matchmaker
    /// pools.
    ///
    /// A machine of another cell passes for a journal of `tenant` while the
    /// registry names it among the machines a move of `tenant` uses (#782
    /// (peers of another cell)).
    pub(crate) fn admits(&self, from: Party, tenant: TenantId) -> bool {
        match from {
            Party::Proxy(_) => true,
            Party::Node(node) => {
                self.fixed.contains(&node)
                    || self.registry.state().contains(node)
                    || self.cached.contains(&node)
                    || self.move_peer(tenant, node)
            }
        }
    }

    /// Whether `node` is a machine of another cell that a move of `tenant`
    /// uses (#782 (peers of another cell)).
    pub(crate) fn move_peer(&self, tenant: TenantId, node: NodeId) -> bool {
        self.registry
            .state()
            .move_peers(tenant)
            .is_some_and(|p| p.peers.iter().any(|peer| peer.id == node))
    }

    /// The pool the registry fold has admitted: every node it holds not
    /// retired, the initial members from its seed included.
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
        self.class.stores()
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
    /// The machines the plan's cached registry fold names: matchmakers
    /// every journal reaches from boot (`ControlPlan::cached`).
    pub(crate) fn cached(&self) -> &[NodeId] {
        &self.cached
    }

    /// The spare template of plan journal `journal` on this machine, when
    /// it is one this machine may serve as a spare (`ControlPlan::planned`).
    pub(crate) fn planned(&self, journal: JournalIdentifier) -> Option<&paros_core::Config> {
        let template = self.planned.iter().find(|config| config.journal == journal);
        if let Some(config) = template {
            assert!(
                config.id == self.self_id,
                "a spare template names this node"
            );
        }
        template
    }

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

    /// Fold one remote answer: the events it folded, and whether the page
    /// left the fold **behind** the seed's committed end (a page with
    /// records that stops short of `next_seq`: the seed's page limit cut
    /// it, so the next read need not wait for a tick).
    pub(crate) fn fold_remote(&mut self, followed: Followed) -> (Vec<(u64, SystemEvent)>, bool) {
        let Followed { journal, reply } = followed;
        // Every answer closes the one read `poll_remote` opened for it.
        let was_outstanding = self.outstanding.remove(&journal);
        assert!(was_outstanding, "a follow answer closes an open read");
        // A tenant dropped while its read was in flight is followed no more.
        if !self.follows(journal) || reply.unknown_journal || !reply.served {
            return (Vec::new(), false);
        }
        if reply.truncated {
            let floor = reply.state.as_ref().map_or(0, |state| state.first_seq);
            self.jump(journal, floor);
            return (Vec::new(), false);
        }
        let end = reply.from_seq + reply.records.len() as u64;
        // A page the local fold overtook while the read was in flight (the
        // journal opened here after the read left: a reboot, a quarantine
        // that ended) holds nothing new: it leaves the cursor where it
        // stands, and the next read opens there on the next tick.
        let overtaken = self.cursor(journal) >= end;
        if overtaken && !reply.records.is_empty() {
            moonpool_assertions::reachable!(
                "driver: a local fold overtook a follow page in flight"
            );
        }
        let behind = !overtaken
            && !reply.records.is_empty()
            && reply
                .state
                .as_ref()
                .is_some_and(|state| state.next_seq > end);
        let events = self.fold(journal, reply.from_seq, reply.records);
        assert!(
            !behind || !self.follows(journal) || self.cursor(journal) <= end,
            "a page behind the end leaves the cursor at most at its end"
        );
        assert!(
            !overtaken || events.is_empty(),
            "a page the fold overtook surfaces nothing"
        );
        (events, behind)
    }

    /// The cell hosts `tenant` (its control journal `control`): follow it.
    fn host(&mut self, tenant: TenantId, control: paros_core::journal::JournalId) {
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
        // A tenant that moved out (#785 (moved record)) is followed no more.
        self.tenants.retain(|t, _| registry.hosts(*t));
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
                    RegistryEvent::TenantHosted { tenant, control }
                    | RegistryEvent::TenantAdopted {
                        tenant, control, ..
                    } => {
                        self.host(*tenant, *control);
                    }
                    RegistryEvent::TenantDropped { tenant } => self.drop_tenant(*tenant),
                    // A tenant that moved out is followed no more (#785
                    // (moved record)); not a tombstone: it may come back.
                    RegistryEvent::TenantMovedOut { tenant, .. } => {
                        self.tenants.remove(tenant);
                        self.seed_turns.retain(|j, _| j.tenant != *tenant);
                    }
                    RegistryEvent::Checkpoint { .. } => self.rehost(),
                    // A seed registered at another address (#349): the next
                    // read dials it there, unless an adopted book is newer. An
                    // initial member that never registered yet (it moved before
                    // its first registration landed) moves with a `Registered`,
                    // not a `Reregistered`: the moved-initial member scenario's
                    // moves reached no follower's seeds through the fold until
                    // both counted (PR #543 (stranded admitted machine)).
                    RegistryEvent::Registered { id, addr, .. }
                    | RegistryEvent::Reregistered { id, addr, .. }
                        if *id != self.self_id && seq >= self.adopted =>
                    {
                        if let Ok(addr) = paros_model::address::Address::parse(addr) {
                            self.reseed(*id, &addr);
                        }
                    }
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

    /// The machines of another cell a remote read of `journal` may also
    /// ask: a moving tenant's control journal runs there (#782 (peers of
    /// another cell)). Empty for every other journal.
    fn peer_seeds(&self, journal: JournalIdentifier) -> Vec<NodeClient<P>> {
        if journal == self.registry_key {
            return Vec::new();
        }
        let Some(peers) = self.registry.state().move_peers(journal.tenant) else {
            return Vec::new();
        };
        peers
            .peers
            .iter()
            .filter(|peer| peer.id != self.self_id)
            .filter_map(|peer| {
                let target = peer_target(&peer.addr).ok()?;
                Some(NodeClient::named(&self.rpc, self.names.clone(), target))
            })
            .collect()
    }

    /// The machines a tenant's control journal runs on that are not seeds
    /// (#685 (no founders)): the machines it was born on and every machine
    /// booked an acceptor slot of it, dialed where the registry has them.
    /// A tenant born on admitted machines runs its control journal on no
    /// seed, and a machine that runs none of it learns its creates only
    /// from them (seed 5507103556224552505 on 57c1bfa: a machine booked for
    /// a created journal never learned the create, and the journal never
    /// started).
    pub(crate) fn member_seeds(&self, journal: JournalIdentifier) -> Vec<NodeClient<P>> {
        if journal == self.registry_key {
            return Vec::new();
        }
        let registry = self.registry.state();
        let Some(hosted) = registry.hosted_tenant(journal.tenant) else {
            return Vec::new();
        };
        let target = paros_model::registry::BookingTarget::Journal(journal);
        let mut members: Vec<NodeId> = hosted
            .members
            .iter()
            .copied()
            .chain(
                registry
                    .bookings()
                    .filter(|(_, b)| {
                        b.role == paros_model::registry::Role::Acceptor && b.target == target
                    })
                    .map(|(_, b)| b.node),
            )
            .filter(|node| *node != self.self_id)
            .filter(|node| self.seeds.iter().all(|(seed, _)| seed != node))
            .collect();
        members.sort_unstable();
        members.dedup();
        assert!(
            !members.contains(&self.self_id),
            "a node is never its own seed"
        );
        members
            .iter()
            .filter_map(|node| {
                let target = peer_target(&registry.get(*node)?.addr).ok()?;
                Some(NodeClient::named(&self.rpc, self.names.clone(), target))
            })
            .collect()
    }

    /// Open a remote follow read of every followed journal `local` says
    /// this node does not run and that has none in flight, each to the seed
    /// after the one its own last read asked.
    pub(crate) fn poll_remote(&mut self, providers: &P, local: impl Fn(JournalIdentifier) -> bool) {
        let followed = self.followed();
        // A dropped tenant's journal keeps no turn.
        self.seed_turns
            .retain(|journal, _| followed.contains(journal));
        for (index, journal) in followed.into_iter().enumerate() {
            if local(journal) || self.outstanding.contains(&journal) {
                continue;
            }
            // Each journal starts at its own seed, so one tick's reads
            // spread over the seeds. A moving tenant's control journal is
            // also read from the machines of the other cell it uses (#782
            // (peers of another cell)): it runs there before or after the
            // move.
            let mut peers = self.peer_seeds(journal);
            let moving = peers.len();
            let members = self.member_seeds(journal);
            if !members.is_empty() {
                moonpool_assertions::reachable!(
                    "driver: a control journal is followed from its booked machines"
                );
            }
            peers.extend(members);
            let targets = self.seeds.len() + peers.len();
            // A node with no seed but itself and no booked machine to ask
            // reads from its own fold alone, until it adopts a book.
            if targets == 0 {
                continue;
            }
            let turn = self.seed_turns.entry(journal).or_insert(index);
            let pick = *turn % targets;
            *turn = pick + 1;
            assert!(*turn <= targets, "a journal's turn never outruns the seeds");
            let client = match pick.checked_sub(self.seeds.len()) {
                None => self.seeds[pick].1.clone(),
                Some(peer) => {
                    if peer < moving {
                        moonpool_assertions::reachable!(
                            "driver: a control journal is followed from another cell"
                        );
                    }
                    peers[peer].clone()
                }
            };
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
                .local_task()
                .spawn_local_task("paros-system-follow", async move {
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
        // Every followed journal this node does not run has a read in
        // flight, unless it has no one to ask.
        assert!(
            self.followed().iter().all(|journal| local(*journal)
                || self.outstanding.contains(journal)
                || (self.seeds.is_empty()
                    && self.peer_seeds(*journal).is_empty()
                    && self.member_seeds(*journal).is_empty())),
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

/// Whether a followed control journal is read from this node's own fold
/// ([`follow_local`]) and not from a seed: only while this node serves it
/// as an acceptor that follows a leader. A spare (in the pool, not a member)
/// learns the chosen prefix only from the leader's messages, and its fold
/// may not admit that leader yet: the pool grows from the registry fold,
/// which grows from the chosen prefix. So a spare keeps the remote read. So
/// does an acceptor with no leader: its own replica moves only on the beats
/// of a leader it follows, and a leader its fold does not hold yet is
/// refused at the edge before the core sees it, so the fold, read from that
/// replica, would wait on itself. Both reads may run: a fold skips what it
/// already folded. Witnesses: recipe `14300594800709187392
/// 57117:11817197411534817122` on f92348a, a restarted spare of the tenant's
/// control journal refused every message of its leader and never folded the
/// set booked on it; seed 10938708657658491908 on a1d1506, an initial member
/// that rebooted with its fold at position 0 behind the cell control
/// journal's leader, an admitted machine, refused that leader's every beat
/// as unpooled for the rest of the run and never converged on the journals
/// it led ("every node converges to the cluster's chosen prefix at the end
/// of the settle tail").
pub(crate) fn followed_locally(node: Option<&paros_core::ColocatedNode>) -> bool {
    let Some(node) = node else {
        return false;
    };
    if !node.is_acceptor() {
        moonpool_assertions::reachable!(
            "driver: a spare keeps a remote follow of a journal it serves"
        );
        return false;
    }
    if node.leader().is_none() {
        moonpool_assertions::reachable!(
            "driver: an acceptor with no leader keeps a remote follow of a journal it serves"
        );
        return false;
    }
    true
}

/// Fold every followed journal this node runs from its own journal fold,
/// page by page, to the end: `(journal, events)` for each that moved, the
/// registry first (a tenant it hosts is folded in the same pass). A local
/// read needs no confirmation: it is this node's own fold, and the driver
/// only acts on what it folded.
pub(crate) fn follow_local<P: ParosProviders, S, A>(
    follower: &mut ControlFollower<P>,
    journals: &Journals<S, A>,
) -> Vec<(JournalIdentifier, Vec<(u64, SystemEvent)>)> {
    let page_records = usize::try_from(FOLLOW_READ_RECORDS).unwrap_or(usize::MAX);
    let mut moved = Vec::new();
    for journal in follower.followed() {
        // A flying journal's fold is not yet durable (`JournalRt::settled`):
        // it is read once its batch landed.
        let Some(rt) = journals.live.get(&journal).filter(|rt| rt.settled()) else {
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

#[cfg(test)]
mod follow_tests {
    use super::followed_locally;
    use paros_core::contract::{Config, Limits, MemStorage};
    use paros_core::journal::{JournalId, JournalIdentifier, TenantId};
    use paros_core::membership::MatchmakerId;
    use paros_core::{Ballot, ColocatedNode, Message, NodeId};

    fn node(id: u64, peers: &[u64]) -> ColocatedNode {
        let storage = MemStorage::new(Config {
            peers: peers.iter().copied().map(NodeId).collect(),
            nodes: vec![NodeId(1), NodeId(2)],
            matchmakers: vec![MatchmakerId(1)],
            ..Config::new(
                NodeId(id),
                JournalIdentifier::new(TenantId(1), JournalId(1)),
            )
        });
        ColocatedNode::new(&storage, Limits::DEFAULT)
    }

    /// `node` follows node 2 as leader: it took node 2's beat.
    fn led(mut node: ColocatedNode) -> ColocatedNode {
        node.step(Message::Heartbeat {
            from: NodeId(2),
            ballot: Ballot {
                round: 1,
                node: NodeId(2),
            },
            commit: None,
            config: None,
            fence: None,
            gc_floor: None,
        });
        assert_eq!(node.leader(), Some(NodeId(2)), "the beat was followed");
        node
    }

    #[test]
    fn an_acceptor_that_follows_a_leader_follows_its_own_fold() {
        assert!(followed_locally(Some(&led(node(1, &[1, 2])))));
    }

    /// An acceptor with no leader keeps the remote read: its own replica
    /// moves only on a leader's beats, and a leader its fold does not hold
    /// is refused at the edge (seed 10938708657658491908 on a1d1506).
    #[test]
    fn an_acceptor_with_no_leader_keeps_the_remote_follow() {
        let booted = node(1, &[1, 2]);
        assert!(booted.is_acceptor(), "the node is a member");
        assert!(
            booted.leader().is_none(),
            "a booted node follows no leader yet"
        );
        assert!(!followed_locally(Some(&booted)));
    }

    #[test]
    fn a_spare_keeps_the_remote_follow() {
        let spare = node(2, &[1]);
        assert!(!spare.is_acceptor(), "the spare is pooled, not a member");
        assert!(!followed_locally(Some(&spare)));
    }

    #[test]
    fn a_journal_this_node_does_not_serve_is_followed_remotely() {
        assert!(!followed_locally(None));
    }
}
