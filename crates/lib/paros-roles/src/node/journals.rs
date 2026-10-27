//! The journals a node serves (#188): one `ColocatedNode` per journal, each
//! with its own store, its own client waiters and its own audit port — the
//! rule "share processes, disks and connections, never protocol state".
//!
//! The node loop owns a [`Journals`] map in id order (iteration order is part
//! of determinism) and routes every client call and every peer message to
//! the journal its envelope names. A journal is **live** (a runtime in the
//! map), **quarantined** (a storage fault ended its incarnation; it re-opens
//! from its store after [`DriverTunables::quarantine_ticks`]), or **down for
//! good** (its store refused to open: a parked or amnesiac store). A node
//! with no live journal and none waiting to re-open has nothing left to
//! serve, and exits with the fault that took the last one — which, for a
//! one-journal node, is exactly the pre-#188 fail-stop crash.
//!
//! The journal id rides the `Deliver` envelope, **per message**, and the
//! loop demuxes on it before any core sees a byte; a message for a journal
//! this node does not run is dropped and the sender's re-send repairs it.
//! The id is never folded into a command fingerprint instead: that would
//! protect only `Accepted`'s vhash, while `Prepare`, `Promise`, `Commit`,
//! `Heartbeat` and catch-up would still cross journals. The matchmaker
//! plane, the proxy leaders and the replica tier are shared processes too
//! (#190, #193): each journal's `Config` names them or not, and they demux
//! on the same identifier.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::task::Poll;

use moonpool_core::RandomProvider;
use paros_core::journal::{JournalIdentifier, TenantId};
use paros_core::{ColocatedNode, NodeId, Slot};
use paros_net::runtime::ParosProviders;

use paros_observe::audit::Audit;
use paros_storage::{LogStorage, StorageError};

use super::boot::{check_format_marker, report_boot_state};
use super::flight::{Flight, Flush, Input, Landing};
use super::ready::{ClientWaiters, storage_fault_crash};
use super::report::{Cadence, Deltas, draw_election_timeout};
use paros_storage::boot::{BootKind, RunError};
use paros_tunables::DriverTunables;

/// The end of what a node's stores host beside its journals
/// ([`JournalStores::host_on_edge`]): resolves once the hosted task ended,
/// or was dropped.
#[derive(Debug)]
pub struct HostEnded(tokio::sync::oneshot::Receiver<()>);

impl HostEnded {
    /// A hosted task's end: the task holds the sender for its whole life.
    #[must_use]
    pub fn pair() -> (tokio::sync::oneshot::Sender<()>, Self) {
        let (alive, ended) = tokio::sync::oneshot::channel();
        (alive, Self(ended))
    }

    /// Wait until the hosted task ended.
    pub async fn wait(self) {
        // The task never sends: its sender drops when it ends.
        let _ = self.0.await;
    }
}

/// Where a node's journals come from (#188): the static list, and a way to
/// open each journal's store — at boot, and again when the driver re-opens
/// a quarantined journal (opening is cheap and touches no device: the
/// store's own boot scan, run by the driver, is where it reads). An opener hands out a **fresh handle over the
/// journal's durable state**, as a process restart would find it.
pub trait JournalStores {
    /// The store every journal runs on.
    /// `Send` and `'static`: a batch's flush owns the store while it flies
    /// (#613 (settle off the node loop)).
    type Store: LogStorage + 'static;
    /// The audit port a journal reports to (one per journal: every
    /// journal-scoped oracle is keyed by it).
    type Audit: Audit + Clone + Send + Sync + 'static;

    /// The journals this node serves, a static list (ids `>= 1`).
    fn journals(&self) -> Vec<JournalIdentifier>;

    /// The created journals whose stores this node holds on disk (#474): a
    /// booking released while the node was down leaves one behind, which
    /// no journal of this incarnation knows. Empty for a static node.
    fn provisioned(&self) -> Vec<JournalIdentifier> {
        Vec::new()
    }

    /// Open `journal`'s store with the operator's boot claim, or `None` when
    /// the journal must stay down on this node for good (its disk is gone).
    ///
    /// Async because the claim may need the disk: an opener whose record
    /// says a format began and never confirmed resolves it by reading the
    /// store's marker (without opening the store), at every open, the
    /// re-open of a quarantined journal included. A format whose sync failed
    /// may have landed anyway, and a first-boot claim on it is refused.
    fn open(
        &mut self,
        journal: JournalIdentifier,
    ) -> impl Future<Output = Option<(Self::Store, BootKind)>>;

    /// The audit port `journal` reports to.
    fn audit(&self, journal: JournalIdentifier) -> Self::Audit;

    /// The audit port the node's own facts report to — what no single
    /// journal owns: the edge's rejections, a peer lane's delivery failures,
    /// a refused journal id, the system journals' folds. Named by the
    /// opener, never borrowed from a journal (no identifier has a default,
    /// §3.8, #243).
    fn node_audit(&self) -> Self::Audit;

    /// The audit port the frontend a machine serves reports to (#723
    /// (frontend as a parosd role)): its resolutions and the calls it
    /// forwards. The node's own port by default.
    fn frontend_audit(&self) -> Self::Audit {
        self.node_audit()
    }

    /// Provision a store for `journal`, a journal a tenant's control journal
    /// created naming this node (#189, #210), or a spare's, under `config`,
    /// its commits under `durability` (the mode its create recorded, #599
    /// (durability mode per journal)); `false` when this opener cannot (the
    /// default: a static list). A later [`JournalStores::open`] of `journal`
    /// opens it. Idempotent: a node that re-folds the control journal after
    /// a restart asks again for a journal it already holds.
    fn create(
        &mut self,
        journal: JournalIdentifier,
        config: paros_core::Config,
        durability: paros_storage::journal::Durability,
    ) -> bool {
        let _ = (journal, config, durability);
        false
    }

    /// Start what this node hosts beside its journals on its own RPC edge
    /// `rpc`, until `shutdown`: a founding member's matchmaker sets (#190),
    /// also served in its `machine` group (#404 (static vs dynamic
    /// endpoints)), serving only requests of cell `cell_id` (#539
    /// (matchmaker cell fence)). The default hosts nothing. What it starts
    /// may run on past `shutdown` while it ends its current step: the
    /// returned [`HostEnded`] resolves once it ended, and the loop waits for
    /// it before the next incarnation opens the same disk.
    ///
    /// # Errors
    ///
    /// A registration the RPC runtime refuses.
    fn host_on_edge<P: ParosProviders>(
        &mut self,
        providers: &P,
        rpc: &moonpool_rpc::RpcHandle<P>,
        machine: Option<&moonpool_rpc::ServiceGroup<paros_wire::methods::MachineInterface>>,
        (tunables, cell_id): (&DriverTunables, u64),
        shutdown: tokio_util::sync::CancellationToken,
    ) -> moonpool_core::SimulationResult<Option<HostEnded>> {
        let _ = (providers, rpc, machine, tunables, cell_id, shutdown);
        Ok(None)
    }

    /// The tenants whose matchmaker set the registry books on this node, as
    /// its fold reads now (#501 (matchmaker sets off the founders)): an
    /// opener that hosts sets beside the journals opens a set outside the
    /// bootstrap generation only when it is booked here. The default does
    /// nothing.
    fn book_sets(&mut self, tenants: std::collections::BTreeSet<paros_core::journal::TenantId>) {
        let _ = tenants;
    }

    /// `journal`'s store passed its boot: scanned, and its format marker
    /// judged against the claim [`JournalStores::open`] returned — on a
    /// first boot, written and synced. From here on the store is
    /// provisioned on disk, so an opener that keeps a provisioning record
    /// outside its stores (#208) records `journal` now, and every later
    /// open is an existing member's. The default does nothing.
    fn opened(&mut self, journal: JournalIdentifier) {
        let _ = journal;
    }

    /// Why the last [`JournalStores::open`] of `journal` answered `None`,
    /// when it was a storage fault a later open may get past (a created
    /// journal's provisioning on a failing disk): the driver quarantines the
    /// journal instead of taking it down for good. Taken once. The default:
    /// never.
    fn open_fault(&mut self, journal: JournalIdentifier) -> Option<RunError> {
        let _ = journal;
        None
    }

    /// `journal` was just quarantined on this node: a storage fault ended its
    /// incarnation, and the driver re-opens it after
    /// [`DriverTunables::quarantine_ticks`]. An opener that already knows
    /// the store will not open again (a disk gone for good) can say so now
    /// rather than at the re-open. The default does nothing.
    fn quarantined(&mut self, journal: JournalIdentifier) {
        let _ = journal;
    }

    /// `journal` was tombstoned (#189): its store will never be opened
    /// again. The default keeps it.
    fn delete(&mut self, journal: JournalIdentifier) {
        let _ = journal;
    }

    /// The node's registry fold reached a new book (#211): an opener that
    /// keeps a cached registry fold beside its stores caches it. Never
    /// waits on the disk. The default keeps nothing.
    fn cache_registry(&self, cache: paros_model::machine::CachedRegistry) {
        let _ = cache;
    }
}

/// The one-journal node [`crate::node::run_node`] runs: the store it was handed,
/// once. A quarantine never re-opens it — a one-journal node that loses its
/// journal has nothing left and exits with the fault, the pre-#188 rule.
pub(crate) struct SingleStore<S, A> {
    pub(crate) journal: JournalIdentifier,
    pub(crate) store: Option<(S, BootKind)>,
    pub(crate) audit: A,
}

impl<S: LogStorage + 'static, A: Audit + Clone + Send + Sync + 'static> JournalStores
    for SingleStore<S, A>
{
    type Store = S;
    type Audit = A;

    fn journals(&self) -> Vec<JournalIdentifier> {
        vec![self.journal]
    }

    async fn open(&mut self, journal: JournalIdentifier) -> Option<(S, BootKind)> {
        // The driver opens only what `journals` listed: the one journal.
        assert!(
            journal == self.journal,
            "a single store opens only its journal"
        );
        let store = self.store.take();
        assert!(self.store.is_none(), "a single store is handed out once");
        store
    }

    fn audit(&self, journal: JournalIdentifier) -> A {
        assert!(
            journal == self.journal,
            "a single store audits only its journal"
        );
        self.audit.clone()
    }

    fn node_audit(&self) -> A {
        self.audit.clone()
    }
}

/// One live journal on this node: everything that used to be the node loop's
/// per-node state.
pub(crate) struct JournalRt<S, A> {
    pub(crate) node: ColocatedNode,
    /// The journal's store; `None` while a batch's flush holds it.
    pub(crate) storage: Option<S>,
    /// The batch whose flush flies, if any (#613 (settle off the node loop)).
    pub(crate) flight: Option<Flight<S>>,
    /// The inputs held back while the batch flies, in arrival order.
    pub(crate) backlog: Vec<Input>,
    /// The read expiry a beat owes the next settle.
    pub(crate) expiry: Option<(u64, bool)>,
    pub(crate) audit: A,
    pub(crate) waiters: ClientWaiters,
    pub(crate) last: Deltas,
    /// Ticks since the open matchmaking request was last (re-)sent.
    pub(crate) match_resend: Cadence,
    /// Ticks since the open GC request was last (re-)sent.
    pub(crate) gc_resend: Cadence,
    /// The overwrites this acceptor already contested, as `(slot, held
    /// value, incoming value)` fingerprints (#396): one contest each, so two
    /// column-mates of a grid never duel forever over one slot. Volatile; a
    /// key below the acceptor's floor is forgotten.
    pub(crate) contested: BTreeSet<(Slot, u64, u64)>,
    /// The members of this journal's configuration the cell holds slow
    /// (#361 (slow machines)), as the registry fold said at the last call
    /// the node loop took: a hint for a grid's column and row, read when
    /// the call steps, which may be after a flight lands.
    pub(crate) slow: BTreeSet<NodeId>,
    /// The tick a full disk took this journal at (#360 (disk full)), while
    /// it stays read-only: the read-only time counts from the fault, so the
    /// quarantine before the re-open is part of it. It votes in
    /// Phase 1 and serves reads, refuses Phase-2 votes, and never leads.
    pub(crate) read_only: Option<u64>,
    /// Whether the journal left read-only and no flush of it landed since:
    /// its next landed flush is the store come back from a full disk.
    pub(crate) back_from_full: bool,
}

impl<S, A> JournalRt<S, A> {
    /// Whether the core holds only durable state: no batch flies. While one
    /// does, the core already holds that batch's effects (a slot its own
    /// unsynced vote decided, a fold walked past it, a `Truncate` applied),
    /// and a crash before the flush lands undoes them. A reader that acts
    /// on the fold (the control-journal follow, the peer book and its
    /// durable cache) reads only a settled core (#613 (settle off the node
    /// loop)): witness 16790173632859305817 on PR #639 (sync hand-off)
    /// folded registry positions 32 to 34 from a flying batch that its
    /// crash then lost, and every other node folded another event there.
    pub(crate) fn settled(&self) -> bool {
        let settled = self.flight.is_none();
        if settled {
            assert!(self.storage.is_some(), "a settled journal holds its store");
        }
        settled
    }
}

/// Boot `journal` from `storage`: the scan, the format marker (#147), the
/// core, the boot report and the first election timeout.
///
/// # Errors
///
/// [`RunError::Storage`] when the scan or the formatting failed,
/// [`RunError::Refused`] when the boot claim and the marker disagree.
pub(crate) async fn boot_journal<P: ParosProviders, S: LogStorage, A: Audit>(
    providers: &P,
    mut storage: S,
    boot: BootKind,
    audit: A,
    tunables: &DriverTunables,
) -> Result<JournalRt<S, A>, RunError> {
    // Stage 7: verify and classify every durable record BEFORE the core
    // reads the store, so no corrupted bytes cross into protocol logic.
    let self_id = storage.initial_state().1.id.0;
    storage
        .boot_scan()
        .await
        .map_err(|e| storage_fault_crash(&audit, self_id, e))?;
    check_format_marker(&mut storage, boot, self_id, &audit).await?;
    let limits = tunables.limits();
    let mut node = ColocatedNode::new(&storage, limits);
    // The core booted from exactly this store's configuration.
    assert!(
        node.config().id.0 == self_id,
        "a journal boots as the node that owns its store"
    );
    report_boot_state(&node, self_id, &audit);
    // The first randomized election timeout (jitter from the driver's RNG).
    let first_timeout = draw_election_timeout(providers, self_id, tunables.election_timeout_base);
    node.set_election_timeout(first_timeout);
    assert!(
        !node.needs_election_timeout(),
        "a booted journal has its first timeout"
    );
    audit.election_timeout_set(NodeId(self_id), first_timeout);
    // The page sizes (#330, #338) and the read window (#386) the core
    // booted under are the driver's.
    let booted = node.limits();
    assert!(
        booted.recovery_page == limits.recovery_page,
        "a booted journal runs the driver's recovery page"
    );
    assert!(
        booted.promise_page == limits.promise_page,
        "a booted journal runs the driver's promise page"
    );
    assert!(
        booted.resend_page == limits.resend_page,
        "a booted journal runs the driver's re-send page"
    );
    assert!(
        booted.apply_page == limits.apply_page,
        "a booted journal runs the driver's apply page"
    );
    assert!(
        booted.read_window >= tunables.read_retry_ticks,
        "a booted journal keeps its reads as long as its driver waits"
    );
    let last = Deltas::new(&node);
    Ok(JournalRt {
        node,
        storage: Some(storage),
        flight: None,
        backlog: Vec::new(),
        expiry: None,
        audit,
        // A fresh random token space per runtime: a reboot or a re-open
        // never reuses a token a dead incarnation's acks may still answer.
        waiters: ClientWaiters::new(providers.random().random()),
        last,
        match_resend: Cadence::default(),
        gc_resend: Cadence::default(),
        contested: BTreeSet::new(),
        slow: BTreeSet::new(),
        read_only: None,
        back_from_full: false,
    })
}

/// A node's journals: the live runtimes, the quarantined ones waiting to
/// re-open (with the tick they went down), and the ones down for good.
pub(crate) struct Journals<S, A> {
    pub(crate) live: BTreeMap<JournalIdentifier, JournalRt<S, A>>,
    /// The flushes of journals that left `live` while their batch flew
    /// (#613 (settle off the node loop)): each lands on its own, and its
    /// journal opens again only after it did.
    orphans: BTreeMap<JournalIdentifier, Flush<S>>,
    quarantined: BTreeMap<JournalIdentifier, u64>,
    /// The quarantined journals whose fault was a full disk (#360 (disk
    /// full)), each with the tick of its fault: each re-opens read-only,
    /// and none ends the node.
    full: BTreeMap<JournalIdentifier, u64>,
    down: BTreeSet<JournalIdentifier>,
    /// Journals quarantined since the loop last told the opener
    /// ([`JournalStores::quarantined`]).
    newly_quarantined: Vec<JournalIdentifier>,
    /// The fault that ended the most recent incarnation, the node's exit
    /// when nothing is left.
    last_fault: Option<RunError>,
    /// The control journals among the ones served (the cell's, the fleet
    /// tenant's, a hosted tenant's): no identifier is fixed (§3.8), so the
    /// deployment says which they are. They serve no user plane and outlive a retirement.
    control: BTreeSet<JournalIdentifier>,
    /// The journal whose configuration carries the deployment (matchmakers,
    /// proxy leaders, replicas), once one booted here: the plane, whether it
    /// is live now or not.
    deployed: Option<JournalIdentifier>,
    /// The node's first journal in its static list: the plane whenever it
    /// names matchmakers, proxies or replicas (#190: every journal may name
    /// matchmakers, so the first one booted is not the plane by itself).
    preferred: Option<JournalIdentifier>,
    /// Every journal that booted in this incarnation: the ones whose
    /// configuration this node has read.
    booted: BTreeSet<JournalIdentifier>,
    /// The journals this incarnation forgot because their booking was
    /// released (#474), until a booking starts one again.
    released: BTreeSet<JournalIdentifier>,
    /// The booted journals that name matchmakers (#190), live or not: every
    /// one hears its tenant's handover.
    matchmade: BTreeSet<JournalIdentifier>,
}

impl<S, A> Journals<S, A> {
    /// Whether a batch's flush still flies, a live journal's or one that
    /// left `live` meanwhile (#560 (clean Ctrl-C shutdown): a clean stop
    /// waits for each to land).
    pub(crate) fn flying(&self) -> bool {
        let flying = !self.orphans.is_empty() || self.live.values().any(|rt| rt.flight.is_some());
        if !flying {
            assert!(
                self.live.values().all(|rt| rt.storage.is_some()),
                "a journal that does not fly holds its store"
            );
        }
        flying
    }

    pub(crate) fn new(control: BTreeSet<JournalIdentifier>) -> Self {
        assert!(
            control.iter().all(|journal| journal.is_set()),
            "a control journal is a named journal"
        );
        Self {
            control,
            live: BTreeMap::new(),
            orphans: BTreeMap::new(),
            quarantined: BTreeMap::new(),
            full: BTreeMap::new(),
            down: BTreeSet::new(),
            newly_quarantined: Vec::new(),
            last_fault: None,
            deployed: None,
            preferred: None,
            booted: BTreeSet::new(),
            released: BTreeSet::new(),
            matchmade: BTreeSet::new(),
        }
    }

    /// Prefer `first`, the node's first journal in its static list, as the
    /// plane.
    pub(crate) fn preferring(mut self, first: Option<JournalIdentifier>) -> Self {
        assert!(
            self.live.is_empty(),
            "the plane is preferred before any boot"
        );
        self.preferred = first;
        self
    }

    /// `journal` booted: it is live, and its configuration is known.
    pub(crate) fn insert(&mut self, journal: JournalIdentifier, rt: JournalRt<S, A>) {
        // A journal opens from none of the three states: never twice live,
        // and out of quarantine before it re-opens.
        assert!(
            !self.live.contains_key(&journal),
            "a journal runs at most one runtime"
        );
        assert!(
            !self.quarantined.contains_key(&journal),
            "a re-opened journal left quarantine"
        );
        assert!(
            !self.down.contains(&journal),
            "a journal down for good never re-opens"
        );
        assert!(
            rt.node.config().journal == journal,
            "a runtime serves the journal it is filed under"
        );
        let config = rt.node.config();
        // A control journal is never the plane, even born with its
        // tenant's matchmaker set (#190).
        if (config.has_matchmakers() || config.proxy_count > 0 || config.replica_count > 0)
            && !self.control.contains(&journal)
            && (self.deployed.is_none() || self.preferred == Some(journal))
        {
            self.deployed = Some(journal);
        }
        if config.has_matchmakers() {
            self.matchmade.insert(journal);
        }
        self.booted.insert(journal);
        self.released.remove(&journal);
        self.live.insert(journal, rt);
        self.assert_invariants();
    }

    /// Take `journal` out of `live`, keeping the flush its batch flies, if
    /// any, as an orphan (#613 (settle off the node loop)).
    fn leave(&mut self, journal: JournalIdentifier) -> Option<JournalRt<S, A>> {
        let mut rt = self.live.remove(&journal)?;
        if let Some(flight) = rt.flight.take() {
            tracing::info!(journal = %journal, "flight_orphaned");
            let orphaned = self.orphans.insert(journal, flight.flush);
            assert!(orphaned.is_none(), "a journal leaves at most one orphan");
        }
        assert!(rt.flight.is_none(), "a journal that left flies nothing");
        Some(rt)
    }

    /// Wait for the orphaned flush of `journal`, if one flies: its store is
    /// opened again only once no older handle writes it.
    pub(crate) async fn land_orphan(&mut self, journal: JournalIdentifier) {
        if let Some(flush) = self.orphans.remove(&journal) {
            // Its store and verdict go with the runtime that left.
            drop(flush.await);
        }
        assert!(
            !self.orphans.contains_key(&journal),
            "a journal opens with no orphan in flight"
        );
    }

    /// The next flight to land, among the live journals' (in id order) and
    /// the orphans', which are dropped as they land. Pending while none
    /// has; the node loop polls it as a `select!` arm.
    pub(crate) fn next_landing(
        &mut self,
    ) -> impl Future<Output = (JournalIdentifier, Landing<S>)> + '_ {
        std::future::poll_fn(move |cx| {
            self.orphans
                .retain(|_, flush| flush.as_mut().poll(cx).is_pending());
            for (journal, rt) in &mut self.live {
                let Some(flight) = rt.flight.as_mut() else {
                    continue;
                };
                let Poll::Ready((storage, done)) = flight.flush.as_mut().poll(cx) else {
                    continue;
                };
                let Some(flight) = rt.flight.take() else {
                    continue;
                };
                assert!(
                    rt.storage.is_none(),
                    "a flying journal's store is in its flush"
                );
                return Poll::Ready((
                    *journal,
                    Landing {
                        storage,
                        done,
                        taken: flight.taken,
                        expiry: flight.expiry,
                        began: flight.began,
                    },
                ));
            }
            Poll::Pending
        })
    }

    /// The three states a journal can be in are disjoint, and the deployed
    /// journal is one this incarnation booted.
    fn assert_invariants(&self) {
        assert!(
            self.live.keys().all(|j| !self.quarantined.contains_key(j)),
            "a live journal is not quarantined"
        );
        assert!(
            self.live.keys().all(|j| !self.down.contains(j)),
            "a live journal is not down"
        );
        assert!(
            self.quarantined.keys().all(|j| !self.down.contains(j)),
            "a quarantined journal is not down"
        );
        // A journal a full disk took waits to re-open: never live, never
        // down for good (#360 (disk full)).
        assert!(
            self.full
                .keys()
                .all(|j| !self.live.contains_key(j) && !self.down.contains(j)),
            "a journal a full disk took is neither live nor down"
        );
        if let Some(deployed) = self.deployed {
            assert!(
                self.booted.contains(&deployed),
                "the deployed journal booted here"
            );
        }
    }

    /// Whether `journal` is one this node serves at all (live, quarantined
    /// or down) — the difference between "not here now" and "unknown".
    pub(crate) fn serves(&self, journal: JournalIdentifier) -> bool {
        let serves = self.live.contains_key(&journal)
            || self.quarantined.contains_key(&journal)
            || self.down.contains(&journal);
        // Only a named journal is ever served.
        if serves {
            assert!(journal.is_set(), "a served journal is named");
        }
        serves
    }

    /// The journal an operator call names (#474: a machine serves many).
    /// `None` when it names none, or a journal not live here: no identifier
    /// has a default (#439 (operator calls name their journal)).
    pub(crate) fn named(
        &mut self,
        journal: JournalIdentifier,
    ) -> Option<(&JournalIdentifier, &mut JournalRt<S, A>)> {
        if !journal.is_set() {
            return None;
        }
        let named = self.live.range_mut(journal..=journal).next();
        assert!(
            named.as_ref().is_none_or(|(id, _)| **id == journal),
            "a named journal answers as itself"
        );
        named
    }

    /// Whether `journal`'s booking was released and this incarnation forgot
    /// it (#474).
    pub(crate) fn released(&self, journal: JournalIdentifier) -> bool {
        let released = self.released.contains(&journal);
        assert!(
            !released || !self.live.contains_key(&journal),
            "a released journal is not live"
        );
        released
    }

    /// Every journal this node serves (live, quarantined or down), in id
    /// order.
    pub(crate) fn known(&self) -> impl Iterator<Item = JournalIdentifier> + '_ {
        let mut all: BTreeSet<JournalIdentifier> = self.live.keys().copied().collect();
        all.extend(self.quarantined.keys().copied());
        all.extend(self.down.iter().copied());
        all.into_iter()
    }

    /// Forget `journal` here, live, quarantined or down (#474): its booking
    /// was released, so the node no longer serves it. Its store stays on
    /// disk, and a later booking starts it again from there.
    pub(crate) fn forget(&mut self, journal: JournalIdentifier) {
        assert!(
            !self.control.contains(&journal),
            "a control journal is never forgotten"
        );
        self.leave(journal);
        self.quarantined.remove(&journal);
        self.full.remove(&journal);
        self.down.remove(&journal);
        self.newly_quarantined.retain(|j| *j != journal);
        if self.deployed == Some(journal) {
            self.deployed = None;
        }
        self.released.insert(journal);
        assert!(!self.serves(journal), "a forgotten journal is not served");
        self.assert_invariants();
    }

    /// [`Journals::first`], read-only: the journal that carries the
    /// deployment, or, on a node with none, its first user journal. `None`
    /// while the plane is not live here, or while a journal that never
    /// booted could be it: answering from another journal would be a lie
    /// (a `no_matchmakers` refusal from a plain journal while the
    /// matchmaker journal is quarantined).
    pub(crate) fn plane(&self) -> Option<(&JournalIdentifier, &JournalRt<S, A>)> {
        if let Some(deployed) = self.deployed {
            let plane = self.live.get_key_value(&deployed);
            // The matchmaker plane runs over a user journal, never a control one.
            assert!(
                !self.control.contains(&deployed),
                "the deployed journal is a user journal"
            );
            return plane;
        }
        let unknown = self
            .quarantined
            .keys()
            .chain(&self.down)
            .any(|journal| !self.booted.contains(journal) && !self.control.contains(journal));
        if unknown {
            return None;
        }
        self.live
            .iter()
            .find(|(journal, _)| !self.control.contains(journal))
    }

    /// The booted journals of `tenant` that name matchmakers (#190), live,
    /// quarantined or held: the ones its set's handover reports to.
    pub(crate) fn matchmade(
        &self,
        tenant: TenantId,
    ) -> impl Iterator<Item = JournalIdentifier> + '_ {
        assert!(
            self.matchmade.is_subset(&self.booted),
            "a matchmade journal booted here"
        );
        self.matchmade
            .iter()
            .copied()
            .filter(move |journal| journal.tenant == tenant)
    }

    /// Whether `journal` is one of the deployment's control journals.
    pub(crate) fn is_control(&self, journal: JournalIdentifier) -> bool {
        let control = self.control.contains(&journal);
        // Pair of the check in `new`: every control journal is named.
        if control {
            assert!(journal.is_set(), "a control journal is a named journal");
        }
        control
    }

    /// Whether the node has nothing left to serve **because of a fault**: no
    /// live journal, and the last incarnation of one ended on a fault. A
    /// node that follows the system journals (#189) runs on with no journal
    /// at all — a joiner boots with none — and exits only on this.
    pub(crate) fn stranded(&self) -> bool {
        let stranded = self.live.is_empty() && self.last_fault.is_some();
        if stranded {
            assert!(self.exhausted(), "a stranded node serves nothing live");
        }
        stranded
    }

    /// Mark `journal` down for good: its store refused to boot (`fault`,
    /// a boot refusal) or its opener has no store for it (`None`, a disk
    /// gone — the node simply stops serving the journal).
    pub(crate) fn park(&mut self, journal: JournalIdentifier, fault: Option<RunError>) {
        self.leave(journal);
        self.quarantined.remove(&journal);
        self.full.remove(&journal);
        self.down.insert(journal);
        if fault.is_some() {
            self.last_fault = fault;
        }
        self.assert_invariants();
    }

    /// The journals whose quarantine is over at tick `now`, removed from the
    /// quarantine list (the caller re-opens each).
    pub(crate) fn due(&mut self, now: u64, quarantine_ticks: u64) -> Vec<JournalIdentifier> {
        let due: Vec<JournalIdentifier> = self
            .quarantined
            .iter()
            .filter(|(_, since)| now.saturating_sub(**since) >= quarantine_ticks.max(1))
            .map(|(journal, _)| *journal)
            .collect();
        for journal in &due {
            self.quarantined.remove(journal);
        }
        // A due journal leaves quarantine to re-open; none of them is live.
        assert!(
            due.iter().all(|j| !self.live.contains_key(j)),
            "a quarantined journal was not live"
        );
        due
    }

    /// Put `journal` back in quarantine from tick `now` (its re-open failed).
    pub(crate) fn requarantine(&mut self, journal: JournalIdentifier, now: u64, fault: RunError) {
        // Only a journal that failed to re-open goes back: it is in no state.
        assert!(
            !self.live.contains_key(&journal),
            "a re-quarantined journal is not live"
        );
        assert!(
            !self.down.contains(&journal),
            "a re-quarantined journal is not down"
        );
        self.quarantined.insert(journal, now);
        self.newly_quarantined.push(journal);
        self.note_fault(journal, now, fault);
        self.assert_invariants();
    }

    /// Record the storage fault that quarantined `journal`: a full disk
    /// (#360 (disk full)) marks it to re-open read-only and is never the
    /// node's exit; every other fault is the node's last fault.
    fn note_fault(&mut self, journal: JournalIdentifier, now: u64, fault: RunError) {
        if matches!(fault, RunError::Storage(StorageError::Full { .. })) {
            // A journal still waiting keeps its first fault's tick: a full
            // re-open is the same full disk.
            self.full.entry(journal).or_insert(now);
            assert!(
                self.quarantined.contains_key(&journal),
                "a journal a full disk took is quarantined"
            );
        } else {
            self.full.remove(&journal);
            self.last_fault = Some(fault);
        }
    }

    /// The tick of the full disk that quarantined `journal`, when one did
    /// (#360 (disk full)): it re-opens read-only, counted from that tick.
    /// The mark is taken: the runtime holds it from here.
    pub(crate) fn take_full(&mut self, journal: JournalIdentifier) -> Option<u64> {
        let full = self.full.remove(&journal);
        assert!(!self.full.contains_key(&journal), "a taken mark is gone");
        full
    }

    /// Whether a journal a full disk took waits to re-open (#360 (disk
    /// full)): a node with one never exits for having nothing live.
    pub(crate) fn waits_for_space(&self) -> bool {
        let waits = !self.full.is_empty();
        if waits {
            assert!(
                !self.quarantined.is_empty(),
                "a journal waits for space only in quarantine"
            );
        }
        waits
    }

    /// The journals quarantined since the last call, for the opener.
    pub(crate) fn take_quarantined(&mut self) -> Vec<JournalIdentifier> {
        std::mem::take(&mut self.newly_quarantined)
    }

    /// Whether the node has nothing left to serve this incarnation: no live
    /// journal.
    pub(crate) fn exhausted(&self) -> bool {
        let exhausted = self.live.is_empty();
        // Nothing live, so no journal answers for the plane.
        if exhausted {
            assert!(self.plane().is_none(), "an exhausted node has no plane");
        }
        exhausted
    }

    /// The node's exit when [`Journals::exhausted`]: the fault that ended
    /// the last incarnation, or `Ok` when every journal is down for good
    /// without one (the node has nothing to serve, ever).
    ///
    /// # Errors
    ///
    /// The fault that ended the last incarnation.
    pub(crate) fn exit(&mut self) -> Result<(), RunError> {
        // The node exits only once it has nothing live left to serve.
        assert!(self.exhausted(), "a node exits only when exhausted");
        let exit = match self.last_fault.take() {
            Some(fault) => Err(fault),
            None => Ok(()),
        };
        assert!(self.last_fault.is_none(), "an exit consumes the last fault");
        exit
    }
}

impl<S, A: Audit> Journals<S, A> {
    /// Fold one journal step's outcome: a storage fault **quarantines** the
    /// journal (#188's decision — the damage is scoped to its own store, so
    /// the node keeps serving its other journals), every other error is the
    /// node's exit (a seam crash is the process dying, for every journal).
    ///
    /// # Errors
    ///
    /// Every error but [`RunError::Storage`], and a storage fault that left
    /// the node with nothing to serve.
    pub(crate) fn fold(
        &mut self,
        journal: JournalIdentifier,
        outcome: Result<(), RunError>,
        now: u64,
        self_id: u64,
    ) -> Result<(), RunError> {
        match outcome {
            Ok(()) => Ok(()),
            Err(RunError::Storage(error)) => {
                if let Some(rt) = self.leave(journal) {
                    rt.audit.journal_quarantined(NodeId(self_id));
                }
                tracing::warn!(node = self_id, journal = %journal, "journal_quarantined");
                self.quarantined.insert(journal, now);
                self.newly_quarantined.push(journal);
                self.note_fault(journal, now, RunError::Storage(error));
                self.assert_invariants();
                if self.live.is_empty() && !self.waits_for_space() {
                    // Nothing left to serve this incarnation: the node
                    // exits with the fault (one journal: the pre-#188 rule).
                    self.exit()
                } else {
                    Ok(())
                }
            }
            Err(other) => Err(other),
        }
    }
}
