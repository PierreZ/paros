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
//! plane, the proxy leaders and the replica tier serve one journal each —
//! the node's first user journal (asserted at boot); every other journal is
//! plain Multi-Paxos over the whole pool.

use std::collections::{BTreeMap, BTreeSet};

use moonpool_core::Providers;
use paros_core::{ColocatedNode, JournalIdentifier, NodeId};

use crate::audit::Audit;
use crate::hooks::DriverHooks;
use crate::storage::LogStorage;

use super::boot::{check_format_marker, report_boot_state};
use super::config::{BootKind, DriverTunables, RunError};
use super::ready::{ClientWaiters, storage_fault_crash};
use super::report::{Cadence, Deltas, draw_election_timeout};

/// Where a node's journals come from (#188): the static list, and a way to
/// open each journal's store — at boot, and again when the driver re-opens
/// a quarantined journal (opening is cheap and touches no device: the
/// store's own boot scan, run by the driver, is where it reads). An opener hands out a **fresh handle over the
/// journal's durable state**, as a process restart would find it.
pub trait JournalStores {
    /// The store every journal runs on.
    type Store: LogStorage;
    /// The audit port a journal reports to (one per journal: every
    /// journal-scoped oracle is keyed by it).
    type Audit: Audit + Clone + Send + Sync + 'static;

    /// The journals this node serves, a static list (ids `>= 1`).
    fn journals(&self) -> Vec<JournalIdentifier>;

    /// Open `journal`'s store with the operator's boot claim, or `None` when
    /// the journal must stay down on this node for good (its disk is gone).
    fn open(&mut self, journal: JournalIdentifier) -> Option<(Self::Store, BootKind)>;

    /// The audit port `journal` reports to.
    fn audit(&self, journal: JournalIdentifier) -> Self::Audit;

    /// The audit port the node's own facts report to — what no single
    /// journal owns: the edge's rejections, a peer lane's delivery failures,
    /// a refused journal id, the system journals' folds. Named by the
    /// opener, never borrowed from a journal (no identifier has a default,
    /// §3.8, #243).
    fn node_audit(&self) -> Self::Audit;

    /// Provision a store for `journal`, a journal the directory created
    /// naming this node (#189), under `config`; `false` when this opener
    /// cannot (the default: a static list). A later [`JournalStores::open`]
    /// of `journal` opens it. Idempotent: a node that re-folds the directory
    /// after a restart asks again for a journal it already holds.
    fn create(&mut self, journal: JournalIdentifier, config: paros_core::Config) -> bool {
        let _ = (journal, config);
        false
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
}

/// The one-journal node [`crate::run_node`] runs: the store it was handed,
/// once. A quarantine never re-opens it — a one-journal node that loses its
/// journal has nothing left and exits with the fault, the pre-#188 rule.
pub(crate) struct SingleStore<S, A> {
    pub(crate) journal: JournalIdentifier,
    pub(crate) store: Option<(S, BootKind)>,
    pub(crate) audit: A,
}

impl<S: LogStorage, A: Audit + Clone + Send + Sync + 'static> JournalStores for SingleStore<S, A> {
    type Store = S;
    type Audit = A;

    fn journals(&self) -> Vec<JournalIdentifier> {
        vec![self.journal]
    }

    fn open(&mut self, journal: JournalIdentifier) -> Option<(S, BootKind)> {
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
    pub(crate) storage: S,
    pub(crate) audit: A,
    pub(crate) waiters: ClientWaiters,
    pub(crate) last: Deltas,
    /// Ticks since the open matchmaking request was last (re-)sent.
    pub(crate) match_resend: Cadence,
    /// Ticks since the open GC request was last (re-)sent.
    pub(crate) gc_resend: Cadence,
}

/// Boot `journal` from `storage`: the scan, the format marker (#147), the
/// core, the boot report and the first election timeout.
///
/// # Errors
///
/// [`RunError::Storage`] when the scan or the formatting failed,
/// [`RunError::Refused`] when the boot claim and the marker disagree.
pub(crate) async fn boot_journal<P: Providers, S: LogStorage, H: DriverHooks, A: Audit>(
    providers: &P,
    mut storage: S,
    boot: BootKind,
    audit: A,
    tunables: &DriverTunables,
    hooks: &H,
) -> Result<JournalRt<S, A>, RunError> {
    // Stage 7: verify and classify every durable record BEFORE the core
    // reads the store, so no corrupted bytes cross into protocol logic.
    let self_id = storage.initial_state().1.id.0;
    storage
        .boot_scan()
        .await
        .map_err(|e| storage_fault_crash(&audit, self_id, e))?;
    check_format_marker(&mut storage, boot, self_id, &audit).await?;
    let mut node = ColocatedNode::new(&storage);
    // The core booted from exactly this store's configuration.
    assert!(
        node.config().id.0 == self_id,
        "a journal boots as the node that owns its store"
    );
    report_boot_state(&node, self_id, &audit);
    // The first randomized election timeout (jitter from the driver's RNG).
    let first_timeout = draw_election_timeout(
        providers,
        hooks,
        &audit,
        self_id,
        tunables.election_timeout_base,
    );
    node.set_election_timeout(first_timeout);
    assert!(
        !node.needs_election_timeout(),
        "a booted journal has its first timeout"
    );
    audit.election_timeout_set(NodeId(self_id), first_timeout);
    let last = Deltas::new(&node);
    Ok(JournalRt {
        node,
        storage,
        audit,
        waiters: ClientWaiters::default(),
        last,
        match_resend: Cadence::default(),
        gc_resend: Cadence::default(),
    })
}

/// A node's journals: the live runtimes, the quarantined ones waiting to
/// re-open (with the tick they went down), and the ones down for good.
pub(crate) struct Journals<S, A> {
    pub(crate) live: BTreeMap<JournalIdentifier, JournalRt<S, A>>,
    quarantined: BTreeMap<JournalIdentifier, u64>,
    down: BTreeSet<JournalIdentifier>,
    /// Journals quarantined since the loop last told the opener
    /// ([`JournalStores::quarantined`]).
    newly_quarantined: Vec<JournalIdentifier>,
    /// The fault that ended the most recent incarnation, the node's exit
    /// when nothing is left.
    last_fault: Option<RunError>,
    /// The control journals among the ones served (the cell's, the fleet tenant's, a
    /// tenant's directory): no identifier is fixed (§3.8), so the deployment says
    /// which they are. They serve no user plane and outlive a retirement.
    control: BTreeSet<JournalIdentifier>,
    /// The journal whose configuration carries the deployment (matchmakers,
    /// proxy leaders, replicas), once one booted here: the plane, whether it
    /// is live now or not.
    deployed: Option<JournalIdentifier>,
    /// Every journal that booted in this incarnation: the ones whose
    /// configuration this node has read.
    booted: BTreeSet<JournalIdentifier>,
}

impl<S, A> Journals<S, A> {
    pub(crate) fn new(control: BTreeSet<JournalIdentifier>) -> Self {
        assert!(
            control.iter().all(|journal| journal.is_set()),
            "a control journal is a named journal"
        );
        Self {
            control,
            live: BTreeMap::new(),
            quarantined: BTreeMap::new(),
            down: BTreeSet::new(),
            newly_quarantined: Vec::new(),
            last_fault: None,
            deployed: None,
            booted: BTreeSet::new(),
        }
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
        if config.has_matchmakers() || config.proxy_count > 0 || config.replica_count > 0 {
            self.deployed.get_or_insert(journal);
        }
        self.booted.insert(journal);
        self.live.insert(journal, rt);
        self.assert_invariants();
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

    /// The node's **plane** journal (the target of a journal-less call and
    /// of the single-journal planes: matchmaking, retirement): the
    /// deployment's journal — the one whose configuration names its
    /// matchmakers, proxies or replicas, which only one journal of a process
    /// may (#188) — or else the first live user journal. No identifier is fixed
    /// (§3.8), so id order says nothing; the control journals serve no
    /// plane.
    pub(crate) fn first(&mut self) -> Option<(&JournalIdentifier, &mut JournalRt<S, A>)> {
        let deployed = *self.plane()?.0;
        assert!(
            self.live.contains_key(&deployed),
            "the plane's journal is live"
        );
        let first = self
            .live
            .iter_mut()
            .find(|(journal, _)| **journal == deployed);
        assert!(first.is_some(), "the plane's runtime is found");
        first
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
        self.live.remove(&journal);
        self.quarantined.remove(&journal);
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
        self.last_fault = Some(fault);
        self.assert_invariants();
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
                if let Some(rt) = self.live.remove(&journal) {
                    rt.audit.journal_quarantined(NodeId(self_id));
                }
                tracing::warn!(node = self_id, journal = %journal, "journal_quarantined");
                self.quarantined.insert(journal, now);
                self.newly_quarantined.push(journal);
                self.last_fault = Some(RunError::Storage(error));
                self.assert_invariants();
                if self.live.is_empty() {
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
