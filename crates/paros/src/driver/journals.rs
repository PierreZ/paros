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
use paros_core::{ColocatedNode, JournalId, NodeId};

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
    fn journals(&self) -> Vec<JournalId>;

    /// Open `journal`'s store with the operator's boot claim, or `None` when
    /// the journal must stay down on this node for good (its disk is gone).
    fn open(&mut self, journal: JournalId) -> Option<(Self::Store, BootKind)>;

    /// The audit port `journal` reports to.
    fn audit(&self, journal: JournalId) -> Self::Audit;

    /// Provision a store for `journal`, a journal the directory created
    /// naming this node (#189), under `config`; `false` when this opener
    /// cannot (the default: a static list). A later [`JournalStores::open`]
    /// of `journal` opens it. Idempotent: a node that re-folds the directory
    /// after a restart asks again for a journal it already holds.
    fn create(&mut self, journal: JournalId, config: paros_core::Config) -> bool {
        let _ = (journal, config);
        false
    }

    /// `journal` was just quarantined on this node: a storage fault ended its
    /// incarnation, and the driver re-opens it after
    /// [`DriverTunables::quarantine_ticks`]. An opener that already knows
    /// the store will not open again (a disk gone for good) can say so now
    /// rather than at the re-open. The default does nothing.
    fn quarantined(&mut self, journal: JournalId) {
        let _ = journal;
    }

    /// `journal` was tombstoned (#189): its store will never be opened
    /// again. The default keeps it.
    fn delete(&mut self, journal: JournalId) {
        let _ = journal;
    }
}

/// The one-journal node [`crate::run_node`] runs: the store it was handed,
/// once. A quarantine never re-opens it — a one-journal node that loses its
/// journal has nothing left and exits with the fault, the pre-#188 rule.
pub(crate) struct SingleStore<S, A> {
    pub(crate) journal: JournalId,
    pub(crate) store: Option<(S, BootKind)>,
    pub(crate) audit: A,
}

impl<S: LogStorage, A: Audit + Clone + Send + Sync + 'static> JournalStores for SingleStore<S, A> {
    type Store = S;
    type Audit = A;

    fn journals(&self) -> Vec<JournalId> {
        vec![self.journal]
    }

    fn open(&mut self, _journal: JournalId) -> Option<(S, BootKind)> {
        self.store.take()
    }

    fn audit(&self, _journal: JournalId) -> A {
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
    pub(crate) live: BTreeMap<JournalId, JournalRt<S, A>>,
    quarantined: BTreeMap<JournalId, u64>,
    down: BTreeSet<JournalId>,
    /// Journals quarantined since the loop last told the opener
    /// ([`JournalStores::quarantined`]).
    newly_quarantined: Vec<JournalId>,
    /// The fault that ended the most recent incarnation, the node's exit
    /// when nothing is left.
    last_fault: Option<RunError>,
}

impl<S, A> Journals<S, A> {
    pub(crate) fn new() -> Self {
        Self {
            live: BTreeMap::new(),
            quarantined: BTreeMap::new(),
            down: BTreeSet::new(),
            newly_quarantined: Vec::new(),
            last_fault: None,
        }
    }

    /// Whether `journal` is one this node serves at all (live, quarantined
    /// or down) — the difference between "not here now" and "unknown".
    pub(crate) fn serves(&self, journal: JournalId) -> bool {
        self.live.contains_key(&journal)
            || self.quarantined.contains_key(&journal)
            || self.down.contains(&journal)
    }

    /// The node's first live **user** journal (the target of a journal-less
    /// call and of the single-journal planes: matchmaking, retirement). The
    /// system journals (#189) sort first and serve no plane.
    pub(crate) fn first(&mut self) -> Option<(&JournalId, &mut JournalRt<S, A>)> {
        self.live.iter_mut().find(|(journal, _)| journal.is_user())
    }

    /// [`Journals::first`], read-only.
    pub(crate) fn plane(&self) -> Option<(&JournalId, &JournalRt<S, A>)> {
        self.live.iter().find(|(journal, _)| journal.is_user())
    }

    /// Whether the node has nothing left to serve **because of a fault**: no
    /// live journal, and the last incarnation of one ended on a fault. A
    /// node that follows the system journals (#189) runs on with no journal
    /// at all — a joiner boots with none — and exits only on this.
    pub(crate) fn stranded(&self) -> bool {
        self.live.is_empty() && self.last_fault.is_some()
    }

    /// Mark `journal` down for good: its store refused to boot (`fault`,
    /// a boot refusal) or its opener has no store for it (`None`, a disk
    /// gone — the node simply stops serving the journal).
    pub(crate) fn park(&mut self, journal: JournalId, fault: Option<RunError>) {
        self.live.remove(&journal);
        self.quarantined.remove(&journal);
        self.down.insert(journal);
        if fault.is_some() {
            self.last_fault = fault;
        }
    }

    /// The journals whose quarantine is over at tick `now`, removed from the
    /// quarantine list (the caller re-opens each).
    pub(crate) fn due(&mut self, now: u64, quarantine_ticks: u64) -> Vec<JournalId> {
        let due: Vec<JournalId> = self
            .quarantined
            .iter()
            .filter(|(_, since)| now.saturating_sub(**since) >= quarantine_ticks.max(1))
            .map(|(journal, _)| *journal)
            .collect();
        for journal in &due {
            self.quarantined.remove(journal);
        }
        due
    }

    /// Put `journal` back in quarantine from tick `now` (its re-open failed).
    pub(crate) fn requarantine(&mut self, journal: JournalId, now: u64, fault: RunError) {
        self.quarantined.insert(journal, now);
        self.newly_quarantined.push(journal);
        self.last_fault = Some(fault);
    }

    /// The journals quarantined since the last call, for the opener.
    pub(crate) fn take_quarantined(&mut self) -> Vec<JournalId> {
        std::mem::take(&mut self.newly_quarantined)
    }

    /// Whether the node has nothing left to serve this incarnation: no live
    /// journal.
    pub(crate) fn exhausted(&self) -> bool {
        self.live.is_empty()
    }

    /// The node's exit when [`Journals::exhausted`]: the fault that ended
    /// the last incarnation, or `Ok` when every journal is down for good
    /// without one (the node has nothing to serve, ever).
    ///
    /// # Errors
    ///
    /// The fault that ended the last incarnation.
    pub(crate) fn exit(&mut self) -> Result<(), RunError> {
        match self.last_fault.take() {
            Some(fault) => Err(fault),
            None => Ok(()),
        }
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
        journal: JournalId,
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
                tracing::warn!(node = self_id, journal = journal.0, "journal_quarantined");
                self.quarantined.insert(journal, now);
                self.newly_quarantined.push(journal);
                self.last_fault = Some(RunError::Storage(error));
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
