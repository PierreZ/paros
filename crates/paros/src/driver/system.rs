//! The system journals' follower (#189): how a node learns the directory
//! (journal 1) and the node registry (journal 2) by reading them.
//!
//! A deployment that runs system journals hands the driver a [`SystemPlan`]:
//! the **seeds** — the nodes that host journals 1 and 2, a static
//! configuration of plain Multi-Paxos — and the genesis pool and journals the
//! folds start from. Every node folds both journals with the same pure folds
//! a client reads back through ([`crate::system`]): a seed reads its own
//! chosen prefix after each tick, every other node keeps one long-polling
//! `Read` per system journal open against a seed. The public `Read` *is* how
//! a seed serves a node outside the pool: no peer message from outside the
//! pool is ever needed, so a seed's peer lanes stay pool-only.
//!
//! The folds drive the node (`run_journals` applies each event):
//!
//! - a journal the directory creates naming this node starts here, and a
//!   tombstoned one stops here for good (a call naming it is then refused as
//!   unknown);
//! - a node the registry admits gets a peer lane (its registered address),
//!   and a peer message from a node the fold does not have in the pool is
//!   refused before the core sees it — a liveness cost until the fold
//!   catches up, never a safety one (a configuration still binds its
//!   membership to a ballot);
//! - this node's own retirement stops every user journal it serves.
//!
//! The follow is volatile: every incarnation folds both journals again from
//! LSN 0 (system journals are never trimmed), so a restart re-derives exactly
//! what it knew. The remote reads run on detached tasks that consult no hook
//! and draw no randomness; which seed a read goes to is chosen on the loop,
//! round-robin, and the answer comes back through an inbox.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use moonpool_core::{
    Detach, Providers, SimulationError, SimulationResult, TaskProvider, TimeProvider,
};
use moonpool_rpc::RpcHandle;
use paros_core::{JournalId, LogPage, LogRead, NodeId, Party, Slot};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::rpc::{NodeClient, Read, ReadAck, decode_records};
use crate::system::{DIRECTORY, Directory, DirectoryEvent, REGISTRY, Registry, SystemEvent};

use super::config::DriverTunables;
use super::journals::Journals;
use super::transport::peer_address;

/// The page budget of one follow read: large enough that a system journal's
/// history arrives in a few pages, small enough to stay far below the frame
/// limit.
const FOLLOW_READ_BYTES: u64 = 64 * 1024;

/// What a deployment that runs **system journals** (#189) tells a node's
/// driver. `None` is the static deployment of #188: no directory, no
/// registry, a pool fixed at boot.
#[derive(Clone, Debug)]
pub struct SystemPlan {
    /// This node's identity (a joiner may serve no journal at boot).
    pub self_id: NodeId,
    /// The nodes that host journals 1 and 2, with their addresses: a node
    /// that does not host them follows them from here.
    pub seeds: Vec<(NodeId, String)>,
    /// The pool the deployment was booted with: always in the pool, never
    /// retired through the registry.
    pub genesis_pool: Vec<NodeId>,
    /// The journals the deployment was booted with: ids the directory never
    /// allocates.
    pub genesis_journals: Vec<JournalId>,
}

/// One remote follow read's answer, back on the loop.
pub(crate) struct Followed {
    journal: JournalId,
    reply: ReadAck,
}

/// The node's follow of journals 1 and 2, and the two folds.
pub(crate) struct SystemFollower<P: Providers> {
    self_id: NodeId,
    seeds: Vec<NodeClient<P>>,
    /// Senders every node accepts whatever the registry says: the genesis
    /// pool and the deployment's static address book (replicas included).
    fixed: BTreeSet<NodeId>,
    directory: Directory,
    registry: Registry,
    /// Per system journal: where the next read starts.
    cursors: BTreeMap<JournalId, u64>,
    /// System journals with a remote read in flight.
    outstanding: BTreeSet<JournalId>,
    next_seed: usize,
    tombstones: BTreeSet<JournalId>,
    replies: mpsc::Sender<Followed>,
    timeout: Duration,
    shutdown: CancellationToken,
}

impl<P: Providers> SystemFollower<P> {
    /// A follower for `plan`, reading remotely through `rpc`; `fixed` is the
    /// static address book. Returns the inbox remote answers arrive on.
    ///
    /// # Errors
    ///
    /// A seed address that does not parse.
    pub(crate) fn new(
        plan: &SystemPlan,
        rpc: &RpcHandle<P>,
        fixed: impl IntoIterator<Item = NodeId>,
        tunables: &DriverTunables,
        shutdown: CancellationToken,
    ) -> SimulationResult<(Self, mpsc::Receiver<Followed>)> {
        let seeds = plan
            .seeds
            .iter()
            .map(|(_, addr)| Ok(NodeClient::new(rpc, peer_address(addr)?)))
            .collect::<SimulationResult<Vec<_>>>()?;
        if seeds.is_empty() {
            return Err(SimulationError::InvalidState(
                "a system plan names at least one seed".into(),
            ));
        }
        let (replies, inbox) = mpsc::channel(4);
        // A long-poll answers empty after `read_poll_ticks`: the follow's
        // deadline covers it and one delivery either way.
        let poll = tunables
            .tick_interval
            .saturating_mul(u32::try_from(tunables.read_poll_ticks).unwrap_or(u32::MAX));
        let timeout = poll + tunables.delivery_timeout.saturating_mul(2);
        let mut fixed: BTreeSet<NodeId> = fixed.into_iter().collect();
        fixed.extend(plan.genesis_pool.iter().copied());
        Ok((
            Self {
                self_id: plan.self_id,
                seeds,
                fixed,
                directory: Directory::new(plan.genesis_journals.iter().copied()),
                registry: Registry::new(plan.genesis_pool.iter().copied()),
                cursors: BTreeMap::new(),
                outstanding: BTreeSet::new(),
                next_seed: 0,
                tombstones: BTreeSet::new(),
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

    /// Whether a peer message from `from` is accepted: a proxy leader (no
    /// pool names one), a node of the static address book, or a node the
    /// registry fold has in the pool.
    pub(crate) fn admits(&self, from: Party) -> bool {
        match from {
            Party::Proxy(_) => true,
            Party::Node(node) => self.fixed.contains(&node) || self.registry.contains(node),
        }
    }

    /// Whether `journal` was tombstoned in the directory fold.
    pub(crate) fn is_tombstoned(&self, journal: JournalId) -> bool {
        self.tombstones.contains(&journal)
    }

    /// Fold one page of `journal` read from this node's own chosen prefix.
    pub(crate) fn fold_local(
        &mut self,
        journal: JournalId,
        page: &LogPage,
    ) -> Vec<(u64, SystemEvent)> {
        let entries: Vec<(u64, Vec<Vec<u8>>)> = page
            .entries
            .iter()
            .map(|(slot, entry)| (slot.0, decode_records(&entry.value.0)))
            .collect();
        self.fold(journal, entries, page.next.0)
    }

    /// Where the next read of `journal` starts.
    pub(crate) fn cursor(&self, journal: JournalId) -> u64 {
        self.cursors.get(&journal).copied().unwrap_or(0)
    }

    /// Fold one remote answer.
    pub(crate) fn fold_remote(&mut self, followed: Followed) -> Vec<(u64, SystemEvent)> {
        let Followed { journal, reply } = followed;
        self.outstanding.remove(&journal);
        if reply.unknown_journal || reply.trimmed_to.is_some() {
            return Vec::new();
        }
        let entries = reply
            .entries
            .into_iter()
            .map(|entry| (entry.lsn, entry.records))
            .collect();
        self.fold(journal, entries, reply.next_lsn)
    }

    /// Fold `entries` of `journal` (a contiguous page: every slot between
    /// them is a hole) and move its cursor to `next`. An entry the fold has
    /// already seen is skipped: pages may overlap.
    fn fold(
        &mut self,
        journal: JournalId,
        entries: Vec<(u64, Vec<Vec<u8>>)>,
        next: u64,
    ) -> Vec<(u64, SystemEvent)> {
        let mut events = Vec::new();
        for (lsn, records) in entries {
            let event = match journal {
                DIRECTORY if lsn >= self.directory.next_lsn() => {
                    let event = self.directory.fold(lsn, &records);
                    if let DirectoryEvent::Deleted { id } = &event {
                        self.tombstones.insert(*id);
                    }
                    SystemEvent::Directory(event)
                }
                REGISTRY if lsn >= self.registry.next_lsn() => {
                    SystemEvent::Registry(self.registry.fold(lsn, &records))
                }
                _ => continue,
            };
            events.push((lsn, event));
        }
        let cursor = self.cursors.entry(journal).or_default();
        *cursor = (*cursor).max(next);
        events
    }

    /// Open a remote follow read of every system journal `local` says this
    /// node does not run and that has none in flight, each to the next seed
    /// in turn.
    pub(crate) fn poll_remote(&mut self, providers: &P, local: impl Fn(JournalId) -> bool) {
        for journal in [DIRECTORY, REGISTRY] {
            if local(journal) || self.outstanding.contains(&journal) {
                continue;
            }
            let client = self.seeds[self.next_seed % self.seeds.len()].clone();
            self.next_seed = self.next_seed.wrapping_add(1);
            self.outstanding.insert(journal);
            let request = Read {
                journal: journal.0,
                from_lsn: self.cursor(journal),
                max_bytes: FOLLOW_READ_BYTES,
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
                            tracing::debug!(node = self_id, journal = journal.0, %error, "system_follow_failed");
                            ReadAck { unknown_journal: true, ..ReadAck::default() }
                        }
                        Err(_) => {
                            tracing::debug!(node = self_id, journal = journal.0, "system_follow_timed_out");
                            ReadAck { unknown_journal: true, ..ReadAck::default() }
                        }
                    };
                    // A failed read comes back too, so the journal's next
                    // read opens on the next tick.
                    let _ = sink.send(Followed { journal, reply }).await;
                })
                .detach();
        }
    }
}

impl Followed {
    /// The system journal the answer is for.
    pub(crate) fn journal(&self) -> JournalId {
        self.journal
    }
}

/// Fold every system journal this node runs from its own chosen prefix,
/// page by page, to the end: `(journal, events)` for each that moved.
pub(crate) fn follow_local<P: Providers, S, A>(
    follower: &mut SystemFollower<P>,
    journals: &Journals<S, A>,
) -> Vec<(JournalId, Vec<(u64, SystemEvent)>)> {
    let page_bytes = usize::try_from(FOLLOW_READ_BYTES).unwrap_or(usize::MAX);
    let mut moved = Vec::new();
    for journal in [DIRECTORY, REGISTRY] {
        let Some(rt) = journals.live.get(&journal) else {
            continue;
        };
        let mut events = Vec::new();
        loop {
            let from = follower.cursor(journal);
            match rt.node.read_log(Slot(from), page_bytes) {
                LogRead::Page(page) if page.next.0 > from => {
                    events.extend(follower.fold_local(journal, &page));
                }
                _ => break,
            }
        }
        if !events.is_empty() {
            moved.push((journal, events));
        }
    }
    moved
}
