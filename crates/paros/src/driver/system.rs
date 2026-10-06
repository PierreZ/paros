//! The system journals' follower (#189): how a node learns the directory
//! (the user tenant's control journal) and the node registry (the cell tenant's, #235) by reading them.
//!
//! A deployment that runs system journals hands the driver a [`SystemPlan`]:
//! the **seeds** — the nodes that host the system journals, a static
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
//! position 0, so a restart re-derives exactly what it knew. The registry is
//! checkpointed and truncated by its owner (#230), so the registry's fold is
//! a [`Folder`]: a read below the floor jumps to it, the checkpoint there is
//! restored (a fold that held every position below verifies it instead), and
//! the driver re-applies the whole restored registry. The driver truncates
//! a system journal like any other (§3.9, #243); the directory's fold does
//! not restore from a checkpoint yet, and its owner never truncates it
//! (#229). The remote reads run on detached tasks that consult no hook
//! and draw no randomness; which seed a read goes to is chosen on the loop,
//! round-robin, and the answer comes back through an inbox.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use moonpool_core::{
    Detach, Providers, SimulationError, SimulationResult, TaskProvider, TimeProvider,
};
use moonpool_rpc::RpcHandle;
use paros_core::{JournalIdentifier, LogPage, LogRead, NodeId, Party, Seq};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::client::checkpoint::{Folded, Folder};
use crate::rpc::{NodeClient, Read, ReadAck};
use crate::system::{Directory, DirectoryEvent, Registry, SystemEvent, registry_event};

use super::config::DriverTunables;
use super::journals::Journals;
use super::transport::peer_address;

/// The records one follow read asks for: enough that a system journal's
/// history arrives in a few pages (the server's byte budget keeps the page
/// far below the frame limit).
const FOLLOW_READ_RECORDS: u64 = 256;

// A follow read that asks for no record never moves its cursor.
const _: () = assert!(FOLLOW_READ_RECORDS > 0);

/// What a deployment that runs **system journals** (#189) tells a node's
/// driver. `None` is the static deployment of #188: no directory, no
/// registry, a pool fixed at boot.
#[derive(Clone, Debug)]
pub struct SystemPlan {
    /// This node's identity (a joiner may serve no journal at boot).
    pub self_id: NodeId,
    /// This machine's class (#211, its `MachineFacts::class`): a
    /// `stateless` machine never serves a journal — neither one the
    /// directory creates naming it nor a spare's.
    pub class: crate::system::Class,
    /// The nodes that host the system journals, with their addresses: a node
    /// that does not host them follows them from here.
    pub seeds: Vec<(NodeId, String)>,
    /// The directory's identifier: a tenant's control journal (#235). No identifier is
    /// fixed (`docs/architecture.md` §3.8): the deployment drew it.
    pub directory: JournalIdentifier,
    /// The registry's identifier: the cell tenant's control journal, drawn like
    /// the directory's.
    pub registry: JournalIdentifier,
    /// The pool the deployment was booted with: always in the pool, never
    /// retired through the registry.
    pub genesis_pool: Vec<NodeId>,
    /// The journals the deployment was booted with: ids the directory never
    /// allocates.
    pub genesis_journals: Vec<JournalIdentifier>,
    /// The journals a node outside the genesis pool joins **as a spare** once
    /// the registry admits it: each a configuration template (its journal,
    /// bootstrap membership, quorum system, matchmakers) whose identity and
    /// pool the driver fills in — the node's own id, and the pool the
    /// registry has admitted. A reconfiguration may then name the node.
    /// Empty where no journal can reconfigure (a deployment without
    /// matchmakers never names a new member).
    pub spares: Vec<paros_core::Config>,
}

/// One remote follow read's answer, back on the loop.
pub(crate) struct Followed {
    journal: JournalIdentifier,
    reply: ReadAck,
}

/// The node's follow of the system journals, and the two folds.
pub(crate) struct SystemFollower<P: Providers> {
    self_id: NodeId,
    class: crate::system::Class,
    seeds: Vec<NodeClient<P>>,
    /// Senders every node accepts whatever the registry says: the genesis
    /// pool and the deployment's static address book (replicas included).
    fixed: BTreeSet<NodeId>,
    directory: Directory,
    /// The directory's identifier.
    directory_key: JournalIdentifier,
    registry: Folder<Registry>,
    /// The registry's identifier.
    registry_key: JournalIdentifier,
    /// Checkpoints the registry fold met since the driver last took them:
    /// `(seq, verified)`, see [`crate::Audit::checkpoint_folded`].
    checkpoints: Vec<(u64, Option<bool>)>,
    /// Per system journal: where the next read starts.
    cursors: BTreeMap<JournalIdentifier, u64>,
    /// System journals with a remote read in flight.
    outstanding: BTreeSet<JournalIdentifier>,
    next_seed: usize,
    tombstones: BTreeSet<JournalIdentifier>,
    spares: Vec<paros_core::Config>,
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
        assert!(
            plan.genesis_pool.iter().all(|n| fixed.contains(n)),
            "the genesis pool is always admitted"
        );
        Ok((
            Self {
                self_id: plan.self_id,
                class: plan.class,
                seeds,
                fixed,
                // The directory allocates inside its own tenant (#235): only
                // that tenant's genesis journals are taken there.
                directory: Directory::new(
                    plan.genesis_journals
                        .iter()
                        .filter(|journal| journal.tenant == plan.directory.tenant)
                        .map(|journal| journal.journal),
                ),
                directory_key: plan.directory,
                registry: Folder::new(Registry::new(plan.genesis_pool.iter().copied())),
                registry_key: plan.registry,
                checkpoints: Vec::new(),
                cursors: BTreeMap::new(),
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

    /// Whether a peer message from `from` is accepted: a proxy leader (no
    /// pool names one), a node of the static address book, or a node the
    /// registry fold has in the pool.
    pub(crate) fn admits(&self, from: Party) -> bool {
        match from {
            Party::Proxy(_) => true,
            Party::Node(node) => self.fixed.contains(&node) || self.registry.state().contains(node),
        }
    }

    /// The pool the registry fold has admitted: the genesis pool and every
    /// registered node not retired.
    pub(crate) fn pool(&self) -> Vec<NodeId> {
        self.registry.state().pool()
    }

    /// The registry as folded so far.
    pub(crate) fn registry(&self) -> &Registry {
        self.registry.state()
    }

    /// Whether this machine takes acceptor work (#211): a `storage` one. A
    /// `stateless` machine never serves a journal.
    pub(crate) fn takes_storage_work(&self) -> bool {
        self.class == crate::system::Class::Storage
    }

    /// The checkpoints the registry fold met since the last call.
    pub(crate) fn take_checkpoints(&mut self) -> Vec<(u64, Option<bool>)> {
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

    /// Whether `journal` was tombstoned in the directory fold.
    pub(crate) fn is_tombstoned(&self, journal: JournalIdentifier) -> bool {
        let tombstoned = self.tombstones.contains(&journal);
        // Pair of the fold's insert: a tombstone names a journal of the
        // directory's own tenant, and never a system journal.
        if tombstoned {
            assert!(
                journal.tenant == self.directory_key.tenant,
                "a tombstone lies in the directory's tenant"
            );
            assert!(
                !self.system().contains(&journal),
                "a system journal is never tombstoned"
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

    /// The two system journals' identifiers.
    fn system(&self) -> [JournalIdentifier; 2] {
        [self.directory_key, self.registry_key]
    }

    /// Where the next read of `journal` starts.
    pub(crate) fn cursor(&self, journal: JournalIdentifier) -> u64 {
        // Only the two system journals are followed.
        assert!(
            self.system().contains(&journal),
            "a follow cursor names a system journal"
        );
        if journal == self.registry_key {
            return self.registry.next_seq();
        }
        self.cursors.get(&journal).copied().unwrap_or(0)
    }

    /// `journal`'s positions below `floor` are gone (a read answered
    /// `truncated`): the registry's fold jumps there, to restore from the
    /// checkpoint at the floor. The directory is never truncated (#229).
    pub(crate) fn jump(&mut self, journal: JournalIdentifier, floor: u64) {
        let before = self.cursor(journal);
        if journal == self.registry_key {
            self.registry.jump(floor);
            assert!(
                self.cursor(journal) >= floor,
                "a registry jump lands at or past the floor"
            );
        }
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
        if reply.unknown_journal || !reply.served {
            return Vec::new();
        }
        if reply.truncated {
            let floor = reply.state.as_ref().map_or(0, |state| state.first_seq);
            self.jump(journal, floor);
            return Vec::new();
        }
        self.fold(journal, reply.from_seq, reply.records)
    }

    /// Fold `records` of `journal` (dense from position `from`) and move its
    /// cursor past them. A record the fold has already seen is skipped:
    /// pages may overlap.
    fn fold(
        &mut self,
        journal: JournalIdentifier,
        from: u64,
        records: Vec<Vec<u8>>,
    ) -> Vec<(u64, SystemEvent)> {
        let mut events = Vec::new();
        let next = from + records.len() as u64;
        for (seq, record) in (from..).zip(records) {
            let event = match journal {
                _ if journal == self.directory_key && seq >= self.directory.next_seq() => {
                    let event = self.directory.fold(seq, &record);
                    if let DirectoryEvent::Deleted { id } = &event {
                        self.tombstones
                            .insert(JournalIdentifier::new(self.directory_key.tenant, *id));
                    }
                    SystemEvent::Directory(event)
                }
                _ if journal == self.registry_key => {
                    let Some(folded) = self.registry.fold(seq, &record) else {
                        continue;
                    };
                    let folded = match folded {
                        Folded::Checkpoint {
                            covers_up_to,
                            verified,
                        } => {
                            self.checkpoints.push((seq, verified));
                            Folded::Checkpoint {
                                covers_up_to,
                                verified,
                            }
                        }
                        // A node reads no checkpoint journal: it moves on,
                        // waiting for an `Inline` checkpoint (the writer
                        // emits no other in M9).
                        Folded::NeedsRef(_) => {
                            self.registry.skip(seq);
                            continue;
                        }
                        other => other,
                    };
                    let Some(event) = registry_event(folded) else {
                        continue;
                    };
                    SystemEvent::Registry(event)
                }
                _ => continue,
            };
            events.push((seq, event));
        }
        let cursor = self.cursors.entry(journal).or_default();
        *cursor = (*cursor).max(next);
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

    /// Open a remote follow read of every system journal `local` says this
    /// node does not run and that has none in flight, each to the next seed
    /// in turn.
    pub(crate) fn poll_remote(&mut self, providers: &P, local: impl Fn(JournalIdentifier) -> bool) {
        for journal in self.system() {
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
        // Every system journal this node does not run has a read in flight.
        assert!(
            self.system()
                .iter()
                .all(|journal| local(*journal) || self.outstanding.contains(journal)),
            "every remote system journal is followed"
        );
    }
}

impl Followed {
    /// The system journal the answer is for.
    pub(crate) fn journal(&self) -> JournalIdentifier {
        self.journal
    }
}

/// Fold every system journal this node runs from its own journal fold,
/// page by page, to the end: `(journal, events)` for each that moved. A
/// local read needs no confirmation: it is this node's own fold, and the
/// driver only acts on what it folded.
pub(crate) fn follow_local<P: Providers, S, A>(
    follower: &mut SystemFollower<P>,
    journals: &Journals<S, A>,
) -> Vec<(JournalIdentifier, Vec<(u64, SystemEvent)>)> {
    let page_records = usize::try_from(FOLLOW_READ_RECORDS).unwrap_or(usize::MAX);
    let mut moved = Vec::new();
    for journal in follower.system() {
        let Some(rt) = journals.live.get(&journal) else {
            continue;
        };
        let mut events = Vec::new();
        loop {
            let from = follower.cursor(journal);
            match rt
                .node
                .read_log(Seq(from), page_records, super::log_reads::READ_PAGE_BYTES)
            {
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
