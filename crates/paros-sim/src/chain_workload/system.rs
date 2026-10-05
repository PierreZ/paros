//! The chain client's **system-journal operations** (#189): the directory's
//! `CreateJournal` / `DeleteJournal` and the node registry's
//! `RegisterNode` / `DrainNode` / `RetireNode`, each one record written to
//! the directory or the registry (two tenants' control journals, #235) at a
//! seed, and the read-back a creator does to learn what its request folded
//! to. A create names the journal id it drew (#235) and redraws once when
//! the directory refuses it as taken.
//!
//! A system journal is written like any journal (#204): a writer claims it
//! with `SetLeader` against the generation it read, then `Write`s at the
//! position the claim answered. Several clients contend for the same
//! journal, so a claim may lose and a write may be fenced by a later claim;
//! both are retried a bounded number of times, and what never lands is
//! ambiguous.
//!
//! A client reads a system journal exactly as a node follows it: `Read` from
//! position 0 and the same pure fold (`paros::system`), so what it reads
//! back is what every node applied. On a seed that runs no system journals
//! the request is still sent, and must be refused as naming an unknown
//! journal — the same parity a `Reconfigure` keeps on a plain seed.
//!
//! The registry (#211) is the cell control journal: a joiner registers with
//! the class and capacity the role map drew for it (`crate::shape::
//! joiner_machines`), registers again on a later step (a reboot), and the
//! client books and releases its slots ([`SystemOps::book`]) as the cell
//! coordinator would. It is checkpointed and truncated with the library's
//! `paros::client::checkpoint` ([`SystemOps::checkpoint`], #230), so every
//! read-back folds it through a [`Folder`]: a read below the floor restarts
//! from the checkpoint there.
//!
//! No function here draws randomness: every choice is read off the caller's
//! step draws.

use std::sync::Arc;
use std::time::Duration;

use moonpool_sim::{SimContext, assert_always, assert_reachable, buggify_with_prob};
use paros::client::checkpoint::{
    CheckpointOutcome, CheckpointPolicy, Checkpointer, Folded, Folder, OpenOutcome,
};
use paros::client::{Answered, Attempted, CallObserver, ClaimOutcome, TruncateOutcome, Writer};
use paros::system::{
    Class, Directory, DirectoryEvent, DirectoryRefusal, NodeStanding, Registry, RegistryEvent,
    SystemCommand, SystemEvent, registry_event,
};
use paros::{
    AcceptorConfig, Command, Entry, Generation, JournalId, JournalIdentifier, NodeId, QuorumSystem,
    Seq, Value,
};

use paros::client::{ReadOutcome, SetLeaderOutcome, WriteOutcome};

use super::rpc::{read_once, set_leader_once, within, write_once};
use crate::audit::audit_world_for;
use crate::audit::system::{lock as board_lock, system_board};
use crate::chain::user_command_hash;
use crate::client::ChainClient;
use crate::shape::JoinerMachine;

/// A journal id spread from one draw (#235: ids are random, never a log
/// position; no range is reserved, §3.8): any set id.
fn drawn_id(draw: u64) -> JournalId {
    JournalId(crate::chain::splitmix(draw).max(1))
}

/// How many asks a system write spends before it calls the outcome
/// ambiguous.
const APPEND_ATTEMPTS: usize = 6;

/// The names a created journal is drawn from: few, so two creates race for
/// one often.
const NAMES: [&[u8]; 4] = [b"alpha", b"beta", b"gamma", b"delta"];

/// A read-back page's record budget.
const READ_RECORDS: u64 = 256;

/// One system append's terminal outcome.
enum Appended {
    /// Written at this position.
    At(u64),
    /// The seed serves no such journal: the run runs no system journals.
    Unknown,
    /// No answer that decides it.
    Ambiguous,
}

/// The chain client's system-journal state across its steps.
pub(super) struct SystemOps {
    /// The run runs the system journals.
    active: bool,
    /// The directory's identifier (drawn per seed: no identifier is fixed, §3.8).
    directory: JournalIdentifier,
    /// The registry's identifier.
    registry: JournalIdentifier,
    /// The deployment's journal.
    main: JournalIdentifier,
    /// How many genesis ranks host the system journals (the seeds).
    seeds: usize,
    /// The genesis pool size.
    pool: usize,
    /// The joiners, `(id, address, machine)`.
    joiners: Vec<(NodeId, String, JoinerMachine)>,
    /// The genesis journals: identifiers the directory never allocates.
    genesis: Vec<JournalIdentifier>,
    /// A registered joiner joins the default journal as a spare (a seed with
    /// matchmakers and neither proxies nor replicas, `process::spare_template`),
    /// so a reconfiguration may name one.
    spares: bool,
    client_id: u64,
    /// Journals this client created (ids in the directory's tenant) and has
    /// not asked to delete.
    created: Vec<JournalId>,
    /// Every id this client ever had created: a deliberate reuse names one.
    ever_created: Vec<JournalId>,
    /// The bookings this client made and has not released.
    booked: Vec<u64>,
    timeout: Duration,
}

/// The system journals' half of the audit's write oracles, as a
/// [`CallObserver`]: a library call that writes to one of `journals` (a
/// [`Checkpointer`]'s, a fleet operation's) announces its records and its
/// exact write to that journal's audit before it leaves, like every
/// hand-built system append here does.
pub(super) struct Announce {
    journals: Vec<(JournalIdentifier, Arc<crate::audit::AuditWorld>)>,
}

impl Announce {
    /// An observer announcing the writes to each of `journals`.
    pub(super) fn new(ctx: &SimContext, journals: &[JournalIdentifier]) -> Self {
        Self {
            journals: journals
                .iter()
                .map(|journal| (*journal, audit_world_for(ctx.state(), *journal)))
                .collect(),
        }
    }
}

impl CallObserver for Announce {
    fn invoked(&self, attempt: Attempted<'_>) -> Option<u64> {
        if let Attempted::Write(write) = attempt
            && let Some((_, audit)) = self.journals.iter().find(|(j, _)| *j == attempt.journal())
        {
            for record in &write.records {
                audit.note_submitted(user_command_hash(record));
            }
            let entry = Entry {
                generation: Generation(write.generation),
                owner: paros::ClientId(write.owner),
                seq: Seq(write.seq),
                records: write.records.iter().cloned().map(Value).collect(),
            };
            audit.note_appended(paros::command_hash(&Command::Write(entry)));
        }
        None
    }

    fn answered(&self, _token: u64, _answer: Answered<'_>) {}
}

impl SystemOps {
    /// The operations of client `client_id` on `deployment`: `active` when
    /// the run runs the system journals, `genesis` the journals it booted
    /// with.
    pub(super) fn new(
        deployment: &crate::roles::Deployment,
        identifiers: crate::shape::Identifiers,
        active: bool,
        genesis: Vec<JournalIdentifier>,
        machines: &[JoinerMachine],
        client_id: u64,
        timeout: Duration,
    ) -> Self {
        let pool = deployment.acceptors().len();
        let matchmakers = !deployment.matchmakers().is_empty();
        Self {
            active,
            directory: identifiers.directory,
            registry: identifiers.registry,
            main: identifiers.main,
            seeds: crate::shape::seed_ranks(pool).len().max(1),
            pool,
            joiners: deployment
                .joiners()
                .iter()
                .enumerate()
                .zip(machines)
                .filter_map(|((rank, ip), machine)| {
                    paros::parse_addr(ip)
                        .ok()
                        .map(|addr| (crate::roles::joiner_node_id(rank), addr, *machine))
                })
                .collect(),
            genesis,
            spares: matchmakers
                && deployment.proxies().is_empty()
                && deployment.replicas().is_empty(),
            client_id,
            created: Vec::new(),
            ever_created: Vec::new(),
            booked: Vec::new(),
            timeout,
        }
    }

    /// Whether `journal` is one of the two system journals.
    fn is_system(&self, journal: JournalIdentifier) -> bool {
        journal == self.directory || journal == self.registry
    }

    /// The genesis pool's registry, empty.
    fn empty_registry(&self) -> Registry {
        Registry::new((0..self.pool as u64).map(NodeId))
    }

    /// The client of the seeds — the nodes hosting the system journals —
    /// that announces its system writes to `journal`'s audit.
    fn seed_client(
        &self,
        ctx: &SimContext,
        nodes: &ChainClient,
        journal: JournalIdentifier,
    ) -> ChainClient {
        let seeds = self.seeds.min(nodes.server_count()).max(1);
        nodes
            .clone()
            .with_observer(Arc::new(Announce::new(ctx, &[journal])))
            .rotating_over(seeds)
    }

    /// Write `command` to `journal` at the seeds, starting at the one
    /// `draw` names and following redirects.
    async fn append(
        &mut self,
        ctx: &SimContext,
        nodes: &ChainClient,
        journal: JournalIdentifier,
        command: &SystemCommand,
        draw: u64,
    ) -> Appended {
        let seeds: Vec<usize> = (0..self.seeds).collect();
        self.claim_and_write(ctx, nodes, journal, &seeds, command.encode(), draw)
            .await
    }

    /// Claim `journal` and write `record` to it (#204), asking the nodes
    /// `targets` names from the one `draw` picks: read the tail's state,
    /// `SetLeader` against its generation, then `Write` at the position the
    /// claim answered. A lost claim, a fenced write and a redirect are
    /// retried within [`APPEND_ATTEMPTS`] asks. On a journal `created` at
    /// runtime (every journal here but a system one) a member that has not
    /// folded the create answers unknown, and the ask moves on.
    async fn claim_and_write(
        &mut self,
        ctx: &SimContext,
        nodes: &ChainClient,
        journal: JournalIdentifier,
        targets: &[usize],
        record: Vec<u8>,
        draw: u64,
    ) -> Appended {
        let created = !self.is_system(journal);
        let audit = audit_world_for(ctx.state(), journal);
        audit.note_submitted(user_command_hash(&record));
        let mut target = usize::try_from(draw % targets.len() as u64).unwrap_or(0);
        let mut claim: Option<(u64, u64)> = None;
        for _ in 0..APPEND_ATTEMPTS {
            let node = targets[target % targets.len()] % nodes.server_count();
            let Some((generation, position)) = claim else {
                // Read where the journal stands, then claim it.
                let read = read_once(nodes, node, journal, 0, 1, 0);
                let answer = within(ctx, self.timeout, ReadOutcome::Ambiguous, read).await;
                if answer == ReadOutcome::UnknownJournal && created {
                    target += 1;
                    continue;
                }
                if answer == ReadOutcome::UnknownJournal {
                    assert_always!(
                        !self.active || !self.is_system(journal),
                        "system: a seed serves the system journals",
                        { "journal" => journal.to_string(), "seed" => node }
                    );
                    if self.is_system(journal) {
                        assert_reachable!(
                            "system: a system append on a seed without system journals is refused"
                        );
                    }
                    return Appended::Unknown;
                }
                assert_always!(
                    answer != ReadOutcome::Malformed,
                    "chain: a node answers a well-formed journal state"
                );
                let Some(tail) = answer.state() else {
                    target += 1;
                    continue;
                };
                let ask = set_leader_once(
                    nodes,
                    journal,
                    node,
                    (tail.generation.0, self.client_id),
                    created,
                );
                match within(ctx, self.timeout, SetLeaderOutcome::Ambiguous, ask).await {
                    SetLeaderOutcome::Won { state } => {
                        claim = Some((state.generation.0, state.next_seq.0));
                    }
                    SetLeaderOutcome::Lost { .. }
                    | SetLeaderOutcome::UnknownJournal
                    | SetLeaderOutcome::Malformed
                    | SetLeaderOutcome::Ambiguous => {}
                    SetLeaderOutcome::Redirect { leader } => {
                        target = leader
                            .and_then(|l| targets.iter().position(|t| *t as u64 == l))
                            .unwrap_or(target + 1);
                    }
                }
                continue;
            };
            let entry = Entry {
                generation: Generation(generation),
                owner: paros::ClientId(self.client_id),
                seq: Seq(position),
                records: vec![Value(record.clone())],
            };
            audit.note_appended(paros::command_hash(&Command::Write(entry.clone())));
            let write = write_once(nodes, journal, node, &entry, false, created);
            match within(ctx, self.timeout, WriteOutcome::Ambiguous, write).await {
                WriteOutcome::Written { seq, .. } => return Appended::At(seq),
                // Fenced by a later claim, or behind: claim again.
                WriteOutcome::Refused { .. } | WriteOutcome::Truncated { .. } => claim = None,
                WriteOutcome::Redirect { leader } => {
                    target = leader
                        .and_then(|l| targets.iter().position(|t| *t as u64 == l))
                        .unwrap_or(target + 1);
                }
                WriteOutcome::UnknownJournal
                | WriteOutcome::Malformed
                | WriteOutcome::Ambiguous => {
                    target += 1;
                }
            }
        }
        Appended::Ambiguous
    }

    /// Read `journal` from position 0 to a seed's tail and fold it: every
    /// event with its position, and the folds. The registry folds through a
    /// [`Folder`] (#230): a read below its floor jumps there and restarts
    /// from the checkpoint, and a checkpoint met with the whole prefix
    /// folded is verified against it. `None` when a page goes unserved, or
    /// the fold ends above a gap no checkpoint healed.
    async fn read_back(
        &self,
        ctx: &SimContext,
        nodes: &ChainClient,
        journal: JournalIdentifier,
        draw: u64,
    ) -> Option<(Vec<(u64, SystemEvent)>, Directory, Registry)> {
        let seed = usize::try_from(draw % self.seeds as u64).unwrap_or(0);
        let mut directory = Directory::new(
            self.genesis
                .iter()
                .filter(|journal| journal.tenant == self.directory.tenant)
                .map(|journal| journal.journal),
        );
        let mut registry = Folder::new(self.empty_registry());
        let mut events = Vec::new();
        let mut from = 0;
        loop {
            let read = read_once(nodes, seed, journal, from, READ_RECORDS, 0);
            let (records, state) =
                match within(ctx, self.timeout, ReadOutcome::Ambiguous, read).await {
                    ReadOutcome::Page { records, state, .. } => (records, state),
                    // A truncation overtook the cursor: the registry's floor is
                    // its owner's checkpoint, where the fold restarts.
                    ReadOutcome::Truncated { state }
                        if journal == self.registry && state.first_seq.0 > from =>
                    {
                        registry.jump(state.first_seq.0);
                        from = state.first_seq.0;
                        continue;
                    }
                    _ => return None,
                };
            let next = from + records.len() as u64;
            for (position, record) in (from..).zip(&records) {
                if journal == self.directory {
                    events.push((
                        position,
                        SystemEvent::Directory(directory.fold(position, record)),
                    ));
                    continue;
                }
                let Some(folded) = registry.fold(position, record) else {
                    continue;
                };
                if let Folded::Checkpoint { verified, .. } = &folded {
                    assert_always!(
                        *verified != Some(false),
                        "checkpoint: a client's read-back finds each checkpoint its prefix's state",
                        { "journal" => journal.to_string(), "seq" => position }
                    );
                    if verified.is_none() {
                        board_lock(&system_board(ctx.state())).reader_restarted();
                    }
                }
                if let Some(event) = registry_event(folded) {
                    events.push((position, SystemEvent::Registry(event)));
                }
            }
            if next <= from || next >= state.next_seq.0 {
                if !registry.is_whole() {
                    return None;
                }
                return Some((events, directory, registry.state().clone()));
            }
            from = next;
        }
    }

    /// `CREATE_JOURNAL`: create a journal named from a four-name alphabet
    /// over three members of the pool — genesis nodes and joiners the
    /// registry has in it — read back what the request folded to, and append
    /// one record to a journal it created.
    pub(super) async fn create(
        &mut self,
        ctx: &SimContext,
        nodes: &ChainClient,
        (class, payload): (u64, u64),
    ) {
        let name = NAMES[usize::try_from(class % NAMES.len() as u64).unwrap_or(0)].to_vec();
        let mut candidates: Vec<NodeId> = (0..self.pool as u64).map(NodeId).collect();
        if self.active
            && let Some((_, _, registry)) = self.read_back(ctx, nodes, self.registry, payload).await
        {
            // A stateless machine never takes acceptor work (#211).
            candidates.extend(
                registry
                    .nodes()
                    .filter(|(_, node)| {
                        node.standing == NodeStanding::Registered && node.class == Class::Storage
                    })
                    .map(|(id, _)| id),
            );
        }
        let size = candidates.len().min(3);
        let start = usize::try_from(payload % candidates.len() as u64).unwrap_or(0);
        let mut members: Vec<NodeId> = (0..size)
            .map(|k| candidates[(start + k) % candidates.len()])
            .collect();
        // The operator who places a journal on a node before registering
        // it — a valid order, and the one that makes a joiner speak to
        // nodes whose registry fold has not admitted it yet: its own
        // location. The joiner takes one seat beside two genesis nodes, so
        // the journal elects without it.
        if self.active && !self.joiners.is_empty() && self.pool >= 2 && buggify_with_prob!(0.25) {
            assert_reachable!(
                "system: a client creates a journal naming a joiner not yet registered"
            );
            let joiner =
                self.joiners[usize::try_from(class % self.joiners.len() as u64).unwrap_or(0)].0;
            // A stateless joiner named here never serves it (#211): the
            // journal elects on its two genesis members.
            members = vec![
                joiner,
                NodeId(start as u64 % self.pool as u64),
                NodeId((start as u64 + 1) % self.pool as u64),
            ];
        }
        let config = AcceptorConfig::new(members, QuorumSystem::Majority);
        // The id is the creator's to draw (#235), off the step's draws. A
        // deliberate reuse of an id this client had created — the collision a
        // random u64 never makes on its own — is its own location: the
        // directory must refuse it, and the creator redraws.
        let reuse = !self.ever_created.is_empty() && buggify_with_prob!(0.2);
        let mut id = if reuse {
            assert_reachable!("system: a client creates a journal under an id it already used");
            self.ever_created[usize::try_from(class % self.ever_created.len() as u64).unwrap_or(0)]
        } else {
            drawn_id(payload ^ class)
        };
        for attempt in 0..2_u64 {
            let command = SystemCommand::CreateJournal {
                id,
                name: name.clone(),
                config: config.clone(),
            };
            let Appended::At(position) = self
                .append(ctx, nodes, self.directory, &command, payload)
                .await
            else {
                return;
            };
            let Some((events, _, _)) = self.read_back(ctx, nodes, self.directory, payload).await
            else {
                return;
            };
            match events
                .into_iter()
                .find(|(at, _)| *at == position)
                .map(|(_, e)| e)
            {
                Some(SystemEvent::Directory(DirectoryEvent::Created { id: created, .. })) => {
                    assert_always!(
                        created == id,
                        "system: a created journal takes the id its creator drew",
                        { "asked" => id.0, "created" => created.0 }
                    );
                    assert_reachable!("system: a client creates a journal and reads back its id");
                    self.created.push(id);
                    self.ever_created.push(id);
                    let created = JournalIdentifier::new(self.directory.tenant, id);
                    self.append_to_created(ctx, nodes, created, &config, payload)
                        .await;
                    return;
                }
                Some(SystemEvent::Directory(DirectoryEvent::Refused(
                    DirectoryRefusal::IdTaken { .. },
                ))) => {
                    assert_reachable!(
                        "system: a client redraws an id the directory refused as taken"
                    );
                    id = drawn_id(payload.rotate_left(17) ^ attempt ^ class.rotate_left(29));
                }
                Some(SystemEvent::Directory(DirectoryEvent::Refused(
                    DirectoryRefusal::NameTaken { .. },
                ))) => {
                    assert_reachable!(
                        "system: a client reads back its create refused for a taken name"
                    );
                    return;
                }
                _ => return,
            }
        }
    }

    /// One record written to a journal this client created, at a genesis
    /// member (the client has no connection to a joiner).
    async fn append_to_created(
        &mut self,
        ctx: &SimContext,
        nodes: &ChainClient,
        journal: JournalIdentifier,
        config: &AcceptorConfig,
        draw: u64,
    ) {
        let genesis: Vec<usize> = config
            .members()
            .iter()
            .filter_map(|id| usize::try_from(id.0).ok().filter(|i| *i < self.pool))
            .collect();
        if genesis.is_empty() {
            return;
        }
        let record = draw.to_le_bytes().to_vec();
        if let Appended::At(_) = self
            .claim_and_write(ctx, nodes, journal, &genesis, record, draw)
            .await
        {
            assert_reachable!("system: a created journal commits an append");
        }
    }

    /// `DELETE_JOURNAL`: tombstone a journal this client created, then ask a
    /// genesis member for one more append to it.
    pub(super) async fn delete(&mut self, ctx: &SimContext, nodes: &ChainClient, draw: u64) {
        let id = if self.created.is_empty() {
            // Nothing of its own: a delete of an id nobody created, which
            // folds to a refusal.
            drawn_id(draw ^ 0x00de_1e7e)
        } else {
            self.created
                .remove(usize::try_from(draw % self.created.len() as u64).unwrap_or(0))
        };
        let command = SystemCommand::DeleteJournal { id };
        if let Appended::At(_) = self
            .append(ctx, nodes, self.directory, &command, draw)
            .await
        {
            assert_reachable!("system: a client deletes a journal");
        }
    }

    /// The joiners a reconfiguration may name now (#189): registered (not
    /// draining, not retired) in the registry a seed serves, and not
    /// reserved for retirement. Empty on a run without system journals.
    pub(super) async fn joinable(
        &self,
        ctx: &SimContext,
        nodes: &ChainClient,
        draw: u64,
    ) -> Vec<u64> {
        // Only where a registered joiner joins the default journal: a
        // configuration naming one anywhere else names a member that runs
        // nothing.
        if !self.active || !self.spares || self.joiners.is_empty() {
            return Vec::new();
        }
        let Some((_, _, registry)) = self.read_back(ctx, nodes, self.registry, draw).await else {
            return Vec::new();
        };
        let world = crate::world::storage_world(ctx.state());
        let world = world
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        registry
            .nodes()
            .filter(|(id, node)| {
                node.standing == NodeStanding::Registered
                    && node.class == Class::Storage
                    && !world.is_retiring_joiner(id.0)
            })
            .map(|(id, _)| id.0)
            .collect()
    }

    /// `REGISTER_NODE` / `DRAIN_NODE` / `RETIRE_NODE`: one registry entry for
    /// a joiner the draw names; a drain or a retirement only for a joiner the
    /// registry has in the standing it needs (one read back first). Returns
    /// whether it registered a joiner a reconfiguration may now name.
    pub(super) async fn registry_step(
        &mut self,
        ctx: &SimContext,
        nodes: &ChainClient,
        standing: Option<NodeStanding>,
        draw: u64,
    ) -> bool {
        if self.joiners.is_empty() {
            return false;
        }
        let command = match standing {
            None => {
                // A first registration, or — for a joiner registered
                // already — a re-registration: a reboot (#211).
                let (id, addr, machine) = self.joiners
                    [usize::try_from(draw % self.joiners.len() as u64).unwrap_or(0)]
                .clone();
                // A machine that comes back as the other class (a
                // misconfigured reboot): the registry must refuse it. Only
                // for a node it has registered under its own class — a
                // first registration under the other class would be a
                // machine lying about itself, which no registry can catch.
                let mut class = machine.class;
                if self.active && buggify_with_prob!(0.1) {
                    let registered = self
                        .read_back(ctx, nodes, self.registry, draw)
                        .await
                        .is_some_and(|(_, _, registry)| {
                            registry.get(id).is_some_and(|node| {
                                node.class == machine.class
                                    && node.standing != NodeStanding::Retired
                            })
                        });
                    if registered {
                        assert_reachable!(
                            "registry: a client registers a joiner again under the other class"
                        );
                        class = match machine.class {
                            Class::Storage => Class::Stateless,
                            Class::Stateless => Class::Storage,
                        };
                    }
                }
                SystemCommand::RegisterNode {
                    id,
                    addr,
                    class,
                    capacity: machine.capacity,
                    failure_domain: format!("zone-{}", id.0 % 2),
                }
            }
            Some(standing) => {
                let registry = if self.active {
                    self.read_back(ctx, nodes, self.registry, draw)
                        .await
                        .map(|(_, _, registry)| registry)
                } else {
                    None
                };
                let eligible: Vec<NodeId> = registry
                    .as_ref()
                    .map(|registry| {
                        registry
                            .nodes()
                            .filter(|(_, node)| node.standing == standing)
                            .map(|(id, _)| id)
                            .collect()
                    })
                    .unwrap_or_default();
                let id = eligible
                    .get(usize::try_from(draw % eligible.len().max(1) as u64).unwrap_or(0))
                    .copied()
                    .unwrap_or(self.joiners[0].0);
                if standing == NodeStanding::Registered {
                    SystemCommand::DrainNode { id }
                } else {
                    // The operators coordinate (#189, the joiner's #198): a
                    // joiner some reconfiguration named is never retired,
                    // and once reserved no composer names it.
                    if self.active
                        && !crate::world::storage_world(ctx.state())
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .reserve_joiner_retirement(id.0)
                    {
                        assert_reachable!(
                            "system: a retirement of a joiner a reconfiguration named is withheld"
                        );
                        return false;
                    }
                    SystemCommand::RetireNode { id }
                }
            }
        };
        if let Appended::At(_) = self.append(ctx, nodes, self.registry, &command, draw).await {
            match command {
                SystemCommand::RegisterNode { class, .. } => {
                    assert_reachable!("system: a client registers a joiner");
                    return self.active && self.spares && class == Class::Storage;
                }
                SystemCommand::DrainNode { .. } => {
                    assert_reachable!("system: a client drains a joiner");
                }
                _ => assert_reachable!("system: a client retires a joiner"),
            }
        }
        false
    }

    /// `CHECKPOINT` (#230): open the registry as its owner with the
    /// library's [`Checkpointer`] — claim it, fold it to the tail, restarting
    /// from the checkpoint at its floor — and, when the policy finds a
    /// checkpoint due, write one and truncate to it. Two BUGGIFY locations
    /// stop between the two steps: an owner that crashes there (the
    /// checkpoint stays mid-log, and the next one truncates past it), and an
    /// owner a rival claims the registry from first (its truncate is
    /// refused by the fence, #228).
    pub(super) async fn checkpoint(
        &mut self,
        ctx: &SimContext,
        nodes: &ChainClient,
        policy: CheckpointPolicy,
        draw: u64,
    ) {
        if !self.active {
            return;
        }
        let client = self.seed_client(ctx, nodes, self.registry);
        let first = usize::try_from(draw % self.seeds as u64).unwrap_or(0);
        let mut owner =
            Checkpointer::new(self.registry, self.client_id, self.empty_registry(), policy);
        match owner.open(&client, first).await {
            OpenOutcome::Open {
                restarted,
                diverged,
                ..
            } => {
                assert_always!(
                    diverged.is_none(),
                    "checkpoint: an owner's load finds each checkpoint its prefix's state",
                    { "journal" => self.registry.to_string(), "seq" => diverged.unwrap_or_default() }
                );
                if restarted {
                    board_lock(&system_board(ctx.state())).reader_restarted();
                }
            }
            _ => return,
        }
        if !owner.due(client.now()) {
            return;
        }
        // BUGGIFY pairing: the policy's decision is reached (a cause; the
        // outcomes are the truncation and the restarts it forces).
        assert_reachable!("checkpoint: an owner finds a registry checkpoint due");
        if buggify_with_prob!(0.15) {
            // A crash between the checkpoint and its truncate.
            if owner.write_checkpoint(&client, first).await.is_ok() {
                assert_reachable!(
                    "checkpoint: an owner stops between its checkpoint and its truncate"
                );
            }
            return;
        }
        if buggify_with_prob!(0.15) {
            // A rival claims the registry between the two steps.
            let Ok(seq) = owner.write_checkpoint(&client, first).await else {
                return;
            };
            let mut rival = Writer::new(self.registry, self.client_id | 1 << 40);
            if !matches!(
                rival.claim(&client, first, true).await,
                ClaimOutcome::Won { .. }
            ) {
                return;
            }
            let truncate = owner.truncate_to(&client, seq, first).await;
            assert_always!(
                !matches!(truncate, Some(TruncateOutcome::Applied { .. })),
                "checkpoint: a superseded owner's truncate is never applied",
                { "seq" => seq }
            );
            if matches!(truncate, Some(TruncateOutcome::Refused { .. })) {
                assert_reachable!(
                    "checkpoint: a superseded owner's truncate to its checkpoint is refused"
                );
            }
            return;
        }
        if let CheckpointOutcome::Checkpointed {
            truncate: Some(TruncateOutcome::Applied { state }),
            seq,
        } = owner.checkpoint(&client, first).await
        {
            assert_always!(
                state.first_seq.0 >= seq,
                "checkpoint: a truncate to a checkpoint raises the floor to it",
                { "seq" => seq, "first" => state.first_seq.0 }
            );
            assert_reachable!("checkpoint: an owner truncates the registry to its checkpoint");
            board_lock(&system_board(ctx.state())).truncated_to_checkpoint();
        }
    }

    /// `BOOK_CAPACITY` (#211): what the cell coordinator writes — book one
    /// slot of a registered joiner for a journal, under a booking id drawn
    /// here, or release one of this client's bookings. One location books a
    /// slot of the other class than the node's, which the registry must
    /// refuse; the capacity knob's floor makes a full node common.
    pub(super) async fn book(&mut self, ctx: &SimContext, nodes: &ChainClient, draw: u64) {
        if self.joiners.is_empty() {
            return;
        }
        if !self.booked.is_empty() && draw.is_multiple_of(3) {
            let booking = self
                .booked
                .remove(usize::try_from(draw % self.booked.len() as u64).unwrap_or(0));
            let command = SystemCommand::ReleaseCapacity { booking };
            if let Appended::At(_) = self.append(ctx, nodes, self.registry, &command, draw).await {
                assert_reachable!("registry: a client releases a booking");
            }
            return;
        }
        let (node, _, machine) = self.joiners
            [usize::try_from((draw >> 8) % self.joiners.len() as u64).unwrap_or(0)]
        .clone();
        let class = if buggify_with_prob!(0.15) {
            assert_reachable!("registry: a client books a slot of the other class");
            match machine.class {
                Class::Storage => Class::Stateless,
                Class::Stateless => Class::Storage,
            }
        } else {
            machine.class
        };
        let journal = self.created.first().map_or(self.main, |id| {
            JournalIdentifier::new(self.directory.tenant, *id)
        });
        let booking = crate::chain::splitmix(draw ^ self.client_id.rotate_left(32));
        let command = SystemCommand::BookCapacity {
            booking,
            node,
            class,
            journal,
        };
        let Appended::At(position) = self.append(ctx, nodes, self.registry, &command, draw).await
        else {
            return;
        };
        let Some((events, _, _)) = self.read_back(ctx, nodes, self.registry, draw).await else {
            return;
        };
        // The class and capacity oracles judge every booking where the
        // nodes fold it (the system board); the client keeps what it holds.
        if let Some(SystemEvent::Registry(RegistryEvent::Booked { .. })) = events
            .into_iter()
            .find(|(at, _)| *at == position)
            .map(|(_, e)| e)
        {
            self.booked.push(booking);
        }
    }
}
