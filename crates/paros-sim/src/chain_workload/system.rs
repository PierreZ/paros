//! The chain client's **system-journal operations** (#189): the node
//! registry's `RegisterNode` / `DrainNode` / `RetireNode`, its capacity
//! bookings and its checkpoint, each one record written to the registry (a
//! harness tenant's control journal on the acceptors, #235) at a seed, and
//! the read-back a writer does to learn what its request folded to. A
//! tenant's journals are created and deleted on the machines, through the
//! tenant coordinator (#210, `super::fleet::journals`).
//!
//! A system journal is written like any journal (#204): a writer claims it
//! with `SetLeader` against the leader it read, then `Write`s at the
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

use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use moonpool_sim::{SimContext, assert_always, assert_reachable, buggify_with_prob};
use paros::client::checkpoint::{
    CheckpointOutcome, CheckpointPolicy, Checkpointer, Folded, Folder, OpenOutcome,
};
use paros::client::{Answered, Attempted, CallObserver, ClaimOutcome, TruncateOutcome, Writer};
use paros::system::{
    BookingTarget, Class, NodeStanding, Registry, RegistryEvent, Role, SystemCommand, SystemEvent,
    registry_event,
};
use paros::{Command, Entry, JournalIdentifier, LeaderUuid, NodeId, Seq, Value};

use paros::client::{ReadOutcome, SetLeaderOutcome, WriteOutcome};

use super::rpc::{CallLog, read_once, set_leader_once, within, write_once};
use crate::audit::audit_world_for;
use crate::audit::system::{lock as board_lock, system_board};
use crate::chain::user_command_hash;
use crate::client::ChainClient;
use crate::shape::JoinerMachine;

/// How many asks a system write spends before it calls the outcome
/// ambiguous.
const APPEND_ATTEMPTS: usize = 6;

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
    /// The registry's identifier (drawn per seed: no identifier is fixed,
    /// §3.8).
    registry: JournalIdentifier,
    /// The deployment's journal.
    main: JournalIdentifier,
    /// How many genesis ranks host the system journals (the seeds).
    seeds: usize,
    /// The genesis pool size.
    pool: usize,
    /// The joiners, `(id, address, machine)`.
    joiners: Vec<(NodeId, String, JoinerMachine)>,
    /// A registered joiner joins the default journal as a spare (a seed with
    /// matchmakers and neither proxies nor replicas, `process::spare_template`),
    /// so a reconfiguration may name one.
    spares: bool,
    client_id: u64,
    /// A fresh seed per claim and checkpointer (#241).
    leader_seeds: super::LeaderSeeds,
    /// The bookings this client made and has not released.
    booked: Vec<u64>,
    /// The booking ids this client released: never bookable again (#211).
    released: Vec<u64>,
    timeout: Duration,
}

/// The system journals' half of the audit's write oracles, as a
/// [`CallObserver`]: a library call that writes to one of `journals` (a
/// [`Checkpointer`]'s, a fleet operation's) announces its records and its
/// exact write to that journal's audit before it leaves, like every
/// hand-built system append here does. It also logs **every attempt** at
/// those journals — the four calls, answered or not — to the control
/// journal's shared history (#247, [`super::rpc::control_attempts`]), which
/// `check()` searches for a linearization against the journal model, as a
/// tenant journal's history is.
///
/// Over the machines it also holds **no unlearned id** (#246): with
/// [`Announce::learned_only`], every attempt names a journal its operator
/// learned from `init`'s reply or through `Inspect`, never one the harness
/// knows (§3.8: no identifier is fixed).
pub(crate) struct Announce {
    /// The journals announced; `None` announces every journal the client
    /// calls — a client over the machines (#246), whose control journals
    /// are learned at runtime.
    only: Option<Vec<JournalIdentifier>>,
    state: moonpool_sim::StateHandle,
    time: moonpool_sim::SimTimeProvider,
    client: u64,
    /// Each announced journal's audit world and shared log, in first-call
    /// order: an attempt token names its index.
    journals: Mutex<Vec<(JournalIdentifier, Arc<crate::audit::AuditWorld>, CallLog)>>,
    /// The journals its operator learned, when every call must name one.
    learned: Option<Learned>,
    /// The fenced call each pending token carries: its journal and uuid,
    /// for the deposed-actor oracle (#240).
    fenced: Mutex<std::collections::BTreeMap<u64, (JournalIdentifier, LeaderUuid)>>,
}

const DEPOSED_KEY: &str = "paros-deposed-uuids";

/// The uuids a fenced call to `journal` was refused under because another
/// leads (#240), shared by every client of the run.
fn deposed(
    state: &moonpool_sim::StateHandle,
    journal: JournalIdentifier,
) -> Arc<Mutex<std::collections::BTreeSet<LeaderUuid>>> {
    crate::state::published_arc(
        state,
        &crate::state::journal_key(DEPOSED_KEY, journal),
        || Mutex::new(std::collections::BTreeSet::new()),
    )
}

/// The journal identifiers one operator learned (#246): from `init`'s reply
/// or through `Inspect`.
pub(super) type Learned = Arc<Mutex<std::collections::BTreeSet<JournalIdentifier>>>;

/// An attempt token names its journal in the high bits: a token is one
/// journal's [`CallLog`] index.
const TOKEN_JOURNAL_SHIFT: u32 = 48;

impl Announce {
    /// An observer announcing the writes to each of `journals`.
    pub(super) fn new(ctx: &SimContext, journals: &[JournalIdentifier]) -> Self {
        Self {
            only: Some(journals.to_vec()),
            ..Self::every(ctx)
        }
    }

    /// An observer announcing the writes to every journal its client calls
    /// (#246): a client over the machines, which serve only the cell's
    /// journals.
    pub(super) fn every(ctx: &SimContext) -> Self {
        Self {
            only: None,
            state: ctx.state().clone(),
            time: ctx.time().clone(),
            client: u64::try_from(ctx.client_id()).unwrap_or(0),
            journals: Mutex::new(Vec::new()),
            learned: None,
            fenced: Mutex::new(std::collections::BTreeMap::new()),
        }
    }

    /// An observer announcing every journal a founding member's own client
    /// calls (the cell coordinator's, #240), as client `client` of the
    /// control journals' histories.
    pub(crate) fn of_machine(
        state: &moonpool_sim::StateHandle,
        time: moonpool_sim::SimTimeProvider,
        client: u64,
    ) -> Self {
        Self {
            only: None,
            state: state.clone(),
            time,
            client,
            journals: Mutex::new(Vec::new()),
            learned: None,
            fenced: Mutex::new(std::collections::BTreeMap::new()),
        }
    }

    /// This observer, holding every attempt to a journal in `learned`: the
    /// "no unlearned id" oracle (#246).
    pub(super) fn learned_only(self, learned: Learned) -> Self {
        Self {
            learned: Some(learned),
            ..self
        }
    }

    /// `journal`'s index, its entry made on the first call; `None` for a
    /// journal this observer does not announce.
    fn entry(
        &self,
        journal: JournalIdentifier,
    ) -> Option<(usize, Arc<crate::audit::AuditWorld>, CallLog)> {
        if self
            .only
            .as_ref()
            .is_some_and(|only| !only.contains(&journal))
        {
            return None;
        }
        let mut journals = self.journals.lock().unwrap_or_else(PoisonError::into_inner);
        let index = if let Some(index) = journals.iter().position(|(j, _, _)| *j == journal) {
            index
        } else {
            journals.push((
                journal,
                audit_world_for(&self.state, journal),
                CallLog::shared(
                    journal,
                    self.client,
                    self.time.clone(),
                    super::rpc::control_attempts(&self.state, journal),
                ),
            ));
            journals.len() - 1
        };
        let (_, audit, log) = &journals[index];
        Some((index, audit.clone(), log.clone()))
    }
}

impl CallObserver for Announce {
    fn invoked(&self, attempt: Attempted<'_>) -> Option<u64> {
        if let Some(learned) = &self.learned {
            let journal = attempt.journal();
            assert_always!(
                learned
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .contains(&journal),
                "fleet: an operator calls only a journal it learned",
                { "client" => self.client, "journal" => journal.to_string() }
            );
        }
        let (index, audit, log) = self.entry(attempt.journal())?;
        // An actor's writer stops at its first refusal (#240): no fenced
        // call leaves under a uuid a refusal already named deposed.
        let fence = match attempt {
            Attempted::Write(write) => Some(paros::leader_uuid_from_proto(write.leader)),
            Attempted::Truncate(truncate) => Some(paros::leader_uuid_from_proto(truncate.leader)),
            Attempted::SetLeader(_) | Attempted::Read(_) => None,
        }
        .filter(|uuid| uuid.is_set());
        if let Some(uuid) = fence {
            let journal = attempt.journal();
            assert_always!(
                !deposed(&self.state, journal)
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .contains(&uuid),
                "election: a deposed actor writes nothing after its first refusal",
                { "client" => self.client, "journal" => journal.to_string() }
            );
        }
        if let Attempted::Write(write) = attempt {
            for record in &write.records {
                audit.note_submitted(user_command_hash(record));
            }
            let entry = Entry {
                leader: paros::leader_uuid_from_proto(write.leader),
                seq: Seq(write.seq),
                records: write.records.iter().cloned().map(Value).collect(),
            };
            audit.note_appended(paros::command_hash(&Command::Write(entry)));
        }
        let token = log.invoked(attempt)?;
        let token = (index as u64) << TOKEN_JOURNAL_SHIFT | token;
        if let Some(uuid) = fence {
            self.fenced
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(token, (attempt.journal(), uuid));
        }
        Some(token)
    }

    fn answered(&self, token: u64, answer: Answered<'_>) {
        let fenced = self
            .fenced
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&token);
        if let Some((journal, uuid)) = fenced {
            let leader = match answer {
                Answered::Write(WriteOutcome::Refused { state })
                | Answered::Truncate(TruncateOutcome::Refused { state }) => Some(state.leader),
                _ => None,
            };
            if let Some(leader) = leader
                && leader != Some(uuid)
            {
                deposed(&self.state, journal)
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .insert(uuid);
                assert_reachable!("election: a fenced call is refused because another leads");
            }
        }
        let index = usize::try_from(token >> TOKEN_JOURNAL_SHIFT).unwrap_or(usize::MAX);
        let log = self
            .journals
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(index)
            .map(|(_, _, log)| log.clone());
        if let Some(log) = log {
            log.answered(token & ((1 << TOKEN_JOURNAL_SHIFT) - 1), answer);
        }
    }
}

impl SystemOps {
    /// The operations of client `client_id` on `deployment`: `active` when
    /// the run runs the system journals.
    pub(super) fn new(
        deployment: &crate::roles::Deployment,
        identifiers: crate::shape::Identifiers,
        active: bool,
        machines: &[JoinerMachine],
        (client_id, leader_seeds): (u64, super::LeaderSeeds),
        timeout: Duration,
    ) -> Self {
        let pool = deployment.acceptors().len();
        let matchmakers = !deployment.matchmakers().is_empty();
        Self {
            active,
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
            spares: matchmakers
                && deployment.proxies().is_empty()
                && deployment.replicas().is_empty(),
            client_id,
            leader_seeds,
            booked: Vec::new(),
            released: Vec::new(),
            timeout,
        }
    }

    /// The genesis pool's registry, empty.
    fn empty_registry(&self) -> Registry {
        Registry::new((0..self.pool as u64).map(NodeId))
    }

    /// The client of the seeds — the nodes hosting the system journals —
    /// that announces its system writes to `journal`'s audit, under a leader
    /// hint of its own (the hint `nodes` carries is its own journal's).
    fn seed_client(
        &self,
        ctx: &SimContext,
        nodes: &ChainClient,
        journal: JournalIdentifier,
    ) -> ChainClient {
        let seeds = self.seeds.min(nodes.server_count()).max(1);
        nodes
            .clone()
            .with_own_leader_hint()
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
    /// `SetLeader` against its leader, then `Write` at the position the
    /// claim answered. A lost claim, a fenced write and a redirect are
    /// retried within [`APPEND_ATTEMPTS`] asks.
    async fn claim_and_write(
        &mut self,
        ctx: &SimContext,
        nodes: &ChainClient,
        journal: JournalIdentifier,
        targets: &[usize],
        record: Vec<u8>,
        draw: u64,
    ) -> Appended {
        // A system journal's calls go through the seeds' client, which logs
        // them to the control journal's history (#247).
        let caller = self.seed_client(ctx, nodes, journal);
        let nodes = &caller;
        let audit = audit_world_for(ctx.state(), journal);
        audit.note_submitted(user_command_hash(&record));
        let mut target = usize::try_from(draw % targets.len() as u64).unwrap_or(0);
        // A multi-writer journal (#241) takes unfenced writes and no claim:
        // its seq is assigned at apply.
        let unfenced = (audit.mode() == paros::WriterMode::Multi).then_some((LeaderUuid::UNSET, 0));
        if unfenced.is_some() {
            assert_reachable!("system: a client appends unfenced to a multi-writer journal");
        }
        let mut claim: Option<(LeaderUuid, u64)> = unfenced;
        for _ in 0..APPEND_ATTEMPTS {
            let node = targets[target % targets.len()] % nodes.server_count();
            let Some((leader, position)) = claim else {
                // Read where the journal stands, then claim it.
                let read = read_once(nodes, node, journal, 0, 1, 0);
                let answer = within(ctx, self.timeout, ReadOutcome::Ambiguous, read).await;
                if answer == ReadOutcome::UnknownJournal {
                    assert_always!(
                        !self.active,
                        "system: a seed serves the system journals",
                        { "journal" => journal.to_string(), "seed" => node }
                    );
                    assert_reachable!(
                        "system: a system append on a seed without system journals is refused"
                    );
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
                let uuid = paros::client::leader_uuid(self.leader_seeds.next(), 0);
                let ask = set_leader_once(nodes, journal, node, (uuid, tail.leader), false);
                match within(ctx, self.timeout, SetLeaderOutcome::Ambiguous, ask).await {
                    SetLeaderOutcome::Won { state } => {
                        claim = Some((uuid, state.next_seq.0));
                    }
                    SetLeaderOutcome::WrongMode { .. } => system_never_of_wrong_mode(),
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
                leader,
                seq: Seq(position),
                records: vec![Value(record.clone())],
            };
            audit.note_appended(paros::command_hash(&Command::Write(entry.clone())));
            let write = write_once(nodes, journal, node, &entry, false, false);
            match within(ctx, self.timeout, WriteOutcome::Ambiguous, write).await {
                WriteOutcome::Written { seq, .. } => return Appended::At(seq),
                // Fenced by a later claim, or behind: claim again.
                WriteOutcome::Refused { .. } | WriteOutcome::Truncated { .. } => claim = unfenced,
                WriteOutcome::WrongMode { .. } => {
                    system_never_of_wrong_mode();
                    claim = unfenced;
                }
                WriteOutcome::Redirect { leader } => {
                    target = leader
                        .and_then(|l| targets.iter().position(|t| *t as u64 == l))
                        .unwrap_or(target + 1);
                }
                // The record limit's floor admits one record, and the byte
                // limit's floor a system command (`paros_sim::shape`).
                WriteOutcome::TooLarge { .. } => {
                    assert_always!(
                        false,
                        "system: a one-record system write fits every node's limits"
                    );
                    target += 1;
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

    /// Read the registry from position 0 to a seed's tail and fold it: every
    /// event with its position, and the fold. It folds through a
    /// [`Folder`] (#230): a read below its floor jumps there and restarts
    /// from the checkpoint, and a checkpoint met with the whole prefix
    /// folded is verified against it. `None` when a page goes unserved, or
    /// the fold ends above a gap no checkpoint healed.
    async fn read_back(
        &self,
        ctx: &SimContext,
        nodes: &ChainClient,
        draw: u64,
    ) -> Option<(Vec<(u64, SystemEvent)>, Registry)> {
        let journal = self.registry;
        let seed = usize::try_from(draw % self.seeds as u64).unwrap_or(0);
        let caller = self.seed_client(ctx, nodes, journal);
        let nodes = &caller;
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
                    ReadOutcome::Truncated { state } if state.first_seq.0 > from => {
                        registry.jump(state.first_seq.0);
                        from = state.first_seq.0;
                        continue;
                    }
                    _ => return None,
                };
            let next = from + records.len() as u64;
            for (position, record) in (from..).zip(&records) {
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
                return Some((events, registry.state().clone()));
            }
            from = next;
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
        let Some((_, registry)) = self.read_back(ctx, nodes, draw).await else {
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
                    let registered =
                        self.read_back(ctx, nodes, draw)
                            .await
                            .is_some_and(|(_, registry)| {
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
                    // A joiner's registration names a fresh incarnation:
                    // each one is a (re)boot.
                    incarnation: u128::from(crate::chain::splitmix(draw)) | 1,
                }
            }
            Some(standing) => {
                let registry = if self.active {
                    self.read_back(ctx, nodes, draw)
                        .await
                        .map(|(_, registry)| registry)
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
        let mut owner = Checkpointer::new(
            self.registry,
            self.leader_seeds.next(),
            self.empty_registry(),
            policy,
        );
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
            let mut rival = Writer::new(self.registry, self.leader_seeds.next());
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
    /// slot of a registered joiner for a role of a journal or matchmaker
    /// set, under a booking id drawn here, or release one of this client's
    /// bookings. One location books a role of the other class than the
    /// node's, which the registry must refuse; another books again under an
    /// id this client released, which the registry must refuse too (ids are
    /// never reused, across checkpoints); the capacity knob's floor makes a
    /// full node common.
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
                self.released.push(booking);
            }
            if !buggify_with_prob!(0.5) {
                return;
            }
        }
        if let Some(booking) = self.released.last().copied()
            && draw.is_multiple_of(3)
        {
            // A booking under an id this client released: the registry
            // must refuse it, whatever the node (ids are never reused).
            assert_reachable!("registry: a client books again under an id it released");
            let (node, _, machine) = self.joiners[0].clone();
            let role = Role::of(machine.class).next().unwrap_or(Role::Acceptor);
            let command = SystemCommand::BookCapacity {
                booking,
                node,
                role,
                target: BookingTarget::Journal(self.main),
            };
            self.append(ctx, nodes, self.registry, &command, draw).await;
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
        let roles: Vec<Role> = Role::of(class).collect();
        let role = roles[usize::try_from((draw >> 16) % roles.len() as u64).unwrap_or(0)];
        let journal = self.main;
        let target = if role == Role::Matchmaker {
            BookingTarget::Set {
                tenant: journal.tenant,
                set: 1 + (draw >> 24) % 2,
            }
        } else {
            BookingTarget::Journal(journal)
        };
        let booking = crate::chain::splitmix(draw ^ self.client_id.rotate_left(32));
        let command = SystemCommand::BookCapacity {
            booking,
            node,
            role,
            target,
        };
        let Appended::At(position) = self.append(ctx, nodes, self.registry, &command, draw).await
        else {
            return;
        };
        let Some((events, _)) = self.read_back(ctx, nodes, draw).await else {
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

/// A system journal is single-writer, and its writers fence every call: a
/// wrong-mode refusal (#241) there is a bug.
fn system_never_of_wrong_mode() {
    assert_always!(
        false,
        "system: a system journal call is never refused as of the wrong mode"
    );
}
