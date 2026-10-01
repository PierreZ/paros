//! The chain client's **system-journal operations** (#189): the directory's
//! `CreateJournal` / `DeleteJournal` and the node registry's
//! `RegisterNode` / `DrainNode` / `RetireNode`, each one record written to
//! journal 1 or 2 at a seed, and the read-back a creator does to learn what
//! its request folded to.
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
//! No function here draws randomness: every choice is read off the caller's
//! step draws.

use std::time::Duration;

use moonpool_sim::{SimContext, assert_always, assert_reachable, buggify_with_prob};
use paros::system::{
    DIRECTORY, Directory, DirectoryEvent, DirectoryRefusal, NodeStanding, REGISTRY, Registry,
    SystemCommand, SystemEvent,
};
use paros::{
    AcceptorConfig, Command, Entry, Generation, JournalId, NodeId, QuorumSystem, Seq, Value,
};

use super::rpc::{
    CallLog, SetLeaderResult, WriteResult, read_once, set_leader_once, state_of, within, write_once,
};
use crate::audit::audit_world_for;
use crate::chain::user_command_hash;
use crate::client::SimClient;

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
    /// How many genesis ranks host journals 1 and 2 (the seeds).
    seeds: usize,
    /// The genesis pool size.
    pool: usize,
    /// The joiners, `(id, address)`.
    joiners: Vec<(NodeId, String)>,
    /// The genesis journals: ids the directory never allocates.
    genesis: Vec<JournalId>,
    /// A registered joiner joins the default journal as a spare (a seed with
    /// matchmakers and neither proxies nor replicas, `process::spare_template`),
    /// so a reconfiguration may name one.
    spares: bool,
    client_id: u64,
    /// Journals this client created and has not asked to delete.
    created: Vec<JournalId>,
    timeout: Duration,
    /// The client's call log: the RPC seam logs the calls naming its own
    /// journal only, so a system or created journal's calls pass through.
    log: CallLog,
}

impl SystemOps {
    /// The operations of client `client_id` on `deployment`: `active` when
    /// the run runs the system journals, `genesis` the journals it booted
    /// with.
    pub(super) fn new(
        deployment: &crate::roles::Deployment,
        active: bool,
        genesis: Vec<JournalId>,
        client_id: u64,
        timeout: Duration,
        log: CallLog,
    ) -> Self {
        let pool = deployment.acceptors().len();
        let matchmakers = !deployment.matchmakers().is_empty();
        Self {
            active,
            seeds: crate::shape::seed_ranks(pool).len().max(1),
            pool,
            joiners: deployment
                .joiners()
                .iter()
                .enumerate()
                .filter_map(|(rank, ip)| {
                    paros::parse_addr(ip)
                        .ok()
                        .map(|addr| (crate::roles::joiner_node_id(rank), addr))
                })
                .collect(),
            genesis,
            spares: matchmakers
                && deployment.proxies().is_empty()
                && deployment.replicas().is_empty(),
            client_id,
            created: Vec::new(),
            timeout,
            log,
        }
    }

    /// Write `command` to `journal` at the seeds, starting at the one
    /// `draw` names and following redirects.
    async fn append(
        &mut self,
        ctx: &SimContext,
        clients: &[SimClient],
        journal: JournalId,
        command: &SystemCommand,
        draw: u64,
    ) -> Appended {
        let seeds: Vec<usize> = (0..self.seeds).collect();
        self.claim_and_write(ctx, clients, journal, &seeds, command.encode(), draw)
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
        clients: &[SimClient],
        journal: JournalId,
        targets: &[usize],
        record: Vec<u8>,
        draw: u64,
    ) -> Appended {
        let created = !paros::system::is_system(journal);
        let audit = audit_world_for(ctx.state(), journal);
        audit.note_submitted(user_command_hash(&record));
        let mut target = usize::try_from(draw % targets.len() as u64).unwrap_or(0);
        let mut claim: Option<(u64, u64)> = None;
        for _ in 0..APPEND_ATTEMPTS {
            let node = targets[target % targets.len()] % clients.len();
            let client = clients[node].clone();
            let Some((generation, position)) = claim else {
                // Read where the journal stands, then claim it.
                let read = read_once(&client, &self.log, journal.0, 0, 1, 0);
                let Some(ack) = within(ctx, self.timeout, None, read).await else {
                    target += 1;
                    continue;
                };
                if ack.unknown_journal && created {
                    target += 1;
                    continue;
                }
                if ack.unknown_journal {
                    assert_always!(
                        !self.active || !paros::system::is_system(journal),
                        "system: a seed serves the system journals",
                        { "journal" => journal.0, "seed" => node }
                    );
                    if paros::system::is_system(journal) {
                        assert_reachable!(
                            "system: a system append on a seed without system journals is refused"
                        );
                    }
                    return Appended::Unknown;
                }
                if !ack.served {
                    target += 1;
                    continue;
                }
                let tail = state_of(ack.state);
                let ask = set_leader_once(
                    clients,
                    &self.log,
                    journal,
                    node,
                    tail.generation.0,
                    self.client_id,
                    created,
                );
                match within(ctx, self.timeout, SetLeaderResult::Ambiguous, ask).await {
                    SetLeaderResult::Won { state } => {
                        claim = Some((state.generation.0, state.next_seq.0));
                    }
                    SetLeaderResult::Lost { .. } | SetLeaderResult::Ambiguous => {}
                    SetLeaderResult::Redirect { leader } => {
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
            let write = write_once(
                clients,
                &self.log,
                ctx.time(),
                journal,
                node,
                &entry,
                false,
                created,
            );
            match within(ctx, self.timeout, WriteResult::Ambiguous, write).await {
                WriteResult::Written { seq, .. } => return Appended::At(seq),
                // Fenced by a later claim, or behind: claim again.
                WriteResult::Refused { .. } | WriteResult::Truncated { .. } => claim = None,
                WriteResult::Redirect { leader } => {
                    target = leader
                        .and_then(|l| targets.iter().position(|t| *t as u64 == l))
                        .unwrap_or(target + 1);
                }
                WriteResult::Ambiguous => target += 1,
            }
        }
        Appended::Ambiguous
    }

    /// Read `journal` from position 0 to a seed's tail and fold it: every
    /// event with its position, and the folds.
    async fn read_back(
        &self,
        ctx: &SimContext,
        clients: &[SimClient],
        journal: JournalId,
        draw: u64,
    ) -> Option<(Vec<(u64, SystemEvent)>, Directory, Registry)> {
        let client = clients[usize::try_from(draw % self.seeds as u64).unwrap_or(0)].clone();
        let mut directory = Directory::new(self.genesis.iter().copied());
        let mut registry = Registry::new((0..self.pool as u64).map(NodeId));
        let mut events = Vec::new();
        let mut from = 0;
        loop {
            let read = read_once(&client, &self.log, journal.0, from, READ_RECORDS, 0);
            let ack = within(ctx, self.timeout, None, read).await?;
            if ack.unknown_journal || ack.truncated || !ack.served {
                return None;
            }
            let next = from + ack.records.len() as u64;
            for (position, record) in (from..).zip(&ack.records) {
                let event = if journal == DIRECTORY {
                    SystemEvent::Directory(directory.fold(position, record))
                } else {
                    SystemEvent::Registry(registry.fold(position, record))
                };
                events.push((position, event));
            }
            if next <= from || next >= state_of(ack.state).next_seq.0 {
                return Some((events, directory, registry));
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
        clients: &[SimClient],
        (class, payload): (u64, u64),
    ) {
        let name = NAMES[usize::try_from(class % NAMES.len() as u64).unwrap_or(0)].to_vec();
        let mut candidates: Vec<NodeId> = (0..self.pool as u64).map(NodeId).collect();
        if self.active
            && let Some((_, _, registry)) = self.read_back(ctx, clients, REGISTRY, payload).await
        {
            candidates.extend(
                registry
                    .nodes()
                    .filter(|(_, node)| node.standing == NodeStanding::Registered)
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
            members = vec![
                joiner,
                NodeId(start as u64 % self.pool as u64),
                NodeId((start as u64 + 1) % self.pool as u64),
            ];
        }
        let config = AcceptorConfig::new(members, QuorumSystem::Majority);
        let command = SystemCommand::CreateJournal {
            name,
            config: config.clone(),
        };
        let Appended::At(position) = self
            .append(ctx, clients, DIRECTORY, &command, payload)
            .await
        else {
            return;
        };
        let Some((events, _, _)) = self.read_back(ctx, clients, DIRECTORY, payload).await else {
            return;
        };
        match events
            .into_iter()
            .find(|(at, _)| *at == position)
            .map(|(_, e)| e)
        {
            Some(SystemEvent::Directory(DirectoryEvent::Created { id, .. })) => {
                assert_reachable!("system: a client creates a journal and reads back its id");
                self.created.push(id);
                self.append_to_created(ctx, clients, id, &config, payload)
                    .await;
            }
            Some(SystemEvent::Directory(DirectoryEvent::Refused(
                DirectoryRefusal::NameTaken { .. },
            ))) => {
                assert_reachable!(
                    "system: a client reads back its create refused for a taken name"
                );
            }
            _ => {}
        }
    }

    /// One record written to a journal this client created, at a genesis
    /// member (the client has no connection to a joiner).
    async fn append_to_created(
        &mut self,
        ctx: &SimContext,
        clients: &[SimClient],
        journal: JournalId,
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
            .claim_and_write(ctx, clients, journal, &genesis, record, draw)
            .await
        {
            assert_reachable!("system: a created journal commits an append");
        }
    }

    /// `DELETE_JOURNAL`: tombstone a journal this client created, then ask a
    /// genesis member for one more append to it.
    pub(super) async fn delete(&mut self, ctx: &SimContext, clients: &[SimClient], draw: u64) {
        let id = if self.created.is_empty() {
            // Nothing of its own: a delete of an id nobody created, which
            // folds to a refusal.
            JournalId(JournalId::FIRST_USER.0 + 1_000 + draw % 16)
        } else {
            self.created
                .remove(usize::try_from(draw % self.created.len() as u64).unwrap_or(0))
        };
        let command = SystemCommand::DeleteJournal { id };
        if let Appended::At(_) = self.append(ctx, clients, DIRECTORY, &command, draw).await {
            assert_reachable!("system: a client deletes a journal");
        }
    }

    /// The joiners a reconfiguration may name now (#189): registered (not
    /// draining, not retired) in the registry a seed serves, and not
    /// reserved for retirement. Empty on a run without system journals.
    pub(super) async fn joinable(
        &self,
        ctx: &SimContext,
        clients: &[SimClient],
        draw: u64,
    ) -> Vec<u64> {
        // Only where a registered joiner joins the default journal: a
        // configuration naming one anywhere else names a member that runs
        // nothing.
        if !self.active || !self.spares || self.joiners.is_empty() {
            return Vec::new();
        }
        let Some((_, _, registry)) = self.read_back(ctx, clients, REGISTRY, draw).await else {
            return Vec::new();
        };
        let world = crate::world::storage_world(ctx.state());
        let world = world
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        registry
            .nodes()
            .filter(|(id, node)| {
                node.standing == NodeStanding::Registered && !world.is_retiring_joiner(id.0)
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
        clients: &[SimClient],
        standing: Option<NodeStanding>,
        draw: u64,
    ) -> bool {
        if self.joiners.is_empty() {
            return false;
        }
        let command = match standing {
            None => {
                let (id, addr) = self.joiners
                    [usize::try_from(draw % self.joiners.len() as u64).unwrap_or(0)]
                .clone();
                SystemCommand::RegisterNode {
                    id,
                    addr,
                    failure_domain: format!("zone-{}", id.0 % 2),
                }
            }
            Some(standing) => {
                let registry = if self.active {
                    self.read_back(ctx, clients, REGISTRY, draw)
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
        if let Appended::At(_) = self.append(ctx, clients, REGISTRY, &command, draw).await {
            match command {
                SystemCommand::RegisterNode { .. } => {
                    assert_reachable!("system: a client registers a joiner");
                    return self.active && self.spares;
                }
                SystemCommand::DrainNode { .. } => {
                    assert_reachable!("system: a client drains a joiner");
                }
                _ => assert_reachable!("system: a client retires a joiner"),
            }
        }
        false
    }
}
