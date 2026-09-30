//! The chain client's **system-journal operations** (#189): the directory's
//! `CreateJournal` / `DeleteJournal` and the node registry's
//! `RegisterNode` / `DrainNode` / `RetireNode`, each one `Append` of one
//! record to journal 1 or 2 at a seed, and the read-back a creator does to
//! learn what its request folded to.
//!
//! A client reads a system journal exactly as a node follows it: `Read` from
//! LSN 0 and the same pure fold (`paros::system`), so what it reads back is
//! what every node applied. On a seed that runs no system journals the
//! request is still sent, and must be refused as naming an unknown journal —
//! the same parity a `Reconfigure` keeps on a plain seed.
//!
//! No function here draws randomness: every choice is read off the caller's
//! step draws.

use std::time::Duration;

use moonpool_sim::{SimContext, assert_always, assert_reachable, buggify_with_prob};
use paros::system::{
    DIRECTORY, Directory, DirectoryEvent, DirectoryRefusal, NodeStanding, REGISTRY, Registry,
    SystemCommand, SystemEvent,
};
use paros::{AcceptorConfig, Append, JournalId, NodeId, QuorumSystem, Read, encode_records};

use super::rpc::within;
use crate::audit::audit_world_for;
use crate::chain::user_command_hash;
use crate::client::SimClient;

/// Where a client's system-journal sequence numbers start: far above any
/// user journal's, so an identity never reads as both.
const SYSTEM_SEQ_BASE: u64 = 1 << 40;

/// How many seeds a system append tries before it calls the outcome
/// ambiguous.
const APPEND_ATTEMPTS: usize = 4;

/// The names a created journal is drawn from: few, so two creates race for
/// one often.
const NAMES: [&[u8]; 4] = [b"alpha", b"beta", b"gamma", b"delta"];

/// A read-back page's byte budget.
const READ_BYTES: u64 = 64 * 1024;

/// One system append's terminal outcome.
enum Appended {
    /// Chosen at this LSN.
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
    next_seq: u64,
    /// Journals this client created and has not asked to delete.
    created: Vec<JournalId>,
    timeout: Duration,
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
            next_seq: SYSTEM_SEQ_BASE,
            created: Vec::new(),
            timeout,
        }
    }

    /// Append `command` to `journal` at the seeds, starting at the one
    /// `draw` names and following redirects.
    async fn append(
        &mut self,
        ctx: &SimContext,
        clients: &[SimClient],
        journal: JournalId,
        command: &SystemCommand,
        draw: u64,
    ) -> Appended {
        let seq = self.next_seq;
        self.next_seq += 1;
        let record = command.encode();
        // The journal's own oracles' ground truth: this identity was
        // appended here, carrying these bytes.
        let audit = audit_world_for(ctx.state(), journal);
        audit.note_appended(self.client_id, seq);
        audit.note_submitted(user_command_hash(&encode_records(std::slice::from_ref(
            &record,
        ))));
        let request = Append {
            journal: journal.0,
            client: self.client_id,
            seq,
            records: vec![record],
        };
        let mut target = usize::try_from(draw % self.seeds as u64).unwrap_or(0);
        for _ in 0..APPEND_ATTEMPTS {
            let client = clients[target % clients.len()].clone();
            let answer = within(ctx, self.timeout, None, async {
                client.append(&request).await.ok()
            })
            .await;
            match answer {
                Some(ack) if ack.unknown_journal => {
                    assert_always!(
                        !self.active,
                        "system: a seed serves the system journals",
                        { "journal" => journal.0, "seed" => target }
                    );
                    assert_reachable!(
                        "system: a system append on a seed without system journals is refused"
                    );
                    return Appended::Unknown;
                }
                Some(ack) if ack.committed => {
                    return ack.first_lsn.map_or(Appended::Ambiguous, Appended::At);
                }
                Some(ack) => {
                    target = ack
                        .leader
                        .and_then(|leader| usize::try_from(leader).ok())
                        .filter(|leader| *leader < self.seeds)
                        .unwrap_or((target + 1) % self.seeds);
                }
                None => target = (target + 1) % self.seeds,
            }
        }
        Appended::Ambiguous
    }

    /// Read `journal` from LSN 0 to a seed's committed end and fold it: every
    /// event with its LSN, and the folds.
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
            let request = Read {
                journal: journal.0,
                from_lsn: from,
                max_bytes: READ_BYTES,
            };
            let ack = within(ctx, self.timeout, None, async {
                client.read(&request).await.ok()
            })
            .await?;
            if ack.unknown_journal || ack.trimmed_to.is_some() {
                return None;
            }
            for entry in ack.entries {
                let event = if journal == DIRECTORY {
                    SystemEvent::Directory(directory.fold(entry.lsn, &entry.records))
                } else {
                    SystemEvent::Registry(registry.fold(entry.lsn, &entry.records))
                };
                events.push((entry.lsn, event));
            }
            if ack.next_lsn <= from || ack.next_lsn >= ack.committed_end {
                return Some((events, directory, registry));
            }
            from = ack.next_lsn;
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
        let Appended::At(lsn) = self
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
            .find(|(at, _)| *at == lsn)
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

    /// One record appended to a journal this client created, at a genesis
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
        let seq = self.next_seq;
        self.next_seq += 1;
        let record = draw.to_le_bytes().to_vec();
        let audit = audit_world_for(ctx.state(), journal);
        audit.note_appended(self.client_id, seq);
        audit.note_submitted(user_command_hash(&encode_records(std::slice::from_ref(
            &record,
        ))));
        let request = Append {
            journal: journal.0,
            client: self.client_id,
            seq,
            records: vec![record],
        };
        let mut target = usize::try_from(draw % genesis.len() as u64).unwrap_or(0);
        for _ in 0..APPEND_ATTEMPTS {
            let client = clients[genesis[target % genesis.len()]].clone();
            let answer = within(ctx, self.timeout, None, async {
                client.append(&request).await.ok()
            })
            .await;
            match answer {
                Some(ack) if ack.committed => {
                    assert_reachable!("system: a created journal commits an append");
                    return;
                }
                Some(ack) => {
                    target = ack
                        .leader
                        .and_then(|leader| genesis.iter().position(|g| *g as u64 == leader))
                        .unwrap_or(target + 1);
                }
                None => target += 1,
            }
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
    /// registry has in the standing it needs (one read back first).
    pub(super) async fn registry_step(
        &mut self,
        ctx: &SimContext,
        clients: &[SimClient],
        standing: Option<NodeStanding>,
        draw: u64,
    ) {
        if self.joiners.is_empty() {
            return;
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
                        return;
                    }
                    SystemCommand::RetireNode { id }
                }
            }
        };
        if let Appended::At(_) = self.append(ctx, clients, REGISTRY, &command, draw).await {
            match command {
                SystemCommand::RegisterNode { .. } => {
                    assert_reachable!("system: a client registers a joiner");
                }
                SystemCommand::DrainNode { .. } => {
                    assert_reachable!("system: a client drains a joiner");
                }
                _ => assert_reachable!("system: a client retires a joiner"),
            }
        }
    }
}
