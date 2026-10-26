//! The operator: the one workload, which runs the action log in order
//! through the library client, the calls `parosctl` makes (`init`,
//! `tenant create`, `journal create`, `write`, `read`). It holds no policy
//! of its own: retries, redirects and claims are `paros::client`'s.
//!
//! After each step it reads where every tenant and journal lives through
//! the admin API only: the fleet directory (`parosctl tenant list`), each
//! tenant's control journal (`parosctl journal list`), and an `Inspect` of
//! every journal on every machine.

use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use async_trait::async_trait;
use moonpool_rpc::{RpcConfig, RpcHandle};
use moonpool_sim::{
    Providers, RandomProvider, SimContext, SimProviders, SimulationError, SimulationResult,
    TimeProvider,
};
use std::collections::BTreeMap;

use paros::client::fleet::{FleetSession, Step, read_directory};
use paros::client::initialize::{InitParams, InitRun, Initialized, initialize};
use paros::client::journals::{self, JournalAnswer, JournalOp, JournalRequest};
use paros::client::{
    ClaimOutcome, Client, ClientTunables, Reader, ReaderOutcome, Server, Writer, WriterOutcome,
};
use paros::model::fleet::TenantKind;
use paros::model::registry::Registry;
use paros::model::tenant::Desired;
use paros::{
    Address, JournalId, JournalIdentifier, Names, NodeClient, NodeId, TenantId, Transport, Value,
    WriterMode,
};
use tokio_util::sync::CancellationToken;

use crate::playground::calls::CallTrace;
use crate::playground::machine::{machine_addr, zone_of};
use crate::playground::model::{
    Action, Config, JournalPlace, Op, Quorum, StepResult, TenantPlace, Topology,
};

/// How long one operator step may take before it gives up.
const PATIENCE: Duration = Duration::from_secs(20);

/// The operator workload.
#[derive(Clone)]
pub(crate) struct Operator {
    config: Config,
    actions: Vec<Action>,
    results: Arc<Mutex<Vec<StepResult>>>,
    topology: Arc<Mutex<Vec<Topology>>>,
}

impl Operator {
    pub(crate) fn new(
        config: Config,
        actions: Vec<Action>,
        results: Arc<Mutex<Vec<StepResult>>>,
        topology: Arc<Mutex<Vec<Topology>>>,
    ) -> Self {
        Self {
            config,
            actions,
            results,
            topology,
        }
    }
}

/// What the operator learned so far.
#[derive(Default)]
struct Known {
    cell: Option<Initialized>,
    fleet: Option<FleetSession>,
    tenants: BTreeMap<String, TenantId>,
    journals: BTreeMap<(String, String), Opened>,
}

/// A journal the operator created, and its writer.
struct Opened {
    id: JournalIdentifier,
    members: Vec<u64>,
    writer: Writer,
    written: u32,
}

#[async_trait]
impl moonpool_sim::Workload for Operator {
    fn name(&self) -> &'static str {
        "operator"
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        // paros's futures are not `Send`: drive them as a local task.
        let operator = self.clone();
        ctx.run_local("operator", |ctx| async move { operator.drive(&ctx).await })
            .await
    }
}

impl Operator {
    /// Play the actions against the machines, recording each step.
    async fn drive(&self, ctx: &SimContext) -> SimulationResult<()> {
        // No fault injection: the shipped BUGGIFY sites are inert, as in
        // production (the machines turn them off too).
        moonpool_sim::buggify_reset();
        let (run, rpc) = Transport::Plaintext
            .client_only(ctx.providers().clone(), rpc_config())
            .map_err(|e| SimulationError::InvalidState(format!("client RPC runtime: {e}")))?;
        let stop = CancellationToken::new();
        let cancelled = stop.clone();
        moonpool_sim::LocalTaskProvider::spawn_local_task(ctx.task(), "operator-rpc", async move {
            moonpool_sim::select! {
                biased;
                () = cancelled.cancelled() => {}
                _ = run => {}
            }
        })
        .detach();
        let _stop = stop.drop_guard();
        let desk = Desk {
            providers: ctx.providers().clone(),
            rpc,
            names: Names::literal(),
            shutdown: ctx.shutdown().clone(),
        };
        let mut known = Known::default();
        for (index, action) in self.actions.iter().enumerate() {
            if !wait_until(ctx, action.at_ms).await {
                return Ok(());
            }
            let start_ms = now_ms(ctx);
            let (ok, detail) = desk.step(&mut known, &action.op).await;
            let end_ms = now_ms(ctx);
            tracing::info!(index, ok, %detail, "operator_step");
            self.results
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(StepResult {
                    index,
                    start_ms,
                    end_ms,
                    ok,
                    detail,
                });
            if let Some((machines, nodes, tenants)) = desk.survey(&self.config, &known).await {
                let topology = Topology {
                    step: index,
                    time_ms: now_ms(ctx),
                    machines,
                    nodes,
                    zones: (0..self.config.machines).map(zone_of).collect(),
                    tenants,
                };
                self.topology
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .push(topology);
            }
        }
        wait_until(ctx, self.config.run_ms).await;
        Ok(())
    }
}

/// What a survey reads: each machine's short and full node id, by index,
/// and the tenants.
type Survey = (Vec<Option<String>>, Vec<Option<String>>, Vec<TenantPlace>);

/// The simulated time, in milliseconds.
fn now_ms(ctx: &SimContext) -> u64 {
    u64::try_from(ctx.time().now().as_millis()).unwrap_or(u64::MAX)
}

/// Sleep until `at_ms`; `false` once the run shuts down.
async fn wait_until(ctx: &SimContext, at_ms: u64) -> bool {
    let now = now_ms(ctx);
    if at_ms > now {
        return ctx
            .time()
            .sleep(Duration::from_millis(at_ms - now))
            .await
            .is_ok();
    }
    true
}

/// The client runtime's shape: the nodes' frame limit, so an `Inspect`
/// reply fits.
fn rpc_config() -> RpcConfig {
    RpcConfig {
        max_frame_bytes: paros::MAX_FRAME_BYTES,
        ..RpcConfig::default()
    }
}

/// What every operator call goes through.
struct Desk {
    providers: SimProviders,
    rpc: RpcHandle<SimProviders>,
    names: Names,
    shutdown: CancellationToken,
}

impl Desk {
    /// The library client over `servers`, each dialed at its address.
    fn client(&self, servers: &[(u64, Address)]) -> Client<SimProviders> {
        let servers = servers
            .iter()
            .map(|(id, addr)| Server {
                id: *id,
                node: NodeClient::named(&self.rpc, self.names.clone(), addr.clone()),
            })
            .collect();
        // A read at the tail answers at once: the sandbox asks for what is there.
        let tunables = ClientTunables {
            wait_ms: 0,
            ..ClientTunables::default()
        };
        Client::new(&self.providers, servers, tunables).with_shutdown(self.shutdown.clone())
    }

    /// A random id that is never 0, as `parosctl` draws one.
    fn nonzero(&self) -> u64 {
        loop {
            let id: u64 = self.providers.random().random();
            if id != 0 {
                return id;
            }
        }
    }

    async fn step(&self, known: &mut Known, op: &Op) -> (bool, String) {
        match op {
            Op::Init { machines } => self.init(*machines, known).await,
            Op::CreateTenant { tenant } => self.create_tenant(known, tenant).await,
            Op::CreateJournal {
                tenant,
                journal,
                mode,
            } => self.create_journal(known, tenant, journal, mode).await,
            Op::Write {
                tenant,
                journal,
                count,
            } => self.write(known, tenant, journal, *count).await,
            Op::Read {
                tenant,
                journal,
                count,
            } => self.read(known, tenant, journal, *count).await,
        }
    }

    /// `parosctl init` over machines `0..machines`.
    async fn init(&self, machines: usize, known: &mut Known) -> (bool, String) {
        if known.cell.is_some() {
            return (false, "the universe is already initialized".into());
        }
        let members: Vec<Address> = (0..machines).map(machine_addr).collect();
        let params = InitParams {
            patience: PATIENCE,
            fleet_id: self.nonzero(),
            leader_seed: u128::from(self.nonzero()),
            names: ("sandbox".into(), "cell-1".into()),
            token: Vec::new(),
            root_key: None,
            failures_per_partition: paros::client::initialize::FAILURES_PER_PARTITION,
            idle: false,
        };
        let run = initialize(
            &self.providers,
            &self.rpc,
            &self.names,
            &members,
            |servers| self.client(servers),
            params,
        )
        .await;
        match run {
            InitRun::Initialized(done) | InitRun::AlreadyInitialized(done) => {
                // The admitted machines serve journals too: the client
                // reaches them as it reaches the founding members.
                let mut done = done;
                let admitted = done.admitted.clone();
                done.servers.extend(admitted);
                let detail = format!(
                    "cell-1 formed over {} machines, {} admitted, coordinator {}",
                    done.members.len(),
                    done.admitted.len(),
                    short(done.coordinator.0)
                );
                known.cell = Some(done);
                (true, detail)
            }
            other => (false, format!("init: {other:?}")),
        }
    }

    /// `parosctl tenant create`.
    async fn create_tenant(&self, known: &mut Known, name: &str) -> (bool, String) {
        let Some(cell) = known.cell.as_ref() else {
            return (false, "no cell: init failed".into());
        };
        if known.tenants.contains_key(name) {
            return (false, format!("tenant {name} exists already"));
        }
        let client = self.client(&cell.servers);
        if known.fleet.is_none() {
            known.fleet = FleetSession::new(
                cell.journals,
                u128::from(self.nonzero()),
                Registry::new(cell.members.iter().copied().map(NodeId)),
                client.tunables().checkpoint_policy(),
            );
        }
        let Some(session) = known.fleet.as_mut() else {
            return (false, "the cell names no fleet journal".into());
        };
        let draws: Vec<JournalIdentifier> = (0..4)
            .map(|_| JournalIdentifier::new(TenantId(self.nonzero()), JournalId(self.nonzero())))
            .collect();
        let run = session
            .create_tenant(&client, 0, name.as_bytes(), draws, PATIENCE)
            .await;
        match run.outcome {
            Step::Done { result, .. } => {
                known.tenants.insert(name.to_owned(), result);
                (true, format!("tenant {name} created in cell-1"))
            }
            other => (false, format!("tenant create: {other:?}")),
        }
    }

    /// `parosctl journal create`: the tenant coordinator places it.
    async fn create_journal(
        &self,
        known: &mut Known,
        tenant: &str,
        name: &str,
        mode: &str,
    ) -> (bool, String) {
        let Ok(desired) = mode.parse::<Desired>() else {
            return (
                false,
                format!("no mode {mode}: failures-per-partition=F,partitions=P"),
            );
        };
        let Some(cell) = known.cell.as_ref() else {
            return (false, "no cell: init failed".into());
        };
        let Some(tenant_id) = known.tenants.get(tenant).copied() else {
            return (false, format!("no tenant {tenant}: create it first"));
        };
        let key = (tenant.to_owned(), name.to_owned());
        if known.journals.contains_key(&key) {
            return (false, format!("journal {tenant}/{name} exists already"));
        }
        let client = self.client(&cell.servers);
        let Some(election) = cell.journals.election else {
            return (false, "the cell names no election journal".into());
        };
        let request = JournalRequest {
            request: self.nonzero(),
            tenant: tenant_id,
            op: JournalOp::Create {
                name: name.as_bytes().to_vec(),
                writer: WriterMode::Single,
                desired: Some(desired),
                tags: paros::model::tags::UserTags::none(),
                store: paros::model::tenant::StoreDurability::Ordered,
            },
        };
        let answer = journals::request(
            &self.providers,
            &self.rpc,
            &self.names,
            &client,
            election,
            &request,
            PATIENCE,
        )
        .await;
        match answer {
            JournalAnswer::Created { id, config } => {
                let id = JournalIdentifier::new(tenant_id, id);
                let members: Vec<u64> = config.members().iter().map(|m| m.0).collect();
                let detail = format!(
                    "journal {tenant}/{name} placed on {} acceptors, {}",
                    members.len(),
                    desired.label()
                );
                let writer = Writer::new(id, u128::from(self.nonzero()));
                known.journals.insert(
                    key,
                    Opened {
                        id,
                        members,
                        writer,
                        written: 0,
                    },
                );
                (true, detail)
            }
            other => (false, format!("journal create: {other:?}")),
        }
    }

    /// The client over a journal's members (all servers when none is known).
    fn journal_client(&self, cell: &Initialized, members: &[u64]) -> Client<SimProviders> {
        let servers: Vec<(u64, Address)> = cell
            .servers
            .iter()
            .filter(|(id, _)| members.contains(id))
            .cloned()
            .collect();
        self.client(if servers.is_empty() {
            &cell.servers
        } else {
            &servers
        })
        .with_observer(Arc::new(CallTrace::default()))
    }

    /// Append `count` records to `tenant`/`journal`, as its one writer.
    async fn write(
        &self,
        known: &mut Known,
        tenant: &str,
        journal: &str,
        count: u32,
    ) -> (bool, String) {
        let Some(cell) = known.cell.as_ref() else {
            return (false, "no cell: init failed".into());
        };
        let Some(opened) = known
            .journals
            .get_mut(&(tenant.to_owned(), journal.to_owned()))
        else {
            return (
                false,
                format!("no journal {tenant}/{journal}: create it first"),
            );
        };
        let client = self.journal_client(cell, &opened.members);
        // A journal just created opens on its members a moment later: until
        // then a claim reads nothing, and the operator asks again.
        let deadline = client.now() + PATIENCE;
        let mut first = None;
        let mut last = 0;
        let mut done = 0;
        while done < count {
            if opened.writer.owned().is_none() {
                match opened.writer.claim(&client, 0, false).await {
                    ClaimOutcome::Won { .. } | ClaimOutcome::Owned { .. } => {}
                    // A claim whose answer was lost is asked again: the
                    // read before it adopts a claim that already won.
                    ClaimOutcome::Unread
                    | ClaimOutcome::UnknownJournal
                    | ClaimOutcome::Ambiguous
                    | ClaimOutcome::Redirect { .. }
                        if client.now() < deadline =>
                    {
                        let pause = client.tunables().retry_backoff;
                        if self.providers.time().sleep(pause).await.is_err() {
                            return (false, "stopped".into());
                        }
                        continue;
                    }
                    other => return (false, format!("claim: {other:?}")),
                }
            }
            // One entry per record: a retry after an unsettled answer is the
            // identical write, which the journal answers as a duplicate.
            let value = format!("{journal}-{}", opened.written + 1);
            let Some(entry) = opened.writer.entry(vec![Value(value.into_bytes())]) else {
                return (false, "write: the writer leads no term".into());
            };
            loop {
                match opened.writer.write_entry(&client, &entry, 0).await {
                    WriterOutcome::Written { seq, .. } => {
                        opened.written += 1;
                        done += 1;
                        first.get_or_insert(seq);
                        last = seq;
                        break;
                    }
                    WriterOutcome::Ambiguous | WriterOutcome::Unavailable { .. }
                        if client.now() < deadline =>
                    {
                        let pause = client.tunables().retry_backoff;
                        if self.providers.time().sleep(pause).await.is_err() {
                            return (false, "stopped".into());
                        }
                    }
                    WriterOutcome::NotOwner
                    | WriterOutcome::NotWritten { .. }
                    | WriterOutcome::Superseded { .. }
                        if client.now() < deadline =>
                    {
                        break;
                    }
                    other => return (false, format!("write: {other:?}")),
                }
            }
        }
        match first {
            Some(first) => (
                true,
                format!("{count} records chosen at positions {first}..={last}"),
            ),
            None => (true, "nothing to write".into()),
        }
    }

    /// Read `count` records of `tenant`/`journal` from its first position.
    async fn read(&self, known: &Known, tenant: &str, journal: &str, count: u32) -> (bool, String) {
        let Some(cell) = known.cell.as_ref() else {
            return (false, "no cell: init failed".into());
        };
        let Some(opened) = known.journals.get(&(tenant.to_owned(), journal.to_owned())) else {
            return (
                false,
                format!("no journal {tenant}/{journal}: create it first"),
            );
        };
        let client = self.journal_client(cell, &opened.members);
        let mut reader = Reader::new(opened.id, 0);
        let deadline = client.now() + PATIENCE;
        let want = usize::try_from(count).unwrap_or(usize::MAX);
        let mut records = Vec::new();
        let mut from = None;
        while records.len() < want {
            match reader.next(&client, 0).await {
                ReaderOutcome::Records {
                    from: at,
                    records: page,
                    ..
                } => {
                    if page.is_empty() {
                        break;
                    }
                    from.get_or_insert(at);
                    records.extend(page);
                }
                ReaderOutcome::Gap { .. } => {}
                other if client.now() >= deadline => return (false, format!("read: {other:?}")),
                _ => {
                    let pause = client.tunables().retry_backoff;
                    if self.providers.time().sleep(pause).await.is_err() {
                        return (false, "stopped".into());
                    }
                }
            }
        }
        records.truncate(want);
        let shown: Vec<String> = records
            .iter()
            .take(8)
            .map(|r| String::from_utf8_lossy(r).into_owned())
            .collect();
        let more = if records.len() > shown.len() {
            ", …"
        } else {
            ""
        };
        (
            true,
            format!(
                "{} records from position {}: {}{more}",
                records.len(),
                from.unwrap_or(0),
                shown.join(", ")
            ),
        )
    }

    /// Where every tenant and journal lives, through the admin API.
    async fn survey(&self, config: &Config, known: &Known) -> Option<Survey> {
        let cell = known.cell.as_ref()?;
        // Server `i` of the client is machine `index[i]`.
        let index: Vec<usize> = cell
            .servers
            .iter()
            .map(|(_, addr)| {
                (0..config.machines)
                    .find(|m| machine_addr(*m) == *addr)
                    .unwrap_or(usize::MAX)
            })
            .collect();
        let mut machines = vec![None; config.machines];
        let mut nodes = vec![None; config.machines];
        let mut by_node = BTreeMap::new();
        for ((id, _), m) in cell.servers.iter().zip(&index) {
            if let (Some(slot), Some(node)) = (machines.get_mut(*m), nodes.get_mut(*m)) {
                *slot = Some(short(*id));
                *node = Some(id.to_string());
                by_node.insert(*id, *m);
            }
        }
        let client = self.client(&cell.servers);
        let directory = read_directory(&client, 0, cell.journals.fleet?)
            .await
            .ok()?;
        let mut tenants = Vec::new();
        for (tenant, entry) in directory.tenants() {
            if directory.is_removed(tenant) {
                continue;
            }
            let control = JournalIdentifier::new(tenant, entry.control);
            let mut list = vec![("control".to_owned(), control)];
            if entry.kind == TenantKind::Users
                && let Some(state) = journals::list(&client, tenant, entry.control).await
            {
                let mut live: Vec<(String, JournalIdentifier)> = state
                    .live()
                    .map(|(id, journal)| {
                        let name = String::from_utf8_lossy(&journal.name).into_owned();
                        (name, JournalIdentifier::new(tenant, id))
                    })
                    .collect();
                live.sort();
                list.extend(live);
            }
            if let Some(election) = cell.journals.election.filter(|e| e.tenant == tenant) {
                list.push(("election".to_owned(), election));
            }
            let mut journals = Vec::new();
            for (name, id) in list {
                journals.push(self.place(&client, &index, &by_node, name, id).await);
            }
            let name = String::from_utf8_lossy(&entry.name).into_owned();
            tenants.push(TenantPlace {
                name: if name.is_empty() {
                    entry.kind.as_str().to_owned()
                } else {
                    name
                },
                kind: entry.kind.as_str().to_owned(),
                cell: String::from_utf8_lossy(directory.cell_name(entry.cell_id)).into_owned(),
                journals,
            });
        }
        tenants.sort_by_key(|t| (t.kind == "users", t.name.clone()));
        Some((machines, nodes, tenants))
    }

    /// One journal, as every machine's `Inspect` answers for it.
    async fn place(
        &self,
        client: &Client<SimProviders>,
        index: &[usize],
        by_node: &BTreeMap<u64, usize>,
        name: String,
        journal: JournalIdentifier,
    ) -> JournalPlace {
        let machines = |nodes: &[u64]| -> Vec<usize> {
            let mut out: Vec<usize> = nodes
                .iter()
                .filter_map(|n| by_node.get(n).copied())
                .collect();
            out.sort_unstable();
            out
        };
        let mut place = JournalPlace {
            name,
            key: format!("{}:{}", journal.tenant.0, journal.journal.0),
            members: Vec::new(),
            quorum: Quorum::default(),
            leader: None,
            matchmakers: Vec::new(),
            hosts: Vec::new(),
            chosen: None,
        };
        for (server, machine) in index.iter().enumerate() {
            let Some(reply) = client.inspect(server, journal).await else {
                continue;
            };
            place.hosts.push(*machine);
            if reply.leader || place.members.is_empty() {
                // Membership order, not machine order: a grid's rows are
                // consecutive runs of it.
                place.members = reply
                    .members
                    .iter()
                    .filter_map(|n| by_node.get(n).copied())
                    .collect();
                place.quorum = Quorum {
                    system: match reply.quorum_system {
                        1 => "flexible",
                        2 => "grid",
                        _ => "majority",
                    }
                    .into(),
                    phase1: reply.phase1_quorum,
                    phase2: reply.phase2_quorum,
                    rows: reply.rows,
                    cols: reply.cols,
                };
                place.matchmakers = machines(&reply.matchmakers);
            }
            if reply.leader {
                place.leader = Some(*machine);
                place.chosen = reply.chosen_index;
            }
        }
        place.hosts.sort_unstable();
        place
    }
}

/// A node id as people read it: its first six hex digits.
fn short(id: u64) -> String {
    format!("{id:016x}")[..6].to_string()
}
