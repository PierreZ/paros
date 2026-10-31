//! The operator: the one workload, which runs the steps it is given, in
//! order, through the library client, the calls `parosctl` makes (`init`,
//! `tenant create`, `journal create`, `cell add-machine`, `write`, `read`),
//! and traffic: writes to every journal at the same time. It holds no policy
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
use std::collections::{BTreeMap, BTreeSet};

use paros::client::cell::CellSession;
use paros::client::cell::Removal;
use paros::client::fleet::{CellJoin, FleetSession, Step, read_directory};
use paros::client::initialize::{InitParams, InitRun, Initialized, initialize};
use paros::client::journals::{self, JournalAnswer, JournalOp, JournalRequest};
use paros::client::multi::append_request;
use paros::client::resolve::{Resolution, resolve};
use paros::client::{
    ClaimOutcome, Client, ClientTunables, Reader, ReaderOutcome, Server, WriteOptions,
    WriteOutcome, Writer, WriterOutcome,
};
use paros::core::NodeId;
use paros::core::Value;
use paros::core::journal::JournalId;
use paros::core::journal::JournalIdentifier;
use paros::core::journal::TenantId;
use paros::core::journal::WriterMode;
use paros::model::address::Address;
use paros::model::address::Names;
use paros::model::fleet::TenantKind;
use paros::model::registry::MaintenanceScope;
use paros::model::registry::Registry;
use paros::model::tenant::Desired;
use paros::net::NodeClient;
use paros::net::transport::Transport;
use tokio_util::sync::CancellationToken;

use crate::playground::calls::CallTrace;
use crate::playground::faults::Faults;
use crate::playground::machine::{machine_addr, machine_index, zone_of};
use crate::playground::model::{
    AcceptorView, BallotView, CellView, Config, JournalPlace, Op, Quorum, StepResult, TenantPlace,
    Topology,
};

/// How long one operator step may take before it gives up.
const PATIENCE: Duration = Duration::from_secs(20);

/// How often an idle operator looks for a new step.
const POLL: Duration = Duration::from_millis(20);

/// How often an idle operator reads where every journal lives again, so
/// the page sees a new leader or a re-placement after a fault.
const SURVEY_EVERY: Duration = Duration::from_secs(1);

/// How often a started writer appends one record.
const STREAM_EVERY: Duration = Duration::from_millis(200);

/// How long a started writer's record may take before it is dropped: a
/// writer that cannot write must not hold the next step back long.
const STREAM_PATIENCE: Duration = Duration::from_secs(2);

/// The steps given to the operator, in order, and whether it must stop.
#[derive(Debug, Default)]
pub(crate) struct Inbox {
    /// The steps not yet started, each with its index in the log.
    pub(crate) queue: std::collections::VecDeque<(usize, Op)>,
    /// Stop once the queue is empty.
    pub(crate) stop: bool,
}

/// What the operator reports, shared with the caller.
#[derive(Clone, Default)]
pub(crate) struct Reports {
    pub(crate) inbox: Arc<Mutex<Inbox>>,
    pub(crate) results: Arc<Mutex<Vec<StepResult>>>,
    pub(crate) topology: Arc<Mutex<Vec<Topology>>>,
}

/// The operator workload.
#[derive(Clone)]
pub(crate) struct Operator {
    config: Config,
    reports: Reports,
    faults: Faults,
}

impl Operator {
    pub(crate) fn new(config: Config, reports: Reports, faults: Faults) -> Self {
        Self {
            config,
            reports,
            faults,
        }
    }

    /// The next step, or `None` with whether to stop.
    fn next(&self) -> Result<(usize, Op), bool> {
        let mut inbox = self
            .reports
            .inbox
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let stop = inbox.stop;
        inbox.queue.pop_front().ok_or(stop)
    }

    /// The machines faults hold down and cut off now, by index.
    fn faulted(&self) -> (Vec<usize>, Vec<usize>) {
        let state = self.faults.lock().unwrap_or_else(PoisonError::into_inner);
        (
            state.down.iter().copied().collect(),
            state.isolated.iter().copied().collect(),
        )
    }
}

/// What the operator learned so far.
#[derive(Default)]
struct Known {
    /// `cell-1`, which `init` forms and which hosts the universe.
    cell: Option<Initialized>,
    /// `cell-2`, once `add_cell` joined it.
    cell2: Option<Initialized>,
    /// Each tenant, with the cell it lives in (0 is `cell-1`).
    tenants: BTreeMap<String, (TenantId, usize)>,
    journals: BTreeMap<(String, String), Opened>,
    /// The journals a writer appends to between the other steps.
    streams: BTreeSet<(String, String)>,
}

impl Known {
    /// Cell `at`: 0 is `cell-1`, 1 is `cell-2`.
    fn cell_at(&self, at: usize) -> Option<&Initialized> {
        match at {
            0 => self.cell.as_ref(),
            _ => self.cell2.as_ref(),
        }
    }

    /// Every server of every cell, `cell-1`'s first.
    fn servers(&self) -> Vec<(u64, Address)> {
        self.cell
            .iter()
            .chain(&self.cell2)
            .flat_map(|cell| cell.servers.iter().cloned())
            .collect()
    }
}

/// A journal the operator created, and its writer.
struct Opened {
    id: JournalIdentifier,
    members: Vec<u64>,
    /// A multi-writer journal: no leader uuid, no claim.
    multi: bool,
    writer: Writer,
    written: u32,
    /// The last position a write of this operator landed at.
    last: Option<u64>,
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
        self.faults
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .client_ip = Some(ctx.my_ip().to_owned());
        let (run, rpc) = Transport::Plaintext
            .client_only(ctx.providers().clone(), rpc_config())
            .map_err(|e| SimulationError::InvalidState(format!("client RPC runtime: {e}")))?;
        let rpc_stop = CancellationToken::new();
        let cancelled = rpc_stop.clone();
        moonpool_sim::LocalTaskProvider::spawn_local_task(ctx.task(), "operator-rpc", async move {
            moonpool_sim::select! {
                biased;
                () = cancelled.cancelled() => {}
                _ = run => {}
            }
        })
        .detach();
        let _rpc_stop = rpc_stop.drop_guard();
        let desk = Desk {
            providers: ctx.providers().clone(),
            rpc,
            names: Names::literal(),
            shutdown: ctx.shutdown().clone(),
        };
        let mut known = Known::default();
        let mut last = None;
        let mut next_survey = Duration::ZERO;
        let mut next_stream = Duration::ZERO;
        loop {
            // A fault played since the last look: read where everything
            // lives after it, before the next step.
            let played = self
                .faults
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .played
                .take();
            if let Some(index) = played {
                last = Some(last.map_or(index, |l: usize| l.max(index)));
                self.record_survey(ctx, &desk, &known, index).await;
                next_survey = ctx.time().now() + SURVEY_EVERY;
            }
            let (index, op) = match self.next() {
                Ok(step) => step,
                Err(true) => return Ok(()),
                Err(false) => {
                    let now = ctx.time().now();
                    if !known.streams.is_empty() && now >= next_stream {
                        next_stream = now + STREAM_EVERY;
                        desk.stream(&mut known).await;
                    }
                    if known.cell.is_some() && ctx.time().now() >= next_survey {
                        next_survey = ctx.time().now() + SURVEY_EVERY;
                        self.record_survey(ctx, &desk, &known, last.unwrap_or(0))
                            .await;
                    }
                    if ctx.time().sleep(POLL).await.is_err() {
                        return Ok(());
                    }
                    continue;
                }
            };
            let start_ms = now_ms(ctx);
            let (ok, detail) = desk.step(&mut known, &op).await;
            let end_ms = now_ms(ctx);
            tracing::info!(index, ok, %detail, "operator_step");
            self.reports
                .results
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(StepResult {
                    index,
                    start_ms,
                    end_ms,
                    ok,
                    detail,
                });
            last = Some(index);
            self.record_survey(ctx, &desk, &known, index).await;
            next_survey = ctx.time().now() + SURVEY_EVERY;
        }
    }

    /// Read where every tenant and journal lives now, and record it after
    /// step `step`.
    async fn record_survey(&self, ctx: &SimContext, desk: &Desk, known: &Known, step: usize) {
        let (down, isolated) = self.faulted();
        let silent: Vec<usize> = down.iter().chain(&isolated).copied().collect();
        if let Some((machines, nodes, cells, tenants)) =
            desk.survey(&self.config, known, &silent).await
        {
            let topology = Topology {
                step,
                time_ms: now_ms(ctx),
                machines,
                nodes,
                zones: (0..self.config.machines).map(zone_of).collect(),
                down,
                isolated,
                cells,
                tenants,
            };
            self.reports
                .topology
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(topology);
        }
    }
}

/// What a survey reads: each machine's short and full node id, by index,
/// the cells and the tenants.
type Survey = (
    Vec<Option<String>>,
    Vec<Option<String>>,
    Vec<CellView>,
    Vec<TenantPlace>,
);

/// The simulated time, in milliseconds.
fn now_ms(ctx: &SimContext) -> u64 {
    u64::try_from(ctx.time().now().as_millis()).unwrap_or(u64::MAX)
}

/// The client runtime's shape: the nodes' frame limit, so an `Inspect`
/// reply fits.
fn rpc_config() -> RpcConfig {
    RpcConfig {
        max_frame_bytes: paros::tunables::MAX_FRAME_BYTES,
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
            Op::CreateTenant { tenant, cell } => {
                self.create_tenant(known, tenant, cell.as_deref()).await
            }
            Op::CreateJournal {
                tenant,
                journal,
                mode,
                multi,
            } => {
                self.create_journal(known, tenant, journal, mode, *multi)
                    .await
            }
            Op::Configure {
                tenant,
                journal,
                mode,
            } => self.configure(known, tenant, journal, mode).await,
            Op::SetLeader { tenant, journal } => self.set_leader(known, tenant, journal).await,
            Op::Stream {
                tenant,
                journal,
                on,
            } => stream_switch(known, tenant, journal, *on),
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
            Op::Traffic { count } => self.traffic(known, *count).await,
            Op::AddMachine { machine } => self.add_machine(known, *machine).await,
            Op::Drain { machine } | Op::Remove { machine } | Op::Maintenance { machine, .. } => {
                self.cell_admin(known, op, *machine).await
            }
            Op::AddCell { machines } => self.add_cell(known, machines).await,
            Op::Resolve { tenant } => self.resolve(known, tenant).await,
            // The fault injector plays faults: they never reach the operator.
            Op::Crash { .. } | Op::Restart { .. } | Op::Isolate { .. } | Op::Heal { .. } => {
                (false, "a fault is no operator step".into())
            }
        }
    }

    /// `parosctl init` over machines `0..machines`.
    async fn init(&self, machines: usize, known: &mut Known) -> (bool, String) {
        if known.cell.is_some() {
            return (false, "the universe is already initialized".into());
        }
        let members: Vec<Address> = (0..machines).map(machine_addr).collect();
        match self.form(&members, "cell-1", false).await {
            Ok(done) => {
                let detail = format!(
                    "cell-1 formed over {} machines, {} admitted, coordinator {}",
                    done.members.len(),
                    done.admitted.len(),
                    short(done.coordinator.0)
                );
                known.cell = Some(done);
                (true, detail)
            }
            Err(detail) => (false, detail),
        }
    }

    /// `parosctl init` over `members`: a cell named `name`, idle (no
    /// universe) when `idle`.
    async fn form(
        &self,
        members: &[Address],
        name: &str,
        idle: bool,
    ) -> Result<Initialized, String> {
        let params = InitParams {
            patience: PATIENCE,
            fleet_id: self.nonzero(),
            leader_seed: u128::from(self.nonzero()),
            names: ("sandbox".into(), name.into()),
            token: Vec::new(),
            root_key: None,
            survive_machines: paros::client::initialize::SURVIVE_MACHINES,
            idle,
        };
        let run = initialize(
            &self.providers,
            &self.rpc,
            &self.names,
            members,
            |servers| self.client(servers),
            params,
        )
        .await;
        match run {
            InitRun::Initialized(mut done) | InitRun::AlreadyInitialized(mut done) => {
                // The admitted machines serve journals too: the client
                // reaches them as it reaches the founding members.
                let admitted = done.admitted.clone();
                done.servers.extend(admitted);
                Ok(done)
            }
            other => Err(format!("init: {other:?}")),
        }
    }

    /// A fleet session over `cell-1`, fresh for each command as
    /// `parosctl` opens one: it claims the fleet journal under a new uuid.
    fn fleet(&self, known: &Known) -> Result<FleetSession, String> {
        let Some(cell) = known.cell.as_ref() else {
            return Err("no cell: run init first".into());
        };
        let policy = self.client(&cell.servers).tunables().checkpoint_policy();
        FleetSession::new(
            cell.journals,
            u128::from(self.nonzero()),
            Registry::new(cell.members.iter().copied().map(NodeId)),
            policy,
        )
        .ok_or_else(|| "the cell names no fleet journal".to_string())
    }

    /// `parosctl tenant create [--cell <cell>]`.
    async fn create_tenant(
        &self,
        known: &mut Known,
        name: &str,
        cell: Option<&str>,
    ) -> (bool, String) {
        if known.tenants.contains_key(name) {
            return (false, format!("tenant {name} exists already"));
        }
        let at = match cell {
            None | Some("cell-1") => 0,
            Some("cell-2") => 1,
            Some(other) => return (false, format!("no cell {other}")),
        };
        let Some(cell_id) = known.cell_at(at).map(|cell| cell.journals.cell_id) else {
            return (false, "no such cell yet: add it first".into());
        };
        let client = self.client(&known.servers());
        let mut session = match self.fleet(known) {
            Ok(session) => session.with_cell(cell_id),
            Err(detail) => return (false, detail),
        };
        let draws: Vec<JournalIdentifier> = (0..4)
            .map(|_| JournalIdentifier::new(TenantId(self.nonzero()), JournalId(self.nonzero())))
            .collect();
        let run = session
            .create_tenant(&client, 0, name.as_bytes(), draws, PATIENCE)
            .await;
        match run.outcome {
            Step::Done { result, .. } => {
                known.tenants.insert(name.to_owned(), (result, at));
                (true, format!("tenant {name} created in cell-{}", at + 1))
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
        multi: bool,
    ) -> (bool, String) {
        let Ok(desired) = mode.parse::<Desired>() else {
            return (false, format!("no mode {mode}: machines=F,partitions=P"));
        };
        let Some((tenant_id, at)) = known.tenants.get(tenant).copied() else {
            return (false, format!("no tenant {tenant}: create it first"));
        };
        let key = (tenant.to_owned(), name.to_owned());
        if known.journals.contains_key(&key) {
            return (false, format!("journal {tenant}/{name} exists already"));
        }
        let op = JournalOp::Create {
            name: name.as_bytes().to_vec(),
            writer: if multi {
                WriterMode::Multi
            } else {
                WriterMode::Single
            },
            desired: Some(desired),
            tags: paros::model::tags::UserTags::none(),
            store: paros::model::tenant::StoreDurability::Ordered,
        };
        match self.journal_request(known, at, tenant_id, op).await {
            Ok(JournalAnswer::Created { id, config }) => {
                let id = JournalIdentifier::new(tenant_id, id);
                let members: Vec<u64> = config.members().iter().map(|m| m.0).collect();
                let detail = format!(
                    "{} journal {tenant}/{name} placed on {} acceptors, {}",
                    if multi {
                        "multi-writer"
                    } else {
                        "single-writer"
                    },
                    members.len(),
                    desired.label()
                );
                let writer = Writer::new(id, u128::from(self.nonzero()));
                known.journals.insert(
                    key,
                    Opened {
                        id,
                        members,
                        multi,
                        writer,
                        written: 0,
                        last: None,
                    },
                );
                (true, detail)
            }
            Ok(other) => (false, format!("journal create: {other:?}")),
            Err(detail) => (false, detail),
        }
    }

    /// `parosctl configure`: record a new desired mode for a journal.
    async fn configure(
        &self,
        known: &mut Known,
        tenant: &str,
        name: &str,
        mode: &str,
    ) -> (bool, String) {
        let Ok(desired) = mode.parse::<Desired>() else {
            return (false, format!("no mode {mode}"));
        };
        let Some((tenant_id, at)) = known.tenants.get(tenant).copied() else {
            return (false, format!("no tenant {tenant}: create it first"));
        };
        let op = JournalOp::Configure {
            name: Some(name.as_bytes().to_vec()),
            desired,
            roles: None,
        };
        match self.journal_request(known, at, tenant_id, op).await {
            Ok(JournalAnswer::Configured { .. }) => (
                true,
                format!(
                    "configured {tenant}/{name}: {}; the tenant coordinator reconfigures it",
                    desired.label()
                ),
            ),
            Ok(other) => (false, format!("configure: {other:?}")),
            Err(detail) => (false, detail),
        }
    }

    /// One request to the tenant coordinator of cell `at`.
    async fn journal_request(
        &self,
        known: &Known,
        at: usize,
        tenant: TenantId,
        op: JournalOp,
    ) -> Result<JournalAnswer, String> {
        let cell = known
            .cell_at(at)
            .ok_or_else(|| "the tenant's cell is gone".to_string())?;
        let election = cell
            .journals
            .election
            .ok_or_else(|| "the cell names no election journal".to_string())?;
        let client = self.client(&cell.servers);
        let request = JournalRequest {
            request: self.nonzero(),
            tenant,
            op,
        };
        Ok(journals::request(
            &self.providers,
            &self.rpc,
            &self.names,
            &client,
            election,
            &request,
            PATIENCE,
        )
        .await)
    }

    /// `parosctl set-leader`: a new writer claims the journal with a fresh
    /// uuid, then the writer it replaced tries one write.
    async fn set_leader(&self, known: &mut Known, tenant: &str, journal: &str) -> (bool, String) {
        let servers = known.servers();
        let Some(opened) = known
            .journals
            .get_mut(&(tenant.to_owned(), journal.to_owned()))
        else {
            return (false, format!("no journal {tenant}/{journal}"));
        };
        if opened.multi {
            return (
                false,
                "a multi-writer journal has no leader to replace".into(),
            );
        }
        let client = self.journal_client(&servers, &opened.members);
        let mut writer = Writer::new(opened.id, u128::from(self.nonzero()));
        if let Err(detail) = self.claim(&client, &mut writer).await {
            return (false, detail);
        }
        let new = short_uuid(writer.uuid());
        let mut old = std::mem::replace(&mut opened.writer, writer);
        let tried = match old.entry(vec![Value(b"stale".to_vec())]) {
            Some(entry) => format!("{:?}", old.write_entry(&client, &entry, 0).await),
            None => "never sent: it knows it leads no term".into(),
        };
        let refused = !tried.starts_with("Written");
        (
            refused,
            format!(
                "writer {new} leads {tenant}/{journal} now; the old writer's write: {}",
                if refused {
                    format!("refused ({})", first_word(&tried))
                } else {
                    "landed".into()
                }
            ),
        )
    }

    /// Claim the journal for `writer`, asking again while it opens.
    async fn claim(
        &self,
        client: &Client<SimProviders>,
        writer: &mut Writer,
    ) -> Result<(), String> {
        let deadline = client.now() + PATIENCE;
        loop {
            match writer.claim(client, 0, false).await {
                ClaimOutcome::Won { .. } | ClaimOutcome::Owned { .. } => return Ok(()),
                // A claim whose answer was lost is asked again: the read
                // before it adopts a claim that already won.
                ClaimOutcome::Unread
                | ClaimOutcome::UnknownJournal
                | ClaimOutcome::Ambiguous
                | ClaimOutcome::Redirect { .. }
                    if client.now() < deadline =>
                {
                    let pause = client.tunables().retry_backoff;
                    if self.providers.time().sleep(pause).await.is_err() {
                        return Err("stopped".into());
                    }
                }
                other => return Err(format!("claim: {other:?}")),
            }
        }
    }

    /// The client over a journal's members (all servers when none is known).
    fn journal_client(&self, all: &[(u64, Address)], members: &[u64]) -> Client<SimProviders> {
        let servers: Vec<(u64, Address)> = all
            .iter()
            .filter(|(id, _)| members.contains(id))
            .cloned()
            .collect();
        self.client(if servers.is_empty() { all } else { &servers })
            .with_observer(Arc::new(CallTrace::default()))
    }

    /// Append `count` records to `tenant`/`journal`.
    async fn write(
        &self,
        known: &mut Known,
        tenant: &str,
        journal: &str,
        count: u32,
    ) -> (bool, String) {
        let servers = known.servers();
        let Some(opened) = known
            .journals
            .get_mut(&(tenant.to_owned(), journal.to_owned()))
        else {
            return (
                false,
                format!("no journal {tenant}/{journal}: create it first"),
            );
        };
        self.write_to(&servers, opened, journal, count, PATIENCE)
            .await
    }

    /// Traffic: `count` records to every journal the operator created, all
    /// the journals at the same time.
    async fn traffic(&self, known: &mut Known, count: u32) -> (bool, String) {
        if known.journals.is_empty() {
            return (false, "no journal: create one first".into());
        }
        let servers = known.servers();
        let servers = &servers;
        let writes = known
            .journals
            .iter_mut()
            .map(|((tenant, journal), opened)| async move {
                let (ok, detail) = self
                    .write_to(servers, opened, journal, count, PATIENCE)
                    .await;
                (ok, format!("{tenant}/{journal}: {detail}"))
            });
        let answers = futures::future::join_all(writes).await;
        let done = answers.iter().filter(|(ok, _)| *ok).count();
        let failed: Vec<&str> = answers
            .iter()
            .filter(|(ok, _)| !ok)
            .map(|(_, detail)| detail.as_str())
            .collect();
        let mut detail = format!(
            "{count} records to each of {} journals at the same time, {done} done",
            answers.len()
        );
        if !failed.is_empty() {
            detail.push_str("; ");
            detail.push_str(&failed.join("; "));
        }
        (failed.is_empty(), detail)
    }

    /// One record to every journal a writer was started on, all at the
    /// same time. A record that does not land soon is dropped.
    async fn stream(&self, known: &mut Known) {
        let servers = known.servers();
        let servers = &servers;
        let streams = &known.streams;
        let writes = known
            .journals
            .iter_mut()
            .filter(|(key, _)| streams.contains(*key))
            .map(|((_, journal), opened)| async move {
                self.write_to(servers, opened, journal, 1, STREAM_PATIENCE)
                    .await
            });
        futures::future::join_all(writes).await;
    }

    /// `parosctl cell add-machine`: admit idle machine `machine` into
    /// `cell-1`. The client then reaches it as it reaches the other members.
    async fn add_machine(&self, known: &mut Known, machine: usize) -> (bool, String) {
        let target = machine_addr(machine);
        if known.servers().iter().any(|(_, addr)| *addr == target) {
            return (false, format!("machine-{machine} is in a cell already"));
        }
        let Some((client, mut session)) = self.admin(known) else {
            return (false, "no cell: run init first".into());
        };
        let run = session
            .add_machine(
                &self.providers,
                &self.rpc,
                &client,
                &self.names,
                0,
                &target,
                PATIENCE,
            )
            .await;
        match run.outcome {
            Step::Done { result, .. } => {
                if let Some(cell) = known.cell.as_mut() {
                    cell.servers.push((result.0, target));
                }
                (
                    true,
                    format!(
                        "machine-{machine} admitted into cell-1 as node {}",
                        short(result.0)
                    ),
                )
            }
            other => (false, format!("add machine: {other:?}")),
        }
    }

    /// The client over `cell-1` and a session that writes its control
    /// journal, fresh for each command as `parosctl` opens one.
    fn admin(&self, known: &Known) -> Option<(Client<SimProviders>, CellSession)> {
        let cell = known.cell.as_ref()?;
        let client = self.client(&cell.servers);
        let members: Vec<NodeId> = cell.members.iter().copied().map(NodeId).collect();
        let founders: Vec<(NodeId, Address)> = cell
            .servers
            .iter()
            .filter(|(id, _)| cell.members.contains(id))
            .map(|(id, addr)| (NodeId(*id), addr.clone()))
            .collect();
        let session = CellSession::new(
            cell.journals,
            &members,
            founders,
            u128::from(self.nonzero()),
            client.tunables().checkpoint_policy(),
        );
        Some((client, session))
    }

    /// `parosctl cell drain-machine`, `remove-machine` and `maintenance`
    /// on a machine of `cell-1`.
    async fn cell_admin(&self, known: &mut Known, op: &Op, machine: usize) -> (bool, String) {
        let target = machine_addr(machine);
        let Some(node) = known
            .cell
            .as_ref()
            .and_then(|cell| cell.servers.iter().find(|(_, addr)| *addr == target))
            .map(|(id, _)| NodeId(*id))
        else {
            return (false, format!("machine-{machine} is not in cell-1"));
        };
        let Some((client, mut session)) = self.admin(known) else {
            return (false, "no cell: run init first".into());
        };
        match op {
            Op::Drain { .. } => match session.drain(&client, 0, node).await {
                Ok(true) => (
                    true,
                    format!("machine-{machine} drains: the cell moves its slots off it"),
                ),
                Ok(false) => (true, format!("machine-{machine} drains already")),
                Err(stop) => (false, format!("drain: {stop:?}")),
            },
            Op::Remove { .. } => match session.remove(&client, 0, node).await {
                Ok(Removal::Removed | Removal::Unchanged) => {
                    if let Some(cell) = known.cell.as_mut() {
                        cell.servers.retain(|(id, _)| *id != node.0);
                    }
                    (
                        true,
                        format!("machine-{machine} retired: its node id never comes back"),
                    )
                }
                Ok(Removal::NotDraining) => (
                    false,
                    format!("machine-{machine} is not draining: drain it first"),
                ),
                Ok(Removal::NotDrained { slots }) => (
                    false,
                    format!("machine-{machine} still holds {slots} slots: wait for the drain"),
                ),
                Ok(Removal::Unknown) => (false, format!("machine-{machine} is not registered")),
                Err(stop) => (false, format!("remove: {stop:?}")),
            },
            Op::Maintenance { for_ms, .. } => {
                let scope = MaintenanceScope::Machine(node);
                match session.maintenance(&client, 0, scope, *for_ms).await {
                    Ok(true) if *for_ms == 0 => {
                        (true, format!("machine-{machine}: maintenance ended"))
                    }
                    Ok(true) => (
                        true,
                        format!(
                            "machine-{machine} in maintenance for {} s: the cell does not re-place its slots",
                            for_ms / 1000
                        ),
                    ),
                    Ok(false) => (false, format!("machine-{machine} is not in the cell")),
                    Err(stop) => (false, format!("maintenance: {stop:?}")),
                }
            }
            _ => (false, "no cell command".into()),
        }
    }

    /// `parosctl init --idle` over `machines`, then `parosctl universe
    /// add-cell`: `cell-2` joins the universe.
    async fn add_cell(&self, known: &mut Known, machines: &[usize]) -> (bool, String) {
        if known.cell2.is_some() {
            return (false, "cell-2 exists already".into());
        }
        let members: Vec<Address> = machines.iter().map(|m| machine_addr(*m)).collect();
        if known
            .servers()
            .iter()
            .any(|(_, addr)| members.contains(addr))
        {
            return (false, "a founding machine is in a cell already".into());
        }
        let idle = match self.form(&members, "cell-2", true).await {
            Ok(idle) => idle,
            Err(detail) => return (false, detail),
        };
        let joining = CellJoin {
            journals: idle.journals,
            endpoint: idle
                .servers
                .iter()
                .filter(|(id, _)| idle.members.contains(id))
                .cloned()
                .collect(),
            name: b"cell-2".to_vec(),
        };
        let client = self.client(&known.servers());
        let mut session = match self.fleet(known) {
            Ok(session) => session,
            Err(detail) => return (false, detail),
        };
        let run = session
            .add_cell(
                &self.providers,
                &self.rpc,
                &self.names,
                &client,
                0,
                &joining,
                PATIENCE,
            )
            .await;
        match run.outcome {
            Step::Done { .. } => {
                let detail = format!(
                    "cell-2 formed idle over {} machines, then joined the universe",
                    idle.members.len()
                );
                known.cell2 = Some(idle);
                (true, detail)
            }
            other => (false, format!("add cell: {other:?}")),
        }
    }

    /// `parosctl resolve <tenant>`: ask `cell-1`, as a client does first.
    async fn resolve(&self, known: &Known, tenant: &str) -> (bool, String) {
        let Some(cell) = known.cell.as_ref() else {
            return (false, "no cell: run init first".into());
        };
        let entry: Vec<Address> = cell.servers.iter().map(|(_, addr)| addr.clone()).collect();
        match resolve(
            &self.providers,
            &self.rpc,
            &self.names,
            &entry,
            tenant.as_bytes(),
            PATIENCE,
        )
        .await
        {
            Resolution::Resolved(found) => {
                let machines: Vec<String> = found
                    .machines
                    .iter()
                    .map(|(_, addr)| machine_name(addr))
                    .collect();
                (
                    true,
                    format!(
                        "tenant={tenant} cell={} machines={}",
                        String::from_utf8_lossy(&found.cell_name),
                        machines.join(",")
                    ),
                )
            }
            other => (false, format!("resolve: {other:?}")),
        }
    }

    /// Append `count` records to one journal: as its one writer, or as one
    /// of its writers when it is a multi-writer journal.
    async fn write_to(
        &self,
        servers: &[(u64, Address)],
        opened: &mut Opened,
        journal: &str,
        count: u32,
        patience: Duration,
    ) -> (bool, String) {
        let client = self.journal_client(servers, &opened.members);
        // A journal just created opens on its members a moment later: until
        // then a claim reads nothing, and the operator asks again.
        let deadline = client.now() + patience;
        let mut first = None;
        let mut done = 0;
        while done < count {
            let value = format!("{journal}-{}", opened.written + 1);
            if opened.multi {
                // No deduplication: a write whose answer never came is not
                // sent again.
                let request = append_request(opened.id, vec![value.into_bytes()]);
                match client
                    .write(&request, 0, WriteOptions::default())
                    .await
                    .outcome
                {
                    WriteOutcome::Written { seq, .. } => {
                        opened.written += 1;
                        done += 1;
                        first.get_or_insert(seq);
                        opened.last = Some(seq);
                    }
                    WriteOutcome::UnknownJournal
                    | WriteOutcome::Redirect { .. }
                    | WriteOutcome::Busy
                        if client.now() < deadline =>
                    {
                        let pause = client.tunables().retry_backoff;
                        if self.providers.time().sleep(pause).await.is_err() {
                            return (false, "stopped".into());
                        }
                    }
                    other => {
                        return (
                            false,
                            format!("write: {}", first_word(&format!("{other:?}"))),
                        );
                    }
                }
                continue;
            }
            if opened.writer.owned().is_none()
                && let Err(detail) = self.claim(&client, &mut opened.writer).await
            {
                return (false, detail);
            }
            // One entry per record: a retry after an unsettled answer is the
            // identical write, which the journal answers as a duplicate.
            let Some(entry) = opened.writer.entry(vec![Value(value.into_bytes())]) else {
                return (false, "write: the writer leads no term".into());
            };
            loop {
                match opened.writer.write_entry(&client, &entry, 0).await {
                    WriterOutcome::Written { seq, .. } => {
                        opened.written += 1;
                        done += 1;
                        first.get_or_insert(seq);
                        opened.last = Some(seq);
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
                    other => {
                        return (
                            false,
                            format!("write: {}", first_word(&format!("{other:?}"))),
                        );
                    }
                }
            }
        }
        match (first, opened.last) {
            (Some(first), Some(last)) => (
                true,
                format!("{count} records chosen at positions {first}..={last}"),
            ),
            _ => (true, "nothing to write".into()),
        }
    }

    /// Read the last `count` records of `tenant`/`journal`: its tail.
    async fn read(&self, known: &Known, tenant: &str, journal: &str, count: u32) -> (bool, String) {
        let Some(opened) = known.journals.get(&(tenant.to_owned(), journal.to_owned())) else {
            return (
                false,
                format!("no journal {tenant}/{journal}: create it first"),
            );
        };
        let client = self.journal_client(&known.servers(), &opened.members);
        let from = opened
            .last
            .map_or(0, |last| last.saturating_sub(u64::from(count) - 1));
        let mut reader = Reader::new(opened.id, from);
        let deadline = client.now() + PATIENCE;
        let want = usize::try_from(count).unwrap_or(usize::MAX);
        let mut records = Vec::new();
        let mut at = None;
        while records.len() < want {
            match reader.next(&client, 0).await {
                ReaderOutcome::Records {
                    from: start,
                    records: page,
                    ..
                } => {
                    if page.is_empty() {
                        break;
                    }
                    at.get_or_insert(start);
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
            .rev()
            .take(8)
            .rev()
            .map(|r| String::from_utf8_lossy(r).into_owned())
            .collect();
        let more = if records.len() > shown.len() {
            "…, "
        } else {
            ""
        };
        (
            true,
            format!(
                "{} records from position {}: {more}{}",
                records.len(),
                at.unwrap_or(from),
                shown.join(", ")
            ),
        )
    }

    /// Where every tenant and journal lives, through the admin API.
    ///
    /// The machines in `silent` (crashed or cut off) are not asked: they
    /// would not answer.
    async fn survey(&self, config: &Config, known: &Known, silent: &[usize]) -> Option<Survey> {
        let fleet = known.cell.as_ref()?.journals.fleet?;
        let servers = known.servers();
        let machine_of = |addr: &Address| (0..config.machines).find(|m| machine_addr(*m) == *addr);
        // Server `i` of the client is machine `index[i]`.
        let index: Vec<usize> = servers
            .iter()
            .map(|(_, addr)| machine_of(addr).unwrap_or(usize::MAX))
            .collect();
        let mut machines = vec![None; config.machines];
        let mut nodes = vec![None; config.machines];
        let mut by_node = BTreeMap::new();
        for ((id, _), m) in servers.iter().zip(&index) {
            if let (Some(slot), Some(node)) = (machines.get_mut(*m), nodes.get_mut(*m)) {
                *slot = Some(short(*id));
                *node = Some(id.to_string());
                by_node.insert(*id, *m);
            }
        }
        let cells: Vec<CellView> = [&known.cell, &known.cell2]
            .into_iter()
            .enumerate()
            .filter_map(|(at, cell)| {
                let cell = cell.as_ref()?;
                let mut machines: Vec<usize> = cell
                    .servers
                    .iter()
                    .filter_map(|(_, addr)| machine_of(addr))
                    .collect();
                machines.sort_unstable();
                Some(CellView {
                    name: format!("cell-{}", at + 1),
                    machines,
                })
            })
            .collect();
        let elections: Vec<JournalIdentifier> = known
            .cell
            .iter()
            .chain(&known.cell2)
            .filter_map(|cell| cell.journals.election)
            .collect();
        let client = self.client(&servers);
        let directory = read_directory(&client, 0, fleet).await.ok()?;
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
            for election in elections.iter().filter(|e| e.tenant == tenant) {
                list.push(("election".to_owned(), *election));
            }
            // Every journal at the same time: a survey must not hold the
            // next step back long.
            let places = list
                .into_iter()
                .map(|(name, id)| self.place(&client, &index, &by_node, silent, name, id));
            let journals = futures::future::join_all(places).await;
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
        Some((machines, nodes, cells, tenants))
    }

    /// One journal, as every machine's `Inspect` answers for it.
    async fn place(
        &self,
        client: &Client<SimProviders>,
        index: &[usize],
        by_node: &BTreeMap<u64, usize>,
        silent: &[usize],
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
            acceptors: Vec::new(),
        };
        let ballot = |b: Option<&paros::wire::common::Ballot>| {
            b.map(|b| BallotView {
                round: b.round,
                node: short(b.node),
            })
        };
        let mut replies = Vec::new();
        for (server, machine) in index.iter().enumerate() {
            if silent.contains(machine) {
                continue;
            }
            let Some(reply) = client.inspect(server, journal).await else {
                continue;
            };
            place.hosts.push(*machine);
            replies.push((*machine, reply.clone()));
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
        replies.sort_by_key(|(machine, _)| *machine);
        place.acceptors = replies
            .into_iter()
            .map(|(machine, reply)| AcceptorView {
                machine,
                member: place.members.contains(&machine),
                promised: ballot(reply.promised.as_ref()),
                watermark: reply.vote_watermark,
                chosen: reply.chosen_index,
                floor: reply.first_slot,
                accepted: reply.accepted_count,
                tail: reply
                    .accepted_tail
                    .iter()
                    .map(|vote| (vote.slot, ballot(vote.ballot.as_ref()).unwrap_or_default()))
                    .collect(),
            })
            .collect();
        place
    }
}

/// A node id as people read it: its first six hex digits.
fn short(id: u64) -> String {
    format!("{id:016x}")[..6].to_string()
}

/// A leader uuid as people read it: its first six hex digits.
fn short_uuid(uuid: paros::core::journal::LeaderUuid) -> String {
    format!("{:032x}", uuid.0)[..6].to_string()
}

/// The first word of an outcome's debug form: its variant.
fn first_word(text: &str) -> &str {
    text.split([' ', '{', '(']).next().unwrap_or(text)
}

/// A machine's name from its address.
fn machine_name(addr: &Address) -> String {
    let text = addr.to_string();
    text.split(':')
        .next()
        .and_then(machine_index)
        .map_or(text.clone(), |m| format!("machine-{m}"))
}

/// Turn a started writer on or off.
fn stream_switch(known: &mut Known, tenant: &str, journal: &str, on: bool) -> (bool, String) {
    let key = (tenant.to_owned(), journal.to_owned());
    if !known.journals.contains_key(&key) {
        return (
            false,
            format!("no journal {tenant}/{journal}: create it first"),
        );
    }
    if on {
        known.streams.insert(key);
        (
            true,
            format!("a writer appends to {tenant}/{journal} five times a second"),
        )
    } else {
        known.streams.remove(&key);
        (true, format!("the writer of {tenant}/{journal} stopped"))
    }
}
