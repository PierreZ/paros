//! The operator: the one workload, which runs the action log's operator
//! steps in order through the library client, the calls `parosctl` makes
//! (`init`, `tenant create`, `journal create`, `write`, `cell
//! add-machine`). It holds no policy of its own: retries, redirects and
//! claims are `paros::client`'s.

use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use async_trait::async_trait;
use moonpool_rpc::{RpcConfig, RpcHandle};
use moonpool_sim::{
    Providers, RandomProvider, SimContext, SimProviders, SimulationError, SimulationResult,
    TaskProvider, TimeProvider,
};
use paros::client::cell::CellSession;
use paros::client::fleet::{FleetSession, Step};
use paros::client::initialize::{InitParams, InitRun, Initialized, initialize};
use paros::client::journals::{self, JournalAnswer, JournalOp, JournalRequest};
use paros::client::{ClaimOutcome, Client, ClientTunables, Server, Writer, WriterOutcome};
use paros::system::Registry;
use paros::tenant::Desired;
use paros::{
    Address, JournalId, JournalIdentifier, Names, NodeClient, NodeId, TenantId, Transport, Value,
    WriterMode,
};
use tokio_util::sync::CancellationToken;

use crate::machine::machine_addr;
use crate::model::{Action, Config, Op, StepResult};

/// How long one operator step may take before it gives up.
const PATIENCE: Duration = Duration::from_secs(20);

/// The tenant and the journal every `Write` goes to.
const TENANT: &str = "sandbox";
const JOURNAL: &str = "log";

/// The operator workload.
pub(crate) struct Operator {
    config: Config,
    actions: Vec<Action>,
    results: Arc<Mutex<Vec<StepResult>>>,
}

impl Operator {
    pub(crate) fn new(
        config: Config,
        actions: Vec<Action>,
        results: Arc<Mutex<Vec<StepResult>>>,
    ) -> Self {
        Self {
            config,
            actions,
            results,
        }
    }
}

/// What the operator learned so far.
#[derive(Default)]
struct Known {
    cell: Option<Initialized>,
    journal: Option<(JournalIdentifier, Vec<u64>)>,
    writer: Option<Writer>,
}

#[async_trait]
impl moonpool_sim::Workload for Operator {
    fn name(&self) -> &'static str {
        "operator"
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        let (run, rpc) = Transport::Plaintext
            .client_only(ctx.providers().clone(), rpc_config())
            .map_err(|e| SimulationError::InvalidState(format!("client RPC runtime: {e}")))?;
        let stop = CancellationToken::new();
        let cancelled = stop.clone();
        ctx.task()
            .spawn_task("operator-rpc", async move {
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
            if !action.op.is_operator() {
                continue;
            }
            if !wait_until(ctx, action.at_ms).await {
                return Ok(());
            }
            let start_ms = now_ms(ctx);
            let (ok, detail) = desk.step(&self.config, &mut known, &action.op).await;
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
        }
        wait_until(ctx, self.config.run_ms).await;
        Ok(())
    }
}

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
        Client::new(&self.providers, servers, ClientTunables::default())
            .with_shutdown(self.shutdown.clone())
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

    async fn step(&self, config: &Config, known: &mut Known, op: &Op) -> (bool, String) {
        match op {
            Op::Init => self.init(config, known).await,
            Op::Write { value } => self.write(known, value).await,
            Op::Admit { machine } => self.admit(known, *machine).await,
            _ => (false, "not an operator step".into()),
        }
    }

    /// `parosctl init` over the founding members.
    async fn init(&self, config: &Config, known: &mut Known) -> (bool, String) {
        let members: Vec<Address> = (0..config.founders).map(machine_addr).collect();
        let params = InitParams {
            patience: PATIENCE,
            fleet_id: self.nonzero(),
            leader_seed: u128::from(self.nonzero()),
            names: ("sandbox".into(), "cell-1".into()),
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
                let detail = format!(
                    "cell formed over {} machines, coordinator {}",
                    done.members.len(),
                    short(done.coordinator.0)
                );
                known.cell = Some(done);
                (true, detail)
            }
            other => (false, format!("init: {other:?}")),
        }
    }

    /// Create the sandbox tenant and journal, once.
    async fn open_journal(
        &self,
        cell: &Initialized,
    ) -> Result<(JournalIdentifier, Vec<u64>), String> {
        let client = self.client(&cell.servers);
        let mut fleet = FleetSession::new(
            cell.journals,
            u128::from(self.nonzero()),
            Registry::new(cell.members.iter().copied().map(NodeId)),
            client.tunables().checkpoint_policy(),
        )
        .ok_or("the cell names no fleet journal")?;
        let draws: Vec<JournalIdentifier> = (0..4)
            .map(|_| JournalIdentifier::new(TenantId(self.nonzero()), JournalId(self.nonzero())))
            .collect();
        let run = fleet
            .create_tenant(&client, 0, TENANT.as_bytes(), draws, PATIENCE)
            .await;
        let tenant = match run.outcome {
            Step::Done { result, .. } => result,
            other => return Err(format!("tenant create: {other:?}")),
        };
        let election = cell
            .journals
            .election
            .ok_or("the cell names no election journal")?;
        let request = JournalRequest {
            request: self.nonzero(),
            tenant,
            op: JournalOp::Create {
                name: JOURNAL.as_bytes().to_vec(),
                writer: WriterMode::Single,
                desired: Desired::DOUBLE,
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
            JournalAnswer::Created { id, config } => Ok((
                JournalIdentifier::new(tenant, id),
                config.members().iter().map(|m| m.0).collect(),
            )),
            other => Err(format!("journal create: {other:?}")),
        }
    }

    /// Append `value` to the sandbox journal, as its one writer.
    async fn write(&self, known: &mut Known, value: &str) -> (bool, String) {
        let Some(cell) = known.cell.as_ref() else {
            return (false, "no cell: run init first".into());
        };
        if known.journal.is_none() {
            match self.open_journal(cell).await {
                Ok(journal) => known.journal = Some(journal),
                Err(error) => return (false, error),
            }
        }
        let Some((journal, members)) = known.journal.clone() else {
            return (false, "no journal".into());
        };
        let servers: Vec<(u64, Address)> = cell
            .servers
            .iter()
            .filter(|(id, _)| members.contains(id))
            .cloned()
            .collect();
        let client = self.client(if servers.is_empty() {
            &cell.servers
        } else {
            &servers
        });
        let writer = known
            .writer
            .get_or_insert_with(|| Writer::new(journal, u128::from(self.nonzero())));
        // A journal just created opens on its members a moment later: until
        // then a claim reads nothing, and the operator asks again.
        let deadline = client.now() + PATIENCE;
        loop {
            if writer.owned().is_none() {
                match writer.claim(&client, 0, false).await {
                    ClaimOutcome::Won { .. } | ClaimOutcome::Owned { .. } => {}
                    ClaimOutcome::Unread | ClaimOutcome::UnknownJournal
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
            match writer
                .write(&client, vec![Value(value.as_bytes().to_vec())], 0)
                .await
            {
                WriterOutcome::Written { seq, .. } => {
                    return (true, format!("{value:?} chosen at position {seq}"));
                }
                WriterOutcome::NotOwner | WriterOutcome::Superseded { .. }
                    if client.now() < deadline => {}
                other => return (false, format!("write: {other:?}")),
            }
        }
    }

    /// `parosctl cell add-machine` for machine `machine`.
    async fn admit(&self, known: &Known, machine: usize) -> (bool, String) {
        let Some(cell) = known.cell.as_ref() else {
            return (false, "no cell: run init first".into());
        };
        let client = self.client(&cell.servers);
        let members: Vec<NodeId> = cell.members.iter().copied().map(NodeId).collect();
        let founders = cell
            .servers
            .iter()
            .map(|(id, addr)| (NodeId(*id), addr.clone()))
            .collect();
        let mut session = CellSession::new(
            cell.journals,
            &members,
            founders,
            u128::from(self.nonzero()),
            client.tunables().checkpoint_policy(),
        );
        let target = machine_addr(machine);
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
            Step::Done { result, .. } => (
                true,
                format!("machine-{machine} admitted as {}", short(result.0)),
            ),
            other => (false, format!("add-machine: {other:?}")),
        }
    }
}

/// A node id as people read it: its first six hex digits.
fn short(id: u64) -> String {
    format!("{id:016x}")[..6].to_string()
}
