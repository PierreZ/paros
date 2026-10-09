//! The cell an operator works on (#246): `init` whole against the machines
//! (`paros::client::initialize`, the code `parosctl init` prints), and the
//! cell learned from the run that formed it or through `Inspect` at the
//! machines — never from the harness (§3.8: no identifier is fixed).

use std::net::SocketAddr;
use std::sync::Arc;

use moonpool_sim::{
    SimContext, assert_always, assert_reachable, assert_sometimes, buggify_with_prob,
};
use paros::JournalIdentifier;
use paros::client::bootstrap;
use paros::client::fleet::Stage;
use paros::client::initialize::{InitParams, InitRefusal, InitRun, Initialized};
use paros::machine::ControlJournals;

use super::super::system::Announce;
use super::{FleetOps, reach};
use crate::client::ChainClient;

/// The cell as this operator learned it (#246): from the `init` it ran, or
/// through `Inspect` at the machines.
#[derive(Clone)]
pub(super) struct Cell {
    /// Its control journals; the fleet tenant's is always named.
    pub(super) journals: ControlJournals,
    /// The fleet tenant's control journal.
    pub(super) fleet: JournalIdentifier,
    /// Its members by id, in id order: the cell control journal's genesis.
    pub(super) members: Vec<u64>,
    /// The members this operator reached, with their addresses: the
    /// servers a fleet step talks to.
    pub(super) servers: Vec<(u64, SocketAddr)>,
    /// A client over `servers`, announcing every call to the audit.
    pub(super) client: ChainClient,
}

impl Cell {
    /// The server a call starts at.
    pub(super) fn first(&self, draw: u64) -> usize {
        usize::try_from(draw % self.servers.len().max(1) as u64).unwrap_or(0)
    }

    /// The machine a server index names, as the topology names it.
    pub(super) fn ip(&self, server: usize) -> Option<String> {
        self.servers
            .get(server)
            .map(|(_, addr)| addr.ip().to_string())
    }
}

impl FleetOps {
    /// A client over `servers`, announcing every call at the cell's
    /// journals to the audit and their shared history.
    pub(super) fn connect(&self, ctx: &SimContext, servers: &[(u64, SocketAddr)]) -> ChainClient {
        self.connector
            .client(servers)
            .with_observer(Arc::new(Announce::every(ctx)))
            .rotating_over(servers.len())
    }

    /// The cell: the one this operator knows, or — while it knows none, or
    /// reached only some of its members — the one the machines name now,
    /// learned through `Inspect` (§3.8), never from the harness.
    #[tracing::instrument(level = "debug", skip_all, fields(client = self.client_id))]
    pub(super) async fn learn(&mut self, ctx: &SimContext) -> Option<Cell> {
        if let Some(cell) = &self.cell
            && cell.servers.len() == cell.members.len()
        {
            return Some(cell.clone());
        }
        let servers = bootstrap::discover(
            self.connector.providers(),
            self.connector.rpc(),
            &self.machines,
            self.patience,
        )
        .await;
        if servers.is_empty() {
            return self.cell.clone();
        }
        let client = self.connect(ctx, &servers);
        let Some(journals) = bootstrap::control_journals(&client).await else {
            return self.cell.clone();
        };
        let Some(fleet) = journals.fleet else {
            assert_always!(
                false,
                "fleet: the cell init formed hosts the fleet tenant",
                { "cell" => journals.cell_id }
            );
            return self.cell.clone();
        };
        let mut members = client
            .inspect(0, journals.cell)
            .await
            .map(|view| view.members)
            .unwrap_or_default();
        members.sort_unstable();
        members.dedup();
        if members.is_empty() {
            return self.cell.clone();
        }
        assert_always!(
            crate::machine::formed_cell(ctx.state()) == Some(journals),
            "fleet: the control journals Inspect names are the ones init formed",
            { "cell" => journals.cell_id }
        );
        assert_reachable!("fleet: an operator learns the cell's control journals through Inspect");
        let servers: Vec<(u64, SocketAddr)> = servers
            .into_iter()
            .filter(|(id, _)| members.contains(id))
            .collect();
        if servers.is_empty() {
            return self.cell.clone();
        }
        self.cell = Some(Cell {
            journals,
            fleet,
            members,
            client: self.connect(ctx, &servers),
            servers,
        });
        self.cell.clone()
    }

    /// The cell an `init` came to (#246).
    fn adopt(&mut self, ctx: &SimContext, initialized: &Initialized) {
        let journals = initialized.journals;
        assert_always!(
            crate::machine::formed_cell(ctx.state()) == Some(journals),
            "init: the cell init reports is the one the machines formed",
            { "cell" => journals.cell_id }
        );
        let Some(fleet) = journals.fleet else {
            assert_always!(
                false,
                "fleet: the cell init formed hosts the fleet tenant",
                { "cell" => journals.cell_id }
            );
            return;
        };
        self.cell = Some(Cell {
            journals,
            fleet,
            members: initialized.members.clone(),
            client: self.connect(ctx, &initialized.servers),
            servers: initialized.servers.clone(),
        });
    }

    /// Every machine's address, the one at rank `target` first: the order
    /// `init` is handed its addresses in.
    fn addrs_from(&self, target: usize) -> Vec<SocketAddr> {
        std::iter::once(self.machines[target])
            .chain(
                self.machines
                    .iter()
                    .enumerate()
                    .filter(|(rank, _)| *rank != target)
                    .map(|(_, addr)| *addr),
            )
            .collect()
    }

    /// `init` whole (#246), as `parosctl init` runs it: sent to the seed the
    /// layout names — or, on its own BUGGIFY location, to a machine outside
    /// the seeds, which must refuse it. Whether it ended.
    #[tracing::instrument(level = "debug", skip_all, fields(client = self.client_id))]
    pub(super) async fn initialize(&mut self, ctx: &SimContext, draw: u64) -> bool {
        let outside = self.layout.seeds..self.machines.len();
        let (target, misdirected) = if !outside.is_empty() && buggify_with_prob!(0.05) {
            assert_reachable!("init: an operator sends init to a machine outside the seeds");
            let span = (outside.end - outside.start) as u64;
            (
                outside.start + usize::try_from(draw % span).unwrap_or(0),
                true,
            )
        } else {
            (self.layout.target, false)
        };
        let addrs = self.addrs_from(target);
        crate::machine::note_init_sent(ctx.state());
        let connector = self.connector.clone();
        let observer: Arc<dyn paros::client::CallObserver> = Arc::new(Announce::every(ctx));
        let run = paros::client::initialize::initialize(
            connector.providers(),
            connector.rpc(),
            &addrs,
            |servers| {
                connector
                    .client(servers)
                    .with_observer(observer.clone())
                    .rotating_over(servers.len())
            },
            InitParams {
                patience: self.patience,
                fleet_id: draw | 1,
                leader_seed: self.leader_seeds.next(),
            },
        )
        .await;
        match run {
            InitRun::Initialized(initialized) | InitRun::AlreadyInitialized(initialized)
                if !misdirected =>
            {
                initialized.steps.iter().copied().for_each(reach);
                let finished = initialized.steps.last() == Some(&Stage::CellReady);
                if finished {
                    assert_sometimes!(
                        initialized.steps.first() != Some(&Stage::FormFleet),
                        "fleet: an init resumes from where the journals stand"
                    );
                }
                assert_sometimes!(finished, "fleet: an init registers the cell READY");
                if initialized.claimed.is_some() {
                    assert_reachable!("init: a run claims the cell control journal");
                }
                if initialized.steps.is_empty() && initialized.claimed.is_none() {
                    assert_reachable!("init: a re-run finds nothing left to do");
                } else {
                    assert_reachable!("init: a run initializes the fleet");
                }
                self.adopt(ctx, &initialized);
                self.initialized = true;
                true
            }
            InitRun::Initialized(_) | InitRun::AlreadyInitialized(_) => {
                assert_always!(
                    false,
                    "init: a machine outside the seeds never initializes the cell",
                    { "target" => target }
                );
                true
            }
            InitRun::Refused(InitRefusal::Formation(label)) if misdirected => {
                assert_always!(
                    label == "not_a_seed",
                    "init: a machine outside the seeds refuses init as not a seed",
                    { "refusal" => label.as_str() }
                );
                assert_reachable!("init: a machine outside the seeds refuses init");
                true
            }
            InitRun::Refused(InitRefusal::Formation(label)) if label == "storage" => {
                // A seed's write failed under it: nothing was decided, and a
                // re-run resumes the plan it recorded.
                assert_reachable!("init: a seed's failed write refuses init, and it is run again");
                false
            }
            InitRun::Refused(refusal) => {
                assert_always!(
                    false,
                    "init: a run on the seeds is refused only for a failed write",
                    { "refusal" => format!("{refusal:?}") }
                );
                true
            }
            InitRun::Unreachable(_) | InitRun::Ambiguous | InitRun::Interrupted(_) => {
                assert_reachable!("init: a run that decided nothing in time is run again");
                false
            }
        }
    }
}
