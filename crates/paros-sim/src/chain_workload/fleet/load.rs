//! `LOAD` (#424 (busyness metrics)): how busy the cell's machines are,
//! through the library's `paros::client::load` — the code `parosctl machine
//! list|show` prints. The operator asks the cell for its admin view, then
//! asks every machine the view names for its `Load`, all at once.
//!
//! The oracles judge every windowed answer:
//!
//! - **range**: every ratio is within its range, and a machine is never
//!   busier than its cores (moonpool's CPU model never counts CPU time past
//!   the sample's instant);
//! - **floor**: a window is never shorter than `LOAD_INTERVAL_FLOOR`;
//! - **ground truth**: the window's two samples are samples the simulator
//!   handed to that machine's process, and the answer is exactly
//!   `Busyness::between` of them. The simulator's own record of what the
//!   process read is the judge, never the process's word for it. This
//!   holds with the CPU model on (#424 (busyness metrics)): the CPU counters
//!   are then real, and still exact.
//!
//! The gates show the gray failures reach the metric: a slow machine
//! answers, and in one round a slow machine is busier than every healthy
//! machine of its cell. No gate asks for a CPU or a disk over 90 % busy: a
//! gray failure ends with the chaos window, and no cell has formed by then,
//! so a slow machine has no work to be busy with (#424 (busyness
//! metrics)).

use std::net::IpAddr;
use std::time::Duration;

use moonpool_sim::{SimContext, assert_always, assert_reachable, assert_sometimes};
use moonpool_sim::{Slowness, SystemSample};
use paros::Address;
use paros::client::load::{LoadOutcome, ask_all};
use paros::client::views::{ViewOutcome, ask, request};
use paros::load::Busyness;
use paros::view::Scope;
use paros::wire::machine::LoadAck;
use paros::wire::view::{CellQuery, view_request::Query};

use super::FleetOps;

impl FleetOps {
    /// `LOAD`: ask every machine of the cell this operator knows how busy
    /// it is. Before a cell formed, the operator asks the machines it was
    /// given, as `parosctl init` takes their addresses: a machine answers
    /// `Load` in every phase, so the chaos window, where the gray failures
    /// are, sees answers too.
    #[tracing::instrument(level = "debug", skip_all, fields(client = self.client_id))]
    pub(in crate::chain_workload) async fn load(&mut self, ctx: &SimContext, draw: u64) {
        let Some(cell) = self.learn(ctx).await else {
            assert_reachable!("load: a load finds no cell formed yet");
            let given: Vec<(u64, Address)> = (0_u64..).zip(self.machines.iter().cloned()).collect();
            self.ask_round(ctx, &given).await;
            return;
        };
        let servers: Vec<_> = cell.servers.iter().map(|(_, addr)| addr.clone()).collect();
        let outcome = ask(
            self.connector.providers(),
            self.connector.rpc(),
            self.connector.names(),
            &servers,
            cell.first(draw),
            &request(&Scope::Admin, Query::Cell(CellQuery {})),
            self.patience,
        )
        .await;
        let ViewOutcome::Answered(reply) = outcome else {
            assert_reachable!("load: no member of the cell answered the view");
            return;
        };
        let machines: Vec<(u64, Address)> = reply
            .machines
            .iter()
            .filter_map(|m| Address::parse(&m.addr).ok().map(|addr| (m.node_id, addr)))
            .collect();
        self.ask_round(ctx, &machines).await;
    }

    /// One round: `Load` to every machine of `machines` (keyed by the
    /// caller's id for it) at once, each answer judged, then the round's
    /// gate.
    async fn ask_round(&self, ctx: &SimContext, machines: &[(u64, Address)]) {
        let outcomes = ask_all(
            self.connector.providers(),
            self.connector.rpc(),
            self.connector.names(),
            machines,
            self.patience,
        )
        .await;
        assert_always!(
            outcomes.len() == machines.len(),
            "load: one outcome per machine asked",
            { "outcomes" => outcomes.len(), "machines" => machines.len() }
        );
        let mut round = Vec::new();
        for (node, addr) in machines {
            match outcomes.get(node) {
                Some(LoadOutcome::Answered(ack)) => {
                    let ip = crate::machine::process_ip(ctx.state(), addr)
                        .and_then(|ip| ip.parse::<IpAddr>().ok());
                    round.extend(judge(ctx, ack, ip));
                }
                Some(LoadOutcome::Silent) | None => {
                    assert_reachable!("load: a machine does not answer its load");
                }
            }
        }
        compare(&round);
    }
}

/// The gate over one round: a machine slowed in CPU or disk reports more of
/// that busyness than every healthy machine of the cell. A gate, never an
/// oracle: a window can straddle the start of the gray window, and a
/// healthy machine can be busy on its own. Judged only when at least one
/// slow and one healthy machine answered with a window.
fn compare(round: &[(Slowness, Busyness)]) {
    let disk_busy = |busyness: &Busyness| busyness.disk.as_ref().map_or(0.0, |disk| disk.busy);
    let healthy: Vec<&Busyness> = round
        .iter()
        .filter(|(slowness, _)| slowness.is_healthy())
        .map(|(_, busyness)| busyness)
        .collect();
    let slow: Vec<&(Slowness, Busyness)> = round
        .iter()
        .filter(|(slowness, _)| slowness.cpu > 1 || slowness.disk > 1)
        .collect();
    if healthy.is_empty() || slow.is_empty() {
        return;
    }
    let busier = slow.iter().any(|(slowness, busyness)| {
        let cpu = slowness.cpu > 1
            && healthy
                .iter()
                .all(|other| busyness.cpu_cores > other.cpu_cores);
        let disk = slowness.disk > 1
            && healthy
                .iter()
                .all(|other| disk_busy(busyness) > disk_busy(other));
        cpu || disk
    });
    assert_sometimes!(busier, "load: a slow machine is busier than the healthy");
}

/// Judge one answer of the machine whose process is at `ip`. Returns the
/// machine's slowness and busyness for the round's gate, when it answered
/// with a window from a process the harness knows.
fn judge(ctx: &SimContext, ack: &LoadAck, ip: Option<IpAddr>) -> Option<(Slowness, Busyness)> {
    let slowness = ip.map_or(Slowness::HEALTHY, |ip| ctx.slowness(ip));
    if !slowness.is_healthy() {
        assert_reachable!("load: a slow machine answers its load");
    }
    assert_sometimes!(ack.windowed, "load: a machine answers a window");
    if !ack.windowed {
        return None;
    }
    let busyness = busyness_of(ack);
    assert_always!(
        busyness.in_range(),
        "load: every ratio of an answer is in range",
        { "node" => ack.node_id }
    );
    assert_always!(
        busyness.cpu_cores <= f64::from(busyness.cores),
        "load: a machine is never busier than its cores",
        { "node" => ack.node_id, "cores" => busyness.cores }
    );
    assert_always!(
        busyness.elapsed >= paros::LOAD_INTERVAL_FLOOR,
        "load: a window is never shorter than the floor",
        { "elapsed_ns" => ack.elapsed_ns }
    );
    assert_always!(
        ack.window_end_ns > ack.window_start_ns,
        "load: a window ends after it starts"
    );
    if let Some(disk) = &busyness.disk {
        assert_sometimes!(disk.busy > 0.0, "load: a disk is busy in a window");
    }
    let ip = ip?;
    let samples = ctx.system_samples(ip);
    let at = |ns: u64| {
        samples
            .iter()
            .filter(|s| s.at == Duration::from_nanos(ns))
            .collect::<Vec<&SystemSample>>()
    };
    let (starts, ends) = (at(ack.window_start_ns), at(ack.window_end_ns));
    if starts.is_empty() || ends.is_empty() {
        // The machine's process moved, or the window is older than the
        // samples the simulator keeps.
        assert_reachable!("load: a window older than the kept samples");
        return Some((slowness, busyness));
    }
    let exact = starts.iter().any(|start| {
        ends.iter()
            .any(|end| Busyness::between(start, end).as_ref() == Some(&busyness))
    });
    assert_always!(
        exact,
        "load: an answer is exactly the simulator's own counters",
        { "node" => ack.node_id, "elapsed_ns" => ack.elapsed_ns }
    );
    assert_sometimes!(exact, "load: an answer matches the ground truth");
    Some((slowness, busyness))
}

/// The busyness an answer carries, as `Busyness::between` would return it.
fn busyness_of(ack: &LoadAck) -> Busyness {
    Busyness {
        elapsed: Duration::from_nanos(ack.elapsed_ns),
        cpu_cores: ack.cpu_cores,
        cores: ack.cores,
        machine_cpu: ack.machine_cpu,
        run_loop_busy: ack.has_run_loop.then_some(ack.run_loop_busy),
        disk: ack.disk.as_ref().map(|disk| paros::load::DiskBusyness {
            device: disk.device.clone(),
            busy: disk.busy,
            queue_depth: disk.queue_depth,
            reads_hz: disk.reads_hz,
            writes_hz: disk.writes_hz,
            read_bps: disk.read_bps,
            write_bps: disk.write_bps,
        }),
    }
}
