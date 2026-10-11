//! `LOAD` (#424 (busyness metrics)): how busy the cell's machines are,
//! through the library's `paros::client::load` — the code `parosctl machine
//! list|show` prints. The operator asks the cell for its admin view, then
//! asks every machine the view names for its `Load`, all at once.
//!
//! The oracles judge every windowed answer:
//!
//! - **range**: every ratio is within its range;
//! - **floor**: a window is never shorter than `LOAD_INTERVAL_FLOOR`;
//! - **ground truth**: the window's two samples are samples the simulator
//!   handed to that machine's process, and the answer is exactly
//!   `Busyness::between` of them. The simulator's own record of what the
//!   process read is the judge, never the process's word for it.

use std::net::IpAddr;
use std::time::Duration;

use moonpool_sim::SystemSample;
use moonpool_sim::{SimContext, assert_always, assert_reachable, assert_sometimes};
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
    /// it is.
    #[tracing::instrument(level = "debug", skip_all, fields(client = self.client_id))]
    pub(in crate::chain_workload) async fn load(&mut self, ctx: &SimContext, draw: u64) {
        let Some(cell) = self.learn(ctx).await else {
            assert_reachable!("load: a load finds no cell formed yet");
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
        let outcomes = ask_all(
            self.connector.providers(),
            self.connector.rpc(),
            self.connector.names(),
            &machines,
            self.patience,
        )
        .await;
        assert_always!(
            outcomes.len() == machines.len(),
            "load: one outcome per machine asked",
            { "outcomes" => outcomes.len(), "machines" => machines.len() }
        );
        for (node, addr) in &machines {
            match outcomes.get(node) {
                Some(LoadOutcome::Answered(ack)) => {
                    let ip = crate::machine::process_ip(ctx.state(), addr)
                        .and_then(|ip| ip.parse::<IpAddr>().ok());
                    judge(ctx, ack, ip);
                }
                Some(LoadOutcome::Silent) | None => {
                    assert_reachable!("load: a machine does not answer its load");
                }
            }
        }
    }
}

/// Judge one answer of the machine whose process is at `ip`.
fn judge(ctx: &SimContext, ack: &LoadAck, ip: Option<IpAddr>) {
    assert_sometimes!(ack.windowed, "load: a machine answers a window");
    if !ack.windowed {
        return;
    }
    let busyness = busyness_of(ack);
    assert_always!(
        busyness.in_range(),
        "load: every ratio of an answer is in range",
        { "node" => ack.node_id }
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
    let Some(ip) = ip else {
        return;
    };
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
        return;
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
