//! **A machine samples its own busyness** (#424, `docs/architecture.md`
//! §3.6) and answers `Load` with its last full window.
//!
//! [`spawn_monitor`] starts one task per machine run: it samples moonpool's
//! cumulative counters (`SystemProvider`, for the disk that holds the
//! machine's data directory) every `load_interval`, and keeps the last full
//! window ([`crate::load::Busyness`]) on the machine's [`LoadBoard`]. Every
//! phase of the machine (idle, admitted, founding member) serves `Load` from
//! that board ([`serve`]): pulled when an operator asks, never pushed and
//! never written into a journal, as FDB's status pulls from its workers.
//!
//! The same code runs in the simulation, where the disk counters are the
//! simulated disk's own and exact. The BUGGIFY decision (a machine that
//! skips its first window, so `Load` is answered with no window yet) is
//! drawn by the caller, never in the task.

use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use moonpool_core::{
    Detach, Providers, SimulationResult, SystemProvider, SystemSample, TaskProvider, TimeProvider,
};
use moonpool_rpc::RpcHandle;
use tokio_util::sync::CancellationToken;

use super::MachineFacts;
use crate::load::Busyness;
use crate::rpc::machine as wire;
use crate::rpc::methods::LoadRpc;
use crate::rpc::{Inbound, serve_well_known};

/// One full window, as the machine sampled it.
#[derive(Clone, Debug, PartialEq)]
pub struct Window {
    /// How busy the machine was.
    pub busyness: Busyness,
    /// The first sample's instant, on the system provider's clock.
    pub start_at: Duration,
    /// The second sample's instant, on the system provider's clock.
    pub end_at: Duration,
    /// When the window ended, on the time provider's clock.
    pub ended: Duration,
}

/// Where a machine keeps its last full window: shared by the monitor that
/// writes it and every phase's `Load` server that reads it.
#[derive(Clone, Default)]
pub struct LoadBoard(Arc<Mutex<Option<Window>>>);

impl LoadBoard {
    /// The last full window, if any.
    #[must_use]
    pub fn window(&self) -> Option<Window> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    fn publish(&self, window: Window) {
        assert!(window.end_at > window.start_at, "a window has a length");
        assert!(window.busyness.in_range(), "a published window is in range");
        *self.0.lock().unwrap_or_else(PoisonError::into_inner) = Some(window);
    }
}

impl std::fmt::Debug for LoadBoard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("LoadBoard").field(&self.window()).finish()
    }
}

/// Two boards are equal when they are the same board: the board is the
/// machine's, never a value.
impl PartialEq for LoadBoard {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for LoadBoard {}

/// Start sampling the machine's counters every `interval`, for the disk
/// that holds `data_dir`, onto `board`, until `shutdown`. `skip_first`
/// (the caller's BUGGIFY decision) throws the first window away, so the
/// machine answers `Load` with no window for one interval more.
pub(crate) fn spawn_monitor<P: Providers>(
    providers: &P,
    data_dir: String,
    interval: Duration,
    board: LoadBoard,
    skip_first: bool,
    shutdown: CancellationToken,
) {
    assert!(!interval.is_zero(), "a load interval is never zero");
    let monitor = monitor(providers.clone(), data_dir, interval, board, skip_first);
    providers
        .task()
        .spawn_task("paros-machine-load", async move {
            moonpool_core::select! {
                biased;
                () = shutdown.cancelled() => {}
                () = monitor => {}
            }
        })
        .detach();
}

/// Sample every `interval` and publish each full window.
async fn monitor<P: Providers>(
    providers: P,
    data_dir: String,
    interval: Duration,
    board: LoadBoard,
    mut skip_first: bool,
) {
    let sample = || match providers.system().sample(&data_dir) {
        Ok(sample) => Some(sample),
        Err(error) => {
            tracing::warn!(%error, "machine_load_unsampled");
            None
        }
    };
    let mut prev: Option<SystemSample> = sample();
    match prev
        .as_ref()
        .map(|s| s.disk.as_ref().map(|d| d.device.clone()))
    {
        Some(Some(device)) => tracing::info!(%device, data_dir, "machine_load_device"),
        Some(None) => tracing::warn!(data_dir, "machine_load_no_device"),
        None => {}
    }
    loop {
        if providers.time().sleep(interval).await.is_err() {
            return;
        }
        let now = sample();
        if let (Some(before), Some(after)) = (&prev, &now) {
            match Busyness::between(before, after) {
                Some(_) if skip_first => {
                    moonpool_assertions::reachable!("load: a machine skips its first window");
                    skip_first = false;
                }
                Some(busyness) => board.publish(Window {
                    busyness,
                    start_at: before.at,
                    end_at: after.at,
                    ended: providers.time().now(),
                }),
                None => {
                    moonpool_assertions::reachable!("load: a sample went back, no window");
                }
            }
        }
        prev = now;
    }
}

/// Serve `Load` on `rpc` from `board` until `shutdown`, in a task of its
/// own.
///
/// # Errors
///
/// The endpoint could not be registered.
pub(crate) fn serve<P: Providers>(
    providers: &P,
    rpc: &RpcHandle<P>,
    facts: &MachineFacts,
    shutdown: CancellationToken,
) -> SimulationResult<()> {
    let mut requests = Inbound::plain(serve_well_known::<P, LoadRpc>(rpc)?);
    let time = providers.time().clone();
    let board = facts.load.clone();
    let (node_id, name) = (facts.node_id.0, facts.name.clone());
    providers
        .task()
        .spawn_task("paros-machine-load-serve", async move {
            loop {
                moonpool_core::select! {
                    biased;
                    () = shutdown.cancelled() => return,
                    Some((_, reply)) = requests.recv() => {
                        reply.send(answer(node_id, &name, board.window(), time.now()));
                    }
                    else => return,
                }
            }
        })
        .detach();
    Ok(())
}

/// The answer of machine `node_id`, named `name`, with `window` at `now`.
#[must_use]
pub fn answer(node_id: u64, name: &str, window: Option<Window>, now: Duration) -> wire::LoadAck {
    let base = wire::LoadAck {
        node_id,
        name: name.to_string(),
        ..wire::LoadAck::default()
    };
    let Some(window) = window else {
        moonpool_assertions::reachable!("load: a machine answers with no window yet");
        return base;
    };
    let nanos = |d: Duration| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX);
    let busy = window.busyness;
    wire::LoadAck {
        windowed: true,
        elapsed_ns: nanos(busy.elapsed),
        window_age_ns: nanos(now.saturating_sub(window.ended)),
        window_start_ns: nanos(window.start_at),
        window_end_ns: nanos(window.end_at),
        cpu_cores: busy.cpu_cores,
        cores: busy.cores,
        machine_cpu: busy.machine_cpu,
        has_run_loop: busy.run_loop_busy.is_some(),
        run_loop_busy: busy.run_loop_busy.unwrap_or(0.0),
        disk: busy.disk.map(|disk| wire::DiskLoad {
            device: disk.device,
            busy: disk.busy,
            queue_depth: disk.queue_depth,
            reads_hz: disk.reads_hz,
            writes_hz: disk.writes_hz,
            read_bps: disk.read_bps,
            write_bps: disk.write_bps,
        }),
        ..base
    }
}
