//! **A machine samples its own busyness** (#424, `docs/architecture/03-6-status.md`
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
    Detach, DiskSpace, Providers, SimulationResult, SystemProvider, SystemSample, TaskProvider,
    TimeProvider,
};
use moonpool_rpc::RpcHandle;
use paros_core::{JournalId, JournalIdentifier, TenantId};
use tokio_util::sync::CancellationToken;

use super::MachineFacts;
use crate::load::Busyness;
use crate::metrics::{CallKind, JournalUsage, Metrics};
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
    /// The data disk's space at the window's end, when the system provider
    /// knows it (#360 (disk full)).
    pub space: Option<DiskSpace>,
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
    (board, metrics): (LoadBoard, Metrics),
    skip_first: bool,
    shutdown: CancellationToken,
) {
    assert!(!interval.is_zero(), "a load interval is never zero");
    let monitor = monitor(
        providers.clone(),
        data_dir,
        interval,
        (board, metrics),
        skip_first,
    );
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
    (board, metrics): (LoadBoard, Metrics),
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
                Some(busyness) => {
                    metrics.busyness(&busyness);
                    board.publish(Window {
                        busyness,
                        start_at: before.at,
                        end_at: after.at,
                        ended: providers.time().now(),
                        space: after.space,
                    });
                }
                // A counter that went back (a wrap, a cgroup change): a
                // production defence. The simulator's counters are
                // monotone, so it has no gate (#446).
                None => {}
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
    let metrics = facts.metrics.clone();
    let (node_id, name) = (facts.node_id.0, facts.name.clone());
    providers
        .task()
        .spawn_task("paros-machine-load-serve", async move {
            loop {
                moonpool_core::select! {
                    biased;
                    () = shutdown.cancelled() => return,
                    Some((_, reply)) = requests.recv() => {
                        let ack = answer(node_id, &name, board.window(), time.now());
                        reply.send(with_usage(ack, &metrics));
                    }
                    else => return,
                }
            }
        })
        .detach();
    Ok(())
}

/// `ack` with what `metrics` counted per journal (#366 (metrics)).
///
/// # Panics
///
/// When metrics that are off report usage.
#[must_use]
pub fn with_usage(ack: wire::LoadAck, metrics: &Metrics) -> wire::LoadAck {
    let usage: Vec<wire::JournalUsage> = metrics.usage().iter().map(usage_to_wire).collect();
    assert!(
        metrics.is_on() || usage.is_empty(),
        "a machine without metrics reports no usage"
    );
    wire::LoadAck {
        metered: metrics.is_on(),
        usage,
        ..ack
    }
}

/// One journal's usage on the wire.
#[must_use]
pub fn usage_to_wire(usage: &JournalUsage) -> wire::JournalUsage {
    wire::JournalUsage {
        tenant: usage.journal.tenant.0,
        journal: usage.journal.journal.0,
        writes: usage.calls_of(CallKind::Write),
        reads: usage.calls_of(CallKind::Read),
        set_leaders: usage.calls_of(CallKind::SetLeader),
        truncates: usage.calls_of(CallKind::Truncate),
        written_bytes: usage.written_bytes,
        read_bytes: usage.read_bytes,
    }
}

/// One journal's usage from the wire.
#[must_use]
pub fn usage_from_wire(usage: &wire::JournalUsage) -> JournalUsage {
    JournalUsage {
        journal: JournalIdentifier::new(TenantId(usage.tenant), JournalId(usage.journal)),
        calls: [
            usage.writes,
            usage.reads,
            usage.set_leaders,
            usage.truncates,
        ],
        written_bytes: usage.written_bytes,
        read_bytes: usage.read_bytes,
    }
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
        has_space: window.space.is_some(),
        free_bytes: window.space.map_or(0, |space| space.free_bytes),
        total_bytes: window.space.map_or(0, |space| space.total_bytes),
        ..base
    }
}
