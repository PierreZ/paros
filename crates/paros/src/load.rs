//! **Busyness** (#424, `docs/architecture.md` §3.6): how busy a machine and
//! its disk were over one window, from two samples of moonpool's cumulative
//! counters (`moonpool_core::SystemProvider`).
//!
//! This is FDB's `getSystemStatistics` (`flow/Platform.cpp`) and the ratios
//! its `status json` computes (`Status.cpp`), and nothing else: pure
//! arithmetic over two [`SystemSample`]s. No I/O, no clock, no randomness,
//! no buggify. A machine samples itself (`crate::machine::load`), keeps its
//! last full window, and answers `Load` with it; nothing is stored.
//!
//! | field | formula | FDB |
//! |---|---|---|
//! | `cpu_cores` | `Δprocess_cpu / elapsed`; can be above 1 | `cpu.usage_cores` |
//! | `machine_cpu` | `clamp(Δmachine_busy / Δmachine_total, 0, 1)` | `machine.cpu.logical_core_utilization` |
//! | `run_loop_busy` | `clamp(Δrun_loop_busy / elapsed, 0, 1)` | `run_loop_busy` |
//! | `disk.busy` | `clamp(Δio_ticks / elapsed, 0, 1)` | `disk.busy` |
//! | `disk.reads_hz`, … | `Δcount / elapsed` | `disk.reads.hz`, … |
//!
//! A counter that went back between the samples (a reboot, a wrapped
//! kernel counter) gives no window at all, never a negative ratio.

use std::time::Duration;

use moonpool_core::{DiskCounters, SystemSample};

/// How busy a machine was over one window.
#[derive(Clone, Debug, PartialEq)]
pub struct Busyness {
    /// The window's length.
    pub elapsed: Duration,
    /// CPU cores the process used on average; can be above 1.
    pub cpu_cores: f64,
    /// Logical cores the process may use.
    pub cores: u32,
    /// The whole machine's CPU use, in `[0, 1]`.
    pub machine_cpu: f64,
    /// The share of the window the run loop was busy, in `[0, 1]`, when the
    /// runtime exposes it.
    pub run_loop_busy: Option<f64>,
    /// The data disk, when its device is known in both samples.
    pub disk: Option<DiskBusyness>,
}

/// How busy the data disk was over one window.
#[derive(Clone, Debug, PartialEq)]
pub struct DiskBusyness {
    /// The device's name.
    pub device: String,
    /// The share of the window with at least one I/O in flight, in `[0, 1]`
    /// (iostat's `%util`: on flash and RAID it reaches 1 long before the
    /// device is at its throughput limit).
    pub busy: f64,
    /// I/Os in flight at the window's end.
    pub queue_depth: u64,
    /// Completed reads per second.
    pub reads_hz: f64,
    /// Completed writes per second.
    pub writes_hz: f64,
    /// Bytes read per second.
    pub read_bps: f64,
    /// Bytes written per second.
    pub write_bps: f64,
}

impl Busyness {
    /// The window from `prev` to `now`, or `None` when there is none: `now`
    /// is not after `prev`, or a counter went back.
    ///
    /// # Panics
    ///
    /// Never on two samples of moonpool's: a computed ratio out of its
    /// range is a bug here.
    #[must_use]
    pub fn between(prev: &SystemSample, now: &SystemSample) -> Option<Self> {
        let elapsed = now.at.checked_sub(prev.at)?;
        if elapsed.is_zero() {
            return None;
        }
        let process_cpu = now.process_cpu.checked_sub(prev.process_cpu)?;
        let machine_busy = now.machine_busy.checked_sub(prev.machine_busy)?;
        let machine_total = now.machine_total.checked_sub(prev.machine_total)?;
        let run_loop_busy = match (prev.run_loop_busy, now.run_loop_busy) {
            (Some(prev), Some(now)) => Some(Some(now.checked_sub(prev)?)),
            _ => Some(None),
        }?;
        let disk = match (&prev.disk, &now.disk) {
            (Some(prev), Some(now)) if prev.device == now.device => {
                Some(DiskBusyness::between(prev, now, elapsed)?)
            }
            _ => None,
        };
        let busyness = Self {
            elapsed,
            cpu_cores: ratio(process_cpu, elapsed),
            cores: now.cores,
            machine_cpu: if machine_total.is_zero() {
                0.0
            } else {
                ratio(machine_busy, machine_total).clamp(0.0, 1.0)
            },
            run_loop_busy: run_loop_busy.map(|busy| ratio(busy, elapsed).clamp(0.0, 1.0)),
            disk,
        };
        assert!(busyness.cpu_cores >= 0.0, "a CPU use is never negative");
        assert!(
            busyness.in_range(),
            "every ratio of a window is within its range"
        );
        Some(busyness)
    }

    /// Whether every ratio is within its range: the oracle a reader can
    /// check on any answer. `cpu_cores` has no upper bound here: Tokio's
    /// workers can add up to more than `cores` when the quota changed.
    #[must_use]
    pub fn in_range(&self) -> bool {
        let unit = |value: f64| (0.0..=1.0).contains(&value);
        self.cpu_cores >= 0.0
            && self.cpu_cores.is_finite()
            && unit(self.machine_cpu)
            && self.run_loop_busy.is_none_or(unit)
            && self.disk.as_ref().is_none_or(|disk| {
                unit(disk.busy)
                    && disk.reads_hz >= 0.0
                    && disk.writes_hz >= 0.0
                    && disk.read_bps >= 0.0
                    && disk.write_bps >= 0.0
            })
    }
}

impl DiskBusyness {
    /// The disk's window from `prev` to `now`, `elapsed` long; `None` when a
    /// counter went back.
    fn between(prev: &DiskCounters, now: &DiskCounters, elapsed: Duration) -> Option<Self> {
        let io_ticks = now.io_ticks.checked_sub(prev.io_ticks)?;
        let reads = now.reads.checked_sub(prev.reads)?;
        let writes = now.writes.checked_sub(prev.writes)?;
        let read_bytes = now.read_bytes.checked_sub(prev.read_bytes)?;
        let write_bytes = now.write_bytes.checked_sub(prev.write_bytes)?;
        let per_second = |count: u64| count_f64(count) / elapsed.as_secs_f64();
        Some(Self {
            device: now.device.clone(),
            busy: ratio(io_ticks, elapsed).clamp(0.0, 1.0),
            queue_depth: now.in_flight,
            reads_hz: per_second(reads),
            writes_hz: per_second(writes),
            read_bps: per_second(read_bytes),
            write_bps: per_second(write_bytes),
        })
    }
}

/// `part / whole` as a float; `whole` is never zero here.
fn ratio(part: Duration, whole: Duration) -> f64 {
    part.as_secs_f64() / whole.as_secs_f64()
}

/// A count as a float (exact below 2^53, which every count here is).
#[allow(clippy::cast_precision_loss)]
fn count_f64(count: u64) -> f64 {
    count as f64
}

#[cfg(test)]
mod tests;
