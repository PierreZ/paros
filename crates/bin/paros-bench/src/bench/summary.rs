//! The bench's report as one JSON document (#194 (throughput benchmark)):
//! the throughput and the latency percentiles of every call, and what the
//! numbers depend on: the commit, the command line, the CPU count, the
//! kernel and the filesystem of the stores. `paros-bench --json` prints
//! it in place of the text tables.

use std::path::Path;
use std::time::Duration;

use hdrhistogram::Histogram;
use serde_json::{Value, json};

use super::histograms::{Call, Calls};

/// The commit `paros-bench` was built from (`build.rs`), or `unknown`.
pub fn commit() -> &'static str {
    option_env!("PAROS_COMMIT").unwrap_or("unknown")
}

/// The machine the bench ran on and, for a local cell, the filesystem of
/// its stores (`data_dir`).
pub fn host(data_dir: Option<&Path>) -> Value {
    let cpus = std::thread::available_parallelism().map_or(0, std::num::NonZero::get);
    json!({
        "cpus": cpus,
        "os": std::env::consts::OS,
        "arch": std::env::consts::ARCH,
        "kernel": kernel(),
        "filesystem": data_dir.and_then(filesystem),
    })
}

/// The running kernel's release, where the system tells it.
fn kernel() -> Option<String> {
    std::fs::read_to_string("/proc/sys/kernel/osrelease")
        .ok()
        .map(|text| text.trim().to_string())
}

/// The type of the filesystem that holds `dir`: the mount with the longest
/// path that contains it, in `/proc/mounts`.
fn filesystem(dir: &Path) -> Option<String> {
    let dir = dir.canonicalize().ok()?;
    let mounts = std::fs::read_to_string("/proc/mounts").ok()?;
    mount_type(&mounts, &dir)
}

/// The type of the mount in `mounts` (`/proc/mounts` text) that holds
/// `dir`.
fn mount_type(mounts: &str, dir: &Path) -> Option<String> {
    mounts
        .lines()
        .filter_map(|line| {
            let mut fields = line.split_whitespace();
            let (_, point, kind) = (fields.next()?, fields.next()?, fields.next()?);
            // `/proc/mounts` writes a space in a path as `\040`.
            let point = point.replace("\\040", " ");
            dir.starts_with(&point)
                .then(|| (point.len(), kind.to_string()))
        })
        .max_by_key(|(length, _)| *length)
        .map(|(_, kind)| kind)
}

/// Milliseconds, as a float.
fn ms(nanos: f64) -> f64 {
    nanos / 1e6
}

#[allow(
    clippy::cast_precision_loss,
    reason = "a reported rate needs no 64-bit precision"
)]
fn float(count: u64) -> f64 {
    count as f64
}

/// One call's numbers over `measured`.
fn call(histogram: &Histogram<u64>, errors: u64, bytes: u64, measured: Duration) -> Value {
    let secs = measured.as_secs_f64().max(1e-9);
    let at = |quantile: f64| ms(float(histogram.value_at_quantile(quantile)));
    json!({
        "ops": histogram.len(),
        "errors": errors,
        "ops_per_s": float(histogram.len()) / secs,
        "bytes": bytes,
        "bytes_per_s": float(bytes) / secs,
        "latency_ms": {
            "mean": ms(histogram.mean()),
            "p50": at(0.50),
            "p90": at(0.90),
            "p99": at(0.99),
            "p99_9": at(0.999),
            "max": ms(float(histogram.max())),
        },
    })
}

/// The whole report: `run` (the seed, the target, the rate, …) with the
/// commit, the command line, the host and every call of `calls`.
pub fn document(
    run: &Value,
    data_dir: Option<&Path>,
    measured: Duration,
    calls: &[Call],
    total: &Calls,
) -> Value {
    let mut by_name = serde_json::Map::new();
    for &c in calls {
        by_name.insert(
            c.name().to_string(),
            call(total.of(c), total.errors_of(c), total.bytes_of(c), measured),
        );
    }
    json!({
        "commit": commit(),
        "command": std::env::args().collect::<Vec<_>>(),
        "host": host(data_dir),
        "run": run,
        "measured_s": measured.as_secs_f64(),
        "calls": by_name,
    })
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::time::Duration;

    use super::{Call, Calls, document, mount_type};

    #[test]
    fn the_longest_mount_that_holds_the_directory_names_its_filesystem() {
        let mounts = "overlay / overlay rw 0 0\n\
                      tmpfs /tmp tmpfs rw 0 0\n\
                      /dev/sda1 /tmp/my\\040disk ext4 rw 0 0\n";
        let kind = |dir: &str| mount_type(mounts, Path::new(dir));
        assert_eq!(kind("/home/user").as_deref(), Some("overlay"));
        assert_eq!(kind("/tmp/bench").as_deref(), Some("tmpfs"));
        assert_eq!(kind("/tmp/my disk/bench").as_deref(), Some("ext4"));
        assert_eq!(
            kind("/tmpfoo").as_deref(),
            Some("overlay"),
            "a whole component"
        );
    }

    #[test]
    fn the_report_names_every_call_with_its_percentiles() {
        let mut total = Calls::default();
        for ms in [1, 2, 3, 4] {
            total.record(Call::Write, Duration::from_millis(ms), 1024);
        }
        total.error(Call::ReadTail);
        let doc = document(
            &serde_json::json!({ "seed": 7 }),
            None,
            Duration::from_secs(2),
            &[Call::Write, Call::ReadTail],
            &total,
        );
        let write = &doc["calls"]["write"];
        assert_eq!(write["ops"], 4);
        assert_eq!(write["bytes"], 4096);
        assert!((write["ops_per_s"].as_f64().unwrap() - 2.0).abs() < 1e-9);
        assert!((write["bytes_per_s"].as_f64().unwrap() - 2048.0).abs() < 1e-9);
        let p99 = write["latency_ms"]["p99_9"].as_f64().unwrap();
        assert!((3.9..=4.1).contains(&p99), "{p99}");
        assert_eq!(doc["calls"]["read-tail"]["errors"], 1);
        assert_eq!(doc["run"]["seed"], 7);
        assert!(doc["host"]["cpus"].as_u64().unwrap() >= 1);
        assert!(doc["commit"].is_string());
    }
}
