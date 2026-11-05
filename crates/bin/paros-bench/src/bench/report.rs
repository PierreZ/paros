//! `paros-bench report FILE...` (#725 (bench phases)), after `mako`'s
//! `--mode report`: merge the `--json` reports of several bench processes
//! (one per machine, run at the same time) into one result.
//!
//! Each report keeps every call's raw `HdrHistogram`, and histograms add
//! without loss: the merged percentiles are those of every call of every
//! process. Counts and bytes add; rates add too, since the processes ran
//! side by side. A file may hold several reports, one per line (one per
//! topology). Reports whose runs differ in roles, preset or topology are
//! refused: their sum would mean nothing.

use std::collections::BTreeMap;
use std::path::PathBuf;

use clap::Args;
use hdrhistogram::Histogram;
use serde_json::{Value, json};

use super::expect::Limits;
use super::histograms::{Kinds, decode, histogram};
use super::summary::{call_value, commit};
use super::units::float;

/// `paros-bench report`.
#[derive(Args, Debug)]
pub struct ReportArgs {
    /// The `paros-bench run --json` reports to merge.
    #[arg(required = true, value_name = "FILE")]
    pub files: Vec<PathBuf>,
    /// Pass/fail limits on the merged result (`run --expect`).
    #[arg(long, default_value = "")]
    pub expect: Limits,
}

/// One call's sums across the reports.
#[derive(Default)]
struct Sum {
    histogram: Option<Histogram<u64>>,
    kinds: Kinds,
    bytes: u64,
    ops_per_s: f64,
    bytes_per_s: f64,
}

/// What two reports must share to merge.
fn shape(report: &Value) -> Value {
    let run = &report["run"];
    json!({
        "roles": run["roles"],
        "preset": run["preset"],
        "topology": run["topology"],
    })
}

/// Every report in `text` (one or more JSON documents).
fn documents(text: &str) -> Result<Vec<Value>, String> {
    serde_json::Deserializer::from_str(text)
        .into_iter::<Value>()
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| e.to_string())
}

/// Read every report of `files`.
///
/// # Errors
///
/// A file could not be read, or holds no report.
pub fn read(files: &[PathBuf]) -> Result<Vec<(String, Value)>, String> {
    let mut out = Vec::new();
    for path in files {
        let name = path.display().to_string();
        let text = std::fs::read_to_string(path).map_err(|e| format!("{name}: {e}"))?;
        let docs = documents(&text).map_err(|e| format!("{name}: {e}"))?;
        if docs.is_empty() {
            return Err(format!("{name}: no report"));
        }
        out.extend(docs.into_iter().map(|doc| (name.clone(), doc)));
    }
    Ok(out)
}

/// The merge of `reports` (each with the file it came from).
///
/// # Errors
///
/// The reports differ in roles, preset or topology, or one holds a call
/// without its histogram.
pub fn merge(reports: &[(String, Value)]) -> Result<Value, String> {
    let (first_name, first) = reports.first().ok_or("no report to merge")?;
    let expected = shape(first);
    let mut sums: BTreeMap<String, Sum> = BTreeMap::new();
    let mut measured: f64 = 0.0;
    for (name, report) in reports {
        if shape(report) != expected {
            return Err(format!(
                "{name} ran {} and {first_name} ran {expected}: only runs of the same roles, preset and topology merge",
                shape(report)
            ));
        }
        measured = measured.max(report["measured_s"].as_f64().unwrap_or(0.0));
        let calls = report["calls"]
            .as_object()
            .ok_or_else(|| format!("{name}: a report without calls"))?;
        for (call, numbers) in calls {
            let sum = sums.entry(call.clone()).or_default();
            let raw = numbers["histogram"].as_str().ok_or_else(|| {
                format!("{name}: {call} has no histogram (a report of an older paros-bench?)")
            })?;
            let decoded = decode(raw).map_err(|e| format!("{name}: {call}: {e}"))?;
            sum.histogram
                .get_or_insert_with(histogram)
                .add(&decoded)
                .map_err(|e| format!("{name}: {call}: {e:?}"))?;
            if let Some(kinds) = numbers["errors_by_kind"].as_object() {
                for (kind, count) in kinds {
                    *sum.kinds.entry(kind.clone()).or_default() += count.as_u64().unwrap_or(0);
                }
            }
            sum.bytes += numbers["bytes"].as_u64().unwrap_or(0);
            sum.ops_per_s += numbers["ops_per_s"].as_f64().unwrap_or(0.0);
            sum.bytes_per_s += numbers["bytes_per_s"].as_f64().unwrap_or(0.0);
        }
    }
    let calls: serde_json::Map<String, Value> = sums
        .into_iter()
        .map(|(call, sum)| {
            let h = sum.histogram.unwrap_or_else(histogram);
            (
                call,
                call_value(&h, &sum.kinds, sum.bytes, (sum.ops_per_s, sum.bytes_per_s)),
            )
        })
        .collect();
    let files: Vec<&str> = reports.iter().map(|(name, _)| name.as_str()).collect();
    Ok(json!({
        "commit": commit(),
        "merged": files,
        "run": expected,
        "measured_s": measured,
        "calls": calls,
    }))
}

/// The merged result as text: one row per call.
pub fn print(merged: &Value) {
    println!(
        "merged {} reports over {:.1}s",
        merged["merged"].as_array().map_or(0, Vec::len),
        merged["measured_s"].as_f64().unwrap_or(0.0)
    );
    println!(
        "{:>8} {:>12} {:>12} {:>9} {:>8} {:>8} {:>8} {:>8} {:>9} {:>8}  call",
        "errors",
        "ops(total)",
        "ops/s",
        "MiB/s",
        "avg(ms)",
        "p50(ms)",
        "p90(ms)",
        "p99(ms)",
        "p99.9(ms)",
        "max(ms)"
    );
    let Some(calls) = merged["calls"].as_object() else {
        return;
    };
    for (name, call) in calls {
        let latency = &call["latency_ms"];
        let at = |key: &str| format!("{:.1}", latency[key].as_f64().unwrap_or(0.0));
        println!(
            "{:>8} {:>12} {:>12.1} {:>9.2} {:>8} {:>8} {:>8} {:>8} {:>9} {:>8}  {name}",
            call["errors"].as_u64().unwrap_or(0),
            call["ops"].as_u64().unwrap_or(0),
            call["ops_per_s"].as_f64().unwrap_or(0.0),
            call["bytes_per_s"].as_f64().unwrap_or(0.0) / float(1 << 20),
            at("mean"),
            at("p50"),
            at("p90"),
            at("p99"),
            at("p99_9"),
            at("max"),
        );
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use serde_json::json;

    use super::{documents, merge};
    use crate::bench::histograms::{Call, Calls};
    use crate::bench::summary::document;

    fn report(latencies_ms: &[u64], roles: &str) -> serde_json::Value {
        let mut total = Calls::default();
        for ms in latencies_ms {
            total.record(Call::Write, Duration::from_millis(*ms), 100);
        }
        total.error(Call::Write, "Unavailable");
        document(
            &json!({ "roles": roles, "preset": "pub-sub", "topology": "3x1", "seed": 1 }),
            None,
            Duration::from_secs(1),
            &[Call::Write],
            &total,
        )
    }

    #[test]
    fn reports_merge_without_loss() {
        let a = report(&[1, 2, 3], "1 writers");
        let b = report(&[100, 200], "1 writers");
        let merged = merge(&[("a".into(), a), ("b".into(), b)]).unwrap();
        let write = &merged["calls"]["write"];
        assert_eq!(write["ops"], 5);
        assert_eq!(write["bytes"], 500);
        assert_eq!(write["errors"], 2);
        assert_eq!(write["errors_by_kind"]["Unavailable"], 2);
        assert!(
            (write["ops_per_s"].as_f64().unwrap() - 5.0).abs() < 1e-9,
            "rates add"
        );
        let max = write["latency_ms"]["max"].as_f64().unwrap();
        assert!((199.0..=201.0).contains(&max), "{max}");
        let p50 = write["latency_ms"]["p50"].as_f64().unwrap();
        assert!((2.9..=3.1).contains(&p50), "the p50 of all five: {p50}");
        assert_eq!(merged["merged"], json!(["a", "b"]));
    }

    #[test]
    fn reports_of_other_runs_are_refused() {
        let a = report(&[1], "1 writers");
        let b = report(&[1], "2 writers");
        let error = merge(&[("a".into(), a), ("b".into(), b)]).unwrap_err();
        assert!(error.contains("same roles"), "{error}");
        let mut old = report(&[1], "1 writers");
        old["calls"]["write"]
            .as_object_mut()
            .unwrap()
            .remove("histogram");
        assert!(merge(&[("old".into(), old)]).is_err());
        assert!(merge(&[]).is_err());
    }

    #[test]
    fn a_file_holds_one_report_per_line() {
        let text = format!("{}\n{}\n", report(&[1], "x"), report(&[2], "x"));
        assert_eq!(documents(&text).unwrap().len(), 2);
        assert!(documents("{").is_err());
    }
}
