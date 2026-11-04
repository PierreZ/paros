//! Pass/fail limits (#725 (bench phases)), after Redpanda's per-load
//! limits for CI: `--expect 'e2e.p50<5ms,write.p99<20ms,throughput>100MiB/s'`.
//!
//! A limit is `METRIC OP VALUE` with `OP` one of `<`, `<=`, `>`, `>=`.
//! The metrics:
//!
//! - `CALL.STAT`, a latency: `STAT` is `mean`, `p50`, `p90`, `p99`,
//!   `p99.9` or `max`, the value a time (`5ms`, `200us`, `1.5s`);
//! - `CALL.throughput`, the call's record bytes per second (`100MiB/s`);
//! - `CALL.rate`, the call's ops per second (a number);
//! - `CALL.errors`, the call's errors (a number);
//! - `throughput`: the writes' bytes per second; `errors`: every error;
//! - `catch-up`: the time a `--backlog` took to drain (a time).
//!
//! `CALL` is `write`, `read`, `tail`, `e2e`, `set-leader` or `truncate`.
//! A limit reads the report document (`summary.rs`), so it holds for one
//! run and for a merged report alike. A metric the report does not hold
//! (a call the run never made) fails its limit.

use std::fmt;
use std::str::FromStr;

use serde_json::Value;

use super::histograms::Call;
use super::units::{bytes_label, parse_bytes_per_sec, parse_time_nanos};

/// How a limit compares.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Op {
    Below,
    AtMost,
    Above,
    AtLeast,
}

impl Op {
    fn holds(self, value: f64, limit: f64) -> bool {
        match self {
            Op::Below => value < limit,
            Op::AtMost => value <= limit,
            Op::Above => value > limit,
            Op::AtLeast => value >= limit,
        }
    }
}

/// What a metric measures, and so how its value reads and prints.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    /// Milliseconds in the report; a time on the command line.
    Millis,
    /// Seconds in the report; a time on the command line.
    Seconds,
    BytesPerSec,
    Count,
}

/// One metric of the report.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Metric {
    Latency(Call, &'static str),
    CallThroughput(Call),
    CallRate(Call),
    CallErrors(Call),
    Throughput,
    Errors,
    CatchUp,
}

impl Metric {
    fn parse(text: &str) -> Result<Self, String> {
        match text {
            "throughput" => return Ok(Metric::Throughput),
            "errors" => return Ok(Metric::Errors),
            "catch-up" => return Ok(Metric::CatchUp),
            _ => {}
        }
        let (call, stat) = text.split_once('.').ok_or_else(|| {
            format!("{text:?}: a metric is CALL.STAT, throughput, errors or catch-up")
        })?;
        let call = Call::of_name(call).ok_or_else(|| {
            format!("{call:?}: the calls are write, read, tail, e2e, set-leader and truncate")
        })?;
        Ok(match stat {
            "mean" => Metric::Latency(call, "mean"),
            "p50" => Metric::Latency(call, "p50"),
            "p90" => Metric::Latency(call, "p90"),
            "p99" => Metric::Latency(call, "p99"),
            "p99.9" => Metric::Latency(call, "p99_9"),
            "max" => Metric::Latency(call, "max"),
            "throughput" => Metric::CallThroughput(call),
            "rate" => Metric::CallRate(call),
            "errors" => Metric::CallErrors(call),
            _ => {
                return Err(format!(
                    "{stat:?}: a call's metric is mean, p50, p90, p99, p99.9, max, throughput, rate or errors"
                ));
            }
        })
    }

    fn kind(&self) -> Kind {
        match self {
            Metric::Latency(..) => Kind::Millis,
            Metric::CatchUp => Kind::Seconds,
            Metric::CallThroughput(_) | Metric::Throughput => Kind::BytesPerSec,
            Metric::CallRate(_) | Metric::CallErrors(_) | Metric::Errors => Kind::Count,
        }
    }

    /// Its value in `report`, in the report's own unit.
    fn value(&self, report: &Value) -> Option<f64> {
        let calls = &report["calls"];
        let of = |call: Call| calls.get(call.name());
        match self {
            Metric::Latency(call, stat) => of(*call)?["latency_ms"][stat].as_f64(),
            Metric::CallThroughput(call) => of(*call)?["bytes_per_s"].as_f64(),
            Metric::CallRate(call) => of(*call)?["ops_per_s"].as_f64(),
            Metric::CallErrors(call) => of(*call)?["errors"].as_f64(),
            Metric::Throughput => of(Call::Write)?["bytes_per_s"].as_f64(),
            Metric::Errors => Some(
                calls
                    .as_object()?
                    .values()
                    .filter_map(|c| c["errors"].as_f64())
                    .sum(),
            ),
            Metric::CatchUp => report["catch_up"]["seconds"].as_f64(),
        }
    }
}

/// One limit.
#[derive(Clone, Debug, PartialEq)]
pub struct Limit {
    text: String,
    metric: Metric,
    op: Op,
    /// In the report's unit for the metric.
    bound: f64,
}

impl FromStr for Limit {
    type Err = String;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let text = text.trim();
        let at = text
            .find(['<', '>'])
            .ok_or_else(|| format!("{text:?}: a limit is METRIC<VALUE or METRIC>VALUE"))?;
        let (metric, rest) = text.split_at(at);
        let (op, value) = if let Some(value) = rest.strip_prefix("<=") {
            (Op::AtMost, value)
        } else if let Some(value) = rest.strip_prefix(">=") {
            (Op::AtLeast, value)
        } else if let Some(value) = rest.strip_prefix('<') {
            (Op::Below, value)
        } else {
            (Op::Above, &rest[1..])
        };
        let metric = Metric::parse(metric.trim())?;
        let value = value.trim();
        let bound = match metric.kind() {
            Kind::Millis => parse_time_nanos(value)? / 1e6,
            Kind::Seconds => parse_time_nanos(value)? / 1e9,
            Kind::BytesPerSec => parse_bytes_per_sec(value)?,
            Kind::Count => value
                .parse::<f64>()
                .ok()
                .filter(|v| v.is_finite())
                .ok_or_else(|| format!("{value:?}: a count is a number"))?,
        };
        Ok(Self {
            text: text.to_string(),
            metric,
            op,
            bound,
        })
    }
}

/// The limits of one `--expect`.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct Limits(Vec<Limit>);

impl FromStr for Limits {
    type Err = String;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        text.split(',')
            .map(str::trim)
            .filter(|part| !part.is_empty())
            .map(str::parse)
            .collect::<Result<Vec<_>, _>>()
            .map(Limits)
    }
}

/// One limit against one report.
#[derive(Clone, Debug, PartialEq)]
pub struct Verdict {
    pub limit: String,
    /// The value, printed in the metric's unit; `None` when the report
    /// does not hold it.
    pub value: Option<String>,
    pub passed: bool,
}

impl fmt::Display for Verdict {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "expect {}: {} {}",
            self.limit,
            self.value.as_deref().unwrap_or("no value"),
            if self.passed { "ok" } else { "FAILED" }
        )
    }
}

impl Limits {
    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Each limit against `report`.
    pub fn check(&self, report: &Value) -> Vec<Verdict> {
        self.0
            .iter()
            .map(|limit| {
                let value = limit.metric.value(report);
                Verdict {
                    limit: limit.text.clone(),
                    value: value.map(|v| print(limit.metric.kind(), v)),
                    passed: value.is_some_and(|v| limit.op.holds(v, limit.bound)),
                }
            })
            .collect()
    }
}

fn print(kind: Kind, value: f64) -> String {
    match kind {
        Kind::Millis => format!("{value:.2}ms"),
        Kind::Seconds => format!("{value:.2}s"),
        Kind::BytesPerSec => format!("{}/s", bytes_label(value)),
        Kind::Count => format!("{value}"),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{Limit, Limits};

    fn report() -> serde_json::Value {
        json!({
            "calls": {
                "write": {
                    "ops_per_s": 1000.0,
                    "bytes_per_s": 200.0 * 1024.0 * 1024.0,
                    "errors": 2,
                    "latency_ms": { "p50": 1.5, "p99": 30.0, "p99_9": 40.0 },
                },
                "e2e": { "errors": 0, "latency_ms": { "p50": 4.0 } },
            },
            "catch_up": { "seconds": 12.5 },
        })
    }

    #[test]
    fn limits_parse_and_check_against_a_report() {
        let limits: Limits =
            "e2e.p50<5ms, write.p99<20ms,throughput>100MiB/s,errors<=2,write.p99.9<1s,catch-up<10s"
                .parse()
                .unwrap();
        let verdicts = limits.check(&report());
        let passed: Vec<bool> = verdicts.iter().map(|v| v.passed).collect();
        assert_eq!(passed, [true, false, true, true, true, false]);
        assert_eq!(verdicts[0].value.as_deref(), Some("4.00ms"));
        assert_eq!(verdicts[2].value.as_deref(), Some("200.0 MiB/s"));
        assert_eq!(
            verdicts[1].to_string(),
            "expect write.p99<20ms: 30.00ms FAILED"
        );
    }

    #[test]
    fn a_metric_the_report_lacks_fails() {
        let limits: Limits = "read.p99<5ms".parse().unwrap();
        let verdict = &limits.check(&report())[0];
        assert!(!verdict.passed);
        assert_eq!(verdict.value, None);
        assert!(verdict.to_string().contains("no value"));
    }

    #[test]
    fn a_bad_limit_is_refused() {
        for bad in [
            "e2e.p50",
            "e2e.p42<5ms",
            "scan.p50<5ms",
            "e2e.p50<5",
            "throughput>fast",
            "errors<some",
            "speed>1",
        ] {
            assert!(bad.parse::<Limit>().is_err(), "{bad:?} is refused");
        }
        assert!("".parse::<Limits>().unwrap().is_empty());
        assert!(">=".parse::<Limit>().is_err());
    }
}
