//! The bench's latency histograms and their export (#517 (`parosctl
//! bench`)), after `CockroachDB`'s load generator.
//!
//! Each client records into histograms of its own, one per call. On every
//! tick the registry swaps them for empty ones and merges them into the
//! interval (printed, exported) and the total. Latencies are nanoseconds,
//! three significant figures, from 1 ns to 100 s; a longer call is clamped
//! to 100 s. Only successful calls go in a histogram; errors are counted
//! apart.
//!
//! The export is one JSON line per tick per call with the raw histogram
//! (`HdrHistogram`'s V2 encoding, base64), never percentiles: a later tool
//! merges runs and decides from it. The bench never decides.

use std::cell::RefCell;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use hdrhistogram::Histogram;
use hdrhistogram::serialization::{Serializer, V2Serializer};
use serde_json::json;

/// The longest latency a histogram tracks: 100 s, in nanoseconds.
const MAX_NANOS: u64 = 100_000_000_000;

/// How many calls the bench knows.
pub const CALLS: usize = 5;

/// The calls the bench makes, by their stable names (#194 (throughput
/// benchmark)).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Call {
    /// A `Write` at the journal's tail, as its writer.
    Write,
    /// A `Read` of the last records the bench wrote.
    ReadTail,
    /// A `Read` from a cursor that starts at the journal's first position:
    /// a reader that catches up.
    ReadStart,
    /// A `SetLeader` under a fresh uuid: the writer takes a new term.
    SetLeader,
    /// A `Truncate` on the `--truncate-every` schedule.
    Truncate,
}

impl Call {
    pub const ALL: [Call; CALLS] = [
        Call::Write,
        Call::ReadTail,
        Call::ReadStart,
        Call::SetLeader,
        Call::Truncate,
    ];

    /// The stable name: in the console, in `--mix` and in the export.
    pub fn name(self) -> &'static str {
        match self {
            Call::Write => "write",
            Call::ReadTail => "read-tail",
            Call::ReadStart => "read-start",
            Call::SetLeader => "set-leader",
            Call::Truncate => "truncate",
        }
    }

    /// The call named `name`.
    pub fn of_name(name: &str) -> Option<Call> {
        Call::ALL.into_iter().find(|call| call.name() == name)
    }

    fn index(self) -> usize {
        self as usize
    }
}

/// An empty latency histogram.
pub fn histogram() -> Histogram<u64> {
    Histogram::new_with_bounds(1, MAX_NANOS, 3).expect("1 ns to 100 s at 3 figures is valid")
}

/// What one client, or one interval, holds: a histogram, an error count
/// and the record bytes carried, per call.
#[derive(Clone, Debug)]
pub struct Calls {
    pub histograms: [Histogram<u64>; CALLS],
    pub errors: [u64; CALLS],
    pub bytes: [u64; CALLS],
}

impl Default for Calls {
    fn default() -> Self {
        Self {
            histograms: std::array::from_fn(|_| histogram()),
            errors: [0; CALLS],
            bytes: [0; CALLS],
        }
    }
}

impl Calls {
    /// A successful `call` that took `latency` (clamped into range) and
    /// carried `bytes` of records (written or read).
    pub fn record(&mut self, call: Call, latency: Duration, bytes: u64) {
        let nanos = u64::try_from(latency.as_nanos()).unwrap_or(u64::MAX);
        self.histograms[call.index()].saturating_record(nanos.clamp(1, MAX_NANOS));
        self.bytes[call.index()] += bytes;
    }

    /// A failed `call`.
    pub fn error(&mut self, call: Call) {
        self.errors[call.index()] += 1;
    }

    pub fn of(&self, call: Call) -> &Histogram<u64> {
        &self.histograms[call.index()]
    }

    pub fn errors_of(&self, call: Call) -> u64 {
        self.errors[call.index()]
    }

    pub fn bytes_of(&self, call: Call) -> u64 {
        self.bytes[call.index()]
    }

    /// Add `other` into this one.
    pub fn add(&mut self, other: &Calls) {
        for call in Call::ALL {
            self.histograms[call.index()]
                .add(other.of(call))
                .expect("histograms of the same bounds add");
            self.errors[call.index()] += other.errors_of(call);
            self.bytes[call.index()] += other.bytes_of(call);
        }
    }
}

/// Every client's histograms, and the total since the last reset.
#[derive(Default)]
pub struct Registry {
    clients: Vec<Rc<RefCell<Calls>>>,
    pub total: Calls,
}

impl Registry {
    /// The histograms of a new client.
    pub fn client(&mut self) -> Rc<RefCell<Calls>> {
        let calls = Rc::new(RefCell::new(Calls::default()));
        self.clients.push(calls.clone());
        calls
    }

    /// Swap every client's histograms for empty ones: their merge is the
    /// interval, also added to the total.
    pub fn tick(&mut self) -> Calls {
        let mut interval = Calls::default();
        for client in &self.clients {
            let calls = std::mem::take(&mut *client.borrow_mut());
            interval.add(&calls);
        }
        self.total.add(&interval);
        interval
    }

    /// Drop everything recorded so far (the ramp ends).
    pub fn reset(&mut self) {
        for client in &self.clients {
            *client.borrow_mut() = Calls::default();
        }
        self.total = Calls::default();
    }
}

/// The `--histograms` file: lines go to a temporary name, synced and
/// renamed when the run ends.
pub struct Export {
    path: PathBuf,
    temporary: PathBuf,
    file: BufWriter<File>,
    seed: u64,
}

impl Export {
    pub fn create(path: &Path, seed: u64) -> std::io::Result<Self> {
        let mut name = path.as_os_str().to_owned();
        name.push(".tmp");
        let temporary = PathBuf::from(name);
        let file = BufWriter::new(File::create(&temporary)?);
        Ok(Self {
            path: path.to_path_buf(),
            temporary,
            file,
            seed,
        })
    }

    /// One line per call of `calls` for the interval that ended `elapsed`
    /// into the run (`unix_ms`: the wall clock then).
    pub fn tick(
        &mut self,
        (elapsed, unix_ms): (Duration, u64),
        calls: &[Call],
        interval: &Calls,
    ) -> std::io::Result<()> {
        for &call in calls {
            let line = json!({
                "name": call.name(),
                "seed": self.seed,
                "unix_ms": unix_ms,
                "elapsed_ms": u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX),
                "errors": interval.errors_of(call),
                "bytes": interval.bytes_of(call),
                "histogram": encode(interval.of(call)),
            });
            writeln!(self.file, "{line}")?;
        }
        Ok(())
    }

    /// Flush, sync, and rename to the final name.
    pub fn finish(self) -> std::io::Result<()> {
        let file = self
            .file
            .into_inner()
            .map_err(std::io::IntoInnerError::into_error)?;
        file.sync_all()?;
        drop(file);
        std::fs::rename(&self.temporary, &self.path)
    }
}

/// A histogram in `HdrHistogram`'s V2 encoding, base64.
fn encode(histogram: &Histogram<u64>) -> String {
    let mut bytes = Vec::new();
    V2Serializer::new()
        .serialize(histogram, &mut bytes)
        .expect("a histogram serializes into memory");
    STANDARD.encode(bytes)
}

/// The reading half of the export, for the tool that merges runs (#194
/// (throughput benchmark)); only the tests use it in `paros-bench`.
#[cfg_attr(not(test), allow(dead_code))]
pub mod read {
    use std::path::Path;

    use base64::Engine;
    use base64::engine::general_purpose::STANDARD;
    use hdrhistogram::Histogram;
    use hdrhistogram::serialization::Deserializer;

    /// One line of an export.
    pub struct Tick {
        pub name: String,
        pub seed: u64,
        pub elapsed_ms: u64,
        pub errors: u64,
        pub histogram: Histogram<u64>,
    }

    /// Every line of the export at `path`.
    pub fn file(path: &Path) -> Result<Vec<Tick>, String> {
        let text = std::fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
        text.lines().filter(|l| !l.is_empty()).map(line).collect()
    }

    fn line(text: &str) -> Result<Tick, String> {
        let doc: serde_json::Value = serde_json::from_str(text).map_err(|e| e.to_string())?;
        let field = |key: &str| doc.get(key).ok_or(format!("no {key}"));
        let number = |key: &str| field(key)?.as_u64().ok_or(format!("{key} is not a number"));
        let bytes = STANDARD
            .decode(
                field("histogram")?
                    .as_str()
                    .ok_or("histogram is not text")?,
            )
            .map_err(|e| e.to_string())?;
        let histogram = Deserializer::new()
            .deserialize(&mut bytes.as_slice())
            .map_err(|e| format!("{e:?}"))?;
        Ok(Tick {
            name: field("name")?
                .as_str()
                .ok_or("name is not text")?
                .to_string(),
            seed: number("seed")?,
            elapsed_ms: number("elapsed_ms")?,
            errors: number("errors")?,
            histogram,
        })
    }

    /// The merge of every tick of `name`, from any number of runs.
    pub fn merge<'a>(ticks: impl IntoIterator<Item = &'a Tick>, name: &str) -> Histogram<u64> {
        let mut merged = super::histogram();
        for tick in ticks.into_iter().filter(|t| t.name == name) {
            merged
                .add(&tick.histogram)
                .expect("histograms of the same bounds add");
        }
        merged
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{Call, Calls, Export, Registry, read};

    fn run(path: &std::path::Path, seed: u64, latencies_ms: &[u64]) {
        let mut registry = Registry::default();
        let client = registry.client();
        let mut export = Export::create(path, seed).unwrap();
        for (second, ms) in latencies_ms.iter().enumerate() {
            client
                .borrow_mut()
                .record(Call::Write, Duration::from_millis(*ms), 10);
            client.borrow_mut().error(Call::ReadTail);
            let interval = registry.tick();
            export
                .tick(
                    (Duration::from_secs(second as u64 + 1), 0),
                    &[Call::Write, Call::ReadTail],
                    &interval,
                )
                .unwrap();
        }
        export.finish().unwrap();
    }

    #[test]
    fn an_export_reads_back_and_two_runs_merge() {
        let dir = tempfile::tempdir().unwrap();
        let (a, b) = (dir.path().join("a.json"), dir.path().join("b.json"));
        run(&a, 1, &[1, 2, 3]);
        run(&b, 2, &[100, 200]);
        assert!(!dir.path().join("a.json.tmp").exists(), "renamed when done");

        let first = read::file(&a).unwrap();
        assert_eq!(first.len(), 6, "a line per tick per call");
        assert_eq!(first[0].name, "write");
        assert_eq!(first[0].seed, 1);
        assert_eq!(first[5].elapsed_ms, 3000);
        assert_eq!(first[1].errors, 1);

        let second = read::file(&b).unwrap();
        let both: Vec<_> = first.iter().chain(&second).collect();
        let merged = read::merge(both.iter().copied(), "write");
        assert_eq!(merged.len(), 5);
        // Three significant figures: round to the millisecond.
        let ms = |nanos: u64| (nanos + 500_000) / 1_000_000;
        assert_eq!(ms(merged.value_at_quantile(0.0)), 1);
        assert_eq!(ms(merged.value_at_quantile(0.5)), 3);
        assert_eq!(ms(merged.max()), 200);
        assert_eq!(read::merge(both.iter().copied(), "read-tail").len(), 0);
        let errors: u64 = both
            .iter()
            .filter(|t| t.name == "read-tail")
            .map(|t| t.errors)
            .sum();
        assert_eq!(errors, 5);
    }

    #[test]
    fn a_tick_swaps_and_a_reset_clears() {
        let mut registry = Registry::default();
        let client = registry.client();
        client.borrow_mut().record(Call::Write, Duration::ZERO, 1);
        client
            .borrow_mut()
            .record(Call::Write, Duration::from_secs(1_000), 2);
        let interval = registry.tick();
        assert_eq!(
            interval.of(Call::Write).len(),
            2,
            "out-of-range values are clamped"
        );
        assert_eq!(client.borrow().of(Call::Write).len(), 0, "swapped out");
        assert_eq!(registry.total.of(Call::Write).len(), 2);
        assert_eq!(registry.total.bytes_of(Call::Write), 3);
        registry.reset();
        assert_eq!(registry.total.of(Call::Write).len(), 0);
        let mut sum = Calls::default();
        sum.add(&interval);
        sum.add(&interval);
        assert_eq!(sum.of(Call::Write).len(), 4);
        assert_eq!(sum.bytes_of(Call::Write), 6);
        assert_eq!(Call::of_name("read-start"), Some(Call::ReadStart));
        assert_eq!(Call::of_name("scan"), None);
    }
}
