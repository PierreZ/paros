//! The bench's load (#724 (bench roles)): roles, not a call mix, after
//! Redpanda's `OpenMessaging` Benchmark (producers and consumers per
//! topic). Each client has one role on its journal:
//!
//! - a **writer** appends `--records-per-write` records at the tail;
//! - a **tailer** follows the journal from the tail and reads each record as
//!   it lands: the `e2e` latency is the time it sees a record minus the
//!   time the writer sent it;
//! - a **reader** reads `--records-per-read` records from `--read-from
//!   start|random|tail`, of a journal `--journal-distribution` picks.
//!
//! Churn runs on a schedule: `--set-leader-every` and `--truncate-every`.
//!
//! A `--preset` is a set of these flags (like YCSB's A to F): it only sets
//! defaults, and an explicit flag wins. Without a preset and without role
//! flags, the load is `append-only`.

use std::str::FromStr;
use std::time::Duration;

use clap::Args;

use super::pace::{Moving, Pace, Shape, Unit};
use super::records::{HEADER, Rng};
use super::units::{float, parse_bytes_per_sec, parse_duration, parse_rate, parse_size};

/// A named load.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Preset {
    /// Writers only: 2 per journal.
    AppendOnly,
    /// 1 writer and 4 tailers per journal.
    PubSub,
    /// `pub-sub` with a 1 GiB backlog (#725 (bench phases)).
    CatchUp,
    /// `pub-sub` with a set-leader every 5 s.
    LeaderChurn,
}

impl Preset {
    pub const ALL: [Preset; 4] = [
        Preset::AppendOnly,
        Preset::PubSub,
        Preset::CatchUp,
        Preset::LeaderChurn,
    ];

    pub fn name(self) -> &'static str {
        match self {
            Preset::AppendOnly => "append-only",
            Preset::PubSub => "pub-sub",
            Preset::CatchUp => "catch-up",
            Preset::LeaderChurn => "leader-churn",
        }
    }

    /// The flags the preset sets, as defaults.
    fn defaults(self) -> Defaults {
        let pub_sub = Defaults {
            writers_per_journal: Some(1),
            tailers_per_journal: Some(4),
            ..Defaults::default()
        };
        match self {
            Preset::AppendOnly => Defaults {
                writers_per_journal: Some(2),
                ..Defaults::default()
            },
            Preset::PubSub => pub_sub,
            Preset::CatchUp => Defaults {
                backlog: Some(1 << 30),
                ..pub_sub
            },
            Preset::LeaderChurn => Defaults {
                set_leader_every: Some(Duration::from_secs(5)),
                ..pub_sub
            },
        }
    }
}

impl FromStr for Preset {
    type Err = String;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        Preset::ALL
            .into_iter()
            .find(|p| p.name() == text)
            .ok_or_else(|| {
                format!("{text:?}: the presets are append-only, pub-sub, catch-up and leader-churn")
            })
    }
}

/// What a preset sets: `None` leaves the flag at its own default.
#[derive(Clone, Copy, Debug, Default)]
struct Defaults {
    writers_per_journal: Option<u64>,
    tailers_per_journal: Option<u64>,
    backlog: Option<u64>,
    set_leader_every: Option<Duration>,
}

/// Where a reader reads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReadFrom {
    /// A cursor from the journal's first position, page after page; at the
    /// tail it starts again (a reader that catches up).
    Start,
    /// A position drawn between the floor and the tail.
    Random,
    /// The last records the bench knows of.
    Tail,
}

impl FromStr for ReadFrom {
    type Err = String;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        match text {
            "start" => Ok(ReadFrom::Start),
            "random" => Ok(ReadFrom::Random),
            "tail" => Ok(ReadFrom::Tail),
            _ => Err(format!(
                "{text:?}: a reader reads from start, random or tail"
            )),
        }
    }
}

impl ReadFrom {
    pub fn name(self) -> &'static str {
        match self {
            ReadFrom::Start => "start",
            ReadFrom::Random => "random",
            ReadFrom::Tail => "tail",
        }
    }
}

/// How a reader picks the journal of each call.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Distribution {
    Uniform,
    /// Journal `k` (from 0) drawn with weight `1 / (k + 1)^0.99`, YCSB's
    /// constant: the first journals are hot.
    Zipfian,
}

impl FromStr for Distribution {
    type Err = String;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        match text {
            "uniform" => Ok(Distribution::Uniform),
            "zipfian" => Ok(Distribution::Zipfian),
            _ => Err(format!(
                "{text:?}: the distributions are uniform and zipfian"
            )),
        }
    }
}

impl Distribution {
    pub fn name(self) -> &'static str {
        match self {
            Distribution::Uniform => "uniform",
            Distribution::Zipfian => "zipfian",
        }
    }
}

/// YCSB's Zipfian constant.
const ZIPF_THETA: f64 = 0.99;

/// Picks a journal index under a [`Distribution`].
#[derive(Clone, Debug)]
pub struct Picker {
    /// The running sum of the weights, ending at 1 (empty for uniform).
    cumulative: Vec<f64>,
    journals: u64,
}

impl Picker {
    pub fn new(distribution: Distribution, journals: u64) -> Self {
        let cumulative = match distribution {
            Distribution::Uniform => Vec::new(),
            Distribution::Zipfian => {
                let weights: Vec<f64> = (0..journals)
                    .map(|k| 1.0 / float(k + 1).powf(ZIPF_THETA))
                    .collect();
                let total: f64 = weights.iter().sum();
                let mut sum = 0.0;
                weights
                    .iter()
                    .map(|w| {
                        sum += w / total;
                        sum
                    })
                    .collect()
            }
        };
        Self {
            cumulative,
            journals,
        }
    }

    /// The next journal index.
    pub fn pick(&self, rng: &mut Rng) -> usize {
        let index = if self.cumulative.is_empty() {
            rng.below(self.journals)
        } else {
            // 53 random bits: a uniform float in [0, 1).
            let at = float(rng.next_u64() >> 11) / float(1 << 53);
            self.cumulative
                .iter()
                .position(|c| at < *c)
                .map_or(self.journals - 1, |k| k as u64)
        };
        usize::try_from(index).expect("a journal index fits")
    }
}

/// The write flags of `run` and `init`: the writers, their records and
/// the rate.
#[derive(Args, Debug, Clone)]
pub struct WriteFlags {
    /// Writers per journal: each appends at the tail. The writers of one
    /// journal share its one writer, and keep their writes in flight on it
    /// at once (#597). Default 2.
    #[arg(long)]
    pub writers_per_journal: Option<u64>,
    /// The records of one write.
    #[arg(long, default_value = "1", value_parser = clap::value_parser!(u64).range(1..))]
    pub records_per_write: u64,
    /// The bytes of one record, the same for each: sets both
    /// `--min-record-bytes` and `--max-record-bytes`.
    #[arg(long, value_parser = parse_size, conflicts_with_all = ["min_record_bytes", "max_record_bytes"])]
    pub record_size: Option<u64>,
    /// The smallest record, in bytes (a record holds its 24-byte header:
    /// the seed, its position and its send time). Each record's size is
    /// drawn between the two, uniform. Default 256.
    #[arg(long, value_parser = parse_size)]
    pub min_record_bytes: Option<u64>,
    /// The largest record, in bytes. Default `--min-record-bytes`.
    #[arg(long, value_parser = parse_size)]
    pub max_record_bytes: Option<u64>,
    /// Run open loop: this many calls per second over every writer and
    /// reader together, each call counted from the moment it was due.
    /// Without a rate, each client calls again as soon as its last call
    /// ends (closed loop).
    #[arg(long, value_parser = parse_rate, conflicts_with = "max_bytes_per_sec")]
    pub max_rate: Option<f64>,
    /// Run open loop at this many record bytes per second (`100MiB`), over
    /// every writer and reader together.
    #[arg(long, value_parser = parse_bytes_per_sec)]
    pub max_bytes_per_sec: Option<f64>,
    /// The low end of a moving `--max-rate`, in calls per second.
    #[arg(long, value_parser = parse_rate, requires = "max_rate")]
    pub min_rate: Option<f64>,
    /// The low end of a moving `--max-bytes-per-sec`.
    #[arg(long, value_parser = parse_bytes_per_sec, requires = "max_bytes_per_sec")]
    pub min_bytes_per_sec: Option<f64>,
    /// The period of a moving rate.
    #[arg(long, default_value = "30s", value_parser = parse_duration)]
    pub rate_period: Duration,
    /// How a moving rate moves between its low and high ends: `sin`,
    /// `square` or `pulse`.
    #[arg(long, default_value = "sin")]
    pub rate_shape: Shape,
}

/// The load flags of `run`. An unset flag takes the preset's value, else
/// its own default.
#[derive(Args, Debug, Clone)]
pub struct LoadFlags {
    /// A named load: `append-only` (2 writers per journal), `pub-sub` (1
    /// writer, 4 tailers per journal), `catch-up` (`pub-sub` with a 1 GiB
    /// `--backlog`) or `leader-churn` (`pub-sub` with `--set-leader-every
    /// 5s`). It only sets defaults: an explicit flag wins.
    #[arg(long)]
    pub preset: Option<Preset>,
    #[command(flatten)]
    pub write: WriteFlags,
    /// Tailers per journal: each follows the journal from the tail and
    /// reads each record as it lands (the `e2e` latency). Default 0.
    #[arg(long)]
    pub tailers_per_journal: Option<u64>,
    /// Readers per journal: each reads `--records-per-read` records a
    /// call, from `--read-from`, of a journal `--journal-distribution`
    /// picks. Default 0.
    #[arg(long)]
    pub readers_per_journal: Option<u64>,
    /// The records of one reader call.
    #[arg(long, default_value = "1", value_parser = clap::value_parser!(u64).range(1..))]
    pub records_per_read: u64,
    /// Where a reader reads: `start` (a cursor from the first position),
    /// `random` (a position between the floor and the tail) or `tail` (the
    /// last records).
    #[arg(long, default_value = "start")]
    pub read_from: ReadFrom,
    /// How a reader picks the journal of each call: `uniform` or
    /// `zipfian` (the first journals are hot).
    #[arg(long, default_value = "uniform")]
    pub journal_distribution: Distribution,
    /// Take a new term on every journal this often, under a fresh uuid
    /// (`0s`: never).
    #[arg(long, value_parser = parse_duration)]
    pub set_leader_every: Option<Duration>,
    /// Truncate every journal this often, keeping its last
    /// `--truncate-keep` records (`0s`: never).
    #[arg(long, default_value = "0s", value_parser = parse_duration)]
    pub truncate_every: Duration,
    /// The records a truncation keeps.
    #[arg(long, default_value = "10000")]
    pub truncate_keep: u64,
    /// Catch-up (#725 (bench phases)): once the ramp and the warmup end,
    /// the tailers stop, the writers write this many bytes (`1GiB`), then
    /// the tailers start again where they stopped; the report adds how
    /// long they took to catch up, and the drain rate.
    #[arg(long, value_parser = parse_size)]
    pub backlog: Option<u64>,
}

/// The load, every flag resolved.
#[derive(Clone, Debug, PartialEq)]
pub struct Load {
    pub preset: Option<Preset>,
    pub writers_per_journal: u64,
    pub tailers_per_journal: u64,
    pub readers_per_journal: u64,
    pub records_per_write: u64,
    pub records_per_read: u64,
    pub min_record_bytes: u64,
    pub max_record_bytes: u64,
    pub read_from: ReadFrom,
    pub journal_distribution: Distribution,
    pub pace: Option<Pace>,
    pub set_leader_every: Duration,
    pub truncate_every: Duration,
    pub truncate_keep: u64,
    /// The backlog, in bytes: `0` for none.
    pub backlog: u64,
}

impl LoadFlags {
    /// The load flags of `init`: its writers only.
    pub fn writers_only(write: WriteFlags) -> Self {
        Self {
            preset: None,
            write,
            tailers_per_journal: None,
            readers_per_journal: None,
            records_per_read: 1,
            read_from: ReadFrom::Start,
            journal_distribution: Distribution::Uniform,
            set_leader_every: None,
            truncate_every: Duration::ZERO,
            truncate_keep: 0,
            backlog: None,
        }
    }

    /// The load: each flag, else the preset's value, else its default;
    /// with the flags the preset set, to print.
    ///
    /// # Errors
    ///
    /// The flags cannot hold a load.
    pub fn resolve(&self) -> Result<(Load, Vec<String>), String> {
        let preset = self.preset.unwrap_or(Preset::AppendOnly);
        let defaults = preset.defaults();
        let mut set = Vec::new();
        let mut pick = |flag: &str, given: Option<u64>, from_preset: Option<u64>, base: u64| match (
            given,
            from_preset,
        ) {
            (Some(value), _) => value,
            (None, Some(value)) => {
                set.push(format!("--{flag} {value}"));
                value
            }
            (None, None) => base,
        };
        let writers = pick(
            "writers-per-journal",
            self.write.writers_per_journal,
            defaults.writers_per_journal,
            0,
        );
        let tailers = pick(
            "tailers-per-journal",
            self.tailers_per_journal,
            defaults.tailers_per_journal,
            0,
        );
        let readers = self.readers_per_journal.unwrap_or(0);
        let backlog = pick("backlog", self.backlog, defaults.backlog, 0);
        let set_leader_every = match (self.set_leader_every, defaults.set_leader_every) {
            (Some(every), _) => every,
            (None, Some(every)) => {
                set.push(format!("--set-leader-every {}s", every.as_secs()));
                every
            }
            (None, None) => Duration::ZERO,
        };
        if self.preset.is_none() {
            // The base load is append-only: no preset was named to print.
            set.clear();
        }
        let write = &self.write;
        let (min_record_bytes, max_record_bytes) = if let Some(size) = write.record_size {
            (size, size)
        } else {
            let min = write.min_record_bytes.unwrap_or(256);
            (min, write.max_record_bytes.unwrap_or(min))
        };
        let pace = write.pace()?;
        let load = Load {
            preset: self.preset,
            writers_per_journal: writers,
            tailers_per_journal: tailers,
            readers_per_journal: readers,
            records_per_write: write.records_per_write,
            records_per_read: self.records_per_read,
            min_record_bytes,
            max_record_bytes,
            read_from: self.read_from,
            journal_distribution: self.journal_distribution,
            pace,
            set_leader_every,
            truncate_every: self.truncate_every,
            truncate_keep: self.truncate_keep,
            backlog,
        };
        load.check()?;
        Ok((load, set))
    }
}

impl WriteFlags {
    fn pace(&self) -> Result<Option<Pace>, String> {
        let (unit, max, min) = match (self.max_rate, self.max_bytes_per_sec) {
            (Some(max), _) => (Unit::Calls, max, self.min_rate),
            (None, Some(max)) => (Unit::Bytes, max, self.min_bytes_per_sec),
            (None, None) => return Ok(None),
        };
        let moving = match min {
            Some(min) if min > max => {
                return Err(format!(
                    "the low end of the rate ({min}) is above its high end ({max})"
                ));
            }
            Some(min) => {
                if self.rate_period.is_zero() {
                    return Err("a moving rate needs a --rate-period above 0s".into());
                }
                Some(Moving {
                    min,
                    period: self.rate_period,
                    shape: self.rate_shape,
                })
            }
            None => None,
        };
        Ok(Some(Pace { unit, max, moving }))
    }
}

impl Load {
    /// Refuse a load that cannot run.
    fn check(&self) -> Result<(), String> {
        if self.min_record_bytes < HEADER as u64 {
            return Err(format!(
                "a record holds its {HEADER}-byte header: --min-record-bytes is at least {HEADER}"
            ));
        }
        if self.max_record_bytes < self.min_record_bytes {
            return Err("--max-record-bytes is below --min-record-bytes".into());
        }
        if self.writers_per_journal + self.tailers_per_journal + self.readers_per_journal == 0 {
            return Err("the load has no client: give writers, tailers or readers".into());
        }
        if self.backlog > 0 && (self.writers_per_journal == 0 || self.tailers_per_journal == 0) {
            return Err("--backlog needs writers and tailers".into());
        }
        Ok(())
    }

    /// Whether the run writes: a writer, a set-leader or a truncation needs
    /// each journal's writer.
    pub fn writes(&self) -> bool {
        self.writers_per_journal > 0 || self.churns() || self.truncates()
    }

    pub fn churns(&self) -> bool {
        !self.set_leader_every.is_zero()
    }

    pub fn truncates(&self) -> bool {
        !self.truncate_every.is_zero()
    }

    /// A record's mean size.
    pub fn mean_record_bytes(&self) -> f64 {
        float(self.min_record_bytes + self.max_record_bytes) / 2.0
    }

    /// A record's size, drawn.
    pub fn record_size(&self, rng: &mut Rng) -> usize {
        let spread = self.max_record_bytes - self.min_record_bytes;
        let size = self.min_record_bytes
            + if spread == 0 {
                0
            } else {
                rng.below(spread + 1)
            };
        usize::try_from(size).expect("a record size fits")
    }

    /// The roles as a label: `2 writers, 4 tailers, 0 readers per journal`.
    pub fn roles(&self) -> String {
        format!(
            "{} writers, {} tailers, {} readers per journal",
            self.writers_per_journal, self.tailers_per_journal, self.readers_per_journal
        )
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use clap::Parser;

    use super::{Distribution, LoadFlags, Picker, Preset, ReadFrom};
    use crate::bench::pace::{Shape, Unit};
    use crate::bench::records::Rng;

    #[derive(Parser)]
    struct Cli {
        #[command(flatten)]
        flags: LoadFlags,
    }

    fn flags(args: &[&str]) -> LoadFlags {
        let mut all = vec!["bench"];
        all.extend_from_slice(args);
        Cli::try_parse_from(all).expect("the flags parse").flags
    }

    #[test]
    fn without_a_preset_the_load_is_append_only() {
        let (load, set) = flags(&[]).resolve().unwrap();
        assert_eq!(load.writers_per_journal, 2);
        assert_eq!(load.tailers_per_journal, 0);
        assert_eq!(load.readers_per_journal, 0);
        assert_eq!((load.min_record_bytes, load.max_record_bytes), (256, 256));
        assert!(set.is_empty(), "no preset to print");
        assert!(load.writes());
        assert_eq!(load.pace, None);
    }

    #[test]
    fn a_preset_sets_defaults_and_an_explicit_flag_wins() {
        let (load, set) = flags(&["--preset", "pub-sub"]).resolve().unwrap();
        assert_eq!((load.writers_per_journal, load.tailers_per_journal), (1, 4));
        assert_eq!(set, ["--writers-per-journal 1", "--tailers-per-journal 4"]);

        let (load, set) = flags(&["--preset", "pub-sub", "--tailers-per-journal", "2"])
            .resolve()
            .unwrap();
        assert_eq!(load.tailers_per_journal, 2, "the flag wins");
        assert_eq!(set, ["--writers-per-journal 1"]);

        let (load, _) = flags(&["--preset", "catch-up"]).resolve().unwrap();
        assert_eq!(load.backlog, 1 << 30);
        let (load, set) = flags(&["--preset", "leader-churn"]).resolve().unwrap();
        assert_eq!(load.set_leader_every, Duration::from_secs(5));
        assert!(set.contains(&"--set-leader-every 5s".to_string()));
        let (load, _) = flags(&["--preset", "leader-churn", "--set-leader-every", "0s"])
            .resolve()
            .unwrap();
        assert!(!load.churns(), "0s turns the preset's churn off");
        let (load, _) = LoadFlags::writers_only(flags(&["--preset", "pub-sub"]).write)
            .resolve()
            .unwrap();
        assert_eq!(
            (load.writers_per_journal, load.tailers_per_journal),
            (2, 0),
            "init writes only"
        );
    }

    #[test]
    fn record_sizes_and_rates_resolve() {
        let (load, _) = flags(&["--record-size", "1KiB"]).resolve().unwrap();
        assert_eq!((load.min_record_bytes, load.max_record_bytes), (1024, 1024));
        let (load, _) = flags(&["--min-record-bytes", "64", "--max-record-bytes", "4096"])
            .resolve()
            .unwrap();
        assert!((load.mean_record_bytes() - 2080.0).abs() < 1e-9);
        let mut rng = Rng::of(&[3]);
        for _ in 0..1000 {
            assert!((64..=4096).contains(&load.record_size(&mut rng)));
        }
        let (load, _) = flags(&[
            "--max-rate",
            "2000",
            "--min-rate",
            "200",
            "--rate-shape",
            "square",
            "--rate-period",
            "10s",
        ])
        .resolve()
        .unwrap();
        let pace = load.pace.unwrap();
        assert_eq!(pace.unit, Unit::Calls);
        let moving = pace.moving.unwrap();
        assert_eq!(moving.shape, Shape::Square);
        assert!((moving.min - 200.0).abs() < 1e-9);
        let (load, _) = flags(&["--max-bytes-per-sec", "100MiB"]).resolve().unwrap();
        assert_eq!(load.pace.unwrap().unit, Unit::Bytes);
    }

    #[test]
    fn a_load_that_cannot_run_is_refused() {
        for bad in [
            &["--record-size", "8"][..],
            &["--min-record-bytes", "512", "--max-record-bytes", "256"],
            &["--writers-per-journal", "0"],
            &["--backlog", "1MiB"],
            &["--max-rate", "10", "--min-rate", "20"],
            &["--max-rate", "10", "--min-rate", "5", "--rate-period", "0s"],
        ] {
            assert!(flags(bad).resolve().is_err(), "{bad:?} is refused");
        }
        let mut all = vec!["bench", "--max-rate", "1", "--max-bytes-per-sec", "1"];
        assert!(Cli::try_parse_from(all.drain(..)).is_err(), "one rate");
        assert!(
            Cli::try_parse_from(["bench", "--min-rate", "1"]).is_err(),
            "a low end needs a high end"
        );
    }

    #[test]
    fn names_parse() {
        for preset in Preset::ALL {
            assert_eq!(preset.name().parse::<Preset>(), Ok(preset));
        }
        assert!("ycsb-a".parse::<Preset>().is_err());
        assert_eq!("random".parse::<ReadFrom>(), Ok(ReadFrom::Random));
        assert!("middle".parse::<ReadFrom>().is_err());
        assert_eq!("zipfian".parse::<Distribution>(), Ok(Distribution::Zipfian));
        assert!("normal".parse::<Distribution>().is_err());
    }

    #[test]
    fn a_zipfian_pick_favours_the_first_journals() {
        let picker = Picker::new(Distribution::Zipfian, 16);
        let mut rng = Rng::of(&[9]);
        let mut counts = [0_u32; 16];
        for _ in 0..20_000 {
            counts[picker.pick(&mut rng)] += 1;
        }
        assert!(counts[0] > 3 * counts[15], "{counts:?}");
        assert!(counts[0] > counts[1] && counts[1] > counts[4], "{counts:?}");
        assert!(counts.iter().all(|c| *c > 0), "every journal is drawn");

        let uniform = Picker::new(Distribution::Uniform, 4);
        let mut counts = [0_u32; 4];
        for _ in 0..8000 {
            counts[uniform.pick(&mut rng)] += 1;
        }
        assert!(
            counts.iter().all(|c| (1700..2300).contains(c)),
            "{counts:?}"
        );
    }
}
