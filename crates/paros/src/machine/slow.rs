//! **Slow machines** (#361 (slow machines), gray failure, `docs/architecture/03-4-tenant-modes.md`
//! §3.4): the cell coordinator finds a machine that is alive but slow, and
//! the cell stops depending on it, the way it does for a draining one.
//!
//! The binary failure detector (`MachineDown`, #211) sees a machine that does
//! not answer. A slow disk or a starved CPU answers, late, and slows every
//! write of every journal whose Phase-2 quorum it is in. So at each watch
//! round the coordinator measures each machine that answered:
//!
//! - its `Identify` round trip, on the coordinator's own clock;
//! - its disk's and CPU's busy ratios, from its last `Load` window (#424
//!   (busyness metrics)), which the machine measures itself.
//!
//! [`SlowJudge`] turns those facts into a judgement with hysteresis: a
//! machine leans slow when a busy ratio is at or above `slow_busy`, or its
//! round trip is at or above `slow_rtt` and [`RTT_FACTOR`] times the round's
//! median; it leans fast again only below both with a margin
//! ([`BUSY_MARGIN`], half the round-trip bounds). Only after `slow_rounds`
//! rounds in a row lean the other way does the judgement change, and the
//! coordinator writes `MachineSlow` (changes only, with the facts). The
//! median makes a coordinator that is slow itself (every round trip is long)
//! mark nobody for its own slowness.
//!
//! **A slow mark never takes a quorum** ([`SlowJudge::cap`]): at most a
//! minority of the watched machines is held slow at once. A mark itself
//! removes nothing: the tenant coordinator moves each journal off a slow
//! machine like off a draining one (`super::moves`: book elsewhere,
//! reconfigure, then release), and placement books a slow machine last.
//!
//! Pure: no clock, I/O or randomness. The watch hands in the facts.

use std::collections::BTreeMap;
use std::time::Duration;

use paros_core::NodeId;

use crate::model::registry::SlowFacts;
use crate::rpc::machine as wire;

/// A round trip leans slow only at or above this many times the round's
/// median: a coordinator that is slow itself sees every round trip long.
pub const RTT_FACTOR: u32 = 4;

pub use crate::tunables::BUSY_MARGIN;

/// The thresholds of the judgement, from the driver's tunables.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SlowBounds {
    /// A round trip at or above this (and [`RTT_FACTOR`] times the median)
    /// leans slow.
    pub rtt: Duration,
    /// A busy ratio at or above this, in thousandths, leans slow.
    pub busy_permille: u32,
    /// How many rounds in a row must lean the other way before the
    /// judgement changes.
    pub rounds: u32,
}

/// What one machine showed in one round.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Sample {
    /// Its `Identify` round trip.
    pub rtt: Duration,
    /// Its disk's busy ratio, in thousandths, when its `Load` had a window.
    pub disk_busy_permille: Option<u32>,
    /// Its CPU's busy ratio, in thousandths, when its `Load` had a window.
    pub cpu_busy_permille: Option<u32>,
}

impl Sample {
    /// The sample of a machine timed at `rtt` whose `Load` answered `ack`
    /// (`None`: no answer).
    ///
    /// # Panics
    ///
    /// Never: a ratio is clamped into `[0, 1]` before it is scaled.
    #[must_use]
    pub fn of(rtt: Duration, ack: Option<&wire::LoadAck>) -> Self {
        let window = ack.filter(|a| a.windowed);
        let permille = |ratio: f64| {
            // A ratio is in [0, 1]; the cast is bounded.
            #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
            let p = (ratio.clamp(0.0, 1.0) * 1000.0).round() as u32;
            assert!(p <= 1000, "a busy ratio is at most one");
            p
        };
        let cpu = window.map(|a| {
            if a.has_run_loop {
                permille(a.run_loop_busy)
            } else {
                permille(a.cpu_cores / f64::from(a.cores.max(1)))
            }
        });
        Self {
            rtt,
            disk_busy_permille: window
                .and_then(|a| a.disk.as_ref())
                .map(|d| permille(d.busy)),
            cpu_busy_permille: cpu,
        }
    }

    /// The facts a `MachineSlow` carries.
    #[must_use]
    pub fn facts(&self) -> SlowFacts {
        SlowFacts {
            rtt_us: u64::try_from(self.rtt.as_micros()).unwrap_or(u64::MAX),
            disk_busy_permille: self.disk_busy_permille.unwrap_or(0),
            cpu_busy_permille: self.cpu_busy_permille.unwrap_or(0),
        }
    }

    /// The highest busy ratio it showed, in thousandths.
    fn busiest(&self) -> Option<u32> {
        self.disk_busy_permille.max(self.cpu_busy_permille)
    }
}

/// Which way one round leans for one machine.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Lean {
    Slow,
    Fast,
    /// Between the bounds: the round counts for neither side.
    Neither,
}

/// One change of judgement the coordinator writes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Judged {
    /// The machine.
    pub id: NodeId,
    /// Whether it is slow now.
    pub slow: bool,
    /// The facts of the round that changed it.
    pub facts: SlowFacts,
}

/// The cell coordinator's slow-machine judgement for one term: how many
/// rounds in a row each machine leaned against what the registry holds.
#[derive(Debug)]
pub struct SlowJudge {
    bounds: SlowBounds,
    streaks: BTreeMap<NodeId, u32>,
}

impl SlowJudge {
    /// A judge with `bounds`.
    ///
    /// # Panics
    ///
    /// If `bounds` asks for no round, or for a busy ratio above one or at
    /// or below the fast margin: the tunables' floors refuse them.
    #[must_use]
    pub fn new(bounds: SlowBounds) -> Self {
        assert!(
            bounds.rounds >= 1,
            "a judgement changes after a round at least"
        );
        assert!(bounds.busy_permille <= 1000, "a busy bound is a ratio");
        assert!(
            bounds.busy_permille > BUSY_MARGIN,
            "a busy bound leaves room to lean fast"
        );
        assert!(!bounds.rtt.is_zero(), "a round-trip bound is positive");
        Self {
            bounds,
            streaks: BTreeMap::new(),
        }
    }

    /// The most machines of `watched` held slow at once: a minority, so the
    /// marks never take a quorum.
    #[must_use]
    pub fn cap(watched: usize) -> usize {
        watched.saturating_sub(1) / 2
    }

    /// Judge one round: `samples` are the machines that answered, `held`
    /// whether the registry holds each slow now, and `watched` the machines
    /// the round watched, answered or not. The changes to write, in id
    /// order; a mark is withheld once [`SlowJudge::cap`] of the watched
    /// machines would be slow.
    ///
    /// # Panics
    ///
    /// If a sample is of a machine `watched` does not name.
    pub fn round(
        &mut self,
        samples: &BTreeMap<NodeId, Sample>,
        held: impl Fn(NodeId) -> bool,
        watched: &[NodeId],
    ) -> Vec<Judged> {
        assert!(
            samples.keys().all(|id| watched.contains(id)),
            "a sample is of a watched machine"
        );
        // A machine that did not answer is the failure detector's: its
        // streak starts again.
        self.streaks.retain(|id, _| samples.contains_key(id));
        let median = median(samples.values().map(|s| s.rtt));
        // Every watched machine held slow counts, a silent one too.
        let held_before = watched.iter().filter(|id| held(**id)).count();
        let mut slow_now = held_before;
        let cap = Self::cap(watched.len());
        let mut changes = Vec::new();
        for (id, sample) in samples {
            let was = held(*id);
            let lean = self.lean(sample, median);
            let against = match lean {
                Lean::Slow => !was,
                Lean::Fast => was,
                Lean::Neither => false,
            };
            if !against {
                self.streaks.remove(id);
                continue;
            }
            let streak = self.streaks.entry(*id).or_default();
            *streak += 1;
            if *streak < self.bounds.rounds {
                continue;
            }
            if !was && slow_now >= cap {
                moonpool_assertions::reachable!(
                    "slow: a mark is withheld so the marks never take a quorum"
                );
                continue;
            }
            self.streaks.remove(id);
            if was {
                slow_now -= 1;
            } else {
                slow_now += 1;
            }
            changes.push(Judged {
                id: *id,
                slow: !was,
                facts: sample.facts(),
            });
        }
        assert!(
            slow_now <= cap.max(held_before),
            "a round never takes the marks past the cap"
        );
        changes
    }

    fn lean(&self, sample: &Sample, median: Duration) -> Lean {
        let busy = self.bounds.busy_permille;
        let rtt_slow = sample.rtt >= self.bounds.rtt && sample.rtt >= median * RTT_FACTOR;
        if rtt_slow || sample.busiest().is_some_and(|b| b >= busy) {
            return Lean::Slow;
        }
        let rtt_fast = sample.rtt < self.bounds.rtt / 2 || sample.rtt < median * RTT_FACTOR / 2;
        let busy_fast = sample.busiest().is_none_or(|b| b < busy - BUSY_MARGIN);
        if rtt_fast && busy_fast {
            Lean::Fast
        } else {
            Lean::Neither
        }
    }
}

/// The median of `rtts` (the lower one of an even count), zero for none.
fn median(rtts: impl Iterator<Item = Duration>) -> Duration {
    let mut all: Vec<Duration> = rtts.collect();
    all.sort_unstable();
    all.get(all.len().saturating_sub(1) / 2)
        .copied()
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    const BOUNDS: SlowBounds = SlowBounds {
        rtt: Duration::from_millis(500),
        busy_permille: 900,
        rounds: 2,
    };

    fn sample(rtt_ms: u64, disk: u32) -> Sample {
        Sample {
            rtt: Duration::from_millis(rtt_ms),
            disk_busy_permille: Some(disk),
            cpu_busy_permille: Some(10),
        }
    }

    fn round(
        judge: &mut SlowJudge,
        held: &mut BTreeMap<NodeId, bool>,
        samples: &[(u64, Sample)],
    ) -> Vec<Judged> {
        let samples: BTreeMap<NodeId, Sample> =
            samples.iter().map(|(id, s)| (NodeId(*id), *s)).collect();
        let watched: Vec<NodeId> = (1..=5).map(NodeId).collect();
        let snapshot = held.clone();
        let changes = judge.round(
            &samples,
            |id| snapshot.get(&id).copied().unwrap_or(false),
            &watched,
        );
        for c in &changes {
            held.insert(c.id, c.slow);
        }
        changes
    }

    #[test]
    fn a_busy_disk_is_marked_after_its_rounds_and_cleared_with_hysteresis() {
        let mut judge = SlowJudge::new(BOUNDS);
        let mut held = BTreeMap::new();
        let healthy = sample(10, 100);
        let busy = sample(10, 990);
        // One round is not enough.
        assert!(
            round(
                &mut judge,
                &mut held,
                &[(1, healthy), (2, healthy), (3, busy)]
            )
            .is_empty()
        );
        let marked = round(
            &mut judge,
            &mut held,
            &[(1, healthy), (2, healthy), (3, busy)],
        );
        assert_eq!(marked.len(), 1);
        assert!(marked[0].slow && marked[0].id == NodeId(3));
        assert_eq!(marked[0].facts.disk_busy_permille, 990);
        // Between the bounds counts for neither side.
        let between = sample(10, 800);
        for _ in 0..4 {
            assert!(
                round(
                    &mut judge,
                    &mut held,
                    &[(1, healthy), (2, healthy), (3, between)]
                )
                .is_empty()
            );
        }
        assert!(
            round(
                &mut judge,
                &mut held,
                &[(1, healthy), (2, healthy), (3, healthy)]
            )
            .is_empty()
        );
        let cleared = round(
            &mut judge,
            &mut held,
            &[(1, healthy), (2, healthy), (3, healthy)],
        );
        assert_eq!(cleared.len(), 1);
        assert!(!cleared[0].slow);
    }

    #[test]
    fn a_slow_coordinator_marks_nobody_for_its_own_slowness() {
        let mut judge = SlowJudge::new(BOUNDS);
        let mut held = BTreeMap::new();
        let late = sample(2_000, 100);
        for _ in 0..4 {
            assert!(round(&mut judge, &mut held, &[(1, late), (2, late), (3, late)]).is_empty());
        }
        // One machine far above the median is.
        let later = sample(9_000, 100);
        round(&mut judge, &mut held, &[(1, late), (2, late), (3, later)]);
        let marked = round(&mut judge, &mut held, &[(1, late), (2, late), (3, later)]);
        assert_eq!(marked.len(), 1);
    }

    #[test]
    fn the_marks_never_take_a_quorum() {
        let mut judge = SlowJudge::new(BOUNDS);
        let mut held = BTreeMap::new();
        let busy = sample(10, 990);
        let all: Vec<(u64, Sample)> = (1..=5).map(|id| (id, busy)).collect();
        for _ in 0..6 {
            round(&mut judge, &mut held, &all);
        }
        assert_eq!(held.values().filter(|s| **s).count(), SlowJudge::cap(5));
        assert_eq!(SlowJudge::cap(1), 0);
        assert_eq!(SlowJudge::cap(3), 1);
    }

    #[test]
    fn a_silent_machine_starts_its_streak_again() {
        let mut judge = SlowJudge::new(BOUNDS);
        let mut held = BTreeMap::new();
        let busy = sample(10, 990);
        let healthy = sample(10, 100);
        round(
            &mut judge,
            &mut held,
            &[(1, healthy), (2, healthy), (3, busy)],
        );
        round(&mut judge, &mut held, &[(1, healthy), (2, healthy)]);
        assert!(
            round(
                &mut judge,
                &mut held,
                &[(1, healthy), (2, healthy), (3, busy)]
            )
            .is_empty()
        );
    }
}
