//! The bench's rate (#724 (bench roles)): fixed, or moving between a
//! minimum and a maximum over a period, after `FoundationDB`'s `mako`
//! (`--tpsmax`, `--tpsmin`, `--tpsinterval`, `--tpschange`).
//!
//! The rate is in calls per second (`--max-rate`) or in bytes per second
//! (`--max-bytes-per-sec`), over every paced client together: the writers
//! and the readers (a tailer follows its journal and is never paced). Each
//! paced client takes an equal share, and asks the rate again at each
//! call's due time, so a moving rate moves the next call, not the next
//! second.
//!
//! The shapes, with `phase` the time into the period over the period:
//!
//! - `sin`: `min + (max - min) × (1 - cos 2π·phase) / 2`: it starts at
//!   `min`, reaches `max` at half the period, then goes back.
//! - `square`: `max` for the first half of each period, `min` for the
//!   second.
//! - `pulse`: `max` for the first tenth of each period, `min` after.

use std::str::FromStr;
use std::time::Duration;

/// How a moving rate moves.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Shape {
    Sin,
    Square,
    Pulse,
}

impl Shape {
    pub fn name(self) -> &'static str {
        match self {
            Shape::Sin => "sin",
            Shape::Square => "square",
            Shape::Pulse => "pulse",
        }
    }
}

impl FromStr for Shape {
    type Err = String;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        match text {
            "sin" => Ok(Shape::Sin),
            "square" => Ok(Shape::Square),
            "pulse" => Ok(Shape::Pulse),
            _ => Err(format!("{text:?}: the shapes are sin, square and pulse")),
        }
    }
}

/// What a rate counts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Unit {
    Calls,
    Bytes,
}

impl Unit {
    pub fn label(self) -> &'static str {
        match self {
            Unit::Calls => "calls/s",
            Unit::Bytes => "bytes/s",
        }
    }
}

/// A rate over every paced client together.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Pace {
    pub unit: Unit,
    pub max: f64,
    /// The low end of a moving rate: `None` for a fixed one.
    pub moving: Option<Moving>,
}

/// A rate that moves between `min` and the pace's `max`.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Moving {
    pub min: f64,
    pub period: Duration,
    pub shape: Shape,
}

impl Pace {
    /// A fixed rate.
    #[cfg(test)]
    pub fn fixed(unit: Unit, max: f64) -> Self {
        Self {
            unit,
            max,
            moving: None,
        }
    }

    /// The rate `elapsed` into the run.
    pub fn at(&self, elapsed: Duration) -> f64 {
        let Some(moving) = self.moving else {
            return self.max;
        };
        let period = moving.period.as_secs_f64();
        let phase = (elapsed.as_secs_f64() / period).fract();
        let (low, high) = (moving.min, self.max);
        match moving.shape {
            Shape::Sin => low + (high - low) * (1.0 - (std::f64::consts::TAU * phase).cos()) / 2.0,
            Shape::Square => {
                if phase < 0.5 {
                    high
                } else {
                    low
                }
            }
            Shape::Pulse => {
                if phase < 0.1 {
                    high
                } else {
                    low
                }
            }
        }
    }

    /// The time from one call of a paced client to its next, `elapsed` into
    /// the run: the client's share of the rate (`clients` paced clients),
    /// over what one of its calls costs (`cost`: 1 call, or its mean
    /// bytes).
    pub fn period(&self, elapsed: Duration, clients: u64, cost: f64) -> Duration {
        let share = self.at(elapsed) / super::units::float(clients.max(1));
        let units = match self.unit {
            Unit::Calls => 1.0,
            Unit::Bytes => cost.max(1.0),
        };
        // A rate of 0 (a `min` of 0) waits a second and asks again.
        if share <= 0.0 {
            return Duration::from_secs(1);
        }
        Duration::from_secs_f64((units / share).min(3600.0))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{Moving, Pace, Shape, Unit};

    fn moving(shape: Shape) -> Pace {
        Pace {
            unit: Unit::Calls,
            max: 1000.0,
            moving: Some(Moving {
                min: 200.0,
                period: Duration::from_secs(10),
                shape,
            }),
        }
    }

    fn at(pace: &Pace, secs: f64) -> f64 {
        pace.at(Duration::from_secs_f64(secs))
    }

    #[test]
    fn a_fixed_rate_never_moves() {
        let pace = Pace::fixed(Unit::Calls, 500.0);
        assert!((at(&pace, 0.0) - 500.0).abs() < 1e-9);
        assert!((at(&pace, 123.4) - 500.0).abs() < 1e-9);
    }

    #[test]
    fn sin_starts_low_peaks_at_half_and_repeats() {
        let pace = moving(Shape::Sin);
        assert!((at(&pace, 0.0) - 200.0).abs() < 1e-6);
        assert!((at(&pace, 5.0) - 1000.0).abs() < 1e-6);
        assert!((at(&pace, 2.5) - 600.0).abs() < 1e-6);
        assert!((at(&pace, 12.5) - 600.0).abs() < 1e-6, "the period repeats");
    }

    #[test]
    fn square_and_pulse_switch_inside_the_period() {
        let square = moving(Shape::Square);
        assert!((at(&square, 1.0) - 1000.0).abs() < 1e-9);
        assert!((at(&square, 6.0) - 200.0).abs() < 1e-9);
        let pulse = moving(Shape::Pulse);
        assert!((at(&pulse, 0.5) - 1000.0).abs() < 1e-9);
        assert!((at(&pulse, 1.5) - 200.0).abs() < 1e-9);
        assert!((at(&pulse, 10.5) - 1000.0).abs() < 1e-9);
    }

    #[test]
    fn a_client_takes_its_share_of_calls_or_bytes() {
        let calls = Pace::fixed(Unit::Calls, 100.0);
        assert_eq!(
            calls.period(Duration::ZERO, 4, 4096.0),
            Duration::from_millis(40)
        );
        let bytes = Pace::fixed(Unit::Bytes, 1024.0 * 1024.0);
        // 4 clients share 1 MiB/s: 256 KiB/s each, 64 KiB a call: 4 calls/s.
        assert_eq!(
            bytes.period(Duration::ZERO, 4, 65536.0),
            Duration::from_millis(250)
        );
        let idle = Pace {
            moving: Some(Moving {
                min: 0.0,
                period: Duration::from_secs(10),
                shape: Shape::Square,
            }),
            ..Pace::fixed(Unit::Calls, 10.0)
        };
        assert_eq!(
            idle.period(Duration::from_secs(6), 1, 1.0),
            Duration::from_secs(1)
        );
    }

    #[test]
    fn shapes_parse_by_name() {
        for shape in [Shape::Sin, Shape::Square, Shape::Pulse] {
            assert_eq!(shape.name().parse::<Shape>(), Ok(shape));
        }
        assert!("saw".parse::<Shape>().is_err());
    }
}
