//! The bench's call mix (#194 (throughput benchmark)): `--mix
//! write=80,read-tail=15,set-leader=5` gives each call a weight, and every
//! client draws its next call by weight. A name with no weight weighs 1.
//! `truncate` is not in the mix: it runs on its own schedule
//! (`--truncate-every`).

use std::str::FromStr;

use super::histograms::{CALLS, Call};
use super::records::Rng;

/// A weight per call; `truncate` weighs 0.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Mix {
    weights: [u64; CALLS],
}

impl Mix {
    /// The weight of `call`.
    pub fn weight(&self, call: Call) -> u64 {
        self.weights[Call::ALL
            .iter()
            .position(|c| *c == call)
            .expect("every call is in ALL")]
    }

    /// Whether the mix draws `call` at all.
    pub fn has(&self, call: Call) -> bool {
        self.weight(call) > 0
    }

    /// The next call, drawn by weight.
    pub fn draw(&self, rng: &mut Rng) -> Call {
        let total: u64 = self.weights.iter().sum();
        let mut at = rng.below(total);
        for call in Call::ALL {
            let weight = self.weight(call);
            if at < weight {
                return call;
            }
            at -= weight;
        }
        unreachable!("a draw below the total lands on a call")
    }
}

impl FromStr for Mix {
    type Err = String;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let mut weights = [0; CALLS];
        for part in text.split(',').map(str::trim).filter(|p| !p.is_empty()) {
            let (name, weight) = match part.split_once('=') {
                Some((name, weight)) => (
                    name.trim(),
                    weight
                        .trim()
                        .parse::<u64>()
                        .map_err(|_| format!("{part:?}: a weight is a whole number"))?,
                ),
                None => (part, 1),
            };
            let call = Call::of_name(name).ok_or_else(|| {
                format!("{name:?}: the calls are write, read-tail, read-start and set-leader")
            })?;
            if call == Call::Truncate {
                return Err("truncate runs on a schedule: use --truncate-every".into());
            }
            let index = Call::ALL
                .iter()
                .position(|c| *c == call)
                .expect("every call is in ALL");
            weights[index] = weight;
        }
        if weights.iter().all(|w| *w == 0) {
            return Err(format!("{text:?}: the mix needs one call with a weight"));
        }
        Ok(Self { weights })
    }
}

#[cfg(test)]
mod tests {
    use super::{Call, Mix, Rng};

    #[test]
    fn a_mix_parses_and_draws_by_weight() {
        let mix: Mix = "write=3, read-tail=1,read-start=0".parse().unwrap();
        assert_eq!(mix.weight(Call::Write), 3);
        assert!(mix.has(Call::ReadTail));
        assert!(!mix.has(Call::ReadStart));
        assert!(!mix.has(Call::SetLeader));
        let mut rng = Rng::of(&[1]);
        let writes = (0..4000)
            .filter(|_| mix.draw(&mut rng) == Call::Write)
            .count();
        assert!((2700..3300).contains(&writes), "{writes} of 4000");
        assert_eq!("write".parse::<Mix>().unwrap().weight(Call::Write), 1);
    }

    #[test]
    fn a_bad_mix_is_refused() {
        assert!("".parse::<Mix>().is_err());
        assert!("write=0".parse::<Mix>().is_err());
        assert!("scan=1".parse::<Mix>().is_err());
        assert!("write=x".parse::<Mix>().is_err());
        assert!("truncate=1".parse::<Mix>().is_err());
    }
}
