//! The knobs of the election. Every time is in nanoseconds of the clock that
//! the program passes to [`crate::Election::tick`].
//!
//! The values are the program's (open point 1 of #1420 (paros-election)):
//! the defaults below are a start for a program on one network, not a
//! decision. Safety never depends on them: a lease that is too short makes a
//! candidate depose a live holder, which costs availability only
//! (`docs/architecture/03-3-coordinators-and-placement.md` §3.3).

/// One second, in nanoseconds.
const SECOND: u64 = 1_000_000_000;

/// The knobs of one election.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Config {
    /// How long a candidate sees the same tail under the same uuid before it
    /// takes the holder for silent.
    pub lease: u64,
    /// How often a candidate reads the tail of the journal.
    pub poll: u64,
    /// How long a holder waits after its last write before it writes a
    /// renewal.
    pub renew: u64,
    /// The largest jitter a candidate waits, after it found the holder
    /// silent, before it claims.
    pub jitter: u64,
    /// The most records one read asks for.
    pub page: u64,
}

impl Default for Config {
    /// A lease of two seconds; a read of the tail and, when idle, a renewal
    /// four times in each lease; a jitter of up to half a lease; pages of 64
    /// records.
    fn default() -> Self {
        let config = Self {
            lease: 2 * SECOND,
            poll: SECOND / 2,
            renew: SECOND / 2,
            jitter: SECOND,
            page: 64,
        };
        config.check();
        config
    }
}

impl Config {
    /// Check that the knobs make an election that can hold a term.
    ///
    /// # Panics
    ///
    /// When a time is zero, when a holder renews no faster than one lease
    /// (a live holder would look silent), or when a page holds no record.
    pub fn check(&self) {
        assert!(self.lease > 0, "a lease lasts");
        assert!(
            self.poll > 0,
            "a candidate reads the tail again after a time"
        );
        assert!(self.renew > 0, "a holder renews after a time");
        assert!(
            self.renew < self.lease,
            "a holder renews more often than once a lease"
        );
        assert!(self.page > 0, "a read asks for at least one record");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_defaults_hold_a_term() {
        let config = Config::default();
        assert!(config.renew < config.lease);
        assert!(config.poll < config.lease);
        assert!(config.jitter <= config.lease);
    }

    #[test]
    #[should_panic(expected = "more often than once a lease")]
    fn a_renewal_as_slow_as_the_lease_is_refused() {
        Config {
            renew: 10,
            lease: 10,
            ..Config::default()
        }
        .check();
    }
}
