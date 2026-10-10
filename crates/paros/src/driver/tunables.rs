//! The production profile of [`DriverTunables`] and the floors every
//! profile must clear (#209).
//!
//! [`DriverTunables::default`] is the simulation's baseline: a 50 ms tick and
//! a 250 ms election base, sized against the simulated client's one-second
//! deadline. [`DriverTunables::production`] is what `parosd` runs on a real
//! network and a real disk. Its values are **reasoned, not measured** — the
//! benchmark that tunes them is M10's — and every one of them lies inside
//! the range the simulation's knobs draw (`paros_sim::shape`), where the
//! sweep also draws the whole profile at once, so the shipped configuration
//! is one the simulation runs.
//!
//! [`DriverTunables::check_floors`] refuses a profile below a field's
//! documented floor: an extreme must stay a valid, winnable configuration,
//! and an operator's override is external input.

use std::fmt;
use std::time::Duration;

use paros_core::HEARTBEAT_TICKS;

use super::config::DriverTunables;

/// The production tick: a leader beats every 100 ms (etcd's heartbeat).
const TICK: Duration = Duration::from_millis(100);
/// The production election base, in ticks: one second, so a follower's
/// timeout is drawn from `[1 s, 2 s)` (etcd's election timeout is ten
/// heartbeats). It must outlast a Phase-1 round trip — a promise is an
/// `fsync` on every acceptor, which a cloud disk can stall for tens of
/// milliseconds — with an order of magnitude to spare.
const ELECTION_BASE: u64 = 10;

// The shipped profile clears the election floor at compile time.
const _: () = assert!(ELECTION_BASE >= 2 * HEARTBEAT_TICKS);
const _: () = assert!(TICK.as_millis() > 0);

/// A field below its floor.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BelowFloor {
    /// The [`DriverTunables`] field, by name.
    pub field: &'static str,
    /// Its value: a count, or milliseconds for a duration.
    pub value: u64,
    /// The smallest value the field may take.
    pub floor: u64,
}

impl fmt::Display for BelowFloor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} = {} is below its floor {}",
            self.field, self.value, self.floor
        )
    }
}

impl std::error::Error for BelowFloor {}

impl DriverTunables {
    /// The profile a real deployment runs (#209): a 100 ms tick and a
    /// one-second election base, with every cadence counted in ticks
    /// following the base, and the network timeouts sized for a link across
    /// zones rather than the simulator's.
    ///
    /// | field | production | why |
    /// |---|---|---|
    /// | `tick_interval` | 100 ms | a beat per tick; etcd's heartbeat |
    /// | `election_timeout_base` | 10 ticks (1 s) | ten beats; outlasts a Phase-1 round trip with its `fsync`s |
    /// | `keep_alive_interval` | 5 s | a ping per idle connection, cheap |
    /// | `keep_alive_timeout` | 3 s | a peer silent past three seconds is half-open, not slow |
    /// | `connection_timeout` | 3 s | a connect and its handshake across zones, a lost SYN included (retransmitted at 1 s) |
    /// | `delivery_timeout` | 2 s | an enqueue on the peer, never its processing |
    /// | `read_retry_ticks` | 20 (2 s) | a quorum read's confirmation, inside `parosctl`'s 5 s deadline |
    /// | `max_wait_ms` | 1 s | a tail wait, inside the client's read deadline |
    /// | `min_wait_ms` | 0 | a client may ask to answer at once |
    /// | `max_read_records`, `max_read_bytes`, `max_batch_*` | the defaults | bounded by bytes, not by time |
    /// | `recovery_page` | the default (64) | the ceiling; a page bounds one `Ready`'s burst |
    /// | `promise_page`, `resend_page`, `apply_page`, `registry_page` | the defaults (64) | the ceilings; a page bounds one message or one `Ready` (#338) |
    /// | `quarantine_ticks` | 80 (8 s) | eight election timeouts before a faulty device is retried |
    /// | `election_backoff_doublings` | 3 | up to `8 × T` (8–16 s) after failed campaigns |
    /// | inbox and queue capacities, `delivery_batch` | the defaults | bounded by bytes, not by time |
    /// | re-send cadences | one election base | a lost reply costs a round trip before the retry |
    /// | `reconfigure_backoff_max_ticks` | two election bases | |
    /// | `proxy_take_back_resends`, `proxy_round_resends` | 20, 40 beats | two and four seconds |
    /// | `election_lease`, `election_renew` | 3 s, 1 s | a cell coordinator renews three times per lease (#240) |
    /// | `election_compact_after` | 64 | the election journal's log stays a few kilobytes |
    /// | `machine_down_after` | 10 s | a machine silent for ten renewal periods is down; a reboot is shorter (#211) |
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn production() -> Self {
        let defaults = Self::default();
        let production = Self {
            tick_interval: TICK,
            election_timeout_base: ELECTION_BASE,
            keep_alive_interval: Duration::from_secs(5),
            keep_alive_timeout: Duration::from_secs(3),
            connection_timeout: Duration::from_secs(3),
            delivery_timeout: Duration::from_secs(2),
            read_retry_ticks: 2 * ELECTION_BASE,
            max_wait_ms: 1000,
            quarantine_ticks: 8 * ELECTION_BASE,
            election_backoff_doublings: 3,
            match_resend_ticks: ELECTION_BASE,
            gc_resend_ticks: ELECTION_BASE,
            reconfigurer_resend_ticks: ELECTION_BASE,
            reconfigure_timeout_elections: 4,
            reconfigure_backoff_max_ticks: 2 * ELECTION_BASE,
            proxy_take_back_resends: 2 * ELECTION_BASE,
            proxy_round_resends: 4 * ELECTION_BASE,
            election_lease: Duration::from_secs(3),
            election_renew: Duration::from_secs(1),
            election_compact_after: 64,
            machine_down_after: Duration::from_secs(10),
            ..defaults
        };
        // What `parosd` ships is a profile the simulation could have drawn:
        // above every floor, its proxies evict only after a take-back.
        assert!(
            production.check_floors().is_ok(),
            "the production profile clears every floor"
        );
        assert!(
            production.proxy_round_resends > production.proxy_take_back_resends,
            "a proxy evicts a round only after its leader would take it back"
        );
        assert!(
            production.keep_alive_timeout < production.keep_alive_interval,
            "a keep-alive times out before the next is due"
        );
        production
    }

    /// Check every field against the floor its documentation names. The
    /// wall-clock floors (an election base, in time, above a Phase-1 round
    /// trip) depend on the network and are the operator's to respect; the
    /// floors below are the ones no network makes valid.
    ///
    /// # Errors
    ///
    /// The first field below its floor.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    pub fn check_floors(&self) -> Result<(), BelowFloor> {
        let ms = |d: Duration| u64::try_from(d.as_millis()).unwrap_or(u64::MAX);
        let count = |n: usize| u64::try_from(n).unwrap_or(u64::MAX);
        let fields: [(&'static str, u64, u64); 35] = [
            ("tick_interval", ms(self.tick_interval), 1),
            (
                "election_timeout_base",
                self.election_timeout_base,
                2 * HEARTBEAT_TICKS,
            ),
            ("keep_alive_interval", ms(self.keep_alive_interval), 1),
            ("keep_alive_timeout", ms(self.keep_alive_timeout), 1),
            ("connection_timeout", ms(self.connection_timeout), 1),
            ("delivery_timeout", ms(self.delivery_timeout), 1),
            ("read_retry_ticks", self.read_retry_ticks, 1),
            ("max_wait_ms", self.max_wait_ms, 0),
            ("min_wait_ms", self.min_wait_ms, 0),
            ("max_read_records", self.max_read_records, 1),
            ("max_read_bytes", self.max_read_bytes, 1),
            ("quarantine_ticks", self.quarantine_ticks, 1),
            (
                "election_backoff_doublings",
                u64::from(self.election_backoff_doublings),
                2,
            ),
            (
                "client_inbox_capacity",
                count(self.client_inbox_capacity),
                1,
            ),
            ("peer_inbox_capacity", count(self.peer_inbox_capacity), 1),
            ("peer_queue_capacity", count(self.peer_queue_capacity), 1),
            // The throughput floor the harness keeps (`paros_sim::shape`):
            // a link carries every journal its two ends serve.
            ("delivery_batch", count(self.delivery_batch), 24),
            ("match_resend_ticks", self.match_resend_ticks, 1),
            ("gc_resend_ticks", self.gc_resend_ticks, 1),
            (
                "reconfigurer_resend_ticks",
                self.reconfigurer_resend_ticks,
                1,
            ),
            (
                "reconfigure_timeout_elections",
                self.reconfigure_timeout_elections,
                1,
            ),
            (
                "reconfigure_backoff_max_ticks",
                self.reconfigure_backoff_max_ticks,
                1,
            ),
            ("proxy_take_back_resends", self.proxy_take_back_resends, 1),
            ("proxy_round_resends", self.proxy_round_resends, 1),
            ("max_batch_records", self.max_batch_records, 1),
            ("max_batch_bytes", self.max_batch_bytes, 1),
            ("election_renew", ms(self.election_renew), 1),
            // The lease outlasts a renewal period (#240).
            (
                "election_lease",
                ms(self.election_lease),
                ms(self.election_renew).saturating_add(1),
            ),
            ("election_compact_after", self.election_compact_after, 1),
            // One missed probe never marks a machine down (#211).
            (
                "machine_down_after",
                ms(self.machine_down_after),
                ms(self.election_renew).saturating_add(1),
            ),
            ("recovery_page", count(self.recovery_page), 1),
            ("promise_page", count(self.promise_page), 1),
            ("resend_page", count(self.resend_page), 1),
            ("apply_page", count(self.apply_page), 1),
            ("registry_page", count(self.registry_page), 1),
        ];
        assert!(
            fields.iter().all(|(field, _, _)| !field.is_empty()),
            "every checked field is named"
        );
        let verdict = match fields.into_iter().find(|(_, value, floor)| value < floor) {
            Some((field, value, floor)) => Err(BelowFloor {
                field,
                value,
                floor,
            }),
            None => Ok(()),
        };
        // A refusal names a field that really is below its floor.
        if let Err(below) = &verdict {
            assert!(
                below.value < below.floor,
                "a refused field lies below its floor"
            );
        }
        verdict
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn both_profiles_clear_every_floor() {
        assert_eq!(DriverTunables::default().check_floors(), Ok(()));
        assert_eq!(DriverTunables::production().check_floors(), Ok(()));
    }

    #[test]
    fn a_field_below_its_floor_is_named() {
        let below = DriverTunables {
            election_timeout_base: 1,
            ..DriverTunables::production()
        };
        assert_eq!(
            below.check_floors(),
            Err(BelowFloor {
                field: "election_timeout_base",
                value: 1,
                floor: 2,
            })
        );
        let zero = DriverTunables {
            tick_interval: Duration::ZERO,
            ..DriverTunables::production()
        };
        assert_eq!(
            zero.check_floors().map_err(|b| b.field),
            Err("tick_interval")
        );
    }
}
