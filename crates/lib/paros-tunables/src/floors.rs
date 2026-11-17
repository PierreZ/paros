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

use paros_core::node::HEARTBEAT_TICKS;

use super::DriverTunables;

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
    /// | `max_wait_ms` | 1 s | a tail wait, inside the client's read deadline; at least one election renewal period when the election truncates at every renewal |
    /// | `min_wait_ms` | 0 | a client may ask to answer at once |
    /// | `max_read_records`, `max_read_bytes`, `max_batch_*` | the defaults | bounded by bytes, not by time |
    /// | `recovery_page` | the default (64) | the ceiling; a page bounds one `Ready`'s burst |
    /// | `promise_page`, `resend_page`, `apply_page`, `registry_page` | the defaults (64) | the ceilings; a page bounds one message or one `Ready` (#338) |
    /// | `quarantine_ticks` | 80 (8 s) | eight election timeouts before a faulty device is retried |
    /// | `idle_witness_ticks` | 20 (2 s) | an idle journal's last batch gets its metainfo witness two election timeouts after its last commit (#991 (metainfo write on idle)) |
    /// | `election_backoff_doublings` | 3 | up to `8 × T` (8–16 s) after failed campaigns |
    /// | inbox and queue capacities, `delivery_batch` | the defaults | bounded by bytes, not by time |
    /// | re-send cadences | one election base | a lost reply costs a round trip before the retry |
    /// | `reconfigure_backoff_max_ticks` | two election bases | |
    /// | `proxy_take_back_resends`, `proxy_round_resends` | 20, 40 beats | two and four seconds |
    /// | `partition_move_resends` | 20 beats | two seconds before a stuck round moves to another acceptor partition (#533) |
    /// | `election_lease`, `election_renew` | 3 s, 1 s | a cell coordinator renews three times per lease (#240) |
    /// | `election_compact_after` | 64 | the election journal's log stays a few kilobytes |
    /// | `machine_down_after` | 10 s | a machine silent for ten renewal periods is down; a reboot is shorter (#211) |
    /// | `machine_replace_after` | 5 min | a machine silent that long is gone: its roles move (#475); a reboot, even a slow one, is shorter |
    /// | `load_interval` | the default (5 s) | FDB's `WORKER_LOGGING_INTERVAL` (#424) |
    /// | `slow_rtt`, `slow_busy_permille`, `slow_rounds` | 2 s, 950, 5 | a machine slow for five renewal periods (one `Load` window) moves its journals off (#361 (slow machines)) |
    /// | `balance_spread`, `balance_rounds` | 1, 10 | machines differ by at most one acceptor; a gap must hold ten renewal periods before an acceptor moves (#687 (rebalance a cell)) |
    /// | `balance_busy_spread`, `balance_leader_spread` | the defaults (5, 1) | an acceptor leaves a machine 50 % busier than another of its zone; machines differ by at most one journal led (#687) |
    /// | `move_warm_lag`, `move_warm_rounds` | the defaults (64, 8) | a new member lacks at most one page when its journal reconfigures onto it, or the move waits eight renewal periods (#687) |
    /// | `status_hold` | 20 s | two `machine_down_after`: a machine that reboots inside it never changes a word (#191 (status)) |
    /// | `internal_request_timeout` | 2 s | a coordinator's slow call ends before its lease runs out, so it renews in time (#457) |
    /// | `internal_retry_budget`, `internal_retry_backoff` | the defaults (4, 100 ms) | the library client's |
    /// | `resolve_answer_within` | the default (2 s) | a `Resolve` is a hint read from folds that go on from where they stopped |
    /// | `scrub_rate` | 4 MiB/s | a few percent of one disk, per tenant (#362 (background scrubbing)) |
    /// | `scrub_pass_within` | 7 days | every record is read again each week, so rot is found long before a second copy rots |
    /// | `scrub_page` | the default (64) | a page bounds one acceptor's read burst |
    /// | `shutdown_within` | 30 s | a clean stop hands every leadership on and lets every flush land; past it the stop is a crash (#560 (clean Ctrl-C shutdown)) |
    /// | `read_only_ticks` | 300 (30 s) | a full disk is tried again twice a minute, one refused segment each time (#360 (disk full)) |
    /// | `admission_rounds` | 256 | a leader holds at most 256 undecided rounds before it answers `busy` (#360 (disk full)) |
    /// | `batch_tick`, `batch_flush_records` | the defaults (2 ms, 1024) | a batcher waits a little to merge many writers' writes into one slot (#145) |
    /// | `free_bytes_floor` | 1 GiB | a few hundred segments of the default geometry before placement passes a machine over (#360 (disk full)) |
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
            idle_witness_ticks: 2 * ELECTION_BASE,
            election_backoff_doublings: 3,
            match_resend_ticks: ELECTION_BASE,
            gc_resend_ticks: ELECTION_BASE,
            reconfigurer_resend_ticks: ELECTION_BASE,
            reconfigure_timeout_elections: 4,
            reconfigure_backoff_max_ticks: 2 * ELECTION_BASE,
            proxy_take_back_resends: 2 * ELECTION_BASE,
            proxy_round_resends: 4 * ELECTION_BASE,
            partition_move_resends: 2 * ELECTION_BASE,
            election_lease: Duration::from_secs(3),
            election_renew: Duration::from_secs(1),
            election_compact_after: 64,
            machine_down_after: Duration::from_secs(10),
            slow_rtt: Duration::from_secs(2),
            slow_busy_permille: 950,
            slow_rounds: 5,
            balance_rounds: 10,
            machine_replace_after: Duration::from_secs(300),
            status_hold: Duration::from_secs(20),
            internal_request_timeout: Duration::from_secs(2),
            scrub_rate: 4 * 1024 * 1024,
            scrub_pass_within: Duration::from_hours(7 * 24),
            shutdown_within: Duration::from_secs(30),
            read_only_ticks: 30 * ELECTION_BASE,
            admission_rounds: 256,
            free_bytes_floor: 1024 * 1024 * 1024,
            ..defaults
        };
        // What `parosd` ships is a profile the simulation could have drawn:
        // above every floor, its proxies evict only after a take-back.
        assert!(
            production.check_floors().is_ok(),
            "the production profile clears every floor"
        );
        // A coordinator's call that times out still leaves it a renewal
        // inside its lease (#457): a slow duty never costs the term.
        assert!(
            production.internal_request_timeout + production.election_renew
                <= production.election_lease,
            "an internal call times out before the coordinator's lease runs out"
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
        let fields = self.floor_table();
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
    use crate::{BUSY_MARGIN, LOAD_INTERVAL_FLOOR, SCRUB_PASS_WITHIN_FLOOR, SCRUB_RATE_FLOOR};

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

    /// Production with one field changed: the floor check's verdict.
    macro_rules! with {
        ($($field:ident: $value:expr),+) => {
            DriverTunables {
                $($field: $value,)+
                ..DriverTunables::production()
            }
            .check_floors()
            .map_err(|below| below.field)
        };
    }

    /// Every field one below its floor is refused, and the refusal names it.
    #[test]
    fn each_field_one_below_its_floor_is_refused_by_name() {
        let production = DriverTunables::production();
        let mut checked = 0;
        for (field, (name, _, floor)) in DriverTunables::FIELDS.iter().zip(production.floor_table())
        {
            assert_eq!(field.name, name, "the two tables keep one order");
            if floor == 0 {
                continue;
            }
            let mut below = production;
            (field.set)(&mut below, floor - 1);
            assert_eq!(
                below.check_floors().map_err(|b| b.field),
                Err(name),
                "{name} one below its floor"
            );
            checked += 1;
        }
        assert!(checked > 0, "some field has a floor above zero");
    }

    /// A field whose floor is zero takes zero.
    #[test]
    fn each_field_with_floor_zero_takes_zero() {
        let production = DriverTunables::production();
        let mut checked = 0;
        for (field, (name, _, floor)) in DriverTunables::FIELDS.iter().zip(production.floor_table())
        {
            if floor != 0 {
                continue;
            }
            let mut zero = production;
            (field.set)(&mut zero, 0);
            assert_eq!(zero.check_floors(), Ok(()), "{name} at zero");
            checked += 1;
        }
        assert!(checked > 0, "some field has a floor of zero");
    }

    /// A floor above one: the value at the floor passes, one below it is
    /// refused.
    #[test]
    fn each_raised_floor_takes_its_floor_and_refuses_one_less() {
        let ms = Duration::from_millis;
        // A zero base is refused by name, never divided by.
        assert_eq!(
            with!(election_timeout_base: 0),
            Err("election_timeout_base")
        );
        assert_eq!(with!(election_backoff_doublings: 2), Ok(()));
        assert_eq!(
            with!(election_backoff_doublings: 1),
            Err("election_backoff_doublings")
        );
        assert_eq!(with!(reconfigure_backoff_max_ticks: 2), Ok(()));
        assert_eq!(
            with!(reconfigure_backoff_max_ticks: 1),
            Err("reconfigure_backoff_max_ticks")
        );
        assert_eq!(with!(delivery_batch: 24), Ok(()));
        assert_eq!(with!(delivery_batch: 23), Err("delivery_batch"));
        assert_eq!(with!(slow_busy_permille: BUSY_MARGIN + 1), Ok(()));
        assert_eq!(
            with!(slow_busy_permille: BUSY_MARGIN),
            Err("slow_busy_permille")
        );
        assert_eq!(with!(load_interval: LOAD_INTERVAL_FLOOR), Ok(()));
        assert_eq!(
            with!(load_interval: LOAD_INTERVAL_FLOOR.saturating_sub(ms(1))),
            Err("load_interval")
        );
        assert_eq!(with!(scrub_rate: SCRUB_RATE_FLOOR), Ok(()));
        assert_eq!(with!(scrub_rate: SCRUB_RATE_FLOOR - 1), Err("scrub_rate"));
        assert_eq!(with!(scrub_pass_within: SCRUB_PASS_WITHIN_FLOOR), Ok(()));
        assert_eq!(
            with!(scrub_pass_within: SCRUB_PASS_WITHIN_FLOOR.saturating_sub(ms(1))),
            Err("scrub_pass_within")
        );
    }

    /// The lease outlasts a renewal, a machine is down only after a missed
    /// renewal, and it is re-placed only once it is down: each bound is one
    /// millisecond above the one it follows.
    #[test]
    fn each_chained_floor_sits_one_millisecond_above_the_one_it_follows() {
        let ms = Duration::from_millis;
        let renew = ms(1000);
        assert_eq!(
            with!(election_renew: renew, election_lease: renew + ms(1)),
            Ok(())
        );
        assert_eq!(
            with!(election_renew: renew, election_lease: renew),
            Err("election_lease")
        );
        assert_eq!(
            with!(election_renew: renew, machine_down_after: renew),
            Err("machine_down_after")
        );
        assert_eq!(
            with!(
                election_renew: renew,
                election_lease: renew + ms(1),
                machine_down_after: renew + ms(1),
                machine_replace_after: renew + ms(2)
            ),
            Ok(())
        );
        assert_eq!(
            with!(
                machine_down_after: ms(20_000),
                machine_replace_after: ms(20_000)
            ),
            Err("machine_replace_after")
        );
    }

    #[test]
    fn a_refusal_reads_as_field_value_and_floor() {
        let below = BelowFloor {
            field: "tick_interval",
            value: 0,
            floor: 1,
        };
        assert_eq!(below.to_string(), "tick_interval = 0 is below its floor 1");
    }

    #[test]
    fn the_handover_stall_budget_outlasts_its_resend_cadence() {
        let edge = DriverTunables {
            election_timeout_base: 5,
            reconfigurer_resend_ticks: 4,
            reconfigure_timeout_elections: 1,
            ..DriverTunables::default()
        };
        assert_eq!(edge.check_floors(), Ok(()));
        let equal = DriverTunables {
            reconfigurer_resend_ticks: 5,
            ..edge
        };
        assert_eq!(
            equal.check_floors(),
            Err(BelowFloor {
                field: "reconfigure_timeout_elections",
                value: 1,
                floor: 2,
            })
        );
    }
}
