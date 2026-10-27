//! The timing bounds the shape derives from the drawn knobs: leases, down-after, call floors, resolve answers.

use std::sync::PoisonError;
use std::time::Duration;

use moonpool_sim::{StateHandle, assert_always, buggify_knob};
use paros::roles::node::DriverTunables;

use crate::shape::node::{BACKOFF_CEILING_MS, RESOLVE_FLOOR_MS, ROUND_TRIP_FLOOR_MS};
use crate::shape::registry;

/// The renewal period's floor (see [`NodeShape::draw`]).
pub(crate) const ELECTION_RENEW_FLOOR: Duration = Duration::from_millis(100);

/// Run `tunables`' cell election at its renewal floor, the lease and the
/// failure detector moved down with it, so each keeps its margin past the
/// renewal period.
/// The longest cell election lease any node booted under so far (#542
/// (coordinator starves its lease while it installs)): the span over which
/// `election_settles` holds one term renewing. Zero before any boot.
pub(crate) fn longest_election_lease(state: &StateHandle) -> Duration {
    let registry = registry(state);
    let guard = registry.lock().unwrap_or_else(PoisonError::into_inner);
    guard
        .nodes
        .values()
        .map(|entry| entry.shape.tunables.election_lease)
        .max()
        .unwrap_or(Duration::ZERO)
}

/// The longest `machine_down_after` any node booted under so far (#654
/// (live machine marked down)). Zero before any boot.
pub(crate) fn longest_down_after(state: &StateHandle) -> Duration {
    let registry = registry(state);
    let guard = registry.lock().unwrap_or_else(PoisonError::into_inner);
    guard
        .nodes
        .values()
        .map(|entry| entry.shape.tunables.machine_down_after)
        .max()
        .unwrap_or(Duration::ZERO)
}

/// The longest `internal_request_timeout` any node booted under so far
/// (#654 (live machine marked down)): how long a cell coordinator's
/// `Identify` may wait. Zero before any boot.
pub(crate) fn longest_internal_timeout(state: &StateHandle) -> Duration {
    let registry = registry(state);
    let guard = registry.lock().unwrap_or_else(PoisonError::into_inner);
    guard
        .nodes
        .values()
        .map(|entry| entry.shape.tunables.internal_request_timeout)
        .max()
        .unwrap_or(Duration::ZERO)
}

/// The lowest slow-busy bound any node booted under so far, in
/// thousandths (#654 (live machine marked down)): the bound a cell
/// coordinator may judge a machine by. The default before any boot.
pub(crate) fn lowest_slow_busy(state: &StateHandle) -> u32 {
    let registry = registry(state);
    let guard = registry.lock().unwrap_or_else(PoisonError::into_inner);
    guard
        .nodes
        .values()
        .map(|entry| entry.shape.tunables.slow_busy_permille)
        .min()
        .unwrap_or(DriverTunables::default().slow_busy_permille)
}

/// The longest re-placement bound any node booted under so far (#475
/// (re-place after the Down bound)): a cell coordinator may wait that long
/// before it re-places a machine's roles. Zero before any boot.
pub(crate) fn longest_replace_after(state: &StateHandle) -> Duration {
    let registry = registry(state);
    let guard = registry.lock().unwrap_or_else(PoisonError::into_inner);
    guard
        .nodes
        .values()
        .map(|entry| entry.shape.tunables.machine_replace_after)
        .max()
        .unwrap_or(Duration::ZERO)
}

pub(crate) fn renew_at_floor(tunables: &mut DriverTunables) {
    let cut = tunables.election_renew.saturating_sub(ELECTION_RENEW_FLOOR);
    tunables.election_renew -= cut;
    tunables.election_lease -= cut;
    tunables.machine_down_after -= cut;
    tunables.machine_replace_after -= cut;
}

/// The longest a machine of this campaign may take over one `Resolve`
/// answer (#549 (resolve oracle red)): the drawn bound never passes the
/// default (`NodeShape::draw`), and the production profile ships its own.
pub(crate) fn resolve_answer_ceiling() -> Duration {
    DriverTunables::default()
        .resolve_answer_within
        .max(DriverTunables::production().resolve_answer_within)
}

/// One `Resolve` call's own round trip on the recovery tail, past the
/// machine's answer bound: two round trips of the driver's floor.
pub(crate) const RESOLVE_ROUND_TRIP: Duration = Duration::from_millis(2 * ROUND_TRIP_FLOOR_MS);

/// The slowest round trip a link keeps into the recovery tail, in
/// milliseconds: moonpool's cross-datacenter link is at most 80 ms one way,
/// and its per-pair latency (`max_pair_latency`, under 100 ms each way) is
/// drawn once per pair and never healed. Recovery mode stops new pairs from
/// degrading, not a pair that already is.
pub(crate) const SLOWEST_ROUND_TRIP_MS: u64 = 2 * (80 + 100);

/// The wall-clock floor of a drawn connect timeout, the driver's
/// `connection_timeout` and the client's `connect_timeout_ms`. A connect is
/// not one round trip: the runtime's timeout covers the TCP connect and the
/// session handshake, TLS on half the seeds ([`crate::tls`]), so two round
/// trips at least. Below that, a slow pair never connects for the whole run,
/// a one-way partition wearing a knob's clothes. Witness (cite only): seed
/// 8160241045276543599 on main 9bcbd0f, a TLS seed of three nodes where
/// node 0 was wiped; node 2 drew a 251 ms timeout, and its connect to node 1
/// (a 231 ms round trip, 715 ms for a first delivery with its connect)
/// timed out 63 times in a row, so no ballot ever met a promise quorum.
pub(crate) const CONNECT_FLOOR_MS: u64 = 1_000;

const _: () = assert!(CONNECT_FLOOR_MS >= 2 * SLOWEST_ROUND_TRIP_MS);
const _: () = assert!(CONNECT_FLOOR_MS > ROUND_TRIP_FLOOR_MS);

/// The wall-clock floor of a drawn `internal_request_timeout`: one internal
/// call that must dial its peer first. A machine's call goes to a peer it
/// may not have a connection to (a renamed founding member reads the
/// election journal from its peers at a new address, and every round
/// dials again), so it waits out a connect ([`CONNECT_FLOOR_MS`]), then a
/// read the peer confirms with a quorum: its own round trip and the
/// confirmation's, each on the slowest link. Below that, a call that dials
/// first is ambiguous every time. Witness (cite only): seed
/// 17884888912503425398 on a0c4aa6, a renamed founder that drew 252 ms;
/// each of its election reads timed out, so it never registered and never
/// answered `Resolve` after chaos (#565 (renamed founder's registration
/// read times out)).
pub(crate) const INTERNAL_CALL_FLOOR_MS: u64 = CONNECT_FLOOR_MS + 2 * SLOWEST_ROUND_TRIP_MS;

const _: () = assert!(INTERNAL_CALL_FLOOR_MS > CONNECT_FLOOR_MS);
const _: () = assert!(
    INTERNAL_CALL_FLOOR_MS < 2000,
    "the floor sits under production's"
);

/// How long one `Resolve` answer may take. Floor four round trips:
/// every answer reads two tails in sequence (the registry, then
/// the directory), and a read served by a peer under chaos takes
/// up to about two (witness 7489717366062895407: 259 ms fit one
/// read and the machine never answered). A fold from position 0
/// then takes several answers, each going on from where the last
/// one stopped (#457). The final resolve check holds every
/// founding member to an answer after chaos. On a slow-link seed,
/// half the time the floor itself: a fold then spans several
/// answers, and an answer is cut short with its folds kept (that
/// gate missed the sweep of PR #551, #446).
pub(crate) fn resolve_answer_within(slow_link: bool) -> Duration {
    if slow_link && moonpool_sim::buggify_with_prob!(0.5) {
        Duration::from_millis(RESOLVE_FLOOR_MS)
    } else {
        Duration::from_millis(buggify_knob!(2000_u64, RESOLVE_FLOOR_MS..2001_u64))
    }
}

/// The tenant scrubber's pace (#362 (background scrubbing)): its rate,
/// its full-pass bound and its records per request.
pub(crate) fn scrub_pace() -> (u64, Duration, usize) {
    // Floor 1 KiB/s (the shipped floor): a pass still moves one small
    // record a second. The low end leaves a pass unfinished inside a run,
    // so its progress record outlives terms; the high end reads a whole
    // journal in one round.
    let rate = buggify_knob!(256 * 1024_u64, 1024_u64..(1024 * 1024 + 1));
    // The full-pass bound. Floor 1 s (the shipped floor). The range stays
    // under the chaos window, so a run sees several passes and, at the low
    // end with a slow rate, late ones.
    let within = Duration::from_millis(buggify_knob!(10_000_u64, 1000_u64..20_001_u64));
    // Records per scrub request. Floor 1: one record per page, the most
    // requests per pass.
    let page = buggify_knob!(64_usize, 1_usize..257_usize);
    (rate, within, page)
}

/// The election backoff's ceiling is wall-clock too: a capped base
/// election timeout (`base << doublings` ticks) stays within
/// `BACKOFF_CEILING_MS`, production's 8 s, unless the floor of two
/// doublings alone passes it (12 ticks of 200 ms: 9.6 s). At a slow
/// tick, six doublings held two leaderless nodes for 27 to 77 s
/// each, and a node that learned a new matchmaker set waited one
/// more capped timeout to ask it, past the recovery budget (witness:
/// seed 12451707706557626383, #232 (multiple cells)'s sweep).
pub(crate) fn cap_backoff(drawn: &mut DriverTunables, tick_ms: u64) {
    let base_ms = drawn.election_timeout_base * tick_ms;
    let capped = |doublings: u32| base_ms << doublings;
    while drawn.election_backoff_doublings > 2
        && capped(drawn.election_backoff_doublings) > BACKOFF_CEILING_MS
    {
        drawn.election_backoff_doublings -= 1;
    }
    assert_always!(
        drawn.election_backoff_doublings == 2
            || capped(drawn.election_backoff_doublings) <= BACKOFF_CEILING_MS,
        "node: a capped election backoff fits the recovery tail",
        { "capped_ms" => capped(drawn.election_backoff_doublings) }
    );
}
