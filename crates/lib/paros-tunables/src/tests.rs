//! The driver tunables' derived values: the core's limits and a machine's
//! own client tunables.

use std::time::Duration;

use paros_core::contract::limits;

use super::{ClientTunables, DriverTunables};

#[test]
fn the_default_limits_are_the_core_ceilings_and_the_read_window() {
    let tunables = DriverTunables::default();
    let got = tunables.limits();
    assert_eq!(got.recovery_page, limits::RECOVERY_BATCH);
    assert_eq!(got.promise_page, limits::PROMISE_BATCH);
    assert_eq!(got.resend_page, limits::RESEND_BATCH);
    assert_eq!(got.apply_page, limits::APPLY_BATCH);
    assert_eq!(got.registry_page, limits::REGISTRY_PAGE);
    assert_eq!(got.read_window, tunables.read_retry_ticks);
}

#[test]
fn each_page_is_capped_at_the_core_ceiling() {
    let got = DriverTunables {
        recovery_page: usize::MAX,
        promise_page: usize::MAX,
        resend_page: usize::MAX,
        apply_page: usize::MAX,
        registry_page: usize::MAX,
        ..DriverTunables::default()
    }
    .limits();
    assert_eq!(got.recovery_page, limits::RECOVERY_BATCH);
    assert_eq!(got.promise_page, limits::PROMISE_BATCH);
    assert_eq!(got.resend_page, limits::RESEND_BATCH);
    assert_eq!(got.apply_page, limits::APPLY_BATCH);
    assert_eq!(got.registry_page, limits::REGISTRY_PAGE);
}

#[test]
fn a_zero_page_is_raised_to_one_and_a_small_page_is_kept() {
    let got = DriverTunables {
        recovery_page: 0,
        promise_page: 0,
        resend_page: 2,
        apply_page: 3,
        registry_page: 0,
        read_retry_ticks: 9,
        ..DriverTunables::default()
    }
    .limits();
    assert_eq!(got.recovery_page, 1);
    assert_eq!(got.promise_page, 1);
    assert_eq!(got.resend_page, 2);
    assert_eq!(got.apply_page, 3);
    assert_eq!(got.registry_page, 1);
    assert_eq!(got.read_window, 9);
}

#[test]
fn an_internal_client_takes_the_internal_timeout_and_budget() {
    let tunables = DriverTunables {
        internal_request_timeout: Duration::from_millis(1234),
        internal_retry_budget: 3,
        internal_retry_backoff: Duration::from_millis(56),
        ..DriverTunables::default()
    };
    let client = tunables.internal_client();
    assert_eq!(
        client,
        ClientTunables {
            request_timeout: Duration::from_millis(1234),
            read_timeout: Duration::from_millis(1234),
            retry_budget: 3,
            retry_backoff: Duration::from_millis(56),
            ..ClientTunables::default()
        }
    );
}

#[test]
#[should_panic(expected = "re-sends an ambiguous write at least once")]
fn an_internal_client_without_a_retry_is_a_programmer_error() {
    let _ = DriverTunables {
        internal_retry_budget: 0,
        ..DriverTunables::default()
    }
    .internal_client();
}
