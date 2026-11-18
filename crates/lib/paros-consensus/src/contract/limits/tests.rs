use super::{
    APPLY_BATCH, Limits, PROMISE_BATCH, READ_TTL_TICKS, RECOVERY_BATCH, REGISTRY_PAGE, RESEND_BATCH,
};

/// The default limits run every page at its ceiling and the read window at
/// its floor. They pass their own check.
#[test]
fn the_default_limits_are_the_ceilings() {
    let limits = Limits::default();
    assert_eq!(limits, Limits::DEFAULT);
    assert_eq!(limits.recovery_page, RECOVERY_BATCH);
    assert_eq!(limits.promise_page, PROMISE_BATCH);
    assert_eq!(limits.resend_page, RESEND_BATCH);
    assert_eq!(limits.apply_page, APPLY_BATCH);
    assert_eq!(limits.registry_page, REGISTRY_PAGE);
    assert_eq!(limits.read_window, READ_TTL_TICKS);
    limits.assert_valid();
}

/// A page of one is the smallest valid page. The check accepts it.
#[test]
fn a_page_of_one_is_valid() {
    Limits {
        recovery_page: 1,
        promise_page: 1,
        resend_page: 1,
        apply_page: 1,
        read_window: 0,
        registry_page: 1,
    }
    .assert_valid();
}

/// A page of zero can never make progress. The check refuses it.
#[test]
#[should_panic(expected = "a promise page carries at least one entry")]
fn a_page_of_zero_is_refused() {
    Limits {
        promise_page: 0,
        ..Limits::DEFAULT
    }
    .assert_valid();
}

/// A page above its ceiling is larger than a receiver accepts. The check
/// refuses it.
#[test]
#[should_panic(expected = "an apply page never exceeds its ceiling")]
fn a_page_above_its_ceiling_is_refused() {
    Limits {
        apply_page: APPLY_BATCH + 1,
        ..Limits::DEFAULT
    }
    .assert_valid();
}

/// The core keeps a read at least as long as the driver waits, and never
/// less than the floor.
#[test]
fn the_kept_read_window_is_never_below_the_floor() {
    let kept = |read_window| {
        Limits {
            read_window,
            ..Limits::DEFAULT
        }
        .kept_read_window()
    };
    assert_eq!(kept(0), READ_TTL_TICKS);
    assert_eq!(kept(READ_TTL_TICKS - 1), READ_TTL_TICKS);
    assert_eq!(kept(READ_TTL_TICKS + 5), READ_TTL_TICKS + 5);
    assert_eq!(kept(u64::MAX), u64::MAX);
}
