//! What the unit tests share: a verdict probe over moonpool's
//! always-violation counter.

use moonpool_sim::{has_always_violations, reset_always_violations};

/// Runs `body` on a clean violation counter and says whether an
/// `assert_always!` in it went red. The counter is per thread, so tests
/// that run in parallel never see each other's verdicts.
pub(crate) fn red(body: impl FnOnce()) -> bool {
    reset_always_violations();
    body();
    let red = has_always_violations();
    reset_always_violations();
    red
}
