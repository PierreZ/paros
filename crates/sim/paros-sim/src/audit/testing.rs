//! What the audit's unit tests share: a verdict probe over moonpool's
//! always-violation counter, and a clock that never moves.

use std::time::Duration;

use moonpool_sim::{TimeError, TimeProvider, has_always_violations, reset_always_violations};

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

/// A clock that never moves: the audit reads it only to stamp floors.
#[derive(Clone)]
pub(crate) struct FrozenClock;

impl TimeProvider for FrozenClock {
    async fn sleep(&self, _duration: Duration) -> Result<(), TimeError> {
        Ok(())
    }

    fn now(&self) -> Duration {
        Duration::ZERO
    }

    async fn timeout<F, T>(&self, _duration: Duration, future: F) -> Result<T, TimeError>
    where
        F: std::future::Future<Output = T> + Send,
        T: Send,
    {
        Ok(future.await)
    }
}
