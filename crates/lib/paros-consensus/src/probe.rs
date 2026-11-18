//! The coverage probe shim (#317): `probe!` marks a rare protocol branch
//! that the simulation sweep must reach.
//!
//! A probe only observes, like a `tracing` span. It never decides anything,
//! draws no randomness and reads no clock, so the core stays sans-IO and is
//! never buggified. With the `assertions` feature on (the default) a probe is
//! moonpool's `reachable` / `sometimes` accounting: outside a simulation no
//! assertion table is installed and a probe is one pointer read. With the
//! feature off a probe compiles to nothing and its condition is never
//! evaluated, so `--no-default-features` stays free of dependencies.
//!
//! The slot is the hash of the message: a core probe and a simulation
//! `assert_reachable!` with the same message share one slot. Never reword a
//! message after it lands, and keep it short with no interpolated value.
//!
//! - `probe!(reachable, "msg")`: this branch ran at least once in the sweep.
//! - `probe!(sometimes, condition, "msg")`: the condition held at least once.

/// Mark a rare branch for the simulation sweep (see the module doc).
macro_rules! probe {
    (reachable, $message:literal) => {{
        #[cfg(feature = "assertions")]
        ::moonpool_assertions::assertion_bool(
            ::moonpool_assertions::AssertKind::Reachable,
            true,
            true,
            $message,
        );
    }};
    (sometimes, $condition:expr, $message:literal) => {{
        #[cfg(feature = "assertions")]
        ::moonpool_assertions::assertion_bool(
            ::moonpool_assertions::AssertKind::Sometimes,
            true,
            $condition,
            $message,
        );
        // Type-check the condition (and keep its bindings used) without
        // evaluating it when the feature is off.
        #[cfg(not(feature = "assertions"))]
        if false {
            let _: bool = $condition;
        }
    }};
}
