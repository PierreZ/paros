//! A durability moment worth a crash (#294): the shipped driver names it with
//! `hint!`, and the simulator decides whether to strike. Outside a
//! simulation the site is inert: no draw, no lock, no allocation.
//!
//! [`crash_moment!`] wraps one site. It consults the hint only when `armed`
//! (the moment has something to lose, so an unarmed site spends no draw), and
//! a strike that kills the process reports its `reachable!` first: the
//! future never resolves after a kill. Each invocation is its own buggify
//! location (`hint!` reads the caller's `file!`/`line!`).

/// Name a crash-worthy moment: `crash_moment!(armed, "label", prob,
/// "reachable message")`.
macro_rules! crash_moment {
    ($armed:expr, $label:literal, $prob:expr, $reached:literal) => {
        if $armed {
            let hinted = moonpool_buggify::hint!($label, $prob);
            if hinted.strike() == moonpool_buggify::hint::Strike::Killed {
                moonpool_assertions::reachable!($reached);
                tracing::info!(moment = $label, "crashed");
            }
            hinted.await;
        }
    };
}

pub(crate) use crash_moment;
