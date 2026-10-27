//! The thin matchmaker sets (#496): every matchmaker that a proposed or
//! chosen set smaller than [`crate::shape::MATCHMAKER_LOSS_FLOOR`] names.
//!
//! The matchmaker-loss budget (the wipe coin and a `Batched` registry's cut)
//! once looked only at the bootstrap set. But a handover's finish proposes
//! the members that answered its freeze, so a set of three can be replaced
//! by a set of two while one member restarts. A set of two keeps no quorum
//! without either member: the wipe of one of them then left a matchmaker
//! set that no node could ever reach, and the run unavailable for good
//! (witness 15923766003842152196 of #496: generation 1 was `{0, 2}`, and
//! matchmaker 2 was wiped). The audit records every set a reconstruction
//! proposes and every set a matchmaker activates; the budget refuses a loss
//! on a matchmaker that any such set too small to spare it names.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex, PoisonError};

use moonpool_sim::{StateHandle, assert_always};

/// Well-known [`StateHandle`] key of the run's [`ThinSets`].
const THIN_SETS_KEY: &str = "paros-thin-matchmaker-sets";

/// The matchmakers that a set too small to lose one member names, run-wide:
/// a matchmaker process hosts the sets of every tenant, so a loss strikes
/// all of them at once.
#[derive(Clone, Debug, Default)]
pub(crate) struct ThinSets(Arc<Mutex<BTreeSet<u64>>>);

impl ThinSets {
    fn lock(&self) -> std::sync::MutexGuard<'_, BTreeSet<u64>> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Record a set that was proposed or put in force: when it cannot keep
    /// a quorum without one of its members, each member becomes one the
    /// budget never takes.
    pub(crate) fn note(&self, members: &[u64]) {
        if members.len() >= crate::shape::MATCHMAKER_LOSS_FLOOR {
            return;
        }
        let mut thin = self.lock();
        thin.extend(members.iter().copied());
        assert_always!(
            members.iter().all(|m| thin.contains(m)),
            "matchmaker: a thin set's members are all recorded"
        );
    }

    /// Whether the run may lose `matchmaker`: no set too small to spare it
    /// was ever proposed with it.
    pub(crate) fn spares(&self, matchmaker: u64) -> bool {
        !self.lock().contains(&matchmaker)
    }
}

/// The run's [`ThinSets`], published by whoever asks first.
pub(crate) fn thin_sets(state: &StateHandle) -> ThinSets {
    (*crate::state::published_arc(state, THIN_SETS_KEY, ThinSets::default)).clone()
}
