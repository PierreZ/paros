//! The faults a seed draws on its own (#951 (live Engineering pages)):
//! the config's [`Chaos`] mapped onto moonpool's chaos surfaces. Nothing
//! here decides a fault: moonpool draws each one from the seed, on the
//! simulated world, and the machines run the shipped code.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

use moonpool_sim::{
    Attrition, AttritionScope, AttritionVictims, Chaos as Surface, ChaosMode, HintVeto,
    StorageFault, StorageFaultMask,
};

use crate::playground::machine::MACHINE_GROUP;
use crate::playground::model::Chaos;

/// The longest a crashed machine stays down before moonpool restarts it, in
/// simulated milliseconds: short, so a run of a few seconds shows the
/// recovery.
const RECOVERY_MS: std::ops::Range<usize> = 500..3000;

/// The surfaces `chaos` turns on. An empty list when it asks for nothing.
#[must_use]
pub(crate) fn surfaces(chaos: &Chaos) -> Vec<Surface> {
    let mut surfaces = Vec::new();
    if chaos.network {
        surfaces.push(Surface::Network(ChaosMode::Swarm));
    }
    if chaos.storage {
        surfaces.push(Surface::Storage(ChaosMode::Swarm));
    } else if chaos.writeback {
        // One family alone, on every seed: random intensities, no subset
        // draw that could leave it off.
        surfaces.push(Surface::Storage(ChaosMode::Random));
    }
    if chaos.crashes {
        surfaces.push(Surface::Attrition {
            config: Attrition {
                // One machine down at a time: the cell keeps a majority.
                max_dead: 1,
                prob_graceful: 0.0,
                prob_crash: 1.0,
                // A wiped machine never rejoins (boot safety): no wipes.
                prob_wipe: 0.0,
                recovery_delay_ms: Some(RECOVERY_MS),
                grace_period_ms: None,
                scope: AttritionScope::PerProcess,
                victims: AttritionVictims::group(MACHINE_GROUP),
                wipe_budget: None,
            },
            mode: ChaosMode::Random,
        });
    }
    surfaces
}

/// The storage families `chaos` keeps. A disk that fails for good parks
/// every later I/O forever, which leaves nothing to watch, so it is never
/// drawn here.
#[must_use]
pub(crate) fn storage_mask(chaos: &Chaos) -> StorageFaultMask {
    if chaos.storage {
        StorageFaultMask::all().without(StorageFault::DiskFailure)
    } else if chaos.writeback {
        StorageFaultMask::none().with(StorageFault::WritebackError)
    } else {
        StorageFaultMask::none()
    }
}

/// How many machines a run may crash at a hint: enough to see a few
/// strikes and the recoveries, few enough that the operator's steps go on.
pub(crate) const HINT_KILLS: u32 = 4;

/// The harness's budget of hint crashes: none before `init` ends (a cell
/// that is not formed yet has nothing to recover), then [`HINT_KILLS`].
#[derive(Debug)]
pub(crate) struct HintBudget {
    armed: AtomicBool,
    left: AtomicU32,
}

impl Default for HintBudget {
    fn default() -> Self {
        Self {
            armed: AtomicBool::new(false),
            left: AtomicU32::new(HINT_KILLS),
        }
    }
}

impl HintBudget {
    /// The cell is formed: hints may strike from now on.
    pub(crate) fn arm(&self) {
        self.armed.store(true, Ordering::Relaxed);
    }

    /// Whether one more crash at a hint is permitted; takes it when it is.
    pub(crate) fn take(&self) -> bool {
        self.armed.load(Ordering::Relaxed)
            && self
                .left
                .try_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_sub(1))
                .is_ok()
    }
}

/// The veto moonpool asks before it crashes a machine at a hint.
pub(crate) fn veto(budget: Arc<HintBudget>) -> HintVeto {
    HintVeto::new(move |_, _| budget.take())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_hint_budget_opens_at_init_and_runs_out() {
        let budget = HintBudget::default();
        assert!(!budget.take(), "no strike before the cell is formed");
        budget.arm();
        for _ in 0..HINT_KILLS {
            assert!(budget.take());
        }
        assert!(!budget.take(), "the budget ran out");
    }

    #[test]
    fn no_chaos_turns_on_no_surface() {
        let none = Chaos::default();
        assert!(!none.any());
        assert!(surfaces(&none).is_empty());
        assert!(!storage_mask(&none).contains(StorageFault::WritebackError));
    }

    #[test]
    fn writeback_alone_keeps_one_family() {
        let chaos = Chaos {
            writeback: true,
            ..Chaos::default()
        };
        assert_eq!(surfaces(&chaos).len(), 1);
        let mask = storage_mask(&chaos);
        assert!(mask.contains(StorageFault::WritebackError));
        assert!(!mask.contains(StorageFault::Corruption));
    }

    #[test]
    fn storage_chaos_never_fails_a_disk_for_good() {
        let chaos = Chaos {
            storage: true,
            writeback: true,
            ..Chaos::default()
        };
        let mask = storage_mask(&chaos);
        assert!(!mask.contains(StorageFault::DiskFailure));
        assert!(mask.contains(StorageFault::Corruption));
    }

    #[test]
    fn crashes_strike_only_machines() {
        let chaos = Chaos {
            crashes: true,
            ..Chaos::default()
        };
        let drawn = surfaces(&chaos);
        let [Surface::Attrition { config, .. }] = drawn.as_slice() else {
            panic!("crashes are one attrition regime");
        };
        assert_eq!(config.victims, AttritionVictims::group(MACHINE_GROUP));
        assert_eq!(config.max_dead, 1);
        assert!(config.prob_wipe.abs() < f64::EPSILON);
    }
}
