//! The **control plane's durability** (#685 (special tenants are tenants)):
//! how many machine failures the cell's own journals survive, and how that
//! number is chosen.
//!
//! The control plane is the special tenants' journals: the cell control
//! journal, the election journal, every tenant control journal and, on the
//! cell that hosts it, the universe directory. They are tenants like any
//! other, managed by the platform (Pierre, 2026-10-18). An `admin` call
//! (`parosctl cell configure --control-plane`) records the mode in the cell
//! control journal ([`ControlPlaneMode`]):
//!
//! - **auto** (the default, and what a cell that recorded no mode follows):
//!   the coordinator wants `F = min(target, (storage machines - 1) / 2)`,
//!   so the control plane grows as machines are admitted (as
//!   `coordinators auto` does in FDB);
//! - **manual**: the operator's `F` stays. A value the cell cannot place
//!   (`2F + 1` storage machines it does not have) is refused at apply.
//!
//! [`ControlPlane`] is what `status` shows: the mode, the `F` it wants,
//! the `F` in force, and why. The journals still live on the founding
//! members: until the moves of #685 land, the `F` in force is the founders'.

use std::fmt;

/// The largest `F` a control plane takes (as a tenant's `survives`, #523).
pub const MAX_FAILURES: u32 = 3;

/// The `target` of the auto mode a cell follows when it recorded no mode.
pub const DEFAULT_TARGET: u32 = 1;

// The default target is a value the mode accepts.
const _: () = assert!(DEFAULT_TARGET <= MAX_FAILURES);

/// How the control plane's `F` is chosen (#685).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ControlPlaneMode {
    /// The coordinator chooses `F`, up to `target`, from the storage
    /// machines the cell has.
    Auto {
        /// The `F` it grows to once the cell has `2 * target + 1` storage
        /// machines.
        target: u32,
    },
    /// The operator's `F`, kept as it is.
    Manual {
        /// The machine failures the control plane survives.
        failures: u32,
    },
}

impl Default for ControlPlaneMode {
    fn default() -> Self {
        ControlPlaneMode::Auto {
            target: DEFAULT_TARGET,
        }
    }
}

impl ControlPlaneMode {
    /// The `F` the mode names: auto's target, or manual's value.
    #[must_use]
    pub fn failures(self) -> u32 {
        match self {
            ControlPlaneMode::Auto { target } => target,
            ControlPlaneMode::Manual { failures } => failures,
        }
    }

    /// Whether the mode is auto.
    #[must_use]
    pub fn is_auto(self) -> bool {
        matches!(self, ControlPlaneMode::Auto { .. })
    }

    /// The mode's name: `auto` or `manual`.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            ControlPlaneMode::Auto { .. } => "auto",
            ControlPlaneMode::Manual { .. } => "manual",
        }
    }

    /// The `F` the mode wants on a cell with `storage` storage machines:
    /// auto grows with the machines up to its target, manual is its value.
    ///
    /// # Panics
    ///
    /// If auto wants past its target: a programmer error.
    #[must_use]
    pub fn desired(self, storage: usize) -> u32 {
        match self {
            ControlPlaneMode::Auto { target } => {
                let room = u32::try_from(storage.saturating_sub(1) / 2).unwrap_or(u32::MAX);
                let wanted = target.min(room);
                assert!(wanted <= target, "auto never wants past its target");
                wanted
            }
            ControlPlaneMode::Manual { failures } => failures,
        }
    }
}

impl fmt::Display for ControlPlaneMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ControlPlaneMode::Auto { target } => write!(f, "auto (target machines={target})"),
            ControlPlaneMode::Manual { failures } => write!(f, "manual (machines={failures})"),
        }
    }
}

/// The acceptors a control plane of `F` failures needs: `2F + 1`.
#[must_use]
pub fn acceptors(failures: u32) -> usize {
    usize::try_from(failures)
        .unwrap_or(usize::MAX)
        .saturating_mul(2)
        .saturating_add(1)
}

/// What `status` shows of the control plane (#685): the mode, the `F` it
/// wants, the `F` in force, and why.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ControlPlane {
    /// The mode the cell control journal records, or the default.
    pub mode: ControlPlaneMode,
    /// The storage machines the cell has: the founding members and every
    /// registered storage machine that is not draining or retired.
    pub storage: usize,
    /// The `F` the mode wants.
    pub desired: u32,
    /// The `F` the control plane's acceptors give now.
    pub in_force: u32,
}

impl ControlPlane {
    /// The control plane of a cell in `mode` with `storage` storage
    /// machines, whose journals have `members` acceptors now.
    ///
    /// # Panics
    ///
    /// If the journals have no acceptor: a programmer error.
    #[must_use]
    pub fn new(mode: ControlPlaneMode, storage: usize, members: usize) -> Self {
        assert!(members >= 1, "a control plane has an acceptor");
        let in_force = u32::try_from((members - 1) / 2).unwrap_or(u32::MAX);
        ControlPlane {
            mode,
            storage,
            desired: mode.desired(storage),
            in_force,
        }
    }

    /// Why the control plane wants its `F`, for people.
    #[must_use]
    pub fn why(&self) -> String {
        match self.mode {
            ControlPlaneMode::Auto { target } if self.desired < target => format!(
                "auto: {} storage machine(s) place machines={} of target {target}",
                self.storage, self.desired
            ),
            ControlPlaneMode::Auto { .. } => "auto: at its target".to_string(),
            ControlPlaneMode::Manual { .. } => "manual: set by an operator".to_string(),
        }
    }

    /// The label a journal view shows as its desired shape.
    #[must_use]
    pub fn label(&self) -> String {
        format!(
            "{} · machines={} wanted · machines={} in force",
            self.mode.as_str(),
            self.desired,
            self.in_force
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auto_grows_with_the_storage_machines_up_to_its_target() {
        let auto = ControlPlaneMode::Auto { target: 1 };
        assert_eq!(auto.desired(0), 0);
        assert_eq!(auto.desired(1), 0);
        assert_eq!(auto.desired(2), 0);
        assert_eq!(auto.desired(3), 1);
        assert_eq!(auto.desired(9), 1);
        let two = ControlPlaneMode::Auto { target: 2 };
        assert_eq!(two.desired(4), 1);
        assert_eq!(two.desired(5), 2);
    }

    #[test]
    fn manual_keeps_its_value() {
        let manual = ControlPlaneMode::Manual { failures: 2 };
        assert_eq!(manual.desired(1), 2);
        assert_eq!(manual.desired(9), 2);
    }

    #[test]
    fn a_cell_with_no_mode_follows_auto_with_target_one() {
        assert_eq!(
            ControlPlaneMode::default(),
            ControlPlaneMode::Auto { target: 1 }
        );
    }

    #[test]
    fn the_control_plane_reads_its_in_force_value_from_its_acceptors() {
        let plane = ControlPlane::new(ControlPlaneMode::default(), 3, 1);
        assert_eq!((plane.desired, plane.in_force), (1, 0));
        assert!(plane.label().contains("machines=1 wanted"));
        assert!(plane.why().starts_with("auto: at"));
        let plane = ControlPlane::new(ControlPlaneMode::default(), 1, 1);
        assert!(plane.why().contains("of target 1"));
        assert_eq!(acceptors(1), 3);
    }
}
