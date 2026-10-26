//! What the operator claims about the store a node boots on ([`BootKind`],
//! #147), the boot's refusals, and the driver's typed exit ([`RunError`]).

use moonpool_core::SimulationError;

use crate::StorageError;

/// What the operator claims about the store [`paros_roles::node::run_node`] is handed
/// (#147): configuration data, never inferred from the store's contents.
///
/// The claim is judged against the store's **format marker**
/// ([`crate::LogStorage::is_formatted`]): a store that has ever belonged to
/// a member carries one, written by the driver on the identity's first boot
/// and never removed. An existing member whose store carries no marker has
/// lost its disk — *amnesia*, not a clean crash — and its durable promise
/// with it; booting it would answer a Phase 1 with "nothing accepted" for
/// slots it once voted on, so the driver refuses
/// ([`BootRefusal::Amnesia`]). A first boot on a store that already carries
/// a marker is two identities on one disk, refused as well
/// ([`BootRefusal::AlreadyFormatted`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BootKind {
    /// This identity has never been provisioned: the store is empty and the
    /// driver formats it (writes the marker, durably) before the core reads
    /// anything.
    FirstBoot,
    /// This identity was provisioned before: the store must carry the marker.
    ExistingMember,
}

/// Why [`paros_roles::node::run_node`] refused to boot (#147): the operator's
/// [`BootKind`] claim and the store's format marker disagree. An operating
/// error — a result value, never an assert — because the claim is external
/// input.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BootRefusal {
    /// An existing member's store carries no format marker: the disk was
    /// lost, and the durable promise with it. The identity never rejoins;
    /// the cluster replaces it by reconfiguration.
    Amnesia,
    /// A first boot found a store already formatted: another identity's
    /// disk, or a provisioning mistake. Nothing is written.
    AlreadyFormatted,
    /// An existing member's store was formatted under another configuration
    /// than the one the operator hands it now (#207): the bootstrap
    /// membership, the quorum system, the counts or the identity changed
    /// across a restart. These are safety inputs, never edited in place —
    /// the operator restores the configuration the store was provisioned
    /// with, and changes the membership by reconfiguration. Nothing is
    /// written.
    ConfigMismatch,
}

impl BootRefusal {
    /// The refusal's stable label, for traces and exit reports.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            BootRefusal::Amnesia => "amnesia",
            BootRefusal::AlreadyFormatted => "already_formatted",
            BootRefusal::ConfigMismatch => "config_mismatch",
        }
    }
}

/// Why a driver loop stopped, typed — the shared exit of every provider-generic
/// driver in this crate ([`paros_roles::node::run_node`] and [`paros_roles::matchmaker::run_matchmaker`]).
/// The driver's *domain* outcomes — a crash it
/// decided to take — are first-class variants a caller matches on; a moonpool
/// [`SimulationError`] appears only wrapped in [`RunError::Infra`], for genuine
/// provider/infrastructure failures. The simulation's error type never carries
/// a protocol-layer decision.
#[derive(Debug)]
pub enum RunError {
    /// A [`crate::LogStorage`] (or [`crate::matchmaker::MatchmakerStorage`]) call failed
    /// and the driver took its fail-stop crash
    /// decision — never an incidental error propagation. In **production**
    /// this is a crash-only process exit; recovery is the next boot. In
    /// simulation the loop recovers the same way: re-run the
    /// driver against whatever the disk *actually* holds (the recovery
    /// path must be correct for both outcomes of an ambiguous write; see
    /// [`crate::WriteOutcome`]).
    Storage(StorageError),
    /// The driver refused to boot this identity on this store (#147): the
    /// operator's [`BootKind`] claim and the store's format marker disagree.
    /// Nothing was written and no message left; the identity stays down
    /// until the operator resolves the claim (an amnesiac member is replaced
    /// by reconfiguration, never rebooted).
    Refused(BootRefusal),
    /// A provider/infrastructure failure (bind, listen, address parsing): the
    /// only place a [`SimulationError`] escapes the driver, and a genuine
    /// failure — never a recovery signal.
    Infra(SimulationError),
}

impl std::fmt::Display for RunError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RunError::Storage(e) => write!(f, "storage fault, crashing: {e}"),
            RunError::Refused(BootRefusal::Amnesia) => write!(
                f,
                "boot refused: an existing member's store carries no format marker (amnesia)"
            ),
            RunError::Refused(BootRefusal::AlreadyFormatted) => {
                write!(
                    f,
                    "boot refused: a first boot on a store that is already formatted"
                )
            }
            RunError::Refused(BootRefusal::ConfigMismatch) => write!(
                f,
                "boot refused: the store was formatted under another configuration \
                 (restore the configuration it was provisioned with)"
            ),
            RunError::Infra(e) => write!(f, "infrastructure failure: {e}"),
        }
    }
}

impl std::error::Error for RunError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            RunError::Storage(e) => Some(e),
            RunError::Infra(e) => Some(e),
            RunError::Refused(_) => None,
        }
    }
}

impl From<SimulationError> for RunError {
    fn from(e: SimulationError) -> Self {
        RunError::Infra(e)
    }
}
