//! Provisioning ahead of the first start (#208): format a store durably,
//! once, as the operator's own act — never as part of a plain start.
//!
//! A driver started as [`BootKind::FirstBoot`](crate::BootKind) formats its
//! store itself before the core reads a byte; that is how the simulation
//! provisions. A production deployment separates the two: `parosd
//! provision` formats every store of an identity and exits, and every
//! ordinary start is an existing member, so a start can never format — a
//! wiped volume is refused as amnesia, not silently rejoined.
//!
//! Provisioning resolves an interrupted provisioning **by reading the
//! disk**, the simulation's rule (`resolve_provisioning` in `paros-sim`): a
//! store that already carries the marker under the configuration it is
//! handed now was formatted by the interrupted run and is
//! [`Provisioned::Resumed`]; one formatted under another configuration is
//! refused as [`BootRefusal::ConfigMismatch`]. Whether the identity was
//! provisioned *before* — the second `provision` an operator must not run —
//! is the caller's record, kept outside the stores (`parosd`'s
//! provisioning record), because a store cannot tell an interrupted
//! provisioning from a completed one.

use paros_core::{MatchmakerConfig, MustSync};

use crate::driver::{BootRefusal, RunError};
use crate::matchmaker::MatchmakerStorage;
use crate::storage::LogStorage;

/// What provisioning one store did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Provisioned {
    /// The store carried no marker: it is formatted now, durably.
    Formatted,
    /// The store already carried the marker under the same configuration:
    /// an interrupted provisioning formatted it, and nothing was written.
    Resumed,
}

/// Provision a node's (or a replica's) store under the configuration it was
/// opened with ([`Storage::initial_state`](paros_core::Storage::initial_state)):
/// scan it, then write the format marker and sync it alone, exactly what a
/// first boot does before the core reads the store.
///
/// # Errors
///
/// [`RunError::Refused`] with [`BootRefusal::ConfigMismatch`] when the store
/// was formatted under another configuration (nothing was written);
/// [`RunError::Storage`] when the scan, the format or the sync failed.
#[tracing::instrument(level = "debug", skip_all)]
pub async fn provision_store<S: LogStorage>(storage: &mut S) -> Result<Provisioned, RunError> {
    storage.boot_scan().await.map_err(RunError::Storage)?;
    let (_, config) = storage.initial_state();
    match storage.formatted_config() {
        Some(formatted) if formatted == config => Ok(Provisioned::Resumed),
        Some(formatted) => {
            tracing::error!(formatted = ?formatted, operator = ?config, "provision_config_mismatch");
            Err(RunError::Refused(BootRefusal::ConfigMismatch))
        }
        None => {
            storage.format(&config).await.map_err(RunError::Storage)?;
            storage
                .sync(MustSync::Sync)
                .await
                .map_err(RunError::Storage)?;
            tracing::info!(node = config.id.0, "store_provisioned");
            Ok(Provisioned::Formatted)
        }
    }
}

/// Provision a matchmaker's registry under `config`: the matchmaker's
/// [`provision_store`].
///
/// # Errors
///
/// As [`provision_store`].
#[tracing::instrument(level = "debug", skip_all, fields(matchmaker = config.id.0))]
pub async fn provision_matchmaker_store<S: MatchmakerStorage>(
    storage: &mut S,
    config: &MatchmakerConfig,
) -> Result<Provisioned, RunError> {
    storage.boot_scan().await.map_err(RunError::Storage)?;
    match storage.formatted_config() {
        Some(formatted) if formatted == *config => Ok(Provisioned::Resumed),
        Some(formatted) => {
            tracing::error!(formatted = ?formatted, operator = ?config, "provision_config_mismatch");
            Err(RunError::Refused(BootRefusal::ConfigMismatch))
        }
        None => {
            storage.format(config).await.map_err(RunError::Storage)?;
            storage.sync().await.map_err(RunError::Storage)?;
            tracing::info!(matchmaker = config.id.0, "matchmaker_store_provisioned");
            Ok(Provisioned::Formatted)
        }
    }
}

#[cfg(test)]
mod tests {
    use paros_core::{Config, MatchmakerId, NodeId};

    use super::*;
    use crate::matchmaker::MemMatchmakerStorage;
    use crate::storage::MemStorage;

    fn config(peers: &[u64]) -> Config {
        Config {
            peers: peers.iter().copied().map(NodeId).collect(),
            nodes: peers.iter().copied().map(NodeId).collect(),
            ..Config::new(NodeId(0), paros_core::JournalIdentifier::UNSET)
        }
    }

    #[test]
    fn a_store_is_formatted_once_and_an_interrupted_run_resumes() {
        let mut store = MemStorage::new(config(&[0, 1, 2]));
        let first = futures::executor::block_on(provision_store(&mut store));
        assert!(matches!(first, Ok(Provisioned::Formatted)));
        assert_eq!(store.formatted_config(), Some(config(&[0, 1, 2])));
        let again = futures::executor::block_on(provision_store(&mut store));
        assert!(matches!(again, Ok(Provisioned::Resumed)));
    }

    #[test]
    fn a_store_formatted_under_another_configuration_is_refused() {
        let mut store = MemStorage::new(config(&[0, 1, 2]));
        futures::executor::block_on(store.format(&config(&[0, 1]))).expect("format");
        let refused = futures::executor::block_on(provision_store(&mut store));
        assert!(matches!(
            refused,
            Err(RunError::Refused(BootRefusal::ConfigMismatch))
        ));
        assert_eq!(store.formatted_config(), Some(config(&[0, 1])));
    }

    #[test]
    fn a_registry_is_formatted_once() {
        let mm = |set: &[u64]| MatchmakerConfig {
            id: MatchmakerId(0),
            bootstrap: set.iter().copied().map(MatchmakerId).collect(),
        };
        let mut store = MemMatchmakerStorage::new();
        let first = futures::executor::block_on(provision_matchmaker_store(&mut store, &mm(&[0])));
        assert!(matches!(first, Ok(Provisioned::Formatted)));
        let again = futures::executor::block_on(provision_matchmaker_store(&mut store, &mm(&[0])));
        assert!(matches!(again, Ok(Provisioned::Resumed)));
        let other =
            futures::executor::block_on(provision_matchmaker_store(&mut store, &mm(&[0, 1])));
        assert!(matches!(
            other,
            Err(RunError::Refused(BootRefusal::ConfigMismatch))
        ));
    }
}
