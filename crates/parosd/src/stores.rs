//! [`DirStores`]: the production [`JournalStores`] — one directory per
//! journal under the node's data directory, each a [`JournalStorage`] over
//! Tokio's filesystem.
//!
//! # The operator's claim
//!
//! [`BootKind`] is the operator's statement, never inferred from a store's
//! contents (#147): the driver judges it against the store's format marker
//! and refuses a disagreement. Until provisioning lands (#208) the claim for
//! the journals a node boots with is one flag on the command line, and this
//! opener hands it to the **first** open of each journal in the process; a
//! re-open after a quarantine is the same identity on the same disk, so it
//! is always [`BootKind::ExistingMember`].
//!
//! Journals the directory creates at runtime (#189) are not opened here:
//! this opener keeps the trait's default `create` (refuse), because their
//! claim must come from the operator's provisioning record (#208) — reading
//! it off the disk would let a wiped node rejoin a created journal as a
//! first boot, exactly the amnesia #147 refuses. `parosd` runs no system
//! journals yet, so nothing asks.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use moonpool_core::TokioStorageProvider;
use paros::{
    BootKind, Config, JournalId, JournalStorage, JournalStoreConfig, JournalStores, NoAudit,
};

/// The directory a journal's store lives in, under `data_dir`.
#[must_use]
pub fn journal_dir(data_dir: &Path, journal: JournalId) -> PathBuf {
    data_dir.join(format!("journal-{}", journal.0))
}

/// One store per journal under a data directory (see the module doc).
#[derive(Debug)]
pub struct DirStores {
    provider: TokioStorageProvider,
    data_dir: PathBuf,
    layout: JournalStoreConfig,
    /// The journals the node boots with, in order.
    genesis: Vec<JournalId>,
    /// Every journal's configuration.
    configs: BTreeMap<JournalId, Config>,
    /// The claim the next open of a journal hands the driver; absent means
    /// [`BootKind::ExistingMember`].
    claims: BTreeMap<JournalId, BootKind>,
}

impl DirStores {
    /// The stores of a node serving `configs` (one per journal, each naming
    /// its journal in [`Config::journal`]) under `data_dir`, every genesis
    /// journal claimed `boot` on its first open.
    ///
    /// # Panics
    ///
    /// When two configurations name the same journal.
    #[must_use]
    pub fn new(
        data_dir: impl Into<PathBuf>,
        configs: Vec<Config>,
        layout: JournalStoreConfig,
        boot: BootKind,
    ) -> Self {
        let genesis: Vec<JournalId> = configs.iter().map(|config| config.journal).collect();
        let configs: BTreeMap<JournalId, Config> = configs
            .into_iter()
            .map(|config| (config.journal, config))
            .collect();
        assert_eq!(
            configs.len(),
            genesis.len(),
            "one configuration per journal"
        );
        let claims = genesis.iter().map(|&journal| (journal, boot)).collect();
        Self {
            provider: TokioStorageProvider::new(),
            data_dir: data_dir.into(),
            layout,
            genesis,
            configs,
            claims,
        }
    }

    fn dir(&self, journal: JournalId) -> String {
        journal_dir(&self.data_dir, journal)
            .to_string_lossy()
            .into_owned()
    }
}

impl JournalStores for DirStores {
    type Store = JournalStorage<TokioStorageProvider>;
    type Audit = NoAudit;

    fn journals(&self) -> Vec<JournalId> {
        self.genesis.clone()
    }

    fn open(&mut self, journal: JournalId) -> Option<(Self::Store, BootKind)> {
        let config = self.configs.get(&journal)?.clone();
        // The claim is consumed: every later open in this process is the
        // same identity on the same disk.
        let boot = self
            .claims
            .remove(&journal)
            .unwrap_or(BootKind::ExistingMember);
        let store = JournalStorage::new(
            self.provider.clone(),
            self.dir(journal),
            config,
            self.layout,
        );
        tracing::info!(journal = journal.0, ?boot, dir = %store.dir(), "journal_store_opened");
        Some((store, boot))
    }

    fn audit(&self, _journal: JournalId) -> NoAudit {
        NoAudit
    }

    fn quarantined(&mut self, journal: JournalId) {
        tracing::warn!(journal = journal.0, "journal_quarantined");
    }
}

#[cfg(test)]
mod tests {
    use paros::NodeId;

    use super::*;

    fn config(journal: u64) -> Config {
        Config {
            id: NodeId(0),
            peers: vec![NodeId(0)],
            journal: JournalId(journal),
            ..Config::default()
        }
    }

    fn opener(dir: &Path, boot: BootKind) -> DirStores {
        DirStores::new(
            dir,
            vec![config(128), config(129)],
            JournalStoreConfig::small(),
            boot,
        )
    }

    #[test]
    fn the_boot_claim_is_handed_to_the_first_open_only() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut stores = opener(dir.path(), BootKind::FirstBoot);
        assert_eq!(stores.journals(), vec![JournalId(128), JournalId(129)]);
        let (store, boot) = stores.open(JournalId(128)).expect("opens");
        assert_eq!(boot, BootKind::FirstBoot);
        assert_eq!(
            store.dir(),
            journal_dir(dir.path(), JournalId(128))
                .to_str()
                .expect("utf-8")
        );
        // A quarantine's re-open is the same identity.
        let (_, boot) = stores.open(JournalId(128)).expect("re-opens");
        assert_eq!(boot, BootKind::ExistingMember);
        let (_, boot) = stores.open(JournalId(129)).expect("opens");
        assert_eq!(boot, BootKind::FirstBoot);
        assert!(stores.open(JournalId(130)).is_none(), "an unknown journal");
    }

    #[test]
    fn an_existing_member_is_claimed_existing_on_every_open() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut stores = opener(dir.path(), BootKind::ExistingMember);
        let (_, boot) = stores.open(JournalId(128)).expect("opens");
        assert_eq!(boot, BootKind::ExistingMember);
    }
}
