//! The production [`JournalStores`]: one [`JournalStorage`] per journal,
//! each in its own directory under the node's data directory, on Tokio's
//! filesystem (#206).
//!
//! ```text
//! <data-dir>/
//!   journals/<journal-id>/    one moonpool-journal per journal a node serves
//!   matchmaker/               a matchmaker's registry
//!   replica/                  a replica's chosen log
//! ```
//!
//! The boot claim is the operator's, as data (#147): the genesis journals
//! take the [`BootKind`] the process was started with. A journal the
//! directory creates (#189) is a first boot the first time it is created
//! here and an existing member after that; until `parosd provision` and its
//! provisioning record land (#208), "created here before" is read off the
//! journal's directory, which the opener makes before the store's first
//! open.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use moonpool_core::TokioStorageProvider;
use paros::{
    BootKind, Config, JournalId, JournalStorage, JournalStoreConfig, JournalStores, NoAudit,
};

/// The directory of `journal`'s store under `data_dir`.
#[must_use]
pub fn journal_dir(data_dir: &Path, journal: JournalId) -> PathBuf {
    data_dir.join("journals").join(journal.0.to_string())
}

/// The directory of a matchmaker's registry under `data_dir`.
#[must_use]
pub fn matchmaker_dir(data_dir: &Path) -> PathBuf {
    data_dir.join("matchmaker")
}

/// The directory of a replica's log under `data_dir`.
#[must_use]
pub fn replica_dir(data_dir: &Path) -> PathBuf {
    data_dir.join("replica")
}

/// A path the storage provider takes (it speaks `&str`).
#[must_use]
pub fn path_str(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

/// The node's journal stores on the real filesystem.
pub struct DirStores {
    provider: TokioStorageProvider,
    data_dir: PathBuf,
    layout: JournalStoreConfig,
    /// The genesis journals and their configurations, in id order.
    genesis: BTreeMap<JournalId, Config>,
    /// The operator's claim for the genesis journals.
    boot: BootKind,
    /// Journals the directory created naming this node (#189), with the
    /// claim their next open takes.
    created: BTreeMap<JournalId, (Config, BootKind)>,
}

impl DirStores {
    /// The stores of a node serving `genesis` under `data_dir`, booted as
    /// `boot` claims.
    #[must_use]
    pub fn new(
        data_dir: PathBuf,
        layout: JournalStoreConfig,
        genesis: BTreeMap<JournalId, Config>,
        boot: BootKind,
    ) -> Self {
        Self {
            provider: TokioStorageProvider::new(),
            data_dir,
            layout,
            genesis,
            boot,
            created: BTreeMap::new(),
        }
    }

    fn store(&self, journal: JournalId, config: Config) -> JournalStorage<TokioStorageProvider> {
        JournalStorage::new(
            self.provider.clone(),
            path_str(&journal_dir(&self.data_dir, journal)),
            config,
            self.layout,
        )
    }
}

impl JournalStores for DirStores {
    type Store = JournalStorage<TokioStorageProvider>;
    type Audit = NoAudit;

    fn journals(&self) -> Vec<JournalId> {
        self.genesis.keys().copied().collect()
    }

    fn open(&mut self, journal: JournalId) -> Option<(Self::Store, BootKind)> {
        if let Some(config) = self.genesis.get(&journal) {
            return Some((self.store(journal, config.clone()), self.boot));
        }
        let (config, boot) = self.created.get_mut(&journal)?;
        let opened = (config.clone(), *boot);
        // Every later open of a created journal is a restart of it.
        *boot = BootKind::ExistingMember;
        Some((self.store(journal, opened.0), opened.1))
    }

    fn audit(&self, _journal: JournalId) -> NoAudit {
        NoAudit
    }

    fn create(&mut self, journal: JournalId, config: Config) -> bool {
        if self.genesis.contains_key(&journal) {
            return true;
        }
        if !self.created.contains_key(&journal) {
            let dir = journal_dir(&self.data_dir, journal);
            let boot = if dir.exists() {
                BootKind::ExistingMember
            } else {
                BootKind::FirstBoot
            };
            if let Err(error) = std::fs::create_dir_all(&dir) {
                tracing::error!(journal = journal.0, %error, "journal_create_failed");
                return false;
            }
            tracing::info!(journal = journal.0, ?boot, "journal_created");
            self.created.insert(journal, (config, boot));
        }
        true
    }

    fn quarantined(&mut self, journal: JournalId) {
        tracing::warn!(journal = journal.0, "journal_quarantined");
    }

    fn delete(&mut self, journal: JournalId) {
        // The store is kept on disk: a tombstoned journal is never opened
        // again, and reclaiming its space is an operator's act.
        self.created.remove(&journal);
        tracing::info!(journal = journal.0, "journal_deleted");
    }
}
