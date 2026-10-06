//! The production [`JournalStores`]: one [`JournalStorage`] per journal,
//! each in its own directory under the node's data directory, on Tokio's
//! filesystem (#206).
//!
//! ```text
//! <data-dir>/
//!   machine                       the machine's identity and cell (#196)
//!   provisioned                   the journal stores it formatted (#208)
//!   journals/<tenant>/<journal>/  one moonpool-journal per journal a node
//!                                 serves, by its identifier (#235)
//! ```
//!
//! Every ordinary start is an existing member's (#208): the stores were
//! formatted when the machine formed its cell, never by a start. A journal the
//! directory creates (#189) is the one store a running node formats: it is
//! a first boot until its store has booted once, then the provisioning
//! record ([`Record`]) names it and every later open is an existing
//! member's. A node killed between that format and the record is resolved
//! at its next start by reading the disk ([`DirStores::load`]), the
//! simulation's rule.
//!
//! This record logic is `parosd`'s alone and runs outside the simulation
//! (whose opener keeps its own provisioning ledger and the default no-op
//! [`JournalStores::opened`]); its unit tests below are its evidence. Its
//! write is a small synchronous `fsync` on the node loop, once per created
//! journal.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use moonpool_core::TokioStorageProvider;
use paros::{
    BootKind, Config, JournalIdentifier, JournalStorage, JournalStoreConfig, JournalStores, NoAudit,
};

use crate::record::{Record, parse_identifier};

/// The directory of `journal`'s store under `data_dir`.
#[must_use]
pub fn journal_dir(data_dir: &Path, journal: JournalIdentifier) -> PathBuf {
    data_dir
        .join("journals")
        .join(journal.tenant.0.to_string())
        .join(journal.journal.0.to_string())
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
    genesis: BTreeMap<JournalIdentifier, Config>,
    /// The provisioning record: the journals whose stores are formatted.
    record: Record,
    /// Journals the directory created naming this node (#189), with their
    /// configuration.
    created: BTreeMap<JournalIdentifier, Config>,
}

impl DirStores {
    /// The stores of node `id` serving `genesis` under `data_dir`, every
    /// one an existing member. A created journal's store formatted before
    /// the record could name it (a kill in between) is found on the disk
    /// and recorded now.
    ///
    /// # Errors
    ///
    /// The record or the journals directory cannot be read or written.
    pub async fn load(
        id: u64,
        data_dir: PathBuf,
        layout: JournalStoreConfig,
        genesis: BTreeMap<JournalIdentifier, Config>,
    ) -> Result<Self, String> {
        let read = Record::read(&data_dir).map_err(|e| format!("provisioning record: {e}"))?;
        if read.is_none() {
            tracing::warn!(data_dir = %data_dir.display(), "parosd_unprovisioned");
        }
        let mut stores = Self {
            provider: TokioStorageProvider::new(),
            record: read.unwrap_or_else(|| Record {
                role: crate::machine_record::ROLE.into(),
                id,
                journals: BTreeSet::new(),
            }),
            data_dir,
            layout,
            genesis,
            created: BTreeMap::new(),
        };
        stores.record.check(crate::machine_record::ROLE, id)?;
        stores.resolve_created().await?;
        Ok(stores)
    }

    /// Record every created journal whose store carries its format marker
    /// but which the record does not name yet.
    async fn resolve_created(&mut self) -> Result<(), String> {
        let journals = self.data_dir.join("journals");
        let entries = match std::fs::read_dir(&journals) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(format!("{}: {error}", journals.display())),
        };
        let mut found = Vec::new();
        for tenant in entries.flatten() {
            let Some(tenant_name) = tenant.file_name().to_str().map(str::to_string) else {
                continue;
            };
            let Ok(inner) = std::fs::read_dir(tenant.path()) else {
                continue;
            };
            for entry in inner.flatten() {
                let Some(journal) = entry
                    .file_name()
                    .to_str()
                    .and_then(|name| parse_identifier(&format!("{tenant_name}/{name}")))
                else {
                    continue;
                };
                if !self.genesis.contains_key(&journal) && !self.record.journals.contains(&journal)
                {
                    found.push(journal);
                }
            }
        }
        found.sort_unstable();
        let mut resolved = false;
        for journal in found {
            let dir = path_str(&journal_dir(&self.data_dir, journal));
            let formatted = JournalStorage::peek_formatted(&self.provider, &dir).await;
            if formatted.unwrap_or(false) {
                tracing::info!(journal = %journal, "created_journal_resolved");
                self.record.journals.insert(journal);
                resolved = true;
            }
        }
        if resolved {
            self.record
                .write(&self.data_dir)
                .map_err(|e| format!("provisioning record: {e}"))?;
        }
        Ok(())
    }

    fn store(
        &self,
        journal: JournalIdentifier,
        config: Config,
    ) -> JournalStorage<TokioStorageProvider> {
        JournalStorage::new(
            self.provider.clone(),
            path_str(&journal_dir(&self.data_dir, journal)),
            config,
            self.layout,
        )
    }

    /// The claim a created journal's store opens with.
    fn created_claim(&self, journal: JournalIdentifier) -> BootKind {
        if self.record.journals.contains(&journal) {
            BootKind::ExistingMember
        } else {
            BootKind::FirstBoot
        }
    }
}

impl JournalStores for DirStores {
    type Store = JournalStorage<TokioStorageProvider>;
    type Audit = NoAudit;

    fn journals(&self) -> Vec<JournalIdentifier> {
        self.genesis.keys().copied().collect()
    }

    fn open(&mut self, journal: JournalIdentifier) -> Option<(Self::Store, BootKind)> {
        if let Some(config) = self.genesis.get(&journal) {
            return Some((
                self.store(journal, config.clone()),
                BootKind::ExistingMember,
            ));
        }
        let config = self.created.get(&journal)?.clone();
        let claim = self.created_claim(journal);
        Some((self.store(journal, config), claim))
    }

    fn opened(&mut self, journal: JournalIdentifier) {
        if !self.created.contains_key(&journal) || !self.record.journals.insert(journal) {
            return;
        }
        // The store is formatted durably: from here on it is an existing
        // member's. A failed write is resolved from the disk at the next
        // start.
        match self.record.write(&self.data_dir) {
            Ok(()) => tracing::info!(journal = %journal, "journal_provisioned"),
            Err(error) => {
                tracing::error!(journal = %journal, %error, "provisioning_record_failed");
            }
        }
    }

    fn audit(&self, _journal: JournalIdentifier) -> NoAudit {
        NoAudit
    }

    fn node_audit(&self) -> NoAudit {
        NoAudit
    }

    fn create(&mut self, journal: JournalIdentifier, config: Config) -> bool {
        if self.genesis.contains_key(&journal) {
            return true;
        }
        if !self.created.contains_key(&journal) {
            let dir = journal_dir(&self.data_dir, journal);
            if let Err(error) = std::fs::create_dir_all(&dir) {
                tracing::error!(journal = %journal, %error, "journal_create_failed");
                return false;
            }
            let boot = self.created_claim(journal);
            tracing::info!(journal = %journal, ?boot, "journal_created");
            self.created.insert(journal, config);
        }
        true
    }

    fn quarantined(&mut self, journal: JournalIdentifier) {
        tracing::warn!(journal = %journal, "journal_quarantined");
    }

    fn delete(&mut self, journal: JournalIdentifier) {
        // The store is kept on disk: a tombstoned journal is never opened
        // again, and reclaiming its space is an operator's act.
        self.created.remove(&journal);
        tracing::info!(journal = %journal, "journal_deleted");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use paros::{JournalId, TenantId};

    fn config(journal: JournalIdentifier) -> Config {
        Config::new(paros::NodeId(1), journal)
    }

    async fn load(dir: &Path) -> DirStores {
        DirStores::load(
            0,
            dir.to_path_buf(),
            JournalStoreConfig::small(),
            BTreeMap::new(),
        )
        .await
        .expect("load")
    }

    #[tokio::test]
    async fn a_created_journal_is_a_first_boot_until_its_store_has_booted() {
        let dir = tempfile::tempdir().expect("tempdir");
        let journal = JournalIdentifier::new(TenantId(0x7e), JournalId(300));
        let mut stores = load(dir.path()).await;
        assert!(stores.create(journal, config(journal)));
        let (_, boot) = stores.open(journal).expect("open");
        assert_eq!(boot, BootKind::FirstBoot);
        // A re-open before the store booted (a quarantined format) is still
        // a first boot; once it booted, an existing member's, across a
        // restart too.
        let (_, boot) = stores.open(journal).expect("open");
        assert_eq!(boot, BootKind::FirstBoot);
        stores.opened(journal);
        let (_, boot) = stores.open(journal).expect("open");
        assert_eq!(boot, BootKind::ExistingMember);
        let mut restarted = load(dir.path()).await;
        assert!(restarted.create(journal, config(journal)));
        let (_, boot) = restarted.open(journal).expect("open");
        assert_eq!(boot, BootKind::ExistingMember);
    }

    #[tokio::test]
    async fn a_format_the_record_missed_is_found_on_the_disk() {
        let dir = tempfile::tempdir().expect("tempdir");
        let journal = JournalIdentifier::new(TenantId(0x7e), JournalId(300));
        let mut stores = load(dir.path()).await;
        assert!(stores.create(journal, config(journal)));
        let (mut store, _) = stores.open(journal).expect("open");
        // The store is formatted, and the node dies before the record.
        assert_eq!(
            paros::provision_store(&mut store).await.expect("format"),
            paros::Provisioned::Formatted
        );
        drop((store, stores));
        let mut restarted = load(dir.path()).await;
        assert!(restarted.create(journal, config(journal)));
        let (_, boot) = restarted.open(journal).expect("open");
        assert_eq!(boot, BootKind::ExistingMember);
        // A created journal whose store never got its marker is a first
        // boot again.
        let other = JournalIdentifier::new(TenantId(0x7e), JournalId(301));
        std::fs::create_dir_all(journal_dir(dir.path(), other)).expect("mkdir");
        let mut restarted = load(dir.path()).await;
        assert!(restarted.create(other, config(other)));
        let (_, boot) = restarted.open(other).expect("open");
        assert_eq!(boot, BootKind::FirstBoot);
    }
}
