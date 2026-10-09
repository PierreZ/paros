//! The machine's disk (#196, #246): its data directory, as the library's
//! [`MachineDisk`]. The lifecycle itself — format, the amnesia check, the
//! wait for a cell, serving it — is `paros::machine::run_machine`'s, the
//! code the simulation runs; this is only where it lands on a filesystem.
//!
//! ```text
//! <data-dir>/
//!   machine       the machine record (`paros::machine::MachineRecord`)
//!   provisioned   the journal stores it formatted ([`Record`], #208)
//!   journals/     the stores ([`DirStores`])
//! ```

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use moonpool_core::TokioStorageProvider;
use paros::machine::{CellPlan, MachineDisk, ProviderDisk};
use paros::{Config, JournalIdentifier, JournalStoreConfig, NodeId};

use crate::record::Record;
use crate::stores::{DirStores, path_str};

/// The role the provisioning record names for a machine's stores.
pub const ROLE: &str = "machine";

/// A machine's data directory.
pub struct DirDisk {
    /// The data directory.
    pub data_dir: PathBuf,
    /// The store layout.
    pub layout: JournalStoreConfig,
}

impl DirDisk {
    /// The record and the formation's stores, on Tokio's filesystem: the
    /// library's code, the one the simulation runs.
    fn disk(&self) -> ProviderDisk<TokioStorageProvider> {
        ProviderDisk::new(
            TokioStorageProvider::new(),
            path_str(&self.data_dir),
            self.layout,
        )
    }
}

impl MachineDisk for DirDisk {
    type Stores = DirStores;

    async fn read_record(&mut self) -> Result<Option<String>, String> {
        self.disk().read_record().await
    }

    async fn write_record(&mut self, text: &str) -> Result<(), String> {
        self.disk().write_record(text).await
    }

    async fn holds_stores(&mut self) -> bool {
        self.disk().holds_journals().await || Record::read(&self.data_dir).ok().flatten().is_some()
    }

    async fn provision(&mut self, node_id: NodeId, plan: &CellPlan) -> Result<(), String> {
        self.disk().format(node_id, plan).await?;
        Record {
            role: ROLE.into(),
            id: node_id.0,
            journals: plan.journals.iter().copied().collect::<BTreeSet<_>>(),
        }
        .write(&self.data_dir)
        .map_err(|e| format!("provisioning record: {e}"))
    }

    async fn stores(
        &mut self,
        node_id: NodeId,
        genesis: BTreeMap<JournalIdentifier, Config>,
    ) -> Result<DirStores, String> {
        DirStores::load(node_id.0, self.data_dir.clone(), self.layout, genesis).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn disk(dir: &std::path::Path) -> DirDisk {
        DirDisk {
            data_dir: dir.to_path_buf(),
            layout: JournalStoreConfig::small(),
        }
    }

    #[tokio::test]
    async fn the_record_is_absent_then_read_back_whole() {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let mut disk = disk(dir.path());
        assert_eq!(disk.read_record().await, Ok(None));
        assert!(!disk.holds_stores().await);
        disk.write_record("node_id 3\n").await.expect("written");
        assert_eq!(disk.read_record().await, Ok(Some("node_id 3\n".into())));
        assert!(!dir.path().join("machine.tmp").exists());
        std::fs::create_dir_all(dir.path().join("journals")).expect("mkdir");
        assert!(
            disk.holds_stores().await,
            "a store without a record is amnesia"
        );
    }
}
