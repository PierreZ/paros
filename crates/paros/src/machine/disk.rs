//! A machine's disk over a storage provider (#246): the machine record's
//! file and the formation's journal stores, written once for `parosd`
//! (`TokioStorageProvider`) and the simulation (`SimStorageProvider`), so the
//! simulation runs the record's write protocol `parosd` ships.
//!
//! ```text
//! <root>/
//!   machine                       the machine record ([`super::MachineRecord`])
//!   registry                      the cached registry fold ([`super::CachedRegistry`], #211)
//!   journals/<tenant>/<journal>/  one journal store per journal of the plan
//! ```
//!
//! The record is rewritten whole and atomically: a temporary file, synced,
//! renamed into place, and every directory on its way synced, so a crash
//! leaves the old record or the new one, and a name `create_dir_all` made is
//! never lost under a record that names the cell. `parosd` and the
//! simulation hand [`super::run_machine`] this type and nothing around it.

use std::io;

use moonpool_core::{OpenOptions, StorageFile, StorageProvider};
use paros_core::{JournalIdentifier, NodeId};

use super::{CellPlan, journal_config};
use crate::journal::sync_names;
use crate::{JournalStorage, JournalStoreConfig};

/// The machine record's file name under the root.
const RECORD: &str = "machine";

/// The staged record a rewrite renames into place.
const STAGED: &str = "machine.tmp";

/// The cached registry fold's file name under the root (#211).
const CACHE: &str = "registry";

/// The staged cache a rewrite renames into place.
const CACHE_STAGED: &str = "registry.tmp";

/// A machine's disk: the record and the journal stores under `root`, on
/// `provider`.
#[derive(Clone, Debug)]
pub struct ProviderDisk<S> {
    provider: S,
    root: String,
    layout: JournalStoreConfig,
}

impl<S: StorageProvider + Clone> ProviderDisk<S> {
    /// The disk under `root` on `provider`, its journal stores laid out by
    /// `layout`.
    ///
    /// # Panics
    ///
    /// When `root` is empty: a machine's disk is a directory.
    #[must_use]
    pub fn new(provider: S, root: impl Into<String>, layout: JournalStoreConfig) -> Self {
        let root = root.into();
        assert!(!root.is_empty(), "a machine's disk has a root directory");
        Self {
            provider,
            root,
            layout,
        }
    }

    /// The provider the disk is on.
    #[must_use]
    pub fn provider(&self) -> &S {
        &self.provider
    }

    /// The journal store layout.
    #[must_use]
    pub fn layout(&self) -> JournalStoreConfig {
        self.layout
    }

    /// The directory of `journal`'s store (#235: by its identifier).
    ///
    /// # Panics
    ///
    /// When `journal` is unset: no store is named by an unset identifier.
    #[must_use]
    pub fn journal_dir(&self, journal: JournalIdentifier) -> String {
        assert!(
            journal.is_set(),
            "a journal store is named by a set identifier"
        );
        format!(
            "{}/journals/{}/{}",
            self.root, journal.tenant.0, journal.journal.0
        )
    }

    fn path(&self, name: &str) -> String {
        format!("{}/{name}", self.root)
    }

    /// The machine record's text, or `None` when there is none.
    ///
    /// # Errors
    ///
    /// The record exists and the disk failed to read it: an I/O failure,
    /// which a restart may not meet again. Bytes that are not text are no
    /// error here: they come back as text the record's parser refuses, a
    /// damage no restart repairs.
    #[tracing::instrument(level = "debug", skip_all, fields(root = %self.root))]
    pub async fn read_record(&self) -> Result<Option<String>, String> {
        self.read(RECORD).await
    }

    /// The cached registry fold's text (#211), or `None` when there is
    /// none.
    ///
    /// # Errors
    ///
    /// The cache exists and the disk failed to read it.
    #[tracing::instrument(level = "debug", skip_all, fields(root = %self.root))]
    pub async fn read_cache(&self) -> Result<Option<String>, String> {
        self.read(CACHE).await
    }

    async fn read(&self, name: &str) -> Result<Option<String>, String> {
        let file = match self
            .provider
            .open(&self.path(name), OpenOptions::read_only())
            .await
        {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.to_string()),
        };
        let bytes = read_all(&file).await.map_err(|e| e.to_string())?;
        Ok(Some(String::from_utf8_lossy(&bytes).into_owned()))
    }

    /// Replace the machine record with `text`, whole and durably.
    ///
    /// # Errors
    ///
    /// Any storage failure; the old record (or none) is then what a crash
    /// leaves.
    ///
    /// # Panics
    ///
    /// When `text` is empty: a record always names its machine.
    #[tracing::instrument(level = "debug", skip_all, fields(root = %self.root, len = text.len()))]
    pub async fn write_record(&self, text: &str) -> Result<(), String> {
        assert!(!text.is_empty(), "a machine record is never empty");
        self.write(text, STAGED, RECORD)
            .await
            .map_err(|e| e.to_string())
    }

    /// Replace the cached registry fold with `text` (#211), whole and
    /// durably, as the record is.
    ///
    /// # Errors
    ///
    /// Any storage failure; the old cache (or none) is then what a crash
    /// leaves.
    ///
    /// # Panics
    ///
    /// When `text` is empty: a cache always names its position.
    #[tracing::instrument(level = "debug", skip_all, fields(root = %self.root, len = text.len()))]
    pub async fn write_cache(&self, text: &str) -> Result<(), String> {
        assert!(!text.is_empty(), "a cached registry fold is never empty");
        self.write(text, CACHE_STAGED, CACHE)
            .await
            .map_err(|e| e.to_string())
    }

    async fn write(&self, text: &str, staged: &str, name: &str) -> io::Result<()> {
        assert_ne!(staged, name, "a rewrite stages beside its file");
        self.provider.create_dir_all(&self.root).await?;
        let staged = self.path(staged);
        {
            let file = self
                .provider
                .open(&staged, OpenOptions::create_write())
                .await?;
            write_all(&file, text.as_bytes()).await?;
            file.sync_all().await?;
            assert_eq!(
                file.size().await?,
                text.len() as u64,
                "a synced record holds exactly its text"
            );
        }
        self.provider.rename(&staged, &self.path(name)).await?;
        sync_names(&self.provider, &self.root).await
    }

    /// Whether the disk holds anything of a journal store: with no record,
    /// a machine that lost its identity.
    ///
    /// Fails closed: a disk that cannot say counts as holding stores. The
    /// two mistakes are not alike — a refused start is an operator's to
    /// resolve, while a new identity minted over an old machine's stores
    /// rejoins as a machine it is not, which no one can undo.
    #[tracing::instrument(level = "debug", skip_all, fields(root = %self.root))]
    pub async fn holds_journals(&self) -> bool {
        self.provider
            .exists(&self.path("journals"))
            .await
            .unwrap_or(true)
    }

    /// Format the store of every journal `plan` names for member `node_id`
    /// ([`crate::provision_store`]). An interrupted run resumes: a store
    /// already formatted under the same configuration is left as it is, and
    /// made durable as it stands ([`crate::journal::settle`], #348).
    ///
    /// # Errors
    ///
    /// A store could not be formatted durably.
    ///
    /// # Panics
    ///
    /// When `node_id` is not a member of `plan`.
    #[tracing::instrument(level = "debug", skip_all, fields(node = node_id.0, cell = plan.cell_id))]
    pub async fn format(&self, node_id: NodeId, plan: &CellPlan) -> Result<(), String> {
        assert!(
            plan.members.iter().any(|(id, _)| *id == node_id),
            "a machine formats only a plan it is a member of"
        );
        assert!(!plan.journals.is_empty(), "a plan names its journals");
        for &journal in &plan.journals {
            let mut store = JournalStorage::new(
                self.provider.clone(),
                self.journal_dir(journal),
                journal_config(plan, node_id, journal),
                self.layout,
            );
            let provisioned = crate::provision_store(&mut store)
                .await
                .map_err(|e| format!("journal {journal}: {e}"))?;
            if provisioned == crate::Provisioned::Resumed {
                // A marker this process can read may be one a failed sync
                // left staged (#348): the vote that follows names it.
                crate::journal::settle(&self.provider, &self.journal_dir(journal))
                    .await
                    .map_err(|e| format!("journal {journal}: {e}"))?;
            }
        }
        Ok(())
    }
}

/// Every byte of `file`, looping over short reads.
async fn read_all<F: StorageFile>(file: &F) -> io::Result<Vec<u8>> {
    let size = usize::try_from(file.size().await?)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "a record too large"))?;
    let mut bytes = vec![0_u8; size];
    let mut done = 0;
    while done < size {
        let read = file.read_at(done as u64, &mut bytes[done..]).await?;
        if read == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        done += read;
    }
    assert_eq!(done, size, "a whole read covers the file");
    Ok(bytes)
}

/// Write every byte of `bytes` at the start of `file`, looping over short
/// writes.
async fn write_all<F: StorageFile>(file: &F, bytes: &[u8]) -> io::Result<()> {
    let mut done = 0;
    while done < bytes.len() {
        let wrote = file.write_at(done as u64, &bytes[done..]).await?;
        if wrote == 0 {
            return Err(io::ErrorKind::WriteZero.into());
        }
        done += wrote;
    }
    assert_eq!(done, bytes.len(), "a whole write covers the text");
    Ok(())
}
