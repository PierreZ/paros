//! The scratch copies an inspection opens (#365): a store's files, copied
//! byte for byte from the source disk, so the recovery an open runs writes
//! the copy, never the original.

use std::io;

use moonpool_core::{OpenOptions, StorageProvider};

use crate::machine::{read_all, write_all};

/// A scratch directory on `provider`: the copies of the stores one
/// inspection opens.
pub(super) struct Scratch<S> {
    provider: S,
    root: String,
}

impl<S: StorageProvider> Scratch<S> {
    /// The scratch directory `root` on `provider`, made if missing.
    pub(super) async fn new(provider: S, root: &str) -> Result<Self, String> {
        assert!(!root.is_empty(), "a scratch directory is named");
        provider
            .create_dir_all(root)
            .await
            .map_err(|e| format!("{root}: {e}"))?;
        Ok(Self {
            provider,
            root: root.to_string(),
        })
    }

    /// The provider the copies are on.
    pub(super) fn provider(&self) -> &S {
        &self.provider
    }

    /// Copy every file of the directory `dir` on `source` into the
    /// directory `name` of the scratch, and give its path. `source` is only
    /// listed and read.
    pub(super) async fn copy<P: StorageProvider>(
        &self,
        source: &P,
        dir: &str,
        name: &str,
    ) -> Result<String, String> {
        assert!(
            !name.contains('/'),
            "a copy is one directory of the scratch"
        );
        let copy = format!("{}/{name}", self.root);
        let names = source
            .list_dir(dir)
            .await
            .map_err(|e| format!("{dir}: {e}"))?;
        self.provider
            .create_dir_all(&copy)
            .await
            .map_err(|e| format!("{copy}: {e}"))?;
        for name in &names {
            let from = format!("{dir}/{name}");
            let to = format!("{copy}/{name}");
            let bytes = read_file(source, &from)
                .await
                .map_err(|e| format!("{from}: {e}"))?;
            self.write_file(&to, &bytes)
                .await
                .map_err(|e| format!("{to}: {e}"))?;
            // Pair of the copy: what the open reads is what the source holds.
            assert_eq!(
                read_file(&self.provider, &to).await.map(|b| b.len()).ok(),
                Some(bytes.len()),
                "a scratch copy holds every byte of its source"
            );
        }
        Ok(copy)
    }

    /// Delete every file of the copy `copy`, best effort: a scratch
    /// directory is the caller's to remove.
    pub(super) async fn remove(&self, copy: &str) {
        assert!(
            copy.starts_with(&self.root),
            "only a scratch copy is removed"
        );
        let Ok(names) = self.provider.list_dir(copy).await else {
            return;
        };
        for name in names {
            let _ = self.provider.delete(&format!("{copy}/{name}")).await;
        }
    }

    async fn write_file(&self, path: &str, bytes: &[u8]) -> io::Result<()> {
        let file = self
            .provider
            .open(path, OpenOptions::create_write())
            .await?;
        write_all(&file, bytes).await
    }
}

/// Every byte of the file `path` on `provider`, opened read-only.
async fn read_file<P: StorageProvider>(provider: &P, path: &str) -> io::Result<Vec<u8>> {
    let file = provider.open(path, OpenOptions::read_only()).await?;
    read_all(&file).await
}
