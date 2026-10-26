//! Make a store's files durable as they stand (#348): the probe an
//! operator runs before it trusts a format marker it can read, and the
//! first step of every store open (#720 (fsyncgate)).
//!
//! A sync that fails leaves its writes in the file image: the process that
//! wrote them reads them back, but a power loss can still drop them. So a
//! marker a living process reads after a failed format sync is staged, not
//! landed. A failed writeback is worse (fsyncgate): the kernel marks the
//! pages it could not write clean, so a retried sync succeeds and flushes
//! nothing, and the page cache keeps serving the bytes, across a process
//! restart too. [`settle`] syncs every file of the store, then drops its
//! clean cached pages, and syncs every directory on the way to it; once it
//! returns `Ok`, what a reader sees is what the device holds and a crash
//! keeps. It writes no byte of its own: a rewrite would make the stranded
//! pages durable, but a crash during it can tear records that were.

use std::io;

use moonpool_core::{LocalStorageFile, LocalStorageProvider, OpenOptions};

/// Sync every file in `dir` and drop its cached pages, then sync every
/// directory from the root down to `dir`. A `dir` that does not exist
/// holds nothing to make durable.
///
/// # Errors
///
/// The first I/O error a listing, an open or a sync returned; nothing is
/// durable then, and a retry syncs everything again.
#[tracing::instrument(level = "debug", skip_all, fields(dir = %dir))]
pub async fn settle<S: LocalStorageProvider>(provider: &S, dir: &str) -> io::Result<()> {
    let names = match provider.list_dir(dir).await {
        Ok(names) => names,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    for name in &names {
        let file = provider
            .open(&format!("{dir}/{name}"), OpenOptions::read_write())
            .await?;
        file.sync_all().await?;
        // Dirty pages are durable now; what is left cached and clean may
        // be a failed writeback's, which only the device can disprove.
        file.drop_cache().await?;
    }
    sync_names(provider, dir).await
}

/// Make every name on the way to `dir`, and the names inside it, durable:
/// a name `create_dir_all` or a rename made is lost in a crash unless its
/// directory is synced, and a synced child does not survive the loss of its
/// parent's own name (`moonpool-journal`'s rule).
pub(crate) async fn sync_names<S: LocalStorageProvider>(provider: &S, dir: &str) -> io::Result<()> {
    let mut current = if dir.starts_with('/') { "/" } else { "." }.to_string();
    for component in dir.split('/').filter(|c| !c.is_empty() && *c != ".") {
        provider.sync_dir(&current).await?;
        current = match current.as_str() {
            "." => component.to_string(),
            "/" => format!("/{component}"),
            _ => format!("{current}/{component}"),
        };
    }
    provider.sync_dir(dir).await
}
