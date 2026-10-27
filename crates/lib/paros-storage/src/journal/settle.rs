//! Make a store's files durable as they stand (#348), and make a store
//! open read what the device holds (#720 (fsyncgate)).
//!
//! A sync that fails leaves its writes in the file image: the process that
//! wrote them reads them back, but a power loss can still drop them. So a
//! marker a living process reads after a failed format sync is staged, not
//! landed. [`settle`] syncs every file of the store and every directory on
//! the way to it, the probe an operator runs before it trusts a format
//! marker it can read.
//!
//! A failed writeback is worse (fsyncgate): the kernel marks the pages it
//! could not write clean, so a retried sync succeeds and flushes nothing,
//! and the page cache keeps serving the bytes, across a process restart
//! too. [`drop_caches`] drops every file's clean cached pages, so the next
//! read of such a page comes from the device. Every store open calls it
//! first, and [`settle`] calls it after its syncs. Neither writes a byte: a
//! rewrite would make the stranded pages durable, but a crash during it
//! can tear records that were. The open does not sync either: a dirty page
//! a failed commit left still reads as written and lands at the next sync,
//! as before fsyncgate (#348), and a sync at every open could fail and
//! take the open with it.

use std::io;

use moonpool_core::{LocalStorageFile, LocalStorageProvider, OpenOptions};

/// Sync every file in `dir`, then every directory from the root down to
/// `dir`, then drop the files' cached pages ([`drop_caches`]). A `dir` that
/// does not exist holds nothing to make durable.
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
    }
    sync_names(provider, dir).await?;
    drop_caches(provider, dir).await
}

/// Drop the clean cached pages of every file in `dir`: what a failed
/// writeback left there reads as written and is not durable, and only the
/// device can tell. Dirty pages stay. A `dir` that does not exist holds
/// nothing.
///
/// # Errors
///
/// The first I/O error a listing, an open or a drop returned; a retry
/// drops everything again.
#[tracing::instrument(level = "debug", skip_all, fields(dir = %dir))]
pub async fn drop_caches<S: LocalStorageProvider>(provider: &S, dir: &str) -> io::Result<()> {
    let names = match provider.list_dir(dir).await {
        Ok(names) => names,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    for name in &names {
        let file = provider
            .open(&format!("{dir}/{name}"), OpenOptions::read_only())
            .await?;
        file.drop_cache().await?;
    }
    Ok(())
}

/// Make every name on the way to `dir`, and the names inside it, durable:
/// a name `create_dir_all` or a rename made is lost in a crash unless its
/// directory is synced, and a synced child does not survive the loss of its
/// parent's own name (`moonpool-journal`'s rule).
///
/// # Errors
///
/// When the call or the decoding fails: the error says why.
pub async fn sync_names<S: LocalStorageProvider>(provider: &S, dir: &str) -> io::Result<()> {
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
