//! A journal store's files as the fault injector sees them (#176): where each
//! entry and its far identifier live on the simulated disk, and the damage a
//! latent fault does to them.
//!
//! The layout is `moonpool-journal`'s documented on-disk format (its crate
//! docs, format version 3): a directory of segments named
//! `seg-<first index, 20 digits>.wal`, each with two header blocks, a slot
//! table of 64-byte identifiers from byte 8 KiB (slot *i* at
//! `8 KiB + 64 × (i − first)`), and a data region of entries, each a 64-byte
//! header (magic, length, index, epoch, CRC, flags, tag) then its payload;
//! the metadata lives in two copies, `meta.0` and `meta.1`. The journal does
//! not expose where an entry lives (PierreZ/moonpool#289), so the harness
//! reads the format itself
//! — only to aim a fault, never to decide anything the store decides: what
//! the damage *means* is still the journal's boot scan's, and the store's
//! own report is what the ledger resolves against.
//!
//! Every function here touches the disk through the node's own
//! [`SimStorageProvider`], so the damage is ordinary simulated I/O on the
//! seed's schedule, synced before the boot that reads it back.

use moonpool_sim::{OpenOptions, SimStorageProvider, StorageFile, StorageProvider};

/// A segment's slot table starts after its two header blocks.
const SLOT_TABLE_OFFSET: u64 = 2 * 4096;
/// One far identifier.
const SLOT_SIZE: u64 = 64;
/// An entry's fixed header, before its payload.
const ENTRY_HEADER_SIZE: u64 = 64;
/// The entry header's magic (`MPJE`, little-endian).
const ENTRY_MAGIC: u32 = u32::from_le_bytes(*b"MPJE");

/// One entry of a journal, located on disk.
#[derive(Clone, Debug)]
pub(crate) struct EntryLoc {
    /// Its log index.
    pub(crate) index: u64,
    /// Its epoch: the store's record kind (`paros::journal`'s frame).
    pub(crate) epoch: u64,
    /// The three little-endian words of its tag (a slot record's
    /// `(slot, round, node)`).
    pub(crate) tag: [u64; 3],
    /// The segment file it lives in.
    pub(crate) path: String,
    /// Byte offset of its slot (its far identifier) in that file.
    pub(crate) slot_at: u64,
    /// Byte offset of the entry itself.
    pub(crate) entry_at: u64,
    /// Its payload length.
    pub(crate) length: u64,
}

fn u32_at(bytes: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(bytes[at..at + 4].try_into().expect("4-byte field"))
}

fn u64_at(bytes: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(bytes[at..at + 8].try_into().expect("8-byte field"))
}

/// The first index a segment file name encodes.
fn segment_first(name: &str) -> Option<u64> {
    let digits = name.strip_prefix("seg-")?.strip_suffix(".wal")?;
    (digits.len() == 20 && digits.bytes().all(|b| b.is_ascii_digit()))
        .then(|| digits.parse().ok())
        .flatten()
}

/// Read `len` bytes at `offset` (short reads looped; past the end reads
/// as zeros).
async fn read_exact(
    file: &<SimStorageProvider as StorageProvider>::File,
    offset: u64,
    len: usize,
) -> std::io::Result<Vec<u8>> {
    let mut buf = vec![0_u8; len];
    let mut done = 0;
    while done < len {
        let n = file.read_at(offset + done as u64, &mut buf[done..]).await?;
        if n == 0 {
            break;
        }
        done += n;
    }
    Ok(buf)
}

/// Write all of `bytes` at `offset`.
async fn write_all(
    file: &<SimStorageProvider as StorageProvider>::File,
    offset: u64,
    bytes: &[u8],
) -> std::io::Result<()> {
    let mut done = 0;
    while done < bytes.len() {
        let n = file.write_at(offset + done as u64, &bytes[done..]).await?;
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "a positioned write made no progress",
            ));
        }
        done += n;
    }
    Ok(())
}

/// Every intact entry of the journal under `dir`, in index order: each
/// segment's slots from its first index up to the first empty one, kept
/// where the entry its slot points at carries the same index (anything
/// else is not a target a fault can be aimed at). `slot_count` is the
/// geometry's.
pub(crate) async fn scan(
    provider: &SimStorageProvider,
    dir: &str,
    slot_count: u64,
) -> std::io::Result<Vec<EntryLoc>> {
    let mut firsts: Vec<(u64, String)> = provider
        .list_dir(dir)
        .await?
        .into_iter()
        .filter_map(|name| segment_first(&name).map(|first| (first, name)))
        .collect();
    firsts.sort_unstable();
    let mut found = Vec::new();
    for (first, name) in firsts {
        let path = format!("{dir}/{name}");
        let file = provider.open(&path, OpenOptions::read_only()).await?;
        let table = read_exact(
            &file,
            SLOT_TABLE_OFFSET,
            usize::try_from(slot_count * SLOT_SIZE).unwrap_or(0),
        )
        .await?;
        for (rel, slot) in table
            .chunks_exact(usize::try_from(SLOT_SIZE).unwrap_or(64))
            .enumerate()
        {
            if slot.iter().all(|byte| *byte == 0) {
                break;
            }
            let index = first + rel as u64;
            if u64_at(slot, 0) != index {
                continue;
            }
            let entry_at = u64::from(u32_at(slot, 16));
            let header = read_exact(
                &file,
                entry_at,
                usize::try_from(ENTRY_HEADER_SIZE).unwrap_or(64),
            )
            .await?;
            if u32_at(&header, 0) != ENTRY_MAGIC || u64_at(&header, 8) != index {
                continue;
            }
            found.push(EntryLoc {
                index,
                epoch: u64_at(slot, 8),
                tag: [u64_at(slot, 32), u64_at(slot, 40), u64_at(slot, 48)],
                path: path.clone(),
                slot_at: SLOT_TABLE_OFFSET + rel as u64 * SLOT_SIZE,
                entry_at,
                length: u64::from(u32_at(slot, 20)),
            });
        }
    }
    Ok(found)
}

/// One latent fault, as it lands on the bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Damage {
    /// Flip the entry's first payload byte: its CRC fails, its identifier
    /// is intact (a bit flip, a latent sector error).
    RotEntry,
    /// Zero the entry, header and payload: the bytes never reached the
    /// medium while its identifier did (a lost write).
    LoseEntry,
    /// Overwrite the entry's header with `from`'s: a checksummed record of
    /// another index sits where this one should (a misdirected write).
    Misdirect,
    /// Garble the entry's identifier as well (with a damaged entry, the
    /// double fault the journal refuses to start on).
    RotSlot,
    /// Zero the entry's identifier and garble the entry: the shape a crash
    /// before the batch's sync leaves, the end of the log.
    Tear,
}

/// Apply `damage` to `entry` (with `from` the source of a misdirected
/// write), and sync the file.
pub(crate) async fn damage(
    provider: &SimStorageProvider,
    entry: &EntryLoc,
    damage: Damage,
    from: Option<&EntryLoc>,
) -> std::io::Result<()> {
    let file = provider
        .open(&entry.path, OpenOptions::read_write())
        .await?;
    let span = usize::try_from(ENTRY_HEADER_SIZE + entry.length).unwrap_or(0);
    match damage {
        Damage::RotEntry => {
            let at = entry.entry_at + ENTRY_HEADER_SIZE;
            let byte = read_exact(&file, at, 1).await?;
            write_all(&file, at, &[byte[0] ^ 0xff]).await?;
        }
        Damage::LoseEntry => {
            write_all(&file, entry.entry_at, &vec![0_u8; span]).await?;
        }
        Damage::Misdirect => {
            let source = from.unwrap_or(entry);
            let source_file = provider
                .open(&source.path, OpenOptions::read_only())
                .await?;
            let header = read_exact(
                &source_file,
                source.entry_at,
                usize::try_from(ENTRY_HEADER_SIZE).unwrap_or(64),
            )
            .await?;
            write_all(&file, entry.entry_at, &header).await?;
        }
        Damage::RotSlot => {
            let garbage: Vec<u8> = (0..SLOT_SIZE)
                .map(|i| 0xa5 ^ u8::try_from(i).unwrap_or(0))
                .collect();
            write_all(&file, entry.slot_at, &garbage).await?;
            let at = entry.entry_at + ENTRY_HEADER_SIZE;
            let byte = read_exact(&file, at, 1).await?;
            write_all(&file, at, &[byte[0] ^ 0xff]).await?;
        }
        Damage::Tear => {
            write_all(&file, entry.slot_at, &[0_u8; 64]).await?;
            write_all(&file, entry.entry_at, &vec![0x5a_u8; span]).await?;
        }
    }
    file.sync_all().await
}

/// Garble one of the two metadata copies (`meta.0` or `meta.1`): the
/// journal repairs it from its twin, or refuses to start with both gone.
pub(crate) async fn rot_meta(
    provider: &SimStorageProvider,
    dir: &str,
    copy: u64,
) -> std::io::Result<bool> {
    let path = format!("{dir}/meta.{copy}");
    if !provider.exists(&path).await? {
        return Ok(false);
    }
    let file = provider.open(&path, OpenOptions::read_write()).await?;
    write_all(&file, 0, b"ROT!").await?;
    file.sync_all().await?;
    Ok(true)
}

/// Grow the newest segment by one block: a file-granularity metadata fault
/// (the size no longer matches the geometry), which the journal refuses to
/// open.
pub(crate) async fn resize_segment(
    provider: &SimStorageProvider,
    dir: &str,
) -> std::io::Result<bool> {
    let Some(name) = provider
        .list_dir(dir)
        .await?
        .into_iter()
        .filter(|name| segment_first(name).is_some())
        .max()
    else {
        return Ok(false);
    };
    let file = provider
        .open(&format!("{dir}/{name}"), OpenOptions::read_write())
        .await?;
    let size = file.size().await?;
    file.set_len(size + 4096).await?;
    file.sync_all().await?;
    Ok(true)
}

/// Delete every file of the journal under `dir` (a wiped disk): the next
/// open finds no segment and no metadata, so no format marker either.
pub(crate) async fn wipe(provider: &SimStorageProvider, dir: &str) -> std::io::Result<()> {
    let Ok(names) = provider.list_dir(dir).await else {
        // No directory: nothing was ever written there.
        return Ok(());
    };
    for name in names {
        provider.delete(&format!("{dir}/{name}")).await?;
    }
    provider.sync_dir(dir).await
}
