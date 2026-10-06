//! The metainfo: a small record kept in two copies, `meta.0` and `meta.1`
//! (CTRL §3.3.1: "two copies of the metainfo locally; if one copy is
//! faulty, the other copy is used").
//!
//! Each copy carries a generation and a CRC. A store spends a new
//! generation, then rewrites each copy in turn through a temporary file, a
//! sync, a rename and a directory sync, so at every instant at least one
//! copy holds the old value or the new one intact. A load takes the valid
//! copy with the highest generation and rewrites the other from it when it
//! is damaged, missing or behind. The generation is spent before either copy
//! is written (moonpool#304's lesson), so two valid copies of one generation
//! always hold one payload: two that disagree are damage, and a load refuses
//! them rather than pick one and maybe roll the value back. Both copies bad
//! is a crash verdict — the promise is a statement about this node's own
//! future behaviour that no peer can restore.
//!
//! ```text
//! 0  u32 magic "PRMI"   4  u32 version   8  u64 generation
//! 16 u32 payload len    20 u32 crc (0..20 + payload)   24 payload, zero-padded
//! ```

use std::io;

use moonpool_core::{BlockFile, OpenOptions, StorageFile, StorageProvider};

const MAGIC: u32 = u32::from_le_bytes(*b"PRMI");
const VERSION: u32 = 1;
const HEADER: usize = 24;
/// The block the copies are written in: the sector, so a copy is whole
/// blocks on any disk the store runs on.
const BLOCK: usize = 512;

/// What one copy holds on disk.
#[derive(Debug, PartialEq, Eq)]
enum Held {
    Missing,
    Damaged,
    Valid(u64, Vec<u8>),
}

/// Why the metainfo could not be loaded.
#[derive(Debug)]
pub(crate) enum DualError {
    /// A copy exists but none is valid, or two valid copies of one
    /// generation disagree.
    Corrupt,
    /// The namespace or a repair could not be read or written.
    Io(io::Error),
}

impl From<io::Error> for DualError {
    fn from(error: io::Error) -> Self {
        DualError::Io(error)
    }
}

/// The newest valid copy's `(generation, payload)`; `None` when neither
/// copy exists.
fn newest(copies: &[Held; 2]) -> Result<Option<(u64, Vec<u8>)>, DualError> {
    match copies {
        [Held::Missing, Held::Missing] => Ok(None),
        [Held::Valid(g0, p0), Held::Valid(g1, p1)] if g0 == g1 && p0 != p1 => {
            Err(DualError::Corrupt)
        }
        [Held::Valid(g0, p0), Held::Valid(g1, p1)] => Ok(Some(if g1 > g0 {
            (*g1, p1.clone())
        } else {
            (*g0, p0.clone())
        })),
        [Held::Valid(g, p), _] | [_, Held::Valid(g, p)] => Ok(Some((*g, p.clone()))),
        _ => Err(DualError::Corrupt),
    }
}

fn crc(header: &[u8], payload: &[u8]) -> u32 {
    crc32c::crc32c_append(crc32c::crc32c(&header[..20]), payload)
}

fn decode(bytes: &[u8]) -> Option<(u64, Vec<u8>)> {
    if bytes.len() < HEADER {
        return None;
    }
    let field = |at: usize| u32::from_le_bytes(bytes[at..at + 4].try_into().expect("4 bytes"));
    if field(0) != MAGIC || field(4) != VERSION {
        return None;
    }
    let generation = u64::from_le_bytes(bytes[8..16].try_into().expect("8 bytes"));
    let len = field(16) as usize;
    let payload = bytes.get(HEADER..HEADER.checked_add(len)?)?;
    (generation > 0 && crc(bytes, payload) == field(20)).then(|| (generation, payload.to_vec()))
}

/// The two-copy metainfo under one directory.
#[derive(Debug)]
pub(crate) struct Dual {
    dir: String,
    /// Generation of the newest copy written or loaded; zero when none.
    generation: u64,
}

impl Dual {
    fn path(dir: &str, copy: u64) -> String {
        format!("{dir}/meta.{copy}")
    }

    /// Whether a value is on disk: a copy loaded, or a store made.
    pub(crate) fn is_stored(&self) -> bool {
        self.generation > 0
    }

    /// Read both copies, keep the newest valid one, and repair the other
    /// from it. Returns the payload (`None` when neither copy exists) and
    /// whether a copy was repaired.
    pub(crate) async fn load<P: StorageProvider>(
        provider: &P,
        dir: &str,
    ) -> Result<(Self, Option<Vec<u8>>, bool), DualError> {
        let copies = read_copies(provider, dir).await?;
        let mut this = Self {
            dir: dir.to_string(),
            generation: 0,
        };
        let Some((generation, payload)) = newest(&copies)? else {
            return Ok((this, None, false));
        };
        this.generation = generation;
        let mut repaired = false;
        for (copy, held) in copies.iter().enumerate() {
            if !matches!(held, Held::Valid(g, _) if *g == generation) {
                store_copy(provider, dir, copy as u64, generation, &payload).await?;
                repaired = true;
            }
        }
        // After a load both copies hold the newest generation's payload.
        assert!(this.is_stored(), "a loaded metainfo is stored");
        Ok((this, Some(payload), repaired))
    }

    /// The payload [`load`](Self::load) would return, without writing or
    /// creating anything.
    pub(crate) async fn peek<P: StorageProvider>(
        provider: &P,
        dir: &str,
    ) -> Result<Option<Vec<u8>>, DualError> {
        let copies = read_copies(provider, dir).await?;
        Ok(newest(&copies)?.map(|(_, payload)| payload))
    }

    /// Durably replace the metainfo with `payload`, in both copies. At least
    /// one copy is intact if an error is returned.
    pub(crate) async fn store<P: StorageProvider>(
        &mut self,
        provider: &P,
        payload: &[u8],
    ) -> Result<(), io::Error> {
        // Spent before the first copy is written: a retry after a failure
        // part-way never reuses it.
        self.generation += 1;
        let generation = self.generation;
        assert!(generation > 0, "a stored metainfo has a generation");
        for copy in 0..2 {
            store_copy(provider, &self.dir, copy, generation, payload).await?;
        }
        Ok(())
    }
}

async fn read_copies<P: StorageProvider>(provider: &P, dir: &str) -> Result<[Held; 2], DualError> {
    let mut copies = [Held::Missing, Held::Missing];
    for (copy, held) in copies.iter_mut().enumerate() {
        let path = Dual::path(dir, copy as u64);
        if !provider.exists(&path).await? {
            continue;
        }
        // A copy that cannot be read is as good as a damaged one: the other
        // copy is what the two-copy scheme is for.
        *held = match read_copy(provider, &path).await {
            Ok(Some((generation, payload))) => Held::Valid(generation, payload),
            Ok(None) | Err(_) => Held::Damaged,
        };
    }
    Ok(copies)
}

async fn read_copy<P: StorageProvider>(
    provider: &P,
    path: &str,
) -> io::Result<Option<(u64, Vec<u8>)>> {
    let file = provider.open(path, OpenOptions::read_only()).await?;
    let size = file.size().await?;
    if size == 0 || !size.is_multiple_of(BLOCK as u64) {
        return Ok(None);
    }
    let blocks = BlockFile::new(file, BLOCK)?;
    let count = usize::try_from(size / BLOCK as u64)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "metainfo too large"))?;
    let mut buf = blocks.buffer(count)?;
    blocks.read_blocks(0, buf.as_mut_slice()).await?;
    Ok(decode(buf.as_slice()))
}

/// Replace one copy through a temporary file, a sync, a rename and a
/// directory sync.
async fn store_copy<P: StorageProvider>(
    provider: &P,
    dir: &str,
    copy: u64,
    generation: u64,
    payload: &[u8],
) -> io::Result<()> {
    let target = Dual::path(dir, copy);
    let temporary = format!("{target}.tmp");
    if provider.exists(&temporary).await? {
        provider.delete(&temporary).await?;
    }
    let file = provider
        .open(&temporary, OpenOptions::create_new_write().read(true))
        .await?;
    let blocks = BlockFile::new(file, BLOCK)?;
    let mut buf = blocks.buffer((HEADER + payload.len()).div_ceil(BLOCK))?;
    let bytes = buf.as_mut_slice();
    bytes[0..4].copy_from_slice(&MAGIC.to_le_bytes());
    bytes[4..8].copy_from_slice(&VERSION.to_le_bytes());
    bytes[8..16].copy_from_slice(&generation.to_le_bytes());
    let len = u32::try_from(payload.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "metainfo too large"))?;
    bytes[16..20].copy_from_slice(&len.to_le_bytes());
    bytes[HEADER..HEADER + payload.len()].copy_from_slice(payload);
    let checksum = crc(bytes, payload);
    bytes[20..24].copy_from_slice(&checksum.to_le_bytes());
    // Pair of `decode`: the copy written is the copy a load reads.
    assert!(
        decode(bytes) == Some((generation, payload.to_vec())),
        "a metainfo copy reads back as itself"
    );
    blocks.write_blocks(0, buf.as_slice()).await?;
    blocks.sync().await?;
    drop(blocks);
    provider.rename(&temporary, &target).await?;
    provider.sync_dir(dir).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_newest_valid_copy_wins_and_a_split_generation_is_damage() {
        let a = Held::Valid(3, vec![1]);
        let b = Held::Valid(4, vec![2]);
        assert_eq!(
            newest(&[a, b]).expect("two valid"),
            Some((4, vec![2])),
            "the higher generation wins"
        );
        assert!(matches!(
            newest(&[Held::Valid(4, vec![1]), Held::Valid(4, vec![2])]),
            Err(DualError::Corrupt)
        ));
        assert_eq!(
            newest(&[Held::Damaged, Held::Valid(1, vec![9])]).expect("one valid"),
            Some((1, vec![9]))
        );
        assert!(matches!(
            newest(&[Held::Damaged, Held::Missing]),
            Err(DualError::Corrupt)
        ));
        assert_eq!(newest(&[Held::Missing, Held::Missing]).expect("none"), None);
    }
}
