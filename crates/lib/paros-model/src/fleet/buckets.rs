//! Tenant placement across cells (#713 (bucket placement), §3.7): an
//! override map, then a bucket table, as Redis Cluster places keys in hash
//! slots.
//!
//! - **Bucket.** A tenant name falls in one of [`BUCKETS`] buckets:
//!   [`bucket_of`] is CRC16 (XMODEM, Redis Cluster's) of the name, modulo
//!   16384. The function is fixed and documented, a function of the name
//!   alone, so every reader that folds the same directory places a name in
//!   the same cell, in simulation and in production.
//! - **Bucket table.** Each bucket names one cell. The universe's first
//!   cell takes every bucket when it is added; `universe buckets assign`
//!   moves a range to another cell. A new cell takes no bucket by itself,
//!   so it fills on purpose, never by surprise.
//! - **Override.** A tenant created with a named cell (`--cell`, or
//!   `--cell-selector`, #672 (cell tag selector)) records an override with
//!   its reason. An override always wins over the bucket. An override names
//!   one tenant exactly: there are no prefix or pattern rows.
//!
//! The directory still records each tenant's cell. The table and the
//! overrides decide only where a new tenant goes and what a bucket move
//! must move. Until tenant moves land (#232 (multiple cells)), a bucket
//! moves only while no tenant placed by it lives in it.

use std::fmt;
use std::str::FromStr;

use paros_wire::fleet as wire;

/// How many buckets the universe has: fixed, never changed.
pub const BUCKETS: u16 = 16384;

/// The longest override reason, in bytes.
pub const MAX_REASON: usize = 64;

/// CRC16, the XMODEM variant (polynomial `0x1021`, initial value `0`):
/// Redis Cluster's key-slot hash.
#[must_use]
pub fn crc16(bytes: &[u8]) -> u16 {
    let mut crc: u16 = 0;
    for byte in bytes {
        crc ^= u16::from(*byte) << 8;
        for _ in 0..8 {
            crc = if crc & 0x8000 == 0 {
                crc << 1
            } else {
                (crc << 1) ^ 0x1021
            };
        }
    }
    crc
}

/// The bucket of the tenant name `name`: `CRC16(name) mod 16384`.
#[must_use]
pub fn bucket_of(name: &[u8]) -> u16 {
    crc16(name) % BUCKETS
}

/// How a `users` tenant was placed in its cell.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Placement {
    /// By its bucket: its cell is the bucket table's for its name.
    Bucket,
    /// By an override entry: the operator named the cell, with a reason
    /// people read (`universe status`, `tenant show`).
    Override {
        /// Why: free text, at most [`MAX_REASON`] bytes, e.g. `dedicated`,
        /// `canary`, `quarantine`, or what chose the cell.
        reason: String,
    },
}

impl Placement {
    /// An override with `reason`, cut to [`MAX_REASON`] bytes on a
    /// character boundary.
    #[must_use]
    pub fn override_for(reason: &str) -> Self {
        let mut end = reason.len().min(MAX_REASON);
        while !reason.is_char_boundary(end) {
            end -= 1;
        }
        Placement::Override {
            reason: reason[..end].to_string(),
        }
    }

    /// Whether the tenant was placed by its bucket.
    #[must_use]
    pub fn is_bucket(&self) -> bool {
        *self == Placement::Bucket
    }

    /// The label `parosctl` prints: `bucket` or `override (<reason>)`.
    #[must_use]
    pub fn label(&self) -> String {
        match self {
            Placement::Bucket => "bucket".to_string(),
            Placement::Override { reason } if reason.is_empty() => "override".to_string(),
            Placement::Override { reason } => format!("override ({reason})"),
        }
    }

    /// The wire pair: the kind (`1` bucket, `2` override) and the reason.
    pub(crate) fn to_wire(&self) -> (u32, String) {
        match self {
            Placement::Bucket => (1, String::new()),
            Placement::Override { reason } => (2, reason.clone()),
        }
    }

    /// From the wire pair. `0` is a tenant placed before buckets
    /// (metadata version 5 and below): it was named or took the
    /// operator's cell, so it reads as an override.
    pub(crate) fn from_wire(kind: u32, reason: String) -> Result<Self, &'static str> {
        if reason.len() > MAX_REASON {
            return Err("an override reason is too long");
        }
        match kind {
            0 => Ok(Placement::Override {
                reason: "placed before buckets".to_string(),
            }),
            1 if reason.is_empty() => Ok(Placement::Bucket),
            2 => Ok(Placement::Override { reason }),
            _ => Err("an unknown placement"),
        }
    }
}

/// An inclusive range of buckets, `first..=last`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BucketRange {
    first: u16,
    last: u16,
}

impl BucketRange {
    /// `first..=last`, when `first <= last < BUCKETS`.
    #[must_use]
    pub fn new(first: u16, last: u16) -> Option<Self> {
        (first <= last && last < BUCKETS).then_some(Self { first, last })
    }

    /// Every bucket.
    #[must_use]
    pub fn all() -> Self {
        Self {
            first: 0,
            last: BUCKETS - 1,
        }
    }

    /// Its first bucket.
    #[must_use]
    pub fn first(self) -> u16 {
        self.first
    }

    /// Its last bucket.
    #[must_use]
    pub fn last(self) -> u16 {
        self.last
    }

    /// How many buckets it holds.
    #[must_use]
    pub fn len(self) -> usize {
        usize::from(self.last - self.first) + 1
    }

    /// Never empty: a range holds at least one bucket.
    #[must_use]
    pub fn is_empty(self) -> bool {
        false
    }

    /// Whether it holds `bucket`.
    #[must_use]
    pub fn contains(self, bucket: u16) -> bool {
        (self.first..=self.last).contains(&bucket)
    }
}

impl fmt::Display for BucketRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.first == self.last {
            write!(f, "{}", self.first)
        } else {
            write!(f, "{}-{}", self.first, self.last)
        }
    }
}

impl FromStr for BucketRange {
    type Err = String;

    /// `N` or `FIRST-LAST`, both inclusive, below 16384.
    fn from_str(text: &str) -> Result<Self, String> {
        let number = |part: &str| {
            part.trim()
                .parse::<u16>()
                .map_err(|_| format!("{part:?} is not a bucket number"))
        };
        let (first, last) = if let Some((first, last)) = text.split_once('-') {
            (number(first)?, number(last)?)
        } else {
            let bucket = number(text)?;
            (bucket, bucket)
        };
        Self::new(first, last).ok_or_else(|| {
            format!(
                "a bucket range is FIRST-LAST with FIRST <= LAST <= {}",
                BUCKETS - 1
            )
        })
    }
}

/// The bucket table: which cell each bucket names. Empty until the
/// universe's first cell is added.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BucketTable {
    /// One cell id per bucket, or nothing before the first cell.
    owners: Vec<u64>,
}

impl BucketTable {
    /// Whether no bucket names a cell yet.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.owners.is_empty()
    }

    /// The cell `bucket` names, once the table is filled.
    #[must_use]
    pub fn cell(&self, bucket: u16) -> Option<u64> {
        self.owners.get(usize::from(bucket)).copied()
    }

    /// Give every bucket to `cell_id`.
    pub(crate) fn fill(&mut self, cell_id: u64) {
        self.owners = vec![cell_id; usize::from(BUCKETS)];
    }

    /// Give `range` to `cell_id`. Whether a bucket changed hands.
    pub(crate) fn assign(&mut self, range: BucketRange, cell_id: u64) -> bool {
        if self.owners.is_empty() {
            return false;
        }
        let owners = &mut self.owners[usize::from(range.first)..=usize::from(range.last)];
        let changed = owners.iter().any(|owner| *owner != cell_id);
        owners.fill(cell_id);
        changed
    }

    /// Whether every bucket of `range` names `cell_id`.
    #[must_use]
    pub fn holds(&self, range: BucketRange, cell_id: u64) -> bool {
        !self.owners.is_empty()
            && self.owners[usize::from(range.first)..=usize::from(range.last)]
                .iter()
                .all(|owner| *owner == cell_id)
    }

    /// The table as runs of buckets, each with its cell, in bucket order.
    #[must_use]
    pub fn runs(&self) -> Vec<(BucketRange, u64)> {
        let mut runs: Vec<(BucketRange, u64)> = Vec::new();
        for (bucket, owner) in (0..BUCKETS).zip(&self.owners) {
            match runs.last_mut() {
                Some((range, cell)) if cell == owner => range.last = bucket,
                _ => runs.push((
                    BucketRange {
                        first: bucket,
                        last: bucket,
                    },
                    *owner,
                )),
            }
        }
        runs
    }

    /// How many buckets name `cell_id`.
    #[must_use]
    pub fn count(&self, cell_id: u64) -> usize {
        self.owners
            .iter()
            .filter(|owner| **owner == cell_id)
            .count()
    }

    /// The table as checkpoint runs.
    pub(crate) fn to_wire(&self) -> Vec<wire::BucketRun> {
        self.runs()
            .into_iter()
            .map(|(range, cell_id)| wire::BucketRun {
                first: u32::from(range.first),
                last: u32::from(range.last),
                cell_id,
            })
            .collect()
    }

    /// From a checkpoint's runs, whose cells must be among `cells`. A state
    /// from before buckets (metadata version 5) holds none: every bucket
    /// then names `first_cell`, the cell that hosts the universe tenant
    /// (`0` before it is added).
    pub(crate) fn restore(
        runs: Vec<wire::BucketRun>,
        first_cell: u64,
        known: impl Fn(u64) -> bool,
    ) -> Result<Self, &'static str> {
        let mut table = Self::from_wire(runs)?;
        if table.is_empty() && first_cell != 0 {
            table.fill(first_cell);
        }
        if table.owners.iter().any(|cell| !known(*cell)) {
            return Err("a bucket names a cell the state does not hold");
        }
        Ok(table)
    }

    /// From checkpoint runs: none, or runs that cover every bucket once, in
    /// order.
    pub(crate) fn from_wire(runs: Vec<wire::BucketRun>) -> Result<Self, &'static str> {
        let mut owners = Vec::with_capacity(if runs.is_empty() {
            0
        } else {
            usize::from(BUCKETS)
        });
        for run in runs {
            let (Ok(first), Ok(last)) = (u16::try_from(run.first), u16::try_from(run.last)) else {
                return Err("a bucket run is out of range");
            };
            if BucketRange::new(first, last).is_none()
                || usize::from(first) != owners.len()
                || run.cell_id == 0
            {
                return Err("bucket runs do not cover the buckets once, in order");
            }
            owners.resize(usize::from(last) + 1, run.cell_id);
        }
        if !owners.is_empty() && owners.len() != usize::from(BUCKETS) {
            return Err("bucket runs do not cover every bucket");
        }
        Ok(Self { owners })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_hash_is_redis_clusters() {
        // The CRC16 check value, and Redis Cluster's documented slot.
        assert_eq!(crc16(b"123456789"), 0x31c3);
        assert_eq!(bucket_of(b"123456789"), 12739);
        assert_eq!(bucket_of(b""), 0);
    }

    #[test]
    fn a_range_parses_and_prints() {
        let range: BucketRange = "0-8191".parse().unwrap();
        assert_eq!((range.first(), range.last(), range.len()), (0, 8191, 8192));
        assert_eq!(range.to_string(), "0-8191");
        assert_eq!("42".parse::<BucketRange>().unwrap().to_string(), "42");
        assert!("9-3".parse::<BucketRange>().is_err());
        assert!("0-16384".parse::<BucketRange>().is_err());
        assert!("x".parse::<BucketRange>().is_err());
    }

    #[test]
    fn the_table_runs_round_trip() {
        let mut table = BucketTable::default();
        assert!(table.runs().is_empty());
        table.fill(7);
        assert!(table.assign(BucketRange::new(100, 199).unwrap(), 9));
        assert!(!table.assign(BucketRange::new(100, 150).unwrap(), 9));
        assert_eq!(table.count(9), 100);
        let runs = table.runs();
        assert_eq!(runs.len(), 3);
        assert_eq!(runs[1], (BucketRange::new(100, 199).unwrap(), 9));
        assert_eq!(BucketTable::from_wire(table.to_wire()).unwrap(), table);
        assert!(BucketTable::from_wire(table.to_wire()[1..].to_vec()).is_err());
    }

    #[test]
    fn a_reason_is_cut_on_a_character_boundary() {
        let long = "é".repeat(MAX_REASON);
        let Placement::Override { reason } = Placement::override_for(&long) else {
            panic!("an override");
        };
        assert!(reason.len() <= MAX_REASON);
        assert!(Placement::from_wire(2, reason).is_ok());
    }
}
