//! Deterministic Chain-of-Blocks application state and wire encoding.

use paros::{Command, Control, Slot};

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;
/// Version 3: the state carries per-lane block digests beside the running
/// chain hash, so a snapshot blob genuinely spans several
/// [`paros::SNAP_CHUNK_BYTES`] chunks and chunk-level repair is observable —
/// and the lane count travels in the blob, so a seed can draw it.
const SNAPSHOT_VERSION: u8 = 3;
/// Upper bound on the digest-lane count (the array is fixed; only
/// `lane_count` lanes are live and encoded).
pub(crate) const MAX_LANES: usize = 128;
/// The lane count the corpus and the default state use: five chunks of
/// [`paros::SNAP_CHUNK_BYTES`], the grid the chunk corpus enumerates.
pub(crate) const DEFAULT_LANES: u8 = 32;
const HEADER_LEN: usize = 1 + 1 + 8 + 8;

/// The complete application value checked across replicas. Command `i` folds
/// into lane `i % lane_count`, so the whole array is a deterministic function
/// of the applied prefix (and of the lane count, which every node of a run
/// shares).
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct ChainState {
    pub(crate) applied_count: u64,
    pub(crate) chain_hash: u64,
    /// Live digest lanes: the snapshot body that makes the blob multi-chunk.
    /// A per-seed draw on the main campaign (1..=128 lanes, so the blob spans
    /// 1 to 17 chunks), fixed at [`DEFAULT_LANES`] on the corpus.
    pub(crate) lane_count: u8,
    /// Per-lane digests; lanes past `lane_count` stay at the offset.
    pub(crate) lanes: [u64; MAX_LANES],
}

/// Compact: the count, the digest, and the live lane count — never the lane
/// array, which would drown every red-path dump in a kilobyte per node.
impl std::fmt::Debug for ChainState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ChainState({}, {}, {} lanes)",
            self.applied_count,
            hash_text(self.chain_hash),
            self.lane_count
        )
    }
}

impl Default for ChainState {
    fn default() -> Self {
        Self::empty(DEFAULT_LANES)
    }
}

impl ChainState {
    /// The empty state with `lane_count` live lanes (at least one).
    pub(crate) fn empty(lane_count: u8) -> Self {
        Self {
            applied_count: 0,
            chain_hash: FNV_OFFSET,
            lane_count: lane_count.clamp(1, u8::try_from(MAX_LANES).unwrap_or(u8::MAX)),
            lanes: [FNV_OFFSET; MAX_LANES],
        }
    }

    pub(crate) fn applied_slot(self) -> Option<Slot> {
        self.applied_count.checked_sub(1).map(Slot)
    }

    pub(crate) fn apply(self, command: &Command) -> AppliedTransition {
        let encoded = encode_command(command);
        let cmd_hash = fnv1a(&encoded);
        let mut chained = self.chain_hash.to_le_bytes().to_vec();
        chained.extend_from_slice(&encoded);
        let lane = usize::try_from(self.applied_count).unwrap_or(0) % usize::from(self.lane_count);
        let mut lanes = self.lanes;
        let mut lane_bytes = lanes[lane].to_le_bytes().to_vec();
        lane_bytes.extend_from_slice(&encoded);
        lanes[lane] = fnv1a(&lane_bytes);
        let next = Self {
            applied_count: self.applied_count.saturating_add(1),
            chain_hash: fnv1a(&chained),
            lane_count: self.lane_count,
            lanes,
        };
        AppliedTransition {
            next,
            cmd_hash,
            kind: command_kind(command),
        }
    }

    fn encoded_len(lane_count: u8) -> usize {
        HEADER_LEN + 8 * usize::from(lane_count)
    }

    pub(crate) fn encode(self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(Self::encoded_len(self.lane_count));
        bytes.push(SNAPSHOT_VERSION);
        bytes.push(self.lane_count);
        bytes.extend_from_slice(&self.applied_count.to_le_bytes());
        bytes.extend_from_slice(&self.chain_hash.to_le_bytes());
        for lane in &self.lanes[..usize::from(self.lane_count)] {
            bytes.extend_from_slice(&lane.to_le_bytes());
        }
        bytes
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() < HEADER_LEN {
            return Err(format!(
                "chain snapshot has {} bytes, too short",
                bytes.len()
            ));
        }
        if bytes[0] != SNAPSHOT_VERSION {
            return Err(format!("unsupported chain snapshot version {}", bytes[0]));
        }
        let lane_count = bytes[1];
        if lane_count == 0 || usize::from(lane_count) > MAX_LANES {
            return Err(format!("invalid lane count {lane_count}"));
        }
        if bytes.len() != Self::encoded_len(lane_count) {
            return Err(format!(
                "chain snapshot has {} bytes, expected {}",
                bytes.len(),
                Self::encoded_len(lane_count)
            ));
        }
        let applied_count = u64::from_le_bytes(
            bytes[2..10]
                .try_into()
                .map_err(|_| "invalid applied-count encoding")?,
        );
        let chain_hash = u64::from_le_bytes(
            bytes[10..18]
                .try_into()
                .map_err(|_| "invalid chain-hash encoding")?,
        );
        let mut lanes = [FNV_OFFSET; MAX_LANES];
        for (i, lane) in lanes.iter_mut().enumerate().take(usize::from(lane_count)) {
            let start = HEADER_LEN + 8 * i;
            *lane = u64::from_le_bytes(
                bytes[start..start + 8]
                    .try_into()
                    .map_err(|_| "invalid lane encoding")?,
            );
        }
        Ok(Self {
            applied_count,
            chain_hash,
            lane_count,
            lanes,
        })
    }
}

pub(crate) struct AppliedTransition {
    pub(crate) next: ChainState,
    pub(crate) cmd_hash: u64,
    pub(crate) kind: &'static str,
}

pub(crate) fn command_hash(command: &Command) -> u64 {
    fnv1a(&encode_command(command))
}

pub(crate) fn user_command_hash(bytes: &[u8]) -> u64 {
    let mut encoded = Vec::with_capacity(1 + bytes.len());
    encoded.push(0);
    encoded.extend_from_slice(bytes);
    fnv1a(&encoded)
}

pub(crate) fn hash_text(hash: u64) -> String {
    format!("{hash:016x}")
}

fn command_kind(command: &Command) -> &'static str {
    match command {
        Command::User(_) => "user",
        Command::Control(Control::Truncate { .. }) => "truncate",
        Command::Control(Control::Noop) => "noop",
        Command::Control(Control::Snap { .. }) => "snap",
    }
}

fn encode_command(command: &Command) -> Vec<u8> {
    match command {
        Command::User(entry) => {
            let mut bytes = Vec::with_capacity(1 + entry.value.0.len());
            bytes.push(0);
            bytes.extend_from_slice(&entry.value.0);
            bytes
        }
        Command::Control(Control::Truncate { up_to }) => {
            let mut bytes = Vec::with_capacity(9);
            bytes.push(1);
            bytes.extend_from_slice(&up_to.0.to_le_bytes());
            bytes
        }
        Command::Control(Control::Noop) => vec![2],
        Command::Control(Control::Snap { at_index }) => {
            let mut bytes = Vec::with_capacity(9);
            bytes.push(3);
            bytes.extend_from_slice(&at_index.0.to_le_bytes());
            bytes
        }
    }
}

fn fnv1a(bytes: &[u8]) -> u64 {
    let mut hash = FNV_OFFSET;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}
