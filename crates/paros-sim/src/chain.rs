//! Deterministic Chain-of-Blocks application state and wire encoding.

use paros::{Command, Control, Slot};

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;
/// Version 2 (#101): the state carries per-lane block digests beside the
/// running chain hash, so a snapshot blob genuinely spans several
/// [`paros::SNAP_CHUNK_BYTES`] chunks and chunk-level repair is observable.
const SNAPSHOT_VERSION: u8 = 2;
/// Fixed digest-lane count. Command `i` folds into lane `i % CHAIN_LANES`, so
/// the whole array is a deterministic function of the applied prefix.
pub(crate) const CHAIN_LANES: usize = 32;
const SNAPSHOT_LEN: usize = 1 + 8 + 8 + 8 * CHAIN_LANES;

/// The complete application value checked across replicas.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ChainState {
    pub(crate) applied_count: u64,
    pub(crate) chain_hash: u64,
    /// Per-lane digests (see [`CHAIN_LANES`]): the snapshot body that makes
    /// the blob multi-chunk. Deterministic in the applied prefix, like
    /// everything else here.
    pub(crate) lanes: [u64; CHAIN_LANES],
}

impl Default for ChainState {
    fn default() -> Self {
        Self {
            applied_count: 0,
            chain_hash: FNV_OFFSET,
            lanes: [FNV_OFFSET; CHAIN_LANES],
        }
    }
}

impl ChainState {
    pub(crate) fn applied_slot(self) -> Option<Slot> {
        self.applied_count.checked_sub(1).map(Slot)
    }

    pub(crate) fn apply(self, command: &Command) -> AppliedTransition {
        let encoded = encode_command(command);
        let cmd_hash = fnv1a(&encoded);
        let mut chained = self.chain_hash.to_le_bytes().to_vec();
        chained.extend_from_slice(&encoded);
        let lane = usize::try_from(self.applied_count).unwrap_or(0) % CHAIN_LANES;
        let mut lanes = self.lanes;
        let mut lane_bytes = lanes[lane].to_le_bytes().to_vec();
        lane_bytes.extend_from_slice(&encoded);
        lanes[lane] = fnv1a(&lane_bytes);
        let next = Self {
            applied_count: self.applied_count.saturating_add(1),
            chain_hash: fnv1a(&chained),
            lanes,
        };
        AppliedTransition {
            next,
            cmd_hash,
            kind: command_kind(command),
        }
    }

    pub(crate) fn encode(self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(SNAPSHOT_LEN);
        bytes.push(SNAPSHOT_VERSION);
        bytes.extend_from_slice(&self.applied_count.to_le_bytes());
        bytes.extend_from_slice(&self.chain_hash.to_le_bytes());
        for lane in self.lanes {
            bytes.extend_from_slice(&lane.to_le_bytes());
        }
        bytes
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() != SNAPSHOT_LEN {
            return Err(format!(
                "chain snapshot has {} bytes, expected {SNAPSHOT_LEN}",
                bytes.len()
            ));
        }
        if bytes[0] != SNAPSHOT_VERSION {
            return Err(format!("unsupported chain snapshot version {}", bytes[0]));
        }
        let applied_count = u64::from_le_bytes(
            bytes[1..9]
                .try_into()
                .map_err(|_| "invalid applied-count encoding")?,
        );
        let chain_hash = u64::from_le_bytes(
            bytes[9..17]
                .try_into()
                .map_err(|_| "invalid chain-hash encoding")?,
        );
        let mut lanes = [0_u64; CHAIN_LANES];
        for (i, lane) in lanes.iter_mut().enumerate() {
            let start = 17 + 8 * i;
            *lane = u64::from_le_bytes(
                bytes[start..start + 8]
                    .try_into()
                    .map_err(|_| "invalid lane encoding")?,
            );
        }
        Ok(Self {
            applied_count,
            chain_hash,
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
