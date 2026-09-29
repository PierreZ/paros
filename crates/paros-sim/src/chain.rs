//! The Chain-of-Blocks application — a **client** of the journal (#186).
//!
//! paros runs no application: a journal's client reads the log through
//! `Read` (#185) and folds what it reads. [`ChainState`] is that fold for the
//! simulation's client: every user entry, in LSN order, chained into one
//! running digest. Holes (a `Noop`, a control command, a #94 duplicate)
//! never reach it — a reader never sees them — so two clients that read the
//! same journal from the start agree on the state after every entry, and the
//! audit checks exactly that (`AuditWorld::fold_applied`).

use paros::{Command, Control, Slot};

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

/// A client's fold of the journal: how many user entries it folded and the
/// running digest over them, each chained with its LSN (so the same bytes at
/// another position fold to another state).
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct ChainState {
    pub(crate) applied_count: u64,
    pub(crate) chain_hash: u64,
}

/// Compact: the count and the digest.
impl std::fmt::Debug for ChainState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ChainState({}, {})",
            self.applied_count,
            hash_text(self.chain_hash)
        )
    }
}

impl Default for ChainState {
    fn default() -> Self {
        Self {
            applied_count: 0,
            chain_hash: FNV_OFFSET,
        }
    }
}

impl ChainState {
    /// Fold the user entry at `lsn` whose slot value is `value` (the framed
    /// records, exactly as the slot decided them).
    pub(crate) fn fold(self, lsn: u64, value: &[u8]) -> Self {
        let mut chained = self.chain_hash.to_le_bytes().to_vec();
        chained.extend_from_slice(&lsn.to_le_bytes());
        chained.push(0);
        chained.extend_from_slice(value);
        Self {
            applied_count: self.applied_count.saturating_add(1),
            chain_hash: fnv1a(&chained),
        }
    }

    /// The analytic fold of a decided command sequence starting at slot 0:
    /// `states[i]` is the state after the first `i` slots, holes (control
    /// commands) folding nothing — what a reader of that log computes.
    pub(crate) fn expected(commands: &[Command]) -> Vec<Self> {
        let mut states = vec![Self::default()];
        for (slot, command) in commands.iter().enumerate() {
            let previous = *states.last().expect("seeded with the initial state");
            let next = match command {
                Command::User(entry) => {
                    previous.fold(u64::try_from(slot).unwrap_or(u64::MAX), &entry.value.0)
                }
                Command::Control(_) => previous,
            };
            states.push(next);
        }
        states
    }
}

/// The hash a user command's slot value is registered under
/// (`AuditWorld::note_submitted`) and checked against when a client folds it.
pub(crate) fn user_command_hash(bytes: &[u8]) -> u64 {
    let mut encoded = Vec::with_capacity(1 + bytes.len());
    encoded.push(0);
    encoded.extend_from_slice(bytes);
    fnv1a(&encoded)
}

pub(crate) fn hash_text(hash: u64) -> String {
    format!("{hash:016x}")
}

/// Log the `Truncate { up_to }` control command a trim request asks for.
pub(crate) fn trace_truncate(up_to: u64) {
    let command = Command::Control(Control::Truncate { up_to: Slot(up_to) });
    tracing::info!(
        cmd = %hash_text(paros::command_hash(&command)),
        up_to,
        "chain_control_submitted"
    );
}

fn fnv1a(bytes: &[u8]) -> u64 {
    let mut hash = FNV_OFFSET;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}
