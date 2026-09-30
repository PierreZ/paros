//! The Chain-of-Blocks application — a **client** of the journal (#186).
//!
//! paros runs no user application: a journal's client reads the journal
//! through `Read` (#204) and folds what it reads. [`ChainState`] is that fold
//! for the simulation's client: every accepted record, in position order,
//! chained into one running digest. Slots that hold no position (a `Noop`, a
//! control command, a refused write, a retry) never reach it — a reader never
//! sees them — so two clients that read the same journal from the start agree
//! on the state after every record, and the audit checks exactly that
//! (`AuditWorld::fold_applied`).

use paros::{Command, Control, JournalState, Seq};

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

/// A client's fold of the journal: how many records it folded and the
/// running digest over them, each chained with its position (so the same
/// bytes at another position fold to another state).
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
    /// Fold the record at position `lsn` whose bytes are `value`.
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
    /// `states[i]` is the state after the first `i` slots — what a reader of
    /// that log computes. Each slot is judged by the core's own pure journal
    /// state machine (`paros::JournalState::apply`, #204), and the records a
    /// slot's write was accepted with fold at their positions; every other
    /// slot folds nothing.
    pub(crate) fn expected(commands: &[Command]) -> Vec<Self> {
        let mut states = vec![Self::default()];
        let mut journal = JournalState::default();
        let mut accepted: Vec<paros::Entry> = Vec::new();
        for command in commands {
            let previous = *states.last().expect("seeded with the initial state");
            let outcome = journal.apply(command, |seq| accepted.iter().find(|e| e.seq == seq));
            let next = match (outcome, command) {
                (paros::Outcome::Accepted { seq, .. }, Command::Write(entry)) => {
                    accepted.push(entry.clone());
                    (seq.0..)
                        .zip(&entry.records)
                        .fold(previous, |state, (position, record)| {
                            state.fold(position, &record.0)
                        })
                }
                _ => previous,
            };
            states.push(next);
        }
        states
    }
}

/// The hash a record is registered under (`AuditWorld::note_submitted`) and
/// checked against when a client folds it.
pub(crate) fn user_command_hash(bytes: &[u8]) -> u64 {
    let mut encoded = Vec::with_capacity(1 + bytes.len());
    encoded.push(0);
    encoded.extend_from_slice(bytes);
    fnv1a(&encoded)
}

pub(crate) fn hash_text(hash: u64) -> String {
    format!("{hash:016x}")
}

/// Log the `Truncate { up_to }` control command a truncation asks for.
pub(crate) fn trace_truncate(up_to: u64) {
    let command = Command::Control(Control::Truncate { up_to: Seq(up_to) });
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
