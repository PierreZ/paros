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

use paros::core::journal::Command;
use paros::core::journal::Control;
use paros::core::journal::LeaderUuid;
use paros::core::journal::Seq;
use paros::core::{FNV_OFFSET, fnv1a};

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
            chain_hash: fnv1a(FNV_OFFSET, &chained),
        }
    }
}

/// The hash a record is registered under (`AuditWorld::note_submitted`) and
/// checked against when a client folds it.
pub(crate) fn user_command_hash(bytes: &[u8]) -> u64 {
    let mut encoded = Vec::with_capacity(1 + bytes.len());
    encoded.push(0);
    encoded.extend_from_slice(bytes);
    fnv1a(FNV_OFFSET, &encoded)
}

pub(crate) fn hash_text(hash: u64) -> String {
    format!("{hash:016x}")
}

/// Log the fenced `Truncate` control command a truncation asks for (#228).
pub(crate) fn trace_truncate(leader: LeaderUuid, up_to: u64) {
    let command = Command::Control(Control::Truncate {
        leader,
        up_to: Seq(up_to),
    });
    tracing::info!(
        cmd = %hash_text(paros::roles::node::command_hash(&command)),
        up_to,
        "chain_control_submitted"
    );
}

/// Spread one draw into another (`splitmix64`): the entries of a burst
/// drawn from one step's draws, and the linearizability search's hashes.
#[must_use]
pub(crate) fn splitmix(mut z: u64) -> u64 {
    z = z.wrapping_add(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

#[cfg(test)]
mod tests {
    //! Pins of the client's fold and of the record hash: the audit compares
    //! them across clients and runs, so a change to either digest is a
    //! change to every seed's verdicts.

    use super::{ChainState, hash_text, splitmix, user_command_hash};

    #[test]
    fn the_empty_fold_is_the_fnv_offset_basis() {
        let empty = ChainState::default();
        assert_eq!(empty.applied_count, 0);
        assert_eq!(empty.chain_hash, 0xcbf2_9ce4_8422_2325);
    }

    #[test]
    fn each_fold_chains_the_digest_with_the_position() {
        let one = ChainState::default().fold(1, b"ab");
        assert_eq!(one.applied_count, 1);
        assert_eq!(one.chain_hash, 0xb123_04d7_7f4f_aec0);
        let two = one.fold(2, b"");
        assert_eq!(two.applied_count, 2);
        assert_eq!(two.chain_hash, 0xabc8_965d_96f0_ef7c);
        assert_ne!(
            ChainState::default().fold(2, b"ab"),
            one,
            "the same bytes at another position fold to another state"
        );
    }

    #[test]
    fn a_record_hash_is_tagged_as_a_user_command() {
        assert_eq!(user_command_hash(b"ab"), 0xd8dc_ca18_6baf_ada8);
        assert_eq!(user_command_hash(b""), 0xaf63_bd4c_8601_b7df);
    }

    #[test]
    fn a_hash_prints_as_sixteen_hex_digits_and_a_state_as_its_count_and_digest() {
        assert_eq!(hash_text(0xab), "00000000000000ab");
        let state = ChainState {
            applied_count: 3,
            chain_hash: 0x10,
        };
        assert_eq!(format!("{state:?}"), "ChainState(3, 0000000000000010)");
    }

    #[test]
    fn splitmix_is_the_reference_splitmix64_step() {
        assert_eq!(splitmix(0), 0xe220_a839_7b1d_cdaf);
        assert_ne!(splitmix(1), splitmix(0));
    }
}
