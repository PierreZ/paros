//! The judges of a paros journal that hold no cell.
//!
//! Two judges came out of the deleted simulation harness (`paros-sim`, tag
//! `last-service`) because they judge one journal and know nothing of a
//! cell, a machine or a coordinator:
//!
//! - [`linearizability`]: every attempt a client made at the four calls of a
//!   journal (`Write`, `Read`, `SetLeader`, `Truncate`), checked against the
//!   sequential model of a journal by a Wing and Gong search with Lowe's
//!   memoization (the Porcupine algorithm).
//! - [`journal_model`]: the verdict each node reached at each slot it
//!   walked, checked against one model of the journal state machine.
//!
//! Both report with moonpool's assertion macros, so a red is an
//! `assert_always!` violation of the run that drives them.

pub mod journal_model;
pub mod linearizability;

#[cfg(test)]
pub(crate) mod testing;

use paros_core::{FNV_OFFSET, fnv1a};

/// The hash of a user record as the journal model keeps it: FNV-1a over a
/// `0` tag byte and the bytes.
#[must_use]
pub fn user_command_hash(bytes: &[u8]) -> u64 {
    let mut encoded = Vec::with_capacity(1 + bytes.len());
    encoded.push(0);
    encoded.extend_from_slice(bytes);
    fnv1a(FNV_OFFSET, &encoded)
}

/// `SplitMix64`: the mixer the checkers use to hash their search states and to
/// draw test histories.
#[must_use]
pub fn splitmix(mut z: u64) -> u64 {
    z = z.wrapping_add(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

#[cfg(test)]
mod tests {
    //! Pins of the record hash and the mixer: a change to either changes
    //! every verdict that a recorded history gets.

    use super::{splitmix, user_command_hash};

    #[test]
    fn a_record_hash_is_tagged_as_a_user_command() {
        assert_eq!(user_command_hash(b"ab"), 0xd8dc_ca18_6baf_ada8);
        assert_eq!(user_command_hash(b""), 0xaf63_bd4c_8601_b7df);
    }

    #[test]
    fn splitmix_spreads_neighbours_apart() {
        assert_ne!(splitmix(0), splitmix(1));
        assert_eq!(splitmix(7), splitmix(7));
    }
}
