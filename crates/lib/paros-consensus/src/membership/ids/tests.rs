use super::MatchmakerId;
use crate::types::{FNV_OFFSET, Fingerprint, fnv1a};

/// A matchmaker set's identity is the FNV-1a fold of its members in
/// order: two different sets get two different identities.
#[test]
fn a_matchmaker_set_fingerprint_folds_its_members() {
    let set = |ids: &[u64]| ids.iter().copied().map(MatchmakerId).collect::<Vec<_>>();
    assert_eq!(set(&[]).fingerprint(), FNV_OFFSET);
    assert_eq!(
        set(&[7]).fingerprint(),
        fnv1a(FNV_OFFSET, &7u64.to_le_bytes())
    );
    assert_ne!(set(&[1, 2]).fingerprint(), set(&[1, 3]).fingerprint());
    assert_ne!(set(&[1, 2]).fingerprint(), set(&[2, 1]).fingerprint());
}
