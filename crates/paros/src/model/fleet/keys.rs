//! The universe entry's root public keys (#245 (Biscuit tokens),
//! `docs/architecture/03-5-frontend-and-resolver.md` §3.5): the keys every entry role verifies a token
//! against. `init --root-public-key` writes the first; `universe key add`
//! adds the next and `universe key remove` takes an old one out, so a
//! rotation adds a key, then removes the old one once its tokens expired.
//!
//! paros stores a key and never reads it: Biscuit stays out of this crate
//! (§3.5). The fold checks only the shape of the text form, so a reader
//! never trusts bytes that cannot be a key. A key id or a label is never
//! reused, and the last key is never removed: no token could administer
//! the universe again.

use std::collections::BTreeMap;

use super::FleetDirectoryRefusal;
use crate::wire::fleet as wire;

/// The prefix of an Ed25519 public key in Biscuit's text form.
const ED25519: &str = "ed25519/";
/// The hex digits of an Ed25519 public key.
const KEY_HEX: usize = 64;

/// One root public key of the universe entry.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct RootKeyEntry {
    /// The Biscuit `root_key_id`: random, never 0.
    pub key_id: u32,
    /// Its name, for people: what an operator types.
    pub label: String,
    /// The key in Biscuit's text form, `ed25519/<64 hex>`.
    pub public_key: String,
}

impl RootKeyEntry {
    /// Whether the entry can be a key: a set id, a label, and the text form
    /// of an Ed25519 public key.
    #[must_use]
    pub fn is_well_formed(&self) -> bool {
        let hex = self.public_key.strip_prefix(ED25519);
        self.key_id != 0
            && !self.label.is_empty()
            && hex.is_some_and(|hex| {
                hex.len() == KEY_HEX && hex.bytes().all(|b| b.is_ascii_hexdigit())
            })
    }

    pub(super) fn to_wire(&self) -> wire::RootKey {
        wire::RootKey {
            key_id: self.key_id,
            label: self.label.clone(),
            public_key: self.public_key.clone(),
        }
    }

    pub(super) fn from_wire(key: wire::RootKey) -> Self {
        Self {
            key_id: key.key_id,
            label: key.label,
            public_key: key.public_key,
        }
    }
}

/// What a fold of the universe entry's keys changed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum KeyChange {
    /// The key joined.
    Added(u32),
    /// The key left.
    Removed(u32),
    /// The directory already held what the entry asks.
    Unchanged,
}

/// The keys in force and the keys removed, each by key id.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(super) struct RootKeys {
    live: BTreeMap<u32, RootKeyEntry>,
    removed: BTreeMap<u32, RootKeyEntry>,
}

impl RootKeys {
    /// The keys in force, in key id order.
    pub(super) fn live(&self) -> impl Iterator<Item = &RootKeyEntry> {
        self.live.values()
    }

    /// Whether any key, in force or removed, holds `key_id` or `label`.
    fn held(&self, key: &RootKeyEntry) -> bool {
        self.live
            .values()
            .chain(self.removed.values())
            .any(|held| held.key_id == key.key_id || held.label == key.label)
    }

    pub(super) fn add(&mut self, key: &RootKeyEntry) -> Result<KeyChange, FleetDirectoryRefusal> {
        if !key.is_well_formed() {
            return Err(FleetDirectoryRefusal::Malformed);
        }
        if self.live.get(&key.key_id) == Some(key) {
            return Ok(KeyChange::Unchanged);
        }
        if self.held(key) {
            moonpool_assertions::reachable!("fleet: a held root key id or label is refused");
            return Err(FleetDirectoryRefusal::RootKeyTaken {
                key_id: key.key_id,
                label: key.label.clone(),
            });
        }
        self.live.insert(key.key_id, key.clone());
        assert!(self.held(key), "an added key is held");
        Ok(KeyChange::Added(key.key_id))
    }

    pub(super) fn remove(&mut self, label: &str) -> Result<KeyChange, FleetDirectoryRefusal> {
        if label.is_empty() {
            return Err(FleetDirectoryRefusal::Malformed);
        }
        if self.removed.values().any(|key| key.label == label) {
            return Ok(KeyChange::Unchanged);
        }
        let Some(key_id) = self
            .live
            .values()
            .find(|key| key.label == label)
            .map(|key| key.key_id)
        else {
            return Err(FleetDirectoryRefusal::UnknownRootKey {
                label: label.to_string(),
            });
        };
        // The client refuses this before it writes (`LastRootKey`); the
        // fold refuses it again for two removals that race.
        if self.live.len() == 1 {
            return Err(FleetDirectoryRefusal::LastRootKey {
                label: label.to_string(),
            });
        }
        let key = self.live.remove(&key_id).expect("found above");
        self.removed.insert(key_id, key);
        assert!(!self.live.is_empty(), "a key stays in force");
        Ok(KeyChange::Removed(key_id))
    }

    pub(super) fn to_wire(&self) -> (Vec<wire::RootKey>, Vec<wire::RootKey>) {
        (
            self.live.values().map(RootKeyEntry::to_wire).collect(),
            self.removed.values().map(RootKeyEntry::to_wire).collect(),
        )
    }

    pub(super) fn restore(
        live: Vec<wire::RootKey>,
        removed: Vec<wire::RootKey>,
    ) -> Result<Self, &'static str> {
        let mut keys = Self::default();
        for key in live.into_iter().map(RootKeyEntry::from_wire) {
            if !key.is_well_formed() || keys.held(&key) {
                return Err("a fleet directory state holds a bad or repeated root key");
            }
            keys.live.insert(key.key_id, key);
        }
        for key in removed.into_iter().map(RootKeyEntry::from_wire) {
            if !key.is_well_formed() || keys.held(&key) {
                return Err("a fleet directory state holds a bad or repeated root key");
            }
            keys.removed.insert(key.key_id, key);
        }
        Ok(keys)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(key_id: u32, label: &str) -> RootKeyEntry {
        RootKeyEntry {
            key_id,
            label: label.into(),
            public_key: format!("{ED25519}{}", "ab".repeat(32)),
        }
    }

    #[test]
    fn a_key_is_added_once_and_never_reused() {
        let mut keys = RootKeys::default();
        assert_eq!(keys.add(&key(1, "a")), Ok(KeyChange::Added(1)));
        assert_eq!(keys.add(&key(1, "a")), Ok(KeyChange::Unchanged));
        assert!(matches!(
            keys.add(&key(1, "b")),
            Err(FleetDirectoryRefusal::RootKeyTaken { .. })
        ));
        assert!(matches!(
            keys.add(&key(2, "a")),
            Err(FleetDirectoryRefusal::RootKeyTaken { .. })
        ));
        assert_eq!(keys.add(&key(2, "b")), Ok(KeyChange::Added(2)));
        assert_eq!(keys.remove("a"), Ok(KeyChange::Removed(1)));
        assert_eq!(keys.remove("a"), Ok(KeyChange::Unchanged));
        // A removed key's id and label stay taken.
        assert!(matches!(
            keys.add(&key(1, "c")),
            Err(FleetDirectoryRefusal::RootKeyTaken { .. })
        ));
        assert!(matches!(
            keys.add(&key(3, "a")),
            Err(FleetDirectoryRefusal::RootKeyTaken { .. })
        ));
    }

    #[test]
    fn the_last_key_stays() {
        let mut keys = RootKeys::default();
        keys.add(&key(1, "a")).unwrap();
        assert!(matches!(
            keys.remove("a"),
            Err(FleetDirectoryRefusal::LastRootKey { .. })
        ));
        assert!(matches!(
            keys.remove("z"),
            Err(FleetDirectoryRefusal::UnknownRootKey { .. })
        ));
    }

    #[test]
    fn a_malformed_key_is_refused() {
        let mut keys = RootKeys::default();
        let mut bad = key(0, "a");
        assert_eq!(keys.add(&bad), Err(FleetDirectoryRefusal::Malformed));
        bad.key_id = 1;
        bad.public_key = "ed25519/zz".into();
        assert_eq!(keys.add(&bad), Err(FleetDirectoryRefusal::Malformed));
        bad.public_key = format!("secp256r1/{}", "ab".repeat(32));
        assert_eq!(keys.add(&bad), Err(FleetDirectoryRefusal::Malformed));
        assert_eq!(keys.add(&key(1, "")), Err(FleetDirectoryRefusal::Malformed));
    }

    #[test]
    fn the_keys_round_trip_through_a_checkpoint() {
        let mut keys = RootKeys::default();
        keys.add(&key(1, "a")).unwrap();
        keys.add(&key(2, "b")).unwrap();
        keys.remove("a").unwrap();
        let (live, removed) = keys.to_wire();
        assert_eq!(RootKeys::restore(live, removed), Ok(keys));
    }
}
