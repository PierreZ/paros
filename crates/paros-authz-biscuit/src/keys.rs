//! Root key pairs, their files, and the ring of public keys a verifier
//! trusts.
//!
//! A key file is one JSON object: `key_id` (the Biscuit `root_key_id`, a
//! random `u32`), `label` (the human name), and the key in Biscuit's own
//! text form (`ed25519-private/<hex>` or `ed25519/<hex>`), which the
//! upstream `biscuit` CLI reads too. Ed25519 only (#245 rule 5).

use std::str::FromStr;

use biscuit_auth::builder::Algorithm;
use biscuit_auth::{PrivateKey, PublicKey};
use rand_chacha::rand_core::Rng;
use serde::{Deserialize, Serialize};

use crate::{Entropy, Error};

/// A root key pair: it signs authority blocks.
pub struct RootKey {
    key_id: u32,
    label: String,
    key: PrivateKey,
}

/// The public half of a root key pair: it verifies tokens.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RootPublicKey {
    key_id: u32,
    label: String,
    key: PublicKey,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PrivateFile {
    key_id: u32,
    label: String,
    private_key: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PublicFile {
    key_id: u32,
    label: String,
    public_key: String,
}

impl RootKey {
    /// A new root key pair, its key id and its key drawn from `entropy`.
    #[must_use]
    pub fn generate(label: &str, entropy: &Entropy) -> Self {
        let mut rng = entropy.rng();
        // `0` is unset in paros (§3.8): the universe entry refuses it.
        let mut key_id = rng.next_u32();
        while key_id == 0 {
            key_id = rng.next_u32();
        }
        let key = PrivateKey::new_with_rng(Algorithm::Ed25519, &mut rng);
        Self {
            key_id,
            label: label.to_string(),
            key,
        }
    }

    /// The Biscuit `root_key_id` of this pair.
    #[must_use]
    pub fn key_id(&self) -> u32 {
        self.key_id
    }

    /// The human name of this pair.
    #[must_use]
    pub fn label(&self) -> &str {
        &self.label
    }

    /// The public half.
    #[must_use]
    pub fn public(&self) -> RootPublicKey {
        RootPublicKey {
            key_id: self.key_id,
            label: self.label.clone(),
            key: self.key.public(),
        }
    }

    pub(crate) fn private_key(&self) -> &PrivateKey {
        &self.key
    }

    /// The `.private` file's content.
    ///
    /// # Panics
    ///
    /// Never: a key file is strings and a number.
    #[must_use]
    pub fn to_file(&self) -> String {
        let file = PrivateFile {
            key_id: self.key_id,
            label: self.label.clone(),
            private_key: self.key.to_prefixed_string(),
        };
        serde_json::to_string_pretty(&file).expect("a key file always serializes")
    }

    /// A key from a `.private` file's content.
    ///
    /// # Errors
    ///
    /// [`Error::BadKey`] when the file is not a private key file or the key
    /// is not Ed25519.
    pub fn from_file(text: &str) -> Result<Self, Error> {
        let file: PrivateFile = serde_json::from_str(text)
            .map_err(|e| Error::BadKey(format!("not a private key file: {e}")))?;
        let key =
            PrivateKey::from_str(&file.private_key).map_err(|e| Error::BadKey(e.to_string()))?;
        if key.algorithm() != Algorithm::Ed25519 {
            return Err(Error::BadKey("not an ed25519 key".to_string()));
        }
        Ok(Self {
            key_id: file.key_id,
            label: file.label,
            key,
        })
    }
}

impl std::fmt::Debug for RootKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RootKey")
            .field("key_id", &self.key_id)
            .field("label", &self.label)
            .finish_non_exhaustive()
    }
}

impl RootPublicKey {
    /// The Biscuit `root_key_id` of this key.
    #[must_use]
    pub fn key_id(&self) -> u32 {
        self.key_id
    }

    /// The human name of this key.
    #[must_use]
    pub fn label(&self) -> &str {
        &self.label
    }

    /// A public key from its parts: the universe entry's record of it
    /// (#245), its text form `ed25519/<hex>`.
    ///
    /// # Errors
    ///
    /// [`Error::BadKey`] when the text is not an Ed25519 public key.
    pub fn from_parts(key_id: u32, label: &str, text: &str) -> Result<Self, Error> {
        let key = PublicKey::from_str(text).map_err(|e| Error::BadKey(e.to_string()))?;
        if key.algorithm() != Algorithm::Ed25519 {
            return Err(Error::BadKey("not an ed25519 key".to_string()));
        }
        Ok(Self {
            key_id,
            label: label.to_string(),
            key,
        })
    }

    /// The key in Biscuit's text form, `ed25519/<hex>`.
    #[must_use]
    pub fn key_text(&self) -> String {
        self.key.to_string()
    }

    pub(crate) fn public_key(&self) -> PublicKey {
        self.key
    }

    /// The `.public` file's content.
    ///
    /// # Panics
    ///
    /// Never: a key file is strings and a number.
    #[must_use]
    pub fn to_file(&self) -> String {
        let file = PublicFile {
            key_id: self.key_id,
            label: self.label.clone(),
            public_key: self.key.to_string(),
        };
        serde_json::to_string_pretty(&file).expect("a key file always serializes")
    }

    /// A public key from a `.public` file's content, or the public half of
    /// a `.private` file's content.
    ///
    /// # Errors
    ///
    /// [`Error::BadKey`] when the text is neither key file, or the key is
    /// not Ed25519.
    pub fn from_file(text: &str) -> Result<Self, Error> {
        if let Ok(file) = serde_json::from_str::<PublicFile>(text) {
            let key =
                PublicKey::from_str(&file.public_key).map_err(|e| Error::BadKey(e.to_string()))?;
            if key.algorithm() != Algorithm::Ed25519 {
                return Err(Error::BadKey("not an ed25519 key".to_string()));
            }
            return Ok(Self {
                key_id: file.key_id,
                label: file.label,
                key,
            });
        }
        RootKey::from_file(text)
            .map(|key| key.public())
            .map_err(|_| Error::BadKey("not a public or private key file".to_string()))
    }

    /// The key on one line, `<label>:<key_id>:ed25519/<hex>`: what a
    /// variable or an argument carries in place of a `.public` file.
    #[must_use]
    pub fn to_line(&self) -> String {
        format!("{}:{}:{}", self.label, self.key_id, self.key)
    }

    /// A public key from its line ([`RootPublicKey::to_line`]).
    ///
    /// # Errors
    ///
    /// [`Error::BadKey`] when the text is not a key line, or the key is not
    /// Ed25519.
    pub fn from_line(text: &str) -> Result<Self, Error> {
        // The key text and the id hold no ':'.
        let mut parts = text.trim().rsplitn(3, ':');
        let (Some(key), Some(key_id), Some(label)) = (parts.next(), parts.next(), parts.next())
        else {
            return Err(Error::BadKey(
                "not a key line: <label>:<key_id>:ed25519/<hex>".to_string(),
            ));
        };
        if !is_label(label) {
            return Err(Error::BadKey(format!(
                "bad label {label:?}: letters, digits, '.', '_' and '-' only"
            )));
        }
        let key_id = key_id
            .parse()
            .map_err(|_| Error::BadKey(format!("bad key id {key_id:?}")))?;
        Self::from_parts(key_id, label, key)
    }
}

/// Whether `text` is a key label: letters, digits, `.`, `_` and `-`, not
/// empty and not starting with `.` (a label names files).
#[must_use]
pub fn is_label(text: &str) -> bool {
    !text.is_empty()
        && !text.starts_with('.')
        && text
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
}

/// The public keys a verifier trusts, each under its key id: the universe
/// entry's list, or a bootstrap pin.
#[derive(Clone, Debug, Default)]
pub struct KeyRing {
    keys: Vec<RootPublicKey>,
}

impl KeyRing {
    /// A ring of `keys`.
    ///
    /// # Errors
    ///
    /// [`Error::BadKey`] when two keys share a key id.
    pub fn new(keys: impl IntoIterator<Item = RootPublicKey>) -> Result<Self, Error> {
        let mut ring = Self::default();
        for key in keys {
            if ring.get(key.key_id).is_some() {
                return Err(Error::BadKey(format!(
                    "two keys with key id {}",
                    key.key_id
                )));
            }
            ring.keys.push(key);
        }
        Ok(ring)
    }

    /// The key under `key_id`.
    #[must_use]
    pub fn get(&self, key_id: u32) -> Option<&RootPublicKey> {
        self.keys.iter().find(|key| key.key_id == key_id)
    }

    /// Biscuit's key choice: a token without a key id, or with one the
    /// ring does not hold, is refused.
    pub(crate) fn choose(
        &self,
        key_id: Option<u32>,
    ) -> Result<PublicKey, biscuit_auth::error::Format> {
        key_id
            .and_then(|id| self.get(id))
            .map(RootPublicKey::public_key)
            .ok_or(biscuit_auth::error::Format::UnknownPublicKey)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_key_file_round_trips() {
        let key = RootKey::generate("prod", &Entropy::from_bytes([1; 32]));
        let back = RootKey::from_file(&key.to_file()).unwrap();
        assert_eq!(back.public(), key.public());
        let public = RootPublicKey::from_file(&key.public().to_file()).unwrap();
        assert_eq!(public, key.public());
        assert_eq!(
            RootPublicKey::from_file(&key.to_file()).unwrap(),
            key.public()
        );
    }

    #[test]
    fn the_same_entropy_gives_the_same_key() {
        let a = RootKey::generate("a", &Entropy::from_bytes([7; 32]));
        let b = RootKey::generate("a", &Entropy::from_bytes([7; 32]));
        let c = RootKey::generate("a", &Entropy::from_bytes([8; 32]));
        assert_eq!(a.public(), b.public());
        assert_ne!(a.public(), c.public());
    }

    #[test]
    fn a_key_is_rebuilt_from_its_parts() {
        let key = RootKey::generate("a", &Entropy::from_bytes([3; 32])).public();
        let back = RootPublicKey::from_parts(key.key_id(), key.label(), &key.key_text()).unwrap();
        assert_eq!(back, key);
        assert!(RootPublicKey::from_parts(1, "a", "ed25519/zz").is_err());
    }

    #[test]
    fn a_key_line_round_trips() {
        let key = RootKey::generate("root-1", &Entropy::from_bytes([5; 32])).public();
        assert_eq!(RootPublicKey::from_line(&key.to_line()).unwrap(), key);
        assert!(RootPublicKey::from_line("ed25519/00").is_err());
        assert!(RootPublicKey::from_line(":1:ed25519/00").is_err());
        assert!(RootPublicKey::from_line("a:x:ed25519/00").is_err());
        let pasted = format!("key={}", key.to_line());
        assert!(RootPublicKey::from_line(&pasted).is_err());
    }

    #[test]
    fn a_ring_refuses_a_duplicate_key_id() {
        let key = RootKey::generate("a", &Entropy::from_bytes([1; 32]));
        assert!(KeyRing::new([key.public(), key.public()]).is_err());
    }

    #[test]
    fn a_public_file_is_not_a_private_key() {
        let key = RootKey::generate("a", &Entropy::from_bytes([1; 32]));
        assert!(RootKey::from_file(&key.public().to_file()).is_err());
    }
}
