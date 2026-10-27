//! The bridge from a provider's random source to a cryptographic RNG.

use rand_chacha::ChaCha20Rng;
use rand_chacha::rand_core::SeedableRng;

/// 32 bytes the caller draws from its provider's random source, the seed
/// of a `ChaCha20Rng` for one key.
///
/// moonpool's `RandomProvider` is not a `CryptoRng`, so the crate never
/// takes it directly. In production the bytes come from the OS-seeded
/// thread RNG; in the simulation from the run's seeded RNG. The same code
/// runs in both. A real root key from fixed bytes is a security bug.
#[derive(Clone)]
pub struct Entropy([u8; 32]);

impl Entropy {
    /// Entropy from 32 drawn bytes.
    #[must_use]
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Entropy from four drawn `u64`s, the shape a provider draws.
    #[must_use]
    pub fn from_words(words: [u64; 4]) -> Self {
        let mut bytes = [0u8; 32];
        for (chunk, word) in bytes.as_chunks_mut::<8>().0.iter_mut().zip(words) {
            chunk.copy_from_slice(&word.to_le_bytes());
        }
        Self(bytes)
    }

    /// The cryptographic RNG this entropy seeds.
    pub(crate) fn rng(&self) -> ChaCha20Rng {
        ChaCha20Rng::from_seed(self.0)
    }
}

impl std::fmt::Debug for Entropy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Entropy(..)")
    }
}
