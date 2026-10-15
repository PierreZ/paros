//! The bench's random choices and record bytes (#517 (`parosctl bench`)).
//!
//! Every choice comes from `(seed, client index)`, and record `i` of a
//! journal is a pure function of `(seed, journal, i)`: a reader checks every
//! byte it reads. A record starts with the seed and its own position, so a
//! record another run wrote (another seed, another size) checks as well.

/// The fixed head of a record: the seed, then the position, little-endian.
pub const HEADER: usize = 16;

/// `SplitMix64`: small, fast and good enough to pick calls and fill bytes.
#[derive(Clone, Debug)]
pub struct Rng(u64);

impl Rng {
    /// The stream of `parts`, mixed in order.
    pub fn of(parts: &[u64]) -> Self {
        let mut state = 0x9e37_79b9_7f4a_7c15_u64;
        for part in parts {
            state = mix(state ^ part);
        }
        Self(state)
    }

    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        mix(self.0)
    }

    /// A value in `0..bound` (`bound > 0`).
    pub fn below(&mut self, bound: u64) -> u64 {
        assert!(bound > 0, "a draw needs a non-empty range");
        self.next_u64() % bound
    }

    pub fn fill(&mut self, bytes: &mut [u8]) {
        for chunk in bytes.chunks_mut(8) {
            let word = self.next_u64().to_le_bytes();
            chunk.copy_from_slice(&word[..chunk.len()]);
        }
    }
}

fn mix(mut z: u64) -> u64 {
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

/// A journal's key in the record stream: FNV-1a of its name, so the same
/// name gives the same bytes in every run.
pub fn journal_key(name: &str) -> u64 {
    name.bytes().fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
        (hash ^ u64::from(byte)).wrapping_mul(0x0100_0000_01b3)
    })
}

/// Record `seq` of journal `journal`, `size` bytes (`size >= HEADER`).
pub fn record(seed: u64, journal: u64, seq: u64, size: usize) -> Vec<u8> {
    assert!(size >= HEADER, "a record holds its header");
    let mut bytes = vec![0; size];
    bytes[..8].copy_from_slice(&seed.to_le_bytes());
    bytes[8..HEADER].copy_from_slice(&seq.to_le_bytes());
    Rng::of(&[seed, journal, seq]).fill(&mut bytes[HEADER..]);
    bytes
}

/// Whether `bytes`, read at position `seq` of journal `journal`, is the
/// record the bench writes there under the seed its header names.
pub fn check(journal: u64, seq: u64, bytes: &[u8]) -> bool {
    if bytes.len() < HEADER {
        return false;
    }
    let seed = u64::from_le_bytes(bytes[..8].try_into().expect("8 bytes"));
    bytes == record(seed, journal, seq, bytes.len()).as_slice()
}

#[cfg(test)]
mod tests {
    use super::{Rng, check, journal_key, record};

    #[test]
    fn a_record_checks_at_its_own_position_only() {
        let journal = journal_key("bench/bench-0");
        let bytes = record(7, journal, 42, 100);
        assert!(check(journal, 42, &bytes));
        assert!(!check(journal, 43, &bytes));
        assert!(!check(journal_key("bench/bench-1"), 42, &bytes));
        let mut flipped = bytes.clone();
        flipped[60] ^= 1;
        assert!(!check(journal, 42, &flipped));
        assert!(!check(journal, 42, &bytes[..10]));
    }

    #[test]
    fn a_stream_is_a_function_of_its_parts() {
        let a: Vec<u64> = (0..4).map(|_| Rng::of(&[1, 2]).next_u64()).collect();
        assert!(a.windows(2).all(|w| w[0] == w[1]));
        assert_ne!(Rng::of(&[1, 2]).next_u64(), Rng::of(&[1, 3]).next_u64());
    }
}
