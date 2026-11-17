//! The property test of the wire messages (#1078 (renumber the proto
//! fields), `docs/architecture/06-verification.md`, "The checks beside the
//! simulation"). For each message of the crate (the list `build.rs`
//! writes from the protos, so a new message is in it with no edit here):
//!
//! - a value drawn from bytes, encoded and then decoded, is the same value;
//! - a decode of any bytes is a value or a refusal, never a panic.
//!
//! The values come from `arbitrary`, which a test build derives on each
//! generated type. The bytes come from a fixed xorshift stream: a seed here
//! names a schedule of draws, not a witness. A case that this test finds
//! becomes a named unit test with its shrunk input.

use arbitrary::{Arbitrary, Unstructured};
use prost::Message as ProstMessage;

/// Draws for each message: one buffer each, of growing length.
const DRAWS: usize = 96;

/// The longest buffer a draw takes.
const MAX_LEN: usize = 512;

const _: () = assert!(DRAWS > 0 && MAX_LEN > 0, "the test draws something");

/// A xorshift64 stream: the bytes of each draw, with no RNG dependency.
struct Bytes(u64);

impl Bytes {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    fn fill(&mut self, len: usize) -> Vec<u8> {
        let mut out = Vec::with_capacity(len + 8);
        while out.len() < len {
            out.extend_from_slice(&self.next().to_le_bytes());
        }
        out.truncate(len);
        assert!(out.len() == len, "a draw has its length");
        out
    }
}

/// Run both properties over `T`. Returns how many drawn values made the
/// round trip, so the caller can check the test judged something.
fn check<T>(name: &str) -> usize
where
    T: ProstMessage + Default + PartialEq + for<'a> Arbitrary<'a>,
{
    let mut bytes = Bytes(0x9E37_79B9_7F4A_7C15 ^ name.len() as u64);
    let mut judged = 0;
    for draw in 0..DRAWS {
        let raw = bytes.fill(draw * MAX_LEN / DRAWS);
        // Any bytes: a value or a refusal, never a panic.
        let _ = T::decode(raw.as_slice());
        // A drawn value crosses the wire unchanged.
        let Ok(value) = T::arbitrary(&mut Unstructured::new(&raw)) else {
            continue;
        };
        let encoded = value.encode_to_vec();
        let decoded = T::decode(encoded.as_slice())
            .unwrap_or_else(|error| panic!("{name}: an encoded value decodes: {error}"));
        // A NaN in a `double` field is not equal to itself: such a value is
        // judged on its bytes alone, by the check below.
        #[allow(clippy::eq_op)]
        let comparable = value == value;
        assert!(
            !comparable || decoded == value,
            "{name}: a value round-trips, draw {draw}"
        );
        assert_eq!(
            decoded.encode_to_vec(),
            encoded,
            "{name}: a decoded value encodes to the same bytes"
        );
        judged += 1;
    }
    judged
}

#[test]
fn every_message_round_trips_and_no_bytes_panic_its_decode() {
    let mut messages = 0;
    let mut judged = 0;
    macro_rules! message {
        ($path:path) => {{
            messages += 1;
            judged += check::<$path>(stringify!($path));
        }};
    }
    include!(concat!(env!("OUT_DIR"), "/wire_messages.rs"));
    assert!(messages > 100, "the list holds every message of the crate");
    assert!(
        judged >= messages,
        "the draws judged at least one value per message"
    );
}
