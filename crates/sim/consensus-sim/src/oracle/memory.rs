//! The memory oracle: the retained bytes a party reports are the bytes of
//! the client records it holds (#1153 (retained log memory)).
//!
//! A driver bounds the memory of the retained log with
//! `ColocatedNode::retained_bytes` and `ReplicaNode::retained_bytes`. The
//! oracle sums the records' payload bytes by hand, from the records the
//! party exposes, and shares no code with the core's sum (`Weigh`). A
//! planted mutant of the driver cannot make the two differ, so the controls
//! of this oracle are its unit tests below and the mutants of the core,
//! which the `mutants` job runs against these walks.

use paros_consensus::journal::Command;

/// The oracle's message.
pub const RETAINED_BYTES: &str = "a party's retained bytes are the bytes of the records it holds";

/// Check that `reported` is the sum of the client records' bytes of
/// `held`, the commands `who` holds.
///
/// # Panics
///
/// If the two differ.
pub fn retained<'a>(who: &str, reported: u64, held: impl IntoIterator<Item = &'a Command>) {
    let mut summed = 0_u64;
    for command in held {
        if let Command::Write(entry) = command {
            for record in &entry.records {
                summed += record.as_bytes().len() as u64;
            }
        }
    }
    assert!(
        reported == summed,
        "{RETAINED_BYTES}: {who} reports {reported} bytes and holds {summed}"
    );
}

#[cfg(test)]
mod tests {
    use paros_consensus::Value;
    use paros_consensus::journal::{Command, Control, Entry, LeaderUuid, Seq};

    use super::{RETAINED_BYTES, retained};

    fn write(bytes: &[u8]) -> Command {
        Command::Write(Entry {
            leader: LeaderUuid(1),
            seq: Seq(0),
            records: vec![Value::from(bytes)],
        })
    }

    #[test]
    fn the_sum_of_the_records_passes_and_a_drift_is_red() {
        let held = [write(b"abc"), Command::Control(Control::Noop), write(b"de")];
        retained("node 0", 5, &held);
        for drifted in [4, 6, 0] {
            let error = std::panic::catch_unwind(|| retained("node 0", drifted, &held))
                .expect_err("the oracle fires");
            let message = error.downcast_ref::<String>().cloned().unwrap_or_default();
            assert!(message.contains(RETAINED_BYTES), "{message}");
        }
    }
}
