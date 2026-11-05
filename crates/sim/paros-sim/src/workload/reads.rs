//! Judging a journal `Read` (#204), and the run's shared tail bookkeeping.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use moonpool_sim::assert_always;
use paros::client::ReadOutcome;
use paros::core::journal::JournalIdentifier;

use super::write::WrittenCommand;
use crate::audit::AuditWorld;
use crate::workload::chain::user_command_hash;

/// Judge one journal `Read` answer (#204) against the audit and this
/// client's own written writes: every record is the one accepted at its
/// position, this client's written records inside the page are in it (a
/// page never hides a record), the state it was served from covers every
/// write this client saw written before the read, and a truncated answer
/// names a floor above where it started.
pub(super) fn judge_read(
    audit: &AuditWorld,
    from: u64,
    answer: &ReadOutcome,
    written: &[WrittenCommand],
) {
    let (records, state) = match answer {
        ReadOutcome::Page { records, state, .. } => (records, state),
        ReadOutcome::Truncated { state } => {
            assert_always!(
                from < state.first_seq.0,
                "chain: a trimmed read carries nothing and names a point above its start",
                { "from" => from, "first_seq" => state.first_seq.0 }
            );
            return;
        }
        _ => return,
    };
    for (position, record) in (from..).zip(records) {
        if let Some(accepted) = audit.record_at(position) {
            assert_always!(
                accepted == user_command_hash(record),
                "chain: a read entry is the value decided at its slot",
                { "position" => position }
            );
        }
    }
    let next = from + records.len() as u64;
    for own in written {
        for (position, record) in (own.seq..own.seq + own.count).zip(&own.entry.records) {
            if position >= from && position < next {
                let offset = usize::try_from(position - from).unwrap_or(usize::MAX);
                assert_always!(
                    records.get(offset) == Some(&record.0),
                    "chain: a read covering an acked append returns it",
                    { "position" => position, "from" => from, "next" => next }
                );
            }
        }
        // A read is linearizable (#204: every `Read` is a quorum read): a
        // write seen written before the read began is below the state's
        // tail.
        assert_always!(
            own.seq + own.count <= state.next_seq.0,
            "chain: a read's state covers the client's written writes",
            { "end" => own.seq + own.count, "next_seq" => state.next_seq.0 }
        );
    }
}

const TAIL_KEY: &str = "paros-chain-tail";

/// How long the cluster must stay converged and unchanged before the run is
/// over. One observation of "every live node equal" is not the end of the
/// tail: the leader can still decide a follow-up control command (a `Snap`
/// marker's `Truncate`, a gap fill) a few beats later, and the audit's final
/// claim would then catch the followers one slot behind. A second's worth of
/// ticks covers those follow-ups. **Never buggified**: this is the definition
/// of the tail's end, not a shape the run takes.
pub(super) const SETTLE: Duration = Duration::from_secs(1);

/// How long a tailing reader's long-poll asks the server to wait, on its
/// BUGGIFY location (#529 (sets oracles after PR #515)): inside the
/// server's default `max_wait_ms` (400 ms), long enough for another
/// client's write to land while it waits. The knob it overrides,
/// `read_wait_ms`, still draws its own range on every other read.
pub(super) const LONG_POLL_MS: u64 = 300;

/// The run's shared tail bookkeeping, one per iteration: how many clients the
/// run has and how many have finished proposing. Convergence is only called
/// once *every* client is quiet — the first client to see it ends the run, and
/// its siblings, cut short by that shutdown, defer to the audit's final claim.
#[derive(Default)]
pub(super) struct Tail {
    pub(super) registered: usize,
    pub(super) done_proposing: usize,
    /// The first moment every registered client was done proposing (#177).
    /// The convergence budget is measured from here, not from a client's own
    /// tail: a client whose program ended early would otherwise spend its
    /// whole budget waiting on a sibling still in its operation program.
    pub(super) all_quiet_at: Option<Duration>,
    /// The journals some client saw converged (#188): the run ends only once
    /// every journal a client appends to is, or a sibling journal still
    /// settling would be cut short.
    pub(super) converged: BTreeSet<JournalIdentifier>,
    /// How many clients finished their fleet operations in the recovery
    /// tail (#247): the last one judges the control plane's final folds.
    pub(super) fleet_settled: usize,
    /// The applied count each journal's tail must move past (#357), taken
    /// once per journal by its first client to reach the tail. A client that
    /// took its own later could find the owner's recovery batch already
    /// applied, and wait on a write nobody makes.
    pub(super) snapshots: BTreeMap<JournalIdentifier, u64>,
}

pub(super) fn tail(state: &moonpool_sim::StateHandle) -> Arc<Mutex<Tail>> {
    crate::state::published(state, TAIL_KEY, Tail::default)
}

#[cfg(test)]
mod tests {
    use paros::client::ReadOutcome;
    use paros::core::Value;
    use paros::core::journal::{Entry, JournalView, LeaderUuid, Seq};

    use super::judge_read;
    use crate::audit::AuditWorld;
    use crate::audit::testing::red;
    use crate::workload::write::WrittenCommand;

    fn entry(seq: u64, records: &[&[u8]]) -> Entry {
        Entry {
            leader: LeaderUuid(1),
            seq: Seq(seq),
            records: records.iter().map(|r| Value(r.to_vec())).collect(),
        }
    }

    fn written(seq: u64, records: &[&[u8]]) -> WrittenCommand {
        WrittenCommand {
            entry: entry(seq, records),
            seq,
            count: records.len() as u64,
            cmd_hash: 0,
            node: 0,
        }
    }

    fn page(from: u64, records: &[&[u8]], next: u64) -> ReadOutcome {
        ReadOutcome::Page {
            from,
            records: records.iter().map(|r| r.to_vec()).collect(),
            state: JournalView {
                leader: Some(LeaderUuid(1)),
                next_seq: Seq(next),
                first_seq: Seq(0),
            },
        }
    }

    /// A world that accepted `a`, `b` at 0 and `c` at 2.
    fn world() -> AuditWorld {
        let world = AuditWorld::default();
        world.accept_for_test(1, entry(0, &[b"a", b"b"]));
        world.accept_for_test(2, entry(2, &[b"c"]));
        world
    }

    #[test]
    fn an_honest_page_passes() {
        let world = world();
        let own = [written(0, &[b"a", b"b"])];
        assert!(!red(|| judge_read(
            &world,
            0,
            &page(0, &[b"a", b"b", b"c"], 3),
            &own
        )));
        assert!(!red(|| judge_read(&world, 1, &page(1, &[b"b"], 3), &own)));
        assert!(!red(|| judge_read(&world, 0, &ReadOutcome::Unserved, &own)));
    }

    #[test]
    fn a_page_entry_other_than_the_decided_record_is_red() {
        let world = world();
        assert!(red(|| judge_read(
            &world,
            0,
            &page(0, &[b"a", b"x"], 3),
            &[]
        )));
    }

    #[test]
    fn a_page_that_hides_an_acked_record_is_red() {
        let world = AuditWorld::default();
        let own = [written(0, &[b"a", b"b"])];
        assert!(red(|| judge_read(
            &world,
            0,
            &page(0, &[b"a", b"q"], 3),
            &own
        )));
        assert!(
            !red(|| judge_read(&world, 0, &page(0, &[b"a"], 3), &own)),
            "a short page judges only the positions it covers"
        );
    }

    #[test]
    fn a_state_behind_the_clients_written_writes_is_red() {
        let world = world();
        let own = [written(2, &[b"c"])];
        assert!(red(|| judge_read(
            &world,
            0,
            &page(0, &[b"a", b"b"], 2),
            &own
        )));
    }

    #[test]
    fn a_trimmed_read_names_a_point_above_its_start() {
        let world = world();
        let truncated = |first| ReadOutcome::Truncated {
            state: JournalView {
                leader: None,
                next_seq: Seq(3),
                first_seq: Seq(first),
            },
        };
        assert!(!red(|| judge_read(&world, 0, &truncated(2), &[])));
        assert!(red(|| judge_read(&world, 2, &truncated(2), &[])));
    }
}
