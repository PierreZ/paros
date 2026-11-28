//! The journal rules, written again from the contract: what every party's
//! fold of the chosen log must answer at each slot.
//!
//! The core judges each `Write`, `SetLeader` and `Truncate` at apply, in
//! slot order (`paros_consensus::journal::state`), and every party runs that
//! same code. An oracle that only checks that the parties agree passes a
//! wrong rule. [`JournalSpec`] shares no code with the core's fold: it is
//! the single-writer contract of `docs/architecture/02-data-plane.md`
//! (§2.1 positions, §2.2 writes and fencing, §2.3 leadership, §2.6
//! truncation), folded over the commands that a durable quorum chose (the
//! [`Ledger`](super::ledger::Ledger) knows them from the votes, never from
//! the core). It uses the core's types only as data.
//!
//! The rules, in the order the contract states them:
//!
//! - A `Noop` moves nothing.
//! - A call under the unset uuid is of the other writer mode: `WrongMode`.
//! - `Write(uuid, seq, batch)`: below `first_seq` it is `Truncated`. Below
//!   `next_seq` it is a `Duplicate` when it is exactly the write accepted
//!   with its first record at `seq` (same uuid, same records), else
//!   `Refused`. At `next_seq` under the current uuid with records it is
//!   `Accepted` and takes `[seq, seq + n)`. Anything else is `Refused`.
//! - `SetLeader(new, old)` wins when `old` is the current leader, `new` is
//!   set and `new` is not the current leader. It answers the view after it.
//! - `Truncate(uuid, up_to)` from the current uuid raises `first_seq` to
//!   `up_to`, clamped to `next_seq`, never lowered. From anyone else it is
//!   `TruncateRefused`.
//! - Every refusal names the view it was judged against.
//! - `Roll(next)` (§4.7 of `docs/architecture/04-7-segments.md`, R2) is
//!   valid when `next` is the tail segment plus one: the tail becomes `next`
//!   from the slot after the `Roll`, and the outcome names the new tail. Any
//!   other `Roll` moves nothing and names the tail it was judged against. A
//!   `Roll` moves no position and no leader, under the same rule in both
//!   writer modes. So every party that folds a slot to these outcomes ends
//!   each segment at the same slot: one end for each segment (`SegsAgree` of
//!   `specs/tla/multi-paxos-roll/`).
//! - `Hold(segment, end, next_seq)` (§4.7, R7 and D9) is valid when it names
//!   the segment right after the held prefix (the first segment when none is
//!   held), that segment is sealed, and `end` and `next_seq` are exactly the
//!   end of that segment and the next position at its `Roll`, as this spec
//!   folded them. The held prefix then reaches `segment`, and the outcome
//!   names the `Hold`. Any other `Hold` moves nothing and names the held
//!   prefix it was judged against. From then on, a write below the held
//!   prefix's `next_seq` and at or above `first_seq` is `Held`: the replicas
//!   of its segment answer for it.
//! - `SetReplicas(segment, replicas)` (§4.7, D10) is valid when it names the
//!   tail segment and no member that an earlier valid `SetReplicas` of the
//!   tail removed. It then names the new set, and every member of the set
//!   before it (the last set a `Roll` or a `SetReplicas` named, else the
//!   bootstrap set) that the new set leaves out is removed until the next
//!   valid `Roll`. One of another segment moves nothing and names the tail;
//!   one that names a removed member moves nothing and names that member
//!   (the lowest such).

use std::collections::{BTreeMap, BTreeSet};

use paros_consensus::journal::ReplicaSet;
use paros_consensus::journal::{
    Command, Control, Entry, Hold, JournalView, LeaderUuid, Outcome, Segment, SegmentId, Seq,
    SetReplicas,
};
use paros_consensus::{NodeId, Slot};

/// The oracle: every party folds a slot to the outcome the journal rules
/// give, whatever the core computes.
pub const JOURNAL_FOLD: &str = "a party folds a slot to the outcome the journal rules give";

/// The journal as the contract defines it, folded in slot order.
#[derive(Debug, Default)]
pub struct JournalSpec {
    leader: Option<LeaderUuid>,
    next: u64,
    first: u64,
    /// The tail segment's id and its first slot.
    segment: u64,
    segment_start: u64,
    /// Each sealed segment, by id: the slot after its `Roll` and the next
    /// position there.
    ends: Vec<(u64, u64)>,
    /// The last valid `Hold`: the held prefix.
    held: Option<Hold>,
    /// The bootstrap set of the first segment, if the deployment names one.
    first_replicas: Option<BTreeSet<NodeId>>,
    /// The last set that a valid `Roll` or `SetReplicas` named.
    replicas: Option<BTreeSet<NodeId>>,
    /// The members that a valid `SetReplicas` of the tail removed.
    removed: BTreeSet<NodeId>,
    /// Every accepted write, by the position of its first record.
    accepted: BTreeMap<u64, Entry>,
    /// Per folded slot: the view before it and the outcome it gets.
    folded: Vec<(JournalView, Outcome)>,
    /// Per folded slot: the tail segment and the removed members after it.
    removed_after: Vec<(u64, BTreeSet<NodeId>)>,
}

impl JournalSpec {
    /// The spec of a deployment whose first segment is kept by `first`.
    #[must_use]
    pub fn new(first: Option<&ReplicaSet>) -> Self {
        Self {
            first_replicas: first.map(|set| set.members().iter().copied().collect()),
            ..Self::default()
        }
    }

    /// The view before `slot` and the outcome of `slot`, once the spec
    /// folded every slot up to it. `chosen(s)` is the command a durable
    /// quorum chose at `s`.
    ///
    /// # Panics
    ///
    /// If a slot at or below `slot` has no chosen command: a party cannot
    /// apply a slot above a hole of the chosen log.
    pub fn judge<'a>(
        &mut self,
        slot: Slot,
        chosen: impl Fn(Slot) -> Option<&'a Command>,
    ) -> &(JournalView, Outcome) {
        let index = usize::try_from(slot.0).expect("a small slot");
        while self.folded.len() <= index {
            let at = Slot(self.folded.len() as u64);
            let command = chosen(at).unwrap_or_else(|| {
                panic!(
                    "{JOURNAL_FOLD}: slot {} was applied, but slot {} below it was never chosen",
                    slot.0, at.0
                )
            });
            let before = self.view();
            let outcome = self.step(at, command);
            self.folded.push((before, outcome));
            self.removed_after
                .push((self.segment, self.removed.clone()));
        }
        &self.folded[index]
    }

    /// The view after `slot`, once the spec folded every slot up to it:
    /// the view before the slot after it.
    ///
    /// # Panics
    ///
    /// As [`JournalSpec::judge`].
    pub fn view_after<'a>(
        &mut self,
        slot: Slot,
        chosen: impl Fn(Slot) -> Option<&'a Command>,
    ) -> JournalView {
        let _ = self.judge(slot, chosen);
        let next = usize::try_from(slot.0 + 1).expect("a small slot");
        self.folded
            .get(next)
            .map_or_else(|| self.view(), |(before, _)| *before)
    }

    /// The slot after the `Roll` that sealed `segment`, and the next
    /// position there, once the spec folded that `Roll`.
    #[must_use]
    pub fn sealed(&self, segment: SegmentId) -> Option<(u64, u64)> {
        usize::try_from(segment.0)
            .ok()
            .and_then(|k| self.ends.get(k))
            .copied()
    }

    /// The `first_seq` of the folded prefix.
    #[must_use]
    pub fn first(&self) -> u64 {
        self.first
    }

    /// The tail segment and the members that a `SetReplicas` of it
    /// removed, after `slot`, once the spec folded it.
    #[must_use]
    pub fn removed_after(&self, slot: Slot) -> Option<&(u64, BTreeSet<NodeId>)> {
        usize::try_from(slot.0)
            .ok()
            .and_then(|k| self.removed_after.get(k))
    }

    /// How many slots the spec folded.
    #[must_use]
    pub fn folded(&self) -> u64 {
        self.folded.len() as u64
    }

    fn view(&self) -> JournalView {
        JournalView {
            leader: self.leader,
            next_seq: Seq(self.next),
            first_seq: Seq(self.first),
        }
    }

    fn step(&mut self, slot: Slot, command: &Command) -> Outcome {
        match command {
            Command::Control(Control::Noop) => Outcome::Noop,
            Command::Roll(roll) => {
                let outcome = self.roll(slot, roll.next);
                if matches!(outcome, Outcome::Rolled(_)) {
                    if let Some(set) = &roll.replicas {
                        self.replicas = Some(set.members().iter().copied().collect());
                    }
                    self.removed.clear();
                }
                outcome
            }
            Command::Hold(hold) => self.hold(*hold),
            Command::SetReplicas(set) => self.set_replicas(set),
            Command::Write(entry) => self.write(entry),
            Command::Control(Control::SetLeader { new, old }) => self.set_leader(*new, *old),
            Command::Control(Control::Truncate { leader, up_to }) => self.truncate(*leader, *up_to),
        }
    }

    fn write(&mut self, entry: &Entry) -> Outcome {
        let seq = entry.seq.0;
        let count = entry.records.len() as u64;
        if entry.leader == LeaderUuid::UNSET {
            return Outcome::WrongMode(self.view());
        }
        if seq < self.first {
            return Outcome::Truncated(self.view());
        }
        if self.held.is_some_and(|held| seq < held.next_seq.0) {
            return Outcome::Held(self.view());
        }
        if seq < self.next {
            return if self.accepted.get(&seq) == Some(entry) {
                Outcome::Duplicate {
                    seq: entry.seq,
                    count,
                }
            } else {
                Outcome::Refused(self.view())
            };
        }
        if self.leader != Some(entry.leader) || seq != self.next || count == 0 {
            return Outcome::Refused(self.view());
        }
        self.accepted.insert(seq, entry.clone());
        self.next = seq + count;
        Outcome::Accepted {
            seq: entry.seq,
            count,
        }
    }

    fn set_leader(&mut self, new: LeaderUuid, old: Option<LeaderUuid>) -> Outcome {
        if old != self.leader || new == LeaderUuid::UNSET || Some(new) == self.leader {
            return Outcome::LeaderRefused(self.view());
        }
        self.leader = Some(new);
        Outcome::Leader(self.view())
    }

    fn tail(&self) -> Segment {
        Segment {
            id: SegmentId(self.segment),
            start: Slot(self.segment_start),
        }
    }

    fn roll(&mut self, slot: Slot, next: SegmentId) -> Outcome {
        if next.0 != self.segment + 1 {
            return Outcome::RollIgnored(self.tail());
        }
        self.ends.push((slot.0 + 1, self.next));
        self.segment = next.0;
        self.segment_start = slot.0 + 1;
        Outcome::Rolled(self.tail())
    }

    /// A `SetReplicas` is valid only for the tail segment and when it names
    /// no removed member; the members it leaves out of the set before it
    /// are removed. The route of the set is judged by the oracle of the
    /// segment replicas.
    fn set_replicas(&mut self, set: &SetReplicas) -> Outcome {
        if set.segment.0 != self.segment {
            return Outcome::ReseatIgnored(self.tail());
        }
        let named: BTreeSet<NodeId> = set.replicas.members().iter().copied().collect();
        if let Some(member) = named.intersection(&self.removed).next() {
            return Outcome::ReseatRemoved(*member);
        }
        let before = self.replicas.as_ref().or(self.first_replicas.as_ref());
        let gone: Vec<NodeId> = before.map_or_else(Vec::new, |before| {
            before.difference(&named).copied().collect()
        });
        self.removed.extend(gone);
        self.replicas = Some(named);
        Outcome::Reseated(set.replicas.clone())
    }

    fn hold(&mut self, hold: Hold) -> Outcome {
        let expected = self.held.map_or(0, |held| held.segment.0 + 1);
        let sealed = usize::try_from(hold.segment.0)
            .ok()
            .and_then(|k| self.ends.get(k))
            .copied();
        if hold.segment.0 != expected || sealed != Some((hold.end.0, hold.next_seq.0)) {
            return Outcome::HoldIgnored(self.held);
        }
        self.held = Some(hold);
        Outcome::HeldTo(hold)
    }

    fn truncate(&mut self, leader: LeaderUuid, up_to: Seq) -> Outcome {
        if leader == LeaderUuid::UNSET {
            return Outcome::WrongMode(self.view());
        }
        if self.leader != Some(leader) {
            return Outcome::TruncateRefused(self.view());
        }
        self.first = self.first.max(up_to.0.min(self.next));
        Outcome::Trimmed(self.view())
    }
}

#[cfg(test)]
mod tests {
    use paros_consensus::Value;
    use paros_consensus::journal::Roll;

    use super::*;

    fn write(uuid: u128, seq: u64, bytes: &[u8]) -> Command {
        Command::Write(Entry {
            leader: LeaderUuid(uuid),
            seq: Seq(seq),
            records: bytes.iter().map(|b| Value::from(vec![*b])).collect(),
        })
    }

    fn claim(new: u128, old: Option<u128>) -> Command {
        Command::Control(Control::SetLeader {
            new: LeaderUuid(new),
            old: old.map(LeaderUuid),
        })
    }

    fn truncate(uuid: u128, up_to: u64) -> Command {
        Command::Control(Control::Truncate {
            leader: LeaderUuid(uuid),
            up_to: Seq(up_to),
        })
    }

    fn roll(next: u64) -> Command {
        Command::Roll(Roll {
            next: SegmentId(next),
            replicas: None,
        })
    }

    fn hold(segment: u64, end: u64, next_seq: u64) -> Command {
        Command::Hold(Hold {
            segment: SegmentId(segment),
            end: Slot(end),
            next_seq: Seq(next_seq),
        })
    }

    #[test]
    fn a_hold_extends_the_held_prefix_and_a_retry_below_it_is_held() {
        let a = write(1, 0, b"a");
        let outcomes = fold(&[
            claim(1, None),
            a.clone(),
            roll(1),
            hold(0, 2, 1),
            hold(0, 3, 1),
            hold(1, 3, 1),
            hold(0, 3, 1),
            a,
            write(1, 1, b"b"),
        ]);
        let held = Hold {
            segment: SegmentId(0),
            end: Slot(3),
            next_seq: Seq(1),
        };
        assert_eq!(outcomes[3], Outcome::HoldIgnored(None), "the wrong end");
        assert_eq!(outcomes[4], Outcome::HeldTo(held));
        assert_eq!(outcomes[5], Outcome::HoldIgnored(Some(held)), "not sealed");
        assert_eq!(
            outcomes[6],
            Outcome::HoldIgnored(Some(held)),
            "held already"
        );
        assert_eq!(outcomes[7], Outcome::Held(view(Some(1), 1, 0)));
        assert!(matches!(outcomes[8], Outcome::Accepted { .. }));
    }

    #[test]
    fn a_roll_opens_the_next_segment_after_its_slot_and_any_other_moves_nothing() {
        let outcomes = fold(&[roll(2), claim(1, None), roll(1), roll(1), write(1, 0, b"a")]);
        let first = Segment::default();
        let second = Segment {
            id: SegmentId(1),
            start: Slot(3),
        };
        assert_eq!(outcomes[0], Outcome::RollIgnored(first));
        assert_eq!(outcomes[2], Outcome::Rolled(second));
        assert_eq!(outcomes[3], Outcome::RollIgnored(second));
        assert!(matches!(outcomes[4], Outcome::Accepted { .. }));
    }

    /// The outcomes of `log`, folded from slot zero.
    fn fold(log: &[Command]) -> Vec<Outcome> {
        let mut spec = JournalSpec::default();
        let last = Slot(log.len() as u64 - 1);
        spec.judge(last, |s| log.get(usize::try_from(s.0).ok()?));
        spec.folded.into_iter().map(|(_, o)| o).collect()
    }

    fn view(leader: Option<u128>, next: u64, first: u64) -> JournalView {
        JournalView {
            leader: leader.map(LeaderUuid),
            next_seq: Seq(next),
            first_seq: Seq(first),
        }
    }

    #[test]
    fn a_write_needs_the_current_uuid_and_the_next_position() {
        let outcomes = fold(&[
            write(1, 0, b"a"),
            claim(1, None),
            write(1, 1, b"a"),
            write(1, 0, b"ab"),
            write(2, 2, b"c"),
            write(1, 2, b""),
        ]);
        assert_eq!(outcomes[0], Outcome::Refused(view(None, 0, 0)));
        assert_eq!(outcomes[1], Outcome::Leader(view(Some(1), 0, 0)));
        assert_eq!(outcomes[2], Outcome::Refused(view(Some(1), 0, 0)), "ahead");
        assert_eq!(
            outcomes[3],
            Outcome::Accepted {
                seq: Seq(0),
                count: 2
            }
        );
        assert_eq!(outcomes[4], Outcome::Refused(view(Some(1), 2, 0)), "fenced");
        assert_eq!(outcomes[5], Outcome::Refused(view(Some(1), 2, 0)), "empty");
    }

    #[test]
    fn a_retry_is_a_duplicate_only_when_identical() {
        let outcomes = fold(&[
            claim(1, None),
            write(1, 0, b"ab"),
            write(1, 0, b"ab"),
            write(1, 0, b"ax"),
            write(1, 1, b"b"),
            claim(2, Some(1)),
            write(1, 0, b"ab"),
        ]);
        let duplicate = Outcome::Duplicate {
            seq: Seq(0),
            count: 2,
        };
        assert_eq!(outcomes[2], duplicate);
        assert_eq!(outcomes[3], Outcome::Refused(view(Some(1), 2, 0)));
        assert_eq!(
            outcomes[4],
            Outcome::Refused(view(Some(1), 2, 0)),
            "mid-batch"
        );
        assert_eq!(outcomes[6], duplicate, "the log answers, not the fence");
    }

    #[test]
    fn set_leader_is_a_compare_and_set() {
        let outcomes = fold(&[
            claim(1, Some(9)),
            claim(1, None),
            claim(1, Some(1)),
            claim(0, Some(1)),
            claim(2, Some(1)),
            claim(1, Some(2)),
        ]);
        assert_eq!(outcomes[0], Outcome::LeaderRefused(view(None, 0, 0)));
        assert_eq!(outcomes[1], Outcome::Leader(view(Some(1), 0, 0)));
        assert_eq!(outcomes[2], Outcome::LeaderRefused(view(Some(1), 0, 0)));
        assert_eq!(outcomes[3], Outcome::LeaderRefused(view(Some(1), 0, 0)));
        assert_eq!(outcomes[4], Outcome::Leader(view(Some(2), 0, 0)));
        assert_eq!(
            outcomes[5],
            Outcome::Leader(view(Some(1), 0, 0)),
            "reinstated"
        );
    }

    #[test]
    fn truncation_is_fenced_clamped_and_monotone() {
        let outcomes = fold(&[
            claim(1, None),
            write(1, 0, b"abc"),
            truncate(2, 1),
            truncate(1, 9),
            truncate(1, 1),
            write(1, 1, b"b"),
            write(0, 3, b"d"),
            truncate(0, 3),
        ]);
        assert_eq!(outcomes[2], Outcome::TruncateRefused(view(Some(1), 3, 0)));
        assert_eq!(
            outcomes[3],
            Outcome::Trimmed(view(Some(1), 3, 3)),
            "clamped"
        );
        assert_eq!(
            outcomes[4],
            Outcome::Trimmed(view(Some(1), 3, 3)),
            "never lowered"
        );
        assert_eq!(outcomes[5], Outcome::Truncated(view(Some(1), 3, 3)));
        assert_eq!(outcomes[6], Outcome::WrongMode(view(Some(1), 3, 3)));
        assert_eq!(outcomes[7], Outcome::WrongMode(view(Some(1), 3, 3)));
    }

    #[test]
    fn a_hole_below_an_applied_slot_is_red() {
        let log = [claim(1, None)];
        let error = std::panic::catch_unwind(|| {
            let mut spec = JournalSpec::default();
            spec.judge(Slot(2), |s| log.get(usize::try_from(s.0).ok()?));
        })
        .expect_err("a hole is red");
        let message = error.downcast_ref::<String>().cloned().unwrap_or_default();
        assert!(message.contains(JOURNAL_FOLD), "{message}");
    }
}
