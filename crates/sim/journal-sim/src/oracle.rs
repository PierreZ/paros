//! The **truth**: the journal fold of the whole log, written again from
//! `docs/journal/02-data-plane.md` (§J2.2, §J2.4.1, §J2.6), with no
//! segment.
//!
//! It shares no code with `paros_journal::fold` (#909 (oracles that do not
//! judge)): it uses the journal's types as data only. It folds every chosen
//! command from slot 0 and keeps every record, so it never answers `Below`.
//! The oracles of the walk compare the code's answers and states with it,
//! as `AckTrue`, `OneFold` and `ResolveTrue` of `specs/tla/journal-state/`
//! compare a replica with the fold of the whole log.

use std::collections::BTreeMap;

use paros_journal::{Command, Fence, Leader, Write};

/// The verdict of the truth on one command, by name. The names are the
/// answers of the code ([`label`]), so the oracles compare two names.
pub type Verdict = &'static str;

/// The client part of one write: its uuid and its records as the client
/// wrote them (headers and payload, no leader time).
type Identity = (u128, Vec<(Vec<(Vec<u8>, Vec<u8>)>, Vec<u8>)>);

/// `command` as its client wrote it: with no leader time, which the
/// leader stamps at propose and which is not part of a write (§J2.8).
#[must_use]
pub fn as_written(command: &Command) -> Command {
    let mut command = command.clone();
    if let Command::Write(write) = &mut command {
        for record in &mut write.records {
            record.headers.leader_time = None;
        }
    }
    command
}

fn identity(uuid: u128, write: &Write) -> Identity {
    (
        uuid,
        write
            .records
            .iter()
            .map(|record| {
                (
                    record
                        .headers
                        .client
                        .iter()
                        .map(|h| (h.key.clone(), h.value.clone()))
                        .collect(),
                    record.payload.clone(),
                )
            })
            .collect(),
    )
}

/// The fold of the whole log.
#[derive(Clone, Debug)]
pub struct Truth {
    /// The leader value.
    pub leader: Leader,
    /// The next position.
    pub next: u64,
    /// The trim floor.
    pub first: u64,
    /// Every fenced write accepted, by the position of its first record.
    written: BTreeMap<u64, Identity>,
}

impl Truth {
    /// The truth of a fresh journal whose leader value is `leader`.
    #[must_use]
    pub fn new(leader: Leader) -> Self {
        Self {
            leader,
            next: 0,
            first: 0,
            written: BTreeMap::new(),
        }
    }

    /// Fold `command`, and give the verdict.
    pub fn apply(&mut self, command: &Command) -> Verdict {
        match command {
            Command::Noop => "noop",
            Command::Upgrade { .. } => "reserved",
            Command::Write(write) if write.records.is_empty() => "empty",
            Command::Write(write) => match write.fence {
                Fence::Open => {
                    if matches!(self.leader, Leader::Multi(_)) {
                        self.next += write.records.len() as u64;
                        "ok"
                    } else {
                        "wrong"
                    }
                }
                Fence::Leader { uuid, expected_seq } => {
                    let at = expected_seq.0;
                    if at < self.next {
                        if at < self.first {
                            "trunc"
                        } else if self.written.get(&at) == Some(&identity(uuid.0, write)) {
                            "dup"
                        } else {
                            "taken"
                        }
                    } else if self.leader != Leader::Uuid(uuid) {
                        "fenced"
                    } else if at > self.next {
                        "early"
                    } else {
                        self.written.insert(at, identity(uuid.0, write));
                        self.next += write.records.len() as u64;
                        "ok"
                    }
                }
            },
            Command::Truncate { fence, up_to } => {
                let allowed = match (fence, self.leader) {
                    (Some(uuid), Leader::Uuid(leader)) if *uuid == leader => true,
                    (None, Leader::Multi(_)) => true,
                    (Some(_), Leader::Uuid(_) | Leader::Unset) => return "fenced",
                    _ => return "wrong",
                };
                if allowed {
                    self.first = self.first.max(up_to.0.min(self.next));
                }
                "ok"
            }
            Command::SetLeader { new, old } => {
                // §J2.4.1: `new` is a uuid leader or a multi-writer mode,
                // each with a set id; the compare is on the whole value, tag
                // included.
                let installed = match *new {
                    Leader::Uuid(id) | Leader::Multi(id) if id.0 != 0 => *new,
                    Leader::Uuid(_) | Leader::Multi(_) | Leader::Unset => return "refused",
                };
                if *old == self.leader && installed != self.leader {
                    self.leader = installed;
                    "ok"
                } else {
                    "refused"
                }
            }
        }
    }
}

/// The name of an answer of the code, as the truth names its verdicts.
#[must_use]
pub fn label(answer: &paros_journal::Answer) -> Verdict {
    use paros_journal::Answer;
    match answer {
        Answer::Written { .. } | Answer::LeaderSet(_) | Answer::Trimmed(_) => "ok",
        Answer::Duplicate { .. } => "dup",
        Answer::Taken(_) => "taken",
        Answer::Fenced(_) => "fenced",
        Answer::Early(_) => "early",
        Answer::Truncated(_) => "trunc",
        Answer::Below(_) => "below",
        Answer::WrongMode(_) => "wrong",
        Answer::Empty(_) => "empty",
        Answer::LeaderRefused(_) => "refused",
        Answer::Reserved(_) => "reserved",
        Answer::Noop => "noop",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use paros_journal::{LeaderUuid, Record, Seq};

    fn write(uuid: u128, at: u64, byte: u8) -> Command {
        Command::Write(Write {
            fence: Fence::Leader {
                uuid: LeaderUuid(uuid),
                expected_seq: Seq(at),
            },
            records: vec![Record::new(vec![byte])],
        })
    }

    #[test]
    fn the_truth_judges_the_position_before_the_fence() {
        let mut truth = Truth::new(Leader::Unset);
        assert_eq!(truth.apply(&write(1, 0, 1)), "fenced");
        assert_eq!(
            truth.apply(&Command::SetLeader {
                new: Leader::uuid(LeaderUuid(1)),
                old: Leader::Unset
            }),
            "ok"
        );
        assert_eq!(truth.apply(&write(1, 0, 1)), "ok");
        assert_eq!(
            truth.apply(&Command::SetLeader {
                new: Leader::multi(LeaderUuid(2)),
                old: Leader::Uuid(LeaderUuid(1))
            }),
            "ok"
        );
        assert_eq!(truth.apply(&write(1, 0, 1)), "dup");
        assert_eq!(truth.apply(&write(1, 0, 2)), "taken");
        assert_eq!(truth.apply(&write(1, 1, 2)), "fenced");
    }

    #[test]
    fn the_truth_refuses_a_late_copy_after_a_later_mode() {
        let m1 = Leader::multi(LeaderUuid(1));
        let claim = Command::SetLeader {
            new: Leader::uuid(LeaderUuid(2)),
            old: m1,
        };
        let mut truth = Truth::new(m1);
        assert_eq!(truth.apply(&claim), "ok");
        assert_eq!(
            truth.apply(&Command::SetLeader {
                new: Leader::multi(LeaderUuid(3)),
                old: Leader::uuid(LeaderUuid(2)),
            }),
            "ok"
        );
        assert_eq!(truth.apply(&claim), "refused");
        assert_eq!(truth.apply(&write(2, 0, 1)), "fenced");
    }
}
