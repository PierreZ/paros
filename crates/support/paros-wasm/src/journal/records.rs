//! The record lab of the site's `/journal/segments/` page (#1364 (register
//! and segment record lab)): a strip with the record of each segment, and
//! the moves of the records as buttons.
//!
//! The world is one value of the journal's matchmaker register. Each move is
//! one compare-and-set of [`RegisterValue::apply`]: its guard runs on the
//! value, and a refused move shows its [`ChangeRefusal`] (for a guard of one
//! record, the [`RecordRefusal`] of `SegmentRecord`). The matchmakers that
//! hold the value are on the register lab ([`super::register`]); here each
//! move is a write seen done.
//!
//! The world picks the numbers a real run would bring: every segment takes
//! four slots before its seal, one record per slot, so the seal end is the
//! start plus four and the boundary state's `next_seq` is that end. The
//! controller holds term 1. `truncate` raises the trim floor of the fold to
//! the highest `next_seq` that a boundary state names. `put_in_force` is no
//! move of its own: the register runs it inside a reshape (the first
//! segment) and inside a seal (the successor), in the same write.

use paros_journal::register::{Change, RegisterValue};
use paros_journal::{
    BoundaryState, Grid, Leader, NodeId, OpeningCall, RELEASE, RecordRefusal, SegmentId,
    SegmentRecord, SegmentState, Seq, Slot,
};
use serde::Serialize;

use super::keyed::{Action, Exercise, World};
use super::register::refusal_text;

/// The slots each segment takes before its seal.
const SLOTS: u64 = 4;

/// The controller term the lab holds.
const TERM: u64 = 1;

/// A refusal of a record's guard in words.
#[must_use]
pub fn record_text(refusal: &RecordRefusal) -> String {
    match refusal {
        RecordRefusal::State(state) => format!("State: the record is {}", state_text(*state)),
        RecordRefusal::Replicas => {
            "Replicas: the replica set is empty or names a node twice".into()
        }
        RecordRefusal::Expected => "Expected: the expected segment is not below the new one".into(),
        RecordRefusal::EndBelowStart { start, end } => {
            format!("EndBelowStart: end {} is below start {}", end.0, start.0)
        }
        RecordRefusal::BoundaryWritten => {
            "BoundaryWritten: the boundary state is written once".into()
        }
        RecordRefusal::BoundaryInvalid => {
            "BoundaryInvalid: the boundary state is not well formed".into()
        }
        RecordRefusal::NoBoundary => "NoBoundary: the boundary state is not written yet".into(),
        RecordRefusal::FewHolders { holders, needed } => {
            format!("FewHolders: {holders} replica holds the segment, the retire needs {needed}")
        }
        RecordRefusal::Damage => "Damage: a found damage is not repaired".into(),
        RecordRefusal::FloorBelowEnd { first_seq, last } => format!(
            "FloorBelowEnd: first_seq {} is below the segment's next_seq {}",
            first_seq.0, last.0
        ),
    }
}

/// A segment state in words.
#[must_use]
pub fn state_text(state: SegmentState) -> &'static str {
    match state {
        SegmentState::Pending => "pending",
        SegmentState::InForce => "in force",
        SegmentState::Sealed => "sealed",
        SegmentState::Retired => "retired",
        SegmentState::Offloaded => "offloaded",
        SegmentState::Trimmed => "trimmed",
    }
}

/// One record, as the page draws it.
#[derive(Clone, Debug, Serialize)]
pub struct RecordView {
    /// The segment's number.
    pub n: u64,
    /// Its state, in words.
    pub state: String,
    /// `slots 0 – 3`, `from slot 4`, or empty while pending.
    pub slots: String,
    /// The boundary state, or empty.
    pub boundary: String,
}

/// One move that went through, in words.
#[derive(Clone, Debug, Serialize)]
pub struct EventView {
    /// The move.
    pub sent: String,
    /// What the write changed.
    pub detail: String,
}

/// The record world, as the page draws it.
#[derive(Clone, Debug, Serialize)]
pub struct RecordsView {
    /// The controller term.
    pub term: u64,
    /// The trim floor of the fold.
    pub first_seq: u64,
    /// Each record, oldest first.
    pub records: Vec<RecordView>,
    /// Every move that went through, oldest first.
    pub history: Vec<EventView>,
}

/// One register value, changed by hand.
#[derive(Clone, Debug)]
pub struct Records {
    value: RegisterValue,
    floor: Seq,
    events: Vec<EventView>,
}

impl Records {
    /// The state of segment `n`.
    #[must_use]
    pub fn state(&self, n: u64) -> Option<SegmentState> {
        self.value.segment(SegmentId(n)).map(SegmentRecord::state)
    }

    /// How many segments the register holds.
    #[must_use]
    pub fn segments(&self) -> usize {
        self.value.segments().len()
    }

    fn opening(&self) -> OpeningCall {
        let nodes = |ids: &[u64]| ids.iter().copied().map(NodeId).collect::<Vec<_>>();
        OpeningCall {
            term: TERM,
            expected: self.value.newest().map(SegmentRecord::id),
            grid: Grid::new(1, vec![nodes(&[11, 12, 13])]).expect("a v1 grid"),
            replicas: nodes(&[21, 22, 23]),
        }
    }

    /// The seal end of `record`: four slots after its start.
    fn end(record: &SegmentRecord) -> Slot {
        Slot(record.start().map_or(0, |s| s.0) + SLOTS)
    }

    /// The boundary state of `record`: one record per slot up to its end.
    fn boundary(&self, record: &SegmentRecord) -> BoundaryState {
        let next_seq = Seq(record.end().unwrap_or_else(|| Self::end(record)).0);
        BoundaryState {
            next_seq,
            first_seq: self.floor.min(next_seq),
            leader: Leader::Unset,
        }
    }

    /// The highest `next_seq` that a boundary state names.
    fn top(&self) -> Option<Seq> {
        self.value
            .segments()
            .values()
            .filter_map(|r| r.boundary().map(|b| b.next_seq))
            .max()
    }

    fn change(&self, key: &str) -> Option<(Change, String)> {
        if key == "reshape" {
            let next = self.value.newest().map_or(1, |r| r.id().0 + 1);
            return Some((
                Change::Reshape {
                    opening: self.opening(),
                    release: RELEASE,
                },
                format!("reshape: open segment {next}"),
            ));
        }
        let mut parts = key.split(':');
        let verb = parts.next()?;
        let n: u64 = parts.next()?.parse().ok()?;
        let segment = SegmentId(n);
        let record = self.value.segment(segment)?;
        let change = match (verb, parts.next()) {
            ("seal", None) => (
                Change::Seal {
                    segment,
                    end: Self::end(record),
                },
                format!("seal segment {n} at end {}", Self::end(record).0),
            ),
            ("boundary", None) => (
                Change::Boundary {
                    segment,
                    state: self.boundary(record),
                },
                format!("write the boundary state of segment {n}"),
            ),
            ("retire", Some(holders)) => {
                let holders: usize = holders.parse().ok()?;
                (
                    Change::Retire {
                        segment,
                        holders,
                        unrepaired_damage: false,
                    },
                    format!("retire segment {n} with {holders} holder(s)"),
                )
            }
            ("trim", None) => (
                Change::Trim {
                    segment,
                    first_seq: self.floor,
                },
                format!("trim segment {n} at first_seq {}", self.floor.0),
            ),
            _ => return None,
        };
        Some(change)
    }

    fn record_view(record: &SegmentRecord) -> RecordView {
        let slots = match (record.start(), record.end()) {
            (Some(start), Some(end)) if end > start => {
                format!("slots {} – {}", start.0, end.0 - 1)
            }
            (Some(_), Some(_)) => "no slot".to_string(),
            (Some(start), None) => format!("from slot {}", start.0),
            (None, _) => String::new(),
        };
        RecordView {
            n: record.id().0,
            state: state_text(record.state()).to_string(),
            slots,
            boundary: record.boundary().map_or(String::new(), |b| {
                format!("next_seq {}, first_seq {}", b.next_seq.0, b.first_seq.0)
            }),
        }
    }
}

impl World for Records {
    type View = RecordsView;

    fn fresh() -> Self {
        let value = RegisterValue::new()
            .apply(&Change::Elect { holder: NodeId(1) })
            .expect("a first election");
        Self {
            value,
            floor: Seq(0),
            events: Vec::new(),
        }
    }

    fn actions(&self) -> Vec<Action> {
        let mut actions = vec![Action {
            key: "reshape".to_string(),
            label: "Reshape: open the next segment".to_string(),
            group: "journal".to_string(),
        }];
        if let Some(top) = self.top().filter(|top| *top > self.floor) {
            actions.push(Action {
                key: "truncate".to_string(),
                label: format!("Truncate below seq {}", top.0),
                group: "journal".to_string(),
            });
        }
        for record in self.value.segments().values() {
            let n = record.id().0;
            let group = format!("segment {n}");
            for (key, label) in [
                (format!("seal:{n}"), "Seal".to_string()),
                (format!("boundary:{n}"), "Write boundary".to_string()),
                (format!("retire:{n}:1"), "Retire, 1 holder".to_string()),
                (format!("retire:{n}:2"), "Retire, 2 holders".to_string()),
                (format!("trim:{n}"), "Trim".to_string()),
            ] {
                actions.push(Action {
                    key,
                    label,
                    group: group.clone(),
                });
            }
        }
        actions
    }

    fn act(&mut self, key: &str) -> Result<(), String> {
        if key == "truncate" {
            let top = self
                .top()
                .filter(|top| *top > self.floor)
                .ok_or_else(|| "No boundary state names a seq above the floor.".to_string())?;
            self.floor = top;
            self.events.push(EventView {
                sent: format!("truncate below seq {}", top.0),
                detail: format!("the fold's first_seq is now {}", top.0),
            });
            return Ok(());
        }
        let (change, sent) = self
            .change(key)
            .ok_or_else(|| format!("There is no move {key:?}."))?;
        let before = self.value.clone();
        match self.value.apply(&change) {
            Ok(next) => {
                let moved: Vec<String> = next
                    .segments()
                    .values()
                    .filter(|r| before.segment(r.id()) != Some(*r))
                    .map(|r| {
                        let was = before
                            .segment(r.id())
                            .map_or("new", |b| state_text(b.state()));
                        let boundary = if r.boundary().is_some()
                            && before
                                .segment(r.id())
                                .and_then(SegmentRecord::boundary)
                                .is_none()
                        {
                            ", boundary written"
                        } else {
                            ""
                        };
                        format!(
                            "segment {}: {was} → {}{boundary}",
                            r.id().0,
                            state_text(r.state())
                        )
                    })
                    .collect();
                self.value = next;
                self.events.push(EventView {
                    sent,
                    detail: moved.join("; "),
                });
                Ok(())
            }
            Err(refusal) => Err(format!("{sent}: refused. {}", refusal_text(&refusal))),
        }
    }

    fn view(&self) -> RecordsView {
        RecordsView {
            term: self.value.term().number,
            first_seq: self.floor.0,
            records: self
                .value
                .segments()
                .values()
                .map(Self::record_view)
                .collect(),
            history: self.events.clone(),
        }
    }

    fn events(&self) -> usize {
        self.events.len()
    }
}

/// Every exercise of the record lab, in page order.
#[must_use]
pub fn exercises() -> &'static [Exercise<Records>] {
    &EXERCISES
}

static EXERCISES: [Exercise<Records>; 3] = [
    Exercise {
        id: "records-retire",
        title: "Retire a segment",
        task: "Segment 1 is in force. Retire it, so that its acceptors may stop. A refused move \
               shows the guard that refused it.",
        done: "A seal needs a pending successor, and it puts the successor in force in the same \
               write. A retire needs the boundary state and F + 1 replicas that hold every slot. \
               Each guard is a compare-and-set on the record.",
        setup: &["reshape"],
        goal: |r| r.state(1) == Some(SegmentState::Retired),
        solution: &["reshape", "seal:1", "boundary:1", "retire:1:2"],
    },
    Exercise {
        id: "records-trim",
        title: "Trim a segment",
        task: "Segment 1 is in force. Mark it trimmed.",
        done: "A replica marks a segment trimmed only when the floor of its fold is at or above \
               the segment's last seq, which the boundary state names. The open segment is never \
               trimmed, and a trimmed record never comes back.",
        setup: &["reshape"],
        goal: |r| r.state(1) == Some(SegmentState::Trimmed),
        solution: &["reshape", "seal:1", "boundary:1", "truncate", "trim:1"],
    },
    Exercise {
        id: "records-third",
        title: "Open a third segment",
        task: "Segment 1 is in force. Open segment 3.",
        done: "A reshape needs the newest segment in force and every older segment retired. \
               So at most two segments hold live slots: the one in force and the one sealed \
               before it.",
        setup: &["reshape"],
        goal: |r| r.segments() == 3,
        solution: &["reshape", "seal:1", "boundary:1", "retire:1:2", "reshape"],
    },
];

#[cfg(test)]
mod tests;
