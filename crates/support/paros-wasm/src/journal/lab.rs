//! The exercises of the journal fold ("your turn" on `/journal/calls/`): the
//! reader makes the calls of three writers, and `paros-journal` answers.
//!
//! Each exercise lists the calls the reader may make and has one goal,
//! read off the answers the writers heard. The page holds no journal logic:
//! it draws the [`LabView`] and sends the reader's [`Move`]s. Each exercise
//! has a solution that a test plays, so an exercise that the fold no longer
//! lets a reader solve fails CI.
//!
//! The world draws no randomness, so a list of moves is a full replay: undo
//! starts the exercise again and plays every move but the last.

use paros_journal::{Answer, JournalView, ResolveAnswer};
use serde::{Deserialize, Serialize};

use crate::journal::world::{Call, Event, Journal, Outcome, WorldView, show_leader};

const A: usize = 0;
const B: usize = 1;
const C: usize = 2;

/// How many moves an exercise takes at most: far above any solution, and a
/// bound on the replay of an undo.
const MOVES: usize = 200;

/// How many past calls the view shows, newest first.
const HISTORY: usize = 8;

/// One exercise.
pub struct Exercise {
    /// A stable id: the address and the progress use it.
    pub id: &'static str,
    /// The tab's text.
    pub title: &'static str,
    /// The goal, in one or two sentences.
    pub task: &'static str,
    /// What the reader sees when the goal holds: why it matters.
    pub done: &'static str,
    /// The calls made before the reader's first one. Undo never takes one
    /// back.
    setup: &'static [Call],
    /// The calls the reader may make.
    calls: &'static [Call],
    /// Whether the goal holds.
    goal: fn(&Journal) -> bool,
    /// A way to reach the goal, played by the tests (and by the browser
    /// smoke test, with the same calls).
    pub solution: &'static [Call],
}

/// Every exercise, in page order.
#[must_use]
pub fn exercises() -> &'static [Exercise] {
    &EXERCISES
}

/// The exercise `id`.
#[must_use]
pub fn exercise(id: &str) -> Option<&'static Exercise> {
    EXERCISES.iter().find(|exercise| exercise.id == id)
}

fn duplicate(outcome: &Outcome) -> bool {
    matches!(outcome, Outcome::Applied(Answer::Duplicate { .. }))
}

static EXERCISES: [Exercise; 5] = [
    Exercise {
        id: "journal-duplicate",
        title: "One write, not two",
        task: "A leads. Its first write lands, but the answer is lost. Make A learn that the \
               write is in the journal, with no second copy: get the answer Duplicate.",
        done: "The log is the dedup table. The resend names the same position, and the fold \
               finds the same write there: Duplicate means written. The journal keeps no table \
               of past requests.",
        setup: &[Call::SetLeader(A)],
        calls: &[
            Call::WriteLost(A),
            Call::Write(A),
            Call::Resend(A),
            Call::Read(A),
        ],
        goal: |journal| journal.heard(A, duplicate),
        solution: &[Call::WriteLost(A), Call::Resend(A)],
    },
    Exercise {
        id: "journal-fenced",
        title: "The deposed writer",
        task: "A leads and wrote a1. Let B take the lead, then make A's next write fail with \
               the answer Fenced.",
        done: "SetLeader is a compare-and-set on the leader value, and the uuid fences every \
               write. A's new write is not written, and never will be. Start again, and let A send a1 again \
               after B takes the lead: the fold judges the position before the fence, so a1 \
               comes back Duplicate, not Fenced.",
        setup: &[Call::SetLeader(A), Call::Write(A)],
        calls: &[
            Call::SetLeader(B),
            Call::Read(B),
            Call::Write(A),
            Call::Resend(A),
            Call::Read(A),
        ],
        goal: |journal| journal.heard(A, |o| matches!(o, Outcome::Applied(Answer::Fenced(_)))),
        solution: &[Call::Read(B), Call::SetLeader(B), Call::Write(A)],
    },
    Exercise {
        id: "journal-multi",
        title: "Many writers",
        task: "A leads. Make the journal multi-writer, then get one unfenced write of B and \
               one of C written.",
        done: "With no leader, the journal gives each write its position at apply. Nothing \
               fences a resend, so a write sent again after a lost answer can land two times: \
               multi-writer is at-least-once.",
        setup: &[Call::SetLeader(A)],
        calls: &[
            Call::WriteOpen(B),
            Call::WriteOpen(C),
            Call::SetMulti(A),
            Call::Write(A),
            Call::SetLeader(A),
        ],
        goal: |journal| {
            let open = |o: &Outcome| matches!(o, Outcome::Applied(Answer::Written { .. }));
            [B, C].iter().all(|&w| {
                journal
                    .events()
                    .iter()
                    .any(|e: &Event| e.call == Call::WriteOpen(w) && !e.lost && open(&e.outcome))
            })
        },
        solution: &[Call::SetMulti(A), Call::WriteOpen(B), Call::WriteOpen(C)],
    },
    Exercise {
        id: "journal-below",
        title: "Across a seal",
        task: "A leads. Its write lands, the answer is lost, and the segment seals before A \
               sends it again. Get the answer Below, then settle the write with Resolve.",
        done: "The new segment folds from the state at the seal, not from the records before \
               it, so it cannot tell Duplicate from Taken: it answers Below. A replica of the \
               sealed segment compares the write as apply does, and a sealed segment does not \
               change, so its answer is final.",
        setup: &[Call::SetLeader(A)],
        calls: &[
            Call::WriteLost(A),
            Call::Seal,
            Call::Resend(A),
            Call::Resolve(A),
            Call::Read(A),
        ],
        goal: |journal| {
            journal.heard(A, |o| matches!(o, Outcome::Applied(Answer::Below(_))))
                && journal.heard(A, |o| {
                    matches!(o, Outcome::Resolved(ResolveAnswer::Written { .. }))
                })
        },
        solution: &[
            Call::WriteLost(A),
            Call::Seal,
            Call::Resend(A),
            Call::Resolve(A),
        ],
    },
    Exercise {
        id: "journal-truncated",
        title: "Below the floor",
        task: "A leads. Its write lands and the answer is lost. Make A's resend come back \
               Truncated.",
        done: "Below first_seq the records are gone, so the fold cannot compare: the fate of \
               the write is unknown. The writer reads the tail. A writer truncates only what \
               its own checkpoint holds.",
        setup: &[Call::SetLeader(A)],
        calls: &[
            Call::WriteLost(A),
            Call::Read(A),
            Call::Truncate(A),
            Call::Resend(A),
        ],
        goal: |journal| journal.heard(A, |o| matches!(o, Outcome::Applied(Answer::Truncated(_)))),
        solution: &[
            Call::WriteLost(A),
            Call::Read(A),
            Call::Truncate(A),
            Call::Resend(A),
        ],
    },
];

/// A move of the reader.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Move {
    /// Make the call at this index of the exercise's list.
    Call {
        /// The index in [`LabView::calls`].
        index: usize,
    },
    /// Take the last move back.
    Undo,
    /// Start the exercise again.
    Reset,
}

/// One call the reader may make.
#[derive(Clone, Debug, Serialize)]
pub struct CallView {
    /// A stable name: `A:write`, `seal`.
    pub key: String,
    /// The button's text, from the state now.
    pub label: String,
}

/// What one call gave, in words.
#[derive(Clone, Debug, Serialize)]
pub struct EventView {
    /// What the client sent.
    pub sent: String,
    /// The answer's name: `Written`, `Below`, ….
    pub answer: String,
    /// Its fields: `seq 0, 1 record`, or the view it was judged against.
    pub detail: String,
    /// Whether the answer is final, from the answers table of §J2.
    pub fin: String,
    /// What the writer does next, from the same table.
    pub then: String,
    /// Whether the answer was lost on its way back.
    pub lost: bool,
}

/// What the page draws.
#[derive(Clone, Debug, Serialize)]
pub struct LabView {
    /// The exercise.
    pub id: String,
    /// The tab's text.
    pub title: String,
    /// The goal.
    pub task: String,
    /// The world.
    pub world: WorldView,
    /// The calls the reader may make.
    pub calls: Vec<CallView>,
    /// The past calls, newest first.
    pub history: Vec<EventView>,
    /// Why the last move was refused.
    pub refused: Option<String>,
    /// The reader's moves so far.
    pub moves: usize,
    /// Whether the goal holds.
    pub solved: bool,
    /// Why it matters, once the goal holds.
    pub done: Option<String>,
}

/// One exercise in progress.
pub struct Lab {
    exercise: &'static Exercise,
    journal: Journal,
    moves: Vec<Move>,
    /// How many events the setup made: the history starts after them.
    setup: usize,
    refusal: Option<String>,
}

impl Lab {
    /// Start exercise `id`.
    ///
    /// # Errors
    ///
    /// When no exercise has that id.
    pub fn new(id: &str) -> Result<Self, String> {
        let exercise = exercise(id).ok_or_else(|| format!("there is no exercise {id:?}"))?;
        Ok(Self::start(exercise))
    }

    fn start(exercise: &'static Exercise) -> Self {
        let mut journal = Journal::new();
        for &call in exercise.setup {
            journal
                .call(call)
                .expect("the setup of an exercise is a run that works");
        }
        Self {
            exercise,
            setup: journal.events().len(),
            journal,
            moves: Vec::new(),
            refusal: None,
        }
    }

    /// Play one move. A refused move changes nothing and says why.
    pub fn play(&mut self, chosen: &Move) {
        match chosen {
            Move::Reset => *self = Self::start(self.exercise),
            Move::Undo => {
                let mut moves = std::mem::take(&mut self.moves);
                moves.pop();
                *self = Self::start(self.exercise);
                for again in &moves {
                    self.play(again);
                }
            }
            Move::Call { index } => {
                if self.moves.len() >= MOVES {
                    self.refusal = Some(format!("An exercise takes at most {MOVES} moves."));
                    return;
                }
                let Some(&call) = self.exercise.calls.get(*index) else {
                    self.refusal = Some("This exercise has no such call.".to_string());
                    return;
                };
                match self.journal.call(call) {
                    Ok(_) => {
                        self.moves.push(chosen.clone());
                        self.refusal = None;
                    }
                    Err(why) => self.refusal = Some(why),
                }
            }
        }
    }

    /// Make `call`, which must be in the exercise's list: what a solution
    /// is written in.
    ///
    /// # Panics
    ///
    /// When the exercise does not list `call`, or the call is refused.
    pub fn make(&mut self, call: Call) {
        let index = self
            .exercise
            .calls
            .iter()
            .position(|&c| c == call)
            .unwrap_or_else(|| panic!("{}: the exercise lists {call:?}", self.exercise.id));
        self.play(&Move::Call { index });
        assert!(
            self.refusal.is_none(),
            "{}: {call:?} is refused: {:?}",
            self.exercise.id,
            self.refusal
        );
    }

    /// Whether the goal holds.
    #[must_use]
    pub fn solved(&self) -> bool {
        (self.exercise.goal)(&self.journal)
    }

    /// The view the page draws.
    #[must_use]
    pub fn view(&self) -> LabView {
        let solved = self.solved();
        LabView {
            id: self.exercise.id.to_string(),
            title: self.exercise.title.to_string(),
            task: self.exercise.task.to_string(),
            world: self.journal.render(),
            calls: self
                .exercise
                .calls
                .iter()
                .map(|&call| CallView {
                    key: call.key(),
                    label: self.journal.label(call),
                })
                .collect(),
            history: self.journal.events()[self.setup..]
                .iter()
                .rev()
                .take(HISTORY)
                .map(explain)
                .collect(),
            refused: self.refusal.clone(),
            moves: self.moves.len(),
            solved,
            done: solved.then(|| self.exercise.done.to_string()),
        }
    }
}

/// One row of the answers table: the answer's name, its fields, whether it
/// is final, and what the writer does next.
type Row = (&'static str, String, &'static str, &'static str);

/// A journal view in words.
fn view_text(v: &JournalView) -> String {
    format!(
        "leader {}, next_seq {}, first_seq {}",
        show_leader(v.leader),
        v.next_seq.0,
        v.first_seq.0
    )
}

/// The positions of a written write in words.
fn records(seq: u64, count: u64) -> String {
    format!(
        "seq {seq}, {count} record{}",
        if count == 1 { "" } else { "s" }
    )
}

/// The row of the answers table of `docs/journal/02-data-plane.md` §J2 that
/// an answer of the fold matches.
fn applied(answer: &Answer) -> Row {
    let refused = |name, v: &JournalView, fin, then| (name, view_text(v), fin, then);
    match answer {
        Answer::Written { seq, count } => (
            "Written",
            records(seq.0, *count),
            "yes",
            "Nothing. The write holds its positions.",
        ),
        Answer::Duplicate { seq, count } => (
            "Duplicate",
            records(seq.0, *count),
            "yes",
            "Nothing. The write was already there.",
        ),
        Answer::Taken(v) => refused("Taken", v, "yes", "Not written. Read the view and decide."),
        Answer::Fenced(v) => refused(
            "Fenced",
            v,
            "yes",
            "Not written, and never will be. Read the view and decide.",
        ),
        Answer::Early(v) => refused(
            "Early",
            v,
            "no",
            "Send it again when the tail reaches its position.",
        ),
        Answer::Truncated(v) => {
            refused("Truncated", v, "no", "The fate is unknown. Read the tail.")
        }
        Answer::Below(v) => refused("Below", v, "no", "Resolve at a replica of the old segment."),
        Answer::WrongMode(v) => refused(
            "WrongMode",
            v,
            "for that copy",
            "The call is shaped for the other mode.",
        ),
        Answer::Empty(v) => refused(
            "Empty",
            v,
            "yes",
            "Not written. A write carries one record or more.",
        ),
        Answer::LeaderSet(v) => refused("LeaderSet", v, "yes", "The view after the change."),
        Answer::LeaderRefused(v) => refused(
            "LeaderRefused",
            v,
            "yes",
            "Lost the compare-and-set. Read the view and decide.",
        ),
        Answer::Trimmed(v) => refused("Trimmed", v, "yes", "The view after the truncate."),
        Answer::Reserved(v) => refused("Reserved", v, "yes", "Nothing moved."),
        Answer::Noop => ("Noop", String::new(), "yes", "Nothing moved."),
    }
}

/// The row of a `Resolve` answer.
fn resolved(answer: ResolveAnswer) -> Row {
    match answer {
        ResolveAnswer::Written { seq, count } => (
            "written",
            records(seq.0, count),
            "yes",
            "Nothing. The write is in the sealed segment.",
        ),
        ResolveAnswer::NotWritten => (
            "not written",
            String::new(),
            "yes",
            "Read the tail, then write again at a new expected_seq.",
        ),
        ResolveAnswer::Truncated => (
            "Truncated",
            String::new(),
            "no",
            "The fate is unknown. Read the tail.",
        ),
        ResolveAnswer::NotCovered => (
            "not covered",
            String::new(),
            "no",
            "This replica does not hold the position. Ask again later.",
        ),
    }
}

/// One event in words.
fn explain(event: &Event) -> EventView {
    let (answer, detail, fin, then) = match &event.outcome {
        Outcome::Applied(answer) => applied(answer),
        Outcome::Resolved(answer) => resolved(*answer),
        Outcome::Read(v) => ("view", view_text(v), "", "The writer now knows this view."),
        Outcome::Sealed(boundary) => (
            "sealed",
            format!(
                "the next segment starts at next_seq {}, first_seq {}, leader {}",
                boundary.next_seq.0,
                boundary.first_seq.0,
                show_leader(boundary.leader)
            ),
            "",
            "The next segment folds from this state.",
        ),
    };
    EventView {
        sent: event.sent.clone(),
        answer: answer.to_string(),
        detail,
        fin: fin.to_string(),
        then: then.to_string(),
        lost: event.lost,
    }
}

#[cfg(test)]
mod tests;
