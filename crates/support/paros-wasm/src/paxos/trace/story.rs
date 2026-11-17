//! What the story mode of a Paxos page reads (#1146 (recorder for the
//! animated chapters)).
//!
//! The step-through draws one message per step. A chapter tells the same run
//! as a story: one screen per idea, one written sentence per screen, and the
//! messages of that idea move on the screen. This module holds the facts the
//! story needs in addition to a [`super::Step`]:
//!
//! - a [`Beat`]: one screen of the story. Its sentence is written in the
//!   script of the run, next to the actions that it describes. A beat holds
//!   the steps from the beat's call to the next beat's call. A beat with no
//!   step is an introduction screen.
//! - a [`Choice`]: at the end of a beat, a label that opens another recorded
//!   run at one of its beats. The reader decides; nothing runs again.
//! - a [`Flight`]: one message on the wire after a step. Every message of one
//!   broadcast has the same `fan`, so the page sends a broadcast as one and
//!   lets it arrive one by one.
//! - a [`NodeState`]: what a node is, as typed fields the page draws (role,
//!   alive, ballot, promise, vote, holes in the log), and the disk writes the
//!   step made on it.
//!
//! Both drivers fill these: the game's [`super::Recorder`] and the
//! compartmentalized page's own driver.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

/// One screen of a story.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Beat {
    /// The sentence of the screen, written in the run's script.
    pub says: String,
    /// The first step of the beat.
    pub from: usize,
    /// The step after the last step of the beat: `from == to` for a beat
    /// with no message.
    pub to: usize,
    /// What the reader can do at the end of the beat.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub choices: Vec<Choice>,
}

/// One choice at the end of a beat: open another run of the same page at one
/// of its beats.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Choice {
    /// The button's text.
    pub label: String,
    /// The id of the run it opens.
    pub run: String,
    /// The beat of that run it opens, from 0.
    pub beat: usize,
}

/// One message on the wire.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Flight {
    /// The message's id: the [`super::Step::message`] of the step that
    /// delivers or drops it.
    pub id: u64,
    /// The sending lane.
    pub from: String,
    /// The receiving lane.
    pub to: String,
    /// The render family: `prepare`, `promise`, `accept`, ….
    pub phase: String,
    /// `Prepare 1.5`, `Accept 1.5 · slot 2`.
    pub label: String,
    /// The broadcast it belongs to: the id of the first message of the
    /// broadcast. A message sent alone has its own id.
    pub fan: u64,
}

/// What a lane is after a step, as typed fields.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeState {
    /// False while it is down.
    pub alive: bool,
    /// Where it stands: `leader`, `candidate` or `follower` for a node of a
    /// log; `idle`, `phase1`, `phase2`, `stopped` or `chosen` for a proposer
    /// of one decree. `None` for a lane with no such state.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    /// The ballot it runs, as `round.node`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ballot: Option<String>,
    /// Its durable promise, as `round.node`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub promise: Option<String>,
    /// The vote of an acceptor of one decree. A node of a log has its votes
    /// in [`super::Step::logs`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vote: Option<Vote>,
    /// The slots below its highest record that hold no record: the holes of
    /// its log.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub holes: Vec<u64>,
    /// What the step wrote on its disk, in words: `promise 1.5`,
    /// `vote alpha at 1.5`, `slot 2: vote bravo at 1.8`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub writes: Vec<String>,
    /// Whether an operator retired it.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub retired: bool,
    /// Whether its disk was erased.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub wiped: bool,
}

/// One vote.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Vote {
    /// The value.
    pub value: String,
    /// The ballot of the vote, as `round.node`.
    pub ballot: String,
}

/// The durable facts of one lane, compared step to step to find what a step
/// wrote on the disk.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(super) struct Durable {
    /// Whether the lane can write: a lane that is down writes nothing.
    pub alive: bool,
    /// The promise, as `round.node`.
    pub promise: Option<String>,
    /// The votes by slot: (ballot, value). One decree uses slot 0.
    pub votes: BTreeMap<u64, (String, String)>,
    /// The slots of `votes` known chosen. A record that changes and is
    /// chosen at once was learned (a `Commit`, a catch-up), not voted.
    pub chosen: BTreeSet<u64>,
    /// The records of a matchmaker: ballot → members.
    pub registrations: BTreeMap<String, String>,
}

impl Durable {
    /// What a step wrote: every fact of `after` that is new or changed since
    /// `self`. A lane that was down, or is down now, wrote nothing a reader
    /// can see. `log` names the slot of each vote.
    pub(super) fn writes(&self, after: &Durable, log: bool) -> Vec<String> {
        if !self.alive || !after.alive {
            return Vec::new();
        }
        let mut writes = Vec::new();
        if after.promise != self.promise
            && let Some(promise) = &after.promise
        {
            writes.push(format!("promise {promise}"));
        }
        for (slot, vote) in &after.votes {
            if self.votes.get(slot) != Some(vote) {
                let (ballot, value) = vote;
                let learned = after.chosen.contains(slot);
                writes.push(if learned {
                    format!("slot {slot}: chosen {value} at {ballot}")
                } else if log {
                    format!("slot {slot}: vote {value} at {ballot}")
                } else {
                    format!("vote {value} at {ballot}")
                });
            }
        }
        for (ballot, members) in &after.registrations {
            if self.registrations.get(ballot) != Some(members) {
                writes.push(format!("record {ballot} → {members}"));
            }
        }
        writes
    }
}

/// The beats of one run, as its script opens them.
#[derive(Clone, Debug, Default)]
pub(super) struct Beats {
    beats: Vec<Beat>,
}

impl Beats {
    /// Open a beat at step `at`.
    ///
    /// # Panics
    ///
    /// When `says` is not one plain sentence that ends with a full stop.
    pub(super) fn open(&mut self, at: usize, says: &str) {
        assert!(
            says.ends_with('.') && !says.trim().is_empty(),
            "a beat says one plain sentence: {says:?}"
        );
        self.beats.push(Beat {
            says: says.to_string(),
            from: at,
            to: at,
            choices: Vec::new(),
        });
    }

    /// Add a choice to the beat that is open.
    ///
    /// # Panics
    ///
    /// When no beat is open.
    pub(super) fn choice(&mut self, label: &str, run: &str, beat: usize) {
        let open = self
            .beats
            .last_mut()
            .expect("a choice ends a beat: open one first");
        open.choices.push(Choice {
            label: label.to_string(),
            run: run.to_string(),
            beat,
        });
    }

    /// The beats of a run of `steps` steps: each beat ends where the next
    /// one starts, the last one at the end of the run.
    ///
    /// # Panics
    ///
    /// When the run has beats and steps before its first beat: a story tells
    /// every step.
    pub(super) fn finish(self, steps: usize) -> Vec<Beat> {
        let mut beats = self.beats;
        if let Some(first) = beats.first() {
            assert_eq!(first.from, 0, "a story opens a beat before its first step");
        }
        let ends: Vec<usize> = beats
            .iter()
            .skip(1)
            .map(|beat| beat.from)
            .chain(std::iter::once(steps))
            .collect();
        for (beat, end) in beats.iter_mut().zip(ends) {
            assert!(beat.from <= end, "beats are in step order");
            beat.to = end;
        }
        beats
    }
}

/// Check that every choice of every run opens a run of the page at one of its
/// beats.
///
/// # Panics
///
/// When a choice names a run the page does not have, or a beat that run does
/// not have.
pub(super) fn check_choices(page: &str, runs: &[super::Scenario]) {
    for run in runs {
        for choice in run.beats.iter().flat_map(|beat| &beat.choices) {
            let target = runs
                .iter()
                .find(|other| other.id == choice.run)
                .unwrap_or_else(|| {
                    panic!(
                        "{page}/{}: the choice {:?} opens no run",
                        run.id, choice.label
                    )
                });
            assert!(
                choice.beat < target.beats.len(),
                "{page}/{}: the choice {:?} opens beat {} of {}, which has {} beats",
                run.id,
                choice.label,
                choice.beat,
                target.id,
                target.beats.len()
            );
        }
    }
}

/// The holes of a log: every slot from `floor` up to the highest slot of
/// `slots` that `slots` does not hold.
pub(super) fn holes(floor: u64, slots: impl IntoIterator<Item = u64>) -> Vec<u64> {
    let held: std::collections::BTreeSet<u64> = slots.into_iter().collect();
    let Some(top) = held.last().copied() else {
        return Vec::new();
    };
    (floor..top).filter(|slot| !held.contains(slot)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_beat_ends_where_the_next_one_starts() {
        let mut beats = Beats::default();
        beats.open(0, "This is an acceptor.");
        beats.open(0, "A proposer asks for a promise.");
        beats.open(3, "The acceptors answer.");
        beats.choice("What if one is down?", "down", 1);
        let beats = beats.finish(5);
        let spans: Vec<(usize, usize)> = beats.iter().map(|beat| (beat.from, beat.to)).collect();
        assert_eq!(spans, [(0, 0), (0, 3), (3, 5)]);
        assert!(beats[0].choices.is_empty());
        assert_eq!(beats[2].choices[0].run, "down");
    }

    #[test]
    #[should_panic(expected = "opens a beat before its first step")]
    fn a_story_tells_every_step() {
        let mut beats = Beats::default();
        beats.open(2, "Late.");
        let _ = beats.finish(4);
    }

    #[test]
    #[should_panic(expected = "one plain sentence")]
    fn a_beat_says_a_sentence() {
        Beats::default().open(0, "no stop");
    }

    #[test]
    fn a_record_learned_chosen_is_not_a_vote() {
        let before = Durable {
            alive: true,
            ..Durable::default()
        };
        let mut after = before.clone();
        after
            .votes
            .insert(1, ("1.0".to_string(), "bravo".to_string()));
        after.chosen.insert(1);
        assert_eq!(before.writes(&after, true), ["slot 1: chosen bravo at 1.0"]);
        // A vote that a later Commit marks chosen writes nothing new.
        let mut voted = before.clone();
        voted
            .votes
            .insert(1, ("1.0".to_string(), "bravo".to_string()));
        assert!(voted.writes(&after, true).is_empty());
        // A catch-up that replaces an old vote with the chosen record.
        let mut old = before.clone();
        old.votes
            .insert(1, ("0.9".to_string(), "alpha".to_string()));
        assert_eq!(old.writes(&after, true), ["slot 1: chosen bravo at 1.0"]);
    }

    #[test]
    fn a_step_writes_what_changed_on_a_live_disk() {
        let before = Durable {
            alive: true,
            promise: Some("1.5".to_string()),
            ..Durable::default()
        };
        let mut after = before.clone();
        after.promise = Some("1.8".to_string());
        after
            .votes
            .insert(2, ("1.8".to_string(), "bravo".to_string()));
        assert_eq!(
            before.writes(&after, true),
            ["promise 1.8", "slot 2: vote bravo at 1.8"]
        );
        assert_eq!(before.writes(&after, false)[1], "vote bravo at 1.8");
        assert!(before.writes(&before, true).is_empty());
        let down = Durable::default();
        assert!(
            down.writes(&after, true).is_empty(),
            "a restart reads, never writes"
        );
    }

    #[test]
    fn a_recorded_run_tells_its_story_in_beats() {
        use crate::paxos::level::common::open;
        use crate::paxos::trace::{Recorder, assemble};

        let mut run = Recorder::new("act1/the-duel");
        run.beat("Three acceptors wait for a proposer.")
            .beat("Proposer 5 asks every acceptor for a promise.")
            .play(open(5, "alpha"), "proposer 5 opens a ballot for alpha")
            .settle(|message| message.phase == "prepare");
        run.beat("Each acceptor writes its promise, then answers.")
            .settle(|message| message.phase == "promise")
            .choice("Start again", "story", 0);
        let run = run.finish("story", "A story", |_| String::new());
        let spans: Vec<(usize, usize)> =
            run.beats.iter().map(|beat| (beat.from, beat.to)).collect();
        assert_eq!(spans, [(0, 0), (0, 4), (4, 7)]);
        assert_eq!(run.beats[2].choices.len(), 1);
        // The choice opens a run of the page: the page checks it.
        let _ = assemble("story", Vec::new(), None, vec![run]);
    }

    #[test]
    #[should_panic(expected = "opens no run")]
    fn a_choice_opens_a_run_of_the_page() {
        use crate::paxos::level::common::open;
        use crate::paxos::trace::{Recorder, assemble};

        let mut run = Recorder::new("act1/the-duel");
        run.beat("Proposer 5 opens a ballot.")
            .play(open(5, "alpha"), "proposer 5 opens a ballot for alpha")
            .choice("Elsewhere", "no-such-run", 0);
        let run = run.finish("story", "A story", |_| String::new());
        let _ = assemble("story", Vec::new(), None, vec![run]);
    }

    #[test]
    fn holes_are_the_missing_slots_below_the_top() {
        assert_eq!(holes(0, [0, 2, 5]), [1, 3, 4]);
        assert_eq!(holes(3, [4]), [3]);
        assert!(holes(0, []).is_empty());
    }
}
