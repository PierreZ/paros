//! The heal of a retired segment's replica set under the walk (#1314
//! (re-replication of a sealed segment's replica set), §J3.1, §J6.1;
//! `specs/tla/re-replication/`): the controller role of
//! `paros-journal-control` plans a heal, a new replica copies the segment
//! from the members of the set, and the controller writes the new set by
//! compare-and-set on the record it read, while replica machines are lost
//! for good or come back, controllers crash and get deposed, requests and
//! replies are lost, and the journal trims the segment.
//!
//! The register is one atomic value, as in the controller walk
//! (`crate::controller`). The walk's driver gives each controller its view,
//! runs each register operation, starts and abandons the copies, and plays
//! the replicas: a copy moves only while a member of the set that the
//! controller read is up and holds the segment, a new replica reports its
//! copy once it is complete (and again, as the replica does on its tick),
//! and a replica that is in no set and runs no copy drops its copy (the
//! roles of `paros_journal::roles::SegmentReplica`).
//!
//! A node runs one copy of the segment: a copy asked with a higher
//! [`CopyId`] replaces it, and a lower one is refused. A deposed controller
//! keeps its heal until the register refuses it, so without that order its
//! copy could wipe the complete copy of the new holder, whose
//! compare-and-set would then count a replica with nothing on it.
//!
//! The oracles, written from §J3.1 and `re-replication/`, share no code with
//! the controller, the register or the replica:
//!
//! - [`HELD`]: while the segment is not trimmed, every member of its replica
//!   set holds a complete copy (up or down).
//! - [`CONVERGES`]: once the faults stop, one live controller holds the term
//!   with no heal and no call in flight, and the set is all up; or the
//!   segment is trimmed and no live machine keeps a copy.

use std::collections::{BTreeMap, BTreeSet};

use moonpool_walk::{Cx, Model, Phase, Walk, sometimes};
use paros_journal::register::{
    Change, ChangeRefusal, MatchmakerGeneration, MatchmakerId, MatchmakerSet, RegisterValue,
};
use paros_journal::roles::CopyId;
use paros_journal::segment::{OpeningCall, SegmentId, SegmentState};
use paros_journal::{BoundaryState, Grid, Leader, NodeId, RELEASE, Seq, Slot};
use paros_journal_control::role::{Ask, Controller, Outcome, Standing};
use paros_journal_control::{Liveness, Tunables, View};

/// Oracle: every member of the set of a segment not trimmed holds a
/// complete copy.
pub const HELD: &str = "every member of a retired segment's replica set holds a complete copy";
/// Oracle (liveness): once the faults stop, the heal is done.
pub const CONVERGES: &str = "once the faults stop, the replica set is whole or the segment trimmed";

/// The coverage sites of the heal walk.
pub mod reach {
    /// A heal wrote a new set.
    pub const HEALED: &str = "heal walk: a new set written";
    /// A member of the set was lost for good.
    pub const LOST: &str = "heal walk: a member of the set lost for good";
    /// A removed member came back and dropped its copy.
    pub const DROPPED: &str = "heal walk: a removed member dropped its copy";
    /// A node refused a copy lower than the one it runs.
    pub const LOWER_REFUSED: &str = "heal walk: a lower copy refused";
    /// A node took a higher copy over the one it runs.
    pub const HIGHER_TAKEN: &str = "heal walk: a higher copy replaced a copy";
    /// A copy waited for a member of the set to come back.
    pub const NO_SOURCE: &str = "heal walk: a copy with no live source waited";
    /// The register refused a heal for its term.
    pub const TERM_REFUSED: &str = "heal walk: a heal of an old term refused";
    /// The register refused a heal on an old read.
    pub const STALE_REFUSED: &str = "heal walk: a heal on an old read refused";
    /// The segment was trimmed during a heal.
    pub const TRIMMED: &str = "heal walk: the segment trimmed during a heal";

    /// The sites of the walk.
    pub const ALL: &[&str] = &[
        HEALED,
        LOST,
        DROPPED,
        LOWER_REFUSED,
        HIGHER_TAKEN,
        NO_SOURCE,
        TERM_REFUSED,
        STALE_REFUSED,
        TRIMMED,
    ];
}

/// A bug planted in the driver of the heal walk.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum HealMutant {
    /// No bug.
    #[default]
    None,
    /// A new replica reports its copy complete as soon as it starts.
    /// [`HELD`] catches it.
    EarlyReport,
    /// A node takes every copy asked, a lower one too. [`HELD`] catches it.
    AnyCopy,
    /// The register writes the new set over the record it holds, with no
    /// compare on the record read or on the term. [`HELD`] catches it.
    NoCompare,
    /// The register compares the record read but not the term: the
    /// `version_only` rule of `re-replication/`, which passes there because
    /// a heal there never reuses a node. Here a deposed controller's
    /// compare-and-set can count a copy that the new holder's copy wiped.
    /// [`HELD`] catches it.
    NoTermCheck,
    /// The driver never sends a heal's compare-and-set again.
    /// [`CONVERGES`] catches it.
    NoResend,
}

/// The numbers of the walk's host (§J6, **Open** for a real host).
const TUNABLES: Tunables = crate::controller::TUNABLES;

/// The F of the segment.
const F: usize = 1;
/// The segment that heals.
const SEGMENT: SegmentId = SegmentId(1);
/// Its first replica set.
const SET: [u64; 3] = [11, 12, 13];
/// The candidates for a new replica.
const CANDIDATES: std::ops::RangeInclusive<u64> = 20..=25;
/// The steps a copy takes from a live source.
const COPY_STEPS: u8 = 2;
/// The first controller id.
const CONTROLLER: u64 = 900;
/// The controllers.
const CONTROLLERS: usize = 2;
/// The most machine failures of a run.
const FAILS: usize = 4;

/// The walk's own moves.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HealAction {
    /// An idle controller wins the term.
    Campaign,
    /// A controller that holds a term reads its view and steps.
    Step,
    /// The register takes a request.
    Process,
    /// A reply reaches its controller.
    Receive,
    /// A request is lost: its controller sees no answer.
    LoseReq,
    /// A reply is lost.
    LoseRep,
    /// A copy moves one step.
    Copy,
    /// A new replica reports its complete copy again.
    Report,
    /// A replica in no set and with no copy drops the segment.
    Drop,
    /// A machine fails.
    Fail,
    /// A failed machine comes back with its disk.
    Recover,
    /// The journal trims the segment.
    Trim,
}

/// A request or a reply in flight.
#[derive(Clone, Debug)]
struct Flight {
    c: usize,
    change: Change,
    outcome: Option<Outcome>,
}

/// One controller.
#[derive(Clone, Debug)]
struct Ctl {
    up: bool,
    role: Controller,
    /// The changes sent once (the mutant `NoResend`).
    sent: Vec<Change>,
}

/// The copy a new replica runs, on its disk.
#[derive(Clone, Debug)]
struct Copying {
    copy: CopyId,
    /// The set the controller read: the sources.
    set: Vec<NodeId>,
    steps: u8,
}

/// The heal walk.
pub struct HealModel {
    mutant: HealMutant,
    register: RegisterValue,
    ctls: Vec<Ctl>,
    requests: Vec<Flight>,
    replies: Vec<Flight>,
    /// The machines that hold a complete copy of the segment.
    holds: BTreeSet<NodeId>,
    /// The copy each new replica runs.
    copies: BTreeMap<NodeId, Copying>,
    down: BTreeSet<NodeId>,
    /// The machines lost for good.
    lost: BTreeSet<NodeId>,
    fails: usize,
}

/// The heal walk with `mutant` planted, over `seeds` seeds of `steps` chaos
/// steps.
#[must_use]
pub fn walk(mutant: HealMutant, seeds: u64, steps: u64) -> Walk<HealModel> {
    Walk::<HealModel>::new()
        .seeds(seeds)
        .chaos_steps(steps)
        .quiet_steps(100)
        // No message of the engine: the walk's moves are its own.
        .deliver_weights(0, 0)
        .crash_weights(1, 3)
        .max_down(1)
        .env_prefix(None)
        .model(move |cx| HealModel::build(mutant, cx))
}

fn nodes(ids: impl IntoIterator<Item = u64>) -> Vec<NodeId> {
    ids.into_iter().map(NodeId).collect()
}

/// Segment 1 retired with the set 11, 12, 13; segment 2 in force.
fn first_register() -> RegisterValue {
    let open = |expected, grid: [u64; 3], replicas: [u64; 3]| Change::Reshape {
        opening: OpeningCall {
            term: 0,
            expected,
            grid: Grid::new(1, vec![nodes(grid)]).expect("a v1 grid"),
            replicas: nodes(replicas),
        },
        release: RELEASE,
    };
    [
        open(None, [1, 2, 3], SET),
        open(Some(SEGMENT), [4, 5, 6], [14, 15, 16]),
        Change::Seal {
            segment: SEGMENT,
            end: Slot(3),
        },
        Change::Boundary {
            segment: SEGMENT,
            state: BoundaryState::fresh(Leader::Unset),
        },
        Change::Retire {
            segment: SEGMENT,
            holders: 2,
            unrepaired_damage: false,
        },
    ]
    .iter()
    .fold(RegisterValue::new(), |value, change| {
        value.apply(change).expect("the first register")
    })
}

impl HealModel {
    /// The walk with `mutant` planted.
    pub fn build(mutant: HealMutant, _cx: &mut Cx<'_, Self>) -> Self {
        Self {
            mutant,
            register: first_register(),
            ctls: (0..CONTROLLERS)
                .map(|c| Ctl {
                    up: true,
                    role: Controller::new(NodeId(CONTROLLER + c as u64)),
                    sent: Vec::new(),
                })
                .collect(),
            requests: Vec::new(),
            replies: Vec::new(),
            holds: nodes(SET).into_iter().collect(),
            copies: BTreeMap::new(),
            down: BTreeSet::new(),
            lost: BTreeSet::new(),
            fails: 0,
        }
    }

    fn record(&self) -> &paros_journal::segment::SegmentRecord {
        self.register.segment(SEGMENT).expect("segment 1")
    }

    fn trimmed(&self) -> bool {
        self.record().state() == SegmentState::Trimmed
    }

    fn set(&self) -> Vec<NodeId> {
        self.record().replicas().to_vec()
    }

    fn live_holder(&self) -> Option<usize> {
        let term = self.register.term();
        let c = self
            .ctls
            .iter()
            .position(|ctl| Some(ctl.role.me()) == term.holder && ctl.up)?;
        (self.ctls[c].role.term() == Some(term.number)).then_some(c)
    }

    /// The ones that make a move now: every one in the settle phase, one
    /// drawn at random otherwise.
    fn pick<T: Clone>(cx: &mut Cx<'_, Self>, eligible: &[T]) -> Vec<T> {
        if eligible.is_empty() {
            return Vec::new();
        }
        if cx.phase() == Phase::Settle {
            return eligible.to_vec();
        }
        let at = cx.rng().index(eligible.len()).expect("not empty");
        vec![eligible[at].clone()]
    }

    fn up_ctls(&self, wanted: impl Fn(&Ctl) -> bool) -> Vec<usize> {
        (0..self.ctls.len())
            .filter(|&c| self.ctls[c].up && wanted(&self.ctls[c]))
            .collect()
    }

    fn campaign(&mut self, cx: &mut Cx<'_, Self>) {
        let chaos = cx.phase() == Phase::Chaos;
        let free = self.live_holder().is_none();
        // A live holder is deposed now and then, so that its heals run long
        // enough to finish or get stuck.
        if !free && !cx.rng().chance(1, 8) {
            return;
        }
        let mut eligible =
            self.up_ctls(|ctl| ctl.role.standing() == &Standing::Idle && (chaos || free));
        if !chaos {
            eligible.truncate(1);
        }
        for c in Self::pick(cx, &eligible) {
            let change = self.ctls[c].role.campaign();
            // The register is atomic: the read of the term and its
            // compare-and-set are one move.
            let value = self.register.apply(&change).expect("an election");
            self.register = value.clone();
            self.ctls[c].role.elected(&Outcome::Done(value));
        }
    }

    fn liveness(&self) -> BTreeMap<NodeId, Liveness> {
        let all = nodes(1..=6)
            .into_iter()
            .chain(nodes(SET))
            .chain(nodes(14..=16))
            .chain(nodes(CANDIDATES));
        all.map(|n| {
            let live = if self.down.contains(&n) {
                Liveness::Down
            } else {
                Liveness::Up
            };
            (n, live)
        })
        .collect()
    }

    fn step(&mut self, cx: &mut Cx<'_, Self>) {
        let eligible = self.up_ctls(|ctl| ctl.role.term().is_some());
        for c in Self::pick(cx, &eligible) {
            self.step_one(c);
        }
    }

    fn step_one(&mut self, c: usize) {
        let liveness = self.liveness();
        let candidates = nodes(CANDIDATES);
        let matchmakers =
            MatchmakerSet::new(MatchmakerGeneration(0), (1..=3).map(MatchmakerId).collect());
        let matchmakers_live: BTreeSet<MatchmakerId> = (1..=3).map(MatchmakerId).collect();
        let repairs = BTreeSet::new();
        let zones = BTreeMap::new();
        let register = self.register.clone();
        let ctl = &mut self.ctls[c];
        let term = ctl.role.term().expect("a holder");
        let view = View {
            me: ctl.role.me(),
            term,
            register: &register,
            liveness: &liveness,
            zones: &zones,
            candidates: &candidates,
            leader_live: true,
            matchmakers: &matchmakers,
            matchmakers_live: &matchmakers_live,
            repairs_pending: &repairs,
            data_lost: false,
            signal_age: 0,
            since_last_reshape: None,
            seal_age: None,
            token_free: true,
        };
        let (ask, _) = ctl.role.step(&view, &TUNABLES);
        self.run(c, ask);
    }

    /// The driver runs what controller `c` asks.
    fn run(&mut self, c: usize, ask: Ask) {
        match ask {
            Ask::Nothing => {}
            Ask::Call(change @ Change::Rereplicate { .. }) => self.send(c, change),
            Ask::Call(other) => panic!("the grid in force never fails here: {other:?}"),
            Ask::Act(action) => panic!("the walk has no repair: {action:?}"),
            Ask::Copy {
                segment,
                node,
                set,
                copy,
            } => {
                assert_eq!(segment, SEGMENT);
                self.join(node, set, copy);
            }
            Ask::Abandon {
                segment,
                node,
                copy,
            } => {
                assert_eq!(segment, SEGMENT);
                // The ask waits on the node's disk until it is back.
                if self.copies.get(&node).is_some_and(|run| run.copy == copy) {
                    self.copies.remove(&node);
                }
            }
        }
    }

    /// Node `node` starts the copy `copy`, from `set`, unless it runs that
    /// copy or a higher one.
    fn join(&mut self, node: NodeId, set: Vec<NodeId>, copy: CopyId) {
        if self.down.contains(&node) {
            return;
        }
        if let Some(run) = self.copies.get(&node) {
            if run.copy == copy {
                return;
            }
            if run.copy > copy && self.mutant != HealMutant::AnyCopy {
                sometimes!(true, reach::LOWER_REFUSED);
                return;
            }
            sometimes!(run.copy < copy, reach::HIGHER_TAKEN);
        }
        assert!(
            !self.set().contains(&node),
            "a heal starts a copy only on a node outside the set"
        );
        // A new copy starts on an empty disk.
        self.holds.remove(&node);
        self.copies.insert(
            node,
            Copying {
                copy,
                set,
                steps: 0,
            },
        );
        if self.mutant == HealMutant::EarlyReport {
            self.report(copy);
        }
    }

    fn send(&mut self, c: usize, change: Change) {
        let ctl = &mut self.ctls[c];
        let again = ctl.sent.contains(&change);
        if again && self.mutant == HealMutant::NoResend {
            return;
        }
        let flying = self
            .requests
            .iter()
            .chain(&self.replies)
            .any(|m| m.c == c && m.change == change);
        if flying {
            return;
        }
        if !again {
            ctl.sent.push(change.clone());
        }
        self.requests.push(Flight {
            c,
            change,
            outcome: None,
        });
    }

    /// A new replica reports its complete copy `copy` to every live
    /// controller.
    fn report(&mut self, copy: CopyId) {
        for c in self.up_ctls(|_| true) {
            let ask = self.ctls[c].role.copied(SEGMENT, copy);
            self.run(c, ask);
        }
    }

    fn process(&mut self, cx: &mut Cx<'_, Self>) {
        let eligible: Vec<usize> = (0..self.requests.len()).collect();
        let mut picked = Self::pick(cx, &eligible);
        picked.sort_unstable_by(|a, b| b.cmp(a));
        for i in picked {
            let request = self.requests.remove(i);
            let outcome = self.apply(&request.change);
            self.replies.push(Flight {
                outcome: Some(outcome),
                ..request
            });
        }
    }

    fn apply(&mut self, change: &Change) -> Outcome {
        let Change::Rereplicate { remove, add, .. } = change else {
            panic!("a controller sends only heals here: {change:?}");
        };
        let result = if self.mutant == HealMutant::NoCompare {
            let mut record = self.record().clone();
            record
                .replace_replica(*remove, *add)
                .map(|()| {
                    let mut segments = self.register.segments().clone();
                    segments.insert(SEGMENT, record);
                    RegisterValue::from_parts(self.register.term(), segments)
                })
                .map_err(|refusal| ChangeRefusal::Record(SEGMENT, refusal))
        } else if self.mutant == HealMutant::NoTermCheck {
            let Change::Rereplicate {
                segment,
                read,
                remove,
                add,
                ..
            } = change.clone()
            else {
                unreachable!("a heal");
            };
            self.register.apply(&Change::Rereplicate {
                segment,
                read,
                term: self.register.term().number,
                remove,
                add,
            })
        } else {
            self.register.apply(change)
        };
        match result {
            Ok(value) => {
                let before = self.set();
                self.register = value.clone();
                let after = self.set();
                if before != after {
                    sometimes!(true, reach::HEALED);
                    assert!(
                        after.contains(add) && !after.contains(remove),
                        "a heal swaps one member"
                    );
                }
                Outcome::Done(value)
            }
            Err(refusal) => {
                sometimes!(
                    matches!(refusal, ChangeRefusal::Term { .. }),
                    reach::TERM_REFUSED
                );
                sometimes!(
                    matches!(refusal, ChangeRefusal::Stale(_)),
                    reach::STALE_REFUSED
                );
                Outcome::Refused(refusal)
            }
        }
    }

    fn receive(&mut self, cx: &mut Cx<'_, Self>) {
        let eligible: Vec<usize> = (0..self.replies.len()).collect();
        let mut picked = Self::pick(cx, &eligible);
        picked.sort_unstable_by(|a, b| b.cmp(a));
        for i in picked {
            let reply = self.replies.remove(i);
            let outcome = reply.outcome.expect("a reply has an outcome");
            if self.ctls[reply.c].up {
                let ask = self.ctls[reply.c]
                    .role
                    .rereplicated(&reply.change, &outcome);
                self.run(reply.c, ask);
            }
        }
    }

    fn lose(&mut self, cx: &mut Cx<'_, Self>, request: bool) {
        let n = if request {
            self.requests.len()
        } else {
            self.replies.len()
        };
        let eligible: Vec<usize> = (0..n).collect();
        for i in Self::pick(cx, &eligible) {
            let lost = if request {
                self.requests.remove(i)
            } else {
                self.replies.remove(i)
            };
            // The driver's timeout: the operation ended with no answer.
            if self.ctls[lost.c].up {
                let ask = self.ctls[lost.c]
                    .role
                    .rereplicated(&lost.change, &Outcome::NoAnswer);
                self.run(lost.c, ask);
            }
        }
    }

    fn copy(&mut self, cx: &mut Cx<'_, Self>) {
        let eligible: Vec<NodeId> = self
            .copies
            .iter()
            .filter(|(n, run)| !self.down.contains(n) && run.steps < COPY_STEPS)
            .map(|(n, _)| *n)
            .collect();
        for node in Self::pick(cx, &eligible) {
            let trimmed = self.trimmed();
            let run = self.copies.get_mut(&node).expect("a copy");
            let source = !trimmed
                && run
                    .set
                    .iter()
                    .any(|s| !self.down.contains(s) && self.holds.contains(s));
            sometimes!(!source, reach::NO_SOURCE);
            if !source {
                continue;
            }
            run.steps += 1;
            if run.steps == COPY_STEPS {
                let copy = run.copy;
                self.holds.insert(node);
                if self.mutant != HealMutant::EarlyReport {
                    self.report(copy);
                }
            }
        }
    }

    fn report_again(&mut self, cx: &mut Cx<'_, Self>) {
        let eligible: Vec<CopyId> = self
            .copies
            .iter()
            .filter(|(n, run)| !self.down.contains(n) && run.steps == COPY_STEPS)
            .map(|(_, run)| run.copy)
            .collect();
        for copy in Self::pick(cx, &eligible) {
            self.report(copy);
        }
    }

    /// The replicas read the register: a new replica in the set ends its
    /// copy (its copy is a member's now), and a replica in no set with no
    /// copy, or of a trimmed segment, drops the segment.
    fn drop_copies(&mut self, cx: &mut Cx<'_, Self>) {
        let set = self.set();
        let trimmed = self.trimmed();
        if trimmed {
            for node in self.copies.keys().copied().collect::<Vec<_>>() {
                if !self.down.contains(&node) {
                    self.copies.remove(&node);
                }
            }
        }
        for node in &set {
            if !self.down.contains(node) {
                self.copies.remove(node);
            }
        }
        let eligible: Vec<NodeId> = self
            .holds
            .iter()
            .filter(|n| {
                !self.down.contains(n)
                    && (trimmed || (!set.contains(n) && !self.copies.contains_key(n)))
            })
            .copied()
            .collect();
        for node in Self::pick(cx, &eligible) {
            sometimes!(nodes(SET).contains(&node) && !trimmed, reach::DROPPED);
            self.holds.remove(&node);
        }
    }

    fn fail(&mut self, cx: &mut Cx<'_, Self>) {
        if self.fails >= FAILS {
            return;
        }
        let set = self.set();
        let set_down = set.iter().filter(|n| self.down.contains(n)).count();
        let eligible: Vec<NodeId> = nodes(SET)
            .into_iter()
            .chain(nodes(CANDIDATES))
            .filter(|n| !self.down.contains(n))
            // At most F members of the set down at once.
            .filter(|n| !set.contains(n) || set_down < F)
            .collect();
        let Some(&node) = cx.rng().pick(&eligible) else {
            return;
        };
        self.fails += 1;
        self.down.insert(node);
        if cx.rng().chance(1, 2) {
            self.lost.insert(node);
            sometimes!(set.contains(&node), reach::LOST);
        }
    }

    fn recover(&mut self, cx: &mut Cx<'_, Self>) {
        let eligible: Vec<NodeId> = self.down.difference(&self.lost).copied().collect();
        for node in Self::pick(cx, &eligible) {
            // A member that is down is still in the set: it comes back only
            // if no more than F members are down after.
            self.down.remove(&node);
        }
    }

    fn trim(&mut self, cx: &mut Cx<'_, Self>) {
        if self.trimmed() || !cx.rng().chance(1, 4) {
            return;
        }
        let healing = self.ctls.iter().any(|ctl| ctl.role.heal().is_some());
        let value = self
            .register
            .apply(&Change::Trim {
                segment: SEGMENT,
                first_seq: Seq(0),
            })
            .expect("a retired segment trims");
        sometimes!(healing, reach::TRIMMED);
        self.register = value;
    }

    /// What the walk still misses to have settled.
    fn unsettled(&self) -> Option<String> {
        if !self.requests.is_empty() || !self.replies.is_empty() {
            return Some("a message in flight".into());
        }
        let Some(holder) = self.live_holder() else {
            return Some("no live controller holds the term".into());
        };
        if self.ctls[holder].role.heal().is_some() {
            return Some("a heal in flight".into());
        }
        if self.trimmed() {
            let kept: Vec<&NodeId> = self
                .holds
                .iter()
                .filter(|n| !self.down.contains(n))
                .collect();
            return (!kept.is_empty()).then(|| format!("{kept:?} keep a trimmed segment"));
        }
        let set = self.set();
        let down: Vec<&NodeId> = set.iter().filter(|n| self.down.contains(n)).collect();
        if !down.is_empty() {
            return Some(format!("{down:?} of the set {set:?} are down"));
        }
        None
    }
}

impl Model for HealModel {
    type Msg = ();
    type Node = usize;
    type Action = HealAction;
    type Event = ();

    fn new(cx: &mut Cx<'_, Self>) -> Self {
        Self::build(HealMutant::None, cx)
    }

    fn actions() -> &'static [HealAction] {
        &[
            HealAction::Campaign,
            HealAction::Step,
            HealAction::Process,
            HealAction::Receive,
            HealAction::LoseReq,
            HealAction::LoseRep,
            HealAction::Copy,
            HealAction::Report,
            HealAction::Drop,
            HealAction::Fail,
            HealAction::Recover,
            HealAction::Trim,
        ]
    }

    fn weight(action: HealAction, phase: Phase) -> u32 {
        use HealAction as A;
        match (action, phase) {
            (A::Step | A::Process | A::Receive | A::Copy, Phase::Chaos | Phase::Quiet) => 8,
            (A::Report | A::Drop, Phase::Chaos | Phase::Quiet) => 4,
            (A::Campaign, Phase::Chaos | Phase::Quiet) => 3,
            (A::LoseReq | A::LoseRep | A::Fail | A::Recover, Phase::Chaos) => 2,
            (A::Trim, Phase::Chaos) => 1,
            (A::LoseReq | A::LoseRep | A::Fail | A::Recover | A::Trim, _) => 0,
            // Settle: each round makes every other move once.
            (_, _) => 1,
        }
    }

    fn act(&mut self, action: HealAction, cx: &mut Cx<'_, Self>) {
        match action {
            HealAction::Campaign => self.campaign(cx),
            HealAction::Step => self.step(cx),
            HealAction::Process => self.process(cx),
            HealAction::Receive => self.receive(cx),
            HealAction::LoseReq => self.lose(cx, true),
            HealAction::LoseRep => self.lose(cx, false),
            HealAction::Copy => self.copy(cx),
            HealAction::Report => self.report_again(cx),
            HealAction::Drop => self.drop_copies(cx),
            HealAction::Fail => self.fail(cx),
            HealAction::Recover => self.recover(cx),
            HealAction::Trim => self.trim(cx),
        }
    }

    fn deliver(&mut self, (): (), _cx: &mut Cx<'_, Self>) {
        unreachable!("the walk sends no message of the engine");
    }

    fn nodes(&self) -> Vec<usize> {
        (0..self.ctls.len()).collect()
    }

    fn is_up(&self, &c: &usize) -> bool {
        self.ctls[c].up
    }

    fn crash(&mut self, &c: &usize, _cx: &mut Cx<'_, Self>) {
        let ctl = &mut self.ctls[c];
        ctl.up = false;
        ctl.role = Controller::new(ctl.role.me());
        ctl.sent.clear();
    }

    fn restart(&mut self, &c: &usize, _cx: &mut Cx<'_, Self>) {
        self.ctls[c].up = true;
    }

    fn check(&mut self) {
        if self.trimmed() {
            return;
        }
        for member in self.set() {
            assert!(
                self.holds.contains(&member),
                "{HELD}: {member:?} is in the set {:?} with no complete copy",
                self.set()
            );
        }
    }

    fn converged(&self) -> Result<(), String> {
        match self.unsettled() {
            Some(missing) => Err(format!("{CONVERGES}: {missing}")),
            None => Ok(()),
        }
    }

    fn describe(&self) -> String {
        format!(
            "term {:?}, segment 1 {:?} set {:?}, holds {:?}, copies {:?}, down {:?}, lost {:?}",
            self.register.term(),
            self.record().state(),
            self.set(),
            self.holds,
            self.copies
                .iter()
                .map(|(n, run)| (n, run.copy, run.steps))
                .collect::<Vec<_>>(),
            self.down,
            self.lost
        )
    }
}
