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
//! - [`COMPARED`]: a heal writes its new set only if no other heal changed
//!   the set since its controller planned it (the walk counts the set
//!   changes it sees; a set that comes back to an earlier value is a new
//!   change).
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
/// Oracle: a heal writes its set only on the set its controller read: no
/// set change came between the plan and the write.
pub const COMPARED: &str = "a heal writes its set only on the set its controller read";
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
    /// The register refused a heal whose set came back to the one it read.
    pub const SET_CAME_BACK: &str = "heal walk: a heal refused on a set that came back";
    /// The segment was trimmed during a heal.
    pub const TRIMMED: &str = "heal walk: the segment trimmed during a heal";
    /// A node ended a copy of a term that is over, with no abandon.
    pub const ORPHAN_ENDED: &str = "heal walk: an orphan copy ended";
    /// A node refused a copy ask of a term that is over.
    pub const ORPHAN_REFUSED: &str = "heal walk: a copy ask of an old term refused";
    /// An abandon waited for its node to come back.
    pub const LATE_ABANDON: &str = "heal walk: an abandon reached a node that came back";

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
        SET_CAME_BACK,
        TRIMMED,
        ORPHAN_ENDED,
        ORPHAN_REFUSED,
        LATE_ABANDON,
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
    /// The register compares the record read with no count of its heals:
    /// a set that came back to an earlier value looks unchanged, and a
    /// delayed copy of an old heal applies again. [`HELD`] catches it.
    NoHealCount,
    /// The driver never sends a heal's compare-and-set again.
    /// [`CONVERGES`] catches it.
    NoResend,
    /// A node never ends a copy of a term that is over: a copy whose
    /// controller crashed stays for ever. [`CONVERGES`] catches it.
    KeepOrphans,
    /// A node takes the ask of the copy it runs again and starts it over.
    /// The oracles pass it (the copy reports only once it is complete); the
    /// trace check refuses it: `Join` takes only a higher copy.
    SameCopyAgain,
}

/// The numbers of the walk's host (§J6, **Open** for a real host).
const TUNABLES: Tunables = crate::controller::TUNABLES;

/// The F of the segment.
const F: usize = 1;
/// The segment that heals.
const SEGMENT: SegmentId = SegmentId(1);
/// Its first replica set.
const SET: [u64; 3] = [11, 12, 13];
/// The machines outside the first set that can host a new replica.
const SPARES: std::ops::RangeInclusive<u64> = 20..=25;
/// The steps a copy takes from a live source.
const COPY_STEPS: u8 = 2;
/// The first controller id.
const CONTROLLER: u64 = 900;
/// The controllers.
const CONTROLLERS: usize = 2;
/// The most machine failures of a run.
const FAILS: usize = 4;
/// The most failures of a new replica while it copies, besides.
const COPY_FAILS: usize = 2;

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
    /// The network delivers a request twice: a copy waits in the network.
    Duplicate,
    /// A copy that waited in the network arrives, late.
    Late,
    /// A copy moves one step.
    Copy,
    /// A new replica reports its complete copy again.
    Report,
    /// A replica in no set and with no copy drops the segment.
    Drop,
    /// An abandon that waited for its node reaches it.
    Abandon,
    /// A machine fails.
    Fail,
    /// A new replica fails while it copies.
    FailCopying,
    /// A failed machine comes back with its disk.
    Recover,
    /// The journal trims the segment.
    Trim,
}

/// One step of the walk, as an action of `specs/tla/heal-reuse/`
/// (`HealTrace.tla`). A copy is the code's [`CopyId`]; the trace numbers
/// the plans of all terms in one count, as the model does.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HealEvent {
    /// Node `n` read the register (`Read`).
    Read(NodeId),
    /// Node `n` started the copy `copy` (`Join`).
    Join(NodeId, CopyId),
    /// The copy of node `n` took one step (`CopyStep`).
    CopyStep(NodeId),
    /// Node `n` reported its complete copy (`Report`).
    Report(NodeId),
    /// An abandon ended the copy of node `n` (`Abandon`).
    Abandon(NodeId),
    /// Node `n` ended a copy of a term that is over (`Ends`).
    Ends(NodeId),
    /// Node `n` dropped the segment (`Drop`).
    Drop(NodeId),
    /// Machine `n` failed (`Crash`).
    Crash(NodeId),
    /// Machine `n` came back with its disk (`Restart`).
    Restart(NodeId),
    /// Controller `c` won the next term (`Elect`).
    Elect(usize),
    /// Controller `c` crashed (`CtlCrash`).
    CtlCrash(usize),
    /// Controller `c` planned a heal (`Plan`).
    Plan {
        /// The controller.
        c: usize,
        /// The set it read.
        read: Vec<NodeId>,
        /// The heals of the record it read.
        heals: u64,
        /// The member that is down.
        remove: NodeId,
        /// The new replica.
        add: NodeId,
        /// Its copy.
        copy: CopyId,
    },
    /// Controller `c` counted the report of its copy and sent its
    /// compare-and-set (`Send`).
    Send(usize),
    /// The register took a compare-and-set and answered `ok` (`Apply`):
    /// the set is `set` after it.
    Apply {
        /// The term of the change.
        term: u64,
        /// The set it read.
        read: Vec<NodeId>,
        /// The heals of the record it read.
        heals: u64,
        /// The member out.
        remove: NodeId,
        /// The member in.
        add: NodeId,
        /// Whether the register answered "ok".
        ok: bool,
        /// The set after.
        set: Vec<NodeId>,
    },
    /// An answer `ok` ended the heal of controller `c` (`Answer`).
    Answer(usize, bool),
    /// Controller `c`'s view shows its heal done (`Done`).
    Done(usize),
    /// Controller `c` ended its heal with no write (`Abort`).
    Abort(usize),
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
    /// Whether the mutant `SameCopyAgain` started this copy over.
    again: bool,
}

/// The heal walk.
pub struct HealModel {
    mutant: HealMutant,
    register: RegisterValue,
    ctls: Vec<Ctl>,
    requests: Vec<Flight>,
    replies: Vec<Flight>,
    /// The copies of requests that wait in the network.
    delayed: Vec<Flight>,
    /// The set changes the walk saw.
    changes: u64,
    /// For each plan (term, new replica, set read), the set changes the walk
    /// had seen when the controller asked its copy.
    planned: BTreeMap<(u64, NodeId, Vec<NodeId>), u64>,
    /// The machines that hold a complete copy of the segment.
    holds: BTreeSet<NodeId>,
    /// The copy each new replica runs.
    copies: BTreeMap<NodeId, Copying>,
    down: BTreeSet<NodeId>,
    /// The machines lost for good.
    lost: BTreeSet<NodeId>,
    fails: usize,
    copy_fails: usize,
    /// The abandons that wait for their node to come back.
    abandons: Vec<(NodeId, CopyId)>,
    /// Every copy ask sent: the network can deliver each again, late.
    asked: Vec<(NodeId, Vec<NodeId>, CopyId)>,
    /// The writes of the record so far, and the write that each node last
    /// read.
    writes: u64,
    seen: BTreeMap<NodeId, u64>,
    /// Whether the journal can trim the segment: the traced walk cannot
    /// (`heal-reuse/` has no trim).
    trims: bool,
    /// The events of the move, recorded at its end.
    events: Vec<HealEvent>,
}

/// The heal walk with `mutant` planted, over `seeds` seeds of `steps` chaos
/// steps.
#[must_use]
pub fn walk(mutant: HealMutant, seeds: u64, steps: u64) -> Walk<HealModel> {
    build(mutant, seeds, steps, true)
}

/// The traced heal walk: the walk with no trim, as `heal-reuse/` has none.
#[must_use]
pub fn traced(mutant: HealMutant, seeds: u64, steps: u64) -> Walk<HealModel> {
    build(mutant, seeds, steps, false)
}

fn build(mutant: HealMutant, seeds: u64, steps: u64, trims: bool) -> Walk<HealModel> {
    Walk::<HealModel>::new()
        .seeds(seeds)
        .chaos_steps(steps)
        .quiet_steps(100)
        // No message of the engine: the walk's moves are its own.
        .deliver_weights(0, 0)
        .crash_weights(1, 3)
        .max_down(1)
        .env_prefix(None)
        .first_seed(crate::campaign::first_seed())
        .model(move |cx| HealModel::build(mutant, trims, cx))
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
    /// The walk with `mutant` planted; the journal trims the segment when
    /// `trims`.
    pub fn build(mutant: HealMutant, trims: bool, _cx: &mut Cx<'_, Self>) -> Self {
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
            delayed: Vec::new(),
            changes: 0,
            planned: BTreeMap::new(),
            holds: nodes(SET).into_iter().collect(),
            copies: BTreeMap::new(),
            down: BTreeSet::new(),
            lost: BTreeSet::new(),
            fails: 0,
            copy_fails: 0,
            abandons: Vec::new(),
            asked: Vec::new(),
            writes: 0,
            seen: BTreeMap::new(),
            trims,
            events: Vec::new(),
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
            self.events.push(HealEvent::Elect(c));
            self.ctls[c].role.elected(&Outcome::Done(value));
        }
    }

    fn liveness(&self) -> BTreeMap<NodeId, Liveness> {
        let all = nodes(1..=6)
            .into_iter()
            .chain(nodes(SET))
            .chain(nodes(14..=16))
            .chain(nodes(SPARES));
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
        // The members of the first set come first: a removed member that
        // came back is the first candidate, so a set can return to an
        // earlier value.
        let candidates: Vec<NodeId> = nodes(SET).into_iter().chain(nodes(SPARES)).collect();
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
        let before = ctl.role.heal().cloned();
        let (ask, _) = ctl.role.step(&view, &TUNABLES);
        match (before, ctl.role.heal()) {
            (None, Some(heal)) => self.events.push(HealEvent::Plan {
                c,
                read: heal.read.replicas().to_vec(),
                heals: heal.read.heals(),
                remove: heal.remove,
                add: heal.add,
                copy: heal.copy,
            }),
            (Some(_), None) if matches!(ask, Ask::Abandon { .. }) => {
                self.events.push(HealEvent::Abort(c));
            }
            (Some(_), None) => self.events.push(HealEvent::Done(c)),
            _ => {}
        }
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
                // The view is the register as it is, so the plan read the
                // set after the changes seen so far.
                self.planned
                    .insert((copy.term, node, set.clone()), self.changes);
                let ask = (node, set.clone(), copy);
                if !self.asked.contains(&ask) {
                    self.asked.push(ask);
                }
                self.join(node, set, copy);
            }
            Ask::Abandon {
                segment,
                node,
                copy,
            } => {
                assert_eq!(segment, SEGMENT);
                // A node that is down gets the abandon once it is back.
                if self.down.contains(&node) {
                    self.abandons.push((node, copy));
                } else {
                    self.abandon(node, copy);
                }
            }
        }
    }

    /// Node `node`, which is up, gets the abandon of `copy`: it reads the
    /// register, and ends the copy if it still runs it.
    fn abandon(&mut self, node: NodeId, copy: CopyId) {
        assert!(!self.down.contains(&node), "an abandon reaches a live node");
        self.read(node);
        if self.copies.get(&node).is_some_and(|run| run.copy == copy) {
            self.copies.remove(&node);
            self.events.push(HealEvent::Abandon(node));
        }
    }

    /// Node `node`, which is up, reads the register: a set that names it
    /// ends its copy (it is a member), and a term above its copy's ends the
    /// copy (no plan of an old term commits it; mutant `KeepOrphans`). A
    /// trimmed segment ends every copy.
    fn read(&mut self, node: NodeId) {
        assert!(
            !self.down.contains(&node),
            "a node that is down reads nothing"
        );
        let seen = self.seen.entry(node).or_insert(0);
        if *seen < self.writes {
            *seen = self.writes;
            self.events.push(HealEvent::Read(node));
        }
        if self.set().contains(&node) || self.trimmed() {
            self.copies.remove(&node);
            return;
        }
        let term = self.register.term().number;
        if self.mutant != HealMutant::KeepOrphans
            && self
                .copies
                .get(&node)
                .is_some_and(|run| run.copy.term < term)
        {
            sometimes!(true, reach::ORPHAN_ENDED);
            self.copies.remove(&node);
            self.events.push(HealEvent::Ends(node));
        }
    }

    /// Node `node` starts the copy `copy`, from `set`, unless it runs that
    /// copy or a higher one.
    fn join(&mut self, node: NodeId, set: Vec<NodeId>, copy: CopyId) {
        if self.down.contains(&node) {
            return;
        }
        self.read(node);
        if self.set().contains(&node) {
            // A late ask reaches a member: a node never copies a segment
            // that it serves (`SegmentReplica::join`).
            return;
        }
        if copy.term < self.register.term().number && self.mutant != HealMutant::KeepOrphans {
            sometimes!(true, reach::ORPHAN_REFUSED);
            return;
        }
        if let Some(run) = self.copies.get(&node) {
            if run.copy == copy && self.mutant != HealMutant::SameCopyAgain {
                return;
            }
            if run.copy > copy && self.mutant != HealMutant::AnyCopy {
                sometimes!(true, reach::LOWER_REFUSED);
                return;
            }
            sometimes!(run.copy < copy, reach::HIGHER_TAKEN);
            if run.copy == copy && (run.steps == COPY_STEPS || run.again) {
                // A complete copy has reported: the mutant starts over, once,
                // only a copy that is not complete yet.
                return;
            }
        }

        // A new copy starts on an empty disk.
        self.holds.remove(&node);
        self.events.push(HealEvent::Join(node, copy));
        let again = self.copies.get(&node).is_some_and(|run| run.copy == copy);
        self.copies.insert(
            node,
            Copying {
                copy,
                set,
                steps: 0,
                again,
            },
        );
        if self.mutant == HealMutant::EarlyReport {
            self.report(node, copy);
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

    /// New replica `node` reports its complete copy `copy` to every live
    /// controller.
    fn report(&mut self, node: NodeId, copy: CopyId) {
        self.events.push(HealEvent::Report(node));
        for c in self.up_ctls(|_| true) {
            let before = self.ctls[c].role.heal().is_some_and(|heal| heal.complete);
            let ask = self.ctls[c].role.copied(SEGMENT, copy);
            if !before && self.ctls[c].role.heal().is_some_and(|heal| heal.complete) {
                self.events.push(HealEvent::Send(c));
            }
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

    /// The register's answer to a heal, through the planted mutant of the compare-and-set.
    fn register_answer(&self, change: &Change) -> Result<RegisterValue, ChangeRefusal> {
        let Change::Rereplicate { remove, add, .. } = change else {
            panic!("a controller sends only heals here: {change:?}");
        };
        if self.mutant == HealMutant::NoCompare {
            let mut record = self.record().clone();
            record
                .replace_replica(*remove, *add)
                .map(|()| {
                    let mut segments = self.register.segments().clone();
                    segments.insert(SEGMENT, record);
                    RegisterValue::from_parts(self.register.term(), segments)
                })
                .map_err(|refusal| ChangeRefusal::Record(SEGMENT, refusal))
        } else if let (HealMutant::NoHealCount, Change::Rereplicate { read, .. }) =
            (self.mutant, change)
        {
            let held = self.record();
            let same = read.replicas() == held.replicas()
                && read.state() == held.state()
                && read.end() == held.end()
                && read.boundary() == held.boundary();
            let mut change = change.clone();
            if same && let Change::Rereplicate { read, .. } = &mut change {
                *read = held.clone();
            }
            self.register.apply(&change)
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
        }
    }

    fn apply(&mut self, change: &Change) -> Outcome {
        let result = self.register_answer(change);
        let Change::Rereplicate {
            read,
            term,
            remove,
            add,
            ..
        } = change
        else {
            unreachable!("a heal");
        };
        self.events.push(HealEvent::Apply {
            term: *term,
            read: read.replicas().to_vec(),
            heals: read.heals(),
            remove: *remove,
            add: *add,
            ok: result.is_ok(),
            set: result.as_ref().map_or_else(
                |_| self.set(),
                |value| {
                    value
                        .segment(SEGMENT)
                        .expect("segment 1")
                        .replicas()
                        .to_vec()
                },
            ),
        });
        match result {
            Ok(value) => {
                let before = self.set();
                if value != self.register {
                    self.writes += 1;
                }
                self.register = value.clone();
                let after = self.set();
                if before != after {
                    let Change::Rereplicate { read, term, .. } = change else {
                        unreachable!("a heal");
                    };
                    let plan = (*term, *add, read.replicas().to_vec());
                    assert_eq!(
                        self.planned.get(&plan),
                        Some(&self.changes),
                        "{COMPARED}: the heal {plan:?} was planned at another set"
                    );
                    self.changes += 1;
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
                if let Change::Rereplicate { read, .. } = change {
                    sometimes!(
                        matches!(refusal, ChangeRefusal::Stale(_))
                            && read.replicas() == self.record().replicas(),
                        reach::SET_CAME_BACK
                    );
                }
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
                let before = self.ctls[reply.c].role.heal().is_some();
                let ask = self.ctls[reply.c]
                    .role
                    .rereplicated(&reply.change, &outcome);
                if before && self.ctls[reply.c].role.heal().is_none() {
                    self.events.push(HealEvent::Answer(
                        reply.c,
                        matches!(outcome, Outcome::Done(_)),
                    ));
                }
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

    fn duplicate(&mut self, cx: &mut Cx<'_, Self>) {
        let eligible: Vec<usize> = (0..self.requests.len()).collect();
        for i in Self::pick(cx, &eligible) {
            let copy = self.requests[i].clone();
            self.delayed.push(copy);
        }
    }

    fn late(&mut self, cx: &mut Cx<'_, Self>) {
        // A copy ask arrives again, late, in the chaos: half the time one
        // that is lower than the copy its node runs now, when there is one.
        if cx.phase() == Phase::Chaos {
            let lower: Vec<(NodeId, Vec<NodeId>, CopyId)> = self
                .asked
                .iter()
                .filter(|(node, _, copy)| self.copies.get(node).is_some_and(|run| run.copy > *copy))
                .cloned()
                .collect();
            let pool = if !lower.is_empty() && cx.rng().chance(1, 2) {
                lower
            } else {
                self.asked.clone()
            };
            if let Some((node, set, copy)) = cx.rng().pick(&pool).cloned() {
                self.join(node, set, copy);
            }
        }
        // A heal whose read set is the set again, on a record that changed,
        // arrives now: the set came back to its read (steer to
        // `SET_CAME_BACK`).
        let record = self.record().clone();
        if let Some(i) = self.delayed.iter().position(|m| {
            matches!(&m.change, Change::Rereplicate { read, .. }
                if read.replicas() == record.replicas() && *read != record)
        }) {
            let copy = self.delayed.remove(i);
            self.requests.push(copy);
        }
        // In the chaos a copy waits through several heals: a long delay is
        // what lets a heal arrive on a set that came back to its read.
        if cx.phase() == Phase::Chaos && !cx.rng().chance(1, 4) {
            return;
        }
        let eligible: Vec<usize> = (0..self.delayed.len()).collect();
        let mut picked = Self::pick(cx, &eligible);
        picked.sort_unstable_by(|a, b| b.cmp(a));
        for i in picked {
            let copy = self.delayed.remove(i);
            self.requests.push(copy);
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
            let (steps, copy) = (run.steps, run.copy);
            self.events.push(HealEvent::CopyStep(node));
            if steps == COPY_STEPS {
                self.holds.insert(node);
                if self.mutant != HealMutant::EarlyReport {
                    self.report(node, copy);
                }
            }
        }
    }

    fn report_again(&mut self, cx: &mut Cx<'_, Self>) {
        let eligible: Vec<(NodeId, CopyId)> = self
            .copies
            .iter()
            .filter(|(n, run)| !self.down.contains(n) && run.steps == COPY_STEPS)
            .map(|(n, run)| (*n, run.copy))
            .collect();
        for (node, copy) in Self::pick(cx, &eligible) {
            self.report(node, copy);
        }
    }

    /// The abandons that waited reach the nodes that are back.
    fn late_abandons(&mut self, cx: &mut Cx<'_, Self>) {
        let eligible: Vec<usize> = (0..self.abandons.len())
            .filter(|&i| !self.down.contains(&self.abandons[i].0))
            .collect();
        let mut picked = Self::pick(cx, &eligible);
        picked.sort_unstable_by(|a, b| b.cmp(a));
        for i in picked {
            let (node, copy) = self.abandons.remove(i);
            sometimes!(true, reach::LATE_ABANDON);
            self.abandon(node, copy);
        }
    }

    /// The replicas read the register ([`Self::read`]), and a replica in no
    /// set with no copy, or of a trimmed segment, drops the segment.
    fn drop_copies(&mut self, cx: &mut Cx<'_, Self>) {
        let eligible: Vec<NodeId> = self
            .holds
            .iter()
            .chain(self.copies.keys())
            .filter(|n| !self.down.contains(n))
            .copied()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        for node in Self::pick(cx, &eligible) {
            self.read(node);
            let trimmed = self.trimmed();
            if self.holds.contains(&node)
                && (trimmed || (!self.set().contains(&node) && !self.copies.contains_key(&node)))
            {
                sometimes!(nodes(SET).contains(&node) && !trimmed, reach::DROPPED);
                self.holds.remove(&node);
                self.events.push(HealEvent::Drop(node));
            }
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
            .chain(nodes(SPARES))
            .filter(|n| !self.down.contains(n))
            // At most F members of the set down at once.
            .filter(|n| !set.contains(n) || set_down < F)
            .collect();
        let Some(&node) = cx.rng().pick(&eligible) else {
            return;
        };
        self.fails += 1;
        self.down.insert(node);
        self.events.push(HealEvent::Crash(node));
        if cx.rng().chance(1, 2) {
            self.lost.insert(node);
            sometimes!(set.contains(&node), reach::LOST);
        }
    }

    /// A new replica fails while it copies, and comes back later: its heal
    /// ends, and a later plan of the same term can ask it again, while the
    /// ask of its first copy is still in the network.
    fn fail_copying(&mut self, cx: &mut Cx<'_, Self>) {
        if self.copy_fails >= COPY_FAILS {
            return;
        }
        let copying: Vec<NodeId> = self
            .copies
            .iter()
            .filter(|(n, run)| !self.down.contains(n) && run.steps < COPY_STEPS)
            .map(|(n, _)| *n)
            .collect();
        let Some(&node) = cx.rng().pick(&copying) else {
            return;
        };
        self.copy_fails += 1;
        self.down.insert(node);
        self.events.push(HealEvent::Crash(node));
    }

    fn recover(&mut self, cx: &mut Cx<'_, Self>) {
        let eligible: Vec<NodeId> = self.down.difference(&self.lost).copied().collect();
        for node in Self::pick(cx, &eligible) {
            // A member that is down is still in the set: it comes back only
            // if no more than F members are down after.
            self.down.remove(&node);
            self.events.push(HealEvent::Restart(node));
        }
    }

    fn trim(&mut self, cx: &mut Cx<'_, Self>) {
        if !self.trims || self.trimmed() || !cx.rng().chance(1, 4) {
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
        self.writes += 1;
    }

    /// What the walk still misses to have settled.
    fn unsettled(&self) -> Option<String> {
        if !self.requests.is_empty()
            || !self.replies.is_empty()
            || !self.delayed.is_empty()
            // An abandon waits while its node is down; the walk brings no
            // machine back once the faults stop.
            || self.abandons.iter().any(|(n, _)| !self.down.contains(n))
        {
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
        // No live node outside the set keeps the segment: an orphan copy
        // ends, and a node with no copy drops what it holds.
        let term = self.register.term().number;
        let orphans: Vec<&NodeId> = self
            .copies
            .iter()
            .filter(|(n, run)| !self.down.contains(n) && run.copy.term < term)
            .map(|(n, _)| n)
            .collect();
        if !orphans.is_empty() {
            return Some(format!("{orphans:?} run a copy of a term that is over"));
        }
        let kept: Vec<&NodeId> = self
            .holds
            .iter()
            .filter(|n| !self.down.contains(n) && !set.contains(n) && !self.copies.contains_key(n))
            .collect();
        if !kept.is_empty() {
            return Some(format!("{kept:?} keep a copy outside the set"));
        }
        None
    }
}

impl Model for HealModel {
    type Msg = ();
    type Node = usize;
    type Action = HealAction;
    type Event = HealEvent;

    fn new(cx: &mut Cx<'_, Self>) -> Self {
        Self::build(HealMutant::None, true, cx)
    }

    fn actions() -> &'static [HealAction] {
        &[
            HealAction::Campaign,
            HealAction::Step,
            HealAction::Process,
            HealAction::Receive,
            HealAction::LoseReq,
            HealAction::LoseRep,
            HealAction::Duplicate,
            HealAction::Late,
            HealAction::Copy,
            HealAction::Report,
            HealAction::Drop,
            HealAction::Abandon,
            HealAction::Fail,
            HealAction::FailCopying,
            HealAction::Recover,
            HealAction::Trim,
        ]
    }

    fn weight(action: HealAction, phase: Phase) -> u32 {
        use HealAction as A;
        match (action, phase) {
            (A::Step | A::Process | A::Receive | A::Copy, Phase::Chaos | Phase::Quiet) => 8,
            // The quiet flushes the late copies of requests.
            (A::Report | A::Drop | A::Abandon, Phase::Chaos | Phase::Quiet)
            | (A::Late, Phase::Quiet) => 4,
            (A::Campaign, Phase::Chaos | Phase::Quiet) => 3,
            (A::LoseReq | A::LoseRep | A::Duplicate | A::Fail | A::Recover, Phase::Chaos) => 2,
            // A late copy waits long in the chaos.
            (A::Trim | A::Late | A::FailCopying, Phase::Chaos) => 1,
            (
                A::LoseReq
                | A::LoseRep
                | A::Duplicate
                | A::Fail
                | A::FailCopying
                | A::Recover
                | A::Trim,
                _,
            ) => 0,
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
            HealAction::Duplicate => self.duplicate(cx),
            HealAction::Late => self.late(cx),
            HealAction::Copy => self.copy(cx),
            HealAction::Report => self.report_again(cx),
            HealAction::Drop => self.drop_copies(cx),
            HealAction::Fail => self.fail(cx),
            HealAction::FailCopying => self.fail_copying(cx),
            HealAction::Recover => self.recover(cx),
            HealAction::Abandon => self.late_abandons(cx),
            HealAction::Trim => self.trim(cx),
        }
        for event in std::mem::take(&mut self.events) {
            cx.record(event);
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

    fn crash(&mut self, &c: &usize, cx: &mut Cx<'_, Self>) {
        let ctl = &mut self.ctls[c];
        ctl.up = false;
        ctl.role = Controller::new(ctl.role.me());
        ctl.sent.clear();
        cx.record(HealEvent::CtlCrash(c));
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
