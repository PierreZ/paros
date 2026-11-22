//! The register lab of the site's `/journal/matchmakers/` page (#1364
//! (register and segment record lab)): two controllers run operations on
//! the journal's matchmaker register, and the reader delivers or drops each
//! read and each write.
//!
//! The world is a driver of `paros_journal::register`. Each matchmaker is a
//! [`RegisterMember`], each operation a [`RegisterCall`]; a request reaches
//! a member through [`RegisterMember::step`] and its reply reaches the
//! operation through [`RegisterCall::on_reply`]. The world keeps the
//! requests in flight and what each controller learned. It holds no rule of
//! the register: every reply, every guard and every "done" comes from
//! `paros-journal`.
//!
//! A controller opens an operation at a generation one round above the
//! highest it knows: its own last one, or the one a conflict named. One
//! matchmaker set (generation 1); no handover runs here.

use std::collections::{BTreeMap, BTreeSet};

use paros_journal::register::{
    Ballot, CallPhase, CallStep, Change, ChangeRefusal, MatchmakerGeneration, MatchmakerId,
    MatchmakerSet, MemberState, RegisterCall, RegisterMember, RegisterValue, Reply, Request,
};
use paros_journal::{Grid, NodeId, OpeningCall, RELEASE};
use serde::Serialize;

use super::keyed::{Action, Exercise, World};
use super::records::state_text;

/// The three matchmakers.
const MEMBERS: [u64; 3] = [1, 2, 3];

/// The two controllers, by name. The node id of controller `i` is `i + 1`.
const CALLERS: [&str; 2] = ["P", "Q"];

/// What an operation asks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Ask {
    /// `Change::Elect`: raise the controller term and hold it.
    Elect,
    /// `Change::Reshape`: open the next segment, with the term the
    /// controller won and the newest segment it saw done.
    Reshape,
}

/// What one controller holds.
#[derive(Clone, Debug)]
struct Caller {
    node: NodeId,
    /// The highest operation round it knows.
    round: u64,
    /// The operation in flight, or the last one.
    call: Option<(Ask, RegisterCall)>,
    /// The last step of that operation that the controller heard.
    last: Option<CallStep>,
    /// The term it won: the term of its own `Elect` seen done.
    won: u64,
    /// The last value it saw done.
    seen: RegisterValue,
}

/// A request on its way to a member.
#[derive(Clone, Debug)]
struct Flight {
    caller: usize,
    to: MatchmakerId,
    request: Request,
}

/// One move, in words.
#[derive(Clone, Debug, Serialize)]
pub struct EventView {
    /// What moved: `P opens Elect`, `P → m1: read`.
    pub sent: String,
    /// The answer, a short name: `value`, `wrote`, `done`, `Refused`.
    pub answer: String,
    /// Its fields.
    pub detail: String,
    /// Whether the controller heard it.
    pub lost: bool,
    /// The kind of chip the page draws: `done`, `gap` or empty.
    pub tone: String,
}

/// A matchmaker, as the page draws it.
#[derive(Clone, Debug, Serialize)]
pub struct MemberView {
    /// `m1`.
    pub name: String,
    /// Whether it is down.
    pub down: bool,
    /// Its read generation, `round.controller`.
    pub read: String,
    /// The generation of the highest write it took.
    pub written: String,
    /// That write's value.
    pub value: ValueView,
}

/// A value of the register.
#[derive(Clone, Debug, Serialize)]
pub struct ValueView {
    /// `term 1, held by P`.
    pub term: String,
    /// Each segment record, `1 in force`.
    pub segments: Vec<String>,
}

/// A controller, as the page draws it.
#[derive(Clone, Debug, Serialize)]
pub struct CallerView {
    /// `P`.
    pub name: String,
    /// The operation: `Elect at 2.P`, or empty.
    pub op: String,
    /// Where it stands: `reading: 1 of 3 answered`.
    pub phase: String,
    /// What it believes: `holds term 1`.
    pub believes: String,
}

/// The register world, as the page draws it.
#[derive(Clone, Debug, Serialize)]
pub struct RegisterView {
    /// The matchmakers.
    pub members: Vec<MemberView>,
    /// The controllers.
    pub callers: Vec<CallerView>,
    /// The requests in flight.
    pub flight: usize,
    /// Every move, oldest first.
    pub history: Vec<EventView>,
}

/// Two controllers and three matchmakers.
#[derive(Clone, Debug)]
pub struct Register {
    set: MatchmakerSet,
    members: BTreeMap<MatchmakerId, RegisterMember>,
    down: BTreeSet<MatchmakerId>,
    callers: Vec<Caller>,
    flight: Vec<Flight>,
    events: Vec<EventView>,
}

fn member_name(id: MatchmakerId) -> String {
    format!("m{}", id.0)
}

fn caller_name(node: NodeId) -> &'static str {
    usize::try_from(node.0)
        .ok()
        .and_then(|n| n.checked_sub(1))
        .and_then(|i| CALLERS.get(i))
        .copied()
        .unwrap_or("?")
}

/// A generation as the page writes it: `round.controller`, `0` for none.
fn ballot(b: Ballot) -> String {
    if b == Ballot::zero() {
        "0".to_string()
    } else {
        format!("{}.{}", b.round, caller_name(b.node))
    }
}

fn term_text(value: &RegisterValue) -> String {
    let term = value.term();
    match term.holder {
        None => "term 0, no controller".to_string(),
        Some(node) => format!("term {}, held by {}", term.number, caller_name(node)),
    }
}

fn value_view(value: &RegisterValue) -> ValueView {
    ValueView {
        term: term_text(value),
        segments: value
            .segments()
            .values()
            .map(|record| format!("segment {} {}", record.id().0, state_text(record.state())))
            .collect(),
    }
}

/// A value in words, on one line.
fn value_text(value: &RegisterValue) -> String {
    let segments = value_view(value).segments;
    if segments.is_empty() {
        term_text(value)
    } else {
        format!("{}; {}", term_text(value), segments.join(", "))
    }
}

/// A refusal of the guard in words.
#[must_use]
pub fn refusal_text(refusal: &ChangeRefusal) -> String {
    match refusal {
        ChangeRefusal::Term { held, asked } => {
            format!("Term: the register holds term {held}, the call names term {asked}")
        }
        ChangeRefusal::Expected { newest, asked } => format!(
            "Expected: the newest segment is {}, the call expected {}",
            newest.map_or("none".to_string(), |s| s.0.to_string()),
            asked.map_or("none".to_string(), |s| s.0.to_string())
        ),
        ChangeRefusal::Unretired(id) => format!("Unretired: segment {} is not retired", id.0),
        ChangeRefusal::NoSegment(id) => format!("NoSegment: no record of segment {}", id.0),
        ChangeRefusal::NoSuccessor(id) => {
            format!("NoSuccessor: segment {} has no pending successor", id.0)
        }
        ChangeRefusal::Confirm(state) => format!("Confirm: the segment is {}", state_text(*state)),
        ChangeRefusal::Stale(id) => format!(
            "Stale: the record of segment {} changed since the controller read it",
            id.0
        ),
        ChangeRefusal::Record(id, refusal) => {
            format!(
                "Record of segment {}: {}",
                id.0,
                super::records::record_text(refusal)
            )
        }
    }
}

/// A member's reply in words: its name, its fields and its tone.
fn reply_text(to: MatchmakerId, reply: &Reply) -> (&'static str, String, &'static str) {
    match reply {
        Reply::Value { written, .. } => (
            "value",
            format!(
                "{} holds the write of {}: {}",
                member_name(to),
                ballot(written.op),
                value_text(&written.value)
            ),
            "",
        ),
        Reply::Wrote { op } => (
            "wrote",
            format!("{} took the write of {}", member_name(to), ballot(*op)),
            "",
        ),
        Reply::Conflict { read, written, .. } => (
            "Conflict",
            format!(
                "{} read {} and holds a write of {}",
                member_name(to),
                ballot(*read),
                ballot(*written)
            ),
            "gap",
        ),
        other => ("refused", format!("{other:?}"), "gap"),
    }
}

/// The grid and replica set of every segment the lab opens.
fn opening(term: u64, expected: Option<paros_journal::SegmentId>) -> OpeningCall {
    let nodes = |ids: &[u64]| ids.iter().copied().map(NodeId).collect::<Vec<_>>();
    OpeningCall {
        term,
        expected,
        grid: Grid::new(1, vec![nodes(&[11, 12, 13])]).expect("a v1 grid"),
        replicas: nodes(&[21, 22, 23]),
    }
}

impl Register {
    fn caller(name: &str) -> Option<usize> {
        CALLERS.iter().position(|c| *c == name)
    }

    fn member(&self, name: &str) -> Option<MatchmakerId> {
        let id = MatchmakerId(name.strip_prefix('m')?.parse().ok()?);
        self.members.contains_key(&id).then_some(id)
    }

    fn in_flight(&self, caller: usize) -> bool {
        self.callers[caller].call.as_ref().is_some_and(|(_, call)| {
            matches!(
                call.phase(),
                CallPhase::Reading(_) | CallPhase::Writing { .. }
            )
        })
    }

    fn event(&mut self, sent: String, answer: &str, detail: String, lost: bool, tone: &str) {
        self.events.push(EventView {
            sent,
            answer: answer.to_string(),
            detail,
            lost,
            tone: tone.to_string(),
        });
    }

    /// Put the requests of `caller`'s operation in flight, to the members
    /// that have none in flight from it.
    fn send(&mut self, caller: usize) {
        let Some((_, call)) = &self.callers[caller].call else {
            return;
        };
        let requests = call.requests();
        for (to, request) in requests {
            if !self.flight.iter().any(|f| f.caller == caller && f.to == to) {
                self.flight.push(Flight {
                    caller,
                    to,
                    request,
                });
            }
        }
    }

    fn open(&mut self, caller: usize, ask: Ask) -> Result<(), String> {
        let name = CALLERS[caller];
        if self.in_flight(caller) {
            return Err(format!("{name} already runs an operation."));
        }
        let state = &mut self.callers[caller];
        state.round += 1;
        let op = Ballot {
            round: state.round,
            node: state.node,
        };
        let change = match ask {
            Ask::Elect => Change::Elect { holder: state.node },
            Ask::Reshape => Change::Reshape {
                opening: opening(
                    state.won,
                    state.seen.newest().map(paros_journal::SegmentRecord::id),
                ),
                release: RELEASE,
            },
        };
        let what = match &change {
            Change::Reshape { opening, .. } => format!(
                "reshape with term {}, expected segment {}",
                opening.term,
                opening
                    .expected
                    .map_or("none".to_string(), |s| s.0.to_string())
            ),
            _ => "elect: raise the term by one and hold it".to_string(),
        };
        state.call = Some((ask, RegisterCall::new(op, self.set.clone(), change)));
        state.last = None;
        self.flight.retain(|f| f.caller != caller);
        self.send(caller);
        self.event(
            format!("{name} opens operation {}", ballot(op)),
            "read",
            format!("{what}; it reads at every matchmaker"),
            false,
            "",
        );
        Ok(())
    }

    /// Deliver the request of `caller` to `to`; the reply reaches the
    /// operation unless `lose` is set.
    fn deliver(&mut self, caller: usize, to: MatchmakerId, lose: bool) -> Result<(), String> {
        if self.down.contains(&to) {
            return Err(format!("{} is down: it takes no request.", member_name(to)));
        }
        let index = self
            .flight
            .iter()
            .position(|f| f.caller == caller && f.to == to)
            .ok_or_else(|| "No such request is in flight.".to_string())?;
        let flight = self.flight.remove(index);
        let member = self.members.get_mut(&to).expect("a member of the set");
        let what = match &flight.request {
            Request::Read { .. } => "read",
            _ => "write",
        };
        let reply = member.step(flight.request).reply;
        let name = CALLERS[caller];
        let sent = format!("{name} → {}: {what}", member_name(to));
        let (answer, detail, tone) = reply_text(to, &reply);
        self.event(sent, answer, detail, lose, tone);
        if lose {
            return Ok(());
        }
        let state = &mut self.callers[caller];
        let (ask, call) = state.call.as_mut().expect("a request has its operation");
        let ask = *ask;
        let step = call.on_reply(to, reply);
        self.on_step(caller, ask, &step);
        self.callers[caller].last = Some(step);
        Ok(())
    }

    /// What the controller does with one step of its operation.
    fn on_step(&mut self, caller: usize, ask: Ask, step: &CallStep) {
        let name = CALLERS[caller];
        match step {
            CallStep::Pending => {}
            CallStep::Write { seen, value } => {
                let detail = format!(
                    "the highest write a majority showed: {}; it writes {}",
                    value_text(seen),
                    value_text(value)
                );
                self.flight.retain(|f| f.caller != caller);
                self.send(caller);
                self.event(
                    format!("{name}: read done at a majority"),
                    "write",
                    detail,
                    false,
                    "",
                );
            }
            CallStep::Done(value) => {
                let state = &mut self.callers[caller];
                state.seen = value.clone();
                if ask == Ask::Elect {
                    state.won = value.term().number;
                }
                self.flight.retain(|f| f.caller != caller);
                self.event(
                    format!("{name}: write done at a majority"),
                    "done",
                    format!("the value holds: {}", value_text(value)),
                    false,
                    "done",
                );
            }
            CallStep::Refused { refusal, seen } => {
                self.flight.retain(|f| f.caller != caller);
                self.event(
                    format!("{name}: the guard refused the change"),
                    "Refused",
                    format!(
                        "{}; the read saw {}",
                        refusal_text(refusal),
                        value_text(seen)
                    ),
                    false,
                    "gap",
                );
            }
            CallStep::Conflict { above } => {
                let state = &mut self.callers[caller];
                state.round = state.round.max(above.round);
                self.flight.retain(|f| f.caller != caller);
                self.event(
                    format!("{name}: the operation ends"),
                    "Conflict",
                    format!(
                        "a later operation reached a matchmaker; the next one opens above {}",
                        ballot(*above)
                    ),
                    false,
                    "gap",
                );
            }
            CallStep::Moved { .. } => unreachable!("the lab runs no handover"),
        }
    }

    fn drop_request(&mut self, caller: usize, to: MatchmakerId) -> Result<(), String> {
        let index = self
            .flight
            .iter()
            .position(|f| f.caller == caller && f.to == to)
            .ok_or_else(|| "No such request is in flight.".to_string())?;
        let flight = self.flight.remove(index);
        let what = match flight.request {
            Request::Read { .. } => "read",
            _ => "write",
        };
        self.event(
            format!("{} → {}: {what}", CALLERS[caller], member_name(to)),
            "dropped",
            "the request is lost on the way".to_string(),
            true,
            "",
        );
        Ok(())
    }

    /// Whether `caller`'s operation of `ask` is done, with `check` on its
    /// value.
    #[must_use]
    pub fn done(&self, caller: &str, check: impl Fn(&RegisterValue) -> bool) -> bool {
        Self::caller(caller).is_some_and(
            |i| matches!(&self.callers[i].last, Some(CallStep::Done(value)) if check(value)),
        )
    }

    /// Whether `caller`'s last operation ended in a conflict.
    #[must_use]
    pub fn conflicted(&self, caller: &str) -> bool {
        Self::caller(caller)
            .is_some_and(|i| matches!(self.callers[i].last, Some(CallStep::Conflict { .. })))
    }

    /// Whether `caller`'s last operation was refused by the guard, with
    /// `check` on the refusal.
    #[must_use]
    pub fn refused(&self, caller: &str, check: impl Fn(&ChangeRefusal) -> bool) -> bool {
        Self::caller(caller).is_some_and(|i| {
            matches!(&self.callers[i].last, Some(CallStep::Refused { refusal, .. }) if check(refusal))
        })
    }

    /// Whether matchmaker `name` is down.
    #[must_use]
    pub fn is_down(&self, name: &str) -> bool {
        self.member(name).is_some_and(|id| self.down.contains(&id))
    }

    /// The members whose highest write is the one of `caller`'s operation
    /// at `round`.
    #[must_use]
    pub fn holders_of(&self, caller: &str, round: u64) -> usize {
        let Some(i) = Self::caller(caller) else {
            return 0;
        };
        let op = Ballot {
            round,
            node: self.callers[i].node,
        };
        self.members
            .values()
            .filter(|m| m.state().written.op == op)
            .count()
    }

    /// The term holder of the highest write at a majority of members: what
    /// every later read finds.
    #[must_use]
    pub fn settled_holder(&self) -> Option<&'static str> {
        let top = self
            .members
            .values()
            .map(|m| &m.state().written)
            .max_by_key(|w| w.op)?;
        let at = self
            .members
            .values()
            .filter(|m| m.state().written == *top)
            .count();
        (at * 2 > self.members.len())
            .then(|| top.value.term().holder.map(caller_name))
            .flatten()
    }
}

impl World for Register {
    type View = RegisterView;

    fn fresh() -> Self {
        let set = MatchmakerSet::new(
            MatchmakerGeneration(1),
            MEMBERS.iter().copied().map(MatchmakerId).collect(),
        );
        let members = MEMBERS
            .iter()
            .map(|&n| {
                let id = MatchmakerId(n);
                (
                    id,
                    RegisterMember::new(id, MemberState::first(id, set.clone())),
                )
            })
            .collect();
        let callers = (0..CALLERS.len())
            .map(|i| Caller {
                node: NodeId(u64::try_from(i).expect("two callers") + 1),
                round: 0,
                call: None,
                last: None,
                won: 0,
                seen: RegisterValue::new(),
            })
            .collect();
        Self {
            set,
            members,
            down: BTreeSet::new(),
            callers,
            flight: Vec::new(),
            events: Vec::new(),
        }
    }

    fn actions(&self) -> Vec<Action> {
        let mut actions = Vec::new();
        for (i, name) in CALLERS.iter().enumerate() {
            let group = format!("controller {name}");
            if !self.in_flight(i) {
                actions.push(Action {
                    key: format!("{name}:elect"),
                    label: "Elect".to_string(),
                    group: group.clone(),
                });
                actions.push(Action {
                    key: format!("{name}:reshape"),
                    label: format!("Reshape with term {}", self.callers[i].won),
                    group: group.clone(),
                });
            }
            for flight in self.flight.iter().filter(|f| f.caller == i) {
                let to = member_name(flight.to);
                let what = match flight.request {
                    Request::Read { .. } => "read",
                    _ => "write",
                };
                if !self.down.contains(&flight.to) {
                    actions.push(Action {
                        key: format!("{name}>{to}:deliver"),
                        label: format!("{what} → {to}: deliver"),
                        group: group.clone(),
                    });
                    actions.push(Action {
                        key: format!("{name}>{to}:lose"),
                        label: format!("{what} → {to}: lose the reply"),
                        group: group.clone(),
                    });
                }
                actions.push(Action {
                    key: format!("{name}>{to}:drop"),
                    label: format!("{what} → {to}: drop"),
                    group: group.clone(),
                });
            }
        }
        for id in self.members.keys() {
            let name = member_name(*id);
            let (key, label) = if self.down.contains(id) {
                (format!("{name}:restart"), format!("Restart {name}"))
            } else {
                (format!("{name}:crash"), format!("Stop {name}"))
            };
            actions.push(Action {
                key,
                label,
                group: "matchmakers".to_string(),
            });
        }
        actions
    }

    fn act(&mut self, key: &str) -> Result<(), String> {
        let unknown = || format!("There is no move {key:?}.");
        let (who, verb) = key.split_once(':').ok_or_else(unknown)?;
        if let Some(id) = self.member(who) {
            let name = member_name(id);
            return match verb {
                "crash" if !self.down.contains(&id) => {
                    self.down.insert(id);
                    self.event(
                        format!("{name} stops"),
                        "down",
                        "it keeps its disk; it answers nothing until it restarts".to_string(),
                        false,
                        "",
                    );
                    Ok(())
                }
                "restart" if self.down.contains(&id) => {
                    self.down.remove(&id);
                    self.event(
                        format!("{name} restarts"),
                        "up",
                        "from its disk: its read generation and its write".to_string(),
                        false,
                        "",
                    );
                    Ok(())
                }
                _ => Err(unknown()),
            };
        }
        if let Some((caller, member)) = who.split_once('>') {
            let caller = Self::caller(caller).ok_or_else(unknown)?;
            let to = self.member(member).ok_or_else(unknown)?;
            return match verb {
                "deliver" => self.deliver(caller, to, false),
                "lose" => self.deliver(caller, to, true),
                "drop" => self.drop_request(caller, to),
                _ => Err(unknown()),
            };
        }
        let caller = Self::caller(who).ok_or_else(unknown)?;
        match verb {
            "elect" => self.open(caller, Ask::Elect),
            "reshape" => self.open(caller, Ask::Reshape),
            _ => Err(unknown()),
        }
    }

    fn view(&self) -> RegisterView {
        RegisterView {
            members: self
                .members
                .values()
                .map(|member| {
                    let state = member.state();
                    MemberView {
                        name: member_name(member.id()),
                        down: self.down.contains(&member.id()),
                        read: ballot(state.read),
                        written: ballot(state.written.op),
                        value: value_view(&state.written.value),
                    }
                })
                .collect(),
            callers: self
                .callers
                .iter()
                .enumerate()
                .map(|(i, caller)| {
                    let (op, phase) = match &caller.call {
                        None => (String::new(), "no operation yet".to_string()),
                        Some((ask, call)) => {
                            let ask = match ask {
                                Ask::Elect => "Elect",
                                Ask::Reshape => "Reshape",
                            };
                            let phase = match call.phase() {
                                CallPhase::Reading(answers) => {
                                    format!("reading: {} of 3 answered", answers.len())
                                }
                                CallPhase::Writing { acks, .. } => {
                                    format!("writing: {} of 3 took it", acks.len())
                                }
                                CallPhase::Done(_) => "done".to_string(),
                                CallPhase::Ended => match &caller.last {
                                    Some(CallStep::Refused { .. }) => "refused".to_string(),
                                    _ => "ended in a conflict".to_string(),
                                },
                            };
                            (format!("{ask} at {}", ballot(call.op())), phase)
                        }
                    };
                    let believes = if caller.won == 0 {
                        "holds no term".to_string()
                    } else {
                        format!("won term {}", caller.won)
                    };
                    CallerView {
                        name: CALLERS[i].to_string(),
                        op,
                        phase,
                        believes,
                    }
                })
                .collect(),
            flight: self.flight.len(),
            history: self.events.clone(),
        }
    }

    fn events(&self) -> usize {
        self.events.len()
    }
}

/// Every exercise of the register lab, in page order.
#[must_use]
pub fn exercises() -> &'static [Exercise<Register>] {
    &EXERCISES
}

fn elected(holder: &'static str) -> impl Fn(&RegisterValue) -> bool {
    move |value| value.term().holder.map(caller_name) == Some(holder)
}

static EXERCISES: [Exercise<Register>; 4] = [
    Exercise {
        id: "register-down",
        title: "One matchmaker down",
        task: "m3 is down. Make P's Elect done, so that P holds the controller term.",
        done: "Two of three is a majority. The read and the write each need a majority, so any \
               two of them meet in one matchmaker: a later read always finds this write, even \
               if m3 never learns it.",
        setup: &["m3:crash"],
        goal: |r| r.is_down("m3") && r.done("P", elected("P")),
        solution: &[
            "P:elect",
            "P>m1:deliver",
            "P>m2:deliver",
            "P>m1:deliver",
            "P>m2:deliver",
        ],
    },
    Exercise {
        id: "register-race",
        title: "Two operations race",
        task: "P and Q both run Elect and both read term 0. Let both reads finish before either \
               write. Which write holds? Make one operation end in a Conflict and the other \
               done.",
        done: "A read raises the read generation of each matchmaker it reaches. The later \
               operation's read fences every earlier write at those matchmakers, so one write \
               per term holds. The loser opens a new operation above, reads again and finds \
               the term taken.",
        setup: &[],
        goal: |r| {
            (r.conflicted("P") && r.done("Q", elected("Q")))
                || (r.conflicted("Q") && r.done("P", elected("P")))
        },
        solution: &[
            "P:elect",
            "Q:elect",
            "P>m1:deliver",
            "P>m2:deliver",
            "Q>m2:deliver",
            "Q>m3:deliver",
            "P>m1:deliver",
            "P>m2:deliver",
            "Q>m2:deliver",
            "Q>m3:deliver",
        ],
    },
    Exercise {
        id: "register-minority",
        title: "A write only a minority took",
        task: "Make P's write of term 1 land at m1 only, and never reach P as done. Then make \
               Q's Elect done without m1. Who holds the term?",
        done: "m1 still holds P's write, but Q holds term 1. Had P acted as controller on its own \
               write before it was done, two controllers would act in term 1. A read that meets \
               m1 can show P's write, and the next majority can lose it. Only a write seen done \
               at a majority holds: act on it, never on a read.",
        setup: &[],
        goal: |r| {
            r.holders_of("P", 1) == 1
                && !r.done("P", |_| true)
                && r.done("Q", elected("Q"))
                && r.settled_holder() == Some("Q")
        },
        solution: &[
            "P:elect",
            "P>m1:deliver",
            "P>m2:deliver",
            "P>m1:lose",
            "P>m2:drop",
            "P>m3:drop",
            "Q:elect",
            "Q>m2:deliver",
            "Q>m3:deliver",
            "Q>m2:deliver",
            "Q>m3:deliver",
        ],
    },
    Exercise {
        id: "register-refused",
        title: "A deposed controller",
        task: "P holds term 1. Make Q win term 2, then let P call reshape with its old term: get \
               the refusal of the guard.",
        done: "The reshape call carries the term of its caller. The guard compares it with the \
               term that the register holds, in the same compare-and-set as the write, so a \
               deposed controller can change nothing.",
        setup: &[
            "P:elect",
            "P>m1:deliver",
            "P>m2:deliver",
            "P>m1:deliver",
            "P>m2:deliver",
        ],
        goal: |r| r.refused("P", |refusal| matches!(refusal, ChangeRefusal::Term { .. })),
        solution: &[
            "Q:elect",
            "Q>m2:deliver",
            "Q>m3:deliver",
            "Q>m2:deliver",
            "Q>m3:deliver",
            "P:reshape",
            "P>m1:deliver",
            "P>m3:deliver",
        ],
    },
];

#[cfg(test)]
mod tests;
