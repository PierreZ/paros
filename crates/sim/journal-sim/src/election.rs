//! The election over a journal under the walk (#1420 (paros-election),
//! `specs/tla/election/`).
//!
//! Three actors, each with one `paros_election::Election`, elect over one
//! journal in memory: the pure fold of `paros-journal`, one segment. The
//! walk is the program: it passes the time, draws the uuids and the
//! jitters, and carries every request and answer through its own network.
//! It loses a request or an answer (the program then gives the client
//! `Lost`), stops an actor (it comes back as a new incarnation, with a new
//! `Election`), makes the leader write, hands a term on to another actor,
//! and, on one seed in four, switches the journal to multi-writer mode.
//!
//! The oracles read the journal and what each actor reports. They decode
//! the election record with their own code, from the layout that
//! `paros_election::record` writes in its documentation, and share no code
//! with the crate:
//!
//! - [`FRESH`]: every `SetLeader` that an election sends names a uuid that
//!   no call named before (`docs/journal/02-data-plane.md` §J2.3).
//! - [`TERMS_GROW`]: the term records in the log carry numbers that grow,
//!   and each is written under the uuid it names.
//! - [`LEAD_TRUE`]: an actor that reports it leads term `t` has the term
//!   record of `t` in the log, with its uuid and its interface, and no other
//!   actor or incarnation reported `t`.
//! - [`MULTI`]: no `SetLeader` of an election applies to a multi-writer
//!   journal, and no election sends one whose `old` is a multi-writer mode.
//!
//! The liveness claim: once the faults stop, an actor leads the uuid in
//! force, or, on a multi-writer journal, no actor leads.
//!
//! The walk records each step that the model `Election.tla` has, for the
//! trace check against `specs/tla/election/ElectionTrace.tla`
//! ([`crate::trace::election`]). A loss is a model step (`Lose`), and the
//! program gives `Lost` only for a request or an answer that is gone, so a
//! resend never races its first copy, as in the model.

use std::collections::{BTreeMap, BTreeSet};

use moonpool_walk::{Cx, Model, Phase, Walk, sometimes};
use paros_election::{Config, Draws, Election, Event};
use paros_journal::client::{ReadAnswer, Request, RequestId, Response};
use paros_journal::{
    Answer, BoundaryState, Command, Fence, Header, Headers, JournalState, Leader, LeaderUuid,
    Record, Reply, Seq, Write,
};

/// Oracle: every uuid of a `SetLeader` is fresh.
pub const FRESH: &str = "election walk: a SetLeader names a fresh uuid";
/// Oracle: the numbers of the term records grow.
pub const TERMS_GROW: &str = "election walk: the numbers of the term records grow";
/// Oracle: a reported term has its record, and one holder.
pub const LEAD_TRUE: &str = "election walk: a reported term has its record and one holder";
/// Oracle: the election keeps a multi-writer journal so.
pub const MULTI: &str = "election walk: the election keeps a multi-writer journal so";

/// The coverage sites of the election walk.
pub mod reach {
    /// An actor led a term.
    pub const LED: &str = "election walk: an actor led a term";
    /// A claim won after a resend: the refusal named its uuid.
    pub const WON_BY_REFUSAL: &str = "election walk: a claim won after a resend";
    /// A claim lost the compare-and-set.
    pub const LOST_CAS: &str = "election walk: a claim lost the compare-and-set";
    /// A term stopped at a refused write.
    pub const DEPOSED: &str = "election walk: a term stopped at a refused write";
    /// A handed term was adopted.
    pub const ADOPTED: &str = "election walk: a handed term was adopted";
    /// A handed term found its actor busy.
    pub const NOT_ADOPTED: &str = "election walk: a handed term found its actor busy";
    /// An actor saw a multi-writer journal.
    pub const MULTI_SEEN: &str = "election walk: an actor saw a multi-writer journal";
    /// A term took the number of a won term that never wrote its record.
    pub const NUMBER_REUSED: &str = "election walk: a number of an unpublished term reused";
    /// A call of a stopped incarnation applied.
    pub const OLD_CALL: &str = "election walk: a call of a stopped incarnation applied";
    /// A third term led.
    pub const THIRD: &str = "election walk: a third term led";
    /// A write of the program written under a term.
    pub const WROTE: &str = "election walk: a write of the program written";
    /// An actor read the holder of another actor.
    pub const HOLDER: &str = "election walk: an actor read another holder";

    /// Every site.
    pub const ALL: &[&str] = &[
        LED,
        WON_BY_REFUSAL,
        LOST_CAS,
        DEPOSED,
        ADOPTED,
        NOT_ADOPTED,
        MULTI_SEEN,
        NUMBER_REUSED,
        OLD_CALL,
        THIRD,
        WROTE,
        HOLDER,
    ];
}

/// A bug planted in the program or in the journal: never in the election.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ElectionMutant {
    /// No bug.
    #[default]
    None,
    /// The program's draws start again at each incarnation of an actor.
    /// [`FRESH`] catches it.
    DrawsRepeat,
    /// The server strips the header of every term record in its pages, so a
    /// reader takes it for a record of the program.
    /// [`TERMS_GROW`] catches it.
    PageDrops,
    /// The journal answers a term record written but keeps another record
    /// at its position. [`LEAD_TRUE`] catches it.
    WriteVanishes,
    /// The journal shows a multi-writer leader as unset and applies a
    /// `SetLeader` that names it so. [`MULTI`] catches it.
    LaxMulti,
    /// The program hands a term on with a number one too high. The oracles
    /// pass it; the trace check refuses it.
    HandSkip,
}

/// The number of actors.
const ACTORS: usize = 3;
/// The tag of the multi-writer mode that the user of the journal sets.
const MULTI_TAG: LeaderUuid = LeaderUuid(1 << 100);
/// The time a tick moves at most.
const TICK: u64 = 15;

/// The knobs of the walk's elections, in ticks of the walk's clock.
#[must_use]
pub fn config() -> Config {
    Config {
        lease: 100,
        poll: 10,
        renew: 20,
        jitter: 30,
        page: 3,
    }
}

/// A step of the model that the walk took, for the trace check.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ElectionEvent {
    /// Actor `a` sent the first copy of a claim: `SetLeader(u, old)`.
    Challenge { a: usize, u: u128, old: u128 },
    /// Actor `a` sent its `SetLeader` again after a loss.
    Retry { a: usize },
    /// The answer of actor `a`'s claim: won with number `t`, or lost.
    CampaignAnswer { a: usize, won: bool, t: u64 },
    /// Actor `a` sent a write of its term.
    Renew { a: usize },
    /// Actor `a` gave up a write whose request or answer was lost.
    Timeout { a: usize },
    /// The answer to a write of actor `a`.
    WriteAnswer { a: usize, ok: bool },
    /// Actor `a` handed its term on to `b`, with the uuid `u`.
    HandOn { a: usize, b: usize, u: u128 },
    /// The answer of actor `a`'s hand on.
    HandAnswer { a: usize },
    /// Actor `b` was handed term `t`, and took it or not.
    Adopt { b: usize, took: bool, t: u64 },
    /// The journal applied a `SetLeader(u, ..)` of incarnation `inc` of `a`.
    ApplySetLeader {
        a: usize,
        inc: u64,
        u: u128,
        ok: bool,
    },
    /// The journal applied a write under `u` of incarnation `inc` of `a`.
    ApplyWrite {
        a: usize,
        inc: u64,
        u: u128,
        ok: bool,
    },
    /// A message of the model was lost: its kind (`sl`, `w`, `slans`,
    /// `wans`), its actor, the incarnation and uuid of a call.
    Lose {
        kind: &'static str,
        a: usize,
        inc: u64,
        u: u128,
    },
    /// Actor `a` stopped and came back.
    Crash { a: usize },
    /// The user switched the journal to multi-writer mode.
    UserMulti,
}

/// A message in the walk's network.
#[derive(Clone, Debug)]
pub enum ElectionMsg {
    /// A request of incarnation `inc` of actor `a`.
    Req {
        a: usize,
        inc: u64,
        request: Request,
    },
    /// The answer to request `id` of incarnation `inc` of actor `a`.
    Resp {
        a: usize,
        inc: u64,
        id: RequestId,
        response: Response,
    },
}

/// The moves of the election walk.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ElectionAction {
    /// Deliver a message (all of them once the faults stop).
    Deliver,
    /// Lose a message.
    Lose,
    /// The time moves; each actor ticks.
    Tick,
    /// An actor stops and comes back as a new incarnation.
    Crash,
    /// A leader writes a record of the program.
    Write,
    /// A leader hands its term on.
    HandOn,
    /// The user switches the journal to multi-writer mode.
    UserMulti,
}

/// The program's draws for one call: uuids from a counter, jitters from a
/// seed of the walk.
struct WalkDraws<'a> {
    counter: &'a mut u128,
    seed: u64,
}

impl Draws for WalkDraws<'_> {
    fn uuid(&mut self) -> LeaderUuid {
        *self.counter += 1;
        LeaderUuid(*self.counter)
    }

    fn jitter(&mut self, max: u64) -> u64 {
        self.seed = self
            .seed
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (self.seed >> 33) % (max + 1)
    }
}

/// One actor: its election and its incarnation.
struct Actor {
    election: Election,
    inc: u64,
    /// The draws of [`ElectionMutant::DrawsRepeat`]: a counter of this
    /// incarnation.
    own: u128,
    /// The last command its client sent, to tell a resend.
    last: Option<Command>,
    /// The copies of its claim in flight so far.
    copies: u32,
    /// The actor it hands its term on to.
    hand_to: Option<usize>,
    /// A term record that `adopt` sent: its trace step is the adoption.
    adopting: bool,
}

/// What a request in flight is, for its answer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    SetLeader(u128),
    Write(u128),
    Read,
}

/// The election walk.
pub struct ElectionModel {
    mutant: ElectionMutant,
    fold: JournalState,
    positions: BTreeMap<Seq, Write>,
    /// The log as the walk keeps it: each position's record.
    log: Vec<Record>,
    actors: Vec<Actor>,
    net: Vec<ElectionMsg>,
    /// What each request in flight is.
    kinds: BTreeMap<(usize, u64, RequestId), Kind>,
    now: u64,
    uuids: u128,
    /// Whether this seed may switch to multi-writer, and whether it did.
    may_multi: bool,
    multi: bool,
    /// Every uuid that a `SetLeader` named.
    named: BTreeSet<u128>,
    /// Each reported term and its holder: actor and incarnation.
    reported: BTreeMap<u64, (usize, u64)>,
    /// The numbers of won claims that wrote no record yet, by actor.
    computed: BTreeMap<u64, (usize, u64)>,
    user_writes: u64,
}

impl Model for ElectionModel {
    type Msg = ();
    type Node = ();
    type Action = ElectionAction;
    type Event = ElectionEvent;

    fn new(cx: &mut Cx<'_, Self>) -> Self {
        Self::build(ElectionMutant::None, cx)
    }

    fn actions() -> &'static [ElectionAction] {
        &[
            ElectionAction::Deliver,
            ElectionAction::Lose,
            ElectionAction::Tick,
            ElectionAction::Crash,
            ElectionAction::Write,
            ElectionAction::HandOn,
            ElectionAction::UserMulti,
        ]
    }

    fn weight(action: ElectionAction, phase: Phase) -> u32 {
        match (phase, action) {
            (Phase::Chaos, ElectionAction::Deliver) => 30,
            (Phase::Chaos, ElectionAction::Tick) => 12,
            (Phase::Chaos, ElectionAction::Lose | ElectionAction::Write) => 4,
            (Phase::Chaos, ElectionAction::Crash | ElectionAction::HandOn)
            | (Phase::Quiet, ElectionAction::Write) => 2,
            (Phase::Chaos, ElectionAction::UserMulti)
            | (Phase::Settle, ElectionAction::Deliver | ElectionAction::Tick) => 1,
            (Phase::Quiet, ElectionAction::Deliver) => 20,
            (Phase::Quiet, ElectionAction::Tick) => 8,
            _ => 0,
        }
    }

    fn act(&mut self, action: ElectionAction, cx: &mut Cx<'_, Self>) {
        match action {
            ElectionAction::Deliver => {
                if cx.phase() == Phase::Chaos {
                    if let Some(at) = cx.rng().index(self.net.len()) {
                        let message = self.net.remove(at);
                        self.deliver_msg(message, cx);
                    }
                } else {
                    for _ in 0..400 {
                        if self.net.is_empty() {
                            break;
                        }
                        let message = self.net.remove(0);
                        self.deliver_msg(message, cx);
                    }
                }
            }
            ElectionAction::Lose => self.lose(cx),
            ElectionAction::Tick => {
                self.now += 1 + cx.rng().below(TICK);
                for a in 0..ACTORS {
                    self.tick(a, cx);
                }
            }
            ElectionAction::Crash => {
                let a = cx.rng().index(ACTORS).expect("actors");
                self.crash(a, cx);
            }
            ElectionAction::Write => {
                let leaders: Vec<usize> = (0..ACTORS)
                    .filter(|a| self.actors[*a].election.leading().is_some())
                    .collect();
                if let Some(&a) = cx.rng().pick(&leaders) {
                    self.user_writes += 1;
                    let record = Record {
                        headers: Headers {
                            leader_time: None,
                            client: vec![Header {
                                key: b"app".to_vec(),
                                value: vec![1],
                            }],
                        },
                        payload: self.user_writes.to_le_bytes().to_vec(),
                    };
                    if self.actors[a].election.write(vec![record]).is_ok() {
                        self.flush(a, cx);
                    }
                }
            }
            ElectionAction::HandOn => self.hand_on(cx),
            ElectionAction::UserMulti => {
                if self.may_multi && !self.multi {
                    let old = self.fold.leader();
                    let command = Command::SetLeader {
                        new: Leader::multi(MULTI_TAG),
                        old,
                    };
                    let answer = self.fold.apply(&command, |_| None);
                    assert!(
                        matches!(answer, Answer::LeaderSet(_)),
                        "the user's switch names the leader in force"
                    );
                    self.multi = true;
                    cx.record(ElectionEvent::UserMulti);
                }
            }
        }
    }

    fn deliver(&mut self, (): (), _cx: &mut Cx<'_, Self>) {}

    fn converged(&self) -> Result<(), String> {
        let leading: Vec<(usize, u128)> = (0..ACTORS)
            .filter_map(|a| {
                self.actors[a]
                    .election
                    .leading()
                    .map(|(_, uuid)| (a, uuid.0))
            })
            .collect();
        if self.multi {
            return if leading.is_empty() {
                Ok(())
            } else {
                Err(format!("{leading:?} still lead a multi-writer journal"))
            };
        }
        let current = match self.fold.leader() {
            Leader::Uuid(uuid) => Some(uuid.0),
            _ => None,
        };
        if leading.iter().any(|(_, u)| Some(*u) == current) {
            Ok(())
        } else {
            Err(format!(
                "no actor leads the uuid in force {current:?}: leading {leading:?}"
            ))
        }
    }

    fn describe(&self) -> String {
        format!(
            "now {}, leader {:?}, {} records, {} in flight, multi {}",
            self.now,
            self.fold.leader(),
            self.log.len(),
            self.net.len(),
            self.multi
        )
    }
}

/// An election record as the oracles decode it: the layout of
/// `paros_election::record`, read with the walk's own code.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Decoded {
    term_record: bool,
    term: u64,
    uuid: u128,
    interface: Vec<u8>,
}

fn decode(record: &Record) -> Option<Decoded> {
    let mine: Vec<&Header> = record
        .headers
        .client
        .iter()
        .filter(|h| h.key == b"paros.election")
        .collect();
    let [header] = mine.as_slice() else {
        return None;
    };
    let v = &header.value;
    if v.len() != 26 || v[0] != 1 || !(v[1] == 1 || v[1] == 2) {
        return None;
    }
    let mut term = [0u8; 8];
    term.copy_from_slice(&v[2..10]);
    let mut uuid = [0u8; 16];
    uuid.copy_from_slice(&v[10..26]);
    Some(Decoded {
        term_record: v[1] == 1,
        term: u64::from_le_bytes(term),
        uuid: u128::from_le_bytes(uuid),
        interface: record.payload.clone(),
    })
}

fn interface(a: usize, inc: u64) -> Vec<u8> {
    format!("a{}#{inc}", a + 1).into_bytes()
}

fn leader_value(leader: Leader) -> u128 {
    match leader {
        Leader::Unset => 0,
        Leader::Uuid(u) => u.0,
        // The model's `None`: its trace encoding maps it.
        Leader::Multi(_) => u128::MAX,
    }
}

impl ElectionModel {
    /// The walk with `mutant` planted.
    pub fn build(mutant: ElectionMutant, cx: &mut Cx<'_, Self>) -> Self {
        let mut actors = Vec::new();
        for a in 0..ACTORS {
            let mut election = Election::new(config(), interface(a, 1), 0);
            election.campaign();
            actors.push(Actor {
                election,
                inc: 1,
                own: 0,
                last: None,
                copies: 0,
                hand_to: None,
                adopting: false,
            });
        }
        Self {
            mutant,
            fold: JournalState::from_boundary(BoundaryState::fresh(Leader::Unset)),
            positions: BTreeMap::new(),
            log: Vec::new(),
            actors,
            net: Vec::new(),
            kinds: BTreeMap::new(),
            now: 0,
            uuids: 0,
            may_multi: cx.rng().chance(1, 4),
            multi: false,
            named: BTreeSet::new(),
            reported: BTreeMap::new(),
            computed: BTreeMap::new(),
            user_writes: 0,
        }
    }

    /// A leader hands its term on to an actor.
    fn hand_on(&mut self, cx: &mut Cx<'_, Self>) {
        let leaders: Vec<usize> = (0..ACTORS)
            .filter(|a| self.actors[*a].election.leading().is_some())
            .collect();
        if let Some(&a) = cx.rng().pick(&leaders) {
            // A steer: an actor that claims is busy and refuses the
            // handed term, a case a pick at random reaches rarely.
            let claiming: Vec<usize> = (0..ACTORS)
                .filter(|x| {
                    *x != a
                        && !self.actors[*x].election.watching()
                        && self.actors[*x].election.leading().is_none()
                })
                .collect();
            let b = match cx.rng().pick(&claiming) {
                Some(&b) if cx.rng().chance(1, 2) => b,
                _ => cx.rng().index(ACTORS).expect("actors"),
            };
            let seed = cx.rng().next_u64();
            let done = if self.mutant == ElectionMutant::DrawsRepeat {
                let actor = &mut self.actors[a];
                let mut own = (a as u128 + 1) * 1_000_000 + actor.own;
                let result = actor.election.hand_on(&mut WalkDraws {
                    counter: &mut own,
                    seed,
                });
                actor.own = own - (a as u128 + 1) * 1_000_000;
                result
            } else {
                self.actors[a].election.hand_on(&mut WalkDraws {
                    counter: &mut self.uuids,
                    seed,
                })
            };
            if done.is_ok() {
                self.actors[a].hand_to = Some(b);
                self.flush(a, cx);
            }
        }
    }

    fn tick(&mut self, a: usize, cx: &mut Cx<'_, Self>) {
        let seed = cx.rng().next_u64();
        let now = self.now;
        if self.mutant == ElectionMutant::DrawsRepeat {
            let actor = &mut self.actors[a];
            let base = (a as u128 + 1) * 1_000_000;
            let mut own = base + actor.own;
            actor.election.tick(
                now,
                &mut WalkDraws {
                    counter: &mut own,
                    seed,
                },
            );
            actor.own = own - base;
        } else {
            self.actors[a].election.tick(
                now,
                &mut WalkDraws {
                    counter: &mut self.uuids,
                    seed,
                },
            );
        }
        self.flush(a, cx);
    }

    fn crash(&mut self, a: usize, cx: &mut Cx<'_, Self>) {
        let inc = self.actors[a].inc + 1;
        let mut election = Election::new(config(), interface(a, inc), self.now);
        election.campaign();
        self.actors[a] = Actor {
            election,
            inc,
            own: 0,
            last: None,
            copies: 0,
            hand_to: None,
            adopting: false,
        };
        // The answers to the stopped incarnation are lost; its requests
        // stay in flight.
        self.net
            .retain(|m| !matches!(m, ElectionMsg::Resp { a: to, .. } if *to == a));
        self.kinds.retain(|(to, i, _), _| *to != a || *i == inc);
        cx.record(ElectionEvent::Crash { a });
    }

    /// Lose a message. The program gives `Lost` for the request whose
    /// request or answer it was, when its incarnation still runs.
    fn lose(&mut self, cx: &mut Cx<'_, Self>) {
        let Some(at) = cx.rng().index(self.net.len()) else {
            return;
        };
        let message = self.net.remove(at);
        let (a, inc, id, request) = match &message {
            ElectionMsg::Req { a, inc, request } => (*a, *inc, request.id(), true),
            ElectionMsg::Resp { a, inc, id, .. } => (*a, *inc, *id, false),
        };
        let kind = self.kinds.remove(&(a, inc, id));
        match (kind, request) {
            (Some(Kind::SetLeader(u)), true) => cx.record(ElectionEvent::Lose {
                kind: "sl",
                a,
                inc,
                u,
            }),
            (Some(Kind::Write(u)), true) => cx.record(ElectionEvent::Lose {
                kind: "w",
                a,
                inc,
                u,
            }),
            (Some(Kind::SetLeader(u)), false) => cx.record(ElectionEvent::Lose {
                kind: "slans",
                a,
                inc,
                u,
            }),
            (Some(Kind::Write(u)), false) => cx.record(ElectionEvent::Lose {
                kind: "wans",
                a,
                inc,
                u,
            }),
            _ => {}
        }
        if inc == self.actors[a].inc {
            if let Some(Kind::SetLeader(_) | Kind::Write(_)) = kind {
                // The program knows the call is gone: the next copy leaves
                // now, as the model's `Retry` or `Timeout`.
            }
            self.actors[a].election.answer(id, Response::Lost);
            self.flush(a, cx);
        }
    }

    fn deliver_msg(&mut self, message: ElectionMsg, cx: &mut Cx<'_, Self>) {
        match message {
            ElectionMsg::Req { a, inc, request } => self.serve(a, inc, &request, cx),
            ElectionMsg::Resp {
                a,
                inc,
                id,
                response,
            } => {
                assert!(
                    inc == self.actors[a].inc,
                    "the answers to a stopped actor are gone"
                );
                let kind = self.kinds.remove(&(a, inc, id));
                if let (Some(Kind::Write(_)), Response::Reply(Reply::Answer(answer))) =
                    (kind, &response)
                {
                    let ok = matches!(answer, Answer::Written { .. } | Answer::Duplicate { .. });
                    cx.record(ElectionEvent::WriteAnswer { a, ok });
                }
                if let Some(Kind::SetLeader(_)) = kind
                    && self.actors[a].hand_to.is_some()
                {
                    cx.record(ElectionEvent::HandAnswer { a });
                }
                self.actors[a].election.answer(id, response);
                self.flush(a, cx);
            }
        }
    }

    /// The journal serves `request` of incarnation `inc` of actor `a`.
    fn serve(&mut self, a: usize, inc: u64, request: &Request, cx: &mut Cx<'_, Self>) {
        let current = inc == self.actors[a].inc;
        let response = match request {
            Request::Command { command, .. } => {
                let answer = self.apply(a, inc, command, cx);
                Response::Reply(Reply::Answer(answer))
            }
            Request::Floor { .. } => Response::Read(ReadAnswer::Page {
                records: Vec::new(),
                view: self.view(),
                sealed: false,
            }),
            Request::Read { from, limit, .. } => {
                let end = (from.0 + limit).min(self.log.len() as u64);
                let records = (from.0..end)
                    .map(|seq| {
                        let at = usize::try_from(seq).expect("a short log");
                        let mut record = self.log[at].clone();
                        if self.mutant == ElectionMutant::PageDrops
                            && decode(&record).is_some_and(|d| d.term_record)
                        {
                            // The page keeps the position, not the header.
                            record.headers.client.clear();
                        }
                        (Seq(seq), record)
                    })
                    .collect();
                Response::Read(ReadAnswer::Page {
                    records,
                    view: self.view(),
                    sealed: false,
                })
            }
            Request::Resolve { .. } => unreachable!("one segment: no write is below"),
        };
        if current {
            self.net.push(ElectionMsg::Resp {
                a,
                inc,
                id: request.id(),
                response,
            });
        } else {
            self.kinds.remove(&(a, inc, request.id()));
        }
    }

    fn view(&self) -> paros_journal::JournalView {
        let mut view = self.fold.view();
        if self.mutant == ElectionMutant::LaxMulti && view.leader.is_multi() {
            view.leader = Leader::Unset;
        }
        view
    }

    fn apply(&mut self, a: usize, inc: u64, command: &Command, cx: &mut Cx<'_, Self>) -> Answer {
        sometimes!(inc != self.actors[a].inc, reach::OLD_CALL);
        let mut command = command.clone();
        if let Command::SetLeader { old, .. } = &mut command
            && self.mutant == ElectionMutant::LaxMulti
            && self.fold.leader().is_multi()
            && *old == Leader::Unset
        {
            *old = self.fold.leader();
        }
        let multi_before = self.fold.leader().is_multi();
        let answer = self.fold.apply(&command, |seq| self.positions.get(&seq));
        match (&command, &answer) {
            (Command::SetLeader { new, .. }, _) => {
                let ok = matches!(answer, Answer::LeaderSet(_));
                assert!(
                    !(ok && multi_before),
                    "{MULTI}: a SetLeader of actor {a} took the journal out of multi-writer mode"
                );
                cx.record(ElectionEvent::ApplySetLeader {
                    a,
                    inc,
                    u: leader_value(*new),
                    ok,
                });
            }
            (Command::Write(write), _) => {
                let Fence::Leader { uuid, .. } = write.fence else {
                    panic!("an election writes fenced");
                };
                let ok = matches!(answer, Answer::Written { .. } | Answer::Duplicate { .. });
                cx.record(ElectionEvent::ApplyWrite {
                    a,
                    inc,
                    u: uuid.0,
                    ok,
                });
                if let Answer::Written { seq, .. } = answer {
                    self.positions.insert(seq, write.clone());
                    for record in &write.records {
                        self.land(uuid.0, record);
                    }
                }
            }
            _ => panic!("an election sends no {command:?}"),
        }
        answer
    }

    /// A record lands in the log: the oracle on term records.
    fn land(&mut self, under: u128, record: &Record) {
        if let Some(found) = decode(record).filter(|d| d.term_record) {
            assert!(
                found.uuid == under,
                "{TERMS_GROW}: a term record of {} written under {under}",
                found.uuid
            );
            let earlier = self
                .log
                .iter()
                .filter_map(decode)
                .filter(|d| d.term_record && d.uuid != found.uuid)
                .map(|d| d.term)
                .max();
            assert!(
                earlier.is_none_or(|t| t < found.term),
                "{TERMS_GROW}: term record {} after a term record {earlier:?}",
                found.term
            );
            if self.mutant == ElectionMutant::WriteVanishes {
                self.log.push(Record::new(b"vanished".to_vec()));
                return;
            }
        }
        self.log.push(record.clone());
    }

    /// Send what actor `a` asks, record its steps, and judge what it
    /// reports.
    fn flush(&mut self, a: usize, cx: &mut Cx<'_, Self>) {
        let inc = self.actors[a].inc;
        for request in self.actors[a].election.take_requests() {
            let kind = match &request {
                Request::Command { command, .. } => {
                    self.record_send(a, command, cx);
                    match command {
                        Command::SetLeader { new, .. } => Kind::SetLeader(leader_value(*new)),
                        Command::Write(Write {
                            fence: Fence::Leader { uuid, .. },
                            ..
                        }) => Kind::Write(uuid.0),
                        other => panic!("an election sends no {other:?}"),
                    }
                }
                _ => Kind::Read,
            };
            self.kinds.insert((a, inc, request.id()), kind);
            self.net.push(ElectionMsg::Req { a, inc, request });
        }
        for event in self.actors[a].election.take_events() {
            self.judge(a, event, cx);
        }
    }

    /// The model's step for a command that actor `a` sends.
    fn record_send(&mut self, a: usize, command: &Command, cx: &mut Cx<'_, Self>) {
        let actor = &mut self.actors[a];
        let again = actor.last.as_ref() == Some(command);
        actor.last = Some(command.clone());
        match command {
            Command::SetLeader { new, old } => {
                assert!(
                    !old.is_multi(),
                    "{MULTI}: actor {a} claims over a multi-writer mode"
                );
                if again {
                    actor.copies += 1;
                    cx.record(ElectionEvent::Retry { a });
                    return;
                }
                let u = leader_value(*new);
                assert!(
                    self.named.insert(u),
                    "{FRESH}: actor {a} sends SetLeader({u}), a uuid named before"
                );
                if let Some(b) = actor.hand_to {
                    cx.record(ElectionEvent::HandOn { a, b, u });
                } else {
                    actor.copies = 1;
                    cx.record(ElectionEvent::Challenge {
                        a,
                        u,
                        old: leader_value(*old),
                    });
                }
            }
            Command::Write(write) => {
                if again {
                    cx.record(ElectionEvent::Timeout { a });
                    cx.record(ElectionEvent::Renew { a });
                    return;
                }
                let term_record = write
                    .records
                    .iter()
                    .filter_map(decode)
                    .find(|d| d.term_record);
                match term_record {
                    Some(_) if actor.adopting => actor.adopting = false,
                    Some(found) => {
                        sometimes!(actor.copies > 1, reach::WON_BY_REFUSAL);
                        if let Some((b, _)) = self.computed.get(&found.term)
                            && *b != a
                        {
                            sometimes!(true, reach::NUMBER_REUSED);
                        }
                        self.computed.insert(found.term, (a, actor.inc));
                        cx.record(ElectionEvent::CampaignAnswer {
                            a,
                            won: true,
                            t: found.term,
                        });
                    }
                    None => cx.record(ElectionEvent::Renew { a }),
                }
            }
            other => panic!("an election sends no {other:?}"),
        }
    }

    /// The oracles on one event of actor `a`.
    fn judge(&mut self, a: usize, event: Event, cx: &mut Cx<'_, Self>) {
        let inc = self.actors[a].inc;
        match event {
            Event::Leading { term, uuid } => {
                let mine = interface(a, inc);
                let found = self.log.iter().filter_map(decode).any(|d| {
                    d.term_record && d.term == term.0 && d.uuid == uuid.0 && d.interface == mine
                });
                assert!(
                    found,
                    "{LEAD_TRUE}: actor {a} leads term {} under {}, no such term record",
                    term.0, uuid.0
                );
                let holder = *self.reported.entry(term.0).or_insert((a, inc));
                assert!(
                    holder == (a, inc),
                    "{LEAD_TRUE}: term {} led by {holder:?} and by {:?}",
                    term.0,
                    (a, inc)
                );
                sometimes!(true, reach::LED);
                sometimes!(term.0 >= 3, reach::THIRD);
            }
            Event::Lost { .. } => {
                sometimes!(true, reach::LOST_CAS);
                cx.record(ElectionEvent::CampaignAnswer {
                    a,
                    won: false,
                    t: 0,
                });
            }
            Event::Deposed { .. } => {
                sometimes!(true, reach::DEPOSED);
                self.actors[a].hand_to = None;
            }
            Event::HandedOn(mut handoff) => {
                let b = self.actors[a]
                    .hand_to
                    .take()
                    .expect("a hand on names its next actor");
                if self.mutant == ElectionMutant::HandSkip {
                    handoff.term = handoff.term.next();
                }
                let took = self.actors[b].election.adopt(handoff);
                sometimes!(took, reach::ADOPTED);
                sometimes!(!took, reach::NOT_ADOPTED);
                cx.record(ElectionEvent::Adopt {
                    b,
                    took,
                    t: handoff.term.0,
                });
                if took {
                    self.actors[b].adopting = true;
                    self.flush(b, cx);
                }
            }
            Event::MultiWriter => sometimes!(true, reach::MULTI_SEEN),
            Event::Holder {
                interface: seen, ..
            } => {
                sometimes!(seen != interface(a, inc), reach::HOLDER);
            }
            Event::Wrote { .. } => sometimes!(true, reach::WROTE),
            Event::Stopped { .. } | Event::NotWritten { .. } => {}
        }
    }
}

/// The election walk with `mutant` planted, over `seeds` seeds of `steps`
/// chaos steps.
#[must_use]
pub fn walk(mutant: ElectionMutant, seeds: u64, steps: u64) -> Walk<ElectionModel> {
    Walk::<ElectionModel>::new()
        .seeds(seeds)
        .first_seed(crate::campaign::first_seed())
        .chaos_steps(steps)
        .quiet_steps(100)
        .settle_rounds(100)
        // No message of the engine: the walk's moves are its own.
        .deliver_weights(0, 0)
        .crash_weights(0, 0)
        .env_prefix(None)
        .model(move |cx| ElectionModel::build(mutant, cx))
}
