//! The grid controller under the walk (#1261 (grid controller decision
//! tests), §J6.2 test 3; `docs/journal/06-controller.md` §J6.1): the
//! controller role of `paros-journal-control` (`role::Controller`) drives
//! the journal's matchmaker register, while controllers race, crash and get
//! deposed, requests and replies are lost, acceptors fail, and the journal
//! seals and retires its segments.
//!
//! The register is one atomic value here, as in `specs/tla/grid-controller/`:
//! the register walk (`crate::register`) checks how a matchmaker quorum
//! keeps it. A campaign reads the term, then writes it by compare-and-set
//! on the term it read (one write at each generation of a `RegisterCall`).
//! The walk's driver gives each controller its view (the liveness of the
//! acceptors, the register, the times) and runs what the role asks.
//!
//! Three shapes:
//!
//! - [`Shape::TRACED`] follows `grid-controller/`: one journal, two
//!   controllers, the first grid on a1, a2, a3 of a pool of four, one
//!   acceptor failure, a view that reads the register as it is. TLC checks
//!   the walk against `ControllerTrace.tla`.
//! - [`Shape::WIDE`]: three controllers, a grid of two columns, failures
//!   and recoveries, requests sent again while one is in flight, and views
//!   that read the register late (a stale view, so the register refuses).
//! - [`Shape::MANY`]: four journals that share one machine, which dies,
//!   and a cap of one expensive action in flight for all of them (the
//!   host's token, Pierre's comment on #1261).
//!
//! The oracles, written from §J6 and `grid-controller/`, share no code with
//! the controller or the register:
//!
//! - [`FENCED`]: a segment opens only by a call of the register's term,
//!   sent by the controller that won that term, and the term never falls.
//! - [`BOUNDED_RESHAPE_RATE`]: two segments open at least the minimum
//!   interval apart.
//! - [`COST_BOUND`]: a reshape replaces only members that failed.
//! - [`NO_OSCILLATION`]: a reshape never takes back a member that an
//!   earlier reshape removed, unless it recovered in between.
//! - [`BOUNDED_IN_FLIGHT`]: the reshape calls in flight of all the journals
//!   never pass the cap of the host's token.
//! - [`CONVERGES`]: once the faults stop, each journal has one live
//!   controller, its newest segment in force on live members only, every
//!   older one retired, and no call in flight.

use std::collections::{BTreeMap, BTreeSet};

use moonpool_walk::{Cx, Model, Phase, Walk, sometimes};
use paros_journal::register::{
    Change, ChangeRefusal, MatchmakerGeneration, MatchmakerId, MatchmakerSet, RegisterValue,
};
use paros_journal::segment::{OpeningCall, SegmentId, SegmentState};
use paros_journal::{BoundaryState, Grid, Leader, NodeId, RELEASE, Seq, Slot};
use paros_journal_control::role::{Ask, Controller, Outcome, Standing};
use paros_journal_control::{Action, FailureDomain, Liveness, Tunables, View, Wait};

/// Oracle: only the holder of the register's term opens a segment, and the
/// term never falls.
pub const FENCED: &str = "a segment opens only by the call of the term's holder";
/// Oracle: two segments open at least the minimum interval apart.
pub const BOUNDED_RESHAPE_RATE: &str = "two reshapes open at least the minimum interval apart";
/// Oracle: a reshape replaces only members that failed.
pub const COST_BOUND: &str = "a reshape replaces only members that failed";
/// Oracle: a removed member comes back only after it recovered.
pub const NO_OSCILLATION: &str = "a reshape takes back a removed member only after it recovered";
/// Oracle: the calls in flight never pass the cap of the token.
pub const BOUNDED_IN_FLIGHT: &str = "the reshape calls in flight never pass the cap";
/// Oracle (liveness): once the faults stop, every journal settles.
pub const CONVERGES: &str = "once the faults stop, every journal settles on a healed grid";

/// The coverage sites of the controller walk.
pub mod reach {
    /// A candidate won a term while the holder was down.
    pub const TAKEOVER: &str = "controller walk: a takeover from a holder that is down";
    /// A candidate won a term while the holder held it.
    pub const DEPOSED: &str = "controller walk: a live holder deposed";
    /// A candidate lost the compare-and-set.
    pub const LOST_RACE: &str = "controller walk: a candidate lost the race";
    /// The register refused a call for its term.
    pub const STALE_REFUSED: &str = "controller walk: a call of an old term refused";
    /// A reply "ok" was lost.
    pub const OK_LOST: &str = "controller walk: an ok reply lost";
    /// A controller crashed with a call it did not send yet.
    pub const CRASH_AFTER_PLAN: &str = "controller walk: a crash between plan and call";
    /// A call sent again after its request or reply was lost.
    pub const RESENT: &str = "controller walk: a call sent again";
    /// A reshape opened a segment.
    pub const OPENED: &str = "controller walk: a reshape opened a segment";
    /// The register answered a call it already took.
    pub const IDEMPOTENT: &str = "controller walk: a call answered ok again";
    /// A deposed holder read the register and stopped.
    pub const STOOD: &str = "controller walk: a deposed holder stopped on its view";
    /// The journal sealed into a new segment.
    pub const SEALED: &str = "controller walk: the journal sealed into a new segment";
    /// The journal retired a segment.
    pub const RETIRED: &str = "controller walk: the journal retired a segment";
    /// An acceptor failed.
    pub const FAILED: &str = "controller walk: an acceptor failed";

    /// The sites of every walk.
    pub const ALL: &[&str] = &[
        TAKEOVER,
        DEPOSED,
        LOST_RACE,
        STALE_REFUSED,
        OK_LOST,
        CRASH_AFTER_PLAN,
        RESENT,
        OPENED,
        IDEMPOTENT,
        STOOD,
        SEALED,
        RETIRED,
        FAILED,
    ];

    /// A stale view made the register refuse a call (wide).
    pub const STALE_VIEW: &str = "controller walk: a call of a stale view refused";
    /// A recovered acceptor came back into a grid (wide).
    pub const TAKEN_BACK: &str = "controller walk: a recovered member taken back";
    /// The sites of the wide walk.
    pub const WIDE: &[&str] = &[STALE_VIEW, TAKEN_BACK];

    /// A controller waited for the token (many).
    pub const NO_TOKEN: &str = "controller walk: a heal waited for the token";
    /// The shared machine died (many).
    pub const SHARED_DOWN: &str = "controller walk: the machine of every journal died";
    /// The sites of the many-journals walk.
    pub const MANY: &[&str] = &[NO_TOKEN, SHARED_DOWN];
}

/// What the walk does.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Shape {
    /// The model's shape: views read the register as it is, a request is
    /// sent again only once none of it is in flight.
    pub traced: bool,
    /// The journals.
    pub journals: usize,
    /// The controllers of each journal.
    pub controllers: usize,
    /// The columns of each grid (F = 1).
    pub columns: usize,
    /// The acceptors, `NodeId(1)` to `NodeId(pool)`.
    pub pool: u64,
    /// The most acceptor failures of a run.
    pub fails: usize,
    /// Whether a failed acceptor recovers.
    pub recover: bool,
    /// The cap of expensive actions in flight, for all the journals.
    pub cap: Option<usize>,
}

impl Shape {
    /// What TLC checks.
    pub const TRACED: Self = Self {
        traced: true,
        journals: 1,
        controllers: 2,
        columns: 1,
        pool: 4,
        fails: 1,
        recover: false,
        cap: None,
    };
    /// Three controllers, two columns, failures and recoveries, stale views.
    pub const WIDE: Self = Self {
        traced: false,
        journals: 1,
        controllers: 3,
        columns: 2,
        pool: 10,
        fails: 4,
        recover: true,
        cap: None,
    };
    /// Four journals on one shared machine, one token.
    pub const MANY: Self = Self {
        traced: false,
        journals: 4,
        controllers: 1,
        columns: 1,
        pool: 8,
        fails: 2,
        recover: false,
        cap: Some(1),
    };
}

/// A bug planted in the driver of the controller walk.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ControllerMutant {
    /// No bug.
    #[default]
    None,
    /// The register writes a call over the value its request saw when it
    /// left, not over the value it holds: a lost update. [`FENCED`] catches
    /// it.
    WriteOnStale,
    /// The driver never sends a call again: a lost request stays lost.
    /// [`CONVERGES`] catches it.
    NoResend,
    /// The driver reports a failed acceptor up once no grid names it.
    /// [`NO_OSCILLATION`] catches it.
    ForgetDown,
    /// The driver reports a live member of the grid down. [`COST_BOUND`]
    /// catches it.
    UpAsDown,
    /// The driver gives no time since the last reshape.
    /// [`BOUNDED_RESHAPE_RATE`] catches it.
    NoInterval,
    /// The driver always says a token is free. [`BOUNDED_IN_FLIGHT`]
    /// catches it.
    NoToken,
    /// The driver writes the term even when the term it read is old: a
    /// campaign with no compare. The oracles pass it (the register still
    /// fences every reshape by its term), and TLC refuses the election: the
    /// trace control.
    ElectNoCas,
}

/// The numbers of the walk's host (§J6, **Open** for a real host: these
/// are the walk's own).
pub const TUNABLES: Tunables = Tunables {
    stale_after: 10,
    min_reshape_interval: 6,
    stall_window: 40,
};

/// The highest term a traced walk campaigns for: `MaxTerm` of the trace's
/// configuration is above it.
pub const TRACED_MAX_TERM: u64 = 30;

/// The first replica: the replica sets never fail in this walk.
const REPLICA: u64 = 101;
/// The first controller id.
const CONTROLLER: u64 = 900;
/// The F of every grid.
const F: u8 = 1;

/// One step of the walk, for `ControllerTrace.tla`. A controller is its
/// index from 0 (`c1` is 0); a grid is its members.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ControllerEvent {
    /// The candidate read the term (`ReadTerm`).
    ReadTerm(usize),
    /// The candidate's compare-and-set on the term it read (`Elect`).
    Elect {
        /// The candidate.
        c: usize,
        /// Whether it won.
        won: bool,
    },
    /// The holder planned a call (`Plan`).
    Plan(usize, Call),
    /// It sent its call (`Call`).
    Call(usize),
    /// A deposed holder read the register and stopped (`Stand`).
    Stand(usize),
    /// The register took a request (`Process`).
    Process(usize, Call, &'static str),
    /// A reply reached its controller (`Receive`).
    Receive(usize, Call, &'static str),
    /// A request was lost (`LoseReq`).
    LoseReq(usize, Call),
    /// A reply was lost (`LoseRep`).
    LoseRep(usize, Call, &'static str),
    /// The journal sealed into segment `g` (`SealIn`).
    Seal(u64),
    /// The journal retired segment `g` (`Retire`).
    Retire(u64),
    /// A controller crashed (`Crash`).
    Crash(usize),
    /// It restarted (`Restart`).
    Restart(usize),
    /// An acceptor failed (`Fail`).
    Fail(u64),
}

/// A call as the model writes it: its term, its expected configuration and
/// the members of its grid.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Call {
    /// The term.
    pub tm: u64,
    /// The expected configuration.
    pub exp: u64,
    /// The members of the grid.
    pub mem: Vec<u64>,
}

impl Call {
    fn of(call: &OpeningCall) -> Self {
        Self {
            tm: call.term,
            exp: call.expected.map_or(0, |SegmentId(g)| g),
            mem: call.grid.columns().iter().flatten().map(|n| n.0).collect(),
        }
    }
}

/// The model's name of a refusal or of "ok".
fn answer(outcome: &Outcome) -> &'static str {
    match outcome {
        Outcome::Done(_) => "ok",
        Outcome::Refused(ChangeRefusal::Term { .. }) => "term",
        Outcome::Refused(ChangeRefusal::Expected { .. }) => "exp",
        Outcome::Refused(ChangeRefusal::Unretired(_)) => "unretired",
        Outcome::Refused(other) => panic!("a reshape is not refused {other:?}"),
        Outcome::NoAnswer => panic!("the atomic register always answers"),
    }
}

/// The walk's own moves.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ControllerAction {
    /// An idle controller reads the term to campaign.
    Campaign,
    /// A candidate writes the term it read.
    Elect,
    /// A controller that holds a term reads its view and steps.
    Step,
    /// A controller reads the register again (wide: its view is late).
    Read,
    /// The register takes a request.
    Process,
    /// A reply reaches its controller.
    Receive,
    /// A request is lost.
    LoseReq,
    /// A reply is lost.
    LoseRep,
    /// The journal seals into its pending segment.
    Seal,
    /// The journal retires its sealed segment.
    Retire,
    /// An acceptor fails.
    Fail,
    /// A failed acceptor recovers (wide).
    Recover,
    /// Time passes.
    Tick,
}

/// A request or a reply in flight.
#[derive(Clone, Debug)]
struct Flight {
    c: usize,
    call: OpeningCall,
    /// The register when the request left (the mutant `WriteOnStale`).
    seen: RegisterValue,
    /// The answer, for a reply.
    outcome: Option<Outcome>,
}

/// One controller and its driver's state.
#[derive(Clone, Debug)]
struct Ctl {
    up: bool,
    role: Controller,
    /// The term a candidate read.
    read: Option<u64>,
    /// The register as the driver last read it (wide: late).
    view: RegisterValue,
    /// The calls the driver sent once (the mutant `NoResend`).
    sent: Vec<OpeningCall>,
}

/// One journal: its register and its controllers.
#[derive(Clone, Debug)]
struct Journal {
    register: RegisterValue,
    ctls: Vec<Ctl>,
    requests: Vec<Flight>,
    replies: Vec<Flight>,
    /// The time each segment opened (the host's clock).
    opened_at: BTreeMap<SegmentId, u64>,
    /// The holder of each term the register gave.
    holders: BTreeMap<u64, usize>,
    /// The highest term the register held.
    term: u64,
    /// The members a reshape removed while they were down, until they recover.
    removed: BTreeSet<NodeId>,
}

/// The controller walk.
pub struct ControllerModel {
    shape: Shape,
    mutant: ControllerMutant,
    journals: Vec<Journal>,
    down: BTreeSet<NodeId>,
    /// Every acceptor that failed at least once.
    ever_down: BTreeSet<NodeId>,
    fails: usize,
    now: u64,
    zones: BTreeMap<NodeId, FailureDomain>,
}

/// The controller walk of `shape` with `mutant` planted, over `seeds` seeds
/// of `steps` chaos steps. The walk's own losses are the model's (its
/// moves `LoseReq` and `LoseRep`), so it sends no message of the engine.
#[must_use]
pub fn walk(
    shape: Shape,
    mutant: ControllerMutant,
    seeds: u64,
    steps: u64,
) -> Walk<ControllerModel> {
    Walk::<ControllerModel>::new()
        .seeds(seeds)
        .chaos_steps(steps)
        .quiet_steps(100)
        // No message of the engine: the walk's moves are its own.
        .deliver_weights(0, 0)
        .crash_weights(1, 3)
        .max_down(shape.controllers.min(2))
        .env_prefix(None)
        .model(move |cx| ControllerModel::build(shape, mutant, cx))
}

/// The first grid of journal `j`: one column of three, or two; in the
/// many-journals shape, every grid shares acceptor 1.
fn first_grid(shape: Shape, j: usize) -> Grid {
    let columns: Vec<Vec<NodeId>> = if shape.journals > 1 {
        let j = j as u64;
        vec![vec![NodeId(1), NodeId(2 + j), NodeId(3 + j)]]
    } else {
        (0..shape.columns as u64)
            .map(|c| (1..=3).map(|r| NodeId(c * 3 + r)).collect())
            .collect()
    };
    Grid::new(F, columns).expect("a v1 grid")
}

fn boundary() -> BoundaryState {
    BoundaryState {
        next_seq: Seq(0),
        first_seq: Seq(0),
        leader: Leader::Multi,
    }
}

impl Journal {
    fn new(shape: Shape, j: usize) -> Self {
        let first = OpeningCall {
            term: 0,
            expected: None,
            grid: first_grid(shape, j),
            replicas: (REPLICA..REPLICA + 3).map(NodeId).collect(),
        };
        let register = RegisterValue::new()
            .apply(&Change::Reshape {
                opening: first,
                release: RELEASE,
            })
            .expect("a new journal takes its first segment");
        let ctls = (0..shape.controllers)
            .map(|c| Ctl {
                up: true,
                role: Controller::new(controller_id(j, c)),
                read: None,
                view: register.clone(),
                sent: Vec::new(),
            })
            .collect();
        Self {
            register,
            ctls,
            requests: Vec::new(),
            replies: Vec::new(),
            opened_at: BTreeMap::from([(SegmentId(1), 0)]),
            holders: BTreeMap::new(),
            term: 0,
            removed: BTreeSet::new(),
        }
    }

    /// The controller that holds the register's term and is up.
    fn live_holder(&self) -> Option<usize> {
        let term = self.register.term();
        let c = term.holder.and_then(|holder| {
            self.ctls
                .iter()
                .position(|ctl| ctl.role.me() == holder && ctl.up)
        })?;
        (self.ctls[c].role.term() == Some(term.number)).then_some(c)
    }

    fn in_flight(&self, c: usize, call: &OpeningCall) -> bool {
        self.requests
            .iter()
            .chain(&self.replies)
            .any(|m| m.c == c && &m.call == call)
    }

    fn newest(&self) -> SegmentId {
        self.register
            .newest()
            .expect("a journal has a segment")
            .id()
    }
}

fn controller_id(j: usize, c: usize) -> NodeId {
    NodeId(CONTROLLER + 10 * j as u64 + c as u64)
}

/// Whether every column of `grid` keeps `F + 1` members out of `down`.
fn live(grid: &Grid, down: &BTreeSet<NodeId>) -> bool {
    grid.columns()
        .iter()
        .all(|column| column.iter().filter(|n| !down.contains(n)).count() > usize::from(F))
}

impl Model for ControllerModel {
    type Msg = ();
    type Node = (usize, usize);
    type Action = ControllerAction;
    type Event = ControllerEvent;

    fn new(cx: &mut Cx<'_, Self>) -> Self {
        Self::build(Shape::WIDE, ControllerMutant::None, cx)
    }

    fn actions() -> &'static [ControllerAction] {
        &[
            ControllerAction::Campaign,
            ControllerAction::Elect,
            ControllerAction::Step,
            ControllerAction::Read,
            ControllerAction::Process,
            ControllerAction::Receive,
            ControllerAction::LoseReq,
            ControllerAction::LoseRep,
            ControllerAction::Seal,
            ControllerAction::Retire,
            ControllerAction::Fail,
            ControllerAction::Recover,
            ControllerAction::Tick,
        ]
    }

    fn weight(action: ControllerAction, phase: Phase) -> u32 {
        use ControllerAction as A;
        match (action, phase) {
            (A::Step | A::Process | A::Receive, Phase::Chaos | Phase::Quiet) => 8,
            (A::Tick, Phase::Chaos | Phase::Quiet) => 6,
            (A::Campaign | A::Elect | A::Seal | A::Retire | A::Read, Phase::Chaos) => 3,
            (A::LoseReq | A::LoseRep | A::Fail | A::Recover, Phase::Chaos) => 2,
            (A::Campaign | A::Elect | A::Seal | A::Retire | A::Read, Phase::Quiet) => 4,
            (A::LoseReq | A::LoseRep | A::Fail | A::Recover, _) => 0,
            // Settle: each round makes every other move once.
            (_, _) => 1,
        }
    }

    fn act(&mut self, action: ControllerAction, cx: &mut Cx<'_, Self>) {
        match action {
            ControllerAction::Campaign => self.campaign(cx),
            ControllerAction::Elect => self.elect(cx),
            ControllerAction::Step => self.step(cx),
            ControllerAction::Read => self.read(cx),
            ControllerAction::Process => self.process(cx),
            ControllerAction::Receive => self.receive(cx),
            ControllerAction::LoseReq => self.lose(cx, true),
            ControllerAction::LoseRep => self.lose(cx, false),
            ControllerAction::Seal => self.seal(cx),
            ControllerAction::Retire => self.retire(cx),
            ControllerAction::Fail => self.fail(cx),
            ControllerAction::Recover => self.recover(cx),
            ControllerAction::Tick => self.now += 1,
        }
    }

    fn deliver(&mut self, (): (), _cx: &mut Cx<'_, Self>) {
        unreachable!("the walk sends no message of the engine");
    }

    fn nodes(&self) -> Vec<(usize, usize)> {
        (0..self.shape.journals)
            .flat_map(|j| (0..self.shape.controllers).map(move |c| (j, c)))
            .collect()
    }

    fn is_up(&self, &(j, c): &(usize, usize)) -> bool {
        self.journals[j].ctls[c].up
    }

    fn crash(&mut self, &(j, c): &(usize, usize), cx: &mut Cx<'_, Self>) {
        let journal = &mut self.journals[j];
        let ctl = &mut journal.ctls[c];
        if !ctl.up {
            return;
        }
        if let Some(call) = ctl.role.call() {
            let call = call.clone();
            sometimes!(!journal.in_flight(c, &call), reach::CRASH_AFTER_PLAN);
        }
        let ctl = &mut journal.ctls[c];
        ctl.up = false;
        ctl.role = Controller::new(ctl.role.me());
        ctl.read = None;
        ctl.sent.clear();
        if j == 0 {
            cx.record(ControllerEvent::Crash(c));
        }
    }

    fn restart(&mut self, &(j, c): &(usize, usize), cx: &mut Cx<'_, Self>) {
        let journal = &mut self.journals[j];
        let ctl = &mut journal.ctls[c];
        if ctl.up {
            return;
        }
        ctl.up = true;
        // A restarted driver reads the register before anything else.
        ctl.view = journal.register.clone();
        if j == 0 {
            cx.record(ControllerEvent::Restart(c));
        }
    }

    fn check(&mut self) {
        for journal in &self.journals {
            assert!(
                journal.register.term().number >= journal.term,
                "{FENCED}: the term fell from {} to {}",
                journal.term,
                journal.register.term().number
            );
        }
        if let Some(cap) = self.shape.cap {
            let flying = self
                .journals
                .iter()
                .flat_map(|journal| &journal.ctls)
                .filter(|ctl| ctl.role.call().is_some())
                .count();
            assert!(
                flying <= cap,
                "{BOUNDED_IN_FLIGHT}: {flying} calls in flight, the cap is {cap}"
            );
        }
        for journal in &mut self.journals {
            journal.term = journal.register.term().number;
        }
    }

    fn converged(&self) -> Result<(), String> {
        for (j, journal) in self.journals.iter().enumerate() {
            let missing = self.unsettled(journal);
            if let Some(missing) = missing {
                return Err(format!("{CONVERGES}: journal {j}: {missing}"));
            }
        }
        Ok(())
    }

    fn describe(&self) -> String {
        self.journals
            .iter()
            .map(|journal| {
                format!(
                    "term {:?}, newest {:?}, down {:?}",
                    journal.register.term(),
                    journal.register.newest().map(|r| (r.id(), r.state())),
                    self.down
                )
            })
            .collect::<Vec<_>>()
            .join("; ")
    }
}

impl ControllerModel {
    /// The walk with `shape` and `mutant` planted.
    pub fn build(shape: Shape, mutant: ControllerMutant, _cx: &mut Cx<'_, Self>) -> Self {
        Self {
            shape,
            mutant,
            journals: (0..shape.journals)
                .map(|j| Journal::new(shape, j))
                .collect(),
            down: BTreeSet::new(),
            ever_down: BTreeSet::new(),
            fails: 0,
            now: 0,
            zones: BTreeMap::new(),
        }
    }

    /// The shape of the walk.
    #[must_use]
    pub fn shape(&self) -> Shape {
        self.shape
    }

    /// What a journal still misses to have settled.
    fn unsettled(&self, journal: &Journal) -> Option<String> {
        if !journal.requests.is_empty() || !journal.replies.is_empty() {
            return Some("a message in flight".into());
        }
        let Some(holder) = journal.live_holder() else {
            return Some("no live controller holds the term".into());
        };
        if journal.ctls[holder].role.call().is_some() {
            return Some("a call in flight".into());
        }
        let newest = journal.register.newest().expect("a segment");
        if newest.state() != SegmentState::InForce {
            return Some(format!("the newest segment is {:?}", newest.state()));
        }
        if let Some(old) = journal
            .register
            .segments()
            .values()
            .find(|r| r.id() != newest.id() && r.state() != SegmentState::Retired)
        {
            return Some(format!("segment {:?} is {:?}", old.id(), old.state()));
        }
        if newest
            .grid()
            .columns()
            .iter()
            .flatten()
            .any(|n| self.down.contains(n))
        {
            return Some("a member of the grid in force is down".into());
        }
        None
    }

    /// The controllers that make `action` now: every one in the settle
    /// phase, one drawn at random otherwise.
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

    fn ctls(&self, wanted: impl Fn(&Journal, &Ctl) -> bool) -> Vec<(usize, usize)> {
        let mut found = Vec::new();
        for (j, journal) in self.journals.iter().enumerate() {
            for (c, ctl) in journal.ctls.iter().enumerate() {
                if ctl.up && wanted(journal, ctl) {
                    found.push((j, c));
                }
            }
        }
        found
    }

    fn campaign(&mut self, cx: &mut Cx<'_, Self>) {
        let chaos = cx.phase() == Phase::Chaos;
        let traced = self.shape.traced;
        let mut eligible = self.ctls(|journal, ctl| {
            ctl.role.standing() == &Standing::Idle
                && (chaos || journal.live_holder().is_none())
                && !(traced && journal.register.term().number >= TRACED_MAX_TERM)
        });
        if !chaos {
            // One candidate for each journal once the faults stop.
            let mut seen = BTreeSet::new();
            eligible.retain(|(j, _)| seen.insert(*j));
        }
        for (j, c) in Self::pick(cx, &eligible) {
            let journal = &mut self.journals[j];
            let term = journal.register.term().number;
            let ctl = &mut journal.ctls[c];
            assert_eq!(
                ctl.role.campaign(),
                Change::Elect {
                    holder: ctl.role.me()
                }
            );
            ctl.read = Some(term);
            if j == 0 {
                cx.record(ControllerEvent::ReadTerm(c));
            }
        }
    }

    fn elect(&mut self, cx: &mut Cx<'_, Self>) {
        let eligible = self.ctls(|_, ctl| ctl.role.standing() == &Standing::Campaigning);
        for (j, c) in Self::pick(cx, &eligible) {
            let journal = &mut self.journals[j];
            let read = journal.ctls[c]
                .read
                .take()
                .expect("a candidate read the term");
            let before = journal.register.term();
            let won = before.number == read || self.mutant == ControllerMutant::ElectNoCas;
            let outcome = if won {
                let me = journal.ctls[c].role.me();
                let value = journal
                    .register
                    .apply(&Change::Elect { holder: me })
                    .expect("an election is never refused");
                if let Some(old) = before.holder {
                    let holder = journal.ctls.iter().find(|ctl| ctl.role.me() == old);
                    let holder = holder.expect("a holder is a controller");
                    sometimes!(!holder.up, reach::TAKEOVER);
                    sometimes!(
                        holder.up && holder.role.term() == Some(before.number),
                        reach::DEPOSED
                    );
                }
                journal.register = value.clone();
                journal.holders.insert(value.term().number, c);
                Outcome::Done(value)
            } else {
                sometimes!(true, reach::LOST_RACE);
                Outcome::NoAnswer
            };
            let traced = self.shape.traced;
            let ctl = &mut journal.ctls[c];
            ctl.role.elected(&outcome);
            ctl.view = if traced {
                journal.register.clone()
            } else {
                // The driver takes the term from the register's answer, and
                // plans on the segment table it read last: a stale table,
                // which the register's guards judge.
                RegisterValue::from_parts(journal.register.term(), ctl.view.segments().clone())
            };
            if j == 0 {
                cx.record(ControllerEvent::Elect { c, won });
            }
        }
    }

    /// The view the driver of `(j, c)` gives its controller.
    fn liveness(&self, j: usize) -> BTreeMap<NodeId, Liveness> {
        let journal = &self.journals[j];
        let named: BTreeSet<NodeId> = journal
            .register
            .segments()
            .values()
            .filter(|r| r.state() < SegmentState::Retired)
            .flat_map(|r| r.grid().columns().iter().flatten().copied())
            .collect();
        let in_force = journal.register.in_force().map(|r| r.grid().clone());
        let mut liveness: BTreeMap<NodeId, Liveness> = (1..=self.shape.pool)
            .map(NodeId)
            .map(|n| {
                let down = self.down.contains(&n)
                    && (self.mutant != ControllerMutant::ForgetDown || named.contains(&n));
                (n, if down { Liveness::Down } else { Liveness::Up })
            })
            .collect();
        if self.mutant == ControllerMutant::UpAsDown
            && let Some(grid) = in_force
            && let Some(live) = grid
                .columns()
                .iter()
                .flatten()
                .find(|n| !self.ever_down.contains(n))
        {
            liveness.insert(*live, Liveness::Down);
        }
        for r in REPLICA..REPLICA + 3 {
            liveness.insert(NodeId(r), Liveness::Up);
        }
        liveness
    }

    fn step(&mut self, cx: &mut Cx<'_, Self>) {
        let eligible = self.ctls(|_, ctl| ctl.role.term().is_some());
        for (j, c) in Self::pick(cx, &eligible) {
            self.step_one(j, c, cx);
        }
    }

    fn step_one(&mut self, j: usize, c: usize, cx: &mut Cx<'_, Self>) {
        let liveness = self.liveness(j);
        let candidates: Vec<NodeId> = (1..=self.shape.pool).map(NodeId).collect();
        let matchmakers =
            MatchmakerSet::new(MatchmakerGeneration(0), (1..=3).map(MatchmakerId).collect());
        let matchmakers_live: BTreeSet<MatchmakerId> = (1..=3).map(MatchmakerId).collect();
        let repairs = BTreeSet::new();
        let flying = self
            .journals
            .iter()
            .flat_map(|journal| &journal.ctls)
            .filter(|ctl| ctl.role.call().is_some())
            .count();
        let token_free = self.mutant == ControllerMutant::NoToken
            || self.shape.cap.is_none_or(|cap| flying < cap);
        let now = self.now;
        let journal = &mut self.journals[j];
        if self.shape.traced {
            // The model's `Plan` reads the record as it is.
            journal.ctls[c].view = journal.register.clone();
        }
        let view_register = journal.ctls[c].view.clone();
        let newest = view_register.newest().expect("a segment").id();
        let since =
            (self.mutant != ControllerMutant::NoInterval).then(|| now - journal.opened_at[&newest]);
        let pending = view_register
            .segments()
            .values()
            .find(|r| r.state() == SegmentState::Pending)
            .map(|r| now - journal.opened_at[&r.id()]);
        let ctl = &mut journal.ctls[c];
        let term = ctl.role.term().expect("a holder");
        let view = View {
            me: ctl.role.me(),
            term,
            register: &view_register,
            liveness: &liveness,
            zones: &self.zones,
            candidates: &candidates,
            leader_live: true,
            matchmakers: &matchmakers,
            matchmakers_live: &matchmakers_live,
            repairs_pending: &repairs,
            data_lost: false,
            signal_age: 0,
            since_last_reshape: since,
            seal_age: pending,
            token_free,
        };
        let before = ctl.role.call().cloned();
        let (ask, decision) = ctl.role.step(&view, &TUNABLES);
        if let Some(decision) = &decision {
            sometimes!(
                decision.action == Action::Wait(Wait::NoToken),
                reach::NO_TOKEN
            );
        }
        match ask {
            Ask::Nothing => {
                if ctl.role.term().is_none() {
                    sometimes!(true, reach::STOOD);
                    if j == 0 {
                        cx.record(ControllerEvent::Stand(c));
                    }
                }
            }
            Ask::Call(Change::Reshape { opening, release }) => {
                assert_eq!(release, RELEASE);
                if before.is_none() {
                    if j == 0 {
                        cx.record(ControllerEvent::Plan(c, Call::of(&opening)));
                    }
                    return;
                }
                self.send(j, c, opening, cx);
            }
            Ask::Call(other) => panic!("a holder asks only a reshape, not {other:?}"),
            Ask::Act(action) => panic!("the walk has no repair or re-replication: {action:?}"),
        }
    }

    /// The driver sends the call in flight of `(j, c)` again: in the traced
    /// shape only once no request or reply of it is in flight (the model's
    /// `Call`), and sometimes while one is in the wide shape.
    fn send(&mut self, j: usize, c: usize, opening: OpeningCall, cx: &mut Cx<'_, Self>) {
        let journal = &mut self.journals[j];
        let flying = journal.in_flight(c, &opening);
        let ctl = &mut journal.ctls[c];
        let again = ctl.sent.contains(&opening);
        let send = if self.mutant == ControllerMutant::NoResend && again {
            false
        } else if self.shape.traced {
            !flying
        } else {
            !flying || cx.rng().index(4).expect("four") == 0
        };
        if send {
            sometimes!(again, reach::RESENT);
            if again {
                // The earlier attempt timed out: the driver says so first.
                ctl.role.reshaped(&opening, &Outcome::NoAnswer);
                assert_eq!(ctl.role.call(), Some(&opening), "no answer keeps the call");
            }
            if !again {
                ctl.sent.push(opening.clone());
            }
            journal.requests.push(Flight {
                c,
                call: opening,
                seen: journal.register.clone(),
                outcome: None,
            });
            if j == 0 {
                cx.record(ControllerEvent::Call(c));
            }
        }
    }

    fn read(&mut self, cx: &mut Cx<'_, Self>) {
        let eligible = self.ctls(|_, _| true);
        for (j, c) in Self::pick(cx, &eligible) {
            let journal = &mut self.journals[j];
            journal.ctls[c].view = journal.register.clone();
        }
    }

    fn process(&mut self, cx: &mut Cx<'_, Self>) {
        let eligible: Vec<(usize, usize)> = self
            .journals
            .iter()
            .enumerate()
            .flat_map(|(j, journal)| (0..journal.requests.len()).map(move |i| (j, i)))
            .collect();
        let mut picked = Self::pick(cx, &eligible);
        // Remove from the back, so each index stays good.
        picked.sort_unstable_by(|a, b| b.cmp(a));
        for (j, i) in picked {
            self.process_one(j, i, cx);
        }
    }

    fn process_one(&mut self, j: usize, i: usize, cx: &mut Cx<'_, Self>) {
        let now = self.now;
        let journal = &mut self.journals[j];
        let request = journal.requests.remove(i);
        let base = if self.mutant == ControllerMutant::WriteOnStale {
            request.seen.clone()
        } else {
            journal.register.clone()
        };
        let change = Change::Reshape {
            opening: request.call.clone(),
            release: RELEASE,
        };
        let outcome = match base.apply(&change) {
            Ok(value) => {
                let opened = value.segments().len() > base.segments().len();
                sometimes!(!opened, reach::IDEMPOTENT);
                if opened {
                    let id = value.newest().expect("a segment").id();
                    let term = journal.register.term();
                    assert!(
                        request.call.term == term.number
                            && journal.holders.get(&term.number) == Some(&request.c),
                        "{FENCED}: segment {id:?} opened by controller {} at term {}, \
                         the register holds {term:?}",
                        request.c,
                        request.call.term
                    );
                    sometimes!(true, reach::OPENED);
                    self.opened(j, id, &request.call, now);
                }
                let journal = &mut self.journals[j];
                journal.register = value.clone();
                Outcome::Done(value)
            }
            Err(refusal) => {
                let journal = &self.journals[j];
                let stale = journal.ctls[request.c].view != journal.register;
                sometimes!(
                    matches!(refusal, ChangeRefusal::Term { .. }),
                    reach::STALE_REFUSED
                );
                sometimes!(
                    stale && !matches!(refusal, ChangeRefusal::Term { .. }),
                    reach::STALE_VIEW
                );
                Outcome::Refused(refusal)
            }
        };
        if j == 0 {
            cx.record(ControllerEvent::Process(
                request.c,
                Call::of(&request.call),
                answer(&outcome),
            ));
        }
        self.journals[j].replies.push(Flight {
            outcome: Some(outcome),
            ..request
        });
    }

    /// The oracles of an opened segment `id`, from `call`, at `now`.
    fn opened(&mut self, j: usize, id: SegmentId, call: &OpeningCall, now: u64) {
        let journal = &mut self.journals[j];
        let from = call.expected.expect("a reshape after the first segment");
        let last = journal.opened_at[&from];
        assert!(
            now - last >= TUNABLES.min_reshape_interval,
            "{BOUNDED_RESHAPE_RATE}: segment {id:?} opened at {now}, {from:?} at {last}"
        );
        journal.opened_at.insert(id, now);
        let old: BTreeSet<NodeId> = journal
            .register
            .segment(from)
            .expect("the expected segment")
            .grid()
            .columns()
            .iter()
            .flatten()
            .copied()
            .collect();
        let new: BTreeSet<NodeId> = call.grid.columns().iter().flatten().copied().collect();
        for removed in old.difference(&new) {
            assert!(
                self.ever_down.contains(removed),
                "{COST_BOUND}: segment {id:?} removed {removed:?}, which never failed"
            );
            // A member that recovered between the plan and the open may
            // come back: only a member still down is held out.
            if self.down.contains(removed) {
                journal.removed.insert(*removed);
            }
        }
        for added in new.difference(&old) {
            assert!(
                !journal.removed.contains(added),
                "{NO_OSCILLATION}: segment {id:?} took back {added:?}, removed and not recovered"
            );
            sometimes!(self.ever_down.contains(added), reach::TAKEN_BACK);
        }
    }

    fn receive(&mut self, cx: &mut Cx<'_, Self>) {
        let eligible: Vec<(usize, usize)> = self
            .journals
            .iter()
            .enumerate()
            .flat_map(|(j, journal)| (0..journal.replies.len()).map(move |i| (j, i)))
            .collect();
        let mut picked = Self::pick(cx, &eligible);
        picked.sort_unstable_by(|a, b| b.cmp(a));
        for (j, i) in picked {
            let journal = &mut self.journals[j];
            let reply = journal.replies.remove(i);
            let outcome = reply.outcome.expect("a reply has an outcome");
            let ctl = &mut journal.ctls[reply.c];
            if ctl.up {
                ctl.role.reshaped(&reply.call, &outcome);
                if let Outcome::Done(value) = &outcome {
                    ctl.view = value.clone();
                }
            }
            if j == 0 {
                cx.record(ControllerEvent::Receive(
                    reply.c,
                    Call::of(&reply.call),
                    answer(&outcome),
                ));
            }
        }
    }

    fn lose(&mut self, cx: &mut Cx<'_, Self>, request: bool) {
        let eligible: Vec<(usize, usize)> = self
            .journals
            .iter()
            .enumerate()
            .flat_map(|(j, journal)| {
                let n = if request {
                    journal.requests.len()
                } else {
                    journal.replies.len()
                };
                (0..n).map(move |i| (j, i))
            })
            .collect();
        for (j, i) in Self::pick(cx, &eligible) {
            let journal = &mut self.journals[j];
            let lost = if request {
                journal.requests.remove(i)
            } else {
                journal.replies.remove(i)
            };
            let call = Call::of(&lost.call);
            let event = match &lost.outcome {
                None => ControllerEvent::LoseReq(lost.c, call),
                Some(outcome) => {
                    sometimes!(matches!(outcome, Outcome::Done(_)), reach::OK_LOST);
                    ControllerEvent::LoseRep(lost.c, call, answer(outcome))
                }
            };
            if j == 0 {
                cx.record(event);
            }
        }
    }

    fn seal(&mut self, cx: &mut Cx<'_, Self>) {
        let down = &self.down;
        let eligible: Vec<usize> = self
            .journals
            .iter()
            .enumerate()
            .filter(|(_, journal)| {
                let newest = journal.register.newest().expect("a segment");
                newest.state() == SegmentState::Pending
                    && live(newest.grid(), down)
                    && journal
                        .register
                        .in_force()
                        .is_some_and(|old| live(old.grid(), down))
            })
            .map(|(j, _)| j)
            .collect();
        for j in Self::pick(cx, &eligible) {
            let now = self.now;
            let journal = &mut self.journals[j];
            let g = journal.newest();
            let old = journal.register.in_force().expect("a segment in force");
            let end = Slot(now.max(old.start().expect("in force").0));
            journal.register = journal
                .register
                .apply(&Change::Seal {
                    segment: old.id(),
                    end,
                })
                .expect("the journal seals into its pending segment");
            sometimes!(true, reach::SEALED);
            if j == 0 {
                cx.record(ControllerEvent::Seal(g.0));
            }
        }
    }

    fn retire(&mut self, cx: &mut Cx<'_, Self>) {
        let eligible: Vec<(usize, SegmentId)> = self
            .journals
            .iter()
            .enumerate()
            .flat_map(|(j, journal)| {
                journal
                    .register
                    .segments()
                    .values()
                    .filter(|r| r.state() == SegmentState::Sealed)
                    .map(move |r| (j, r.id()))
            })
            .collect();
        for (j, id) in Self::pick(cx, &eligible) {
            let journal = &mut self.journals[j];
            let holders = journal
                .register
                .segment(id)
                .expect("a segment")
                .grid()
                .acks();
            let bounded = journal
                .register
                .apply(&Change::Boundary {
                    segment: id,
                    state: boundary(),
                })
                .expect("the first replica writes the boundary state");
            journal.register = bounded
                .apply(&Change::Retire {
                    segment: id,
                    holders,
                    unrepaired_damage: false,
                })
                .expect("the leader retires the sealed segment");
            sometimes!(true, reach::RETIRED);
            if j == 0 {
                cx.record(ControllerEvent::Retire(id.0));
            }
        }
    }

    /// An acceptor fails, inside the contract: every grid that is not
    /// retired keeps `F + 1` live members in each column. In the
    /// many-journals shape, the first failure is the shared machine.
    fn fail(&mut self, cx: &mut Cx<'_, Self>) {
        if self.fails >= self.shape.fails {
            return;
        }
        let fits = |down: &BTreeSet<NodeId>, journals: &[Journal]| {
            journals.iter().all(|journal| {
                journal
                    .register
                    .segments()
                    .values()
                    .filter(|r| r.state() < SegmentState::Retired)
                    .all(|r| live(r.grid(), down))
            })
        };
        let eligible: Vec<NodeId> = if self.shape.journals > 1 && self.fails == 0 {
            vec![NodeId(1)]
        } else {
            (1..=self.shape.pool)
                .map(NodeId)
                .filter(|n| !self.down.contains(n))
                .filter(|n| {
                    let mut down = self.down.clone();
                    down.insert(*n);
                    fits(&down, &self.journals)
                })
                .collect()
        };
        let Some(&node) = eligible.get(cx.rng().index(eligible.len().max(1)).expect("not empty"))
        else {
            return;
        };
        self.fails += 1;
        self.down.insert(node);
        self.ever_down.insert(node);
        sometimes!(true, reach::FAILED);
        sometimes!(
            self.shape.journals > 1 && node == NodeId(1),
            reach::SHARED_DOWN
        );
        cx.record(ControllerEvent::Fail(node.0));
    }

    fn recover(&mut self, cx: &mut Cx<'_, Self>) {
        if !self.shape.recover {
            return;
        }
        let eligible: Vec<NodeId> = self.down.iter().copied().collect();
        for node in Self::pick(cx, &eligible) {
            self.down.remove(&node);
            for journal in &mut self.journals {
                journal.removed.remove(&node);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_first_grids_are_v1_grids() {
        for shape in [Shape::TRACED, Shape::WIDE, Shape::MANY] {
            for j in 0..shape.journals {
                let grid = first_grid(shape, j);
                assert_eq!(grid.columns().len(), shape.columns);
                assert!(grid.columns().iter().flatten().all(|n| n.0 <= shape.pool));
            }
        }
    }
}
