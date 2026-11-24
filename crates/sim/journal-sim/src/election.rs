//! The coordination store under the walk (#1420 (paros-election),
//! `docs/architecture/04-roles.md` §4.4, "The coordination store"; the
//! model is `specs/tla/coord/`).
//!
//! Three members of `paros_election`, each a [`Member`] with its own
//! journal client, share one multi-writer journal. They open sessions,
//! join a leader latch, act as the holder with guarded writes, send orders
//! with their term to a receiver that keeps the highest term it saw, leave
//! and join the latch again, revoke the holder's node, write checkpoints and
//! trim to them. Every request and every answer goes through the walk's
//! mailbox: lost, repeated and reordered, so an entry lands two times and a
//! member reads a prefix that lags. A program stops waiting for a request
//! (`Lost`, and the client sends it again). A member crashes and comes back
//! with no memory and a fresh session id; its entries in flight still land.
//! Any member may expire any session early (`EagerExpire`): that costs
//! availability, never safety.
//!
//! The journal is the fold of `paros_journal` over one segment, as the
//! client walk's. The oracles judge the log that the walk keeps:
//!
//! - [`AGREE`]: each member's tree is the fold of the whole log from the
//!   start, at the member's position, a member that started from a
//!   checkpoint too (`ViewTrue` of the model). No member stops on an
//!   unreadable floor: only the store truncates.
//! - [`TRUTH`]: the fold of the whole log is [`Truth`], the store's rules
//!   written again for the walk's entries from the model, with no code of
//!   the fold: the live sessions, their last positions and `op_seq`, the
//!   tombstones, the latch's candidates, its holder and term, the counter's
//!   version, the checkpoints taken.
//! - [`ONCE`]: each act that a program asked for applies at most once
//!   (`OnceEach`), on [`Truth`]'s reading of the log.
//! - [`ACTS`]: each act that applies is by the latch's holder
//!   (`ActsByHolder`), on [`Truth`]'s reading of the log. `ONCE` and `ACTS`
//!   judge what the programs and the journal land; a dedup or a guard bug of
//!   the fold itself is caught by `TRUTH`.
//! - [`SETTLED`]: an act that a member reports applied is applied once; one
//!   it reports failed or expired is not, and never applies later; one it
//!   reports unknown applied at most once (`SettledTrue`). Every settle is
//!   judged.
//! - [`ONE_HOLDER`]: each term of an order has one session (`OneSender`).
//! - [`FENCE`]: the receiver never applies an order below one it applied
//!   (`NoRegress`). It judges the walk's own receiver: how a program uses
//!   the term that the store gives.
//!
//! [`Truth`] reads the entries with the store's `codec`: a codec bug that
//! encodes and decodes the same wrong way is not seen here, and the codec's
//! own tests judge it.
//!
//! The liveness claim: once the faults stop, every member has a live
//! session, every member folds the whole log, and a live session holds the
//! latch.

use std::collections::{BTreeMap, BTreeSet};

use moonpool_walk::{Cx, Model, Phase, Walk, sometimes};
use paros_election::recipes::Latch;
use paros_election::{
    Check, Config, Entry, Event, Member, Op, Path, SessionId, Settled, State, Term, Tree, TxnId,
    codec,
};
use paros_journal::client::{Request, RequestId, Response};
use paros_journal::{
    Answer, BoundaryState, Command, JournalState, Leader, LeaderUuid, ReadAnswer, Record, Reply,
    Seq, Write,
};

/// Oracle: each member's tree is the fold of the whole log at its position.
pub const AGREE: &str = "election walk: a member folds the whole log";
/// Oracle: the fold of the whole log is the truth.
pub const TRUTH: &str = "election walk: the fold of the log is the truth";
/// Oracle: each act applies at most once.
pub const ONCE: &str = "election walk: an act applies at most once";
/// Oracle: each act that applies is by the holder.
pub const ACTS: &str = "election walk: an act applies only by the holder";
/// Oracle: a settled act is as the member reports it.
pub const SETTLED: &str = "election walk: a settled act is as reported";
/// Oracle: one session for each term of an order.
pub const ONE_HOLDER: &str = "election walk: one session for each term";
/// Oracle: the receiver never goes back.
pub const FENCE: &str = "election walk: the receiver never goes back";

/// The coverage sites of the election walk.
pub mod reach {
    /// A copy of an entry that the fold already applied.
    pub const DUP_VOID: &str = "election walk: a copy of an entry is void";
    /// An entry of an expired session.
    pub const DEAD_VOID: &str = "election walk: an entry of an expired session is void";
    /// A late `Open` of an expired session.
    pub const TOMB_VOID: &str = "election walk: a late open of an expired session is void";
    /// A guarded act of a deposed holder.
    pub const GUARD_VOID: &str = "election walk: a deposed holder's act fails";
    /// An expiry whose session wrote after it.
    pub const EXPIRE_VOID: &str = "election walk: a stale expiry is void";
    /// An expiry that applied.
    pub const EXPIRED: &str = "election walk: a session expired";
    /// The latch moved from one holder to another.
    pub const NEW_HOLDER: &str = "election walk: the latch moved to a new holder";
    /// The receiver refused an order of an old term.
    pub const REFUSED_ORDER: &str = "election walk: an old order refused";
    /// A checkpoint that landed behind another record.
    pub const CKPT_VOID: &str = "election walk: a stale checkpoint is void";
    /// A checkpoint that the fold took.
    pub const CKPT: &str = "election walk: a checkpoint taken";
    /// A trim to a checkpoint.
    pub const TRIMMED: &str = "election walk: a trim to a checkpoint";
    /// A member that started from a checkpoint.
    pub const JUMP: &str = "election walk: a member started from a checkpoint";
    /// A member that learned its session expired.
    pub const SESSION_EXPIRED: &str = "election walk: a member learned it expired";
    /// An entry settled below the checkpoint a member started from.
    pub const UNKNOWN: &str = "election walk: an entry settled under a checkpoint";
    /// An act that applied.
    pub const ACTED: &str = "election walk: an act applied";

    /// Every site.
    pub const ALL: &[&str] = &[
        DUP_VOID,
        DEAD_VOID,
        TOMB_VOID,
        GUARD_VOID,
        EXPIRE_VOID,
        EXPIRED,
        NEW_HOLDER,
        REFUSED_ORDER,
        CKPT_VOID,
        CKPT,
        TRIMMED,
        JUMP,
        SESSION_EXPIRED,
        UNKNOWN,
        ACTED,
    ];
}

/// A bug planted in the program that drives the members, or in the
/// journal they talk to: never in the store.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ElectionMutant {
    /// No bug.
    #[default]
    None,
    /// The journal serves a page one position late. [`AGREE`] catches it.
    PageLate,
    /// The walk's fold of the whole log takes the first expiry as a record
    /// that is not an entry. [`TRUTH`] catches it.
    SkipExpire,
    /// The journal lands a copy of each act with the next `op_seq` too:
    /// a program that numbers a resend again. [`ONCE`] catches it.
    Renumber,
    /// Every live member acts with no guard. [`ACTS`] catches it.
    Unguarded,
    /// The program files the result of an act under the next act. [`SETTLED`]
    /// catches it.
    SwapTxn,
    /// A member sends an order with the latch's term whoever holds it.
    /// [`ONE_HOLDER`] catches it.
    AnyTerm,
    /// The receiver takes an order of any term. [`FENCE`] catches it.
    NoFence,
}

/// A message in the walk's mailbox.
#[derive(Clone, Debug)]
pub enum ElectionMsg {
    /// A request of member `0`, incarnation `1`, to the journal.
    Request(usize, u64, Request),
    /// The answer of request `2` to member `0`, incarnation `1`.
    Response(usize, u64, RequestId, Response),
    /// An order of the holder of a term to the receiver.
    Order(Term, SessionId),
}

/// The moves of the election walk.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ElectionAction {
    /// The time moves on: every member reads, renews and expires.
    Tick,
    /// A member with no session opens one and joins the latch.
    Join,
    /// The holder acts with a guarded write.
    Act,
    /// The holder sends an order with its term.
    Order,
    /// A member leaves the latch.
    Leave,
    /// A member with no node in the latch joins it again.
    Rejoin,
    /// A member deletes the holder's node.
    Revoke,
    /// Somebody expires a session now.
    EagerExpire,
    /// A member writes a checkpoint.
    Checkpoint,
    /// A member trims to its last checkpoint.
    Trim,
    /// A program stops waiting for one request.
    Timeout,
}

/// The members of the walk.
const MEMBERS: usize = 3;
/// The member numbers.
const CONFIG: Config = Config {
    page: 4,
    heartbeat: 3,
    timeout: 12,
};

fn latch_parent() -> Path {
    Path::new("/election").expect("a path")
}

fn count_path() -> Path {
    Path::new("/count").expect("a path")
}

/// The operations of a session's `Open`: join the latch, and make sure the
/// counter exists.
fn open_ops(latch: &Latch) -> Vec<Op> {
    let mut ops = latch.join(Vec::new());
    ops.push(Op::Ensure { path: count_path() });
    ops
}

/// A member of the walk and its program.
struct Slot {
    member: Member,
    up: bool,
    incarnation: u64,
    /// The act that each transaction of the member carries.
    acts: BTreeMap<TxnId, u64>,
}

/// The walk's journal: one segment of `paros_journal`'s fold, and its
/// records.
struct Journal {
    state: JournalState,
    records: Vec<Record>,
}

/// The election walk.
pub struct ElectionModel {
    mutant: ElectionMutant,
    latch: Latch,
    journal: Journal,
    /// The fold of the whole log at each position: `prefix[k]` folded the
    /// records below `k`.
    prefix: Vec<Tree>,
    truth: Truth,
    slots: Vec<Slot>,
    now: u64,
    /// The last session id drawn: ids are fresh.
    sessions: u64,
    /// The last act id drawn.
    act_ids: u64,
    /// The requests whose answer a program waits for.
    waiting: BTreeSet<(usize, u64, RequestId)>,
    /// The session of each term an order was sent with.
    senders: BTreeMap<Term, SessionId>,
    /// The receiver: the highest term it applied, on its disk.
    seen: Option<Term>,
    /// Whether the walk skipped an expiry already (`SkipExpire`).
    skipped: bool,
}

impl Model for ElectionModel {
    type Msg = ElectionMsg;
    type Node = usize;
    type Action = ElectionAction;
    type Event = ();

    fn new(cx: &mut Cx<'_, Self>) -> Self {
        Self::build(ElectionMutant::None, cx)
    }

    fn actions() -> &'static [ElectionAction] {
        &[
            ElectionAction::Tick,
            ElectionAction::Join,
            ElectionAction::Act,
            ElectionAction::Order,
            ElectionAction::Leave,
            ElectionAction::Rejoin,
            ElectionAction::Revoke,
            ElectionAction::EagerExpire,
            ElectionAction::Checkpoint,
            ElectionAction::Trim,
            ElectionAction::Timeout,
        ]
    }

    fn weight(action: ElectionAction, phase: Phase) -> u32 {
        match (phase, action) {
            (_, ElectionAction::Tick) => 8,
            (_, ElectionAction::Join | ElectionAction::Timeout) => 2,
            (Phase::Chaos, ElectionAction::Act | ElectionAction::Order) => 4,
            (Phase::Chaos, _) => 1,
            _ => 0,
        }
    }

    fn act(&mut self, action: ElectionAction, cx: &mut Cx<'_, Self>) {
        match action {
            ElectionAction::Tick => {
                self.now += 1;
                for index in 0..MEMBERS {
                    if self.slots[index].up {
                        let jitter = cx.rng().below(3);
                        self.slots[index].member.tick(self.now, jitter);
                        self.flush(index, cx);
                    }
                }
            }
            ElectionAction::Timeout => {
                let waiting: Vec<_> = self.waiting.iter().copied().collect();
                if let Some(&(index, incarnation, id)) = cx.rng().pick(&waiting) {
                    self.waiting.remove(&(index, incarnation, id));
                    self.answer(index, incarnation, id, Response::Lost, cx);
                }
            }
            ElectionAction::EagerExpire => {
                let whole = self.prefix.last().expect("the empty prefix");
                let live: Vec<_> = whole.sessions().map(|(s, r)| (s, r.last)).collect();
                if let Some(&(session, last)) = cx.rng().pick(&live) {
                    self.land(&codec::encode(&Entry::Expire { session, last }));
                }
            }
            _ => {
                let Some(index) = cx.rng().index(MEMBERS) else {
                    return;
                };
                if self.slots[index].up {
                    self.member_move(index, action, cx);
                    self.flush(index, cx);
                }
            }
        }
    }

    fn deliver(&mut self, message: ElectionMsg, cx: &mut Cx<'_, Self>) {
        match message {
            ElectionMsg::Request(index, incarnation, request) => {
                let id = request.id();
                let response = self.serve(request);
                cx.send(ElectionMsg::Response(index, incarnation, id, response));
            }
            ElectionMsg::Response(index, incarnation, id, response) => {
                self.waiting.remove(&(index, incarnation, id));
                self.answer(index, incarnation, id, response, cx);
            }
            ElectionMsg::Order(term, session) => self.receive(term, session),
        }
    }

    fn nodes(&self) -> Vec<usize> {
        (0..MEMBERS).collect()
    }

    fn is_up(&self, node: &usize) -> bool {
        self.slots[*node].up
    }

    fn crash(&mut self, node: &usize, _cx: &mut Cx<'_, Self>) {
        let slot = &mut self.slots[*node];
        slot.up = false;
        slot.acts.clear();
        self.waiting.retain(|(i, _, _)| i != node);
    }

    fn restart(&mut self, node: &usize, _cx: &mut Cx<'_, Self>) {
        let slot = &mut self.slots[*node];
        if slot.up {
            return;
        }
        slot.up = true;
        slot.incarnation += 1;
        slot.member = Member::new(CONFIG);
    }

    fn check(&mut self) {
        for slot in self.slots.iter().filter(|s| s.up) {
            let Some(tree) = slot.member.tree() else {
                continue;
            };
            let at = usize::try_from(tree.next().0).expect("a position fits in memory");
            assert!(
                self.prefix.get(at) == Some(tree),
                "{AGREE}: a member at {at} folds {tree:?}"
            );
        }
    }

    fn converged(&self) -> Result<(), String> {
        let tail = self.journal.records.len() as u64;
        let whole = self.prefix.last().expect("the empty prefix");
        for (index, slot) in self.slots.iter().enumerate() {
            let State::Live(id) = slot.member.state() else {
                return Err(format!("member {index} has no live session"));
            };
            if whole.session(id).is_none() {
                return Err(format!("member {index}'s session is not live in the log"));
            }
            if slot.member.tree().map(|t| t.next().0) != Some(tail) {
                return Err(format!("member {index} has not folded the whole log"));
            }
        }
        let live: BTreeSet<SessionId> = self
            .slots
            .iter()
            .filter_map(|s| match s.member.state() {
                State::Live(id) => Some(id),
                _ => None,
            })
            .collect();
        match self.latch.holder(whole) {
            Some(turn) if live.contains(&turn.holder) => Ok(()),
            other => Err(format!(
                "the latch is held by {other:?}, the members are {live:?}"
            )),
        }
    }

    fn describe(&self) -> String {
        format!(
            "{} records, floor {:?}, now {}, states {:?}",
            self.journal.records.len(),
            self.journal.state.first_seq(),
            self.now,
            self.slots
                .iter()
                .map(|s| (s.up, s.member.state()))
                .collect::<Vec<_>>()
        )
    }
}

impl ElectionModel {
    /// The walk with `mutant` planted: the journal is made multi-writer,
    /// and no member has a session yet.
    ///
    /// # Panics
    ///
    /// When the fresh journal refuses the multi-writer mode: a bug of the
    /// walk.
    pub fn build(mutant: ElectionMutant, cx: &mut Cx<'_, Self>) -> Self {
        let _ = cx;
        let mut state = JournalState::from_boundary(BoundaryState::fresh(Leader::Unset));
        let mode = Command::SetLeader {
            new: Leader::multi(LeaderUuid(0x3A)),
            old: Leader::Unset,
        };
        assert!(matches!(state.apply(&mode, |_| None), Answer::LeaderSet(_)));
        Self {
            mutant,
            latch: Latch::new(latch_parent()),
            journal: Journal {
                state,
                records: Vec::new(),
            },
            prefix: vec![Tree::new()],
            truth: Truth::default(),
            slots: (0..MEMBERS)
                .map(|_| Slot {
                    member: Member::new(CONFIG),
                    up: true,
                    incarnation: 0,
                    acts: BTreeMap::new(),
                })
                .collect(),
            now: 0,
            sessions: 0,
            act_ids: 0,
            waiting: BTreeSet::new(),
            senders: BTreeMap::new(),
            seen: None,
            skipped: false,
        }
    }

    fn member_move(&mut self, index: usize, action: ElectionAction, cx: &mut Cx<'_, Self>) {
        let parent = latch_parent();
        let slot = &mut self.slots[index];
        let member = &mut slot.member;
        let live = matches!(member.state(), State::Live(_));
        let own = match member.state() {
            State::Live(id) => Some(id),
            _ => None,
        };
        match action {
            ElectionAction::Join => {
                if matches!(member.state(), State::Closed | State::Expired(_)) {
                    self.sessions += 1;
                    let ops = open_ops(&self.latch);
                    member.open(SessionId(self.sessions), paros_election::Txn::ops(ops));
                }
            }
            ElectionAction::Act => {
                let term = member.holds(&parent);
                let unguarded = self.mutant == ElectionMutant::Unguarded && live;
                if term.is_none() && !unguarded {
                    return;
                }
                self.act_ids += 1;
                let set = Op::SetData {
                    path: count_path(),
                    data: self.act_ids.to_le_bytes().to_vec(),
                    version: None,
                };
                let checks = match term {
                    Some(term) if !unguarded => vec![self.latch.guard(term)],
                    _ => Vec::new(),
                };
                let txn = member.submit(paros_election::Txn {
                    checks,
                    ops: vec![set],
                });
                let filed = if self.mutant == ElectionMutant::SwapTxn {
                    TxnId(txn.0 + 1)
                } else {
                    txn
                };
                slot.acts.insert(filed, self.act_ids);
            }
            ElectionAction::Order => {
                let term = match self.mutant {
                    ElectionMutant::AnyTerm if live => member
                        .tree()
                        .and_then(|t| self.latch.holder(t))
                        .map(|t| t.term),
                    _ => member.holds(&parent),
                };
                if let (Some(term), Some(session)) = (term, own) {
                    let previous = self.senders.insert(term, session);
                    assert!(
                        previous.is_none_or(|p| p == session),
                        "{ONE_HOLDER}: term {term:?} has {previous:?} and {session:?}"
                    );
                    cx.send(ElectionMsg::Order(term, session));
                }
            }
            ElectionAction::Leave => {
                if let (Some(id), Some(tree)) = (own, member.tree()) {
                    let ops = self.latch.leave(tree, id);
                    if !ops.is_empty() {
                        member.submit(paros_election::Txn::ops(ops));
                    }
                }
            }
            ElectionAction::Rejoin => {
                if let (Some(id), Some(tree)) = (own, member.tree())
                    && self.latch.leave(tree, id).is_empty()
                {
                    member.submit(paros_election::Txn::ops(self.latch.join(Vec::new())));
                }
            }
            ElectionAction::Revoke => {
                if let (true, Some(tree)) = (live, member.tree())
                    && let Some(turn) = self.latch.holder(tree)
                    && Some(turn.holder) != own
                {
                    let ops = self.latch.leave(tree, turn.holder);
                    member.submit(paros_election::Txn::ops(ops));
                }
            }
            ElectionAction::Checkpoint => {
                let _ = member.checkpoint();
            }
            ElectionAction::Trim => {
                let last = member.tree().and_then(|t| t.checkpoints().last().copied());
                if let Some(at) = last
                    && at > self.journal.state.first_seq()
                {
                    member.trim(at).expect("a checkpoint the fold took");
                }
            }
            ElectionAction::Tick | ElectionAction::EagerExpire | ElectionAction::Timeout => {
                unreachable!("handled by act")
            }
        }
    }

    /// Send the member's requests and judge its events.
    fn flush(&mut self, index: usize, cx: &mut Cx<'_, Self>) {
        let slot = &mut self.slots[index];
        let incarnation = slot.incarnation;
        for request in slot.member.take_requests() {
            self.waiting.insert((index, incarnation, request.id()));
            cx.send(ElectionMsg::Request(index, incarnation, request));
        }
        let start = slot.member.start();
        sometimes!(start > Seq(0) && slot.member.tree().is_some(), reach::JUMP);
        for event in slot.member.take_events() {
            match event {
                Event::Settled { txn, settled } => {
                    sometimes!(settled == Settled::Unknown, reach::UNKNOWN);
                    let Some(act) = slot.acts.remove(&txn) else {
                        continue;
                    };
                    let applied = self.truth.acts.get(&act).copied().unwrap_or(0);
                    match settled {
                        Settled::Applied => assert!(
                            applied == 1,
                            "{SETTLED}: act {act} reported applied, applied {applied} times"
                        ),
                        Settled::Failed(_) | Settled::Expired => {
                            assert!(
                                applied == 0,
                                "{SETTLED}: act {act} reported {settled:?}, applied {applied} times"
                            );
                            self.truth.never.insert(act);
                        }
                        Settled::Unknown => assert!(
                            applied <= 1,
                            "{SETTLED}: act {act} reported unknown, applied {applied} times"
                        ),
                    }
                }
                Event::Unreadable(at) => panic!("{AGREE}: member {index} stops at {at:?}"),
                Event::SessionExpired(_) => sometimes!(true, reach::SESSION_EXPIRED),
                Event::Checkpoint { taken } => {
                    sometimes!(taken.is_some(), reach::CKPT);
                    sometimes!(taken.is_none(), reach::CKPT_VOID);
                }
                Event::Journal(outcome) => {
                    sometimes!(
                        matches!(outcome, paros_journal::client::Outcome::Trimmed(_)),
                        reach::TRIMMED
                    );
                }
                Event::Jumped(at) => assert!(
                    at > Seq(0) && at <= start,
                    "{AGREE}: member {index} jumped to {at:?}, starts at {start:?}"
                ),
                Event::Changed { .. } | Event::Refused(_) => {}
            }
        }
    }

    fn answer(
        &mut self,
        index: usize,
        incarnation: u64,
        id: RequestId,
        response: Response,
        cx: &mut Cx<'_, Self>,
    ) {
        let slot = &mut self.slots[index];
        if !slot.up || slot.incarnation != incarnation {
            return;
        }
        slot.member.answer(id, response);
        self.flush(index, cx);
    }

    /// The journal's answer to one request.
    fn serve(&mut self, request: Request) -> Response {
        let view = self.journal.state.view();
        match request {
            Request::Command { command, .. } => {
                let answer = self.journal.state.apply(&command, |_| None);
                if let (Answer::Written { .. }, Command::Write(write)) = (&answer, &command) {
                    for record in &write.records {
                        self.landed(record);
                    }
                }
                Response::Reply(Reply::Answer(answer))
            }
            Request::Floor { .. } => Response::Read(ReadAnswer::Page {
                records: Vec::new(),
                view,
                sealed: false,
            }),
            Request::Read { from, limit, .. } => {
                if from < view.first_seq {
                    return Response::Read(ReadAnswer::Truncated(view));
                }
                let late = u64::from(self.mutant == ElectionMutant::PageLate);
                let records = (from.0 + late..view.next_seq.0)
                    .take(usize::try_from(limit).expect("a page fits in memory"))
                    .map(|s| {
                        let at = usize::try_from(s).expect("a position fits in memory");
                        (Seq(s), self.journal.records[at].clone())
                    })
                    .collect();
                Response::Read(ReadAnswer::Page {
                    records,
                    view,
                    sealed: false,
                })
            }
            Request::Resolve { .. } => unreachable!("the store writes unfenced"),
        }
    }

    /// A record that somebody other than a member writes: it lands at the
    /// tail.
    fn land(&mut self, record: &Record) {
        let write = Command::Write(Write {
            fence: paros_journal::Fence::Open,
            records: vec![record.clone()],
        });
        let answer = self.journal.state.apply(&write, |_| None);
        assert!(
            matches!(answer, Answer::Written { .. }),
            "an open write lands"
        );
        self.landed(record);
    }

    /// `record` landed at the tail of the log: fold it, judge it.
    fn landed(&mut self, record: &Record) {
        let seq = Seq(self.journal.records.len() as u64);
        self.journal.records.push(record.clone());
        let mut entry = codec::decode(record);
        if self.mutant == ElectionMutant::SkipExpire
            && !self.skipped
            && matches!(entry, Some(Entry::Expire { .. }))
        {
            self.skipped = true;
            entry = None;
        }
        let mut whole = self.prefix.last().expect("the empty prefix").clone();
        let before = self.latch.holder(&whole).map(|t| t.holder);
        let (fate, _) = whole.apply(seq, entry.as_ref());
        let after = self.latch.holder(&whole).map(|t| t.holder);
        sometimes!(
            before.is_some() && after.is_some() && before != after,
            reach::NEW_HOLDER
        );
        whole.assert_invariants();
        fate_sites(&fate, matches!(entry, Some(Entry::Open { .. })));
        self.truth.apply(seq.0, codec::decode(record).as_ref());
        self.truth.judge(&whole, &self.latch);
        self.prefix.push(whole);
        if self.mutant == ElectionMutant::Renumber
            && let Some(Entry::Txn {
                session,
                op_seq,
                txn,
            }) = codec::decode(record)
            && txn.ops.iter().any(|op| matches!(op, Op::SetData { .. }))
        {
            let copy = Entry::Txn {
                session,
                op_seq: op_seq + 1,
                txn,
            };
            self.land(&codec::encode(&copy));
        }
    }

    /// The receiver takes an order, with the term fence.
    fn receive(&mut self, term: Term, session: SessionId) {
        let _ = session;
        if self.mutant != ElectionMutant::NoFence && self.seen.is_some_and(|seen| term < seen) {
            sometimes!(true, reach::REFUSED_ORDER);
            return;
        }
        assert!(
            self.seen.is_none_or(|seen| term >= seen),
            "{FENCE}: an order at {term:?} after {:?}",
            self.seen
        );
        self.seen = Some(self.seen.map_or(term, |seen| seen.max(term)));
    }
}

/// The store's rules for the walk's entries, written again from
/// `specs/tla/coord/Coord.tla` (operator `Apply`), with no code of the
/// fold: it reads the entries as the walk builds them.
#[derive(Clone, Debug, Default)]
pub struct Truth {
    /// The live sessions: the position of the last entry and its `op_seq`.
    live: BTreeMap<SessionId, (u64, u64)>,
    /// The expired sessions.
    dead: BTreeSet<SessionId>,
    /// The latch's candidates: the position of the create, and the session.
    latch: BTreeMap<u64, SessionId>,
    /// The holder and its term.
    holder: Option<(SessionId, u64)>,
    /// The version of the counter.
    count: u64,
    /// How many times each act applied.
    acts: BTreeMap<u64, u32>,
    /// The acts that a member reported failed or expired: they never apply.
    never: BTreeSet<u64>,
    /// The checkpoints taken.
    checkpoints: BTreeSet<u64>,
}

impl Truth {
    fn apply(&mut self, p: u64, entry: Option<&Entry>) {
        match entry {
            Some(Entry::Open { session, .. }) => {
                if !self.live.contains_key(session) && !self.dead.contains(session) {
                    self.live.insert(*session, (p, 0));
                    self.latch.insert(p, *session);
                }
            }
            Some(Entry::Txn {
                session,
                op_seq,
                txn,
            }) => {
                let Some(row) = self.live.get_mut(session) else {
                    return;
                };
                if *op_seq <= row.1 {
                    return;
                }
                *row = (p, *op_seq);
                self.effect(p, *session, txn);
            }
            Some(Entry::Expire { session, last }) => {
                if self.live.get(session).is_some_and(|row| row.0 == last.0) {
                    self.live.remove(session);
                    self.dead.insert(*session);
                    self.latch.retain(|_, s| s != session);
                }
            }
            Some(Entry::Checkpoint { at, .. }) if at.0 == p => {
                self.checkpoints.insert(p);
            }
            None | Some(Entry::Checkpoint { .. }) => {}
        }
        // The holder: the session of the lowest candidate; the term moves
        // when the holder does.
        let holder = self.latch.values().next().copied();
        if holder != self.holder.map(|h| h.0) {
            self.holder = holder.map(|h| (h, p));
        }
    }

    /// The effect of a transaction of the walk's shapes: a heartbeat, an
    /// act, a join, a delete of candidates.
    fn effect(&mut self, p: u64, session: SessionId, txn: &paros_election::Txn) {
        for check in &txn.checks {
            let Check::Holds { term, .. } = check else {
                unreachable!("the walk guards with the latch only")
            };
            if self.holder != Some((session, term.0.0)) {
                return;
            }
        }
        let deletes: Vec<u64> = txn
            .ops
            .iter()
            .filter_map(|op| match op {
                Op::Delete { path, .. } => Some(
                    path.name()
                        .strip_prefix("n-")
                        .and_then(|n| n.parse().ok())
                        .expect("the walk deletes candidates"),
                ),
                _ => None,
            })
            .collect();
        if deletes.iter().any(|d| !self.latch.contains_key(d)) {
            return;
        }
        for op in &txn.ops {
            match op {
                Op::SetData { data, .. } => {
                    let act = u64::from_le_bytes(data.as_slice().try_into().expect("an act id"));
                    assert!(
                        self.holder.is_some_and(|h| h.0 == session),
                        "{ACTS}: act {act} of {session:?} applied, the holder is {:?}",
                        self.holder
                    );
                    self.count += 1;
                    assert!(
                        !self.never.contains(&act),
                        "{SETTLED}: act {act} applied after a member reported it would not"
                    );
                    let applied = self.acts.entry(act).or_insert(0);
                    *applied += 1;
                    assert!(*applied == 1, "{ONCE}: act {act} applied {applied} times");
                    sometimes!(true, reach::ACTED);
                }
                Op::Create { .. } => {
                    self.latch.insert(p, session);
                }
                Op::Delete { .. } | Op::Ensure { .. } => {}
            }
        }
        for d in deletes {
            self.latch.remove(&d);
        }
    }

    /// The fold of the whole log against the truth.
    fn judge(&self, whole: &Tree, latch: &Latch) {
        let live: BTreeMap<SessionId, (u64, u64)> = whole
            .sessions()
            .map(|(s, r)| (s, (r.last.0, r.op_seq)))
            .collect();
        assert!(
            live == self.live,
            "{TRUTH}: sessions {live:?}, truth {:?}",
            self.live
        );
        for session in &self.dead {
            assert!(whole.expired(*session), "{TRUTH}: {session:?} expired");
        }
        let candidates: Vec<SessionId> = latch.candidates(whole).into_iter().map(|c| c.0).collect();
        let truth: Vec<SessionId> = self.latch.values().copied().collect();
        assert!(
            candidates == truth,
            "{TRUTH}: candidates {candidates:?}, truth {truth:?}"
        );
        let holder = latch.holder(whole).map(|t| (t.holder, t.term.0.0));
        assert!(
            holder == self.holder,
            "{TRUTH}: holder {holder:?}, truth {:?}",
            self.holder
        );
        let count = whole.node(&count_path()).map_or(0, |n| n.version);
        assert!(
            count == self.count,
            "{TRUTH}: count {count}, truth {}",
            self.count
        );
        let checkpoints: BTreeSet<u64> = whole.checkpoints().iter().map(|s| s.0).collect();
        assert!(
            checkpoints == self.checkpoints,
            "{TRUTH}: checkpoints {checkpoints:?}, truth {:?}",
            self.checkpoints
        );
    }
}

/// The reach sites of the fate of each landed record.
fn fate_sites(fate: &paros_election::Fate, open: bool) {
    use paros_election::Fate;
    sometimes!(matches!(fate, Fate::Duplicate { .. }), reach::DUP_VOID);
    sometimes!(matches!(fate, Fate::Gone { .. }) && !open, reach::DEAD_VOID);
    sometimes!(matches!(fate, Fate::Gone { .. }) && open, reach::TOMB_VOID);
    sometimes!(
        matches!(
            fate,
            Fate::Done {
                result: Err(paros_election::TxnError {
                    kind: paros_election::ErrorKind::NotHolder,
                    ..
                }),
                ..
            }
        ),
        reach::GUARD_VOID
    );
    sometimes!(matches!(fate, Fate::ExpireVoid { .. }), reach::EXPIRE_VOID);
    sometimes!(matches!(fate, Fate::Expired { .. }), reach::EXPIRED);
}

/// The election walk with `mutant` planted, over `seeds` seeds.
#[must_use]
pub fn walk(mutant: ElectionMutant, seeds: u64) -> Walk<ElectionModel> {
    Walk::<ElectionModel>::new()
        .seeds(seeds)
        .chaos_steps(400)
        .settle_rounds(200)
        .max_down(1)
        .env_prefix(None)
        .first_seed(crate::campaign::first_seed())
        .model(move |cx| ElectionModel::build(mutant, cx))
}
