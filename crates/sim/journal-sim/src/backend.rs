//! The backends of the walks that run the generic driver of
//! `paros-journal-io` (the driver walk, [`crate::host`], and the whole
//! journal walk, [`crate::whole`]): a disk that survives its process and
//! fails commits inside the contract, a transport that sends into the
//! walk's mailbox and judges the flight rule at the wire, and a register
//! on three matchmakers whose write a minority can take, and that loses a
//! request or a reply.
//!
//! Both are the walk's, never the driver's: a fault they inject stays inside
//! the contract of `paros_journal_io::Storage` (a commit that fails `Full`
//! wrote nothing; one that fails `Unknown` may have written its batch; a
//! lost record makes each scan of its role fail `Lost`; a commit that
//! returned `Ok` is durable).

use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::rc::Rc;

use paros_journal::NodeId;
use paros_journal::register::{
    Ballot, CallStep, Change, ChangeRefusal, MatchmakerGeneration, MatchmakerId, MatchmakerSet,
    MemberState, RegisterCall, RegisterMember, RegisterValue, Reply, Written,
};
use paros_journal::roles::Message;
use paros_journal::segment::{RecordRefusal, SegmentRecord, SegmentState};
use paros_journal_io::mem::MemStorage;
use paros_journal_io::{
    Address, ChangeError, Entry, Envelope, Journal, JournalId, Key, Kind, Op, Register, Storage,
    StorageError, Transport,
};

/// The faults that a walk arms on a disk, for its next commit.
#[derive(Clone, Copy, Debug, Default)]
pub struct Faults {
    /// The commit fails `Full` and writes nothing.
    pub fail_before: bool,
    /// The commit writes its batch, then fails `Unknown`.
    pub fail_after: bool,
    /// A planted bug: the commit writes nothing and says it is done.
    pub lie: bool,
}

/// The disk of one process: it survives the process. `seen` looks at each
/// batch that a commit holds, for the reach sites of a walk.
#[derive(Clone)]
pub struct Disk {
    /// The records.
    pub store: Rc<RefCell<MemStorage>>,
    /// The faults armed for the next commit.
    pub faults: Rc<RefCell<Faults>>,
    /// The ops of the last commit that failed `Unknown` after its write.
    unknown: Rc<RefCell<Vec<Op>>>,
    seen: fn(&[Op]),
}

impl Disk {
    /// An empty disk.
    #[must_use]
    pub fn new(seen: fn(&[Op])) -> Self {
        Self {
            store: Rc::default(),
            faults: Rc::default(),
            unknown: Rc::default(),
            seen,
        }
    }

    /// The value at `key`, if the disk holds one. A role whose disk lost a
    /// record has none that can be read.
    ///
    /// # Panics
    ///
    /// When a scan of the memory fails with another fault: a scan of memory
    /// only reports a loss.
    #[must_use]
    pub fn record(&self, key: Key) -> Option<Vec<u8>> {
        let entries = match self.store.borrow_mut().scan(key.address) {
            Ok(entries) => entries,
            Err(StorageError::Lost) => return None,
            Err(fault) => panic!("a scan of memory reports only a loss: {fault}"),
        };
        entries.into_iter().find_map(|(k, entry)| match entry {
            Entry::Value(bytes) if k == key => Some(bytes),
            _ => None,
        })
    }

    /// Whether the disk holds the last op of the last commit that failed
    /// `Unknown`: that commit wrote its batch, and only a new scan can tell.
    #[must_use]
    pub fn wrote_unknown(&self) -> bool {
        self.unknown
            .borrow()
            .last()
            .is_some_and(|op| self.record(op.key).as_ref() == Some(&op.value))
    }

    /// Lose every record: a planted bug, never a fault of the contract.
    pub fn wipe(&self) {
        *self.store.borrow_mut() = MemStorage::new();
    }

    /// Arm a fault of the contract for the next commit: it fails before or
    /// after its write, as `after` says.
    pub fn arm(&self, after: bool) {
        let mut faults = self.faults.borrow_mut();
        if after {
            faults.fail_after = true;
        } else {
            faults.fail_before = true;
        }
    }

    /// Arm the planted bug: the next commit writes nothing and says it is
    /// done.
    pub fn arm_lie(&self) {
        self.faults.borrow_mut().lie = true;
    }

    /// The disk loses the record at `key`: its role is faulty from now on
    /// (a fault of the contract: the loss is reported, never hidden).
    pub fn lose(&self, key: Key) {
        self.store.borrow_mut().lose(key);
    }
}

impl Storage<Journal> for Disk {
    fn commit(&mut self, ops: &[Op]) -> Result<(), StorageError> {
        (self.seen)(ops);
        let faults = std::mem::take(&mut *self.faults.borrow_mut());
        if faults.lie {
            return Ok(());
        }
        if faults.fail_before {
            return Err(StorageError::Full);
        }
        self.store.borrow_mut().commit(ops)?;
        if faults.fail_after {
            *self.unknown.borrow_mut() = ops.to_vec();
            return Err(StorageError::Unknown);
        }
        Ok(())
    }

    fn scan(&mut self, address: Address) -> Result<Vec<(Key, Entry)>, StorageError> {
        self.store.borrow_mut().scan(address)
    }

    fn list(&mut self) -> Result<Vec<Address>, StorageError> {
        self.store.borrow_mut().list()
    }
}

/// The bytes of a ballot as the layout writes it at the head of a promise
/// and of a vote: the round, then the node, big-endian.
fn ballot_bytes(ballot: Ballot) -> Vec<u8> {
    let mut bytes = ballot.round.to_be_bytes().to_vec();
    bytes.extend(ballot.node.0.to_be_bytes());
    bytes
}

/// The transport of one process: it sends into the walk's mailbox (through
/// `outbox`) and receives what the walk delivered. Each send is judged by
/// the flight rule (rule 1 of §J4.2) against the sender's disk: an
/// acceptor's `Accepted` leaves only once a vote at least as new is on its
/// disk, and a `Promise` only once a promise at least as high is. A send
/// that breaks it fails with the walk's oracle text, `flight`.
pub struct Wire {
    /// What the walk delivered to this process.
    pub inbox: Rc<RefCell<VecDeque<Envelope>>>,
    /// What this process sent, for the walk to put in flight.
    pub outbox: Rc<RefCell<Vec<Envelope>>>,
    /// The disk of the process.
    pub disk: Disk,
    /// The text of the walk's flight oracle.
    pub flight: &'static str,
}

impl Transport<Journal> for Wire {
    fn send(&mut self, envelope: Envelope) {
        let flight = self.flight;
        match &envelope.message {
            Message::Accepted { ballot, slot } => {
                let vote = self
                    .disk
                    .record(Key::new(envelope.sender(), Kind::Vote, slot.0));
                assert!(
                    vote.is_some_and(|v| v[..16] >= ballot_bytes(*ballot)[..]),
                    "{flight}: {envelope:?} leaves with no vote on the disk"
                );
            }
            Message::Promise { ballot, .. } => {
                let promise = self
                    .disk
                    .record(Key::new(envelope.sender(), Kind::Promise, 0));
                assert!(
                    promise.is_some_and(|p| p >= ballot_bytes(*ballot)),
                    "{flight}: {envelope:?} leaves with no promise on the disk"
                );
            }
            _ => {}
        }
        self.outbox.borrow_mut().push(envelope);
    }

    fn receive(&mut self) -> Option<Envelope> {
        self.inbox.borrow_mut().pop_front()
    }
}

/// How a register change that got no answer was lost.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Lost {
    /// The request was lost: no write took place.
    Request,
    /// The reply was lost: the change was done (`true`) or refused.
    Reply(bool),
    /// The write reached one member only, and no answer came: a later read
    /// sees it or misses it, and a later operation that sees it carries it
    /// on.
    Half,
}

/// A fault that a walk arms on a process's way to the register, for its
/// next change.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Fault {
    /// The request is lost: the change does not take place.
    Request,
    /// The reply is lost: the change takes place or is refused, and the
    /// process hears nothing.
    Reply,
    /// The write reaches one member only, and the process hears nothing.
    Half,
}

/// How the walk's register runs the write of one change.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Reach {
    /// A majority takes it, and its caller hears the answer.
    Answered,
    /// A majority takes it, and its caller hears nothing.
    Unanswered,
    /// One member takes it, and its caller hears nothing.
    Half,
}

/// A change asked again after a write of it got no answer, and done.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Again {
    /// Its read found its own first write.
    Own,
    /// Its read found a later write that holds it: its first write carried
    /// on by another operation (a replica's boundary state over a seal, a
    /// bind, an election), or the same change of another caller.
    Carried,
}

/// Oracle of the register: a change asked again after a write of it got
/// no answer, over a value that already holds what it writes, is never
/// refused (§J3.4, "a change asked again").
/// A refusal would tell its caller that its write lost, and a first leader
/// would stop for good on its own seal end.
pub const AGAIN: &str = "register: a change asked again over its own write is never refused";

/// The faults armed for a process's next change.
#[derive(Clone, Copy, Debug, Default)]
struct RegisterFaults {
    /// The request is lost: the change does not take place.
    lose_request: bool,
    /// The reply is lost: the change takes place or is refused, and the
    /// process hears nothing.
    lose_reply: bool,
    /// The write reaches one member only, and the process hears nothing.
    half: bool,
}

/// The register of one journal: its members, and the value of the last
/// done write.
struct Members {
    at: BTreeMap<MatchmakerId, RegisterMember>,
    done: RegisterValue,
    /// Each change whose write got no answer (one member took it, or a
    /// majority did and the reply was lost), with its operation: its caller
    /// asks it again.
    half: Vec<(Change, Ballot)>,
}

struct Quorum {
    set: MatchmakerSet,
    journals: BTreeMap<JournalId, Members>,
    next: u64,
    /// The register's own draw of the members that an operation reaches,
    /// a fixed sequence that the walk's seed does not move: which members
    /// a change reaches follows the order of the changes only.
    draw: u64,
    /// The planted bug of [`AGAIN`]: a change asked again over a value
    /// that holds it is refused.
    refuse_again: bool,
    again: fn(Again),
}

impl Quorum {
    /// The three members in a drawn order: a read and a write reach the
    /// first two, a half write the first one.
    fn order(&mut self) -> Vec<MatchmakerId> {
        self.draw ^= self.draw << 13;
        self.draw ^= self.draw >> 7;
        self.draw ^= self.draw << 17;
        let mut members = self.set.members().to_vec();
        let n = members.len();
        let first = usize::try_from(self.draw % 6).expect("small");
        members.rotate_left(first % n);
        if first >= n {
            members.swap(0, 1);
        }
        members
    }
}

/// The journal's register as the matchmakers run it, three members in one
/// generation: each change is one [`RegisterCall`] that reads at two
/// members, checks the guard on the highest write and writes at two
/// members. A walk can make the write of a change reach one member only
/// ([`LossyRegister::arm`], [`Lost::Half`]): its caller hears nothing, a
/// later read may see the write or miss it, and a later operation that sees
/// it carries it on (`specs/tla/segment-records/`, `Write`). A read
/// ([`Register::read`]) gives the value of the last done write.
#[derive(Clone)]
pub struct QuorumRegister(Rc<RefCell<Quorum>>);

impl QuorumRegister {
    /// A register on three members with no journal; `again` is called on
    /// each change asked again after a half write that is done.
    #[must_use]
    pub fn new(again: fn(Again)) -> Self {
        let set = MatchmakerSet::new(
            MatchmakerGeneration(0),
            vec![MatchmakerId(1), MatchmakerId(2), MatchmakerId(3)],
        );
        Self(Rc::new(RefCell::new(Quorum {
            set,
            journals: BTreeMap::new(),
            next: 1,
            draw: 0x9E37_79B9_7F4A_7C15,
            refuse_again: false,
            again,
        })))
    }

    /// Plant the bug of [`AGAIN`].
    pub fn plant_refuse_again(&self) {
        self.0.borrow_mut().refuse_again = true;
    }

    /// The register of `journal`, holding `value` at every member.
    ///
    /// # Panics
    ///
    /// When `journal` has a register already.
    pub fn create(&self, journal: JournalId, value: RegisterValue) {
        let mut quorum = self.0.borrow_mut();
        let set = quorum.set.clone();
        let at = set
            .members()
            .iter()
            .map(|id| {
                let mut state = MemberState::first(*id, set.clone());
                state.written = Written {
                    op: Ballot::zero(),
                    value: value.clone(),
                };
                (*id, RegisterMember::new(*id, state))
            })
            .collect();
        let old = quorum.journals.insert(
            journal,
            Members {
                at,
                done: value,
                half: Vec::new(),
            },
        );
        assert!(old.is_none(), "a journal's register is created once");
    }

    /// The value of the last done write on the register of `journal`.
    #[must_use]
    pub fn value(&self, journal: JournalId) -> Option<RegisterValue> {
        self.0
            .borrow()
            .journals
            .get(&journal)
            .map(|members| members.done.clone())
    }

    /// Run `change` on the register of `journal`, its write as `reach`
    /// says. A write that one member takes answers
    /// [`ChangeError::NoAnswer`].
    ///
    /// # Errors
    ///
    /// The register refused the change, holds no journal of this id, or
    /// the write reached one member only ([`ChangeError`]).
    ///
    /// # Panics
    ///
    /// On [`AGAIN`], or when an operation meets a conflict or a handover:
    /// the walk runs one operation at a time in one generation.
    fn run(
        &self,
        journal: JournalId,
        change: &Change,
        reach: Reach,
    ) -> Result<RegisterValue, ChangeError> {
        let half = reach == Reach::Half;
        let mut quorum = self.0.borrow_mut();
        let op = Ballot {
            round: quorum.next,
            node: NodeId(0),
        };
        quorum.next += 1;
        let readers = quorum.order();
        let writers = quorum.order();
        let (set, refuse_again, again) = (quorum.set.clone(), quorum.refuse_again, quorum.again);
        let Some(register) = quorum.journals.get_mut(&journal) else {
            return Err(ChangeError::Unknown);
        };
        let mut call = RegisterCall::new(op, set, change.clone());
        let mut top = Ballot::zero();
        let mut read = CallStep::Pending;
        for to in &readers {
            let request = call
                .requests()
                .into_iter()
                .find(|(member, _)| member == to)
                .map(|(_, request)| request)
                .expect("a read request to each member");
            let reply = register
                .at
                .get_mut(to)
                .expect("a member")
                .step(request)
                .reply;
            if let Reply::Value { written, .. } = &reply {
                top = top.max(written.op);
            }
            read = call.on_reply(*to, reply);
            if read != CallStep::Pending {
                break;
            }
        }
        let (seen, value) = match read {
            CallStep::Write { seen, value } => (seen, value),
            CallStep::Refused { refusal, seen } => {
                return Err(refuse(register, change, &seen, refusal));
            }
            other => panic!("a read of the walk's register ends in {other:?}"),
        };
        if refuse_again
            && seen == value
            && let Some(refusal) = planted(&seen, change)
        {
            return Err(refuse(register, change, &seen, refusal));
        }
        let takers: &[MatchmakerId] = if half { &writers[..1] } else { &writers[..2] };
        let mut done = None;
        for (to, request) in call.requests() {
            if !takers.contains(&to) {
                continue;
            }
            let reply = register
                .at
                .get_mut(&to)
                .expect("a member")
                .step(request)
                .reply;
            match call.on_reply(to, reply) {
                CallStep::Pending => {}
                CallStep::Done(value) => done = Some(value),
                other => panic!("a write of the walk's register ends in {other:?}"),
            }
        }
        if half {
            assert!(done.is_none(), "one member is never a majority");
            if !register.half.iter().any(|(c, _)| c == change) {
                register.half.push((change.clone(), op));
            }
            return Err(ChangeError::NoAnswer);
        }
        let done = done.expect("two members are a majority");
        assert!(done == value, "a done write is the value it wrote");
        if let Some(at) = register.half.iter().position(|(c, _)| c == change) {
            let (_, first) = register.half.remove(at);
            if seen == value && top >= first {
                again(if top == first {
                    Again::Own
                } else {
                    Again::Carried
                });
            }
        }
        if reach == Reach::Unanswered && !register.half.iter().any(|(c, _)| c == change) {
            register.half.push((change.clone(), op));
        }
        register.done = done.clone();
        Ok(done)
    }
}

/// [`AGAIN`] on a refusal of `change` over `seen`: a change asked again
/// after a write of it got no answer is never refused on a value that
/// holds it.
fn refuse(
    register: &Members,
    change: &Change,
    seen: &RegisterValue,
    refusal: ChangeRefusal,
) -> ChangeError {
    let asked_again = register.half.iter().any(|(c, _)| c == change);
    assert!(
        !(asked_again && holds(seen, change)),
        "{AGAIN}: {change:?} asked again was refused ({refusal:?}) on a value that holds it: {seen:?}"
    );
    ChangeError::Refused(refusal)
}

/// The value holds what `change` writes, by the oracle's own reading of
/// §J3.4: the seal end, the boundary state, the retired or trimmed state.
/// The roles ask a retire and a trim only when their own guard holds.
fn holds(value: &RegisterValue, change: &Change) -> bool {
    let record = |segment| value.segment(segment);
    match change {
        Change::Seal { segment, end } => {
            record(*segment).and_then(SegmentRecord::end) == Some(*end)
        }
        Change::Boundary { segment, state } => {
            record(*segment).and_then(SegmentRecord::boundary) == Some(*state)
        }
        Change::Retire { segment, .. } => {
            record(*segment).map(SegmentRecord::state) == Some(SegmentState::Retired)
        }
        Change::Trim { segment, .. } => {
            record(*segment).map(SegmentRecord::state) == Some(SegmentState::Trimmed)
        }
        _ => false,
    }
}

/// The refusal of the planted bug: the change is refused on the state of
/// its record, as if the register took each record step once.
fn planted(seen: &RegisterValue, change: &Change) -> Option<ChangeRefusal> {
    let segment = match change {
        Change::Seal { segment, .. }
        | Change::Boundary { segment, .. }
        | Change::Retire { segment, .. }
        | Change::Trim { segment, .. } => *segment,
        _ => return None,
    };
    let state = seen.segment(segment)?.state();
    Some(ChangeRefusal::Record(segment, RecordRefusal::State(state)))
}

impl Register for QuorumRegister {
    fn change(
        &mut self,
        journal: JournalId,
        change: &Change,
    ) -> Result<RegisterValue, ChangeError> {
        self.run(journal, change, Reach::Answered)
    }

    fn read(&mut self, journal: JournalId) -> Option<RegisterValue> {
        self.value(journal)
    }
}

/// The shared register as one process reaches it: a change whose request
/// or reply the walk lost answers [`ChangeError::NoAnswer`], and so does a
/// change whose write only one member took, inside the contract of
/// `paros_journal_io::Register` (the write may or may not have taken place,
/// and a minority may hold it). Reads are never lost.
#[derive(Clone)]
pub struct LossyRegister {
    register: QuorumRegister,
    faults: Rc<RefCell<RegisterFaults>>,
    /// Called on each change that got no answer, with how it was lost.
    lost: fn(Lost),
}

impl LossyRegister {
    /// The shared `register` as one process reaches it, which calls `lost`
    /// on each change that gets no answer.
    #[must_use]
    pub fn new(register: QuorumRegister, lost: fn(Lost)) -> Self {
        Self {
            register,
            faults: Rc::default(),
            lost,
        }
    }

    /// Lose the next change as `fault` says.
    pub fn arm(&self, fault: Fault) {
        let mut faults = self.faults.borrow_mut();
        match fault {
            Fault::Request => faults.lose_request = true,
            Fault::Reply => faults.lose_reply = true,
            Fault::Half => faults.half = true,
        }
    }
}

impl Register for LossyRegister {
    fn change(
        &mut self,
        journal: JournalId,
        change: &Change,
    ) -> Result<RegisterValue, ChangeError> {
        let faults = std::mem::take(&mut *self.faults.borrow_mut());
        if faults.lose_request {
            (self.lost)(Lost::Request);
            return Err(ChangeError::NoAnswer);
        }
        let reach = if faults.half {
            Reach::Half
        } else if faults.lose_reply {
            Reach::Unanswered
        } else {
            Reach::Answered
        };
        let result = self.register.run(journal, change, reach);
        if faults.half && result == Err(ChangeError::NoAnswer) {
            (self.lost)(Lost::Half);
            return result;
        }
        if faults.lose_reply {
            (self.lost)(Lost::Reply(result.is_ok()));
            return Err(ChangeError::NoAnswer);
        }
        result
    }

    fn read(&mut self, journal: JournalId) -> Option<RegisterValue> {
        self.register.read(journal)
    }
}
