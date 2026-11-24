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
//! wrote nothing; one that fails `Unknown` wrote its batch or wrote nothing;
//! a scan that fails `Full` or `Unknown` read nothing; a list that fails
//! `Full` read nothing and may be asked again; a lost record makes each scan
//! of its role fail `Lost`; a commit that returned `Ok` is durable).

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::rc::Rc;

use paros_journal::register::{
    Ballot, CallStep, Change, ChangeRefusal, MatchmakerGeneration, MatchmakerId, MatchmakerSet,
    MemberState, RegisterCall, RegisterMember, RegisterValue, Reply, Written,
};
use paros_journal::roles::Message;
use paros_journal::segment::{RecordRefusal, SegmentRecord, SegmentState};
use paros_journal::{NodeId, SegmentId, Slot};
use paros_journal_io::mem::MemStorage;
use paros_journal_io::{
    Address, ChangeError, Entry, Envelope, Journal, JournalId, Key, Kind, Op, Register, Storage,
    StorageError, Transport,
};

/// Oracle: the list at a boot names each role that the disk holds, and
/// only those.
pub const LISTED: &str =
    "disk: a list at a boot names each role that the disk holds, and only those";

/// A fault of the contract that a walk arms on a disk. Each fires once, on
/// the next call of its kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DiskFault {
    /// The next commit fails `Full` and writes nothing.
    CommitFull,
    /// The next commit writes its batch, then fails `Unknown`.
    CommitUnknownWritten,
    /// The next commit writes nothing and fails `Unknown`.
    CommitUnknownUnwritten,
    /// The next scan fails `Full` and reads nothing.
    ScanFull,
    /// The next scan fails `Unknown` and reads nothing.
    ScanUnknown,
    /// The next list fails `Full` and reads nothing.
    ListFull,
}

impl DiskFault {
    /// The two commit faults that a walk draws on each disk fault: one of
    /// them, each with chance 1/2.
    pub const COMMIT: [Self; 2] = [Self::CommitFull, Self::CommitUnknownWritten];
    /// The other faults of the contract: a walk draws one of them too, on
    /// each disk fault, so they add to the commit faults and never take
    /// their place.
    pub const MORE: [Self; 4] = [
        Self::CommitUnknownUnwritten,
        Self::ScanFull,
        Self::ScanUnknown,
        Self::ListFull,
    ];
}

/// The faults armed on a disk.
#[derive(Clone, Debug, Default)]
pub struct Faults {
    /// The faults of the next commits, in order: each commit takes the
    /// first, so a fault armed on top of another never replaces it.
    pub commit: VecDeque<DiskFault>,
    /// The fault of the next scan: `Full` or `Unknown`.
    pub scan: Option<StorageError>,
    /// The next list fails `Full`.
    pub list: bool,
    /// A planted bug: the commit writes nothing and says it is done.
    pub lie: bool,
    /// A planted bug: each list leaves out its first role.
    pub hide: bool,
}

/// The disk of one process: it survives the process. `seen` looks at each
/// batch that a commit holds, for the reach sites of a walk.
#[derive(Clone)]
pub struct Disk {
    /// The records.
    pub store: Rc<RefCell<MemStorage>>,
    /// The faults armed for the next commit.
    pub faults: Rc<RefCell<Faults>>,
    /// The ops of the last commit that failed `Unknown`, and whether it
    /// wrote them.
    unknown: Rc<RefCell<(Vec<Op>, bool)>>,
    /// The fault of the last scan that failed.
    scan_failed: Rc<Cell<Option<StorageError>>>,
    /// The roles that the disk holds a record of, or lost one of, as the
    /// commits and the losses wrote them: the list at a boot names these.
    roles: Rc<RefCell<BTreeSet<Address>>>,
    /// One more on each change of the records: a commit that writes, a
    /// wipe, a loss.
    version: Rc<Cell<u64>>,
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
            scan_failed: Rc::default(),
            roles: Rc::default(),
            version: Rc::default(),
            seen,
        }
    }

    /// The version of the records: it changes on each change of them, so
    /// a judge of the records judges a version once.
    #[must_use]
    pub fn version(&self) -> u64 {
        self.version.get()
    }

    fn changed(&self) {
        self.version.set(self.version.get() + 1);
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

    /// Whether the last commit that failed `Unknown` wrote its batch: only
    /// a new scan can tell the driver.
    ///
    /// # Panics
    ///
    /// When the disk does not hold what the commit wrote: a later commit
    /// came from a driver that the fault halted.
    #[must_use]
    pub fn wrote_unknown(&self) -> bool {
        let (ops, wrote) = &*self.unknown.borrow();
        if *wrote {
            assert!(
                ops.last()
                    .is_none_or(|op| self.record(op.key).as_ref() == Some(&op.value)),
                "a disk holds the batch of a commit that failed Unknown after its write"
            );
        }
        *wrote
    }

    /// The fault of the last scan that failed, once: a walk takes it when
    /// the driver halted or a role stopped unreadable.
    #[must_use]
    pub fn take_scan_failed(&self) -> Option<StorageError> {
        self.scan_failed.take()
    }

    /// Judge the list of a boot: it names each role that a commit wrote or
    /// a loss hit, and only those. The disk keeps that set itself, so the
    /// judge does not trust the list of the store.
    ///
    /// # Panics
    ///
    /// When the list misses a held role or names a role with no record.
    pub fn judge_list(&self, listed: &[Address]) {
        let roles = self.roles.borrow();
        for address in listed {
            assert!(
                roles.contains(address),
                "{LISTED}: it names {address:?}, with no record"
            );
        }
        for address in roles.iter() {
            assert!(listed.contains(address), "{LISTED}: it misses {address:?}");
        }
    }

    /// A new driver boots on the disk: the scan fault and the `Unknown`
    /// commit of the driver before it belong to that driver, so a halt of
    /// the new one never reads them.
    pub fn boot(&self) {
        self.scan_failed.set(None);
        *self.unknown.borrow_mut() = (Vec::new(), false);
    }

    /// Arm one disk fault of a walk: one commit fault, `Full` or `Unknown`
    /// after the write, each with chance 1/2, and one more fault of
    /// [`DiskFault::MORE`] on top.
    pub fn arm_drawn(&self, commit_after: bool, more: DiskFault) {
        self.arm(DiskFault::COMMIT[usize::from(commit_after)]);
        self.arm(more);
    }

    /// Lose every record: a planted bug, never a fault of the contract.
    pub fn wipe(&self) {
        *self.store.borrow_mut() = MemStorage::new();
        self.roles.borrow_mut().clear();
        self.changed();
    }

    /// Lose every record of the role at `address`, with no report: a
    /// planted bug, never a fault of the contract. The other roles of the
    /// disk keep their records.
    ///
    /// # Panics
    ///
    /// When a scan of the memory fails, or the disk holds a corrupt record:
    /// the walks that plant this bug corrupt nothing.
    pub fn wipe_role(&self, address: Address) {
        let mut store = self.store.borrow_mut();
        let mut kept = MemStorage::new();
        for other in store.list().expect("a list of memory reads it") {
            if other == address {
                continue;
            }
            let ops: Vec<Op> = store
                .scan(other)
                .expect("a disk with a wiped role lost no record")
                .into_iter()
                .map(|(key, entry)| match entry {
                    Entry::Value(value) => Op { key, value },
                    Entry::Corrupt => panic!("a disk with a wiped role holds no corrupt record"),
                })
                .collect();
            kept.commit(&ops).expect("a commit of memory writes it");
        }
        *store = kept;
        drop(store);
        self.roles.borrow_mut().remove(&address);
        self.changed();
    }

    /// Arm a fault of the contract for the next call of its kind.
    pub fn arm(&self, fault: DiskFault) {
        let mut faults = self.faults.borrow_mut();
        match fault {
            DiskFault::CommitFull
            | DiskFault::CommitUnknownWritten
            | DiskFault::CommitUnknownUnwritten => faults.commit.push_back(fault),
            DiskFault::ScanFull => faults.scan = Some(StorageError::Full),
            DiskFault::ScanUnknown => faults.scan = Some(StorageError::Unknown),
            DiskFault::ListFull => faults.list = true,
        }
    }

    /// Plant the bug of a list that leaves out its first role, for good.
    pub fn hide_a_role(&self) {
        self.faults.borrow_mut().hide = true;
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
        self.roles.borrow_mut().insert(key.address);
        self.changed();
    }
}

impl Storage<Journal> for Disk {
    fn commit(&mut self, ops: &[Op]) -> Result<(), StorageError> {
        (self.seen)(ops);
        let commit = {
            let mut faults = self.faults.borrow_mut();
            if std::mem::take(&mut faults.lie) {
                return Ok(());
            }
            faults.commit.pop_front()
        };
        match commit {
            Some(DiskFault::CommitFull) => return Err(StorageError::Full),
            Some(DiskFault::CommitUnknownUnwritten) => {
                *self.unknown.borrow_mut() = (ops.to_vec(), false);
                return Err(StorageError::Unknown);
            }
            _ => {}
        }
        self.store.borrow_mut().commit(ops)?;
        self.roles
            .borrow_mut()
            .extend(ops.iter().map(|op| op.key.address));
        self.changed();
        if commit == Some(DiskFault::CommitUnknownWritten) {
            *self.unknown.borrow_mut() = (ops.to_vec(), true);
            return Err(StorageError::Unknown);
        }
        Ok(())
    }

    fn scan(&mut self, address: Address) -> Result<Vec<(Key, Entry)>, StorageError> {
        if let Some(fault) = self.faults.borrow_mut().scan.take() {
            self.scan_failed.set(Some(fault));
            return Err(fault);
        }
        self.store.borrow_mut().scan(address)
    }

    fn list(&mut self) -> Result<Vec<Address>, StorageError> {
        let hide = {
            let mut faults = self.faults.borrow_mut();
            if std::mem::take(&mut faults.list) {
                return Err(StorageError::Full);
            }
            faults.hide
        };
        let mut listed = self.store.borrow_mut().list()?;
        if hide && !listed.is_empty() {
            listed.remove(0);
        }
        Ok(listed)
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
    /// A majority of the members was down: the read did not close, and
    /// nothing was written.
    Down,
    /// The write is in flight: it lands later ([`QuorumRegister::land`]),
    /// at a majority, at one member, or nowhere when a later operation
    /// came first.
    Late,
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
    /// The read runs now and the write later, and the process hears
    /// nothing: two operations interleave between a read and a write.
    Late,
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
    /// The write waits in flight, and its caller hears nothing.
    Late,
}

/// What a late write did when it landed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Landed {
    /// A majority took it: its change is done.
    Done,
    /// A later operation reached a member first: the write lost there,
    /// and at most a minority took it.
    Conflict,
    /// Fewer than a majority of the members were up: at most one took it.
    Partial,
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

/// The facts that a member of a journal's register ever held, written by
/// a done write or by a half write: the truth that a judge of a stop reads
/// (a role may stop on a value that a minority holds). Each fact only grows,
/// though a later write may replace a half write at its member.
#[derive(Clone, Debug, Default)]
pub struct Held {
    /// The highest ballot ever bound.
    pub bound: Option<Ballot>,
    /// Each seal end ever written, by segment.
    pub ends: BTreeSet<(SegmentId, Slot)>,
    /// The newest segment ever written.
    pub newest: Option<SegmentId>,
    /// Each controller term ever written: its number and its holder.
    pub terms: BTreeSet<(u64, Option<NodeId>)>,
}

impl Held {
    fn fold(&mut self, value: &RegisterValue) {
        self.bound = self.bound.max(value.bound());
        self.newest = self.newest.max(value.newest().map(SegmentRecord::id));
        self.terms
            .insert((value.term().number, value.term().holder));
        for record in value.segments().values() {
            if let Some(end) = record.end() {
                self.ends.insert((record.id(), end));
            }
        }
    }

    /// Whether a member ever held an end of `segment` other than `end`.
    #[must_use]
    pub fn other_end(&self, segment: SegmentId, end: Slot) -> bool {
        self.ends.iter().any(|(s, e)| *s == segment && *e != end)
    }

    /// Whether a member ever held a term that deposes `me` at term
    /// `number`: a later number, or the same number won by another.
    #[must_use]
    pub fn deposes(&self, number: u64, me: NodeId) -> bool {
        self.terms
            .iter()
            .any(|(n, holder)| *n > number || (*n == number && *holder != Some(me)))
    }

    /// Whether a member ever held an end of `segment`.
    #[must_use]
    pub fn sealed(&self, segment: SegmentId) -> bool {
        self.ends.iter().any(|(s, _)| *s == segment)
    }
}

/// The register of one journal: its members, and the value of the last
/// done write.
struct Members {
    at: BTreeMap<MatchmakerId, RegisterMember>,
    done: RegisterValue,
    /// The operation of the last done write.
    done_op: Ballot,
    /// Every fact a member ever held.
    held: Held,
    /// Each change whose write got no answer (one member took it, or a
    /// majority did and the reply was lost), with its operation: its caller
    /// asks it again.
    half: Vec<(Change, Ballot)>,
}

/// A write whose read closed and that waits in flight.
struct LateWrite {
    journal: JournalId,
    op: Ballot,
    call: RegisterCall,
}

struct Quorum {
    set: MatchmakerSet,
    journals: BTreeMap<JournalId, Members>,
    next: u64,
    /// The members whose machine is down: no request reaches them.
    down: BTreeSet<MatchmakerId>,
    /// The writes in flight, oldest first.
    late: Vec<LateWrite>,
    /// The register's own draw of the members that an operation reaches.
    /// It starts from the walk's seed, and then follows the order of the
    /// register's operations (changes, the repairs of reads and the late
    /// writes that land): the walk's other draws do not move it.
    draw: u64,
    /// The planted bug of [`AGAIN`]: a change asked again over a value
    /// that holds it is refused.
    refuse_again: bool,
    /// A planted bug: a seal is refused on a value whose guard takes it,
    /// unless it is asked again after a write of it got no answer.
    refuse_seal: bool,
    again: fn(Again),
    /// Called on each read that repaired.
    repaired: fn(),
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
/// members that are up, checks the guard on the highest write and writes
/// at two members that are up. A member is up while its machine is
/// ([`QuorumRegister::set_up`]): with two members down no read closes, and
/// the register takes no change. A walk can make the write of a change
/// reach one member only ([`LossyRegister::arm`], [`Lost::Half`]), or wait
/// in flight and land later ([`Lost::Late`], [`QuorumRegister::land`]),
/// after other operations ran between its read and its write: its caller
/// hears nothing, a later read may see the write or miss it, and a later
/// operation that sees it carries it on (`specs/tla/segment-records/`,
/// `Write`). A read ([`Register::read`]) is a quorum read
/// ([`QuorumRegister::quorum_read`]): it can give nothing. The walk's own
/// truth is [`QuorumRegister::value`], the value of the last done write.
#[derive(Clone)]
pub struct QuorumRegister(Rc<RefCell<Quorum>>);

impl QuorumRegister {
    /// A register on three members with no journal; `again` is called on
    /// each change asked again after a half write that is done, `repaired`
    /// on each read that repairs. `draw`
    /// starts the draw of the members that each operation reaches: a walk
    /// takes it from its seed, so each seed sees its own orders.
    #[must_use]
    pub fn new(again: fn(Again), repaired: fn(), draw: u64) -> Self {
        let set = MatchmakerSet::new(
            MatchmakerGeneration(0),
            vec![MatchmakerId(1), MatchmakerId(2), MatchmakerId(3)],
        );
        Self(Rc::new(RefCell::new(Quorum {
            set,
            journals: BTreeMap::new(),
            next: 1,
            down: BTreeSet::new(),
            late: Vec::new(),
            // A xorshift never leaves zero.
            draw: draw.max(1),
            refuse_again: false,
            refuse_seal: false,
            again,
            repaired,
        })))
    }

    /// Plant the bug of [`AGAIN`].
    pub fn plant_refuse_again(&self) {
        self.0.borrow_mut().refuse_again = true;
    }

    /// Plant a register that refuses each seal on a value whose guard
    /// takes it, unless the seal is asked again after a write of it got no
    /// answer: a sealing leader stops with no true cause (the `STOPPED`
    /// oracle of the whole walk).
    pub fn plant_refuse_seal(&self) {
        self.0.borrow_mut().refuse_seal = true;
    }

    /// Every fact a member of the register of `journal` ever held.
    #[must_use]
    pub fn held(&self, journal: JournalId) -> Held {
        self.0
            .borrow()
            .journals
            .get(&journal)
            .map_or_else(Held::default, |members| members.held.clone())
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
        let mut held = Held::default();
        held.fold(&value);
        let old = quorum.journals.insert(
            journal,
            Members {
                at,
                done: value,
                done_op: Ballot::zero(),
                held,
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

    /// Whether the member `id` is up: a member is up while its machine is.
    pub fn set_up(&self, id: MatchmakerId, up: bool) {
        let mut quorum = self.0.borrow_mut();
        if up {
            quorum.down.remove(&id);
        } else {
            quorum.down.insert(id);
        }
    }

    /// The number of writes in flight.
    #[must_use]
    pub fn in_flight(&self) -> usize {
        self.0.borrow().late.len()
    }

    /// Land the write in flight at `pick` (an index below
    /// [`in_flight`](Self::in_flight)) at the first two members of a drawn
    /// order that are up. It is done at a majority, loses to a later
    /// operation that reached a member first, or reaches fewer than two
    /// members.
    ///
    /// # Panics
    ///
    /// When `pick` names no write in flight, or a late write is done below
    /// a done write: two majorities always meet, so a later operation that
    /// was done read at a member that refuses the late write.
    #[must_use]
    pub fn land(&self, pick: usize) -> Landed {
        let mut quorum = self.0.borrow_mut();
        let LateWrite {
            journal,
            op,
            mut call,
        } = quorum.late.remove(pick);
        let writers = quorum.order();
        let down = quorum.down.clone();
        let register = quorum.journals.get_mut(&journal).expect("a journal");
        let takers: Vec<MatchmakerId> = writers
            .into_iter()
            .filter(|m| !down.contains(m))
            .take(2)
            .collect();
        match write(register, &mut call, &takers) {
            Err(()) => Landed::Conflict,
            Ok(None) => Landed::Partial,
            Ok(Some(done)) => {
                assert!(
                    op > register.done_op,
                    "a late write is done above every done write: {op:?} after {:?}",
                    register.done_op
                );
                register.done = done;
                register.done_op = op;
                Landed::Done
            }
        }
    }

    /// A quorum read of the register of `journal`: the members that are up
    /// answer what they hold; the top write is the answer when a majority
    /// holds it. Else the read repairs, a choice of this fake: it runs a
    /// `Confirm` of the newest segment's state of the top write, an
    /// operation that reads at two members and writes the value it reads
    /// again, so a write that a minority took is done or replaced, and the
    /// read gives the done value. The `Confirm` can be refused (its read
    /// missed the minority's write, and the record it names differs or is
    /// not there), and then the read gives `None`. `None` too when fewer
    /// than two members are up.
    #[must_use]
    pub fn quorum_read(&self, journal: JournalId) -> Option<RegisterValue> {
        let confirm = {
            let quorum = self.0.borrow();
            let register = quorum.journals.get(&journal)?;
            let held: Vec<&Written> = register
                .at
                .iter()
                .filter(|(m, _)| !quorum.down.contains(m))
                .map(|(_, member)| &member.state().written)
                .collect();
            let top = held.iter().max_by_key(|w| w.op)?;
            let holders = held.iter().filter(|w| w.op == top.op).count();
            if holders > register.at.len() / 2 {
                return Some(top.value.clone());
            }
            let newest = top.value.newest()?;
            Change::Confirm {
                segment: newest.id(),
                state: newest.state(),
            }
        };
        let (result, _) = self.run(journal, &confirm, Reach::Answered);
        if result.is_ok() {
            (self.0.borrow().repaired)();
        }
        result.ok()
    }

    /// Run `change` on the register of `journal` with `fault`, if any: the
    /// result its caller hears, and how a change that got no answer was
    /// lost.
    pub fn change_with(
        &self,
        journal: JournalId,
        change: &Change,
        fault: Option<Fault>,
    ) -> (Result<RegisterValue, ChangeError>, Option<Lost>) {
        if fault == Some(Fault::Request) {
            return (Err(ChangeError::NoAnswer), Some(Lost::Request));
        }
        let reach = match fault {
            None => Reach::Answered,
            Some(Fault::Reply) => Reach::Unanswered,
            Some(Fault::Half) => Reach::Half,
            Some(Fault::Late) => Reach::Late,
            Some(Fault::Request) => unreachable!("answered above"),
        };
        let (result, lost) = self.run(journal, change, reach);
        if reach == Reach::Unanswered {
            return (
                Err(ChangeError::NoAnswer),
                Some(Lost::Reply(result.is_ok())),
            );
        }
        (result, lost)
    }

    /// Run `change` on the register of `journal`, its write as `reach`
    /// says, at the members that are up. A write that one member takes, a
    /// read that a majority of down members does not let close, and a
    /// write left in flight answer [`ChangeError::NoAnswer`], with how.
    ///
    /// # Panics
    ///
    /// On [`AGAIN`], or when a new operation meets a conflict or a
    /// handover: each new operation is above every earlier one, and the
    /// walk runs one generation.
    fn run(
        &self,
        journal: JournalId,
        change: &Change,
        reach: Reach,
    ) -> (Result<RegisterValue, ChangeError>, Option<Lost>) {
        let mut quorum = self.0.borrow_mut();
        let op = Ballot {
            round: quorum.next,
            node: NodeId(0),
        };
        quorum.next += 1;
        let readers = quorum.order();
        let writers = quorum.order();
        let (set, refuse_again, refuse_seal, again) = (
            quorum.set.clone(),
            quorum.refuse_again,
            quorum.refuse_seal,
            quorum.again,
        );
        let down = quorum.down.clone();
        let up = |m: &&MatchmakerId| !down.contains(*m);
        let Some(register) = quorum.journals.get_mut(&journal) else {
            return (Err(ChangeError::Unknown), None);
        };
        let mut call = RegisterCall::new(op, set, change.clone());
        let mut top = Ballot::zero();
        let mut read = CallStep::Pending;
        for to in readers.iter().filter(up) {
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
                return (Err(refuse(register, change, &seen, refusal)), None);
            }
            CallStep::Pending => return (Err(ChangeError::NoAnswer), Some(Lost::Down)),
            other => panic!("a read of the walk's register ends in {other:?}"),
        };
        if let Some(error) = plant(register, (refuse_again, refuse_seal), change, &seen, &value) {
            return (Err(error), None);
        }
        if reach != Reach::Answered && !register.half.iter().any(|(c, _)| c == change) {
            register.half.push((change.clone(), op));
        }
        if reach == Reach::Late {
            quorum.late.push(LateWrite { journal, op, call });
            return (Err(ChangeError::NoAnswer), Some(Lost::Late));
        }
        let width = if reach == Reach::Half { 1 } else { 2 };
        let takers: Vec<MatchmakerId> = writers.iter().filter(up).take(width).copied().collect();
        let done = write(register, &mut call, &takers)
            .unwrap_or_else(|()| panic!("a new operation of the walk's register meets a conflict"));
        let Some(done) = done else {
            assert!(reach == Reach::Half, "two up members take a write");
            return (Err(ChangeError::NoAnswer), Some(Lost::Half));
        };
        assert!(done == value, "a done write is the value it wrote");
        if reach == Reach::Answered
            && let Some(at) = register.half.iter().position(|(c, _)| c == change)
        {
            let (_, first) = register.half.remove(at);
            if seen == value && top >= first {
                again(if top == first {
                    Again::Own
                } else {
                    Again::Carried
                });
            }
        }
        register.done = done.clone();
        register.done_op = op;
        (Ok(done), None)
    }
}

/// Send the write requests of `call` to `takers`, and fold what each
/// member holds after into the facts of the register. `Ok(Some)` when a
/// majority took it, `Ok(None)` when fewer did, `Err` when a member had
/// seen a later operation.
fn write(
    register: &mut Members,
    call: &mut RegisterCall,
    takers: &[MatchmakerId],
) -> Result<Option<RegisterValue>, ()> {
    let mut done = None;
    for (to, request) in call.requests() {
        if !takers.contains(&to) {
            continue;
        }
        let member = register.at.get_mut(&to).expect("a member");
        let reply = member.step(request).reply;
        register.held.fold(&member.state().written.value);
        match call.on_reply(to, reply) {
            CallStep::Pending => {}
            CallStep::Done(value) => done = Some(value),
            CallStep::Conflict { .. } => return Err(()),
            other => panic!("a write of the walk's register ends in {other:?}"),
        }
    }
    Ok(done)
}

/// The refusal of a planted bug, if one is planted and applies:
/// `refuse_again` refuses a change over a value that holds it already,
/// `refuse_seal` refuses a seal that is not asked again after a write of
/// it got no answer.
fn plant(
    register: &Members,
    (refuse_again, refuse_seal): (bool, bool),
    change: &Change,
    seen: &RegisterValue,
    value: &RegisterValue,
) -> Option<ChangeError> {
    let refusal = planted(seen, change)?;
    if refuse_again && seen == value {
        return Some(refuse(register, change, seen, refusal));
    }
    let first = !register.half.iter().any(|(c, _)| c == change);
    (refuse_seal && first && matches!(change, Change::Seal { .. }))
        .then_some(ChangeError::Refused(refusal))
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
        self.change_with(journal, change, None).0
    }

    fn read(&mut self, journal: JournalId) -> Option<RegisterValue> {
        self.quorum_read(journal)
    }
}

/// The shared register as one process reaches it: a change whose request
/// or reply the walk lost answers [`ChangeError::NoAnswer`], and so does a
/// change whose write only one member took or that waits in flight, inside
/// the contract of `paros_journal_io::Register` (the write may or may not
/// have taken place, may take place later, and a minority may hold it).
#[derive(Clone)]
pub struct LossyRegister {
    register: QuorumRegister,
    /// The fault armed for the next change.
    fault: Rc<RefCell<Option<Fault>>>,
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
            fault: Rc::default(),
            lost,
        }
    }

    /// Lose the next change as `fault` says.
    pub fn arm(&self, fault: Fault) {
        *self.fault.borrow_mut() = Some(fault);
    }
}

impl Register for LossyRegister {
    fn change(
        &mut self,
        journal: JournalId,
        change: &Change,
    ) -> Result<RegisterValue, ChangeError> {
        let fault = self.fault.borrow_mut().take();
        let (result, lost) = self.register.change_with(journal, change, fault);
        if let Some(lost) = lost {
            (self.lost)(lost);
        }
        result
    }

    fn read(&mut self, journal: JournalId) -> Option<RegisterValue> {
        self.register.read(journal)
    }
}
