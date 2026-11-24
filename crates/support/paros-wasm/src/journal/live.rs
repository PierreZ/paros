//! The live journal of the site's `/journal/live/` page: the whole journal
//! v1 on six hosts, run in the browser, with buttons.
//!
//! It is the scenario of the facade example `full_journal`
//! (`crates/lib/paros/examples/full_journal.rs`) on one thread: the same
//! roles on the same code, and the reader drives it. Each host is one
//! generic [`Driver`] of `paros-journal-io` with its disk, its endpoint on
//! the shared network and the shared clock. The roles:
//!
//! - three **matchmakers** on hosts 1 to 3 hold the journal's register: a
//!   [`RegisterMember`] on each, one [`RegisterCall`] for each change;
//! - six **acceptors**, one on each host, on a v1 grid of two columns of
//!   three (`F = 1`);
//! - three **replicas** for each segment (odd segments on hosts 1, 3 and 5,
//!   even segments on hosts 2, 4 and 6);
//! - one **leader** for each segment, on the first host of its replicas that
//!   is up;
//! - one writer, the pure **client** of `paros_journal::client`.
//!
//! The page moves the clock one tick at a time ([`Live::step`]) and draws
//! the [`View`] after each. The reader's moves ([`Live::act`]) ask a call of
//! the client (append, read), crash or restart a host, or open the next
//! segment (a roll). Every answer, every refusal, every seal and every
//! retire comes from the roles; this module only routes the messages and
//! the client's requests, as a program does.
//!
//! No randomness: the same moves at the same ticks give the same run.

use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::rc::Rc;

use paros_journal::client::{
    CallId as ClientCall, Client, Mode, Outcome, ReadAnswer, Request, RequestId, Response,
};
use paros_journal::register::{
    Ballot, CallStep, Change, MatchmakerGeneration, MatchmakerId, MatchmakerSet, MemberState,
    RegisterCall, RegisterMember, RegisterValue,
};
use paros_journal::resolve::ResolveAnswer;
use paros_journal::roles::{Campaign, JournalAcceptor, LeaderPhase};
use paros_journal::{
    Grid, Leader, LeaderUuid, NodeId, OpeningCall, RELEASE, Record, SegmentId, SegmentRecord,
    SegmentState, Seq, Slot,
};
use paros_journal_io::mem::{Endpoint, ManualClock, MemNetwork, MemStorage};
use paros_journal_io::{
    Address, Assignment, CallId, CallOutcome, ChangeError, Driver, HostConfig, JournalId, Register,
};
use serde::{Deserialize, Serialize};

use super::records::state_text;

/// The journal of the page.
const JOURNAL: JournalId = JournalId(1);
/// The nanoseconds of one tick.
const TICK: u64 = 1_000_000;
const CONFIG: HostConfig = HostConfig {
    tick: TICK,
    resend_page: 16,
    window: 64,
};
/// The hosts, 1 to 6. Acceptor `n` runs on host `n`.
pub const HOSTS: [u64; 6] = [1, 2, 3, 4, 5, 6];
/// The two columns of the grid.
const COLUMNS: [[u64; 3]; 2] = [[1, 2, 3], [4, 5, 6]];
/// The matchmakers run on hosts 1 to 3.
const MATCHMAKERS: [u64; 3] = [1, 2, 3];
/// The writer's uuid.
const A: LeaderUuid = LeaderUuid(0xA);
/// The events the view keeps.
const EVENTS: usize = 14;

type Process = Driver<MemStorage, Endpoint, ManualClock, Matchmakers, ()>;

fn at(node: u64) -> Address {
    Address::new(JOURNAL, NodeId(node))
}

/// The address of replica `node`: its segment is `node / 10`.
fn replica_at(node: u64) -> Address {
    Address::replica(JOURNAL, NodeId(node), SegmentId(node / 10))
}

/// The replicas of segment `s`: nodes `10 s + 1 ..= 10 s + 3`.
fn replicas(segment: u64) -> [u64; 3] {
    [1, 2, 3].map(|i| 10 * segment + i)
}

/// The host of replica `node`: odd segments on hosts 1, 3 and 5, even
/// segments on hosts 2, 4 and 6.
fn replica_host(node: u64) -> u64 {
    let k = node % 10;
    if (node / 10) % 2 == 1 {
        2 * k - 1
    } else {
        2 * k
    }
}

/// The first node id of the leaders, above every replica id.
const LEADERS: u64 = 1_000_000;

/// The leader of segment `s`: node `LEADERS + s`, apart from every replica
/// node `10 s + k`.
fn leader_node(segment: u64) -> u64 {
    LEADERS + segment
}

// ---- the register on the matchmakers ---------------------------------------

/// One matchmaker: its register state (its disk, kept across a crash), and
/// whether its host is up.
struct Matchmaker {
    member: RegisterMember,
    up: bool,
}

/// The journal's register on its three matchmakers. A [`RegisterCall`]
/// reads at a majority, checks the guard of its change and writes at a
/// majority; one runs at a time, so a write is never left at a minority.
#[derive(Clone)]
struct Matchmakers {
    set: MatchmakerSet,
    members: Rc<RefCell<BTreeMap<MatchmakerId, Matchmaker>>>,
    ops: Rc<RefCell<u64>>,
    caller: NodeId,
}

impl Matchmakers {
    fn new() -> Self {
        let set = MatchmakerSet::new(
            MatchmakerGeneration(1),
            MATCHMAKERS.map(MatchmakerId).to_vec(),
        );
        let members = MATCHMAKERS
            .map(|m| {
                let id = MatchmakerId(m);
                let member = RegisterMember::new(id, MemberState::first(id, set.clone()));
                (id, Matchmaker { member, up: true })
            })
            .into_iter()
            .collect();
        Self {
            set,
            members: Rc::new(RefCell::new(members)),
            ops: Rc::new(RefCell::new(0)),
            caller: NodeId(0),
        }
    }

    fn client(&self, caller: u64) -> Self {
        Self {
            caller: NodeId(caller),
            ..self.clone()
        }
    }

    fn set_up(&self, host: u64, up: bool) {
        if let Some(m) = self.members.borrow_mut().get_mut(&MatchmakerId(host)) {
            m.up = up;
        }
    }

    /// One operation of `change`: the step that ends it.
    fn run(&self, change: &Change) -> CallStep {
        let round = {
            let mut ops = self.ops.borrow_mut();
            *ops += 1;
            *ops
        };
        let op = Ballot {
            round,
            node: self.caller,
        };
        let mut members = self.members.borrow_mut();
        let mut call = RegisterCall::new(op, self.set.clone(), change.clone());
        loop {
            let mut next_phase = false;
            for (id, request) in call.requests() {
                let m = members.get_mut(&id).expect("a member of the set");
                if !m.up {
                    continue;
                }
                match call.on_reply(id, m.member.step(request).reply) {
                    CallStep::Pending => {}
                    CallStep::Write { .. } => {
                        next_phase = true;
                        break;
                    }
                    step => return step,
                }
            }
            if !next_phase {
                return CallStep::Pending;
            }
        }
    }

    /// What each matchmaker holds: up, the generation of its write, and its
    /// value.
    fn holdings(&self) -> Vec<(u64, bool, u64, RegisterValue)> {
        self.members
            .borrow()
            .iter()
            .map(|(id, m)| {
                let written = &m.member.state().written;
                (id.0, m.up, written.op.round, written.value.clone())
            })
            .collect()
    }
}

impl Register for Matchmakers {
    fn change(
        &mut self,
        journal: JournalId,
        change: &Change,
    ) -> Result<RegisterValue, ChangeError> {
        if journal != JOURNAL {
            return Err(ChangeError::Unknown);
        }
        match self.run(change) {
            CallStep::Done(value) => Ok(value),
            CallStep::Refused { refusal, .. } => Err(ChangeError::Refused(refusal)),
            CallStep::Pending
            | CallStep::Write { .. }
            | CallStep::Conflict { .. }
            | CallStep::Moved { .. } => Err(ChangeError::NoAnswer),
        }
    }

    /// The value of a done write (§J3.4, "act on a write seen done"). The
    /// highest write is done when a majority of the matchmakers holds it.
    /// Else (a write reached only the members that answered first, and one
    /// of them is down now) a `Confirm` of its newest segment reads it at a
    /// majority and writes it again, so it is done; with no majority up,
    /// there is no value.
    fn read(&mut self, journal: JournalId) -> Option<RegisterValue> {
        if journal != JOURNAL {
            return None;
        }
        let confirm = {
            let members = self.members.borrow();
            let held: Vec<_> = members
                .values()
                .filter(|m| m.up)
                .map(|m| &m.member.state().written)
                .collect();
            let top = held.iter().max_by_key(|w| w.op)?;
            let holders = held.iter().filter(|w| w.op == top.op).count();
            if holders > members.len() / 2 {
                return Some(top.value.clone());
            }
            let newest = top.value.newest()?;
            Change::Confirm {
                segment: newest.id(),
                state: newest.state(),
            }
        };
        match self.run(&confirm) {
            CallStep::Done(value) => Some(value),
            _ => None,
        }
    }
}

// ---- the hosts --------------------------------------------------------------

/// One host: its disk and its roles, and its driver while it runs.
struct HostSlot {
    disk: Option<MemStorage>,
    roles: Vec<Assignment>,
    driver: Option<Process>,
}

/// A request of the client that waits for an answer from a host.
#[derive(Clone, Copy)]
enum Waiting {
    Call(RequestId),
    Read(RequestId),
}

/// The reader's moves.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Move {
    /// Write one record.
    Append,
    /// Read the whole journal from position 0.
    Read,
    /// Host `host` crashes: its driver goes, its disk stays.
    Crash {
        /// The host.
        host: u64,
    },
    /// Host `host` starts again on its disk.
    Restart {
        /// The host.
        host: u64,
    },
    /// The controller opens the next segment, and its leader seals the
    /// segment before.
    Roll,
}

/// The live journal.
pub struct Live {
    clock: ManualClock,
    network: MemNetwork,
    matchmakers: Matchmakers,
    hosts: BTreeMap<u64, HostSlot>,
    /// The newest segment that a leader was placed for.
    segment: u64,
    client: Client,
    /// The client's requests that wait for a host: the host, the driver's
    /// id, and the request.
    waiting: Vec<(u64, CallId, Waiting)>,
    /// What each call of the client is, for the event it ends with.
    calls: BTreeMap<ClientCall, String>,
    /// The whole read, while it runs: the client's call and the records
    /// read so far.
    reading: Option<(ClientCall, Vec<String>)>,
    /// The records the last whole read gave.
    read: Option<Vec<String>>,
    appended: u64,
    written: u64,
    ticks: u64,
    events: VecDeque<String>,
    last_value: Option<RegisterValue>,
    open: BTreeMap<u64, bool>,
}

impl Default for Live {
    fn default() -> Self {
        Self::new()
    }
}

impl Live {
    /// The controller wins term 1 and opens segment 1; six hosts start, and
    /// leader L1 campaigns.
    ///
    /// # Panics
    ///
    /// When the matchmakers refuse the first election or the first reshape:
    /// they hold nothing yet, so this is a bug of the page.
    #[must_use]
    pub fn new() -> Self {
        let mut live = Self {
            clock: ManualClock::new(),
            network: MemNetwork::new(),
            matchmakers: Matchmakers::new(),
            hosts: HOSTS
                .iter()
                .map(|h| {
                    let slot = HostSlot {
                        disk: Some(MemStorage::new()),
                        roles: Vec::new(),
                        driver: None,
                    };
                    (*h, slot)
                })
                .collect(),
            segment: 0,
            client: Client::new(),
            waiting: Vec::new(),
            calls: BTreeMap::new(),
            reading: None,
            read: None,
            appended: 0,
            written: 0,
            ticks: 0,
            events: VecDeque::new(),
            last_value: None,
            open: BTreeMap::new(),
        };
        let mut register = live.matchmakers.client(0);
        let elected = register
            .change(JOURNAL, &Change::Elect { holder: NodeId(0) })
            .expect("the first election of the controller");
        register
            .change(JOURNAL, &reshape(&elected, 1))
            .expect("the first reshape");
        live.event("The controller wins term 1 and opens segment 1.");
        for column in COLUMNS {
            for node in column {
                live.slot(node).roles.push(Assignment::Acceptor {
                    address: at(node),
                    page: 16,
                });
            }
        }
        for node in replicas(1) {
            live.slot(replica_host(node)).roles.push(replica(node, 1));
        }
        for host in HOSTS {
            live.start(host);
        }
        live.place_leader(1, Slot(0));
        let call = live.client.set_leader(Leader::uuid(A), Leader::Unset);
        live.calls.insert(call, "SetLeader(A)".into());
        live
    }

    fn slot(&mut self, host: u64) -> &mut HostSlot {
        self.hosts.get_mut(&host).expect("a host of the page")
    }

    fn event(&mut self, text: &str) {
        self.events.push_front(format!("t{} · {text}", self.ticks));
        self.events.truncate(EVENTS);
    }

    /// Host `host` starts on its disk and serves its roles.
    fn start(&mut self, host: u64) {
        let endpoint = self.network.endpoint();
        let register = self.matchmakers.client(host);
        let clock = self.clock.clone();
        self.matchmakers.set_up(host, true);
        let slot = self.hosts.get_mut(&host).expect("a host of the page");
        let disk = slot.disk.take().expect("a host that is down has its disk");
        let mut driver = Driver::new(CONFIG, disk, endpoint, clock, register, ());
        for role in &slot.roles {
            driver.transport_mut().route(role.address());
            driver.assign(role.clone());
        }
        slot.driver = Some(driver);
    }

    /// Host `host` also serves `role`.
    fn assign(&mut self, host: u64, role: Assignment) {
        let slot = self.slot(host);
        slot.roles.push(role.clone());
        if let Some(driver) = slot.driver.as_mut() {
            driver.transport_mut().route(role.address());
            driver.assign(role);
        }
    }

    /// The leader of `segment` goes to the first host of its replicas that
    /// is up, and reads from `from`.
    fn place_leader(&mut self, segment: u64, from: Slot) {
        let host = replicas(segment)
            .map(replica_host)
            .into_iter()
            .find(|h| self.hosts[h].driver.is_some())
            .unwrap_or(replica_host(replicas(segment)[0]));
        let node = leader_node(segment);
        self.assign(
            host,
            Assignment::Leader {
                address: at(node),
                proxies: Vec::new(),
                campaign: Campaign {
                    ballot: Ballot {
                        round: segment,
                        node: NodeId(node),
                    },
                    from,
                },
            },
        );
        self.segment = segment;
        self.event(&format!("Leader L{segment} starts on host {host}."));
    }

    /// The host that serves the role at `address`.
    fn host_of(&self, address: Address) -> Option<u64> {
        self.hosts
            .iter()
            .find(|(_, slot)| slot.roles.iter().any(|r| r.address() == address))
            .map(|(h, _)| *h)
    }

    /// Play one move of the reader.
    pub fn act(&mut self, chosen: &Move) {
        match chosen {
            Move::Append => {
                let text = format!("record {}", self.appended);
                self.appended += 1;
                let call = self
                    .client
                    .write(vec![Record::new(text.clone().into_bytes())]);
                self.calls.insert(call, format!("Write(\"{text}\")"));
                self.event(&format!("The writer asks Write(\"{text}\")."));
            }
            Move::Read => {
                if self.reading.is_none() {
                    let call = self.client.read(Seq(0), 64);
                    self.calls.insert(call, "Read(0)".into());
                    self.reading = Some((call, Vec::new()));
                    self.event("The writer reads the journal from position 0.");
                }
            }
            Move::Crash { host } => self.crash(*host),
            Move::Restart { host } => {
                if self.hosts.get(host).is_some_and(|s| s.driver.is_none()) {
                    self.start(*host);
                    self.event(&format!("Host {host} starts again on its disk."));
                }
            }
            Move::Roll => self.roll(),
        }
    }

    fn crash(&mut self, host: u64) {
        let Some(slot) = self.hosts.get_mut(&host) else {
            return;
        };
        let Some(driver) = slot.driver.take() else {
            return;
        };
        slot.disk = Some(driver.into_parts().0);
        self.matchmakers.set_up(host, false);
        // The requests that wait on the host get no answer.
        let lost: Vec<Waiting> = self
            .waiting
            .iter()
            .filter(|(h, _, _)| *h == host)
            .map(|(_, _, w)| *w)
            .collect();
        self.waiting.retain(|(h, _, _)| *h != host);
        for waiting in lost {
            let (Waiting::Call(id) | Waiting::Read(id)) = waiting;
            self.client.answer(id, Response::Lost);
        }
        self.event(&format!(
            "Host {host} crashes: its roles stop; its disk stays."
        ));
    }

    /// The controller opens the next segment; the old leader goes, and the
    /// leader of the new segment seals the segment before.
    fn roll(&mut self) {
        let mut register = self.matchmakers.client(0);
        let Some(value) = register.read(JOURNAL) else {
            self.event("Roll: no majority of the matchmakers is up.");
            return;
        };
        let next = self.segment + 1;
        match register.change(JOURNAL, &reshape(&value, next)) {
            Ok(_) => self.event(&format!("The controller opens segment {next}.")),
            Err(ChangeError::Refused(refusal)) => {
                self.event(&format!("Roll refused by the register: {refusal:?}."));
                return;
            }
            Err(_) => {
                self.event("Roll: no majority of the matchmakers answered.");
                return;
            }
        }
        for node in replicas(next) {
            self.assign(replica_host(node), replica(node, next));
        }
        let old = at(leader_node(self.segment));
        if let Some(host) = self.host_of(old) {
            let slot = self.slot(host);
            slot.roles.retain(|r| r.address() != old);
            if let Some(driver) = slot.driver.as_mut() {
                driver.unassign(old);
            }
        }
        // The new leader reads from the applied prefix of the segment
        // before.
        let from = replicas(self.segment)
            .into_iter()
            .filter_map(|node| {
                self.hosts[&replica_host(node)]
                    .driver
                    .as_ref()?
                    .host()
                    .replica(replica_at(node))?
                    .front()
            })
            .max()
            .unwrap_or(Slot(0));
        self.place_leader(next, from);
    }

    /// Run `ticks` ticks: each one sends the client's requests, polls every
    /// host until none has work, gives the answers back, and moves the
    /// clock.
    pub fn step(&mut self, ticks: u32) {
        for _ in 0..ticks {
            self.send();
            for _ in 0..64 {
                let mut busy = false;
                for slot in self.hosts.values_mut() {
                    if let Some(driver) = slot.driver.as_mut() {
                        busy |= driver.poll();
                    }
                }
                if !busy {
                    break;
                }
            }
            self.receive();
            self.watch();
            self.clock.advance(TICK);
            self.ticks += 1;
        }
    }

    /// Send what the client asks.
    fn send(&mut self) {
        for request in self.client.take_requests() {
            let id = request.id();
            match request {
                Request::Command { command, .. } => {
                    let leader = at(leader_node(self.segment));
                    match self.driver_of(leader) {
                        Some((host, driver)) => {
                            let call = driver.call(leader, command);
                            self.waiting.push((host, call, Waiting::Call(id)));
                        }
                        None => self.client.answer(id, Response::Lost),
                    }
                }
                Request::Floor { .. } => {
                    let address = self.newest_replica();
                    self.ask_read(id, address, Request::FLOOR_FROM, 1);
                }
                Request::Read { from, limit, .. } => {
                    let address = self.replica_for(from);
                    let limit = usize::try_from(limit).unwrap_or(usize::MAX);
                    self.ask_read(id, address, from, limit);
                }
                Request::Resolve {
                    uuid,
                    expected_seq,
                    records,
                    ..
                } => {
                    // The segment that holds the write's position, as a
                    // read of that position is routed; each replica of it
                    // on a host that is up, until one covers the write.
                    let set = self.segment_of(expected_seq).map(|r| r.replicas().to_vec());
                    let answer = set
                        .unwrap_or_default()
                        .iter()
                        .filter_map(|node| {
                            let address = replica_at(node.0);
                            let driver = self.hosts[&replica_host(node.0)].driver.as_ref()?;
                            let replica = driver.host().replica(address)?;
                            Some(replica.resolve(uuid, expected_seq, &records).0)
                        })
                        .find(|a| *a != ResolveAnswer::NotCovered)
                        .unwrap_or(ResolveAnswer::NotCovered);
                    self.client.answer(id, Response::Resolved(answer));
                }
            }
        }
        self.take_outcomes();
    }

    fn driver_of(&mut self, address: Address) -> Option<(u64, &mut Process)> {
        let host = self.host_of(address)?;
        let driver = self.hosts.get_mut(&host)?.driver.as_mut()?;
        Some((host, driver))
    }

    fn ask_read(&mut self, id: RequestId, address: Option<Address>, from: Seq, limit: usize) {
        let Some(address) = address else {
            self.client.answer(id, Response::Lost);
            return;
        };
        match self.driver_of(address) {
            Some((host, driver)) => {
                let read = driver.read(address, from, limit);
                self.waiting.push((host, read, Waiting::Read(id)));
            }
            None => self.client.answer(id, Response::Lost),
        }
    }

    /// A replica of the newest segment, on a host that is up, picked by
    /// the tick so that a replica that does not serve is not asked forever.
    fn newest_replica(&self) -> Option<Address> {
        let value = self.matchmakers.clone().read(JOURNAL)?;
        let record = value.in_force()?;
        self.up_replica(record.replicas())
    }

    /// A replica of the segment that holds position `from`: the oldest
    /// segment whose boundary state is above it, else the segment in force.
    fn replica_for(&self, from: Seq) -> Option<Address> {
        let record = self.segment_of(from)?;
        self.up_replica(record.replicas())
    }

    /// The record of the segment that holds position `at`: the oldest
    /// segment whose boundary state is above it, else the segment in force.
    fn segment_of(&self, at: Seq) -> Option<SegmentRecord> {
        let value = self.matchmakers.clone().read(JOURNAL)?;
        value
            .segments()
            .values()
            .find(|r| {
                r.state() != SegmentState::Trimmed && r.boundary().is_none_or(|b| b.next_seq > at)
            })
            .or_else(|| value.in_force())
            .cloned()
    }

    fn up_replica(&self, set: &[NodeId]) -> Option<Address> {
        let up: Vec<u64> = set
            .iter()
            .map(|n| n.0)
            .filter(|n| self.hosts[&replica_host(*n)].driver.is_some())
            .collect();
        let pick = usize::try_from(self.ticks).unwrap_or(0) % up.len().max(1);
        up.get(pick).map(|n| replica_at(*n))
    }

    /// Give the client the answers of the hosts.
    fn receive(&mut self) {
        let mut answers = Vec::new();
        for (host, slot) in &mut self.hosts {
            let Some(driver) = slot.driver.as_mut() else {
                continue;
            };
            for reply in driver.take_replies() {
                answers.push((*host, reply.call, Answer::Call(reply.outcome)));
            }
            for reply in driver.take_reads() {
                answers.push((*host, reply.read, Answer::Read(reply.answer)));
            }
            driver.take_notices();
            driver.take_stopped();
        }
        for (host, id, answer) in answers {
            let Some(i) = self
                .waiting
                .iter()
                .position(|(h, c, _)| *h == host && *c == id)
            else {
                continue;
            };
            let (_, _, waiting) = self.waiting.swap_remove(i);
            let response = match (waiting, answer) {
                (Waiting::Call(_), Answer::Call(CallOutcome::Reply(reply))) => {
                    Response::Reply(reply)
                }
                (Waiting::Call(_), Answer::Call(_)) => Response::NotLeader,
                (Waiting::Read(_), Answer::Read(answer)) => Response::Read(answer),
                (Waiting::Call(_) | Waiting::Read(_), _) => Response::Lost,
            };
            let (Waiting::Call(request) | Waiting::Read(request)) = waiting;
            self.client.answer(request, response);
        }
        self.take_outcomes();
    }

    fn take_outcomes(&mut self) {
        for (call, outcome) in self.client.take_outcomes() {
            let what = self.calls.remove(&call).unwrap_or_default();
            if let Some((reading, mut records)) = self.reading.take() {
                if reading == call {
                    if let Outcome::Page {
                        from,
                        records: page,
                        sealed,
                        ..
                    } = &outcome
                    {
                        records.extend(
                            page.iter()
                                .map(|r| String::from_utf8_lossy(&r.payload).into_owned()),
                        );
                        if *sealed {
                            // The next records are in a later segment.
                            let next = Seq(from.0 + page.len() as u64);
                            let call = self.client.read(next, 64);
                            self.calls.insert(call, format!("Read({})", next.0));
                            self.event(&format!(
                                "{what}: {} records, the end of a sealed segment; read on from {}.",
                                page.len(),
                                next.0
                            ));
                            self.reading = Some((call, records));
                            continue;
                        }
                    }
                    self.event(&format!("{what}: {} records in all.", records.len()));
                    self.read = Some(records);
                    continue;
                }
                self.reading = Some((reading, records));
            }
            if matches!(outcome, Outcome::Written { .. }) {
                self.written += 1;
            }
            self.event(&format!("{what} -> {}", outcome_text(&outcome)));
        }
    }

    /// Events of the register and of the leaders.
    fn watch(&mut self) {
        let value = self.matchmakers.clone().read(JOURNAL);
        if let Some(value) = value
            && self.last_value.as_ref() != Some(&value)
        {
            for record in value.segments().values() {
                let before = self
                    .last_value
                    .as_ref()
                    .and_then(|v| v.segment(record.id()))
                    .map(SegmentRecord::state);
                if before != Some(record.state()) && before.is_some() {
                    self.event(&format!(
                        "Register: segment {} is {}.",
                        record.id().0,
                        state_text(record.state())
                    ));
                }
            }
            self.last_value = Some(value);
        }
        for segment in 1..=self.segment {
            let node = leader_node(segment);
            let open = self
                .hosts
                .values()
                .filter_map(|s| s.driver.as_ref())
                .filter_map(|d| d.host().leader(at(node)))
                .any(|l| matches!(l.phase(), LeaderPhase::Open { .. }));
            let was = self.open.insert(node, open).unwrap_or(false);
            if open && !was {
                self.event(&format!(
                    "Leader L{segment} won Phase 1 on F + 1 of each column: it writes."
                ));
            }
        }
    }

    /// What the page draws now.
    #[must_use]
    pub fn view(&self) -> View {
        let hosts = self
            .hosts
            .iter()
            .map(|(id, slot)| HostView {
                id: *id,
                up: slot.driver.is_some(),
                roles: slot
                    .roles
                    .iter()
                    .map(|role| role_view(role, slot.driver.as_ref()))
                    .collect(),
            })
            .collect();
        let matchmakers = self
            .matchmakers
            .holdings()
            .into_iter()
            .map(|(id, up, op, value)| MatchmakerView {
                id,
                up,
                op,
                term: value.term().number,
                segments: value
                    .segments()
                    .values()
                    .map(|r| format!("{} {}", r.id().0, state_text(r.state())))
                    .collect(),
            })
            .collect();
        let value = self.matchmakers.clone().read(JOURNAL);
        let segments = value.as_ref().map_or_else(Vec::new, |v| {
            v.segments()
                .values()
                .map(|r| SegmentView {
                    id: r.id().0,
                    state: state_text(r.state()).into(),
                    start: r.start().map(|s| s.0),
                    end: r.end().map(|s| s.0),
                    replicas: r
                        .replicas()
                        .iter()
                        .map(|n| format!("{} on host {}", n.0, replica_host(n.0)))
                        .collect(),
                })
                .collect()
        });
        let writer = match self.client.mode() {
            Mode::Open => "no SetLeader won yet".into(),
            Mode::Fenced { uuid, next } => {
                format!("fenced by uuid {:#x}, next write at {}", uuid.0, next.0)
            }
        };
        View {
            ticks: self.ticks,
            hosts,
            matchmakers,
            segments,
            writer,
            busy: !self.client.idle(),
            written: self.written,
            reading: self.reading.is_some(),
            read: self.read.clone(),
            register: value.is_some(),
            events: self.events.iter().cloned().collect(),
        }
    }
}

/// An answer of a host to a request of the client.
enum Answer {
    Call(CallOutcome),
    Read(ReadAnswer),
}

/// The controller's reshape that opens segment `segment` on the grid.
fn reshape(value: &RegisterValue, segment: u64) -> Change {
    let columns = COLUMNS.map(|c| c.map(NodeId).to_vec()).to_vec();
    Change::Reshape {
        opening: OpeningCall {
            term: value.term().number,
            expected: (segment > 1).then(|| SegmentId(segment - 1)),
            grid: Grid::new(1, columns).expect("a v1 grid"),
            replicas: replicas(segment).map(NodeId).to_vec(),
        },
        release: RELEASE,
    }
}

fn replica(node: u64, segment: u64) -> Assignment {
    Assignment::Replica {
        address: replica_at(node),
        segment: SegmentId(segment),
        set: replicas(segment).map(NodeId).to_vec(),
        initial: Leader::Unset,
    }
}

fn outcome_text(outcome: &Outcome) -> String {
    match outcome {
        Outcome::Written { seq, .. } => format!("written at position {}", seq.0),
        Outcome::LeaderSet(view) => format!("the writer leads; next position {}", view.next_seq.0),
        Outcome::Refused(answer) => format!("refused: {answer:?}"),
        other => format!("{other:?}"),
    }
}

/// The votes of an acceptor: every slot up to a run of `GAP` slots with no
/// vote. The slots of a column alternate with the other column's, and a
/// seal fills each slot below its end, so a run of `GAP` empty slots ends
/// the votes.
fn votes(acceptor: &JournalAcceptor) -> usize {
    const GAP: u64 = 64;
    let mut count = 0;
    let mut empty = 0;
    let mut slot = 0;
    while empty < GAP {
        if acceptor.vote(Slot(slot)).is_some() {
            count += 1;
            empty = 0;
        } else {
            empty += 1;
        }
        slot += 1;
    }
    count
}

fn role_view(role: &Assignment, driver: Option<&Process>) -> RoleView {
    let host = driver.map(Driver::host);
    match role {
        Assignment::Acceptor { address, .. } => {
            let detail = host.and_then(|h| h.acceptor(*address)).map(|a| {
                let votes = votes(a);
                format!("promised round {} · {votes} votes", a.promised().round)
            });
            RoleView {
                kind: "acceptor".into(),
                name: format!("acceptor {}", address.node.0),
                detail,
            }
        }
        Assignment::Replica {
            address, segment, ..
        } => {
            let detail = host.and_then(|h| h.replica(*address)).map(|r| {
                r.state().map_or_else(
                    || "waits for its base".to_string(),
                    |s| format!("next_seq {}", s.next_seq().0),
                )
            });
            RoleView {
                kind: "replica".into(),
                name: format!("replica {} of segment {}", address.node.0, segment.0),
                detail,
            }
        }
        Assignment::Leader { address, .. } => {
            let detail = host.and_then(|h| h.leader(*address)).map(|l| {
                let phase = match l.phase() {
                    LeaderPhase::Open { .. } => "writes".to_string(),
                    LeaderPhase::Stopped => "stopped".to_string(),
                    _ => "Phase 1".to_string(),
                };
                match l.segment() {
                    Some(s) => format!("{phase} · bound to segment {}", s.0),
                    None => phase,
                }
            });
            RoleView {
                kind: "leader".into(),
                name: format!("leader L{}", address.node.0 - LEADERS),
                detail,
            }
        }
        other @ Assignment::Proxy { .. } => RoleView {
            kind: "other".into(),
            name: format!("{:?}", other.address()),
            detail: None,
        },
    }
}

/// One role on a host.
#[derive(Clone, Debug, Serialize)]
pub struct RoleView {
    /// `acceptor`, `replica` or `leader`.
    pub kind: String,
    /// Its name.
    pub name: String,
    /// What it holds now; none while its host is down.
    pub detail: Option<String>,
}

/// One host.
#[derive(Clone, Debug, Serialize)]
pub struct HostView {
    /// The host, 1 to 6.
    pub id: u64,
    /// Whether it runs.
    pub up: bool,
    /// Its roles.
    pub roles: Vec<RoleView>,
}

/// One matchmaker.
#[derive(Clone, Debug, Serialize)]
pub struct MatchmakerView {
    /// The matchmaker, on the host of the same number.
    pub id: u64,
    /// Whether its host runs.
    pub up: bool,
    /// The generation of the write it holds.
    pub op: u64,
    /// The controller term of the value it holds.
    pub term: u64,
    /// Each segment of that value, with its state.
    pub segments: Vec<String>,
}

/// One segment of the register's done value.
#[derive(Clone, Debug, Serialize)]
pub struct SegmentView {
    /// The segment.
    pub id: u64,
    /// Its state.
    pub state: String,
    /// The first slot, once in force.
    pub start: Option<u64>,
    /// The seal end, once sealed.
    pub end: Option<u64>,
    /// Its replicas, with their hosts.
    pub replicas: Vec<String>,
}

/// What the page draws.
#[derive(Clone, Debug, Serialize)]
pub struct View {
    /// The ticks since the start.
    pub ticks: u64,
    /// The hosts.
    pub hosts: Vec<HostView>,
    /// The matchmakers.
    pub matchmakers: Vec<MatchmakerView>,
    /// The segments of the register's done value.
    pub segments: Vec<SegmentView>,
    /// The writer's mode.
    pub writer: String,
    /// Whether the writer has a call in flight or waiting.
    pub busy: bool,
    /// The writes done.
    pub written: u64,
    /// Whether a whole read runs.
    pub reading: bool,
    /// The records the last whole read gave.
    pub read: Option<Vec<String>>,
    /// Whether a majority of the matchmakers holds a done value.
    pub register: bool,
    /// The newest events first.
    pub events: Vec<String>,
}

#[cfg(test)]
mod tests;
