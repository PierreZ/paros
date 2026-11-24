//! **The whole journal on six hosts, each a tokio task, through `paros` alone.**
//!
//! Run it: `cargo run -p paros --example full_journal`
//!
//! Every role of the journal v1 runs here, each on the real library code:
//!
//! - three **matchmakers**, which hold the journal's compare-and-set
//!   register (`paros::journal::register`: [`RegisterMember`] on each
//!   matchmaker, one [`RegisterCall`] for each change: a read at a majority,
//!   the guard, a write at a majority);
//! - six **acceptors** on a v1 grid of two columns of three (`F = 1`);
//! - the **replicas** of each segment, three for each;
//! - one **leader** at a time;
//! - one writer, the pure **client** of `paros::journal::client`.
//!
//! Each host is one [`Driver`] on its own tokio task, on a multi-thread
//! runtime. The hosts share no state but the parts below, each one an
//! `Arc<Mutex<..>>` that the tasks pass around:
//!
//! - the **network** (`Net`): the inbox of each role. A send puts the
//!   message on the channel of the host that serves the receiver and wakes
//!   that host's task. The route of a host that is down is gone, so a
//!   message to it is lost.
//! - the **disks** (`Disk`): one [`MemStorage`] for each host. A crash ends
//!   the task and drops its driver; the disk stays, and a restart is a new
//!   driver on the same disk.
//! - the **matchmakers** (`Matchmakers`): the register state of each
//!   matchmaker. A host reaches a matchmaker through its lock, and a
//!   matchmaker on a host that is down does not answer.
//!
//! The controller's two calls (it wins the term, it opens a segment) are
//! made by hand here, through the matchmakers' register: its pure decision
//! is the crate `paros-journal-control`, outside the facade.
//!
//! # What the run shows
//!
//! 1. The controller wins term 1 and opens segment 1 on the matchmakers.
//! 2. Six hosts start. Leader L1, on host 1, runs Phase 1 on the grid.
//! 3. Writer A takes the journal with `SetLeader` and writes three records.
//! 4. The writer reads them back from a replica, with the floor of the
//!    segment in force.
//! 5. Host 3 crashes: acceptor 3, replica 12 and matchmaker 3 are gone. Two
//!    more writes are chosen, and the register still takes a change, at
//!    matchmakers 1 and 2.
//! 6. Host 3 starts again on its disk: acceptor 3 reads back the promise and
//!    the votes it held, and replica 12 catches up.
//! 7. The controller opens segment 2 (a roll). Leader L2, on host 2, seals
//!    segment 1 and writes in segment 2.
//! 8. The writer reads the whole journal: a sealed page of segment 1, then a
//!    page of segment 2.
//!
//! Each step asserts its result, so CI runs this example as a test.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::sync::{Notify, oneshot};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use paros::journal::client::{
    CallId as ClientCall, Client, Outcome, ReadAnswer, Request, Response,
};
use paros::journal::register::{
    Ballot, CallStep, Change, MatchmakerGeneration, MatchmakerId, MatchmakerSet, MemberState,
    RegisterCall, RegisterMember, RegisterValue,
};
use paros::journal::resolve::ResolveAnswer;
use paros::journal::roles::{Campaign, LeaderPhase};
use paros::journal::{
    Command, Grid, Leader, LeaderUuid, NodeId, OpeningCall, RELEASE, Record, SegmentId, Seq, Slot,
};
use paros::journal_io::mem::MemStorage;
use paros::journal_io::{
    Address, Assignment, CallId, CallOutcome, ChangeError, Clock, Driver, Entry, Envelope,
    HostConfig, Journal, JournalId, Key, Notice, Op, Register, Storage, StorageError, Time,
    Transport,
};

const JOURNAL: JournalId = JournalId(1);
const TICK: u64 = 1_000_000;
const CONFIG: HostConfig = HostConfig {
    tick: TICK,
    resend_page: 16,
    window: 64,
};
/// The hosts, 1 to 6.
const HOSTS: [u64; 6] = [1, 2, 3, 4, 5, 6];
/// Acceptor `n` runs on host `n`: column one on hosts 1 to 3, column two on
/// hosts 4 to 6.
const COLUMNS: [[u64; 3]; 2] = [[1, 2, 3], [4, 5, 6]];
/// The matchmakers run on hosts 1 to 3.
const MATCHMAKERS: [u64; 3] = [1, 2, 3];
/// Leader L1 at round 1 on host 1, leader L2 at round 2 on host 2.
const L1: u64 = 101;
const L2: u64 = 102;
/// The writer's uuid.
const A: LeaderUuid = LeaderUuid(0xA);
/// How long one step may take before the example fails. The whole run
/// takes well under a second, so a CI runner that is many times slower
/// still passes; a step that never ends fails here, with the state of each
/// host.
const DEADLINE: Duration = Duration::from_secs(20);
/// How long the program waits for one answer before it takes it as lost.
/// No result depends on it: the client sends a lost request again, and the
/// journal settles a copy that lands twice.
const ANSWER_WAIT: Duration = Duration::from_millis(500);

type Process = Driver<Disk, Wire, WallClock, Matchmakers, ()>;

fn at(node: u64) -> Address {
    Address::new(JOURNAL, NodeId(node))
}

/// The address of replica `node`: its segment is `node / 10`.
fn replica_at(node: u64) -> Address {
    Address::replica(JOURNAL, NodeId(node), SegmentId(node / 10))
}

/// The replicas of segment `s`: nodes `10 s + 1 ..= 10 s + 3`. Segment 1 on
/// hosts 1, 3 and 5; segment 2 on hosts 2, 4 and 6.
fn replicas(segment: u64) -> [u64; 3] {
    [1, 2, 3].map(|i| 10 * segment + i)
}

/// The host that serves node `node`.
fn host_of(node: u64) -> u64 {
    match node {
        1..=6 => node,
        11 | L1 => 1,
        21 | L2 => 2,
        12 => 3,
        22 => 4,
        13 => 5,
        23 => 6,
        _ => unreachable!("no node {node} in this example"),
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().expect("no task panics while it holds a lock")
}

// ---------------------------------------------------------------------------
// The four backends: clock, disk, network, matchmakers.
// ---------------------------------------------------------------------------

/// The time since the start of the program, shared by every host.
#[derive(Clone, Copy)]
struct WallClock {
    start: Instant,
}

impl Clock for WallClock {
    fn now(&self) -> Time {
        Time(u64::try_from(self.start.elapsed().as_nanos()).unwrap_or(u64::MAX))
    }
}

/// The disk of one host. It outlives the host's driver: a crash keeps it.
#[derive(Clone, Default)]
struct Disk(Arc<Mutex<MemStorage>>);

impl Storage<Journal> for Disk {
    fn commit(&mut self, ops: &[Op]) -> Result<(), StorageError> {
        lock(&self.0).commit(ops)
    }

    fn scan(&mut self, address: Address) -> Result<Vec<(Key, Entry)>, StorageError> {
        lock(&self.0).scan(address)
    }
    fn list(&mut self) -> Result<Vec<Address>, StorageError> {
        lock(&self.0).list()
    }
}

/// The inbox of one host, and the signal that wakes its task.
#[derive(Clone)]
struct Inbox {
    sender: UnboundedSender<Envelope>,
    wake: Arc<Notify>,
}

/// The network: the inbox of the host that serves each role.
#[derive(Default)]
struct Net {
    routes: BTreeMap<Address, Inbox>,
}

/// The transport of one host: the shared routes, and its own channel.
struct Wire {
    net: Arc<Mutex<Net>>,
    receiver: UnboundedReceiver<Envelope>,
}

impl Transport<Journal> for Wire {
    fn send(&mut self, envelope: Envelope) {
        if let Some(inbox) = lock(&self.net).routes.get(&envelope.receiver()) {
            // A host that stopped has no route: the message is lost, and
            // the roles send again on their timers.
            let _ = inbox.sender.send(envelope);
            inbox.wake.notify_one();
        }
    }

    fn receive(&mut self) -> Option<Envelope> {
        self.receiver.try_recv().ok()
    }
}

/// One matchmaker: its register state, and whether its host is up. The
/// state is its disk: it survives a crash of the host.
struct Matchmaker {
    member: RegisterMember,
    up: bool,
}

/// The journal's register, on its three matchmakers.
///
/// A [`RegisterCall`] reads at a majority, checks the guard of its change,
/// and writes at a majority. Each copy of this handle is the register
/// client of one host; they share the matchmakers and the counter of
/// operation generations.
///
/// **One operation at a time.** An operation holds the matchmakers' lock
/// from its first read to its last write, so two operations never
/// interleave and no write stays at a minority. An operation in conflict
/// can leave its write at a minority; the same change asked again then
/// finds that write and is refused, and the leader takes the refusal of
/// its own seal as a loss and stops.
#[derive(Clone)]
struct Matchmakers {
    set: MatchmakerSet,
    members: Arc<Mutex<BTreeMap<MatchmakerId, Matchmaker>>>,
    ops: Arc<AtomicU64>,
    /// The node that this copy names in its operation generations.
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
            members: Arc::new(Mutex::new(members)),
            ops: Arc::new(AtomicU64::new(0)),
            caller: NodeId(0),
        }
    }

    /// The register client of `caller`, on the same matchmakers.
    fn client(&self, caller: u64) -> Self {
        Self {
            caller: NodeId(caller),
            ..self.clone()
        }
    }

    /// The matchmaker on host `host` is up or down.
    fn set_up(&self, host: u64, up: bool) {
        if let Some(m) = lock(&self.members).get_mut(&MatchmakerId(host)) {
            m.up = up;
        }
    }

    /// Run one operation of `change`: the step that ends it. A request to
    /// a matchmaker that is down is lost; when no majority answers a phase,
    /// the operation ends with no answer ([`CallStep::Pending`]).
    fn run(&self, change: &Change) -> CallStep {
        let mut members = lock(&self.members);
        let op = Ballot {
            round: self.ops.fetch_add(1, Ordering::SeqCst) + 1,
            node: self.caller,
        };
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
            CallStep::Write { .. } => unreachable!("an operation runs on past its write step"),
            CallStep::Pending | CallStep::Conflict { .. } | CallStep::Moved { .. } => {
                Err(ChangeError::NoAnswer)
            }
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
            let members = lock(&self.members);
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

// ---------------------------------------------------------------------------
// A host: one driver on one tokio task.
// ---------------------------------------------------------------------------

/// What the outside asks of a host's task.
enum Order {
    Assign(Assignment),
    Unassign(Address),
    Call(Address, Command, oneshot::Sender<CallOutcome>),
    Read(Address, Seq, usize, oneshot::Sender<ReadAnswer>),
    /// A look at the driver's state, from the task that owns it.
    Look(Box<dyn FnOnce(&Process) + Send>),
}

/// Serve one host until its order channel closes (a crash): poll the
/// driver until it has no work, give each answer back, then sleep until an
/// order, a message or the driver's timer.
async fn serve(
    host: u64,
    mut process: Process,
    mut orders: UnboundedReceiver<Order>,
    wake: Arc<Notify>,
    clock: WallClock,
) {
    let mut calls: Vec<(CallId, oneshot::Sender<CallOutcome>)> = Vec::new();
    let mut reads: Vec<(CallId, oneshot::Sender<ReadAnswer>)> = Vec::new();
    loop {
        while process.poll() {}
        for reply in process.take_replies() {
            if let Some(i) = calls.iter().position(|(c, _)| *c == reply.call) {
                let _ = calls.swap_remove(i).1.send(reply.outcome);
            }
        }
        for reply in process.take_reads() {
            if let Some(i) = reads.iter().position(|(c, _)| *c == reply.read) {
                let _ = reads.swap_remove(i).1.send(reply.answer);
            }
        }
        for (address, stop) in process.take_stopped() {
            println!("  host {host}: the role at {address:?} stopped: {stop:?}");
        }
        for notice in process.take_notices() {
            if let Notice::Retired { segment, .. } = notice {
                println!("  host {host}: the leader saw {segment:?} retired");
            }
        }
        let due = process
            .timer()
            .map_or(TICK, |t| t.0.saturating_sub(clock.now().0).max(1));
        tokio::select! {
            order = orders.recv() => match order {
                // The host crashed: the driver goes, the disk stays.
                None => return,
                Some(Order::Assign(role)) => process.assign(role),
                Some(Order::Unassign(address)) => process.unassign(address),
                Some(Order::Call(address, command, answer)) => {
                    calls.push((process.call(address, command), answer));
                }
                Some(Order::Read(address, from, limit, answer)) => {
                    reads.push((process.read(address, from, limit), answer));
                }
                Some(Order::Look(look)) => look(&process),
            },
            () = wake.notified() => {}
            () = tokio::time::sleep(Duration::from_nanos(due)) => {}
        }
    }
}

/// One host as the data center sees it: its disk, its roles, and its task
/// while it runs.
struct HostSlot {
    disk: Disk,
    roles: Vec<Assignment>,
    running: Option<(UnboundedSender<Order>, JoinHandle<()>)>,
}

/// The data center: the shared network, the matchmakers, the clock and
/// each host.
struct Cluster {
    net: Arc<Mutex<Net>>,
    matchmakers: Matchmakers,
    clock: WallClock,
    hosts: BTreeMap<u64, HostSlot>,
}

impl Cluster {
    fn new() -> Self {
        Self {
            net: Arc::default(),
            matchmakers: Matchmakers::new(),
            clock: WallClock {
                start: Instant::now(),
            },
            hosts: HOSTS
                .iter()
                .map(|h| {
                    let slot = HostSlot {
                        disk: Disk::default(),
                        roles: Vec::new(),
                        running: None,
                    };
                    (*h, slot)
                })
                .collect(),
        }
    }

    fn slot(&mut self, host: u64) -> &mut HostSlot {
        self.hosts.get_mut(&host).expect("a host of the example")
    }

    /// Host `host` starts on its disk and serves its roles: each one reads
    /// its records back from the disk.
    fn start(&mut self, host: u64) {
        let (sender, receiver) = unbounded_channel();
        let wake = Arc::new(Notify::new());
        let inbox = Inbox {
            sender,
            wake: wake.clone(),
        };
        let wire = Wire {
            net: self.net.clone(),
            receiver,
        };
        let register = self.matchmakers.client(host);
        let clock = self.clock;
        self.matchmakers.set_up(host, true);
        let slot = self.hosts.get_mut(&host).expect("a host of the example");
        assert!(slot.running.is_none(), "host {host} already runs");
        let mut process = Driver::new(CONFIG, slot.disk.clone(), wire, clock, register, ());
        for role in &slot.roles {
            lock(&self.net).routes.insert(role.address(), inbox.clone());
            process.assign(role.clone());
        }
        let (orders, inbox_of_orders) = unbounded_channel();
        let task = tokio::spawn(serve(host, process, inbox_of_orders, wake, clock));
        slot.running = Some((orders, task));
    }

    /// Host `host` crashes: its routes and its matchmaker are gone, its task
    /// ends, its disk stays.
    async fn crash(&mut self, host: u64) {
        let slot = self.slot(host);
        let (orders, task) = slot.running.take().expect("a running host");
        let addresses: Vec<Address> = slot.roles.iter().map(Assignment::address).collect();
        {
            let mut net = lock(&self.net);
            for address in addresses {
                net.routes.remove(&address);
            }
        }
        self.matchmakers.set_up(host, false);
        drop(orders);
        task.await.expect("the host's task ends");
    }

    /// Host `host` also serves `role`.
    fn assign(&mut self, host: u64, role: Assignment) {
        // The route goes to the same inbox as the host's other roles: copy
        // it from one of them, before the role starts and sends.
        let inbox = {
            let net = lock(&self.net);
            self.hosts[&host]
                .roles
                .iter()
                .find_map(|r| net.routes.get(&r.address()).cloned())
        };
        if let Some(inbox) = inbox {
            lock(&self.net).routes.insert(role.address(), inbox);
        }
        let slot = self.slot(host);
        slot.roles.push(role.clone());
        if let Some((orders, _)) = &slot.running {
            let _ = orders.send(Order::Assign(role));
        }
    }

    /// Host `host` no longer serves the role at `address`.
    fn unassign(&mut self, host: u64, address: Address) {
        let slot = self.slot(host);
        slot.roles.retain(|r| r.address() != address);
        if let Some((orders, _)) = &slot.running {
            let _ = orders.send(Order::Unassign(address));
        }
        lock(&self.net).routes.remove(&address);
    }

    fn orders(&self, host: u64) -> Option<&UnboundedSender<Order>> {
        self.hosts[&host].running.as_ref().map(|(orders, _)| orders)
    }

    /// A look at host `host`'s driver: `None` when the host is down.
    async fn look<T: Send + 'static>(
        &self,
        host: u64,
        look: impl FnOnce(&Process) -> T + Send + 'static,
    ) -> Option<T> {
        let (sender, receiver) = oneshot::channel();
        let order = Order::Look(Box::new(move |p| {
            let _ = sender.send(look(p));
        }));
        self.orders(host)?.send(order).ok()?;
        receiver.await.ok()
    }

    /// Wait until `done` holds of the cluster, checked once a tick.
    async fn wait_until(&self, what: &str, mut done: impl AsyncFnMut(&Self) -> bool) {
        let end = Instant::now() + DEADLINE;
        while !done(self).await {
            if Instant::now() >= end {
                println!("State when the wait ended:");
                self.report().await;
                println!("  register: {:?}", self.matchmakers.clone().read(JOURNAL));
                panic!("{what}: not reached in {DEADLINE:?}");
            }
            tokio::time::sleep(Duration::from_nanos(TICK)).await;
        }
    }

    /// Whether leader `node` writes.
    async fn leader_open(&self, node: u64) -> bool {
        self.look(host_of(node), move |p| {
            p.host()
                .leader(at(node))
                .is_some_and(|l| matches!(l.phase(), LeaderPhase::Open { .. }))
        })
        .await
        .unwrap_or(false)
    }

    /// The fold of replica `node`: its next position, and its front.
    async fn replica(&self, node: u64) -> Option<(Seq, Option<Slot>)> {
        self.look(host_of(node), move |p| {
            let replica = p.host().replica(replica_at(node))?;
            Some((replica.state()?.next_seq(), replica.front()))
        })
        .await
        .flatten()
    }

    /// The controller's call `change` on the register. A majority of the
    /// matchmakers is up at each call of this run, and one operation runs at
    /// a time, so the call is done or refused at once.
    fn controller(&self, change: &Change) -> RegisterValue {
        self.matchmakers
            .client(0)
            .change(JOURNAL, change)
            .unwrap_or_else(|e| panic!("the controller's {change:?}: {e:?}"))
    }

    /// The controller's register read: the done value.
    fn register(&self) -> RegisterValue {
        self.matchmakers
            .client(0)
            .read(JOURNAL)
            .expect("a majority of the matchmakers holds a done value")
    }

    /// The generation of the write that each matchmaker holds.
    fn held(&self) -> Vec<(u64, u64)> {
        lock(&self.matchmakers.members)
            .iter()
            .map(|(id, m)| (id.0, m.member.state().written.op.round))
            .collect()
    }

    /// One line for each role of each host.
    async fn report(&self) {
        for host in HOSTS {
            let lines = self
                .look(host, |p| {
                    let host = p.host();
                    let mut lines = Vec::new();
                    for node in 1..=6 {
                        if let Some(a) = host.acceptor(at(node)) {
                            let votes = (0..64).filter(|s| a.vote(Slot(*s)).is_some()).count();
                            lines.push(format!(
                                "acceptor {node}: promised round {}, {votes} votes",
                                a.promised().round
                            ));
                        }
                    }
                    for node in replicas(1).into_iter().chain(replicas(2)) {
                        if let Some(r) = host.replica(replica_at(node)) {
                            let next = r.state().map(|s| s.next_seq().0);
                            lines.push(format!("replica {node}: next_seq {next:?}"));
                        }
                    }
                    for node in [L1, L2] {
                        if let Some(l) = host.leader(at(node)) {
                            lines.push(format!(
                                "leader {node}: bound to {:?}, {:?}",
                                l.segment(),
                                l.phase()
                            ));
                        }
                    }
                    lines
                })
                .await;
            match lines {
                Some(lines) => println!("  host {host}: {}", lines.join("; ")),
                None => println!("  host {host}: down"),
            }
        }
    }
}

// ---------------------------------------------------------------------------
// The writer's program: the pure client, and the glue that routes its
// requests.
// ---------------------------------------------------------------------------

/// A program of one writer: a [`Client`], and the glue that sends each of
/// its requests to the host that serves the role, and gives the answer
/// back. The client settles every ambiguous answer itself.
struct Program {
    client: Client,
    /// The leader the program sends commands to.
    leader: u64,
    outcomes: Vec<(ClientCall, Outcome)>,
}

impl Program {
    fn new(leader: u64) -> Self {
        Self {
            client: Client::new(),
            leader,
            outcomes: Vec::new(),
        }
    }

    /// Send the client's requests until `call` ends: its outcome.
    async fn run(&mut self, cluster: &Cluster, call: ClientCall) -> Outcome {
        let end = Instant::now() + DEADLINE;
        loop {
            for request in self.client.take_requests() {
                let id = request.id();
                let response = self.send(cluster, request).await;
                self.client.answer(id, response);
            }
            self.outcomes.extend(self.client.take_outcomes());
            if let Some(i) = self.outcomes.iter().position(|(c, _)| *c == call) {
                return self.outcomes.remove(i).1;
            }
            assert!(Instant::now() < end, "{call:?}: no outcome in {DEADLINE:?}");
            tokio::time::sleep(Duration::from_nanos(TICK)).await;
        }
    }

    /// Send one request: the answer, or `Lost` when none came in time.
    async fn send(&self, cluster: &Cluster, request: Request) -> Response {
        match request {
            Request::Command { command, .. } => {
                match ask_call(cluster, self.leader, command).await {
                    Some(CallOutcome::Reply(reply)) => Response::Reply(reply),
                    // Both say that the command holds no slot: the client
                    // sends it again.
                    Some(CallOutcome::NotLeader | CallOutcome::Busy) => Response::NotLeader,
                    None => Response::Lost,
                }
            }
            Request::Resolve {
                uuid,
                expected_seq,
                records,
                ..
            } => {
                // Ask the replicas of every segment in turn, until one covers
                // the position.
                let value = cluster.register();
                for record in value.segments().values() {
                    for node in record.replicas() {
                        let node = node.0;
                        let records = records.clone();
                        let found = cluster
                            .look(host_of(node), move |p| {
                                p.host()
                                    .replica(replica_at(node))
                                    .map(|r| r.resolve(uuid, expected_seq, &records).0)
                            })
                            .await
                            .flatten();
                        if let Some(found) = found.filter(|a| *a != ResolveAnswer::NotCovered) {
                            return Response::Resolved(found);
                        }
                    }
                }
                Response::Resolved(ResolveAnswer::NotCovered)
            }
            Request::Floor { .. } => {
                let value = cluster.register();
                let in_force = value.in_force().expect("a segment in force");
                read_at(cluster, in_force.replicas(), Request::FLOOR_FROM, 1).await
            }
            Request::Read { from, limit, .. } => {
                // Ask the newest segment first; a replica that answers
                // `Below` sends the read to the segment before.
                let value = cluster.register();
                let limit = usize::try_from(limit).unwrap_or(usize::MAX);
                let mut last = Response::Lost;
                for record in value.segments().values().rev() {
                    last = read_at(cluster, record.replicas(), from, limit).await;
                    if !matches!(last, Response::Read(ReadAnswer::Below(_))) {
                        return last;
                    }
                }
                last
            }
        }
    }

    /// Ask `call` of the client and run it to its outcome.
    async fn call(
        &mut self,
        cluster: &Cluster,
        call: impl FnOnce(&mut Client) -> ClientCall,
    ) -> Outcome {
        let id = call(&mut self.client);
        self.run(cluster, id).await
    }
}

/// Send `command` to leader `node` on its host: its answer, if one came.
async fn ask_call(cluster: &Cluster, node: u64, command: Command) -> Option<CallOutcome> {
    let (sender, receiver) = oneshot::channel();
    cluster
        .orders(host_of(node))?
        .send(Order::Call(at(node), command, sender))
        .ok()?;
    tokio::time::timeout(ANSWER_WAIT, receiver).await.ok()?.ok()
}

/// Read at the replicas of `set` in turn: the first answer that serves.
async fn read_at(cluster: &Cluster, set: &[NodeId], from: Seq, limit: usize) -> Response {
    for node in set {
        let node = node.0;
        let (sender, receiver) = oneshot::channel();
        let Some(orders) = cluster.orders(host_of(node)) else {
            continue;
        };
        if orders
            .send(Order::Read(replica_at(node), from, limit, sender))
            .is_err()
        {
            continue;
        }
        if let Ok(Ok(answer)) = tokio::time::timeout(ANSWER_WAIT, receiver).await
            && answer != ReadAnswer::Unserved
        {
            return Response::Read(answer);
        }
    }
    Response::Lost
}

// ---------------------------------------------------------------------------
// The run.
// ---------------------------------------------------------------------------

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    // A panic on a host's task (an assertion of a role) ends the program
    // with an error, as a panic of `main` does.
    let report = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        report(info);
        std::process::exit(101);
    }));
    let mut cluster = Cluster::new();
    let mut writer = Program::new(L1);
    let mut written: Vec<Record> = Vec::new();

    open_journal(&cluster);
    start_hosts(&mut cluster).await;
    write(&cluster, &mut writer, &mut written, 0..3, true).await;
    read_back(&cluster, &mut writer, &written).await;
    let held = crash(&mut cluster, &mut writer, &mut written).await;
    restart(&mut cluster, held).await;
    roll(&mut cluster, &mut writer, &mut written).await;
    read_all(&cluster, &mut writer, &written).await;

    println!("Final state:");
    cluster.report().await;
    for (host, slot) in &cluster.hosts {
        let (_, task) = slot.running.as_ref().expect("every host runs at the end");
        assert!(!task.is_finished(), "the task of host {host} ended");
    }
    println!(
        "ok: six hosts, three matchmakers, a grid of six acceptors, {} records in two segments, \
         one host restarted from its disk.",
        written.len()
    );
}

/// The controller's reshape that opens segment `segment` after the one
/// before, on the grid of two columns.
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

fn leader(node: u64, round: u64, from: Slot) -> Assignment {
    Assignment::Leader {
        address: at(node),
        proxies: Vec::new(),
        campaign: Campaign {
            ballot: Ballot {
                round,
                node: NodeId(node),
            },
            from,
        },
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

/// Step 1: the controller wins term 1 and opens segment 1, through the
/// matchmakers.
fn open_journal(cluster: &Cluster) {
    println!("1. The controller wins term 1 and opens segment 1 on the matchmakers.");
    // No host runs yet, so no other operation is in conflict with the
    // election, and it is asked one time only (`Elect` raises the term each
    // time it applies).
    let elected = cluster
        .matchmakers
        .clone()
        .change(JOURNAL, &Change::Elect { holder: NodeId(0) })
        .expect("the first election of the controller");
    show("Elect(controller)", &elected.term());
    let opened = cluster.controller(&reshape(&elected, 1));
    let record = opened.in_force().expect("segment 1 is in force");
    show(
        "Reshape(segment 1)",
        &(record.id(), record.grid().columns(), record.replicas()),
    );
    assert_eq!(record.id(), SegmentId(1));
}

/// Step 2: six hosts start, and leader L1 runs Phase 1 on the grid.
async fn start_hosts(cluster: &mut Cluster) {
    println!("2. Six hosts start; leader L1 on host 1 runs Phase 1 on the grid.");
    for column in COLUMNS {
        for node in column {
            cluster.slot(node).roles.push(Assignment::Acceptor {
                address: at(node),
                page: 16,
            });
        }
    }
    for node in replicas(1) {
        cluster.slot(host_of(node)).roles.push(replica(node, 1));
    }
    cluster.slot(host_of(L1)).roles.push(leader(L1, 1, Slot(0)));
    for host in HOSTS {
        cluster.start(host);
    }
    cluster
        .wait_until("L1 opens", async |c| c.leader_open(L1).await)
        .await;
    let phase = cluster
        .look(host_of(L1), |p| {
            p.host().leader(at(L1)).map(|l| l.phase().clone())
        })
        .await
        .flatten();
    show("L1 phase", &phase);
}

/// Writer A writes one record at each position of `seqs` (and first takes
/// the journal with `SetLeader` when `claim` holds).
async fn write(
    cluster: &Cluster,
    writer: &mut Program,
    written: &mut Vec<Record>,
    seqs: std::ops::Range<u64>,
    claim: bool,
) {
    if claim {
        println!("3. Writer A takes the journal and writes three records.");
        let set = writer
            .call(cluster, |c| c.set_leader(Leader::uuid(A), Leader::Unset))
            .await;
        show("SetLeader(A, Unset)", &set);
        assert!(matches!(set, Outcome::LeaderSet(_)));
    }
    for seq in seqs {
        let record = Record::new(format!("record {seq}").into_bytes());
        let outcome = writer
            .call(cluster, |c| c.write(vec![record.clone()]))
            .await;
        show(&format!("Write(\"record {seq}\")"), &outcome);
        assert_eq!(
            outcome,
            Outcome::Written {
                seq: Seq(seq),
                count: 1
            }
        );
        written.push(record);
    }
}

/// Step 4: the writer reads the three records back.
async fn read_back(cluster: &Cluster, writer: &mut Program, written: &[Record]) {
    println!("4. The writer reads them back from a replica of segment 1.");
    let page = writer.call(cluster, |c| c.read(Seq(0), 16)).await;
    let Outcome::Page {
        records, sealed, ..
    } = &page
    else {
        panic!("a page: {page:?}");
    };
    show("Read(0, 16)", &texts(records));
    assert_eq!(texts(records), texts(written));
    assert!(!sealed, "segment 1 is in force");
}

/// Step 5: host 3 crashes; the grid, the replicas and the matchmakers each
/// keep a majority.
async fn crash(
    cluster: &mut Cluster,
    writer: &mut Program,
    written: &mut Vec<Record>,
) -> (u64, Vec<u64>) {
    println!("5. Host 3 crashes: acceptor 3, replica 12 and matchmaker 3 are gone.");
    // L1 opened on F + 1 of each column; acceptor 3 may get its Prepare a
    // little later.
    cluster
        .wait_until("acceptor 3 promises L1", async |c| {
            c.look(3, |p| p.host().acceptor(at(3)).map(|a| a.promised().round))
                .await
                .flatten()
                == Some(1)
        })
        .await;
    let votes = cluster.look(3, votes_of_3).await.expect("host 3 runs");
    cluster.crash(3).await;
    write(cluster, writer, written, 3..5, false).await;
    // A register change at the two matchmakers that are up.
    let before = cluster.held();
    let value = cluster.register();
    let newest = value.in_force().expect("a segment in force");
    cluster.controller(&Change::Confirm {
        segment: newest.id(),
        state: newest.state(),
    });
    let after = cluster.held();
    show("matchmakers' write generations", &(&before, &after));
    for ((id, old), (_, new)) in before.iter().zip(&after) {
        if *id == 3 {
            assert_eq!(new, old, "matchmaker 3 is down: it takes no write");
        } else {
            assert!(new > old, "matchmaker {id} takes the write");
        }
    }
    votes
}

/// The promise and the slots that acceptor 3 holds a vote at.
fn votes_of_3(p: &Process) -> (u64, Vec<u64>) {
    let acceptor = p.host().acceptor(at(3)).expect("acceptor 3");
    let votes = (0..16)
        .filter(|s| acceptor.vote(Slot(*s)).is_some())
        .collect();
    (acceptor.promised().round, votes)
}

/// Step 6: host 3 starts again on its disk.
async fn restart(cluster: &mut Cluster, before: (u64, Vec<u64>)) {
    println!("6. Host 3 starts again on its disk.");
    let records = lock(&cluster.hosts[&3].disk.0).len();
    show("records on host 3's disk", &records);
    assert!(records > 0, "acceptor 3 and replica 12 wrote their disk");
    cluster.start(3);
    let read_back = cluster.look(3, votes_of_3).await.expect("host 3 runs");
    show("acceptor 3: (promise, votes) before the crash", &before);
    show("acceptor 3: (promise, votes) read back", &read_back);
    assert_eq!(read_back, before, "acceptor 3 reads back what it held");
    cluster
        .wait_until("replica 12 catches up", async |c| {
            c.replica(12).await.is_some_and(|(next, _)| next == Seq(5))
        })
        .await;
    show("replica 12 next_seq", &Seq(5));
}

/// Step 7: the controller opens segment 2; leader L2 seals segment 1.
async fn roll(cluster: &mut Cluster, writer: &mut Program, written: &mut Vec<Record>) {
    println!("7. The controller opens segment 2; leader L2 on host 2 seals segment 1.");
    let value = cluster.register();
    cluster.controller(&reshape(&value, 2));
    for node in replicas(2) {
        cluster.assign(host_of(node), replica(node, 2));
    }
    cluster.unassign(host_of(L1), at(L1));
    // L2 reads from the applied prefix of segment 1.
    let mut from = Slot(0);
    for node in replicas(1) {
        if let Some((_, Some(front))) = cluster.replica(node).await {
            from = from.max(front);
        }
    }
    cluster.assign(host_of(L2), leader(L2, 2, from));
    cluster
        .wait_until("L2 opens and segment 2 folds", async |c| {
            if !c.leader_open(L2).await {
                return false;
            }
            for node in replicas(2) {
                if c.replica(node).await.is_none() {
                    return false;
                }
            }
            true
        })
        .await;
    let value = cluster.register();
    let first = value.segment(SegmentId(1)).expect("segment 1");
    show("segment 1", &(first.state(), first.end()));
    assert!(first.end().is_some(), "segment 1 is sealed");
    assert_eq!(
        value.in_force().map(paros::journal::SegmentRecord::id),
        Some(SegmentId(2))
    );
    writer.leader = L2;
    write(cluster, writer, written, 5..7, false).await;
}

/// Step 8: the writer reads the whole journal, one segment at a time.
async fn read_all(cluster: &Cluster, writer: &mut Program, written: &[Record]) {
    println!("8. The writer reads the whole journal.");
    let mut pages = Vec::new();
    let mut from = Seq(0);
    // Segment 1 ends at its seal, so the read takes two pages: the sealed
    // page of segment 1, then the tail of segment 2.
    for _ in 0..2 {
        let page = writer.call(cluster, |c| c.read(from, 16)).await;
        let Outcome::Page {
            records, sealed, ..
        } = page
        else {
            panic!("a page: {page:?}");
        };
        show(
            &format!("Read({}, 16)", from.0),
            &(texts(&records), if sealed { "sealed" } else { "tail" }),
        );
        from = Seq(from.0 + records.len() as u64);
        pages.push((texts(&records), sealed));
    }
    let all = texts(written);
    assert_eq!(
        pages,
        [(all[..5].to_vec(), true), (all[5..].to_vec(), false)],
        "the leader time is a header the leader stamps: the payloads are compared"
    );
}

fn texts(records: &[Record]) -> Vec<String> {
    records
        .iter()
        .map(|r| String::from_utf8_lossy(&r.payload).into_owned())
        .collect()
}

/// Print one step: what was asked, and what came back.
fn show(what: &str, answer: &impl std::fmt::Debug) {
    println!("  {what:<46} -> {answer:?}");
}
