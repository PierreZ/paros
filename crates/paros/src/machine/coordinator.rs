//! **The cell coordinator** (#240, `docs/architecture/03-3-coordinators-and-placement.md` §3.3, §3.7): every
//! founding member campaigns for it in the cell's election journal through
//! [`crate::client::election`], and the winner leads the cell control journal
//! for its term.
//!
//! The candidate runs in a task of its own beside the node loop, through a
//! [`Client`] over the cell's members, exactly as any other client of the
//! cell would. A term, once won, is served in the actor's order (§3.3):
//!
//! 1. **Install** the term's uuid on the cell control journal with
//!    `SetLeader(uuid, current)` (`SetLeader(uuid, unset)` for the cell's
//!    first coordinator), then fold that journal to its tail
//!    ([`CellSession::open`]).
//! 2. **Finish in-flight work**: admit every machine the registry holds that
//!    is neither a founding member nor retired. `Admit` is idempotent, so a
//!    machine an earlier term admitted answers that it is in the cell. Then
//!    follow the universe directory ([`super::universe`]): join the cell to
//!    the universe when the directory holds the cell and the cell has not
//!    joined (`JoinFleet`, #513 (fleet Init duel)), and write the cell's step
//!    of each tenant operation in flight (`HostTenant` for a `REGISTERING`
//!    tenant, `DropTenant` for a `REMOVING` one, #225). `init` and a
//!    tenant's creation or removal wait for these writes and claim nothing
//!    on the cell control journal.
//! 3. **Publish** the interface: register the term's group, a moonpool
//!    dynamic endpoint serving `Register` and `JournalRequest` (#404 (static
//!    vs dynamic endpoints)), and let the next renewal name the member's
//!    address and the group's reference ([`Election::publish`]). The group
//!    is dropped when the term ends, so a request sent to an ended term
//!    fails at the transport (`EndpointNotFound`), never reaching a later
//!    term of the same member.
//! 4. **Stop at the first refused write**: a claim that loses ends the term,
//!    and the coordinator resigns. Its writer sends nothing once superseded.
//! 5. **Watch the cell's machines** (#211, D6, [`Watch`]): at every renewal
//!    period, `Identify` each founding member and each registered machine
//!    not retired. A machine that answers as a new incarnation of a
//!    registered one registers again (a reboot); one marked down, or seen
//!    as another incarnation, is written `MachineUp`; a registered machine
//!    that answers as a machine of this cell is written `MachineAdmitted`
//!    once, and only then may placement book it (#511 (placement books
//!    machines that never served)); one silent for
//!    `machine_down_after` is written `MachineDown`. A machine held down
//!    and silent for `machine_replace_after` is gone (#475 (re-place after
//!    the Down bound)): the term's moves re-place its roles as a draining
//!    machine's, unless an operator's maintenance window over it or its
//!    zone is open (#359). The silence is measured on the candidacy's
//!    clock, across its terms: the watch of a term that ended is resumed
//!    by this member's next term, less the time between the two that it
//!    watched nothing. Without that, every term restarted the clock, and a
//!    coordinator whose terms an operator's claims fence more often than
//!    `machine_replace_after` never named a wiped machine gone (witness
//!    recipe 12277531871500182271 91348:13584114706456816475 on 448be19:
//!    eighteen terms in 41 s, two wiped founders down and never gone,
//!    PR #684 (lost set excuse)). Only changes are
//!    written, never a heartbeat. Each round also times each `Identify`
//!    and asks every machine that answered for its `Load`, and judges from
//!    those facts which machines are alive but slow ([`super::slow`], #361
//!    (slow machines)): a change of judgement is written `MachineSlow`,
//!    and the moves take each journal off a slow machine like off a
//!    draining one. Each round also follows the universe
//!    directory, as in step 2. The watch ends with the term, or at the
//!    term's first write that does not land: a coordinator an admin session
//!    fenced does not fight back. It resigns the term instead, so that the
//!    next term installs itself under a fresh uuid and its duties go on
//!    (#349: a moved machine is otherwise refused for the rest of the term).
//! 6. **Register moved machines** (#349): a machine of the cell asks the
//!    coordinator with `Register` to record the address it advertises now
//!    (`super::register`). The coordinator checks that the machine answers
//!    `Identify` there as itself, then writes `RegisterNode` with that
//!    address and its machine interface (#404), unless the registry holds
//!    them already. Only the term's group serves `Register`; a request that
//!    reaches the group of a term that just ended is refused
//!    (`not_coordinator`).
//!
//! Each duty runs beside the term's renewals (`renewing`, #542 (coordinator
//! starves its lease while it installs)): one duty can outlast a lease, and
//! a term that renewed only between duties was deposed before it served.
//!
//! For a term it served, the coordinator also acts as the **tenant
//! coordinator** of every tenant its cell hosts (#210, until #212 and
//! #225): it answers the journal requests sent to the interface it
//! published, writing each tenant's control journal under its term uuid
//! ([`super::tenants`]). A group whose term ended answers `not_coordinator`
//! until it is dropped. A tenant control journal that refuses the term uuid
//! ends the term as a refused cell control journal does: the desk stops and
//! the coordinator resigns, so no write under that uuid follows.
//!
//! Until admin calls become requests to the coordinator (#212, #225), an
//! admin session (`cell add-machine`, `cell drain-machine`; never a fleet
//! operation, #513, #225) still claims the cell control
//! journal and fences the coordinator. The coordinator does not fight back within its term: it
//! resigns, and the next term claims the journal again.
//!
//! The candidate draws no randomness of its own: its seed and its BUGGIFY
//! decisions (a stalled leader, a hand-off, a reused journal id, an
//! impatient term, a lost resignation, a group that outlives its term by one
//! step, a slow install, a set's handover asked again) are drawn on the node loop before the task starts, and the backoff
//! jitter and the journal ids derive from the seed.

use std::collections::BTreeMap;
use std::sync::{Arc, PoisonError};
use std::time::Duration;

use moonpool_core::{Detach, Providers, RandomProvider, TaskProvider, TimeProvider};
use moonpool_rpc::{AccessClass, RpcHandle, ServiceGroup};
use paros_core::{JournalIdentifier, LeaderUuid, NodeId, TenantId};
use tokio_util::sync::CancellationToken;

use super::capacity;
use super::footprints::Footprints;
use super::modes::Modes;
use super::moves::Moves;
use super::scrub::{ScrubPace, Scrubs};
use super::sets::{SetStep, Sets};
use super::slow::{Sample, SlowBounds, SlowJudge};
use super::tenants::TenantDesk;
use super::{CellPlan, ControlJournals, FormedCell, incarnation_from_halves};
use crate::client::capacity::{CapacityAnswer, CapacityRequest};
use crate::client::cell::CellSession;
use crate::client::checkpoint::{CheckpointPolicy, Folder, LoadOutcome, load};
use crate::client::election::{Candidate, Election, ElectionTunables, Leader, Step, hand_off};
use crate::client::fleet::{Interrupted, Stage, Step as FleetStep};
use crate::client::journals::{JournalAnswer, JournalRequest};
use crate::client::universe::{JoinAnswer, JoinRequest};
use crate::client::{CallObserver, ClaimOutcome, Client, Server, bootstrap};
use crate::fleet::FleetDirectory;
use crate::rpc::machine as wire;
use crate::rpc::methods::{
    CapacityRequestRpc, CoordinatorInterface, JoinUniverseRpc, JournalRequestRpc, RegisterRpc,
};
use crate::rpc::{Inbound, NodeClient, ReplySender};
use crate::system::{MaintenanceScope, NodeStanding, Registry, SystemCommand};
use crate::{Address, DriverTunables, Names};

/// A served term's session and its watch over the cell's machines (#211).
type Watching = Option<(CellSession, Watch)>;

/// End a term's duties: its session is dropped, and its watch is parked for
/// this candidacy's next term to resume ([`Watch::resumed`]).
fn park(watching: &mut Watching, parked: &mut Option<Watch>) {
    if let Some((_, watch)) = watching.take() {
        *parked = Some(watch);
    }
}

/// The journal requests' endpoint (#210).
type Requests = Inbound<JournalRequestRpc, wire::JournalRequest, wire::JournalRequestAck>;

/// The machines' `Register` endpoint (#349).
type Registers = Inbound<RegisterRpc, wire::Register, wire::RegisterAck>;

/// The tenant coordinators' capacity requests' endpoint (#476).
type Capacities = Inbound<CapacityRequestRpc, wire::CapacityRequest, wire::CapacityRequestAck>;

/// `universe add-cell`'s join requests' endpoint (#232 (multiple cells)).
type Joins = Inbound<JoinUniverseRpc, wire::JoinUniverse, wire::JoinUniverseAck>;

/// A served term's dynamic endpoint (#404 (static vs dynamic endpoints)):
/// one moonpool group serving `Register` and `JournalRequest`, published in
/// the election journal. Dropping it destroys the endpoint: a request sent
/// to it afterwards fails at the transport, in this incarnation or any
/// other.
struct TermGroup {
    /// The term it serves.
    term: u64,
    group: ServiceGroup<CoordinatorInterface>,
    registers: Registers,
    requests: Requests,
    capacities: Capacities,
    joins: Joins,
}

impl TermGroup {
    /// Register term `term`'s group on `rpc`; `None` once the runtime is
    /// gone.
    fn open<P: Providers>(rpc: &RpcHandle<P>, term: u64) -> Option<Self> {
        assert!(term != 0, "a served term is set");
        let group = rpc
            .register_group::<CoordinatorInterface>(AccessClass::Public)
            .ok()?;
        let registers = Inbound::plain(group.serve::<RegisterRpc>().ok()?);
        let requests = Inbound::plain(group.serve::<JournalRequestRpc>().ok()?);
        let capacities = Inbound::plain(group.serve::<CapacityRequestRpc>().ok()?);
        let joins = Inbound::plain(group.serve::<JoinUniverseRpc>().ok()?);
        assert!(
            !group.endpoint().token().is_well_known(),
            "a term group is a dynamic endpoint"
        );
        Some(Self {
            term,
            group,
            registers,
            requests,
            capacities,
            joins,
        })
    }

    /// The group's reference, encoded, as the election publishes it.
    fn reference(&self) -> Vec<u8> {
        crate::rpc::published(&self.group.interface_ref())
    }
}

/// What serving one term's duties came to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TermDuty {
    /// The term is installed, the cell control journal folded to its tail
    /// and every in-flight admission asked again: `admitted` of them
    /// finished in this term.
    Served {
        /// The admissions this term finished.
        admitted: usize,
    },
    /// The install lost: another uuid leads the cell control journal now.
    /// The term is over for this actor.
    Refused,
    /// The install or the fold did not end: try again at the next step.
    Unavailable,
}

/// Serve the duties of the term `leader` (this actor's) on the cell's
/// control journals: install the term's uuid, fold the cell control journal
/// to its tail, then finish every admission in flight, through `client`.
/// `cell` is the control journals, every founding member, and the founders
/// the caller can dial.
/// The cell coordinator runs it for every term it wins; a harness candidate
/// runs the very same code.
///
/// # Panics
///
/// If `leader` carries the unset uuid, or a postcondition fails (a
/// programmer error).
#[tracing::instrument(level = "debug", skip_all, fields(term = leader.term, candidate = leader.candidate.id))]
pub async fn serve_term<P: Providers>(
    providers: &P,
    rpc: &RpcHandle<P>,
    client: &Client<P>,
    names: &Names,
    cell: (ControlJournals, Vec<NodeId>, Vec<(NodeId, Address)>),
    leader: &Leader,
    policy: CheckpointPolicy,
) -> TermDuty {
    open_term(providers, rpc, client, names, cell, leader, policy)
        .await
        .0
}

/// [`serve_term`], handing back the open session of a served term, which
/// the coordinator's [`Watch`] writes through for the rest of the term.
async fn open_term<P: Providers>(
    providers: &P,
    rpc: &RpcHandle<P>,
    client: &Client<P>,
    names: &Names,
    (journals, members, founders): (ControlJournals, Vec<NodeId>, Vec<(NodeId, Address)>),
    leader: &Leader,
    policy: CheckpointPolicy,
) -> (TermDuty, Option<CellSession>) {
    assert!(leader.uuid.is_set(), "a term's uuid is set");
    // `members` are every founding member, reached or not; `founders` the
    // ones the caller can dial. An admission names every founding member,
    // so every journal of the admitted machine is born with the same
    // matchmaker set as the founders' (#608 (an admission names only the
    // reached founders)). The term once read the founding members out of
    // the book: a harness candidate that reached two founders of three
    // admitted a machine with a two-member set (witness
    // 16600285087896190261 on a04c94d, red on "matchmaker: every node
    // derives the same matchmaker set").
    // `with_leader` keeps only the reached founders that are members.
    let mut session = CellSession::with_leader(journals, &members, founders, leader.uuid, policy);
    match session.open(client, 0).await {
        Ok(()) => {}
        Err(Interrupted::NotClaimed {
            outcome: ClaimOutcome::Lost { .. },
            ..
        }) => {
            moonpool_assertions::reachable!("coordinator: a lost install ended the term");
            return (TermDuty::Refused, None);
        }
        Err(_) => return (TermDuty::Unavailable, None),
    }
    let founder = |id: NodeId| session.founders().iter().any(|(f, _)| *f == id);
    let in_flight: Vec<Address> = session
        .registry()
        .nodes()
        .filter(|(id, n)| n.standing != NodeStanding::Retired && !founder(*id))
        .filter_map(|(_, n)| Address::parse(&n.addr).ok())
        .collect();
    let mut admitted = 0;
    for target in in_flight {
        if let FleetStep::Done {
            last: Some(Stage::Admit),
            ..
        } = session
            .admit_step(providers, rpc, client, names, 0, &target)
            .await
        {
            admitted += 1;
            moonpool_assertions::sometimes!(
                true,
                "coordinator: a successor finished an in-flight admission"
            );
        }
    }
    assert!(
        session.registry().nodes().count() >= admitted,
        "an admission finished is a registered machine"
    );
    // An `init` that added the cell to the universe directory waits for the
    // cell's side of the join (#513 (fleet Init duel)), and a tenant's
    // creation or removal waits for the cell's step (#225). A write that
    // does not land ends the term's session, as a refused admission does.
    if session.is_open() {
        let mut directory = Folder::new(FleetDirectory::default());
        let _ = super::universe::follow(client, &mut session, &mut directory).await;
    }
    let open = session.is_open().then_some(session);
    (TermDuty::Served { admitted }, open)
}

/// The cell coordinator's failure detector over the cell's machines (#211,
/// D6): when each machine last answered `Identify`, and when its next round
/// is due. It writes only changes into the cell control journal, through
/// the term's session.
#[derive(Debug)]
pub struct Watch {
    /// How long a machine is silent before it is marked down.
    down_after: Duration,
    /// How long a machine is silent before its roles are re-placed (#475).
    replace_after: Duration,
    /// When this watch first folded each open maintenance window, keyed by
    /// what it covers, with the position of the entry that opened it
    /// (#359): the window runs on this watch's clock from then.
    windows: BTreeMap<MaintenanceScope, (u64, Duration)>,
    /// The machines this watch named gone: they stay gone while a slot is
    /// booked on them and the registry still holds them down, so a watch
    /// that hears one again does not undo a move before the registry
    /// records it up. Once it is up, it is bookable again: a slot a move
    /// left outside the configuration in force is the mode change's
    /// (`super::modes`), as it is for a later term, and a journal a move
    /// shrank grows back onto it. A slot no move releases (a deleted
    /// journal's, a tenant's) must not keep it gone for the term.
    departed: std::collections::BTreeSet<NodeId>,
    /// How often a round runs.
    every: Duration,
    /// When the watch started: a machine never heard is silent since then.
    started: Duration,
    /// When the next round is due.
    next: Duration,
    /// When this watch last watched: the end of its last round, or its
    /// start.
    seen: Duration,
    /// When each machine last answered as itself.
    heard: BTreeMap<NodeId, Duration>,
    /// Whether the last call ran a round.
    ran: bool,
    /// The `Identify` checks a `Register` left running, by machine (#454
    /// (control plane meets chaos)): see [`register`].
    probes: BTreeMap<NodeId, Probe>,
    /// The registry fold position each admitted machine reported at its
    /// last answer (#505 (stranded admitted machine)).
    positions: BTreeMap<NodeId, u64>,
    /// The universe directory's fold, read at each round: the cell's join
    /// (#513 (fleet Init duel)) and the tenant operations in flight (#225).
    directory: Folder<FleetDirectory>,
    /// The slow-machine judgement (#361 (slow machines)).
    slow: SlowJudge,
    /// The free-bytes floor (#360 (disk full)): a machine that answers
    /// `Load` with less free space is marked low on space.
    space_floor: u64,
}

/// One `Identify` check of a machine's new address, run off the renewal
/// path with the client's request timeout. `answered` is `None` while it
/// runs, then whether the machine answered there as that incarnation.
#[derive(Debug)]
struct Probe {
    addr: Address,
    incarnation: u128,
    answered: Arc<std::sync::Mutex<Option<bool>>>,
}

impl Watch {
    /// A watch started `now`, running a round `every` period, marking a
    /// machine down after `down_after` of silence, naming it gone after
    /// `replace_after` (#475), and slow by `slow` (#361 (slow machines)).
    ///
    /// # Panics
    ///
    /// If `down_after` is not above `every`: one missed round would mark a
    /// machine down (the tunables' floor refuses it). If `replace_after` is
    /// not above `down_after`: a machine would be re-placed before it is
    /// down (the tunables' floor refuses it too).
    #[must_use]
    pub fn new(
        now: Duration,
        every: Duration,
        (down_after, replace_after): (Duration, Duration),
        slow: SlowBounds,
    ) -> Self {
        assert!(!every.is_zero(), "a watch round has a period");
        assert!(
            down_after > every,
            "one missed round never marks a machine down"
        );
        assert!(
            replace_after > down_after,
            "a machine is re-placed only once it is down"
        );
        Self {
            down_after,
            replace_after,
            windows: BTreeMap::new(),
            departed: std::collections::BTreeSet::new(),
            every,
            started: now,
            next: now,
            seen: now,
            heard: BTreeMap::new(),
            ran: false,
            probes: BTreeMap::new(),
            positions: BTreeMap::new(),
            directory: Folder::new(FleetDirectory::default()),
            slow: SlowJudge::new(slow),
            space_floor: 0,
        }
    }

    /// The same watch, marking a machine low on space below `floor` free
    /// bytes (#360 (disk full)); `0` marks none.
    #[must_use]
    pub fn with_space_floor(mut self, floor: u64) -> Self {
        self.space_floor = floor;
        self
    }

    /// Whether the last [`Watch::round`] call ran a round.
    #[must_use]
    pub fn ran(&self) -> bool {
        self.ran
    }

    /// This watch resumed at `now` by a later term of the same candidacy:
    /// it keeps what it observed, and every moment it holds moves forward
    /// by the time since it last watched, so a machine's silence counts
    /// only the time a round could have heard it. The next round is due at
    /// once.
    ///
    /// # Panics
    ///
    /// If `now` is before the watch last watched.
    #[must_use]
    pub fn resumed(mut self, now: Duration) -> Self {
        assert!(now >= self.seen, "a watch resumes after it last watched");
        let gap = now.saturating_sub(self.seen);
        if !self.heard.is_empty() {
            moonpool_assertions::reachable!("coordinator: a term resumed the last term's watch");
        }
        self.started += gap;
        for since in self.heard.values_mut() {
            *since += gap;
        }
        for (_, seen) in self.windows.values_mut() {
            *seen += gap;
        }
        self.seen = now;
        self.next = now;
        self.ran = false;
        assert!(self.started <= now, "a resumed watch started by now");
        assert!(
            self.heard.values().all(|since| *since <= now),
            "a resumed watch heard every machine by now"
        );
        self
    }

    /// The machines whose roles the term re-places at `now` (#475 (re-place
    /// after the Down bound)): registered machines `registry` holds down
    /// that this watch has not heard for `replace_after`, outside every open
    /// maintenance window over them or their zone (#359), and every machine
    /// named gone before that still holds a slot and is still down. A
    /// machine that answers again inside the bound keeps its roles: a
    /// reboot rejoins in place.
    ///
    /// # Panics
    ///
    /// A machine named gone is not in `registry`: a watch bug.
    pub fn gone(
        &mut self,
        registry: &Registry,
        now: Duration,
    ) -> std::collections::BTreeSet<NodeId> {
        // Each window runs from when this watch first folded the entry that
        // opened it: the registry holds no time.
        let open: BTreeMap<MaintenanceScope, u64> = registry
            .maintenance()
            .map(|(scope, window)| (scope.clone(), window.seq))
            .collect();
        self.windows
            .retain(|scope, (seq, _)| open.get(scope) == Some(seq));
        for (scope, seq) in open {
            self.windows.entry(scope).or_insert((seq, now));
        }
        self.seen = self.seen.max(now);
        self.departed
            .retain(|node| registry.booked(*node) > 0 && !registry.liveness(*node).up);
        let mut gone = self.departed.clone();
        for (id, node) in registry.nodes() {
            if node.standing != NodeStanding::Registered
                || registry.liveness(id).up
                || gone.contains(&id)
            {
                continue;
            }
            let since = self.heard.get(&id).copied().unwrap_or(self.started);
            if now.saturating_sub(since) < self.replace_after {
                continue;
            }
            let covered = registry.maintenance_of(id).next().is_some();
            let held = self.held_one(registry, id, now);
            if held {
                if registry.booked(id) > 0 {
                    moonpool_assertions::reachable!(
                        "coordinator: a maintenance window holds a down machine's roles"
                    );
                }
                continue;
            }
            if covered && registry.booked(id) > 0 {
                moonpool_assertions::reachable!(
                    "coordinator: a maintenance window ended while its machine was down"
                );
            }
            if registry.booked(id) > 0 {
                moonpool_assertions::reachable!(
                    "coordinator: a machine down past its bound is gone"
                );
                tracing::info!(machine = id.0, "machine_gone");
            }
            gone.insert(id);
        }
        self.departed
            .extend(gone.iter().copied().filter(|n| registry.booked(*n) > 0));
        for id in &gone {
            assert!(
                registry.get(*id).is_some(),
                "a machine named gone is registered"
            );
        }
        gone
    }

    /// Whether an open maintenance window over `id` or its zone holds its
    /// roles at `now` (#359), on this watch's clock.
    fn held_one(&self, registry: &Registry, id: NodeId, now: Duration) -> bool {
        registry.maintenance_of(id).any(|(scope, window)| {
            self.windows
                .get(scope)
                .is_some_and(|(_, seen)| now < *seen + Duration::from_millis(window.for_ms))
        })
    }

    /// The registered machines an open maintenance window holds at `now`
    /// (#359): a slow mark moves none of their roles (#361 (slow
    /// machines)). Read after [`Watch::gone`], which opens the windows.
    ///
    /// # Panics
    ///
    /// If it names a machine the registry does not hold.
    #[must_use]
    pub fn held(&self, registry: &Registry, now: Duration) -> std::collections::BTreeSet<NodeId> {
        let held: std::collections::BTreeSet<NodeId> = registry
            .nodes()
            .filter(|(id, node)| {
                node.standing == NodeStanding::Registered && self.held_one(registry, *id, now)
            })
            .map(|(id, _)| id)
            .collect();
        assert!(
            held.iter().all(|id| registry.get(*id).is_some()),
            "a held machine is registered"
        );
        held
    }

    /// Run one round if one is due at `now`, writing each change through
    /// `session`: how many entries it wrote.
    ///
    /// # Errors
    ///
    /// A write did not land: the term writes nothing more.
    #[tracing::instrument(level = "trace", skip_all, fields(cell = session.registry().fleet().map_or(0, |f| f.cell_id)))]
    pub async fn round<P: Providers>(
        &mut self,
        providers: &P,
        rpc: &RpcHandle<P>,
        client: &Client<P>,
        names: &Names,
        session: &mut CellSession,
    ) -> Result<usize, Interrupted> {
        self.ran = client.now() >= self.next;
        if !self.ran {
            return Ok(0);
        }
        self.next = client.now() + self.every;
        let timeout = client.tunables().request_timeout;
        // Each machine where the cell's address book says it is (#349).
        let machines: BTreeMap<NodeId, Address> =
            super::cell_book(session.founders(), session.registry())
                .into_iter()
                .collect();
        // The slots of the tenants the cell dropped go back to it (#212
        // (placement)): no request for such a tenant is answered again, so
        // nothing else would release them.
        let dropped: std::collections::BTreeSet<_> = session
            .registry()
            .bookings()
            .map(|(_, b)| b.target.tenant())
            .filter(|tenant| session.registry().dropped(*tenant))
            .collect();
        let mut wrote = 0;
        // The cell's side of an `init` (#513 (fleet Init duel)) and of each
        // tenant operation in flight (#225): the operator waits for it, and
        // claims nothing here.
        wrote += super::universe::follow(client, session, &mut self.directory).await?;
        if !dropped.is_empty() {
            wrote +=
                super::tenants::release(client, session, |b| dropped.contains(&b.target.tenant()))
                    .await?;
            moonpool_assertions::reachable!("coordinator: a dropped tenant's slots are released");
        }
        let watched: Vec<NodeId> = machines
            .keys()
            .copied()
            .filter(|id| session.registry().contains(*id))
            .collect();
        let mut answered: Vec<(NodeId, Address, Duration)> = Vec::new();
        for (id, addr) in machines {
            let asked = client.now();
            let answer = bootstrap::identify(providers, rpc, names, &addr, timeout)
                .await
                .filter(|ack| ack.node_id == id.0);
            if answer.is_some() {
                answered.push((id, addr.clone(), client.now().saturating_sub(asked)));
            }
            // Asked now, answered or not: a moment this watch watched.
            self.seen = client.now();
            let command = match &answer {
                Some(ack) => {
                    self.heard.insert(id, client.now());
                    Self::answered(session, id, &addr, ack)
                }
                None => self.silent(session, id, client.now()),
            };
            if let Some(command) = command {
                session.record(client, 0, &command).await?;
                wrote += 1;
            }
            // A machine whose registry fold is stuck gets the cell's book
            // again (#505 (stranded admitted machine)).
            if let Some(ack) = answer
                && self.stuck(session, id, &ack)
                && session
                    .readmit(providers, rpc, names, (id, &addr), timeout)
                    .await
            {
                moonpool_assertions::reachable!(
                    "coordinator: a stuck machine is handed the cell's book again"
                );
                tracing::info!(
                    machine = id.0,
                    position = session.position(),
                    "machine_readmitted"
                );
            }
        }
        wrote += self
            .judge_slow(
                providers,
                rpc,
                names,
                client,
                session,
                (&answered, &watched),
            )
            .await?;
        self.seen = client.now();
        assert!(
            self.heard.values().all(|since| *since <= self.seen),
            "a watch heard every machine by its last round"
        );
        Ok(wrote)
    }

    /// The slow-machine half of a round (#361 (slow machines)): ask every
    /// machine that answered `Identify` for its `Load`, judge the round's
    /// facts, and write each change of judgement `MachineSlow`, for the
    /// incarnation the registry holds. How many entries it wrote.
    async fn judge_slow<P: Providers>(
        &mut self,
        providers: &P,
        rpc: &RpcHandle<P>,
        names: &Names,
        client: &Client<P>,
        session: &mut CellSession,
        (answered, watched): (&[(NodeId, Address, Duration)], &[NodeId]),
    ) -> Result<usize, Interrupted> {
        // Only a machine of the pool is judged: the registry refuses a mark
        // of any other.
        let judged: Vec<&(NodeId, Address, Duration)> = answered
            .iter()
            .filter(|(id, _, _)| watched.contains(id))
            .collect();
        let asks: Vec<(u64, Address)> = judged
            .iter()
            .map(|(id, addr, _)| (id.0, addr.clone()))
            .collect();
        let loads = crate::client::load::ask_all(
            providers,
            rpc,
            names,
            &asks,
            client.tunables().request_timeout,
        )
        .await;
        let samples: BTreeMap<NodeId, Sample> = judged
            .iter()
            .map(|(id, _, rtt)| {
                let ack = match loads.get(&id.0) {
                    Some(crate::client::load::LoadOutcome::Answered(ack)) => Some(&**ack),
                    _ => None,
                };
                (*id, Sample::of(*rtt, ack))
            })
            .collect();
        let registry = session.registry();
        let changes = self
            .slow
            .round(&samples, |id| registry.is_slow(id), watched);
        let mut wrote = 0;
        for change in changes {
            let held = session.registry().liveness(change.id);
            moonpool_assertions::reachable!("coordinator: a machine's slow judgement changes");
            if change.slow {
                moonpool_assertions::reachable!("coordinator: a slow machine is marked slow");
            } else {
                moonpool_assertions::reachable!("coordinator: a slow machine is fast again");
            }
            tracing::info!(
                machine = change.id.0,
                slow = change.slow,
                rtt_us = change.facts.rtt_us,
                disk = change.facts.disk_busy_permille,
                cpu = change.facts.cpu_busy_permille,
                "machine_slow"
            );
            let command = SystemCommand::MachineSlow {
                id: change.id,
                incarnation: held.incarnation,
                slow: change.slow,
                facts: change.facts,
            };
            session.record(client, 0, &command).await?;
            wrote += 1;
        }
        wrote += self.judge_space(client, session, &judged, &loads).await?;
        Ok(wrote)
    }

    /// The space half of a round (#360 (disk full)): every machine whose
    /// `Load` answer reports its disk's space is low on space below the
    /// free-bytes floor, and each change of judgement is written
    /// `MachineSpace`, for the incarnation the registry holds. How many
    /// entries it wrote.
    async fn judge_space<P: Providers>(
        &self,
        client: &Client<P>,
        session: &mut CellSession,
        judged: &[&(NodeId, Address, Duration)],
        loads: &BTreeMap<u64, crate::client::load::LoadOutcome>,
    ) -> Result<usize, Interrupted> {
        let mut wrote = 0;
        for (id, _, _) in judged {
            let Some(crate::client::load::LoadOutcome::Answered(ack)) = loads.get(&id.0) else {
                continue;
            };
            if !ack.has_space {
                continue;
            }
            let low = ack.free_bytes < self.space_floor;
            let held = session.registry().liveness(*id);
            if held.low_space == low {
                continue;
            }
            if low {
                moonpool_assertions::reachable!("coordinator: a machine is marked low on space");
            } else {
                moonpool_assertions::reachable!(
                    "coordinator: a machine low on space has space again"
                );
            }
            tracing::info!(
                machine = id.0,
                low,
                free_bytes = ack.free_bytes,
                "machine_space"
            );
            let command = SystemCommand::MachineSpace {
                id: *id,
                incarnation: held.incarnation,
                low,
                free_bytes: ack.free_bytes,
            };
            session.record(client, 0, &command).await?;
            wrote += 1;
        }
        Ok(wrote)
    }

    /// Whether the admitted machine `id`, answering `ack`, folds the
    /// registry no more (#505 (stranded admitted machine)): it serves the
    /// cell, and it reports the same fold position below the registry's
    /// tail as at its last answer. Such a machine dials its cell through a
    /// book whose every founding member moved or was wiped, and nothing
    /// booked on it can start until it learns where they are.
    fn stuck(&mut self, session: &CellSession, id: NodeId, ack: &wire::IdentifyAck) -> bool {
        let position = ack.registry_position;
        let before = self.positions.insert(id, position);
        // A founding member folds its own copy of the registry: it reports
        // no position, and is never admitted.
        if session.members().contains(&id) {
            return false;
        }
        let serving = session.registry().serves(id) && ack.cell_id == session.journals().cell_id;
        serving && position < session.position() && before == Some(position)
    }

    /// The change machine `id` answering `ack` at `addr` makes, if any: a
    /// reboot registers again, a registered machine seen in the cell is
    /// admitted, then liveness.
    fn answered(
        session: &CellSession,
        id: NodeId,
        addr: &Address,
        ack: &crate::rpc::machine::IdentifyAck,
    ) -> Option<SystemCommand> {
        let incarnation = incarnation_from_halves(ack.incarnation_high, ack.incarnation_low);
        if incarnation == 0 {
            return None;
        }
        let registry = session.registry();
        let cell_id = session.journals().cell_id;
        let held = registry.liveness(id);
        if !held.up && registry.booked(id) > 0 {
            // A machine back before its roles moved keeps them: a reboot
            // rejoins in place (#475).
            moonpool_assertions::reachable!("coordinator: a machine marked down rejoins in place");
        }
        match registry.get(id) {
            Some(node) if node.incarnation != incarnation => {
                // A reboot of a registered machine: it registers again, as
                // the incarnation that answered (up, by its registration).
                let (class, locality) = super::identity_tags(ack)?;
                if class != node.class {
                    return None;
                }
                moonpool_assertions::reachable!("coordinator: a rebooted machine registers again");
                Some(SystemCommand::RegisterNode {
                    id,
                    addr: addr.to_string(),
                    class,
                    capacity: ack.capacity,
                    locality,
                    name: ack.name.clone(),
                    incarnation,
                    machine_interface: ack.machine_interface.clone(),
                })
            }
            // A registered machine that answers as a machine of this cell
            // won its `Admit`: placement may book it from now on (#511
            // (placement books machines that never served)). One idle, or
            // stuck in another cell decree, never serves.
            Some(_) if !registry.serves(id) && cell_id != 0 && ack.cell_id == cell_id => {
                moonpool_assertions::reachable!("coordinator: a machine's admission is recorded");
                Some(SystemCommand::MachineAdmitted { id })
            }
            _ if !held.up || held.incarnation != incarnation => {
                if !held.up {
                    moonpool_assertions::reachable!(
                        "coordinator: a machine marked down answers again"
                    );
                }
                Some(SystemCommand::MachineUp { id, incarnation })
            }
            _ => None,
        }
    }

    /// The change machine `id`'s silence at `now` makes, if any: `Down`
    /// once it was silent for `down_after`, unless it is held down.
    fn silent(&self, session: &CellSession, id: NodeId, now: Duration) -> Option<SystemCommand> {
        let held = session.registry().liveness(id);
        let since = self.heard.get(&id).copied().unwrap_or(self.started);
        if !held.up || now.saturating_sub(since) < self.down_after {
            return None;
        }
        assert!(
            session.registry().seen_alive(id),
            "a machine marked down was seen alive"
        );
        moonpool_assertions::reachable!("coordinator: a silent machine is marked down");
        Some(SystemCommand::MachineDown {
            id,
            incarnation: held.incarnation,
        })
    }
}

/// The election's timing, from the driver's tunables.
#[must_use]
pub fn election_tunables(tunables: &DriverTunables) -> ElectionTunables {
    ElectionTunables {
        lease: tunables.election_lease,
        renew_every: tunables.election_renew,
        compact_after: tunables.election_compact_after,
    }
}

/// The library client of the cell's members, each dialed by its advertised
/// address, resolved at each call (#257), with the machine's internal client
/// tunables (#457 (hard-coded tunables)).
pub(crate) fn cell_client<P: Providers>(
    providers: &P,
    rpc: &RpcHandle<P>,
    formed: &FormedCell,
    tunables: &DriverTunables,
) -> Client<P> {
    // This member at the address it advertises now (#349), the others where
    // its cached registry fold dials them, else at the plan's (#211).
    let book = super::with_own(&formed.founders(), formed.facts.node_id, &formed.facts.addr);
    let servers = book
        .iter()
        .map(|(id, addr)| Server {
            id: id.0,
            node: NodeClient::named(rpc, formed.facts.names.clone(), addr.clone()),
        })
        .collect();
    Client::new(providers, servers, tunables.internal_client())
}

/// `client` over the founding members where the cell control journal's
/// registry holds them now, this member at the address it advertises
/// (#349): a founder renamed while this machine ran is dialed at its new
/// name. `None` when the registry cannot be read to its tail. A term that
/// could not claim the cell's journal at the address this candidacy
/// started with dialed a renamed leader at a name nobody listens on, and
/// kept its election for the rest of the run (#529 (sets oracles after PR
/// #515): witness 2036487808999208197, a set never moved off a drained
/// machine).
async fn follow_book<P: Providers>(
    rpc: &RpcHandle<P>,
    client: &Client<P>,
    names: &Names,
    plan: &CellPlan,
    (me, own): (NodeId, &Address),
) -> Option<Client<P>> {
    let mut folder = Folder::new(Registry::default());
    let loaded = load(&mut folder, plan.control, client, 0, 0).await;
    if !matches!(loaded, LoadOutcome::Loaded { .. }) {
        return None;
    }
    let book = followed_book(plan, folder.state(), (me, own));
    Some(dial_book(rpc, client, names, &book))
}

/// The founding members where `registry` holds them, this member at the
/// address it advertises (#349).
fn followed_book(
    plan: &CellPlan,
    registry: &Registry,
    (me, own): (NodeId, &Address),
) -> Vec<(NodeId, Address)> {
    let book = super::with_own(&super::address_book(&plan.members, registry), me, own);
    assert_eq!(
        book.len(),
        plan.members.len(),
        "a followed book names every founding member"
    );
    assert!(
        book.iter().any(|(id, addr)| *id == me && addr == own),
        "a followed book dials this member where it advertises"
    );
    book
}

/// `client` over `book`, each member dialed by its address.
fn dial_book<P: Providers>(
    rpc: &RpcHandle<P>,
    client: &Client<P>,
    names: &Names,
    book: &[(NodeId, Address)],
) -> Client<P> {
    let servers: Vec<_> = book
        .iter()
        .map(|(id, addr)| Server {
            id: id.0,
            node: NodeClient::named(rpc, names.clone(), addr.clone()),
        })
        .collect();
    assert_eq!(
        servers.len(),
        book.len(),
        "every member of the book is dialed"
    );
    client.with_servers(servers)
}

/// A founding member's candidacy, as the node loop hands it to the task.
/// Its bools are independent BUGGIFY decisions.
#[allow(clippy::struct_excessive_bools)]
struct Candidacy {
    me: NodeId,
    addr: Address,
    names: Names,
    plan: CellPlan,
    seed: u128,
    tunables: ElectionTunables,
    /// The failure detector's timeout (#211), and the re-placement bound
    /// (#475).
    down_after: (Duration, Duration),
    /// The first term that finds a machine gone defers its re-placement by
    /// one round (a BUGGIFY decision, #475 (re-place after the Down
    /// bound)): the machine must still move at the next.
    defer: bool,
    /// The slow-machine judgement's bounds (#361 (slow machines)).
    slow: SlowBounds,
    /// The free-bytes floor (#360 (disk full)).
    space_floor: u64,
    /// The leader stops renewing for one lease and a half after its first
    /// term's duties (a BUGGIFY decision: the takeover's way in). A fault:
    /// never in the recovery tail ([`chaos`]), like `hand_off` and
    /// `quit_mid_move`.
    stall: bool,
    /// The leader hands its first term on to another founding member after
    /// its duties (a BUGGIFY decision).
    hand_off: bool,
    /// The tenant coordinator reuses a taken journal id at the first draw of
    /// every create (a BUGGIFY decision): the fold must refuse it.
    reuse: bool,
    /// Each term's tenant coordinator stops listening at once on its first
    /// create and its first catch-up (a BUGGIFY decision, #445 (ignored load
    /// result)): the desk must answer unavailable, never panic.
    impatient: bool,
    /// Each term's tenant coordinator abandons its first create right after
    /// booking its slots (a BUGGIFY decision, #212 (placement)): the next
    /// create in that tenant must release them.
    abandon: bool,
    /// The first fenced term steps down without a `Resign` record, as if
    /// the record were lost (a BUGGIFY decision): the fold still names the
    /// term's leader, and the term must not come back. A fault: never in
    /// the recovery tail ([`chaos`]).
    lose_resign: bool,
    /// The first term that advances a move resigns right after that step
    /// (a BUGGIFY decision, #474 (journals on admitted machines)), a
    /// journal's or a matchmaker set's (#501 (matchmaker sets off the
    /// founders)): a later term must finish the move.
    quit_mid_move: bool,
    /// The first ended term's group outlives the term by one step (a
    /// BUGGIFY decision, #404): a request that reaches it is refused by the
    /// application (`not_coordinator`), then the group drops and the next
    /// request fails at the transport.
    linger: bool,
    /// The first term this member serves takes a lease and a half longer
    /// to install (a BUGGIFY decision, #542 (coordinator starves its lease
    /// while it installs)): its renewals must keep the term through it. A
    /// fault: never in the recovery tail ([`chaos`]).
    slow_install: bool,
    /// Each term's first finished matchmaker-set move asks its handover
    /// again, as a retry that lands after the move (a BUGGIFY decision,
    /// #501 (matchmaker sets off the founders)): the set may then leave its
    /// booked machines, and the term must move it back.
    ask_again: bool,
    /// How fast the term's tenant scrubbers read (#362 (background
    /// scrubbing)).
    scrub: ScrubPace,
}

/// Start the candidacy of the founding member `formed` for the cell
/// coordinator, in a task that stops with `shutdown`, and serve the journal
/// requests (#210) and the machines' `Register` requests (#349) there, in
/// each served term's group (#404). Draws its seed and its BUGGIFY
/// decisions here, on the node loop. Nothing starts when the plan names no
/// election journal or the tunables are not a working election.
///
/// `stop` is the machine's clean stop (#560 (clean Ctrl-C shutdown)): the
/// candidacy ends, and a term it leads is handed on to the next founding
/// member, through the node loop that still serves. The receiver turns
/// `true` once the candidacy ended, or at once when none started.
pub(crate) fn spawn<P: Providers>(
    providers: &P,
    rpc: &RpcHandle<P>,
    formed: &FormedCell,
    tunables: &DriverTunables,
    observer: Option<Arc<dyn CallObserver>>,
    hold: Option<tokio::sync::watch::Receiver<bool>>,
    (shutdown, halt): (CancellationToken, CancellationToken),
) -> tokio::sync::watch::Receiver<bool> {
    let election = election_tunables(tunables);
    if !election.is_valid() || !formed.plan.election.is_set() {
        return tokio::sync::watch::channel(true).1;
    }
    let (ended, ended_rx) = tokio::sync::watch::channel(false);
    let candidacy = Candidacy {
        me: formed.facts.node_id,
        addr: formed.facts.addr.clone(),
        names: formed.facts.names.clone(),
        plan: formed.plan.clone(),
        seed: providers.random().random(),
        tunables: election,
        down_after: (tunables.machine_down_after, tunables.machine_replace_after),
        defer: moonpool_buggify::buggify_with_prob!(0.5),
        slow: SlowBounds {
            rtt: tunables.slow_rtt,
            busy_permille: tunables.slow_busy_permille,
            rounds: tunables.slow_rounds,
        },
        space_floor: tunables.free_bytes_floor,
        stall: moonpool_buggify::buggify_fault_with_prob!(0.3),
        hand_off: formed.plan.members.len() > 1 && moonpool_buggify::buggify_fault_with_prob!(0.25),
        reuse: moonpool_buggify::buggify_with_prob!(0.3),
        impatient: moonpool_buggify::buggify_with_prob!(0.3),
        abandon: moonpool_buggify::buggify_with_prob!(0.3),
        lose_resign: moonpool_buggify::buggify_with_prob!(0.5),
        linger: moonpool_buggify::buggify_with_prob!(0.5),
        quit_mid_move: moonpool_buggify::buggify_fault_with_prob!(0.5)
            || crate::scenario::quit_mid_move(),
        slow_install: moonpool_buggify::buggify_with_prob!(0.3),
        ask_again: moonpool_buggify::buggify_with_prob!(0.3),
        scrub: ScrubPace::of(tunables),
    };
    let mut client = cell_client(providers, rpc, formed, tunables).with_shutdown(shutdown.clone());
    if let Some(observer) = observer {
        client = client.with_observer(observer);
    }
    let providers = providers.clone();
    let rpc = rpc.clone();
    providers
        .task()
        .spawn_task("paros-cell-coordinator", {
            let providers = providers.clone();
            async move {
                campaign(providers, rpc, client, candidacy, hold, (shutdown, halt)).await;
                let _ = ended.send(true);
            }
        })
        .detach();
    ended_rx
}

/// A jitter in `[0, lease / 2]` derived from `seed` and the step count.
fn jitter(seed: u128, step: u64, lease: Duration) -> Duration {
    let low = u64::try_from(seed & u128::from(u64::MAX)).unwrap_or(0);
    let high = u64::try_from(seed >> 64).unwrap_or(0);
    let mut x = low ^ high ^ step.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    x ^= x >> 30;
    x = x.wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x ^= x >> 27;
    let half = u64::try_from(lease.as_millis() / 2).unwrap_or(u64::MAX);
    Duration::from_millis(x % half.saturating_add(1))
}

/// The founding member after `me` in id order: the successor a hand-off
/// names.
fn successor(plan: &CellPlan, me: NodeId) -> Option<Candidate> {
    let others: Vec<NodeId> = plan
        .members
        .iter()
        .map(|(id, _)| *id)
        .filter(|id| *id != me)
        .collect();
    others
        .iter()
        .find(|id| **id > me)
        .or_else(|| others.first())
        .map(|id| Candidate {
            id: id.0,
            ..Candidate::default()
        })
}

/// The candidate's loop: step the election, serve every term it wins,
/// answer the journal requests between steps, and stop with `shutdown`.
/// A clean `stop` (#560 (clean Ctrl-C shutdown)) ends it too, and hands a
/// led term on first ([`hand_on`]).
#[tracing::instrument(level = "debug", skip_all, fields(node = candidacy.me.0, cell = candidacy.plan.cell_id))]
// One loop over the election's steps: each arm is a thin call into the
// term's duties, and splitting the arms out would scatter its shared state.
#[allow(clippy::too_many_lines)]
async fn campaign<P: Providers>(
    providers: P,
    rpc: RpcHandle<P>,
    mut client: Client<P>,
    candidacy: Candidacy,
    mut hold: Option<tokio::sync::watch::Receiver<bool>>,
    (shutdown, halt): (CancellationToken, CancellationToken),
) {
    let Candidacy {
        me,
        addr,
        names,
        plan,
        seed,
        tunables,
        down_after,
        mut defer,
        slow,
        space_floor,
        mut stall,
        hand_off: mut handing,
        reuse,
        impatient,
        abandon,
        mut lose_resign,
        mut linger,
        mut quit_mid_move,
        mut slow_install,
        ask_again,
        scrub,
    } = candidacy;
    let founder_ids: Vec<NodeId> = plan.members.iter().map(|(id, _)| *id).collect();
    let journals = plan.control_journals();
    let founders = plan.members.clone();
    let policy = client.tunables().checkpoint_policy();
    let pace = tunables.renew_every / 4;
    let mut election = Election::new(
        client.clone(),
        plan.election,
        Candidate {
            id: me.0,
            ..Candidate::default()
        },
        seed,
        tunables,
    );
    // The term whose duties are done.
    let mut served: Option<u64> = None;
    // The founders' addresses a served term last dialed from its session's
    // registry; `None` until its first watch round.
    let mut dialed: Option<Vec<(NodeId, Address)>> = None;
    // The tenant coordinator of the term served, while it leads it.
    let mut desk: Option<(u64, TenantDesk)> = None;
    let open_desk = |term: u64, uuid: LeaderUuid, reuse: bool| {
        let desk = TenantDesk::new(
            uuid,
            journals.cell,
            &founder_ids,
            (seed ^ u128::from(term), reuse, impatient, abandon),
            policy,
        );
        (term, desk)
    };
    // The term a superseded desk resigned: resigned again while the
    // election still names it (a resignation can be lost).
    let mut resigned: Option<u64> = None;
    // The served term's session and its watch over the cell's machines.
    let mut watching: Option<(CellSession, Watch)> = None;
    // The last term's watch, resumed by the next (#475): a machine's silence
    // outlives the term that observed it.
    let mut parked: Option<Watch> = None;
    // The served term's moves off draining machines (#474), and its
    // matchmaker sets' placements and moves (#501 (matchmaker sets off the
    // founders)).
    let mut moves = Moves::default();
    // Its journals' changes to their desired modes (#214 (tenant modes)).
    let mut changes = Modes::default();
    let mut sets = Sets::new(ask_again);
    // The served term's footprint steps (#476).
    let internal: Vec<TenantId> = std::iter::once(journals.cell.tenant)
        .chain(journals.fleet.map(|fleet| fleet.tenant))
        .collect();
    let mut footprints = Footprints::new(internal.clone());
    // The served term's scrubbers (#362 (background scrubbing)).
    let mut scrubs = Scrubs::new(scrub);
    // The served term's dynamic endpoint (#404).
    let mut group: Option<TermGroup> = None;
    // The term whose install was slowed, until the next step judges it.
    let mut slowed: Option<u64> = None;
    let mut steps = 0_u64;
    let mut waited = false;
    while !shutdown.is_cancelled() && !halt.is_cancelled() {
        // A moved founding member campaigns only once its registration
        // ended (#506 (renamed founder not registered)): its peers cannot
        // reach it, and the reads it serves never confirm, so a term it won
        // would read a stale election and fence the terms the others win.
        if hold.as_ref().is_some_and(|ended| !*ended.borrow()) {
            if !waited {
                waited = true;
                moonpool_assertions::reachable!(
                    "coordinator: a moved founding member waits for its registration"
                );
            }
            if providers.time().sleep(pace).await.is_err() {
                return;
            }
            continue;
        }
        hold = None;
        steps += 1;
        let step = election.step(jitter(seed, steps, tunables.lease)).await;
        if let Some(term) = slowed.take() {
            moonpool_assertions::sometimes!(
                matches!(&step, Step::Leading { leader, .. } if leader.term == term),
                "coordinator: a term kept its lease through a slow install"
            );
        }
        match step {
            Step::Leading { leader, .. } if resigned == Some(leader.term) => {
                desk = None;
                park(&mut watching, &mut parked);
                election.resign(None).await;
            }
            Step::Leading { leader, .. } if served != Some(leader.term) => {
                let install = async {
                    let opened = open_term(
                        &providers,
                        &rpc,
                        &client,
                        &names,
                        (journals, founder_ids.clone(), founders.clone()),
                        &leader,
                        policy,
                    )
                    .await;
                    // Never in the recovery tail ([`chaos`]): a slow install
                    // there spends a lease and a half of the settle the
                    // election oracle allows.
                    if slow_install && chaos() && matches!(opened.0, TermDuty::Served { .. }) {
                        slow_install = false;
                        slowed = Some(leader.term);
                        moonpool_assertions::reachable!("coordinator: a term installs slowly");
                        let _ = providers
                            .time()
                            .sleep(tunables.lease + tunables.lease / 2)
                            .await;
                    }
                    opened
                };
                let (duty, session) = renewing(&providers, &mut election, install).await;
                watching = session.map(|session| {
                    let watch = parked.take().map_or_else(
                        || {
                            Watch::new(client.now(), tunables.renew_every, down_after, slow)
                                .with_space_floor(space_floor)
                        },
                        |watch| watch.resumed(client.now()),
                    );
                    (session, watch)
                });
                moves = Moves::default();
                changes = Modes::default();
                sets = Sets::new(ask_again);
                footprints = Footprints::new(internal.clone());
                scrubs = Scrubs::new(scrub);
                match duty {
                    TermDuty::Served { .. } => {
                        served = Some(leader.term);
                        desk = Some(open_desk(leader.term, leader.uuid, reuse));
                        // The term's group replaces any earlier one: a
                        // request to an ended term never reaches this one.
                        group = TermGroup::open(&rpc, leader.term);
                        let reference = group.as_ref().map(TermGroup::reference);
                        election.publish(addr.to_string(), reference.unwrap_or_default());
                    }
                    TermDuty::Refused => {
                        desk = None;
                        election.resign(None).await;
                    }
                    TermDuty::Unavailable => {
                        // The next try dials the founders where the
                        // registry holds them now.
                        let follow = follow_book(&rpc, &client, &names, &plan, (me, &addr));
                        if let Some(followed) = renewing(&providers, &mut election, follow).await {
                            client = followed;
                            dialed = None;
                        }
                    }
                }
            }
            Step::Leading { .. } if served.is_some() && watching.is_none() => {
                // Another writer fenced the term's session (an operator's
                // claim): the term cannot do its duties, and a machine that
                // asks it to register is refused. Never fight the session:
                // give the term to the next campaign, which installs itself
                // under a fresh uuid (#349).
                moonpool_assertions::reachable!("coordinator: a fenced term resigned");
                // Never in the recovery tail ([`chaos`]): a lost resignation
                // there leaves the cell with no coordinator for a lease.
                if lose_resign && chaos() {
                    lose_resign = false;
                    moonpool_assertions::reachable!(
                        "coordinator: a fenced term's resignation was lost"
                    );
                    election.step_down();
                } else {
                    election.resign(None).await;
                }
                assert!(election.leading().is_none(), "a resigned term is not led");
                served = None;
                desk = None;
                assert!(watching.is_none(), "a resigned term holds no session");
            }
            Step::Leading { leader, .. } if handing && chaos() => {
                handing = false;
                desk = None;
                if let Some(next) = successor(&plan, me)
                    && let Some(uuid) = election.resign(Some(next)).await
                {
                    moonpool_assertions::reachable!(
                        "coordinator: a coordinator handed its term on"
                    );
                    hand_off(&client, journals.cell, leader.uuid, uuid, 0).await;
                }
            }
            Step::Leading { .. } if stall && chaos() => {
                // A leader that stops calling: no renewal for longer than a
                // lease, so the other members take over.
                stall = false;
                moonpool_assertions::reachable!("coordinator: a coordinator skipped its renewals");
                let _ = providers
                    .time()
                    .sleep(tunables.lease + tunables.lease / 2)
                    .await;
                // Step the election before the desk answers again: the term
                // may be over.
                continue;
            }
            Step::Leading { .. } if watching.is_some() => {
                // A served term dials the founders where its session's
                // registry holds them now: a founder renamed while the term
                // runs (#349) is reached at its new name. Without it, a
                // term that opened Served kept the old name for its whole
                // run, every desk write to a tenant control journal led
                // there came back ambiguous, and a mode change waited on
                // its `Placed` record for good (#529's class on the desk
                // path: witness recipe 13334306166441940671
                // 77:327766447974513469 on PR #643 (durability mode)).
                if let Some((session, _)) = watching.as_ref() {
                    let book = followed_book(&plan, session.registry(), (me, &addr));
                    if dialed.as_ref() != Some(&book) {
                        if dialed.is_some() {
                            moonpool_assertions::reachable!(
                                "coordinator: a served term follows a renamed founder"
                            );
                        }
                        client = dial_book(&rpc, &client, &names, &book);
                        dialed = Some(book);
                    }
                }
                let duties = async {
                    let mut stepped = None;
                    if let Some((session, watch)) = watching.as_mut() {
                        match watch
                            .round(&providers, &rpc, &client, &names, session)
                            .await
                        {
                            // The term's first write that did not land ends its
                            // watch: never fight the session that fenced it.
                            Err(_) => park(&mut watching, &mut parked),
                            // A round ran: the tenant coordinator takes one step
                            // of a move off a draining machine (#474), one step
                            // of a journal's change to its desired mode (#214
                            // (tenant modes)), then one step of a matchmaker
                            // set's placement or move (#501 (matchmaker sets off
                            // the founders)).
                            Ok(_) if watch.ran() => {
                                if let Some((_, desk)) = desk.as_mut() {
                                    let ctx = (&providers, &rpc, &names, &client);
                                    let mut gone = super::moves::Leaving {
                                        gone: watch.gone(session.registry(), client.now()),
                                        held: watch.held(session.registry(), client.now()),
                                    };
                                    if defer && !gone.gone.is_empty() {
                                        defer = false;
                                        gone.gone.clear();
                                        moonpool_assertions::reachable!(
                                            "coordinator: a re-placement is deferred"
                                        );
                                    }
                                    stepped =
                                        Some(match moves.step(ctx, session, desk, &gone).await {
                                            None => None,
                                            Some(step) => {
                                                match changes.step(ctx, session, desk, &gone).await
                                                {
                                                    None => None,
                                                    Some(mode) => {
                                                        let set = sets
                                                            .step(ctx, session, desk, &gone)
                                                            .await;
                                                        // Then one step of a footprint (#476):
                                                        // a tenant coordinator placed, or a slot
                                                        // moved off a draining machine.
                                                        let footprint = match set {
                                                            Some(_) => {
                                                                footprints
                                                                    .step(&client, session)
                                                                    .await
                                                            }
                                                            None => None,
                                                        };
                                                        // Then one step of a tenant's
                                                        // scrub pass (#362 (background
                                                        // scrubbing)).
                                                        let scrubbed = match footprint {
                                                            Some(_) => {
                                                                scrubs
                                                                    .step(ctx, session, desk)
                                                                    .await
                                                            }
                                                            None => None,
                                                        };
                                                        // A set's move is left mid-way once
                                                        // its handover started, so the next
                                                        // term finds it running.
                                                        set.zip(footprint).zip(scrubbed).map(
                                                            |((set, _), _)| {
                                                                step.in_flight()
                                                                    || mode.in_flight()
                                                                    || set == SetStep::HandedOver
                                                            },
                                                        )
                                                    }
                                                }
                                            }
                                        });
                                }
                            }
                            Ok(_) => {}
                        }
                    }
                    stepped
                };
                let stepped = renewing(&providers, &mut election, duties).await;
                match stepped {
                    Some(None) => park(&mut watching, &mut parked),
                    Some(Some(true)) if quit_mid_move && chaos() => {
                        // A term that stops in the middle of a move: the
                        // next term finds the move where this one left it.
                        quit_mid_move = false;
                        moonpool_assertions::reachable!("coordinator: a term resigned mid-move");
                        desk = None;
                        park(&mut watching, &mut parked);
                        served = None;
                        election.resign(None).await;
                    }
                    _ => {}
                }
            }
            Step::Deposed { .. } => {
                served = None;
                desk = None;
                park(&mut watching, &mut parked);
                moonpool_assertions::reachable!(
                    "coordinator: a deposed coordinator stopped acting"
                );
            }
            Step::Following { .. } => desk = None,
            Step::Leading { .. } => {}
        }
        // A term this member no longer serves closes its group (#404): a
        // request sent to it fails at the transport from now on.
        if group
            .as_ref()
            .is_some_and(|g| desk.is_none() || served != Some(g.term))
        {
            if linger {
                linger = false;
                moonpool_assertions::reachable!("coordinator: an ended term's group lingers");
            } else {
                group = None;
                moonpool_assertions::reachable!("coordinator: an ended term's group is dropped");
            }
        }
        // Until the next step, answer the journal requests (#210) and the
        // machines' `Register` requests (#349) the term's group receives.
        let until = providers.time().now() + pace;
        let ctx = (&providers, &rpc, &client, &names);
        let cell = (plan.cell_id, tunables.renew_every);
        let answers = answer_until(
            ctx,
            cell,
            until,
            group.as_mut(),
            (&mut watching, &mut parked, &mut desk),
            &halt,
        );
        renewing(&providers, &mut election, answers).await;
        if let Some((term, _)) = desk.as_ref().filter(|(_, desk)| desk.superseded()) {
            // A tenant control journal refused the term's uuid: end the
            // term, as a refused cell control journal does.
            resigned = Some(*term);
            desk = None;
            park(&mut watching, &mut parked);
            election.resign(None).await;
        }
    }
    // A clean stop (#560 (clean Ctrl-C shutdown)), not the node loop's end:
    // the node loop still serves, so a led term can still be handed on.
    if halt.is_cancelled() && !shutdown.is_cancelled() {
        drop(group);
        hand_on(&client, &mut election, (&plan, me), journals.cell).await;
    }
}

/// Hand the term `election` leads, if any, on to the founding member after
/// `me` (#560 (clean Ctrl-C shutdown)): resign naming it, then move the
/// cell control journal's leader uuid to its, as a hand-off BUGGIFY
/// decision does. A lone founding member has nobody to hand it to, and a
/// lost hand-off costs the cell an election, never a second leader.
#[tracing::instrument(level = "debug", skip_all, fields(node = me.0))]
async fn hand_on<P: Providers>(
    client: &Client<P>,
    election: &mut Election<P>,
    (plan, me): (&CellPlan, NodeId),
    governed: JournalIdentifier,
) {
    let Some(leader) = election.leading().cloned() else {
        tracing::info!(node = me.0, "coordinator_stop_not_leading");
        return;
    };
    let Some(next) = successor(plan, me) else {
        moonpool_assertions::reachable!("coordinator: a stopping lone coordinator keeps its term");
        tracing::info!(
            node = me.0,
            term = leader.term,
            "coordinator_stop_no_successor"
        );
        return;
    };
    let to = next.id;
    if let Some(uuid) = election.resign(Some(next)).await {
        assert!(uuid != leader.uuid, "a hand-off names another uuid");
        let outcome = hand_off(client, governed, leader.uuid, uuid, 0).await;
        moonpool_assertions::reachable!("coordinator: a stopping coordinator hands its term on");
        tracing::info!(
            node = me.0,
            term = leader.term,
            to,
            ?outcome,
            "coordinator_stop_handed_on"
        );
    }
    assert!(
        election.leading().is_none(),
        "a stopped coordinator leads no term"
    );
}

/// Whether a term-ending BUGGIFY decision the node loop drew at spawn (a
/// stall, a hand-off, a resignation mid-move) may fire: never once the run's
/// recovery tail began. Drawn as plain BUGGIFY, they fired in the tail: each
/// founding member of a cell formed there ended its first term in turn, and
/// the election never settled (witness `replay-main 15129731564713398037` on
/// 7ec931e: three stalls in 18 s of the tail). Each is now a fault, silent
/// when the candidacy starts in the tail, and this read drops one drawn in
/// the chaos window and carried to a term the tail elects. A read of the
/// run's phase, no draw; in production no decision is ever drawn. A lost
/// resignation and a slow install read it too: in the tail, an `init` fenced
/// a term whose resignation was then lost, the next term installed slowly,
/// and it published too late for the election to settle (witness
/// `replay-main 94182577319794271` on 4c14100).
fn chaos() -> bool {
    !moonpool_buggify::buggify_is_recovering()
}

/// Run `work`, one of a led term's duties, and renew the term each time a
/// renewal falls due until it ends (#542 (coordinator starves its lease
/// while it installs)). [`Election::step`] renews only between duties, and
/// one duty may outlast a lease: an install claims and folds the cell
/// control journal, a watch round `Identify`s every machine. Without these
/// renewals every term of a slow cell was deposed before it served
/// (witness 17167272764625629279, a 1.6 s lease over a slow link). A
/// renewal still in flight when `work` ends is dropped: the next step sends
/// it again. With no term led, `work` runs alone.
async fn renewing<P: Providers, T>(
    providers: &P,
    election: &mut Election<P>,
    work: impl std::future::Future<Output = T>,
) -> T {
    let mut work = std::pin::pin!(work);
    while election.leading().is_some() {
        let wait = election.renewal_due_in();
        let renewal = async {
            let _ = providers.time().sleep(wait).await;
            election.renew_if_due().await
        };
        moonpool_core::select! {
            biased;
            out = &mut work => return out,
            led = renewal => {
                if !led {
                    break;
                }
                moonpool_assertions::reachable!("coordinator: a term renewed during its duties");
            }
        }
    }
    assert!(
        election.leading().is_none(),
        "only a term no longer led runs its duty without renewals"
    );
    work.await
}

/// The next request `inbound` holds, or never when there is no endpoint.
async fn next_request<M: moonpool_rpc::RpcMethod, T, U: 'static>(
    inbound: Option<&mut Inbound<M, T, U>>,
) -> Option<(T, ReplySender<U>)> {
    match inbound {
        Some(inbound) => inbound.recv().await,
        None => std::future::pending().await,
    }
}

/// Answer the `Register` requests (#349) and journal requests (#210) the
/// term's `group` receives until `until`; with no group, only wait. The
/// deadline is absolute: a request does not push the candidate's next step
/// back. A journal request goes through `desk` while this member serves the
/// term, and is refused `not_coordinator` by a group that outlived its term;
/// a desk the request found superseded stops the wait, so the term ends at
/// once.
async fn answer_until<P: Providers>(
    (providers, rpc, client, names): (&P, &RpcHandle<P>, &Client<P>, &Names),
    cell: (u64, Duration),
    until: Duration,
    mut group: Option<&mut TermGroup>,
    (watching, parked, desk): (
        &mut Watching,
        &mut Option<Watch>,
        &mut Option<(u64, TenantDesk)>,
    ),
    shutdown: &CancellationToken,
) {
    loop {
        let (registers, requests, capacities, joins) = match group.as_deref_mut() {
            Some(g) => (
                Some(&mut g.registers),
                Some(&mut g.requests),
                Some(&mut g.capacities),
                Some(&mut g.joins),
            ),
            None => (None, None, None, None),
        };
        let left = until.saturating_sub(providers.time().now());
        if left.is_zero() || shutdown.is_cancelled() {
            return;
        }
        moonpool_core::select! {
            biased;
            () = shutdown.cancelled() => return,
            Some((request, reply)) = next_request(registers) => {
                let ack =
                    register(providers, rpc, client, names, cell, (watching, parked), request)
                        .await;
                reply.send(ack);
            }
            Some((request, reply)) = next_request(requests) => {
                let answer = match (JournalRequest::from_wire(&request), desk.as_mut()) {
                    (Err(_), _) => JournalAnswer::Malformed,
                    (Ok(request), Some((_, desk))) => {
                        let session = watching.as_mut().map(|(session, _)| session);
                        let answer = desk.answer(client, &request, session).await;
                        if watching.as_ref().is_some_and(|(session, _)| !session.is_open()) {
                            // The term's first write that did not land ends
                            // its watch, as in `Watch::round`.
                            park(watching, parked);
                        }
                        reply.send(answer.to_wire());
                        if desk.superseded() {
                            return;
                        }
                        continue;
                    }
                    (Ok(_), None) => {
                        moonpool_assertions::reachable!(
                            "coordinator: a request to an ended term's group is refused"
                        );
                        JournalAnswer::NotCoordinator
                    }
                };
                reply.send(answer.to_wire());
            }
            Some((request, reply)) = next_request(capacities) => {
                // A tenant coordinator's capacity request (#476): decided
                // through the term's session, its booking ids drawn by the
                // desk.
                let answer = match (CapacityRequest::from_wire(&request), desk.as_mut()) {
                    (Err(_), _) => CapacityAnswer::Malformed,
                    (Ok(_), None) => CapacityAnswer::NotCoordinator,
                    (Ok(request), Some((_, desk))) => match watching.as_mut() {
                        None => CapacityAnswer::Unavailable,
                        Some((session, _)) => {
                            let answer = capacity::grant(client, session, &request, || {
                                desk.draw_booking()
                            })
                            .await;
                            if !session.is_open() {
                                park(watching, parked);
                            }
                            answer
                        }
                    },
                };
                reply.send(answer.to_wire());
            }
            Some((request, reply)) = next_request(joins) => {
                // `universe add-cell` (#232 (multiple cells)): the cell
                // half of a join, written through the term's session.
                let answer = match (JoinRequest::from_wire(&request), desk.as_ref()) {
                    (Err(_), _) => JoinAnswer::Malformed,
                    (Ok(_), None) => JoinAnswer::NotCoordinator,
                    (Ok(request), Some(_)) => match watching.as_mut() {
                        None => JoinAnswer::Unavailable,
                        Some((session, _)) => {
                            let answer = super::universe::join(client, session, &request).await;
                            if !session.is_open() {
                                park(watching, parked);
                            }
                            answer
                        }
                    },
                };
                reply.send(answer.to_wire());
            }
            _ = providers.time().sleep(left) => return,
        }
    }
}

/// The cell's address book for a `Register` from `id`, from a fold caught
/// up to the tail when the term's fold does not name `id`; `None` when the
/// catch-up failed. The term's fold moves only on its own writes, and an
/// operator's `cell add-machine` registers machines between them. The
/// machine takes `unknown_machine` as final, so it is answered only from a
/// fold at the tail (witness: recipe 1769568628990727465
/// 102806:3654508340332248788 on PR #691 (client write inputs): an admitted
/// machine renamed while it asked a term whose fold predated its
/// registration was never registered at its new name).
async fn book_naming<P: Providers>(
    session: &mut CellSession,
    client: &Client<P>,
    id: NodeId,
) -> Option<Vec<(NodeId, Address)>> {
    let book = super::cell_book(session.founders(), session.registry());
    if book.iter().any(|(member, _)| *member == id) {
        return Some(book);
    }
    let before = session.position();
    if !session.refresh(client, 0).await {
        return None;
    }
    assert!(
        session.position() >= before,
        "a refreshed fold never goes back"
    );
    moonpool_assertions::reachable!("coordinator: a register catches its fold up first");
    Some(super::cell_book(session.founders(), session.registry()))
}

/// Answer a machine's `Register` (#349): register the address it advertises
/// now in the cell control journal, through the served term's session, when
/// the cell's address book does not hold it. Refused while this member
/// serves no term. A write that does not land ends the term's watch, as in
/// [`Watch::round`]. The machine's `Identify` waits at most `patience`, one
/// renewal period, so a request to an address nobody answers at does not
/// cost the term its renewals. An `Identify` that does not answer in time
/// goes on in a task with the client's request timeout, and the machine's
/// next `Register` reads its answer: a link slower than one renewal period
/// stays slow for the whole run, and the inline check alone refused such a
/// machine forever (#454 (control plane meets chaos): witness
/// 3556034011252864713, a founding member renamed onto a link with 81 ms
/// of extra latency, under a 100 ms renewal period).
#[tracing::instrument(level = "debug", skip_all, fields(cell = cell_id))]
async fn register<P: Providers>(
    providers: &P,
    rpc: &RpcHandle<P>,
    client: &Client<P>,
    names: &Names,
    (cell_id, patience): (u64, Duration),
    (watching, parked): (&mut Watching, &mut Option<Watch>),
    request: wire::Register,
) -> wire::RegisterAck {
    let refuse = |refusal: &str| wire::RegisterAck {
        registered: false,
        refusal: refusal.into(),
    };
    let Some(identity) = request.identity else {
        return refuse("malformed");
    };
    let (Ok(addr), Some((class, locality))) = (
        Address::parse(&identity.addr),
        super::identity_tags(&identity),
    ) else {
        return refuse("malformed");
    };
    let id = NodeId(identity.node_id);
    if id.0 == 0 {
        return refuse("malformed");
    }
    if identity.cell_id != cell_id {
        return refuse("other_cell");
    }
    let Some((session, watch)) = watching.as_mut() else {
        return refuse("not_coordinator");
    };
    let Some(book) = book_naming(session, client, id).await else {
        return refuse("unavailable");
    };
    let Some((_, known)) = book.iter().find(|(member, _)| *member == id) else {
        return refuse("unknown_machine");
    };
    let registered = wire::RegisterAck {
        registered: true,
        refusal: String::new(),
    };
    // Registered already: the registry holds this address, this name and
    // this machine interface. A founding member the registry never held
    // registers once, for its name (#399), and again on every start that
    // serves another machine interface (#404).
    let held = session.registry().get(id).is_some_and(|node| {
        node.name == identity.name && node.machine_interface == identity.machine_interface
    });
    if *known == addr && held {
        return registered;
    }
    // The machine answers there as itself, and as the incarnation that
    // asked: a late request from an earlier start names an address the
    // machine left.
    let incarnation = incarnation_from_halves(identity.incarnation_high, identity.incarnation_low);
    let timeout = client.tunables().request_timeout;
    let probed = watch.probes.get(&id).and_then(|probe| {
        (probe.addr == addr && probe.incarnation == incarnation).then(|| {
            *probe
                .answered
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
        })
    });
    match probed {
        Some(Some(true)) => {
            moonpool_assertions::reachable!(
                "coordinator: a slow machine registers through its probe"
            );
        }
        // A probe of this address still runs: its answer is read next time.
        Some(None) => return refuse("unreachable"),
        Some(Some(false)) | None => {
            let answered = identified(
                bootstrap::identify(providers, rpc, names, &addr, patience.min(timeout)).await,
                id,
                incarnation,
            );
            if !answered {
                if timeout > patience {
                    let probe =
                        spawn_probe(providers, rpc, names, &addr, (id, incarnation), timeout);
                    watch.probes.insert(id, probe);
                }
                return refuse("unreachable");
            }
        }
    }
    watch.probes.remove(&id);
    let command = SystemCommand::RegisterNode {
        id,
        addr: addr.to_string(),
        class,
        capacity: identity.capacity,
        locality,
        name: identity.name,
        incarnation,
        machine_interface: identity.machine_interface,
    };
    if session.record(client, 0, &command).await.is_err() {
        // The term's first write that did not land ends its watch.
        park(watching, parked);
        return refuse("unavailable");
    }
    let book = super::cell_book(session.founders(), session.registry());
    if !book.iter().any(|entry| *entry == (id, addr.clone())) {
        // The registry refused it (a changed class): the book stands.
        return refuse("unknown_machine");
    }
    moonpool_assertions::reachable!("coordinator: a moved machine registers its new address");
    tracing::info!(cell = cell_id, node = id.0, %addr, "machine_address_registered");
    registered
}

/// Whether `answer` is `id`'s incarnation `incarnation` answering
/// `Identify` (see [`register`]).
fn identified(answer: Option<wire::IdentifyAck>, id: NodeId, incarnation: u128) -> bool {
    answer.is_some_and(|ack| {
        ack.node_id == id.0
            && incarnation_from_halves(ack.incarnation_high, ack.incarnation_low) == incarnation
    })
}

/// Start an `Identify` of `addr` that waits up to `timeout`, off the
/// renewal path (see [`register`]).
fn spawn_probe<P: Providers>(
    providers: &P,
    rpc: &RpcHandle<P>,
    names: &Names,
    addr: &Address,
    (id, incarnation): (NodeId, u128),
    timeout: Duration,
) -> Probe {
    let answered = Arc::new(std::sync::Mutex::new(None));
    let (slot, task_providers, rpc, names, target) = (
        answered.clone(),
        providers.clone(),
        rpc.clone(),
        names.clone(),
        addr.clone(),
    );
    providers
        .task()
        .spawn_task("paros-register-probe", async move {
            let answer = bootstrap::identify(&task_providers, &rpc, &names, &target, timeout).await;
            *slot.lock().unwrap_or_else(PoisonError::into_inner) =
                Some(identified(answer, id, incarnation));
        })
        .detach();
    Probe {
        addr: addr.clone(),
        incarnation,
        answered,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_jitter_stays_within_half_a_lease_and_moves_with_the_step() {
        let lease = Duration::from_millis(3000);
        let draws: Vec<Duration> = (0..64).map(|step| jitter(0xABCD, step, lease)).collect();
        assert!(draws.iter().all(|d| *d <= lease / 2));
        assert!(draws.windows(2).any(|w| w[0] != w[1]));
    }

    #[test]
    fn the_successor_is_the_next_founder_by_id_and_wraps() {
        let addr: Address = "10.0.0.1:1".parse().expect("an address");
        let plan = CellPlan {
            cell_id: 1,
            members: vec![
                (NodeId(3), addr.clone()),
                (NodeId(7), addr.clone()),
                (NodeId(9), addr),
            ],
            control: paros_core::JournalIdentifier::new(
                paros_core::TenantId(1),
                paros_core::JournalId(1),
            ),
            election: paros_core::JournalIdentifier::new(
                paros_core::TenantId(1),
                paros_core::JournalId(2),
            ),
            fleet: None,
            journals: Vec::new(),
        };
        assert_eq!(successor(&plan, NodeId(3)).map(|c| c.id), Some(7));
        assert_eq!(successor(&plan, NodeId(9)).map(|c| c.id), Some(3));
    }
}
