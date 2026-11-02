//! **The cell coordinator** (#240, `docs/architecture/03-3-coordinators-and-placement.md` §3.3, §3.7): every
//! founding member campaigns for it in the cell's election journal through
//! [`paros_client::election`], and the winner leads the cell control journal
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
//!    follow the universe directory ([`crate::universe`]): join the cell to
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
//!    those facts which machines are alive but slow ([`crate::cell::slow`], #361
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
//! ([`crate::tenant`]). A group whose term ended answers `not_coordinator`
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

mod answers;
mod campaign;
pub mod capacity;
pub mod footprints;
pub mod slow;
mod watch;

pub use campaign::spawn;
pub use watch::Watch;

use moonpool_rpc::{AccessClass, RpcHandle, ServiceGroup};
use paros_client::cell::CellSession;
use paros_client::checkpoint::{CheckpointPolicy, Folder, LoadOutcome, load};
use paros_client::election::{ElectionTunables, Leader, read_election};
use paros_client::fleet::{Interrupted, Stage, Step as FleetStep};
use paros_client::{ClaimOutcome, Client, Server};
use paros_core::NodeId;
use paros_model::address::{Address, Names};
use paros_model::fleet::FleetDirectory;
use paros_model::machine::{CellPlan, ControlJournals};
use paros_model::registry::{NodeStanding, Registry};
use paros_net::runtime::ParosProviders;
use paros_net::{Inbound, NodeClient};
use paros_roles::node::host::FormedCell;
use paros_tunables::DriverTunables;
use paros_wire::machine as wire;
use paros_wire::methods::{
    CapacityRequestRpc, CoordinatorInterface, JoinUniverseRpc, JournalRequestRpc, RegisterRpc,
};

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
    fn open<P: ParosProviders>(rpc: &RpcHandle<P>, term: u64) -> Option<Self> {
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
        paros_net::published(&self.group.interface_ref())
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
pub async fn serve_term<P: ParosProviders>(
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
async fn open_term<P: ParosProviders>(
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
    // A term installs only over a leader it judged (#653 (election term to
    // installed leader)): read the cell journal's leader, then the election;
    // a term the election moved past leads nothing, and the claim replaces
    // only the leader read. A later term campaigns before it installs, so a
    // leader read first that a later term installed is in the election read
    // after. Without it, a deposed term whose fold still leads (its lease is
    // its own clock's) read the next term's uuid and installed over it
    // (witness 593093569845979540: term 8 over term 9 at slot 20).
    let opened = match journals.election {
        Some(election) => {
            let Some(current) = client.journal_state(journals.cell, 0).await else {
                return (TermDuty::Unavailable, None);
            };
            let Some(fold) = read_election(client, election, 0).await else {
                return (TermDuty::Unavailable, None);
            };
            match fold.leader().map(|now| now.term) {
                Some(term) if term > leader.term => {
                    moonpool_assertions::reachable!(
                        "coordinator: a term the election moved past installs nothing"
                    );
                    return (TermDuty::Refused, None);
                }
                Some(term) if term == leader.term => {}
                // A fold behind the term this candidacy won: read again.
                _ => return (TermDuty::Unavailable, None),
            }
            session.open_over(client, 0, current.leader).await
        }
        None => session.open(client, 0).await,
    };
    match opened {
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
        let _ = crate::universe::follow(client, &mut session, &mut directory).await;
    }
    let open = session.is_open().then_some(session);
    (TermDuty::Served { admitted }, open)
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
pub fn cell_client<P: ParosProviders>(
    providers: &P,
    rpc: &RpcHandle<P>,
    formed: &FormedCell,
    tunables: &DriverTunables,
) -> Client<P> {
    // This member at the address it advertises now (#349), the others where
    // its cached registry fold dials them, else at the plan's (#211).
    let book = paros_model::machine::with_own(
        &formed.founders(),
        formed.facts.node_id,
        &formed.facts.addr,
    );
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
async fn follow_book<P: ParosProviders>(
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
    Some(dial_book(rpc, client, names, &book).rotating_over(plan.members.len()))
}

/// The founding members where `registry` holds them, this member at the
/// address it advertises (#349).
fn followed_book(
    plan: &CellPlan,
    registry: &Registry,
    (me, own): (NodeId, &Address),
) -> Vec<(NodeId, Address)> {
    let mut book = paros_model::machine::with_own(
        &paros_model::machine::address_book(&plan.members, registry),
        me,
        own,
    );
    assert_eq!(
        book.len(),
        plan.members.len(),
        "a followed book names every founding member"
    );
    // Then every machine a tenant control journal grew onto (#685 (special
    // tenants are tenants)), at the address it registered: a leader hint
    // may name it.
    let founders: Vec<NodeId> = plan.members.iter().map(|(id, _)| *id).collect();
    let planned = crate::tenant::controls::plan_journals(plan.control_journals());
    for extra in crate::tenant::controls::extra_members(registry, &planned, &founders) {
        if let Some(addr) = registry
            .address(extra)
            .and_then(|addr| Address::parse(addr).ok())
        {
            book.push((extra, addr));
        }
    }
    assert!(
        book.iter()
            .take(plan.members.len())
            .all(|(id, _)| founders.contains(id)),
        "a followed book names the founding members first"
    );
    assert!(
        book.iter().any(|(id, addr)| *id == me && addr == own),
        "a followed book dials this member where it advertises"
    );
    book
}

/// `client` over `book`, each member dialed by its address.
fn dial_book<P: ParosProviders>(
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cell::campaign::jitter;
    use crate::cell::campaign::successor;
    use std::time::Duration;

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
            control: paros_core::journal::JournalIdentifier::new(
                paros_core::journal::TenantId(1),
                paros_core::journal::JournalId(1),
            ),
            election: paros_core::journal::JournalIdentifier::new(
                paros_core::journal::TenantId(1),
                paros_core::journal::JournalId(2),
            ),
            fleet: None,
            journals: Vec::new(),
        };
        assert_eq!(successor(&plan, NodeId(3)).map(|c| c.id), Some(7));
        assert_eq!(successor(&plan, NodeId(9)).map(|c| c.id), Some(3));
    }
}
