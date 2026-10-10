//! **The cell coordinator** (#240, `docs/architecture.md` §3.3, §3.7): every
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
//!    machine an earlier term admitted answers that it is in the cell.
//! 3. **Publish** the interface: the next renewal names the member's
//!    address ([`Election::publish`]).
//! 4. **Stop at the first refused write**: a claim that loses ends the term,
//!    and the coordinator resigns. Its writer sends nothing once superseded.
//! 5. **Watch the cell's machines** (#211, D6, [`Watch`]): at every renewal
//!    period, `Identify` each founding member and each registered machine
//!    not retired. A machine that answers as a new incarnation of a
//!    registered one registers again (a reboot); one marked down, or seen
//!    as another incarnation, is written `MachineUp`; one silent for
//!    `machine_down_after` is written `MachineDown`. Only changes are
//!    written, never a heartbeat. The watch ends with the term, or at the
//!    term's first write that does not land: a coordinator an admin session
//!    fenced does not fight back. It resigns the term instead, so that the
//!    next term installs itself under a fresh uuid and its duties go on
//!    (#349: a moved machine is otherwise refused for the rest of the term).
//! 6. **Register moved machines** (#349): a machine of the cell asks the
//!    coordinator with `Register` to record the address it advertises now
//!    (`super::register`). The coordinator checks that the machine answers
//!    `Identify` there as itself, then writes `RegisterNode` with that
//!    address, unless the cell's address book holds it already. Every
//!    founding member serves `Register`; one that does not serve a term
//!    refuses it (`not_coordinator`).
//!
//! For a term it served, the coordinator also acts as the **tenant
//! coordinator** of every tenant its cell hosts (#210, until #212 and
//! #225): it answers the journal requests sent to the interface it
//! published, writing each tenant's control journal under its term uuid
//! ([`super::tenants`]). A machine that serves no term answers
//! `not_coordinator`. A tenant control journal that refuses the term uuid
//! ends the term as a refused cell control journal does: the desk stops and
//! the coordinator resigns, so no write under that uuid follows.
//!
//! Until admin calls become requests to the coordinator (#212, #225), an
//! admin session still claims the cell control journal and fences the
//! coordinator. The coordinator does not fight back within its term: it
//! resigns, and the next term claims the journal again.
//!
//! The candidate draws no randomness of its own: its seed and its three
//! BUGGIFY decisions (a stalled leader, a hand-off, a reused journal id) are
//! drawn on the node loop before the task starts, and the backoff jitter and
//! the journal ids derive from the seed.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use moonpool_core::{
    Detach, Providers, RandomProvider, SimulationResult, TaskProvider, TimeProvider,
};
use moonpool_rpc::RpcHandle;
use paros_core::{LeaderUuid, NodeId};
use tokio_util::sync::CancellationToken;

use super::tenants::TenantDesk;
use super::{CellPlan, Class, ControlJournals, FormedCell, incarnation_from_halves};
use crate::client::cell::CellSession;
use crate::client::checkpoint::CheckpointPolicy;
use crate::client::election::{Candidate, Election, ElectionTunables, Leader, Step, hand_off};
use crate::client::fleet::{Interrupted, Stage, Step as FleetStep};
use crate::client::journals::{JournalAnswer, JournalRequest};
use crate::client::{CallObserver, ClaimOutcome, Client, ClientTunables, Server, bootstrap};
use crate::rpc::machine as wire;
use crate::rpc::methods::{JournalRequestRpc, RegisterRpc};
use crate::rpc::{Inbound, NodeClient, serve_well_known};
use crate::system::{NodeStanding, SystemCommand};
use crate::{Address, DriverTunables, Names};

/// A served term's session and its watch over the cell's machines (#211).
type Watching = Option<(CellSession, Watch)>;

/// The journal requests' endpoint (#210).
type Requests = Inbound<JournalRequestRpc, wire::JournalRequest, wire::JournalRequestAck>;

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
    cell: (ControlJournals, Vec<(NodeId, Address)>),
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
    (journals, founders): (ControlJournals, Vec<(NodeId, Address)>),
    leader: &Leader,
    policy: CheckpointPolicy,
) -> (TermDuty, Option<CellSession>) {
    assert!(leader.uuid.is_set(), "a term's uuid is set");
    let mut session = CellSession::with_leader(journals, founders, leader.uuid, policy);
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
    /// How often a round runs.
    every: Duration,
    /// When the watch started: a machine never heard is silent since then.
    started: Duration,
    /// When the next round is due.
    next: Duration,
    /// When each machine last answered as itself.
    heard: BTreeMap<NodeId, Duration>,
}

impl Watch {
    /// A watch started `now`, running a round `every` period and marking a
    /// machine down after `down_after` of silence.
    ///
    /// # Panics
    ///
    /// If `down_after` is not above `every`: one missed round would mark a
    /// machine down (the tunables' floor refuses it).
    #[must_use]
    pub fn new(now: Duration, every: Duration, down_after: Duration) -> Self {
        assert!(!every.is_zero(), "a watch round has a period");
        assert!(
            down_after > every,
            "one missed round never marks a machine down"
        );
        Self {
            down_after,
            every,
            started: now,
            next: now,
            heard: BTreeMap::new(),
        }
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
        if client.now() < self.next {
            return Ok(0);
        }
        self.next = client.now() + self.every;
        let timeout = client.tunables().request_timeout;
        // Each machine where the cell's address book says it is (#349).
        let machines: BTreeMap<NodeId, Address> =
            super::cell_book(session.founders(), session.registry())
                .into_iter()
                .collect();
        let mut wrote = 0;
        for (id, addr) in machines {
            let answer = bootstrap::identify(providers, rpc, names, &addr, timeout)
                .await
                .filter(|ack| ack.node_id == id.0);
            let command = match answer {
                Some(ack) => {
                    self.heard.insert(id, client.now());
                    Self::answered(session, id, &addr, &ack)
                }
                None => self.silent(session, id, client.now()),
            };
            if let Some(command) = command {
                session.record(client, 0, &command).await?;
                wrote += 1;
            }
        }
        Ok(wrote)
    }

    /// The change machine `id` answering `ack` at `addr` makes, if any.
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
        let held = registry.liveness(id);
        match registry.get(id) {
            Some(node) if node.incarnation != incarnation => {
                // A reboot of a registered machine: it registers again, as
                // the incarnation that answered (up, by its registration).
                let class = ack.class.parse::<Class>().ok()?;
                if class != node.class {
                    return None;
                }
                moonpool_assertions::reachable!("coordinator: a rebooted machine registers again");
                Some(SystemCommand::RegisterNode {
                    id,
                    addr: addr.to_string(),
                    class,
                    capacity: ack.capacity,
                    failure_domain: ack.failure_domain.clone(),
                    incarnation,
                })
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
/// address, resolved at each call (#257).
fn cell_client<P: Providers>(providers: &P, rpc: &RpcHandle<P>, formed: &FormedCell) -> Client<P> {
    // This member at the address it advertises now (#349), the others at
    // the plan's.
    let book = super::with_own(
        &formed.plan.members,
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
    Client::new(providers, servers, ClientTunables::default())
}

/// A founding member's candidacy, as the node loop hands it to the task.
struct Candidacy {
    me: NodeId,
    addr: Address,
    names: Names,
    plan: CellPlan,
    seed: u128,
    tunables: ElectionTunables,
    /// The failure detector's timeout (#211).
    down_after: Duration,
    /// The leader stops renewing for one lease and a half after its first
    /// term's duties (a BUGGIFY decision: the takeover's way in).
    stall: bool,
    /// The leader hands its first term on to another founding member after
    /// its duties (a BUGGIFY decision).
    hand_off: bool,
    /// The tenant coordinator reuses a taken journal id at the first draw of
    /// every create (a BUGGIFY decision): the fold must refuse it.
    reuse: bool,
    /// The machines' `Register` requests (#349).
    registers: Inbound<RegisterRpc, wire::Register, wire::RegisterAck>,
}

/// Start the candidacy of the founding member `formed` for the cell
/// coordinator, in a task that stops with `shutdown`, and serve the journal
/// requests (#210) and the machines' `Register` requests (#349) there.
/// Draws its seed and its BUGGIFY decisions here, on the node loop. Nothing
/// starts when the plan names no election journal or the tunables are not a
/// working election.
///
/// # Errors
///
/// The journal requests' or the `Register` endpoint could not be
/// registered.
pub(crate) fn spawn<P: Providers>(
    providers: &P,
    rpc: &RpcHandle<P>,
    formed: &FormedCell,
    tunables: &DriverTunables,
    observer: Option<Arc<dyn CallObserver>>,
    shutdown: CancellationToken,
) -> SimulationResult<()> {
    let election = election_tunables(tunables);
    if !election.is_valid() || !formed.plan.election.is_set() {
        return Ok(());
    }
    let requests = Inbound::plain(serve_well_known::<P, JournalRequestRpc>(rpc)?);
    let candidacy = Candidacy {
        me: formed.facts.node_id,
        addr: formed.facts.addr.clone(),
        names: formed.facts.names.clone(),
        plan: formed.plan.clone(),
        seed: providers.random().random(),
        tunables: election,
        down_after: tunables.machine_down_after,
        stall: moonpool_buggify::buggify_with_prob!(0.3),
        hand_off: formed.plan.members.len() > 1 && moonpool_buggify::buggify_with_prob!(0.25),
        reuse: moonpool_buggify::buggify_with_prob!(0.3),
        registers: Inbound::plain(serve_well_known::<P, RegisterRpc>(rpc)?),
    };
    let mut client = cell_client(providers, rpc, formed).with_shutdown(shutdown.clone());
    if let Some(observer) = observer {
        client = client.with_observer(observer);
    }
    let providers = providers.clone();
    let rpc = rpc.clone();
    providers
        .task()
        .spawn_task(
            "paros-cell-coordinator",
            campaign(
                providers.clone(),
                rpc,
                client,
                candidacy,
                requests,
                shutdown,
            ),
        )
        .detach();
    Ok(())
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
            interface: String::new(),
        })
}

/// The candidate's loop: step the election, serve every term it wins,
/// answer the journal requests between steps, and stop with `shutdown`.
#[tracing::instrument(level = "debug", skip_all, fields(node = candidacy.me.0, cell = candidacy.plan.cell_id))]
// One loop over the election's steps: each arm is a thin call into the
// term's duties, and splitting the arms out would scatter its shared state.
#[allow(clippy::too_many_lines)]
async fn campaign<P: Providers>(
    providers: P,
    rpc: RpcHandle<P>,
    client: Client<P>,
    candidacy: Candidacy,
    mut requests: Requests,
    shutdown: CancellationToken,
) {
    let Candidacy {
        me,
        addr,
        names,
        plan,
        seed,
        tunables,
        down_after,
        mut stall,
        hand_off: mut handing,
        reuse,
        mut registers,
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
            interface: String::new(),
        },
        seed,
        tunables,
    );
    // The term whose duties are done.
    let mut served: Option<u64> = None;
    // The tenant coordinator of the term served, while it leads it.
    let mut desk: Option<(u64, TenantDesk)> = None;
    let open_desk = |term: u64, uuid: LeaderUuid, reuse: bool| {
        let desk = TenantDesk::new(
            uuid,
            journals.cell,
            &founder_ids,
            (seed ^ u128::from(term), reuse),
            policy,
        );
        (term, desk)
    };
    // The term a superseded desk resigned: resigned again while the
    // election still names it (a resignation can be lost).
    let mut resigned: Option<u64> = None;
    // The served term's session and its watch over the cell's machines.
    let mut watching: Option<(CellSession, Watch)> = None;
    let mut steps = 0_u64;
    while !shutdown.is_cancelled() {
        steps += 1;
        match election.step(jitter(seed, steps, tunables.lease)).await {
            Step::Leading { leader, .. } if resigned == Some(leader.term) => {
                desk = None;
                watching = None;
                election.resign(None).await;
            }
            Step::Leading { leader, .. } if served != Some(leader.term) => {
                let (duty, session) = open_term(
                    &providers,
                    &rpc,
                    &client,
                    &names,
                    (journals, founders.clone()),
                    &leader,
                    policy,
                )
                .await;
                watching = session.map(|session| {
                    let watch = Watch::new(client.now(), tunables.renew_every, down_after);
                    (session, watch)
                });
                match duty {
                    TermDuty::Served { .. } => {
                        served = Some(leader.term);
                        desk = Some(open_desk(leader.term, leader.uuid, reuse));
                        election.publish(addr.to_string());
                    }
                    TermDuty::Refused => {
                        desk = None;
                        election.resign(None).await;
                    }
                    TermDuty::Unavailable => {}
                }
            }
            Step::Leading { .. } if served.is_some() && watching.is_none() => {
                // Another writer fenced the term's session (an operator's
                // claim): the term cannot do its duties, and a machine that
                // asks it to register is refused. Never fight the session:
                // give the term to the next campaign, which installs itself
                // under a fresh uuid (#349).
                moonpool_assertions::reachable!("coordinator: a fenced term resigned");
                election.resign(None).await;
                served = None;
                desk = None;
                assert!(watching.is_none(), "a resigned term holds no session");
            }
            Step::Leading { leader, .. } if handing => {
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
            Step::Leading { .. } if stall => {
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
                if let Some((session, watch)) = watching.as_mut()
                    && watch
                        .round(&providers, &rpc, &client, &names, session)
                        .await
                        .is_err()
                {
                    // The term's first write that did not land ends its
                    // watch: never fight the session that fenced it.
                    watching = None;
                }
            }
            Step::Deposed { .. } => {
                served = None;
                desk = None;
                watching = None;
                moonpool_assertions::reachable!(
                    "coordinator: a deposed coordinator stopped acting"
                );
            }
            Step::Following { .. } => desk = None,
            Step::Leading { .. } => {}
        }
        // Until the next step, answer the journal requests (#210) and the
        // machines' `Register` requests (#349).
        let until = providers.time().now() + pace;
        let ctx = (&providers, &rpc, &client, &names);
        let cell = (plan.cell_id, tunables.renew_every);
        let inboxes = (&mut registers, &mut requests);
        answer_until(
            ctx,
            cell,
            until,
            inboxes,
            (&mut watching, &mut desk),
            &shutdown,
        )
        .await;
        if let Some((term, _)) = desk.as_ref().filter(|(_, desk)| desk.superseded()) {
            // A tenant control journal refused the term's uuid: end the
            // term, as a refused cell control journal does.
            resigned = Some(*term);
            desk = None;
            watching = None;
            election.resign(None).await;
        }
    }
}

/// Answer `Register` requests (#349) and journal requests (#210) until
/// `until`. The deadline is absolute: a request does not push the
/// candidate's next step back. A journal request goes through `desk` while
/// this member serves a term, and is refused `not_coordinator` otherwise;
/// a desk the request found superseded stops the wait, so the term ends at
/// once.
async fn answer_until<P: Providers>(
    (providers, rpc, client, names): (&P, &RpcHandle<P>, &Client<P>, &Names),
    cell: (u64, Duration),
    until: Duration,
    (registers, requests): (
        &mut Inbound<RegisterRpc, wire::Register, wire::RegisterAck>,
        &mut Requests,
    ),
    (watching, desk): (&mut Watching, &mut Option<(u64, TenantDesk)>),
    shutdown: &CancellationToken,
) {
    loop {
        let left = until.saturating_sub(providers.time().now());
        if left.is_zero() || shutdown.is_cancelled() {
            return;
        }
        moonpool_core::select! {
            biased;
            () = shutdown.cancelled() => return,
            Some((request, reply)) = registers.recv() => {
                let ack = register(providers, rpc, client, names, cell, watching, request).await;
                reply.send(ack);
            }
            Some((request, reply)) = requests.recv() => {
                let answer = match (JournalRequest::from_wire(&request), desk.as_mut()) {
                    (Err(_), _) => JournalAnswer::Malformed,
                    (Ok(request), Some((_, desk))) => {
                        let answer = desk.answer(client, &request).await;
                        reply.send(answer.to_wire());
                        if desk.superseded() {
                            return;
                        }
                        continue;
                    }
                    (Ok(_), None) => {
                        moonpool_assertions::reachable!(
                            "coordinator: a request to a machine serving no term is refused"
                        );
                        JournalAnswer::NotCoordinator
                    }
                };
                reply.send(answer.to_wire());
            }
            _ = providers.time().sleep(left) => return,
        }
    }
}

/// Answer a machine's `Register` (#349): register the address it advertises
/// now in the cell control journal, through the served term's session, when
/// the cell's address book does not hold it. Refused while this member
/// serves no term. A write that does not land ends the term's watch, as in
/// [`Watch::round`]. The machine's `Identify` waits at most `patience`, one
/// renewal period, so a request to an address nobody answers at does not
/// cost the term its renewals.
#[tracing::instrument(level = "debug", skip_all, fields(cell = cell_id))]
async fn register<P: Providers>(
    providers: &P,
    rpc: &RpcHandle<P>,
    client: &Client<P>,
    names: &Names,
    (cell_id, patience): (u64, Duration),
    watching: &mut Option<(CellSession, Watch)>,
    request: wire::Register,
) -> wire::RegisterAck {
    let refuse = |refusal: &str| wire::RegisterAck {
        registered: false,
        refusal: refusal.into(),
    };
    let Some(identity) = request.identity else {
        return refuse("malformed");
    };
    let (Ok(addr), Ok(class)) = (
        Address::parse(&identity.addr),
        identity.class.parse::<Class>(),
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
    let Some((session, _)) = watching.as_mut() else {
        return refuse("not_coordinator");
    };
    let book = super::cell_book(session.founders(), session.registry());
    let Some((_, known)) = book.iter().find(|(member, _)| *member == id) else {
        return refuse("unknown_machine");
    };
    let registered = wire::RegisterAck {
        registered: true,
        refusal: String::new(),
    };
    if *known == addr {
        return registered;
    }
    // The machine answers there as itself, and as the incarnation that
    // asked: a late request from an earlier start names an address the
    // machine left.
    let incarnation = incarnation_from_halves(identity.incarnation_high, identity.incarnation_low);
    let answer = bootstrap::identify(
        providers,
        rpc,
        names,
        &addr,
        patience.min(client.tunables().request_timeout),
    )
    .await
    .filter(|ack| {
        ack.node_id == id.0
            && incarnation_from_halves(ack.incarnation_high, ack.incarnation_low) == incarnation
    });
    if answer.is_none() {
        return refuse("unreachable");
    }
    let command = SystemCommand::RegisterNode {
        id,
        addr: addr.to_string(),
        class,
        capacity: identity.capacity,
        failure_domain: identity.failure_domain,
        incarnation,
    };
    if session.record(client, 0, &command).await.is_err() {
        // The term's first write that did not land ends its watch.
        *watching = None;
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
