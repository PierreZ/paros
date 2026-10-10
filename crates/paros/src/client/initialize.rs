//! `init` whole (#196, #229, #246, #277): the cell step, then the fleet
//! steps, as one resumable operation. `parosctl init` prints what it comes
//! to, and the simulation's workload runs the very same code.
//!
//! The cell step is `cell init` over the founding members: sent to the
//! first listed machine still idle, which drives the cell decree over every
//! listed machine ([`crate::machine`]); a formed one has no `CellInit` and
//! the next is asked. Then `init` waits for the cell's first coordinator:
//! the founding members campaign in the cell's election journal, and the
//! winner installs its uuid on the cell control journal with
//! `SetLeader(uuid, unset)` (#240, [`crate::machine`]'s coordinator). `init`
//! claims nothing itself. A re-run resumes: an interrupted decree is
//! finished by whichever listed machine is asked, and a cell every member
//! formed is learned from the members.
//!
//! Then the fleet steps ([`super::fleet`]): the fleet tenant (served by the
//! cell's members) records the fleet's id — drawn by the caller, kept on a
//! re-run — and adds the cell with its cell tenant, the cell records the
//! fleet on its side, and the fleet tenant marks the cell `READY`. No
//! identifier is fixed (§3.8): a first run takes them from the plan it
//! formed, a re-run learns them from the members' `Inspect`. Both journals are
//! written under the run's leader uuids: the run takes them over (#241),
//! fencing the coordinator too until admin calls become requests to it
//! (#212, #225; the coordinator stops at its first refused write). Every step is idempotent: `init` is
//! refused only when it found nothing left to do.

use std::net::SocketAddr;
use std::time::Duration;

use moonpool_core::Providers;
use moonpool_rpc::RpcHandle;
use paros_core::{JournalIdentifier, NodeId};

use super::Client;
use super::bootstrap::{self, InitOutcome};
use super::election::read_election;
use super::fleet::{FleetRefusal, FleetSession, Interrupted, Stage, Step};
use crate::machine::ControlJournals;
use crate::system::Registry;

/// What a whole `init` came to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InitRun {
    /// The fleet is initialized, and this run wrote something on the way.
    Initialized(Initialized),
    /// The run found nothing left to do: `already_initialized`.
    AlreadyInitialized(Initialized),
    /// The run cannot go on: running it again changes nothing.
    Refused(InitRefusal),
    /// Nothing decided in time: run it again, it resumes.
    Unreachable(Unreachable),
    /// A fleet step's write did not land: run it again, it resumes.
    Interrupted(Interrupted),
}

/// What an initialized fleet is, as one `init` saw it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Initialized {
    /// The fleet's id.
    pub fleet_id: u64,
    /// The cell coordinator: the candidate the election journal named
    /// leader once the cell control journal had one.
    pub coordinator: NodeId,
    /// The cell's members by id, in id order: the cell control journal's
    /// genesis pool, whether or not each answered this run.
    pub members: Vec<u64>,
    /// The cell's servers this run reached: `(node id, address)`.
    pub servers: Vec<(u64, SocketAddr)>,
    /// The control journals, learned from the plan or `Inspect`.
    pub journals: ControlJournals,
    /// The static assignment's user journals, known only to the run that
    /// formed the cell (the only time they are printed).
    pub users: Vec<JournalIdentifier>,
    /// The fleet steps this run wrote, in order.
    pub steps: Vec<Stage>,
}

/// Why `init` cannot go on.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InitRefusal {
    /// The cell decree refused: its label (see [`InitOutcome::Refused`]).
    Formation(String),
    /// The formed cell hosts no fleet tenant.
    NoFleet,
    /// A fleet step refused.
    Fleet(FleetRefusal),
}

/// What `init` waited on in vain.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Unreachable {
    /// No member was listed.
    NoTarget,
    /// A member answered with a plan that does not decode.
    Malformed,
    /// The formation decided nothing in time: a member is not up yet.
    Formation,
    /// Nothing answered `CellInit` or `Inspect`.
    NothingAnswered,
    /// No server named its cell's control journals.
    NoControlJournals,
    /// No server described the cell control journal, or it names no member.
    NoMembers,
    /// The cell named no election journal, or elected no coordinator in
    /// time.
    NoCoordinator,
}

/// The caller's half of `init`: how long a step may take, the fleet id a
/// first run records and the seed of the run's leader uuids (both drawn by
/// the caller: `paros::client` draws no randomness).
#[derive(Clone, Copy, Debug)]
pub struct InitParams {
    /// How long the decree may take to form the cell, the cell to elect its
    /// first leader, and an interrupted fleet step to be taken again.
    pub patience: Duration,
    /// The fleet id a first run records; a re-run keeps the recorded one.
    pub fleet_id: u64,
    /// The seed of this run's leader uuids ([`super::Writer::new`]): every
    /// run draws its own.
    pub leader_seed: u128,
}

/// What a found cell is: its servers, its members, its control journals
/// and its user journals (none: only the run that formed the cell knows
/// them).
type Found = (
    Vec<(u64, SocketAddr)>,
    Vec<u64>,
    ControlJournals,
    Vec<JournalIdentifier>,
);

/// A cell an earlier run formed, learned from its servers: their ids and
/// its control journals from a node-only `Inspect` (no identifier is fixed,
/// §3.8), its members from the cell control journal's. The fleet steps fold
/// the cell control journal over its genesis pool, the cell's members — the
/// pool the run that formed the cell folds it over — never over whichever
/// servers answered this time (not reproduced in the simulation: a fold's
/// pool must not depend on who was up).
async fn found<P: Providers>(
    providers: &P,
    rpc: &RpcHandle<P>,
    addrs: &[SocketAddr],
    connect: &impl Fn(&[(u64, SocketAddr)]) -> Client<P>,
    patience: Duration,
) -> Result<Found, Unreachable> {
    let servers = bootstrap::discover(providers, rpc, addrs, patience).await;
    if servers.is_empty() {
        return Err(Unreachable::NothingAnswered);
    }
    let client = connect(&servers);
    let journals = bootstrap::control_journals(&client)
        .await
        .ok_or(Unreachable::NoControlJournals)?;
    // The genesis pool is the cell's members, never only the servers that
    // answered: one of them may be down.
    let mut members = client
        .inspect(0, journals.cell)
        .await
        .map(|view| view.members)
        .unwrap_or_default();
    members.sort_unstable();
    members.dedup();
    if members.is_empty() {
        return Err(Unreachable::NoMembers);
    }
    Ok((servers, members, journals, Vec::new()))
}

/// Wait up to `patience` for the cell's coordinator: the cell control
/// journal has a leader, and the election journal names one. The
/// coordinator installs the cell control journal's first leader (#240), so
/// a fleet step never claims an unset journal.
async fn coordinator<P: Providers>(
    client: &Client<P>,
    journals: &ControlJournals,
    patience: Duration,
) -> Result<NodeId, Unreachable> {
    let election = journals.election.ok_or(Unreachable::NoCoordinator)?;
    let deadline = client.now() + patience;
    loop {
        let installed = client
            .journal_state(journals.cell, 0)
            .await
            .is_some_and(|state| state.leader.is_some());
        if installed
            && let Some(fold) = read_election(client, election, 0).await
            && let Some(leader) = fold.leader()
        {
            assert_ne!(leader.candidate.id, 0, "a candidate id is never zero");
            return Ok(NodeId(leader.candidate.id));
        }
        if client.now() >= deadline || !client.pause(client.tunables().retry_backoff).await {
            return Err(Unreachable::NoCoordinator);
        }
    }
}

/// Run `init` whole: form the cell over the founding `members` (or learn it,
/// when every member already serves it), wait for its coordinator, then
/// run the fleet steps, through a client `connect` builds over the cell's
/// servers.
///
/// # Panics
///
/// If the fleet half ends advanced (a broken [`FleetSession::init`]
/// contract), or `params` carries an unset fleet id.
#[tracing::instrument(level = "debug", skip_all, fields(members = members.len()))]
pub async fn initialize<P: Providers>(
    providers: &P,
    rpc: &RpcHandle<P>,
    members: &[SocketAddr],
    connect: impl Fn(&[(u64, SocketAddr)]) -> Client<P>,
    params: InitParams,
) -> InitRun {
    assert_ne!(params.fleet_id, 0, "a fleet id is never unset");
    if members.is_empty() {
        return InitRun::Unreachable(Unreachable::NoTarget);
    }
    let patience = params.patience;
    let mut formation = None;
    for &target in members {
        match bootstrap::cell_init(providers, rpc, target, members, patience).await {
            InitOutcome::Formed(plan) => {
                formation = Some(plan);
                break;
            }
            InitOutcome::Refused(refusal) => {
                return InitRun::Refused(InitRefusal::Formation(refusal));
            }
            InitOutcome::Malformed => return InitRun::Unreachable(Unreachable::Malformed),
            InitOutcome::Unreachable => return InitRun::Unreachable(Unreachable::Formation),
            // Formed already: the next listed machine may still be idle.
            InitOutcome::NotWaiting => {}
        }
    }
    let (servers, members, journals, users) = match formation {
        Some(plan) => {
            let servers: Vec<(u64, SocketAddr)> = plan
                .members
                .iter()
                .map(|(id, addr)| (id.0, *addr))
                .collect();
            let ids = servers.iter().map(|(id, _)| *id).collect();
            let Some(fleet_control) = plan.fleet else {
                return InitRun::Refused(InitRefusal::NoFleet);
            };
            assert!(
                plan.journals.contains(&fleet_control),
                "the plan serves its fleet journal"
            );
            (servers, ids, plan.control_journals(), plan.users())
        }
        // Every listed machine is formed (a re-run after its formation):
        // learn the cell from them.
        None => match found(providers, rpc, members, &connect, patience).await {
            Ok(cell) => cell,
            Err(unreachable) => return InitRun::Unreachable(unreachable),
        },
    };
    assert!(!servers.is_empty(), "a formed or found cell has servers");
    let client = connect(&servers);
    let coordinator = match coordinator(&client, &journals, patience).await {
        Ok(node) => node,
        Err(unreachable) => return InitRun::Unreachable(unreachable),
    };
    let Some(mut fleet) = FleetSession::new(
        journals,
        params.leader_seed,
        Registry::new(members.iter().copied().map(NodeId)),
        client.tunables().checkpoint_policy(),
    ) else {
        return InitRun::Refused(InitRefusal::NoFleet);
    };
    let run = fleet.init(&client, 0, params.fleet_id, patience).await;
    match run.outcome {
        Step::Done {
            result: fleet_id, ..
        } => {
            let initialized = Initialized {
                fleet_id,
                coordinator,
                members,
                servers,
                journals,
                users,
                steps: run.steps,
            };
            assert_ne!(initialized.fleet_id, 0, "a recorded fleet id is set");
            if initialized.steps.is_empty() {
                InitRun::AlreadyInitialized(initialized)
            } else {
                InitRun::Initialized(initialized)
            }
        }
        Step::Refused(refusal) => InitRun::Refused(InitRefusal::Fleet(refusal)),
        Step::Interrupted(stop) => InitRun::Interrupted(stop),
        Step::Advanced(_) => unreachable!("a run never ends advanced"),
    }
}
