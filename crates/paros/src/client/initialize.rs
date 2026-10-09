//! `init` whole (#196, #229, #246): the cell step, then the fleet steps, as
//! one resumable operation. `parosctl init` prints what it comes to, and the
//! simulation's workload runs the very same code.
//!
//! Sent to the first address — a waiting seed every seed's join list names
//! — which identifies every seed, mints the cell's id and forms every seed
//! ([`crate::machine`]); then the first cell coordinator claims the cell
//! control journal with `SetLeader(expected_gen = 0)`
//! ([`super::bootstrap::claim_cell`]). A re-run resumes: a seed that already
//! serves the cell is asked for it, and the claim is made if it is still
//! missing.
//!
//! Then the fleet steps ([`super::fleet`]): the fleet tenant (served by the
//! cell's seeds) records the fleet's id — drawn by the caller, kept on a
//! re-run — and adds the cell with its cell tenant, the cell records the
//! fleet on its side, and the fleet tenant marks the cell `READY`. No
//! identifier is fixed (§3.8): a first run takes them from the plan it
//! formed, a re-run learns them from the seeds' `Inspect`. Both journals are
//! written as the cell coordinator. Every step is idempotent: `init` is
//! refused only when it found nothing left to do.

use std::net::SocketAddr;
use std::time::Duration;

use moonpool_core::Providers;
use moonpool_rpc::RpcHandle;
use paros_core::{JournalIdentifier, NodeId};

use super::Client;
use super::bootstrap::{self, ClaimCellOutcome, InitOutcome};
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
    /// The claim's answer never came: it may have won; run it again.
    Ambiguous,
    /// A fleet step's write did not land: run it again, it resumes.
    Interrupted(Interrupted),
}

/// What an initialized fleet is, as one `init` saw it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Initialized {
    /// The fleet's id.
    pub fleet_id: u64,
    /// The cell coordinator, which wrote both control journals.
    pub coordinator: NodeId,
    /// The cell's servers: `(node id, address)`.
    pub servers: Vec<(u64, SocketAddr)>,
    /// The control journals, learned from the plan or `Inspect`.
    pub journals: ControlJournals,
    /// The static assignment's user journals, known only to the run that
    /// formed the cell (the only time they are printed).
    pub users: Vec<JournalIdentifier>,
    /// The generation this run claimed the cell control journal at, if it
    /// did.
    pub claimed: Option<u64>,
    /// The fleet steps this run wrote, in order.
    pub steps: Vec<Stage>,
}

/// Why `init` cannot go on.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InitRefusal {
    /// The seed refused the formation: its label (see [`InitOutcome::Refused`]).
    Formation(String),
    /// The formed cell hosts no fleet tenant.
    NoFleet,
    /// A fleet step refused.
    Fleet(FleetRefusal),
}

/// What `init` waited on in vain.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Unreachable {
    /// No address was given.
    NoTarget,
    /// The seed answered with a plan that does not decode.
    Malformed,
    /// The formation decided nothing in time: a seed is not up yet.
    Formation,
    /// Nothing answered `Init` or `Inspect`.
    NothingAnswered,
    /// No server named its cell's control journals.
    NoControlJournals,
    /// No server described the cell control journal, or it names no member.
    NoCoordinator,
    /// The cell did not confirm its control journal in time.
    Claim,
}

/// The caller's half of `init`: how long a step may take, and the fleet id
/// a first run records (drawn by the caller: `paros::client` draws no
/// randomness).
#[derive(Clone, Copy, Debug)]
pub struct InitParams {
    /// How long the seed may take to form the cell, the cell to elect its
    /// first leader, and an interrupted fleet step to be taken again.
    pub patience: Duration,
    /// The fleet id a first run records; a re-run keeps the recorded one.
    pub fleet_id: u64,
}

/// Run `init` whole: form the cell at `addrs[0]` (or learn it, when that
/// seed already serves one), claim its control journal, then run the fleet
/// steps, through a client `connect` builds over the cell's servers.
///
/// # Panics
///
/// If the fleet half ends advanced (a broken [`FleetSession::init`]
/// contract), or `params` carries an unset fleet id.
#[tracing::instrument(level = "debug", skip_all, fields(addrs = addrs.len()))]
pub async fn initialize<P: Providers>(
    providers: &P,
    rpc: &RpcHandle<P>,
    addrs: &[SocketAddr],
    connect: impl Fn(&[(u64, SocketAddr)]) -> Client<P>,
    params: InitParams,
) -> InitRun {
    assert_ne!(params.fleet_id, 0, "a fleet id is never unset");
    let Some(&target) = addrs.first() else {
        return InitRun::Unreachable(Unreachable::NoTarget);
    };
    let patience = params.patience;
    let (servers, coordinator, journals, users) =
        match bootstrap::init(providers, rpc, target, patience).await {
            InitOutcome::Formed(plan) => {
                let servers: Vec<(u64, SocketAddr)> = plan
                    .members
                    .iter()
                    .map(|(id, addr)| (id.0, *addr))
                    .collect();
                let Some(fleet_control) = plan.fleet else {
                    return InitRun::Refused(InitRefusal::NoFleet);
                };
                let users: Vec<JournalIdentifier> = plan
                    .journals
                    .iter()
                    .copied()
                    .filter(|j| *j != plan.control && *j != fleet_control)
                    .collect();
                (servers, plan.coordinator(), plan.control_journals(), users)
            }
            InitOutcome::Refused(refusal) => {
                return InitRun::Refused(InitRefusal::Formation(refusal));
            }
            InitOutcome::Malformed => return InitRun::Unreachable(Unreachable::Malformed),
            InitOutcome::Unreachable => return InitRun::Unreachable(Unreachable::Formation),
            // No machine endpoint: the target serves a cell already (a re-run
            // after its formation), or nothing listens there.
            InitOutcome::NotWaiting => {
                let servers = bootstrap::discover(providers, rpc, addrs, patience).await;
                if servers.is_empty() {
                    return InitRun::Unreachable(Unreachable::NothingAnswered);
                }
                let client = connect(&servers);
                // No identifier is fixed (§3.8): the cell's are learned from it.
                let Some(journals) = bootstrap::control_journals(&client).await else {
                    return InitRun::Unreachable(Unreachable::NoControlJournals);
                };
                let Some(coordinator) = client
                    .inspect(0, journals.cell)
                    .await
                    .and_then(|view| view.members.iter().copied().min())
                else {
                    return InitRun::Unreachable(Unreachable::NoCoordinator);
                };
                (servers, NodeId(coordinator), journals, Vec::new())
            }
        };
    assert!(!servers.is_empty(), "a formed or found cell has servers");
    let client = connect(&servers);
    let claimed = match bootstrap::claim_cell(&client, journals.cell, coordinator, patience).await {
        ClaimCellOutcome::Claimed { generation } => Some(generation),
        // Claimed by an earlier run: the fleet steps resume, and decide
        // whether anything was left to do.
        ClaimCellOutcome::AlreadyInitialized { .. } => None,
        ClaimCellOutcome::Unavailable => return InitRun::Unreachable(Unreachable::Claim),
        ClaimCellOutcome::Ambiguous => return InitRun::Ambiguous,
    };
    let ids: Vec<u64> = servers.iter().map(|(id, _)| *id).collect();
    let Some(mut fleet) = FleetSession::new(
        journals,
        coordinator.0,
        coordinator,
        Registry::new(ids.iter().copied().map(NodeId)),
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
                servers,
                journals,
                users,
                claimed,
                steps: run.steps,
            };
            assert_ne!(initialized.fleet_id, 0, "a recorded fleet id is set");
            if initialized.claimed.is_none() && initialized.steps.is_empty() {
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
