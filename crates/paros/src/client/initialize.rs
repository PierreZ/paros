//! `init` whole (#196, #229, #246, #277): the cell step, then the fleet
//! steps, as one resumable operation. `parosctl init` prints what it comes
//! to, and the simulation's workload runs the very same code.
//!
//! The cell step is `cell init` over the founding members: sent to the
//! first listed machine still idle, which drives the cell decree over every
//! listed machine ([`crate::machine`]); a formed one has no `CellInit` and
//! the next is asked, and so is one `cell add-machine` admitted (#423), which
//! refuses it as `cell_exists`. Then `init` waits for the cell's first coordinator:
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
//! formed, a re-run learns them from the members' `Inspect`. The run writes
//! the fleet tenant's journal under its own leader uuid (#241). It never
//! claims the cell control journal: the cell coordinator writes the cell's
//! side of the join, and the run waits for it (#513 (fleet Init duel)), so
//! `init` never fences the coordinator. Every step is idempotent: `init` is
//! refused only when it found nothing left to do.

use std::time::Duration;

use moonpool_core::Providers;
use moonpool_rpc::RpcHandle;
use paros_core::NodeId;

use super::Client;
use super::bootstrap::{self, InitOutcome};
use super::cell::CellSession;
use super::checkpoint::{self, Folder, LoadOutcome};
use super::election::read_election;
use super::fleet::{FleetRefusal, FleetSession, Interrupted, Stage, Step};
use crate::fleet::RootKeyEntry;
use crate::machine::ControlJournals;
use crate::system::Registry;
use crate::tenant::Durability;
use crate::{Address, Names};

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
    pub servers: Vec<(u64, Address)>,
    /// The listed machines past the founding members, admitted into the
    /// cell as `cell add-machine` admits a machine: `(node id, address)`,
    /// in listed order.
    pub admitted: Vec<(u64, Address)>,
    /// The control journals, learned from the plan or `Inspect`.
    pub journals: ControlJournals,
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
    /// Nothing answered `CellInit`, or no cell answered `Inspect` from a
    /// majority of the founding members.
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
#[derive(Clone, Debug)]
pub struct InitParams {
    /// How long the decree may take to form the cell, the cell to elect its
    /// first leader, and an interrupted fleet step to be taken again.
    pub patience: Duration,
    /// The fleet id a first run records; a re-run keeps the recorded one.
    pub fleet_id: u64,
    /// The seed of this run's leader uuids ([`super::Writer::new`]): every
    /// run draws its own.
    pub leader_seed: u128,
    /// The universe's name and the cell's, for people (#252, #399): kept
    /// by the first run that writes them.
    pub names: (String, String),
    /// The operator's `admin` token, sent with `CellInit`: a founding
    /// member with a pinned root key checks it (#245 (Biscuit tokens)).
    pub token: Vec<u8>,
    /// The universe's first root public key, written into the universe
    /// entry while it holds none (#245).
    pub root_key: Option<RootKeyEntry>,
    /// `F` of the cell's control journals (#523 (redundancy naming)): the
    /// first `2F+1` listed machines are the founding members, so the cell
    /// and tenant control journals have `2F+1` acceptors and survive `F`
    /// failures. `init` admits every other listed machine. The default is
    /// [`FAILURES_PER_PARTITION`].
    pub failures_per_partition: u32,
}

/// The default `F` of the cell's control journals: three founding members,
/// so the cell and tenant control journals have three acceptors however
/// many machines `init` lists (decided on 2026-10-15).
pub const FAILURES_PER_PARTITION: u32 = Durability::DEFAULT.failures_per_partition;

/// How many of `listed` machines found the cell for `failures_per_partition`:
/// `2F+1`, or every listed machine when fewer are listed.
///
/// # Panics
///
/// Never: the count is at most `listed`, and at least one of them.
#[must_use]
pub fn founding_members(listed: usize, failures_per_partition: u32) -> usize {
    let wanted = Durability {
        failures_per_partition,
        partitions: 1,
    }
    .acceptors_per_partition();
    let founding = listed.min(wanted);
    assert!(
        founding <= listed,
        "the founding members are listed machines"
    );
    assert!(
        listed == 0 || founding >= 1,
        "a listed machine founds the cell"
    );
    founding
}

/// What a found cell is: its servers, its members and its control journals.
type Found = (Vec<(u64, Address)>, Vec<u64>, ControlJournals);

/// A cell an earlier run formed, learned from its servers: the cell a
/// majority of the founding members serve, with its control journals, from
/// their node-only `Inspect` (no identifier is fixed, §3.8; an address a
/// wiped member left may host another cell's machine, #216), and its
/// members from the cell control journal's. The fleet steps fold the cell
/// control journal over its genesis pool, the cell's members — the pool the
/// run that formed the cell folds it over — never over whichever servers
/// answered this time (not reproduced in the simulation: a fold's pool must
/// not depend on who was up).
async fn found<P: Providers>(
    providers: &P,
    rpc: &RpcHandle<P>,
    names: &Names,
    addrs: &[Address],
    connect: &impl Fn(&[(u64, Address)]) -> Client<P>,
    patience: Duration,
) -> Result<Found, Unreachable> {
    let (journals, servers) = bootstrap::majority_cell(providers, rpc, names, addrs, patience)
        .await
        .ok_or(Unreachable::NothingAnswered)?;
    let servers: Vec<(u64, Address)> = servers
        .into_iter()
        .map(|server| (server.id, server.address))
        .collect();
    if journals.fleet.is_none() {
        return Err(Unreachable::NoControlJournals);
    }
    let client = connect(&servers);
    // The genesis pool is the cell's members, never only the servers that
    // answered: one of them may be down. A server that answered may also
    // be a machine `cell add-machine` admitted (#423), which serves no
    // journal yet and refuses the `Inspect`: the next server is asked.
    let mut members = Vec::new();
    for target in 0..servers.len() {
        if let Some(view) = client.inspect(target, journals.cell).await
            && !view.members.is_empty()
        {
            if target > 0 {
                moonpool_assertions::reachable!(
                    "init: a found cell's members come from a server past the first"
                );
            }
            members = view.members;
            break;
        }
    }
    members.sort_unstable();
    members.dedup();
    if members.is_empty() {
        return Err(Unreachable::NoMembers);
    }
    Ok((servers, members, journals))
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

/// Wait up to `patience` until the registry holds every founding member
/// (#476 (tenant footprint)). A founding member registers through the cell
/// coordinator (`Register`), and the fleet steps claim the cell control
/// journal, which fences the coordinator: the cell's and the universe's
/// footprints are booked over registered machines only. The wait reads the
/// journal and claims nothing. After `patience` the fleet steps go on: the
/// coordinator books what they could not.
async fn registered<P: Providers>(
    client: &Client<P>,
    journals: &ControlJournals,
    founders: &[u64],
    patience: Duration,
) {
    assert!(!founders.is_empty(), "a cell has founding members");
    let deadline = client.now() + patience;
    loop {
        let mut folder = Folder::new(Registry::new(founders.iter().copied().map(NodeId)));
        if let LoadOutcome::Loaded { .. } =
            checkpoint::load(&mut folder, journals.cell, client, 0, 0).await
            && crate::footprint::ready(folder.state())
        {
            return;
        }
        if client.now() >= deadline || !client.pause(client.tunables().retry_backoff).await {
            moonpool_assertions::reachable!("init: a founding member is not registered in time");
            return;
        }
    }
}

/// Run `init` whole: form the cell over the founding members (or learn it,
/// when every member already serves it), wait for its coordinator, run the
/// fleet steps, then admit the other listed machines, through a client
/// `connect` builds over the cell's servers. `listed` are the machines'
/// advertised addresses (#257), each resolved through `names` as it is
/// dialed. The founding members are the first `2F+1` listed
/// ([`founding_members`], `F` from [`InitParams::failures_per_partition`]):
/// a re-run lists the same machines in the same order.
///
/// # Panics
///
/// If the fleet half ends advanced (a broken [`FleetSession::init`]
/// contract), or `params` carries an unset fleet id.
// The steps in order: formation, coordinator, fleet steps, admissions.
#[allow(clippy::too_many_lines)]
#[tracing::instrument(level = "debug", skip_all, fields(listed = listed.len()))]
pub async fn initialize<P: Providers>(
    providers: &P,
    rpc: &RpcHandle<P>,
    names: &Names,
    listed: &[Address],
    connect: impl Fn(&[(u64, Address)]) -> Client<P>,
    params: InitParams,
) -> InitRun {
    assert_ne!(params.fleet_id, 0, "a fleet id is never unset");
    if listed.is_empty() {
        return InitRun::Unreachable(Unreachable::NoTarget);
    }
    let (members, others) = listed.split_at(founding_members(
        listed.len(),
        params.failures_per_partition,
    ));
    assert!(!members.is_empty(), "a listed machine founds the cell");
    if !others.is_empty() {
        moonpool_assertions::reachable!("init: more machines are listed than found the cell");
    }
    let patience = params.patience;
    let mut formation = None;
    for target in members {
        let token = &params.token;
        match bootstrap::cell_init(providers, rpc, names, target, members, token, patience).await {
            InitOutcome::Formed(plan) => {
                formation = Some(plan);
                break;
            }
            // A machine `cell add-machine` admitted (#216) at a listed
            // address: a wiped member's replacement, in a cell already, like
            // a formed one (#423). The next listed machine may still be idle,
            // and `found` names the cell a majority of the members serve.
            InitOutcome::Refused(refusal) if refusal == "cell_exists" => {
                moonpool_assertions::reachable!(
                    "init: a re-run passes an admitted machine at a listed address"
                );
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
    let (servers, members, journals) = match formation {
        Some(plan) => {
            let servers: Vec<(u64, Address)> = plan
                .members
                .iter()
                .map(|(id, addr)| (id.0, addr.clone()))
                .collect();
            let ids = servers.iter().map(|(id, _)| *id).collect();
            let Some(fleet_control) = plan.fleet else {
                return InitRun::Refused(InitRefusal::NoFleet);
            };
            assert!(
                plan.journals.contains(&fleet_control),
                "the plan serves its fleet journal"
            );
            (servers, ids, plan.control_journals())
        }
        // Every listed machine is formed (a re-run after its formation):
        // learn the cell from them.
        None => match found(providers, rpc, names, members, &connect, patience).await {
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
    registered(&client, &journals, &members, patience).await;
    let Some(fleet) = FleetSession::new(
        journals,
        params.leader_seed,
        Registry::new(members.iter().copied().map(NodeId)),
        client.tunables().checkpoint_policy(),
    ) else {
        return InitRun::Refused(InitRefusal::NoFleet);
    };
    let mut fleet = fleet.with_names(params.names.0.as_bytes(), params.names.1.as_bytes());
    if let Some(key) = params.root_key {
        fleet = fleet.with_root_key(key);
    }
    let run = fleet.init(&client, 0, params.fleet_id, patience).await;
    match run.outcome {
        Step::Done {
            result: fleet_id, ..
        } => {
            let mut steps = run.steps;
            let admission = Admission {
                journals,
                members: &members,
                servers: &servers,
                seed: params.leader_seed.rotate_left(64),
                token: params.token,
                patience,
            };
            let admitted = match admission
                .admit(providers, rpc, names, &client, others, &mut steps)
                .await
            {
                Step::Done { result, .. } => result,
                Step::Refused(refusal) => return InitRun::Refused(InitRefusal::Fleet(refusal)),
                Step::Interrupted(stop) => return InitRun::Interrupted(stop),
                Step::Advanced(_) => unreachable!("a run never ends advanced"),
            };
            let initialized = Initialized {
                fleet_id,
                coordinator,
                members,
                servers,
                admitted,
                journals,
                steps,
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

/// `init`'s last half: the listed machines past the founding members join
/// the formed cell as `cell add-machine` admits a machine (#216), so the
/// cell and tenant control journals keep their `2F+1` founding acceptors
/// however many machines are listed.
struct Admission<'a> {
    journals: ControlJournals,
    members: &'a [u64],
    servers: &'a [(u64, Address)],
    /// The seed of the admissions' leader uuids on the cell control
    /// journal, apart from the fleet steps' seed.
    seed: u128,
    token: Vec<u8>,
    patience: Duration,
}

impl Admission<'_> {
    /// Admit each of `others` in listed order, appending the steps written
    /// to `steps`. An admitted machine is admitted again unchanged, so a
    /// re-run resumes. Ends with the admitted machines, or with what
    /// stopped an admission.
    async fn admit<P: Providers>(
        self,
        providers: &P,
        rpc: &RpcHandle<P>,
        names: &Names,
        client: &Client<P>,
        others: &[Address],
        steps: &mut Vec<Stage>,
    ) -> Step<Vec<(u64, Address)>> {
        if others.is_empty() {
            return Step::Done {
                result: Vec::new(),
                last: None,
            };
        }
        let members: Vec<NodeId> = self.members.iter().copied().map(NodeId).collect();
        let founders = self
            .servers
            .iter()
            .map(|(id, addr)| (NodeId(*id), addr.clone()))
            .collect();
        let mut session = CellSession::new(
            self.journals,
            &members,
            founders,
            self.seed,
            client.tunables().checkpoint_policy(),
        )
        .with_token(self.token);
        let mut admitted = Vec::with_capacity(others.len());
        for target in others {
            let run = session
                .add_machine(providers, rpc, client, names, 0, target, self.patience)
                .await;
            steps.extend(run.steps);
            match run.outcome {
                Step::Done { result, .. } => admitted.push((result.0, target.clone())),
                Step::Refused(refusal) => return Step::Refused(refusal),
                Step::Interrupted(stop) => return Step::Interrupted(stop),
                Step::Advanced(_) => unreachable!("a run never ends advanced"),
            }
        }
        moonpool_assertions::reachable!("init: admits the listed machines past the founders");
        assert_eq!(
            admitted.len(),
            others.len(),
            "init admits every other listed machine"
        );
        Step::Done {
            result: admitted,
            last: steps.last().copied(),
        }
    }
}
