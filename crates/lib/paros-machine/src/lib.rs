//! The machine (#196, #216, #246, #277): a machine's whole lifecycle
//! ([`run_machine`]: format, wait for a cell, serve it), and what an idle
//! `parosd` does until `cell init` forms its cell.
//!
//! Every `parosd` starts the same way: with an identity minted at format —
//! its random `node_id` (#225), so a wiped disk is a new machine and "a wiped
//! identity never rejoins" holds by construction. Its configuration names no
//! cell and no peer. It never forms a cell on its own
//! (`docs/architecture/03-1-cell-tenant-and-bootstrap.md` §3.1): it waits, serving the machine contract
//! (`proto/machine.proto`), until a `cell init` that lists it forms it.
//!
//! **`cell init` is a single-decree Paxos on the cell plan** (decided on
//! 2026-10-09, #277): paros eats its own food, so the one-shot decision is
//! paros-core's [`Decree`](paros_core::decree::Decree) over the shared
//! [`Proposer`](paros_core::proposer::Proposer) and
//! [`Acceptor`](paros_core::acceptor::Acceptor), the matchmaker handover's
//! machinery. The machine that receives `CellInit` is the proposer; every
//! listed machine is an acceptor. All of them must answer the ask, and a
//! majority of accepts chooses the plan (#246), so an initial member wiped
//! midway does not block it:
//!
//! - **`PrepareCell`** (ask, as a reservation) — a fresh random `init_id`,
//!   the ballot. Each machine answers who it is, promises to accept no plan
//!   under a lower ballot, and reports the plan it accepted, if any.
//! - **Adopt or draw.** A reported plan is finished instead of a new one
//!   (P2c): over the same addresses the two `init`s converge on one cell;
//!   over another list the receiver refuses (`other_cell_init`). A listed
//!   address that now hosts another machine than the plan names is a wiped
//!   one: the new machine never accepts the plan as the old one, and the
//!   others choose it while they are a majority (else `cell_lost`). With
//!   nothing reported the receiver draws the plan.
//! - **`FormCell`** (accept) — each machine accepts the plan unless it
//!   promised a higher ballot, and accepting is forming: it formats the
//!   plan's journals and records the plan durably ([`CellLedger::form`]),
//!   then serves them. A `FormCell` with no prior ask is both steps.
//!
//! A receiver that crashes midway leaves no lock: the next `cell init`, sent
//! to any listed machine, finds the accepted plan in its ask and finishes
//! it. A formed machine's vote is final, so it keeps answering both phases
//! from its record while it serves its cell ([`FormedCell`]).
//!
//! The plan is a [`CellPlan`]: the cell's id, its initial members by id and
//! address, and the journals they serve from formation — the cell tenant's
//! control journal ([`CellPlan::control`]), the fleet tenant's control
//! journal ([`CellPlan::fleet`]: the fleet's one cell hosts the fleet tenant
//! in M9, #226), every identifier drawn at `cell init` (no identifier is
//! fixed, `docs/architecture/03-8-identifiers-checkpoints-recovery-security.md` §3.8). A tenant's journals are not in the
//! plan: they live in its own control journal (#210). The plan also names the cell's
//! first election journal ([`CellPlan::election`], #240): the initial
//! members campaign over it for the cell coordinator, which installs itself
//! as the control journal's leader (`coordinator.rs`).
//!
//! Provider-generic like every driver here: `parosd` runs it on a data
//! directory and the simulation on its simulated disk, each machine formed by
//! the workload's `init` (#246). Durability is the caller's ([`MachineDisk`]);
//! the record's write protocol and the formation's stores are
//! [`ProviderDisk`], over any storage provider, which both callers use.

mod admitted;
mod cache;
mod cell_init;
mod disk;
mod follow;
mod formed;
pub mod inspect;
mod lifecycle;
pub mod load;
mod matchmakers;
mod pin;
pub(crate) mod register;
mod resolve;
mod stores;
pub(crate) mod views;
mod wait;

#[cfg(test)]
mod rig;

use std::sync::Arc;

use moonpool_rpc::RpcHandle;
use tokio_util::sync::CancellationToken;

use formed::FormedExt as _;
use paros_client::CallObserver;
use paros_net::runtime::ParosProviders;
use paros_observe::audit::Audit;
use paros_storage::RunError;
use paros_tunables::DriverTunables;

pub use admitted::AdmittedMachine;
pub use cache::{WriterEnd, spawn_writer};
pub use disk::ProviderDisk;
pub(crate) use disk::{read_all, write_all};
pub use formed::{FormationToken, FormedCell};
pub use lifecycle::{MachineAddresses, MachineError, MachineSettings, run_machine};
pub use paros_coordinator::tenant::sets::{SET, set_of};
pub use paros_roles::node::host::{CellHost, Hosted, MachineFacts, incarnation_of};
pub use pin::AdminPin;
pub use stores::AuditScope;
pub use wait::{CellLedger, Joined, wait_for_cell};

pub use paros_model::machine::*;

/// A machine of a cell as its node loop serves it (#474): one `init`
/// formed the cell on, or one `cell add-machine` admitted, with the cache
/// its follow of the registry writes (#211). Both serve journals: a formed
/// one its plan's first configuration and every journal placed on it, an
/// admitted one every journal placed on it.
#[derive(Clone, Debug)]
pub enum CellMachine {
    /// A machine `init` formed the cell on.
    Formed {
        /// The machine.
        cell: FormedCell,
        /// Its cached registry fold's writer: the node loop offers it each
        /// address book, which the decree's teacher reads.
        cache: CacheSink,
    },
    /// An admitted machine.
    Admitted {
        /// The machine.
        machine: AdmittedMachine,
        /// Its cached registry fold's writer.
        cache: CacheSink,
    },
}

impl CellMachine {
    /// The addresses of every machine this one dials at start, in its
    /// book's order: the cell its frontend fronts (#685 (no founders)).
    fn cell_addrs(&self) -> Vec<paros_model::address::Address> {
        let book = match self {
            CellMachine::Formed { cell, .. } => {
                starting_book(&cell.plan.members, cell.cached.as_ref())
            }
            CellMachine::Admitted { machine, .. } => machine.book(),
        };
        let addrs: Vec<paros_model::address::Address> =
            book.iter().map(|(_, addr)| addr.clone()).collect();
        assert_eq!(addrs.len(), book.len(), "one address per machine");
        assert!(
            !addrs.is_empty(),
            "a machine in a cell knows a machine of it"
        );
        addrs
    }

    /// The book this machine's registry fold moves, for its frontend
    /// (`paros_roles::frontend::FrontendSettings::book`): every machine
    /// follows one, whatever it was formed or admitted with (#685 (no
    /// founders)).
    fn frontend_book(
        &self,
    ) -> tokio::sync::watch::Receiver<Option<paros_model::machine::CachedRegistry>> {
        let (CellMachine::Formed { cache, .. } | CellMachine::Admitted { cache, .. }) = self;
        cache.subscribe()
    }
}

impl CellHost for CellMachine {
    fn control_journals(&self) -> ControlJournals {
        match self {
            CellMachine::Formed { cell, .. } => cell.control_journals(),
            CellMachine::Admitted { machine, .. } => machine.admission.cell,
        }
    }

    fn facts(&self) -> &MachineFacts {
        match self {
            CellMachine::Formed { cell, .. } => &cell.facts,
            CellMachine::Admitted { machine, .. } => &machine.facts,
        }
    }

    fn facts_mut(&mut self) -> &mut MachineFacts {
        match self {
            CellMachine::Formed { cell, .. } => &mut cell.facts,
            CellMachine::Admitted { machine, .. } => &mut machine.facts,
        }
    }

    fn formed(&self) -> Option<&FormedCell> {
        match self {
            CellMachine::Formed { cell, .. } => Some(cell),
            CellMachine::Admitted { .. } => None,
        }
    }

    #[allow(clippy::too_many_lines)]
    fn serve<P: ParosProviders, A: Audit + Clone + Send + Sync + 'static>(
        self,
        providers: &P,
        rpc_handle: &RpcHandle<P>,
        tunables: &DriverTunables,
        (observer, frontend_audit): (Option<Arc<dyn CallObserver>>, A),
        (incarnation_shutdown, stopping): (CancellationToken, CancellationToken),
    ) -> Result<Hosted, RunError> {
        let mut hosted = Hosted::default();
        // Every machine given a frontend role serves it on its own edge, in
        // front of its cell's machines (#723 (frontend as a parosd role)):
        // the tenant coordinator books a tenant's frontends on the
        // `stateless` machines and `Resolve` names them, as it does its
        // batchers.
        if let Some(role) = &self.facts().frontend {
            let cell = self.cell_addrs();
            let settings = role.settings(
                (self.facts().listen, self.facts().addr.clone()),
                cell,
                self.facts().names.clone(),
                *tunables,
                Some(self.frontend_book()),
            );
            // It begins its stop with the machine's (`stopping`), and the
            // node loop waits for it to end.
            hosted.frontend_ended = Some(paros_roles::frontend::spawn(
                providers,
                rpc_handle,
                settings,
                (role.authz.clone(), frontend_audit),
                (incarnation_shutdown.clone(), stopping.clone()),
            )?);
        }
        match self {
            CellMachine::Admitted { machine, cache } => {
                hosted.offered_books = Some(cache.subscribe());
                // A storage machine campaigns for the cell coordinator once
                // the election journal grows onto it (#685 (special tenants
                // are tenants) step 5): the candidacy waits until then.
                hosted.coordinator_ended = serve_candidate(
                    &machine,
                    (providers, rpc_handle, tunables),
                    observer.clone(),
                    (incarnation_shutdown.clone(), stopping.clone()),
                )?;
                // An admitted machine answers its admission beside the journals
                // placed on it (#474), follows the registry for its cache (#211)
                // and registers the address it advertises now (#349).
                machine.serve_beside(
                    providers,
                    rpc_handle,
                    tunables,
                    cache,
                    observer,
                    incarnation_shutdown,
                )?;
            }
            CellMachine::Formed {
                cell: formed,
                cache,
            } => {
                // The machine answers the cell's views (#399), as an admitted
                // one does once it may campaign.
                views::spawn(
                    providers,
                    rpc_handle,
                    &formed,
                    tunables,
                    incarnation_shutdown.clone(),
                )?;
                // It asks the coordinator to register the address it advertises
                // now (#349): the cell may know another one.
                if let Some(election) = formed
                    .plan
                    .election
                    .is_set()
                    .then_some(formed.plan.election)
                {
                    // A machine that came back at another address holds its
                    // journals until its registration ends (#390). The one
                    // exception is about the first configuration itself: a
                    // plan of one member. At boot the machine knows only its
                    // plan, not the configuration in force, because its
                    // journals are not open yet. The plan's lone member is the
                    // whole first configuration of the election journal, so a
                    // hold could stop the only campaign that elects the
                    // coordinator its registration asks. Its plan journals may
                    // have grown past it (#851 (grow the cell plan journals)),
                    // but its peers dial it where its own batches say, so its
                    // campaign is what heals it.
                    let lone_plan = formed.plan.members.len() == 1;
                    let moved = !lone_plan
                        && formed.initial_members().iter().any(|(id, addr)| {
                            *id == formed.facts.node_id && *addr != formed.facts.addr
                        });
                    let ended = register::spawn(
                        providers,
                        rpc_handle,
                        register::Registration {
                            facts: formed.facts.serving(rpc_handle),
                            cell_id: formed.plan.cell_id,
                            election,
                            book: with_own(
                                &formed.initial_members(),
                                formed.facts.node_id,
                                &formed.facts.addr,
                            ),
                        },
                        tunables,
                        incarnation_shutdown.clone(),
                    );
                    hosted.unregistered = moved.then_some(ended);
                }
                // The machine runs a candidacy for the cell coordinator (#240),
                // which campaigns while it holds a slot of the election journal
                // (the plan's members until a slot is booked); a moved one
                // campaigns once its registration ended (#506 (renamed founder
                // not registered)).
                hosted.coordinator_ended = Some(paros_coordinator::cell::spawn(
                    providers,
                    rpc_handle,
                    (
                        &formed,
                        formed.plan.members.iter().map(|(id, _)| *id).collect(),
                    ),
                    tunables,
                    observer.clone(),
                    hosted.unregistered.clone(),
                    (incarnation_shutdown.clone(), stopping),
                ));
                // A book handed to a stuck member reaches its follower (#897).
                let (adopted, offered) = tokio::sync::watch::channel(None);
                hosted.offered_books = Some(offered);
                formed.serve(
                    providers,
                    rpc_handle,
                    tunables,
                    (cache.subscribe(), adopted),
                    observer,
                    incarnation_shutdown,
                )?;
            }
        }
        Ok(hosted)
    }
}

/// The cell roles of the admitted machine `machine` once it may campaign
/// (#685 (special tenants are tenants) step 5): its candidacy for the cell
/// coordinator, which waits until the election journal grows onto it, and
/// the cell's views, since the initial members may all leave. The
/// candidacy's end, or `None` on a machine with no candidate plan.
///
/// # Errors
///
/// The `View` endpoint could not be registered.
fn serve_candidate<P: ParosProviders>(
    machine: &admitted::AdmittedMachine,
    (providers, rpc_handle, tunables): (&P, &RpcHandle<P>, &DriverTunables),
    observer: Option<Arc<dyn CallObserver>>,
    (shutdown, stopping): (CancellationToken, CancellationToken),
) -> Result<Option<tokio::sync::watch::Receiver<bool>>, RunError> {
    let Some(formed) = candidate_plan(machine) else {
        return Ok(None);
    };
    assert!(
        !formed.plan.members.is_empty(),
        "a candidate knows an initial member"
    );
    views::spawn(providers, rpc_handle, &formed, tunables, shutdown.clone())?;
    Ok(Some(paros_coordinator::cell::spawn(
        providers,
        rpc_handle,
        (&formed, machine.admission.initial_members.clone()),
        tunables,
        observer,
        None,
        (shutdown, stopping),
    )))
}

/// The cell an admitted `machine` campaigns in (#685 (special tenants are
/// tenants) step 5): its admission's cell and control journals, over the
/// initial members where its book dials them. An initial member the book does
/// not name is left out of the plan: the admission names no address for an
/// initial member its operator did not reach (a wiped one), and it stays a
/// candidate by id through the genesis pool the candidacy is given. `None` for
/// a machine that stores nothing, a cell with no election journal known, or no
/// initial member with a known address (witness: recipe 10537902676925133085
/// 851411:16249735473007141123 on d5f5d47, where an initial member wiped before
/// the cell formed kept the one slot holder left from campaigning, and the
/// election never settled).
fn candidate_plan(
    machine: &admitted::AdmittedMachine,
) -> Option<paros_roles::node::host::FormedCell> {
    let admission = &machine.admission;
    let election = admission.cell.election?;
    if !machine.facts.class.stores() || admission.initial_members.is_empty() {
        return None;
    }
    let book = machine.book();
    let members: Vec<_> = admission
        .initial_members
        .iter()
        .filter_map(|id| book.iter().find(|(known, _)| known == id).cloned())
        .collect();
    if members.is_empty() {
        return None;
    }
    let journals: std::collections::BTreeSet<_> = std::iter::once(admission.cell.cell)
        .chain(std::iter::once(election))
        .chain(admission.cell.fleet)
        .collect();
    let plan = paros_model::machine::CellPlan {
        cell_id: admission.cell.cell_id,
        members,
        control: admission.cell.cell,
        fleet: admission.cell.fleet,
        election,
        journals: journals.into_iter().collect(),
    };
    if plan.check().is_err() {
        return None;
    }
    assert!(
        !admission.initial_members.contains(&machine.facts.node_id),
        "an admitted machine is not an initial member"
    );
    Some(paros_roles::node::host::FormedCell {
        facts: machine.facts.clone(),
        plan,
        ballot: paros_core::types::Ballot::default(),
        cached: machine.cached.clone(),
        token: None,
    })
}

#[cfg(test)]
mod tests {
    use paros_core::NodeId;

    use super::admitted::AdmittedMachine;
    use super::candidate_plan;
    use crate::rig::{facts, machine, machine_addr, plan};
    use crate::{Admission, Class};

    /// Machine 3, admitted into cell 9 formed by machines 1 and 2, whose
    /// admission names an address for the initial members in `known` only.
    fn admitted(known: Vec<(NodeId, paros_model::address::Address)>) -> AdmittedMachine {
        let mut members = known;
        members.push(machine(3));
        members.sort_unstable();
        AdmittedMachine {
            facts: facts(NodeId(3), machine_addr(3), Class::All),
            admission: Admission {
                cell: plan(9, vec![machine(1), machine(2)]).control_journals(),
                members,
                initial_members: vec![NodeId(1), NodeId(2)],
            },
            cached: None,
        }
    }

    #[test]
    fn an_initial_member_with_no_address_known_is_left_out_of_the_candidate_plan() {
        // Every initial member known: the plan names them all.
        let both = candidate_plan(&admitted(vec![machine(1), machine(2)])).expect("a plan");
        assert_eq!(both.plan.members, vec![machine(1), machine(2)]);
        // An initial member the admission did not reach (a wiped one) is left
        // out, and the machine still campaigns.
        let one = candidate_plan(&admitted(vec![machine(2)])).expect("a plan");
        assert_eq!(one.plan.members, vec![machine(2)]);
        assert_eq!(one.plan.cell_id, 9);
        // The negative space: no initial member known, no plan.
        assert!(candidate_plan(&admitted(Vec::new())).is_none());
    }
}
