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
//! majority of accepts chooses the plan (#246), so a founding member wiped
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
//! The plan is a [`CellPlan`]: the cell's id, its founding members by id and
//! address, and the journals they serve from formation — the cell tenant's
//! control journal ([`CellPlan::control`]), the fleet tenant's control
//! journal ([`CellPlan::fleet`]: the fleet's one cell hosts the fleet tenant
//! in M9, #226), every identifier drawn at `cell init` (no identifier is
//! fixed, `docs/architecture/03-8-identifiers-checkpoints-recovery-security.md` §3.8). A tenant's journals are not in the
//! plan: they live in its own control journal (#210). The plan also names the cell's
//! first election journal ([`CellPlan::election`], #240): the founding
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

/// A machine of a cell as its node loop serves it (#474): a founding
/// member, or a machine `cell add-machine` admitted, with the cache its
/// follow of the registry writes (#211). Both serve journals: a founding
/// member its plan's and every journal placed on it, an admitted one every
/// journal placed on it.
#[derive(Clone, Debug)]
pub enum CellMachine {
    /// A founding member.
    Founded {
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
    /// The founding members' addresses, where this machine dials them: the
    /// cell its frontend fronts. An admitted machine that knows no founding
    /// member's address fronts every machine it knows.
    fn founder_addrs(&self) -> Vec<paros_model::address::Address> {
        let addrs: Vec<paros_model::address::Address> = match self {
            CellMachine::Founded { cell, .. } => {
                cell.founders().into_iter().map(|(_, addr)| addr).collect()
            }
            CellMachine::Admitted { machine, .. } => {
                let book = machine.book();
                let founders: Vec<_> = book
                    .iter()
                    .filter(|(id, _)| machine.admission.founders.contains(id))
                    .map(|(_, addr)| addr.clone())
                    .collect();
                if founders.is_empty() {
                    book.into_iter().map(|(_, addr)| addr).collect()
                } else {
                    founders
                }
            }
        };
        assert!(
            !addrs.is_empty(),
            "a machine in a cell knows a machine of it"
        );
        addrs
    }
}

impl CellHost for CellMachine {
    fn control_journals(&self) -> ControlJournals {
        match self {
            CellMachine::Founded { cell, .. } => cell.control_journals(),
            CellMachine::Admitted { machine, .. } => machine.admission.cell,
        }
    }

    fn facts(&self) -> &MachineFacts {
        match self {
            CellMachine::Founded { cell, .. } => &cell.facts,
            CellMachine::Admitted { machine, .. } => &machine.facts,
        }
    }

    fn facts_mut(&mut self) -> &mut MachineFacts {
        match self {
            CellMachine::Founded { cell, .. } => &mut cell.facts,
            CellMachine::Admitted { machine, .. } => &mut machine.facts,
        }
    }

    fn formed(&self) -> Option<&FormedCell> {
        match self {
            CellMachine::Founded { cell, .. } => Some(cell),
            CellMachine::Admitted { .. } => None,
        }
    }

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
        // front of the founding members (#723 (frontend as a parosd role)):
        // the tenant coordinator books a tenant's frontends on the
        // `stateless` machines and `Resolve` names them, as it does its
        // batchers.
        if let Some(role) = &self.facts().frontend {
            let cell = self.founder_addrs();
            let settings = role.settings(
                (self.facts().listen, self.facts().addr.clone()),
                cell,
                self.facts().names.clone(),
                *tunables,
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
            CellMachine::Founded {
                cell: formed,
                cache,
            } => {
                // Every founding member answers the cell's views (#399).
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
                    // A lone founding member is its own quorum: it needs no peer.
                    let moved = formed.plan.members.len() > 1
                        && formed.founders().iter().any(|(id, addr)| {
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
                                &formed.founders(),
                                formed.facts.node_id,
                                &formed.facts.addr,
                            ),
                        },
                        tunables,
                        incarnation_shutdown.clone(),
                    );
                    hosted.unregistered = moved.then_some(ended);
                }
                // Every founding member campaigns for the cell coordinator (#240):
                // a moved one once its registration ended (#506 (renamed founder
                // not registered)).
                hosted.coordinator_ended = Some(paros_coordinator::cell::spawn(
                    providers,
                    rpc_handle,
                    &formed,
                    tunables,
                    observer.clone(),
                    hosted.unregistered.clone(),
                    (incarnation_shutdown.clone(), stopping),
                ));
                formed.serve(
                    providers,
                    rpc_handle,
                    tunables,
                    cache.subscribe(),
                    observer,
                    incarnation_shutdown,
                )?;
            }
        }
        Ok(hosted)
    }
}
