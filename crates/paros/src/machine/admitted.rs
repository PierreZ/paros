//! An admitted machine (#216): a machine that `cell add-machine` admitted
//! into a cell. Its record holds the admission — the cell, its control
//! journals and the cell's machines it knew then — and on every start it
//! finds its cell there, with no configuration and no peer to ask.
//!
//! It runs the node loop of the cell's machines ([`crate::run_journals`],
//! #474) and serves every journal placed on it: the registry books an
//! acceptor slot of the journal on it, and the journal's tenant created it
//! (`crate::driver::system`). It follows the control journals from the
//! founding members its admission names, and its journals are born with
//! their tenant's matchmaker set on the founding members. A `stateless`
//! machine serves no journal. It answers what any machine of a cell
//! answers:
//!
//! - `Identify`, with its cell;
//! - a node-only `Inspect`, with its cell's control journals (§3.2), so a
//!   client handed its address learns the cell from it (the node loop
//!   answers it, and a journal it does not serve is refused as unknown);
//! - `Admit` into its own cell, acked with no change (the caller's retry
//!   after a lost answer), and into any other cell, refused;
//! - the cell decree, refused: it is in a cell, so a `cell init` that lists
//!   it is refused as `cell_exists`, and it accepts no plan;
//! - `Resolve`, from its folds of the registry and the universe directory
//!   (`super::resolve`).
//!
//! On every start it asks the cell coordinator to register the address it
//! advertises now (`super::register`, #349), so a machine that moved is
//! dialed at its new address. It dials the cell's machines where its
//! durable cached registry fold says (#211), else where its admission says,
//! and follows the registry to keep that cache current (`super::follow`).

use std::collections::BTreeSet;

use moonpool_core::{Detach, Providers, SimulationResult, TaskProvider};
use moonpool_rpc::RpcHandle;
use tokio_util::sync::CancellationToken;

use paros_core::NodeId;

use super::{Admission, CacheSink, CachedRegistry, MachineFacts};
use crate::Address;
use crate::driver::DriverTunables;
use crate::rpc::machine as wire;
use crate::rpc::methods::{AdmitRpc, CellInitRpc, FormCellRpc, IdentifyRpc, PrepareCellRpc};
use crate::rpc::{Inbound, serve_well_known};

/// A machine of a cell by admission: what it knows of itself, and the
/// admission its record holds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdmittedMachine {
    /// The machine.
    pub facts: MachineFacts,
    /// Its admission.
    pub admission: Admission,
    /// The cached registry fold its disk held at start (#211), if any.
    pub cached: Option<CachedRegistry>,
}

impl AdmittedMachine {
    /// The cell's machines this one knows, by address: its admission's,
    /// where its cached registry fold dials them (§3.2, #211), and every
    /// other machine the cache names.
    #[must_use]
    pub fn known(&self) -> BTreeSet<Address> {
        self.book().into_iter().map(|(_, addr)| addr).collect()
    }

    /// The cell's machines this one dials at start, by id: see
    /// [`super::starting_book`].
    #[must_use]
    pub fn book(&self) -> Vec<(NodeId, Address)> {
        super::starting_book(&self.admission.members, self.cached.as_ref())
    }

    /// `Admit` at an admitted machine: acked for its own cell, refused for
    /// any other.
    fn admit_ack(&self, request: &wire::Admit) -> wire::AdmitAck {
        let refusal = match Admission::from_wire(request) {
            Ok(admission) if admission.cell == self.admission.cell => "",
            Ok(_) => "other_cell",
            Err(_) => "malformed",
        };
        if refusal.is_empty() {
            moonpool_assertions::reachable!(
                "machine: an admitted machine acks its admission again"
            );
        }
        wire::AdmitAck {
            admitted: refusal.is_empty(),
            refusal: refusal.into(),
        }
    }

    /// Phase 1b of a cell decree at an admitted machine: refused, with its
    /// identity. The proposer refuses the `cell init` as `cell_exists`.
    fn prepare_ack(&self) -> wire::PrepareCellAck {
        wire::PrepareCellAck {
            identity: Some(self.facts.identify_ack(self.admission.cell.cell_id)),
            refusal: "in_cell".into(),
            ..wire::PrepareCellAck::default()
        }
    }

    /// Start what an admitted machine runs beside its answers: the follow
    /// that keeps its cached registry fold current (#211), and its
    /// registration of the address it advertises now (#349).
    fn follow_and_register<P: Providers>(
        &self,
        providers: &P,
        rpc: &moonpool_rpc::RpcHandle<P>,
        cache: CacheSink,
        tunables: &DriverTunables,
        shutdown: &CancellationToken,
    ) {
        let cell = self.admission.cell;
        let book = self.book();
        // It keeps its cached registry fold current (#211).
        super::follow::spawn(
            providers,
            rpc,
            super::follow::Follow {
                facts: self.facts.clone(),
                cell,
                book: book.clone(),
                floor: self.cached.as_ref().map_or(0, |c| c.position),
                sink: cache,
            },
            tunables,
            shutdown.clone(),
        );
        // It asks the cell coordinator to register the address it
        // advertises now (#349): the registry may hold another one.
        if let Some(election) = cell.election {
            super::register::spawn(
                providers,
                rpc,
                super::register::Registration {
                    facts: self.facts.clone(),
                    cell_id: cell.cell_id,
                    election,
                    book: super::with_own(&book, self.facts.node_id, &self.facts.addr),
                },
                tunables,
                shutdown.clone(),
            );
        }
    }

    /// Answer the machine contract on `rpc` until `shutdown`, in a task of
    /// its own beside the node loop that serves the journals placed here
    /// (#474), and start the follow and the registration beside it. The
    /// node loop answers `Inspect`.
    ///
    /// # Errors
    ///
    /// An endpoint could not be registered.
    #[tracing::instrument(level = "debug", skip_all, fields(node = self.facts.node_id.0, cell = self.admission.cell.cell_id))]
    pub(crate) fn serve_beside<P: Providers>(
        mut self,
        providers: &P,
        rpc: &RpcHandle<P>,
        tunables: &DriverTunables,
        cache: CacheSink,
        shutdown: CancellationToken,
    ) -> SimulationResult<()> {
        assert!(
            self.admission.check().is_ok(),
            "an admitted machine holds a checked admission"
        );
        assert!(
            self.facts.node_id.0 != 0,
            "an admitted machine has a minted identity"
        );
        self.facts = self.facts.serving(rpc);
        let mut identify = Inbound::plain(serve_well_known::<P, IdentifyRpc>(rpc)?);
        let mut prepare = Inbound::plain(serve_well_known::<P, PrepareCellRpc>(rpc)?);
        let mut form = Inbound::plain(serve_well_known::<P, FormCellRpc>(rpc)?);
        let mut init = Inbound::plain(serve_well_known::<P, CellInitRpc>(rpc)?);
        let mut admit = Inbound::plain(serve_well_known::<P, AdmitRpc>(rpc)?);
        let cell = self.admission.cell;
        // Every machine answers `Load` (#424).
        super::load::serve(providers, rpc, &self.facts, shutdown.clone())?;
        self.follow_and_register(providers, rpc, cache, tunables, &shutdown);
        // Any machine of the cell answers `Resolve` (#216).
        super::resolve::spawn(
            providers,
            rpc,
            super::resolve::Resolver {
                facts: self.facts.clone(),
                cell,
                founders: Some(self.admission.founders.clone()),
                book: self.book(),
                serves: false,
            },
            shutdown.clone(),
        )?;
        tracing::info!(
            machine = %self.facts.name,
            known = self.admission.members.len(),
            "machine_serving_admitted"
        );
        providers
            .task()
            .spawn_task("paros-machine-admitted", async move {
                loop {
                    moonpool_core::select! {
                        biased;
                        () = shutdown.cancelled() => return,
                        Some((_, reply)) = identify.recv() => {
                            reply.send(self.facts.identify_ack(cell.cell_id));
                        }
                        Some((_, reply)) = prepare.recv() => {
                            reply.send(self.prepare_ack());
                        }
                        Some((_, reply)) = form.recv() => {
                            reply.send(wire::FormCellAck {
                                formed: false,
                                refusal: "other_cell".into(),
                                promise: None,
                            });
                        }
                        Some((_, reply)) = init.recv() => {
                            reply.send(wire::CellInitAck {
                                refusal: "cell_exists".into(),
                                ..wire::CellInitAck::default()
                            });
                        }
                        Some((request, reply)) = admit.recv() => {
                            reply.send(self.admit_ack(&request));
                        }
                        else => return,
                    }
                }
            })
            .detach();
        Ok(())
    }
}
