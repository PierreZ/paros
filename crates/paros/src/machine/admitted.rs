//! An admitted machine (#216): a machine that `cell add-machine` admitted
//! into a cell. Its record holds the admission — the cell, its control
//! journals and the cell's machines it knew then — and on every start it
//! finds its cell there, with no configuration and no peer to ask.
//!
//! It serves no journal until placement gives it a role (#211, #212). It
//! answers what any machine of a cell answers:
//!
//! - `Identify`, with its cell;
//! - a node-only `Inspect`, with its cell's control journals (§3.2), so a
//!   client handed its address learns the cell from it; a journal it does
//!   not serve is refused as unknown;
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

use moonpool_core::Providers;
use tokio_util::sync::CancellationToken;

use paros_core::NodeId;

use super::{Admission, CacheSink, CachedRegistry, MachineFacts};
use crate::Address;
use crate::driver::edge::RpcEdge;
use crate::driver::{DriverTunables, RunError};
use crate::rpc::machine as wire;
use crate::rpc::methods::{
    AdmitRpc, CellInitRpc, FormCellRpc, IdentifyRpc, InspectRpc, PrepareCellRpc,
};
use crate::rpc::{Inbound, InspectRefusal, InspectTarget, serve_well_known};

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

    /// Serve the machine contract and a node-only `Inspect` at the
    /// machine's listen address until `shutdown`.
    ///
    /// # Errors
    ///
    /// The listener could not bind, or the runtime failed, as
    /// [`RunError::Infra`].
    #[tracing::instrument(level = "debug", skip_all, fields(node = self.facts.node_id.0, cell = self.admission.cell.cell_id))]
    pub async fn serve<P: Providers>(
        mut self,
        providers: &P,
        tunables: &DriverTunables,
        cache: CacheSink,
        shutdown: CancellationToken,
    ) -> Result<(), RunError> {
        assert!(
            self.admission.check().is_ok(),
            "an admitted machine holds a checked admission"
        );
        assert!(
            self.facts.node_id.0 != 0,
            "an admitted machine has a minted identity"
        );
        let addr = self.facts.listen.to_string();
        let mut edge = RpcEdge::listen(providers, &addr, "machine", tunables)
            .await
            .map_err(RunError::Infra)?;
        let rpc = edge.handle().clone();
        self.facts = self.facts.serving(&rpc);
        let served = RunError::Infra;
        let mut identify =
            Inbound::plain(serve_well_known::<P, IdentifyRpc>(&rpc).map_err(served)?);
        let mut prepare =
            Inbound::plain(serve_well_known::<P, PrepareCellRpc>(&rpc).map_err(served)?);
        let mut form = Inbound::plain(serve_well_known::<P, FormCellRpc>(&rpc).map_err(served)?);
        let mut init = Inbound::plain(serve_well_known::<P, CellInitRpc>(&rpc).map_err(served)?);
        let mut admit = Inbound::plain(serve_well_known::<P, AdmitRpc>(&rpc).map_err(served)?);
        let mut inspect = Inbound::plain(serve_well_known::<P, InspectRpc>(&rpc).map_err(served)?);
        let cell = self.admission.cell;
        self.follow_and_register(providers, &rpc, cache, tunables, &shutdown);
        // Any machine of the cell answers `Resolve` (#216): an admitted one
        // learns the genesis pool from the control journal's membership.
        super::resolve::spawn(
            providers,
            &rpc,
            super::resolve::Resolver {
                facts: self.facts.clone(),
                cell,
                founders: None,
                book: self.book(),
                serves: false,
            },
            shutdown.clone(),
        )
        .map_err(served)?;
        tracing::info!(
            node = self.facts.node_id.0,
            cell = cell.cell_id,
            known = self.admission.members.len(),
            "machine_serving_admitted"
        );
        loop {
            moonpool_core::select! {
                () = shutdown.cancelled() => return Ok(()),
                error = edge.run() => return Err(RunError::Infra(error)),
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
                Some((request, reply)) = inspect.recv() => {
                    let facts =
                        crate::driver::operator::node_facts(self.facts.node_id.0, Some(&cell));
                    let answer = match request.target() {
                        Ok(InspectTarget::Node) => {
                            moonpool_assertions::reachable!(
                                "machine: an admitted machine names its cell to Inspect"
                            );
                            facts
                        }
                        Ok(InspectTarget::Journal(_)) => {
                            facts.refused(InspectRefusal::UnknownJournal)
                        }
                        Err(refusal) => facts.refused(refusal),
                    };
                    let _ = reply.send(answer);
                }
            }
        }
    }
}
