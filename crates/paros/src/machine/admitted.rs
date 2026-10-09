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
//!   it is refused as `cell_exists`, and it accepts no plan.

use std::collections::BTreeSet;
use std::net::SocketAddr;

use moonpool_core::Providers;
use tokio_util::sync::CancellationToken;

use super::{Admission, MachineFacts};
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
}

impl AdmittedMachine {
    /// The cell's machines this one knows, by address: what it caches of the
    /// registry (§3.2).
    #[must_use]
    pub fn known(&self) -> BTreeSet<SocketAddr> {
        self.admission
            .members
            .iter()
            .map(|(_, addr)| *addr)
            .collect()
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

    /// Serve the machine contract and a node-only `Inspect` at the
    /// machine's address until `shutdown`.
    ///
    /// # Errors
    ///
    /// The listener could not bind, or the runtime failed, as
    /// [`RunError::Infra`].
    #[tracing::instrument(level = "debug", skip_all, fields(node = self.facts.node_id.0, cell = self.admission.cell.cell_id))]
    pub async fn serve<P: Providers>(
        self,
        providers: &P,
        tunables: &DriverTunables,
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
        let addr = self.facts.addr.to_string();
        let mut edge = RpcEdge::listen(providers, &addr, "machine", tunables)
            .await
            .map_err(RunError::Infra)?;
        let rpc = edge.handle().clone();
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
