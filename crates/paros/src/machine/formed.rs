//! A formed machine's half of the cell decree (#277): its vote is final, so
//! it answers `PrepareCell` and `FormCell` from its record — the plan and the
//! ballot it accepted — while it serves its cell. A `cell init` that a crash
//! interrupted after some members formed still hears every member, and
//! finishes the plan they hold.
//!
//! A formed member answers every ballot with its vote and moves no promise:
//! it never accepts another plan, so any later proposer hears its vote, and
//! P2c makes that proposer finish this plan or refuse (`other_cell_init`).

use moonpool_core::{Detach, Providers, SimulationResult, TaskProvider};
use moonpool_rpc::RpcHandle;
use paros_core::{Ballot, NodeId};
use tokio_util::sync::CancellationToken;

use super::{Admission, CachedRegistry, CellPlan, Class, ControlJournals, MachineFacts};
use crate::Address;
use crate::rpc::machine as wire;
use crate::rpc::methods::{AdmitRpc, FormCellRpc, IdentifyRpc, PrepareCellRpc};
use crate::rpc::{Inbound, serve_well_known};

/// A machine of a cell: what it knows of itself, the plan it accepted and
/// the decree ballot it accepted it at — the record's commit point.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FormedCell {
    /// The machine.
    pub facts: MachineFacts,
    /// The plan it formed.
    pub plan: CellPlan,
    /// The ballot it accepted the plan at.
    pub ballot: Ballot,
    /// The cached registry fold its disk held at start (#211), if any.
    pub cached: Option<CachedRegistry>,
}

impl FormedCell {
    /// The control journals the cell's machines know (see
    /// [`ControlJournals`]).
    #[must_use]
    pub fn control_journals(&self) -> ControlJournals {
        self.plan.control_journals()
    }

    /// The founding members where this machine dials them at start: at the
    /// addresses its cached registry fold names (#211), else the plan's.
    /// The registry fold takes over once it passes the cache's position.
    ///
    /// # Panics
    ///
    /// Never: the assertion checks that every founding member keeps one
    /// address.
    #[must_use]
    pub fn founders(&self) -> Vec<(NodeId, Address)> {
        let founders: Vec<(NodeId, Address)> = self
            .plan
            .members
            .iter()
            .map(|(id, planned)| {
                let cached = self.cached.as_ref().and_then(|c| c.address(*id));
                (*id, cached.unwrap_or(planned).clone())
            })
            .collect();
        assert_eq!(
            founders.len(),
            self.plan.members.len(),
            "every founding member keeps one address"
        );
        founders
    }

    /// This member's vote, as the decree reports it.
    pub(super) fn vote(&self) -> wire::FormCell {
        self.plan.form_request(self.ballot)
    }

    /// Phase 1b from a formed member: its vote, whatever the ballot.
    pub(super) fn prepare_ack(&self) -> wire::PrepareCellAck {
        assert!(self.ballot.round != 0, "a formed member voted at a ballot");
        // A member by id: the address it advertises may have changed since
        // the plan (#349).
        assert!(
            self.plan
                .members
                .iter()
                .any(|(id, _)| *id == self.facts.node_id),
            "a formed member is a member of its plan"
        );
        wire::PrepareCellAck {
            identity: Some(self.facts.identify_ack(self.plan.cell_id)),
            promised: true,
            promise: None,
            vote: Some(self.vote()),
            refusal: String::new(),
        }
    }

    /// Phase 2b from a formed member: acked for the plan it holds, at any
    /// ballot. Another plan below its vote's ballot is a preemption, as at
    /// any acceptor: a proposer whose prepare finished before this member
    /// voted still sends its own plan, and must reopen above the vote to
    /// hear it. Another plan at or above it is another cell's. Witness: two
    /// concurrent `cell init`s, the slower one's prepare done before the
    /// faster one's vote, were refused `other_cell_init` over the very cell
    /// they formed (seed 9979247141707847612 on #319's branch).
    pub(super) fn form_ack(&self, request: &wire::FormCell) -> wire::FormCellAck {
        assert_eq!(self.facts.class, Class::Storage, "only storage forms");
        let refusal = match CellPlan::from_form(request) {
            Ok(plan) if plan == self.plan => "",
            Ok(_) if vote_ballot(request).is_some_and(|ballot| ballot < self.ballot) => {
                return wire::FormCellAck {
                    formed: false,
                    refusal: "promised_higher".into(),
                    promise: Some(super::ballot_to_wire(self.ballot)),
                };
            }
            Ok(_) => "other_cell",
            Err(_) => "malformed",
        };
        wire::FormCellAck {
            formed: refusal.is_empty(),
            refusal: refusal.into(),
            promise: None,
        }
    }

    /// `Admit` at a founding member (#216): it is in its cell already, so an
    /// admission into that cell is acked and changes nothing; any other is
    /// refused.
    pub(super) fn admit_ack(&self, request: &wire::Admit) -> wire::AdmitAck {
        let refusal = match Admission::from_wire(request) {
            Ok(admission) if admission.cell == self.control_journals() => "",
            Ok(_) => "other_cell",
            Err(_) => "malformed",
        };
        wire::AdmitAck {
            admitted: refusal.is_empty(),
            refusal: refusal.into(),
        }
    }

    /// Answer the decree's calls (and `Identify`, `Admit`, `Resolve`) on `rpc` until `shutdown`,
    /// in a task of its own: every answer is a pure function of the record,
    /// so nothing here touches the node loop.
    ///
    /// # Errors
    ///
    /// An endpoint could not be registered.
    #[tracing::instrument(level = "debug", skip_all, fields(node = self.facts.node_id.0, cell = self.plan.cell_id))]
    pub(crate) fn serve<P: Providers>(
        mut self,
        providers: &P,
        rpc: &RpcHandle<P>,
        shutdown: CancellationToken,
    ) -> SimulationResult<()> {
        self.facts = self.facts.serving(rpc);
        let mut identify = Inbound::plain(serve_well_known::<P, IdentifyRpc>(rpc)?);
        let mut prepare = Inbound::plain(serve_well_known::<P, PrepareCellRpc>(rpc)?);
        let mut form = Inbound::plain(serve_well_known::<P, FormCellRpc>(rpc)?);
        let mut admit = Inbound::plain(serve_well_known::<P, AdmitRpc>(rpc)?);
        // Any machine of the cell answers `Resolve` (#216).
        super::resolve::spawn(
            providers,
            rpc,
            super::resolve::Resolver {
                facts: self.facts.clone(),
                cell: self.control_journals(),
                founders: Some(self.plan.members.iter().map(|(id, _)| *id).collect()),
                book: super::with_own(&self.founders(), self.facts.node_id, &self.facts.addr),
                serves: true,
            },
            shutdown.clone(),
        )?;
        providers
            .task()
            .spawn_task("paros-machine-formed", async move {
                loop {
                    moonpool_core::select! {
                        biased;
                        () = shutdown.cancelled() => return,
                        Some((_, reply)) = identify.recv() => {
                            reply.send(self.facts.identify_ack(self.plan.cell_id));
                        }
                        Some((_, reply)) = prepare.recv() => {
                            reply.send(self.prepare_ack());
                        }
                        Some((request, reply)) = form.recv() => {
                            reply.send(self.form_ack(&request));
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

/// The wire form of a vote's ballot.
pub(super) fn vote_ballot(vote: &wire::FormCell) -> Option<Ballot> {
    super::ballot_from_wire(vote.init.as_ref())
}
