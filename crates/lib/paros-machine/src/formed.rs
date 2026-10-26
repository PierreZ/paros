//! A formed machine's half of the cell decree (#277): its vote is final, so
//! it answers `PrepareCell` and `FormCell` from its record — the plan and the
//! ballot it accepted — while it serves its cell. A `cell init` that a crash
//! interrupted after some members formed still hears every member, and
//! finishes the plan they hold.
//!
//! A formed member answers every ballot with its vote and moves no promise:
//! it never accepts another plan, so any later proposer hears its vote, and
//! P2c makes that proposer finish this plan or refuse (`other_cell_init`).
//!
//! **A formed member teaches the decree** (`Teacher`): the proposer sends
//! `FormCell` once, so a member that was down at that moment never hears
//! the plan from it, and stays idle while the plan names it. Every formed
//! member asks each other member of its plan for its identity at every
//! election renewal period. A member that answers idle gets this member's
//! vote again, as a `FormCell` at the ballot this member accepted it at.
//! That is a re-send of Phase 2a: the same plan at the same ballot, which an
//! acceptor may take at any time. The idle member took the promise at that
//! ballot in Phase 1 (every member answers Phase 1), so it accepts.
//!
//! The `FormCell` carries the operator's `admin` token, the one the
//! `FormCell` that formed this member carried (#245 (Biscuit tokens)), so a
//! pinned learner checks it as it checks any `FormCell`: the pin has no
//! exception. The token stays in memory only (`FormedCell::token`), never in
//! the record. A member that restarted holds none and teaches nothing, and
//! a learner whose pin refuses the token is not asked again: in both cases
//! the operator runs `init` again, as for an admission
//! (`docs/architecture/03-1-cell-tenant-and-bootstrap.md` §3.1, Trust).
//!
//! The teaching of a member ends once it answers in this cell, answers as
//! another machine (a wiped member never forms), refuses the token, or the
//! cell's address book names it at another address than the plan's (it
//! registered there, so it formed; an idle member accepts the plan only at
//! the plan's address). A member that does not answer (down, or gone) is
//! asked again at the next period: one `Identify` a period costs little.
//! Witnesses: a
//! founding member that missed the proposer's `FormCell` waited idle until
//! an operator ran `init` again, and with one more founder wiped the cell
//! served no quorum (seed 7461995534021400970 on main 7feccc9, three
//! founders; seed 16403044264830861989 on main 629aa62, four founders).
//! With the teacher, the idle member formed 0.2 s after its peers.

use std::collections::BTreeMap;
use std::time::Duration;

use moonpool_core::{Detach, LocalTaskProvider, LocalTimeProvider, SimulationResult};
use moonpool_rpc::RpcHandle;
use paros_core::{Ballot, NodeId};
use paros_net::runtime::ParosProviders;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use super::{Admission, CachedRegistry, CellPlan, Class};
use paros_model::address::Address;
use paros_model::address::Names;
use paros_net::{Inbound, serve_well_known};
pub use paros_roles::node::host::{FormationToken, FormedCell};
use paros_tunables::DriverTunables;
use paros_wire::machine as wire;
use paros_wire::methods::{AdmitRpc, FormCellRpc, IdentifyRpc, PrepareCellRpc};

/// A formed member's half of the cell decree (see the module doc), and
/// what it serves beside its journals.
pub(crate) trait FormedExt {
    /// This member's vote, as the decree reports it.
    fn vote(&self) -> wire::FormCell;
    /// The answer to a `PrepareCell`: this member's vote.
    fn prepare_ack(&self) -> wire::PrepareCellAck;
    /// The answer to a `FormCell`.
    fn form_ack(&self, request: &wire::FormCell) -> wire::FormCellAck;
    /// The answer to an `Admit`.
    fn admit_ack(&self, request: &wire::Admit) -> wire::AdmitAck;
    /// Serve the decree beside the journals.
    #[allow(clippy::too_many_arguments)]
    fn serve<P: ParosProviders>(
        self,
        providers: &P,
        rpc: &RpcHandle<P>,
        tunables: &DriverTunables,
        books: watch::Receiver<Option<CachedRegistry>>,
        observer: Option<std::sync::Arc<dyn paros_client::CallObserver>>,
        shutdown: CancellationToken,
    ) -> SimulationResult<()>;
}

impl FormedExt for FormedCell {
    /// This member's vote, as the decree reports it.
    fn vote(&self) -> wire::FormCell {
        self.plan.form_request(self.ballot)
    }

    /// Phase 1b from a formed member: its vote, whatever the ballot.
    fn prepare_ack(&self) -> wire::PrepareCellAck {
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
    fn form_ack(&self, request: &wire::FormCell) -> wire::FormCellAck {
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
    fn admit_ack(&self, request: &wire::Admit) -> wire::AdmitAck {
        let refusal = match Admission::from_wire(request) {
            Ok(admission) if admission.cell == self.control_journals() => "",
            Ok(_) => "other_cell",
            Err(_) => "malformed",
        };
        wire::AdmitAck {
            admitted: refusal.is_empty(),
            refusal: refusal.into(),
            in_cell: refusal.is_empty(),
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
    fn serve<P: ParosProviders>(
        mut self,
        providers: &P,
        rpc: &RpcHandle<P>,
        tunables: &DriverTunables,
        books: watch::Receiver<Option<CachedRegistry>>,
        observer: Option<std::sync::Arc<dyn paros_client::CallObserver>>,
        shutdown: CancellationToken,
    ) -> SimulationResult<()> {
        self.facts = self.facts.serving(rpc);
        let mut identify = Inbound::plain(serve_well_known::<P, IdentifyRpc>(rpc)?);
        let mut prepare = Inbound::plain(serve_well_known::<P, PrepareCellRpc>(rpc)?);
        let mut form = Inbound::plain(serve_well_known::<P, FormCellRpc>(rpc)?);
        let mut admit = Inbound::plain(serve_well_known::<P, AdmitRpc>(rpc)?);
        // Every machine answers `Load` (#424).
        super::load::serve(providers, rpc, &self.facts, shutdown.clone())?;
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
                client: tunables.internal_client(),
                answer_within: tunables.resolve_answer_within,
            },
            shutdown.clone(),
        )?;
        // Every machine serves the batcher endpoint (#145) over the cell's
        // address book; the tenant coordinator books a tenant's batchers
        // (#604) and `Resolve` names them.
        paros_roles::batcher::spawn(
            providers,
            rpc,
            paros_roles::batcher::BatcherSettings::from_tunables(tunables),
            (
                paros_roles::batcher::BatcherBook {
                    nodes: super::with_own(&self.founders(), self.facts.node_id, &self.facts.addr),
                    updates: Some(books.clone()),
                    skip: None,
                    observer,
                },
                self.facts.names.clone(),
            ),
            shutdown.clone(),
        )?;
        // Teach the decree to a member that missed it (see the module doc).
        let teacher = Teacher {
            cell: self.clone(),
            names: self.facts.names.clone(),
            books,
            period: tunables.election_renew,
            patience: tunables.connection_timeout,
        };
        let teach_rpc = rpc.clone();
        let teach_providers = providers.clone();
        let teach_shutdown = shutdown.clone();
        providers
            .local_task()
            .spawn_local_task("paros-machine-teach", async move {
                moonpool_core::select! {
                    () = teach_shutdown.cancelled() => {}
                    () = teacher.run(&teach_providers, &teach_rpc) => {}
                }
            })
            .detach();
        providers
            .local_task()
            .spawn_local_task("paros-machine-formed", async move {
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

/// The decree's learner catch-up on a formed member (see the module doc):
/// what it teaches, and at which cadence.
struct Teacher {
    /// This member, with the plan, the ballot it voted at and the token of
    /// the `FormCell` that formed it.
    cell: FormedCell,
    /// How the plan's addresses resolve.
    names: Names,
    /// The cell's address books the node loop offers its cache (#211).
    books: watch::Receiver<Option<CachedRegistry>>,
    /// The pause between two rounds.
    period: Duration,
    /// How long one call waits for its answer.
    patience: Duration,
}

/// What a member's answer to `Identify` says about the decree there.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Learner {
    /// It formed this cell: nothing to teach.
    Formed,
    /// It is the member, idle: it missed the decree.
    Idle,
    /// Another machine answers at the member's address (a wiped member),
    /// or the member is in another cell: it never forms this plan.
    Other,
    /// No answer: ask again.
    Unknown,
}

/// A refusal of the teacher's token by the learner's pin (#245 (Biscuit
/// tokens)): sending it again changes nothing.
fn token_refused(refusal: &str) -> bool {
    matches!(refusal, "invalid_token" | "expired" | "forbidden")
}

impl Teacher {
    /// Teach every other member of the plan until each one formed, is
    /// another machine, moved away from the plan's address, or refused the
    /// token. Returns when nothing is left to teach.
    ///
    /// # Panics
    ///
    /// Never: the assertions check that a teacher re-sends its own vote, with
    /// its own token, to the others only.
    #[tracing::instrument(level = "debug", skip_all, fields(node = self.cell.facts.node_id.0, cell = self.cell.plan.cell_id))]
    async fn run<P: ParosProviders>(mut self, providers: &P, rpc: &RpcHandle<P>) {
        let me = self.cell.facts.node_id;
        // By address: an idle member accepts the plan only at the address
        // the plan names (`not_a_member` otherwise).
        let mut pending: BTreeMap<Address, NodeId> = self
            .cell
            .plan
            .members
            .iter()
            .filter(|(id, _)| *id != me)
            .map(|(id, addr)| (addr.clone(), *id))
            .collect();
        assert!(
            pending.len() < self.cell.plan.members.len(),
            "a teacher never teaches itself"
        );
        let Some(token) = self.cell.token.clone() else {
            // A restart: the token was never written down. The operator
            // runs `init` again for a member still idle.
            tracing::debug!(
                members = pending.len(),
                "cell_decree_not_taught_without_token"
            );
            return;
        };
        let mut vote = self.cell.vote();
        assert!(
            vote_ballot(&vote) == Some(self.cell.ballot),
            "a teacher re-sends its own vote at its own ballot"
        );
        assert!(vote.token.is_empty(), "a vote carries no token");
        vote.token = token.into_bytes();
        while !pending.is_empty() {
            if providers.time().sleep(self.period).await.is_err() {
                return;
            }
            self.moved_away(&mut pending);
            if pending.is_empty() {
                break;
            }
            let addrs: Vec<Address> = pending.keys().cloned().collect();
            let answers = super::cell_init::fan_out::<P, IdentifyRpc>(
                providers,
                rpc,
                &self.names,
                &addrs,
                &wire::Identify {},
                self.patience,
            )
            .await;
            let mut idle = Vec::new();
            for (addr, answer) in answers {
                match self.learner(pending[&addr], answer.as_ref()) {
                    Learner::Formed | Learner::Other => {
                        pending.remove(&addr);
                    }
                    Learner::Idle => idle.push(addr),
                    Learner::Unknown => {}
                }
            }
            if idle.is_empty() {
                continue;
            }
            let answers = super::cell_init::fan_out::<P, FormCellRpc>(
                providers,
                rpc,
                &self.names,
                &idle,
                &vote,
                self.patience,
            )
            .await;
            for (addr, answer) in answers {
                let Some(ack) = answer else { continue };
                let member = pending[&addr];
                if ack.formed {
                    tracing::info!(member = member.0, "cell_decree_taught");
                    moonpool_assertions::reachable!(
                        "machine: a founding member that missed the decree is formed by a peer"
                    );
                    pending.remove(&addr);
                } else if token_refused(&ack.refusal) {
                    // The learner's pin refuses the operator's token: the
                    // operator runs `init` again with a valid one.
                    tracing::warn!(member = member.0, refusal = %ack.refusal, "cell_decree_token_refused");
                    pending.remove(&addr);
                } else {
                    // A higher promise (the `cell init` that holds it
                    // finishes this plan there), or a disk that failed:
                    // ask again at the next round.
                    tracing::debug!(member = member.0, refusal = %ack.refusal, "cell_decree_not_taught");
                }
            }
        }
        tracing::debug!("cell_decree_known_everywhere");
    }

    /// Drop every member the latest address book names at another address
    /// than the plan's: it registered there, so it formed, and an idle
    /// member accepts the plan only at the plan's address.
    fn moved_away(&mut self, pending: &mut BTreeMap<Address, NodeId>) {
        let Some(book) = self.books.borrow_and_update().clone() else {
            return;
        };
        assert!(
            book.node == self.cell.facts.node_id,
            "a teacher reads its own machine's book"
        );
        pending.retain(|planned, member| {
            let moved = book
                .machines
                .iter()
                .any(|(id, addr)| id == member && addr != planned);
            if moved {
                tracing::debug!(member = member.0, "cell_decree_learner_moved");
            }
            !moved
        });
    }

    /// Classify `member`'s answer to `Identify`.
    fn learner(&self, member: NodeId, answer: Option<&wire::IdentifyAck>) -> Learner {
        assert!(
            member != self.cell.facts.node_id,
            "a teacher asks the others"
        );
        assert!(self.cell.plan.cell_id != 0, "a formed plan names its cell");
        let Some(ack) = answer else {
            return Learner::Unknown;
        };
        if ack.node_id != member.0 {
            return Learner::Other;
        }
        if ack.cell_id == self.cell.plan.cell_id {
            return Learner::Formed;
        }
        if ack.cell_id != 0
            || super::identity_tags(ack).is_none_or(|(class, _)| class != Class::Storage)
        {
            return Learner::Other;
        }
        Learner::Idle
    }
}

/// The wire form of a vote's ballot.
pub(super) fn vote_ballot(vote: &wire::FormCell) -> Option<Ballot> {
    super::ballot_from_wire(vote.init.as_ref())
}
