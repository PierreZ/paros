//! Waiting for a cell (#196, #216, #277): the machine contract an idle
//! machine serves — `Identify`, the cell decree's two phases (`PrepareCell`,
//! `FormCell`) as an acceptor, `CellInit` as the decree's proposer, and
//! `Admit` — until it forms a cell or a cell admits it (see the parent
//! module).

use std::collections::BTreeMap;
use std::pin::Pin;
use std::time::Duration;

use moonpool_core::{Providers, TimeProvider};
use paros_core::acceptor::{AcceptOutcome, Acceptor, PrepareOutcome};
use paros_core::decree::DECREE_SLOT;
use paros_core::{AcceptorWrite, Ballot};
use tokio_util::sync::CancellationToken;

use super::admitted::AdmittedMachine;
use super::formed::{FormedCell, vote_ballot};
use super::{Admission, CellPlan, Class, MachineFacts, ballot_from_wire, ballot_to_wire};
use crate::driver::edge::RpcEdge;
use crate::driver::{DriverTunables, RunError};
use crate::rpc::machine as wire;
use crate::rpc::methods::{AdmitRpc, CellInitRpc, FormCellRpc, IdentifyRpc, PrepareCellRpc};
use crate::rpc::{Inbound, ReplySender, serve_well_known};

/// How long a waiting machine keeps its listener up after the answer that
/// ends its wait, so the answer leaves before the listener closes.
const FLUSH: Duration = Duration::from_millis(250);

/// Where an idle machine keeps its acceptor state in the cell decree
/// durably: the caller's (`parosd` records it in its data directory). Every
/// method returns only once what it recorded survives a crash.
pub trait CellLedger {
    /// The promise held: no plan under a lower ballot is accepted.
    fn promised(&self) -> Ballot;

    /// The vote: the plan this machine accepted, and the ballot it accepted
    /// it at. A machine with a vote is formed.
    fn vote(&self) -> Option<(Ballot, CellPlan)>;

    /// Raise the promise to `ballot`, durably, before the answer leaves.
    ///
    /// # Errors
    ///
    /// The record could not be made durable.
    fn promise(&mut self, ballot: Ballot) -> impl Future<Output = Result<(), String>>;

    /// Format the store of every journal `plan` names, ahead of the vote.
    /// Idempotent: a format an earlier attempt finished is resumed. A
    /// format with no vote recorded after it is no cell: a later plan may
    /// format over it.
    ///
    /// # Errors
    ///
    /// A store could not be made durable.
    fn format(&mut self, plan: &CellPlan) -> impl Future<Output = Result<(), String>>;

    /// Accept `plan` at `ballot`, which forms it here: record the vote (the
    /// promise raised to `ballot`) as this machine's cell — the commit
    /// point — once [`CellLedger::format`] formatted its stores.
    ///
    /// # Errors
    ///
    /// The record could not be made durable.
    fn form(&mut self, ballot: Ballot, plan: &CellPlan)
    -> impl Future<Output = Result<(), String>>;

    /// Record `admission` as this machine's cell (#216), durably, before
    /// the answer leaves. Only an idle machine with no promise is admitted.
    ///
    /// # Errors
    ///
    /// The record could not be made durable.
    fn admit(&mut self, admission: &Admission) -> impl Future<Output = Result<(), String>>;
}

/// How a wait ended: the machine formed a cell, or a cell admitted it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Joined {
    /// It accepted a plan: a founding member of the cell it formed.
    Founded(FormedCell),
    /// `cell add-machine` admitted it (#216).
    Admitted(AdmittedMachine),
}

/// The decree a `CellInit` drives, in flight on the waiting loop, with the
/// answer it owes.
type Proposal<'a> = (
    Pin<Box<dyn Future<Output = Result<CellPlan, &'static str>> + Send + 'a>>,
    ReplySender<wire::CellInitAck>,
);

/// Wait for a cell: serve the machine contract at `facts.listen` until this
/// machine forms — it accepted a `FormCell` and no `CellInit` it drives is
/// still in flight — or a cell admits it, and return how it joined, or
/// `None` on `shutdown`.
/// `assignment` is how many user journals a cell this machine draws serves
/// beside its control journals — the static assignment, until #212 — each
/// under an identifier `cell init` draws.
///
/// # Errors
///
/// The listener could not bind, or the runtime failed, as
/// [`RunError::Infra`].
///
/// # Panics
///
/// If `ledger` already holds a vote: a formed machine does not wait.
#[tracing::instrument(level = "debug", skip_all, fields(node = facts.node_id.0, addr = %facts.addr))]
pub async fn wait_for_cell<P: Providers, L: CellLedger>(
    providers: P,
    facts: &MachineFacts,
    assignment: usize,
    ledger: &mut L,
    tunables: &DriverTunables,
    shutdown: CancellationToken,
) -> Result<Option<Joined>, RunError> {
    assert!(ledger.vote().is_none(), "a formed machine does not wait");
    let addr = facts.listen.to_string();
    let mut edge = RpcEdge::listen(&providers, &addr, "machine", tunables)
        .await
        .map_err(RunError::Infra)?;
    let rpc = edge.handle().clone();
    let mut identify =
        Inbound::plain(serve_well_known::<P, IdentifyRpc>(&rpc).map_err(RunError::Infra)?);
    let mut prepare =
        Inbound::plain(serve_well_known::<P, PrepareCellRpc>(&rpc).map_err(RunError::Infra)?);
    let mut form =
        Inbound::plain(serve_well_known::<P, FormCellRpc>(&rpc).map_err(RunError::Infra)?);
    let mut init =
        Inbound::plain(serve_well_known::<P, CellInitRpc>(&rpc).map_err(RunError::Infra)?);
    let mut admit = Inbound::plain(serve_well_known::<P, AdmitRpc>(&rpc).map_err(RunError::Infra)?);
    tracing::info!(
        node = facts.node_id.0,
        class = facts.class.as_str(),
        "machine_waiting"
    );
    let mut proposal: Option<Proposal<'_>> = None;
    let formed = |ledger: &L| {
        ledger.vote().map(|(ballot, plan)| {
            Joined::Founded(FormedCell {
                facts: facts.clone(),
                plan,
                ballot,
            })
        })
    };
    loop {
        moonpool_core::select! {
            () = shutdown.cancelled() => return Ok(None),
            error = edge.run() => return Err(RunError::Infra(error)),
            Some((_, reply)) = identify.recv() => {
                reply.send(facts.identify_ack(0));
            }
            Some((request, reply)) = admit.recv() => {
                let in_init = proposal.is_some();
                let (ack, admitted) = answer_admit(facts, ledger, &request, in_init).await;
                reply.send(ack);
                if let Some(admission) = admitted {
                    flush(&providers, &mut edge).await;
                    return Ok(Some(Joined::Admitted(AdmittedMachine {
                        facts: facts.clone(),
                        admission,
                    })));
                }
            }
            Some((request, reply)) = prepare.recv() => {
                reply.send(answer_prepare(facts, ledger, &request).await);
            }
            Some((request, reply)) = form.recv() => {
                reply.send(answer_form(facts, ledger, &request).await);
                // A decree this machine drives finishes before it stops
                // waiting: its own accept is the decree's last.
                if proposal.is_none()
                    && let Some(cell) = formed(ledger)
                {
                    flush(&providers, &mut edge).await;
                    return Ok(Some(cell));
                }
            }
            Some((request, reply)) = init.recv() => {
                if proposal.is_some() {
                    // One decree at a time per receiver: the caller asks
                    // again, and finds what this one decided.
                    reply.send(refused("contended"));
                } else {
                    let run = super::cell_init::propose(
                        &providers,
                        &rpc,
                        facts,
                        ledger.promised(),
                        assignment,
                        request,
                        tunables,
                    );
                    proposal = Some((Box::pin(run), reply));
                }
            }
            outcome = async { proposal.as_mut().expect("guarded").0.as_mut().await },
                if proposal.is_some() =>
            {
                let (_, reply) = proposal.take().expect("guarded");
                match outcome {
                    Ok(plan) => {
                        tracing::info!(cell = plan.cell_id, members = plan.members.len(), "cell_initialized");
                        reply.send(plan.cell_init_ack());
                    }
                    Err(refusal) => {
                        tracing::warn!(refusal, "cell_init_refused");
                        reply.send(refused(refusal));
                    }
                }
                if let Some(cell) = formed(ledger) {
                    flush(&providers, &mut edge).await;
                    return Ok(Some(cell));
                }
            }
        }
    }
}

/// A `CellInitAck` that refuses, with its label.
fn refused(refusal: &str) -> wire::CellInitAck {
    wire::CellInitAck {
        refusal: refusal.into(),
        ..wire::CellInitAck::default()
    }
}

/// Keep the runtime running for [`FLUSH`], so an answer just sent leaves
/// before the listener closes.
async fn flush<P: Providers>(providers: &P, edge: &mut RpcEdge<P>) {
    let time = providers.time().clone();
    moonpool_core::select! {
        _ = edge.run() => {}
        _ = time.sleep(FLUSH) => {}
    }
}

/// This machine's acceptor half of the decree, over the ledger: the shared
/// [`Acceptor`] role over a one-value log — slot zero, floor at zero, no
/// tri-state, exactly as a matchmaker runs the handover's decree.
fn acceptor<L: CellLedger>(ledger: &L) -> Acceptor<CellPlan> {
    let records = ledger
        .vote()
        .map(|vote| BTreeMap::from([(DECREE_SLOT, vote)]))
        .unwrap_or_default();
    let acceptor = Acceptor::new(ledger.promised(), records, DECREE_SLOT, BTreeMap::new());
    assert!(
        acceptor.promised() == ledger.promised(),
        "the decree acceptor holds the durable promise"
    );
    assert!(
        acceptor.record(DECREE_SLOT).cloned() == ledger.vote(),
        "the decree acceptor holds the durable vote"
    );
    acceptor
}

/// Phase 1b on an idle machine: promise (durably, before the answer
/// leaves) and report no vote, or refuse under a higher promise. A formed
/// machine answers from its record ([`FormedCell::prepare_ack`]). Between
/// the durable promise and the answer is a moment worth a crash (#246): the
/// promise stays, the answer is lost, and the next ballot meets a promised,
/// unvoted machine.
#[tracing::instrument(level = "trace", skip_all, fields(node = facts.node_id.0))]
async fn answer_prepare<L: CellLedger>(
    facts: &MachineFacts,
    ledger: &mut L,
    request: &wire::PrepareCell,
) -> wire::PrepareCellAck {
    let identity = Some(facts.identify_ack(0));
    let answer = |refusal: &str| wire::PrepareCellAck {
        identity: identity.clone(),
        refusal: refusal.into(),
        ..wire::PrepareCellAck::default()
    };
    if facts.class != Class::Storage {
        return answer("stateless");
    }
    let Some(ballot) = ballot_from_wire(request.init.as_ref()) else {
        return answer("malformed");
    };
    if let Some((voted, plan)) = ledger.vote() {
        return FormedCell {
            facts: facts.clone(),
            plan,
            ballot: voted,
        }
        .prepare_ack();
    }
    let mut acceptor = acceptor(ledger);
    let mut writes: Vec<AcceptorWrite<CellPlan>> = Vec::new();
    match acceptor.prepare(ballot, DECREE_SLOT, &mut writes) {
        PrepareOutcome::Promised { raised } => {
            if raised {
                if let Err(error) = ledger.promise(ballot).await {
                    tracing::error!(%error, "cell_promise_failed");
                    return answer("storage");
                }
                moonpool_buggify::hint!("cell init promise durable, answer not sent").await;
            }
            assert!(
                ledger.promised() == ballot,
                "a decree promise lands on the prepared ballot"
            );
            assert!(ledger.vote().is_none(), "an idle machine reports no vote");
            wire::PrepareCellAck {
                identity,
                promised: true,
                ..wire::PrepareCellAck::default()
            }
        }
        PrepareOutcome::Refused | PrepareOutcome::BelowFloor => {
            // The decree slot is the floor, so only a higher promise refuses.
            assert!(
                acceptor.promised() > ballot,
                "a decree refusal names a higher promise"
            );
            wire::PrepareCellAck {
                identity,
                promised: false,
                promise: Some(ballot_to_wire(acceptor.promised())),
                ..wire::PrepareCellAck::default()
            }
        }
    }
}

/// Phase 2b on an idle machine: accept the plan unless a higher ballot was
/// promised — and accepting is forming. A formed machine answers from its
/// record ([`FormedCell::form_ack`]). Between the format and the vote is a
/// moment worth a crash (#246): formatted stores and no cell, so the
/// machine never accepted the plan and a later `cell init` may form
/// another one over those stores (#277).
#[tracing::instrument(level = "trace", skip_all, fields(node = facts.node_id.0))]
async fn answer_form<L: CellLedger>(
    facts: &MachineFacts,
    ledger: &mut L,
    request: &wire::FormCell,
) -> wire::FormCellAck {
    let refuse = |refusal: &str| wire::FormCellAck {
        formed: false,
        refusal: refusal.into(),
        promise: None,
    };
    if facts.class != Class::Storage {
        return refuse("stateless");
    }
    let (Ok(plan), Some(ballot)) = (CellPlan::from_form(request), vote_ballot(request)) else {
        return refuse("malformed");
    };
    if !plan.members.contains(&(facts.node_id, facts.addr.clone())) {
        return refuse("not_a_member");
    }
    if let Some((voted, held)) = ledger.vote() {
        return FormedCell {
            facts: facts.clone(),
            plan: held,
            ballot: voted,
        }
        .form_ack(request);
    }
    let mut acceptor = acceptor(ledger);
    match acceptor.admit(ballot, DECREE_SLOT) {
        AcceptOutcome::Admitted => {
            // A vote is a promise too: the acceptor raises the promise
            // before it records, exactly as the log wiring does.
            let mut writes: Vec<AcceptorWrite<CellPlan>> = Vec::new();
            acceptor.set_promise(ballot, &mut writes);
            acceptor.record_accepted(DECREE_SLOT, ballot, plan.clone(), &mut writes);
            if let Err(error) = ledger.format(&plan).await {
                tracing::error!(%error, "cell_format_failed");
                return refuse("storage");
            }
            moonpool_buggify::hint!("cell stores formatted, vote not recorded", 0.1).await;
            if let Err(error) = ledger.form(ballot, &plan).await {
                tracing::error!(%error, "cell_form_failed");
                return refuse("storage");
            }
            assert!(
                ledger.vote().as_ref() == acceptor.record(DECREE_SLOT),
                "the ledger holds the vote the acceptor recorded"
            );
            assert!(
                ledger.promised() == acceptor.promised(),
                "the ledger holds the promise the vote raised"
            );
            tracing::info!(cell = plan.cell_id, "cell_formed");
            wire::FormCellAck {
                formed: true,
                refusal: String::new(),
                promise: None,
            }
        }
        AcceptOutcome::Refused | AcceptOutcome::BelowFloor => {
            assert!(
                acceptor.promised() > ballot,
                "a decree refusal names a higher promise"
            );
            wire::FormCellAck {
                formed: false,
                refusal: "promised_higher".into(),
                promise: Some(ballot_to_wire(acceptor.promised())),
            }
        }
    }
}

/// `Admit` on an idle machine (#216): record the admission durably, then
/// answer. Refused while the machine holds a promise in the cell decree or
/// drives one (`in_init`): a `cell init` that lists it may still form a cell
/// over it, and a machine is in one cell only — a stalled `cell init` is
/// safer than a machine in two cells. Between the durable admission and the
/// answer is a moment worth a crash: the machine is in the cell, the caller
/// does not know, and its next `Admit` is acked by the admitted machine.
/// The admission, when this call admitted the machine.
#[tracing::instrument(level = "trace", skip_all, fields(node = facts.node_id.0))]
async fn answer_admit<L: CellLedger>(
    facts: &MachineFacts,
    ledger: &mut L,
    request: &wire::Admit,
    in_init: bool,
) -> (wire::AdmitAck, Option<Admission>) {
    let refuse = |refusal: &str| {
        (
            wire::AdmitAck {
                admitted: false,
                refusal: refusal.into(),
            },
            None,
        )
    };
    assert!(ledger.vote().is_none(), "an idle machine holds no vote");
    let Ok(admission) = Admission::from_wire(request) else {
        return refuse("malformed");
    };
    if in_init || ledger.promised() != Ballot::default() {
        moonpool_assertions::reachable!(
            "machine: a machine promised in cell init refuses an admission"
        );
        return refuse("in_cell_init");
    }
    if let Err(error) = ledger.admit(&admission).await {
        tracing::error!(%error, "machine_admit_failed");
        return refuse("storage");
    }
    moonpool_buggify::hint!("admission durable, answer not sent").await;
    tracing::info!(cell = admission.cell.cell_id, "machine_admitted");
    (
        wire::AdmitAck {
            admitted: true,
            refusal: String::new(),
        },
        Some(admission),
    )
}
