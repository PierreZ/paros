//! Waiting for a cell (#196, #216, #277): the machine contract an idle
//! machine serves — `Identify`, the cell decree's two phases (`PrepareCell`,
//! `FormCell`) as an acceptor, and `CellInit` as the decree's proposer —
//! until it forms (see the parent module).

use std::collections::BTreeMap;
use std::pin::Pin;
use std::time::Duration;

use moonpool_core::{Providers, TimeProvider};
use paros_core::acceptor::{AcceptOutcome, Acceptor, PrepareOutcome};
use paros_core::decree::DECREE_SLOT;
use paros_core::{AcceptorWrite, Ballot};
use tokio_util::sync::CancellationToken;

use super::formed::{FormedCell, vote_ballot};
use super::{CellPlan, Class, MachineFacts, ballot_from_wire, ballot_to_wire};
use crate::driver::edge::RpcEdge;
use crate::driver::{DriverTunables, RunError};
use crate::hooks::{DriverHooks, Seam};
use crate::rpc::machine as wire;
use crate::rpc::methods::{CellInitRpc, FormCellRpc, IdentifyRpc, PrepareCellRpc};
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
}

/// The decree a `CellInit` drives, in flight on the waiting loop, with the
/// answer it owes.
type Proposal<'a> = (
    Pin<Box<dyn Future<Output = Result<CellPlan, &'static str>> + Send + 'a>>,
    ReplySender<wire::CellInitAck>,
);

/// Wait for a cell: serve the machine contract at `facts.addr` until this
/// machine forms — it accepted a `FormCell` and no `CellInit` it drives is
/// still in flight — and return its cell, or `None` on `shutdown`.
/// `assignment` is how many user journals a cell this machine draws serves
/// beside its control journals — the static assignment, until #212 — each
/// under an identifier `cell init` draws.
///
/// # Errors
///
/// The listener could not bind, or the runtime failed, as
/// [`RunError::Infra`]; `hooks` crashed the machine at a durability seam
/// of the decree ([`Seam::CellPromised`], [`Seam::CellFormatted`]), as
/// [`RunError::SeamCrash`].
///
/// # Panics
///
/// If `ledger` already holds a vote: a formed machine does not wait.
#[tracing::instrument(level = "debug", skip_all, fields(node = facts.node_id.0, addr = %facts.addr))]
pub async fn wait_for_cell<P: Providers, L: CellLedger, H: DriverHooks>(
    providers: P,
    facts: &MachineFacts,
    assignment: usize,
    ledger: &mut L,
    tunables: &DriverTunables,
    shutdown: CancellationToken,
    hooks: &H,
) -> Result<Option<FormedCell>, RunError> {
    assert!(ledger.vote().is_none(), "a formed machine does not wait");
    let addr = facts.addr.to_string();
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
    tracing::info!(
        node = facts.node_id.0,
        class = facts.class.as_str(),
        "machine_waiting"
    );
    let mut proposal: Option<Proposal<'_>> = None;
    let formed = |ledger: &L| {
        ledger.vote().map(|(ballot, plan)| FormedCell {
            facts: facts.clone(),
            plan,
            ballot,
        })
    };
    loop {
        moonpool_core::select! {
            () = shutdown.cancelled() => return Ok(None),
            error = edge.run() => return Err(RunError::Infra(error)),
            Some((_, reply)) = identify.recv() => {
                reply.send(facts.identify_ack());
            }
            Some((request, reply)) = prepare.recv() => {
                // A seam crash drops the answer: the process dies with it.
                reply.send(answer_prepare(facts, ledger, &request, hooks).await.map_err(RunError::SeamCrash)?);
            }
            Some((request, reply)) = form.recv() => {
                reply.send(answer_form(facts, ledger, &request, hooks).await.map_err(RunError::SeamCrash)?);
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
/// machine answers from its record ([`FormedCell::prepare_ack`]). The
/// [`Seam::CellPromised`] crash, between the durable promise and the
/// answer, is the `Err`.
#[tracing::instrument(level = "trace", skip_all, fields(node = facts.node_id.0))]
async fn answer_prepare<L: CellLedger, H: DriverHooks>(
    facts: &MachineFacts,
    ledger: &mut L,
    request: &wire::PrepareCell,
    hooks: &H,
) -> Result<wire::PrepareCellAck, Seam> {
    let identity = Some(facts.identify_ack());
    let answer = |refusal: &str| wire::PrepareCellAck {
        identity: identity.clone(),
        refusal: refusal.into(),
        ..wire::PrepareCellAck::default()
    };
    if facts.class != Class::Storage {
        return Ok(answer("stateless"));
    }
    let Some(ballot) = ballot_from_wire(request.init.as_ref()) else {
        return Ok(answer("malformed"));
    };
    if let Some((voted, plan)) = ledger.vote() {
        return Ok(FormedCell {
            facts: facts.clone(),
            plan,
            ballot: voted,
        }
        .prepare_ack());
    }
    let mut acceptor = acceptor(ledger);
    let mut writes: Vec<AcceptorWrite<CellPlan>> = Vec::new();
    match acceptor.prepare(ballot, DECREE_SLOT, &mut writes) {
        PrepareOutcome::Promised { raised } => {
            if raised {
                if let Err(error) = ledger.promise(ballot).await {
                    tracing::error!(%error, "cell_promise_failed");
                    return Ok(answer("storage"));
                }
                if hooks.crash_at(Seam::CellPromised) {
                    tracing::warn!(seam = Seam::CellPromised.label(), "seam_crash");
                    return Err(Seam::CellPromised);
                }
            }
            assert!(
                ledger.promised() == ballot,
                "a decree promise lands on the prepared ballot"
            );
            assert!(ledger.vote().is_none(), "an idle machine reports no vote");
            Ok(wire::PrepareCellAck {
                identity,
                promised: true,
                ..wire::PrepareCellAck::default()
            })
        }
        PrepareOutcome::Refused | PrepareOutcome::BelowFloor => {
            // The decree slot is the floor, so only a higher promise refuses.
            assert!(
                acceptor.promised() > ballot,
                "a decree refusal names a higher promise"
            );
            Ok(wire::PrepareCellAck {
                identity,
                promised: false,
                promise: Some(ballot_to_wire(acceptor.promised())),
                ..wire::PrepareCellAck::default()
            })
        }
    }
}

/// Phase 2b on an idle machine: accept the plan unless a higher ballot was
/// promised — and accepting is forming. A formed machine answers from its
/// record ([`FormedCell::form_ack`]). The [`Seam::CellFormatted`] crash,
/// between the format and the vote, is the `Err`.
#[tracing::instrument(level = "trace", skip_all, fields(node = facts.node_id.0))]
async fn answer_form<L: CellLedger, H: DriverHooks>(
    facts: &MachineFacts,
    ledger: &mut L,
    request: &wire::FormCell,
    hooks: &H,
) -> Result<wire::FormCellAck, Seam> {
    let refuse = |refusal: &str| wire::FormCellAck {
        formed: false,
        refusal: refusal.into(),
        promise: None,
    };
    if facts.class != Class::Storage {
        return Ok(refuse("stateless"));
    }
    let (Ok(plan), Some(ballot)) = (CellPlan::from_form(request), vote_ballot(request)) else {
        return Ok(refuse("malformed"));
    };
    if !plan.members.contains(&(facts.node_id, facts.addr)) {
        return Ok(refuse("not_a_member"));
    }
    if let Some((voted, held)) = ledger.vote() {
        return Ok(FormedCell {
            facts: facts.clone(),
            plan: held,
            ballot: voted,
        }
        .form_ack(request));
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
                return Ok(refuse("storage"));
            }
            if hooks.crash_at(Seam::CellFormatted) {
                tracing::warn!(seam = Seam::CellFormatted.label(), "seam_crash");
                return Err(Seam::CellFormatted);
            }
            if let Err(error) = ledger.form(ballot, &plan).await {
                tracing::error!(%error, "cell_form_failed");
                return Ok(refuse("storage"));
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
            Ok(wire::FormCellAck {
                formed: true,
                refusal: String::new(),
                promise: None,
            })
        }
        AcceptOutcome::Refused | AcceptOutcome::BelowFloor => {
            assert!(
                acceptor.promised() > ballot,
                "a decree refusal names a higher promise"
            );
            Ok(wire::FormCellAck {
                formed: false,
                refusal: "promised_higher".into(),
                promise: Some(ballot_to_wire(acceptor.promised())),
            })
        }
    }
}
