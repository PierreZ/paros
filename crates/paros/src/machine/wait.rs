//! Waiting for a cell (#196, #216): the machine contract a machine with no
//! plan serves — `Identify`, `Init` (a seed's: the cell step of `init`) and
//! `FormCell` — until one forms it (see the parent module).

use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::time::Duration;

use moonpool_core::{Providers, RandomProvider, TimeProvider};
use moonpool_rpc::RpcHandle;
use paros_core::{JournalId, JournalIdentifier, NodeId, TenantId};
use tokio_util::sync::CancellationToken;

use super::{CellPlan, Class, MachineFacts};
use crate::driver::edge::RpcEdge;
use crate::driver::{DriverTunables, RunError};
use crate::rpc::machine as wire;
use crate::rpc::methods::{FormCellRpc, IdentifyRpc, InitRpc, InspectRpc};
use crate::rpc::{Inbound, InspectRequest, serve_well_known, well_known};

/// A random identifier: a random tenant and a random journal, both set (no id
/// is fixed, `docs/architecture.md` §3.8).
fn draw_identifier<P: Providers>(providers: &P) -> JournalIdentifier {
    let draw = || loop {
        let id: u64 = providers.random().random();
        if id != 0 {
            break id;
        }
    };
    JournalIdentifier::new(TenantId(draw()), JournalId(draw()))
}

/// How long a waiting machine keeps its listener up after the answer that
/// ends its wait, so the answer leaves before the listener closes.
const FLUSH: Duration = Duration::from_millis(250);

/// Where a waiting machine keeps its cell's plan durably: the caller's
/// (`parosd` records it in its data directory). Every method returns only
/// once what it recorded survives a crash.
pub trait CellLedger {
    /// The plan this machine recorded as pending while it ran `Init`, if
    /// any: a re-run resumes it, never redraws it.
    fn pending(&self) -> Option<CellPlan>;

    /// Record `plan` as pending, before any seed is asked to form it.
    ///
    /// # Errors
    ///
    /// The record could not be made durable.
    fn record_pending(&mut self, plan: &CellPlan) -> impl Future<Output = Result<(), String>>;

    /// Form `plan` on this machine: format the store of every journal it
    /// names, then record the plan as this machine's cell — the commit
    /// point. Idempotent: a format an earlier attempt finished is resumed.
    ///
    /// # Errors
    ///
    /// A store or the record could not be made durable.
    fn form(&mut self, plan: &CellPlan) -> impl Future<Output = Result<(), String>>;
}

/// Wait for a cell: serve the machine contract at `facts.addr` until a
/// `FormCell` (from a seed running `Init`) or an `Init` (from `parosctl`)
/// forms this machine, and return the plan it formed — or `None` on
/// `shutdown`. `assignment` is how many user journals a cell this machine
/// initializes serves beside its control journals — the static assignment,
/// until #212 — each under an identifier `init` draws.
///
/// # Errors
///
/// The listener could not bind, or the runtime failed, as
/// [`RunError::Infra`].
#[tracing::instrument(level = "debug", skip_all, fields(node = facts.node_id.0, addr = %facts.addr))]
pub async fn wait_for_cell<P: Providers, L: CellLedger>(
    providers: P,
    facts: &MachineFacts,
    assignment: usize,
    ledger: &mut L,
    tunables: &DriverTunables,
    shutdown: CancellationToken,
) -> Result<Option<CellPlan>, RunError> {
    let addr = facts.addr.to_string();
    let mut edge = RpcEdge::listen(&providers, &addr, "machine", tunables)
        .await
        .map_err(RunError::Infra)?;
    let rpc = edge.handle().clone();
    let mut identify =
        Inbound::plain(serve_well_known::<P, IdentifyRpc>(&rpc).map_err(RunError::Infra)?);
    let mut form =
        Inbound::plain(serve_well_known::<P, FormCellRpc>(&rpc).map_err(RunError::Infra)?);
    let mut init = Inbound::plain(serve_well_known::<P, InitRpc>(&rpc).map_err(RunError::Infra)?);
    tracing::info!(
        node = facts.node_id.0,
        seed = facts.is_seed(),
        class = facts.class.as_str(),
        "machine_waiting"
    );
    loop {
        moonpool_core::select! {
            () = shutdown.cancelled() => return Ok(None),
            error = edge.run() => return Err(RunError::Infra(error)),
            Some((_, reply)) = identify.recv() => {
                reply.send(facts.identify_ack());
            }
            Some((request, reply)) = form.recv() => {
                let (ack, formed) = form_cell(facts, ledger, &request).await;
                reply.send(ack);
                if let Some(plan) = formed {
                    flush(&providers, &mut edge).await;
                    return Ok(Some(plan));
                }
            }
            Some((_, reply)) = init.recv() => {
                // The cell step calls the other seeds: the runtime must keep
                // running while it does.
                let outcome = moonpool_core::select! {
                    error = edge.run() => return Err(RunError::Infra(error)),
                    outcome = run_init(&providers, &rpc, facts, assignment, ledger, tunables) => outcome,
                };
                match outcome {
                    Ok(plan) => {
                        tracing::info!(cell = plan.cell_id, members = plan.members.len(), "cell_initialized");
                        reply.send(plan.init_ack());
                        flush(&providers, &mut edge).await;
                        return Ok(Some(plan));
                    }
                    Err(refusal) => {
                        tracing::warn!(refusal, "init_refused");
                        reply.send(wire::InitAck {
                            refusal: refusal.into(),
                            ..wire::InitAck::default()
                        });
                    }
                }
            }
        }
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

/// Answer one `FormCell`: the ack, and the plan when this machine formed it.
async fn form_cell<L: CellLedger>(
    facts: &MachineFacts,
    ledger: &mut L,
    request: &wire::FormCell,
) -> (wire::FormCellAck, Option<CellPlan>) {
    let refuse = |refusal: &str| {
        (
            wire::FormCellAck {
                formed: false,
                refusal: refusal.into(),
            },
            None,
        )
    };
    if facts.class != Class::Storage {
        return refuse("stateless");
    }
    let Ok(plan) = CellPlan::from_form(request) else {
        return refuse("malformed");
    };
    if !plan.members.contains(&(facts.node_id, facts.addr)) {
        return refuse("not_a_member");
    }
    if ledger
        .pending()
        .is_some_and(|pending| pending.cell_id != plan.cell_id)
    {
        return refuse("other_cell");
    }
    if let Err(error) = ledger.form(&plan).await {
        tracing::error!(%error, "cell_form_failed");
        return refuse("storage");
    }
    tracing::info!(cell = plan.cell_id, "cell_formed");
    (
        wire::FormCellAck {
            formed: true,
            refusal: String::new(),
        },
        Some(plan),
    )
}

/// The cell step of `init`, run by the seed it was sent to: the formed plan,
/// or the refusal's label.
async fn run_init<P: Providers, L: CellLedger>(
    providers: &P,
    rpc: &RpcHandle<P>,
    facts: &MachineFacts,
    assignment: usize,
    ledger: &mut L,
    tunables: &DriverTunables,
) -> Result<CellPlan, &'static str> {
    if !facts.is_seed() {
        return Err("not_a_seed");
    }
    if facts.class != Class::Storage {
        return Err("stateless_seed");
    }
    let time = providers.time().clone();
    let patience = tunables.connection_timeout;
    let plan = if let Some(plan) = ledger.pending() {
        plan
    } else {
        let mut members = Vec::with_capacity(facts.seeds.len());
        for &seed in &facts.seeds {
            if seed == facts.addr {
                members.push((facts.node_id, seed));
                continue;
            }
            let client = well_known::<P, IdentifyRpc>(rpc, seed);
            match time
                .timeout(patience, client.try_get_reply(&wire::Identify {}))
                .await
            {
                Ok(Ok(ack)) if ack.class == Class::Storage.as_str() => {
                    members.push((NodeId(ack.node_id), seed));
                }
                Ok(Ok(_)) => return Err("stateless_seed"),
                // A seed that serves a cell already, while this one waits
                // with no plan: this machine is new (a wiped volume mints a
                // new identity) and never forms a second cell beside it.
                _ if serves_cell(&time, rpc, seed, patience).await => {
                    return Err("cell_exists");
                }
                _ => return Err("seed_unreachable"),
            }
        }
        members.sort_unstable();
        let cell_id = loop {
            let id: u64 = providers.random().random();
            if id != 0 {
                break id;
            }
        };
        // Every identifier is drawn (§3.8): the cell tenant's control journal,
        // the fleet tenant's (this cell hosts it: the fleet's first), and the static
        // user journals under one drawn user tenant.
        let control = draw_identifier(providers);
        let fleet = draw_identifier(providers);
        let users = draw_identifier(providers).tenant;
        let mut journals: BTreeSet<JournalIdentifier> = [control, fleet].into_iter().collect();
        while journals.len() < 2 + assignment {
            journals.insert(JournalIdentifier::new(
                users,
                draw_identifier(providers).journal,
            ));
        }
        let plan = CellPlan {
            cell_id,
            members,
            control,
            fleet: Some(fleet),
            journals: journals.into_iter().collect(),
        };
        plan.check().map_err(|_| "malformed")?;
        ledger.record_pending(&plan).await.map_err(|error| {
            tracing::error!(%error, "cell_pending_record_failed");
            "storage"
        })?;
        plan
    };
    // Every other seed first, this one last: a seed that formed is serving
    // the cell, so a re-run that finds this one still waiting resumes.
    let request = plan.form_request();
    for &(id, seed) in &plan.members {
        if id == facts.node_id {
            continue;
        }
        let client = well_known::<P, FormCellRpc>(rpc, seed);
        match time.timeout(patience, client.try_get_reply(&request)).await {
            Ok(Ok(ack)) if ack.formed => {}
            Ok(Ok(ack)) if ack.refusal == "other_cell" => return Err("other_cell"),
            Ok(Ok(_)) => return Err("seed_unreachable"),
            // No machine endpoint there: a seed an earlier run formed serves
            // the cell control journal instead.
            _ => {
                if !serves_cell(&time, rpc, seed, patience).await {
                    return Err("seed_unreachable");
                }
            }
        }
    }
    ledger.form(&plan).await.map_err(|error| {
        tracing::error!(%error, "cell_form_failed");
        "storage"
    })?;
    Ok(plan)
}

/// Whether the machine at `seed` serves a cell: it answers a node-only
/// `Inspect` (a waiting machine serves no `Inspect` at all) naming a cell. A
/// machine that has no plan cannot know any identifier to ask for: no
/// identifier is fixed (§3.8).
async fn serves_cell<P: Providers>(
    time: &P::Time,
    rpc: &RpcHandle<P>,
    seed: SocketAddr,
    patience: Duration,
) -> bool {
    let client = well_known::<P, InspectRpc>(rpc, seed);
    let request = InspectRequest::node_only();
    matches!(
        time.timeout(patience, client.try_get_reply(&request)).await,
        Ok(Ok(reply)) if reply.cell_id != 0
    )
}
