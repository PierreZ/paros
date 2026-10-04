//! The machine before its cell (#196, #216): what an uninitialized `parosd`
//! does until `parosctl init` forms the cell.
//!
//! Every `parosd` starts the same way: with an identity minted at format —
//! its random `node_id` (#225), so a wiped disk is a new machine and "a wiped
//! identity never rejoins" holds by construction — and its rendezvous join
//! list, resolved to the cell's **seeds**. It never forms a cell on its own
//! (`docs/architecture.md` §3.1, `CockroachDB`'s `cockroach init`): it waits,
//! serving the machine contract (`proto/machine.proto`), until one `Init` is
//! sent to a seed that every seed's join list names.
//!
//! - **`Identify`** — who this machine is: its id, class, capacity, failure
//!   domain, address, and whether its own address is in its join list.
//! - **`Init`** (a seed's) — the cell step of `init`: identify every seed,
//!   mint the cell's id, record the plan as *pending* durably, form every
//!   other seed, then form this one last. A crash at any step is resumed by
//!   running `init` again: the pending plan is reused, never redrawn, and a
//!   seed already serving the cell counts as formed — but a seed with no
//!   plan that finds another seed serving a cell is refused (`cell_exists`):
//!   it is a new machine (a wiped volume), and never forms a second cell.
//!   Claiming the cell
//!   control journal with `SetLeader(expected_gen = 0)` is the caller's next
//!   step, on the formed cell (`parosctl init`).
//! - **`FormCell`** — join the cell a plan names: format its journals and
//!   record the plan durably ([`CellLedger::form`]); idempotent for the same
//!   plan. The machine then stops waiting, and its caller starts serving
//!   the plan's journals.
//!
//! The plan is a [`CellPlan`]: the cell's id, its bootstrap members (every
//! seed, by id and address) and the journals they serve from formation —
//! the cell control journal ([`CELL_CONTROL`]), meta's control journal
//! ([`META_CONTROL`], #229: the fleet's directory, placed on the seeds at
//! bootstrap, §3.1) and the static assignment that stands in for placement
//! until #212. Its first coordinator, the
//! one that claims the cell control journal, is the lowest member id
//! ([`CellPlan::coordinator`]) until the cell coordinator of #225.
//!
//! Provider-generic like every driver here, so the simulation can run it
//! when the cell's bootstrap joins the campaign; today only `parosd` does
//! (#216). Durability is the caller's ([`CellLedger`]): the library never
//! touches a path.

use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::time::Duration;

use moonpool_core::{Providers, RandomProvider, TimeProvider};
use moonpool_rpc::RpcHandle;
use paros_core::{JournalId, JournalKey, NodeId, TenantId};
use tokio_util::sync::CancellationToken;

use crate::driver::edge::RpcEdge;
use crate::driver::{DriverTunables, RunError};
use crate::rpc::machine as wire;
use crate::rpc::methods::{FormCellRpc, IdentifyRpc, InitRpc, ReadRpc};
use crate::rpc::{Inbound, Read, serve_well_known, well_known};

/// The cell tenant's control journal (`2/1`): the machine registry and the
/// capacity bookings (`docs/architecture.md` §3.1), claimed by the first
/// cell coordinator at `init`.
pub const CELL_CONTROL: JournalKey = JournalKey::control(TenantId::CELL);

/// The meta tenant's control journal (`1/1`, #229): the fleet's directory,
/// on the seeds from formation (`docs/architecture.md` §3.1).
pub const META_CONTROL: JournalKey = JournalKey::control(TenantId::META);

/// How long a waiting machine keeps its listener up after the answer that
/// ends its wait, so the answer leaves before the listener closes.
const FLUSH: Duration = Duration::from_millis(250);

/// A machine's class (FDB's process classes, `docs/architecture.md` §3.2).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Class {
    /// Anything with a durable store: acceptors, replicas, matchmakers.
    Storage,
    /// The front door, proxy leaders, batchers, coordinators.
    Stateless,
}

impl Class {
    /// The class's name on the wire and in configuration.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Class::Storage => "storage",
            Class::Stateless => "stateless",
        }
    }
}

impl core::str::FromStr for Class {
    type Err = &'static str;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        match text {
            "storage" => Ok(Class::Storage),
            "stateless" => Ok(Class::Stateless),
            _ => Err("a class is `storage` or `stateless`"),
        }
    }
}

/// What a machine knows of itself before it has a cell.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MachineFacts {
    /// Its identity, random, minted at format (#225).
    pub node_id: NodeId,
    /// Its class.
    pub class: Class,
    /// Its capacity, in the placement's units (opaque here).
    pub capacity: u64,
    /// Its failure domain (opaque here).
    pub failure_domain: String,
    /// The address it serves at, which its peers dial.
    pub addr: SocketAddr,
    /// Its rendezvous join list, resolved: the cell's seeds.
    pub seeds: Vec<SocketAddr>,
}

impl MachineFacts {
    /// Whether this machine is a seed: its own address is in its join list.
    #[must_use]
    pub fn is_seed(&self) -> bool {
        self.seeds.contains(&self.addr)
    }

    fn identify_ack(&self) -> wire::IdentifyAck {
        wire::IdentifyAck {
            node_id: self.node_id.0,
            class: self.class.as_str().into(),
            capacity: self.capacity,
            failure_domain: self.failure_domain.clone(),
            addr: self.addr.to_string(),
            seed: self.is_seed(),
        }
    }
}

/// A cell's bootstrap: what every seed records at formation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CellPlan {
    /// The cell's id, random, minted at `init` (#226).
    pub cell_id: u64,
    /// The bootstrap members — every seed — by id and address, in id order.
    pub members: Vec<(NodeId, SocketAddr)>,
    /// The journals every member serves from formation, in frame order.
    pub journals: Vec<JournalKey>,
}

impl CellPlan {
    /// The first cell coordinator: the lowest member id (until #225's
    /// coordinator election).
    ///
    /// # Panics
    ///
    /// On a plan with no member (a [`CellPlan::check`]ed plan has one).
    #[must_use]
    pub fn coordinator(&self) -> NodeId {
        self.members
            .iter()
            .map(|(id, _)| *id)
            .min()
            .expect("a checked plan names a member")
    }

    /// Whether the plan is one a machine may form: a set cell id, at least
    /// one member, member ids and addresses unique, every journal frame set
    /// and unique, the cell control journal and meta's among them.
    ///
    /// # Errors
    ///
    /// The first thing wrong with it.
    pub fn check(&self) -> Result<(), &'static str> {
        if self.cell_id == 0 {
            return Err("a cell id is non-zero");
        }
        if self.members.is_empty() {
            return Err("a cell has at least one member");
        }
        let ids: BTreeSet<NodeId> = self.members.iter().map(|(id, _)| *id).collect();
        let addrs: BTreeSet<SocketAddr> = self.members.iter().map(|(_, a)| *a).collect();
        if ids.len() != self.members.len() || addrs.len() != self.members.len() {
            return Err("a cell's members are unique by id and by address");
        }
        let frames: BTreeSet<JournalKey> = self.journals.iter().copied().collect();
        if frames.len() != self.journals.len() || self.journals.iter().any(|j| !j.is_set()) {
            return Err("a cell's journals are set and unique");
        }
        if !frames.contains(&CELL_CONTROL) || !frames.contains(&META_CONTROL) {
            return Err("a cell serves its control journal and meta's");
        }
        Ok(())
    }

    fn members_to_wire(&self) -> Vec<wire::Member> {
        self.members
            .iter()
            .map(|(id, addr)| wire::Member {
                node_id: id.0,
                addr: addr.to_string(),
            })
            .collect()
    }

    fn journals_to_wire(&self) -> Vec<wire::Frame> {
        self.journals
            .iter()
            .map(|key| wire::Frame {
                tenant: key.tenant.0,
                journal: key.journal.0,
            })
            .collect()
    }

    /// The plan a `FormCell` carries, normalized (members by id, journals
    /// by frame) and checked.
    ///
    /// # Errors
    ///
    /// An address that does not parse, or a plan [`CellPlan::check`]
    /// refuses.
    pub fn from_wire(
        cell_id: u64,
        members: &[wire::Member],
        journals: &[wire::Frame],
    ) -> Result<Self, &'static str> {
        let mut members = members
            .iter()
            .map(|m| {
                m.addr
                    .parse()
                    .map(|addr| (NodeId(m.node_id), addr))
                    .map_err(|_| "a member's address is HOST:PORT, resolved")
            })
            .collect::<Result<Vec<_>, _>>()?;
        members.sort_unstable();
        let mut journals: Vec<JournalKey> = journals
            .iter()
            .map(|f| JournalKey::new(TenantId(f.tenant), JournalId(f.journal)))
            .collect();
        journals.sort_unstable();
        let plan = Self {
            cell_id,
            members,
            journals,
        };
        plan.check()?;
        Ok(plan)
    }

    fn form_request(&self) -> wire::FormCell {
        wire::FormCell {
            cell_id: self.cell_id,
            members: self.members_to_wire(),
            journals: self.journals_to_wire(),
        }
    }

    fn init_ack(&self) -> wire::InitAck {
        wire::InitAck {
            initialized: true,
            refusal: String::new(),
            cell_id: self.cell_id,
            members: self.members_to_wire(),
            journals: self.journals_to_wire(),
            coordinator: self.coordinator().0,
        }
    }
}

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
    fn record_pending(&mut self, plan: &CellPlan) -> Result<(), String>;

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
/// `shutdown`. `assignment` is the journals a cell this machine initializes
/// serves beside its control journal (the static assignment, until #212).
///
/// # Errors
///
/// The listener could not bind, or the runtime failed, as
/// [`RunError::Infra`].
#[tracing::instrument(level = "debug", skip_all, fields(node = facts.node_id.0, addr = %facts.addr))]
pub async fn wait_for_cell<P: Providers, L: CellLedger>(
    providers: P,
    facts: &MachineFacts,
    assignment: &[JournalKey],
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
    let Ok(plan) = CellPlan::from_wire(request.cell_id, &request.members, &request.journals) else {
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
    assignment: &[JournalKey],
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
        let mut journals: Vec<JournalKey> = assignment
            .iter()
            .copied()
            .chain([CELL_CONTROL, META_CONTROL])
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        journals.sort_unstable();
        let plan = CellPlan {
            cell_id,
            members,
            journals,
        };
        plan.check().map_err(|_| "malformed")?;
        ledger.record_pending(&plan).map_err(|error| {
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

/// Whether the machine at `seed` serves the cell control journal.
async fn serves_cell<P: Providers>(
    time: &P::Time,
    rpc: &RpcHandle<P>,
    seed: SocketAddr,
    patience: Duration,
) -> bool {
    let client = well_known::<P, ReadRpc>(rpc, seed);
    let read = Read {
        journal: CELL_CONTROL.journal.0,
        tenant: CELL_CONTROL.tenant.0,
        from_seq: 0,
        limit: 1,
        wait_ms: 0,
    };
    matches!(
        time.timeout(patience, client.try_get_reply(&read)).await,
        Ok(Ok(ack)) if !ack.unknown_journal
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(port: u16) -> SocketAddr {
        SocketAddr::from(([10, 0, 0, 1], port))
    }

    fn plan() -> CellPlan {
        CellPlan {
            cell_id: 7,
            members: vec![(NodeId(9), addr(1)), (NodeId(3), addr(2))],
            journals: vec![CELL_CONTROL, META_CONTROL, JournalKey::default()],
        }
    }

    #[test]
    fn a_plan_round_trips_normalized_and_names_its_coordinator() {
        let plan = plan();
        assert_eq!(plan.check(), Ok(()));
        assert_eq!(plan.coordinator(), NodeId(3));
        let wire = plan.form_request();
        let back = CellPlan::from_wire(wire.cell_id, &wire.members, &wire.journals)
            .expect("a checked plan decodes");
        assert_eq!(
            back.members,
            vec![(NodeId(3), addr(2)), (NodeId(9), addr(1))]
        );
        assert_eq!(
            back.journals,
            vec![META_CONTROL, CELL_CONTROL, JournalKey::default()]
        );
    }

    #[test]
    fn a_malformed_plan_is_refused() {
        let mut no_cell = plan();
        no_cell.cell_id = 0;
        assert!(no_cell.check().is_err());
        let mut twice = plan();
        twice.members.push((NodeId(9), addr(3)));
        assert!(twice.check().is_err());
        let mut no_control = plan();
        no_control.journals = vec![JournalKey::default()];
        assert!(no_control.check().is_err());
        assert!("cloud".parse::<Class>().is_err());
        assert_eq!("storage".parse::<Class>(), Ok(Class::Storage));
    }
}
