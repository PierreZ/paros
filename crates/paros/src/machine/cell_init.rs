//! `cell init`'s proposer (#277): the machine a `CellInit` reaches drives
//! the single-decree Paxos on the cell plan over the listed machines — every
//! one an acceptor — through paros-core's [`Decree`], over the network, its
//! own acceptor included (served by the waiting loop,
//! [`super::wait_for_cell`]).
//!
//! **Quorums: every member in Phase 1, a majority in Phase 2** (#246). Every
//! listed machine must answer the ask, so every vote still on a disk is heard
//! and adopted (P2c). A majority of accepts chooses the plan, so a founding
//! member that can never accept does not block it. That member is a wiped
//! one: its disk is gone, and the machine at its address is a new one with a
//! new `node_id`. The new machine refuses a plan that names the old one
//! (`not_a_member`), so it never votes as the old one, and the old id stays
//! a dead member of the cell until a reconfiguration replaces it. The wiped
//! machine lost its promises (amnesia), but it answers Phase 1 only as a new
//! acceptor with no vote. Every Phase-1 quorum (all members) meets every
//! Phase-2 quorum (a majority) in a member that kept its disk while fewer
//! than a majority of the members are wiped. When a majority of the plan's
//! members are wiped, no plan can be chosen any more: the receiver refuses
//! `cell_lost`, the Paxos limit and not a gap.
//!
//! Every ballot this machine opens lies strictly above its own durable
//! promise and above any promise that refused an earlier one: a ballot that
//! reached a `FormCell` was first promised here (the receiver is one of its
//! own acceptors), so one ballot never carries two plans across `cell init`
//! runs.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use moonpool_core::{Detach, Providers, RandomProvider, TaskProvider, TimeProvider};
use moonpool_rpc::RpcHandle;
use paros_core::decree::{AcceptFold, DecreePromise};
use paros_core::{
    AcceptorConfig, Ballot, Decree, Fingerprint, JournalId, JournalIdentifier, NodeId,
    QuorumSystem, TenantId,
};
use tokio::sync::mpsc;

use super::formed::vote_ballot;
use super::{CellPlan, Class, MachineFacts, ballot_from_wire, ballot_to_wire};
use crate::driver::DriverTunables;
use crate::rpc::machine as wire;
use crate::rpc::methods::{FormCellRpc, PrepareCellRpc, WellKnownMethod};
use crate::rpc::well_known;
use crate::{Address, Names};

/// A random identifier: a random tenant and a random journal, both set (no id
/// is fixed, `docs/architecture.md` §3.8).
fn draw_identifier<P: Providers>(providers: &P) -> JournalIdentifier {
    JournalIdentifier::new(
        TenantId(draw_nonzero(providers)),
        JournalId(draw_nonzero(providers)),
    )
}

/// A random non-zero `u64`.
fn draw_nonzero<P: Providers>(providers: &P) -> u64 {
    loop {
        let id: u64 = providers.random().random();
        if id != 0 {
            break id;
        }
    }
}

/// How many ballots one `CellInit` opens before it answers `contended`: a
/// preempted decree reopens above the promise that refused it, and a
/// concurrent `cell init` that keeps outbidding it is the caller's to retry.
/// A schedule ceiling, not a tunable: the client re-sends `CellInit` while
/// its patience lasts, so the bound only decides who retries, and any value
/// of at least one is live.
const DECREE_BALLOTS: usize = 4;

/// The spread of a reopened ballot above the promise that refused it: a
/// random step, so two receivers outbidding each other part.
const REOPEN_SPREAD: u64 = 1 << 16;

/// How one ballot of the decree ended.
enum Attempt {
    /// The plan is chosen: every member accepted it.
    Chosen(CellPlan),
    /// A member promised above this ballot: reopen above it.
    Preempted(Ballot),
    /// Fewer accepts than a quorum, with no higher promise: a member was
    /// wiped between the two phases. Reopen above this ballot, whose ask
    /// meets the new machine.
    Short,
    /// Refused, with the label the caller sees.
    Failed(&'static str),
}

/// `cell init`, driven by the machine it was sent to: the decree over the
/// listed machines, opened above `promised` (this machine's own durable
/// promise) and reopened above a preempting promise up to
/// [`DECREE_BALLOTS`] times. The chosen plan, or the refusal's label.
#[tracing::instrument(level = "debug", skip_all, fields(node = facts.node_id.0))]
pub(super) async fn propose<P: Providers>(
    providers: &P,
    rpc: &RpcHandle<P>,
    facts: &MachineFacts,
    promised: Ballot,
    assignment: usize,
    request: wire::CellInit,
    tunables: &DriverTunables,
) -> Result<CellPlan, &'static str> {
    let members: BTreeSet<Address> = request
        .members
        .iter()
        .map(|addr| Address::parse(addr))
        .collect::<Result<_, _>>()
        .map_err(|_| "malformed")?;
    if members.is_empty() {
        return Err("malformed");
    }
    if !members.contains(&facts.addr) {
        return Err("not_a_member");
    }
    if facts.class != Class::Storage {
        return Err("stateless_member");
    }
    let members: Vec<Address> = members.into_iter().collect();
    // Above every ballot this machine ever opened that got as far as a
    // `FormCell`: each was promised here first.
    let mut floor: Option<Ballot> = (promised != Ballot::default()).then_some(promised);
    for _ in 0..DECREE_BALLOTS {
        let Some(ballot) = draw_ballot(providers, facts.node_id, floor) else {
            return Err("contended");
        };
        assert!(
            floor.is_none_or(|floor| ballot > floor),
            "a reopened decree opens above the promise that refused it"
        );
        match attempt(
            providers, rpc, facts, assignment, &members, ballot, tunables,
        )
        .await
        {
            Attempt::Chosen(plan) => {
                assert!(
                    plan.addrs().into_iter().eq(members.iter().cloned()),
                    "the chosen plan is over the listed machines"
                );
                return Ok(plan);
            }
            Attempt::Preempted(promise) => {
                assert!(promise > ballot, "only a higher promise preempts");
                floor = Some(floor.map_or(promise, |held| held.max(promise)));
            }
            Attempt::Short => {
                assert!(
                    floor.is_none_or(|held| held < ballot),
                    "a short ballot lies above the floor it opened over"
                );
                floor = Some(ballot);
            }
            Attempt::Failed(label) => return Err(label),
        }
    }
    Err("contended")
}

/// A fresh decree ballot for `node`: a random `init_id` while nothing bounds
/// it, else a random step above `floor` (this machine's promise, or a
/// promise that refused an earlier ballot).
fn draw_ballot<P: Providers>(providers: &P, node: NodeId, floor: Option<Ballot>) -> Option<Ballot> {
    let round = match floor {
        None => draw_nonzero(providers),
        Some(floor) => {
            let step: u64 = providers.random().random();
            floor.round.checked_add(1 + step % REOPEN_SPREAD)?
        }
    };
    Some(Ballot { round, node })
}

/// One ballot of the decree: ask every member, adopt a reported plan or
/// draw one, then form every other member and this one last.
#[allow(clippy::too_many_lines)]
#[tracing::instrument(level = "debug", skip_all, fields(round = ballot.round))]
async fn attempt<P: Providers>(
    providers: &P,
    rpc: &RpcHandle<P>,
    facts: &MachineFacts,
    assignment: usize,
    members: &[Address],
    ballot: Ballot,
    tunables: &DriverTunables,
) -> Attempt {
    let patience = tunables.connection_timeout;
    let ask = wire::PrepareCell {
        init: Some(ballot_to_wire(ballot)),
    };
    let answers =
        fan_out::<P, PrepareCellRpc>(providers, rpc, &facts.names, members, &ask, patience).await;
    // Who answered where: a listed address and the machine there now.
    let mut identities: BTreeMap<Address, NodeId> = BTreeMap::new();
    let mut promises = Vec::with_capacity(answers.len());
    for (addr, answer) in answers {
        let Some(ack) = answer else {
            return Attempt::Failed("member_unreachable");
        };
        match ack.refusal.as_str() {
            "" => {}
            "stateless" => return Attempt::Failed("stateless_member"),
            // A machine a cell admitted (#216): it is in a cell already.
            "in_cell" => return Attempt::Failed("cell_exists"),
            // That member's disk failed under it: nothing was decided.
            _ => return Attempt::Failed("member_unreachable"),
        }
        let Some(identity) = &ack.identity else {
            return Attempt::Failed("malformed");
        };
        if identity.class != Class::Storage.as_str() {
            return Attempt::Failed("stateless_member");
        }
        if identity.node_id == 0 {
            return Attempt::Failed("malformed");
        }
        identities.insert(addr, NodeId(identity.node_id));
        promises.push((NodeId(identity.node_id), ack));
    }
    let ids: BTreeSet<NodeId> = identities.values().copied().collect();
    if ids.len() != members.len() {
        // One machine answering at two listed addresses.
        return Attempt::Failed("malformed");
    }
    let Some(proposal) = draw_plan(providers, &identities, assignment) else {
        return Attempt::Failed("malformed");
    };
    let n = members.len();
    let q2 = n / 2 + 1;
    // Every listed machine is an acceptor: all of them answer Phase 1, a
    // majority chooses (see the module doc).
    let acceptors = AcceptorConfig::new(
        ids.into_iter().collect(),
        QuorumSystem::Flexible { q1: n, q2 },
    );
    assert!(
        acceptors.quorum_system().phase2_quorum_size(n) == q2,
        "a majority of the listed machines chooses the plan"
    );
    // A vote for a plan over another list is another cell's decree, never
    // this one's (#216): a machine of another cell answers at an address a
    // wiped member left. It voted once, there, so it never accepts here; its
    // vote is not adopted and its ballot preempts nothing.
    let ours = |plan: &CellPlan| plan.addrs().into_iter().eq(members.iter().cloned());
    // Votes are wire input: two that disagree at one ballot would break the
    // decree's own agreement rule, so they are refused here as malformed.
    let mut seen: BTreeMap<Ballot, u64> = BTreeMap::new();
    let mut foreign = 0_usize;
    for (_, ack) in &promises {
        let Some(vote) = &ack.vote else { continue };
        let (Ok(plan), Some(voted)) = (CellPlan::from_form(vote), vote_ballot(vote)) else {
            return Attempt::Failed("malformed");
        };
        if !ours(&plan) {
            foreign += 1;
            continue;
        }
        if *seen.entry(voted).or_insert(plan.fingerprint()) != plan.fingerprint() {
            return Attempt::Failed("malformed");
        }
    }
    let mut decree = Decree::new(ballot, acceptors, proposal);
    let mut selected = None;
    for (id, ack) in promises {
        if !ack.promised {
            match ballot_from_wire(ack.promise.as_ref()) {
                Some(promise) if promise > ballot => decree.on_nack(promise),
                _ => return Attempt::Failed("malformed"),
            }
            continue;
        }
        let vote = match &ack.vote {
            None => None,
            Some(vote) => {
                let (Ok(plan), Some(voted)) = (CellPlan::from_form(vote), vote_ballot(vote)) else {
                    return Attempt::Failed("malformed");
                };
                // Another cell's vote (see above): no vote in this decree.
                if ours(&plan) {
                    // A formed member answers every ballot with its vote,
                    // which may lie above this one: reopen above it.
                    if voted > ballot {
                        decree.on_nack(voted);
                        continue;
                    }
                    if voted == ballot {
                        // This ballot is this receiver's alone, and fresh.
                        return Attempt::Failed("malformed");
                    }
                    Some((voted, plan))
                } else {
                    None
                }
            }
        };
        if let DecreePromise::Quorum(value) = decree.on_promise(id, vote) {
            selected = Some(value);
        }
    }
    if let Some(promise) = decree.preempted() {
        return Attempt::Preempted(promise);
    }
    let Some(plan) = selected else {
        return Attempt::Failed("malformed");
    };
    if decree.adopted_prior_vote() {
        tracing::info!(cell = plan.cell_id, "cell_init_adopts_a_plan");
    } else if foreign > 0 {
        // No vote of this list's, and a listed machine is in another cell
        // already: the list is not one cell's, and that machine never forms
        // a fresh plan.
        return Attempt::Failed("other_cell_init");
    }
    if foreign > 0 {
        moonpool_assertions::reachable!(
            "cell init: a decree passes over another cell's vote at a wiped member's address"
        );
    }
    // Adopt or refuse (#277): another list's plan is another cell's.
    if !plan.addrs().into_iter().eq(members.iter().cloned()) {
        return Attempt::Failed("other_cell_init");
    }
    // A listed address that now hosts another machine than the plan names
    // is a wiped member (#246): a new machine, which never accepts the plan
    // as the old one. The others choose it while they are a quorum.
    let intact: BTreeSet<Address> = plan
        .members
        .iter()
        .filter(|(id, addr)| identities.get(addr) == Some(id))
        .map(|(_, addr)| addr.clone())
        .collect();
    if intact.len() < q2 {
        return Attempt::Failed("cell_lost");
    }
    if intact.len() < n {
        tracing::info!(
            cell = plan.cell_id,
            wiped = n - intact.len(),
            "cell_init_around_wiped"
        );
    }
    // Every other member first, this one last: a receiver formed is a
    // receiver that stopped driving, so it accepts once the others did. A
    // receiver the plan does not name (it replaced a wiped member) never
    // accepts it.
    let form = plan.form_request(ballot);
    let others: Vec<Address> = members
        .iter()
        .filter(|addr| **addr != facts.addr && intact.contains(*addr))
        .cloned()
        .collect();
    let mut accepted = 0_usize;
    let mut chosen = None;
    for batch in [others, vec![facts.addr.clone()]] {
        if batch == [facts.addr.clone()] {
            // This receiver votes only for a plan the others can still
            // choose with it: a vote is final, and a lone vote for a plan
            // that names a wiped member would keep it for good.
            if !intact.contains(&facts.addr) || accepted + 1 < q2 {
                break;
            }
        }
        let answers =
            fan_out::<P, FormCellRpc>(providers, rpc, &facts.names, &batch, &form, patience).await;
        for (addr, answer) in answers {
            let Some(ack) = answer else {
                return Attempt::Failed("member_unreachable");
            };
            if ack.formed {
                accepted += 1;
                if let AcceptFold::Chosen(value) = decree.on_accepted(identities[&addr]) {
                    chosen = Some(value);
                }
                continue;
            }
            match ack.refusal.as_str() {
                "promised_higher" => match ballot_from_wire(ack.promise.as_ref()) {
                    Some(promise) if promise > ballot => decree.on_nack(promise),
                    _ => return Attempt::Failed("malformed"),
                },
                "other_cell" => return Attempt::Failed("other_cell_init"),
                // The machine there is not the one the plan names: a member
                // wiped between the two phases, a new machine. It counts as
                // no accept.
                "not_a_member" => {}
                "storage" => return Attempt::Failed("member_unreachable"),
                _ => return Attempt::Failed("malformed"),
            }
        }
        if let Some(promise) = decree.preempted() {
            return Attempt::Preempted(promise);
        }
    }
    if let Some(value) = chosen {
        assert!(value == plan, "the decree chooses the plan it proposed");
        assert!(accepted >= q2, "a chosen plan has a quorum of accepts");
        Attempt::Chosen(value)
    } else {
        assert!(accepted < q2, "a quorum of accepts chooses the plan");
        Attempt::Short
    }
}

/// A fresh plan over the machines that answered: the cell's id and every
/// identifier drawn (§3.8) — the cell tenant's control journal, the fleet
/// tenant's (this cell hosts it: the fleet's first), and the static user
/// journals under one drawn user tenant.
fn draw_plan<P: Providers>(
    providers: &P,
    identities: &BTreeMap<Address, NodeId>,
    assignment: usize,
) -> Option<CellPlan> {
    let mut members: Vec<(NodeId, Address)> = identities
        .iter()
        .map(|(addr, id)| (*id, addr.clone()))
        .collect();
    members.sort_unstable();
    let control = draw_identifier(providers);
    let election = JournalIdentifier::new(control.tenant, draw_identifier(providers).journal);
    let fleet = draw_identifier(providers);
    let users = draw_identifier(providers).tenant;
    let mut journals: BTreeSet<JournalIdentifier> =
        [control, election, fleet].into_iter().collect();
    while journals.len() < 3 + assignment {
        journals.insert(JournalIdentifier::new(
            users,
            draw_identifier(providers).journal,
        ));
    }
    let plan = CellPlan {
        cell_id: draw_nonzero(providers),
        members,
        control,
        fleet: Some(fleet),
        election,
        journals: journals.into_iter().collect(),
    };
    plan.check().ok().map(|()| plan)
}

/// Send `request` to every address at once, one at-most-once attempt each
/// within `patience`, and collect every answer in address order: `None`
/// where nothing came back. Each address is resolved as it is dialed
/// (#257): a name that does not resolve is an address that did not answer.
async fn fan_out<P: Providers, M: WellKnownMethod>(
    providers: &P,
    rpc: &RpcHandle<P>,
    names: &Names,
    addrs: &[Address],
    request: &M::Request,
    patience: Duration,
) -> Vec<(Address, Option<M::Reply>)>
where
    M::Request: Clone + Send + Sync + 'static,
    M::Reply: Send + 'static,
{
    let (sender, mut answers) = mpsc::channel(addrs.len().max(1));
    for (index, addr) in addrs.iter().enumerate() {
        let rpc = rpc.clone();
        let names = names.clone();
        let addr = addr.clone();
        let time = providers.time().clone();
        let request = request.clone();
        let sender = sender.clone();
        providers
            .task()
            .spawn_task("paros-cell-decree", async move {
                let call = async {
                    let resolved = names.resolve(&addr).await.ok()?;
                    well_known::<P, M>(&rpc, resolved)
                        .try_get_reply(&request)
                        .await
                        .ok()
                };
                let answer = time.timeout(patience, call).await.ok().flatten();
                let _ = sender.send((index, answer)).await;
            })
            .detach();
    }
    drop(sender);
    let mut collected: Vec<(Address, Option<M::Reply>)> =
        addrs.iter().map(|addr| (addr.clone(), None)).collect();
    while let Some((index, answer)) = answers.recv().await {
        collected[index].1 = answer;
    }
    assert!(
        collected.len() == addrs.len(),
        "a fan-out answers every address"
    );
    collected
}
