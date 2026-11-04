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

use moonpool_core::{Detach, LocalTaskProvider, LocalTimeProvider, RandomProvider};
use moonpool_rpc::RpcHandle;
use paros_core::decree::Decree;
use paros_core::decree::{AcceptFold, DecreePromise};
use paros_core::journal::{JournalId, JournalIdentifier, TenantId};
use paros_core::membership::{AcceptorConfig, QuorumSystem};
use paros_core::types::Fingerprint;
use paros_core::{Ballot, NodeId};
use paros_net::runtime::ParosProviders;
use tokio::sync::mpsc;

use super::formed::vote_ballot;
use super::{CellPlan, MachineFacts, ballot_from_wire, ballot_to_wire};
use paros_model::address::Address;
use paros_model::address::Names;
use paros_net::well_known;
use paros_tunables::DriverTunables;
use paros_wire::machine as wire;
use paros_wire::methods::{FormCellRpc, PrepareCellRpc, WellKnownMethod};

/// A random identifier: a random tenant and a random journal, both set (no id
/// is fixed, `docs/architecture/03-8-identifiers-checkpoints-recovery-security.md` §3.8).
fn draw_identifier<P: ParosProviders>(providers: &P) -> JournalIdentifier {
    JournalIdentifier::new(
        TenantId(draw_nonzero(providers)),
        JournalId(draw_nonzero(providers)),
    )
}

/// A random non-zero `u64`: an identifier, a `node_id`, a first ballot.
pub(super) fn draw_nonzero<P: ParosProviders>(providers: &P) -> u64 {
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
pub(super) async fn propose<P: ParosProviders>(
    providers: &P,
    rpc: &RpcHandle<P>,
    facts: &MachineFacts,
    promised: Ballot,
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
    if !facts.class.stores() {
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
            providers,
            rpc,
            facts,
            &members,
            ballot,
            (&request.token, request.idle),
            tunables,
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
fn draw_ballot<P: ParosProviders>(
    providers: &P,
    node: NodeId,
    floor: Option<Ballot>,
) -> Option<Ballot> {
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
#[tracing::instrument(level = "debug", skip_all, fields(round = ballot.round))]
async fn attempt<P: ParosProviders>(
    providers: &P,
    rpc: &RpcHandle<P>,
    facts: &MachineFacts,
    members: &[Address],
    ballot: Ballot,
    (token, idle): (&[u8], bool),
    tunables: &DriverTunables,
) -> Attempt {
    let patience = tunables.connection_timeout;
    let ask = wire::PrepareCell {
        init: Some(ballot_to_wire(ballot)),
        token: token.to_vec(),
    };
    let answers =
        fan_out::<P, PrepareCellRpc>(providers, rpc, &facts.names, members, &ask, patience).await;
    let asked = match judge_asks(answers) {
        Ok(asked) => asked,
        Err(label) => return Attempt::Failed(label),
    };
    let Some(proposal) = draw_plan(providers, &asked.identities, !idle) else {
        return Attempt::Failed("malformed");
    };
    let ids = asked.identities.values().copied().collect();
    let mut decree = decree_over(ballot, ids, proposal);
    let plan = match fold_promises(&mut decree, asked.promises, members) {
        Ok(Phase1::Selected(plan)) => plan,
        Ok(Phase1::Preempted(promise)) => return Attempt::Preempted(promise),
        Err(label) => return Attempt::Failed(label),
    };
    let q2 = majority(members.len());
    let intact = match intact(&plan, &asked.identities, q2) {
        Ok(intact) => intact,
        Err(label) => return Attempt::Failed(label),
    };
    // Every other member first, this one last: a receiver formed is a
    // receiver that stopped driving, so it accepts once the others did. A
    // receiver the plan does not name (it replaced a wiped member) never
    // accepts it.
    let mut form = plan.form_request(ballot);
    form.token = token.to_vec();
    let others: Vec<Address> = members
        .iter()
        .filter(|addr| **addr != facts.addr && intact.contains(*addr))
        .cloned()
        .collect();
    let mut phase2 = Phase2::new(decree, &asked.identities);
    let answers =
        fan_out::<P, FormCellRpc>(providers, rpc, &facts.names, &others, &form, patience).await;
    if let Err(end) = phase2.fold(answers) {
        return end;
    }
    // This receiver votes only for a plan the others can still choose
    // with it: a vote is final, and a lone vote for a plan that names a
    // wiped member would keep it for good.
    if intact.contains(&facts.addr) && phase2.accepted + 1 >= q2 {
        let me = [facts.addr.clone()];
        let answers =
            fan_out::<P, FormCellRpc>(providers, rpc, &facts.names, &me, &form, patience).await;
        if let Err(end) = phase2.fold(answers) {
            return end;
        }
    }
    phase2.end(&plan, q2)
}

/// Phase 2 of one ballot: the decree, the machine at each listed address,
/// and the accepts so far.
struct Phase2<'a> {
    decree: Decree<NodeId, CellPlan>,
    identities: &'a BTreeMap<Address, NodeId>,
    accepted: usize,
    chosen: Option<CellPlan>,
}

impl<'a> Phase2<'a> {
    /// Phase 2 of `decree` over the machines at `identities`, no accept yet.
    fn new(decree: Decree<NodeId, CellPlan>, identities: &'a BTreeMap<Address, NodeId>) -> Self {
        Self {
            decree,
            identities,
            accepted: 0,
            chosen: None,
        }
    }

    /// Fold one batch of `FormCell` answers: the end of the ballot when one
    /// refused or a higher promise preempts it.
    fn fold(&mut self, answers: Vec<(Address, Option<wire::FormCellAck>)>) -> Result<(), Attempt> {
        for (addr, answer) in answers {
            match fold_form(&mut self.decree, self.identities[&addr], answer) {
                Ok(Formed::Accepted(value)) => {
                    self.accepted += 1;
                    self.chosen = self.chosen.take().or(value);
                }
                Ok(Formed::Not) => {}
                Err(label) => return Err(Attempt::Failed(label)),
            }
        }
        match self.decree.preempted() {
            Some(promise) => Err(Attempt::Preempted(promise)),
            None => Ok(()),
        }
    }

    /// How the ballot ended once every batch was folded: `plan` chosen by
    /// a quorum of `q2` accepts, or short of one.
    fn end(self, plan: &CellPlan, q2: usize) -> Attempt {
        if let Some(value) = self.chosen {
            assert!(value == *plan, "the decree chooses the plan it proposed");
            assert!(self.accepted >= q2, "a chosen plan has a quorum of accepts");
            Attempt::Chosen(value)
        } else {
            assert!(self.accepted < q2, "a quorum of accepts chooses the plan");
            Attempt::Short
        }
    }
}

/// A majority of `n` listed machines: the decree's Phase-2 quorum.
fn majority(n: usize) -> usize {
    n / 2 + 1
}

/// The Phase-1 answers, judged: the machine at each listed address, and
/// each machine's answer.
struct Asked {
    identities: BTreeMap<Address, NodeId>,
    promises: Vec<(NodeId, wire::PrepareCellAck)>,
}

/// Judge the answers to the ask, one per listed address: every listed
/// machine answers, as a storage machine with a set id, and no machine
/// answers at two addresses. Else the refusal's label.
fn judge_asks(
    answers: Vec<(Address, Option<wire::PrepareCellAck>)>,
) -> Result<Asked, &'static str> {
    let listed = answers.len();
    let mut identities: BTreeMap<Address, NodeId> = BTreeMap::new();
    let mut promises = Vec::with_capacity(listed);
    for (addr, answer) in answers {
        let ack = answer.ok_or("member_unreachable")?;
        match ack.refusal.as_str() {
            "" => {}
            "stateless" => return Err("stateless_member"),
            // A machine a cell admitted (#216): it is in a cell already.
            "in_cell" => return Err("cell_exists"),
            // A member whose pinned root key refuses the token (#245);
            // else that member's disk failed under it: nothing was decided.
            label => return Err(super::pin::denied(label).unwrap_or("member_unreachable")),
        }
        let identity = ack.identity.as_ref().ok_or("malformed")?;
        if !super::identity_tags(identity).is_some_and(|(class, _)| class.stores()) {
            return Err("stateless_member");
        }
        if identity.node_id == 0 {
            return Err("malformed");
        }
        let id = NodeId(identity.node_id);
        identities.insert(addr, id);
        promises.push((id, ack));
    }
    let ids: BTreeSet<NodeId> = identities.values().copied().collect();
    if ids.len() != listed {
        // One machine answering at two listed addresses.
        return Err("malformed");
    }
    assert!(
        promises.len() == listed,
        "every listed machine answered the ask"
    );
    Ok(Asked {
        identities,
        promises,
    })
}

/// The decree at `ballot` over the machines `ids`, proposing `proposal`:
/// every listed machine is an acceptor, all of them answer Phase 1, and a
/// majority chooses (see the module doc).
fn decree_over(
    ballot: Ballot,
    ids: BTreeSet<NodeId>,
    proposal: CellPlan,
) -> Decree<NodeId, CellPlan> {
    let n = ids.len();
    let q2 = majority(n);
    let acceptors = AcceptorConfig::new(
        ids.into_iter().collect(),
        QuorumSystem::Flexible { q1: n, q2 },
    );
    assert!(
        acceptors.quorum_system().phase2_quorum_size(n) == q2,
        "a majority of the listed machines chooses the plan"
    );
    Decree::new(ballot, acceptors, proposal)
}

/// How Phase 1 ended.
enum Phase1 {
    /// A member promised above this ballot: reopen above it.
    Preempted(Ballot),
    /// The plan this ballot proposes: a vote it adopted, or the drawn one.
    Selected(CellPlan),
}

/// Fold the promises into `decree`, over the listed `members`: the plan
/// this ballot proposes, a preemption, or the refusal's label.
fn fold_promises(
    decree: &mut Decree<NodeId, CellPlan>,
    promises: Vec<(NodeId, wire::PrepareCellAck)>,
    members: &[Address],
) -> Result<Phase1, &'static str> {
    let ballot = decree.ballot();
    let (answers, foreign) = read_votes(promises, members)?;
    let mut selected = None;
    for (id, promised, promise, vote) in answers {
        if !promised {
            match promise {
                Some(promise) if promise > ballot => decree.on_nack(promise),
                _ => return Err("malformed"),
            }
            continue;
        }
        if let Some((voted, _)) = &vote {
            // A formed member answers every ballot with its vote, which may
            // lie above this one: reopen above it.
            if *voted > ballot {
                decree.on_nack(*voted);
                continue;
            }
            if *voted == ballot {
                // This ballot is this receiver's alone, and fresh.
                return Err("malformed");
            }
        }
        if let DecreePromise::Quorum(value) = decree.on_promise(id, vote) {
            selected = Some(value);
        }
    }
    if let Some(promise) = decree.preempted() {
        return Ok(Phase1::Preempted(promise));
    }
    let plan = selected.ok_or("malformed")?;
    if decree.adopted_prior_vote() {
        tracing::info!(cell = plan.cell_id, "cell_init_adopts_a_plan");
    } else if foreign > 0 {
        // No vote of this list's, and a listed machine is in another cell
        // already: the list is not one cell's, and that machine never forms
        // a fresh plan.
        return Err("other_cell_init");
    }
    if foreign > 0 {
        moonpool_assertions::reachable!(
            "cell init: a decree passes over another cell's vote at a wiped member's address"
        );
    }
    assert!(
        over(&plan, members),
        "a selected plan is over the listed machines"
    );
    Ok(Phase1::Selected(plan))
}

/// One member's promise as the decree folds it: who, whether it
/// promised, the promise that refused, and its vote of this list.
type Promise = (NodeId, bool, Option<Ballot>, Option<(Ballot, CellPlan)>);

/// Read the votes the promises report, over the listed `members`: each
/// promise with its vote of this list, and how many listed machines voted
/// in another cell. Else `malformed`.
fn read_votes(
    promises: Vec<(NodeId, wire::PrepareCellAck)>,
    members: &[Address],
) -> Result<(Vec<Promise>, usize), &'static str> {
    // A vote for a plan over another list is another cell's decree, never
    // this one's (#216): a machine of another cell answers at an address a
    // wiped member left. It voted once, there, so it never accepts here; its
    // vote is not adopted and its ballot preempts nothing.
    let ours = |plan: &CellPlan| over(plan, members);
    // Votes are wire input: two that disagree at one ballot would break the
    // decree's own agreement rule, so they are refused here as malformed.
    let mut seen: BTreeMap<Ballot, u64> = BTreeMap::new();
    let mut answers = Vec::with_capacity(promises.len());
    let mut foreign = 0_usize;
    for (id, ack) in promises {
        let vote = match &ack.vote {
            None => None,
            Some(vote) => {
                let (Ok(plan), Some(voted)) = (CellPlan::from_form(vote), vote_ballot(vote)) else {
                    return Err("malformed");
                };
                if ours(&plan) {
                    if *seen.entry(voted).or_insert(plan.fingerprint()) != plan.fingerprint() {
                        return Err("malformed");
                    }
                    Some((voted, plan))
                } else {
                    // Another cell's vote (see above): no vote in this decree.
                    foreign += 1;
                    None
                }
            }
        };
        answers.push((
            id,
            ack.promised,
            ballot_from_wire(ack.promise.as_ref()),
            vote,
        ));
    }
    Ok((answers, foreign))
}

/// Whether `plan` is over the listed `members`.
fn over(plan: &CellPlan, members: &[Address]) -> bool {
    plan.addrs().into_iter().eq(members.iter().cloned())
}

/// The listed addresses whose machine is the one `plan` names. A listed
/// address that now hosts another machine is a wiped member (#246): a new
/// machine, which never accepts the plan as the old one. The others choose
/// it while they are a quorum of `q2`, else the cell is lost.
fn intact(
    plan: &CellPlan,
    identities: &BTreeMap<Address, NodeId>,
    q2: usize,
) -> Result<BTreeSet<Address>, &'static str> {
    let intact: BTreeSet<Address> = plan
        .members
        .iter()
        .filter(|(id, addr)| identities.get(addr) == Some(id))
        .map(|(_, addr)| addr.clone())
        .collect();
    if intact.len() < q2 {
        return Err("cell_lost");
    }
    let n = plan.members.len();
    if intact.len() < n {
        tracing::info!(
            cell = plan.cell_id,
            wiped = n - intact.len(),
            "cell_init_around_wiped"
        );
    }
    Ok(intact)
}

/// What one `FormCell` answer did.
#[derive(Debug, PartialEq)]
enum Formed {
    /// The member accepted; the plan, when its accept chose it.
    Accepted(Option<CellPlan>),
    /// No accept, and nothing refused.
    Not,
}

/// Fold machine `id`'s answer to `FormCell` into `decree`: an accept, a
/// higher promise, no accept, or the refusal's label.
fn fold_form(
    decree: &mut Decree<NodeId, CellPlan>,
    id: NodeId,
    answer: Option<wire::FormCellAck>,
) -> Result<Formed, &'static str> {
    let ack = answer.ok_or("member_unreachable")?;
    if ack.formed {
        return Ok(match decree.on_accepted(id) {
            AcceptFold::Chosen(value) => Formed::Accepted(Some(value)),
            _ => Formed::Accepted(None),
        });
    }
    match ack.refusal.as_str() {
        "promised_higher" => match ballot_from_wire(ack.promise.as_ref()) {
            Some(promise) if promise > decree.ballot() => decree.on_nack(promise),
            _ => return Err("malformed"),
        },
        "other_cell" => return Err("other_cell_init"),
        // The machine there is not the one the plan names: a member wiped
        // between the two phases, a new machine. It counts as no accept.
        "not_a_member" => {}
        "storage" => return Err("member_unreachable"),
        label => return Err(super::pin::denied(label).unwrap_or("malformed")),
    }
    Ok(Formed::Not)
}

/// A fresh plan over the machines that answered: the cell's id and every
/// identifier drawn (§3.8) — the cell tenant's control and election
/// journals, and the fleet tenant's when `universe` (this cell hosts it: the
/// fleet's first; an idle cell, #232 (multiple cells), hosts none). No user
/// journal: a tenant creates its journals through its coordinator (#210).
fn draw_plan<P: ParosProviders>(
    providers: &P,
    identities: &BTreeMap<Address, NodeId>,
    universe: bool,
) -> Option<CellPlan> {
    let mut members: Vec<(NodeId, Address)> = identities
        .iter()
        .map(|(addr, id)| (*id, addr.clone()))
        .collect();
    members.sort_unstable();
    let control = draw_identifier(providers);
    let election = JournalIdentifier::new(control.tenant, draw_identifier(providers).journal);
    let fleet = universe.then(|| draw_identifier(providers));
    let journals: BTreeSet<JournalIdentifier> = [Some(control), Some(election), fleet]
        .into_iter()
        .flatten()
        .collect();
    let plan = CellPlan {
        cell_id: draw_nonzero(providers),
        members,
        control,
        fleet,
        election,
        journals: journals.into_iter().collect(),
    };
    plan.check().ok().map(|()| plan)
}

/// Send `request` to every address at once, one at-most-once attempt each
/// within `patience`, and collect every answer in address order: `None`
/// where nothing came back. Each address is resolved as it is dialed
/// (#257): a name that does not resolve is an address that did not answer.
pub(super) async fn fan_out<P: ParosProviders, M: WellKnownMethod>(
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
            .local_task()
            .spawn_local_task("paros-cell-decree", async move {
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

#[cfg(test)]
mod tests;
