//! `ELECTION` (#240): this operator stands as one more candidate in the
//! cell's election journal, beside the founding members' coordinators,
//! through the library's `paros::client::election` — the code the
//! coordinator runs. A term it wins is served with the coordinator's own
//! duties (`paros::coordinator::cell::serve_term`): it installs its uuid
//! on the cell control journal and finishes the admissions in flight.
//!
//! The candidacy lasts a few steps. On its own BUGGIFY locations a leading
//! candidate hands its term on to a founding member, or resigns; otherwise it
//! stops calling, and the members take over once its lease runs out. The
//! backoff jitter is the run's draw: the library draws none.
//!
//! Every fold it makes feeds the run's term table: one leader per term,
//! whichever fold names it.

use std::collections::BTreeMap;
use std::sync::{Mutex, PoisonError};
use std::time::Duration;

use moonpool_sim::{
    RandomProvider, SimContext, TimeProvider, assert_always, assert_reachable, buggify_with_prob,
};
use paros::client::checkpoint::CheckpointPolicy;
use paros::client::election::{
    Candidate, Election, ElectionFold, ElectionTunables, Step, hand_off, read_election,
};
use paros::coordinator::cell::{TermDuty, serve_term};
use paros::core::NodeId;
use paros::core::journal::JournalIdentifier;
use paros::core::journal::LeaderUuid;

use super::{Cell, FleetOps};

/// The most steps one candidacy takes.
const MAX_STEPS: u64 = 16;

const TERMS_KEY: &str = "paros-election-terms";

/// Hold `fold`'s leader to the run's term table: every fold of `journal`
/// names one leader per term, the same candidate under the same uuid.
pub(in crate::workload) fn note_terms(
    ctx: &SimContext,
    journal: JournalIdentifier,
    fold: &ElectionFold,
) {
    let Some(leader) = fold.leader() else {
        return;
    };
    let terms = crate::state::published_arc(
        ctx.state(),
        &crate::state::journal_key(TERMS_KEY, journal),
        || Mutex::new(BTreeMap::<u64, (u64, LeaderUuid)>::new()),
    );
    let mut terms = terms.lock().unwrap_or_else(PoisonError::into_inner);
    let named = *terms
        .entry(leader.term)
        .or_insert((leader.candidate.id, leader.uuid));
    assert_always!(
        named == (leader.candidate.id, leader.uuid),
        "election: every fold names one leader per term",
        { "term" => leader.term, "candidate" => leader.candidate.id, "first" => named.0 }
    );
}

impl FleetOps {
    /// `ELECTION`: stand as a candidate in the cell's election for a few
    /// steps.
    #[tracing::instrument(level = "debug", skip_all, fields(client = self.client_id))]
    pub(in crate::workload) async fn elect(
        &mut self,
        ctx: &SimContext,
        policy: CheckpointPolicy,
        tunables: ElectionTunables,
        draw: u64,
    ) {
        let Some(cell) = self.learn(ctx).await else {
            assert_reachable!("election: a candidacy finds no cell formed yet");
            return;
        };
        let Some(journal) = cell.journals.election else {
            assert_always!(
                false,
                "election: a formed cell names its election journal",
                { "cell" => cell.journals.cell_id }
            );
            return;
        };
        // Every founding member, reached or not: an admission the term
        // finishes names them all.
        let members: Vec<NodeId> = cell.members.iter().copied().map(NodeId).collect();
        let founders: Vec<(NodeId, paros::model::address::Address)> = cell
            .servers
            .iter()
            .map(|(id, addr)| (NodeId(*id), addr.clone()))
            .collect();
        // Far from every minted machine id: a candidate of its own.
        let me = Candidate {
            id: u64::MAX - self.client_id,
            ..Candidate::default()
        };
        let mut election = Election::new(
            cell.client.clone(),
            journal,
            me,
            self.leader_seeds.next(),
            tunables,
        );
        // The in-flight scenario (#240): an admission stopped after its
        // registration, then a term won, whose duties finish it.
        if self.admitting.is_none() && buggify_with_prob!(0.2) {
            self.register_only(ctx, policy, draw).await;
        }
        let providers = self.connector.providers().clone();
        let rpc = self.connector.rpc().clone();
        let names = self.connector.names().clone();
        let half_lease = u64::try_from(tunables.lease.as_millis() / 2).unwrap_or(0);
        let pace = tunables.renew_every / 4;
        let mut served = None;
        for _ in 0..(4 + draw % (MAX_STEPS - 3)) {
            if ctx.shutdown().is_cancelled() {
                return;
            }
            let jitter = Duration::from_millis(ctx.random().random_range(0..half_lease + 1));
            let mut step = election.step(jitter).await;
            // An admission this operator stopped after its registration is
            // in flight: a term won now finishes it (#240).
            let rogue = if self.admitting.is_some() {
                buggify_with_prob!(0.9)
            } else {
                buggify_with_prob!(0.3)
            };
            if matches!(step, Step::Following { .. }) && rogue {
                // A candidate that does not wait out the lease: it deposes
                // a live leader, which the fence and the deposed leader's
                // own fold must absorb.
                let wait_ms = u64::try_from(tunables.renew_every.as_millis()).unwrap_or(0);
                if election.campaign(wait_ms).await {
                    assert_reachable!("election: a harness candidate deposes a live leader");
                    step = election.step(jitter).await;
                }
            }
            note_terms(ctx, journal, election.fold());
            match step {
                Step::Leading { leader, .. } if served != Some(leader.term) => {
                    let duty = serve_term(
                        &providers,
                        &rpc,
                        &cell.client,
                        &names,
                        (cell.journals, members.clone(), founders.clone()),
                        &leader,
                        policy,
                    )
                    .await;
                    match duty {
                        TermDuty::Served { .. } => {
                            assert_reachable!("election: a harness candidate served a term");
                            served = Some(leader.term);
                        }
                        TermDuty::Refused => {
                            election.resign(None).await;
                        }
                        TermDuty::Unavailable => {}
                    }
                }
                Step::Leading { leader, .. } => {
                    if leave(&mut election, &cell, &founders, &leader, draw).await {
                        return;
                    }
                }
                Step::Deposed { .. } => {
                    assert_reachable!("election: a harness candidate is deposed");
                    served = None;
                }
                Step::Following { .. } => {}
            }
            if ctx.time().sleep(pace).await.is_err() {
                return;
            }
        }
        if election.leading().is_some() {
            // It stops calling: the members take over once its lease runs
            // out.
            assert_reachable!("election: a harness candidate abandons its term");
        }
    }
}

/// A served term's end, on its own BUGGIFY locations: hand it on to a
/// founding member, resign, or keep leading. Whether the candidacy ends.
async fn leave(
    election: &mut Election<moonpool_sim::SimProviders>,
    cell: &Cell,
    founders: &[(NodeId, paros::model::address::Address)],
    leader: &paros::client::election::Leader,
    draw: u64,
) -> bool {
    if buggify_with_prob!(0.3) {
        let pick = crate::draw_index(draw, founders.len());
        let Some((next, _)) = founders.get(pick) else {
            return true;
        };
        let successor = Candidate {
            id: next.0,
            ..Candidate::default()
        };
        if let Some(uuid) = election.resign(Some(successor)).await {
            assert_reachable!("election: a harness candidate hands its term on");
            hand_off(&cell.client, cell.journals.cell, leader.uuid, uuid, 0).await;
        }
        return true;
    }
    if buggify_with_prob!(0.2) {
        assert_reachable!("election: a harness candidate resigns");
        election.resign(None).await;
        return true;
    }
    false
}

/// How many times one published term's anchor moves before
/// [`election_settles`] judges the election settled (#542 (coordinator
/// starves its lease while it installs)): a doomed term over a slow link
/// renewed once in 3 s and passed a check of one move. Not a knob: an
/// oracle threshold.
const RENEWALS_SEEN: u32 = 3;

/// The cell coordinator's current term, and whether it is served (its
/// record names the published interface, so the term installed); `None`
/// without a read or a leader.
pub(in crate::workload) async fn coordinator_term(
    ctx: &SimContext,
    cell: &Cell,
    attempt: usize,
) -> Option<(u64, bool)> {
    let journal = cell.journals.election?;
    let fold = read_election(&cell.client, journal, attempt).await?;
    note_terms(ctx, journal, &fold);
    let leader = fold.leader()?;
    Some((leader.term, !leader.candidate.interface.is_empty()))
}

/// Liveness (#240): once the chaos window closed, the cell's election
/// settles on one leader that keeps renewing. Reads of the election journal
/// by `deadline` see one term's anchor move [`RENEWALS_SEEN`] times, from
/// its first record that names its published interface, over a span longer
/// than a lease and a half of the longest lease any node runs (#542
/// (coordinator starves its lease while it installs)): a term deposed about
/// 1.5 s after each publish renews twice within 100 ms too, so two reads
/// alone let that livelock pass to a downstream check (witness
/// 17167272764625629279).
pub(in crate::workload) async fn election_settles(
    ctx: &SimContext,
    cell: &Cell,
    deadline: Duration,
) {
    let Some(journal) = cell.journals.election else {
        return;
    };
    let lease = crate::shape::longest_election_lease(ctx.state());
    let span = lease + lease / 2;
    // The term followed, its latest anchor seen, when it was first seen,
    // and how many times its anchor moved since.
    let mut first: Option<(u64, u64, Duration, u32)> = None;
    let mut seen: Option<(u64, u64)> = None;
    let mut settled = false;
    let mut attempt = 0_usize;
    while !settled && ctx.time().now() < deadline && !ctx.shutdown().is_cancelled() {
        attempt += 1;
        if let Some(fold) = read_election(&cell.client, journal, attempt).await {
            note_terms(ctx, journal, &fold);
            // A term counts from its first record that names the
            // published interface: the term served its install.
            if let Some(leader) = fold.leader().filter(|l| !l.candidate.interface.is_empty()) {
                let now = ctx.time().now();
                match first.as_mut() {
                    Some((term, anchor, since, moved)) if *term == leader.term => {
                        if *anchor < leader.anchor {
                            *anchor = leader.anchor;
                            *moved += 1;
                        }
                        settled = *moved >= RENEWALS_SEEN && now.saturating_sub(*since) > span;
                    }
                    _ => first = Some((leader.term, leader.anchor, now, 0)),
                }
                seen = Some((leader.term, leader.anchor));
            }
        }
        if !settled && ctx.time().sleep(Duration::from_millis(100)).await.is_err() {
            break;
        }
    }
    if ctx.shutdown().is_cancelled() {
        return;
    }
    assert_always!(
        settled,
        "election: the election settles on one renewing leader after chaos",
        { "cell" => cell.journals.cell_id, "seen" => format!("{seen:?}") }
    );
    assert_reachable!("election: the cell's election is judged settled after chaos");
}
