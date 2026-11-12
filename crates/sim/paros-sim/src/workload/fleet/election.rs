//! `ELECTION` (#240): this operator stands as one more candidate in the
//! cell's election journal, beside the initial members' coordinators,
//! through the library's `paros::client::election` — the code the
//! coordinator runs. A term it wins is served with the coordinator's own
//! duties (`paros::coordinator::cell::serve_term`): it installs its uuid
//! on the cell control journal and finishes the admissions in flight.
//!
//! The candidacy lasts a few steps. On its own BUGGIFY locations a leading
//! candidate hands its term on to an initial member, or resigns; otherwise it
//! stops calling, and the members take over once its lease runs out. The
//! backoff jitter is the run's draw: the library draws none.
//!
//! Every fold it makes feeds the run's term table: one leader per term,
//! whichever fold names it.

use std::collections::BTreeMap;
use std::sync::{Mutex, PoisonError};
use std::time::Duration;

use moonpool_sim::{
    RandomProvider, SimContext, TimeProvider, assert_always, assert_always_or_unreachable,
    assert_reachable, buggify_with_prob,
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
            assert_always_or_unreachable!(
                false,
                "election: a formed cell names its election journal",
                { "cell" => cell.journals.cell_id }
            );
            return;
        };
        // Every initial member, reached or not: an admission the term
        // finishes names them all.
        let members: Vec<NodeId> = cell.members.iter().copied().map(NodeId).collect();
        let initial_members: Vec<(NodeId, paros::model::address::Address)> = cell
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
                        (cell.journals, members.clone(), initial_members.clone()),
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
                    if leave(&mut election, &cell, &initial_members, &leader, draw).await {
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

/// A served term's end, on its own BUGGIFY locations: hand it on to an
/// initial member, resign, or keep leading. Whether the candidacy ends.
async fn leave(
    election: &mut Election<moonpool_sim::SimProviders>,
    cell: &Cell,
    initial_members: &[(NodeId, paros::model::address::Address)],
    leader: &paros::client::election::Leader,
    draw: u64,
) -> bool {
    if buggify_with_prob!(0.3) {
        let pick = crate::draw_index(draw, initial_members.len());
        let Some((next, _)) = initial_members.get(pick) else {
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

/// The election check's deadline once a term first serves at `now`: a
/// term that replaces another one (`replaces`) gets [`super::FLEET_SETTLE`]
/// again from `now`, never past `ceiling`; the first term seen keeps the
/// deadline. An operator's claim on the cell control journal fences the
/// term's session, and the term resigns ("Never fight the session",
/// `paros-coordinator` `cell/campaign.rs`), so a recovery tail may change
/// terms once more. Recipe 4991055162180930074 274447:11378892526482825455
/// on 9ea8926: a machine's `Register` was superseded 14 s into the check,
/// term 3 resigned, and term 4 served 1.8 s before the fixed deadline.
///
/// The first term seen gets at least `shown` from `now`, the time its
/// renewals take to show, still never past `ceiling`: a term that serves
/// before the fixed deadline settled in time, and the check must see it
/// renew. Recipe 1048516404867812791 763974:177308986066528649 on e10ad3a:
/// a disk full until recovery mode left the cell no matchmaker majority,
/// the election backoff grew to its 16 s cap, and term 4 served 4.5 s
/// before the deadline, short of the 5.9 s span its renewals need.
fn retimed(
    deadline: Duration,
    ceiling: Duration,
    now: Duration,
    (replaces, shown): (bool, Duration),
) -> Duration {
    let next = if replaces {
        deadline
            .max(now + super::FLEET_SETTLE)
            .min(ceiling.max(deadline))
    } else {
        deadline.max(now + shown).min(ceiling.max(deadline))
    };
    assert!(next >= deadline, "a deadline never moves back");
    assert!(
        next <= ceiling.max(deadline),
        "a deadline never passes the ceiling"
    );
    next
}

/// A check's deadline once a read names `term` as the served term, `seen`
/// the term the check counts from (the one the election check judged
/// settled, else the first one it reads): a term that replaces it gets
/// [`super::FLEET_SETTLE`] again from `now`, never past `ceiling`
/// ([`retimed`]). A check that needs the coordinator to act (a renamed
/// machine's `Register`) is judged over one term's window, never over a term
/// fenced as the check began. Seed 1206413593777923859 on 29195f6: the
/// election check judged term m2 settled at 135 s, a fleet claim had fenced
/// it, it resigned at 136 s, and term m1 registered the renamed initial member
/// 16 s into its term, 2 s after the fixed deadline.
pub(in crate::workload) fn window_for_term(
    deadline: Duration,
    ceiling: Duration,
    now: Duration,
    seen: &mut Option<u64>,
    term: Option<u64>,
) -> Duration {
    let next = match (term, *seen) {
        (Some(term), Some(first)) if term != first => {
            *seen = Some(term);
            retimed(deadline, ceiling, now, (true, Duration::ZERO))
        }
        (Some(term), None) => {
            *seen = Some(term);
            deadline
        }
        _ => deadline,
    };
    assert!(next >= deadline, "a deadline never moves back");
    next
}

/// Liveness (#240): once the chaos window closed, the cell's election
/// settles on one leader that keeps renewing. Reads of the election journal
/// by `deadline` see one term's anchor move [`RENEWALS_SEEN`] times, from
/// its first record that names its published interface, over a span longer
/// than a lease and a half of the longest lease any node runs (#542
/// (coordinator starves its lease while it installs)): a term deposed about
/// 1.5 s after each publish renews twice within 100 ms too, so two reads
/// alone let that livelock pass to a downstream check (witness
/// 17167272764625629279). Returns the term judged settled, if any.
pub(in crate::workload) async fn election_settles(
    ctx: &SimContext,
    cell: &Cell,
    deadline: Duration,
) -> Option<u64> {
    let journal = cell.journals.election?;
    let lease = crate::shape::longest_election_lease(ctx.state());
    let span = lease + lease / 2;
    let mut deadline = deadline;
    let ceiling = ctx.time().now() + super::HANDOVER_CEILING;
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
                    _ => {
                        // A term that serves after the check began (the
                        // last one was fenced by an operator's claim, and
                        // resigned) gets the whole window again.
                        // A first term has a lease and a half to renew
                        // in, and a lease more for the reads around it.
                        let shown = span + lease;
                        let next = retimed(deadline, ceiling, now, (first.is_some(), shown));
                        if next > deadline && first.is_some() {
                            assert_reachable!(
                                "election: a new term after chaos restarts the settle window"
                            );
                        }
                        deadline = next;
                        first = Some((leader.term, leader.anchor, now, 0));
                    }
                }
                seen = Some((leader.term, leader.anchor));
            }
        }
        if !settled && ctx.time().sleep(Duration::from_millis(100)).await.is_err() {
            break;
        }
    }
    if ctx.shutdown().is_cancelled() {
        return None;
    }
    assert_always!(
        settled,
        "election: the election settles on one renewing leader after chaos",
        { "cell" => cell.journals.cell_id, "seen" => format!("{seen:?}") }
    );
    assert_reachable!("election: the cell's election is judged settled after chaos");
    first.filter(|_| settled).map(|(term, ..)| term)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{retimed, window_for_term};

    #[test]
    fn a_new_term_restarts_the_window_up_to_the_ceiling() {
        let s = Duration::from_secs;
        let replaces = (true, s(9));
        assert_eq!(
            retimed(s(20), s(60), s(15), replaces),
            s(15) + super::super::FLEET_SETTLE,
            "a term that replaces another gets the window again"
        );
        assert_eq!(
            retimed(s(20), s(60), s(50), replaces),
            s(60),
            "never past the ceiling"
        );
        assert_eq!(retimed(s(70), s(60), s(65), replaces), s(70), "never back");
    }

    #[test]
    fn a_first_term_gets_the_time_to_show_its_renewals() {
        let s = Duration::from_secs;
        assert_eq!(
            retimed(s(20), s(60), s(5), (false, s(9))),
            s(20),
            "an early first term keeps the deadline"
        );
        assert_eq!(
            retimed(s(20), s(60), s(15), (false, s(9))),
            s(24),
            "a late first term gets the time its renewals take"
        );
        assert_eq!(
            retimed(s(20), s(22), s(15), (false, s(9))),
            s(22),
            "never past the ceiling"
        );
        assert_eq!(
            retimed(s(70), s(60), s(65), (false, s(9))),
            s(70),
            "never back"
        );
    }

    #[test]
    fn a_check_gets_a_new_window_when_the_served_term_changes() {
        let s = Duration::from_secs;
        let mut seen = None;
        assert_eq!(
            window_for_term(s(20), s(60), s(5), &mut seen, None),
            s(20),
            "no read keeps it"
        );
        assert_eq!(seen, None, "and sees no term");
        assert_eq!(
            window_for_term(s(20), s(60), s(6), &mut seen, Some(3)),
            s(20),
            "the first term keeps it"
        );
        assert_eq!(
            window_for_term(s(20), s(60), s(10), &mut seen, Some(3)),
            s(20),
            "the same term keeps it"
        );
        assert_eq!(
            window_for_term(s(20), s(60), s(12), &mut seen, Some(4)),
            s(12) + super::super::FLEET_SETTLE,
            "a new term gets the window again"
        );
        assert_eq!(seen, Some(4), "and is the one seen now");
        assert_eq!(
            window_for_term(s(32), s(60), s(55), &mut seen, Some(5)),
            s(60),
            "never past the ceiling"
        );
    }
}
