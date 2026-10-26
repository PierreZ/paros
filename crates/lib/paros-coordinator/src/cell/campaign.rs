//! A founding member's candidacy (#240): the election loop, the terms it serves, the hand-on and the renewals.

use std::sync::Arc;
use std::time::Duration;

use moonpool_core::{Detach, LocalTaskProvider, LocalTimeProvider, RandomProvider};
use moonpool_rpc::RpcHandle;
use paros_client::cell::CellSession;
use paros_client::election::{Candidate, Election, ElectionTunables, Step, hand_off};
use paros_client::{CallObserver, Client};
use paros_core::NodeId;
use paros_core::journal::{JournalIdentifier, LeaderUuid, TenantId};
use paros_model::address::{Address, Names};
use paros_model::machine::CellPlan;
use paros_net::runtime::ParosProviders;
use paros_roles::node::host::FormedCell;
use paros_tunables::DriverTunables;
use tokio_util::sync::CancellationToken;

use crate::cell::answers::answer_until;
use crate::cell::footprints::Footprints;
use crate::cell::slow::SlowBounds;
use crate::cell::watch::Watch;
use crate::cell::{
    TermDuty, TermGroup, cell_client, dial_book, election_tunables, follow_book, followed_book,
    open_term, park,
};
use crate::tenant::TenantDesk;
use crate::tenant::modes::Modes;
use crate::tenant::moves::Moves;
use crate::tenant::roles::Roles;
use crate::tenant::scrub::{ScrubPace, Scrubs};
use crate::tenant::sets::{SetStep, Sets};

/// A founding member's candidacy, as the node loop hands it to the task.
/// Its bools are independent BUGGIFY decisions.
#[allow(clippy::struct_excessive_bools)]
pub(crate) struct Candidacy {
    pub(crate) me: NodeId,
    pub(crate) addr: Address,
    pub(crate) names: Names,
    pub(crate) plan: CellPlan,
    pub(crate) seed: u128,
    pub(crate) tunables: ElectionTunables,
    /// The failure detector's timeout (#211), and the re-placement bound
    /// (#475).
    pub(crate) down_after: (Duration, Duration),
    /// The first term that finds a machine gone defers its re-placement by
    /// one round (a BUGGIFY decision, #475 (re-place after the Down
    /// bound)): the machine must still move at the next.
    pub(crate) defer: bool,
    /// The slow-machine judgement's bounds (#361 (slow machines)).
    pub(crate) slow: SlowBounds,
    /// The free-bytes floor (#360 (disk full)).
    pub(crate) space_floor: u64,
    /// The leader stops renewing for one lease and a half after its first
    /// term's duties (a BUGGIFY decision: the takeover's way in). A fault:
    /// never in the recovery tail ([`chaos`]), like `hand_off` and
    /// `quit_mid_move`.
    pub(crate) stall: bool,
    /// The leader hands its first term on to another founding member after
    /// its duties (a BUGGIFY decision).
    pub(crate) hand_off: bool,
    /// The tenant coordinator reuses a taken journal id at the first draw of
    /// every create (a BUGGIFY decision): the fold must refuse it.
    pub(crate) reuse: bool,
    /// Each term's tenant coordinator stops listening at once on its first
    /// create and its first catch-up (a BUGGIFY decision, #445 (ignored load
    /// result)): the desk must answer unavailable, never panic.
    pub(crate) impatient: bool,
    /// Each term's tenant coordinator abandons its first create right after
    /// booking its slots (a BUGGIFY decision, #212 (placement)): the next
    /// create in that tenant must release them.
    pub(crate) abandon: bool,
    /// The first fenced term steps down without a `Resign` record, as if
    /// the record were lost (a BUGGIFY decision): the fold still names the
    /// term's leader, and the term must not come back. A fault: never in
    /// the recovery tail ([`chaos`]).
    pub(crate) lose_resign: bool,
    /// The first term that advances a move resigns right after that step
    /// (a BUGGIFY decision, #474 (journals on admitted machines)), a
    /// journal's or a matchmaker set's (#501 (matchmaker sets off the
    /// founders)): a later term must finish the move.
    pub(crate) quit_mid_move: bool,
    /// The first ended term's group outlives the term by one step (a
    /// BUGGIFY decision, #404): a request that reaches it is refused by the
    /// application (`not_coordinator`), then the group drops and the next
    /// request fails at the transport.
    pub(crate) linger: bool,
    /// The first term this member serves takes a lease and a half longer
    /// to install (a BUGGIFY decision, #542 (coordinator starves its lease
    /// while it installs)): its renewals must keep the term through it. A
    /// fault: never in the recovery tail ([`chaos`]).
    pub(crate) slow_install: bool,
    /// Each term's first finished matchmaker-set move asks its handover
    /// again, as a retry that lands after the move (a BUGGIFY decision,
    /// #501 (matchmaker sets off the founders)): the set may then leave its
    /// booked machines, and the term must move it back.
    pub(crate) ask_again: bool,
    /// How fast the term's tenant scrubbers read (#362 (background
    /// scrubbing)).
    pub(crate) scrub: ScrubPace,
}

/// Start the candidacy of the founding member `formed` for the cell
/// coordinator, in a task that stops with `shutdown`, and serve the journal
/// requests (#210) and the machines' `Register` requests (#349) there, in
/// each served term's group (#404). Draws its seed and its BUGGIFY
/// decisions here, on the node loop. Nothing starts when the plan names no
/// election journal or the tunables are not a working election.
///
/// `stop` is the machine's clean stop (#560 (clean Ctrl-C shutdown)): the
/// candidacy ends, and a term it leads is handed on to the next founding
/// member, through the node loop that still serves. The receiver turns
/// `true` once the candidacy ended, or at once when none started.
pub fn spawn<P: ParosProviders>(
    providers: &P,
    rpc: &RpcHandle<P>,
    formed: &FormedCell,
    tunables: &DriverTunables,
    observer: Option<Arc<dyn CallObserver>>,
    hold: Option<tokio::sync::watch::Receiver<bool>>,
    (shutdown, halt): (CancellationToken, CancellationToken),
) -> tokio::sync::watch::Receiver<bool> {
    let election = election_tunables(tunables);
    if !election.is_valid() || !formed.plan.election.is_set() {
        return tokio::sync::watch::channel(true).1;
    }
    let (ended, ended_rx) = tokio::sync::watch::channel(false);
    let candidacy = Candidacy {
        me: formed.facts.node_id,
        addr: formed.facts.addr.clone(),
        names: formed.facts.names.clone(),
        plan: formed.plan.clone(),
        seed: providers.random().random(),
        tunables: election,
        down_after: (tunables.machine_down_after, tunables.machine_replace_after),
        defer: moonpool_buggify::buggify_with_prob!(0.5),
        slow: SlowBounds {
            rtt: tunables.slow_rtt,
            busy_permille: tunables.slow_busy_permille,
            rounds: tunables.slow_rounds,
        },
        space_floor: tunables.free_bytes_floor,
        stall: moonpool_buggify::buggify_fault_with_prob!(0.3),
        hand_off: formed.plan.members.len() > 1 && moonpool_buggify::buggify_fault_with_prob!(0.25),
        reuse: moonpool_buggify::buggify_with_prob!(0.3),
        impatient: moonpool_buggify::buggify_with_prob!(0.3),
        abandon: moonpool_buggify::buggify_with_prob!(0.3),
        lose_resign: moonpool_buggify::buggify_with_prob!(0.5),
        linger: moonpool_buggify::buggify_with_prob!(0.5),
        quit_mid_move: moonpool_buggify::buggify_fault_with_prob!(0.5)
            || paros_observe::scenario::quit_mid_move(),
        slow_install: moonpool_buggify::buggify_with_prob!(0.3),
        ask_again: moonpool_buggify::buggify_with_prob!(0.3),
        scrub: ScrubPace::of(tunables),
    };
    let mut client = cell_client(providers, rpc, formed, tunables).with_shutdown(shutdown.clone());
    if let Some(observer) = observer {
        client = client.with_observer(observer);
    }
    let providers = providers.clone();
    let rpc = rpc.clone();
    providers
        .local_task()
        .spawn_local_task("paros-cell-coordinator", {
            let providers = providers.clone();
            async move {
                campaign(providers, rpc, client, candidacy, hold, (shutdown, halt)).await;
                let _ = ended.send(true);
            }
        })
        .detach();
    ended_rx
}

/// A jitter in `[0, lease / 2]` derived from `seed` and the step count.
pub(crate) fn jitter(seed: u128, step: u64, lease: Duration) -> Duration {
    let low = u64::try_from(seed & u128::from(u64::MAX)).unwrap_or(0);
    let high = u64::try_from(seed >> 64).unwrap_or(0);
    let mut x = low ^ high ^ step.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    x ^= x >> 30;
    x = x.wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x ^= x >> 27;
    let half = u64::try_from(lease.as_millis() / 2).unwrap_or(u64::MAX);
    Duration::from_millis(x % half.saturating_add(1))
}

/// The founding member after `me` in id order: the successor a hand-off
/// names.
pub(crate) fn successor(plan: &CellPlan, me: NodeId) -> Option<Candidate> {
    let others: Vec<NodeId> = plan
        .members
        .iter()
        .map(|(id, _)| *id)
        .filter(|id| *id != me)
        .collect();
    others
        .iter()
        .find(|id| **id > me)
        .or_else(|| others.first())
        .map(|id| Candidate {
            id: id.0,
            ..Candidate::default()
        })
}

/// The candidate's loop: step the election, serve every term it wins,
/// answer the journal requests between steps, and stop with `shutdown`.
/// A clean `stop` (#560 (clean Ctrl-C shutdown)) ends it too, and hands a
/// led term on first ([`hand_on`]).
#[tracing::instrument(level = "debug", skip_all, fields(node = candidacy.me.0, cell = candidacy.plan.cell_id))]
// One loop over the election's steps: each arm is a thin call into the
// term's duties, and splitting the arms out would scatter its shared state.
#[allow(clippy::too_many_lines)]
pub(crate) async fn campaign<P: ParosProviders>(
    providers: P,
    rpc: RpcHandle<P>,
    mut client: Client<P>,
    candidacy: Candidacy,
    mut hold: Option<tokio::sync::watch::Receiver<bool>>,
    (shutdown, halt): (CancellationToken, CancellationToken),
) {
    let Candidacy {
        me,
        addr,
        names,
        plan,
        seed,
        tunables,
        down_after,
        mut defer,
        slow,
        space_floor,
        mut stall,
        hand_off: mut handing,
        reuse,
        impatient,
        abandon,
        mut lose_resign,
        mut linger,
        mut quit_mid_move,
        mut slow_install,
        ask_again,
        scrub,
    } = candidacy;
    let founder_ids: Vec<NodeId> = plan.members.iter().map(|(id, _)| *id).collect();
    let journals = plan.control_journals();
    let founders = plan.members.clone();
    let policy = client.tunables().checkpoint_policy();
    let pace = tunables.renew_every / 4;
    let mut election = Election::new(
        client.clone(),
        plan.election,
        Candidate {
            id: me.0,
            ..Candidate::default()
        },
        seed,
        tunables,
    );
    // The term whose duties are done.
    let mut served: Option<u64> = None;
    // The founders' addresses a served term last dialed from its session's
    // registry; `None` until its first watch round.
    let mut dialed: Option<Vec<(NodeId, Address)>> = None;
    // The tenant coordinator of the term served, while it leads it.
    let mut desk: Option<(u64, TenantDesk)> = None;
    let open_desk = |term: u64, uuid: LeaderUuid, reuse: bool| {
        let desk = TenantDesk::new(
            uuid,
            journals.cell,
            &founder_ids,
            (seed ^ u128::from(term), reuse, impatient, abandon),
            policy,
        );
        (term, desk)
    };
    // The term a superseded desk resigned: resigned again while the
    // election still names it (a resignation can be lost).
    let mut resigned: Option<u64> = None;
    // The served term's session and its watch over the cell's machines.
    let mut watching: Option<(CellSession, Watch)> = None;
    // The last term's watch, resumed by the next (#475): a machine's silence
    // outlives the term that observed it.
    let mut parked: Option<Watch> = None;
    // The served term's moves off draining machines (#474), and its
    // matchmaker sets' placements and moves (#501 (matchmaker sets off the
    // founders)).
    let mut moves = Moves::default();
    // Its journals' changes to their desired modes (#214 (tenant modes)).
    let mut changes = Modes::default();
    let mut sets = Sets::new(ask_again);
    // The served term's footprint steps (#476).
    let internal: Vec<TenantId> = std::iter::once(journals.cell.tenant)
        .chain(journals.fleet.map(|fleet| fleet.tenant))
        .collect();
    let mut footprints = Footprints::new(internal.clone());
    // The served term's role counts (#604).
    let mut roles = Roles::default();
    // The served term's scrubbers (#362 (background scrubbing)).
    let mut scrubs = Scrubs::new(scrub);
    // The served term's dynamic endpoint (#404).
    let mut group: Option<TermGroup> = None;
    // The term whose install was slowed, until the next step judges it.
    let mut slowed: Option<u64> = None;
    let mut steps = 0_u64;
    let mut waited = false;
    while !shutdown.is_cancelled() && !halt.is_cancelled() {
        // A moved founding member campaigns only once its registration
        // ended (#506 (renamed founder not registered)): its peers cannot
        // reach it, and the reads it serves never confirm, so a term it won
        // would read a stale election and fence the terms the others win.
        if hold.as_ref().is_some_and(|ended| !*ended.borrow()) {
            if !waited {
                waited = true;
                moonpool_assertions::reachable!(
                    "coordinator: a moved founding member waits for its registration"
                );
            }
            if providers.time().sleep(pace).await.is_err() {
                return;
            }
            continue;
        }
        hold = None;
        steps += 1;
        let step = election.step(jitter(seed, steps, tunables.lease)).await;
        if let Some(term) = slowed.take() {
            moonpool_assertions::sometimes!(
                matches!(&step, Step::Leading { leader, .. } if leader.term == term),
                "coordinator: a term kept its lease through a slow install"
            );
        }
        match step {
            Step::Leading { leader, .. } if resigned == Some(leader.term) => {
                desk = None;
                park(&mut watching, &mut parked);
                election.resign(None).await;
            }
            Step::Leading { leader, .. } if served != Some(leader.term) => {
                let install = async {
                    let opened = open_term(
                        &providers,
                        &rpc,
                        &client,
                        &names,
                        (journals, founder_ids.clone(), founders.clone()),
                        &leader,
                        policy,
                    )
                    .await;
                    // Never in the recovery tail ([`chaos`]): a slow install
                    // there spends a lease and a half of the settle the
                    // election oracle allows.
                    if slow_install && chaos() && matches!(opened.0, TermDuty::Served { .. }) {
                        slow_install = false;
                        slowed = Some(leader.term);
                        moonpool_assertions::reachable!("coordinator: a term installs slowly");
                        let _ = providers
                            .time()
                            .sleep(tunables.lease + tunables.lease / 2)
                            .await;
                    }
                    opened
                };
                let (duty, session) = renewing(&providers, &mut election, install).await;
                watching = session.map(|session| {
                    let watch = parked.take().map_or_else(
                        || {
                            Watch::new(client.now(), tunables.renew_every, down_after, slow)
                                .with_space_floor(space_floor)
                        },
                        |watch| watch.resumed(client.now()),
                    );
                    (session, watch)
                });
                moves = Moves::default();
                changes = Modes::default();
                sets = Sets::new(ask_again);
                footprints = Footprints::new(internal.clone());
                roles = Roles::default();
                scrubs = Scrubs::new(scrub);
                match duty {
                    TermDuty::Served { .. } => {
                        served = Some(leader.term);
                        desk = Some(open_desk(leader.term, leader.uuid, reuse));
                        // The term's group replaces any earlier one: a
                        // request to an ended term never reaches this one.
                        group = TermGroup::open(&rpc, leader.term);
                        let reference = group.as_ref().map(TermGroup::reference);
                        election.publish(addr.to_string(), reference.unwrap_or_default());
                    }
                    TermDuty::Refused => {
                        desk = None;
                        election.resign(None).await;
                    }
                    TermDuty::Unavailable => {
                        // The next try dials the founders where the
                        // registry holds them now.
                        let follow = follow_book(&rpc, &client, &names, &plan, (me, &addr));
                        if let Some(followed) = renewing(&providers, &mut election, follow).await {
                            client = followed;
                            dialed = None;
                        }
                    }
                }
            }
            Step::Leading { .. } if served.is_some() && watching.is_none() => {
                // Another writer fenced the term's session (an operator's
                // claim): the term cannot do its duties, and a machine that
                // asks it to register is refused. Never fight the session:
                // give the term to the next campaign, which installs itself
                // under a fresh uuid (#349).
                moonpool_assertions::reachable!("coordinator: a fenced term resigned");
                // Never in the recovery tail ([`chaos`]): a lost resignation
                // there leaves the cell with no coordinator for a lease.
                if lose_resign && chaos() {
                    lose_resign = false;
                    moonpool_assertions::reachable!(
                        "coordinator: a fenced term's resignation was lost"
                    );
                    election.step_down();
                } else {
                    election.resign(None).await;
                }
                assert!(election.leading().is_none(), "a resigned term is not led");
                served = None;
                desk = None;
                assert!(watching.is_none(), "a resigned term holds no session");
            }
            Step::Leading { leader, .. } if handing && chaos() => {
                handing = false;
                desk = None;
                if let Some(next) = successor(&plan, me)
                    && let Some(uuid) = election.resign(Some(next)).await
                {
                    moonpool_assertions::reachable!(
                        "coordinator: a coordinator handed its term on"
                    );
                    hand_off(&client, journals.cell, leader.uuid, uuid, 0).await;
                }
            }
            Step::Leading { .. } if stall && chaos() => {
                // A leader that stops calling: no renewal for longer than a
                // lease, so the other members take over.
                stall = false;
                moonpool_assertions::reachable!("coordinator: a coordinator skipped its renewals");
                let _ = providers
                    .time()
                    .sleep(tunables.lease + tunables.lease / 2)
                    .await;
                // Step the election before the desk answers again: the term
                // may be over.
                continue;
            }
            Step::Leading { .. } if watching.is_some() => {
                // A served term dials the founders where its session's
                // registry holds them now: a founder renamed while the term
                // runs (#349) is reached at its new name. Without it, a
                // term that opened Served kept the old name for its whole
                // run, every desk write to a tenant control journal led
                // there came back ambiguous, and a mode change waited on
                // its `Placed` record for good (#529's class on the desk
                // path: witness recipe 13334306166441940671
                // 77:327766447974513469 on PR #643 (durability mode)).
                if let Some((session, _)) = watching.as_ref() {
                    let book = followed_book(&plan, session.registry(), (me, &addr));
                    if dialed.as_ref() != Some(&book) {
                        if dialed.is_some() {
                            moonpool_assertions::reachable!(
                                "coordinator: a served term follows a renamed founder"
                            );
                        }
                        client = dial_book(&rpc, &client, &names, &book);
                        dialed = Some(book);
                    }
                }
                let duties = async {
                    let mut stepped = None;
                    if let Some((session, watch)) = watching.as_mut() {
                        match watch
                            .round(&providers, &rpc, &client, &names, session)
                            .await
                        {
                            // The term's first write that did not land ends its
                            // watch: never fight the session that fenced it.
                            Err(_) => park(&mut watching, &mut parked),
                            // A round ran: the tenant coordinator takes one step
                            // of a move off a draining machine (#474), one step
                            // of a journal's change to its desired mode (#214
                            // (tenant modes)), then one step of a matchmaker
                            // set's placement or move (#501 (matchmaker sets off
                            // the founders)).
                            Ok(_) if watch.ran() => {
                                if let Some((_, desk)) = desk.as_mut() {
                                    let ctx = (&providers, &rpc, &names, &client);
                                    let mut gone = crate::tenant::moves::Leaving {
                                        gone: watch.gone(session.registry(), client.now()),
                                        held: watch.held(session.registry(), client.now()),
                                    };
                                    if defer && !gone.gone.is_empty() {
                                        defer = false;
                                        gone.gone.clear();
                                        moonpool_assertions::reachable!(
                                            "coordinator: a re-placement is deferred"
                                        );
                                    }
                                    stepped =
                                        Some(match moves.step(ctx, session, desk, &gone).await {
                                            None => None,
                                            Some(step) => {
                                                match changes.step(ctx, session, desk, &gone).await
                                                {
                                                    None => None,
                                                    Some(mode) => {
                                                        let set = sets
                                                            .step(ctx, session, desk, &gone)
                                                            .await;
                                                        // Then one step of a footprint (#476):
                                                        // a tenant coordinator placed, or a slot
                                                        // moved off a draining machine.
                                                        let footprint = match set {
                                                            Some(_) => {
                                                                footprints
                                                                    .step(&client, session)
                                                                    .await
                                                            }
                                                            None => None,
                                                        };
                                                        // Then one step of a tenant's
                                                        // role counts (#604): its batchers.
                                                        let role_step = match footprint {
                                                            Some(_) => {
                                                                roles
                                                                    .step(
                                                                        &client, session, desk,
                                                                        &gone,
                                                                    )
                                                                    .await
                                                            }
                                                            None => None,
                                                        };
                                                        // Then one step of a tenant's
                                                        // scrub pass (#362 (background
                                                        // scrubbing)).
                                                        let scrubbed = match role_step {
                                                            Some(_) => {
                                                                scrubs
                                                                    .step(ctx, session, desk)
                                                                    .await
                                                            }
                                                            None => None,
                                                        };
                                                        // A set's move is left mid-way once
                                                        // its handover started, so the next
                                                        // term finds it running.
                                                        set.zip(footprint).zip(scrubbed).map(
                                                            |((set, _), _)| {
                                                                step.in_flight()
                                                                    || mode.in_flight()
                                                                    || set == SetStep::HandedOver
                                                            },
                                                        )
                                                    }
                                                }
                                            }
                                        });
                                }
                            }
                            Ok(_) => {}
                        }
                    }
                    stepped
                };
                let stepped = renewing(&providers, &mut election, duties).await;
                match stepped {
                    Some(None) => park(&mut watching, &mut parked),
                    Some(Some(true)) if quit_mid_move && chaos() => {
                        // A term that stops in the middle of a move: the
                        // next term finds the move where this one left it.
                        quit_mid_move = false;
                        moonpool_assertions::reachable!("coordinator: a term resigned mid-move");
                        desk = None;
                        park(&mut watching, &mut parked);
                        served = None;
                        election.resign(None).await;
                    }
                    _ => {}
                }
            }
            Step::Deposed { .. } => {
                served = None;
                desk = None;
                park(&mut watching, &mut parked);
                moonpool_assertions::reachable!(
                    "coordinator: a deposed coordinator stopped acting"
                );
            }
            Step::Following { .. } => desk = None,
            Step::Leading { .. } => {}
        }
        // A term this member no longer serves closes its group (#404): a
        // request sent to it fails at the transport from now on.
        if group
            .as_ref()
            .is_some_and(|g| desk.is_none() || served != Some(g.term))
        {
            if linger {
                linger = false;
                moonpool_assertions::reachable!("coordinator: an ended term's group lingers");
            } else {
                group = None;
                moonpool_assertions::reachable!("coordinator: an ended term's group is dropped");
            }
        }
        // Until the next step, answer the journal requests (#210) and the
        // machines' `Register` requests (#349) the term's group receives.
        let until = providers.time().now() + pace;
        let ctx = (&providers, &rpc, &client, &names);
        let cell = (plan.cell_id, tunables.renew_every);
        let answers = answer_until(
            ctx,
            cell,
            until,
            group.as_mut(),
            (&mut watching, &mut parked, &mut desk),
            &halt,
        );
        renewing(&providers, &mut election, answers).await;
        if let Some((term, _)) = desk.as_ref().filter(|(_, desk)| desk.superseded()) {
            // A tenant control journal refused the term's uuid: end the
            // term, as a refused cell control journal does.
            resigned = Some(*term);
            desk = None;
            park(&mut watching, &mut parked);
            election.resign(None).await;
        }
    }
    // A clean stop (#560 (clean Ctrl-C shutdown)), not the node loop's end:
    // the node loop still serves, so a led term can still be handed on.
    if halt.is_cancelled() && !shutdown.is_cancelled() {
        drop(group);
        hand_on(&client, &mut election, (&plan, me), journals.cell).await;
    }
}

/// Hand the term `election` leads, if any, on to the founding member after
/// `me` (#560 (clean Ctrl-C shutdown)): resign naming it, then move the
/// cell control journal's leader uuid to its, as a hand-off BUGGIFY
/// decision does. A lone founding member has nobody to hand it to, and a
/// lost hand-off costs the cell an election, never a second leader.
#[tracing::instrument(level = "debug", skip_all, fields(node = me.0))]
pub(crate) async fn hand_on<P: ParosProviders>(
    client: &Client<P>,
    election: &mut Election<P>,
    (plan, me): (&CellPlan, NodeId),
    governed: JournalIdentifier,
) {
    let Some(leader) = election.leading().cloned() else {
        tracing::info!(node = me.0, "coordinator_stop_not_leading");
        return;
    };
    let Some(next) = successor(plan, me) else {
        moonpool_assertions::reachable!("coordinator: a stopping lone coordinator keeps its term");
        tracing::info!(
            node = me.0,
            term = leader.term,
            "coordinator_stop_no_successor"
        );
        return;
    };
    let to = next.id;
    if let Some(uuid) = election.resign(Some(next)).await {
        assert!(uuid != leader.uuid, "a hand-off names another uuid");
        let outcome = hand_off(client, governed, leader.uuid, uuid, 0).await;
        moonpool_assertions::reachable!("coordinator: a stopping coordinator hands its term on");
        tracing::info!(
            node = me.0,
            term = leader.term,
            to,
            ?outcome,
            "coordinator_stop_handed_on"
        );
    }
    assert!(
        election.leading().is_none(),
        "a stopped coordinator leads no term"
    );
}

/// Whether a term-ending BUGGIFY decision the node loop drew at spawn (a
/// stall, a hand-off, a resignation mid-move) may fire: never once the run's
/// recovery tail began. Drawn as plain BUGGIFY, they fired in the tail: each
/// founding member of a cell formed there ended its first term in turn, and
/// the election never settled (witness `replay-main 15129731564713398037` on
/// 7ec931e: three stalls in 18 s of the tail). Each is now a fault, silent
/// when the candidacy starts in the tail, and this read drops one drawn in
/// the chaos window and carried to a term the tail elects. A read of the
/// run's phase, no draw; in production no decision is ever drawn. A lost
/// resignation and a slow install read it too: in the tail, an `init` fenced
/// a term whose resignation was then lost, the next term installed slowly,
/// and it published too late for the election to settle (witness
/// `replay-main 94182577319794271` on 4c14100).
pub(crate) fn chaos() -> bool {
    !moonpool_buggify::buggify_is_recovering()
}

/// Run `work`, one of a led term's duties, and renew the term each time a
/// renewal falls due until it ends (#542 (coordinator starves its lease
/// while it installs)). [`Election::step`] renews only between duties, and
/// one duty may outlast a lease: an install claims and folds the cell
/// control journal, a watch round `Identify`s every machine. Without these
/// renewals every term of a slow cell was deposed before it served
/// (witness 17167272764625629279, a 1.6 s lease over a slow link). A
/// renewal still waiting for its due time when `work` ends is dropped. A
/// renewal already writing is finished first: a dropped write is due again
/// at once, so when one renewal outlasts each short duty, every step wrote
/// a renewal that never counted, near twice the renewals the period asks
/// for on the coordinator's disk (witness `replay-main
/// 10435710832576718291` on 0fe713a). With no term led, `work` runs alone.
pub(crate) async fn renewing<P: ParosProviders, T>(
    providers: &P,
    election: &mut Election<P>,
    work: impl std::future::Future<Output = T>,
) -> T {
    let mut work = std::pin::pin!(work);
    while election.leading().is_some() {
        let wait = election.renewal_due_in();
        let writing = std::cell::Cell::new(false);
        let mut renewal = std::pin::pin!(async {
            let _ = providers.time().sleep(wait).await;
            writing.set(true);
            election.renew_if_due().await
        });
        moonpool_core::select! {
            biased;
            out = &mut work => {
                if writing.get() {
                    moonpool_assertions::reachable!(
                        "coordinator: a renewal in flight outlives its duty"
                    );
                    let _ = renewal.await;
                }
                return out;
            }
            led = &mut renewal => {
                if !led {
                    break;
                }
                moonpool_assertions::reachable!("coordinator: a term renewed during its duties");
            }
        }
    }
    assert!(
        election.leading().is_none(),
        "only a term no longer led runs its duty without renewals"
    );
    work.await
}
