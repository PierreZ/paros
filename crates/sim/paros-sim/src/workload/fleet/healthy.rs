//! The cell heals by itself after the cutoff (rule 8, one quiescence
//! oracle, `docs/architecture/06-verification.md`): within the bound of the
//! one tail clock (`crate::tail::Clock`), [`crate::tail::PASSES`] passes in
//! a row judge the cell healthy (`crate::tail::predicate`).
//!
//! A pass reads two things. The cell's own admin view, as `parosctl status`
//! asks it: the registry, the election journal and every tenant control
//! journal read to their tails, the coordinator, each machine's liveness
//! and each journal's placed configuration and word. And the sim's ground
//! truth (`truth.rs`), never a reply of the system under test: the
//! configuration in force a quorum chose of each journal (`crate::audit`),
//! the generation in force of each tenant's matchmaker set, each machine's
//! process and disk, the drains, removals and maintenance windows an
//! operator sent, and the machines that could take a missing member. A
//! registry bug that marks a live machine down, or a machine full, does not
//! make a group healthy.
//!
//! Each red names a typed cause: one assertion per cause, and every
//! assertion runs on every seed that reaches the check.

use std::collections::BTreeSet;
use std::fmt::Write as _;
use std::time::Duration;

use moonpool_sim::{SimContext, StateHandle, TimeProvider, assert_always, assert_reachable};
use paros::client::views::{ViewOutcome, ask, request};
use paros::core::NodeId;
use paros::core::journal::{JournalId, JournalIdentifier, TenantId};
use paros::core::membership::{MatchmakerGeneration, MatchmakerId, MatchmakerSet};
use paros::model::registry::BookingTarget;
use paros::model::status::{Word, observe, placed};
use paros::model::view::Scope;
use paros::wire::view::{CellQuery, ViewReply, view_request::Query};

use super::FleetOps;
use super::cell::Cell;
use super::truth;
use crate::tail::predicate::{self, Cause, Group, Kind, Unhealthy};
use crate::tail::{Clock, PASS_GAP, PASSES, RETRY_GAP};

/// How long the check waits for one admin view: the ceiling of the
/// operators' `init_patience_ms` knob. An answer reads the registry, the
/// election journal and each tenant's control journal one after the
/// other, some 1.5 to 2.5 s over the tail's slow links; the knob's 500 ms
/// floor timed out every ask. Never buggified: an oracle threshold.
const VIEW_PATIENCE: Duration = Duration::from_secs(10);

impl FleetOps {
    /// Judge the cell once each pass until [`PASSES`] passes in a row find
    /// it healthy, or until the clock's deadline, then assert each cause
    /// the last pass found.
    pub(super) async fn cell_heals(&self, ctx: &SimContext, cell: &Cell, clock: Clock) {
        // The cell's servers first, then every other machine the operator
        // knows and did not shut down for good: a drain may have moved the
        // cell off the members this operator learned. A machine that cannot
        // read the cell's journals passes the ask on.
        let servers: Vec<_> = cell
            .servers
            .iter()
            .map(|(_, addr)| addr.clone())
            .chain(
                self.machines
                    .iter()
                    .filter(|addr| {
                        !cell.servers.iter().any(|(_, known)| known == *addr)
                            && !crate::machine::shut_down(ctx.state(), addr)
                    })
                    .cloned(),
            )
            .collect();
        let mut attempt = 0_u64;
        let mut passes = 0_u32;
        let mut healed = false;
        let mut last: Vec<Unhealthy> = vec![Unhealthy::NoView];
        let mut detail = String::new();
        while ctx.time().now() < clock.deadline() && !ctx.shutdown().is_cancelled() {
            attempt += 1;
            let outcome = ask(
                self.connector.providers(),
                self.connector.rpc(),
                self.connector.names(),
                &servers,
                cell.first(attempt),
                &request(&Scope::Admin, Query::Cell(CellQuery {})),
                VIEW_PATIENCE,
            )
            .await;
            let reply = match &outcome {
                ViewOutcome::Answered(reply) => Some(reply),
                _ => None,
            };
            let facts = gather(ctx.state(), cell, reply, ctx.time().now());
            let causes = predicate::judge_cell(&facts);
            let pause = if causes.is_empty() {
                passes += 1;
                if passes == PASSES {
                    healed = true;
                    break;
                }
                PASS_GAP
            } else {
                passes = 0;
                detail = describe(&facts);
                last = causes;
                RETRY_GAP
            };
            if ctx.time().sleep(pause).await.is_err() {
                break;
            }
        }
        let took = clock.since_cutoff(ctx.time().now());
        crate::tail::cut_short(ctx, "heal");
        if healed {
            assert_reachable!("heal: the cell is healthy within the bound after the cutoff");
            tracing::info!(heal_ms = took.as_millis(), "cell_healed");
        } else {
            tracing::info!(?last, %detail, "cell_not_healed");
        }
        assert_causes(healed, &last, &detail, attempt, took.as_millis());
    }
}

/// Assert each cause the last pass found, unless the cell `healed`: one
/// gate per cause, so the sweep shows which one fails. `ms` is how long
/// after the cutoff the check stopped.
fn assert_causes(healed: bool, last: &[Unhealthy], detail: &str, attempt: u64, ms: u128) {
    let found = |cause: Unhealthy| !healed && last.contains(&cause);
    let group = |kind: Kind, cause: Cause| found(Unhealthy::Group(kind, cause));
    assert_always!(
        !found(Unhealthy::NoView),
        "heal: the cell answers an admin view within the bound",
        { "attempts" => attempt, "since_cutoff_ms" => ms }
    );
    assert_always!(
        !found(Unhealthy::NoCoordinator),
        "heal: the cell has a coordinator on a live machine within the bound",
        { "since_cutoff_ms" => ms }
    );
    assert_always!(
        !found(Unhealthy::LiveHeldDown),
        "heal: the registry holds every live machine up within the bound",
        { "since_cutoff_ms" => ms, "detail" => detail }
    );
    assert_always!(
        !group(Kind::Journal, Cause::NoQuorum),
        "heal: a journal keeps a live quorum",
        { "detail" => detail }
    );
    assert_always!(
        !group(Kind::Journal, Cause::UnderMaintenance),
        "heal: no journal waits on a member under maintenance",
        { "detail" => detail }
    );
    assert_always!(
        !group(Kind::Journal, Cause::Placeable),
        "heal: a journal gets every member a live machine can take within the bound",
        { "detail" => detail }
    );
    assert_always!(
        !group(Kind::Journal, Cause::NotServing),
        "heal: the cell serves every journal within the bound",
        { "detail" => detail }
    );
    assert_always!(
        !group(Kind::Control, Cause::NoQuorum),
        "heal: a control journal keeps a live quorum",
        { "detail" => detail }
    );
    assert_always!(
        !group(Kind::Control, Cause::UnderMaintenance),
        "heal: no control journal waits on a member under maintenance",
        { "detail" => detail }
    );
    assert_always!(
        !group(Kind::Control, Cause::Placeable),
        "heal: a control journal gets every member a live machine can take within the bound",
        { "detail" => detail }
    );
    assert_always!(
        !group(Kind::Control, Cause::NotServing),
        "heal: the cell serves every control journal within the bound",
        { "detail" => detail }
    );
    assert_always!(
        !group(Kind::Set, Cause::NoQuorum),
        "heal: a matchmaker set keeps a live majority",
        { "detail" => detail }
    );
    assert_always!(
        !group(Kind::Set, Cause::UnderMaintenance),
        "heal: no matchmaker set waits on a member under maintenance",
        { "detail" => detail }
    );
    assert_always!(
        !group(Kind::Set, Cause::Placeable),
        "heal: a matchmaker set gets every member a live machine can take within the bound",
        { "detail" => detail }
    );
    assert_always!(
        !group(Kind::Set, Cause::NotServing),
        "heal: the cell serves every matchmaker set within the bound",
        { "detail" => detail }
    );
}

/// What one pass knows of the cell: the facts of `reply` (`None`: no
/// machine answered) judged against the ground truth at `now`.
fn gather(
    state: &StateHandle,
    cell: &Cell,
    reply: Option<&ViewReply>,
    now: Duration,
) -> predicate::Cell {
    let Some(reply) = reply else {
        return predicate::Cell {
            answered: false,
            coordinator_live: false,
            live_held_down: Vec::new(),
            groups: Vec::new(),
        };
    };
    let coordinator_live = reply.coordinator != 0 && truth::live(state, reply.coordinator);
    let live_held_down: Vec<u64> = reply
        .machines
        .iter()
        .filter(|machine| {
            !machine.up && machine.standing != "retired" && truth::live(state, machine.node_id)
        })
        .map(|machine| machine.node_id)
        .collect();
    let mut groups = Vec::new();
    for tenant in &reply.tenants {
        for journal in &tenant.journals {
            if let Some(group) = journal_group(state, cell, reply, tenant.tenant, journal, now) {
                groups.push(group);
            }
        }
        if let Some(group) = set_group(state, cell, TenantId(tenant.tenant), now) {
            groups.push(group);
        }
    }
    predicate::Cell {
        answered: true,
        coordinator_live,
        live_held_down,
        groups,
    }
}

/// The group of one journal of the view: its members are the ground
/// truth's configuration in force, else the configuration the tenant's
/// create recorded, else the one the view shows placed. `None` when none is
/// known.
fn journal_group(
    state: &StateHandle,
    cell: &Cell,
    reply: &ViewReply,
    tenant: u64,
    journal: &paros::wire::view::JournalView,
    now: Duration,
) -> Option<Group> {
    let id = JournalIdentifier::new(TenantId(tenant), JournalId(journal.journal));
    let data = journal.kind == "data";
    let (created, desired) = {
        let board = crate::audit::tenants::tenant_board(state);
        let board = crate::audit::tenants::lock(&board);
        (board.live_config(id).cloned(), board.desired_acceptors(id))
    };
    let config = crate::audit::config_in_force(state, id)
        .or(created)
        .or_else(|| placed(journal))?;
    let members: BTreeSet<u64> = config.members().iter().map(|m| m.0).collect();
    let standing = truth::members(state, &members, now);
    let alive: BTreeSet<NodeId> = members
        .iter()
        .copied()
        .filter(|id| truth::live(state, *id))
        .map(NodeId)
        .collect();
    // A control journal's target is the cell's to set (`special_size`);
    // its every member live is what the predicate judges. **Open**: its
    // target count is judged at the gate of phase 4.
    let short = if data {
        desired.unwrap_or(0).saturating_sub(members.len())
    } else {
        0
    };
    let word = observe(journal, &reply.machines);
    Some(Group {
        kind: if data { Kind::Journal } else { Kind::Control },
        tenant,
        journal: journal.journal,
        members: members.len(),
        gone: standing.gone,
        leaving: standing.leaving,
        maintained: standing.maintained,
        short,
        live_quorum: config.has_phase1_quorum(&alive) && config.has_phase2_quorum(&alive),
        free: truth::free_machines(state, cell, &members, Some(BookingTarget::Journal(id)), now),
        serves: matches!(word, Some(Word::Healthy | Word::Degraded)),
    })
}

/// The group of `tenant`'s matchmaker set in force, as the audit bound it.
/// `None` on plain Multi-Paxos, or before the audit bound one. A set serves
/// while a majority of it is live: every campaign of the tenant registers
/// at a majority first. **Open**: its target count (`2F+1` of the tenant's
/// largest survive number) is judged at the gate of phase 4.
fn set_group(state: &StateHandle, cell: &Cell, tenant: TenantId, now: Duration) -> Option<Group> {
    let members: BTreeSet<u64> = crate::audit::set_in_force(state, tenant)?
        .into_iter()
        .collect();
    let standing = truth::members(state, &members, now);
    let set = MatchmakerSet::new(
        MatchmakerGeneration(0),
        members.iter().copied().map(MatchmakerId).collect(),
    );
    let live: BTreeSet<MatchmakerId> = members
        .iter()
        .copied()
        .filter(|id| truth::live(state, *id))
        .map(MatchmakerId)
        .collect();
    let live_quorum = set.has_quorum(&live);
    Some(Group {
        kind: Kind::Set,
        tenant: tenant.0,
        journal: 0,
        members: members.len(),
        gone: standing.gone,
        leaving: standing.leaving,
        maintained: standing.maintained,
        short: 0,
        live_quorum,
        free: truth::free_machines(state, cell, &members, None, now),
        serves: live_quorum,
    })
}

/// The groups of `facts` that are not healthy, for the detail map.
fn describe(facts: &predicate::Cell) -> String {
    let mut out = String::new();
    for group in &facts.groups {
        if let Err(cause) = predicate::judge(group) {
            if !out.is_empty() {
                out.push_str("; ");
            }
            let _ = write!(
                out,
                "{} {:x}/{:x} {}: {} members, {} gone, {} leaving, {} short, {} free",
                group.kind.as_str(),
                group.tenant,
                group.journal,
                cause.as_str(),
                group.members,
                group.gone,
                group.leaving,
                group.short,
                group.free
            );
        }
    }
    if !facts.live_held_down.is_empty() {
        if !out.is_empty() {
            out.push_str("; ");
        }
        let _ = write!(out, "held down: {:x?}", facts.live_held_down);
    }
    out
}
