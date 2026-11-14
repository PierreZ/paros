//! Every journal heals after chaos (#475 (re-place after the Down bound),
//! #191 (status)): once chaos ends, every live journal whose shape the cell
//! can hold heals. A member that died for good, or stayed down past
//! `machine_replace_after`, has its slot re-placed; one that answers again
//! rejoins in place.
//!
//! Two judges must agree (#909 (oracles that do not judge)). The sim's own
//! ground truth (`truth::shape`): no member of the audit's configuration in
//! force is gone, and the journal has the members its tenant asked for.
//! A coordinator that books a re-placement and never runs it fails here.
//! And the cell's own admin view, as `parosctl status` judges it
//! (`paros::model::status::observe`): the journal shows healthy. A cell
//! that answers no view fails too.
//!
//! A journal is excused when the cell cannot hold its shape, as the sim's
//! own ground truth says (#739 (heal from ground truth), `truth.rs`), never
//! as the registry or the view the cell answers says: its members that
//! serve hold no quorum (no leader can reconfigure it; a wipe of a quorum
//! loses it for good, witness 17795399505419045907 on a0c4aa6), a
//! maintenance window may cover one of its members (#359), or the cell has
//! fewer storage machines to go to than members that fail to serve. It is
//! excused when its tenant's matchmaker set in force holds no majority
//! outside the wiped machines: a re-placement is a round change, every
//! campaign registers at a matchmaker quorum first, and a wiped matchmaker
//! never rejoins, so no leader can ever reconfigure a journal of that
//! tenant (recipe 12277531871500182271 91348:13584114706456816475 on
//! 448be19: two of a set's three wiped, a journal of its tenant left at its
//! bootstrap ballot, no leader to move it off the dead id).
//!
//! The check has its own window: [`FLEET_SETTLE`], plus twice the longest
//! `machine_replace_after` up to [`HEAL_EXTRA`]. On a seed whose
//! coordinators wait longer than that before they re-place (the shipped
//! profile's 5 min bound), a journal with a gone member is excused, one
//! journal at a time; every other journal is judged.

use std::time::Duration;

use moonpool_sim::{SimContext, StateHandle, TimeProvider, assert_always, assert_reachable};
use paros::client::views::{ViewOutcome, ask, request};
use paros::core::journal::JournalId;
use paros::core::journal::JournalIdentifier;
use paros::core::journal::TenantId;
use paros::model::status::{Word, observe};
use paros::model::view::Scope;
use paros::wire::view::{CellQuery, ViewReply, view_request::Query};

use super::cell::Cell;
use super::truth;
use super::{FLEET_SETTLE, FleetOps};
use crate::excuses::{self, Excuse};

/// The heal check's window past [`FLEET_SETTLE`]: room for twice the
/// longest `machine_replace_after` the sim draws (some 11 s). Never
/// buggified: an oracle threshold.
const HEAL_EXTRA: Duration = Duration::from_secs(40);

/// How long the heal check waits for one admin view: the ceiling of the
/// operators' `init_patience_ms` knob. An answer reads the registry, the
/// election journal and each tenant's control journal one after the
/// other, some 1.5 to 2.5 s over the tail's slow links; the knob's 500 ms
/// floor timed out every ask (#909, seed 4800749190587351858 on 0bedc26).
/// Never buggified: an oracle threshold.
const VIEW_PATIENCE: Duration = Duration::from_secs(10);

impl FleetOps {
    /// Ask the cell for an admin view until every journal it can place
    /// heals, as both judges say, or the heal window ends.
    pub(super) async fn journals_heal(&self, ctx: &SimContext, cell: &Cell) {
        let replace_after = crate::shape::longest_replace_after(ctx.state());
        let window = FLEET_SETTLE + replace_after.saturating_mul(2).min(HEAL_EXTRA);
        let deadline = ctx.time().now() + window;
        // A coordinator that waits past the window never re-places in it:
        // a journal with a gone member is a counted skip, one journal at a
        // time (#738 (count excuses, the baseline)).
        let past_window = replace_after.saturating_mul(2) > window;
        if past_window {
            assert_reachable!("status: a heal window shorter than the re-place bound still judges");
        }
        // The cell's servers first, then every other machine the operator
        // knows and did not shut down for good: a drain may have moved the
        // cell off the members this operator learned, and removed and shut
        // down the last of them (#881 (moves in the sweep); CI shard 2 on
        // PR #934, seed 9069243817935975819: the one initial member was
        // drained, removed and shut down 144 s in, and the check asked only
        // it, 62 times). A machine that cannot read the cell's journals
        // passes the ask on.
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
        if servers.len() > cell.servers.len()
            && cell
                .servers
                .iter()
                .any(|(_, addr)| crate::machine::shut_down(ctx.state(), addr))
        {
            assert_reachable!("status: the heal check asks past a cell member shut down for good");
        }
        let mut attempt = 0_u64;
        let mut answered = false;
        let mut healed = false;
        let mut last: Option<Unhealed> = None;
        while ctx.time().now() < deadline && !ctx.shutdown().is_cancelled() {
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
            if let ViewOutcome::Answered(reply) = outcome {
                answered = true;
                let wiped = crate::machine::wiped_ids(ctx.state());
                let now = ctx.time().now();
                let mut found = None;
                let (unhealed, excused) = unhealthy(ctx.state(), cell, &reply, now, past_window);
                for (journal, excuse) in excused {
                    excuses::note(ctx.state(), excuse, Some(journal));
                }
                for journal in unhealed {
                    if truth::set_lost(ctx.state(), TenantId(journal.tenant), &wiped) {
                        assert_reachable!("status: a lost matchmaker set excuses a journal");
                        excuses::note(
                            ctx.state(),
                            Excuse::HealSetLost,
                            Some(JournalIdentifier::new(
                                TenantId(journal.tenant),
                                JournalId(journal.journal),
                            )),
                        );
                        continue;
                    }
                    found = Some(journal);
                    break;
                }
                match found {
                    None => {
                        assert_reachable!("status: every journal shows healthy after chaos");
                        healed = true;
                        break;
                    }
                    Some(found) => last = Some(found),
                }
            }
            if ctx.time().sleep(Duration::from_millis(500)).await.is_err() {
                break;
            }
        }
        // Both judgements run on every run that reaches them, healed or not
        // (#909): an oracle that runs only when it fails cannot show that it
        // ran.
        let cell_gone = !healed && excuses::cell_lost(ctx.state(), Excuse::SkipHealCellLost);
        assert_always!(
            answered || cell_gone || ctx.shutdown().is_cancelled(),
            "status: the cell answers an admin view after chaos",
            { "attempts" => attempt }
        );
        let (tenant, journal, word, whole) = last.map_or((0, 0, String::new(), true), |l| {
            (l.tenant, l.journal, l.word, l.whole)
        });
        assert_always!(
            healed || cell_gone || !answered,
            "status: a journal the cell can hold shows healthy after chaos",
            { "tenant" => tenant, "journal" => journal, "word" => word, "whole" => whole }
        );
    }
}

/// A data journal that has not healed, as one of the two judges says.
struct Unhealed {
    tenant: u64,
    journal: u64,
    /// The word the cell's view shows.
    word: String,
    /// Whether the ground truth says it holds its full shape.
    whole: bool,
}

/// A journal that has not healed, with the excuse that holds.
type Excused = (JournalIdentifier, Excuse);

/// Every data journal of `reply` that has not healed and is not excused,
/// in the reply's order; and every one that has not healed and is
/// excused, with its excuse. A journal heals when the ground truth says
/// it holds its full shape and the reply shows it healthy. The reply names
/// the journals; the excuses are the sim's own (`truth.rs`). When
/// `past_window`, a journal with a gone member is excused.
fn unhealthy(
    state: &StateHandle,
    cell: &Cell,
    reply: &ViewReply,
    now: Duration,
    past_window: bool,
) -> (Vec<Unhealed>, Vec<Excused>) {
    let mut found = Vec::new();
    let mut excused = Vec::new();
    // A tenant on the move (#784 (coordinator handover)): its journals are
    // on the other cell's machines until it settles, and the move's own
    // oracles judge them.
    let moving = match super::second_cell::moved(state) {
        Some(super::second_cell::Moved::Open(tenant)) => Some(tenant.0),
        _ => None,
    };
    for tenant in reply.tenants.iter().filter(|t| Some(t.tenant) != moving) {
        for journal in tenant.journals.iter().filter(|j| j.kind == "data") {
            let id = JournalIdentifier::new(TenantId(tenant.tenant), JournalId(journal.journal));
            let word = observe(journal, &reply.machines);
            let shape = truth::shape(state, id);
            // A journal the audit knows no configuration of is judged by
            // the view alone.
            let whole = shape.is_none_or(truth::Shape::whole);
            if whole && word == Some(Word::Healthy) {
                continue;
            }
            if !whole {
                assert_reachable!("status: the ground truth sees a journal not yet whole");
            }
            if past_window && shape.is_some_and(|s| s.gone > 0) {
                excused.push((id, Excuse::SkipHealReplacePastTail));
                continue;
            }
            // The placement oracle's last read found no machine with disk
            // room for its move (#985): the journal stays on the machine it
            // leaves.
            if super::placement::no_room(state, id) {
                excused.push((id, Excuse::HealNoRoom));
                continue;
            }
            if let Some(excuse) = truth::excuse(state, cell, id, now) {
                excused.push((id, excuse));
                continue;
            }
            found.push(Unhealed {
                tenant: tenant.tenant,
                journal: journal.journal,
                word: word.map_or("none", Word::as_str).to_string(),
                whole,
            });
        }
    }
    (found, excused)
}
