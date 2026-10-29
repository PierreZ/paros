//! Every journal heals after chaos (#475 (re-place after the Down bound),
//! #191 (status)): once chaos ends, every live journal whose shape the cell
//! can hold shows healthy, as `parosctl status` judges it from an admin cell
//! view (`paros::model::status::observe`). A member that died for good, or stayed
//! down past `machine_replace_after`, has its slot re-placed; one that
//! answers again rejoins in place.
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
//! bootstrap ballot, no leader to move it off the dead id). It is skipped
//! on a seed whose coordinators wait longer than the settle tail before
//! they re-place (the shipped profile's bound).

use std::time::Duration;

use moonpool_sim::{SimContext, StateHandle, TimeProvider, assert_always, assert_reachable};
use paros::client::views::{ViewOutcome, ask, request};
use paros::core::journal::JournalId;
use paros::core::journal::JournalIdentifier;
use paros::core::journal::TenantId;
use paros::model::status::{Word, observe};
use paros::model::view::Scope;
use paros::wire::view::{CellQuery, ViewReply, view_request::Query};

use super::FleetOps;
use super::cell::Cell;
use super::truth;
use crate::excuses::{self, Excuse};

impl FleetOps {
    /// Ask the cell for an admin view until every journal it can place
    /// shows healthy, or `deadline`.
    pub(super) async fn journals_heal(&self, ctx: &SimContext, cell: &Cell, deadline: Duration) {
        // A coordinator that waits past the tail never re-places in it: a
        // counted skip (#738 (count excuses, the baseline)).
        if crate::shape::longest_replace_after(ctx.state()) * 2
            > deadline.saturating_sub(ctx.time().now())
        {
            excuses::note(ctx.state(), Excuse::SkipHealReplacePastTail, None);
            return;
        }
        let servers: Vec<_> = cell.servers.iter().map(|(_, addr)| addr.clone()).collect();
        let mut attempt = 0_u64;
        let mut last: Option<(u64, u64, String)> = None;
        while ctx.time().now() < deadline && !ctx.shutdown().is_cancelled() {
            attempt += 1;
            let outcome = ask(
                self.connector.providers(),
                self.connector.rpc(),
                self.connector.names(),
                &servers,
                cell.first(attempt),
                &request(&Scope::Admin, Query::Cell(CellQuery {})),
                self.patience,
            )
            .await;
            if let ViewOutcome::Answered(reply) = outcome {
                let wiped = crate::machine::wiped_ids(ctx.state());
                let mut found = None;
                let (unhealthy, excused) = unhealthy(ctx.state(), cell, &reply, ctx.time().now());
                for (journal, excuse) in excused {
                    excuses::note(ctx.state(), excuse, Some(journal));
                }
                for (tenant, journal, word) in unhealthy {
                    if truth::set_lost(ctx.state(), TenantId(tenant), &wiped) {
                        assert_reachable!("status: a lost matchmaker set excuses a journal");
                        excuses::note(
                            ctx.state(),
                            Excuse::HealSetLost,
                            Some(JournalIdentifier::new(TenantId(tenant), JournalId(journal))),
                        );
                        continue;
                    }
                    found = Some((tenant, journal, word));
                    break;
                }
                match found {
                    None => {
                        assert_reachable!("status: every journal shows healthy after chaos");
                        return;
                    }
                    Some(found) => last = Some(found),
                }
            }
            if ctx.time().sleep(Duration::from_millis(500)).await.is_err() {
                return;
            }
        }
        if let Some((tenant, journal, word)) = last {
            if crate::machine::cell_lost(ctx.state()) {
                excuses::note(ctx.state(), Excuse::SkipHealCellLost, None);
            }
            assert_always!(
                crate::machine::cell_lost(ctx.state()),
                "status: a journal the cell can hold shows healthy after chaos",
                { "tenant" => tenant, "journal" => journal, "word" => word }
            );
        }
    }
}

/// A journal that does not show healthy, with the excuse that holds.
type Excused = (JournalIdentifier, Excuse);

/// Every data journal of `reply` that does not show healthy and is not
/// excused, as `(tenant, journal, word)`, in the reply's order; and every
/// one that does not show healthy and is excused, with its excuse. The
/// reply names the journals and their words; the excuses are the sim's
/// own (`truth.rs`).
fn unhealthy(
    state: &StateHandle,
    cell: &Cell,
    reply: &ViewReply,
    now: Duration,
) -> (Vec<(u64, u64, String)>, Vec<Excused>) {
    let mut found = Vec::new();
    let mut excused = Vec::new();
    for tenant in &reply.tenants {
        for journal in tenant.journals.iter().filter(|j| j.kind == "data") {
            let word = observe(journal, &reply.machines);
            if word == Some(Word::Healthy) {
                continue;
            }
            let id = JournalIdentifier::new(TenantId(tenant.tenant), JournalId(journal.journal));
            if let Some(excuse) = truth::excuse(state, cell, id, now) {
                excused.push((id, excuse));
                continue;
            }
            let word = word.map_or("none", Word::as_str).to_string();
            found.push((tenant.tenant, journal.journal, word));
        }
    }
    (found, excused)
}
