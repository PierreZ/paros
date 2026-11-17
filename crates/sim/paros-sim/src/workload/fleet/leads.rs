//! Every created journal takes a write after chaos (#700 (a journal with a
//! slot on an admitted machine never elects a leader)): once chaos ends, a
//! client writes one record to every live journal a tenant created whose
//! shape the cell can hold, within the settle bound, through the frontends
//! as a client reaches its data (#723 (frontend as a parosd role)). A
//! single-writer journal is claimed first; a multi-writer one takes an
//! unfenced append. A write needs a leader that ran Phase 1 and Phase 2,
//! and a frontend that reaches it: a journal stuck between its campaign and
//! its first chosen slot fails here, and so does a journal placed on, or
//! led from, a machine the frontend does not know. Seed
//! 16849052004639731290 on f75260d: a journal of three members with a slot
//! on the admitted machine took no write, since the frontend forwarded to
//! the initial members only.
//!
//! The convergence check (`converge.rs`) and the status check
//! (`healthy.rs`) do not see that: a journal with no write and no leader
//! converges, and its word is judged from its members' standing alone.
//!
//! Every data journal of the cell's view is owed a write (rule 8, no
//! skip). A journal that takes none has one typed cause, from the sim's
//! ground truth (#739 (heal from ground truth)): a fault beyond its
//! promise the audit sees (`crate::audit::Beyond`), live members that hold
//! no quorum, a member under a maintenance window, or none, a bug. A
//! journal whose cause makes a write impossible (a fault beyond its
//! promise, no live quorum) is not written, and its red has that cause. A
//! journal whose tenant's matchmaker set is lost to wipes is under the one
//! verdict (`crate::tail::judge_lost`), which judges it.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use moonpool_sim::{SimContext, StateHandle, TimeProvider, assert_always, assert_reachable};
use paros::client::views::{ViewOutcome, ask, request};
use paros::client::{ClaimOutcome, WriteOptions, WriteOutcome, Writer, WriterOutcome, multi};
use paros::core::Value;
use paros::core::journal::{JournalId, JournalIdentifier, TenantId};
use paros::model::view::Scope;
use paros::net::Pass;
use paros::wire::view::{CellQuery, ViewReply, view_request::Query};

use super::FleetOps;
use super::cell::Cell;
use super::created::{acked, hashes};
use super::truth;
use crate::audit::Beyond;
use crate::audit::audit_world_named;
use crate::audit::tenants::tenant_board;
use crate::client::ChainClient;
use crate::frontend::{self, Presented};
use crate::tail::predicate::Cause;

/// The pause between two passes over the journals not written yet.
const PASS_PAUSE: Duration = Duration::from_millis(500);

/// One data journal of the view, and whether a member of it is a machine
/// outside the initial members.
struct Owed {
    journal: JournalIdentifier,
    admitted: bool,
}

/// Every data journal of `reply`.
fn owed(cell: &Cell, reply: &ViewReply) -> Vec<Owed> {
    let mut owed = Vec::new();
    for tenant in &reply.tenants {
        for journal in tenant.journals.iter().filter(|j| j.kind == "data") {
            let id = JournalIdentifier::new(TenantId(tenant.tenant), JournalId(journal.journal));
            let admitted = journal
                .acceptors
                .iter()
                .any(|id| !cell.members.contains(id));
            owed.push(Owed {
                journal: id,
                admitted,
            });
        }
    }
    owed
}

/// Why `journal` took no write, as the sim's ground truth says at `now`:
/// a fault beyond its promise first, then the cell's own causes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Unwritten {
    Beyond(Beyond),
    Cell(Cause),
}

impl Unwritten {
    /// The cause of `journal` at `now`, if any.
    fn of(
        state: &StateHandle,
        world: &crate::audit::AuditWorld,
        journal: JournalIdentifier,
        now: Duration,
    ) -> Option<Self> {
        world
            .beyond()
            .map(Self::Beyond)
            .or_else(|| truth::cause(state, journal, now).map(Self::Cell))
    }

    /// Whether no write can land while the cause holds.
    fn blocks(self) -> bool {
        matches!(self, Self::Beyond(_) | Self::Cell(Cause::NoQuorum))
    }
}

/// Judge each journal of `left`, none written, each with its cause: every
/// assertion runs on each call, so each is judged on each seed.
fn judge_unwritten(left: &[(JournalIdentifier, Option<Unwritten>)]) {
    let with = |cause: Option<Unwritten>| -> String {
        let found: Vec<(u64, u64)> = left
            .iter()
            .filter(|(_, c)| *c == cause)
            .map(|(j, _)| (j.tenant.0, j.journal.0))
            .collect();
        format!("{found:?}")
    };
    let has = |cause: Option<Unwritten>| left.iter().any(|(_, c)| *c == cause);
    assert_always!(
        !has(None),
        "leads: a created journal the cell can hold takes a write after chaos",
        { "journals" => with(None) }
    );
    assert_always!(
        !has(Some(Unwritten::Beyond(Beyond::Loss))),
        "leads: a journal takes a write unless an outage lost a slot beyond its quorum",
        { "journals" => with(Some(Unwritten::Beyond(Beyond::Loss))) }
    );
    assert_always!(
        !has(Some(Unwritten::Beyond(Beyond::Wipes))),
        "leads: a journal takes a write unless wipes took its Phase-1 quorum",
        { "journals" => with(Some(Unwritten::Beyond(Beyond::Wipes))) }
    );
    assert_always!(
        !has(Some(Unwritten::Cell(Cause::NoQuorum))),
        "leads: a journal keeps a live quorum to take a write after chaos",
        { "journals" => with(Some(Unwritten::Cell(Cause::NoQuorum))) }
    );
    assert_always!(
        !has(Some(Unwritten::Cell(Cause::UnderMaintenance))),
        "leads: no journal waits on a member under maintenance to take a write",
        { "journals" => with(Some(Unwritten::Cell(Cause::UnderMaintenance))) }
    );
}

impl FleetOps {
    /// Write one record to every created journal the cell can hold, until
    /// each took one or `deadline`; then judge.
    pub(super) async fn journals_lead(
        &mut self,
        ctx: &SimContext,
        cell: &Cell,
        deadline: Duration,
    ) {
        if self.frontends.is_empty() {
            return;
        }
        let servers: Vec<_> = cell.servers.iter().map(|(_, addr)| addr.clone()).collect();
        let mut written: BTreeSet<JournalIdentifier> = BTreeSet::new();
        let mut attempt = 0_u64;
        let mut answered = false;
        let mut left: Vec<(JournalIdentifier, Option<Unwritten>)> = Vec::new();
        while ctx.time().now() < deadline {
            if crate::tail::cut_short(ctx, "leads") {
                return;
            }
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
            let ViewOutcome::Answered(reply) = outcome else {
                if ctx.time().sleep(PASS_PAUSE).await.is_err() {
                    crate::tail::cut_short(ctx, "leads");
                    return;
                }
                continue;
            };
            answered = true;
            // Through the frontends, under an `admin` token that names each
            // journal by its ids.
            let pass = Arc::new(Pass::new(frontend::token(
                &self.state,
                "",
                "",
                Presented::Admin,
                ctx.time().now(),
            )));
            let client = self.connector.through(&self.frontends, &pass);
            let frontends = self.frontends.len();
            let owed = owed(cell, &reply);
            let tenants: BTreeSet<TenantId> = owed.iter().map(|o| o.journal.tenant).collect();
            let lost = crate::tail::lost_sets(ctx.state(), &tenants);
            left.clear();
            for owed in owed {
                if written.contains(&owed.journal) || lost.contains(&owed.journal.tenant) {
                    continue;
                }
                // A holder that stopped the journal (a spare whose booking
                // was released) is out of reach of a catch-up, so its stops
                // are noted first, as `worlds_converge` does: the audit then
                // sees a slot whose every clean copy is out of reach.
                let world = audit_world_named(&self.state, owed.journal);
                super::converge::note_stops(&tenant_board(ctx.state()), owed.journal, &world);
                let cause = Unwritten::of(ctx.state(), &world, owed.journal, ctx.time().now());
                if cause.is_some_and(Unwritten::blocks) {
                    left.push((owed.journal, cause));
                    continue;
                }
                if self
                    .write_one((&client, frontends), owed.journal, attempt)
                    .await
                {
                    if owed.admitted {
                        assert_reachable!(
                            "leads: a journal with a slot on an admitted machine takes a write"
                        );
                    }
                    written.insert(owed.journal);
                } else {
                    let cause = Unwritten::of(ctx.state(), &world, owed.journal, ctx.time().now());
                    left.push((owed.journal, cause));
                }
            }
            if left.is_empty() {
                assert_reachable!("leads: every created journal takes a write after chaos");
                break;
            }
            if left
                .iter()
                .all(|(_, cause)| cause.is_some_and(Unwritten::blocks))
            {
                break;
            }
            if ctx.time().sleep(PASS_PAUSE).await.is_err() {
                crate::tail::cut_short(ctx, "leads");
                return;
            }
        }
        assert_always!(
            answered,
            "leads: the cell answers an admin view after chaos",
            { "attempts" => attempt }
        );
        judge_unwritten(&left);
    }

    /// One record written to `journal` through `client`'s `frontends`,
    /// claimed first when it is a single-writer journal; whether it was
    /// acked.
    async fn write_one(
        &mut self,
        (client, frontends): (&ChainClient, usize),
        journal: JournalIdentifier,
        draw: u64,
    ) -> bool {
        // Learned from the cell's view, as an operator learns it.
        self.note_journals([journal]);
        let audit = audit_world_named(&self.state, journal);
        let mut first = crate::draw_index(draw, frontends);
        let mut writer = Writer::new(journal, self.leader_seeds.next());
        // A frontend that cannot place the journal yet passes the claim on
        // to the next one, once around.
        let mut claim = writer.claim(client, first, false).await;
        for _ in 1..frontends {
            if !matches!(claim, ClaimOutcome::UnknownJournal | ClaimOutcome::Unread) {
                break;
            }
            first = (first + 1) % frontends;
            claim = writer.claim(client, first, false).await;
        }
        let records = self.records(Some(&audit));
        let sent = hashes(&records);
        match claim {
            ClaimOutcome::Won { .. } => {
                // A node whose batch limits the seed drew small refuses a
                // batch over them, proposes nothing and names them: the
                // batch is cut to fit and sent once more (witness recipe
                // 12665019189975174713 232008:10773240230173250979 on
                // bc60ebc: every write to a journal led by a node that
                // takes one record per write was refused).
                let mut records = records;
                let mut sent = sent;
                for _ in 0..2 {
                    match writer.write(client, records.clone(), first).await {
                        WriterOutcome::Written { seq, count, .. } => {
                            acked(&self.state, Some(&audit), (seq, count), sent);
                            return true;
                        }
                        WriterOutcome::TooLarge {
                            max_records,
                            max_bytes,
                        } => {
                            records = fit(records, max_records, max_bytes);
                            sent = hashes(&records);
                        }
                        _ => return false,
                    }
                }
                false
            }
            ClaimOutcome::WrongMode { .. } => {
                let request = multi::append_request(
                    journal,
                    records.into_iter().map(|value| value.0).collect(),
                );
                if let WriteOutcome::Written { seq, count, .. } = client
                    .write(&request, first, WriteOptions::default())
                    .await
                    .outcome
                {
                    acked(&self.state, Some(&audit), (seq, count), sent);
                    return true;
                }
                false
            }
            _ => false,
        }
    }
}

/// The longest prefix of `records` within `max_records` records and
/// `max_bytes` bytes, and at least its first record: a node's limits always
/// admit one record (`paros::roles::node::calls::batch_too_large`).
fn fit(records: Vec<Value>, max_records: u64, max_bytes: u64) -> Vec<Value> {
    let mut bytes = 0_u64;
    let mut kept = Vec::new();
    for record in records {
        bytes = bytes.saturating_add(u64::try_from(record.0.len()).unwrap_or(u64::MAX));
        let count = u64::try_from(kept.len()).unwrap_or(u64::MAX);
        if !kept.is_empty() && (count >= max_records || bytes > max_bytes) {
            break;
        }
        kept.push(record);
    }
    kept
}
