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
//! the founding members only.
//!
//! The convergence check (`converge.rs`) and the status check
//! (`healthy.rs`) do not see that: a journal with no write and no leader
//! converges, and its word is judged from its members' standing alone.
//!
//! A journal is excused as the status check excuses it, from the sim's
//! ground truth (`truth::excused`, `truth::set_lost`, #739 (heal from
//! ground truth)): its members that serve hold no quorum, a maintenance
//! window may cover a member, the cell has no machine to re-place a member
//! onto, or its tenant's matchmaker set lost its majority.

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
use super::truth::{excused, set_lost};
use crate::audit::audit_world_named;
use crate::audit::tenants::tenant_board;
use crate::client::ChainClient;
use crate::frontend::{self, Presented};

/// The pause between two passes over the journals not written yet.
const PASS_PAUSE: Duration = Duration::from_millis(500);

/// One data journal of the view, and whether a member of it is a machine
/// outside the founding members.
struct Owed {
    journal: JournalIdentifier,
    admitted: bool,
}

/// Every data journal of `reply` the cell can hold at `now`, not excused.
fn owed(state: &StateHandle, cell: &Cell, reply: &ViewReply, now: Duration) -> Vec<Owed> {
    let mut owed = Vec::new();
    for tenant in &reply.tenants {
        for journal in tenant.journals.iter().filter(|j| j.kind == "data") {
            let id = JournalIdentifier::new(TenantId(tenant.tenant), JournalId(journal.journal));
            if excused(state, cell, id, now) {
                continue;
            }
            let admitted = journal.acceptors.iter().any(|id| {
                reply
                    .machines
                    .iter()
                    .any(|m| m.node_id == *id && !m.founder)
            });
            owed.push(Owed {
                journal: id,
                admitted,
            });
        }
    }
    owed
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
        let mut left: Option<JournalIdentifier> = None;
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
            let ViewOutcome::Answered(reply) = outcome else {
                if ctx.time().sleep(PASS_PAUSE).await.is_err() {
                    return;
                }
                continue;
            };
            let wiped = crate::machine::wiped_ids(ctx.state());
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
            left = None;
            for owed in owed(ctx.state(), cell, &reply, ctx.time().now()) {
                if written.contains(&owed.journal) {
                    continue;
                }
                if set_lost(ctx.state(), owed.journal.tenant, &wiped) {
                    continue;
                }
                // A slot an outage's losses or faulty copies left no leader
                // able to decide again holds every claim of the journal
                // (the leader's repair probe stays blocked and resigns, so
                // no read is served): its liveness is excused, as the
                // convergence check excuses it, and the write is owed to
                // no one. A holder that stopped the journal (a spare whose
                // booking was released) is out of reach of a catch-up, so
                // its stops are noted first, as `worlds_converge` does
                // (witness: sweep recipe `207858:9784360079472352245` on
                // seed 16247384942910892624 on PR #778, a `1x2` grid
                // `batched` journal whose one member's copy of slot 0 tore
                // at `(4, B)` while the only clean copy sat on a spare's
                // unbooked store).
                let world = audit_world_named(&self.state, owed.journal);
                super::converge::note_stops(&tenant_board(ctx.state()), owed.journal, &world);
                if world.loss_excuses_liveness() {
                    crate::excuses::note_all(ctx.state(), &world.excuses(), owed.journal);
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
                } else if left.is_none() {
                    left = Some(owed.journal);
                }
            }
            if left.is_none() {
                assert_reachable!("leads: every created journal takes a write after chaos");
                return;
            }
            if ctx.time().sleep(PASS_PAUSE).await.is_err() {
                return;
            }
        }
        if let Some(journal) = left {
            assert_always!(
                ctx.shutdown().is_cancelled() || crate::machine::cell_lost(ctx.state()),
                "leads: a created journal the cell can hold takes a write after chaos",
                { "tenant" => journal.tenant.0, "journal" => journal.journal.0 }
            );
        }
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
                            acked(Some(&audit), seq, count, sent);
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
                    acked(Some(&audit), seq, count, sent);
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
