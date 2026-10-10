//! `CREATE_JOURNAL` and `DELETE_JOURNAL` (#210): a tenant's journals,
//! created and deleted through the tenant coordinator with the library's
//! `paros::client::journals` — the code `parosctl journal` prints — against
//! the machines. The tenant is a `READY` served tenant of the fleet
//! directory; its control journal is the one the directory names, so the
//! operator learns it there (§3.8).
//!
//! A request carries an idempotency id this client draws. An answer that
//! does not decide it (no coordinator, a coordinator change, patience spent)
//! leaves the request pending: the client's next journal step sends the
//! same id again. On its own BUGGIFY location the client sends a decided
//! request again, which must read back the first answer: a retry acts once.
//! A created single-writer journal takes one append, which proves it is
//! served on the members the coordinator picked.
//!
//! The oracles of the fold (one outcome per request, ids never reused, a
//! journal created inside its own tenant) are the machines'
//! (`crate::audit::tenants`); the ones here are the client's half: what the
//! coordinator answered is what the control journal holds.

use moonpool_sim::{
    RandomProvider, SimContext, assert_always, assert_reachable, assert_sometimes,
    buggify_with_prob,
};
use paros::client::checkpoint::CheckpointPolicy;
use paros::client::fleet::read_directory;
use paros::client::journals::{self, JournalAnswer, JournalOp, JournalRequest};
use paros::client::{Writer, WriterOutcome};
use paros::fleet::{Groups, TenantState};
use paros::tenant::Desired;
use paros::{JournalIdentifier, TenantId, Value, WriterMode};

use super::FleetOps;
use super::cell::Cell;

/// The journal names a request is drawn from: few, so two requests race for
/// one often.
const JOURNAL_NAMES: [&[u8]; 3] = [b"orders", b"events", b"audit"];

impl FleetOps {
    /// `CREATE_JOURNAL`: create a journal in a `READY` tenant, or send the
    /// pending request again.
    #[tracing::instrument(level = "debug", skip_all, fields(client = self.client_id))]
    pub(in crate::chain_workload) async fn create_journal(
        &mut self,
        ctx: &SimContext,
        _policy: CheckpointPolicy,
        (class, payload): (u64, u64),
    ) {
        let Some((cell, tenant, _)) = self.journal_tenant(ctx, payload).await else {
            return;
        };
        let request = match self.journal_pending.take() {
            Some(request) => request,
            None => {
                let name = JOURNAL_NAMES
                    [usize::try_from(class % JOURNAL_NAMES.len() as u64).unwrap_or(0)]
                .to_vec();
                JournalRequest {
                    request: ctx.random().random_range(1..u64::MAX),
                    tenant,
                    op: JournalOp::Create {
                        name,
                        writer: drawn_mode(),
                        desired: drawn_desired(payload),
                    },
                }
            }
        };
        self.send_journal_request(&cell, request, payload).await;
    }

    /// `DELETE_JOURNAL`: delete a journal of a `READY` tenant by name, or
    /// send the pending request again.
    #[tracing::instrument(level = "debug", skip_all, fields(client = self.client_id))]
    pub(in crate::chain_workload) async fn delete_journal(
        &mut self,
        ctx: &SimContext,
        _policy: CheckpointPolicy,
        payload: u64,
    ) {
        let Some((cell, tenant, _)) = self.journal_tenant(ctx, payload).await else {
            return;
        };
        let request = match self.journal_pending.take() {
            Some(request) => request,
            None => JournalRequest {
                request: ctx.random().random_range(1..u64::MAX),
                tenant,
                op: JournalOp::Delete {
                    name: JOURNAL_NAMES
                        [usize::try_from((payload >> 8) % JOURNAL_NAMES.len() as u64).unwrap_or(0)]
                    .to_vec(),
                },
            },
        };
        self.send_journal_request(&cell, request, payload).await;
    }

    /// The cell and a `READY` served tenant of its fleet directory, with
    /// its control journal (learned from the directory). `None` while there
    /// is no cell or no such tenant.
    async fn journal_tenant(
        &mut self,
        ctx: &SimContext,
        draw: u64,
    ) -> Option<(Cell, TenantId, JournalIdentifier)> {
        let Some(cell) = self.learn(ctx).await else {
            assert_reachable!("journals: a journal request finds no cell formed yet");
            return None;
        };
        let directory = read_directory(&cell.client, cell.first(draw), cell.fleet)
            .await
            .ok()?;
        let ready: Vec<(TenantId, JournalIdentifier)> = directory
            .tenants()
            .filter(|(_, t)| t.state == TenantState::Ready && t.groups == Groups::SERVED)
            .map(|(id, t)| (id, JournalIdentifier::new(id, t.control)))
            .collect();
        if let Some(pending) = &self.journal_pending
            && let Some(held) = ready.iter().find(|(id, _)| *id == pending.tenant)
        {
            self.note_journals([held.1]);
            return Some((cell, held.0, held.1));
        }
        if ready.is_empty() {
            assert_reachable!("journals: a journal request finds no tenant ready");
            return None;
        }
        let (tenant, control) = ready[usize::try_from(draw % ready.len() as u64).unwrap_or(0)];
        self.note_journals([control]);
        Some((cell, tenant, control))
    }

    /// Record `journals` as learned by this operator: from the fleet
    /// directory, or from the coordinator's answer.
    fn note_journals(&self, journals: impl IntoIterator<Item = JournalIdentifier>) {
        self.learned
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .extend(journals);
    }

    /// Send `request` to the tenant coordinator until an answer decides it
    /// or patience runs out (then it stays pending), and judge the answer.
    async fn send_journal_request(&mut self, cell: &Cell, request: JournalRequest, draw: u64) {
        let Some(election) = cell.journals.election else {
            return;
        };
        let providers = self.connector.providers().clone();
        let rpc = self.connector.rpc().clone();
        let answer = journals::request(
            &providers,
            &rpc,
            &cell.client,
            election,
            &request,
            self.patience,
        )
        .await;
        if answer.is_retryable() {
            assert_reachable!("journals: a request is left pending, to send again");
            self.journal_pending = Some(request);
            return;
        }
        self.judge_answer(cell, &request, &answer, draw).await;
        if buggify_with_prob!(0.2) {
            // A retry of a decided request: the client lost the answer.
            let again = journals::request(
                &providers,
                &rpc,
                &cell.client,
                election,
                &request,
                self.patience,
            )
            .await;
            if !again.is_retryable() {
                assert_always!(
                    again == answer,
                    "journals: a retried request reads back its first answer",
                    { "tenant" => request.tenant.0, "first" => answer.as_str(), "again" => again.as_str() }
                );
                assert_reachable!("journals: a retried request reads back its first answer");
            }
        }
    }

    /// The client's oracles of a decided answer.
    async fn judge_answer(
        &mut self,
        cell: &Cell,
        request: &JournalRequest,
        answer: &JournalAnswer,
        draw: u64,
    ) {
        match (answer, &request.op) {
            (
                JournalAnswer::Created { id, config },
                JournalOp::Create {
                    writer, desired, ..
                },
            ) => {
                assert_sometimes!(
                    true,
                    "journals: a journal is created through the coordinator"
                );
                assert_always!(
                    config.members().len() <= desired.acceptors().max(1)
                        && config
                            .members()
                            .iter()
                            .all(|m| cell.members.contains(&m.0)),
                    "journals: a created journal is placed on the cell's members",
                    { "tenant" => request.tenant.0, "members" => config.members().len() }
                );
                let journal = JournalIdentifier::new(request.tenant, *id);
                if *writer == WriterMode::Single {
                    self.note_journals([journal]);
                    self.append_to_created(cell, journal, draw).await;
                }
            }
            (JournalAnswer::Deleted { .. }, JournalOp::Delete { .. }) => {
                assert_reachable!("journals: a journal is deleted through the coordinator");
            }
            (JournalAnswer::NameTaken { .. }, JournalOp::Create { .. }) => {
                assert_reachable!("journals: a create is answered name_taken");
            }
            (JournalAnswer::UnknownJournal, JournalOp::Delete { .. }) => {
                assert_reachable!("journals: a delete of an unknown name is answered");
            }
            (JournalAnswer::Unplaceable, JournalOp::Create { .. }) => {
                assert_reachable!("journals: a create too wide for the cell is unplaceable");
            }
            (JournalAnswer::UnknownTenant, _) => {
                assert_reachable!("journals: a request for a tenant the cell dropped is refused");
            }
            (answer, _) => {
                assert_always!(
                    false,
                    "journals: a request is answered for its own op",
                    { "answer" => answer.as_str() }
                );
            }
        }
    }

    /// One record written to `journal`, a single-writer journal this client
    /// had created: claim it, then write.
    async fn append_to_created(&mut self, cell: &Cell, journal: JournalIdentifier, draw: u64) {
        let first = cell.first(draw);
        let mut writer = Writer::new(journal, self.leader_seeds.next());
        let _ = writer.claim(&cell.client, first, false).await;
        if let WriterOutcome::Written { .. } = writer
            .write(
                &cell.client,
                vec![Value(draw.to_le_bytes().to_vec())],
                first,
            )
            .await
        {
            assert_reachable!("system: a created journal commits an append");
        }
    }
}

/// The writer mode a create draws (#241): fixed for the journal's life.
fn drawn_mode() -> WriterMode {
    if buggify_with_prob!(0.5) {
        assert_reachable!("system: a client creates a multi-writer journal");
        WriterMode::Multi
    } else {
        WriterMode::Single
    }
}

/// The desired mode a create draws: mostly `double`; sometimes `single`,
/// `triple`, or a grid no cell of three founders can place.
fn drawn_desired(draw: u64) -> Desired {
    let mode = match (draw >> 16) % 8 {
        0 => "single",
        1 => "triple",
        2 => "grid:2x2",
        _ => "double",
    };
    mode.parse().unwrap_or(Desired::DOUBLE)
}
