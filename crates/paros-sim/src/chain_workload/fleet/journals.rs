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
//! The one other answer it may read is `unknown_tenant`, once the tenant was
//! removed in between; on its own location the client removes it itself
//! before the retry (#395).
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
use paros::client::names::{JournalNames, JournalResolution};
use paros::client::{ClaimOutcome, ReadOutcome};
use paros::client::{Writer, WriterOutcome};
use paros::fleet::{Groups, TenantState};
use paros::tenant::{Desired, Durability};
use paros::{JournalIdentifier, TenantId, Value, WriterMode};

use std::collections::BTreeSet;
use std::sync::Arc;

use moonpool_sim::{Providers, TimeProvider};
use paros::Pass;
use paros::frontend::Denial;
use paros::name::JournalName;

use super::FleetOps;
use super::cell::Cell;
use crate::audit::tenants;
use crate::client::ChainClient;
use crate::frontend::{self, Presented};

/// The identifier a call by name is bound to on the client: the frontend
/// never reads it (#192 (the frontend)).
const LOCAL: JournalIdentifier = JournalIdentifier::new(TenantId(1), paros::JournalId(1));

/// The journal names a request is drawn from: few, so two requests race for
/// one often.
const JOURNAL_NAMES: [&[u8]; 3] = [b"orders", b"events", b"audit"];

impl FleetOps {
    /// The journal name `draw` picks: always the first on a reused-name
    /// seed ([`crate::shape::reused_name`]).
    fn journal_name(&self, draw: u64) -> &'static [u8] {
        if crate::shape::reused_name(&self.state) {
            return JOURNAL_NAMES[0];
        }
        JOURNAL_NAMES[usize::try_from(draw % JOURNAL_NAMES.len() as u64).unwrap_or(0)]
    }

    /// On a reused-name seed ([`crate::shape::reused_name`]), a decided
    /// request leaves its opposite pending on the same name: a create a
    /// delete, a delete a create. The next journal steps send them, so a
    /// name is created, deleted and created again.
    fn reuse_name(&mut self, request: &JournalRequest, answer: &JournalAnswer, draw: u64) {
        if self.journal_pending.is_some() || !crate::shape::reused_name(&self.state) {
            return;
        }
        let op = match (answer, &request.op) {
            (JournalAnswer::Created { .. }, JournalOp::Create { name, .. }) => {
                JournalOp::Delete { name: name.clone() }
            }
            (JournalAnswer::Deleted { .. }, JournalOp::Delete { name }) => {
                assert_reachable!("journals: a reused-name seed creates a deleted name again");
                JournalOp::Create {
                    name: name.clone(),
                    writer: drawn_mode(),
                    desired: Some(drawn_desired(draw)),
                }
            }
            _ => return,
        };
        self.journal_pending = Some(JournalRequest {
            request: moonpool_sim::sim_random_range(1..u64::MAX),
            tenant: request.tenant,
            op,
        });
    }

    /// `CREATE_JOURNAL`: create a journal in a `READY` tenant, or send the
    /// pending request again.
    #[tracing::instrument(level = "debug", skip_all, fields(client = self.client_id))]
    pub(in crate::chain_workload) async fn create_journal(
        &mut self,
        ctx: &SimContext,
        policy: CheckpointPolicy,
        (class, payload): (u64, u64),
    ) {
        let Some((cell, tenant, control, label)) = self.journal_tenant(ctx, payload).await else {
            self.frontend_before_ready(payload).await;
            return;
        };
        self.through_frontend(&label, control, payload).await;
        let request = if let Some(request) = self.journal_pending.take() {
            request
        } else {
            let name = self.journal_name(class).to_vec();
            // A name this client resolved on an earlier step is read through
            // its cached resolution first: the journal may be gone since.
            if self.cached_name(tenant, &name).is_some() && buggify_with_prob!(0.5) {
                self.read_through_name(&cell, control, &name, payload).await;
            }
            // FDB's `configure` (#214 (tenant modes)) on a draw in four, on
            // a draw in two on a changed-mode seed, and on a BUGGIFY coin
            // besides.
            let every = if crate::shape::changed_mode(ctx.state()) {
                2
            } else {
                4
            };
            let configure = (payload >> 56).is_multiple_of(every) || buggify_with_prob!(0.3);
            let op = if configure {
                // A new desired mode for a journal of the tenant, or on a
                // draw in four for the tenant's default. The coordinator
                // applies a journal's by reconfiguration, under this
                // client's load.
                // On a coin, the widest mode of one partition: it grows the
                // journal onto every storage machine of the cell, so one full
                // machine refuses it for want of slots.
                let desired = if buggify_with_prob!(0.3) {
                    assert_reachable!("journals: a configure asks for every machine");
                    widest_configure()
                } else {
                    drawn_configure(payload)
                };
                JournalOp::Configure {
                    name: (!(payload >> 12).is_multiple_of(4)).then_some(name),
                    desired,
                }
            } else {
                JournalOp::Create {
                    name,
                    writer: drawn_mode(),
                    // On a draw in four the create names no mode: it takes the
                    // tenant's default (#214 (tenant modes)).
                    desired: (!(payload >> 20).is_multiple_of(4)).then(|| drawn_desired(payload)),
                }
            };
            JournalRequest {
                request: ctx.random().random_range(1..u64::MAX),
                tenant,
                op,
            }
        };
        self.send_journal_request(ctx, &cell, control, request, (payload, policy))
            .await;
        // A tenant coordinator's capacity request (#476), as the operator.
        if buggify_with_prob!(0.3) {
            self.ask_capacity(ctx, &cell, tenant).await;
        }
        // The drained-machine scenario (#474 (journals on admitted
        // machines)) drains right after a create, when a journal may sit on
        // an admitted machine.
        if crate::shape::drained_machine(ctx.state()) {
            self.drain(ctx, policy, payload, true).await;
        }
    }

    /// `DELETE_JOURNAL`: delete a journal of a `READY` tenant by name, or
    /// send the pending request again.
    #[tracing::instrument(level = "debug", skip_all, fields(client = self.client_id))]
    pub(in crate::chain_workload) async fn delete_journal(
        &mut self,
        ctx: &SimContext,
        policy: CheckpointPolicy,
        payload: u64,
    ) {
        let Some((cell, tenant, control, label)) = self.journal_tenant(ctx, payload).await else {
            self.frontend_before_ready(payload).await;
            return;
        };
        self.through_frontend(&label, control, payload).await;
        let request = match self.journal_pending.take() {
            Some(request) => request,
            None => JournalRequest {
                request: ctx.random().random_range(1..u64::MAX),
                tenant,
                op: JournalOp::Delete {
                    name: self.journal_name(payload >> 8).to_vec(),
                },
            },
        };
        self.send_journal_request(ctx, &cell, control, request, (payload, policy))
            .await;
    }

    /// The cell and a `READY` served tenant of its fleet directory, with
    /// its control journal (learned from the directory). `None` while there
    /// is no cell or no such tenant.
    async fn journal_tenant(
        &mut self,
        ctx: &SimContext,
        draw: u64,
    ) -> Option<(Cell, TenantId, JournalIdentifier, String)> {
        let Some(cell) = self.learn(ctx).await else {
            assert_reachable!("journals: a journal request finds no cell formed yet");
            return None;
        };
        let directory = read_directory(&cell.client, cell.first(draw), cell.fleet)
            .await
            .ok()?;
        let ready: Vec<(TenantId, JournalIdentifier, String)> = directory
            .tenants()
            .filter(|(_, t)| t.state == TenantState::Ready && t.groups == Groups::SERVED)
            .map(|(id, t)| {
                let name = String::from_utf8_lossy(&t.name).into_owned();
                (id, JournalIdentifier::new(id, t.control), name)
            })
            .collect();
        if let Some(pending) = &self.journal_pending
            && let Some(held) = ready.iter().find(|(id, ..)| *id == pending.tenant)
        {
            self.note_journals([held.1]);
            return Some((cell, held.0, held.1, held.2.clone()));
        }
        if ready.is_empty() {
            assert_reachable!("journals: a journal request finds no tenant ready");
            return None;
        }
        let (tenant, control, name) =
            ready[usize::try_from(draw % ready.len() as u64).unwrap_or(0)].clone();
        self.note_journals([control]);
        Some((cell, tenant, control, name))
    }

    /// Record `journals` as learned by this operator: from the fleet
    /// directory, or from the coordinator's answer.
    pub(super) fn note_journals(&self, journals: impl IntoIterator<Item = JournalIdentifier>) {
        self.learned
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .extend(journals);
    }

    /// Send `request` to the tenant coordinator until an answer decides it
    /// or patience runs out (then it stays pending), and judge the answer.
    async fn send_journal_request(
        &mut self,
        ctx: &SimContext,
        cell: &Cell,
        control: JournalIdentifier,
        request: JournalRequest,
        (draw, policy): (u64, CheckpointPolicy),
    ) {
        let Some(election) = cell.journals.election else {
            return;
        };
        let answer = self.ask_coordinator(cell, election, &request).await;
        if answer.is_retryable() {
            assert_reachable!("journals: a request is left pending, to send again");
            self.journal_pending = Some(request);
            return;
        }
        // The machines a create may be placed on (#474 (journals on admitted
        // machines)): the founding members and every machine of the run, an
        // admission in flight and a wiped machine's old id included.
        let mut machines: BTreeSet<u64> = cell.members.iter().copied().collect();
        machines.extend(crate::machine::minted_ids(ctx.state()));
        self.judge_answer(cell, &machines, control, &request, &answer, draw)
            .await;
        self.reuse_name(&request, &answer, draw);
        if buggify_with_prob!(0.2) {
            // A retry of a decided request: the client lost the answer. On
            // its own location an operator removes the request's tenant
            // first (#395): the retry then meets a tenant the cell dropped.
            if buggify_with_prob!(0.3) {
                self.remove_before_retry(cell, request.tenant, draw, policy)
                    .await;
            }
            let again = self.ask_coordinator(cell, election, &request).await;
            if again.is_retryable() {
                return;
            }
            // A tenant removed since the first answer is dropped by its
            // cell for good, and its control journal with it: the retry is
            // refused `unknown_tenant`, and acts no more than the first
            // send did. The directory, read after the retry, shows the
            // removal (it marks the tenant `REMOVING` before the cell drops
            // it, and never makes it `READY` again).
            if again == JournalAnswer::UnknownTenant && again != answer {
                let removed = read_directory(&cell.client, cell.first(draw), cell.fleet)
                    .await
                    .ok()
                    .map(|directory| {
                        directory
                            .tenant(request.tenant)
                            .is_none_or(|t| t.state != TenantState::Ready)
                    });
                if removed == Some(true) {
                    assert_reachable!(
                        "journals: a retry after its tenant's removal is refused unknown_tenant"
                    );
                    return;
                }
                if removed.is_none() {
                    // The directory could not be read: nothing to judge by.
                    return;
                }
            }
            assert_always!(
                again == answer,
                "journals: a retried request reads back its first answer",
                { "tenant" => request.tenant.0, "first" => answer.as_str(), "again" => again.as_str() }
            );
            assert_reachable!("journals: a client retries a decided request");
        }
    }

    /// One journal request through the library, to the coordinator the
    /// election journal `election` publishes.
    async fn ask_coordinator(
        &self,
        cell: &Cell,
        election: JournalIdentifier,
        request: &JournalRequest,
    ) -> JournalAnswer {
        journals::request(
            self.connector.providers(),
            self.connector.rpc(),
            self.connector.names(),
            &cell.client,
            election,
            request,
            self.patience,
        )
        .await
    }

    /// Remove `tenant` through the fleet tenant and the cell, as `TENANT`
    /// does, between a decided journal request and its retry (#395). A
    /// removal that does not end stays this client's pending operation, and
    /// its next `TENANT` resumes it.
    async fn remove_before_retry(
        &mut self,
        cell: &Cell,
        tenant: TenantId,
        draw: u64,
        policy: CheckpointPolicy,
    ) {
        if self.pending.is_some() {
            return;
        }
        let first = cell.first(draw);
        let Ok(directory) = read_directory(&cell.client, first, cell.fleet).await else {
            return;
        };
        let Some(name) = directory
            .tenant(tenant)
            .filter(|t| t.state == TenantState::Ready && !t.name.is_empty())
            .map(|t| t.name.clone())
        else {
            return;
        };
        let Some(mut session) = self.session(cell, cell.journals, policy) else {
            return;
        };
        assert_reachable!("journals: an operator removes a tenant between a request and its retry");
        let client = cell.client.clone();
        let ended = self.remove(&client, &mut session, first, &name).await;
        self.stopped(ended, false, super::Pending::Remove(name));
    }

    /// The client's oracles of a decided answer.
    async fn judge_answer(
        &mut self,
        cell: &Cell,
        machines: &BTreeSet<u64>,
        control: JournalIdentifier,
        request: &JournalRequest,
        answer: &JournalAnswer,
        draw: u64,
    ) {
        if let JournalOp::Configure { name, desired } = &request.op
            && !matches!(answer, JournalAnswer::UnknownTenant)
        {
            Self::judge_configure(cell, control, request, answer, (name.as_deref(), desired)).await;
            return;
        }
        match (answer, &request.op) {
            (
                JournalAnswer::Created { id, config },
                JournalOp::Create {
                    name,
                    writer,
                    desired,
                },
            ) => {
                assert_sometimes!(
                    true,
                    "journals: a journal is created through the coordinator"
                );
                if self.refused.remove(&request.tenant) {
                    // #212 (placement): the cell refused a create for want of
                    // a slot, and a later one fitted (a delete released
                    // slots, or the coordinator released an abandoned
                    // create's).
                    assert_sometimes!(
                        true,
                        "journals: a create refused for capacity is asked again and placed"
                    );
                }
                // A create that named no mode took the tenant's default, which
                // this client does not know.
                let wanted = desired.map_or(usize::MAX, |d| d.acceptors().max(1));
                if desired.is_none() {
                    assert_reachable!("journals: a create takes its tenant's default mode");
                }
                assert_always!(
                    config.members().len() <= wanted
                        && config
                            .members()
                            .iter()
                            .all(|m| machines.contains(&m.0)),
                    "journals: a created journal is placed on the cell's members",
                    { "tenant" => request.tenant.0, "members" => config.members().len() }
                );
                let journal = JournalIdentifier::new(request.tenant, *id);
                let resolution = self.resolve_name(cell, control, name, draw).await;
                assert_sometimes!(
                    matches!(
                        resolution,
                        JournalResolution::Resolved { journal: resolved, .. } if resolved == journal
                    ),
                    "names: a journal name resolves to the journal its create made"
                );
                if *writer == WriterMode::Single {
                    self.note_journals([journal]);
                    self.append_to_created(cell, journal, draw).await;
                }
            }
            (JournalAnswer::Deleted { .. }, JournalOp::Delete { name }) => {
                assert_reachable!("journals: a journal is deleted through the coordinator");
                // The name this client may have cached now names nothing:
                // its read-through must find it gone (#446 (a gate that
                // never fires cannot fail the sweep)).
                self.read_through_name(cell, control, name, draw).await;
            }
            (JournalAnswer::NameTaken { .. }, JournalOp::Create { .. }) => {
                assert_reachable!("journals: a create is answered name_taken");
            }
            (JournalAnswer::UnknownJournal, JournalOp::Delete { .. }) => {
                assert_reachable!("journals: a delete of an unknown name is answered");
            }
            (JournalAnswer::Unplaceable, JournalOp::Create { desired, .. }) => {
                if desired.is_none_or(|desired| desired_fits(&desired, cell.members.len())) {
                    // Enough founding members, no free slot (#212
                    // (placement)): the refusal is recorded, and the client
                    // asks again under a new id on a later step.
                    assert_reachable!("journals: a create the cell has no slot for is unplaceable");
                    self.refused.insert(request.tenant);
                } else {
                    assert_reachable!("journals: a create too wide for the cell is unplaceable");
                }
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

    /// The client's oracles of a decided configure (#214 (tenant modes)).
    async fn judge_configure(
        cell: &Cell,
        control: JournalIdentifier,
        request: &JournalRequest,
        answer: &JournalAnswer,
        (name, desired): (Option<&[u8]>, &Desired),
    ) {
        match answer {
            JournalAnswer::Configured { id } => {
                assert_sometimes!(
                    true,
                    "journals: a desired mode is configured through the coordinator"
                );
                assert_always!(
                    id.is_some() == name.is_some(),
                    "journals: a configure answers the journal it named",
                    { "tenant" => request.tenant.0 }
                );
                // The control journal records the new mode under the journal
                // the answer names.
                if control.tenant == request.tenant
                    && let (Some(id), Some(tenant)) = (
                        id,
                        journals::list(&cell.client, control.tenant, control.journal).await,
                    )
                    && let Some(journal) = tenant.get(*id)
                    && journal.deleted_at.is_none()
                    && journal.desired == *desired
                {
                    assert_reachable!(
                        "journals: a configured journal's control journal holds its new mode"
                    );
                }
            }
            JournalAnswer::UnknownJournal => {
                assert_reachable!("journals: a configure of an unknown name is answered");
            }
            JournalAnswer::Unplaceable => {
                // A refused configure changes nothing: the control journal
                // records the refusal under its id, and no new mode. A
                // pending request resent after its tenant left the
                // directory's ready list goes out beside another tenant's
                // control journal: that one does not hold it.
                if control.tenant == request.tenant
                    && let Some(tenant) =
                        journals::list(&cell.client, control.tenant, control.journal).await
                {
                    assert_always!(
                        tenant.outcome(request.request)
                            == Some(paros::tenant::RequestOutcome::Unplaceable),
                        "journals: a refused configure changes nothing",
                        { "tenant" => request.tenant.0 }
                    );
                }
                assert_reachable!("journals: a configure the cell cannot place is refused");
            }
            answer => {
                assert_always!(
                    false,
                    "journals: a request is answered for its own op",
                    { "answer" => answer.as_str() }
                );
            }
        }
    }

    /// The journal `name` of `tenant` this client resolved on an earlier
    /// step, if it still holds that resolution.
    fn cached_name(&self, tenant: TenantId, name: &[u8]) -> Option<JournalIdentifier> {
        self.journal_names
            .get(&tenant)
            .and_then(|names| names.cached(name))
            .map(|(journal, _)| journal)
    }

    /// Resolve `name` afresh through the library (#239 (names at the
    /// edge)), and hold what it came to against the tenant control journal
    /// `control` as the machines folded it: a resolved name names the
    /// journal the fold records at the position the resolution was read at,
    /// so a recreated name never resolves to the deleted journal.
    async fn resolve_name(
        &mut self,
        cell: &Cell,
        control: JournalIdentifier,
        name: &[u8],
        draw: u64,
    ) -> JournalResolution {
        let first = cell.first(draw);
        let resolution = self
            .journal_names
            .entry(control.tenant)
            .or_insert_with(|| JournalNames::new(control))
            .refresh(&cell.client, first, name)
            .await;
        let (resolved, at) = match resolution {
            JournalResolution::Resolved { journal, at } => {
                assert_always!(
                    journal.tenant == control.tenant,
                    "names: a journal name resolves inside its tenant",
                    { "journal" => journal.to_string() }
                );
                // The operator learned the journal from its tenant's control
                // journal (§3.8).
                self.note_journals([journal]);
                (Some(journal.journal), at)
            }
            JournalResolution::Unknown { at } => (None, at),
            JournalResolution::Unreadable(_) => return resolution,
        };
        let recorded =
            tenants::lock(&tenants::tenant_board(&self.state)).named_at(control, name, at);
        if let Ok(recorded) = recorded {
            assert_always!(
                recorded == resolved,
                "names: a resolved name is the journal its directory records there",
                {
                    "at" => at,
                    "resolved" => resolved.map_or(0, |j| j.0),
                    "recorded" => recorded.map_or(0, |j| j.0)
                }
            );
            assert_reachable!("names: a journal resolution is judged against the directory");
        }
        resolution
    }

    /// Read the journal `name` resolved to on an earlier step, through that
    /// cached resolution (#239 (names at the edge)). When every server
    /// refuses it as unknown, the resolution is stale: the client drops it
    /// and resolves the name again.
    async fn read_through_name(
        &mut self,
        cell: &Cell,
        control: JournalIdentifier,
        name: &[u8],
        draw: u64,
    ) {
        let Some(cached) = self.cached_name(control.tenant, name) else {
            return;
        };
        assert_reachable!("names: a client reads a journal through a cached resolution");
        let request = paros::client::Reader::new(cached, 0).request(1, 0);
        let report = cell.client.read_any(&request, 0).await;
        let stale = report.outcome == ReadOutcome::UnknownJournal
            && self
                .journal_names
                .get_mut(&control.tenant)
                .is_some_and(|names| names.stale(name, cached));
        if !stale {
            return;
        }
        match self.resolve_name(cell, control, name, draw).await {
            JournalResolution::Resolved { journal, .. } if journal != cached => {
                assert_reachable!(
                    "names: a stale resolution is refused and the name resolves to its new journal"
                );
            }
            JournalResolution::Unknown { .. } => {
                assert_reachable!("names: a stale resolution is refused and the name is gone");
            }
            _ => {}
        }
    }

    /// On a seed that runs frontends, one call to a journal of the tenant
    /// `tenant` (its control journal `control`) through a frontend (#192
    /// (the frontend)): a name from [`JOURNAL_NAMES`] and a token of the
    /// kind `draw` picks. A call in the token's rights is never denied; any
    /// other is denied for its cause and never served.
    async fn through_frontend(&mut self, tenant: &str, control: JournalIdentifier, draw: u64) {
        if self.frontends.is_empty() {
            return;
        }
        let name = self.journal_name(draw >> 40);
        let Ok(named) = JournalName::new(tenant, &String::from_utf8_lossy(name)) else {
            return;
        };
        // Once an operator rotated a key in, a call presents the rotated
        // key's token on its own coin: the two ingredients of a served
        // rotated token line up (#245 (Biscuit tokens)).
        let presented = if frontend::rotation_tried(&self.state) && buggify_with_prob!(0.3) {
            Presented::Rotated
        } else {
            Presented::drawn(draw >> 24)
        };
        // On one draw in three the call names a journal by its ids instead:
        // the tenant's control journal, an internal journal only `admin`
        // reaches.
        let internal = (draw >> 48).is_multiple_of(3);
        let now = self.connector.providers().time().now();
        let journal_name = String::from_utf8_lossy(name);
        let pass = Arc::new(Pass::new(frontend::token(
            &self.state,
            tenant,
            &journal_name,
            presented,
            now,
        )));
        let journal = if internal {
            control
        } else {
            pass.bind(LOCAL, named);
            LOCAL
        };
        let writes = !internal && (draw >> 52).is_multiple_of(2);
        let call = Call {
            presented,
            expected: expected_denial(presented, internal, writes),
        };
        let client = self.connector.through(&self.frontends, &pass);
        let first = usize::try_from(draw % self.frontends.len() as u64).unwrap_or(0);
        if writes {
            self.write_through_frontend(&client, journal, call, first, draw)
                .await;
            return;
        }
        let request = paros::client::Reader::new(journal, 0).request(1, 0);
        match client.read_any(&request, first).await.outcome {
            ReadOutcome::Denied(denial) if call.denied(denial) => {
                assert_reachable!("frontend: a call outside its token's rights is denied");
            }
            ReadOutcome::Page { .. } | ReadOutcome::Truncated { .. }
                if call.served(&self.state) =>
            {
                if internal {
                    assert_reachable!("frontend: an admin reads an internal journal by its ids");
                } else {
                    assert_reachable!("frontend: a tenant reads its journal by name");
                }
            }
            _ => {}
        }
    }

    /// On a seed with frontends, one call while no tenant is `READY` (#446
    /// (a gate that never fires cannot fail the sweep)): the frontend meets
    /// a cell with no universe tenant yet, or a name no `READY` tenant
    /// holds. A frontend otherwise sees about one call a run, after the
    /// fleet formed, so its cold-cell and stale-cache paths never ran. One
    /// call per client: each such call may wait out its timeouts, and more
    /// of them take the run's steps from the journal gates.
    async fn frontend_before_ready(&mut self, draw: u64) {
        if std::mem::replace(&mut self.frontend_cold, true) {
            return;
        }
        let tenant =
            super::NAMES[usize::try_from((draw >> 32) % super::NAMES.len() as u64).unwrap_or(0)];
        self.through_frontend(&String::from_utf8_lossy(tenant), LOCAL, draw)
            .await;
    }

    /// A claim and one write through `client`, a frontend's: a tenant's
    /// fenced writes by name (#192 (the frontend)).
    async fn write_through_frontend(
        &mut self,
        client: &ChainClient,
        journal: JournalIdentifier,
        call: Call,
        first: usize,
        draw: u64,
    ) {
        let mut writer = Writer::new(journal, self.leader_seeds.next());
        // A denied call is denied at the claim's state read: the write then
        // sends nothing (`NotOwner`), so the claim is where the denial is
        // judged (#446 (a gate that never fires cannot fail the sweep)).
        let outcome = match writer.claim(client, first, false).await {
            ClaimOutcome::Denied(denial) => WriterOutcome::Denied(denial),
            _ => {
                writer
                    .write(client, vec![Value(draw.to_le_bytes().to_vec())], first)
                    .await
            }
        };
        match outcome {
            WriterOutcome::Written { .. } if call.served(&self.state) => {
                assert_reachable!("frontend: a tenant writes its journal by name");
            }
            WriterOutcome::Denied(denial) if call.denied(denial) => {
                assert_reachable!("frontend: a write outside its token's rights is denied");
            }
            _ => {}
        }
    }

    /// One record written to `journal`, a single-writer journal this client
    /// had created: claim it, then write.
    async fn append_to_created(&mut self, cell: &Cell, journal: JournalIdentifier, draw: u64) {
        let first = cell.first(draw);
        let mut writer = Writer::new(journal, self.leader_seeds.next());
        let started = self.time.now();
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
            // #215 (zones): its claim and its write were chosen without a
            // whole zone, the journal placed under the zone rule.
            if crate::machine::zone_down_through(&self.state, started, self.time.now()) {
                assert_reachable!("zones: a created journal commits an append with a zone down");
            }
        }
    }
}

/// One call through a frontend: the token presented and the denial its
/// rights call for (`None`: it is served).
#[derive(Clone, Copy)]
struct Call {
    presented: Presented,
    expected: Option<Denial>,
}

impl Call {
    /// Judge a denial; whether it was the one the token's rights call for.
    /// A token the rotated key signed may also be `invalid_token` while the
    /// frontend has not read that key yet (#245 (Biscuit tokens)).
    fn denied(self, denial: Denial) -> bool {
        if denial == Denial::InvalidToken && self.presented.may_be_unknown() {
            assert_reachable!("frontend: a token signed by a key not read yet is refused");
            return false;
        }
        match self.expected {
            None => {
                assert_always!(
                    false,
                    "frontend: a call in its token's rights is never denied",
                    { "denial" => denial.as_str() }
                );
                false
            }
            Some(want) => {
                assert_always!(
                    denial == want,
                    "frontend: a denial names its cause",
                    { "want" => want.as_str(), "denial" => denial.as_str() }
                );
                if matches!(
                    self.presented,
                    Presented::ReadOnly | Presented::OtherJournal
                ) {
                    assert_reachable!("frontend: a derived token's restriction denies a call");
                }
                true
            }
        }
    }

    /// Judge a served call; whether its token's rights allow it.
    fn served(self, state: &moonpool_sim::StateHandle) -> bool {
        assert_always!(
            self.presented.may_be_served(state),
            "frontend: a token signed by a key the universe never held is never served"
        );
        if self.presented.may_be_unknown() {
            assert_reachable!("frontend: a token signed by a rotated-in key is served");
        }
        if matches!(self.presented, Presented::ReadOnly | Presented::OwnJournal)
            && self.expected.is_none()
        {
            assert_reachable!("frontend: a derived token is served within its restriction");
        }
        if let Some(want) = self.expected {
            assert_always!(
                false,
                "frontend: a call outside its token's rights is never served",
                { "want" => want.as_str() }
            );
            return false;
        }
        true
    }
}

/// The denial a call's token rights call for (`None`: it is served): a
/// call by ids is to an internal journal, which only `admin` reaches; a
/// derived token (#400) keeps its restriction.
fn expected_denial(presented: Presented, internal: bool, writes: bool) -> Option<Denial> {
    match presented {
        Presented::Admin => None,
        Presented::Own | Presented::Rotated | Presented::OwnJournal if !internal => None,
        Presented::ReadOnly if !internal && !writes => None,
        Presented::Own
        | Presented::Rotated
        | Presented::OwnJournal
        | Presented::ReadOnly
        | Presented::OtherTenant
        | Presented::OtherJournal => Some(Denial::Forbidden),
        Presented::Expired => Some(Denial::Expired),
        Presented::UnknownKey | Presented::Garbage => Some(Denial::InvalidToken),
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

/// The desired mode a create draws (#523): mostly the default (one failure
/// in one acceptor partition); sometimes no failure, two or three failures,
/// or four acceptor partitions no cell of three founders can place.
fn drawn_desired(draw: u64) -> Desired {
    let (failures_per_partition, partitions) = match (draw >> 16) % 8 {
        0 => (0, 1),
        1 => (2, 1),
        2 => (0, 4),
        3 => (3, 1),
        _ => (1, 1),
    };
    Durability::new(failures_per_partition, partitions).map_or(Desired::DEFAULT, |durability| {
        Desired {
            durability,
            replicas: 0,
        }
    })
}

/// The desired mode a configure draws (#214 (tenant modes)): the default,
/// no failure, two failures, or two to four acceptor partitions of one
/// acceptor each (on the founding members, the same machines under another
/// quorum system), or of three acceptors each, which a cell of four
/// machines cannot place, or three failures in one partition, which takes
/// every storage machine.
fn drawn_configure(draw: u64) -> Desired {
    // On three storage machines most modes place the same three slots, so
    // the draw leans to the ones that change them: one acceptor (a shrink)
    // and acceptor partitions (another quorum system on the same machines).
    let (failures_per_partition, partitions) = match (draw >> 24) % 10 {
        0 => (1, 1),
        1 | 2 => (0, 1),
        3 => (2, 1),
        4 => (0, 2),
        5 | 6 => (0, 3),
        7 => (0, 4),
        8 => (1, 2),
        // The widest mode of one partition grows a journal onto every
        // storage machine: on a full cell it is refused for want of slots.
        _ => (3, 1),
    };
    Durability::new(failures_per_partition, partitions).map_or(Desired::DEFAULT, |durability| {
        Desired {
            durability,
            replicas: 0,
        }
    })
}

/// The widest mode of one acceptor partition, `F = 3`: one partition takes
/// at most every storage machine, so the cell never finds it too wide.
fn widest_configure() -> Desired {
    Durability::new(3, 1).map_or(Desired::DEFAULT, |durability| Desired {
        durability,
        replicas: 0,
    })
}

/// Whether a create under `desired` fits a cell of `members` founding
/// members, slots aside: one acceptor partition takes at most all of them,
/// more acceptor partitions need all their acceptors.
fn desired_fits(desired: &Desired, members: usize) -> bool {
    desired.durability.partitions == 1 || desired.acceptors() <= members
}
