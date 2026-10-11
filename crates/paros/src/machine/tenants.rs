//! **The tenant coordinator** (#210, `docs/architecture.md` §3.3): the
//! single writer of every hosted tenant's control journal, which answers the
//! journal requests ([`crate::client::journals`]).
//!
//! Until a tenant has its own coordinator (#212, #225), the elected cell
//! coordinator acts for every tenant its cell hosts, for the term it won. It
//! fences each tenant control journal with its term uuid, as it fences the
//! cell control journal: the first request of the term for a tenant claims
//! that tenant's control journal with `SetLeader(uuid, current)` and folds
//! it to the tail. A successor claims it the same way, so a superseded
//! coordinator's writes are refused, and it stops.
//!
//! A request is written as one entry, and the fold judges it at apply
//! ([`TenantControl`]): the coordinator answers with what its fold recorded
//! under the request id. A request the fold already answered is answered from
//! that outcome, written again by no one. The steps:
//!
//! 1. find the tenant's control journal in the cell control journal's
//!    `HostTenant`, its fold caught up first (a dropped or unknown tenant is
//!    refused, #395);
//! 2. claim and fold the control journal, and write the tenant's
//!    `Describe` when it is empty;
//! 3. answer from the recorded outcome, when there is one;
//! 4. draw an id and pick the members (a create), or find the live journal
//!    the name names (a delete: id `0` when none);
//! 5. write the request, and answer with what the fold recorded. An id the
//!    tenant used (`IdTaken`) records nothing: the coordinator draws again.
//!
//! **Placement inside granted capacity** (#212 (placement)). Only the
//! founding members serve journals until journals move onto admitted
//! machines, so they are the machines a create may use: a redundancy mode
//! takes as many as it asks for, at most all of them, and a grid needs all
//! its cells. Inside them, [`crate::placement::plan`] picks the members
//! (zone by zone, least-loaded first, ties by a draw seeded with the
//! tenant), and the coordinator books one acceptor slot on each in the cell
//! control journal, through the term's cell session, *before* it writes the
//! create. The cell control journal has one writer, the elected cell
//! coordinator, which is also the tenant coordinator until #225; the
//! request for capacity is the call into that session. A create the cell has
//! no slot for is refused and the refusal recorded (`Unplaceable`), so a
//! retry reads it back; the caller asks again under a new id. A delete
//! releases the journal's bookings after its tombstone. A booking an
//! unanswered create left behind (a crash between the bookings and the
//! create, or an abandoned create) names a journal the tenant never
//! created: the next create in that tenant releases it, after the claim on
//! the tenant control journal fenced every earlier term, and only while the
//! desk's fold is settled: an unanswered write of the term itself may still
//! land, so its fold may lack a live journal until a later write lands at
//! the tail.
//!
//! The desk draws no randomness of its own: its journal and booking ids
//! derive from the seed the node loop drew for the candidacy, and its
//! BUGGIFY decisions (reuse a taken id once, an impatient term, a create
//! abandoned after its bookings) were drawn there too.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use moonpool_core::Providers;
use paros_core::{AcceptorConfig, JournalId, JournalIdentifier, LeaderUuid, NodeId, TenantId};

use crate::client::cell::CellSession;
use crate::client::checkpoint::{
    AppendOutcome, CheckpointPolicy, Checkpointer, Folder, LoadOutcome, OpenOutcome, load,
};
use crate::client::fleet::Interrupted;
use crate::client::journals::{JournalAnswer, JournalOp, JournalRequest};
use crate::client::{ClaimOutcome, Client, ClientTunables, WriterOutcome};
use crate::placement::{self, Shortfall, Step, Want};
use crate::system::{Booking, BookingTarget, NodeStanding, Registry, Role, SystemCommand};
use crate::tenant::{
    CellKind, Description, Desired, DesiredMode, RequestOutcome, TenantCommand, TenantControl,
};

/// What `HostTenant` records of a tenant: its control journal, its name and
/// what it survives.
type Hosted = (JournalId, Vec<u8>, crate::tenant::Survives);

/// How many ids one create draws before it answers unavailable: an
/// `IdTaken` costs one, and a random 64-bit id is taken almost never.
const ID_DRAWS: u64 = 4;

/// `client`, or, when `impatient`, a copy of it that stops listening at
/// once: every call and read it makes is abandoned unanswered (the floors of
/// [`ClientTunables`] broken on purpose, for one call).
fn impatient<P: Providers>(client: &Client<P>, impatient: bool) -> Client<P> {
    if !impatient {
        return client.clone();
    }
    client.with_tunables(ClientTunables {
        request_timeout: Duration::from_millis(1),
        read_timeout: Duration::from_millis(1),
        retry_budget: 1,
        wait_ms: 0,
        ..*client.tunables()
    })
}

/// The tenant coordinator's state for one term.
pub(crate) struct TenantDesk {
    /// The term's uuid: the fence on every tenant control journal.
    uuid: LeaderUuid,
    /// The cell control journal, where `HostTenant` names each tenant's
    /// control journal.
    cell: JournalIdentifier,
    /// The founding members, in id order: the only machines that serve
    /// journals, so the only ones a create may use.
    founders: Vec<NodeId>,
    /// The cell control journal's fold, read (never written) here.
    registry: Folder<Registry>,
    /// Each tenant's control journal, claimed under the term's uuid.
    tenants: BTreeMap<TenantId, Checkpointer<TenantControl>>,
    /// The tenants whose last write is not known written: an unanswered
    /// write may still land past the desk's fold, so the fold may lack a
    /// live journal. A write that lands at the tail settles the fold again.
    unsettled: BTreeSet<TenantId>,
    /// The seed ids derive from, and how many were drawn.
    seed: u128,
    draws: u64,
    /// Reuse a taken id at the first draw of every create in a tenant that
    /// has one (a BUGGIFY decision of the node loop): the `IdTaken`
    /// redraw's way in.
    reuse: bool,
    /// The term's first create write and its first catch-up after a write
    /// stop listening at once (a BUGGIFY decision of the node loop, #445
    /// (ignored load result)): an unanswered create, then an unavailable
    /// catch-up. Each is cleared once used.
    impatient: (bool, bool),
    /// The term's first create that books a slot stops right after its
    /// bookings and answers unavailable (a BUGGIFY decision of the node
    /// loop, #212 (placement)): the bookings are left to the next create's
    /// release. Cleared once used.
    abandon: bool,
    policy: CheckpointPolicy,
    /// A tenant control journal refused the term's uuid: a later term
    /// fenced it. The term is over for the desk, which claims nothing again
    /// under that uuid (#240's actor rule: stop at the first refusal).
    superseded: bool,
}

impl TenantDesk {
    /// The desk of the term led under `uuid`, over the cell control journal
    /// `cell` and the founding members `founders`.
    pub(crate) fn new(
        uuid: LeaderUuid,
        cell: JournalIdentifier,
        founders: &[NodeId],
        (seed, reuse, impatient, abandon): (u128, bool, bool, bool),
        policy: CheckpointPolicy,
    ) -> Self {
        assert!(uuid.is_set(), "a term's uuid is set");
        assert!(!founders.is_empty(), "a cell has founding members");
        let mut founders = founders.to_vec();
        founders.sort_unstable();
        Self {
            uuid,
            cell,
            registry: Folder::new(Registry::new(founders.iter().copied())),
            founders,
            tenants: BTreeMap::new(),
            unsettled: BTreeSet::new(),
            seed,
            draws: 0,
            reuse,
            impatient: (impatient, impatient),
            abandon,
            policy,
            superseded: false,
        }
    }

    /// A tenant control journal refused the term's uuid: the coordinator
    /// must end its term, never claim again under that uuid.
    pub(crate) fn superseded(&self) -> bool {
        self.superseded
    }

    /// Answer `request`, through `client`, booking and releasing capacity
    /// through the term's cell session `cell` (`None` once the term's
    /// session ended: a create or a delete then answers unavailable or
    /// leaves its release to a later term).
    #[tracing::instrument(level = "debug", skip_all, fields(tenant = request.tenant.0))]
    pub(crate) async fn answer<P: Providers>(
        &mut self,
        client: &Client<P>,
        request: &JournalRequest,
        cell: Option<&mut CellSession>,
    ) -> JournalAnswer {
        if self.superseded {
            return JournalAnswer::NotCoordinator;
        }
        let answer = self.decide(client, request, cell).await;
        if matches!(answer, JournalAnswer::NotCoordinator) {
            // A later term's claim fenced a tenant control journal. A fresh
            // claim under the same uuid would take back what the journal
            // refused: the desk stops, and its term ends.
            moonpool_assertions::reachable!("tenant coordinator: a desk superseded");
            self.superseded = true;
            self.tenants.clear();
        }
        answer
    }

    #[allow(clippy::too_many_lines)]
    async fn decide<P: Providers>(
        &mut self,
        client: &Client<P>,
        request: &JournalRequest,
        cell: Option<&mut CellSession>,
    ) -> JournalAnswer {
        let (control, name, survives) = match self.hosted(client, request.tenant).await {
            Ok(Some(hosted)) => hosted,
            Ok(None) => {
                moonpool_assertions::reachable!("tenant coordinator: an unknown tenant is refused");
                return JournalAnswer::UnknownTenant;
            }
            Err(()) => return JournalAnswer::Unavailable,
        };
        moonpool_buggify::hint!("tenant request taken, nothing written").await;
        if let Err(answer) = self.open(client, request.tenant, control).await {
            return answer;
        }
        if self.desk(request.tenant).state().description().is_none() {
            let description = Description {
                tenant: request.tenant,
                name,
                survives,
                cell_name: Vec::new(),
                cell_kind: CellKind::default(),
                desired: Desired::DOUBLE,
            };
            if let Err(answer) = self
                .append(
                    client,
                    request.tenant,
                    &TenantCommand::Describe(description),
                )
                .await
            {
                return answer;
            }
            moonpool_assertions::reachable!("tenant coordinator: a tenant described itself");
        }
        if let Some(outcome) = self.desk(request.tenant).state().outcome(request.request) {
            moonpool_assertions::reachable!(
                "tenant coordinator: a request answered from its outcome"
            );
            return self.answer_of(request.tenant, outcome);
        }
        match &request.op {
            JournalOp::Create {
                name,
                writer,
                desired,
            } => {
                self.create(client, request, (name, *writer, *desired), cell)
                    .await
            }
            JournalOp::Delete { name } => {
                let id = self
                    .desk(request.tenant)
                    .state()
                    .named(name)
                    .unwrap_or(JournalId(0));
                let command = TenantCommand::DeleteJournal {
                    request: request.request,
                    id,
                };
                if let Err(answer) = self.append(client, request.tenant, &command).await {
                    return answer;
                }
                let Some(outcome) = self.desk(request.tenant).state().outcome(request.request)
                else {
                    return JournalAnswer::Unavailable;
                };
                if let (RequestOutcome::Deleted(deleted), Some(cell)) = (outcome, cell) {
                    // The tombstone is written: its slots go back to the
                    // cell. A release that does not land is left to the next
                    // create in the tenant.
                    let target =
                        BookingTarget::Journal(JournalIdentifier::new(request.tenant, deleted));
                    if release(client, cell, |b| b.target == target)
                        .await
                        .is_ok_and(|released| released > 0)
                    {
                        moonpool_assertions::reachable!(
                            "tenant coordinator: a delete releases its journal's slots"
                        );
                    }
                }
                self.answer_of(request.tenant, outcome)
            }
        }
    }

    /// A create: place it inside the free slots of the cell's storage
    /// machines, founding or admitted (#474), book them, then write it. A
    /// create the cell has no slot for is recorded `Unplaceable`; one whose
    /// machines are not all registered yet answers unavailable.
    async fn create<P: Providers>(
        &mut self,
        client: &Client<P>,
        request: &JournalRequest,
        (name, writer, desired): (&[u8], paros_core::WriterMode, Desired),
        cell: Option<&mut CellSession>,
    ) -> JournalAnswer {
        let tenant = request.tenant;
        let Some(cell) = cell else {
            // The term's cell session ended: no slot can be booked.
            return JournalAnswer::Unavailable;
        };
        let storage = self.storage(cell.registry());
        assert!(
            storage.len() >= self.founders.len(),
            "every founding member stores"
        );
        let wanted = desired.acceptors();
        let count = match desired.mode {
            DesiredMode::Redundancy(_) => wanted.max(1).min(storage.len()),
            DesiredMode::Grid { .. } => wanted,
        };
        if count > storage.len() {
            moonpool_assertions::reachable!("tenant coordinator: a create is unplaceable");
            return self.refuse(client, request, name).await;
        }
        // A live name answers `NameTaken` at apply: it books nothing. An
        // unsettled fold may lack a live journal whose create landed
        // unanswered: its orphans wait for a settled one.
        let named = self.desk(tenant).state().named(name).is_some();
        if !named
            && !self.unsettled.contains(&tenant)
            && self.release_orphans(client, tenant, cell).await.is_err()
        {
            return JournalAnswer::Unavailable;
        }
        for attempt in 0..ID_DRAWS {
            let id = self.draw(tenant, attempt == 0);
            let journal = JournalIdentifier::new(tenant, id);
            let want = Want {
                role: Role::Acceptor,
                target: BookingTarget::Journal(journal),
                count,
            };
            let members = if named || self.desk(tenant).state().is_taken(id) {
                // Refused at apply (`NameTaken` or `IdTaken`): any members do.
                storage.iter().copied().take(count).collect()
            } else {
                let machines = placement::machines(cell.registry(), storage.iter().copied());
                let current: Vec<(u64, Booking)> = cell
                    .registry()
                    .bookings()
                    .filter(|(_, b)| b.target == want.target)
                    .map(|(id, b)| (id, *b))
                    .collect();
                match placement::plan(&want, &machines, &current, tenant.0) {
                    Ok(steps) => {
                        if let Err(answer) = self.book(client, cell, &want, &steps).await {
                            return answer;
                        }
                        moonpool_buggify::hint!("capacity booked, journal not written").await;
                        if std::mem::take(&mut self.abandon) {
                            moonpool_assertions::reachable!(
                                "tenant coordinator: a create abandoned after its bookings"
                            );
                            return JournalAnswer::Unavailable;
                        }
                        placement::booked(&steps)
                    }
                    Err(Shortfall::Capacity { .. }) => {
                        moonpool_assertions::reachable!(
                            "tenant coordinator: a create the cell has no slot for is refused"
                        );
                        return self.refuse(client, request, name).await;
                    }
                    Err(Shortfall::Machines { .. }) => {
                        moonpool_assertions::reachable!(
                            "tenant coordinator: a create waits for its founding members to register"
                        );
                        return JournalAnswer::Unavailable;
                    }
                }
            };
            assert_eq!(
                members.len(),
                count,
                "a create names as many members as it asked"
            );
            let mut chosen = members;
            chosen.sort_unstable();
            let config = AcceptorConfig::new(chosen, desired.quorum_system());
            let command = TenantCommand::CreateJournal {
                request: request.request,
                id,
                name: name.to_vec(),
                writer,
                desired,
                config,
            };
            moonpool_buggify::hint!("tenant create drawn, not written").await;
            if let Err(answer) = self.append(client, tenant, &command).await {
                return answer;
            }
            if let Some(outcome) = self.desk(tenant).state().outcome(request.request) {
                return self.answer_of(tenant, outcome);
            }
            moonpool_assertions::reachable!("tenant coordinator: an id taken is drawn again");
        }
        JournalAnswer::Unavailable
    }

    /// The cell's storage machines a create may be placed on (#474): the
    /// founding members, registered or not, and every registered machine
    /// whose class stores. A draining or retired machine takes no new
    /// journal.
    pub(super) fn storage(&self, registry: &Registry) -> BTreeSet<NodeId> {
        let mut storage: BTreeSet<NodeId> = self.founders.iter().copied().collect();
        storage.extend(
            registry
                .nodes()
                .filter(|(_, n)| {
                    n.standing == NodeStanding::Registered && n.class == Role::Acceptor.class()
                })
                .map(|(id, _)| id),
        );
        if storage.len() > self.founders.len() {
            moonpool_assertions::reachable!(
                "tenant coordinator: an admitted machine may take a create"
            );
        }
        storage
    }

    /// Release every slot booked for a journal of `tenant` that is not a
    /// live journal of its control journal: what an unanswered create left
    /// (a crash between its bookings and its write, or an abandoned create),
    /// or a delete whose release did not land. Only after the desk claimed
    /// the tenant control journal, so no earlier term can still write the
    /// create those slots were booked for, and only on a settled fold, so no
    /// write of this term can either.
    async fn release_orphans<P: Providers>(
        &self,
        client: &Client<P>,
        tenant: TenantId,
        cell: &mut CellSession,
    ) -> Result<(), Interrupted> {
        let state = self.desk(tenant).state();
        let orphan = |b: &Booking| match b.target {
            BookingTarget::Journal(journal) => {
                journal.tenant == tenant
                    && state
                        .get(journal.journal)
                        .is_none_or(|j| j.deleted_at.is_some())
            }
            BookingTarget::Set { .. } => false,
        };
        if release(client, cell, orphan).await? > 0 {
            moonpool_assertions::reachable!(
                "tenant coordinator: a slot no journal holds is released"
            );
        }
        Ok(())
    }

    /// Record `request`, a create of `name`, as unplaceable (or name taken,
    /// when a live journal holds the name), and answer what the fold
    /// recorded.
    async fn refuse<P: Providers>(
        &mut self,
        client: &Client<P>,
        request: &JournalRequest,
        name: &[u8],
    ) -> JournalAnswer {
        let command = TenantCommand::Unplaceable {
            request: request.request,
            name: name.to_vec(),
        };
        if let Err(answer) = self.append(client, request.tenant, &command).await {
            return answer;
        }
        match self.desk(request.tenant).state().outcome(request.request) {
            Some(outcome) => self.answer_of(request.tenant, outcome),
            None => JournalAnswer::Unavailable,
        }
    }

    /// Book the slots `steps` asks for `want`, through the term's cell
    /// session, each under a booking id drawn from the seed (drawn again
    /// when the registry spent it).
    ///
    /// # Errors
    ///
    /// `Unavailable` when a booking did not land: the term's session is
    /// over.
    ///
    /// # Panics
    ///
    /// If the registry refuses a booking the placement computed from its own
    /// fold for another reason than a spent id: the session is the cell
    /// control journal's one writer, so its fold is the registry at its tail.
    pub(super) async fn book<P: Providers>(
        &mut self,
        client: &Client<P>,
        cell: &mut CellSession,
        want: &Want,
        steps: &[Step],
    ) -> Result<(), JournalAnswer> {
        for step in steps {
            let Step::Book { node } = *step else {
                continue;
            };
            let held = Booking {
                node,
                role: want.role,
                target: want.target,
            };
            let mut booked = false;
            for _ in 0..ID_DRAWS {
                let booking = self.mix(0xB00C).max(1);
                if cell.registry().spent().any(|spent| spent == booking) {
                    continue;
                }
                let command = SystemCommand::BookCapacity {
                    booking,
                    node,
                    role: want.role,
                    target: want.target,
                };
                if cell.record(client, 0, &command).await.is_err() {
                    return Err(JournalAnswer::Unavailable);
                }
                assert_eq!(
                    cell.registry().booking(booking),
                    Some(&held),
                    "a booking the placement computed from the registry's tail lands"
                );
                booked = true;
                break;
            }
            if !booked {
                return Err(JournalAnswer::Unavailable);
            }
        }
        assert!(
            placement::booked(steps)
                .iter()
                .all(
                    |node| cell.registry().bookings().any(|(_, b)| b.node == *node
                        && b.role == want.role
                        && b.target == want.target)
                ),
            "every slot a plan asked for is booked"
        );
        moonpool_assertions::reachable!("tenant coordinator: a create books its acceptors' slots");
        Ok(())
    }

    /// The control journal, name and `survives` the cell control journal's
    /// `HostTenant` gives `tenant`, its fold read to the tail first. `Ok(None)`
    /// when the cell hosts no such tenant (or dropped it).
    ///
    /// The fold is caught up at every request, not only for a tenant it does
    /// not hold yet (#395): a tenant removed since the desk last read the
    /// cell is refused at once, by this term as by the next, so the answer to
    /// a request never depends on how stale one coordinator's fold is. A
    /// dropped tenant is dropped for good: the desk forgets its control
    /// journal and never reads the cell for it again.
    ///
    /// # Errors
    ///
    /// The fold could not be read to the tail.
    async fn hosted<P: Providers>(
        &mut self,
        client: &Client<P>,
        tenant: TenantId,
    ) -> Result<Option<Hosted>, ()> {
        if !self.registry.state().dropped(tenant) {
            match load(&mut self.registry, self.cell, client, 0, 0).await {
                LoadOutcome::Loaded { .. } => {}
                _ => return Err(()),
            }
        }
        if self.registry.state().dropped(tenant) {
            if self.tenants.remove(&tenant).is_some() {
                moonpool_assertions::reachable!(
                    "tenant coordinator: a desk forgets a tenant the cell dropped"
                );
            }
            return Ok(None);
        }
        Ok(self
            .registry
            .state()
            .hosted_tenant(tenant)
            .map(|hosted| (hosted.control, hosted.name.clone(), hosted.survives)))
    }

    /// Claim `tenant`'s control journal under the term's uuid and fold it to
    /// its tail, unless the desk holds it already.
    async fn open<P: Providers>(
        &mut self,
        client: &Client<P>,
        tenant: TenantId,
        control: JournalId,
    ) -> Result<(), JournalAnswer> {
        if self.tenants.contains_key(&tenant) {
            return Ok(());
        }
        assert!(
            !self.superseded,
            "a refused term uuid is never claimed again"
        );
        let journal = JournalIdentifier::new(tenant, control);
        let mut desk = Checkpointer::with_uuid(
            journal,
            self.uuid,
            TenantControl::new(tenant, control),
            self.policy,
        );
        match desk.open(client, 0).await {
            OpenOutcome::Open { .. } => {
                assert_eq!(
                    desk.writer().owned(),
                    Some(self.uuid),
                    "an open desk leads under the term's uuid"
                );
                moonpool_assertions::reachable!(
                    "tenant coordinator: a tenant control journal claimed"
                );
                self.tenants.insert(tenant, desk);
                Ok(())
            }
            OpenOutcome::NotClaimed(ClaimOutcome::Lost { .. }) => {
                moonpool_assertions::reachable!(
                    "tenant coordinator: a claim on a tenant control journal lost"
                );
                Err(JournalAnswer::NotCoordinator)
            }
            OpenOutcome::NotClaimed(_) | OpenOutcome::Behind(_) => Err(JournalAnswer::Unavailable),
        }
    }

    /// The open desk of `tenant`.
    fn desk(&self, tenant: TenantId) -> &Checkpointer<TenantControl> {
        self.tenants
            .get(&tenant)
            .expect("a tenant's desk is open before it is read")
    }

    /// Write `command` to `tenant`'s control journal, and fold it. A write
    /// that lands past the fold (an earlier, unanswered write landed first)
    /// is settled by reading the journal to its tail.
    ///
    /// # Errors
    ///
    /// `NotCoordinator` when a later term fenced the journal; `Unavailable`
    /// when the write is not known written, or the catch-up read falls short
    /// of it (#445 (ignored load result): once a panic, red on the main
    /// campaign's seed 14738496120122066363, green since).
    async fn append<P: Providers>(
        &mut self,
        client: &Client<P>,
        tenant: TenantId,
        command: &TenantCommand,
    ) -> Result<(), JournalAnswer> {
        let desk = self
            .tenants
            .get_mut(&tenant)
            .expect("a tenant's desk is open before it is written");
        let before = desk.folder().next_seq();
        // The impatient term's first create stops listening at once: its
        // write may land unanswered, and the retry's fresh id finds its
        // position taken (the way to a write past the fold).
        let impatient_write = matches!(command, TenantCommand::CreateJournal { .. })
            && std::mem::take(&mut self.impatient.0);
        if impatient_write {
            moonpool_assertions::reachable!("tenant coordinator: an impatient create");
        }
        let writer = impatient(client, impatient_write);
        // Unsettled until the write is known written and folded.
        self.unsettled.insert(tenant);
        match desk.append(&writer, command.encode(), 0).await {
            AppendOutcome::Written(WriterOutcome::Written { .. }) => {
                let tail = desk.writer().next_seq();
                if desk.folder().next_seq() < tail {
                    // Another entry landed first (a lost answer's retry):
                    // fold up to this one.
                    moonpool_assertions::reachable!(
                        "tenant coordinator: a write lands behind an entry the desk had not folded"
                    );
                    let impatient_load = std::mem::take(&mut self.impatient.1);
                    let reader = impatient(client, impatient_load);
                    match desk.load(&reader, 0, tail).await {
                        LoadOutcome::Loaded { up_to, .. } if up_to >= tail => {}
                        _ => {
                            // The entry is written but not folded: answer
                            // unavailable, and the next request's catch-up
                            // folds it (#445 (ignored load result)).
                            moonpool_assertions::reachable!(
                                "tenant coordinator: a catch-up after a write falls short"
                            );
                            return Err(JournalAnswer::Unavailable);
                        }
                    }
                }
            }
            AppendOutcome::Written(WriterOutcome::Superseded { .. } | WriterOutcome::NotOwner) => {
                moonpool_assertions::reachable!(
                    "tenant coordinator: a superseded coordinator stops"
                );
                return Err(JournalAnswer::NotCoordinator);
            }
            AppendOutcome::Written(_) => return Err(JournalAnswer::Unavailable),
            AppendOutcome::ReservedPrefix => {
                unreachable!("a tenant command never starts with the checkpoint magic")
            }
        }
        assert!(
            desk.folder().next_seq() > before,
            "a written entry moves the fold"
        );
        // The write landed at the tail and the fold reached it: every
        // earlier write of the term is folded, or never lands.
        self.unsettled.remove(&tenant);
        Ok(())
    }

    /// The next journal id of `tenant`: derived from the seed and the draw
    /// count, set. On a `first` draw, the BUGGIFY decision reuses an id the
    /// tenant took, which the fold must refuse.
    fn draw(&mut self, tenant: TenantId, first: bool) -> JournalId {
        let taken = self
            .desk(tenant)
            .state()
            .journals()
            .next()
            .map(|(id, _)| id);
        if self.reuse
            && first
            && let Some(taken) = taken
        {
            moonpool_assertions::reachable!("tenant coordinator: a create reuses a taken id");
            return taken;
        }
        JournalId(self.mix(tenant.0).max(1))
    }

    /// The next value of the desk's own sequence, salted with `salt`:
    /// derived from the seed and the draw count.
    fn mix(&mut self, salt: u64) -> u64 {
        self.draws += 1;
        let low = u64::try_from(self.seed & u128::from(u64::MAX)).unwrap_or(0);
        let high = u64::try_from(self.seed >> 64).unwrap_or(0);
        let mut x = low ^ high.rotate_left(17) ^ self.draws.wrapping_mul(0x9E37_79B9_7F4A_7C15);
        x ^= salt.rotate_left(29);
        x ^= x >> 30;
        x = x.wrapping_mul(0xBF58_476D_1CE4_E5B9);
        x ^= x >> 27;
        x = x.wrapping_mul(0x94D0_49BB_1331_11EB);
        x ^ (x >> 31)
    }

    /// The answer `outcome` gives, from `tenant`'s fold.
    fn answer_of(&self, tenant: TenantId, outcome: RequestOutcome) -> JournalAnswer {
        match outcome {
            RequestOutcome::Created(id) => {
                let journal = self
                    .desk(tenant)
                    .state()
                    .get(id)
                    .expect("a created outcome names a journal the fold holds");
                JournalAnswer::Created {
                    id,
                    config: journal.config.clone(),
                }
            }
            RequestOutcome::Deleted(id) => JournalAnswer::Deleted { id },
            RequestOutcome::NameTaken(id) => JournalAnswer::NameTaken { id },
            RequestOutcome::UnknownJournal => JournalAnswer::UnknownJournal,
            RequestOutcome::Unplaceable => JournalAnswer::Unplaceable,
        }
    }
}

/// Release every live booking `which` selects, through the term's cell
/// session: how many it released.
///
/// # Errors
///
/// A write did not land: the session is closed, and the rest stay booked.
pub(crate) async fn release<P: Providers>(
    client: &Client<P>,
    cell: &mut CellSession,
    which: impl Fn(&Booking) -> bool,
) -> Result<usize, Interrupted> {
    let bookings: Vec<u64> = cell
        .registry()
        .bookings()
        .filter(|(_, b)| which(b))
        .map(|(id, _)| id)
        .collect();
    let mut released = 0;
    for booking in bookings {
        let command = SystemCommand::ReleaseCapacity { booking };
        cell.record(client, 0, &command).await?;
        assert!(
            cell.registry().booking(booking).is_none(),
            "a released booking is no longer live"
        );
        released += 1;
    }
    Ok(released)
}
