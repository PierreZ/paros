//! **The tenant coordinator** (#210, `docs/architecture/03-3-coordinators-and-placement.md` §3.3): the
//! single writer of every hosted tenant's control journal, which answers the
//! journal requests ([`paros_client::journals`]).
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
//! machines, so they are the machines a create may use: one acceptor
//! partition takes as many as it asks for, at most all of them, and more
//! acceptor partitions need all their acceptors (#523). Inside them, [`paros_model::placement::plan`] picks the members
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

pub mod controls;
pub mod modes;
pub mod moves;
pub mod roles;
pub mod scrub;
pub mod sets;

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use paros_core::NodeId;
use paros_core::journal::{JournalId, JournalIdentifier, LeaderUuid, TenantId};
use paros_core::membership::AcceptorConfig;
use paros_net::runtime::ParosProviders;

use paros_client::capacity::{CapacityAnswer, CapacityRequest, Wanted};
use paros_client::cell::CellSession;
use paros_client::checkpoint::{
    AppendOutcome, CheckpointPolicy, Checkpointer, Folder, LoadOutcome, OpenOutcome, load,
};
use paros_client::fleet::Interrupted;
use paros_client::journals::{JournalAnswer, JournalOp, JournalRequest};
use paros_client::{ClaimOutcome, Client, ClientTunables, WriterOutcome};
use paros_model::placement::{self, Shortfall, Step, Want};
use paros_model::registry::{
    Booking, BookingTarget, GrantKey, NodeStanding, Registry, Role, SystemCommand,
};
use paros_model::tenant::{
    ConfigureTarget, Description, Desired, RequestOutcome, StoreDurability, TenantCommand,
    TenantControl, TenantRoles,
};

/// What `HostTenant` records of a tenant: its control journal, its name and
/// what it survives.
type Hosted = (JournalId, Vec<u8>, paros_model::tenant::Survives);

/// How many ids one create draws before it answers unavailable: an
/// `IdTaken` costs one, and a random 64-bit id is taken almost never.
const ID_DRAWS: u64 = 4;

/// `client`, or, when `impatient`, a copy of it that stops listening at
/// once: every call and read it makes is abandoned unanswered (the floors of
/// [`ClientTunables`] broken on purpose, for one call).
fn impatient<P: ParosProviders>(client: &Client<P>, impatient: bool) -> Client<P> {
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
    /// Each machine's busyness step, as the cell coordinator's last watch
    /// round measured it (#837 (remove role slots)): placement books the
    /// least busy machine of a zone first.
    busy: BTreeMap<NodeId, u8>,
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
            busy: BTreeMap::new(),
            impatient: (impatient, impatient),
            abandon,
            policy,
            superseded: false,
        }
    }

    /// The busyness the last watch round measured (#837 (remove role
    /// slots)).
    pub(crate) fn set_busyness(&mut self, busy: BTreeMap<NodeId, u8>) {
        self.busy = busy;
    }

    /// The machines of `nodes` placement may book, as
    /// [`placement::machines`] gives them, with the busyness the last
    /// watch round measured.
    pub(crate) fn machines(
        &self,
        registry: &Registry,
        nodes: impl IntoIterator<Item = NodeId>,
    ) -> Vec<placement::Machine> {
        placement::with_busyness(placement::machines(registry, nodes), &self.busy)
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
    pub(crate) async fn answer<P: ParosProviders>(
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
    async fn decide<P: ParosProviders>(
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
                desired: Desired::DEFAULT,
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
                tags,
                store,
            } => {
                // A create that names no mode takes the tenant's default
                // (#214 (tenant modes)).
                let desired =
                    desired.unwrap_or_else(|| self.desk(request.tenant).state().default_desired());
                self.create(
                    client,
                    request,
                    (name, *writer, desired, *store),
                    tags,
                    cell,
                )
                .await
            }
            JournalOp::Configure {
                name,
                desired,
                roles,
            } => {
                self.configure(client, request, (name.as_deref(), *desired, *roles), cell)
                    .await
            }
            JournalOp::SetTags { name, tags } => {
                let id = self
                    .desk(request.tenant)
                    .state()
                    .named(name)
                    .unwrap_or(JournalId(0));
                let command = TenantCommand::SetJournalTags {
                    request: request.request,
                    id,
                    tags: tags.clone(),
                };
                if let Err(answer) = self.append(client, request.tenant, &command).await {
                    return answer;
                }
                match self.desk(request.tenant).state().outcome(request.request) {
                    Some(outcome) => {
                        moonpool_assertions::reachable!(
                            "tenant coordinator: a tag request is judged at apply"
                        );
                        self.answer_of(request.tenant, outcome)
                    }
                    None => JournalAnswer::Unavailable,
                }
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
    #[allow(clippy::too_many_lines)] // placement, booking and the write, in order
    async fn create<P: ParosProviders>(
        &mut self,
        client: &Client<P>,
        request: &JournalRequest,
        (name, writer, desired, store): (
            &[u8],
            paros_core::journal::WriterMode,
            Desired,
            StoreDurability,
        ),
        tags: &paros_model::tags::UserTags,
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
        let count = placed_count(&desired, storage.len());
        if count > storage.len() {
            moonpool_assertions::reachable!("tenant coordinator: a create is unplaceable");
            return self.refuse(client, request, name).await;
        }
        // A live name answers `NameTaken` at apply: it books nothing. An
        // unsettled fold may lack a live journal whose create landed
        // unanswered: its orphans wait for a settled one. It may also lack
        // this term's delete of the name, so a name it holds live is
        // trusted only on a settled fold: else the create books, and the
        // fold judges the name. Seed 4507917792168049601 (recipe
        // 686941:15533205754184659368) on fba843a: a delete the catch-up
        // missed freed the name, and the fold accepted a create that
        // booked nothing.
        let named =
            !self.unsettled.contains(&tenant) && self.desk(tenant).state().named(name).is_some();
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
            let config = if named || self.desk(tenant).state().is_taken(id) {
                // Refused at apply (`NameTaken` or `IdTaken`): any members do.
                let members: Vec<NodeId> = storage.iter().copied().take(count).collect();
                AcceptorConfig::new(members, desired.quorum_system())
            } else {
                let machines = self.machines(cell.registry(), storage.iter().copied());
                let current: Vec<(u64, Booking)> = cell
                    .registry()
                    .bookings()
                    .filter(|(_, b)| b.target == want.target)
                    .map(|(id, b)| (id, *b))
                    .collect();
                match zone_plan(&want, &machines, &current, tenant.0, desired) {
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
                        // Owned by this cell (#215 (zones), §3.7).
                        placement::zoned(
                            &placement::booked(&steps),
                            &machines,
                            desired.quorum_system(),
                        )
                        .with_cell(cell.journals().cell_id)
                    }
                    Err(why) => return self.unplaced(client, request, name, why).await,
                }
            };
            assert_eq!(
                config.members().len(),
                count,
                "a create names as many members as it asked"
            );
            let command = TenantCommand::CreateJournal {
                request: request.request,
                id,
                name: name.to_vec(),
                writer,
                desired,
                config,
                tags: tags.clone(),
                store,
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

    /// A create placed nothing: refused and recorded `Unplaceable` when the
    /// zone rule refuses every draw (#215 (zones)); unavailable while too
    /// few machines serve the cell (its founding members register).
    async fn unplaced<P: ParosProviders>(
        &mut self,
        client: &Client<P>,
        request: &JournalRequest,
        name: &[u8],
        why: ZonePlan,
    ) -> JournalAnswer {
        match why {
            ZonePlan::Refused => {
                moonpool_assertions::reachable!(
                    "tenant coordinator: a create the zone rule refuses is unplaceable"
                );
            }
            ZonePlan::Short(Shortfall::Machines { .. }) => {
                moonpool_assertions::reachable!(
                    "tenant coordinator: a create waits for its founding members to register"
                );
                return JournalAnswer::Unavailable;
            }
        }
        self.refuse(client, request, name).await
    }

    /// A configure (#214 (tenant modes)): record a new desired mode for the
    /// live journal `name` names, or for the tenant's default when `None`.
    /// A mode the cell cannot place now is refused and the refusal recorded
    /// (`Unplaceable` with no name), so it changes nothing and a retry reads
    /// it back; the coordinator's watch applies a recorded mode
    /// ([`crate::tenant::modes`]).
    async fn configure<P: ParosProviders>(
        &mut self,
        client: &Client<P>,
        request: &JournalRequest,
        (name, desired, roles): (Option<&[u8]>, Desired, Option<TenantRoles>),
        cell: Option<&mut CellSession>,
    ) -> JournalAnswer {
        let tenant = request.tenant;
        let Some(cell) = cell else {
            // The term's cell session ended: no placement can be judged.
            return JournalAnswer::Unavailable;
        };
        let target = match name {
            None => ConfigureTarget::TenantDefault,
            // A name no live journal holds is recorded `UnknownJournal`.
            Some(name) => ConfigureTarget::Journal(
                self.desk(tenant)
                    .state()
                    .named(name)
                    .unwrap_or(JournalId(0)),
            ),
        };
        let storage = self.storage(cell.registry());
        // The machines placement may book, as the mode change counts them
        // ([`crate::tenant::modes`]): a mode wider than those never lands.
        let eligible = self
            .machines(cell.registry(), storage.iter().copied())
            .len();
        let count = placed_count(&desired, eligible);
        if count > eligible {
            moonpool_assertions::reachable!(
                "tenant coordinator: a configure too wide for the cell is refused"
            );
            return self.refuse(client, request, &[]).await;
        }
        if let ConfigureTarget::Journal(id) = target
            && id.is_set()
        {
            let journal = JournalIdentifier::new(tenant, id);
            let want = Want {
                role: Role::Acceptor,
                target: BookingTarget::Journal(journal),
                count,
            };
            let machines = self.machines(cell.registry(), storage.iter().copied());
            let current: Vec<(u64, Booking)> = cell
                .registry()
                .bookings()
                .filter(|(_, b)| b.role == Role::Acceptor && b.target == want.target)
                .map(|(id, b)| (id, *b))
                .collect();
            match placement::plan(&want, &machines, &current, tenant.0) {
                Ok(_) => {}
                Err(Shortfall::Machines { .. }) => return JournalAnswer::Unavailable,
            }
        }
        let command = TenantCommand::Configure {
            request: request.request,
            target,
            desired,
            roles,
        };
        if let Err(answer) = self.append(client, tenant, &command).await {
            return answer;
        }
        match self.desk(tenant).state().outcome(request.request) {
            Some(outcome) => {
                moonpool_assertions::reachable!("tenant coordinator: a configure is recorded");
                self.answer_of(tenant, outcome)
            }
            None => JournalAnswer::Unavailable,
        }
    }

    /// The fold of `tenant`'s control journal `control`, claimed under the
    /// term's uuid and folded to its tail first when the desk does not hold
    /// it yet: the coordinator's watch reads a tenant's desired modes here
    /// ([`crate::tenant::modes`], [`crate::tenant::sets`]).
    ///
    /// # Errors
    ///
    /// `NotCoordinator` once a later term fenced the journal (the desk is
    /// then superseded), `Unavailable` when it could not be claimed or read.
    pub(crate) async fn tenant<P: ParosProviders>(
        &mut self,
        client: &Client<P>,
        tenant: TenantId,
        control: JournalId,
    ) -> Result<&TenantControl, JournalAnswer> {
        if self.superseded {
            return Err(JournalAnswer::NotCoordinator);
        }
        if let Err(answer) = self.open(client, tenant, control).await {
            if matches!(answer, JournalAnswer::NotCoordinator) {
                self.superseded = true;
                self.tenants.clear();
            }
            return Err(answer);
        }
        Ok(self.desk(tenant).state())
    }

    /// Record `config` as journal `id`'s configuration in force in
    /// `tenant`'s control journal, once the watch applied its desired mode
    /// (#214 (tenant modes)).
    ///
    /// # Errors
    ///
    /// The write did not land, or a later term fenced the journal.
    pub(crate) async fn record_placed<P: ParosProviders>(
        &mut self,
        client: &Client<P>,
        tenant: TenantId,
        id: JournalId,
        config: AcceptorConfig,
    ) -> Result<(), JournalAnswer> {
        let command = TenantCommand::Placed { id, config };
        let written = self.append(client, tenant, &command).await;
        if matches!(written, Err(JournalAnswer::NotCoordinator)) {
            self.superseded = true;
            self.tenants.clear();
        }
        written
    }

    /// Record the tenant scrubber's `progress` in `tenant`'s control
    /// journal (#362 (background scrubbing)).
    ///
    /// # Errors
    ///
    /// The write did not land, or a later term fenced the journal.
    pub(crate) async fn record_scrub<P: ParosProviders>(
        &mut self,
        client: &Client<P>,
        tenant: TenantId,
        progress: paros_model::tenant::ScrubProgress,
    ) -> Result<(), JournalAnswer> {
        let command = TenantCommand::Scrubbed(progress);
        let written = self.append(client, tenant, &command).await;
        if matches!(written, Err(JournalAnswer::NotCoordinator)) {
            self.superseded = true;
            self.tenants.clear();
        }
        written
    }

    /// The cell's storage machines a create may be placed on (#474): the
    /// founding members, registered or not, and every registered machine
    /// whose class stores. A draining or retired machine takes no new
    /// journal.
    pub(crate) fn storage(&self, registry: &Registry) -> BTreeSet<NodeId> {
        let mut storage: BTreeSet<NodeId> = self.founders.iter().copied().collect();
        storage.extend(
            registry
                .nodes()
                .filter(|(_, n)| {
                    n.standing == NodeStanding::Registered && n.class.takes(Role::Acceptor.class())
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
    async fn release_orphans<P: ParosProviders>(
        &self,
        client: &Client<P>,
        tenant: TenantId,
        cell: &mut CellSession,
    ) -> Result<(), Interrupted> {
        let state = self.desk(tenant).state();
        let orphan = |b: &Booking| match b.target {
            // The tenant's control journal holds slots of its own once the
            // coordinator grows it (#685 (special tenants are tenants)):
            // never an orphan.
            BookingTarget::Journal(journal) => {
                journal.tenant == tenant
                    && journal.journal != state.control()
                    && state
                        .get(journal.journal)
                        .is_none_or(|j| j.deleted_at.is_some())
            }
            BookingTarget::Set { .. } | BookingTarget::Tenant { .. } => false,
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
    /// recorded. A configure's refusal names no journal: no live journal
    /// holds the empty name.
    async fn refuse<P: ParosProviders>(
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

    /// Book the slots `steps` asks for `want` as one capacity request
    /// (#476) through the term's cell session: every slot or none, under a
    /// request id and booking ids drawn from the seed.
    ///
    /// # Errors
    ///
    /// `Unavailable` when the grant did not land, or the registry refused
    /// it: the term's session is over, or its fold was behind.
    ///
    /// # Panics
    ///
    /// If a grant books another number of slots than it asked for.
    pub(crate) async fn book<P: ParosProviders>(
        &mut self,
        client: &Client<P>,
        cell: &mut CellSession,
        want: &Want,
        steps: &[Step],
    ) -> Result<(), JournalAnswer> {
        let slots: Vec<Wanted> = placement::booked(steps)
            .into_iter()
            .map(|node| Wanted {
                node,
                role: want.role,
                target: want.target,
            })
            .collect();
        if !slots.is_empty() {
            // The tenant coordinator asks the cell coordinator for the
            // slots it placed (#476): here a call into the term's own
            // session, under an idempotency id of its own.
            let Some(request) = (0..ID_DRAWS)
                .map(|_| self.mix(0xCA9A).max(1))
                .find(|id| cell.registry().grant_of(GrantKey::Request(*id)).is_none())
            else {
                return Err(JournalAnswer::Unavailable);
            };
            let request = CapacityRequest {
                request,
                tenant: want.target.tenant(),
                slots,
            };
            match crate::cell::capacity::grant(client, cell, &request, || self.mix(0xB00C)).await {
                CapacityAnswer::Granted(bookings) => {
                    assert_eq!(
                        bookings.len(),
                        request.slots.len(),
                        "a grant books every slot"
                    );
                }
                _ => return Err(JournalAnswer::Unavailable),
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
    async fn hosted<P: ParosProviders>(
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
    async fn open<P: ParosProviders>(
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
    async fn append<P: ParosProviders>(
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
    /// A booking id from the candidacy's seed (#476): a capacity request's.
    pub(crate) fn draw_booking(&mut self) -> u64 {
        self.mix(0xB00C)
    }

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
            RequestOutcome::Tagged(id) => JournalAnswer::Tagged { id },
            RequestOutcome::NameTaken(id) => JournalAnswer::NameTaken { id },
            RequestOutcome::UnknownJournal => JournalAnswer::UnknownJournal,
            RequestOutcome::Unplaceable => JournalAnswer::Unplaceable,
            RequestOutcome::Configured(id) => JournalAnswer::Configured { id },
        }
    }
}

/// How many acceptors a journal under `desired` is placed on, in a cell of
/// `storage` storage machines (#212 (placement), #214 (tenant modes)): one
/// acceptor partition is a majority, so it takes at most every storage
/// machine; more acceptor partitions lay out a grid that needs all its
/// acceptors.
pub(crate) fn placed_count(desired: &Desired, storage: usize) -> usize {
    let wanted = desired.acceptors();
    if desired.durability.partitions == 1 {
        wanted.max(1).min(storage)
    } else {
        wanted
    }
}

/// Release every live booking `which` selects, through the term's cell
/// session: how many it released.
///
/// # Errors
///
/// A write did not land: the session is closed, and the rest stay booked.
pub(crate) async fn release<P: ParosProviders>(
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

/// Why [`zone_plan`] placed nothing.
enum ZonePlan {
    /// The machines cannot hold the count.
    Short(Shortfall),
    /// The zone rule binds and no draw met it (#215 (zones)).
    Refused,
}

/// The placement of a create (#212 (placement)) under the zone rule (#215
/// (zones)): where the rule binds ([`placement::zone_rule_binds`]), a plan
/// whose labeled configuration breaks it is planned again under another
/// draw, up to [`placement::ZONE_DRAWS`], and the shape is refused when
/// none holds. Where it does not bind, the first plan stands.
fn zone_plan(
    want: &Want,
    machines: &[placement::Machine],
    current: &[(u64, Booking)],
    seed: u64,
    desired: Desired,
) -> Result<Vec<Step>, ZonePlan> {
    let first = placement::plan(want, machines, current, seed).map_err(ZonePlan::Short)?;
    if !placement::zone_rule_binds(
        desired.durability.survive_machines,
        paros_model::machine::Class::Storage,
        machines,
    ) {
        return Ok(first);
    }
    let holds = |steps: &[Step]| {
        placement::zoned(&placement::booked(steps), machines, desired.quorum_system())
            .zone_rule()
            .holds()
    };
    if holds(&first) {
        moonpool_assertions::reachable!("tenant coordinator: a create meets the zone rule");
        return Ok(first);
    }
    for draw in 1..placement::ZONE_DRAWS {
        let reseeded = seed ^ draw.wrapping_mul(0x9E37_79B9_7F4A_7C15);
        if let Ok(steps) = placement::plan(want, machines, current, reseeded)
            && holds(&steps)
        {
            return Ok(steps);
        }
    }
    Err(ZonePlan::Refused)
}
