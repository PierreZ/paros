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
//! Placement is the founding members until #212: a redundancy mode takes
//! as many as it asks for, at most all of them; a grid needs all its cells.
//!
//! The desk draws no randomness of its own: its ids derive from the seed the
//! node loop drew for the candidacy, and its one BUGGIFY decision (reuse a
//! taken id once) was drawn there too.

use std::collections::BTreeMap;

use moonpool_core::Providers;
use paros_core::{AcceptorConfig, JournalId, JournalIdentifier, LeaderUuid, NodeId, TenantId};

use crate::client::Client;
use crate::client::checkpoint::{
    AppendOutcome, CheckpointPolicy, Checkpointer, Folder, LoadOutcome, OpenOutcome, load,
};
use crate::client::journals::{JournalAnswer, JournalOp, JournalRequest};
use crate::client::{ClaimOutcome, WriterOutcome};
use crate::system::Registry;
use crate::tenant::{
    CellKind, Description, Desired, DesiredMode, RequestOutcome, TenantCommand, TenantControl,
};

/// What `HostTenant` records of a tenant: its control journal, its name and
/// what it survives.
type Hosted = (JournalId, Vec<u8>, crate::tenant::Survives);

/// How many ids one create draws before it answers unavailable: an
/// `IdTaken` costs one, and a random 64-bit id is taken almost never.
const ID_DRAWS: u64 = 4;

/// The tenant coordinator's state for one term.
pub(crate) struct TenantDesk {
    /// The term's uuid: the fence on every tenant control journal.
    uuid: LeaderUuid,
    /// The cell control journal, where `HostTenant` names each tenant's
    /// control journal.
    cell: JournalIdentifier,
    /// The founding members, in id order: the placement until #212.
    founders: Vec<NodeId>,
    /// The cell control journal's fold, read (never written) here.
    registry: Folder<Registry>,
    /// Each tenant's control journal, claimed under the term's uuid.
    tenants: BTreeMap<TenantId, Checkpointer<TenantControl>>,
    /// The seed ids derive from, and how many were drawn.
    seed: u128,
    draws: u64,
    /// Reuse a taken id at the first draw of every create in a tenant that
    /// has one (a BUGGIFY decision of the node loop): the `IdTaken`
    /// redraw's way in.
    reuse: bool,
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
        (seed, reuse): (u128, bool),
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
            seed,
            draws: 0,
            reuse,
            policy,
            superseded: false,
        }
    }

    /// A tenant control journal refused the term's uuid: the coordinator
    /// must end its term, never claim again under that uuid.
    pub(crate) fn superseded(&self) -> bool {
        self.superseded
    }

    /// Answer `request`, through `client`.
    #[tracing::instrument(level = "debug", skip_all, fields(tenant = request.tenant.0))]
    pub(crate) async fn answer<P: Providers>(
        &mut self,
        client: &Client<P>,
        request: &JournalRequest,
    ) -> JournalAnswer {
        if self.superseded {
            return JournalAnswer::NotCoordinator;
        }
        let answer = self.decide(client, request).await;
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
                let Some(members) = self.place(desired) else {
                    moonpool_assertions::reachable!("tenant coordinator: a create is unplaceable");
                    return JournalAnswer::Unplaceable;
                };
                for attempt in 0..ID_DRAWS {
                    let id = self.draw(request.tenant, attempt == 0);
                    let start = usize::try_from(id.0 % members.len() as u64).unwrap_or(0);
                    let mut chosen: Vec<NodeId> = members
                        .iter()
                        .cycle()
                        .skip(start)
                        .take(members.len())
                        .copied()
                        .collect();
                    chosen.sort_unstable();
                    let config = AcceptorConfig::new(chosen, desired.quorum_system());
                    let command = TenantCommand::CreateJournal {
                        request: request.request,
                        id,
                        name: name.clone(),
                        writer: *writer,
                        desired: *desired,
                        config,
                    };
                    moonpool_buggify::hint!("tenant create drawn, not written").await;
                    if let Err(answer) = self.append(client, request.tenant, &command).await {
                        return answer;
                    }
                    if let Some(outcome) =
                        self.desk(request.tenant).state().outcome(request.request)
                    {
                        return self.answer_of(request.tenant, outcome);
                    }
                    moonpool_assertions::reachable!(
                        "tenant coordinator: an id taken is drawn again"
                    );
                }
                JournalAnswer::Unavailable
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
                match self.desk(request.tenant).state().outcome(request.request) {
                    Some(outcome) => self.answer_of(request.tenant, outcome),
                    None => JournalAnswer::Unavailable,
                }
            }
        }
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

    /// Write `command` to `tenant`'s control journal, and fold it. An
    /// ambiguous write is settled by reading the journal to its tail.
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
        match desk.append(client, command.encode(), 0).await {
            AppendOutcome::Written(WriterOutcome::Written { .. }) => {
                let tail = desk.writer().next_seq();
                if desk.folder().next_seq() < tail {
                    // Another entry landed first (a lost answer's retry):
                    // fold up to this one.
                    let _ = desk.load(client, 0, tail).await;
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
        Ok(())
    }

    /// The members a create under `desired` takes: the founding members, as
    /// many as a redundancy asks for (at most all of them), exactly a
    /// grid's cells. `None` when a grid has too few.
    fn place(&self, desired: &Desired) -> Option<Vec<NodeId>> {
        let wanted = desired.acceptors();
        match desired.mode {
            DesiredMode::Redundancy(_) => {
                Some(self.founders.iter().copied().take(wanted.max(1)).collect())
            }
            DesiredMode::Grid { .. } if self.founders.len() >= wanted => {
                Some(self.founders.iter().copied().take(wanted).collect())
            }
            DesiredMode::Grid { .. } => None,
        }
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
        self.draws += 1;
        let low = u64::try_from(self.seed & u128::from(u64::MAX)).unwrap_or(0);
        let high = u64::try_from(self.seed >> 64).unwrap_or(0);
        let mut x = low ^ high.rotate_left(17) ^ self.draws.wrapping_mul(0x9E37_79B9_7F4A_7C15);
        x ^= tenant.0.rotate_left(29);
        x ^= x >> 30;
        x = x.wrapping_mul(0xBF58_476D_1CE4_E5B9);
        x ^= x >> 27;
        x = x.wrapping_mul(0x94D0_49BB_1331_11EB);
        x ^= x >> 31;
        JournalId(x.max(1))
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
        }
    }
}
