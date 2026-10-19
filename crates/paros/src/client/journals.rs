//! **A tenant's journals** (#210, `docs/architecture/03-3-coordinators-and-placement.md` §3.3): create and
//! delete them through the tenant coordinator, and list them from the
//! tenant's control journal.
//!
//! A create or a delete is a request with an idempotency id the caller
//! draws. The library sends it to the tenant coordinator — until #212 and
//! #225, the elected cell coordinator, found at the interface the election
//! journal's leader publishes: its address and the reference of its term's
//! group, a dynamic endpoint ([`Coordinator`], #404 (static vs dynamic
//! endpoints)) — and re-sends the same id when the answer
//! does not decide it (no coordinator, a coordinator change, a timeout). The
//! coordinator writes the request to the tenant's control journal, and the
//! fold judges it at apply and records its outcome under the id
//! ([`crate::tenant::TenantControl`]): a retry reads that outcome back, so
//! it acts once. A list is a plain read: fold the control journal.
//!
//! The library draws no randomness: the request id is the caller's.

use std::time::Duration;

use moonpool_core::{Providers, TimeProvider};
use moonpool_rpc::{InterfaceMethod, InterfaceRef, RpcError, RpcHandle};
use paros_core::{AcceptorConfig, JournalId, JournalIdentifier, TenantId, WriterMode};

use super::Client;
use super::checkpoint::{Folder, LoadOutcome, load};
use super::election::{Candidate, read_election};
use crate::rpc::in_group;
use crate::rpc::machine as wire;
use crate::rpc::methods::{CoordinatorInterface, JournalRequestRpc};
use crate::rpc::{
    config_from_proto, config_to_proto, writer_mode_from_proto, writer_mode_to_proto,
};
use crate::tags::UserTags;
use crate::tenant::{Desired, StoreDurability, TenantControl, TenantRoles};
use crate::{Address, Names};

/// What a request asks for.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum JournalOp {
    /// Create a journal named `name` in `writer` mode under `desired`.
    Create {
        /// Its name, unique among the tenant's live journals.
        name: Vec<u8>,
        /// Who may write it (#241), fixed for its life.
        writer: WriterMode,
        /// The desired mode: the coordinator picks the members. `None`
        /// takes the tenant's default (#214 (tenant modes)).
        desired: Option<Desired>,
        /// Its first user tags (#575).
        tags: UserTags,
        /// How its stores commit a batch (#599 (durability mode per
        /// journal)), fixed for its life; `Ordered` is the default.
        store: StoreDurability,
    },
    /// Delete the live journal named `name`.
    Delete {
        /// Its name.
        name: Vec<u8>,
    },
    /// The live journal named `name`'s user tags become `tags` (#575).
    SetTags {
        /// Its name.
        name: Vec<u8>,
        /// Its user tags, all of them.
        tags: UserTags,
    },
    /// Change a desired mode (#214 (tenant modes)): of the live journal
    /// named `name`, or the tenant's default when `None`. The coordinator
    /// applies a journal's new mode by reconfiguration and placement.
    Configure {
        /// The journal's name; `None` for the tenant's default.
        name: Option<Vec<u8>>,
        /// The new mode.
        desired: Desired,
        /// The tenant's new role counts (#604), on the tenant default only
        /// (`name` is `None`); `None` keeps the counts it has.
        roles: Option<TenantRoles>,
    },
}

/// One request to the tenant coordinator.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JournalRequest {
    /// The idempotency id: set, the same on every retry.
    pub request: u64,
    /// The tenant whose journal it is.
    pub tenant: TenantId,
    /// What it asks for.
    pub op: JournalOp,
}

impl JournalRequest {
    /// The wire form.
    #[must_use]
    pub fn to_wire(&self) -> wire::JournalRequest {
        use wire::journal_request::Op;
        let op = match &self.op {
            JournalOp::Create {
                name,
                writer,
                desired,
                tags,
                store,
            } => Op::Create(wire::CreateJournalOp {
                name: name.clone(),
                writer: writer_mode_to_proto(*writer).into(),
                replicas: desired.map_or(0, |d| d.replicas),
                failures_per_partition: desired.map_or(0, |d| d.durability.failures_per_partition),
                // 0: the tenant's default.
                partitions: desired.map_or(0, |d| d.durability.partitions),
                tags: tags.to_wire(),
                store_durability: store.to_proto().into(),
            }),
            JournalOp::Delete { name } => Op::Delete(wire::DeleteJournalOp { name: name.clone() }),
            JournalOp::SetTags { name, tags } => Op::SetTags(wire::SetJournalTagsOp {
                name: name.clone(),
                tags: tags.to_wire(),
            }),
            JournalOp::Configure {
                name,
                desired,
                roles,
            } => Op::Configure(wire::ConfigureOp {
                name: name.clone().unwrap_or_default(),
                replicas: desired.replicas,
                failures_per_partition: desired.durability.failures_per_partition,
                partitions: desired.durability.partitions,
                roles: roles.is_some(),
                frontends: roles.map_or(0, |r| r.frontends),
                proxy_leaders: roles.map_or(0, |r| r.proxy_leaders),
                batchers: roles.map_or(0, |r| r.batchers),
            }),
        };
        wire::JournalRequest {
            request: self.request,
            tenant: self.tenant.0,
            op: Some(op),
        }
    }

    /// From the wire.
    ///
    /// # Errors
    ///
    /// An unset request id or tenant, an empty name, no op, or a field out
    /// of range: a malformed or reserved tag, or a set over the cap (#575),
    /// included.
    pub fn from_wire(request: &wire::JournalRequest) -> Result<Self, &'static str> {
        use wire::journal_request::Op;
        if request.request == 0 {
            return Err("a request names its idempotency id");
        }
        if request.tenant == 0 {
            return Err("a request names its tenant");
        }
        let op = match request.op.as_ref().ok_or("a request names an op")? {
            Op::Create(create) => JournalOp::Create {
                name: create.name.clone(),
                writer: writer_mode_from_proto(create.writer)?,
                desired: if create.partitions == 0
                    && create.failures_per_partition == 0
                    && create.replicas == 0
                {
                    None
                } else {
                    Some(Desired::from_wire(Some(crate::rpc::tenant::Desired {
                        replicas: create.replicas,
                        failures_per_partition: create.failures_per_partition,
                        partitions: create.partitions,
                    }))?)
                },
                tags: UserTags::from_wire(create.tags.clone()).map_err(|_| "malformed tags")?,
                store: StoreDurability::from_proto(create.store_durability)?,
            },
            Op::Delete(delete) => JournalOp::Delete {
                name: delete.name.clone(),
            },
            Op::SetTags(set) => JournalOp::SetTags {
                name: set.name.clone(),
                tags: UserTags::from_wire(set.tags.clone()).map_err(|_| "malformed tags")?,
            },
            Op::Configure(configure) => JournalOp::Configure {
                name: (!configure.name.is_empty()).then(|| configure.name.clone()),
                desired: Desired::from_wire(Some(crate::rpc::tenant::Desired {
                    replicas: configure.replicas,
                    failures_per_partition: configure.failures_per_partition,
                    partitions: configure.partitions,
                }))?,
                roles: match (configure.roles, configure.name.is_empty()) {
                    (false, _) => None,
                    (true, false) => return Err("role counts are the tenant default's"),
                    (true, true) => Some(TenantRoles::new(
                        configure.frontends,
                        configure.proxy_leaders,
                        configure.batchers,
                    )?),
                },
            },
        };
        if let JournalOp::Create { name, .. }
        | JournalOp::Delete { name }
        | JournalOp::SetTags { name, .. } = &op
            && name.is_empty()
        {
            return Err("a journal has a name");
        }
        Ok(Self {
            request: request.request,
            tenant: TenantId(request.tenant),
            op,
        })
    }
}

/// What a request came to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum JournalAnswer {
    /// The journal exists as `id`, on `config`.
    Created {
        /// Its id, drawn by the coordinator.
        id: JournalId,
        /// Its members, picked by the coordinator.
        config: AcceptorConfig,
    },
    /// The journal `id` is a tombstone.
    Deleted {
        /// Its id.
        id: JournalId,
    },
    /// The journal `id`'s user tags are the request's (#575).
    Tagged {
        /// Its id.
        id: JournalId,
    },
    /// A live journal holds the name: `id`.
    NameTaken {
        /// The live journal's id.
        id: JournalId,
    },
    /// A desired mode is recorded (#214 (tenant modes)): the journal `id`'s,
    /// or the tenant default's (`None`). The coordinator applies a journal's
    /// mode afterwards.
    Configured {
        /// The journal, or `None` for the tenant's default.
        id: Option<JournalId>,
    },
    /// No live journal has the name.
    UnknownJournal,
    /// The cell hosts no such tenant.
    UnknownTenant,
    /// Too few machines for the desired mode.
    Unplaceable,
    /// The request is not well-formed.
    Malformed,
    /// The machine asked does not coordinate the tenant now: retry.
    NotCoordinator,
    /// The coordinator could not reach the fold, or no answer came: retry
    /// with the same id.
    Unavailable,
}

impl JournalAnswer {
    /// Whether a retry with the same id may decide it.
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            JournalAnswer::NotCoordinator | JournalAnswer::Unavailable
        )
    }

    /// Its label on the wire and in `parosctl`.
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            JournalAnswer::Created { .. } => "created",
            JournalAnswer::Deleted { .. } => "deleted",
            JournalAnswer::Tagged { .. } => "tagged",
            JournalAnswer::NameTaken { .. } => "name_taken",
            JournalAnswer::Configured { .. } => "configured",
            JournalAnswer::UnknownJournal => "unknown_journal",
            JournalAnswer::UnknownTenant => "unknown_tenant",
            JournalAnswer::Unplaceable => "unplaceable",
            JournalAnswer::Malformed => "malformed",
            JournalAnswer::NotCoordinator => "not_coordinator",
            JournalAnswer::Unavailable => "unavailable",
        }
    }

    /// The wire form.
    #[must_use]
    pub fn to_wire(&self) -> wire::JournalRequestAck {
        let (journal, config) = match self {
            JournalAnswer::Created { id, config } => (id.0, Some(config_to_proto(config))),
            JournalAnswer::Deleted { id }
            | JournalAnswer::Tagged { id }
            | JournalAnswer::NameTaken { id } => (id.0, None),
            JournalAnswer::Configured { id } => (id.map_or(0, |id| id.0), None),
            _ => (0, None),
        };
        wire::JournalRequestAck {
            outcome: self.as_str().into(),
            journal,
            config,
        }
    }

    /// From the wire: an answer the library cannot read is
    /// [`JournalAnswer::Unavailable`], retried.
    #[must_use]
    pub fn from_wire(ack: wire::JournalRequestAck) -> Self {
        let id = JournalId(ack.journal);
        match ack.outcome.as_str() {
            "created" if id.is_set() => match config_from_proto(ack.config) {
                Ok(Some(config)) => JournalAnswer::Created { id, config },
                _ => JournalAnswer::Unavailable,
            },
            "deleted" if id.is_set() => JournalAnswer::Deleted { id },
            "tagged" if id.is_set() => JournalAnswer::Tagged { id },
            "name_taken" if id.is_set() => JournalAnswer::NameTaken { id },
            "configured" => JournalAnswer::Configured {
                id: id.is_set().then_some(id),
            },
            "unknown_journal" => JournalAnswer::UnknownJournal,
            "unknown_tenant" => JournalAnswer::UnknownTenant,
            "unplaceable" => JournalAnswer::Unplaceable,
            "malformed" => JournalAnswer::Malformed,
            "not_coordinator" => JournalAnswer::NotCoordinator,
            _ => JournalAnswer::Unavailable,
        }
    }
}

/// What a tag request through a frontend came to (#575 (key:value tags)):
/// one attempt.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FrontTagged {
    /// The frontend refused the call itself: no machine saw it.
    Denied(crate::frontend::Denial),
    /// The tenant coordinator's answer, as the frontend relayed it.
    /// `Unavailable` and `NotCoordinator` decide nothing: send the same
    /// request again.
    Answered(JournalAnswer),
    /// No answer came: the request may have acted. Send the same request
    /// again, which acts once.
    Lost,
}

/// Set the user tags of the journal `name` to `tags` through the frontend
/// `frontend`, under the idempotency id `request`: one attempt (#575).
pub async fn tag_through_frontend<P: Providers>(
    frontend: &crate::rpc::NodeClient<P>,
    name: &crate::name::JournalName,
    request: u64,
    tags: &UserTags,
) -> FrontTagged {
    match frontend
        .set_journal_tags(name, request, tags.to_wire())
        .await
    {
        Err(_) => FrontTagged::Lost,
        Ok(ack) => match crate::frontend::Denial::of(ack.frontend()) {
            Some(denial) => FrontTagged::Denied(denial),
            None if ack.frontend() == crate::rpc::FrontendVerdict::Unanswered => FrontTagged::Lost,
            None => FrontTagged::Answered(JournalAnswer::from_wire(wire::JournalRequestAck {
                outcome: ack.outcome,
                journal: ack.journal,
                config: None,
            })),
        },
    }
}

/// The pause between two attempts of one request.
const RETRY_PAUSE: Duration = Duration::from_millis(200);

/// The cell coordinator of one term, as the election journal's leader
/// publishes it (#404 (static vs dynamic endpoints)): the address it
/// advertises, a literal or a name (#257), and the reference of its term's
/// group. The address says *where*; the reference says *which instance*,
/// so a request to a term that ended fails at the transport.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Coordinator {
    /// The address the coordinator advertises.
    pub address: Address,
    /// Its term's group.
    pub reference: InterfaceRef<CoordinatorInterface>,
}

impl Coordinator {
    /// The coordinator `candidate` publishes: `None` while it published no
    /// address or no reference, or one that does not decode as the
    /// coordinator's interface.
    #[must_use]
    pub fn of(candidate: &Candidate) -> Option<Self> {
        let address = Address::parse(&candidate.interface).ok()?;
        let reference = InterfaceRef::from_bytes(&candidate.reference).ok()?;
        Some(Self { address, reference })
    }

    /// One attempt of `M` at this term's group, the address resolved now
    /// through `names`, within `timeout`: `None` when the name does not
    /// resolve or nothing came back.
    pub(crate) async fn call<P: Providers, M: InterfaceMethod<CoordinatorInterface>>(
        &self,
        providers: &P,
        rpc: &RpcHandle<P>,
        names: &Names,
        request: &M::Request,
        timeout: Duration,
    ) -> Option<Result<M::Reply, RpcError>>
    where
        M::Request: Sync,
    {
        let call = async {
            let addr = names.resolve(&self.address).await.ok()?;
            let client = in_group::<P, CoordinatorInterface, M>(rpc, &self.reference, addr)?;
            let reply = client.try_get_reply(request).await;
            if reply
                .as_ref()
                .is_err_and(moonpool_rpc::RpcError::is_terminal_for_reference)
            {
                moonpool_assertions::reachable!(
                    "coordinator: a request to an ended term fails at the transport"
                );
            }
            Some(reply)
        };
        providers.time().timeout(timeout, call).await.ok().flatten()
    }
}

/// The tenant coordinator: the interface the election journal's leader
/// publishes. `None` while no leader published one.
pub async fn coordinator<P: Providers>(
    client: &Client<P>,
    election: JournalIdentifier,
) -> Option<Coordinator> {
    let fold = read_election(client, election, 0).await?;
    Coordinator::of(&fold.leader()?.candidate)
}

/// Send `request` to the coordinator `target`, once, within `timeout`. A
/// term that ended answers nothing: [`JournalAnswer::Unavailable`], retried.
pub async fn send<P: Providers>(
    providers: &P,
    rpc: &RpcHandle<P>,
    names: &Names,
    target: &Coordinator,
    request: &JournalRequest,
    timeout: Duration,
) -> JournalAnswer {
    match target
        .call::<P, JournalRequestRpc>(providers, rpc, names, &request.to_wire(), timeout)
        .await
    {
        Some(Ok(ack)) => JournalAnswer::from_wire(ack),
        _ => JournalAnswer::Unavailable,
    }
}

/// Send `request` to the tenant coordinator of the cell whose election
/// journal is `election`, the same id again until an answer decides it or
/// `patience` runs out (then the last answer, retryable). The coordinator's
/// address is resolved through `names`.
#[tracing::instrument(level = "debug", skip_all, fields(tenant = request.tenant.0))]
pub async fn request<P: Providers>(
    providers: &P,
    rpc: &RpcHandle<P>,
    names: &Names,
    client: &Client<P>,
    election: JournalIdentifier,
    request: &JournalRequest,
    patience: Duration,
) -> JournalAnswer {
    let deadline = providers.time().now() + patience;
    let timeout = client.tunables().request_timeout;
    let mut attempts = 0_u32;
    loop {
        attempts += 1;
        let answer = match coordinator(client, election).await {
            Some(target) => send(providers, rpc, names, &target, request, timeout).await,
            None => JournalAnswer::NotCoordinator,
        };
        if !answer.is_retryable() || providers.time().now() >= deadline {
            if attempts > 1 && !answer.is_retryable() {
                moonpool_assertions::reachable!("journals: a retried request is decided");
            }
            return answer;
        }
        let _ = providers.time().sleep(RETRY_PAUSE).await;
    }
}

/// Set the user tags of the journal `name` through the frontends
/// `frontends` (#575 (key:value tags)): the same request id sent to each in
/// turn until an answer decides it or `patience` runs out. A denial ends
/// it at once.
#[tracing::instrument(level = "debug", skip_all, fields(request))]
pub async fn tag_through<P: Providers>(
    providers: &P,
    frontends: &Client<P>,
    (name, request): (&crate::name::JournalName, u64),
    tags: &UserTags,
    patience: Duration,
) -> FrontTagged {
    assert!(frontends.server_count() > 0, "a tag request has a frontend");
    let deadline = providers.time().now() + patience;
    let mut index = 0;
    loop {
        let frontend = frontends.node(index % frontends.server_count());
        index += 1;
        let got = tag_through_frontend(frontend, name, request, tags).await;
        let decided = match &got {
            FrontTagged::Denied(_) => true,
            FrontTagged::Answered(answer) => !answer.is_retryable(),
            FrontTagged::Lost => false,
        };
        if decided || providers.time().now() >= deadline {
            return got;
        }
        let _ = providers.time().sleep(RETRY_PAUSE).await;
    }
}

/// Fold tenant `tenant`'s control journal `control` to its tail: every
/// journal it created, and its description. `None` when no page could be
/// read to the tail.
pub async fn list<P: Providers>(
    client: &Client<P>,
    tenant: TenantId,
    control: JournalId,
) -> Option<TenantControl> {
    let journal = JournalIdentifier::new(tenant, control);
    let mut folder = Folder::new(TenantControl::new(tenant, control));
    match load(&mut folder, journal, client, 0, 0).await {
        LoadOutcome::Loaded { .. } => Some(folder.state().clone()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_request_round_trips_and_an_unset_field_is_refused() {
        let request = JournalRequest {
            request: 7,
            tenant: TenantId(3),
            op: JournalOp::Create {
                name: b"orders".to_vec(),
                writer: WriterMode::Multi,
                desired: Some(
                    "failures-per-partition=0,partitions=3"
                        .parse()
                        .expect("a durability"),
                ),
                tags: UserTags::parse(&["team:payments"]).expect("a tag"),
                store: StoreDurability::Batched,
            },
        };
        let wire = request.to_wire();
        assert_eq!(JournalRequest::from_wire(&wire), Ok(request));
        // A create that names no mode takes the tenant's default (#214).
        let defaulted = JournalRequest {
            request: 9,
            tenant: TenantId(3),
            op: JournalOp::Create {
                name: b"events".to_vec(),
                writer: WriterMode::Single,
                desired: None,
                tags: UserTags::none(),
                store: StoreDurability::Ordered,
            },
        };
        assert_eq!(
            JournalRequest::from_wire(&defaulted.to_wire()),
            Ok(defaulted)
        );
        // A configure names a journal, or none for the tenant default.
        for name in [Some(b"orders".to_vec()), None] {
            let configure = JournalRequest {
                request: 10,
                tenant: TenantId(3),
                op: JournalOp::Configure {
                    name,
                    desired: "failures-per-partition=2".parse().expect("a durability"),
                    roles: None,
                },
            };
            assert_eq!(
                JournalRequest::from_wire(&configure.to_wire()),
                Ok(configure)
            );
        }
        let mut no_partition = JournalRequest {
            request: 11,
            tenant: TenantId(3),
            op: JournalOp::Configure {
                name: None,
                desired: Desired::DEFAULT,
                roles: None,
            },
        }
        .to_wire();
        if let Some(crate::rpc::machine::journal_request::Op::Configure(op)) = &mut no_partition.op
        {
            op.partitions = 0;
        }
        assert!(JournalRequest::from_wire(&no_partition).is_err());
        let mut unset = wire.clone();
        unset.request = 0;
        assert!(JournalRequest::from_wire(&unset).is_err());
        let delete = JournalRequest {
            request: 8,
            tenant: TenantId(3),
            op: JournalOp::Delete { name: Vec::new() },
        };
        assert!(JournalRequest::from_wire(&delete.to_wire()).is_err());
    }

    #[test]
    fn a_coordinator_needs_an_address_and_its_own_interface() {
        use moonpool_rpc::{AccessClass, Endpoint, EndpointToken, Incarnation};

        use crate::rpc::methods::MachineInterface;

        let endpoint = Endpoint::new(
            "0.0.0.0:4500".parse().expect("an address"),
            Incarnation::from_raw(7),
            EndpointToken::from_parts(3, 1),
        );
        let reference = InterfaceRef::<CoordinatorInterface>::new(endpoint, AccessClass::Public);
        let candidate = Candidate {
            id: 1,
            interface: "node1:4500".into(),
            reference: crate::rpc::published(&reference),
        };
        let coordinator = Coordinator::of(&candidate).expect("a published coordinator");
        // A wildcard-bound reference is published at the loopback.
        assert_eq!(coordinator.reference.endpoint().token(), endpoint.token());
        assert!(
            !coordinator
                .reference
                .endpoint()
                .address()
                .ip()
                .is_unspecified()
        );
        assert_eq!(coordinator.address.to_string(), "node1:4500");
        // Aimed at the resolved address, the reference keeps its instance.
        let resolved = "10.0.0.1:4500".parse().expect("an address");
        let aimed = crate::rpc::client::aimed(&reference, resolved);
        assert_eq!(aimed.endpoint().address(), resolved);
        assert_eq!(aimed.endpoint().incarnation(), endpoint.incarnation());
        assert_eq!(aimed.endpoint().token(), endpoint.token());
        // Nothing published yet, or another interface's reference: none.
        let unpublished = Candidate {
            reference: Vec::new(),
            ..candidate.clone()
        };
        assert_eq!(Coordinator::of(&unpublished), None);
        let machine = InterfaceRef::<MachineInterface>::new(endpoint, AccessClass::Public);
        let other = Candidate {
            reference: crate::rpc::published(&machine),
            ..candidate
        };
        assert_eq!(Coordinator::of(&other), None);
    }

    #[test]
    fn an_answer_round_trips_and_an_unread_one_is_retried() {
        for answer in [
            JournalAnswer::Deleted { id: JournalId(4) },
            JournalAnswer::NameTaken { id: JournalId(5) },
            JournalAnswer::UnknownJournal,
            JournalAnswer::UnknownTenant,
            JournalAnswer::Unplaceable,
            JournalAnswer::NotCoordinator,
        ] {
            assert_eq!(JournalAnswer::from_wire(answer.to_wire()), answer);
        }
        let unread = wire::JournalRequestAck {
            outcome: "created".into(),
            journal: 0,
            config: None,
        };
        assert_eq!(JournalAnswer::from_wire(unread), JournalAnswer::Unavailable);
    }
}
