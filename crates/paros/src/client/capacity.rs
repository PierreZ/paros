//! **Capacity requests** (#476, `docs/architecture/03-3-coordinators-and-placement.md` §3.3): a tenant
//! coordinator asks the cell coordinator for the slots it placed, under an
//! idempotency id. Only the cell coordinator writes capacity: it draws the
//! booking ids, writes one `GrantCapacity` (every slot booked, or none) and
//! records the outcome under the id in the cell control journal, so a retry
//! that crosses a coordinator change reads the answer back instead of
//! booking twice. A refusal is recorded too: the requester asks again under
//! a new id once slots are released.
//!
//! The coordinator is found through the interface it publishes in the
//! election journal (`crate::client::journals::coordinator`), like a journal
//! request. Like the rest of the client, this draws no randomness: the
//! caller draws the id.

use std::time::Duration;

use crate::ParosProviders;
use moonpool_core::LocalTimeProvider;
use moonpool_rpc::RpcHandle;
use paros_core::{JournalId, JournalIdentifier, NodeId, TenantId};

use super::Client;
use super::journals::{Coordinator, coordinator};
use crate::Names;
use crate::model::registry::{BookingTarget, Role};
use crate::rpc::machine as wire;
use crate::rpc::methods::CapacityRequestRpc;

/// How long a retry waits after an answer that decided nothing.
const RETRY_PAUSE: Duration = Duration::from_millis(200);

/// One slot a request asks for: `role` of `target` on `node`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Wanted {
    /// The machine.
    pub node: NodeId,
    /// The role.
    pub role: Role,
    /// What the slot is for: a target of the request's tenant.
    pub target: BookingTarget,
}

/// A request for capacity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CapacityRequest {
    /// The idempotency id: set, and the same on every retry.
    pub request: u64,
    /// The tenant.
    pub tenant: TenantId,
    /// The slots, all granted or none.
    pub slots: Vec<Wanted>,
}

/// What a capacity request came to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CapacityAnswer {
    /// Every slot is booked: its booking ids, in the request's order.
    Granted(Vec<u64>),
    /// The cell had no room: ask again under a new id.
    Refused,
    /// The term that received it does not serve: retry with the same id.
    NotCoordinator,
    /// The request did not reach the cell control journal: retry with the
    /// same id.
    Unavailable,
    /// The cell hosts no such tenant.
    UnknownTenant,
    /// The request is not well formed: an unset id or tenant, no slot, or a
    /// slot of another tenant.
    Malformed,
}

impl CapacityAnswer {
    /// Whether the same request may be sent again.
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        matches!(self, Self::NotCoordinator | Self::Unavailable)
    }

    /// The wire answer.
    #[must_use]
    pub fn to_wire(&self) -> wire::CapacityRequestAck {
        let (outcome, bookings) = match self {
            Self::Granted(bookings) => ("granted", bookings.clone()),
            Self::Refused => ("refused", Vec::new()),
            Self::NotCoordinator => ("not_coordinator", Vec::new()),
            Self::Unavailable => ("unavailable", Vec::new()),
            Self::UnknownTenant => ("unknown_tenant", Vec::new()),
            Self::Malformed => ("malformed", Vec::new()),
        };
        wire::CapacityRequestAck {
            outcome: outcome.into(),
            bookings,
        }
    }

    /// From the wire: an outcome this client does not know is unavailable,
    /// retried.
    #[must_use]
    pub fn from_wire(ack: wire::CapacityRequestAck) -> Self {
        match ack.outcome.as_str() {
            "granted" => Self::Granted(ack.bookings),
            "refused" => Self::Refused,
            "not_coordinator" => Self::NotCoordinator,
            "unknown_tenant" => Self::UnknownTenant,
            "malformed" => Self::Malformed,
            _ => Self::Unavailable,
        }
    }
}

impl CapacityRequest {
    /// The wire request.
    #[must_use]
    pub fn to_wire(&self) -> wire::CapacityRequest {
        wire::CapacityRequest {
            request: self.request,
            tenant: self.tenant.0,
            slots: self
                .slots
                .iter()
                .map(|slot| {
                    let (journal, set, tenant_level) = match slot.target {
                        BookingTarget::Journal(journal) => (journal.journal.0, 0, false),
                        BookingTarget::Set { set, .. } => (0, set, false),
                        BookingTarget::Tenant { .. } => (0, 0, true),
                    };
                    wire::CapacitySlot {
                        node: slot.node.0,
                        role: slot.role.as_str().into(),
                        journal,
                        set,
                        tenant_level,
                    }
                })
                .collect(),
        }
    }

    /// From the wire: every slot's target is of the request's tenant.
    ///
    /// # Errors
    ///
    /// An unset id or tenant, no slot, an unknown role, or a slot that names
    /// a journal and a set, or neither without being tenant-wide.
    pub fn from_wire(request: &wire::CapacityRequest) -> Result<Self, &'static str> {
        let tenant = TenantId(request.tenant);
        if request.request == 0 || !tenant.is_set() || request.slots.is_empty() {
            return Err("a capacity request names its id, its tenant and a slot");
        }
        let slots = request
            .slots
            .iter()
            .map(|slot| {
                let target = match (slot.journal, slot.set, slot.tenant_level) {
                    (0, 0, true) => BookingTarget::Tenant { tenant },
                    (journal, 0, false) if journal != 0 => {
                        BookingTarget::Journal(JournalIdentifier::new(tenant, JournalId(journal)))
                    }
                    (0, set, false) if set != 0 => BookingTarget::Set { tenant, set },
                    _ => return Err("a slot names one journal, one set, or its tenant"),
                };
                Ok(Wanted {
                    node: NodeId(slot.node),
                    role: slot.role.parse()?,
                    target,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            request: request.request,
            tenant,
            slots,
        })
    }
}

/// Send `request` to the coordinator `target`, once, within `timeout`. A
/// term that ended answers nothing: [`CapacityAnswer::Unavailable`], retried.
pub async fn send<P: ParosProviders>(
    providers: &P,
    rpc: &RpcHandle<P>,
    names: &Names,
    target: &Coordinator,
    request: &CapacityRequest,
    timeout: Duration,
) -> CapacityAnswer {
    match target
        .call::<P, CapacityRequestRpc>(providers, rpc, names, &request.to_wire(), timeout)
        .await
    {
        Some(Ok(ack)) => CapacityAnswer::from_wire(ack),
        _ => CapacityAnswer::Unavailable,
    }
}

/// Send `request` to the cell coordinator of the cell whose election journal
/// is `election`, the same id again until an answer decides it or
/// `patience` runs out (then the last answer, retryable).
#[tracing::instrument(level = "debug", skip_all, fields(tenant = request.tenant.0))]
pub async fn request<P: ParosProviders>(
    providers: &P,
    rpc: &RpcHandle<P>,
    names: &Names,
    client: &Client<P>,
    election: JournalIdentifier,
    request: &CapacityRequest,
    patience: Duration,
) -> CapacityAnswer {
    let deadline = providers.time().now() + patience;
    let timeout = client.tunables().request_timeout;
    let mut attempts = 0_u32;
    loop {
        attempts += 1;
        let answer = match coordinator(client, election).await {
            Some(target) => send(providers, rpc, names, &target, request, timeout).await,
            None => CapacityAnswer::NotCoordinator,
        };
        if !answer.is_retryable() || providers.time().now() >= deadline {
            if attempts > 1 && !answer.is_retryable() {
                moonpool_assertions::reachable!("capacity: a retried request is decided");
            }
            return answer;
        }
        let _ = providers.time().sleep(RETRY_PAUSE).await;
    }
}
