//! **A tenant's journals** (#210, `docs/architecture.md` §3.3): create and
//! delete them through the tenant coordinator, and list them from the
//! tenant's control journal.
//!
//! A create or a delete is a request with an idempotency id the caller
//! draws. The library sends it to the tenant coordinator — until #212 and
//! #225, the elected cell coordinator, found at the interface the election
//! journal's leader publishes — and re-sends the same id when the answer
//! does not decide it (no coordinator, a coordinator change, a timeout). The
//! coordinator writes the request to the tenant's control journal, and the
//! fold judges it at apply and records its outcome under the id
//! ([`crate::tenant::TenantControl`]): a retry reads that outcome back, so
//! it acts once. A list is a plain read: fold the control journal.
//!
//! The library draws no randomness: the request id is the caller's.

use std::net::SocketAddr;
use std::time::Duration;

use moonpool_core::{Providers, TimeProvider};
use moonpool_rpc::RpcHandle;
use paros_core::{AcceptorConfig, JournalId, JournalIdentifier, TenantId, WriterMode};

use super::Client;
use super::checkpoint::{Folder, LoadOutcome, load};
use super::election::read_election;
use crate::rpc::machine as wire;
use crate::rpc::methods::JournalRequestRpc;
use crate::rpc::well_known;
use crate::rpc::{
    config_from_proto, config_to_proto, writer_mode_from_proto, writer_mode_to_proto,
};
use crate::tenant::{Desired, DesiredMode, Redundancy, TenantControl};

/// What a request asks for.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum JournalOp {
    /// Create a journal named `name` in `writer` mode under `desired`.
    Create {
        /// Its name, unique among the tenant's live journals.
        name: Vec<u8>,
        /// Who may write it (#241), fixed for its life.
        writer: WriterMode,
        /// The desired mode: the coordinator picks the members.
        desired: Desired,
    },
    /// Delete the live journal named `name`.
    Delete {
        /// Its name.
        name: Vec<u8>,
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
            } => {
                let (redundancy, rows, cols) = match desired.mode {
                    DesiredMode::Redundancy(Redundancy::Single) => (1, 0, 0),
                    DesiredMode::Redundancy(Redundancy::Double) => (2, 0, 0),
                    DesiredMode::Redundancy(Redundancy::Triple) => (3, 0, 0),
                    DesiredMode::Grid { rows, cols } => (0, rows, cols),
                };
                Op::Create(wire::CreateJournalOp {
                    name: name.clone(),
                    writer: writer_mode_to_proto(*writer).into(),
                    redundancy,
                    rows,
                    cols,
                    replicas: desired.replicas,
                })
            }
            JournalOp::Delete { name } => Op::Delete(wire::DeleteJournalOp { name: name.clone() }),
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
    /// of range.
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
                desired: Desired::from_wire(Some(crate::rpc::tenant::Desired {
                    redundancy: create.redundancy,
                    rows: create.rows,
                    cols: create.cols,
                    replicas: create.replicas,
                }))?,
            },
            Op::Delete(delete) => JournalOp::Delete {
                name: delete.name.clone(),
            },
        };
        let (JournalOp::Create { name, .. } | JournalOp::Delete { name }) = &op;
        if name.is_empty() {
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
    /// A live journal holds the name: `id`.
    NameTaken {
        /// The live journal's id.
        id: JournalId,
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
            JournalAnswer::NameTaken { .. } => "name_taken",
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
            JournalAnswer::Deleted { id } | JournalAnswer::NameTaken { id } => (id.0, None),
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
            "name_taken" if id.is_set() => JournalAnswer::NameTaken { id },
            "unknown_journal" => JournalAnswer::UnknownJournal,
            "unknown_tenant" => JournalAnswer::UnknownTenant,
            "unplaceable" => JournalAnswer::Unplaceable,
            "malformed" => JournalAnswer::Malformed,
            "not_coordinator" => JournalAnswer::NotCoordinator,
            _ => JournalAnswer::Unavailable,
        }
    }
}

/// The pause between two attempts of one request.
const RETRY_PAUSE: Duration = Duration::from_millis(200);

/// The tenant coordinator's address: the interface the election journal's
/// leader publishes. `None` while no leader published one.
pub async fn coordinator<P: Providers>(
    client: &Client<P>,
    election: JournalIdentifier,
) -> Option<SocketAddr> {
    let fold = read_election(client, election, 0).await?;
    fold.leader()?.candidate.interface.parse().ok()
}

/// Send `request` to the machine at `target`, once, within `timeout`.
pub async fn send<P: Providers>(
    providers: &P,
    rpc: &RpcHandle<P>,
    target: SocketAddr,
    request: &JournalRequest,
    timeout: Duration,
) -> JournalAnswer {
    let client = well_known::<P, JournalRequestRpc>(rpc, target);
    match providers
        .time()
        .timeout(timeout, client.try_get_reply(&request.to_wire()))
        .await
    {
        Ok(Ok(ack)) => JournalAnswer::from_wire(ack),
        _ => JournalAnswer::Unavailable,
    }
}

/// Send `request` to the tenant coordinator of the cell whose election
/// journal is `election`, the same id again until an answer decides it or
/// `patience` runs out (then the last answer, retryable).
#[tracing::instrument(level = "debug", skip_all, fields(tenant = request.tenant.0))]
pub async fn request<P: Providers>(
    providers: &P,
    rpc: &RpcHandle<P>,
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
            Some(target) => send(providers, rpc, target, request, timeout).await,
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
                desired: "grid:2x3".parse().expect("a grid"),
            },
        };
        let wire = request.to_wire();
        assert_eq!(JournalRequest::from_wire(&wire), Ok(request));
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
