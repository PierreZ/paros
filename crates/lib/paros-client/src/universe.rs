//! **Joining a universe another cell hosts** (#232 (multiple cells),
//! `docs/architecture/03-1-cell-tenant-and-bootstrap.md` §3.1, §3.7): `universe add-cell`'s cell step.
//!
//! An idle cell (formed by `cell init` with no universe tenant) joins a
//! universe in two halves, like `init`'s fleet half. The operator writes
//! the directory half (`AddCell`, then `MarkCell READY`,
//! [`crate::fleet`]). The cell's coordinator, the cell control
//! journal's one writer, writes the cell half: `JoinFleet` with the
//! **universe pointer**, the directory's journal and the machines of the
//! cell that hosts it. The idle cell's coordinator cannot find a universe
//! on its own, so the operator sends it a [`JoinRequest`]; the coordinator
//! reads the directory at the pointer and joins only when the directory
//! holds this cell under its cell tenant's control journal. A re-send is
//! harmless: a cell joins once.
//!
//! The coordinator is found through the interface it publishes in the
//! election journal ([`coordinator`]), like a capacity request. Like the
//! rest of the client, this draws no randomness.

use std::time::Duration;

use moonpool_core::LocalTimeProvider;
use moonpool_rpc::RpcHandle;
use paros_core::{JournalId, JournalIdentifier, TenantId};
use paros_net::runtime::ParosProviders;

use super::Client;
use super::journals::{Coordinator, coordinator};
use paros_model::address::Address;
use paros_model::address::Names;
use paros_model::registry::UniversePointer;
use paros_wire::machine as wire;
use paros_wire::methods::JoinUniverseRpc;

/// How long a retry waits after an answer that decided nothing.
const RETRY_PAUSE: Duration = Duration::from_millis(200);

/// What an idle cell's coordinator is asked: join universe `fleet_id`,
/// whose directory `pointer` names.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JoinRequest {
    /// The universe's id.
    pub fleet_id: u64,
    /// Where its directory is.
    pub pointer: UniversePointer,
}

/// What a join request came to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JoinAnswer {
    /// The cell joined this universe.
    Joined,
    /// The term that received it does not serve: retry.
    NotCoordinator,
    /// The directory or the cell control journal could not be read or
    /// written: retry.
    Unavailable,
    /// The directory does not hold this cell under its cell tenant yet:
    /// retry once `AddCell` landed.
    NotListed,
    /// The cell joined another universe.
    OtherUniverse,
    /// The request is not well formed.
    Malformed,
}

impl JoinAnswer {
    /// Whether the same request may be sent again.
    #[must_use]
    pub fn is_retryable(self) -> bool {
        matches!(
            self,
            Self::NotCoordinator | Self::Unavailable | Self::NotListed
        )
    }

    /// The wire answer.
    #[must_use]
    pub fn to_wire(self) -> wire::JoinUniverseAck {
        let outcome = match self {
            Self::Joined => "joined",
            Self::NotCoordinator => "not_coordinator",
            Self::Unavailable => "unavailable",
            Self::NotListed => "not_listed",
            Self::OtherUniverse => "other_universe",
            Self::Malformed => "malformed",
        };
        wire::JoinUniverseAck {
            outcome: outcome.into(),
        }
    }

    /// From the wire: an outcome this client does not know is unavailable,
    /// retried.
    #[must_use]
    pub fn from_wire(ack: &wire::JoinUniverseAck) -> Self {
        match ack.outcome.as_str() {
            "joined" => Self::Joined,
            "not_coordinator" => Self::NotCoordinator,
            "not_listed" => Self::NotListed,
            "other_universe" => Self::OtherUniverse,
            "malformed" => Self::Malformed,
            _ => Self::Unavailable,
        }
    }
}

impl JoinRequest {
    /// The wire request.
    #[must_use]
    pub fn to_wire(&self) -> wire::JoinUniverse {
        wire::JoinUniverse {
            fleet_id: self.fleet_id,
            directory_tenant: self.pointer.directory.tenant.0,
            directory_journal: self.pointer.directory.journal.0,
            hosts: self
                .pointer
                .hosts
                .iter()
                .map(|(node_id, addr)| wire::Member {
                    node_id: *node_id,
                    addr: addr.to_string(),
                    machine_interface: Vec::new(),
                })
                .collect(),
        }
    }

    /// From the wire.
    ///
    /// # Errors
    ///
    /// An unset universe id or directory, no host, an unset host id or an
    /// address that does not parse.
    pub fn from_wire(request: &wire::JoinUniverse) -> Result<Self, &'static str> {
        let directory = JournalIdentifier::new(
            TenantId(request.directory_tenant),
            JournalId(request.directory_journal),
        );
        if request.fleet_id == 0 || !directory.is_set() || request.hosts.is_empty() {
            return Err("a join names its universe, its directory and a host");
        }
        let hosts = request
            .hosts
            .iter()
            .map(|member| {
                if member.node_id == 0 {
                    return Err("a host names its node");
                }
                let addr = Address::parse(&member.addr).map_err(|_| "a host's address")?;
                Ok((member.node_id, addr))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            fleet_id: request.fleet_id,
            pointer: UniversePointer { directory, hosts },
        })
    }
}

/// Send `request` to the coordinator `target`, once, within `timeout`.
pub async fn send<P: ParosProviders>(
    providers: &P,
    rpc: &RpcHandle<P>,
    names: &Names,
    target: &Coordinator,
    request: &JoinRequest,
    timeout: Duration,
) -> JoinAnswer {
    match target
        .call::<P, JoinUniverseRpc>(providers, rpc, names, &request.to_wire(), timeout)
        .await
    {
        Some(Ok(ack)) => JoinAnswer::from_wire(&ack),
        _ => JoinAnswer::Unavailable,
    }
}

/// Send `request` to the coordinator of the idle cell whose election
/// journal is `election`, read through `client` (a client of that cell's
/// machines), again until an answer decides it or `patience` runs out (then
/// the last answer, retryable).
#[tracing::instrument(level = "debug", skip_all, fields(fleet = request.fleet_id))]
pub async fn join<P: ParosProviders>(
    providers: &P,
    rpc: &RpcHandle<P>,
    names: &Names,
    client: &Client<P>,
    election: JournalIdentifier,
    request: &JoinRequest,
    patience: Duration,
) -> JoinAnswer {
    let deadline = providers.time().now() + patience;
    let timeout = client.tunables().request_timeout;
    loop {
        let answer = match coordinator(client, election).await {
            Some(target) => send(providers, rpc, names, &target, request, timeout).await,
            None => JoinAnswer::NotCoordinator,
        };
        if !answer.is_retryable() || providers.time().now() >= deadline {
            return answer;
        }
        let _ = providers.time().sleep(RETRY_PAUSE).await;
    }
}
