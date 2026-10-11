//! Outbound clients: a well-known method bound to one address, the public
//! [`NodeClient`], and the driver's per-matchmaker link.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, PoisonError};

use moonpool_core::Providers;
use moonpool_rpc::{
    AccessClass, BootstrapAddress, ErrorReason, RpcError, RpcHandle, ServiceClient, WellKnownRef,
};

use super::methods::{
    FrontReadRpc, FrontSetLeaderRpc, FrontTruncateRpc, FrontWriteRpc, GarbageCollectRpc,
    InspectRpc, MatchmakeRpc, MatchmakerReconfigureRpc, ReadRpc, ReconfigureMatchmakersRpc,
    ReconfigureRpc, RetireRpc, SetLeaderRpc, TruncateRpc, WellKnownMethod, WriteRpc,
};
use super::{
    Entry, FrontRead, FrontSetLeader, FrontTruncate, FrontWrite, InspectReply, InspectRequest,
    Read, ReadAck, Reconfigure, ReconfigureAck, ReconfigureMatchmakers, ReconfigureMatchmakersAck,
    RetireAck, RetireRequest, SetLeader, SetLeaderAck, Truncate, TruncateAck, Write, WriteAck,
};
use crate::name::JournalName;
use crate::{Address, Names};
use paros_core::{JournalId, JournalIdentifier, TenantId};

/// `M`'s well-known endpoint at `addr`, bound to the runtime `rpc`: calls
/// reach whichever incarnation is serving that address.
pub(crate) fn well_known<P: Providers, M: WellKnownMethod>(
    rpc: &RpcHandle<P>,
    addr: SocketAddr,
) -> ServiceClient<P, M> {
    WellKnownRef::<M>::new(BootstrapAddress::Resolved(addr), M::ID, AccessClass::Public)
        .at(addr)
        .bind(rpc)
}

/// A client of one paros node (or replica) at one address, over the
/// caller's own moonpool-rpc runtime. The address is a literal or a name
/// (#257): a name is resolved through the caller's [`Names`] as each call
/// dials it, so a node whose IP changed behind its name is reached again.
///
/// Every call is **one attempt** (`try_get_reply`): executed by the node zero
/// or one times, never retransmitted behind the caller's back. A failure's
/// [`RpcError`] says what it proves; a caller that cannot tell (a timeout, a
/// lost session) must treat the outcome as ambiguous. A name that does not
/// resolve fails the call unexecuted (`LookupFailed`). Cancelling a call —
/// dropping its future — releases the reply route; the node may still run
/// it.
///
/// A client of a **frontend** ([`NodeClient::frontend`], #192 (the
/// frontend)) sends the four journal calls through the frontend contract,
/// each with its [`Pass`]: the token, and the journal's name in place of
/// its ids.
pub struct NodeClient<P: Providers> {
    rpc: RpcHandle<P>,
    address: Address,
    names: Names,
    pass: Option<Arc<Pass>>,
}

impl<P: Providers> Clone for NodeClient<P> {
    fn clone(&self) -> Self {
        Self {
            rpc: self.rpc.clone(),
            address: self.address.clone(),
            names: self.names.clone(),
            pass: self.pass.clone(),
        }
    }
}

/// What a client of a frontend presents with every call (#192 (the
/// frontend)): its token, and the name of each journal it calls by name.
///
/// The client's library is written over [`JournalIdentifier`]s; a client of
/// a frontend knows names, not ids. Its caller picks an identifier for each
/// name it calls (any set identifier: the frontend never reads it) and
/// binds the two here. A call on an identifier with no name bound names its
/// journal by the ids it carries: only an internal journal is reached so,
/// with an `admin` token.
#[derive(Debug, Default)]
pub struct Pass {
    token: Vec<u8>,
    names: Mutex<BTreeMap<JournalIdentifier, JournalName>>,
}

impl Pass {
    /// A pass that presents `token`.
    #[must_use]
    pub fn new(token: Vec<u8>) -> Self {
        Self {
            token,
            names: Mutex::default(),
        }
    }

    /// The token this pass presents.
    #[must_use]
    pub fn token(&self) -> &[u8] {
        &self.token
    }

    /// Call `journal` by `name` from now on.
    ///
    /// # Panics
    ///
    /// If `journal` is unset: a call never names the unset identifier.
    pub fn bind(&self, journal: JournalIdentifier, name: JournalName) {
        assert!(journal.is_set(), "a bound identifier is set");
        self.names
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(journal, name);
    }

    /// The name `journal` is called by, if one is bound.
    #[must_use]
    pub fn name_of(&self, journal: JournalIdentifier) -> Option<JournalName> {
        self.names
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&journal)
            .cloned()
    }

    /// The entry a call on `journal` carries, and whether it names the
    /// journal (the call's ids are then cleared: a name is sent, not ids).
    fn entry(&self, journal: JournalIdentifier) -> (Entry, bool) {
        let name = self.name_of(journal);
        let named = name.is_some();
        let (tenant, journal) = name
            .map(|name| (name.tenant().to_string(), name.journal().to_string()))
            .unwrap_or_default();
        (
            Entry {
                token: self.token.clone(),
                tenant,
                journal,
            },
            named,
        )
    }
}

/// The ids a named call carries: unset, since a client of a frontend sends
/// names (§3.5).
const UNSET: JournalIdentifier = JournalIdentifier {
    tenant: TenantId(0),
    journal: JournalId(0),
};

impl<P: Providers> NodeClient<P> {
    /// The node serving at the literal `addr`, called through `rpc`.
    #[must_use]
    pub fn new(rpc: &RpcHandle<P>, addr: SocketAddr) -> Self {
        Self::named(rpc, Names::literal(), Address::from(addr))
    }

    /// The node serving at `address`, resolved through `names` at each
    /// call, called through `rpc`.
    #[must_use]
    pub fn named(rpc: &RpcHandle<P>, names: Names, address: Address) -> Self {
        Self {
            rpc: rpc.clone(),
            address,
            names,
            pass: None,
        }
    }

    /// The frontend serving at `address` (#192 (the frontend)), resolved
    /// through `names` at each call, called through `rpc`: every journal
    /// call carries `pass`.
    #[must_use]
    pub fn frontend(rpc: &RpcHandle<P>, names: Names, address: Address, pass: Arc<Pass>) -> Self {
        Self {
            rpc: rpc.clone(),
            address,
            names,
            pass: Some(pass),
        }
    }

    /// The pass this client presents, when it calls a frontend.
    #[must_use]
    pub fn pass(&self) -> Option<&Arc<Pass>> {
        self.pass.as_ref()
    }

    /// The address this client dials.
    #[must_use]
    pub fn address(&self) -> &Address {
        &self.address
    }

    /// One attempt of `M` at the node, its address resolved now.
    async fn call<M: WellKnownMethod>(&self, request: &M::Request) -> Result<M::Reply, RpcError> {
        let addr = self.names.resolve(&self.address).await.map_err(|error| {
            RpcError::not_admitted(ErrorReason::LookupFailed(error.to_string()))
        })?;
        well_known::<P, M>(&self.rpc, addr)
            .try_get_reply(request)
            .await
    }

    /// Write a batch to a journal (#204); answered with the journal state
    /// machine's verdict once the deciding slot applies, or with a redirect.
    ///
    /// # Errors
    ///
    /// The attempt's [`RpcError`].
    pub async fn write(&self, request: &Write) -> Result<WriteAck, RpcError> {
        let Some(pass) = &self.pass else {
            return self.call::<WriteRpc>(request).await;
        };
        let (entry, named) = pass.entry(JournalIdentifier::new(
            TenantId(request.tenant),
            JournalId(request.journal),
        ));
        let mut call = request.clone();
        if named {
            (call.tenant, call.journal) = (UNSET.tenant.0, UNSET.journal.0);
        }
        self.call::<FrontWriteRpc>(&FrontWrite {
            entry: Some(entry),
            call: Some(call),
        })
        .await
    }

    /// Read a journal's records from a position up (#204): a leaderless
    /// read any node or replica serves, long-polling at the tail.
    ///
    /// # Errors
    ///
    /// The attempt's [`RpcError`].
    pub async fn read(&self, request: &Read) -> Result<ReadAck, RpcError> {
        let Some(pass) = &self.pass else {
            return self.call::<ReadRpc>(request).await;
        };
        let (entry, named) = pass.entry(JournalIdentifier::new(
            TenantId(request.tenant),
            JournalId(request.journal),
        ));
        let mut call = *request;
        if named {
            (call.tenant, call.journal) = (UNSET.tenant.0, UNSET.journal.0);
        }
        self.call::<FrontReadRpc>(&FrontRead {
            entry: Some(entry),
            call: Some(call),
        })
        .await
    }

    /// Ask the leader to truncate a journal below a position (#204).
    ///
    /// # Errors
    ///
    /// The attempt's [`RpcError`].
    pub async fn truncate(&self, request: &Truncate) -> Result<TruncateAck, RpcError> {
        let Some(pass) = &self.pass else {
            return self.call::<TruncateRpc>(request).await;
        };
        let (entry, named) = pass.entry(JournalIdentifier::new(
            TenantId(request.tenant),
            JournalId(request.journal),
        ));
        let mut call = *request;
        if named {
            (call.tenant, call.journal) = (UNSET.tenant.0, UNSET.journal.0);
        }
        self.call::<FrontTruncateRpc>(&FrontTruncate {
            entry: Some(entry),
            call: Some(call),
        })
        .await
    }

    /// Compare-and-swap a journal's writer (#204).
    ///
    /// # Errors
    ///
    /// The attempt's [`RpcError`].
    pub async fn set_leader(&self, request: &SetLeader) -> Result<SetLeaderAck, RpcError> {
        let Some(pass) = &self.pass else {
            return self.call::<SetLeaderRpc>(request).await;
        };
        let (entry, named) = pass.entry(JournalIdentifier::new(
            TenantId(request.tenant),
            JournalId(request.journal),
        ));
        let mut call = *request;
        if named {
            (call.tenant, call.journal) = (UNSET.tenant.0, UNSET.journal.0);
        }
        self.call::<FrontSetLeaderRpc>(&FrontSetLeader {
            entry: Some(entry),
            call: Some(call),
        })
        .await
    }

    /// Ask the leader to reconfigure the acceptor set.
    ///
    /// # Errors
    ///
    /// The attempt's [`RpcError`].
    pub async fn reconfigure(&self, request: &Reconfigure) -> Result<ReconfigureAck, RpcError> {
        self.call::<ReconfigureRpc>(request).await
    }

    /// Ask this node to drive a matchmaker-set handover (#125).
    ///
    /// # Errors
    ///
    /// The attempt's [`RpcError`].
    pub async fn reconfigure_matchmakers(
        &self,
        request: &ReconfigureMatchmakers,
    ) -> Result<ReconfigureMatchmakersAck, RpcError> {
        self.call::<ReconfigureMatchmakersRpc>(request).await
    }

    /// Inspect the node alone (#243): its id, its cell and the control
    /// journals' identifiers, and no journal's state.
    ///
    /// # Errors
    ///
    /// The attempt's [`RpcError`].
    pub async fn inspect_node(&self) -> Result<InspectReply, RpcError> {
        self.call::<InspectRpc>(&InspectRequest::node_only()).await
    }

    /// Inspect `journal` on the node (#188, #235). An answer whose
    /// `refusal` is set describes the node but no journal.
    ///
    /// # Errors
    ///
    /// The attempt's [`RpcError`].
    pub async fn inspect_journal(
        &self,
        journal: JournalIdentifier,
    ) -> Result<InspectReply, RpcError> {
        self.call::<InspectRpc>(&InspectRequest {
            journal: journal.journal.0,
            tenant: journal.tenant.0,
            node_only: false,
        })
        .await
    }

    /// Decommission the node (#123).
    ///
    /// # Errors
    ///
    /// The attempt's [`RpcError`].
    pub async fn retire(&self, request: &RetireRequest) -> Result<RetireAck, RpcError> {
        self.call::<RetireRpc>(request).await
    }
}

/// The node driver's link to one matchmaker: the contract's three methods at
/// its address.
pub(crate) struct MatchmakerClient<P: Providers> {
    pub(crate) matchmake: ServiceClient<P, MatchmakeRpc>,
    pub(crate) collect: ServiceClient<P, GarbageCollectRpc>,
    pub(crate) reconfigure: ServiceClient<P, MatchmakerReconfigureRpc>,
}

impl<P: Providers> Clone for MatchmakerClient<P> {
    fn clone(&self) -> Self {
        Self {
            matchmake: self.matchmake.clone(),
            collect: self.collect.clone(),
            reconfigure: self.reconfigure.clone(),
        }
    }
}

impl<P: Providers> MatchmakerClient<P> {
    /// The matchmaker serving at `addr`, called through `rpc`.
    pub(crate) fn new(rpc: &RpcHandle<P>, addr: SocketAddr) -> Self {
        Self {
            matchmake: well_known(rpc, addr),
            collect: well_known(rpc, addr),
            reconfigure: well_known(rpc, addr),
        }
    }
}
