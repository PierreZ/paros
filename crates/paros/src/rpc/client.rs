//! Outbound clients: a well-known method bound to one address, the public
//! [`NodeClient`], and the driver's per-matchmaker link.

use std::net::SocketAddr;

use moonpool_core::Providers;
use moonpool_rpc::{
    AccessClass, BootstrapAddress, ErrorReason, RpcError, RpcHandle, ServiceClient, WellKnownRef,
};

use super::methods::{
    GarbageCollectRpc, InspectRpc, MatchmakeRpc, MatchmakerReconfigureRpc, ReadRpc,
    ReconfigureMatchmakersRpc, ReconfigureRpc, RetireRpc, SetLeaderRpc, TruncateRpc,
    WellKnownMethod, WriteRpc,
};
use super::{
    InspectReply, InspectRequest, Read, ReadAck, Reconfigure, ReconfigureAck,
    ReconfigureMatchmakers, ReconfigureMatchmakersAck, RetireAck, RetireRequest, SetLeader,
    SetLeaderAck, Truncate, TruncateAck, Write, WriteAck,
};
use crate::{Address, Names};
use paros_core::JournalIdentifier;

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
pub struct NodeClient<P: Providers> {
    rpc: RpcHandle<P>,
    address: Address,
    names: Names,
}

impl<P: Providers> Clone for NodeClient<P> {
    fn clone(&self) -> Self {
        Self {
            rpc: self.rpc.clone(),
            address: self.address.clone(),
            names: self.names.clone(),
        }
    }
}

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
        }
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
        self.call::<WriteRpc>(request).await
    }

    /// Read a journal's records from a position up (#204): a leaderless
    /// read any node or replica serves, long-polling at the tail.
    ///
    /// # Errors
    ///
    /// The attempt's [`RpcError`].
    pub async fn read(&self, request: &Read) -> Result<ReadAck, RpcError> {
        self.call::<ReadRpc>(request).await
    }

    /// Ask the leader to truncate a journal below a position (#204).
    ///
    /// # Errors
    ///
    /// The attempt's [`RpcError`].
    pub async fn truncate(&self, request: &Truncate) -> Result<TruncateAck, RpcError> {
        self.call::<TruncateRpc>(request).await
    }

    /// Compare-and-swap a journal's writer (#204).
    ///
    /// # Errors
    ///
    /// The attempt's [`RpcError`].
    pub async fn set_leader(&self, request: &SetLeader) -> Result<SetLeaderAck, RpcError> {
        self.call::<SetLeaderRpc>(request).await
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
