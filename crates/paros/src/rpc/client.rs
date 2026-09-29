//! Outbound clients: a well-known method bound to one address, the public
//! [`NodeClient`], and the driver's per-matchmaker link.

use std::net::SocketAddr;

use moonpool_core::Providers;
use moonpool_rpc::{
    AccessClass, BootstrapAddress, RpcError, RpcHandle, ServiceClient, WellKnownRef,
};

use super::methods::{
    AppendRpc, CheckTailRpc, GarbageCollectRpc, InspectRpc, LogReadRpc, MatchmakeRpc,
    MatchmakerReconfigureRpc, ReconfigureMatchmakersRpc, ReconfigureRpc, RetireRpc, TrimRpc,
    WellKnownMethod,
};
use super::{
    Append, AppendAck, CheckTail, CheckTailAck, InspectReply, InspectRequest, Read, ReadAck,
    Reconfigure, ReconfigureAck, ReconfigureMatchmakers, ReconfigureMatchmakersAck, RetireAck,
    RetireRequest, Trim, TrimAck,
};

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

/// A client of one paros node (or replica) at a fixed address, over the
/// caller's own moonpool-rpc runtime.
///
/// Every call is **one attempt** (`try_get_reply`): executed by the node zero
/// or one times, never retransmitted behind the caller's back. A failure's
/// [`RpcError`] says what it proves; a caller that cannot tell (a timeout, a
/// lost session) must treat the outcome as ambiguous. Cancelling a call —
/// dropping its future — releases the reply route; the node may still run
/// it.
pub struct NodeClient<P: Providers> {
    append: ServiceClient<P, AppendRpc>,
    read: ServiceClient<P, LogReadRpc>,
    check_tail: ServiceClient<P, CheckTailRpc>,
    trim: ServiceClient<P, TrimRpc>,
    reconfigure: ServiceClient<P, ReconfigureRpc>,
    reconfigure_matchmakers: ServiceClient<P, ReconfigureMatchmakersRpc>,
    inspect: ServiceClient<P, InspectRpc>,
    retire: ServiceClient<P, RetireRpc>,
}

impl<P: Providers> Clone for NodeClient<P> {
    fn clone(&self) -> Self {
        Self {
            append: self.append.clone(),
            read: self.read.clone(),
            check_tail: self.check_tail.clone(),
            trim: self.trim.clone(),
            reconfigure: self.reconfigure.clone(),
            reconfigure_matchmakers: self.reconfigure_matchmakers.clone(),
            inspect: self.inspect.clone(),
            retire: self.retire.clone(),
        }
    }
}

impl<P: Providers> NodeClient<P> {
    /// The node serving at `addr`, called through `rpc`.
    #[must_use]
    pub fn new(rpc: &RpcHandle<P>, addr: SocketAddr) -> Self {
        Self {
            append: well_known(rpc, addr),
            read: well_known(rpc, addr),
            check_tail: well_known(rpc, addr),
            trim: well_known(rpc, addr),
            reconfigure: well_known(rpc, addr),
            reconfigure_matchmakers: well_known(rpc, addr),
            inspect: well_known(rpc, addr),
            retire: well_known(rpc, addr),
        }
    }

    /// Append records to a journal; answered once they commit (naming the
    /// slot they landed in), or with a redirect.
    ///
    /// # Errors
    ///
    /// The attempt's [`RpcError`].
    pub async fn append(&self, request: &Append) -> Result<AppendAck, RpcError> {
        self.append.try_get_reply(request).await
    }

    /// Read a journal's chosen entries from an LSN up; long-polls above the
    /// serving node's end. Any node or replica serves it.
    ///
    /// # Errors
    ///
    /// The attempt's [`RpcError`].
    pub async fn read(&self, request: &Read) -> Result<ReadAck, RpcError> {
        self.read.try_get_reply(request).await
    }

    /// A linearizable tail: read-index on the leader, or a quorum read on
    /// any node or replica, as the request's path says.
    ///
    /// # Errors
    ///
    /// The attempt's [`RpcError`].
    pub async fn check_tail(&self, request: &CheckTail) -> Result<CheckTailAck, RpcError> {
        self.check_tail.try_get_reply(request).await
    }

    /// Ask the leader to trim the journal.
    ///
    /// # Errors
    ///
    /// The attempt's [`RpcError`].
    pub async fn trim(&self, request: &Trim) -> Result<TrimAck, RpcError> {
        self.trim.try_get_reply(request).await
    }

    /// Ask the leader to reconfigure the acceptor set.
    ///
    /// # Errors
    ///
    /// The attempt's [`RpcError`].
    pub async fn reconfigure(&self, request: &Reconfigure) -> Result<ReconfigureAck, RpcError> {
        self.reconfigure.try_get_reply(request).await
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
        self.reconfigure_matchmakers.try_get_reply(request).await
    }

    /// Inspect the node's chosen prefix, configuration and application.
    ///
    /// # Errors
    ///
    /// The attempt's [`RpcError`].
    pub async fn inspect(&self) -> Result<InspectReply, RpcError> {
        self.inspect.try_get_reply(&InspectRequest {}).await
    }

    /// Decommission the node (#123).
    ///
    /// # Errors
    ///
    /// The attempt's [`RpcError`].
    pub async fn retire(&self, request: &RetireRequest) -> Result<RetireAck, RpcError> {
        self.retire.try_get_reply(request).await
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
