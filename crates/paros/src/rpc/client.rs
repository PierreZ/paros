//! Outbound clients: a well-known method bound to one address, the public
//! [`NodeClient`], and the driver's per-matchmaker link.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, PoisonError};

use moonpool_core::Providers;
use moonpool_rpc::{
    AccessClass, BootstrapAddress, Endpoint, ErrorReason, InterfaceMethod, InterfaceRef, RpcError,
    RpcHandle, RpcInterface, ServiceClient, WellKnownRef,
};

use super::methods::{
    FrontReadRpc, FrontSetJournalTagsRpc, FrontSetLeaderRpc, FrontTruncateRpc, FrontWriteRpc,
    GarbageCollectRpc, InspectRpc, MachineInterface, MatchmakeRpc, MatchmakerReconfigureRpc,
    ReadRpc, ReconfigureMatchmakersRpc, ReconfigureRpc, RetireRpc, SetLeaderRpc, TruncateRpc,
    WellKnownMethod, WriteRpc,
};
use super::{
    Entry, FrontRead, FrontSetJournalTags, FrontSetLeader, FrontTruncate, FrontWrite, InspectReply,
    InspectRequest, Read, ReadAck, Reconfigure, ReconfigureAck, ReconfigureMatchmakers,
    ReconfigureMatchmakersAck, RetireAck, RetireRequest, SetJournalTagsAck, SetLeader,
    SetLeaderAck, Truncate, TruncateAck, Write, WriteAck,
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

/// `reference` aimed at `addr` (#404 (static vs dynamic endpoints)): the
/// same incarnation and token, at the address its holder resolved now.
///
/// paros keeps the advertised [`Address`] (a literal or a name, #257) as
/// the source of truth for *where* a group is, and the reference only for
/// *which instance*: the address inside a reference is the one the
/// runtime bound, which may be a wildcard.
#[must_use]
pub(crate) fn aimed<I: RpcInterface>(
    reference: &InterfaceRef<I>,
    addr: SocketAddr,
) -> InterfaceRef<I> {
    let endpoint = reference.endpoint();
    let aimed = InterfaceRef::new(
        Endpoint::new(addr, endpoint.incarnation(), endpoint.token()),
        reference.access(),
    );
    assert_eq!(
        aimed.endpoint().token(),
        endpoint.token(),
        "aiming keeps the instance"
    );
    aimed
}

/// `reference`'s bytes as paros publishes them (#404 (static vs dynamic
/// endpoints)). A runtime bound to a wildcard (`PAROS_LISTEN=0.0.0.0:…`,
/// #257) puts that wildcard in its references, and moonpool refuses to
/// decode a reference with no address: the published form names the
/// loopback at the same port instead. Every holder aims it at the
/// advertised address before it dials ([`aimed`]), so only the incarnation
/// and the token of the published form are ever used.
#[must_use]
pub(crate) fn published<I: RpcInterface>(reference: &InterfaceRef<I>) -> Vec<u8> {
    let addr = reference.endpoint().address();
    let reference = if addr.ip().is_unspecified() {
        let loopback: std::net::IpAddr = if addr.is_ipv4() {
            std::net::Ipv4Addr::LOCALHOST.into()
        } else {
            std::net::Ipv6Addr::LOCALHOST.into()
        };
        aimed(reference, SocketAddr::new(loopback, addr.port()))
    } else {
        reference.clone()
    };
    let bytes = reference.to_bytes();
    assert!(
        InterfaceRef::<I>::from_bytes(&bytes).is_ok(),
        "a published reference decodes"
    );
    bytes
}

/// Method `M` of the group `reference` names, at `addr`, bound to `rpc`:
/// `None` when the reference does not name `I`.
pub(crate) fn in_group<P: Providers, I: RpcInterface, M: InterfaceMethod<I>>(
    rpc: &RpcHandle<P>,
    reference: &InterfaceRef<I>,
    addr: SocketAddr,
) -> Option<ServiceClient<P, M>> {
    let method = aimed(reference, addr).method::<M>().ok()?;
    Some(method.bind(rpc))
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
///
/// A client of a **machine** given its machine interface
/// ([`NodeClient::with_interface`], #404 (static vs dynamic endpoints))
/// sends the data plane through that reference, aimed at the resolved
/// address: a call reaches only the incarnation the reference names. A
/// machine that restarted since refuses it unrun (`StaleIncarnation`,
/// `EndpointNotFound`): the caller learns the machine's new reference, and
/// this client never falls back to the well-known endpoint on its own.
pub struct NodeClient<P: Providers> {
    rpc: RpcHandle<P>,
    address: Address,
    names: Names,
    pass: Option<Arc<Pass>>,
    interface: Option<InterfaceRef<MachineInterface>>,
}

impl<P: Providers> Clone for NodeClient<P> {
    fn clone(&self) -> Self {
        Self {
            rpc: self.rpc.clone(),
            address: self.address.clone(),
            names: self.names.clone(),
            pass: self.pass.clone(),
            interface: self.interface.clone(),
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
            interface: None,
        }
    }

    /// A client of the node at `address`, through this one's runtime and
    /// resolver (#232 (multiple cells)): another cell's machine, learned
    /// from the universe directory. No pass and no machine interface.
    #[must_use]
    pub fn elsewhere(&self, address: Address) -> Self {
        Self::named(&self.rpc, self.names.clone(), address)
    }

    /// This client, sending the data plane through the machine interface
    /// `interface` (#404 (static vs dynamic endpoints)): the encoded
    /// reference a machine published. Empty bytes, or bytes that do not
    /// decode to a [`MachineInterface`], leave the well-known endpoints.
    ///
    /// # Panics
    ///
    /// On a client of a frontend: a frontend serves no machine interface.
    #[must_use]
    pub fn with_interface(mut self, interface: &[u8]) -> Self {
        assert!(self.pass.is_none(), "a frontend has no machine interface");
        self.interface = InterfaceRef::<MachineInterface>::from_bytes(interface).ok();
        self
    }

    /// Whether the data plane goes through a machine interface.
    #[must_use]
    pub fn dynamic(&self) -> bool {
        self.interface.is_some()
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
            interface: None,
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

    /// One attempt of the data-plane method `M` at the node: through its
    /// machine interface when this client holds one (#404), else at the
    /// well-known endpoint.
    async fn data<M>(&self, request: &M::Request) -> Result<M::Reply, RpcError>
    where
        M: WellKnownMethod + InterfaceMethod<MachineInterface>,
    {
        let Some(reference) = &self.interface else {
            return self.call::<M>(request).await;
        };
        let addr = self.names.resolve(&self.address).await.map_err(|error| {
            RpcError::not_admitted(ErrorReason::LookupFailed(error.to_string()))
        })?;
        let client = in_group::<P, MachineInterface, M>(&self.rpc, reference, addr)
            .ok_or_else(|| RpcError::not_admitted(ErrorReason::EndpointNotFound))?;
        let reply = client.try_get_reply(request).await;
        if reply.is_ok() {
            moonpool_assertions::reachable!(
                "client: a journal call goes through a machine interface"
            );
        }
        reply
    }

    /// Write a batch to a journal (#204); answered with the journal state
    /// machine's verdict once the deciding slot applies, or with a redirect.
    ///
    /// # Errors
    ///
    /// The attempt's [`RpcError`].
    pub async fn write(&self, request: &Write) -> Result<WriteAck, RpcError> {
        let Some(pass) = &self.pass else {
            return self.data::<WriteRpc>(request).await;
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
            return self.data::<ReadRpc>(request).await;
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
            return self.data::<TruncateRpc>(request).await;
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
            return self.data::<SetLeaderRpc>(request).await;
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

    /// Set a `users` journal's user tags through a frontend (#575
    /// (key:value tags)): the entry is the pass's token and `name`.
    ///
    /// # Errors
    ///
    /// The attempt's [`RpcError`].
    ///
    /// # Panics
    ///
    /// If this is not a client of a frontend.
    pub async fn set_journal_tags(
        &self,
        name: &JournalName,
        request: u64,
        tags: Vec<crate::wire::common::Tag>,
    ) -> Result<SetJournalTagsAck, RpcError> {
        let pass = self
            .pass
            .as_ref()
            .expect("a tag request goes through a frontend");
        self.call::<FrontSetJournalTagsRpc>(&FrontSetJournalTags {
            entry: Some(Entry {
                token: pass.token.clone(),
                tenant: name.tenant().to_string(),
                journal: name.journal().to_string(),
            }),
            request,
            tags,
        })
        .await
    }

    /// Ask the leader to reconfigure the acceptor set.
    ///
    /// # Errors
    ///
    /// The attempt's [`RpcError`].
    pub async fn reconfigure(&self, request: &Reconfigure) -> Result<ReconfigureAck, RpcError> {
        self.data::<ReconfigureRpc>(request).await
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
        self.data::<ReconfigureMatchmakersRpc>(request).await
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
/// its address. The address is a literal or a name, resolved through the
/// node's [`Names`] at each call (#257), and a machine's address book may
/// move it (#349, #190: a founding member hosts its cell's matchmaker sets).
///
/// **Which endpoint** (#404 (static vs dynamic endpoints)), as a peer lane
/// chooses: the hosting member's machine interface, when the registry holds
/// one, aimed at the resolved address; else the well-known endpoint. A call
/// the interface refuses for good (the member restarted since it
/// registered) marks it stale, and the link calls the well-known endpoint
/// until the book names another interface.
pub(crate) struct MatchmakerClient<P: Providers> {
    rpc: RpcHandle<P>,
    names: Names,
    target: Arc<Mutex<MatchmakerTarget>>,
}

/// Where a [`MatchmakerClient`] calls now.
struct MatchmakerTarget {
    address: Address,
    /// The hosting member's machine interface, from the registry.
    interface: Option<InterfaceRef<MachineInterface>>,
    /// The interface refused a call for good.
    stale: bool,
}

impl<P: Providers> Clone for MatchmakerClient<P> {
    fn clone(&self) -> Self {
        Self {
            rpc: self.rpc.clone(),
            names: self.names.clone(),
            target: Arc::clone(&self.target),
        }
    }
}

impl<P: Providers> MatchmakerClient<P> {
    /// The matchmaker serving at `address`, resolved through `names` at
    /// each call, called through `rpc`.
    pub(crate) fn named(rpc: &RpcHandle<P>, names: Names, address: Address) -> Self {
        Self {
            rpc: rpc.clone(),
            names,
            target: Arc::new(Mutex::new(MatchmakerTarget {
                address,
                interface: None,
                stale: false,
            })),
        }
    }

    /// Dial this matchmaker at `address`, through the hosting member's
    /// machine `interface` (#404; one that does not decode is none), from
    /// the next call on (#349).
    pub(crate) fn readdress(&self, address: Address, interface: &[u8]) {
        let interface = InterfaceRef::<MachineInterface>::from_bytes(interface).ok();
        let mut target = self.target.lock().unwrap_or_else(PoisonError::into_inner);
        if interface != target.interface {
            moonpool_assertions::reachable!(
                "transport: a matchmaker link follows a registered interface"
            );
            target.interface = interface;
            target.stale = false;
        }
        target.address = address;
    }

    /// One attempt of `M` at the matchmaker's current address.
    async fn call<M>(&self, request: &M::Request) -> Result<M::Reply, RpcError>
    where
        M: WellKnownMethod + InterfaceMethod<MachineInterface>,
    {
        let (address, interface) = {
            let target = self.target.lock().unwrap_or_else(PoisonError::into_inner);
            let interface = target.interface.clone().filter(|_| !target.stale);
            (target.address.clone(), interface)
        };
        let addr = self.names.resolve(&address).await.map_err(|error| {
            RpcError::not_admitted(ErrorReason::LookupFailed(error.to_string()))
        })?;
        let Some(client) = interface
            .as_ref()
            .and_then(|reference| in_group::<P, MachineInterface, M>(&self.rpc, reference, addr))
        else {
            return well_known::<P, M>(&self.rpc, addr)
                .try_get_reply(request)
                .await;
        };
        let reply = client.try_get_reply(request).await;
        match &reply {
            Ok(_) => moonpool_assertions::reachable!(
                "transport: a matchmaker call goes through a machine interface"
            ),
            Err(error) if error.is_terminal_for_reference() => {
                let mut target = self.target.lock().unwrap_or_else(PoisonError::into_inner);
                // Only the interface this call used: the book may have
                // named a fresher one meanwhile.
                if target.interface == interface {
                    moonpool_assertions::reachable!(
                        "transport: a stale machine interface sends a matchmaker call back to the well-known endpoint"
                    );
                    target.stale = true;
                }
            }
            Err(_) => {}
        }
        reply
    }

    /// Register or probe (#120, #173).
    pub(crate) async fn matchmake(
        &self,
        request: &crate::wire::matchmaker::MatchRequest,
    ) -> Result<crate::wire::matchmaker::MatchReply, RpcError> {
        self.call::<MatchmakeRpc>(request).await
    }

    /// Raise a journal's GC watermark (#123).
    pub(crate) async fn collect(
        &self,
        request: &crate::wire::matchmaker::GarbageCollect,
    ) -> Result<crate::wire::matchmaker::GarbageCollectAck, RpcError> {
        self.call::<GarbageCollectRpc>(request).await
    }

    /// One step of a matchmaker-set handover (#125).
    pub(crate) async fn reconfigure(
        &self,
        request: &crate::wire::matchmaker::ReconfigureRequest,
    ) -> Result<crate::wire::matchmaker::ReconfigureReply, RpcError> {
        self.call::<MatchmakerReconfigureRpc>(request).await
    }
}
