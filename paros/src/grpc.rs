//! Generated gRPC contract and the bridge into the single-owner node driver.

use paros_core::Message;
use tokio::sync::{mpsc, oneshot};
use tonic::{Request, Response, Status};

/// Client-facing journal contract generated from `proto/paros.proto`.
pub mod public {
    #![allow(missing_docs, clippy::pedantic)]
    tonic::include_proto!("paros.v1");
}

/// Cluster-internal consensus contract generated from `proto/internal.proto`.
pub(crate) mod internal {
    #![allow(missing_docs, clippy::pedantic)]
    tonic::include_proto!("paros.internal.v1");
}

pub(crate) use internal::paros_internal_client::ParosInternalClient;
pub(crate) use internal::paros_internal_server::ParosInternalServer;
pub use public::paros_client::ParosClient;
pub(crate) use public::paros_server::ParosServer;
pub use public::{Compact, CompactAck, Propose, ProposeAck, Read, ReadAck};

pub(crate) type ReplySender<T> = oneshot::Sender<T>;
type Call<T, U> = (T, ReplySender<U>);

/// Requests accepted concurrently by tonic and consumed serially by the node
/// driver, which exclusively owns the sans-IO core.
pub(crate) struct RpcInbox {
    pub(crate) propose: mpsc::Receiver<Call<Propose, ProposeAck>>,
    pub(crate) read: mpsc::Receiver<Call<Read, ReadAck>>,
    pub(crate) deliver: mpsc::Receiver<Call<Message, ()>>,
    pub(crate) compact: mpsc::Receiver<Call<Compact, CompactAck>>,
}

/// Cloneable tonic handler. Each method forwards to [`RpcInbox`] and holds the
/// HTTP/2 response open until the driver completes that request.
#[derive(Clone)]
pub(crate) struct RpcService {
    propose: mpsc::Sender<Call<Propose, ProposeAck>>,
    read: mpsc::Sender<Call<Read, ReadAck>>,
    deliver: mpsc::Sender<Call<Message, ()>>,
    compact: mpsc::Sender<Call<Compact, CompactAck>>,
}

/// Construct a handler/inbox pair for one node incarnation.
pub(crate) fn rpc_channel() -> (RpcService, RpcInbox) {
    // Bounded queues make overload visible as backpressure while leaving ample
    // room for one simulation tick's peer-message fanout.
    let (propose_tx, propose_rx) = mpsc::channel(256);
    let (read_tx, read_rx) = mpsc::channel(256);
    let (deliver_tx, deliver_rx) = mpsc::channel(1024);
    let (compact_tx, compact_rx) = mpsc::channel(256);
    (
        RpcService {
            propose: propose_tx,
            read: read_tx,
            deliver: deliver_tx,
            compact: compact_tx,
        },
        RpcInbox {
            propose: propose_rx,
            read: read_rx,
            deliver: deliver_rx,
            compact: compact_rx,
        },
    )
}

async fn dispatch<T, U>(sender: &mpsc::Sender<Call<T, U>>, value: T) -> Result<U, Status> {
    let (reply_tx, reply_rx) = oneshot::channel();
    sender
        .send((value, reply_tx))
        .await
        .map_err(|_| Status::unavailable("node driver stopped"))?;
    reply_rx
        .await
        .map_err(|_| Status::unavailable("node driver dropped the reply"))
}

#[tonic::async_trait]
impl public::paros_server::Paros for RpcService {
    async fn propose(&self, request: Request<Propose>) -> Result<Response<ProposeAck>, Status> {
        dispatch(&self.propose, request.into_inner())
            .await
            .map(Response::new)
    }

    async fn read(&self, request: Request<Read>) -> Result<Response<ReadAck>, Status> {
        dispatch(&self.read, request.into_inner())
            .await
            .map(Response::new)
    }

    async fn compact(&self, request: Request<Compact>) -> Result<Response<CompactAck>, Status> {
        dispatch(&self.compact, request.into_inner())
            .await
            .map(Response::new)
    }
}

#[tonic::async_trait]
impl internal::paros_internal_server::ParosInternal for RpcService {
    async fn deliver(
        &self,
        request: Request<internal::Deliver>,
    ) -> Result<Response<internal::DeliverAck>, Status> {
        for bytes in request.into_inner().messages {
            let message = serde_json::from_slice(&bytes)
                .map_err(|e| Status::invalid_argument(format!("invalid Paxos message: {e}")))?;
            dispatch(&self.deliver, message).await?;
        }
        Ok(Response::new(internal::DeliverAck {}))
    }
}
