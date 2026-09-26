//! The matchmaker's tonic handler and the bridge into its single-owner
//! driver loop.

use paros_core::{
    GcAck, GcRequest, MatchReply, MatchRequest, ReconfigureReply, ReconfigureRequest,
};
use tokio::sync::mpsc;
use tonic::{Request, Response, Status};

use super::matchmaker_codec::{
    garbage_collect_from_wire, match_request_from_wire, reconfigure_request_from_wire,
    wire_garbage_collect_ack, wire_match_reply, wire_reconfigure_reply,
};
use super::service::{call, decode_wire};
use super::{
    Call, WireGarbageCollect, WireGarbageCollectAck, WireMatchReply, WireMatchRequest,
    WireReconfigureReply, WireReconfigureRequest, matchmaker,
};

/// Matchmaker requests accepted concurrently by tonic and consumed serially by
/// the matchmaker driver, which exclusively owns the sans-IO core.
pub(crate) struct MatchmakerInbox {
    pub(crate) requests: mpsc::Receiver<Call<MatchRequest, MatchReply>>,
    pub(crate) collects: mpsc::Receiver<Call<GcRequest, GcAck>>,
    pub(crate) reconfigures: mpsc::Receiver<Call<ReconfigureRequest, ReconfigureReply>>,
}

/// Cloneable tonic handler for the matchmaker contract; each method forwards
/// into [`MatchmakerInbox`] and holds the response open until the driver
/// answers.
#[derive(Clone)]
pub(crate) struct MatchmakerService {
    requests: mpsc::Sender<Call<MatchRequest, MatchReply>>,
    collects: mpsc::Sender<Call<GcRequest, GcAck>>,
    reconfigures: mpsc::Sender<Call<ReconfigureRequest, ReconfigureReply>>,
}

/// Construct a matchmaker handler/inbox pair; `capacity` bounds each queue
/// (at least 1).
#[tracing::instrument(level = "debug", skip_all)]
pub(crate) fn matchmaker_channel(capacity: usize) -> (MatchmakerService, MatchmakerInbox) {
    let (requests_tx, requests_rx) = mpsc::channel(capacity);
    let (collects_tx, collects_rx) = mpsc::channel(capacity);
    let (reconfigures_tx, reconfigures_rx) = mpsc::channel(capacity);
    (
        MatchmakerService {
            requests: requests_tx,
            collects: collects_tx,
            reconfigures: reconfigures_tx,
        },
        MatchmakerInbox {
            requests: requests_rx,
            collects: collects_rx,
            reconfigures: reconfigures_rx,
        },
    )
}

#[tonic::async_trait]
impl matchmaker::paros_matchmaker_server::ParosMatchmaker for MatchmakerService {
    #[tracing::instrument(level = "debug", skip_all)]
    async fn matchmake(
        &self,
        request: Request<WireMatchRequest>,
    ) -> Result<Response<WireMatchReply>, Status> {
        let request = decode_wire(request, match_request_from_wire, "match request")?;
        call(&self.requests, request)
            .await
            .map(|reply| Response::new(wire_match_reply(&reply)))
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn garbage_collect(
        &self,
        request: Request<WireGarbageCollect>,
    ) -> Result<Response<WireGarbageCollectAck>, Status> {
        let request = decode_wire(
            request,
            garbage_collect_from_wire,
            "garbage-collect request",
        )?;
        call(&self.collects, request)
            .await
            .map(|ack| Response::new(wire_garbage_collect_ack(&ack)))
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn reconfigure(
        &self,
        request: Request<WireReconfigureRequest>,
    ) -> Result<Response<WireReconfigureReply>, Status> {
        let request = decode_wire(
            request,
            reconfigure_request_from_wire,
            "reconfigure request",
        )?;
        call(&self.reconfigures, request)
            .await
            .map(|reply| Response::new(wire_reconfigure_reply(&reply)))
    }
}
