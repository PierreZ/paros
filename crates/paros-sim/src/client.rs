//! The client bundle a sim workload talks to a cluster through: one public
//! and one internal gRPC client per server, sharing one
//! [`ReconnectingChannel`] each, every channel closed when the bundle drops.
//!
//! The corpus builds its [`CorpusClients`](crate::corpus) on it, and the
//! chain workload opens one per run.

use moonpool_hyper::ReconnectingChannel;
use moonpool_sim::{SimContext, SimulationError, SimulationResult};
use paros::{ParosClient, ParosInternalClient, parse_addr};

/// A sim workload's channel to one server.
pub(crate) type SimChannel = ReconnectingChannel<moonpool_sim::SimProviders, tonic::body::Body>;

/// One public and one internal client per server, in `servers` order.
pub(crate) struct ClientSet {
    pub(crate) public: Vec<ParosClient<SimChannel>>,
    pub(crate) internal: Vec<ParosInternalClient<SimChannel>>,
    channels: Vec<SimChannel>,
}

impl ClientSet {
    /// Open one channel per server under `channel_config` and wrap it in
    /// both clients.
    pub(crate) fn connect(
        ctx: &SimContext,
        servers: &[String],
        channel_config: &moonpool_hyper::ChannelConfig,
    ) -> SimulationResult<Self> {
        let mut public = Vec::with_capacity(servers.len());
        let mut internal = Vec::with_capacity(servers.len());
        let mut channels = Vec::with_capacity(servers.len());
        for ip in servers {
            let addr = parse_addr(ip)?;
            let origin = http::Uri::try_from(format!("http://{addr}"))
                .map_err(|e| SimulationError::InvalidState(format!("bad gRPC origin: {e}")))?;
            let channel = ReconnectingChannel::new(ctx.providers(), addr, channel_config.clone());
            public.push(ParosClient::with_origin(channel.clone(), origin.clone()));
            internal.push(ParosInternalClient::with_origin(channel.clone(), origin));
            channels.push(channel);
        }
        Ok(Self {
            public,
            internal,
            channels,
        })
    }
}

impl Drop for ClientSet {
    /// Closing is idempotent and shared by every clone: the channels' connect,
    /// backoff, and keep-alive tasks stop on every exit path from a workload.
    fn drop(&mut self) {
        for channel in &self.channels {
            channel.close();
        }
    }
}
