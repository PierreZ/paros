//! The one gRPC edge every driver in this crate serves from: the bound
//! listener, the h2 server, the prepared routes, and the persistent accept
//! future. The node, matchmaker and proxy loops differ only in the task name
//! their connection tasks carry and the `role` their connection errors name.

use std::fmt::Display;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;

use moonpool_core::{
    Detach, NetworkProvider, Providers, SimulationError, SimulationResult, TaskProvider,
    TcpListenerTrait,
};
use moonpool_hyper::{H2Server, H2ServerConfig};
use tokio_util::sync::CancellationToken;

use super::config::{DriverTunables, RunError, grpc_keep_alive};

/// The listener type a provider bundle binds.
type Listener<P> = <<P as Providers>::Network as NetworkProvider>::TcpListener;
/// The stream type that listener accepts.
type Stream<P> = <<P as Providers>::Network as NetworkProvider>::TcpStream;
/// One pending `accept`, owning its share of the listener so the future and
/// the listener can live side by side in [`GrpcEdge`].
type AcceptFuture<P> = Pin<Box<dyn Future<Output = io::Result<(Stream<P>, String)>> + Send>>;

/// A driver's inbound gRPC edge: the bound listener, the h2 server with the
/// driver's keep-alive, the prepared routes it serves, and the **persistent
/// accept future** below.
///
/// The accept future is PERSISTENT across select passes, for the same
/// reason a driver's tick deadline is absolute: `select!` drops and
/// re-creates its futures every pass, and a dropped accept forfeits its
/// progress. moonpool charges the accept latency per `accept()` call and
/// returns the reserved connection to the listener's queue when the future
/// is dropped, so under a client storm arriving faster than that latency
/// (a retry loop with a zero backoff pinned to one node, ~3 ms apart
/// against a 1–10 ms accept) no peer connection is ever accepted: the node
/// answers every client, hears no `Prepare` or `Heartbeat`, and on a
/// flexible seed whose Phase 1 needs every acceptor the cluster never
/// elects (seed 17898267817771645730 on 3484b13: 14,497 accepts cancelled
/// at one listener in 60 s, none completed after the chaos window; green
/// with this future). A kernel finishes the handshake whether or not an
/// `accept()` is pending, so production never saw it; polling one future
/// until it completes keeps the reservation and its delay in the sim too,
/// and only a completed accept creates the next one ([`GrpcEdge::serve_next`]).
pub(crate) struct GrpcEdge<P: Providers> {
    // Declared first so it drops before the listener it holds a share of.
    accept: AcceptFuture<P>,
    listener: Arc<Listener<P>>,
    server: H2Server<P>,
    routes: tonic::service::Routes,
    /// The incarnation's shutdown: every served connection ends with it.
    shutdown: CancellationToken,
    /// The name of the task each accepted connection is served on.
    task: &'static str,
    /// The role the listener's and its connections' errors name.
    role: &'static str,
}

/// The next `accept` on `listener`, owning its share of it. The provider's
/// `accept` is an `async fn`, so nothing happens until the future is first
/// polled — exactly when a bare `listener.accept()` would have started.
fn accept_on<P: Providers>(listener: &Arc<Listener<P>>) -> AcceptFuture<P> {
    let listener = Arc::clone(listener);
    Box::pin(async move { listener.accept().await })
}

impl<P: Providers> GrpcEdge<P> {
    /// Bind `local_addr` and prepare to serve `routes` on it with the
    /// driver's h2 keep-alive; the first accept is armed here and polled by
    /// the first [`GrpcEdge::serve_next`].
    ///
    /// # Errors
    ///
    /// [`RunError::Infra`] when the bind fails.
    pub(crate) async fn bind(
        providers: &P,
        local_addr: &str,
        task: &'static str,
        role: &'static str,
        tunables: &DriverTunables,
        routes: tonic::service::Routes,
        shutdown: CancellationToken,
    ) -> Result<Self, RunError> {
        let listener = providers
            .network()
            .bind(local_addr)
            .await
            .map_err(|e| SimulationError::InvalidState(format!("{role} gRPC listener: {e}")))?;
        let listener = Arc::new(listener);
        let server = H2Server::new(providers).with_config(H2ServerConfig {
            keep_alive: Some(grpc_keep_alive(tunables)),
            vectored_writes: true,
        });
        Ok(Self {
            accept: accept_on::<P>(&listener),
            listener,
            server,
            routes,
            shutdown,
            task,
            role,
        })
    }

    /// Await the pending accept, arm the next one, and serve the accepted
    /// connection on its own detached task until the incarnation ends. One
    /// `select!` arm of every driver loop.
    ///
    /// # Errors
    ///
    /// The accept's own error, a genuine infrastructure failure.
    pub(crate) async fn serve_next(&mut self, providers: &P) -> SimulationResult<()> {
        let accepted = self.accept.as_mut().await;
        self.accept = accept_on::<P>(&self.listener);
        let (stream, addr) = accepted.map_err(|e| {
            SimulationError::InvalidState(format!("{} gRPC accept: {e}", self.role))
        })?;
        let connection = self.server.serve_connection_with_shutdown(
            stream,
            self.routes.clone(),
            self.shutdown.clone().cancelled_owned(),
        );
        accept_and_serve(providers, self.task, self.role, addr, connection);
        Ok(())
    }
}

/// Serve one accepted gRPC connection on its own detached task, ending when
/// the incarnation does.
fn accept_and_serve<P, F, E>(
    providers: &P,
    task: &'static str,
    role: &'static str,
    addr: impl Display + Send + 'static,
    connection: F,
) where
    P: Providers,
    F: Future<Output = Result<(), E>> + Send + 'static,
    E: Display + Send + 'static,
{
    providers
        .task()
        .spawn_task(task, async move {
            if let Err(error) = connection.await {
                tracing::warn!(%addr, %error, role, "gRPC connection ended");
            }
        })
        .detach();
}
