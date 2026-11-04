//! A test rig: machines on moonpool's simulated network, driven on one
//! thread by the simulation's executor, so a test calls the shipped edge
//! and clients over a real RPC runtime.

use std::future::Future;
use std::net::{IpAddr, SocketAddr};
use std::task::Poll;

use moonpool_core::{LocalTaskProvider, ThreadProviders};
use moonpool_rpc::security::SecurityConfig;
use moonpool_rpc::{RpcConfig, RpcDriver, RpcHandle};
use moonpool_sim::executor::Executor;
use moonpool_sim::{SimProviders, SimWorld};

/// The rig's seed: a display seed, not a witness.
const SEED: u64 = 7;

/// The most simulation steps one test may take: a test that needs more is
/// stuck.
const MAX_STEPS: u32 = 100_000;

/// The port every rig machine listens on.
pub const PORT: u16 = 4500;

/// One simulated world.
pub struct Rig {
    sim: SimWorld,
}

impl Rig {
    pub fn new() -> Self {
        moonpool_sim::set_sim_seed(SEED);
        Self {
            sim: SimWorld::new_with_seed(SEED),
        }
    }

    /// The providers of the machine at `ip`.
    pub fn providers(&self, ip: &str) -> SimProviders {
        let ip: IpAddr = ip.parse().expect("a test IP");
        SimProviders::new(self.sim.downgrade(), ip)
    }

    /// Run `future` to its end, stepping the simulated network and clock
    /// while it waits.
    pub fn run<F: Future>(&mut self, future: F) -> F::Output {
        let sim = &mut self.sim;
        let mut steps = 0;
        Executor::new(SEED).block_on(async {
            let mut future = std::pin::pin!(future);
            std::future::poll_fn(|cx| match future.as_mut().poll(cx) {
                Poll::Ready(output) => Poll::Ready(output),
                Poll::Pending => {
                    steps += 1;
                    assert!(steps < MAX_STEPS, "the test is stuck");
                    if sim.has_pending_events() {
                        sim.step();
                    }
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            })
            .await
        })
    }
}

/// A machine's RPC runtime listening at `ip`, its driver spawned.
pub async fn listen(providers: &SimProviders, ip: &str) -> (RpcHandle<SimProviders>, SocketAddr) {
    let addr: SocketAddr = format!("{ip}:{PORT}").parse().expect("a test address");
    let config = RpcConfig {
        security: SecurityConfig::trusted_network(),
        ..RpcConfig::default()
    };
    let (driver, rpc) = RpcDriver::listen(providers.clone(), &addr.to_string(), config)
        .await
        .expect("listen");
    providers
        .local_task()
        .spawn_local_task("rig-server", async move {
            let _ = driver.run().await;
        })
        .detach();
    (rpc, addr)
}

/// A client-only RPC runtime on `providers`, its driver spawned.
pub fn client(providers: &SimProviders) -> RpcHandle<SimProviders> {
    let (driver, rpc) =
        RpcDriver::client_only(providers.clone(), RpcConfig::default()).expect("a valid config");
    providers
        .local_task()
        .spawn_local_task("rig-client", async move {
            let _ = driver.run().await;
        })
        .detach();
    rpc
}

/// What a rig server heard: one tag per answered request, in order.
pub type Heard = std::rc::Rc<std::cell::RefCell<Vec<&'static str>>>;

/// A note of `tag` in `heard` for each request.
pub fn note<T>(heard: &Heard, tag: &'static str) -> impl FnMut(&T) + 'static {
    let heard = heard.clone();
    move |_| heard.borrow_mut().push(tag)
}

/// Answer every request on `stream` with the default reply, after `on`
/// saw it.
pub fn answer<M>(
    providers: &SimProviders,
    mut stream: moonpool_rpc::RequestStream<M>,
    mut on: impl FnMut(&M::Request) + 'static,
) where
    M: moonpool_rpc::RpcMethod,
    M::Reply: Default,
{
    providers
        .local_task()
        .spawn_local_task("rig-answer", async move {
            while let Some(moonpool_rpc::IncomingRequest { request, reply }) = stream.recv().await {
                on(&request);
                reply.send(&M::Reply::default());
            }
        })
        .detach();
}
