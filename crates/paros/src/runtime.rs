//! The providers paros runs on: one thread, local tasks.
//!
//! Every role of paros runs on one thread. Production runs on compio
//! (`CompioProviders`, `parosd`), and the simulation runs on
//! `SimProviders`. Both are [`ThreadProviders`]: the local provider traits
//! (no `Send` futures) and a local task slot. Thus the same code runs in
//! both, and a task may hold a future that is not `Send`.

use moonpool_core::ThreadProviders;
use moonpool_rpc::RpcProviders;

/// The provider bundle every paros role takes: a [`ThreadProviders`] bundle
/// that moonpool's RPC runtime also runs on ([`RpcProviders`]).
///
/// Spawn with `providers.local_task().spawn_local_task(..)`. The futures of
/// the network, time and storage slots need not be `Send`.
pub trait ParosProviders: ThreadProviders + RpcProviders {}

impl<P: ThreadProviders + RpcProviders> ParosProviders for P {}
