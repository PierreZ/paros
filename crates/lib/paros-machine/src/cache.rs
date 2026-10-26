//! The writer of a machine's durable cached registry fold (#211): a task
//! beside the machine that writes each newer book the node loop offers.
//! The cache itself is [`paros_model::machine::CachedRegistry`].

use moonpool_core::{Detach, LocalStorageProvider, LocalTaskProvider};
use paros_core::NodeId;
use paros_net::runtime::ParosProviders;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use super::ProviderDisk;
use paros_observe::audit::Audit;

use paros_model::machine::newer;
use paros_model::machine::{CacheSink, CachedRegistry};

/// Start the writer of machine `node`'s cache on `disk`, which holds
/// `cached` now, in a task that ends with `shutdown`: the sink the machine
/// offers each new book to.
///
/// # Panics
///
/// When `node` is unset: a cache is a machine's.
pub fn spawn_writer<P, S, A>(
    providers: &P,
    disk: ProviderDisk<S>,
    audit: A,
    node: NodeId,
    cached: Option<CachedRegistry>,
    shutdown: CancellationToken,
) -> CacheSink
where
    P: ParosProviders,
    S: LocalStorageProvider + Clone + 'static,
    A: Audit + Send + Sync + 'static,
{
    assert!(node.0 != 0, "a cache is a machine's");
    let (latest, mut offered) = watch::channel(None::<CachedRegistry>);
    providers
        .local_task()
        .spawn_local_task("paros-machine-registry-cache", async move {
            let mut written = cached;
            loop {
                moonpool_core::select! {
                    () = shutdown.cancelled() => return,
                    changed = offered.changed() => {
                        if changed.is_err() {
                            return;
                        }
                    }
                }
                let Some(cache) = offered.borrow_and_update().clone() else {
                    continue;
                };
                if !newer(written.as_ref(), &cache) {
                    continue;
                }
                let landed = disk.write_cache(&cache.render()).await;
                audit.registry_cached(node, &cache, landed.is_ok());
                match landed {
                    Ok(()) => {
                        moonpool_assertions::reachable!(
                            "machine: a machine caches its registry fold durably"
                        );
                        tracing::info!(
                            node = node.0,
                            position = cache.position,
                            machines = cache.machines.len(),
                            "registry_cached"
                        );
                        written = Some(cache);
                    }
                    // The cache on disk stands: the next book tries again.
                    Err(error) => {
                        tracing::warn!(node = node.0, %error, "registry_cache_failed");
                    }
                }
            }
        })
        .detach();
    CacheSink::new(latest)
}
