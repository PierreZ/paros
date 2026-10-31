//! The writer of a machine's durable cached registry fold (#211): a task
//! beside the machine that writes each newer book the node loop offers.
//! The cache itself is [`paros_model::machine::CachedRegistry`].

use moonpool_core::{Detach, LocalStorageProvider, LocalTaskProvider};
use paros_core::NodeId;
use paros_net::runtime::ParosProviders;
use tokio::sync::{oneshot, watch};
use tokio_util::sync::CancellationToken;

use super::ProviderDisk;
use paros_observe::audit::Audit;

use paros_model::machine::newer;
use paros_model::machine::{CacheSink, CachedRegistry};

/// The end of a cache writer: the incarnation that started it stops it
/// and waits for its last write before a next incarnation starts. A
/// storage fault restarts the machine in its process, and an old writer's
/// write in flight could land behind the new one's, moving the cache back.
pub struct WriterEnd {
    stop: CancellationToken,
    ended: oneshot::Receiver<()>,
}

impl WriterEnd {
    /// Stop the writer and wait until it wrote its last cache.
    pub async fn stop(self) {
        self.stop.cancel();
        // The writer drops its half when it returns.
        let _ = self.ended.await;
        moonpool_assertions::reachable!("machine: an incarnation waits for its cache writer");
    }
}

/// Start the writer of machine `node`'s cache on `disk`, which holds
/// `cached` now, in a task that ends with `shutdown` or [`WriterEnd::stop`]:
/// the sink the machine offers each new book to.
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
    shutdown: &CancellationToken,
) -> (CacheSink, WriterEnd)
where
    P: ParosProviders,
    S: LocalStorageProvider + Clone + 'static,
    A: Audit + Send + Sync + 'static,
{
    assert!(node.0 != 0, "a cache is a machine's");
    let (latest, mut offered) = watch::channel(None::<CachedRegistry>);
    let shutdown = shutdown.child_token();
    let stop = shutdown.clone();
    let (ending, ended) = oneshot::channel::<()>();
    providers
        .local_task()
        .spawn_local_task("paros-machine-registry-cache", async move {
            let _ending = ending;
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
                // The select is fair: an offer that raced the incarnation's
                // end is the next incarnation's to write, never this one's.
                if shutdown.is_cancelled() {
                    return;
                }
                // The select is fair: an offer that raced the stop is the
                // next incarnation's to write.
                if shutdown.is_cancelled() {
                    return;
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
    (CacheSink::new(latest), WriterEnd { stop, ended })
}
