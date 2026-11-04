//! The cache writer (#211) writes each newer book the node loop offers to
//! the machine's disk, skips a book that is not newer, and writes nothing
//! once stopped.

use std::time::Duration;

use moonpool_core::{LocalTimeProvider as _, Providers as _};
use moonpool_sim::SimProviders;
use paros_core::NodeId;
use paros_observe::audit::NoAudit;
use tokio_util::sync::CancellationToken;

use super::spawn_writer;
use crate::rig::{Rig, compact};
use crate::{CachedRegistry, ProviderDisk};
use paros_model::address::Address;

const ME: NodeId = NodeId(5);

fn disk(
    providers: &SimProviders,
) -> ProviderDisk<<SimProviders as moonpool_core::Providers>::Storage> {
    ProviderDisk::new(providers.storage().clone(), "/m", compact())
}

fn cache(position: u64, machines: &[u64]) -> CachedRegistry {
    CachedRegistry {
        node: ME,
        position,
        machines: machines
            .iter()
            .map(|id| {
                let addr = Address::parse(&format!("10.0.5.{id}:4500")).expect("an address");
                (NodeId(*id), addr)
            })
            .collect(),
    }
}

/// The cache on the disk of `providers`, if any.
async fn on_disk(providers: &SimProviders) -> Option<CachedRegistry> {
    let text = disk(providers).read_cache().await.expect("readable")?;
    Some(CachedRegistry::parse(&text).expect("a valid cache"))
}

/// A moment for the writer to write.
async fn pause(providers: &SimProviders) {
    let _ = providers.time().sleep(Duration::from_secs(1)).await;
}

#[test]
fn the_writer_writes_each_newer_book_and_skips_the_others() {
    let mut rig = Rig::new();
    let providers = rig.providers("10.0.5.1");
    let seen = rig.run(async {
        let shutdown = CancellationToken::new();
        let (sink, end) = spawn_writer(&providers, disk(&providers), NoAudit, ME, None, &shutdown);
        let mut seen = Vec::new();
        for offered in [
            cache(3, &[1, 2]),
            // Not newer: an older position.
            cache(2, &[1, 2, 3]),
            // Not newer: the same machines.
            cache(4, &[1, 2]),
            cache(5, &[1, 3]),
        ] {
            sink.offer(offered);
            pause(&providers).await;
            seen.push(on_disk(&providers).await.map(|c| c.position));
        }
        end.stop().await;
        sink.offer(cache(9, &[4]));
        pause(&providers).await;
        seen.push(on_disk(&providers).await.map(|c| c.position));
        seen
    });
    assert_eq!(
        seen,
        vec![Some(3), Some(3), Some(3), Some(5), Some(5)],
        "a stopped writer writes nothing"
    );
}

#[test]
fn the_writer_starts_from_the_cache_the_disk_held() {
    let mut rig = Rig::new();
    let providers = rig.providers("10.0.5.1");
    let written = rig.run(async {
        let shutdown = CancellationToken::new();
        let held = Some(cache(7, &[1, 2]));
        let (sink, end) = spawn_writer(&providers, disk(&providers), NoAudit, ME, held, &shutdown);
        sink.offer(cache(6, &[1, 3]));
        pause(&providers).await;
        let first = on_disk(&providers).await;
        shutdown.cancel();
        end.stop().await;
        first
    });
    assert_eq!(
        written, None,
        "a book older than the held one is not written"
    );
}
