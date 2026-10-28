//! Every journal store's on-disk layout, published to moonpool (#760
//! (moonpool owns fault placement)): moonpool places storage damage and
//! judges a crash's dirty sectors by these regions, each in the journal's
//! shard (`journal/<tenant>/<journal>`, the name its leader declares with
//! `shard!`), so a stripe is `(journal, slot)`.
//!
//! moonpool keeps one layout per process, and a node holds a store per
//! journal: the board keeps each store's regions by IP and directory, and
//! every publish sends the union for the IP.

use std::collections::BTreeMap;
use std::sync::PoisonError;

use moonpool_sim::{LayoutRegion, SimStorageProvider, StateHandle};

/// The run's state key of the layout board.
const LAYOUTS_KEY: &str = "paros-world-layouts";

/// Per IP, per shard: the regions of that shard's store on the IP.
type Board = BTreeMap<String, BTreeMap<String, Vec<LayoutRegion>>>;

/// Publish `regions`, the store of `shard` on `ip`: the IP's layout becomes
/// every store's regions on it.
pub(crate) fn publish(
    state: &StateHandle,
    provider: &SimStorageProvider,
    ip: &str,
    shard: &str,
    regions: Vec<LayoutRegion>,
) {
    let board = crate::state::published(state, LAYOUTS_KEY, Board::new);
    let union: Vec<LayoutRegion> = {
        let mut board = board.lock().unwrap_or_else(PoisonError::into_inner);
        let stores = board.entry(ip.to_string()).or_default();
        stores.insert(
            shard.to_string(),
            regions
                .into_iter()
                .map(|region| region.sharded(Some(shard.to_string())))
                .collect(),
        );
        stores.values().flatten().cloned().collect()
    };
    // Only a shut-down simulation refuses, and then nothing is placed.
    let _ = provider.publish_layout(&union);
}
