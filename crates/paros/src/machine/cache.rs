//! A machine's **durable cached registry fold** (#211,
//! `docs/architecture.md` §3.2): where each machine of its cell was dialed
//! when the machine last folded the registry, kept on its own disk beside
//! the record.
//!
//! It is a static-stability requirement, not an optimisation. The registry
//! is the cell control journal, so a machine reads it through the cell's
//! machines. A machine that restarts after others moved (same `node_id`, new
//! address, #349) cannot reach them at the addresses its plan or its
//! admission names. The cache names where they are, so the machine finds
//! its cell while the registry is unavailable to it (`CockroachDB`'s gossip
//! plays this role).
//!
//! The cache is the cell's address book ([`super::cell_book`]) at one
//! position of the registry fold:
//!
//! ```text
//! node 6150928431937019931
//! position 42
//! machine 6150928431937019931 10.0.0.2:4500
//! machine 7240096361733624127 node-b:4500
//! ```
//!
//! - **Written forward only.** A machine writes it when its fold reaches a
//!   position above the cached one and the book changed. A new incarnation
//!   folds again from position 0, so its book follows the registry only
//!   once its fold passes the cached position ([`CachedRegistry::position`]):
//!   below it, the cache is the newer book.
//! - **Atomic.** The file is rewritten whole, as the record is
//!   ([`super::ProviderDisk::write_cache`]).
//! - **A hint, never a fact.** An address only says where to dial. A wrong
//!   one costs liveness, never safety: a batch names its cell (#216), and an
//!   answer names its machine. A cache that does not parse is ignored.
//!
//! The writer runs in a task of its own beside the machine: the node loop
//! only hands it the latest book ([`CacheSink`]) and never waits on the
//! disk.

use std::fmt::Write as _;

use moonpool_core::{Detach, Providers, StorageProvider, TaskProvider};
use paros_core::NodeId;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use super::ProviderDisk;
use crate::{Address, Audit};

/// The cell's address book at one position of the registry fold.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CachedRegistry {
    /// The machine that wrote it: a cache another identity left on the
    /// disk is ignored.
    pub node: NodeId,
    /// The fold's next position when the book was taken: the book holds
    /// every registry entry below it.
    pub position: u64,
    /// Every machine of the cell and where it is dialed, unique by id.
    pub machines: Vec<(NodeId, Address)>,
}

impl CachedRegistry {
    /// The cache's text.
    ///
    /// # Panics
    ///
    /// When the cache breaks its contract ([`CachedRegistry::check`]).
    #[must_use]
    pub fn render(&self) -> String {
        assert_eq!(
            self.check(),
            Ok(()),
            "a cache is checked before it is written"
        );
        let mut text = format!("node {}\nposition {}\n", self.node.0, self.position);
        for (id, addr) in &self.machines {
            let _ = writeln!(text, "machine {} {addr}", id.0);
        }
        assert!(text.ends_with('\n'), "a cache ends with its last line");
        text
    }

    /// Parse a cache's text.
    ///
    /// # Errors
    ///
    /// A line that does not parse, no position, or a cache
    /// [`CachedRegistry::check`] refuses.
    pub fn parse(text: &str) -> Result<Self, String> {
        let mut node = None;
        let mut position = None;
        let mut machines = Vec::new();
        for line in text.lines().filter(|l| !l.trim().is_empty()) {
            let (key, value) = line
                .split_once(' ')
                .ok_or_else(|| format!("bad cache line {line:?}"))?;
            match key {
                "node" if node.is_none() => {
                    node = Some(NodeId(value.parse().map_err(|e| format!("bad node: {e}"))?));
                }
                "position" if position.is_none() => {
                    position = Some(value.parse().map_err(|e| format!("bad position: {e}"))?);
                }
                "machine" => {
                    let (id, addr) = value
                        .split_once(' ')
                        .ok_or_else(|| format!("bad machine line {value:?}"))?;
                    let id = NodeId(id.parse().map_err(|e| format!("bad machine id: {e}"))?);
                    let addr = Address::parse(addr).map_err(|e| format!("bad address: {e}"))?;
                    machines.push((id, addr));
                }
                _ => return Err(format!("unknown cache line {line:?}")),
            }
        }
        let cache = Self {
            node: node.ok_or("a cache names its machine")?,
            position: position.ok_or("a cache names its position")?,
            machines,
        };
        cache.check()?;
        Ok(cache)
    }

    /// Whether the cache keeps its contract: a position past the registry's
    /// first entry, its machine and at least one machine of the cell named,
    /// every id set and unique.
    ///
    /// # Errors
    ///
    /// The first thing wrong with it.
    pub fn check(&self) -> Result<(), &'static str> {
        if self.node.0 == 0 {
            return Err("a cache names the machine that wrote it");
        }
        if self.position == 0 {
            return Err("a cache follows a fold that folded an entry");
        }
        if self.machines.is_empty() {
            return Err("a cache names a machine of the cell");
        }
        if self.machines.iter().any(|(id, _)| id.0 == 0) {
            return Err("a cached machine has a set id");
        }
        let mut ids: Vec<NodeId> = self.machines.iter().map(|(id, _)| *id).collect();
        ids.sort_unstable();
        ids.dedup();
        if ids.len() != self.machines.len() {
            return Err("a cache names each machine once");
        }
        Ok(())
    }

    /// The address the cache names for `id`.
    #[must_use]
    pub fn address(&self, id: NodeId) -> Option<&Address> {
        self.machines
            .iter()
            .find(|(m, _)| *m == id)
            .map(|(_, addr)| addr)
    }
}

/// `known` (the plan's founding members, or an admission's machines) where
/// `cache` dials them, and every other machine the cache names after them:
/// the book a machine starts from. A machine the cache does not name keeps
/// its known address.
///
/// # Panics
///
/// Never: the assertions check that every known machine keeps its place.
#[must_use]
pub fn starting_book(
    known: &[(NodeId, Address)],
    cache: Option<&CachedRegistry>,
) -> Vec<(NodeId, Address)> {
    let Some(cache) = cache else {
        return known.to_vec();
    };
    let mut book: Vec<(NodeId, Address)> = known
        .iter()
        .map(|(id, addr)| (*id, cache.address(*id).unwrap_or(addr).clone()))
        .collect();
    let moved = book.iter().zip(known).any(|(a, b)| a.1 != b.1);
    if moved {
        // The static-stability case (§3.2): a machine moved since the plan
        // or the admission, and only the cache knows where it is.
        moonpool_assertions::reachable!(
            "machine: a boot dials a moved machine from its cached registry fold"
        );
    }
    book.extend(
        cache
            .machines
            .iter()
            .filter(|(id, _)| !known.iter().any(|(k, _)| k == id))
            .cloned(),
    );
    assert!(
        book.len() >= known.len(),
        "the cache never drops a known machine"
    );
    assert!(
        book.iter().zip(known).all(|(a, b)| a.0 == b.0),
        "the known machines come first, in their order"
    );
    book
}

/// Where the node loop hands the writer its latest book.
#[derive(Clone, Debug)]
pub struct CacheSink {
    latest: watch::Sender<Option<CachedRegistry>>,
}

impl CacheSink {
    /// Hand the writer `cache`: it writes it unless the cache on disk is
    /// as new, or names the same book. Never waits.
    ///
    /// # Panics
    ///
    /// When `cache` breaks its contract ([`CachedRegistry::check`]).
    pub fn offer(&self, cache: CachedRegistry) {
        assert_eq!(cache.check(), Ok(()), "an offered cache is checked");
        self.latest.send_replace(Some(cache));
    }
}

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
    P: Providers,
    S: StorageProvider + Clone + 'static,
    A: Audit + Send + Sync + 'static,
{
    assert!(node.0 != 0, "a cache is a machine's");
    let (latest, mut offered) = watch::channel(None::<CachedRegistry>);
    providers
        .task()
        .spawn_task("paros-machine-registry-cache", async move {
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
    CacheSink { latest }
}

/// Whether `offered` is worth a write over `written`: a later position and
/// another book.
fn newer(written: Option<&CachedRegistry>, offered: &CachedRegistry) -> bool {
    written.is_none_or(|w| offered.position > w.position && offered.machines != w.machines)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(text: &str) -> Address {
        Address::parse(text).expect("an address")
    }

    fn cache(position: u64, machines: &[(u64, &str)]) -> CachedRegistry {
        CachedRegistry {
            node: NodeId(7),
            position,
            machines: machines
                .iter()
                .map(|(id, a)| (NodeId(*id), addr(a)))
                .collect(),
        }
    }

    #[test]
    fn a_cache_round_trips_and_a_broken_one_is_refused() {
        let c = cache(42, &[(7, "10.0.0.2:4500"), (3, "node-b:4500")]);
        assert_eq!(CachedRegistry::parse(&c.render()), Ok(c));
        assert!(CachedRegistry::parse("node 7\nmachine 7 10.0.0.2:4500\n").is_err());
        assert!(CachedRegistry::parse("position 3\nmachine 7 a:1\n").is_err());
        assert!(CachedRegistry::parse("node 7\nposition 0\nmachine 7 a:1\n").is_err());
        assert!(CachedRegistry::parse("node 7\nposition 3\n").is_err());
        assert!(
            CachedRegistry::parse("node 7\nposition 3\nmachine 7 a:1\nmachine 7 b:1\n").is_err()
        );
        assert!(CachedRegistry::parse("node 7\nposition 3\nmachine 0 a:1\n").is_err());
        assert!(CachedRegistry::parse("node 7\nposition 3\npeer 7 a:1\n").is_err());
        assert!(CachedRegistry::parse("\u{0}\u{1}garbage").is_err());
    }

    #[test]
    fn a_boot_dials_the_cached_addresses_then_the_known_ones() {
        let known = vec![(NodeId(1), addr("a:1")), (NodeId(2), addr("b:1"))];
        assert_eq!(starting_book(&known, None), known);
        let c = cache(9, &[(2, "b2:1"), (5, "e:1")]);
        assert_eq!(
            starting_book(&known, Some(&c)),
            vec![
                (NodeId(1), addr("a:1")),
                (NodeId(2), addr("b2:1")),
                (NodeId(5), addr("e:1")),
            ]
        );
    }

    #[test]
    fn only_a_later_other_book_is_written() {
        let c = cache(9, &[(2, "b:1")]);
        assert!(newer(None, &c));
        assert!(!newer(Some(&c), &c));
        assert!(!newer(Some(&c), &cache(12, &[(2, "b:1")])));
        assert!(!newer(Some(&c), &cache(8, &[(2, "c:1")])));
        assert!(newer(Some(&c), &cache(12, &[(2, "c:1")])));
    }
}
