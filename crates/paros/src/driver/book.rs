//! A formed machine's **peer book** (#349, `docs/architecture.md` §3.2):
//! where its lanes dial each founding member, folded from the registry, never
//! from the plan.
//!
//! The registry is the cell tenant's control journal, which every founding
//! member serves. After each tick the node folds its own chosen prefix of that
//! journal (a local read, as a seed's system follower does, #189), and when
//! the [`address_book`] moves a peer, the peer's lane dials the new address
//! from its next batch on. A peer the registry never moved keeps the plan's
//! address. The fold is volatile: every incarnation folds again from what its
//! own log holds, starting from the machine's **cached registry fold**
//! (#211): the lanes dial where the cache says until the fold passes the
//! cache's position, and every later book is offered to the cache.

use std::collections::BTreeMap;

use paros_core::{JournalIdentifier, LogRead, NodeId, Seq};

use super::journals::Journals;
use crate::Address;
use crate::client::checkpoint::Folder;
use crate::machine::{CachedRegistry, FormedCell, address_book, cell_book};
use crate::system::Registry;

/// The records one local read of the registry takes.
const BOOK_READ_RECORDS: usize = 256;

/// The record bytes one local read of the registry takes.
const BOOK_READ_BYTES: usize = 64 * 1024;

// A read that takes nothing never moves the fold.
const _: () = assert!(BOOK_READ_RECORDS > 0 && BOOK_READ_BYTES > 0);

/// A formed machine's fold of its cell's registry, and the address each
/// peer's lane dials now.
pub(crate) struct PeerBook {
    /// This machine.
    me: NodeId,
    /// The cell control journal: the registry.
    registry: JournalIdentifier,
    /// The founding members with the plan's addresses.
    founders: Vec<(NodeId, Address)>,
    fold: Folder<Registry>,
    /// The cached registry fold's position: below it, the cache's book is
    /// newer than the fold's, so the fold moves no lane.
    floor: u64,
    /// The address each peer's lane dials now.
    dialed: BTreeMap<NodeId, Address>,
}

impl PeerBook {
    /// The book of the founding member `formed`: every peer where its
    /// cached registry fold dials it, else at the plan's address, until the
    /// registry moves it.
    pub(crate) fn new(formed: &FormedCell) -> Self {
        let me = formed.facts.node_id;
        let founders = formed.plan.members.clone();
        let dialed: BTreeMap<NodeId, Address> = formed
            .founders()
            .into_iter()
            .filter(|(id, _)| *id != me)
            .collect();
        assert!(
            founders.iter().any(|(id, _)| *id == me),
            "a peer book is a founding member's"
        );
        assert!(!dialed.contains_key(&me), "a machine is not its own peer");
        Self {
            me,
            registry: formed.plan.control,
            founders: founders.clone(),
            fold: Folder::new(Registry::new(founders.iter().map(|(id, _)| *id))),
            floor: formed.cached.as_ref().map_or(0, |c| c.position),
            dialed,
        }
    }

    /// Fold what this node's own registry log chose since the last call, and
    /// return every peer the book moved, with its new address, and the
    /// cell's book when the fold reached a new one at or past the cache's
    /// position (#211).
    pub(crate) fn follow<S, A>(
        &mut self,
        journals: &Journals<S, A>,
    ) -> (Vec<(NodeId, Address)>, Option<CachedRegistry>) {
        let Some(rt) = journals.live.get(&self.registry) else {
            return (Vec::new(), None);
        };
        let before = self.fold.next_seq();
        loop {
            let from = self.fold.next_seq();
            match rt
                .node
                .read_log(Seq(from), BOOK_READ_RECORDS, BOOK_READ_BYTES)
            {
                LogRead::Page(page) if page.next().0 > from => {
                    for (seq, record) in (page.from.0..).zip(&page.records) {
                        self.fold.fold(seq, &record.0);
                    }
                }
                LogRead::Truncated(state) if state.first_seq.0 > from => {
                    self.fold.jump(state.first_seq.0);
                }
                _ => break,
            }
        }
        assert!(self.fold.next_seq() >= before, "the fold never moves back");
        // A fold that jumped a gap holds no registry until its checkpoint:
        // the book stands until then.
        if self.fold.next_seq() == before || !self.fold.is_whole() {
            return (Vec::new(), None);
        }
        // Below the cache's position the cache is the newer book (#211): a
        // restarted machine's fold re-reads history it already cached.
        if self.fold.next_seq() < self.floor {
            moonpool_assertions::reachable!(
                "machine: a restarted fold stays behind its cached registry fold"
            );
            return (Vec::new(), None);
        }
        let mut moved = Vec::new();
        for (id, addr) in address_book(&self.founders, self.fold.state()) {
            if id == self.me {
                continue;
            }
            let dialed = self.dialed.get_mut(&id);
            let Some(dialed) = dialed else { continue };
            if *dialed != addr {
                dialed.clone_from(&addr);
                moved.push((id, addr));
            }
        }
        assert!(
            moved.iter().all(|(id, _)| *id != self.me),
            "a machine never moves its own lane"
        );
        let cache = CachedRegistry {
            node: self.me,
            position: self.fold.next_seq(),
            machines: cell_book(&self.founders, self.fold.state()),
        };
        assert!(cache.position >= self.floor, "a cache is written forward");
        assert_eq!(cache.check(), Ok(()), "a fold's book is a valid cache");
        (moved, Some(cache))
    }
}
