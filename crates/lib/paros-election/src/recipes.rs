//! The **recipes**: Curator's recipes as pure functions of the fold
//! (`/mnt/project-files/design/election-zookeeper-2026-11-24/proposal.md`,
//! section 3). A recipe builds the operations of a transaction and reads
//! its answer from a [`Tree`](crate::tree::Tree). No recipe watches its
//! predecessor (there is no herd effect): every member tails the journal
//! anyway.
//!
//! - [`Latch`]: Curator's `LeaderLatch`, and `InterProcessMutex` over a
//!   lock path. The turn of the parent is the latch.
//! - [`Group`]: Curator's `GroupMember`.
//! - [`Counter`]: Curator's `SharedCount`.
//!
//! **Open**: the other recipes of the proposal (`LeaderSelector`, the
//! semaphore, the read-write lock, barriers, queues, `CuratorCache`) come
//! in later pull requests.

mod counter;
mod group;
mod latch;

pub use counter::Counter;
pub use group::{Group, GroupMember};
pub use latch::Latch;

use crate::entry::Op;
use crate::path::Path;

/// The operations that create every missing ancestor of `path`, and `path`
/// itself, as persistent nodes with no data, from the top (Curator's
/// `creatingParentsIfNeeded`).
///
/// # Panics
///
/// When an assertion on its own postconditions fails: a programmer
/// error, never an operating condition.
#[must_use]
pub fn ensure(path: &Path) -> Vec<Op> {
    let mut chain = Vec::new();
    let mut at = Some(path.clone());
    while let Some(p) = at {
        if p.is_root() {
            break;
        }
        at = p.parent();
        chain.push(Op::Ensure { path: p });
    }
    chain.reverse();
    assert!(
        chain.len() == path.as_str().matches('/').count() || path.is_root(),
        "one operation for each name of the path"
    );
    chain
}
