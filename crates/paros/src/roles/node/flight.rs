//! A journal's flush off the node loop (#613 (settle off the node loop)):
//! the loop takes a journal's batch from its core, hands the batch's writes
//! and the journal's store to a [`Flight`], and goes on serving every other
//! journal while the store syncs. The loop polls the flights as one of its
//! `select!` arms and finishes a batch the moment its flush lands.
//!
//! **One flight per journal, and its core waits for it.** While a journal's
//! batch flies, the loop steps nothing into that journal's core: a message,
//! a call, a read or a tick for it waits in the journal's backlog
//! ([`Input`]). Once the flush lands and the batch is finished, the loop
//! steps the backlog and takes one batch for the inputs of one phase: the
//! writes that arrived during one flush share the next one (group commit),
//! and an input that moves the core's phase (a campaign opened, a ballot
//! moved, a leader learned) ends its batch, so every per-batch observer
//! still sees at most one phase change (`Shared::step_backlog`). The core
//! therefore sees what it saw before: inputs, then one `Ready`, then its
//! finish. Nothing that rests on the batch's durable writes (a vote
//! counted, a reply, a `Commit`, a read served, a control journal's fold)
//! reaches the core, the wire or a reader before the flush.
//!
//! **A flight outlives its journal's runtime.** A journal parked or forgotten
//! while its batch flies leaves the flight behind as an orphan
//! ([`super::journals::Journals`]): it lands on its own, and the journal's
//! store is opened again only after it did, so two handles never write one
//! store.

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use paros_core::Message;

use crate::net::ReplySender;
use crate::observe::audit::Audit;
use crate::storage::LogStorage;
use crate::wire::{Read, ReadAck};

use super::calls::Call;
use super::ready::{Taken, persist_writes};
use crate::storage::boot::RunError;

/// A flush in flight: the journal's store, given back when it lands, and
/// the flush's verdict.
pub(crate) type Flush<S> = Pin<Box<dyn Future<Output = (S, Result<(), RunError>)>>>;

/// One journal's batch while its flush flies.
pub(crate) struct Flight<S> {
    /// The flush: persist and sync the batch's writes.
    pub(crate) flush: Flush<S>,
    /// The rest of the batch, finished once the flush lands.
    pub(crate) taken: Taken,
    /// The read expiry the batch's beat owed (`NodeLoop::finish`).
    pub(crate) expiry: Option<(u64, bool)>,
    /// When the batch was taken (metrics only, #366 (metrics)).
    pub(crate) began: Duration,
}

impl<S: LogStorage + 'static> Flight<S> {
    /// Launch the flush of `taken`'s writes on `storage`. The writes move
    /// into the flush; the batch keeps the rest.
    pub(crate) fn launch<A: Audit + Clone + Send + Sync + 'static>(
        mut storage: S,
        mut taken: Taken,
        (audit, self_id): (&A, u64),
        expiry: Option<(u64, bool)>,
        began: Duration,
    ) -> Self {
        let writes = std::mem::take(&mut taken.writes);
        assert!(!writes.is_empty(), "only a batch with writes flies");
        assert!(taken.writes.is_empty(), "the flush takes every write");
        let (must_sync, promised) = (taken.must_sync, taken.promised);
        let audit = audit.clone();
        let flush = Box::pin(async move {
            let done =
                persist_writes(&mut storage, &writes, must_sync, promised, self_id, &audit).await;
            (storage, done)
        });
        Self {
            flush,
            taken,
            expiry,
            began,
        }
    }
}

/// A landed flight: the store back, the flush's verdict, and the batch to
/// finish.
pub(crate) struct Landing<S> {
    pub(crate) storage: S,
    pub(crate) done: Result<(), RunError>,
    pub(crate) taken: Taken,
    pub(crate) expiry: Option<(u64, bool)>,
    pub(crate) began: Duration,
}

/// An input the node loop holds back while its journal's batch flies,
/// stepped into the core once the batch is finished, in arrival order.
pub(crate) enum Input {
    /// A journal call to propose (#204).
    Propose(Call),
    /// Journal reads to open (#204), at least one: every read that waited
    /// in the inbox together, served by one quorum read. Each arrived
    /// before the row is asked, so one watermark sample answers them all;
    /// a read opened for each would send one `PreRead` per read per peer,
    /// and a burst of them overflows the peer's keep-newest lane
    /// (`peer_queue_capacity`, `log_reads`).
    Read(Vec<(Read, ReplySender<ReadAck>)>),
    /// A peer's Paxos message.
    Deliver(Message),
    /// A beat, at the node's tick count. At most one waits: a journal whose
    /// flush outlasts a tick is a slow node, exactly as one whose loop
    /// stalled.
    Tick(u64),
}

/// The most inputs a journal holds back while its batch flies. Past it, the
/// node loop waits for the flight where the next input arrives, as it did
/// for every flush before #613: the backlog is the inbox's overflow, never
/// an unbounded queue.
pub(crate) const BACKLOG: usize = 1024;

const _: () = assert!(BACKLOG > 1, "a backlog holds more than one input");
