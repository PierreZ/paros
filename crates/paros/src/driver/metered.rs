//! The node loop's metering (#366 (metrics), [`crate::metrics`]): every
//! data-plane call that reaches a journal this node runs is counted where
//! the loop takes it, and its answer is timed and its bytes counted where
//! the answer leaves, through the reply handle ([`ReplySender::observed`]).
//!
//! Observation only: the counts are written after the outcome they count,
//! and nothing here is read back by the loop. With metrics off the reply
//! handle passes through untouched and nothing is reported.
//!
//! The audit port hears the same two moments
//! ([`Audit::call_received`], [`Audit::call_answered`]), only while the
//! machine records metrics, so the simulation can check the counters
//! against its own fold of them.

use moonpool_core::TimeProvider;
use paros_core::{JournalIdentifier, NodeId};

use crate::audit::Audit;
use crate::machine::ControlJournals;
use crate::metrics::{CallKind, Metrics};
use crate::rpc::{ReadAck, ReplySender, WriteAck, WriteOutcome};

use super::system::ControlFollower;

/// The names a journal's metrics are labelled with, `(tenant, journal)`:
/// the cell's own journals by their role, a tenant's control journal as
/// `control`, a tenant's journal by its name. A journal whose names this
/// node does not know is labelled with its ids in hex.
pub(crate) fn journal_names<P: moonpool_core::Providers>(
    follower: Option<&ControlFollower<P>>,
    cell: Option<&ControlJournals>,
    journal: JournalIdentifier,
) -> (String, String) {
    let text = |bytes: &[u8]| String::from_utf8_lossy(bytes).into_owned();
    if let Some(cell) = cell {
        if journal == cell.cell {
            return ("cell".into(), "registry".into());
        }
        if Some(journal) == cell.election {
            return ("cell".into(), "election".into());
        }
        if Some(journal) == cell.fleet {
            return ("universe".into(), "directory".into());
        }
    }
    if let Some(follower) = follower {
        let hosted = follower.registry().hosted_tenant(journal.tenant);
        if let Some(hosted) = hosted
            && hosted.control == journal.journal
        {
            return (text(&hosted.name), "control".into());
        }
        if let Some(tenant) = follower.tenant(journal.tenant)
            && let Some(created) = tenant.get(journal.journal)
        {
            let name = tenant
                .description()
                .map(|d| text(&d.name))
                .or_else(|| hosted.map(|h| text(&h.name)))
                .unwrap_or_else(|| format!("{:x}", journal.tenant.0));
            return (name, text(&created.name));
        }
    }
    (
        format!("{:x}", journal.tenant.0),
        format!("{:x}", journal.journal.0),
    )
}

/// What an answer carried: bytes accepted and bytes served.
pub(crate) type Carried<U> = fn(&U, u64) -> (u64, u64);

/// A `Write`'s answer: its records' bytes when accepted.
pub(crate) fn write_carried(ack: &WriteAck, bytes: u64) -> (u64, u64) {
    if ack.outcome == WriteOutcome::Accepted as i32 {
        (bytes, 0)
    } else {
        (0, 0)
    }
}

/// A `Read`'s answer: its page's bytes.
pub(crate) fn read_carried(ack: &ReadAck, _: u64) -> (u64, u64) {
    let bytes = ack.records.iter().map(Vec::len).sum::<usize>();
    (0, u64::try_from(bytes).unwrap_or(u64::MAX))
}

/// An answer that carries no records (`SetLeader`, `Truncate`).
pub(crate) fn nothing_carried<U>(_: &U, _: u64) -> (u64, u64) {
    (0, 0)
}

/// One node's meter: its metrics, its clock and its node audit port.
pub(crate) struct Meter<'a, T, N> {
    pub(crate) metrics: &'a Metrics,
    pub(crate) time: &'a T,
    pub(crate) audit: &'a N,
    pub(crate) node: NodeId,
}

impl<T: TimeProvider, N: Audit + Clone + Send + Sync + 'static> Meter<'_, T, N> {
    /// Count call `call` to `journal` (labelled with `names` at its first
    /// call) and hand back its reply handle, which times the answer and
    /// counts what it `carried`; `bytes` are the records the request
    /// carried. With metrics off, `reply` comes back untouched.
    pub(crate) fn call<U: 'static>(
        &self,
        journal: JournalIdentifier,
        names: impl FnOnce() -> (String, String),
        (call, bytes): (CallKind, u64),
        reply: ReplySender<U>,
        carried: Carried<U>,
    ) -> ReplySender<U> {
        let Some(handles) = self.metrics.journal(journal, names) else {
            return reply;
        };
        handles.received(call);
        self.audit.call_received(self.node, journal, call);
        let (time, audit, node) = (self.time.clone(), self.audit.clone(), self.node);
        let started = time.now();
        reply.observed(move |ack| {
            let (written, read) = carried(ack, bytes);
            assert!(
                written == 0 || call == CallKind::Write,
                "only a write's answer counts written bytes"
            );
            assert!(
                read == 0 || call == CallKind::Read,
                "only a read's answer counts read bytes"
            );
            let took = time.now().saturating_sub(started);
            handles.answered(call, took, written, read);
            audit.call_answered(node, journal, call, written, read);
        })
    }
}

/// The bytes of a request's records.
pub(crate) fn request_bytes(records: &[Vec<u8>]) -> u64 {
    u64::try_from(records.iter().map(Vec::len).sum::<usize>()).unwrap_or(u64::MAX)
}
