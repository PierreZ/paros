//! The node loop's metering (#366 (metrics), [`crate::observe::metrics`]): every
//! data-plane call that reaches a journal this node runs is counted where
//! the loop takes it, and its answer is timed and counted by outcome and
//! bytes where the answer leaves, through the reply handle
//! ([`ReplySender::observed`]).
//!
//! Observation only: the counts are written after the outcome they count,
//! and nothing here is read back by the loop. With metrics off the reply
//! handle passes through untouched and nothing is reported.
//!
//! The audit port hears the same two moments
//! ([`Audit::call_received`], [`Audit::call_answered`]), only while the
//! machine records metrics, so the simulation can check the counters
//! against its own fold of them.

use moonpool_core::LocalTimeProvider;
use paros_core::{JournalIdentifier, NodeId};

use crate::machine::ControlJournals;
use crate::net::ReplySender;
use crate::observe::audit::Audit;
use crate::observe::metrics::{CallKind, CallOutcome, Metrics};
use crate::wire::{ReadAck, SetLeaderAck, TruncateAck, WriteAck, WriteOutcome};

use super::system::ControlFollower;

/// The names a journal's metrics are labelled with, `(tenant, journal)`,
/// the names `parosctl` prints (decided on 2026-10-15, #575 (key:value
/// tags)): the cell's own journals under the tenant `cell` or `universe`
/// as `(control)`, `(election)` and `(directory)`, a tenant's control
/// journal as `(control)`, a tenant's journal by its name. A journal whose
/// names this node does not know is labelled with its ids in hex.
pub(crate) fn journal_names<P: crate::ParosProviders>(
    follower: Option<&ControlFollower<P>>,
    cell: Option<&ControlJournals>,
    journal: JournalIdentifier,
) -> (String, String) {
    let text = |bytes: &[u8]| String::from_utf8_lossy(bytes).into_owned();
    if let Some(cell) = cell {
        if journal == cell.cell {
            return ("cell".into(), "(control)".into());
        }
        if Some(journal) == cell.election {
            return ("cell".into(), "(election)".into());
        }
        if Some(journal) == cell.fleet {
            return ("universe".into(), "(directory)".into());
        }
    }
    if let Some(follower) = follower {
        let hosted = follower.registry().hosted_tenant(journal.tenant);
        if let Some(hosted) = hosted
            && hosted.control == journal.journal
        {
            return (text(&hosted.name), "(control)".into());
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

/// What an answer said: its outcome, the bytes it accepted and the bytes
/// it served.
pub(crate) struct Answer {
    pub(crate) outcome: CallOutcome,
    pub(crate) written: u64,
    pub(crate) read: u64,
}

impl Answer {
    fn bare(outcome: CallOutcome) -> Self {
        Self {
            outcome,
            written: 0,
            read: 0,
        }
    }

    /// The outcome of an answer with no verdict: a redirect when it names
    /// a leader.
    fn no_verdict(unknown_journal: bool, leader: Option<u64>) -> Self {
        Self::bare(if unknown_journal {
            CallOutcome::UnknownJournal
        } else if leader.is_some() {
            CallOutcome::Redirected
        } else {
            CallOutcome::Unserved
        })
    }
}

/// How an answer is judged; the `u64` is the bytes the request carried.
pub(crate) type Judged<U> = fn(&U, u64) -> Answer;

/// A `Write`'s answer: its records' bytes when accepted.
pub(crate) fn write_answer(ack: &WriteAck, bytes: u64) -> Answer {
    if ack.busy {
        return Answer::bare(CallOutcome::Busy);
    }
    let outcome = match WriteOutcome::try_from(ack.outcome) {
        Ok(WriteOutcome::Accepted) => {
            return Answer {
                outcome: CallOutcome::Ok,
                written: bytes,
                read: 0,
            };
        }
        Ok(WriteOutcome::Duplicate) => CallOutcome::Duplicate,
        Ok(WriteOutcome::Refused) => CallOutcome::Refused,
        Ok(WriteOutcome::Truncated) => CallOutcome::Truncated,
        Ok(WriteOutcome::TooLarge) => CallOutcome::TooLarge,
        Ok(WriteOutcome::WrongMode) => CallOutcome::WrongMode,
        Ok(WriteOutcome::None) | Err(_) => {
            return Answer::no_verdict(ack.unknown_journal, ack.leader);
        }
    };
    Answer::bare(outcome)
}

/// A `Read`'s answer: its page's bytes.
pub(crate) fn read_answer(ack: &ReadAck, _: u64) -> Answer {
    if ack.unknown_journal || !ack.served {
        return Answer::no_verdict(ack.unknown_journal, None);
    }
    if ack.truncated {
        return Answer::bare(CallOutcome::Truncated);
    }
    let bytes = ack.records.iter().map(Vec::len).sum::<usize>();
    Answer {
        outcome: CallOutcome::Ok,
        written: 0,
        read: u64::try_from(bytes).unwrap_or(u64::MAX),
    }
}

/// A `SetLeader`'s answer: a lost compare-and-set is refused.
pub(crate) fn set_leader_answer(ack: &SetLeaderAck, _: u64) -> Answer {
    if ack.wrong_mode {
        Answer::bare(CallOutcome::WrongMode)
    } else if ack.decided {
        Answer::bare(if ack.won {
            CallOutcome::Ok
        } else {
            CallOutcome::Refused
        })
    } else {
        Answer::no_verdict(ack.unknown_journal, ack.leader)
    }
}

/// A `Truncate`'s answer.
pub(crate) fn truncate_answer(ack: &TruncateAck, _: u64) -> Answer {
    if ack.wrong_mode {
        Answer::bare(CallOutcome::WrongMode)
    } else if ack.refused {
        Answer::bare(CallOutcome::Refused)
    } else if ack.decided {
        Answer::bare(CallOutcome::Ok)
    } else {
        Answer::no_verdict(ack.unknown_journal, ack.leader)
    }
}

/// One node's meter: its metrics, its clock and its node audit port.
pub(crate) struct Meter<'a, T, N> {
    pub(crate) metrics: &'a Metrics,
    pub(crate) time: &'a T,
    pub(crate) audit: &'a N,
    pub(crate) node: NodeId,
}

impl<T: LocalTimeProvider, N: Audit + Clone + Send + Sync + 'static> Meter<'_, T, N> {
    /// Count call `call` to `journal` (labelled with `names` at its first
    /// call) and hand back its reply handle, which times the answer and
    /// counts it as `judged`; `bytes` are the records the request
    /// carried. With metrics off, `reply` comes back untouched.
    pub(crate) fn call<U: 'static>(
        &self,
        journal: JournalIdentifier,
        names: impl FnOnce() -> (String, String),
        (call, bytes): (CallKind, u64),
        reply: ReplySender<U>,
        judged: Judged<U>,
    ) -> ReplySender<U> {
        let Some(handles) = self.metrics.journal(journal, names) else {
            return reply;
        };
        handles.received(call);
        self.audit.call_received(self.node, journal, call);
        let (time, audit, node) = (self.time.clone(), self.audit.clone(), self.node);
        let started = time.now();
        reply.observed(move |ack| {
            let Answer {
                outcome,
                written,
                read,
            } = judged(ack, bytes);
            assert!(
                written == 0 || call == CallKind::Write,
                "only a write's answer counts written bytes"
            );
            assert!(
                read == 0 || call == CallKind::Read,
                "only a read's answer counts read bytes"
            );
            let took = time.now().saturating_sub(started);
            handles.answered((call, outcome), took, written, read);
            audit.call_answered(node, journal, (call, outcome), written, read);
        })
    }
}

/// The bytes of a request's records.
pub(crate) fn request_bytes(records: &[Vec<u8>]) -> u64 {
    u64::try_from(records.iter().map(Vec::len).sum::<usize>()).unwrap_or(u64::MAX)
}
