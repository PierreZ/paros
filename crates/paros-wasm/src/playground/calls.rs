//! The client calls the page draws: a read-only [`CallObserver`] on the
//! operator's journal clients. Each attempt is a `client_call` event and
//! each answer a `client_answer` event, named the way `msg_sent` names a
//! journal, so the timeline records them beside the machines' events.

use std::sync::atomic::{AtomicU64, Ordering};

use paros::client::{Answered, Attempted, CallObserver, ResolveReport};
use paros::wire::Write;

/// Logs every attempt and answer of one client.
#[derive(Default)]
pub(crate) struct CallTrace {
    next: AtomicU64,
}

/// The first word of a value's `Debug` form: its variant.
fn variant(value: &impl std::fmt::Debug) -> String {
    let text = format!("{value:?}");
    text.split(|c: char| !c.is_alphanumeric())
        .next()
        .unwrap_or_default()
        .to_owned()
}

impl CallObserver for CallTrace {
    fn invoked(&self, attempt: Attempted<'_>) -> Option<u64> {
        let token = self.next.fetch_add(1, Ordering::Relaxed) + 1;
        let journal = attempt.journal();
        let kind = match attempt {
            Attempted::Write(_) => "Write",
            Attempted::SetLeader(_) => "SetLeader",
            Attempted::Read(_) => "Read",
            Attempted::Truncate(_) => "Truncate",
        };
        tracing::info!(
            token,
            kind,
            tenant = journal.tenant.0,
            journal = journal.journal.0,
            "client_call"
        );
        Some(token)
    }

    fn answered(&self, token: u64, answer: Answered<'_>) {
        let outcome = match answer {
            Answered::Write(o) => variant(o),
            Answered::SetLeader(o) => variant(o),
            Answered::Read(o) => variant(o),
            Answered::Truncate(o) => variant(o),
        };
        tracing::info!(token, %outcome, "client_answer");
    }

    fn resolving(&self, _write: &Write) -> Option<u64> {
        None
    }

    fn resolved(&self, _token: u64, _report: &ResolveReport) {}
}
