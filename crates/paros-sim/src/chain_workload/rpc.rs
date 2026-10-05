//! The chain client's seam onto `paros::client` (#221): the history the
//! linearizability checker reads, recorded as the library's
//! [`CallObserver`], and the handful of one-attempt calls the workload
//! makes on purpose — the misbehaviours and the races, which no policy loop
//! of the library would make — each judged here against the oracles every
//! answer must pass.
//!
//! The one-attempt calls build their request (and log it) **when called**
//! and return the attempt's future, so the run's call sequence — and the
//! `select!`s each one races — is fixed by the caller alone. No function
//! here draws randomness.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use moonpool_sim::{SimContext, SimTimeProvider, TimeProvider, assert_always, assert_reachable};
use paros::client::{
    Answered, Attempted, CallObserver, ReadOutcome, SetLeaderOutcome, TruncateOutcome,
    WriteOutcome, write_request,
};
use paros::{Entry, JournalKey, Read};

use crate::audit::{Attempt, Call, Seen};
use crate::chain::user_command_hash;
use crate::client::ChainClient;

/// One client's journal calls as the linearizability checker reads them
/// (#205): every attempt at its own journal, recorded **at the RPC seam** —
/// the library client reports every attempt it builds and every answer it
/// judges ([`CallObserver`]), so no call site can forget one. An attempt is
/// logged when its request is built and answered when a verdict comes back;
/// one whose future is dropped (a timeout, an abandoned observation, the
/// shutdown) or that comes back without a verdict stays unknown. Calls
/// naming any other journal (the system journals, a created one, a stray
/// id) are not this history's and are not logged.
///
/// It also holds the library to **a retry is the same write** (#204, #221):
/// while the workload has an operation open ([`CallLog::open_write`]),
/// every `Write` attempt the library makes for it must carry the request
/// the first one did — generation, owner, position and bytes. A client
/// that re-sent a write at a fresh position would write it twice; the
/// journal model cannot tell two such attempts from two writes, so this is
/// where it is caught.
#[derive(Clone)]
pub(crate) struct CallLog {
    journal: JournalKey,
    client: u64,
    time: SimTimeProvider,
    attempts: Arc<Mutex<Vec<Attempt>>>,
    retries: Arc<Mutex<Retries>>,
}

/// The write operation open now, and the request each operation first sent.
#[derive(Default)]
struct Retries {
    open: Option<u64>,
    first: BTreeMap<u64, Call>,
}

impl CallLog {
    pub(crate) fn new(journal: JournalKey, client: u64, time: SimTimeProvider) -> Self {
        Self::shared(journal, client, time, Arc::default())
    }

    /// A log of `client`'s attempts at `journal` that appends to `attempts`,
    /// a history several clients share: a control journal's (#247), which
    /// every client writes ([`control_attempts`]).
    pub(crate) fn shared(
        journal: JournalKey,
        client: u64,
        time: SimTimeProvider,
        attempts: Arc<Mutex<Vec<Attempt>>>,
    ) -> Self {
        Self {
            journal,
            client,
            time,
            attempts,
            retries: Arc::default(),
        }
    }

    /// The `Write` attempts from now until [`CallLog::close_write`] are all
    /// the workload's operation `op`: one write, however many times the
    /// library sends it.
    pub(crate) fn open_write(&self, op: u64) {
        self.retries
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .open = Some(op);
    }

    /// Close the operation [`CallLog::open_write`] opened.
    pub(crate) fn close_write(&self) {
        self.retries
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .open = None;
    }

    /// Hold an attempt of the open operation to the request it first sent.
    fn judge_retry(&self, call: &Call) {
        let mut retries = self.retries.lock().unwrap_or_else(PoisonError::into_inner);
        let Some(op) = retries.open else {
            return;
        };
        let first = retries.first.entry(op).or_insert_with(|| call.clone());
        assert_always!(
            first == call,
            "client: every attempt of one write is the identical write",
            { "op" => op }
        );
    }

    fn now(&self) -> u64 {
        u64::try_from(self.time.now().as_nanos()).unwrap_or(u64::MAX)
    }

    /// Every attempt so far, handed to the history at `check()`.
    pub(crate) fn take(&self) -> Vec<Attempt> {
        std::mem::take(&mut *self.attempts.lock().unwrap_or_else(PoisonError::into_inner))
    }
}

impl CallObserver for CallLog {
    fn invoked(&self, attempt: Attempted<'_>) -> Option<u64> {
        if attempt.journal() != self.journal {
            return None;
        }
        let call = match attempt {
            Attempted::Write(w) => Call::Write {
                generation: w.generation,
                owner: w.owner,
                seq: w.seq,
                records: w.records.iter().map(|r| user_command_hash(r)).collect(),
            },
            Attempted::SetLeader(s) => Call::SetLeader {
                expected: s.expected,
                owner: s.owner,
            },
            Attempted::Read(r) => Call::Read {
                from: r.from_seq,
                limit: r.limit,
            },
            Attempted::Truncate(t) => Call::Truncate {
                generation: t.generation,
                owner: t.owner,
                up_to: t.up_to,
            },
        };
        if matches!(call, Call::Write { .. }) {
            self.judge_retry(&call);
        }
        let mut attempts = self.attempts.lock().unwrap_or_else(PoisonError::into_inner);
        attempts.push(Attempt {
            client: self.client,
            inv: self.now(),
            call,
            seen: None,
        });
        Some(attempts.len() as u64 - 1)
    }

    fn answered(&self, token: u64, answer: Answered<'_>) {
        let Some(seen) = seen(answer) else {
            return;
        };
        let now = self.now();
        let mut attempts = self.attempts.lock().unwrap_or_else(PoisonError::into_inner);
        let Some(attempt) = usize::try_from(token)
            .ok()
            .and_then(|id| attempts.get_mut(id))
        else {
            return;
        };
        attempt.seen = Some((now.max(attempt.inv), seen));
    }
}

const CONTROL_LOG_KEY: &str = "paros-control-attempts";

/// Every client's attempts at the control journal `journal` (#247: meta,
/// the registry, the directory), the history `check()` searches once the
/// run is over (`crate::state::published_arc`).
pub(crate) fn control_attempts(
    state: &moonpool_sim::StateHandle,
    journal: JournalKey,
) -> Arc<Mutex<Vec<Attempt>>> {
    crate::state::published_arc(
        state,
        &crate::state::journal_key(CONTROL_LOG_KEY, journal),
        || Mutex::new(Vec::new()),
    )
}

/// The verdict the checker reads off an answer; `None` for no verdict (a
/// redirect, an unserved read, no answer).
fn seen(answer: Answered<'_>) -> Option<Seen> {
    match answer {
        Answered::Write(WriteOutcome::Written {
            seq,
            count,
            duplicate,
        }) => Some(Seen::Written {
            seq: *seq,
            count: *count,
            duplicate: *duplicate,
        }),
        Answered::Write(WriteOutcome::Refused { state }) => Some(Seen::Refused(*state)),
        Answered::Write(WriteOutcome::Truncated { state }) => Some(Seen::WriteTruncated(*state)),
        Answered::SetLeader(SetLeaderOutcome::Won { state }) => Some(Seen::Won(*state)),
        Answered::SetLeader(SetLeaderOutcome::Lost { state }) => Some(Seen::Lost(*state)),
        Answered::Read(ReadOutcome::Page { records, state, .. }) => Some(Seen::Page {
            records: records.iter().map(|r| user_command_hash(r)).collect(),
            state: *state,
        }),
        Answered::Read(ReadOutcome::Truncated { state }) => Some(Seen::ReadTruncated(*state)),
        Answered::Truncate(TruncateOutcome::Applied { state }) => Some(Seen::Trimmed(*state)),
        Answered::Truncate(TruncateOutcome::Refused { state }) => {
            Some(Seen::TruncateRefused(*state))
        }
        _ => None,
    }
}

/// Judge a `Write` outcome against the oracles every answer passes: a node
/// serves the journal the client names, and names a well-formed state. On
/// a journal `created` at runtime (#189) an unknown answer is no verdict:
/// a member that has not folded the create yet does not serve it.
pub(super) fn judged_write(outcome: WriteOutcome, created: bool) -> WriteOutcome {
    assert_always!(
        outcome != WriteOutcome::Malformed,
        "chain: a node answers a well-formed journal state"
    );
    if created && outcome == WriteOutcome::UnknownJournal {
        assert_reachable!("system: a member that has not folded a create refuses its journal");
        return WriteOutcome::Redirect { leader: None };
    }
    assert_always!(
        outcome != WriteOutcome::UnknownJournal,
        "chain: a node serves the journal the client names"
    );
    match outcome {
        WriteOutcome::UnknownJournal => WriteOutcome::Redirect { leader: None },
        WriteOutcome::Malformed => WriteOutcome::Ambiguous,
        outcome => outcome,
    }
}

/// [`judged_write`] for a `SetLeader` outcome.
pub(super) fn judged_set_leader(outcome: SetLeaderOutcome, created: bool) -> SetLeaderOutcome {
    assert_always!(
        outcome != SetLeaderOutcome::Malformed,
        "chain: a node answers a well-formed journal state"
    );
    if created && outcome == SetLeaderOutcome::UnknownJournal {
        assert_reachable!("system: a member that has not folded a create refuses its journal");
        return SetLeaderOutcome::Redirect { leader: None };
    }
    assert_always!(
        outcome != SetLeaderOutcome::UnknownJournal,
        "chain: a node serves the journal the client names"
    );
    match outcome {
        SetLeaderOutcome::UnknownJournal => SetLeaderOutcome::Redirect { leader: None },
        SetLeaderOutcome::Malformed => SetLeaderOutcome::Ambiguous,
        outcome => outcome,
    }
}

/// The journal state a truncation answered with, judged well-formed.
pub(super) fn judged_truncate(outcome: TruncateOutcome) -> TruncateOutcome {
    assert_always!(
        outcome != TruncateOutcome::Malformed,
        "chain: a node answers a well-formed journal state"
    );
    match outcome {
        TruncateOutcome::Malformed => TruncateOutcome::Ambiguous,
        outcome => outcome,
    }
}

/// Race `request` against `timeout` and the run's shutdown: `fallback` when
/// either comes first (an ambiguous observation, never a refusal).
pub(super) async fn within<T>(
    ctx: &SimContext,
    timeout: Duration,
    fallback: T,
    request: impl Future<Output = T>,
) -> T {
    moonpool_sim::select! {
        result = request => result,
        _ = ctx.time().sleep(timeout) => fallback,
        () = ctx.shutdown().cancelled() => fallback,
    }
}

/// One `Write` of `entry` to `journal` at server `target` — one attempt,
/// no redirect followed. With `abandon` the client stops listening after
/// 10 ms and records the observation as ambiguous. `created` as for
/// [`judged_write`].
pub(super) fn write_once(
    nodes: &ChainClient,
    journal: JournalKey,
    target: usize,
    entry: &Entry,
    abandon: bool,
    created: bool,
) -> impl Future<Output = WriteOutcome> + use<> {
    let listen = abandon.then_some(Duration::from_millis(10));
    let attempt = nodes.write_attempt(target, write_request(journal, entry), listen);
    async move { judged_write(attempt.await, created) }
}

/// One `SetLeader(expected, owner)` asked of server `target`; `created` as
/// for [`judged_write`].
pub(super) fn set_leader_once(
    nodes: &ChainClient,
    journal: JournalKey,
    target: usize,
    (expected, owner): (u64, u64),
    created: bool,
) -> impl Future<Output = SetLeaderOutcome> + use<> {
    let attempt = nodes.set_leader_attempt(target, journal, expected, owner);
    async move { judged_set_leader(attempt.await, created) }
}

/// One journal `Read` of `journal` from `from`, asked of server `target`.
pub(super) fn read_once(
    client: &ChainClient,
    target: usize,
    journal: JournalKey,
    from: u64,
    limit: u64,
    wait_ms: u64,
) -> impl Future<Output = ReadOutcome> + use<> {
    client.read_attempt(
        target,
        Read {
            journal: journal.journal.0,
            tenant: journal.tenant.0,
            from_seq: from,
            limit,
            wait_ms,
        },
    )
}
