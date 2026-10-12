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
    Answered, Attempted, CallObserver, ReadOutcome, Resolution, ResolveReport, SetLeaderOutcome,
    TruncateOutcome, WriteOutcome, write_request,
};
use paros::{
    Entry, JournalIdentifier, LeaderUuid, Read, Write, leader_from_proto, leader_uuid_from_proto,
};

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
///
/// And it records every write the library's `resolve` settled as not
/// written (#444): a marker in the history ([`Seen::NotWritten`]) the
/// search holds the write to.
#[derive(Clone)]
pub(crate) struct CallLog {
    journal: JournalIdentifier,
    client: u64,
    time: SimTimeProvider,
    attempts: Arc<Mutex<Vec<Attempt>>>,
    retries: Arc<Mutex<Retries>>,
    /// Each settlement started, by token: the write and when it started.
    settling: Arc<Mutex<Vec<(Call, u64)>>>,
}

/// The write operation open now, and the request each operation first sent.
#[derive(Default)]
struct Retries {
    open: Option<u64>,
    first: BTreeMap<u64, Call>,
}

impl CallLog {
    pub(crate) fn new(journal: JournalIdentifier, client: u64, time: SimTimeProvider) -> Self {
        Self::shared(journal, client, time, Arc::default())
    }

    /// A log of `client`'s attempts at `journal` that appends to `attempts`,
    /// a history several clients share: a control journal's (#247), which
    /// every client writes ([`control_attempts`]).
    pub(crate) fn shared(
        journal: JournalIdentifier,
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
            settling: Arc::default(),
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
            Attempted::Write(w) => write_call(w),
            Attempted::SetLeader(s) => Call::SetLeader {
                new: leader_uuid_from_proto(s.new),
                old: leader_from_proto(s.old),
            },
            Attempted::Read(r) => Call::Read {
                from: r.from_seq,
                limit: r.limit,
            },
            Attempted::Truncate(t) => Call::Truncate {
                leader: leader_uuid_from_proto(t.leader),
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
        // Every call this log sees goes to a node directly: only a
        // frontend denies a call (#192 (the frontend)).
        assert_always!(
            !denied(answer),
            "client: a node never denies a call, only a frontend does"
        );
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

    fn resolving(&self, write: &Write) -> Option<u64> {
        let journal = Attempted::Write(write).journal();
        if journal != self.journal {
            return None;
        }
        let mut settling = self.settling.lock().unwrap_or_else(PoisonError::into_inner);
        settling.push((write_call(write), self.now()));
        Some(settling.len() as u64 - 1)
    }

    fn resolved(&self, token: u64, report: &ResolveReport) {
        if !matches!(report.resolution, Resolution::NotWritten { .. }) {
            return;
        }
        let Some((call, started)) = usize::try_from(token).ok().and_then(|id| {
            self.settling
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .get(id)
                .cloned()
        }) else {
            return;
        };
        assert_reachable!("client: resolve proves a write not written");
        if !report.by_read_back {
            assert_reachable!("client: a re-send proves a write not written");
        }
        let now = self.now();
        self.attempts
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(Attempt {
                client: self.client,
                inv: started,
                call,
                seen: Some((now.max(started), Seen::NotWritten)),
            });
    }
}

/// A `Write` as the history records it: its fence, its position and one
/// hash per record.
fn write_call(write: &Write) -> Call {
    Call::Write {
        leader: leader_uuid_from_proto(write.leader),
        seq: write.seq,
        records: write.records.iter().map(|r| user_command_hash(r)).collect(),
    }
}

const CONTROL_LOG_KEY: &str = "paros-control-attempts";

/// Every client's attempts at the control journal `journal` (#247: the fleet tenant's,
/// the registry, the directory), the history `check()` searches once the
/// run is over (`crate::state::published_arc`).
pub(crate) fn control_attempts(
    state: &moonpool_sim::StateHandle,
    journal: JournalIdentifier,
) -> Arc<Mutex<Vec<Attempt>>> {
    crate::state::published_arc(
        state,
        &crate::state::journal_key(CONTROL_LOG_KEY, journal),
        || {
            logged_journals(state)
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(journal);
            Mutex::new(Vec::new())
        },
    )
}

const LOGGED_JOURNALS_KEY: &str = "paros-control-logged";

/// Every journal [`control_attempts`] holds a history for, in identifier
/// order (#442 (search every journal's history)): `check()` searches each
/// one, so no logged history is dropped unjudged.
pub(crate) fn logged_journals(
    state: &moonpool_sim::StateHandle,
) -> Arc<Mutex<std::collections::BTreeSet<JournalIdentifier>>> {
    crate::state::published_arc(state, LOGGED_JOURNALS_KEY, || {
        Mutex::new(std::collections::BTreeSet::new())
    })
}

/// Whether `answer` is a frontend's denial.
fn denied(answer: Answered<'_>) -> bool {
    matches!(
        answer,
        Answered::Write(WriteOutcome::Denied(_))
            | Answered::SetLeader(SetLeaderOutcome::Denied(_))
            | Answered::Read(ReadOutcome::Denied(_))
            | Answered::Truncate(TruncateOutcome::Denied(_))
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
        Answered::Write(WriteOutcome::WrongMode { state })
        | Answered::SetLeader(SetLeaderOutcome::WrongMode { state })
        | Answered::Truncate(TruncateOutcome::WrongMode { state }) => Some(Seen::WrongMode(*state)),
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
/// a member that has not folded the create yet does not serve it. Nor is it
/// from a joiner (#189): a joiner restarted by a storage fault serves its
/// journals again only once its registry follower admits it (#454 (control
/// plane meets chaos): hunt seed 5583448763827591485, a joiner that led the
/// default journal refused it for 2 s after a failed sync). `node`
/// is the node that answered and `journal` the one the client named, for
/// the violation's detail (#265).
pub(super) fn judged_write(
    outcome: WriteOutcome,
    created: bool,
    node: u64,
    journal: JournalIdentifier,
) -> WriteOutcome {
    assert_always!(
        outcome != WriteOutcome::Malformed,
        "chain: a node answers a well-formed journal state"
    );
    if (created || crate::roles::is_joiner(node)) && outcome == WriteOutcome::UnknownJournal {
        assert_reachable!("system: a member that has not folded a create refuses its journal");
        return WriteOutcome::Redirect { leader: None };
    }
    assert_always!(
        outcome != WriteOutcome::UnknownJournal,
        "chain: a node serves the journal the client names",
        { "node" => node, "tenant" => journal.tenant.0, "journal" => journal.journal.0 }
    );
    match outcome {
        WriteOutcome::UnknownJournal => WriteOutcome::Redirect { leader: None },
        WriteOutcome::Malformed => WriteOutcome::Ambiguous,
        outcome => outcome,
    }
}

/// [`judged_write`] for a `SetLeader` outcome.
pub(super) fn judged_set_leader(
    outcome: SetLeaderOutcome,
    created: bool,
    node: u64,
    journal: JournalIdentifier,
) -> SetLeaderOutcome {
    assert_always!(
        outcome != SetLeaderOutcome::Malformed,
        "chain: a node answers a well-formed journal state"
    );
    if (created || crate::roles::is_joiner(node)) && outcome == SetLeaderOutcome::UnknownJournal {
        assert_reachable!("system: a member that has not folded a create refuses its journal");
        return SetLeaderOutcome::Redirect { leader: None };
    }
    assert_always!(
        outcome != SetLeaderOutcome::UnknownJournal,
        "chain: a node serves the journal the client names",
        { "node" => node, "tenant" => journal.tenant.0, "journal" => journal.journal.0 }
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
    journal: JournalIdentifier,
    target: usize,
    entry: &Entry,
    abandon: bool,
    created: bool,
) -> impl Future<Output = WriteOutcome> + use<> {
    let listen = abandon.then_some(Duration::from_millis(10));
    let attempt = nodes.write_attempt(target, write_request(journal, entry), listen);
    let node = nodes.id_of(target);
    async move { judged_write(attempt.await, created, node, journal) }
}

/// One `SetLeader(new, old)` asked of server `target`; `created` as for
/// [`judged_write`].
pub(super) fn set_leader_once(
    nodes: &ChainClient,
    journal: JournalIdentifier,
    target: usize,
    (new, old): (LeaderUuid, Option<LeaderUuid>),
    created: bool,
) -> impl Future<Output = SetLeaderOutcome> + use<> {
    let attempt = nodes.set_leader_attempt(target, journal, new, old);
    let node = nodes.id_of(target);
    async move { judged_set_leader(attempt.await, created, node, journal) }
}

/// One journal `Read` of `journal` from `from`, asked of server `target`.
pub(super) fn read_once(
    client: &ChainClient,
    target: usize,
    journal: JournalIdentifier,
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
