//! The deposed-actor oracle (#240): an actor's writer stops at its first
//! refusal, so no fenced call leaves under a uuid a refusal already named
//! deposed. The rule is judged where the actor **sends**: a frontend sees
//! each call only when it relays it, a network delay later, so a call the
//! actor sent before its refusal can reach one frontend after another
//! frontend relayed the refusal (seed 17505150823314862491 on PR #691's
//! head: a fenced `Truncate` timed out ambiguous, the actor's next write was
//! refused through another frontend, and the delayed `Truncate` reached its
//! frontend 1.6 s later; the journal refused it under the new leader, so
//! the fence held). [`Announce`](super::system::Announce) relaying for a
//! frontend keeps the bookkeeping and asserts nothing; [`ActorFence`], the
//! observer of every client over the frontends, judges the actor's sends.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Mutex, PoisonError};

use moonpool_sim::{assert_always, assert_reachable};
use paros::client::{Answered, Attempted, CallObserver, TruncateOutcome, WriteOutcome};
use paros::core::Value;
use paros::core::journal::Command;
use paros::core::journal::Entry;
use paros::core::journal::JournalIdentifier;
use paros::core::journal::LeaderUuid;
use paros::core::journal::Seq;

/// The uuid a fenced attempt carries (#241): a `Write`'s or a `Truncate`'s
/// leader uuid, when set.
pub(super) fn fence_of(attempt: Attempted<'_>) -> Option<LeaderUuid> {
    match attempt {
        Attempted::Write(write) => Some(paros::wire::leader_uuid_from_proto(write.leader)),
        Attempted::Truncate(truncate) => Some(paros::wire::leader_uuid_from_proto(truncate.leader)),
        Attempted::SetLeader(_) | Attempted::Read(_) => None,
    }
    .filter(|uuid| uuid.is_set())
}

/// The command hash of `write`, the identity a re-send keeps.
pub(super) fn write_hash(write: &paros::wire::Write) -> u64 {
    let entry = Entry {
        leader: paros::wire::leader_uuid_from_proto(write.leader),
        seq: Seq(write.seq),
        records: write.records.iter().cloned().map(Value).collect(),
    };
    paros::roles::node::command_hash(&Command::Write(entry))
}

/// Judge a fenced call leaving under `uuid` to `journal` from `client`:
/// `write_hash` names a write's command. `deposed` holds the uuids a
/// refusal named deposed, `early` the writes sent before their uuid was.
pub(super) fn sending(
    deposed: &BTreeSet<LeaderUuid>,
    early: &mut BTreeSet<(LeaderUuid, u64)>,
    (client, journal): (u64, JournalIdentifier),
    uuid: LeaderUuid,
    write_hash: Option<u64>,
) {
    let gone = deposed.contains(&uuid);
    // The one call that may still leave: the identical re-send of
    // a write first sent before its uuid was deposed, the library's
    // `resolve` settling it (PR #678: a pipeline's sibling tickets
    // were refused while one of its writes was ambiguous). The
    // journal refuses it in place; a new write is never re-sent.
    let resend = gone && write_hash.is_some_and(|hash| early.contains(&(uuid, hash)));
    if resend {
        assert_reachable!("election: a deposed actor's ambiguous write is re-sent to resolve it");
    }
    assert_always!(
        !gone || resend,
        "election: a deposed actor writes nothing after its first refusal",
        { "client" => client, "journal" => journal.to_string() }
    );
    if let (false, Some(hash)) = (gone, write_hash) {
        early.insert((uuid, hash));
    }
}

/// Fold the answer to a fenced call under `uuid` into `deposed`: a refusal
/// naming another leader deposes the uuid.
pub(super) fn answered(deposed: &mut BTreeSet<LeaderUuid>, uuid: LeaderUuid, answer: Answered<'_>) {
    let leader = match answer {
        Answered::Write(WriteOutcome::Refused { state })
        | Answered::Truncate(TruncateOutcome::Refused { state }) => Some(state.leader),
        _ => None,
    };
    if let Some(leader) = leader
        && leader != Some(uuid)
    {
        deposed.insert(uuid);
        assert_reachable!("election: a fenced call is refused because another leads");
    }
}

/// The actor's side of the oracle, as the [`CallObserver`] of a client over
/// the frontends (#192 (the frontend)): it judges each fenced call as the
/// actor sends it and learns each refusal as the actor does. It logs no
/// attempt: the frontend's [`Announce`](super::system::Announce) logs every
/// call it relays to the journal's history.
pub(crate) struct ActorFence {
    client: u64,
    inner: Mutex<Inner>,
}

#[derive(Default)]
struct Inner {
    deposed: BTreeSet<LeaderUuid>,
    early: BTreeSet<(LeaderUuid, u64)>,
    /// The uuid each pending fenced token carries.
    fenced: BTreeMap<u64, LeaderUuid>,
    next_token: u64,
}

impl ActorFence {
    /// The observer of client `client`'s sends.
    pub(crate) fn new(client: u64) -> Self {
        Self {
            client,
            inner: Mutex::new(Inner::default()),
        }
    }
}

impl CallObserver for ActorFence {
    fn invoked(&self, attempt: Attempted<'_>) -> Option<u64> {
        let uuid = fence_of(attempt)?;
        let hash = match attempt {
            Attempted::Write(write) => Some(write_hash(write)),
            _ => None,
        };
        let mut inner = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        let Inner {
            deposed,
            early,
            fenced,
            next_token,
        } = &mut *inner;
        sending(deposed, early, (self.client, attempt.journal()), uuid, hash);
        let token = *next_token;
        *next_token += 1;
        fenced.insert(token, uuid);
        Some(token)
    }

    fn answered(&self, token: u64, answer: Answered<'_>) {
        let mut inner = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(uuid) = inner.fenced.remove(&token) {
            answered(&mut inner.deposed, uuid, answer);
        }
    }

    fn resolving(&self, _write: &paros::wire::Write) -> Option<u64> {
        None
    }

    fn resolved(&self, _token: u64, _report: &paros::client::ResolveReport) {}
}
