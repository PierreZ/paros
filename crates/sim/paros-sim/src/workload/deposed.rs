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

#[cfg(test)]
mod tests {
    use paros::client::{Answered, Attempted, CallObserver, TruncateOutcome, WriteOutcome};
    use paros::core::Value;
    use paros::core::journal::{
        Entry, JournalId, JournalIdentifier, JournalView, LeaderUuid, Seq, TenantId,
    };

    use super::{ActorFence, fence_of, write_hash};
    use crate::audit::testing::red;

    const A: LeaderUuid = LeaderUuid(7);
    const B: LeaderUuid = LeaderUuid(9);

    fn journal() -> JournalIdentifier {
        JournalIdentifier::new(TenantId(3), JournalId(4))
    }

    fn write(uuid: LeaderUuid, seq: u64, record: &[u8]) -> paros::wire::Write {
        let entry = Entry {
            leader: uuid,
            seq: Seq(seq),
            records: vec![Value(record.to_vec())],
        };
        paros::client::write_request(journal(), &entry)
    }

    fn truncate(uuid: LeaderUuid) -> paros::wire::Truncate {
        paros::wire::Truncate {
            journal: journal().journal.0,
            tenant: journal().tenant.0,
            up_to: 1,
            leader: Some(paros::wire::leader_uuid_to_proto(uuid)),
        }
    }

    fn view(leader: Option<LeaderUuid>) -> JournalView {
        JournalView {
            leader,
            next_seq: Seq(1),
            first_seq: Seq(0),
        }
    }

    /// Send `w` through `fence` and answer it with a refusal naming `leader`.
    fn refuse(fence: &ActorFence, w: &paros::wire::Write, leader: Option<LeaderUuid>) {
        let token = fence.invoked(Attempted::Write(w)).expect("a fenced write");
        let refused = WriteOutcome::Refused {
            state: view(leader),
        };
        fence.answered(token, Answered::Write(&refused));
    }

    #[test]
    fn a_fence_is_a_set_uuid_on_a_write_or_a_truncate() {
        assert_eq!(fence_of(Attempted::Write(&write(A, 0, b"x"))), Some(A));
        assert_eq!(fence_of(Attempted::Truncate(&truncate(B))), Some(B));
        assert_eq!(
            fence_of(Attempted::Write(&write(LeaderUuid(0), 0, b"x"))),
            None
        );
        let wide = LeaderUuid((1 << 64) | 2);
        assert_eq!(
            fence_of(Attempted::Write(&write(wide, 0, b"x"))),
            Some(wide)
        );
    }

    #[test]
    fn a_write_hash_is_its_command_identity() {
        assert_eq!(
            write_hash(&write(A, 0, b"x")),
            write_hash(&write(A, 0, b"x"))
        );
        assert_ne!(
            write_hash(&write(A, 0, b"x")),
            write_hash(&write(A, 1, b"x"))
        );
        assert_ne!(
            write_hash(&write(A, 0, b"x")),
            write_hash(&write(B, 0, b"x"))
        );
        assert_ne!(
            write_hash(&write(A, 0, b"x")),
            write_hash(&write(A, 0, b"y"))
        );
    }

    #[test]
    fn writes_under_a_live_uuid_are_clean() {
        let fence = ActorFence::new(1);
        assert!(!red(|| {
            refuse(&fence, &write(A, 0, b"x"), Some(A));
            refuse(&fence, &write(A, 1, b"y"), Some(A));
            fence.invoked(Attempted::Write(&write(A, 2, b"z")));
        }));
    }

    #[test]
    fn a_write_after_a_refusal_naming_another_is_red() {
        let fence = ActorFence::new(1);
        refuse(&fence, &write(A, 0, b"x"), Some(B));
        assert!(red(|| {
            fence.invoked(Attempted::Write(&write(A, 1, b"y")));
        }));
        assert!(red(|| {
            fence.invoked(Attempted::Truncate(&truncate(A)));
        }));
        assert!(
            !red(|| {
                fence.invoked(Attempted::Write(&write(B, 1, b"y")));
            }),
            "only the deposed uuid stops"
        );
    }

    #[test]
    fn a_refusal_naming_no_leader_deposes_the_uuid() {
        let fence = ActorFence::new(1);
        refuse(&fence, &write(A, 0, b"x"), None);
        assert!(red(|| {
            fence.invoked(Attempted::Write(&write(A, 1, b"y")));
        }));
    }

    #[test]
    fn a_refused_truncate_deposes_and_an_applied_one_does_not() {
        let fence = ActorFence::new(1);
        let token = fence.invoked(Attempted::Truncate(&truncate(A))).unwrap();
        let applied = TruncateOutcome::Applied {
            state: view(Some(B)),
        };
        fence.answered(token, Answered::Truncate(&applied));
        assert!(!red(|| {
            fence.invoked(Attempted::Write(&write(A, 0, b"x")));
        }));
        let token = fence.invoked(Attempted::Truncate(&truncate(A))).unwrap();
        let refused = TruncateOutcome::Refused {
            state: view(Some(B)),
        };
        fence.answered(token, Answered::Truncate(&refused));
        assert!(red(|| {
            fence.invoked(Attempted::Write(&write(A, 1, b"x")));
        }));
    }

    #[test]
    fn a_resend_of_an_early_write_is_clean() {
        let fence = ActorFence::new(1);
        let early = write(A, 0, b"x");
        // Sent, its answer unknown, before a sibling's refusal deposes A.
        fence.invoked(Attempted::Write(&early));
        refuse(&fence, &write(A, 1, b"y"), Some(B));
        assert!(!red(|| {
            fence.invoked(Attempted::Write(&early));
        }));
        assert!(
            red(|| {
                fence.invoked(Attempted::Write(&write(A, 2, b"z")));
            }),
            "a write first sent after the refusal is new"
        );
    }

    #[test]
    fn an_unknown_token_and_an_unfenced_call_change_nothing() {
        let fence = ActorFence::new(1);
        let refused = WriteOutcome::Refused {
            state: view(Some(B)),
        };
        fence.answered(99, Answered::Write(&refused));
        assert!(!red(|| {
            fence.invoked(Attempted::Write(&write(A, 0, b"x")));
        }));
        assert_eq!(
            fence.invoked(Attempted::Write(&write(LeaderUuid(0), 0, b"x"))),
            None
        );
    }
}
