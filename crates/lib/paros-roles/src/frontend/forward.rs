//! Forwarding (#192 (the frontend)): one call to the machines and their
//! answer back, the leader hints followed here and stripped from the
//! answer, never handed to the client as a redirect.

use std::future::Future;

use moonpool_core::LocalTimeProvider;
use moonpool_rpc::{Execution, RpcError};
use paros_core::journal::{JournalId, JournalIdentifier, TenantId};
use paros_net::runtime::ParosProviders;

use super::{Denial, Draws, Operation, Shared};
use paros_client::{
    Answered, Attempted, CallObserver, Client, ReadOutcome, SetLeaderOutcome, TruncateOutcome,
    WriteOutcome,
};
use paros_net::NodeClient;
use paros_observe::audit::Audit;
use paros_wire::public::WriteOutcome as WireWriteOutcome;
use paros_wire::{
    FrontendVerdict, Read, ReadAck, SetLeader, SetLeaderAck, Truncate, TruncateAck, Write, WriteAck,
};

#[cfg(test)]
mod tests;

/// What one answer from a machine means to the forwarding loop.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Route {
    /// The answer to hand back.
    Answer,
    /// No verdict here: ask the server the hint names, or the next one.
    Redirect(Option<u64>),
    /// This server does not serve the journal: ask the next one.
    Unknown,
}

/// One of the four journal calls, as a frontend forwards it.
pub(super) trait Forwarded: Clone + Send + Sync + 'static {
    /// Its answer.
    type Ack: Send + 'static;
    /// What a token is checked for.
    const OPERATION: Operation;
    /// The name of the task that serves one.
    const TASK: &'static str;
    /// The journal the call's ids name.
    fn ids(&self) -> JournalIdentifier;
    /// Name `journal` by its ids, as the machines know it.
    fn aim(&mut self, journal: JournalIdentifier);
    /// One attempt at `node`.
    fn send<P: ParosProviders>(
        node: &NodeClient<P>,
        call: &Self,
    ) -> impl Future<Output = Result<Self::Ack, RpcError>>;
    /// What `ack` means to the loop.
    fn route(ack: &Self::Ack) -> Route;
    /// `ack` with its leader hint removed: the client never learns a node.
    fn strip(ack: Self::Ack) -> Self::Ack;
    /// The answer to a call the frontend denied.
    fn denied(denial: Denial) -> Self::Ack;
    /// The answer to a call whose name no live journal holds.
    fn unknown() -> Self::Ack;
    /// The answer to a call whose tenant lives in another cell now (#785
    /// (moved record)): nothing forwarded.
    fn moved() -> Self::Ack;
    /// The answer to a call nothing was forwarded for: no verdict.
    fn unavailable() -> Self::Ack;
    /// The answer of a stopping frontend (#560 (clean Ctrl-C shutdown)):
    /// unavailable, marked `stopping`, so the caller retries elsewhere.
    fn stopping() -> Self::Ack;
    /// The answer to a call that reached a machine and got no answer.
    fn unanswered() -> Self::Ack;
    /// Whether `ack` refuses the journal as unknown.
    fn is_unknown(ack: &Self::Ack) -> bool;
    /// Report the attempt to `observer` as it leaves.
    fn invoked(&self, observer: &dyn CallObserver) -> Option<u64>;
    /// Report the attempt `token`'s answer to `observer`, judged as the
    /// library judges it. An attempt with no answer is never reported.
    fn answered(observer: &dyn CallObserver, token: u64, ack: &Self::Ack);
}

/// The answers a frontend gives of its own for one call's answer type
/// `$ack`: denied, unknown, unavailable, stopping, and the unknown-journal
/// test, one rule for every call. With `ambiguous`, also the answer to a
/// call that may have run and got no answer (a read writes nothing, so
/// its own is "not served").
macro_rules! own_answers {
    ($ack:ident) => {
        fn denied(denial: Denial) -> $ack {
            let mut ack = $ack::default();
            ack.set_frontend(denial.verdict());
            ack
        }

        fn unknown() -> $ack {
            $ack {
                unknown_journal: true,
                ..$ack::default()
            }
        }

        fn moved() -> $ack {
            let mut ack = $ack::default();
            ack.set_frontend(FrontendVerdict::Moved);
            ack
        }

        fn unavailable() -> $ack {
            $ack::default()
        }

        fn stopping() -> $ack {
            $ack {
                stopping: true,
                ..Self::unavailable()
            }
        }

        fn is_unknown(ack: &$ack) -> bool {
            ack.unknown_journal && ack.frontend() == FrontendVerdict::None
        }
    };
    ($ack:ident, ambiguous) => {
        own_answers!($ack);

        fn unanswered() -> $ack {
            let mut ack = $ack::default();
            ack.set_frontend(FrontendVerdict::Unanswered);
            ack
        }

        fn strip(mut ack: $ack) -> $ack {
            ack.leader = None;
            ack
        }
    };
}

/// A call named by its tenant and journal ids, as every journal call is.
macro_rules! named_by_ids {
    () => {
        fn ids(&self) -> JournalIdentifier {
            JournalIdentifier::new(TenantId(self.tenant), JournalId(self.journal))
        }

        fn aim(&mut self, journal: JournalIdentifier) {
            (self.tenant, self.journal) = (journal.tenant.0, journal.journal.0);
        }
    };
}

/// The route of a decided call's answer (`Truncate`, `SetLeader`): an
/// answer once decided, else a redirect to the leader it names.
fn decided_route(unknown_journal: bool, decided: bool, leader: Option<u64>) -> Route {
    if unknown_journal {
        Route::Unknown
    } else if decided {
        Route::Answer
    } else {
        Route::Redirect(leader)
    }
}

impl Forwarded for Write {
    type Ack = WriteAck;
    const OPERATION: Operation = Operation::Write;
    const TASK: &'static str = "paros-frontend-write";

    named_by_ids!();

    fn send<P: ParosProviders>(
        node: &NodeClient<P>,
        call: &Self,
    ) -> impl Future<Output = Result<WriteAck, RpcError>> {
        node.write(call)
    }

    fn route(ack: &WriteAck) -> Route {
        if ack.unknown_journal {
            Route::Unknown
        } else if ack.outcome() == WireWriteOutcome::None {
            Route::Redirect(ack.leader)
        } else {
            Route::Answer
        }
    }

    own_answers!(WriteAck, ambiguous);

    fn invoked(&self, observer: &dyn CallObserver) -> Option<u64> {
        observer.invoked(Attempted::Write(self))
    }

    fn answered(observer: &dyn CallObserver, token: u64, ack: &WriteAck) {
        observer.answered(
            token,
            Answered::Write(&WriteOutcome::judge(&Ok(ack.clone()))),
        );
    }
}

impl Forwarded for Read {
    type Ack = ReadAck;
    const OPERATION: Operation = Operation::Read;
    const TASK: &'static str = "paros-frontend-read";

    named_by_ids!();

    fn send<P: ParosProviders>(
        node: &NodeClient<P>,
        call: &Self,
    ) -> impl Future<Output = Result<ReadAck, RpcError>> {
        node.read(call)
    }

    fn route(ack: &ReadAck) -> Route {
        if ack.unknown_journal {
            Route::Unknown
        } else if ack.served {
            Route::Answer
        } else {
            // Not confirmed in time: an honest unavailability, ask another.
            Route::Redirect(None)
        }
    }

    fn strip(ack: ReadAck) -> ReadAck {
        ack
    }

    own_answers!(ReadAck);

    fn unanswered() -> ReadAck {
        // A read changes nothing: no answer is "not served".
        ReadAck::default()
    }

    fn invoked(&self, observer: &dyn CallObserver) -> Option<u64> {
        observer.invoked(Attempted::Read(self))
    }

    fn answered(observer: &dyn CallObserver, token: u64, ack: &ReadAck) {
        observer.answered(token, Answered::Read(&ReadOutcome::judge(Ok(ack.clone()))));
    }
}

impl Forwarded for Truncate {
    type Ack = TruncateAck;
    const OPERATION: Operation = Operation::Truncate;
    const TASK: &'static str = "paros-frontend-truncate";

    named_by_ids!();

    fn send<P: ParosProviders>(
        node: &NodeClient<P>,
        call: &Self,
    ) -> impl Future<Output = Result<TruncateAck, RpcError>> {
        node.truncate(call)
    }

    fn route(ack: &TruncateAck) -> Route {
        decided_route(ack.unknown_journal, ack.decided, ack.leader)
    }

    own_answers!(TruncateAck, ambiguous);

    fn invoked(&self, observer: &dyn CallObserver) -> Option<u64> {
        observer.invoked(Attempted::Truncate(self))
    }

    fn answered(observer: &dyn CallObserver, token: u64, ack: &TruncateAck) {
        observer.answered(
            token,
            Answered::Truncate(&TruncateOutcome::judge(&Ok(ack.clone()))),
        );
    }
}

impl Forwarded for SetLeader {
    type Ack = SetLeaderAck;
    const OPERATION: Operation = Operation::SetLeader;
    const TASK: &'static str = "paros-frontend-set-leader";

    named_by_ids!();

    fn send<P: ParosProviders>(
        node: &NodeClient<P>,
        call: &Self,
    ) -> impl Future<Output = Result<SetLeaderAck, RpcError>> {
        node.set_leader(call)
    }

    fn route(ack: &SetLeaderAck) -> Route {
        decided_route(ack.unknown_journal, ack.decided, ack.leader)
    }

    own_answers!(SetLeaderAck, ambiguous);

    fn invoked(&self, observer: &dyn CallObserver) -> Option<u64> {
        observer.invoked(Attempted::SetLeader(self))
    }

    fn answered(observer: &dyn CallObserver, token: u64, ack: &SetLeaderAck) {
        observer.answered(
            token,
            Answered::SetLeader(&SetLeaderOutcome::judge(&Ok(ack.clone()))),
        );
    }
}

/// Forward `call` through `client` (its journal's own leader hint): follow
/// each redirect to the server it names, move on from a server that does
/// not serve the journal or refused the attempt unrun, and stop at the
/// first answer. An attempt that may have run and got no answer ends the
/// call unanswered (a write is never sent twice by the frontend: a
/// multi-writer append re-sent may land twice, #241). A read, which
/// changes nothing, moves on from any failure.
///
/// A leader out of the frontend's reach (a redirect names a node it does
/// not know, or the named leader refuses the connection at the address the
/// frontend learned) sends the frontend back to its cell at once, and the
/// call goes on over the cell learned again, once: a machine renamed since
/// the frontend learned the cell (#349) is where the registry names it now.
/// A second leader out of reach ends the call: the cell learned again
/// cannot reach it either. Before, the call circled between the followers
/// and the dead address until its budget ran out, past the client's own
/// timeout, so the client saw every claim ambiguous and the frontend
/// learned its cell again only after the client gave up (witness: recipe
/// 16688601762152675435 626417:9182517770549962650 on 45e0926, a created
/// journal led from a renamed machine took no write after chaos).
pub(super) async fn forward<P, A, C>(
    shared: &Shared<P, A>,
    client: &Client<P>,
    call: &C,
    draws: Draws,
) -> C::Ack
where
    P: ParosProviders,
    A: Audit,
    C: Forwarded,
{
    let mut client = client.clone();
    let mut count = client.server_count();
    assert!(count > 0, "a frontend forwards to at least one server");
    let tunables = &shared.settings.client;
    let mut target = match draws.stray {
        Some(draw) => {
            moonpool_assertions::reachable!(
                "frontend: a call is routed to a server other than the known leader"
            );
            usize::try_from(draw % count as u64).unwrap_or(0)
        }
        None => client.leader().unwrap_or(0),
    };
    // Every server once for an unknown journal or a refused attempt, plus
    // the redirects the tunables allow.
    let budget = count + usize::try_from(tunables.redirect_limit).unwrap_or(usize::MAX);
    let mut last = None;
    // Whether a server named a leader the frontend does not know, or no
    // server knew the journal: a machine admitted since the frontend
    // learned the cell may serve it (#700 (a journal with a slot on an
    // admitted machine never elects a leader)).
    let mut unknown_leader = false;
    let mut known = false;
    // The server a redirect named, and whether it refused the connection
    // at the address the frontend learned: a renamed machine (#349) keeps
    // its id and moves, and the registry names where it went.
    let mut named: Option<usize> = None;
    let mut dark_leader = false;
    let mut relearned = false;
    let time = shared.providers.time();
    for _ in 0..budget {
        let observer = client.observer();
        let token = call.invoked(observer.as_ref());
        let attempt = time
            .timeout(
                tunables.request_timeout,
                C::send(&client.node(target), call),
            )
            .await;
        if let (Some(token), Ok(Ok(ack))) = (token, &attempt) {
            C::answered(observer.as_ref(), token, ack);
        }
        let next = (target + 1) % count;
        if let Ok(Err(error)) = &attempt {
            stale_reference(shared, error, draws);
        }
        // The leader, by id, this attempt found out of the frontend's reach.
        let mut out_of_reach = None;
        match attempt {
            Ok(Ok(ack)) => match C::route(&ack) {
                Route::Answer => {
                    if C::OPERATION.writes() {
                        client.observe_leader_at(target);
                    }
                    return C::strip(ack);
                }
                Route::Redirect(hint) => {
                    known = true;
                    let unknown = hint.is_some_and(|id| client.index_of(id).is_none());
                    unknown_leader |= unknown;
                    client.observe_leader(hint);
                    target = client.leader().filter(|l| *l != target).unwrap_or(next);
                    named = hint.and_then(|id| client.index_of(id));
                    last = Some(ack);
                    out_of_reach = hint.filter(|_| unknown);
                }
                Route::Unknown => {
                    target = next;
                    last = Some(ack);
                }
            },
            Ok(Err(error))
                if !C::OPERATION.writes() || error.execution() == Execution::NotAdmitted =>
            {
                let dark = named == Some(target) && error.execution() == Execution::NotAdmitted;
                dark_leader |= dark;
                out_of_reach = dark.then(|| client.id_of(target));
                target = next;
            }
            Ok(Err(_)) | Err(_) if !C::OPERATION.writes() => target = next,
            Ok(Err(_)) | Err(_) => {
                moonpool_assertions::reachable!(
                    "frontend: a forwarded call goes unanswered and is answered ambiguous"
                );
                return C::unanswered();
            }
        }
        if let Some(leader) = out_of_reach {
            let Some(again) = relearn(shared, call, leader, &mut relearned).await else {
                break;
            };
            (client, count, target, named) = again;
        }
    }
    if last.is_none() {
        // No server answered: every attempt was refused unrun or failed to
        // connect. The cell the frontend learned moved under it whole, as a
        // cell of one initial member that re-registered under a new name
        // (#349) does (seed 9453986614175781425 on f6e1337: the frontends
        // kept dialing the dead address for the whole tail).
        moonpool_assertions::reachable!(
            "frontend: a cell no server answers for sends the frontend back to its cell"
        );
    }
    if back_to_cell(unknown_leader, dark_leader, known) {
        // The next call learns the cell, and its admitted machines, again,
        // each at the address it registered last.
        moonpool_assertions::reachable!(
            "frontend: a journal out of the frontend's reach sends it back to its cell"
        );
        super::lock(&shared.routes).stale_cell();
    }
    last.map_or_else(C::unavailable, C::strip)
}

/// What a call goes on with once `leader` was out of its reach and it
/// learned the cell again: `call`'s journal client over the cell learned
/// again, its server count, the server to ask next (`leader`, where the new
/// cell knows it) and that server's index as a named leader. `None` when the call already
/// learned the cell once (`relearned`), or the cell cannot be learned now:
/// the call ends, and the next call learns the cell again.
async fn relearn<P, A, C>(
    shared: &Shared<P, A>,
    call: &C,
    leader: u64,
    relearned: &mut bool,
) -> Option<(Client<P>, usize, usize, Option<usize>)>
where
    P: ParosProviders,
    A: Audit,
    C: Forwarded,
{
    if *relearned {
        return None;
    }
    *relearned = true;
    moonpool_assertions::reachable!("frontend: a leader out of reach learns the cell again");
    super::lock(&shared.routes).stale_cell();
    let client = super::routes::client_for(shared, call.ids()).await?;
    let count = client.server_count();
    assert!(count > 0, "a cell learned again holds a server");
    let named = client.index_of(leader);
    if named.is_some() {
        moonpool_assertions::reachable!(
            "frontend: a cell learned again names the leader out of reach"
        );
    }
    client.observe_leader(Some(leader));
    let target = named.or_else(|| client.leader()).unwrap_or(0);
    assert!(target < count, "the next server is one of the cell's");
    Some((client, count, target, named))
}

/// The server refused `error`'s attempt through a machine interface it no
/// longer serves (#404 (static vs dynamic endpoints)): it restarted since
/// the frontend learned the cell, and the attempt was refused unrun. The
/// rest of the call keeps its client; the next call learns the cell again,
/// unless a BUGGIFY decision keeps it one more call, so the stale path runs
/// again.
fn stale_reference<P: ParosProviders, A>(shared: &Shared<P, A>, error: &RpcError, draws: Draws) {
    if !error.is_terminal_for_reference() {
        return;
    }
    if draws.keep_stale {
        moonpool_assertions::reachable!(
            "frontend: a stale machine interface is kept one more call"
        );
    } else {
        super::lock(&shared.routes).stale_cell();
    }
}

/// Whether a call that found no answer sends the frontend back to its cell:
/// a server named a leader the frontend does not know (`unknown_leader`), the
/// named leader refused the connection (`dark_leader`), or no server knew the
/// journal (`known` is false): one answered so, or none answered at all. A
/// call whose servers only redirected keeps the cell: the leader it named is
/// known and reachable.
fn back_to_cell(unknown_leader: bool, dark_leader: bool, known: bool) -> bool {
    unknown_leader || dark_leader || !known
}
