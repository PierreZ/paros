//! Forwarding (#192 (the frontend)): one call to the machines and their
//! answer back, the leader hints followed here and stripped from the
//! answer, never handed to the client as a redirect.

use std::future::Future;

use moonpool_core::{Providers, TimeProvider};
use moonpool_rpc::{Execution, RpcError};
use paros_core::{JournalId, JournalIdentifier, TenantId};

use super::{Denial, Draws, Operation, Shared};
use crate::client::{
    Answered, Attempted, CallObserver, Client, ReadOutcome, SetLeaderOutcome, TruncateOutcome,
    WriteOutcome,
};
use crate::rpc::public::WriteOutcome as WireWriteOutcome;
use crate::rpc::{
    FrontendVerdict, NodeClient, Read, ReadAck, SetLeader, SetLeaderAck, Truncate, TruncateAck,
    Write, WriteAck,
};

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
    fn send<P: Providers>(
        node: &NodeClient<P>,
        call: &Self,
    ) -> impl Future<Output = Result<Self::Ack, RpcError>> + Send;
    /// What `ack` means to the loop.
    fn route(ack: &Self::Ack) -> Route;
    /// `ack` with its leader hint removed: the client never learns a node.
    fn strip(ack: Self::Ack) -> Self::Ack;
    /// The answer to a call the frontend denied.
    fn denied(denial: Denial) -> Self::Ack;
    /// The answer to a call whose name no live journal holds.
    fn unknown() -> Self::Ack;
    /// The answer to a call nothing was forwarded for: no verdict.
    fn unavailable() -> Self::Ack;
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

impl Forwarded for Write {
    type Ack = WriteAck;
    const OPERATION: Operation = Operation::Write;
    const TASK: &'static str = "paros-frontend-write";

    fn ids(&self) -> JournalIdentifier {
        JournalIdentifier::new(TenantId(self.tenant), JournalId(self.journal))
    }

    fn aim(&mut self, journal: JournalIdentifier) {
        (self.tenant, self.journal) = (journal.tenant.0, journal.journal.0);
    }

    fn send<P: Providers>(
        node: &NodeClient<P>,
        call: &Self,
    ) -> impl Future<Output = Result<WriteAck, RpcError>> + Send {
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

    fn strip(mut ack: WriteAck) -> WriteAck {
        ack.leader = None;
        ack
    }

    fn denied(denial: Denial) -> WriteAck {
        let mut ack = WriteAck::default();
        ack.set_frontend(denial.verdict());
        ack
    }

    fn unknown() -> WriteAck {
        WriteAck {
            unknown_journal: true,
            ..WriteAck::default()
        }
    }

    fn unavailable() -> WriteAck {
        WriteAck::default()
    }

    fn unanswered() -> WriteAck {
        let mut ack = WriteAck::default();
        ack.set_frontend(FrontendVerdict::Unanswered);
        ack
    }

    fn is_unknown(ack: &WriteAck) -> bool {
        ack.unknown_journal && ack.frontend() == FrontendVerdict::None
    }

    fn invoked(&self, observer: &dyn CallObserver) -> Option<u64> {
        observer.invoked(Attempted::Write(self))
    }

    fn answered(observer: &dyn CallObserver, token: u64, ack: &WriteAck) {
        observer.answered(token, Answered::Write(&WriteOutcome::judge(&Ok(*ack))));
    }
}

impl Forwarded for Read {
    type Ack = ReadAck;
    const OPERATION: Operation = Operation::Read;
    const TASK: &'static str = "paros-frontend-read";

    fn ids(&self) -> JournalIdentifier {
        JournalIdentifier::new(TenantId(self.tenant), JournalId(self.journal))
    }

    fn aim(&mut self, journal: JournalIdentifier) {
        (self.tenant, self.journal) = (journal.tenant.0, journal.journal.0);
    }

    fn send<P: Providers>(
        node: &NodeClient<P>,
        call: &Self,
    ) -> impl Future<Output = Result<ReadAck, RpcError>> + Send {
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

    fn denied(denial: Denial) -> ReadAck {
        let mut ack = ReadAck::default();
        ack.set_frontend(denial.verdict());
        ack
    }

    fn unknown() -> ReadAck {
        ReadAck {
            unknown_journal: true,
            ..ReadAck::default()
        }
    }

    fn unavailable() -> ReadAck {
        ReadAck::default()
    }

    fn unanswered() -> ReadAck {
        // A read changes nothing: no answer is "not served".
        ReadAck::default()
    }

    fn is_unknown(ack: &ReadAck) -> bool {
        ack.unknown_journal && ack.frontend() == FrontendVerdict::None
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

    fn ids(&self) -> JournalIdentifier {
        JournalIdentifier::new(TenantId(self.tenant), JournalId(self.journal))
    }

    fn aim(&mut self, journal: JournalIdentifier) {
        (self.tenant, self.journal) = (journal.tenant.0, journal.journal.0);
    }

    fn send<P: Providers>(
        node: &NodeClient<P>,
        call: &Self,
    ) -> impl Future<Output = Result<TruncateAck, RpcError>> + Send {
        node.truncate(call)
    }

    fn route(ack: &TruncateAck) -> Route {
        if ack.unknown_journal {
            Route::Unknown
        } else if ack.decided {
            Route::Answer
        } else {
            Route::Redirect(ack.leader)
        }
    }

    fn strip(mut ack: TruncateAck) -> TruncateAck {
        ack.leader = None;
        ack
    }

    fn denied(denial: Denial) -> TruncateAck {
        let mut ack = TruncateAck::default();
        ack.set_frontend(denial.verdict());
        ack
    }

    fn unknown() -> TruncateAck {
        TruncateAck {
            unknown_journal: true,
            ..TruncateAck::default()
        }
    }

    fn unavailable() -> TruncateAck {
        TruncateAck::default()
    }

    fn unanswered() -> TruncateAck {
        let mut ack = TruncateAck::default();
        ack.set_frontend(FrontendVerdict::Unanswered);
        ack
    }

    fn is_unknown(ack: &TruncateAck) -> bool {
        ack.unknown_journal && ack.frontend() == FrontendVerdict::None
    }

    fn invoked(&self, observer: &dyn CallObserver) -> Option<u64> {
        observer.invoked(Attempted::Truncate(self))
    }

    fn answered(observer: &dyn CallObserver, token: u64, ack: &TruncateAck) {
        observer.answered(
            token,
            Answered::Truncate(&TruncateOutcome::judge(&Ok(*ack))),
        );
    }
}

impl Forwarded for SetLeader {
    type Ack = SetLeaderAck;
    const OPERATION: Operation = Operation::SetLeader;
    const TASK: &'static str = "paros-frontend-set-leader";

    fn ids(&self) -> JournalIdentifier {
        JournalIdentifier::new(TenantId(self.tenant), JournalId(self.journal))
    }

    fn aim(&mut self, journal: JournalIdentifier) {
        (self.tenant, self.journal) = (journal.tenant.0, journal.journal.0);
    }

    fn send<P: Providers>(
        node: &NodeClient<P>,
        call: &Self,
    ) -> impl Future<Output = Result<SetLeaderAck, RpcError>> + Send {
        node.set_leader(call)
    }

    fn route(ack: &SetLeaderAck) -> Route {
        if ack.unknown_journal {
            Route::Unknown
        } else if ack.decided {
            Route::Answer
        } else {
            Route::Redirect(ack.leader)
        }
    }

    fn strip(mut ack: SetLeaderAck) -> SetLeaderAck {
        ack.leader = None;
        ack
    }

    fn denied(denial: Denial) -> SetLeaderAck {
        let mut ack = SetLeaderAck::default();
        ack.set_frontend(denial.verdict());
        ack
    }

    fn unknown() -> SetLeaderAck {
        SetLeaderAck {
            unknown_journal: true,
            ..SetLeaderAck::default()
        }
    }

    fn unavailable() -> SetLeaderAck {
        SetLeaderAck::default()
    }

    fn unanswered() -> SetLeaderAck {
        let mut ack = SetLeaderAck::default();
        ack.set_frontend(FrontendVerdict::Unanswered);
        ack
    }

    fn is_unknown(ack: &SetLeaderAck) -> bool {
        ack.unknown_journal && ack.frontend() == FrontendVerdict::None
    }

    fn invoked(&self, observer: &dyn CallObserver) -> Option<u64> {
        observer.invoked(Attempted::SetLeader(self))
    }

    fn answered(observer: &dyn CallObserver, token: u64, ack: &SetLeaderAck) {
        observer.answered(
            token,
            Answered::SetLeader(&SetLeaderOutcome::judge(&Ok(*ack))),
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
pub(super) async fn forward<P, A, C>(
    shared: &Shared<P, A>,
    client: &Client<P>,
    call: &C,
    draws: Draws,
) -> C::Ack
where
    P: Providers,
    C: Forwarded,
{
    let count = client.server_count();
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
    let time = shared.providers.time();
    for _ in 0..budget {
        let observer = client.observer();
        let token = call.invoked(observer.as_ref());
        let attempt = time
            .timeout(tunables.request_timeout, C::send(client.node(target), call))
            .await;
        if let (Some(token), Ok(Ok(ack))) = (token, &attempt) {
            C::answered(observer.as_ref(), token, ack);
        }
        let next = (target + 1) % count;
        match attempt {
            Ok(Ok(ack)) => match C::route(&ack) {
                Route::Answer => {
                    if C::OPERATION.writes() {
                        client.observe_leader_at(target);
                    }
                    return C::strip(ack);
                }
                Route::Redirect(hint) => {
                    client.observe_leader(hint);
                    target = client.leader().filter(|l| *l != target).unwrap_or(next);
                    last = Some(ack);
                }
                Route::Unknown => {
                    target = next;
                    last = Some(ack);
                }
            },
            Ok(Err(error))
                if !C::OPERATION.writes() || error.execution() == Execution::NotAdmitted =>
            {
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
    }
    last.map_or_else(C::unavailable, C::strip)
}
