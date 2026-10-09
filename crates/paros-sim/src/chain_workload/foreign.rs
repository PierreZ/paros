//! The chain client's **cross-tenant attack** (#247): a `Write`, a
//! `Truncate` or a `SetLeader` sent under an identifier that is not its own
//! journal's (#235: every call is named by `(TenantId, JournalId)`).
//!
//! Two identifiers:
//!
//! - **another tenant's journal**, served by the same nodes (a plan journal
//!   under another tenant), for a `Write` or a `Truncate` from a client that
//!   owns its own journal: sent under this client's own fence — its
//!   generation and its owner id, which never own that journal (a client
//!   claims only its own) — it must be refused by that journal's fence,
//!   never written or applied. The write is noted as foreign on that
//!   journal's audit (`AuditWorld::note_foreign`), which holds every verdict
//!   on it to a refusal; an accepted one would also be a record no history
//!   of that journal wrote, which its linearizability search refutes. A
//!   `SetLeader` is not sent there: paros has no tenant authentication yet,
//!   and a claim against that journal's own generation is a valid claim, not
//!   an attack the journal can refuse;
//! - **an identifier nobody serves** (a claim, a client that owns nothing, a run
//!   with no other tenant): this journal's id under another tenant (the
//!   other plan tenant's when that identifier is not served, else a tenant no run
//!   draws). Every call, a `SetLeader` included, must be refused as naming an
//!   unknown journal, never answered from a journal.
//!
//! No function here draws randomness: every choice is read off the
//! caller's step draws.

use std::time::Duration;

use moonpool_sim::{SimContext, assert_always, assert_reachable};
use paros::client::{SetLeaderOutcome, TruncateOutcome, WriteOutcome, Writer, write_request};
use paros::{Command, JournalIdentifier, TenantId, Truncate, Value, leader_uuid_to_proto};

use super::rpc::within;
use crate::client::ChainClient;

/// The redirects a call to another tenant's journal follows: it starts at
/// a drawn node, which seldom leads that journal.
const REDIRECTS: usize = 3;

/// The tenant no run draws (`u64::MAX` is outside every draw, as the stray
/// read's).
const NOBODY: TenantId = TenantId(u64::MAX);

/// Send `op`'s call (`super::WRITE`, `super::TRUNCATE` or
/// `super::SET_LEADER`) under an identifier that is not `own`, asked of
/// `target` and judged as the module doc says. `journals` is the run's
/// plan.
#[tracing::instrument(level = "debug", skip_all, fields(op))]
pub(super) async fn attack(
    ctx: &SimContext,
    nodes: &ChainClient,
    (op, writer): (u8, &Writer),
    (own, journals): (JournalIdentifier, &[JournalIdentifier]),
    (target, draw): (usize, u64),
    timeout: Duration,
) {
    let other = journals
        .iter()
        .copied()
        .filter(|j| j.tenant != own.tenant)
        .nth(usize::try_from(draw % journals.len().max(1) as u64).unwrap_or(0))
        .or_else(|| journals.iter().copied().find(|j| j.tenant != own.tenant));
    // The other tenant's journal, for a write or a truncation this client
    // can fence (it leads its own journal).
    if let (Some(other), true) = (other, op != super::SET_LEADER && writer.owned().is_some()) {
        served(ctx, nodes, (op, writer), other, target, timeout).await;
        return;
    }
    let mixed = other
        .map(|o| JournalIdentifier::new(o.tenant, own.journal))
        .filter(|mixed| !journals.contains(mixed))
        .unwrap_or(JournalIdentifier::new(NOBODY, own.journal));
    unserved(ctx, nodes, (op, writer), mixed, target, timeout).await;
}

/// A write or a truncation under this client's fence, at another tenant's
/// journal: refused by its fence, never written or applied.
async fn served(
    ctx: &SimContext,
    nodes: &ChainClient,
    (op, writer): (u8, &Writer),
    other: JournalIdentifier,
    target: usize,
    timeout: Duration,
) {
    let Some(entry) = writer.entry(vec![Value(other.journal.0.to_le_bytes().to_vec())]) else {
        return;
    };
    if op == super::TRUNCATE {
        assert_reachable!("chain: a client truncates another tenant's journal under its own fence");
        let request = Truncate {
            journal: other.journal.0,
            tenant: other.tenant.0,
            up_to: entry.seq.0,
            leader: Some(leader_uuid_to_proto(entry.leader)),
        };
        let mut at = target;
        for _ in 0..REDIRECTS {
            let ask = nodes.truncate_attempt(at, request);
            let outcome = within(ctx, timeout, TruncateOutcome::Ambiguous, ask).await;
            assert_always!(
                !matches!(outcome, TruncateOutcome::Applied { .. }),
                "chain: a truncation naming another tenant's journal is never applied",
                { "journal" => other.to_string() }
            );
            match outcome {
                TruncateOutcome::Redirect {
                    leader: Some(leader),
                } => {
                    at = nodes.index_of(leader).unwrap_or(at);
                }
                TruncateOutcome::Refused { .. } => {
                    assert_reachable!(
                        "chain: another tenant's journal refuses a truncation by its fence"
                    );
                    return;
                }
                _ => return,
            }
        }
        return;
    }
    assert_reachable!("chain: a client writes to another tenant's journal under its own fence");
    // Sent to the other journal (a slot of it may hold the write), never to
    // be accepted there: its audit judges every verdict on it.
    crate::audit::audit_world_for(ctx.state(), other)
        .note_foreign(paros::command_hash(&Command::Write(entry.clone())));
    let mut at = target;
    for _ in 0..REDIRECTS {
        let ask = nodes.write_attempt(at, write_request(other, &entry), None);
        let outcome = within(ctx, timeout, WriteOutcome::Ambiguous, ask).await;
        assert_always!(
            !matches!(outcome, WriteOutcome::Written { .. }),
            "chain: a write naming another tenant's journal is never written",
            { "journal" => other.to_string() }
        );
        match outcome {
            WriteOutcome::Redirect {
                leader: Some(leader),
            } => {
                at = nodes.index_of(leader).unwrap_or(at);
            }
            WriteOutcome::Refused { .. } => {
                assert_reachable!("chain: another tenant's journal refuses a write by its fence");
                return;
            }
            _ => return,
        }
    }
}

/// Any of the three calls under an identifier nobody serves: refused as naming an
/// unknown journal, never answered from a journal.
async fn unserved(
    ctx: &SimContext,
    nodes: &ChainClient,
    (op, writer): (u8, &Writer),
    identifier: JournalIdentifier,
    target: usize,
    timeout: Duration,
) {
    assert_reachable!("chain: a client calls under an identifier nobody serves");
    let answered_from_a_journal = match op {
        super::SET_LEADER => {
            let ask = nodes.set_leader_attempt(target, identifier, writer.uuid(), writer.owned());
            matches!(
                within(ctx, timeout, SetLeaderOutcome::Ambiguous, ask).await,
                SetLeaderOutcome::Won { .. } | SetLeaderOutcome::Lost { .. }
            )
        }
        super::TRUNCATE => {
            let request = Truncate {
                journal: identifier.journal.0,
                tenant: identifier.tenant.0,
                up_to: writer.next_seq(),
                leader: Some(leader_uuid_to_proto(writer.fence())),
            };
            let ask = nodes.truncate_attempt(target, request);
            matches!(
                within(ctx, timeout, TruncateOutcome::Ambiguous, ask).await,
                TruncateOutcome::Applied { .. } | TruncateOutcome::Refused { .. }
            )
        }
        _ => {
            let entry = writer.stale_entry(vec![Value(identifier.tenant.0.to_le_bytes().to_vec())]);
            let ask = nodes.write_attempt(target, write_request(identifier, &entry), None);
            matches!(
                within(ctx, timeout, WriteOutcome::Ambiguous, ask).await,
                WriteOutcome::Written { .. }
                    | WriteOutcome::Refused { .. }
                    | WriteOutcome::Truncated { .. }
            )
        }
    };
    assert_always!(
        !answered_from_a_journal,
        "chain: a call naming an identifier nobody serves is never answered from a journal",
        { "op" => op, "identifier" => identifier.to_string() }
    );
}
