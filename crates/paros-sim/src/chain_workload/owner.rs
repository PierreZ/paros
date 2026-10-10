//! An owner's own misbehaviours on a single-writer journal (#339): the
//! calls the journal must refuse even from the writer in force.
//!
//! - **A claim of its own term**: `SetLeader(L, Some(L))` from the owner
//!   that leads with `L`. The compare-and-set names the leader in force, but
//!   the current leader never wins its own term again: refused.
//! - **A write ahead of the journal**: the owner's uuid at a position past
//!   `next_seq`. Accepted only at the position it names, so refused.
//!
//! Each is its own BUGGIFY location in the chain workload, and each goes
//! through the library client, so the linearizability search judges it.
//! The journal model judges the verdicts every node reached
//! (`audit::journal_model`).

use std::time::Duration;

use moonpool_sim::{SimContext, assert_always, assert_reachable};
use paros::client::{SetLeaderOutcome, WriteOutcome, Writer};
use paros::{Command, Entry, JournalIdentifier, Seq, Value, command_hash};

use super::rpc::{set_leader_once, within, write_once};
use crate::audit::AuditWorld;
use crate::client::ChainClient;

/// How far past the owner's next position the ahead write names: no run
/// writes this many records, so the write never lands on its own position
/// later (a write that did would be honest, but no longer the misbehaviour).
const AHEAD_GAP: u64 = 1 << 20;

/// The owner claims the term it leads (#339): `SetLeader(L, Some(L))`,
/// refused, or no verdict. Sent to the believed leader, and once more to
/// the leader a redirect names. A refusal names the state, which the
/// writer learns from.
#[tracing::instrument(level = "debug", skip_all, fields(journal = %journal))]
pub(super) async fn own_term_refused(
    ctx: &SimContext,
    nodes: &ChainClient,
    (journal, writer): (JournalIdentifier, &mut Writer),
    target: usize,
    timeout: Duration,
) {
    let Some(leader) = writer.owned() else {
        return;
    };
    let send = |target: usize| {
        within(
            ctx,
            timeout,
            SetLeaderOutcome::Ambiguous,
            set_leader_once(nodes, journal, target, (leader, Some(leader)), false),
        )
    };
    let mut outcome = send(nodes.leader().unwrap_or(target)).await;
    if let SetLeaderOutcome::Redirect { leader: Some(node) } = outcome
        && let Some(target) = nodes.index_of(node)
    {
        outcome = send(target).await;
    }
    assert_always!(
        !matches!(outcome, SetLeaderOutcome::Won { .. }),
        "chain: the current leader never wins its own term again"
    );
    if let SetLeaderOutcome::Lost { state } = &outcome {
        if state.leader == Some(leader) {
            assert_reachable!("chain: a claim of the owner's own term is refused");
        }
        writer.learn(state);
    }
}

/// The owner writes ahead of the journal (#339): its uuid at a position
/// past its next one, refused, or no verdict. Sent to the believed leader,
/// and once more to the leader a redirect names. A refusal names the
/// state, which the writer learns from.
#[tracing::instrument(level = "debug", skip_all, fields(journal = %journal))]
pub(super) async fn ahead_write_refused(
    ctx: &SimContext,
    (nodes, audit): (&ChainClient, &AuditWorld),
    (journal, writer): (JournalIdentifier, &mut Writer),
    (target, draw): (usize, u64),
    timeout: Duration,
) {
    let Some(leader) = writer.owned() else {
        return;
    };
    let seq = writer.next_seq() + 1 + AHEAD_GAP + draw % 1024;
    let entry = Entry {
        leader,
        seq: Seq(seq),
        records: vec![Value(b"ahead".to_vec())],
    };
    // Sent to this journal: a slot of it may hold the write (refused).
    audit.note_appended(command_hash(&Command::Write(entry.clone())));
    let send = |target: usize| {
        within(
            ctx,
            timeout,
            WriteOutcome::Ambiguous,
            write_once(nodes, journal, target, &entry, false, false),
        )
    };
    let mut outcome = send(nodes.leader().unwrap_or(target)).await;
    if let WriteOutcome::Redirect { leader: Some(node) } = outcome
        && let Some(target) = nodes.index_of(node)
    {
        outcome = send(target).await;
    }
    assert_always!(
        !matches!(outcome, WriteOutcome::Written { .. }),
        "chain: a write ahead of the journal is never accepted",
        { "seq" => seq }
    );
    if let WriteOutcome::Refused { state } = &outcome {
        if state.leader == Some(leader) {
            assert_reachable!("chain: an owner's write ahead of the journal is refused");
        }
        writer.learn(state);
    }
}
