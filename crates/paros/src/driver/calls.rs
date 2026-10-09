//! The journal calls a node holds open (#204): a `Write`, a `SetLeader` or a
//! `Truncate`, each proposed into one slot and answered only once that slot
//! applies, with the verdict the journal state machine gave there
//! ([`paros_core::Outcome`]).
//!
//! The safety rule is the judgement at apply: whatever is checked before a
//! slot is proposed (a propose-time refusal is allowed as an optimisation,
//! `docs/architecture.md` §2.2), the verdict a client gets is the one the
//! fold gave its slot. This driver checks nothing early: the node proposes it
//! ([`ColocatedNode::propose_in`](paros_core::ColocatedNode::propose_in),
//! [`propose_control_in`](paros_core::ColocatedNode::propose_control_in)),
//! parks the reply on the slot, and `drain_ready` answers it from the fold.
//! A call whose slot decided another command, or that never applies here,
//! gets no verdict ([`Call::no_verdict`]) — ambiguous to the client, which
//! retries, and a retried `Write` is answered from the log itself.

use paros_core::{Command, Control, Entry, LeaderUuid, NodeId, Outcome, Seq};

use crate::audit::Audit;
use crate::hooks::{DriverHooks, Reply};
use crate::rpc::{
    ReplySender, SetLeaderAck, TruncateAck, WriteAck, WriteOutcome, journal_view_to_proto,
};

use super::config::DriverTunables;
use super::reply::answer;

/// One held journal call.
pub(crate) enum Call {
    /// A `Write`: the entry proposed, and its reply.
    Write {
        entry: Entry,
        reply: ReplySender<WriteAck>,
    },
    /// A `SetLeader`.
    SetLeader {
        new: LeaderUuid,
        old: Option<LeaderUuid>,
        reply: ReplySender<SetLeaderAck>,
    },
    /// A `Truncate`, fenced by its leader uuid (#228, #241).
    Truncate {
        leader: LeaderUuid,
        up_to: Seq,
        reply: ReplySender<TruncateAck>,
    },
}

impl Call {
    /// The command this call proposed: what its slot must have decided for
    /// the slot's verdict to be this call's.
    pub(crate) fn command(&self) -> Command {
        let command = self.command_unchecked();
        // A call proposes exactly its own kind of command.
        assert!(
            matches!(self, Call::Write { .. }) == command.write().is_some(),
            "a Write call proposes a write and nothing else does"
        );
        // The fence a Truncate carries is the one it proposes (#228).
        if let Call::Truncate { up_to, .. } = self {
            assert!(
                matches!(command, Command::Control(Control::Truncate { up_to: u, .. }) if u == *up_to),
                "a Truncate call proposes its own trim point"
            );
        }
        command
    }

    /// [`Call::command`] before its postcondition.
    fn command_unchecked(&self) -> Command {
        match self {
            Call::Write { entry, .. } => Command::Write(entry.clone()),
            Call::SetLeader { new, old, .. } => Command::Control(Control::SetLeader {
                new: *new,
                old: *old,
            }),
            Call::Truncate { leader, up_to, .. } => Command::Control(Control::Truncate {
                leader: *leader,
                up_to: *up_to,
            }),
        }
    }

    /// Answer the call with the verdict its slot applied to.
    pub(crate) fn answer<H: DriverHooks, A: Audit>(
        self,
        outcome: &Outcome,
        self_id: u64,
        hooks: &H,
        audit: &A,
    ) {
        let me = NodeId(self_id);
        // A verdict answers the call whose command its slot decided: the
        // outcome is always of the call's own kind.
        match &self {
            Call::Write { .. } => assert!(
                matches!(
                    outcome,
                    Outcome::Accepted { .. }
                        | Outcome::Duplicate { .. }
                        | Outcome::Refused(_)
                        | Outcome::Truncated(_)
                        | Outcome::WrongMode(_)
                ),
                "a Write is answered with a write's verdict"
            ),
            Call::SetLeader { .. } => assert!(
                matches!(
                    outcome,
                    Outcome::Leader(_) | Outcome::LeaderRefused(_) | Outcome::WrongMode(_)
                ),
                "a SetLeader is answered with a SetLeader's verdict"
            ),
            Call::Truncate { .. } => assert!(
                matches!(
                    outcome,
                    Outcome::Trimmed(_) | Outcome::TruncateRefused(_) | Outcome::WrongMode(_)
                ),
                "a Truncate is answered with a Truncate's verdict"
            ),
        }
        match self {
            Call::Write { reply, .. } => {
                answer(hooks, audit, me, Reply::Write, reply, write_ack(outcome));
            }
            Call::SetLeader { reply, .. } => {
                answer(
                    hooks,
                    audit,
                    me,
                    Reply::SetLeader,
                    reply,
                    set_leader_ack(outcome),
                );
            }
            Call::Truncate { reply, .. } => {
                answer(
                    hooks,
                    audit,
                    me,
                    Reply::Truncate,
                    reply,
                    truncate_ack(outcome),
                );
            }
        }
    }

    /// Answer the call with no verdict, naming `leader` as a hint.
    pub(crate) fn no_verdict<H: DriverHooks, A: Audit>(
        self,
        leader: Option<u64>,
        hooks: &H,
        audit: &A,
        self_id: u64,
    ) {
        let me = NodeId(self_id);
        match self {
            Call::Write { reply, .. } => answer(
                hooks,
                audit,
                me,
                Reply::Redirect,
                reply,
                WriteAck {
                    leader,
                    ..WriteAck::default()
                },
            ),
            Call::SetLeader { reply, .. } => answer(
                hooks,
                audit,
                me,
                Reply::Redirect,
                reply,
                SetLeaderAck {
                    leader,
                    ..SetLeaderAck::default()
                },
            ),
            Call::Truncate { reply, .. } => answer(
                hooks,
                audit,
                me,
                Reply::Redirect,
                reply,
                TruncateAck {
                    leader,
                    ..TruncateAck::default()
                },
            ),
        }
    }
}

/// The wire verdict of a `Write` whose slot applied to `outcome`.
pub(crate) fn write_ack(outcome: &Outcome) -> WriteAck {
    let ack = write_ack_unchecked(outcome);
    // An accepted or duplicate write names its records; a refusal names the
    // state it was judged against.
    if matches!(
        outcome,
        Outcome::Accepted { .. } | Outcome::Duplicate { .. }
    ) {
        assert!(ack.count > 0, "an acked write carries records");
        assert!(ack.state.is_none(), "an acked write names no refusal state");
    }
    if matches!(
        outcome,
        Outcome::Refused(_) | Outcome::Truncated(_) | Outcome::WrongMode(_)
    ) {
        assert!(
            ack.state.is_some(),
            "a refused write names the state it was judged against"
        );
    }
    // A verdict is an answer, never a redirect.
    assert!(
        ack.leader.is_none(),
        "a write verdict carries no leader hint"
    );
    ack
}

/// The edge's verdict on a `Write` batch of `records` (#241,
/// `docs/architecture.md` §2.7): `Some(TooLarge)` when it carries more
/// records or more record bytes than `tunables` allow, `None` when it may be
/// proposed. Judged before consensus, so a refused batch is in no slot.
pub(crate) fn batch_too_large(records: &[Vec<u8>], tunables: &DriverTunables) -> Option<WriteAck> {
    assert!(
        tunables.max_batch_records >= 1,
        "a one-record batch is always within the record limit"
    );
    assert!(
        tunables.max_batch_bytes >= 1,
        "the byte limit admits a non-empty record"
    );
    let count = u64::try_from(records.len()).unwrap_or(u64::MAX);
    let bytes = records
        .iter()
        .map(|record| u64::try_from(record.len()).unwrap_or(u64::MAX))
        .fold(0_u64, u64::saturating_add);
    if count <= tunables.max_batch_records && bytes <= tunables.max_batch_bytes {
        return None;
    }
    let ack = WriteAck {
        outcome: WriteOutcome::TooLarge.into(),
        max_records: tunables.max_batch_records,
        max_bytes: tunables.max_batch_bytes,
        ..WriteAck::default()
    };
    // A refusal at the edge names the limits and nothing the journal said.
    assert!(
        ack.state.is_none(),
        "an edge refusal names no journal state"
    );
    assert!(ack.count == 0, "an edge refusal acks no records");
    Some(ack)
}

/// [`write_ack`] before its postconditions.
fn write_ack_unchecked(outcome: &Outcome) -> WriteAck {
    match outcome {
        Outcome::Accepted { seq, count } => WriteAck {
            outcome: WriteOutcome::Accepted.into(),
            seq: seq.0,
            count: *count,
            ..WriteAck::default()
        },
        Outcome::Duplicate { seq, count } => WriteAck {
            outcome: WriteOutcome::Duplicate.into(),
            seq: seq.0,
            count: *count,
            ..WriteAck::default()
        },
        Outcome::Refused(state) => WriteAck {
            outcome: WriteOutcome::Refused.into(),
            state: Some(journal_view_to_proto(*state)),
            ..WriteAck::default()
        },
        Outcome::Truncated(state) => WriteAck {
            outcome: WriteOutcome::Truncated.into(),
            state: Some(journal_view_to_proto(*state)),
            ..WriteAck::default()
        },
        Outcome::WrongMode(state) => WriteAck {
            outcome: WriteOutcome::WrongMode.into(),
            state: Some(journal_view_to_proto(*state)),
            ..WriteAck::default()
        },
        // A write's slot applies to a write's verdict; anything else is no
        // verdict at all.
        _ => WriteAck::default(),
    }
}

/// The wire verdict of a `SetLeader` whose slot applied to `outcome`.
pub(crate) fn set_leader_ack(outcome: &Outcome) -> SetLeaderAck {
    let ack = set_leader_ack_unchecked(outcome);
    // Only a decided SetLeader wins, and a decided one names its state.
    if ack.won {
        assert!(ack.decided, "a won SetLeader was decided");
        assert!(!ack.wrong_mode, "a won SetLeader is of the journal's mode");
    }
    if ack.wrong_mode {
        assert!(ack.decided, "a wrong-mode SetLeader was decided");
    }
    if ack.decided {
        assert!(ack.state.is_some(), "a decided SetLeader names its state");
    } else {
        assert!(ack.state.is_none(), "an undecided SetLeader names no state");
    }
    assert!(
        ack.leader.is_none(),
        "a SetLeader verdict carries no leader hint"
    );
    ack
}

/// [`set_leader_ack`] before its postconditions.
fn set_leader_ack_unchecked(outcome: &Outcome) -> SetLeaderAck {
    match outcome {
        Outcome::Leader(state) => SetLeaderAck {
            decided: true,
            won: true,
            state: Some(journal_view_to_proto(*state)),
            ..SetLeaderAck::default()
        },
        Outcome::LeaderRefused(state) => SetLeaderAck {
            decided: true,
            won: false,
            state: Some(journal_view_to_proto(*state)),
            ..SetLeaderAck::default()
        },
        Outcome::WrongMode(state) => SetLeaderAck {
            decided: true,
            won: false,
            wrong_mode: true,
            state: Some(journal_view_to_proto(*state)),
            ..SetLeaderAck::default()
        },
        _ => SetLeaderAck::default(),
    }
}

/// The wire verdict of a `Truncate` whose slot applied to `outcome`.
pub(crate) fn truncate_ack(outcome: &Outcome) -> TruncateAck {
    let ack = truncate_ack_unchecked(outcome);
    if ack.refused {
        assert!(ack.decided, "a refused Truncate was decided");
        assert!(
            !ack.wrong_mode,
            "a fenced refusal is of the journal's mode"
        );
    }
    if ack.wrong_mode {
        assert!(ack.decided, "a wrong-mode Truncate was decided");
    }
    if ack.decided {
        assert!(ack.state.is_some(), "a decided Truncate names its state");
    } else {
        assert!(ack.state.is_none(), "an undecided Truncate names no state");
    }
    assert!(
        ack.leader.is_none(),
        "a Truncate verdict carries no leader hint"
    );
    ack
}

/// [`truncate_ack`] before its postconditions.
fn truncate_ack_unchecked(outcome: &Outcome) -> TruncateAck {
    match outcome {
        Outcome::Trimmed(state) => TruncateAck {
            decided: true,
            state: Some(journal_view_to_proto(*state)),
            ..TruncateAck::default()
        },
        Outcome::TruncateRefused(state) => TruncateAck {
            decided: true,
            refused: true,
            state: Some(journal_view_to_proto(*state)),
            ..TruncateAck::default()
        },
        Outcome::WrongMode(state) => TruncateAck {
            decided: true,
            wrong_mode: true,
            state: Some(journal_view_to_proto(*state)),
            ..TruncateAck::default()
        },
        _ => TruncateAck::default(),
    }
}

#[cfg(test)]
mod tests {
    use super::{DriverTunables, WriteOutcome, batch_too_large};

    fn limits(records: u64, bytes: u64) -> DriverTunables {
        DriverTunables {
            max_batch_records: records,
            max_batch_bytes: bytes,
            ..DriverTunables::default()
        }
    }

    #[test]
    fn a_batch_at_its_limits_passes_and_one_past_is_refused() {
        let tunables = limits(2, 8);
        assert!(batch_too_large(&[vec![0; 4], vec![0; 4]], &tunables).is_none());
        // One record too many, then one byte too many.
        let refused = [
            batch_too_large(&[vec![0; 1], vec![0; 1], vec![0; 1]], &tunables),
            batch_too_large(&[vec![0; 4], vec![0; 5]], &tunables),
        ];
        for ack in refused {
            let ack = ack.expect("over a limit is refused");
            assert_eq!(ack.outcome(), WriteOutcome::TooLarge);
            assert_eq!((ack.max_records, ack.max_bytes), (2, 8));
        }
    }
}
