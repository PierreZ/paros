//! The group-commit batch (#613 (settle off the node loop)): what ends a batch, the inputs held while it flies, and the calls parked on their slot.

use paros_core::{ColocatedNode, NodeId, NodeRole, ProposeResult};
use paros_net::runtime::ParosProviders;
use paros_observe::audit::Audit;

use super::calls::Call;
use super::flight::Input;
use super::journals::JournalRt;
use crate::node::shared::Shared;

/// What the per-batch observers read of a core (#613 (settle off the node
/// loop)): an input that moves any of it ends a group-commit batch
/// ([`Shared::step_backlog`]). The role, ballot and leader (`maintain`'s
/// role report and election clock), the open matchmaking campaign and
/// membership probe (`surface_matchmaking`), and the counters `maintain`
/// reports under the core's ballot. Inputs that move none of it (the
/// steady traffic of one leadership) still share one flush.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Phase {
    pub(crate) role: NodeRole,
    pub(crate) ballot: paros_core::Ballot,
    pub(crate) leader: Option<NodeId>,
    pub(crate) campaign: Option<paros_core::Ballot>,
    pub(crate) probe: Option<paros_core::Ballot>,
    pub(crate) reasks: u64,
    pub(crate) quorum_lost: u64,
    pub(crate) watermark_fills: u64,
    pub(crate) repair: paros_core::RepairCounters,
    pub(crate) handoff: paros_core::HandoffCounters,
    pub(crate) generation: u64,
    pub(crate) needs_timeout: bool,
}

impl Phase {
    pub(crate) fn of(node: &ColocatedNode) -> Self {
        let phase = Self {
            role: node.role(),
            ballot: node.ballot(),
            leader: node.leader(),
            campaign: node
                .matchmaking_role()
                .map(paros_core::matchmaking::Matchmaking::ballot),
            probe: node
                .membership_probe()
                .map(paros_core::matchmaking::MembershipProbe::ballot),
            reasks: node.counters().matchmaking_timeouts,
            quorum_lost: node.counters().quorum_lost_step_downs,
            watermark_fills: node.counters().watermark_fills,
            repair: node.counters().repair,
            handoff: node.counters().handoff,
            generation: node.matchmaker_set().map_or(0, |set| set.generation.0),
            needs_timeout: node.needs_election_timeout(),
        };
        // A probe and a campaign never coexist (`surface_matchmaking`).
        assert!(
            phase.campaign.is_none() || phase.probe.is_none(),
            "a phase holds a campaign or a probe, never both"
        );
        phase
    }
}

/// Hold `input` back while its journal's batch flies (#613 (settle off the
/// node loop)): in arrival order, except that a beat replaces the one
/// already waiting (a slow node's clock, never a burst of ticks).
pub(crate) fn hold(backlog: &mut Vec<Input>, input: Input) {
    let before = backlog.len();
    if let Input::Tick(ticks) = input
        && let Some(waiting) = backlog.iter_mut().find_map(|held| match held {
            Input::Tick(at) => Some(at),
            _ => None,
        })
    {
        assert!(*waiting <= ticks, "a later beat replaces an earlier one");
        *waiting = ticks;
        assert!(backlog.len() == before, "a replaced beat holds no more");
        return;
    }
    backlog.push(input);
    assert!(
        backlog.len() == before + 1,
        "a held input waits in the backlog"
    );
}

/// Park `call` on the slot its proposal took (`result`), with the ballot
/// it was proposed under, to be answered when that slot applies, or
/// redirect it at once when this node does not lead.
pub(crate) fn park_call<S, A: Audit, P: ParosProviders>(
    rt: &mut JournalRt<S, A>,
    result: ProposeResult,
    call: Call,
    shared: &Shared<'_, P>,
) {
    match result {
        // A lost redirect is a legal outcome: the client's deadline turns it
        // into a retry elsewhere.
        ProposeResult::NotLeader(hint) => {
            call.no_verdict(hint.map(|n| n.0), &rt.audit, shared.self_id);
        }
        ProposeResult::Accepted(slot) => {
            // A held call waits on a slot this leader just allocated.
            assert!(
                slot < rt.node.proposer().next_slot(),
                "a call waits on an allocated slot"
            );
            // The proposal's ballot: only a decision at it is this call's
            // (`answer_applied_calls`).
            let ballot = rt.node.ballot();
            assert!(rt.node.is_leader(), "only a leader parks a call");
            rt.waiters
                .pending
                .entry(slot)
                .or_default()
                .push((ballot, call));
            assert!(
                rt.waiters.pending.contains_key(&slot),
                "a held call is parked on its slot"
            );
        }
    }
}
