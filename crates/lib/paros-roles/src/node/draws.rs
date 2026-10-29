//! The choices the node loop draws: a contested overwrite, an early handover abandon, a handoff, a delegation, a stretched tick; and the receive trace.

use std::collections::BTreeSet;

use paros_core::matchmaker::ReconfigurerPhase;
use paros_core::membership::ProxyId;
use paros_core::node::{Delegation, NodeRole};
use paros_core::types::Fingerprint;
use paros_core::{ColocatedNode, Message, Slot};
use paros_observe::events::{message_kind, message_route};

use super::report::HandoffContext;

/// Campaign before an `Accept` overwrites a different value this node
/// accepted at a lower ballot, or not (#376). The `Accept` proves that a
/// later leadership chose another value for the slot without this node's
/// copy. Campaigning first is a rare-but-valid choice (any node may campaign
/// at any time, at any fresh round; `ColocatedNode::campaign_above`): the raised promise refuses
/// the `Accept`, and the campaign's Phase 1 reads both values for the slot,
/// this node's own and the later one, where P2c must keep the higher ballot
/// (the gate "phase1: a promise quorum reports two different values for one
/// slot and P2c keeps the higher ballot"). Without it the `Accept` erases
/// the lower copy before any Phase 1 reads it, and the gate fired on about
/// 0.25% of seeds.
///
/// A plain choice, not a disruptive one: it costs one election, and only
/// when two values for one slot already exist, so it stays on in the
/// recovery tail, where most of a run's Phase-2 traffic is.
///
/// One contest per `(slot, held value, incoming value)` (#396). Under a
/// grid the campaign's Phase 1 closes on the contester's row, which need
/// not hold the other column-mate's value: P2c then keeps the contester's
/// own value, and the other column-mate contests back. Unbounded, two
/// column-mates of a `Grid{2,2}` dueled from round 12 to round 1652 and the
/// slot never chose (a liveness loss; safety held). The second overwriting
/// `Accept` of the same pair is taken as plain Paxos takes it.
pub(crate) fn contest_overwrite(
    node: &mut ColocatedNode,
    contested: &mut BTreeSet<(Slot, u64, u64)>,
    msg: &Message,
    self_id: u64,
) {
    let Message::Accept {
        ballot,
        slot,
        command,
        ..
    } = msg
    else {
        return;
    };
    if node.role() == NodeRole::Leader {
        return;
    }
    let Some(key) = node
        .acceptor()
        .record(*slot)
        .filter(|(held, value)| held < ballot && value != command)
        .map(|(_, value)| (*slot, value.fingerprint(), command.fingerprint()))
    else {
        return;
    };
    if contested.contains(&key) {
        moonpool_assertions::reachable!("an acceptor declines a repeat contest of one overwrite");
        tracing::info!(
            node = self_id,
            slot = slot.0,
            round = ballot.round,
            "overwrite_contest_declined"
        );
        return;
    }
    if !moonpool_buggify::buggify_with_prob!(1.0) {
        return;
    }
    let floor = node.acceptor().first_slot();
    contested.retain(|(contested_slot, _, _)| *contested_slot >= floor);
    contested.insert(key);
    assert!(
        contested.contains(&key),
        "a contest is recorded before it runs"
    );
    moonpool_assertions::reachable!(
        "an acceptor campaigns before an Accept overwrites a different value"
    );
    tracing::info!(
        node = self_id,
        slot = slot.0,
        round = ballot.round,
        "overwrite_contested"
    );
    let promised = node.acceptor().promised();
    node.campaign_above(*ballot);
    assert!(
        node.acceptor().promised() >= promised,
        "a contest never lowers the promise"
    );
}

/// Surface a peer message's arrival (mirror of `msg_sent`), so a human reading
/// the trace can pair sends with receives and spot the unmatched ones as
/// network drops.
pub(crate) fn trace_received(self_id: u64, msg: &Message) {
    let kind = message_kind(msg);
    match message_route(msg) {
        Some((from, ballot, Some(slot))) => tracing::info!(
            node = self_id,
            from = %from,
            kind,
            bround = ballot.round,
            bnode = ballot.node.0,
            slot = slot.0,
            "msg_received"
        ),
        // The empty-prefix beat: no slot field, mirroring `msg_sent`.
        Some((from, ballot, None)) => tracing::info!(
            node = self_id,
            from = %from,
            kind,
            bround = ballot.round,
            bnode = ballot.node.0,
            "msg_received"
        ),
        None => tracing::info!(node = self_id, kind, "msg_received"),
    }
}

/// Whether to give up the running handover early, on the node loop. One
/// location per phase, one macro line per arm: a seed that abandons freezes
/// readily must be able to leave decrees alone. `Publishing` is absent: the
/// successor is already chosen there, so giving up loses nothing a
/// straggler's republication does not already cover.
///
/// Always safe: the reconfigurer holds no durable state, and the freeze, the
/// bootstrap and the votes are idempotent or durable elsewhere. It puts a
/// second reconfigurer on a half-replaced generation. Silent in the recovery
/// tail. The freeze and the decree last a few ticks each, so their rates are
/// far above the bootstrap's (#484 (silent driver fault sites)): at 0.02 the
/// freeze's fired on 5 of 810 hunt seeds and the decree's on none; at 0.25,
/// with the early-handover scenario on twice as many seeds, on 15 and 6 of
/// 1,000.
pub(crate) fn abandon_early(phase: &ReconfigurerPhase) -> bool {
    let fired = match phase {
        ReconfigurerPhase::Stopping { .. } => moonpool_buggify::buggify_fault_with_prob!(0.25),
        ReconfigurerPhase::Bootstrapping { .. } => {
            moonpool_buggify::buggify_fault_with_prob!(0.02)
        }
        ReconfigurerPhase::Deciding { .. } => moonpool_buggify::buggify_fault_with_prob!(0.25),
        ReconfigurerPhase::Idle | ReconfigurerPhase::Publishing { .. } => false,
    };
    if fired {
        // BUGGIFY pairing, one gate per location.
        match phase {
            ReconfigurerPhase::Stopping { .. } => moonpool_assertions::reachable!(
                "generation: a handover is abandoned while freezing"
            ),
            ReconfigurerPhase::Bootstrapping { .. } => moonpool_assertions::reachable!(
                "generation: a handover is abandoned while bootstrapping"
            ),
            ReconfigurerPhase::Deciding { .. } => {
                moonpool_assertions::reachable!("generation: a handover is abandoned mid-decree");
            }
            ReconfigurerPhase::Idle | ReconfigurerPhase::Publishing { .. } => {}
        }
    }
    fired
}

/// Whether a transferable leadership hands itself off on this tick. One
/// location per shape of transfer, biased toward the hard states: a handoff
/// that carries unfinished business (an accepted-but-unchosen tail, or a
/// leader still healing a hole) fires an order of magnitude more often than
/// the clean one, which stays armed because it is the common production
/// shape.
///
/// The rates stay near the resignation site's (0.004), and that ceiling
/// matters. A handoff replaces an election, so a high rate makes it the
/// main way leadership moves and starves every campaign that needs a
/// settled cell. `healing` is true for any leader that holds a pipelined
/// slot decided out of order, an ordinary streaming state: at 0.30 it moved
/// leadership every few ticks. Always safe: a failed handoff costs
/// availability, never safety. Silent in the recovery tail.
pub(crate) fn initiate_handoff(ctx: &HandoffContext) -> bool {
    let fired = if ctx.healing {
        moonpool_buggify::buggify_fault_with_prob!(0.03)
    } else if !ctx.settled {
        moonpool_buggify::buggify_fault_with_prob!(0.02)
    } else {
        moonpool_buggify::buggify_fault_with_prob!(0.002)
    };
    // The handoff-storm scenario's location (#484 (silent driver fault
    // sites)): drawn after the shape's own, so the shape keeps its gate.
    let fired = fired || paros_observe::scenario::hand_off();
    if fired {
        // BUGGIFY pairing: each shape fires on some seed, split so that
        // saturation cannot hide one shape behind another's samples.
        if ctx.healing {
            moonpool_assertions::reachable!(
                "a handoff leaves a leader that is still healing a hole"
            );
        } else if ctx.settled {
            moonpool_assertions::reachable!("a handoff leaves a fully settled leader");
        } else {
            moonpool_assertions::reachable!("a handoff carries an accepted-but-unchosen tail");
        }
    }
    fired
}

/// The driver's delegation choice for the proposal about to open (#142):
/// drawn only where it can have an effect (this node leads a deployment
/// with proxies), on the node loop. First "run it colocated?", then "which
/// proxy?", each its own location; [`Delegation::Auto`] otherwise, which is
/// the core's own rule.
///
/// The count is the configuration's in force (#754 (counts off the format
/// marker)), and the driver resolves a proxy rank only to a lane it holds
/// (`reachable`): a round whose proxy has no lane here runs colocated, so a
/// count in force before its proxy leaders are served never stalls a write.
pub(crate) fn delegation_choice(
    node: &ColocatedNode,
    reachable: impl Fn(ProxyId) -> bool,
) -> Delegation {
    let count = node.acceptors().proxy_count();
    if count == 0 || !node.is_leader() {
        return Delegation::Auto;
    }
    let next = ProxyId::of(node.proposer().next_slot(), count).expect("a count names a proxy");
    if !reachable(next) {
        moonpool_assertions::reachable!("proxy: a round whose proxy has no lane runs colocated");
        return Delegation::Colocated;
    }
    // The round runs colocated instead, so a proxied deployment's log mixes
    // proxied and colocated slots and the two Phase-2 paths interleave in
    // one leadership. Shy, so most slots still go through a proxy.
    if moonpool_buggify::buggify_fault_with_prob!(0.10) {
        moonpool_assertions::reachable!(
            "proxy: the driver runs a round colocated on a proxied deployment"
        );
        return Delegation::Colocated;
    }
    // The next proxy the modulus would not pick, `(slot + 1) % count`: so
    // consecutive slots land on one proxy, and a handoff successor's
    // re-delegation (which derives `slot % count` afresh) lands on another
    // proxy. Two proxies then fan out the same round: the P2b idempotency
    // the delegation rests on.
    let modulus = u64::try_from(count).unwrap_or(u64::MAX);
    let other = ProxyId(node.proposer().next_slot().0.wrapping_add(1) % modulus);
    let choice =
        if count >= 2 && reachable(other) && moonpool_buggify::buggify_fault_with_prob!(0.10) {
            moonpool_assertions::reachable!("proxy: the driver overrides a round's proxy");
            Delegation::To(other)
        } else {
            Delegation::Auto
        };
    // A named proxy is one the deployment has, chosen by its leader.
    if let Delegation::To(proxy) = choice {
        assert!(proxy.is_in(count), "a delegation names a deployed proxy");
        assert!(node.is_leader(), "only a leader delegates");
    }
    choice
}

/// Whether the next tick waits twice the normal interval. Always safe: every
/// timeout the core owns counts ticks, so a node that ticks slower is a slow
/// node, which the cell must tolerate. Deliberately shy: a node that
/// stretches most ticks runs its protocol clock at half speed, a stalled
/// node rather than a slow one. At this rate a node loses a handful of
/// ticks per chaos window, enough to desynchronize the protocol clocks.
/// Silent in the recovery tail.
pub(crate) fn stretch_tick() -> bool {
    let stretched = moonpool_buggify::buggify_fault_with_prob!(0.05);
    if stretched {
        moonpool_assertions::reachable!("a node stretches its tick interval");
    }
    stretched
}
