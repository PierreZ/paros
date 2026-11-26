//! The **rules** of the journal controller: one ordered table, read from
//! the top. The first rule whose guard holds gives the decision.
//!
//! Below, a feedback is **fresh** when its age is at most
//! [`Policy::max_age`]. `L` is the fresh leader feedback with the highest
//! ballot, and `C` is its configuration.
//!
//! | # | Guard | Decision |
//! |---|---|---|
//! | 1 | No fresh feedback of a leader | Hold: [`HoldReason::NoLeader`] |
//! | 2 | A fresh node or proxy reports a ballot above the ballot of `L` | Hold: [`HoldReason::LeadershipMoves`] |
//! | 3 | No set of the latest generation that the fresh, active matchmakers report has a majority of its own members that report it, or one of them reports a configuration in force above the one `L` believes | Hold: [`HoldReason::Matchmakers`] |
//! | 4 | The fresh members of `C` are not a Phase-1 quorum of `C` | Hold: [`HoldReason::Phase1Lost`] |
//! | 5 | A column of `C` has no Phase-2 quorum of fresh members | No hold by itself: a lost column is a hold only when Phase 1 is lost too, which is rule 4 (Pierre, 2026-11-26) |
//! | 6 | `L` made the GC floor `w` effective, and a fresh node outside `C` may retire at `w` once the matchmakers checked it | Order: [`Order::Retire`] |
//! | 7 | No member of `C` is both silent for the leader and without fresh feedback | [`Decision::Steady`] |
//!
//! Past rule 7 a member is silent on two witnesses: the leader missed it
//! for [`Policy::silent_windows`] windows in a row, and the member itself
//! sent no fresh feedback.

use std::collections::BTreeSet;

use crate::contract::feedback::{LeaderFeedback, NodeFeedback, NodeInsight};
use crate::membership::{AcceptorConfig, MatchmakerId};
use crate::types::NodeId;

use super::{Decision, HoldReason, Order, Policy, Spare, View};

/// The fresh leader of the view: the fresh node feedback that leads, with
/// the highest ballot.
fn fresh_leader<'v>(
    view: &'v View,
    policy: &Policy,
) -> Option<(&'v NodeFeedback, &'v LeaderFeedback)> {
    view.nodes()
        .values()
        .filter(|aged| aged.age <= policy.max_age)
        .filter_map(|aged| {
            aged.feedback
                .leading
                .as_ref()
                .map(|leading| (&aged.feedback, leading))
        })
        .max_by_key(|(node, _)| node.ballot)
        .inspect(|(node, _)| {
            let aged = &view.nodes()[&node.id];
            assert!(
                aged.age <= policy.max_age,
                "the leader of the view is fresh"
            );
            assert!(
                aged.feedback.leading.is_some(),
                "the leader of the view reports a leadership"
            );
        })
}

/// The nodes with a fresh feedback.
fn fresh_nodes(view: &View, policy: &Policy) -> BTreeSet<NodeId> {
    let fresh: BTreeSet<NodeId> = view
        .nodes()
        .iter()
        .filter(|(_, aged)| aged.age <= policy.max_age)
        .map(|(id, _)| *id)
        .collect();
    assert!(
        fresh.len() <= view.nodes().len(),
        "the fresh nodes are nodes of the view"
    );
    assert!(
        fresh
            .iter()
            .all(|id| view.nodes()[id].age <= policy.max_age),
        "each fresh node is fresh"
    );
    fresh
}

/// Rule 2: a fresh node or proxy works for a ballot above the leader's.
fn leadership_moves(view: &View, policy: &Policy, leader: &NodeFeedback) -> bool {
    assert!(
        leader.leading.is_some(),
        "the moves are judged against a leader"
    );
    let node_above = view
        .nodes()
        .values()
        .any(|aged| aged.age <= policy.max_age && aged.feedback.ballot > leader.ballot);
    let proxy_above = view
        .proxies()
        .values()
        .any(|aged| aged.age <= policy.max_age && aged.feedback.ballot > leader.ballot);
    node_above || proxy_above
}

/// Rule 3: no set of the latest generation that the fresh, active
/// matchmakers report has a majority of its own members that report it,
/// or one member of that majority holds a configuration in force above
/// the one the leader believes.
///
/// A matchmaker that reports a set it is not a member of, or a set of the
/// same generation with other members, is the feedback of another set that
/// the caller mixed in: data from outside, so it is not counted, and no
/// assertion judges it.
fn matchmakers_hold(view: &View, policy: &Policy, leader: &NodeFeedback) -> bool {
    let fresh: Vec<_> = view
        .matchmakers()
        .values()
        .filter(|aged| aged.age <= policy.max_age && aged.feedback.active)
        .map(|aged| &aged.feedback)
        .collect();
    let Some(generation) = fresh.iter().map(|feedback| feedback.set.generation).max() else {
        return true;
    };
    let in_force = fresh
        .iter()
        .filter(|feedback| feedback.set.generation == generation)
        .map(|feedback| &feedback.set)
        .find_map(|set| {
            let voters: Vec<_> = fresh
                .iter()
                .filter(|feedback| feedback.set == *set && set.contains(feedback.id))
                .collect();
            let ids: BTreeSet<MatchmakerId> = voters.iter().map(|feedback| feedback.id).collect();
            set.has_quorum(&ids).then_some(voters)
        });
    let Some(voters) = in_force else {
        return true;
    };
    assert!(!voters.is_empty(), "a quorum of the set has a voter");
    assert!(
        voters
            .iter()
            .all(|feedback| feedback.set.generation == generation),
        "the voters report the latest generation"
    );
    voters.iter().any(|feedback| {
        feedback
            .effective
            .as_ref()
            .is_some_and(|(ballot, _)| *ballot > leader.since)
    })
}

/// Rule 6: the fresh nodes outside `C` that may retire at the effective
/// floor once the matchmakers checked them, lowest first.
fn retirable(
    view: &View,
    policy: &Policy,
    config: &AcceptorConfig,
    leading: &LeaderFeedback,
) -> Option<Order> {
    let (watermark, _) = leading.gc_effective.as_ref()?;
    // One feedback is one snapshot of one leader, and a correct leader
    // makes a floor effective only once it forgot every prior
    // configuration (`node/gc.rs`), so this holds for each feedback of a
    // peer of the pool.
    assert!(
        leading.prior.is_empty(),
        "an effective floor leaves no prior configuration in force"
    );
    view.nodes()
        .values()
        .filter(|aged| aged.age <= policy.max_age)
        .map(|aged| &aged.feedback)
        .find(|node| {
            !config.contains(node.id)
                && !node.retire_mark.checked
                && node
                    .insights
                    .contains(&NodeInsight::RetirementUnchecked { floor: *watermark })
        })
        .map(|node| Order::Retire {
            node: node.id,
            watermark: *watermark,
        })
        .inspect(|order| {
            if let Order::Retire { node, .. } = order {
                assert!(!config.contains(*node), "a retired node is outside");
            }
        })
}

/// Rule 7: the members of `C` that the leader missed for
/// [`Policy::silent_windows`] windows in a row and that sent no fresh
/// feedback, lowest first.
fn silent_members(
    config: &AcceptorConfig,
    leading: &LeaderFeedback,
    fresh: &BTreeSet<NodeId>,
    policy: &Policy,
) -> Vec<NodeId> {
    let silent: Vec<NodeId> = config
        .members()
        .iter()
        .copied()
        .filter(|member| {
            !fresh.contains(member)
                && leading
                    .missed
                    .iter()
                    .any(|(id, windows)| id == member && *windows >= policy.silent_windows)
        })
        .collect();
    assert!(
        silent.windows(2).all(|pair| pair[0] < pair[1]),
        "the silent members come in order"
    );
    assert!(
        silent.iter().all(|id| !fresh.contains(id)),
        "a silent member sent no fresh feedback"
    );
    silent
}

/// Decide at most one order for the journal of `view`, from the spares
/// that the caller offers, under `policy` (the table of this module).
///
/// # Panics
///
/// If `policy.silent_windows` is zero: a member is never silent for no
/// missed window.
#[must_use]
#[cfg_attr(feature = "tracing", tracing::instrument(level = "debug", skip_all))]
pub fn evaluate(view: &View, spares: &[Spare], policy: &Policy) -> Decision {
    assert!(
        policy.silent_windows > 0,
        "a member is silent for one window at least"
    );
    let _ = spares;
    // Rule 1.
    let Some((leader, leading)) = fresh_leader(view, policy) else {
        return Decision::Hold(HoldReason::NoLeader);
    };
    let config = &leader.acceptors;
    // Rule 2.
    if leadership_moves(view, policy, leader) {
        return Decision::Hold(HoldReason::LeadershipMoves);
    }
    // Rule 3.
    if matchmakers_hold(view, policy, leader) {
        return Decision::Hold(HoldReason::Matchmakers);
    }
    // Rule 4. Rule 5 gives no hold by itself.
    let fresh = fresh_nodes(view, policy);
    assert!(
        fresh.contains(&leader.id),
        "the leader of the view is fresh"
    );
    if !config.has_phase1_quorum(&fresh) {
        return Decision::Hold(HoldReason::Phase1Lost);
    }
    // Rule 6.
    if let Some(order) = retirable(view, policy, config, leading) {
        return Decision::Order(order);
    }
    // Rule 7.
    let silent = silent_members(config, leading, &fresh, policy);
    let Some(&node) = silent.first() else {
        return Decision::Steady;
    };
    assert!(config.contains(node), "a silent member is a member");
    Decision::Hold(HoldReason::MemberSilent { node })
}

#[cfg(test)]
mod tests;
