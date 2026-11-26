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
//! | 6 | `L` made the GC floor `w` effective, and a fresh node outside `C` that is not a spare of the caller may retire at `w` once the matchmakers checked it | Order: [`Order::Retire`] |
//! | 7 | No member of `C` is both silent for the leader and without fresh feedback | [`Decision::Steady`] |
//! | 8 | `L` is not settled | Hold: [`HoldReason::Unsettled`] |
//! | 9 | A configuration other than `C` is still in force | Hold: [`HoldReason::GcNotEffective`] |
//! | 10 | No spare is fresh, outside `C`, not retired and, on a zoned `C`, of the domain of the silent member | Hold: [`HoldReason::NoSpare`] |
//! | 11 | No spare of rule 10 is in the pool of `L`, of each fresh member of `C` and of each fresh proxy leader, with `L` and each member of `C` in its own pool | Hold: [`HoldReason::SpareNotAdmitted`] |
//! | 12 | For each spare of rule 11, `C'` is `C` with the spare in place of the silent member: the same size, quorum system, counts and domains | An assertion, never a hold: such a `C'` is always well formed |
//! | 13 | For no spare of rule 12 are the fresh nodes a Phase-1 quorum of `C'` that holds a Phase-2 quorum of each column of `C'`; or `C` keeps the zone rule and no such `C'` keeps it | Hold: [`HoldReason::NewConfigurationLost`]; [`HoldReason::ZoneRuleLost`] |
//! | 14 | Else | Order: [`Order::Reconfigure`] to the first `C'` that rule 13 keeps |
//!
//! Past rule 7 a member is **silent** on two witnesses: the leader missed it
//! for [`Policy::silent_windows`] windows in a row, and the member itself
//! sent no fresh feedback. Rules 10 to 14 take the silent members from the
//! lowest, and replace the first one that a spare can replace, with the
//! first spare of the caller's list that rules 10 to 13 keep. When none can
//! be replaced, the hold is the reason of the lowest one. A
//! spare is **fresh** as a node is: the view holds a fresh feedback of it,
//! so it runs the journal and can answer a `Prepare`.
//!
//! A grid places each member in a column by its position in the sorted
//! membership. So the spare takes the place of the silent member only when
//! its id sorts there: otherwise the members between the two places move to
//! other columns. Rule 13 judges `C'` as it is built, so a move that
//! loses a column or the zone rule is a hold, never an order.
//!
//! A spare that another controller (with an older view) started to retire
//! may be recorded as retired at some matchmakers. The campaign of its
//! reconfiguration is then refused and abandoned: a lost campaign, never a
//! safety break (§4.8.1). This controller never starts such a check on a
//! spare: rule 6 skips the spares of the caller.

use std::collections::BTreeSet;

use crate::contract::feedback::{LeaderFeedback, NodeFeedback, NodeInsight};
use crate::membership::{AcceptorConfig, MatchmakerId, ShapeError, ZoneRule};
use crate::types::{NodeId, Slot};

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
    spares: &[Spare],
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
                && spares.iter().all(|spare| spare.node != node.id)
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
                assert!(
                    spares.iter().all(|spare| spare.node != *node),
                    "a spare of the caller is never retired"
                );
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
    if let Some(order) = retirable(view, policy, config, leading, spares) {
        return Decision::Order(order);
    }
    // Rule 7.
    let silent = silent_members(config, leading, &fresh, policy);
    let Some(&lowest) = silent.first() else {
        return Decision::Steady;
    };
    assert!(config.contains(lowest), "a silent member is a member");
    // Rule 8.
    if !leading.settled {
        return Decision::Hold(HoldReason::Unsettled);
    }
    // Rule 9.
    if !leading.prior.is_empty() {
        return Decision::Hold(HoldReason::GcNotEffective);
    }
    // Rules 10 to 13, for each silent member in order; the hold of the
    // lowest one when none can be replaced.
    let mut first_hold = None;
    for &node in &silent {
        match replacement(view, policy, leader, &fresh, node, spares) {
            Ok(next) => {
                // Rule 14.
                assert!(!next.contains(node), "the silent member leaves");
                assert!(
                    next != *config,
                    "a reconfiguration changes the configuration"
                );
                return Decision::Order(Order::Reconfigure {
                    leader: leader.id,
                    ballot: leader.ballot,
                    since: leader.since,
                    config: next,
                });
            }
            Err(reason) => {
                first_hold.get_or_insert(reason);
            }
        }
    }
    let reason = first_hold.expect("rule 7 names a silent member");
    assert!(
        reason.node() == Some(lowest),
        "the hold names the lowest silent member"
    );
    Decision::Hold(reason)
}

/// Rules 10 to 13 for the silent member `node`: the first `C'` that
/// replaces it, or the reason none does.
fn replacement(
    view: &View,
    policy: &Policy,
    leader: &NodeFeedback,
    fresh: &BTreeSet<NodeId>,
    node: NodeId,
    spares: &[Spare],
) -> Result<AcceptorConfig, HoldReason> {
    let config = &leader.acceptors;
    // Rule 10.
    let candidates = candidate_spares(view, policy, config, node, spares);
    if candidates.is_empty() {
        return Err(HoldReason::NoSpare { node });
    }
    // Rule 11.
    let admitted: Vec<&Spare> = candidates
        .into_iter()
        .filter(|spare| admitted(view, policy, leader, node, spare.node))
        .collect();
    if admitted.is_empty() {
        return Err(HoldReason::SpareNotAdmitted { node });
    }
    // Rule 12, and the first half of rule 13.
    let live: Vec<AcceptorConfig> = admitted
        .into_iter()
        .map(|spare| {
            let next = replaced(config, node, spare);
            assert!(!next.contains(node), "the silent member leaves");
            assert!(next.contains(spare.node), "the spare joins");
            next
        })
        .filter(|next| is_live(next, fresh))
        .collect();
    if live.is_empty() {
        return Err(HoldReason::NewConfigurationLost { node });
    }
    // The second half of rule 13: a `C` that breaks the zone rule asks
    // nothing of `C'`.
    let zones_free = config.zone_rule() != ZoneRule::Holds;
    let next = live
        .into_iter()
        .find(|next| zones_free || next.zone_rule() == ZoneRule::Holds)
        .ok_or(HoldReason::ZoneRuleLost { node })?;
    assert!(
        zones_free || next.zone_rule() == ZoneRule::Holds,
        "an order keeps the zone rule of the configuration in force"
    );
    Ok(next)
}

/// Rule 10: the spares, in the caller's order, that can replace `silent`:
/// fresh, outside `config`, not retired, and on a zoned `config` of the
/// domain of `silent`.
fn candidate_spares<'s>(
    view: &View,
    policy: &Policy,
    config: &AcceptorConfig,
    silent: NodeId,
    spares: &'s [Spare],
) -> Vec<&'s Spare> {
    assert!(config.contains(silent), "the silent member is a member");
    let domain = config.domain_of(silent);
    assert!(
        domain.is_some() == config.is_zoned(),
        "a member of a zoned configuration has a domain"
    );
    let candidates: Vec<&Spare> = spares
        .iter()
        .filter(|spare| !config.contains(spare.node))
        .filter(|spare| domain.is_none() || spare.domain.as_ref() == domain)
        .filter(|spare| {
            view.nodes().get(&spare.node).is_some_and(|aged| {
                aged.age <= policy.max_age && !aged.feedback.retire_mark.checked
            })
        })
        .collect();
    assert!(
        candidates.iter().all(|spare| spare.node != silent),
        "the silent member is no spare of itself"
    );
    candidates
}

/// Rule 11: whether `spare` is in the pool of the leader, of each fresh
/// member of the leader's configuration and of each fresh proxy leader,
/// and whether its own pool holds the leader and each member but `silent`.
/// A role whose pool does not hold it could not address it, and
/// `ColocatedNode::reconfigure` refuses a configuration outside the
/// leader's pool.
fn admitted(
    view: &View,
    policy: &Policy,
    leader: &NodeFeedback,
    silent: NodeId,
    spare: NodeId,
) -> bool {
    let config = &leader.acceptors;
    assert!(!config.contains(spare), "a spare is outside");
    assert!(config.contains(silent), "the silent member is a member");
    if !leader.pool.contains(&spare) {
        return false;
    }
    // A node drops a `Prepare` or an `Accept` from outside its own pool, so
    // a spare whose pool lacks the leader or a member that stays would never
    // vote.
    let Some(own) = view.nodes().get(&spare) else {
        return false;
    };
    if !config
        .members()
        .iter()
        .filter(|id| **id != silent)
        .chain(std::iter::once(&leader.id))
        .all(|id| own.feedback.pool.contains(id))
    {
        return false;
    }
    let members = view
        .nodes()
        .values()
        .filter(|aged| aged.age <= policy.max_age && config.contains(aged.feedback.id));
    let proxies = view
        .proxies()
        .values()
        .filter(|aged| aged.age <= policy.max_age);
    members
        .map(|aged| &aged.feedback.pool)
        .chain(proxies.map(|aged| &aged.feedback.pool))
        .all(|pool| pool.contains(&spare))
}

/// Rule 12: `config` with `spare` in place of `silent`, with the same
/// quorum system, the same counts of proxy leaders and replicas, and on a
/// zoned `config` the domain of the spare, which rule 10 made the domain of
/// `silent`.
///
/// # Panics
///
/// If the new configuration cannot be built: a swap of one member keeps the
/// size, so the quorum system still admits it, and rule 10 keeps only a
/// spare whose domain is the well-formed domain of `silent`.
fn replaced(config: &AcceptorConfig, silent: NodeId, spare: &Spare) -> AcceptorConfig {
    assert!(config.contains(silent), "the silent member is a member");
    assert!(!config.contains(spare.node), "the spare is outside");
    let built: Result<AcceptorConfig, ShapeError> = if config.is_zoned() {
        let domain = spare
            .domain
            .clone()
            .expect("rule 10 keeps a spare with a domain on a zoned configuration");
        assert!(
            config.domain_of(silent) == Some(&domain),
            "rule 10 keeps a spare of the domain of the silent member"
        );
        let members = config
            .members()
            .iter()
            .zip(config.domains())
            .filter(|(id, _)| **id != silent)
            .map(|(id, domain)| (*id, domain.clone()))
            .chain(std::iter::once((spare.node, domain)))
            .collect();
        AcceptorConfig::try_zoned(members, config.quorum_system())
    } else {
        let members = config
            .members()
            .iter()
            .copied()
            .filter(|id| *id != silent)
            .chain(std::iter::once(spare.node))
            .collect();
        AcceptorConfig::try_new(members, config.quorum_system())
    };
    let next = built
        .and_then(|next| next.try_with_counts(config.proxy_count(), config.replica_count()))
        .unwrap_or_else(|e| panic!("a swap of one member keeps the shape: {e}"));
    assert!(
        next.members().len() == config.members().len(),
        "a replacement keeps the size"
    );
    assert!(
        next.quorum_system() == config.quorum_system(),
        "a replacement keeps the quorum system"
    );
    assert!(
        next.is_zoned() == config.is_zoned(),
        "a replacement keeps the labels"
    );
    next
}

/// Rule 13: whether the `fresh` nodes are a Phase-1 quorum of `config`
/// and hold a Phase-2 quorum of each of its columns.
fn is_live(config: &AcceptorConfig, fresh: &BTreeSet<NodeId>) -> bool {
    // Each column is the column of some slot below the size of the
    // membership: a grid has at most as many columns as members.
    let columns: BTreeSet<Option<usize>> = (0..config.members().len() as u64)
        .map(|slot| config.column_of(Slot(slot)))
        .collect();
    assert!(!columns.is_empty(), "a configuration has a column");
    assert!(
        columns.len() <= config.members().len(),
        "a column holds a member"
    );
    config.has_phase1_quorum(fresh)
        && columns
            .into_iter()
            .all(|column| config.has_phase2_quorum_in(fresh, column))
}

#[cfg(test)]
mod tests;
