//! The candidate-side matchmaking oracles (#120, #173) on hand-built
//! campaigns: an honest campaign stays clean, and each broken step goes
//! red at the oracle that owns it.

use std::collections::{BTreeMap, BTreeSet};

use paros::core::matchmaker::{MatchRefusal, RegistrationKind};
use paros::core::membership::{AcceptorConfig, MatchmakerId, QuorumSystem};
use paros::core::{Ballot, NodeId};

use crate::audit::matchmaker::MatchmakerAudit;
use crate::audit::testing::red;

const NODE: NodeId = NodeId(1);

fn ballot(round: u64, node: u64) -> Ballot {
    Ballot {
        round,
        node: NodeId(node),
    }
}

fn config(members: &[u64]) -> AcceptorConfig {
    AcceptorConfig::new(
        members.iter().map(|m| NodeId(*m)).collect(),
        QuorumSystem::Majority,
    )
}

/// An audit of three matchmakers whose nodes boot with `[1, 2, 3]`.
fn audit() -> MatchmakerAudit {
    let mut audit = MatchmakerAudit::default();
    audit.note_deployment(&[MatchmakerId(10), MatchmakerId(11), MatchmakerId(12)]);
    audit.note_bootstrap(&config(&[1, 2, 3]));
    audit
}

fn open(audit: &mut MatchmakerAudit, round: u64, members: &[u64], kind: RegistrationKind) {
    audit.campaign_started(NODE, ballot(round, 1), &config(members), kind, 0);
}

#[test]
fn a_campaign_opens_once_on_a_ballot_naming_its_node() {
    let mut audit = audit();
    assert_eq!(audit.quorum(0), 2);
    assert!(!red(|| open(
        &mut audit,
        1,
        &[1, 2, 3],
        RegistrationKind::Belief
    )));
    assert!(audit.campaign_opened);
    assert!(red(|| open(
        &mut audit,
        1,
        &[1, 2, 3],
        RegistrationKind::Belief
    )));
    assert!(red(|| audit.campaign_started(
        NODE,
        ballot(2, 9),
        &config(&[1, 2, 3]),
        RegistrationKind::Belief,
        0
    )));
}

#[test]
fn a_belief_is_the_bootstrap_or_a_configuration_on_the_wire() {
    let mut audit = audit();
    assert!(red(|| open(
        &mut audit,
        1,
        &[1, 2, 4],
        RegistrationKind::Belief
    )));
    // A leader's reconfiguration puts `[1, 2, 4]` on the wire.
    assert!(!red(|| open(
        &mut audit,
        2,
        &[1, 2, 4],
        RegistrationKind::Reconfiguration
    )));
    assert!(audit.reconfiguration_opened);
    assert!(!red(|| open(
        &mut audit,
        3,
        &[1, 2, 4],
        RegistrationKind::Belief
    )));
    // Before the bootstrap is known, nothing is judged.
    let mut fresh = MatchmakerAudit::default();
    assert!(!red(|| open(&mut fresh, 1, &[7], RegistrationKind::Belief)));
}

#[test]
fn a_refused_candidate_re_campaigns_above_the_refusers_round() {
    let mut audit = audit();
    open(&mut audit, 1, &[1, 2, 3], RegistrationKind::Belief);
    let stale = MatchRefusal::Stale {
        highest: ballot(5, 2),
    };
    assert!(!red(|| audit.campaign_refused(NODE, ballot(1, 1), &stale)));
    assert!(audit.campaign_refused);
    assert!(red(|| open(
        &mut audit,
        5,
        &[1, 2, 3],
        RegistrationKind::Belief
    )));
    // The floor binds one campaign: the red one consumed it.
    assert!(!red(|| open(
        &mut audit,
        6,
        &[1, 2, 3],
        RegistrationKind::Belief
    )));
    audit.campaign_refused(NODE, ballot(6, 1), &stale);
    audit.node_booted(NODE);
    assert!(
        !red(|| open(&mut audit, 7, &[1, 2, 3], RegistrationKind::Belief)),
        "a reboot drops the floor"
    );
}

#[test]
fn the_refused_floor_is_the_highest_refusers_round() {
    let mut audit = audit();
    open(&mut audit, 1, &[1, 2, 3], RegistrationKind::Belief);
    // Two matchmakers refuse the one campaign.
    for highest in [8, 4] {
        audit.campaign_refused(
            NODE,
            ballot(1, 1),
            &MatchRefusal::Stale {
                highest: ballot(highest, 2),
            },
        );
    }
    assert_eq!(audit.refused_floor.get(&1), Some(&8));
}

#[test]
fn a_request_and_a_re_ask_leave_only_an_open_campaign() {
    let mut audit = audit();
    let mm = MatchmakerId(10);
    assert!(red(|| audit.request_sent(NODE, mm, ballot(1, 1))));
    assert!(red(|| audit.clock_reasked(NODE, ballot(1, 1))));
    open(&mut audit, 2, &[1, 2, 3], RegistrationKind::Belief);
    assert!(!red(|| audit.request_sent(NODE, mm, ballot(2, 1))));
    assert!(!red(|| audit.clock_reasked(NODE, ballot(2, 1))));
    assert!(audit.clock_reasked);
    audit.campaign_refused(
        NODE,
        ballot(2, 1),
        &MatchRefusal::BelowWatermark {
            watermark: ballot(1, 0),
        },
    );
    assert!(red(|| audit.request_sent(NODE, mm, ballot(2, 1))));
    assert!(red(|| audit.clock_reasked(NODE, ballot(2, 1))));
    assert!(
        audit.refused_floor.is_empty(),
        "only a stale refusal floors"
    );
}

#[test]
fn a_refusal_never_lands_on_a_completed_campaign() {
    let mut audit = audit();
    open(&mut audit, 1, &[1, 2, 3], RegistrationKind::Belief);
    audit
        .campaigns
        .get_mut(&(1, ballot(1, 1)))
        .expect("open")
        .completed = true;
    let stale = MatchRefusal::Stale {
        highest: ballot(1, 2),
    };
    assert!(red(|| audit.campaign_refused(NODE, ballot(1, 1), &stale)));
}

#[test]
fn a_refused_probe_is_spent_and_judges_nothing() {
    let mut audit = audit();
    audit.probe_opened(NODE, ballot(1, 1), 0);
    let stale = MatchRefusal::Stale {
        highest: ballot(9, 2),
    };
    assert!(!red(|| audit.campaign_refused(NODE, ballot(1, 1), &stale)));
    assert!(audit.probes.is_empty() && audit.refused_floor.is_empty());
    assert!(!audit.campaign_refused);
}

#[test]
fn a_probe_closes_on_a_quorum_and_learns_what_a_quorum_held() {
    let mut audit = audit();
    // A quorum holds a reconfiguration at round 3 before the probe opens.
    for mm in [10, 11] {
        audit
            .reconfigurations
            .entry(ballot(3, 2))
            .or_insert_with(|| (config(&[1, 2, 4]), BTreeSet::new()))
            .1
            .insert(mm);
    }
    audit.probe_opened(NODE, ballot(1, 1), 0);
    assert!(red(|| audit.probe_closed(
        NODE,
        ballot(1, 1),
        Some(ballot(3, 2)),
        true,
        1
    )));
    audit.probe_opened(NODE, ballot(2, 1), 0);
    assert!(red(|| audit.probe_closed(
        NODE,
        ballot(2, 1),
        Some(ballot(2, 2)),
        true,
        2
    )));
    audit.probe_opened(NODE, ballot(4, 1), 0);
    assert!(red(|| audit.probe_closed(
        NODE,
        ballot(4, 1),
        None,
        true,
        2
    )));
    audit.probe_opened(NODE, ballot(5, 1), 0);
    assert!(!red(|| audit.probe_closed(
        NODE,
        ballot(5, 1),
        Some(ballot(3, 2)),
        true,
        2
    )));
    assert!(audit.probe_rejoined);
}

#[test]
fn a_node_outside_its_belief_re_probes_and_rejoins() {
    let mut audit = audit();
    audit.probe_opened(NODE, ballot(1, 1), 0);
    audit.probe_closed(NODE, ballot(1, 1), Some(ballot(1, 2)), false, 2);
    assert!(audit.probed_outside.contains(&1));
    audit.probe_opened(NODE, ballot(2, 1), 0);
    audit.probe_closed(NODE, ballot(2, 1), None, true, 2);
    assert!(audit.reprobe_rejoined && !audit.probed_outside.contains(&1));
    audit.probed_outside.insert(1);
    audit.probe_late(NODE, false);
    assert!(audit.late_answer_moved && audit.probed_outside.contains(&1));
    audit.probe_late(NODE, true);
    assert!(!audit.probed_outside.contains(&1));
    audit.probed_outside.insert(1);
    audit.node_booted(NODE);
    assert!(audit.probed_outside.is_empty());
}

#[test]
fn a_stale_abort_adopts_a_reconfiguration_on_an_ordinary_campaign() {
    let mut audit = audit();
    open(&mut audit, 4, &[1, 2, 3], RegistrationKind::Belief);
    assert!(
        red(|| audit.campaign_stale(NODE, ballot(4, 1), ballot(3, 2))),
        "no registration at the newest ballot"
    );
    let mut audit = audit_with_ever(RegistrationKind::Belief);
    open(&mut audit, 4, &[1, 2, 3], RegistrationKind::Belief);
    assert!(red(|| audit.campaign_stale(
        NODE,
        ballot(4, 1),
        ballot(3, 2)
    )));
    let mut audit = audit_with_ever(RegistrationKind::Reconfiguration);
    open(&mut audit, 4, &[1, 2, 3], RegistrationKind::Belief);
    assert!(!red(|| audit.campaign_stale(
        NODE,
        ballot(4, 1),
        ballot(3, 2)
    )));
    assert!(audit.campaign_stale);
    assert!(red(|| audit.request_sent(
        NODE,
        MatchmakerId(10),
        ballot(4, 1)
    )));
    let mut audit = audit_with_ever(RegistrationKind::Reconfiguration);
    open(&mut audit, 4, &[1, 2, 4], RegistrationKind::Reconfiguration);
    assert!(
        red(|| audit.campaign_stale(NODE, ballot(4, 1), ballot(3, 2))),
        "a reconfiguration never aborts as stale"
    );
}

/// An audit whose ledger holds a `kind` registration of `[1, 2, 4]` at
/// round 3 of node 2.
fn audit_with_ever(kind: RegistrationKind) -> MatchmakerAudit {
    let mut audit = audit();
    audit.ever.insert(
        (10, ballot(3, 2)),
        paros::core::matchmaker::Registration {
            config: config(&[1, 2, 4]),
            kind,
        },
    );
    audit
}

#[test]
fn a_campaign_is_exempt_only_below_another_leadership_or_by_a_newer_fact() {
    let mine = config(&[1, 2, 3]);
    let other = config(&[1, 2, 4]);
    let won = BTreeMap::from([(ballot(2, 2), other.clone())]);
    let none = BTreeMap::new();
    let exemptions = |won, newest, at, mine: Option<&AcceptorConfig>| {
        MatchmakerAudit::exemptions(won, &none, (newest, at), &config(&[1, 2, 5]), mine)
    };
    let nobody = BTreeMap::new();
    assert_eq!(
        exemptions(&won, ballot(1, 0), ballot(3, 1), None),
        (true, false)
    );
    assert_eq!(
        exemptions(&won, ballot(2, 2), ballot(3, 1), None),
        (false, false),
        "the leadership at the newest ballot is not between"
    );
    assert_eq!(
        exemptions(&won, ballot(1, 0), ballot(2, 2), None),
        (false, false),
        "nor is one at the campaign's ballot"
    );
    assert_eq!(
        exemptions(&won, ballot(4, 0), ballot(3, 1), None),
        (false, false)
    );
    let same = BTreeMap::from([(ballot(2, 2), config(&[1, 2, 5]))]);
    assert_eq!(
        exemptions(&same, ballot(1, 0), ballot(3, 1), None),
        (false, false),
        "a leadership under the same configuration"
    );
    let above = BTreeMap::from([(ballot(5, 3), (mine.clone(), BTreeSet::from([10])))]);
    let newer = |newest, at, belief: &AcceptorConfig| {
        MatchmakerAudit::exemptions(&nobody, &above, (newest, at), &other, Some(belief)).1
    };
    assert!(
        newer(ballot(4, 0), ballot(3, 1), &mine),
        "above the ballot too"
    );
    assert!(
        !newer(ballot(5, 3), ballot(6, 1), &mine),
        "at the newest is not above it"
    );
    assert!(!newer(ballot(4, 0), ballot(6, 1), &other));
}
