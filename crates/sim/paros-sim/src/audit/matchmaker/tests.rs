//! The matchmaker audit's pure judgements on hand-built ledgers and
//! replies: the union a candidate folds, a reply's page window, the
//! effective configuration, and the shared per-tenant tables.

use std::collections::BTreeMap;

use paros::core::journal::JournalId;
use paros::core::matchmaker::{MatchmakerPhase, Registration, RegistrationKind};
use paros::core::membership::{AcceptorConfig, MatchmakerId, QuorumSystem};
use paros::core::{Ballot, NodeId};
use paros::observe::audit::HistoryPage;

use super::{
    Campaign, Registry, ReplyCopy, TenantBootstraps, TenantReconstructions, TenantSets,
    TenantStops, check_effective_survives, check_folded_union, check_page_window, folded_effective,
    history_digest, ledger_disagreement, phase_name, serves_generation, union_above,
};
use crate::audit::testing::red;

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

fn belief(members: &[u64]) -> Registration {
    Registration {
        config: config(members),
        kind: RegistrationKind::Belief,
    }
}

fn reconfiguration(members: &[u64]) -> Registration {
    Registration {
        config: config(members),
        kind: RegistrationKind::Reconfiguration,
    }
}

const MM: MatchmakerId = MatchmakerId(4);

#[test]
fn the_union_above_the_watermark_keeps_each_configuration_once() {
    let a: ReplyCopy = (
        vec![
            (ballot(1, 0), belief(&[0, 1, 2])),
            (ballot(3, 0), belief(&[0, 1, 3])),
        ],
        ballot(2, 0),
        None,
    );
    let b: ReplyCopy = (
        vec![
            (ballot(3, 0), belief(&[0, 1, 3])),
            (ballot(4, 1), belief(&[0, 1, 2])),
        ],
        ballot(2, 0),
        None,
    );
    let (expected, histories) = union_above(&[&a, &b], ballot(2, 0));
    assert_eq!(expected, vec![&config(&[0, 1, 3]), &config(&[0, 1, 2])]);
    assert_eq!(histories.len(), 2);
    let (all, _) = union_above(&[&a, &a], Ballot::zero());
    assert_eq!(all.len(), 2, "the same reply twice adds nothing");
}

#[test]
fn a_candidate_folds_the_maximum_watermark_and_the_union_above_it() {
    let mut campaign = Campaign::default();
    campaign.folded.insert(
        1,
        vec![(vec![(ballot(3, 0), belief(&[0, 1, 3]))], ballot(2, 0), None)],
    );
    campaign.folded.insert(
        2,
        vec![(vec![(ballot(1, 0), belief(&[0, 1, 2]))], ballot(1, 0), None)],
    );
    let me = NodeId(0);
    let b = ballot(5, 0);
    assert!(!red(|| {
        check_folded_union(&campaign, me, b, &[config(&[0, 1, 3])], ballot(2, 0));
    }));
    assert!(
        red(|| {
            check_folded_union(&campaign, me, b, &[config(&[0, 1, 3])], ballot(1, 0));
        }),
        "not the maximum watermark"
    );
    assert!(
        red(|| {
            check_folded_union(&campaign, me, b, &[], ballot(2, 0));
        }),
        "a missing configuration"
    );
    assert!(
        red(|| {
            check_folded_union(
                &campaign,
                me,
                b,
                &[config(&[0, 1, 3]), config(&[7])],
                ballot(2, 0),
            );
        }),
        "a foreign configuration"
    );
}

fn registry(entries: &[(Ballot, Registration)], watermark: Ballot) -> Registry {
    Registry {
        registered: entries.iter().cloned().collect(),
        watermark,
        ..Registry::default()
    }
}

fn page(
    from: Ballot,
    history: &BTreeMap<Ballot, Registration>,
    next: Option<Ballot>,
    limit: usize,
) -> HistoryPage<'_> {
    HistoryPage {
        from_ballot: from,
        history,
        next_from_ballot: next,
        gc_watermark: Ballot::zero(),
        effective: None,
        page_limit: limit,
    }
}

#[test]
fn a_page_is_the_folded_window_less_repeated_beliefs_cut_at_its_size() {
    let entries = [
        (ballot(1, 0), belief(&[0, 1, 2])),
        (ballot(2, 0), belief(&[0, 1, 2])),
        (ballot(3, 0), reconfiguration(&[0, 1, 3])),
        (ballot(4, 0), reconfiguration(&[0, 1, 3])),
    ];
    let entry = registry(&entries, Ballot::zero());
    let asked = ballot(9, 0);
    // The belief at 1 is repeated at 2; a reconfiguration is never left out.
    let whole: BTreeMap<_, _> = entries[1..].iter().cloned().collect();
    assert!(!red(|| check_page_window(
        MM,
        asked,
        &entry,
        &page(Ballot::zero(), &whole, None, 8)
    )));
    let first: BTreeMap<_, _> = entries[1..3].iter().cloned().collect();
    assert!(!red(|| check_page_window(
        MM,
        asked,
        &entry,
        &page(Ballot::zero(), &first, Some(ballot(4, 0)), 2)
    )));
    assert!(
        red(|| check_page_window(MM, asked, &entry, &page(Ballot::zero(), &first, None, 2))),
        "no cursor"
    );
    let all: BTreeMap<_, _> = entries.iter().cloned().collect();
    assert!(
        red(|| check_page_window(MM, asked, &entry, &page(Ballot::zero(), &all, None, 8))),
        "a repeated belief"
    );
    let empty = BTreeMap::new();
    assert!(
        red(|| check_page_window(MM, asked, &entry, &page(Ballot::zero(), &whole, None, 0))),
        "a zero page size"
    );
    assert!(
        red(|| check_page_window(
            MM,
            asked,
            &entry,
            &page(
                Ballot::zero(),
                &whole,
                None,
                paros::core::contract::limits::REGISTRY_PAGE + 1
            )
        )),
        "above the page ceiling"
    );
    let floored = registry(&entries, ballot(3, 0));
    assert!(
        red(|| check_page_window(MM, asked, &floored, &page(ballot(2, 0), &empty, None, 8))),
        "below the watermark"
    );
}

#[test]
fn the_effective_configuration_is_the_highest_shown_or_reported() {
    let mut campaign = Campaign::default();
    assert_eq!(folded_effective(&campaign), None);
    campaign.folded.insert(
        1,
        vec![(
            vec![
                (ballot(3, 0), reconfiguration(&[0, 1, 3])),
                (ballot(5, 0), belief(&[9])),
            ],
            Ballot::zero(),
            None,
        )],
    );
    assert_eq!(
        folded_effective(&campaign),
        Some((ballot(3, 0), config(&[0, 1, 3])))
    );
    campaign.folded.insert(
        2,
        vec![(
            Vec::new(),
            Ballot::zero(),
            Some((ballot(4, 0), config(&[0, 1, 4]))),
        )],
    );
    assert_eq!(
        folded_effective(&campaign),
        Some((ballot(4, 0), config(&[0, 1, 4])))
    );
    campaign.folded.insert(
        3,
        vec![(
            Vec::new(),
            Ballot::zero(),
            Some((ballot(2, 0), config(&[0, 1, 2]))),
        )],
    );
    assert_eq!(
        folded_effective(&campaign),
        Some((ballot(4, 0), config(&[0, 1, 4]))),
        "the highest report wins"
    );
    let mut reported = Campaign::default();
    reported.folded.insert(
        1,
        vec![(
            vec![(ballot(6, 0), reconfiguration(&[0, 1, 6]))],
            Ballot::zero(),
            Some((ballot(4, 0), config(&[0, 1, 4]))),
        )],
    );
    assert_eq!(
        folded_effective(&reported),
        Some((ballot(6, 0), config(&[0, 1, 6])))
    );
}

#[test]
fn a_history_digest_is_the_drivers() {
    let history = vec![(ballot(1, 0), belief(&[0, 1, 2]))];
    assert_eq!(history_digest(&history), history_digest(&history.clone()));
    assert_ne!(history_digest(&history), history_digest(&[]));
}

#[test]
fn a_matchmaker_serves_the_generation_its_scalars_name() {
    assert!(serves_generation(None, 0, true));
    assert!(!serves_generation(None, 0, false));
    assert!(!serves_generation(None, 1, true));
    assert!(serves_generation(
        Some((0, MatchmakerPhase::Fresh)),
        0,
        true
    ));
    assert!(!serves_generation(
        Some((1, MatchmakerPhase::Fresh)),
        1,
        true
    ));
    assert!(serves_generation(
        Some((2, MatchmakerPhase::Active)),
        2,
        false
    ));
    assert!(!serves_generation(
        Some((2, MatchmakerPhase::Active)),
        1,
        true
    ));
    assert!(!serves_generation(
        Some((2, MatchmakerPhase::Stopped)),
        2,
        true
    ));
    assert!(!serves_generation(
        Some((2, MatchmakerPhase::Inactive)),
        2,
        true
    ));
}

#[test]
fn the_first_entry_that_disagrees_with_the_ledger_is_named() {
    let mut ever = BTreeMap::new();
    ever.insert((1, ballot(2, 0)), belief(&[0, 1, 2]));
    ever.insert((2, ballot(3, 0)), belief(&[0, 1, 3]));
    let honest = [
        (ballot(2, 0), belief(&[0, 1, 2])),
        (ballot(5, 0), belief(&[7])),
    ];
    assert_eq!(
        ledger_disagreement(&ever, honest.iter().map(|(b, r)| (b, r))),
        None
    );
    let other = [
        (ballot(2, 0), belief(&[0, 1, 2])),
        (ballot(3, 0), belief(&[9])),
    ];
    assert_eq!(
        ledger_disagreement(&ever, other.iter().map(|(b, r)| (b, r))),
        Some(3)
    );
}

#[test]
fn the_effective_configuration_survives_the_watermark() {
    let mut entry = Registry::default();
    assert!(
        !red(|| check_effective_survives(MM, &entry)),
        "no reconfiguration yet"
    );
    entry.highest_reconfiguration = Some(ballot(3, 0));
    assert!(red(|| check_effective_survives(MM, &entry)));
    entry.effective = Some(ballot(2, 0));
    assert!(red(|| check_effective_survives(MM, &entry)));
    entry.effective = Some(ballot(3, 0));
    assert!(!red(|| check_effective_survives(MM, &entry)));
}

#[test]
fn each_phase_has_its_name() {
    let names: Vec<_> = [
        MatchmakerPhase::Fresh,
        MatchmakerPhase::Inactive,
        MatchmakerPhase::Active,
        MatchmakerPhase::Stopped,
    ]
    .into_iter()
    .map(phase_name)
    .collect();
    assert_eq!(names, ["fresh", "inactive", "active", "stopped"]);
}

#[test]
fn a_generation_binds_its_first_members() {
    let sets = TenantSets::default();
    assert_eq!(sets.get(1), None);
    assert_eq!(sets.bind_first(1, vec![1, 2, 3]), vec![1, 2, 3]);
    assert_eq!(sets.bind_first(1, vec![4, 5, 6]), vec![1, 2, 3]);
    sets.bind_first(0, vec![9]);
    assert_eq!(sets.get(1), Some(vec![1, 2, 3]));
    assert_eq!(sets.generations(), vec![0, 1]);
}

#[test]
fn a_bootstrap_counts_its_holders() {
    let bootstraps = TenantBootstraps::default();
    let proposed = (2, vec![1, 2, 3]);
    assert_eq!(bootstraps.holders(&proposed), 0);
    bootstraps.hold(proposed.clone(), 1);
    bootstraps.hold(proposed.clone(), 1);
    bootstraps.hold(proposed.clone(), 2);
    assert_eq!(bootstraps.holders(&proposed), 2);
    assert!(bootstraps.holds(&proposed, 2) && !bootstraps.holds(&proposed, 3));
    assert!(!bootstraps.holds(&(3, vec![1]), 1));
}

#[test]
fn a_reconstruction_is_noted_once_and_a_left_out_journal_reads_blank() {
    let reconstructions = TenantReconstructions::default();
    let proposed = (2, vec![1, 2, 3]);
    assert_eq!(reconstructions.candidates(&proposed, JournalId(1)), None);
    let candidate = (ballot(3, 0), BTreeMap::from([(ballot(3, 0), belief(&[0]))]));
    reconstructions.note(proposed.clone(), JournalId(1), candidate.clone());
    reconstructions.note(proposed.clone(), JournalId(1), candidate.clone());
    assert_eq!(
        reconstructions.candidates(&proposed, JournalId(1)),
        Some(vec![candidate])
    );
    assert_eq!(
        reconstructions.candidates(&proposed, JournalId(2)),
        Some(vec![(Ballot::zero(), BTreeMap::new())])
    );
}

#[test]
fn a_handovers_stop_acks_count_once_whole() {
    let stops = TenantStops::default();
    let frozen = |round| BTreeMap::from([(JournalId(1), (ballot(round, 0), Vec::new()))]);
    stops.insert((0, 1, 7), frozen(2), true, false);
    assert!(
        stops.folded(0, 1, JournalId(1)).is_empty(),
        "a report not whole yet"
    );
    stops.insert((0, 1, 7), frozen(3), false, true);
    assert_eq!(stops.folded(0, 1, JournalId(1))[&7].0, ballot(3, 0));
    assert_eq!(
        stops.folded(0, 1, JournalId(2))[&7],
        (Ballot::zero(), Vec::new())
    );
    // A first page starts the report again.
    stops.insert((0, 1, 7), BTreeMap::new(), true, true);
    assert_eq!(
        stops.folded(0, 1, JournalId(1))[&7],
        (Ballot::zero(), Vec::new())
    );
    stops.clear(0, 1);
    assert!(stops.folded(0, 1, JournalId(1)).is_empty());
}
