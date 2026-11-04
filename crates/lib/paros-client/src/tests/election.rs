//! The election library over fake nodes: a candidate campaigns for a
//! vacant journal and renews, another follows, a takeover deposes, a
//! resignation hands off, the leader truncates to its renewal, and a
//! reader that is no candidate folds the journal from its floor.

use std::time::Duration;

use paros_core::journal::{JournalIdentifier, WriterMode};

use crate::election::{
    Candidate, Election, ElectionRecord, ElectionTunables, Step, hand_off, read_election,
};
use crate::rig::Rig;
use crate::rig::cell::{Act, Cell, ids};
use crate::{Client, SetLeaderOutcome};
use moonpool_sim::SimProviders;

fn journal() -> JournalIdentifier {
    ids(1, 9)
}

fn governed() -> JournalIdentifier {
    ids(1, 10)
}

fn candidate(id: u64) -> Candidate {
    Candidate {
        id,
        interface: format!("10.0.2.{id}:4500"),
        reference: Vec::new(),
    }
}

fn tunables(compact_after: u64) -> ElectionTunables {
    ElectionTunables {
        lease: Duration::from_millis(300),
        renew_every: Duration::from_millis(100),
        compact_after,
    }
}

/// A cell led by node 1 with an empty election journal and an unled
/// governed journal.
fn cell() -> Cell {
    let cell = Cell::led_by(1);
    cell.create(journal(), WriterMode::Multi);
    cell.create(governed(), WriterMode::Single);
    cell
}

fn election(client: &Client<SimProviders>, id: u64, compact_after: u64) -> Election<SimProviders> {
    Election::new(
        client.clone(),
        journal(),
        candidate(id),
        u128::from(id) * 1000,
        tunables(compact_after),
    )
}

/// Step `election` until it leads, a pause between steps: the steps taken.
async fn step_until_leading(
    client: &Client<SimProviders>,
    election: &mut Election<SimProviders>,
) -> Vec<Step> {
    let mut steps = Vec::new();
    for _ in 0..50 {
        let step = election.step(Duration::ZERO).await;
        let leading = matches!(step, Step::Leading { .. });
        steps.push(step);
        if leading {
            return steps;
        }
        client.pause(Duration::from_millis(50)).await;
    }
    panic!("no leadership in 50 steps: {steps:?}");
}

/// The election records `cell` holds.
fn records(cell: &Cell) -> Vec<ElectionRecord> {
    cell.records(journal())
        .iter()
        .map(|record| {
            ElectionRecord::decode(&record.0)
                .expect("an election record")
                .expect("well formed")
        })
        .collect()
}

#[test]
fn a_lone_candidate_takes_a_vacant_journal_and_renews() {
    let cell = cell();
    let (first, second, leader) = Rig::new().run_with(&cell, &[1], |client| async move {
        let mut one = election(&client, 1, 64);
        let first = step_until_leading(&client, &mut one).await;
        client.pause(Duration::from_millis(150)).await;
        let second = one.step(Duration::ZERO).await;
        (first, second, one.leading().cloned())
    });
    let Some(Step::Leading { leader: won, fresh }) = first.last().cloned() else {
        panic!("leading at the end: {first:?}");
    };
    assert!(fresh, "the step that won begins the term");
    assert_eq!(won.term, 1);
    assert_eq!(won.candidate.id, 1);
    assert_eq!(
        second,
        Step::Leading {
            leader: leader.clone().expect("still leading"),
            fresh: false
        }
    );
    let held = records(&cell);
    assert!(
        matches!(held[0], ElectionRecord::Campaign { term: 1, .. }),
        "{held:?}"
    );
    assert!(
        held[1..]
            .iter()
            .all(|record| matches!(record, ElectionRecord::Renew { term: 1, .. })),
        "{held:?}"
    );
    assert!(held.len() >= 3, "a renewal per step past the campaign");
}

#[test]
fn a_second_candidate_follows_a_live_leader() {
    let cell = cell();
    let followed = Rig::new().run_with(&cell, &[1], |client| async move {
        let mut one = election(&client, 1, 64);
        let mut two = election(&client, 2, 64);
        step_until_leading(&client, &mut one).await;
        let mut seen = Vec::new();
        for _ in 0..6 {
            one.step(Duration::ZERO).await;
            seen.push(two.step(Duration::ZERO).await);
            client.pause(Duration::from_millis(60)).await;
        }
        seen
    });
    for step in followed {
        let Step::Following {
            leader: Some(leader),
        } = step
        else {
            panic!("a live leader is followed: {step:?}");
        };
        assert_eq!(leader.candidate.id, 1);
    }
}

#[test]
fn a_campaign_deposes_the_leader_and_it_stops() {
    let cell = cell();
    let (won, deposed, expired) = Rig::new().run_with(&cell, &[1], |client| async move {
        let mut one = election(&client, 1, 64);
        let mut two = election(&client, 2, 64);
        step_until_leading(&client, &mut one).await;
        two.step(Duration::ZERO).await;
        let won = two.campaign(0).await;
        let deposed = one.step(Duration::ZERO).await;
        (won, deposed, one.expired(Duration::ZERO))
    });
    assert!(won, "a campaign for the next term wins");
    assert_eq!(deposed, Step::Deposed { term: 1 });
    assert!(!expired, "a live new leader holds the lease");
}

#[test]
fn a_leader_that_steps_down_is_replaced_once_its_lease_runs_out() {
    let cell = cell();
    let (gave_up, steps, again) = Rig::new().run_with(&cell, &[1], |client| async move {
        let mut one = election(&client, 1, 64);
        let mut two = election(&client, 2, 64);
        step_until_leading(&client, &mut one).await;
        let gave_up = one.step_down();
        let steps = step_until_leading(&client, &mut two).await;
        let again = one.step(Duration::ZERO).await;
        (gave_up, steps, again)
    });
    assert_eq!(gave_up, Some(1));
    let Some(Step::Leading { leader, .. }) = steps.last() else {
        panic!("two leads: {steps:?}");
    };
    assert_eq!((leader.term, leader.candidate.id), (2, 2));
    assert!(steps.len() > 1, "two waits out the lease first");
    assert!(matches!(again, Step::Following { leader: Some(_) }));
}

#[test]
fn a_resignation_hands_the_next_term_and_the_governed_journal_on() {
    let cell = cell();
    let probe = cell.clone();
    let (successor_uuid, step, moved, installed) =
        Rig::new().run_with(&cell, &[1], |client| async move {
            let mut one = election(&client, 1, 64);
            let mut two = election(&client, 2, 64);
            let Step::Leading { leader, .. } = step_until_leading(&client, &mut one)
                .await
                .pop()
                .expect("a step")
            else {
                panic!("one leads");
            };
            let installed = client.claim(governed(), leader.uuid, 0).await;
            two.step(Duration::ZERO).await;
            let successor = one.resign(Some(candidate(2))).await.expect("named");
            let step = two.step(Duration::ZERO).await;
            let moved = hand_off(&client, governed(), leader.uuid, successor, 0).await;
            (successor, step, moved, installed)
        });
    assert!(matches!(installed, crate::ClaimOutcome::Won { .. }));
    let Step::Leading { leader, fresh } = step else {
        panic!("the successor leads: {step:?}");
    };
    assert!(fresh);
    assert_eq!((leader.term, leader.candidate.id), (2, 2));
    assert_eq!(leader.uuid, successor_uuid);
    let SetLeaderOutcome::Won { state } = moved else {
        panic!("the hand-off moves the governed journal: {moved:?}");
    };
    assert_eq!(state.leader, Some(successor_uuid));
    assert_eq!(probe.records(governed()), Vec::new());
}

#[test]
fn a_resignation_without_a_successor_names_no_uuid() {
    let cell = cell();
    let (none, idle) = Rig::new().run_with(&cell, &[1], |client| async move {
        let mut one = election(&client, 1, 64);
        let idle = one.resign(Some(candidate(2))).await;
        step_until_leading(&client, &mut one).await;
        (one.resign(Some(candidate(1))).await, idle)
    });
    assert_eq!(idle, None, "a candidate that leads nothing resigns nothing");
    assert_eq!(none, None, "a successor that is itself is none");
    assert!(matches!(
        records(&cell).last(),
        Some(ElectionRecord::Resign {
            term: 1,
            candidate: 1,
            successor: None
        })
    ));
}

#[test]
fn the_leader_truncates_to_its_renewal_and_a_reader_anchors_there() {
    let cell = cell();
    let (fold, leader) = Rig::new().run_with(&cell, &[1], |client| async move {
        let mut one = election(&client, 1, 2);
        step_until_leading(&client, &mut one).await;
        for _ in 0..6 {
            client.pause(Duration::from_millis(110)).await;
            one.step(Duration::ZERO).await;
        }
        let fold = read_election(&client, journal(), 0).await;
        (fold, one.leading().cloned())
    });
    let state = cell.state(journal());
    assert!(state.first_seq.0 > 0, "the leader truncated: {state:?}");
    let fold = fold.expect("the reader reached the tail");
    let leader = leader.expect("one still leads");
    assert_eq!(
        fold.leader().map(|l| (l.term, l.candidate.id, l.uuid)),
        Some((leader.term, 1, leader.uuid))
    );
    assert!(fold.is_known());
}

#[test]
fn a_reader_of_an_unread_journal_gets_nothing() {
    let cell = cell();
    cell.script(1, |s| s.reads.extend((0..4).map(|_| Act::Hold)));
    let fold = Rig::new().run_with(&cell, &[1], |client| async move {
        read_election(&client, journal(), 0).await
    });
    assert_eq!(fold, None);
}

#[test]
fn a_reader_of_an_empty_journal_knows_nobody_leads() {
    let cell = cell();
    let fold = Rig::new()
        .run_with(&cell, &[1], |client| async move {
            read_election(&client, journal(), 0).await
        })
        .expect("read");
    assert!(fold.is_known());
    assert_eq!(fold.leader(), None);
}

#[test]
fn a_new_incarnation_deposes_its_own_earlier_term_at_once() {
    let cell = cell();
    let (expired, step) = Rig::new().run_with(&cell, &[1], |client| async move {
        let mut one = election(&client, 1, 64);
        step_until_leading(&client, &mut one).await;
        // The same candidate id, rebooted: another seed, no uuid minted.
        let mut reborn = Election::new(client.clone(), journal(), candidate(1), 99, tunables(64));
        reborn.watch(0).await;
        let expired = reborn.expired(Duration::ZERO);
        let step = reborn.step(Duration::ZERO).await;
        (expired, step)
    });
    assert!(expired);
    let Step::Leading { leader, fresh } = step else {
        panic!("the new incarnation takes over: {step:?}");
    };
    assert!(fresh);
    assert_eq!(leader.term, 2);
}

#[test]
fn a_leader_renews_between_duties_only_when_due() {
    let cell = cell();
    let (idle, due_now, renewed, due_later) =
        Rig::new().run_with(&cell, &[1], |client| async move {
            let mut one = election(&client, 1, 64);
            let idle = one.renew_if_due().await;
            step_until_leading(&client, &mut one).await;
            let due_now = one.renewal_due_in();
            client.pause(Duration::from_millis(120)).await;
            let renewed = one.renew_if_due().await;
            (idle, due_now, renewed, one.renewal_due_in())
        });
    assert!(!idle, "a candidate that leads nothing renews nothing");
    assert!(due_now > Duration::ZERO, "just renewed: {due_now:?}");
    assert!(renewed);
    assert!(due_later > Duration::from_millis(50), "{due_later:?}");
}
