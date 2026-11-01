use std::collections::BTreeMap;

use super::*;
use crate::machine::{Class, Locality};
use crate::registry::SystemCommand;
use paros_core::journal::{JournalId, TenantId};

fn journal(n: u64) -> JournalIdentifier {
    JournalIdentifier::new(TenantId(7), JournalId(n))
}

/// Fold `command` at the registry's next position.
fn fold(reg: &mut Registry, command: &SystemCommand) {
    let seq = reg.next_seq();
    reg.fold(seq, &command.encode());
}

/// A registry of `nodes` storage machines in zone `z`, each admitted, with
/// the acceptor bookings `bookings` names (booking id, node, journal).
fn registry(nodes: &[u64], bookings: &[(u64, u64, u64)]) -> Registry {
    let mut reg = Registry::new(std::iter::empty());
    for &id in nodes {
        fold(
            &mut reg,
            &SystemCommand::RegisterNode {
                id: NodeId(id),
                addr: format!("n{id}"),
                class: Class::Storage,
                locality: Locality::zone("z"),
                name: format!("m{id}"),
                incarnation: 1,
                machine_interface: Vec::new(),
            },
        );
        fold(&mut reg, &SystemCommand::MachineAdmitted { id: NodeId(id) });
    }
    for &(booking, node, j) in bookings {
        fold(
            &mut reg,
            &SystemCommand::BookCapacity {
                booking,
                node: NodeId(node),
                role: Role::Acceptor,
                target: BookingTarget::Journal(journal(j)),
            },
        );
        assert_eq!(reg.booking(booking).map(|b| b.node), Some(NodeId(node)));
    }
    reg
}

fn machines(reg: &Registry) -> Vec<placement::Machine> {
    placement::machines(reg, reg.pool())
}

/// The machines with busyness steps `busy`.
fn busy(reg: &Registry, busy: &[(u64, u8)]) -> Vec<placement::Machine> {
    let busy: BTreeMap<NodeId, u8> = busy.iter().map(|&(n, b)| (NodeId(n), b)).collect();
    placement::with_busyness(machines(reg), &busy)
}

/// Every machine stays.
fn all(_: NodeId) -> bool {
    true
}

const BOUNDS: Bounds = Bounds {
    spread: 1,
    rounds: 2,
    busy_spread: 3,
    leader_spread: 1,
};

const AT_ONCE: Bounds = Bounds {
    rounds: 1,
    ..BOUNDS
};

/// Three journals of one acceptor each on machine 1, none on 2: a new
/// machine joined.
fn joined() -> Registry {
    registry(&[1, 2], &[(10, 1, 1), (11, 1, 2), (12, 1, 3)])
}

/// Record `step`'s move as the coordinator does.
fn record(reg: &mut Registry, step: Step) -> Rebalance {
    let Step::Start(r) = step else {
        panic!("a move starts, not {step:?}")
    };
    fold(
        reg,
        &SystemCommand::BalanceStarted {
            booking: r.booking,
            journal: r.journal,
            from: r.from,
            to: r.to,
            reason: r.reason,
        },
    );
    assert_eq!(reg.balance().moving().map(|m| m.booking), Some(r.booking));
    r
}

#[test]
fn a_gap_held_for_its_rounds_moves_the_first_acceptor_off_the_loaded_machine() {
    let mut reg = joined();
    let mut balance = Balance::new(BOUNDS);
    assert_eq!(
        balance.round(&reg, &machines(&reg), all),
        Step::Idle,
        "one round is no trend"
    );
    let step = balance.round(&reg, &machines(&reg), all);
    let expected = Rebalance {
        journal: journal(1),
        booking: 10,
        from: NodeId(1),
        to: NodeId(2),
        reason: BalanceReason::Roles,
    };
    assert_eq!(step, Step::Start(expected));
    record(&mut reg, step);
    // In flight, it is named again every round.
    assert_eq!(
        balance.round(&reg, &machines(&reg), all),
        Step::Moving(expected)
    );
}

#[test]
fn a_move_ends_once_the_registry_releases_its_booking() {
    let mut reg = joined();
    let mut balance = Balance::new(AT_ONCE);
    {
        let step = balance.round(&reg, &machines(&reg), all);
        record(&mut reg, step)
    };
    fold(&mut reg, &SystemCommand::ReleaseCapacity { booking: 10 });
    assert!(reg.balance().moving().is_none(), "the release ended it");
    assert!(
        !matches!(balance.round(&reg, &machines(&reg), all), Step::Moving(_)),
        "an ended move is not in flight"
    );
    assert!(
        balance.moving.is_none(),
        "nothing is in flight once it ended"
    );
}

#[test]
fn a_move_that_does_not_end_is_abandoned() {
    let mut reg = joined();
    let mut balance = Balance::new(AT_ONCE);
    let chosen = {
        let step = balance.round(&reg, &machines(&reg), all);
        record(&mut reg, step)
    };
    for _ in 0..ABANDON_ROUNDS {
        assert_eq!(
            balance.round(&reg, &machines(&reg), all),
            Step::Moving(chosen)
        );
    }
    assert_eq!(
        balance.round(&reg, &machines(&reg), all),
        Step::Abandon(10),
        "abandoned"
    );
}

#[test]
fn a_later_term_goes_on_with_the_move_in_flight() {
    let mut reg = joined();
    let chosen = {
        let step = Balance::new(AT_ONCE).round(&reg, &machines(&reg), all);
        record(&mut reg, step)
    };
    let mut later = Balance::new(BOUNDS);
    assert_eq!(
        later.round(&reg, &machines(&reg), all),
        Step::Moving(chosen)
    );
}

#[test]
fn a_paused_balancer_starts_nothing() {
    let mut reg = joined();
    fold(
        &mut reg,
        &SystemCommand::SetBalance {
            request: 1,
            paused: true,
        },
    );
    let mut balance = Balance::new(AT_ONCE);
    assert_eq!(
        judge(&reg, &machines(&reg), all, AT_ONCE),
        Judgement::Paused
    );
    assert_eq!(balance.round(&reg, &machines(&reg), all), Step::Idle);
    fold(
        &mut reg,
        &SystemCommand::SetBalance {
            request: 2,
            paused: false,
        },
    );
    assert!(matches!(
        balance.round(&reg, &machines(&reg), all),
        Step::Start(_)
    ));
}

#[test]
fn a_cell_within_the_spread_moves_nothing() {
    let reg = registry(&[1, 2], &[(10, 1, 1), (11, 1, 2), (12, 2, 3)]);
    let mut balance = Balance::new(AT_ONCE);
    assert_eq!(
        judge(&reg, &machines(&reg), all, AT_ONCE),
        Judgement::Balanced
    );
    for _ in 0..4 {
        assert_eq!(balance.round(&reg, &machines(&reg), all), Step::Idle);
    }
}

#[test]
fn repair_comes_before_balance() {
    let reg = joined();
    let mut balance = Balance::new(AT_ONCE);
    // A machine gone (#475) holds every move back.
    let gone = |node: NodeId| node != NodeId(2);
    assert_eq!(
        judge(&reg, &machines(&reg), gone, AT_ONCE),
        Judgement::Repairing(Repair::Unwell(NodeId(2)))
    );
    assert_eq!(balance.round(&reg, &machines(&reg), gone), Step::Idle);
    // A draining machine too.
    let mut draining = registry(&[1, 2, 3], &[(10, 1, 1), (11, 1, 2), (12, 1, 3)]);
    fold(&mut draining, &SystemCommand::DrainNode { id: NodeId(3) });
    assert_eq!(
        judge(&draining, &machines(&draining), all, AT_ONCE),
        Judgement::Repairing(Repair::Draining(NodeId(3)))
    );
    assert_eq!(
        balance.round(&draining, &machines(&draining), all),
        Step::Idle
    );
    assert!(repairing(&reg, all).is_none());
}

#[test]
fn an_acceptor_whose_journal_is_on_the_light_machine_stays() {
    // Journal 1 has a member on both: only journal 2 may move.
    let reg = registry(&[1, 2], &[(10, 1, 1), (11, 1, 2), (12, 1, 4), (13, 2, 1)]);
    let Judgement::Unbalanced(gap) = judge(&reg, &machines(&reg), all, AT_ONCE) else {
        panic!("a gap of two")
    };
    assert_eq!(choose(&reg, &gap).map(|r| r.journal), Some(journal(2)));
    // Every journal on the loaded machine also sits on the light one.
    let shared = registry(&[1, 2], &[(10, 1, 1), (11, 2, 1)]);
    let gap = Gap {
        zone: "z".into(),
        reason: BalanceReason::Roles,
        from: (NodeId(1), 1, 0),
        to: (NodeId(2), 1, 0),
    };
    assert_eq!(choose(&shared, &gap), None);
}

#[test]
fn the_widest_gap_names_the_most_and_the_least_loaded_machine() {
    let reg = registry(
        &[1, 2, 3],
        &[(10, 1, 1), (11, 1, 2), (12, 1, 3), (13, 2, 4)],
    );
    let gap = widest_gap(&machines(&reg), all, AT_ONCE).expect("a gap of three");
    assert_eq!((gap.from.0, gap.to.0), (NodeId(1), NodeId(3)));
    assert_eq!(gap.reason, BalanceReason::Roles);
    let wide = Bounds {
        spread: 3,
        ..AT_ONCE
    };
    assert_eq!(widest_gap(&machines(&reg), all, wide), None);
}

#[test]
fn a_busy_machine_gives_an_acceptor_to_an_idle_one_when_the_roles_allow() {
    // Machine 1 hosts one role more than machine 2, and is far busier.
    let reg = registry(&[1, 2], &[(10, 1, 1), (11, 1, 2), (12, 2, 3)]);
    let hot = busy(&reg, &[(1, 8), (2, 1)]);
    let Judgement::Unbalanced(gap) = judge(&reg, &hot, all, AT_ONCE) else {
        panic!("a busy gap")
    };
    assert_eq!(gap.reason, BalanceReason::Busy);
    assert_eq!((gap.from.0, gap.to.0), (NodeId(1), NodeId(2)));
    // A gap below the busy spread is none.
    let warm = busy(&reg, &[(1, 3), (2, 1)]);
    assert_eq!(judge(&reg, &warm, all, AT_ONCE), Judgement::Balanced);
    // Equal roles: the move would leave the idle machine two roles above,
    // and the roles balance would move it back.
    let even = registry(&[1, 2], &[(10, 1, 1), (11, 2, 2)]);
    let hot = busy(&even, &[(1, 9), (2, 0)]);
    assert_eq!(judge(&even, &hot, all, AT_ONCE), Judgement::Balanced);
    // With a spread of two, it may.
    let wide = Bounds {
        spread: 2,
        ..AT_ONCE
    };
    assert!(matches!(
        judge(&even, &hot, all, wide),
        Judgement::Unbalanced(Gap {
            reason: BalanceReason::Busy,
            ..
        })
    ));
}

#[test]
fn the_machine_that_leads_the_most_hands_one_to_a_member_that_leads_fewer() {
    // Journals 1, 2 and 3 have members on machines 1 and 2; machine 1 leads
    // all three.
    let reg = registry(
        &[1, 2],
        &[
            (10, 1, 1),
            (11, 2, 1),
            (12, 1, 2),
            (13, 2, 2),
            (14, 1, 3),
            (15, 2, 3),
        ],
    );
    let leading: Leading = [(NodeId(1), [journal(1), journal(2), journal(3)].into())].into();
    let handoff = leader_gap(&reg, &machines(&reg), all, &leading, 1);
    assert_eq!(
        handoff,
        Some(LeaderMove {
            journal: journal(1),
            from: NodeId(1),
            to: NodeId(2),
        })
    );
    // Within the spread, nothing moves.
    let even: Leading = [
        (NodeId(1), [journal(1), journal(2)].into()),
        (NodeId(2), [journal(3)].into()),
    ]
    .into();
    assert_eq!(leader_gap(&reg, &machines(&reg), all, &even, 1), None);
    // A journal with no member elsewhere hands nothing on.
    let alone = registry(&[1, 2], &[(10, 1, 1), (11, 1, 2), (12, 1, 3)]);
    assert_eq!(
        leader_gap(&alone, &machines(&alone), all, &leading, 1),
        None
    );
}

#[test]
fn leaders_balance_only_on_a_balanced_cell_after_their_rounds() {
    let reg = registry(
        &[1, 2],
        &[
            (10, 1, 1),
            (11, 2, 1),
            (12, 1, 2),
            (13, 2, 2),
            (14, 1, 3),
            (15, 2, 3),
        ],
    );
    let leading: Leading = [(NodeId(1), [journal(1), journal(2), journal(3)].into())].into();
    let mut balance = Balance::new(BOUNDS);
    // No round judged the cell yet.
    assert_eq!(
        balance.leader_round(&reg, &machines(&reg), all, &leading),
        None
    );
    assert_eq!(balance.round(&reg, &machines(&reg), all), Step::Idle);
    assert_eq!(
        balance.leader_round(&reg, &machines(&reg), all, &leading),
        None,
        "one round is no trend"
    );
    assert_eq!(balance.round(&reg, &machines(&reg), all), Step::Idle);
    assert!(
        balance
            .leader_round(&reg, &machines(&reg), all, &leading)
            .is_some()
    );
    // An unbalanced cell hands no leadership on.
    let joined = joined();
    let mut balance = Balance::new(AT_ONCE);
    let _ = balance.round(&joined, &machines(&joined), all);
    assert_eq!(
        balance.leader_round(&joined, &machines(&joined), all, &leading),
        None
    );
}
