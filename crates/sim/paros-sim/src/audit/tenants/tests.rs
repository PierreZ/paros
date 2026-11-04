//! The tenant-control oracles of [`TenantBoard`] on hand-built folds: an
//! honest sequence of tenant events stays clean, and each broken event
//! goes red at the oracle that owns it.

use moonpool_sim::{has_always_violations, reset_always_violations};
use paros::core::journal::{
    Command, Control, Entry, JournalId, JournalIdentifier, LeaderUuid, Seq, TenantId, WriterMode,
};
use paros::core::membership::{AcceptorConfig, FailureDomain, QuorumSystem};
use paros::core::{NodeId, Value};
use paros::model::tenant::{
    Desired, Durability, RequestOutcome, ScrubCounts, ScrubProgress, StoreDurability, TenantEvent,
    TenantRefusal,
};

use super::{TenantBoard, Unreported};

const CONTROL: JournalIdentifier = JournalIdentifier::new(TenantId(5), JournalId(1));
const MACHINE: NodeId = NodeId(10);

fn journal(id: u64) -> JournalIdentifier {
    JournalIdentifier::new(CONTROL.tenant, JournalId(id))
}

fn red(body: impl FnOnce()) -> bool {
    reset_always_violations();
    body();
    let red = has_always_violations();
    reset_always_violations();
    red
}

fn three() -> AcceptorConfig {
    AcceptorConfig::new(
        vec![NodeId(1), NodeId(2), NodeId(3)],
        QuorumSystem::Majority,
    )
}

fn created(request: u64, id: u64, name: &[u8]) -> TenantEvent {
    TenantEvent::Created {
        request,
        id: JournalId(id),
        name: name.to_vec(),
        config: three(),
        writer: WriterMode::Single,
        store: StoreDurability::Ordered,
        desired: Desired::DEFAULT,
    }
}

fn deleted(request: u64, id: u64) -> TenantEvent {
    TenantEvent::Deleted {
        request,
        id: JournalId(id),
    }
}

/// A board that folded a create of journal 7 named `a` at lsn 1.
fn board() -> TenantBoard {
    let mut board = TenantBoard::default();
    board.folded(MACHINE, CONTROL, 0, &TenantEvent::Described);
    board.folded(MACHINE, CONTROL, 1, &created(100, 7, b"a"));
    board
}

#[test]
fn an_honest_tenant_history_stays_clean_and_raises_its_gates() {
    assert!(!red(|| {
        let mut board = board();
        let events = [
            TenantEvent::Tagged {
                request: 101,
                id: JournalId(7),
            },
            TenantEvent::Answered {
                request: 102,
                outcome: RequestOutcome::NameTaken(JournalId(7)),
            },
            TenantEvent::Refused(TenantRefusal::IdTaken { id: JournalId(7) }),
            TenantEvent::Repeated {
                request: 100,
                outcome: RequestOutcome::Created(JournalId(7)),
            },
            TenantEvent::Configured {
                request: 103,
                id: Some(JournalId(7)),
                desired: Desired::DEFAULT,
            },
            TenantEvent::Configured {
                request: 104,
                id: None,
                desired: Desired::DEFAULT,
            },
            TenantEvent::Placed {
                id: JournalId(7),
                config: three(),
            },
            deleted(105, 7),
            created(106, 8, b"a"),
            TenantEvent::Checkpoint { covers_up_to: 3 },
        ];
        for (lsn, event) in (2..).zip(&events) {
            board.folded(MACHINE, CONTROL, lsn, event);
            // A second machine folds every position alike.
            board.folded(NodeId(11), CONTROL, lsn, event);
        }
        assert!(board.armed && board.name_taken && board.id_taken && board.name_reused);
        board.check_gates();
    }));
}

#[test]
fn two_machines_that_fold_one_position_differently_are_red() {
    let mut board = board();
    assert!(red(|| board.folded(
        NodeId(11),
        CONTROL,
        1,
        &created(100, 8, b"a")
    )));
}

#[test]
fn a_created_journal_takes_a_set_id_never_used_before() {
    let mut board = board();
    assert!(red(|| board.folded(
        MACHINE,
        CONTROL,
        2,
        &created(101, 0, b"b")
    )));
    let mut board = self::board();
    assert!(
        red(|| board.folded(MACHINE, CONTROL, 2, &created(101, 1, b"b"))),
        "the control journal's id"
    );
    let mut board = self::board();
    assert!(
        red(|| board.folded(MACHINE, CONTROL, 2, &created(101, 7, b"b"))),
        "a used id"
    );
}

#[test]
fn a_request_folds_to_one_outcome() {
    let mut board = board();
    let other = TenantEvent::Answered {
        request: 100,
        outcome: RequestOutcome::Unplaceable,
    };
    assert!(red(|| board.folded(MACHINE, CONTROL, 2, &other)));
    let mut board = self::board();
    let repeated = TenantEvent::Repeated {
        request: 100,
        outcome: RequestOutcome::Unplaceable,
    };
    assert!(red(|| board.folded(MACHINE, CONTROL, 2, &repeated)));
    let mut board = self::board();
    let unknown = TenantEvent::Repeated {
        request: 999,
        outcome: RequestOutcome::Unplaceable,
    };
    assert!(!red(|| board.folded(MACHINE, CONTROL, 2, &unknown)));
}

#[test]
fn a_journal_event_names_a_journal_the_tenant_created_before() {
    let events = [
        TenantEvent::Tagged {
            request: 101,
            id: JournalId(9),
        },
        deleted(101, 9),
        TenantEvent::Answered {
            request: 101,
            outcome: RequestOutcome::NameTaken(JournalId(9)),
        },
        TenantEvent::Refused(TenantRefusal::IdTaken { id: JournalId(9) }),
        TenantEvent::Configured {
            request: 101,
            id: Some(JournalId(9)),
            desired: Desired::DEFAULT,
        },
        TenantEvent::Placed {
            id: JournalId(9),
            config: three(),
        },
    ];
    for event in &events {
        let mut board = board();
        assert!(
            red(|| board.folded(MACHINE, CONTROL, 2, event)),
            "{event:?}"
        );
    }
    // The control journal's own id is always taken.
    let mut board = board();
    let own = TenantEvent::Refused(TenantRefusal::IdTaken { id: JournalId(1) });
    assert!(!red(|| board.folded(MACHINE, CONTROL, 2, &own)));
}

#[test]
fn a_journal_is_deleted_once_and_a_tag_after_its_tombstone_is_red() {
    let mut board = board();
    board.folded(MACHINE, CONTROL, 2, &deleted(101, 7));
    assert!(board.tombstoned(journal(7)));
    assert!(board.live_config(journal(7)).is_none());
    assert!(red(|| board.folded(MACHINE, CONTROL, 3, &deleted(102, 7))));
    let tag = TenantEvent::Tagged {
        request: 103,
        id: JournalId(7),
    };
    assert!(red(|| board.folded(MACHINE, CONTROL, 4, &tag)));
    // A machine that re-folds below the tombstone is judged by lsn.
    let mut board = self::board();
    board.folded(MACHINE, CONTROL, 3, &deleted(101, 7));
    assert!(!red(|| board.folded(NodeId(11), CONTROL, 2, &tag)));
}

#[test]
fn a_node_serves_nothing_of_a_journal_after_folding_its_tombstone() {
    let mut board = board();
    assert!(!red(|| {
        board.acked(MACHINE, journal(7));
        board.served(MACHINE, journal(7));
    }));
    board.folded(MACHINE, CONTROL, 2, &deleted(101, 7));
    assert!(red(|| board.acked(MACHINE, journal(7))));
    assert!(red(|| board.served(MACHINE, journal(7))));
    assert!(
        !red(|| board.acked(NodeId(11), journal(7))),
        "another machine"
    );
}

#[test]
fn a_node_starts_only_a_genesis_journal_or_one_a_tenant_created() {
    let mut board = board();
    board.stopped(MACHINE, journal(7));
    assert_eq!(board.stopped_on(journal(7)), [10].into());
    assert!(!red(|| {
        board.started(MACHINE, journal(7));
        board.started(MACHINE, CONTROL);
        // A tenant no machine folded is not judged.
        board.started(MACHINE, JournalIdentifier::new(TenantId(6), JournalId(3)));
    }));
    assert!(
        board.stopped_on(journal(7)).is_empty(),
        "a start takes the stop back"
    );
    assert!(red(|| board.started(MACHINE, journal(9))));
}

#[test]
fn a_created_journal_keeps_its_store_durability_mode_and_config() {
    let mut board = board();
    assert_eq!(board.store(journal(7)), Some(StoreDurability::Ordered));
    assert_eq!(board.mode(journal(7)), Some(WriterMode::Single));
    assert_eq!(board.live_config(journal(7)), Some(&three()));
    assert_eq!(board.created_modes().len(), 1);
    assert_eq!(board.controls(), [CONTROL].into());
    let batched = TenantEvent::Created {
        request: 100,
        id: JournalId(7),
        name: b"a".to_vec(),
        config: three(),
        writer: WriterMode::Single,
        store: StoreDurability::Batched,
        desired: Desired::DEFAULT,
    };
    assert!(red(|| board.folded(NodeId(11), CONTROL, 9, &batched)));
}

#[test]
fn a_batched_journal_that_boots_faulty_lost_its_last_batch() {
    let mut board = board();
    board.note_faulty(journal(7));
    assert!(
        !board.batch_lost(journal(7)),
        "an ordered store loses nothing"
    );
    let batched = TenantEvent::Created {
        request: 101,
        id: JournalId(8),
        name: b"b".to_vec(),
        config: three(),
        writer: WriterMode::Multi,
        store: StoreDurability::Batched,
        desired: Desired::DEFAULT,
    };
    board.folded(MACHINE, CONTROL, 2, &batched);
    board.note_faulty(journal(8));
    assert!(board.batch_lost(journal(8)));
}

#[test]
fn the_desired_acceptors_follow_the_latest_position_folded() {
    let mut board = board();
    assert_eq!(board.desired_acceptors(journal(7)), Some(3));
    let five = Desired {
        durability: Durability::new(2, 1).unwrap(),
        ..Desired::DEFAULT
    };
    let configure = |desired| TenantEvent::Configured {
        request: 101,
        id: Some(JournalId(7)),
        desired,
    };
    board.folded(MACHINE, CONTROL, 4, &configure(five));
    assert_eq!(board.desired_acceptors(journal(7)), Some(5));
    // A lagging machine folds an older configure: the newer one holds.
    board.set_desired(journal(7), 3, Desired::DEFAULT);
    assert_eq!(board.desired_acceptors(journal(7)), Some(5));
    assert_eq!(board.desired_acceptors(journal(9)), None);
}

#[test]
fn a_name_resolves_to_the_live_journal_that_holds_it() {
    let mut board = board();
    assert_eq!(board.named_at(CONTROL, b"a", 2), Ok(Some(JournalId(7))));
    assert_eq!(board.named_at(CONTROL, b"b", 2), Ok(None));
    assert_eq!(board.named_at(CONTROL, b"a", 3), Err(Unreported));
    board.folded(MACHINE, CONTROL, 2, &deleted(101, 7));
    assert_eq!(board.named_at(CONTROL, b"a", 3), Ok(None));
    // A machine that folds the same position again changes no name.
    board.folded(NodeId(11), CONTROL, 1, &created(100, 7, b"a"));
    assert_eq!(board.named_at(CONTROL, b"a", 2), Ok(Some(JournalId(7))));
}

#[test]
fn a_released_replica_slot_ends_its_replica() {
    let mut board = board();
    board.replica_booked(1, NodeId(3), journal(7));
    board.replica_booked(2, NodeId(4), journal(7));
    board.replica_booked(3, NodeId(3), journal(8));
    board.booking_released(1);
    assert_eq!(board.stopped_on(journal(7)), [3].into());
    board.booking_released(1);
    board.node_retired(NodeId(3));
    assert_eq!(board.stopped_on(journal(8)), [3].into());
    assert_eq!(
        board.stopped_on(journal(7)),
        [3].into(),
        "node 4 still serves"
    );
}

#[test]
fn a_dropped_tenant_is_remembered() {
    let mut board = board();
    assert!(!board.dropped(CONTROL.tenant));
    board.tenant_dropped(CONTROL.tenant);
    assert!(board.dropped(CONTROL.tenant));
}

#[test]
fn a_call_a_frontend_had_to_deny_never_changes_a_journal() {
    let mut board = board();
    let write = |bytes: &[u8]| {
        Command::Write(Entry {
            leader: LeaderUuid(1),
            seq: Seq(0),
            records: vec![Value(bytes.to_vec())],
        })
    };
    let claim = |uuid| {
        Command::Control(Control::SetLeader {
            new: LeaderUuid(uuid),
            old: None,
        })
    };
    assert!(
        !red(|| board.applied(MACHINE, &write(b"evil"))),
        "nothing denied yet"
    );
    board.must_deny(Some(b"evil".to_vec()), Some(66));
    assert!(red(|| board.applied(MACHINE, &write(b"evil"))));
    assert!(red(|| board.applied(MACHINE, &claim(66))));
    assert!(!red(|| {
        board.applied(MACHINE, &write(b"fine"));
        board.applied(MACHINE, &claim(67));
        board.applied(MACHINE, &Command::Control(Control::Noop));
    }));
}

#[test]
fn a_scrub_never_finds_two_values_under_one_ballot() {
    let progress = |mismatched_total| {
        TenantEvent::Scrubbed(ScrubProgress {
            pass: 1,
            done: false,
            journal: JournalId(7),
            next_slot: 0,
            counts: ScrubCounts::default(),
            damaged_total: 0,
            repaired_total: 0,
            mismatched_total,
            late: false,
        })
    };
    let mut board = board();
    assert!(!red(|| board.folded(MACHINE, CONTROL, 2, &progress(0))));
    assert!(red(|| board.folded(MACHINE, CONTROL, 3, &progress(1))));
}

#[test]
fn a_journal_over_three_zones_survives_any_one() {
    let zoned = |zones: [&str; 3]| {
        AcceptorConfig::zoned(
            (1..=3)
                .zip(zones)
                .map(|(id, az)| (NodeId(id), FailureDomain::new("r", az)))
                .collect(),
            QuorumSystem::Majority,
        )
    };
    let event = |config| TenantEvent::Created {
        request: 101,
        id: JournalId(8),
        name: b"b".to_vec(),
        config,
        writer: WriterMode::Single,
        store: StoreDurability::Ordered,
        desired: Desired::DEFAULT,
    };
    let mut board = board();
    assert!(!red(|| board.folded(
        MACHINE,
        CONTROL,
        2,
        &event(zoned(["a", "b", "c"]))
    )));
    let mut board = self::board();
    assert!(
        !red(|| board.folded(MACHINE, CONTROL, 2, &event(zoned(["a", "a", "b"])))),
        "two zones: the rule does not bind"
    );
    // Five acceptors, three in zone a: losing it loses the quorum.
    let lopsided = AcceptorConfig::zoned(
        (1..=5)
            .zip(["a", "a", "a", "b", "c"])
            .map(|(id, az)| (NodeId(id), FailureDomain::new("r", az)))
            .collect(),
        QuorumSystem::Majority,
    );
    let mut board = self::board();
    assert!(red(|| board.folded(MACHINE, CONTROL, 2, &event(lopsided))));
}
