use std::time::Duration;

use paros_core::journal::{JournalId, JournalIdentifier, TenantId};

use paros_model::load::{Busyness, DiskBusyness};

use super::{CallKind, CallOutcome, JournalUsage, Metrics, Phase, Totals};

fn journal(n: u64) -> JournalIdentifier {
    JournalIdentifier::new(TenantId(7), JournalId(n))
}

fn names() -> (String, String) {
    ("acme".into(), "orders".into())
}

#[test]
fn metrics_off_record_nothing() {
    let metrics = Metrics::off();
    assert!(!metrics.is_on());
    assert!(metrics.journal(journal(1), names).is_none());
    metrics.settled(journal(1), 3, 4, Duration::from_millis(1));
    assert!(metrics.usage().is_empty());
    assert!(metrics.render().is_empty());
    assert_eq!(metrics.phase(), Phase::Starting);
    metrics.set_phase(Phase::Serving);
    assert!(metrics.ready(), "readiness does not need metrics");
}

#[test]
fn calls_and_bytes_fold_into_usage_and_the_text() {
    let metrics = Metrics::on();
    let handles = metrics.journal(journal(1), names).expect("on");
    handles.received(CallKind::Write);
    handles.received(CallKind::Write);
    handles.received(CallKind::Read);
    handles.answered(
        (CallKind::Write, CallOutcome::Ok),
        Duration::from_millis(2),
        100,
        0,
    );
    handles.answered(
        (CallKind::Write, CallOutcome::Refused),
        Duration::from_millis(1),
        0,
        0,
    );
    handles.answered(
        (CallKind::Read, CallOutcome::Ok),
        Duration::from_millis(1),
        0,
        40,
    );
    metrics.settled(journal(1), 5, 7, Duration::from_micros(300));
    let usage = metrics.usage();
    assert_eq!(usage.len(), 1);
    assert_eq!(usage[0].calls_of(CallKind::Write), 2);
    assert_eq!(usage[0].calls_of(CallKind::Read), 1);
    assert_eq!(usage[0].total_calls(), 3);
    assert_eq!((usage[0].written_bytes, usage[0].read_bytes), (100, 40));
    let text = metrics.render();
    // The text sorts the labels by name.
    let journal = r#"journal="orders",journal_id="1",tenant="acme",tenant_id="7""#;
    assert!(text.contains(&format!(r#"paros_calls_total{{call="write",{journal}}} 2"#)));
    assert!(text.contains(
        r#"paros_call_answers_total{call="write",journal="orders",journal_id="1",outcome="refused",tenant="acme",tenant_id="7"} 1"#
    ));
    assert!(text.contains(&format!(
        r#"paros_call_answer_seconds_total{{call="write",{journal}}} 0.003"#
    )));
    assert!(text.contains(&format!("paros_written_bytes_total{{{journal}}} 100")));
    assert!(text.contains(&format!("paros_chosen_slot{{{journal}}} 7")));
    assert!(text.contains("paros_settles_total 1"));
    assert!(!text.contains("_bucket"), "no histogram");
}

#[test]
fn a_restart_with_the_same_metrics_keeps_counting() {
    let first = Metrics::on();
    first
        .journal(journal(1), names)
        .expect("on")
        .received(CallKind::Truncate);
    first.set_phase(Phase::Serving);
    // The simulation hands a rebooted machine the metrics it had.
    let second = first.clone();
    second.set_phase(Phase::Starting);
    assert!(!first.ready(), "a restart starts again");
    second
        .journal(journal(1), names)
        .expect("on")
        .received(CallKind::Truncate);
    assert_eq!(second.usage()[0].calls_of(CallKind::Truncate), 2);
}

#[test]
fn call_labels_round_trip() {
    for call in CallKind::ALL {
        assert_eq!(CallKind::of_label(call.label()), Some(call));
    }
    assert_eq!(CallKind::of_label("delete"), None);
    for outcome in CallOutcome::ALL {
        assert_eq!(CallOutcome::of_label(outcome.label()), Some(outcome));
    }
    assert_eq!(CallOutcome::of_label("maybe"), None);
}

#[test]
fn totals_read_back_every_journal_from_the_registry() {
    let metrics = Metrics::on();
    assert_eq!(super::Totals::of(&metrics), super::Totals::default());
    assert_eq!(super::Totals::of(&Metrics::off()), super::Totals::default());
    for n in 1..=2 {
        let handles = metrics
            .journal(journal(n), || ("acme".into(), format!("j{n}")))
            .expect("on");
        handles.received(CallKind::Write);
        handles.answered((CallKind::Write, CallOutcome::Ok), Duration::ZERO, 10, 0);
        handles.received(CallKind::Read);
        handles.received(CallKind::Read);
        handles.answered((CallKind::Read, CallOutcome::Ok), Duration::ZERO, 0, 3);
    }
    let totals = super::Totals::of(&metrics);
    assert_eq!(totals.calls, [2, 4, 0, 0]);
    assert_eq!(totals.answers, [2, 2, 0, 0], "a read is still unanswered");
    assert_eq!((totals.written_bytes, totals.read_bytes), (20, 6));
    assert_eq!(totals.total_calls(), 6);
}

#[test]
fn families_list_every_family_before_its_first_series() {
    assert!(Metrics::off().families().is_empty());
    let metrics = Metrics::on();
    let families = metrics.families();
    let named = |name: &str| families.iter().find(|f| f.name == name).cloned();
    let calls = named("paros_calls_total").expect("listed with no series yet");
    assert_eq!(calls.kind, super::FamilyKind::Counter);
    let slot = named("paros_applied_slot").expect("listed");
    assert_eq!(slot.kind, super::FamilyKind::Gauge);
    let gathered = metrics.registry().expect("on").gather().len();
    assert_eq!(families.len(), 19);
    assert!(
        gathered < families.len(),
        "a family with no series is not gathered"
    );
    for family in &families {
        assert!(family.name.starts_with("paros_"), "{}", family.name);
        assert_eq!(
            family.name.ends_with("_total"),
            family.kind == super::FamilyKind::Counter,
            "{}",
            family.name
        );
    }
}

#[test]
fn each_phase_is_named_read_back_and_exported() {
    let metrics = Metrics::on();
    for (phase, name, value) in [
        (Phase::Starting, "starting", 0),
        (Phase::Idle, "idle", 1),
        (Phase::Serving, "serving", 2),
    ] {
        assert_eq!(phase.as_str(), name);
        metrics.set_phase(phase);
        assert_eq!(metrics.phase(), phase);
        assert_eq!(metrics.ready(), phase == Phase::Serving);
        assert!(
            metrics
                .render()
                .contains(&format!("paros_machine_phase {value}\n")),
            "{name}"
        );
    }
}

fn busyness(disk: Option<(f64, u64)>) -> Busyness {
    Busyness {
        elapsed: Duration::from_secs(1),
        cpu_cores: 1.5,
        cores: 4,
        machine_cpu: 0.25,
        run_loop_busy: None,
        disk: disk.map(|(busy, queue_depth)| DiskBusyness {
            device: "vda".into(),
            busy,
            queue_depth,
            reads_hz: 0.0,
            writes_hz: 0.0,
            read_bps: 0.0,
            write_bps: 0.0,
        }),
    }
}

#[test]
fn a_busyness_window_sets_the_machine_gauges() {
    let metrics = Metrics::on();
    metrics.busyness(&busyness(None));
    let text = metrics.render();
    assert!(text.contains("paros_machine_load_windows_total 1\n"));
    assert!(text.contains("paros_machine_cpu_cores 1.5\n"));
    assert!(text.contains("paros_machine_cpu_ratio 0.25\n"));
    assert!(text.contains("paros_machine_disk_busy_ratio 0\n"));
    assert!(text.contains("paros_machine_disk_queue_depth 0\n"));
    metrics.busyness(&busyness(Some((0.75, 3))));
    let text = metrics.render();
    assert!(text.contains("paros_machine_load_windows_total 2\n"));
    assert!(text.contains("paros_machine_disk_busy_ratio 0.75\n"));
    assert!(text.contains("paros_machine_disk_queue_depth 3\n"));
}

#[test]
fn a_busyness_window_with_metrics_off_records_nothing() {
    let metrics = Metrics::off();
    metrics.busyness(&busyness(Some((0.75, 3))));
    assert!(metrics.render().is_empty());
}

#[test]
fn usage_adds_up_per_journal_and_saturates() {
    let mut mine = JournalUsage {
        journal: journal(1),
        calls: [1, 2, 3, u64::MAX],
        written_bytes: 10,
        read_bytes: u64::MAX - 1,
    };
    let theirs = JournalUsage {
        journal: journal(1),
        calls: [10, 20, 30, 1],
        written_bytes: 5,
        read_bytes: 7,
    };
    mine.add(&theirs);
    assert_eq!(mine.calls, [11, 22, 33, u64::MAX]);
    assert_eq!(mine.written_bytes, 15);
    assert_eq!(mine.read_bytes, u64::MAX);
}

#[test]
#[should_panic(expected = "usage adds up per journal")]
fn usage_of_another_journal_does_not_add() {
    let usage = |n| JournalUsage {
        journal: journal(n),
        calls: [0; 4],
        written_bytes: 0,
        read_bytes: 0,
    };
    usage(1).add(&usage(2));
}

#[test]
fn totals_add_every_count() {
    let mut mine = Totals::default();
    mine.received(CallKind::Write);
    mine.answered(CallKind::Write, 10, 0);
    let mut theirs = Totals::default();
    theirs.received(CallKind::Read);
    theirs.received(CallKind::Write);
    theirs.answered(CallKind::Read, 0, 4);
    mine.add(&theirs);
    assert_eq!(mine.calls, [2, 1, 0, 0]);
    assert_eq!(mine.answers, [1, 1, 0, 0]);
    assert_eq!((mine.written_bytes, mine.read_bytes), (10, 4));
    assert_eq!(mine.total_calls(), 3);
}

fn scrubbed(damaged: bool, repaired: bool) -> paros_wire::internal::ScrubbedSlot {
    paros_wire::internal::ScrubbedSlot {
        damaged,
        repaired,
        ..paros_wire::internal::ScrubbedSlot::default()
    }
}

#[test]
fn a_scrub_page_counts_its_records_bytes_damage_and_repairs() {
    let metrics = Metrics::on();
    let page = paros_wire::ScrubReply {
        slots: vec![
            scrubbed(false, false),
            scrubbed(true, true),
            scrubbed(true, false),
        ],
        bytes: 300,
        ..paros_wire::ScrubReply::default()
    };
    metrics.scrubbed(journal(1), &page);
    let text = metrics.render();
    assert!(text.contains("paros_scrub_records_total 3\n"));
    assert!(text.contains("paros_scrub_bytes_total 300\n"));
    assert!(text.contains("paros_scrub_damaged_total 2\n"));
    assert!(text.contains("paros_scrub_repaired_total 1\n"));
    let off = Metrics::off();
    off.scrubbed(journal(1), &page);
    assert!(off.render().is_empty());
}

#[test]
fn two_handles_are_equal_only_when_they_share_one_machine() {
    let metrics = Metrics::on();
    let same = metrics == metrics.clone();
    let other = metrics == Metrics::on();
    assert!(same, "a clone is the same machine's handle");
    assert!(!other, "another machine's handle is not");
}
