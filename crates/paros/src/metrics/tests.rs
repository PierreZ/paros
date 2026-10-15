use std::sync::Arc;
use std::time::Duration;

use moonpool_prometheus::PrometheusSource;
use paros_core::{JournalId, JournalIdentifier, TenantId};

use super::{CallKind, Metrics, Phase};

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
    let metrics = Metrics::new(Arc::new(PrometheusSource::default()));
    let handles = metrics.journal(journal(1), names).expect("on");
    handles.received(CallKind::Write);
    handles.received(CallKind::Write);
    handles.received(CallKind::Read);
    handles.answered(CallKind::Write, Duration::from_millis(2), 100, 0);
    handles.answered(CallKind::Read, Duration::from_millis(1), 0, 40);
    metrics.settled(journal(1), 5, 7, Duration::from_micros(300));
    let usage = metrics.usage();
    assert_eq!(usage.len(), 1);
    assert_eq!(usage[0].calls_of(CallKind::Write), 2);
    assert_eq!(usage[0].calls_of(CallKind::Read), 1);
    assert_eq!(usage[0].total_calls(), 3);
    assert_eq!((usage[0].written_bytes, usage[0].read_bytes), (100, 40));
    let text = metrics.render();
    assert!(text.contains(r#"paros_calls_total{call="write",journal="orders",tenant="acme"} 2"#));
    assert!(text.contains(r#"paros_written_bytes_total{journal="orders",tenant="acme"} 100"#));
    assert!(text.contains(r#"paros_chosen_slot{journal="orders",tenant="acme"} 7"#));
    assert!(text.contains("paros_settle_seconds_count 1"));
}

#[test]
fn a_restart_on_the_same_source_keeps_counting() {
    let source = Arc::new(PrometheusSource::default());
    let first = Metrics::new(source.clone());
    first
        .journal(journal(1), names)
        .expect("on")
        .received(CallKind::Truncate);
    let second = Metrics::new(source);
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
}

#[test]
fn totals_read_back_every_journal_from_the_registry() {
    let source = Arc::new(PrometheusSource::default());
    let metrics = Metrics::new(source.clone());
    assert_eq!(super::Totals::of(&source), super::Totals::default());
    for n in 1..=2 {
        let handles = metrics
            .journal(journal(n), || ("acme".into(), format!("j{n}")))
            .expect("on");
        handles.received(CallKind::Write);
        handles.answered(CallKind::Write, Duration::ZERO, 10, 0);
        handles.received(CallKind::Read);
        handles.answered(CallKind::Read, Duration::ZERO, 0, 3);
    }
    let totals = super::Totals::of(&source);
    assert_eq!(totals.calls, [2, 2, 0, 0]);
    assert_eq!((totals.written_bytes, totals.read_bytes), (20, 6));
    assert_eq!(totals.total_calls(), 4);
}
