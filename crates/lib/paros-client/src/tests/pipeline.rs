//! A pipeline over fake nodes: writes sent together merge into one write;
//! a write that arrives before the one in front of it is sent again; a
//! merged write over a node's limits comes apart; a claim and a
//! truncation fold into the shared writer.

use std::time::Duration;

use paros_core::Value;
use paros_core::journal::{Command, Control, LeaderUuid, WriterMode};
use paros_wire::WriteAck;
use paros_wire::public::WriteOutcome as Wire;

use crate::rig::Rig;
use crate::rig::cell::{Act, Cell, ids};
use crate::{ClaimOutcome, Pipeline, TruncateOutcome, Writer, WriterOutcome};

const U: LeaderUuid = LeaderUuid(11);

fn journal() -> paros_core::journal::JournalIdentifier {
    ids(5, 6)
}

fn cell() -> Cell {
    let cell = Cell::led_by(1);
    cell.create(journal(), WriterMode::Single);
    cell
}

fn one(byte: u8) -> Vec<Value> {
    vec![Value(vec![byte])]
}

fn written(outcome: &WriterOutcome) -> Option<(u64, u64)> {
    match outcome {
        WriterOutcome::Written { seq, count, .. } => Some((*seq, *count)),
        _ => None,
    }
}

#[test]
fn a_pipeline_that_leads_nothing_opens_nothing() {
    let cell = cell();
    let (opened, truncated) = Rig::new().run_with(&cell, &[1], |client| async move {
        let pipeline = Pipeline::new(Writer::with_uuid(journal(), U));
        let opened = pipeline.write(&client, |_| one(1), 0).await;
        let truncated = pipeline.truncate(&client, 0, 0).await;
        (opened.is_none(), truncated)
    });
    assert!(opened);
    assert_eq!(truncated, None);
    assert!(cell.heard().is_empty());
}

#[test]
fn writes_sent_together_merge_into_one_write() {
    let cell = cell();
    let (claimed, sent) = Rig::new().run_with(&cell, &[1], |client| async move {
        let pipeline = Pipeline::new(Writer::with_uuid(journal(), U));
        let claimed = pipeline.claim(&client, 0, false).await;
        let mut tickets = Vec::new();
        for byte in 1..=3 {
            tickets.push(pipeline.open(|_| one(byte)).await.expect("leads"));
        }
        let sends = tickets
            .into_iter()
            .map(|ticket| pipeline.send(&client, ticket, 0));
        let sent = futures::future::join_all(sends).await;
        (claimed, sent)
    });
    assert!(matches!(claimed, ClaimOutcome::Won { .. }));
    let seqs: Vec<_> = sent.iter().map(|p| written(&p.outcome)).collect();
    assert_eq!(seqs, vec![Some((0, 1)), Some((1, 1)), Some((2, 1))]);
    assert!(sent.iter().all(|p| p.merged && !p.resent));
    assert_eq!(cell.writes_heard(), vec![1], "one write for the three");
    assert_eq!(
        cell.records(journal()),
        vec![Value(vec![1]), Value(vec![2]), Value(vec![3])]
    );
}

#[test]
fn a_merged_write_over_the_limits_comes_apart() {
    let cell = cell();
    cell.script(1, |s| {
        s.writes.push_back(Act::Answer(WriteAck {
            outcome: Wire::TooLarge.into(),
            max_records: 1,
            max_bytes: 1 << 10,
            ..WriteAck::default()
        }));
    });
    let sent = Rig::new().run_with(&cell, &[1], |client| async move {
        let pipeline = Pipeline::new(Writer::with_uuid(journal(), U));
        pipeline.claim(&client, 0, false).await;
        let first = pipeline.open(|_| one(1)).await.expect("leads");
        let second = pipeline.open(|_| one(2)).await.expect("leads");
        let (a, b) = futures::join!(
            pipeline.send(&client, first, 0),
            pipeline.send(&client, second, 0)
        );
        vec![a, b]
    });
    let seqs: Vec<_> = sent.iter().map(|p| written(&p.outcome)).collect();
    assert_eq!(seqs, vec![Some((0, 1)), Some((1, 1))]);
    assert_eq!(cell.writes_heard().len(), 3, "merged, then each alone");
    assert_eq!(
        cell.records(journal()),
        vec![Value(vec![1]), Value(vec![2])]
    );
}

#[test]
fn a_write_that_arrives_early_waits_and_is_sent_again() {
    let cell = cell();
    // The first write's answer is lost: the second, sent meanwhile, finds
    // the tail below it and is refused early.
    cell.script(1, |s| s.writes.push_back(Act::Hold));
    let sent = Rig::new().run_with(&cell, &[1], |client| async move {
        let pipeline = Pipeline::new(Writer::with_uuid(journal(), U));
        pipeline.claim(&client, 0, false).await;
        let first = pipeline.open(|_| one(1)).await.expect("leads");
        let second = pipeline.open(|_| one(2)).await.expect("leads");
        let pause = client.clone();
        let (a, b) = futures::join!(pipeline.send(&client, first, 0), async {
            pause.pause(Duration::from_millis(100)).await;
            pipeline.send(&client, second, 0).await
        });
        vec![a, b]
    });
    let seqs: Vec<_> = sent.iter().map(|p| written(&p.outcome)).collect();
    assert_eq!(seqs, vec![Some((0, 1)), Some((1, 1))]);
    assert!(sent[1].resent, "the early write is sent again");
    assert!(!sent[1].merged);
    assert_eq!(
        cell.records(journal()),
        vec![Value(vec![1]), Value(vec![2])]
    );
}

#[test]
fn a_superseded_pipeline_stops_and_a_fresh_claim_reopens_at_the_tail() {
    let cell = cell();
    let probe = cell.clone();
    let (refused, stopped, reclaimed, again) =
        Rig::new().run_with(&cell, &[1], |client| async move {
            let pipeline = Pipeline::new(Writer::with_uuid(journal(), U));
            pipeline.claim(&client, 0, false).await;
            pipeline.write(&client, |_| one(1), 0).await;
            probe.apply(
                journal(),
                &Command::Control(Control::SetLeader {
                    new: LeaderUuid(99),
                    old: Some(U),
                }),
            );
            let refused = pipeline.truncate(&client, 1, 0).await;
            let stopped = pipeline.write(&client, |_| one(2), 0).await.is_none();
            let reclaimed = pipeline.claim(&client, 0, true).await;
            let again = pipeline
                .write(&client, |_| one(3), 0)
                .await
                .map(|p| p.outcome);
            (refused, stopped, reclaimed, again)
        });
    assert!(matches!(refused, Some(TruncateOutcome::Refused { .. })));
    assert!(stopped, "a superseded pipeline opens nothing");
    assert!(matches!(reclaimed, ClaimOutcome::Won { .. }));
    assert_eq!(again.as_ref().and_then(written), Some((1, 1)));
}
