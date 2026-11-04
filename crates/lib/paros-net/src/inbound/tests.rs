//! The `Deliver` edge's per-batch rule: each message goes to the loop's
//! queue in order, and the first one that names no journal or does not
//! decode stops the batch and is reported.

use std::sync::Mutex;

use futures::executor::block_on;
use paros_core::{NodeId, Slot};
use paros_wire::message_to_proto;

use super::*;

/// A peer message on the wire, addressed to `(tenant, journal)`.
fn wire(tenant: u64, journal: u64, from_slot: u64) -> internal::ConsensusMessage {
    let mut message = message_to_proto(&catch_up(from_slot)).expect("encodes");
    message.tenant = tenant;
    message.journal = journal;
    message
}

fn catch_up(from_slot: u64) -> Message {
    Message::CatchUpRequest {
        from: NodeId(1),
        from_slot: Slot(from_slot),
    }
}

fn batch(messages: Vec<internal::ConsensusMessage>) -> internal::Deliver {
    internal::Deliver {
        messages,
        ..internal::Deliver::default()
    }
}

/// An observer that records every refusal.
fn recorder() -> (OnReject, Arc<Mutex<Vec<EdgeRejection>>>) {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&seen);
    let on_reject: OnReject = Arc::new(move |r| sink.lock().expect("lock").push(r));
    (on_reject, seen)
}

fn ids(tenant: u64, journal: u64) -> JournalIdentifier {
    JournalIdentifier::new(TenantId(tenant), JournalId(journal))
}

fn drain(
    messages: &mut mpsc::Receiver<(JournalIdentifier, Message)>,
) -> Vec<(JournalIdentifier, Message)> {
    std::iter::from_fn(|| messages.try_recv().ok()).collect()
}

const ME: Party = Party::Node(NodeId(9));

/// A good batch is queued whole, in order, each message with its journal.
#[test]
fn a_good_batch_is_queued_in_order() {
    let (inbox, mut messages) = mpsc::channel(4);
    let (on_reject, seen) = recorder();
    let ok = block_on(enqueue_batch(
        batch(vec![wire(1, 2, 5), wire(1, 3, 6)]),
        &inbox,
        ME,
        &on_reject,
    ));
    assert!(ok, "the batch is acked");
    assert_eq!(
        drain(&mut messages),
        vec![(ids(1, 2), catch_up(5)), (ids(1, 3), catch_up(6))]
    );
    assert!(seen.lock().expect("lock").is_empty());
}

/// A message with an unset half of its journal is refused: the messages
/// before it stay queued, the ones after it are dropped.
#[test]
fn a_message_that_names_no_journal_stops_the_batch() {
    for (tenant, journal) in [(0, 2), (1, 0)] {
        let (inbox, mut messages) = mpsc::channel(4);
        let (on_reject, seen) = recorder();
        let ok = block_on(enqueue_batch(
            batch(vec![wire(1, 2, 5), wire(tenant, journal, 6), wire(1, 2, 7)]),
            &inbox,
            ME,
            &on_reject,
        ));
        assert!(!ok, "the batch is not acked");
        assert_eq!(drain(&mut messages), vec![(ids(1, 2), catch_up(5))]);
        assert_eq!(
            *seen.lock().expect("lock"),
            vec![EdgeRejection::UnsetJournal]
        );
    }
}

/// A message that decodes from the wire but not into a `Message` is
/// refused as such.
#[test]
fn an_undecodable_message_stops_the_batch() {
    let (inbox, mut messages) = mpsc::channel(4);
    let (on_reject, seen) = recorder();
    let mut broken = wire(1, 2, 6);
    broken.kind = None;
    let ok = block_on(enqueue_batch(
        batch(vec![wire(1, 2, 5), broken, wire(1, 2, 7)]),
        &inbox,
        ME,
        &on_reject,
    ));
    assert!(!ok);
    assert_eq!(drain(&mut messages), vec![(ids(1, 2), catch_up(5))]);
    assert_eq!(
        *seen.lock().expect("lock"),
        vec![EdgeRejection::MessageDecode]
    );
}

/// When the loop is gone, the batch is not acked and nothing is reported:
/// it is no peer's fault.
#[test]
fn a_gone_loop_does_not_ack() {
    let (inbox, messages) = mpsc::channel(4);
    drop(messages);
    let (on_reject, seen) = recorder();
    let ok = block_on(enqueue_batch(
        batch(vec![wire(1, 2, 5)]),
        &inbox,
        ME,
        &on_reject,
    ));
    assert!(!ok);
    assert!(seen.lock().expect("lock").is_empty());
}

/// An empty batch is acked.
#[test]
fn an_empty_batch_is_acked() {
    let (inbox, mut messages) = mpsc::channel(1);
    let (on_reject, _) = recorder();
    assert!(block_on(enqueue_batch(
        batch(Vec::new()),
        &inbox,
        ME,
        &on_reject
    )));
    assert!(drain(&mut messages).is_empty());
}
