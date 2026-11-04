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

/// The `Deliver` edge of machine `10.0.0.1` in cell `7`, and a peer's
/// client: the batch `deliver` sends is acked or refused, as the edge
/// decides.
fn over_the_edge(cell_id: u64) -> (bool, Vec<EdgeRejection>, Vec<JournalIdentifier>) {
    use crate::rig::{self, Rig};
    use crate::well_known;

    let mut world = Rig::new();
    let server = world.providers("10.0.0.1");
    let peer = world.providers("10.0.0.2");
    world.run(async move {
        let (rpc, addr) = rig::listen(&server, "10.0.0.1").await;
        let (on_reject, seen) = recorder();
        let shutdown = CancellationToken::new();
        let mut inbox = serve_deliveries(
            &server,
            &rpc,
            4,
            Party::Node(NodeId(1)),
            7,
            on_reject,
            shutdown.clone(),
            None,
        )
        .expect("the edge serves");
        let client = rig::client(&peer);
        let mut deliver = batch(vec![wire(3, 4, 1)]);
        deliver.cell_id = cell_id;
        let acked = well_known::<_, DeliverRpc>(&client, addr)
            .try_get_reply(&deliver)
            .await
            .is_ok();
        let mut queued = Vec::new();
        while let Ok((journal, _)) = inbox.try_recv() {
            queued.push(journal);
        }
        shutdown.cancel();
        let seen = seen.lock().expect("lock").clone();
        (acked, seen, queued)
    })
}

/// A batch from the receiver's own cell is queued and acked.
#[test]
fn a_batch_from_the_own_cell_is_queued_and_acked() {
    let (acked, seen, queued) = over_the_edge(7);
    assert!(acked, "acked");
    assert_eq!(seen, Vec::new());
    assert_eq!(queued, vec![ids(3, 4)]);
}

/// A batch from another cell (#216) is refused whole: nothing is queued,
/// it is never acked, and the refusal is reported.
#[test]
fn a_batch_from_another_cell_is_refused_whole() {
    let (acked, seen, queued) = over_the_edge(8);
    assert!(!acked, "never acked");
    assert_eq!(seen, vec![EdgeRejection::ForeignCell]);
    assert_eq!(queued, Vec::new());
}
