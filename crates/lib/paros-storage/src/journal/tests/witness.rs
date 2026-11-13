//! The two-fault guards around the journal's tail and metainfo (#991
//! (metainfo write on idle), #992 (rolled-back promise, scrub of records)):
//! an idle store writes its metainfo as the witness of its last batch, a
//! rolled-back promise is a corruption verdict for its journal, and the
//! scrub reads the persist records again.

use super::*;
use crate::ScrubbedSlot;
use crate::boot::check_promise;
use paros_model::corruption::IntegrityFault;

/// Read `len` bytes of `path` at `at`.
async fn read(provider: &SimStorageProvider, path: &str, at: u64, len: usize) -> Vec<u8> {
    let file = provider
        .open(path, OpenOptions::read_write())
        .await
        .expect("open");
    let mut bytes = vec![0; len];
    file.read_at(at, &mut bytes).await.expect("read");
    bytes
}

/// Write `bytes` to `path` at `at`, durably.
async fn write(provider: &SimStorageProvider, path: &str, at: u64, bytes: &[u8]) {
    let file = provider
        .open(path, OpenOptions::read_write())
        .await
        .expect("open");
    file.write_at(at, bytes).await.expect("write");
    file.sync_all().await.expect("sync");
}

/// Flip one bit of a region: rot in one known place.
async fn flip(provider: &SimStorageProvider, region: &moonpool_core::LayoutRegion, at: u64) {
    let byte = read(provider, &region.path, at, 1).await[0];
    write(provider, &region.path, at, &[byte ^ 0x10]).await;
}

/// Rot both the persist record and the entry of `slot`: two faults on one
/// slot.
async fn rot_both(provider: &SimStorageProvider, node: &Node, slot: u64) {
    let layout = node.layout(Slot(slot)).expect("a held slot");
    flip(provider, &layout.record, layout.record.bytes.start + 20).await;
    flip(provider, &layout.entry, layout.entry.bytes.end - 1).await;
}

/// A formatted store with a promise, then slots 0..3 each in a commit of
/// its own with no metainfo: the last batch has no witness yet.
async fn unwitnessed(provider: &SimStorageProvider, dir: &str, durability: Durability) -> Node {
    let mut node = open_node(provider.clone(), dir, store(durability))
        .await
        .expect("open");
    node.format(&config()).await.expect("format");
    node.persist_ballot(ballot(5)).await.expect("promise");
    node.sync(MustSync::Sync).await.expect("sync promise");
    for slot in 0..3 {
        node.append_accepted(Slot(slot), ballot(5), user(slot, 0xE1))
            .await
            .expect("accept");
        node.sync(MustSync::Sync).await.expect("sync");
    }
    node
}

/// An idle store writes its metainfo once, after the idle bound and not
/// before, and then never again until it commits entries.
#[test]
fn an_idle_store_writes_its_metainfo_once_after_the_bound() {
    runtime().block_on(async {
        let mut sim = sim(21);
        run(&mut sim, |provider| async move {
            let mut node = unwitnessed(&provider, "idle", Durability::Batched).await;
            assert!(!node.idle_beat(3).await.expect("beat 1"));
            assert!(!node.idle_beat(3).await.expect("beat 2"));
            assert!(node.idle_beat(3).await.expect("beat 3"), "the bound writes");
            for _ in 0..5 {
                assert!(
                    !node.idle_beat(3).await.expect("witnessed"),
                    "a witnessed tail writes nothing more"
                );
            }
            // A commit with entries and no metainfo starts the count again.
            node.append_accepted(Slot(3), ballot(5), user(3, 0xE1))
                .await
                .expect("accept");
            node.sync(MustSync::Sync).await.expect("sync");
            assert!(
                node.idle_beat(1).await.expect("floor one"),
                "floor 1 writes at once"
            );
            // A commit that writes the metainfo witnesses its own batch.
            node.append_accepted(Slot(4), ballot(6), user(4, 0xE1))
                .await
                .expect("accept");
            node.persist_ballot(ballot(6)).await.expect("promise");
            node.sync(MustSync::Sync).await.expect("sync with metainfo");
            assert!(
                !node.idle_beat(1).await.expect("beat"),
                "a commit with its metainfo needs no idle write"
            );
        })
        .await;
    });
}

/// Two faults on an idle journal's last batch: with its witness, the next
/// open refuses the journal (a crash verdict for that journal); without
/// it, the open would read the batch as torn and forget the vote.
#[test]
fn two_faults_on_a_witnessed_last_batch_refuse_the_open() {
    for durability in BOTH {
        runtime().block_on(async {
            let mut sim = sim(22);
            run(&mut sim, move |provider| async move {
                let mut node = unwitnessed(&provider, "witnessed", durability).await;
                assert!(node.idle_beat(1).await.expect("beat"));
                rot_both(&provider, &node, 2).await;
                drop(node);
                let refused = open_node(provider, "witnessed", store(durability)).await;
                assert!(
                    matches!(
                        refused,
                        Err(StorageError::Corruption {
                            record: StorageRecord::Store,
                            verdict: CorruptionVerdict::Corrupted,
                            ..
                        })
                    ),
                    "{durability:?}: {refused:?}"
                );
            })
            .await;
        });
    }
}

/// The same two faults with no idle write: the open discards the slot as a
/// torn tail. This is the hole the idle write closes.
#[test]
fn two_faults_on_an_unwitnessed_last_batch_read_as_torn() {
    runtime().block_on(async {
        let mut sim = sim(23);
        run(&mut sim, |provider| async move {
            let node = unwitnessed(&provider, "torn", Durability::Batched).await;
            rot_both(&provider, &node, 2).await;
            drop(node);
            let node = open_node(provider, "torn", store(Durability::Batched))
                .await
                .expect("a torn tail opens");
            assert!(node.accepted(Slot(2)).is_none(), "the vote is gone");
            assert!(node.faulty_entries().is_empty(), "and not reported");
        })
        .await;
    });
}

/// A metainfo rolled back inside one open (two lost writes of one update)
/// puts an older promise under a vote: the check is a corruption of the
/// promise, for this journal alone, and a whole store passes it.
#[test]
fn a_rolled_back_promise_is_a_corruption_verdict() {
    runtime().block_on(async {
        let mut sim = sim(24);
        run(&mut sim, |provider| async move {
            let mut node = open_node(provider.clone(), "rollback", compact())
                .await
                .expect("open");
            node.format(&config()).await.expect("format");
            node.persist_ballot(ballot(5)).await.expect("promise");
            node.append_accepted(Slot(0), ballot(5), user(0, 0xF1))
                .await
                .expect("accept");
            node.sync(MustSync::Sync).await.expect("sync");
            assert_eq!(check_promise(&node), Ok(()), "a whole store passes");
            let size = usize::try_from(
                provider
                    .open("rollback/meta", OpenOptions::read_write())
                    .await
                    .expect("meta")
                    .size()
                    .await
                    .expect("size"),
            )
            .expect("small");
            let older = read(&provider, "rollback/meta", 0, size).await;
            node.persist_ballot(ballot(7)).await.expect("promise");
            node.append_accepted(Slot(1), ballot(7), user(1, 0xF2))
                .await
                .expect("accept");
            node.sync(MustSync::Sync).await.expect("sync");
            drop(node);
            // Both copies of the later metainfo writes are lost.
            write(&provider, "rollback/meta", 0, &older).await;
            let node = open_node(provider, "rollback", compact())
                .await
                .expect("the journal itself opens");
            assert_eq!(node.initial_state().0.max_promised_ballot, ballot(5));
            assert_eq!(
                check_promise(&node),
                Err(StorageError::Corruption {
                    record: StorageRecord::Promise,
                    fault: IntegrityFault::LostWrite,
                    verdict: CorruptionVerdict::Corrupted,
                })
            );
        })
        .await;
    });
}

/// A persist record that rots while its entry stands is found by a scrub
/// page, and the next append of the slot writes it again.
#[test]
fn a_scrub_finds_a_rotted_persist_record() {
    runtime().block_on(async {
        let mut sim = sim(25);
        run(&mut sim, |provider| async move {
            let mut node = unwitnessed(&provider, "record", Durability::Ordered).await;
            let layout = node.layout(Slot(1)).expect("held");
            flip(&provider, &layout.record, layout.record.bytes.start + 20).await;
            let page = node.scrub(Slot(0), 10, u64::MAX).await.expect("scrub");
            assert_eq!(
                page.slots[&Slot(1)],
                ScrubbedSlot::Damaged { ballot: ballot(5) },
                "the record is damaged"
            );
            assert!(matches!(page.slots[&Slot(0)], ScrubbedSlot::Intact { .. }));
            assert!(matches!(page.slots[&Slot(2)], ScrubbedSlot::Intact { .. }));
            node.append_accepted(Slot(1), ballot(5), user(1, 0xE1))
                .await
                .expect("re-accept");
            assert_eq!(node.staged_entries(), 1, "the slot is written again");
            node.sync(MustSync::Sync).await.expect("sync repair");
            let page = node.scrub(Slot(0), 10, u64::MAX).await.expect("scrub");
            assert!(
                page.slots
                    .values()
                    .all(|s| matches!(s, ScrubbedSlot::Intact { .. })),
                "repaired: {page:?}"
            );
            drop(node);
            let node = open_node(provider, "record", store(Durability::Ordered))
                .await
                .expect("reboots");
            assert!(node.faulty_entries().is_empty());
            assert!(node.accepted(Slot(1)).is_some());
        })
        .await;
    });
}
