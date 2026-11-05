//! The journal store's scrub (#362 (background scrubbing)) against the
//! disk: a record that rotted under an open store reads back damaged and is
//! written again at its next append; a vote found faulty at boot is
//! damaged; a slot staged for the next sync is not read.

use super::*;
use crate::{ScrubPage, ScrubbedSlot};
use paros_core::journal::command_fingerprint;

fn intact(slot: u64, round: u64, command: &Command) -> (Slot, ScrubbedSlot) {
    (
        Slot(slot),
        ScrubbedSlot::Intact {
            ballot: ballot(round),
            digest: command_fingerprint(command),
        },
    )
}

fn damaged(slot: u64, round: u64) -> (Slot, ScrubbedSlot) {
    (
        Slot(slot),
        ScrubbedSlot::Damaged {
            ballot: ballot(round),
        },
    )
}

/// Three slots of 48 bytes each, durable, under ballot 5.
async fn three_slots(provider: &SimStorageProvider, dir: &str) -> Node {
    let mut node = open_node(provider.clone(), dir, compact())
        .await
        .expect("open");
    node.format(&config()).await.expect("format");
    node.persist_ballot(ballot(5)).await.expect("promise");
    for (slot, byte) in [(0, 0xC1), (1, 0xC2), (2, 0xC3)] {
        node.append_accepted(Slot(slot), ballot(5), user(slot, byte))
            .await
            .expect("accept");
    }
    node.sync(MustSync::Sync).await.expect("sync");
    node
}

/// A record that rots on the device while the store is open reads back
/// damaged; the store then writes it again at its next append, even with
/// the same ballot and value, and the copy is whole after a reboot.
#[test]
fn a_rotted_record_reads_back_damaged_and_is_written_again() {
    runtime().block_on(async {
        let mut sim = sim(11);
        run(&mut sim, |provider| async move {
            let mut node = three_slots(&provider, "scrub").await;
            assert!(rot(&provider, "scrub", &[0xC2; 48]).await);
            let page = node.scrub(Slot(0), 10, u64::MAX).await.expect("scrub");
            assert_eq!(
                page,
                ScrubPage {
                    slots: [
                        intact(0, 5, &user(0, 0xC1)),
                        damaged(1, 5),
                        intact(2, 5, &user(2, 0xC3)),
                    ]
                    .into(),
                    bytes: 3 * 48,
                    next: None,
                }
            );
            node.append_accepted(Slot(1), ballot(5), user(1, 0xC2))
                .await
                .expect("re-accept");
            assert_eq!(node.staged_entries(), 1, "a damaged copy is written again");
            node.sync(MustSync::Sync).await.expect("sync repair");
            let again = node.scrub(Slot(1), 1, u64::MAX).await.expect("scrub");
            assert_eq!(again.slots, [intact(1, 5, &user(1, 0xC2))].into());
            assert_eq!(again.next, Some(Slot(2)));
            drop(node);
            let node = open_node(provider, "scrub", compact())
                .await
                .expect("reboots");
            assert!(node.faulty_entries().is_empty(), "the copy is whole");
        })
        .await;
    });
}

/// A vote found faulty at boot has no value: the scrub reports it damaged
/// with the ballot the store holds, and keeps it faulty (no copy to write).
#[test]
fn a_faulty_vote_reads_back_damaged() {
    runtime().block_on(async {
        let mut sim = sim(12);
        run(&mut sim, |provider| async move {
            drop(three_slots(&provider, "faulty").await);
            assert!(rot(&provider, "faulty", &[0xC2; 48]).await);
            let mut node = open_node(provider.clone(), "faulty", compact())
                .await
                .expect("reboots");
            assert_eq!(node.faulty_entries(), vec![(Slot(1), ballot(5))]);
            let page = node.scrub(Slot(1), 1, u64::MAX).await.expect("scrub");
            assert_eq!(page.slots, [damaged(1, 5)].into());
            assert_eq!(page.bytes, 1, "a faulty vote costs one byte of the budget");
            assert_eq!(page.next, Some(Slot(2)));
            assert_eq!(node.faulty_entries(), vec![(Slot(1), ballot(5))]);
        })
        .await;
    });
}

/// A slot staged for the next sync is not on the device yet: the scrub
/// skips it, and reads it once it is durable.
#[test]
fn a_staged_slot_is_not_read() {
    runtime().block_on(async {
        let mut sim = sim(13);
        run(&mut sim, |provider| async move {
            let mut node = three_slots(&provider, "staged").await;
            node.append_accepted(Slot(3), ballot(5), user(3, 0xC4))
                .await
                .expect("stage");
            node.append_accepted(Slot(1), ballot(6), user(1, 0xD2))
                .await
                .expect("stage over a durable slot");
            let page = node.scrub(Slot(0), 10, u64::MAX).await.expect("scrub");
            assert_eq!(
                page.slots,
                [intact(0, 5, &user(0, 0xC1)), intact(2, 5, &user(2, 0xC3))].into()
            );
            node.sync(MustSync::Sync).await.expect("sync");
            let page = node.scrub(Slot(0), 10, u64::MAX).await.expect("scrub");
            assert_eq!(page.slots.len(), 4);
            assert_eq!(page.slots[&Slot(1)], intact(1, 6, &user(1, 0xD2)).1);
            assert_eq!(page.slots[&Slot(3)], intact(3, 5, &user(3, 0xC4)).1);
        })
        .await;
    });
}

/// A store with no slot at or above `from` returns an empty page.
#[test]
fn a_scrub_past_the_last_slot_is_empty() {
    runtime().block_on(async {
        let mut sim = sim(14);
        run(&mut sim, |provider| async move {
            let mut node = three_slots(&provider, "past").await;
            let page = node.scrub(Slot(3), 10, u64::MAX).await.expect("scrub");
            assert_eq!(page, ScrubPage::default());
        })
        .await;
    });
}
