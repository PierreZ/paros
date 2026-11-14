//! The behavioral contract suite every [`LogStorage`] implementation must
//! pass, shared by the in-memory store and the journal store.

use std::future::Future;

use paros_core::contract::{MustSync, RetireMark};
use paros_core::journal::Command;
use paros_core::{Ballot, Slot};

use super::LogStorage;

/// The behavioral **contract suite** every [`LogStorage`] implementation must
/// pass (issue #21 item F): one suite, run against both [`MemStorage`] and the
/// journal store ([`crate::journal`], here and in `paros-sim` on the simulated
/// disk), so no implementation drifts from the trait contract. The fault
/// budget stays *outside* this contract (#70): from the trait's point of view
/// both implementations behave identically on the clean path this suite drives.
///
/// `fresh` must return (a future of) an empty storage for the same
/// single-node membership on every call. `reopen` simulates a clean reboot
/// of the same store — asynchronously, because a disk-backed store opens and
/// scans its records on the way up: the reads
/// the suite asserts are the *recovery port's* (the core reads durable state
/// once, at construction), so every read-back goes through a reopen — an
/// in-memory implementation may return the same handle, a durable one
/// re-restores from its records.
///
/// # Panics
///
/// Panics on any contract violation.
#[doc(hidden)]
// One linear behavioral walk; splitting it would scatter the contract.
#[allow(clippy::too_many_lines)]
#[tracing::instrument(level = "debug", skip_all)]
pub async fn storage_contract_suite<S, Fresh, Reopened>(
    mut fresh: impl FnMut() -> Fresh,
    mut reopen: impl FnMut(S) -> Reopened,
) where
    S: LogStorage,
    Fresh: Future<Output = S>,
    Reopened: Future<Output = S>,
{
    use paros_core::Value;
    use paros_core::journal::{Entry, JournalState, LeaderUuid, Seq};
    let ballot = |round: u64| Ballot {
        round,
        node: paros_core::NodeId(0),
    };
    let user = |seq: u64, byte: u8| {
        Command::Write(Entry {
            leader: LeaderUuid(7),
            seq: Seq(seq),
            records: vec![Value(vec![byte])],
        })
    };
    let state = |next: u64, first: u64| JournalState {
        leader: Some(LeaderUuid(7)),
        term: 1,
        next_seq: Seq(next),
        first_seq: Seq(first),
    };

    // The format marker (#147): absent on a fresh store, present once
    // `format` is flushed and reopened, and written by nothing else — a
    // store that took protocol writes without ever being formatted stays
    // unformatted (that is exactly the wiped-disk shape the driver refuses).
    let s = fresh().await;
    assert!(!s.is_formatted(), "a fresh store carries no format marker");
    assert_eq!(
        s.formatted_config(),
        None,
        "a fresh store records no configuration"
    );
    let mut s = fresh().await;
    s.persist_ballot(ballot(1)).await.expect("ballot");
    s.sync(MustSync::Sync).await.expect("sync ballot");
    let s = reopen(s).await;
    assert!(
        !s.is_formatted(),
        "protocol writes never format a store on their own"
    );
    // #207: the marker records the configuration it was written under —
    // the one handed to `format`, byte for byte, never the one the store is
    // opened with later. The store keeps answering the operator's own
    // configuration through the recovery port; the driver compares the two.
    let mut s = fresh().await;
    let (_, operator) = s.initial_state();
    let provisioned = paros_core::Config {
        cell_id: operator.cell_id + 1,
        ..operator.clone()
    };
    s.format(&provisioned).await.expect("format");
    s.sync(MustSync::Sync).await.expect("sync format");
    let s = reopen(s).await;
    assert!(s.is_formatted(), "the format marker survives a reopen");
    assert_eq!(
        s.formatted_config().as_ref(),
        Some(&provisioned),
        "the format marker records the configuration it was written under"
    );
    assert_eq!(
        s.initial_state().1,
        operator,
        "the recorded configuration never replaces the operator's"
    );
    let mut s = reopen(s).await;
    s.persist_ballot(ballot(2))
        .await
        .expect("ballot after format");
    s.sync(MustSync::Sync).await.expect("sync after format");
    let s = reopen(s).await;
    assert!(s.is_formatted(), "the format marker is never removed");
    assert_eq!(
        s.formatted_config().as_ref(),
        Some(&provisioned),
        "later writes never edit the recorded configuration"
    );

    // The registration fence (#1002 (retire fence gaps)): zero on a fresh
    // store, raised by its own write, kept across a reboot and beside a
    // promise written later.
    let s = fresh().await;
    assert_eq!(s.registration_fence(), Ballot::zero());
    let mut s = fresh().await;
    s.persist_ballot(ballot(4)).await.expect("ballot");
    s.persist_registration_fence(ballot(4))
        .await
        .expect("registration fence");
    s.sync(MustSync::Sync).await.expect("sync fence");
    let mut s = reopen(s).await;
    assert_eq!(
        s.registration_fence(),
        ballot(4),
        "the registration fence survives a reboot"
    );
    s.persist_ballot(ballot(6))
        .await
        .expect("ballot after fence");
    s.sync(MustSync::Sync).await.expect("sync after fence");
    let s = reopen(s).await;
    assert_eq!(
        s.registration_fence(),
        ballot(4),
        "a later promise keeps the registration fence"
    );
    assert_eq!(s.initial_state().0.max_promised_ballot, ballot(6));

    // The retire mark (#1002 (retire fence gaps)): the default on a fresh
    // store, replaced by its own write and kept across a reboot.
    assert_eq!(fresh().await.retire_mark(), RetireMark::default());
    let mut s = fresh().await;
    let checked = RetireMark {
        incarnation: 0,
        checked: true,
    };
    s.persist_retire_mark(checked).await.expect("retire mark");
    s.sync(MustSync::Sync).await.expect("sync mark");
    let mut s = reopen(s).await;
    assert_eq!(s.retire_mark(), checked, "a checked mark survives a reboot");
    let rejoined = RetireMark {
        incarnation: 1,
        checked: false,
    };
    s.persist_retire_mark(rejoined).await.expect("rejoin mark");
    s.sync(MustSync::Sync).await.expect("sync rejoin");
    let s = reopen(s).await;
    assert_eq!(
        s.retire_mark(),
        rejoined,
        "a rejoin's incarnation survives a reboot"
    );

    // Scalars + per-slot records round-trip through a Sync flush.
    let mut s = fresh().await;
    s.persist_ballot(ballot(4)).await.expect("ballot");
    s.append_accepted(Slot(0), ballot(4), user(1, 0xa))
        .await
        .expect("append 0");
    s.append_accepted(Slot(1), ballot(4), user(2, 0xb))
        .await
        .expect("append 1");
    s.set_chosen_index(Slot(1)).await.expect("chosen index");
    s.sync(MustSync::Sync).await.expect("sync");
    let mut s = reopen(s).await;
    let (hs, _config) = s.initial_state();
    assert_eq!(hs.max_promised_ballot, ballot(4), "promise round-trips");
    assert_eq!(hs.chosen_index, Some(Slot(1)), "chosen index round-trips");
    assert_eq!(s.first_slot(), Slot(0), "floor starts at zero");
    assert_eq!(s.last_slot(), Slot(1), "last slot reflects the appends");
    assert_eq!(
        s.accepted(Slot(0)).map(|(b, _)| b),
        Some(ballot(4)),
        "an accepted record reads back"
    );
    assert!(
        s.faulty_entries().is_empty(),
        "a clean store reports no rot"
    );

    // An append is an upsert-by-slot: the newer record replaces the older.
    s.append_accepted(Slot(1), ballot(5), user(3, 0xc))
        .await
        .expect("re-append 1");
    s.sync(MustSync::Sync).await.expect("sync upsert");
    let mut s = reopen(s).await;
    assert_eq!(
        s.accepted(Slot(1)).map(|(b, _)| b),
        Some(ballot(5)),
        "append is an upsert by slot"
    );

    // Truncation raises the floor, drops the prefix, and seals the journal
    // state the prefix folded to.
    s.truncate(Slot(1), state(1, 1)).await.expect("truncate");
    s.sync(MustSync::Sync).await.expect("sync truncate");
    let mut s = reopen(s).await;
    assert_eq!(s.first_slot(), Slot(1), "the floor rose");
    assert!(
        s.accepted(Slot(0)).is_none(),
        "a truncated record is unreadable"
    );
    assert_eq!(
        s.sealed_state(),
        state(1, 1),
        "the sealed journal state survives the truncation"
    );
    // A floor never moves backward, and keeps the state sealed with it.
    s.truncate(Slot(0), JournalState::default())
        .await
        .expect("re-truncate lower");
    s.sync(MustSync::Sync).await.expect("sync no-op truncate");
    let s = reopen(s).await;
    assert_eq!(s.first_slot(), Slot(1), "the floor is monotone");
    assert_eq!(
        s.sealed_state(),
        state(1, 1),
        "a lower truncation seals nothing"
    );

    // A trim-point jump (#186): the chosen index rises to one below the
    // point, the promise does not move, the floor lands on the point, the
    // records below it go, and the peer's journal state seals.
    let mut s = fresh().await;
    s.persist_ballot(ballot(9)).await.expect("high promise");
    s.append_accepted(Slot(0), ballot(1), user(10, 0x40))
        .await
        .expect("append below the point");
    s.append_accepted(Slot(6), ballot(1), user(16, 0x40))
        .await
        .expect("append above the point");
    s.sync(MustSync::Sync).await.expect("sync promise");
    s.trimmed_to(Slot(5), state(4, 2)).await.expect("jump");
    s.sync(MustSync::Sync).await.expect("sync jump");
    let s = reopen(s).await;
    let (hs, _config) = s.initial_state();
    assert_eq!(
        hs.chosen_index,
        Some(Slot(4)),
        "the jump chose everything below the point"
    );
    assert_eq!(
        hs.max_promised_ballot,
        ballot(9),
        "a trim-point jump never moves the promise"
    );
    assert_eq!(s.first_slot(), Slot(5), "the floor is the point");
    assert!(
        s.accepted(Slot(0)).is_none(),
        "the records below the point are gone"
    );
    assert!(
        s.accepted(Slot(6)).is_some(),
        "the records above the point stay"
    );
    assert_eq!(
        s.sealed_state(),
        state(4, 2),
        "the jump sealed the peer's journal state"
    );

    scrub_walk(&mut fresh, &mut reopen).await;
}

/// The scrub's clean path (#362 (background scrubbing)): every record at or
/// above the floor reads back intact with its ballot and fingerprint, in
/// pages of at most `limit` records and about `max_bytes` bytes, each page
/// at least one record.
async fn scrub_walk<S, Fresh, Reopened>(
    fresh: &mut impl FnMut() -> Fresh,
    reopen: &mut impl FnMut(S) -> Reopened,
) where
    S: LogStorage,
    Fresh: Future<Output = S>,
    Reopened: Future<Output = S>,
{
    use paros_core::Value;
    use paros_core::journal::{Entry, JournalState, LeaderUuid, Seq, command_fingerprint};

    use crate::{ScrubPage, ScrubbedSlot};
    let ballot = |round: u64| Ballot {
        round,
        node: paros_core::NodeId(0),
    };
    // Slot `n` holds `n + 1` payload bytes, so a byte budget is easy to aim.
    let command = |slot: u64| {
        Command::Write(Entry {
            leader: LeaderUuid(7),
            seq: Seq(slot + 1),
            records: vec![Value(vec![0x5c; usize::try_from(slot + 1).expect("small")])],
        })
    };
    let intact = |slot: u64| {
        (
            Slot(slot),
            ScrubbedSlot::Intact {
                ballot: ballot(slot + 1),
                digest: command_fingerprint(&command(slot)),
            },
        )
    };
    let page = |slots: &[u64], next: Option<u64>| ScrubPage {
        slots: slots.iter().map(|slot| intact(*slot)).collect(),
        bytes: slots.iter().map(|slot| slot + 1).sum(),
        next: next.map(Slot),
    };

    let mut s = fresh().await;
    for slot in 0..4 {
        s.append_accepted(Slot(slot), ballot(slot + 1), command(slot))
            .await
            .expect("append");
    }
    s.sync(MustSync::Sync).await.expect("sync appends");
    s.truncate(Slot(1), JournalState::default())
        .await
        .expect("raise the floor");
    s.sync(MustSync::Sync).await.expect("sync floor");
    let mut s = reopen(s).await;

    let all = s.scrub(Slot(0), 10, u64::MAX).await.expect("scrub");
    assert_eq!(all, page(&[1, 2, 3], None), "from the floor up, all intact");
    let first = s.scrub(Slot(0), 2, u64::MAX).await.expect("scrub");
    assert_eq!(
        first,
        page(&[1, 2], Some(3)),
        "a page holds `limit` records"
    );
    let rest = s.scrub(Slot(3), 2, u64::MAX).await.expect("scrub");
    assert_eq!(rest, page(&[3], None), "the next page starts at `next`");
    let one = s.scrub(Slot(1), 0, u64::MAX).await.expect("scrub");
    assert_eq!(one, page(&[1], Some(2)), "a page holds at least one record");
    // Slot 1 holds 2 bytes, slot 2 holds 3: a budget of 2 stops after slot
    // 1, a budget of 3 reads slot 2 too (the budget is spent once reached).
    let budget = s.scrub(Slot(1), 10, 2).await.expect("scrub");
    assert_eq!(budget, page(&[1], Some(2)), "the byte budget ends a page");
    let budget = s.scrub(Slot(1), 10, 3).await.expect("scrub");
    assert_eq!(
        budget,
        page(&[1, 2], Some(3)),
        "a budget not yet spent reads on"
    );
    let tiny = s.scrub(Slot(2), 10, 0).await.expect("scrub");
    assert_eq!(tiny, page(&[2], Some(3)), "a zero budget still reads one");
    let past = s.scrub(Slot(9), 10, u64::MAX).await.expect("scrub");
    assert_eq!(past, ScrubPage::default(), "nothing past the last slot");
}
