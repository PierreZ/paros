//! The behavioral contract suite every [`NodeStorage`] implementation must
//! pass, shared by the in-memory store and the simulation's world-backed one.

use paros_core::{Ballot, Command, MustSync, Slot};

use super::{NodeStorage, snap_chunk_count};

/// The behavioral **contract suite** every [`NodeStorage`] implementation must
/// pass (issue #21 item F): one suite, run against both [`MemStorage`] (in this crate)
/// and the simulation's world-backed storage (in `paros-sim`), so a fake can
/// never drift from the trait contract. The Stage-6/7 fault-budget logic stays
/// *outside* this contract (#70): from the trait's point of view both
/// implementations behave identically on the clean path this suite drives.
///
/// `fresh` must return an empty storage for the same single-node membership on
/// every call. `reopen` simulates a clean reboot of the same store: the reads
/// the suite asserts are the *recovery port's* (the core reads durable state
/// once, at construction), so every read-back goes through a reopen — an
/// in-memory implementation may return the same handle, a world-backed one
/// re-restores from its durable records.
///
/// # Panics
///
/// Panics on any contract violation.
#[doc(hidden)]
// One linear behavioral walk; splitting it would scatter the contract.
#[allow(clippy::too_many_lines)]
#[tracing::instrument(level = "debug", skip_all)]
pub async fn storage_contract_suite<S: NodeStorage>(
    mut fresh: impl FnMut() -> S,
    mut reopen: impl FnMut(S) -> S,
) {
    use paros_core::{ClientId, ClientSeq, Entry, Value};
    let ballot = |round: u64| Ballot {
        round,
        node: paros_core::NodeId(0),
    };
    let user = |seq: u64, byte: u8| {
        Command::User(Entry {
            client: ClientId(7),
            seq: ClientSeq(seq),
            value: Value(vec![byte]),
        })
    };

    // The format marker (#147): absent on a fresh store, present once
    // `format` is flushed and reopened, and written by nothing else — a
    // store that took protocol writes without ever being formatted stays
    // unformatted (that is exactly the wiped-disk shape the driver refuses).
    let s = fresh();
    assert!(!s.is_formatted(), "a fresh store carries no format marker");
    let mut s = fresh();
    s.persist_ballot(ballot(1)).await.expect("ballot");
    s.sync(MustSync::Sync).await.expect("sync ballot");
    let s = reopen(s);
    assert!(
        !s.is_formatted(),
        "protocol writes never format a store on their own"
    );
    let mut s = fresh();
    s.format().await.expect("format");
    s.sync(MustSync::Sync).await.expect("sync format");
    let s = reopen(s);
    assert!(s.is_formatted(), "the format marker survives a reopen");
    let mut s = reopen(s);
    s.persist_ballot(ballot(2))
        .await
        .expect("ballot after format");
    s.sync(MustSync::Sync).await.expect("sync after format");
    let s = reopen(s);
    assert!(s.is_formatted(), "the format marker is never removed");

    // Scalars + per-slot records round-trip through a Sync flush.
    let mut s = fresh();
    s.persist_ballot(ballot(4)).await.expect("ballot");
    s.append_accepted(Slot(0), ballot(4), user(1, 0xa))
        .await
        .expect("append 0");
    s.append_accepted(Slot(1), ballot(4), user(2, 0xb))
        .await
        .expect("append 1");
    s.set_chosen_index(Slot(1)).await.expect("chosen index");
    s.sync(MustSync::Sync).await.expect("sync");
    let mut s = reopen(s);
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
    let mut s = reopen(s);
    assert_eq!(
        s.accepted(Slot(1)).map(|(b, _)| b),
        Some(ballot(5)),
        "append is an upsert by slot"
    );

    // Truncation raises the floor, drops the prefix, and seals the ledger.
    s.truncate(Slot(1), &[(ClientId(7), ClientSeq(1), Slot(0))])
        .await
        .expect("truncate");
    s.sync(MustSync::Sync).await.expect("sync truncate");
    let mut s = reopen(s);
    assert_eq!(s.first_slot(), Slot(1), "the floor rose");
    assert!(
        s.accepted(Slot(0)).is_none(),
        "a truncated record is unreadable"
    );
    assert_eq!(
        s.sealed_sessions(),
        vec![(ClientId(7), ClientSeq(1), Slot(0))],
        "the sealed ledger survives the truncation"
    );
    // A floor never moves backward.
    s.truncate(Slot(0), &[]).await.expect("re-truncate lower");
    s.sync(MustSync::Sync).await.expect("sync no-op truncate");
    let s = reopen(s);
    assert_eq!(s.first_slot(), Slot(1), "the floor is monotone");

    // A snapshot install: chosen index jumps, promise takes the max (never
    // regresses), floor lands one past the boundary, sessions seal. The blob
    // comes from a *source* storage whose applied prefix genuinely covers the
    // boundary, so an application-typed implementation's boundary checks hold.
    let mut source = fresh();
    for s in 0..=4u64 {
        source
            .append_accepted(Slot(s), ballot(1), user(10 + s, 0x40))
            .await
            .expect("source append");
    }
    source
        .set_chosen_index(Slot(4))
        .await
        .expect("source index");
    for s in 0..=4u64 {
        source
            .apply(Slot(4), Slot(s), &user(10 + s, 0x40))
            .await
            .expect("source apply");
    }
    source.sync(MustSync::Sync).await.expect("source sync");
    let blob = source.snapshot().await;

    let mut s = fresh();
    s.persist_ballot(ballot(9)).await.expect("high promise");
    s.sync(MustSync::Sync).await.expect("sync promise");
    s.install_snapshot(
        Slot(4),
        ballot(2),
        blob,
        &[(ClientId(7), ClientSeq(2), Slot(3))],
    )
    .await
    .expect("install");
    s.sync(MustSync::Sync).await.expect("sync install");
    let s = reopen(s);
    let (hs, _config) = s.initial_state();
    assert_eq!(hs.chosen_index, Some(Slot(4)), "the install set the index");
    assert_eq!(
        hs.max_promised_ballot,
        ballot(9),
        "an install never lowers the promise"
    );
    assert_eq!(
        s.first_slot(),
        Slot(5),
        "the floor is one past the boundary"
    );
    assert_eq!(
        s.sealed_sessions(),
        vec![(ClientId(7), ClientSeq(2), Slot(3))],
        "the install sealed the peer's ledger"
    );

    // A decided snapshot point (#101): recorded at its marker slot, retained
    // across a reopen, chunked at the fixed size, and chunk reads reassemble
    // exactly the blob the point captured.
    let mut s = fresh();
    for slot in 0..=2u64 {
        s.append_accepted(Slot(slot), ballot(1), user(20 + slot, 0x50))
            .await
            .expect("point append");
    }
    s.set_chosen_index(Slot(2)).await.expect("point index");
    for slot in 0..=2u64 {
        s.apply(Slot(2), Slot(slot), &user(20 + slot, 0x50))
            .await
            .expect("point apply");
    }
    let blob = s.snapshot().await;
    s.record_snapshot(Slot(2)).await.expect("record point");
    s.sync(MustSync::Sync).await.expect("sync point");
    let mut s = reopen(s);
    assert_eq!(
        s.latest_snap_point(),
        Some(Slot(2)),
        "the decided point survives a reopen"
    );
    let chunks = s
        .snap_chunk_count(Slot(2))
        .expect("the retained point reports its chunk count");
    assert_eq!(
        chunks,
        snap_chunk_count(blob.len()),
        "the chunk count matches the fixed chunk size"
    );
    let mut reassembled = Vec::new();
    for chunk in 0..chunks {
        reassembled.extend(
            s.read_snap_chunk(Slot(2), chunk)
                .await
                .expect("a clean chunk reads back"),
        );
    }
    assert_eq!(reassembled, blob, "chunks reassemble the exact blob");
    assert!(
        s.read_snap_chunk(Slot(3), 0).await.is_none(),
        "a point this store does not retain answers nothing"
    );
    // A chunk write round-trips (repair installs the identical bytes).
    let first = s.read_snap_chunk(Slot(2), 0).await.expect("first chunk");
    s.write_snap_chunk(Slot(2), 0, &first)
        .await
        .expect("chunk write succeeds");
    s.sync(MustSync::Sync).await.expect("sync chunk write");
    let s = reopen(s);
    assert_eq!(
        s.read_snap_chunk(Slot(2), 0).await,
        Some(first),
        "a written chunk reads back identically"
    );
}
