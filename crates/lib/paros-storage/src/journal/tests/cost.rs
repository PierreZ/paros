//! What a new store allocates (#985 (placement counts store disk)):
//! [`JournalStoreConfig::store_bytes`] is the bytes the files of a store
//! take once its first batch created its first segment, a node's and a
//! matchmaker set's alike, and still the same once more records landed
//! inside that segment. A store that holds only a format and a promise has
//! its meta file alone: the first entry allocates the segment, and a booked
//! store takes one at once, so placement counts the segment from the
//! booking on.

use super::*;

/// The bytes of every file under `dir`, and the files' names.
async fn bytes_under(provider: &SimStorageProvider, dir: &str) -> (u64, Vec<String>) {
    let mut names = provider.list_dir(dir).await.expect("list");
    names.sort();
    let mut total = 0;
    for name in &names {
        let file = provider
            .open(&format!("{dir}/{name}"), OpenOptions::read_only())
            .await
            .expect("a store holds files only");
        total += file.size().await.expect("size");
    }
    (total, names)
}

fn layouts() -> [JournalStoreConfig; 2] {
    [compact(), JournalStoreConfig::default()]
}

#[test]
fn the_default_store_costs_its_64_mib_segment_and_its_meta_file() {
    let bytes = JournalStoreConfig::default().store_bytes();
    assert_eq!(bytes, 64 * 1024 * 1024 + 8 * 1024 + 17 * 4096);
    // The geometry decides it: the small one costs far less.
    assert!(compact().store_bytes() < 1024 * 1024);
}

#[test]
fn a_node_store_allocates_what_store_bytes_says() {
    for (n, store) in layouts().into_iter().enumerate() {
        runtime().block_on(async {
            let mut sim = sim(40 + n as u64);
            run(&mut sim, move |provider| async move {
                let dir = "data/n1";
                let mut node = open_node(provider.clone(), dir, store).await.expect("open");
                node.format(&config()).await.expect("format");
                node.persist_ballot(ballot(4)).await.expect("promise");
                node.sync(MustSync::Sync).await.expect("sync");
                let (formatted, names) = bytes_under(&provider, dir).await;
                assert_eq!(names, ["meta"], "a promise lives in the metainfo");
                assert!(formatted < store.store_bytes());
                node.append_accepted(Slot(0), ballot(4), user(0, 0x5a))
                    .await
                    .expect("accept");
                node.sync(MustSync::Sync).await.expect("sync");
                let (empty, names) = bytes_under(&provider, dir).await;
                assert_eq!(empty, store.store_bytes(), "files {names:?}");
                for slot in 1..4 {
                    node.append_accepted(Slot(slot), ballot(4), user(slot, 0x5a))
                        .await
                        .expect("accept");
                }
                node.sync(MustSync::Sync).await.expect("sync");
                let (held, _) = bytes_under(&provider, dir).await;
                assert_eq!(held, empty, "records inside a segment take no new byte");
            })
            .await;
        });
    }
}

#[test]
fn a_matchmaker_store_allocates_what_store_bytes_says() {
    runtime().block_on(async {
        let mut sim = sim(42);
        run(&mut sim, |provider| async move {
            let dir = "data/mm";
            let mut registry = open_registry(provider.clone(), dir).await.expect("open");
            registry
                .register(J, ballot(1), &belief(0xC100))
                .await
                .expect("register");
            registry.sync().await.expect("sync");
            drop(registry);
            let (bytes, names) = bytes_under(&provider, dir).await;
            assert_eq!(bytes, compact().store_bytes(), "files {names:?}");
        })
        .await;
    });
}
