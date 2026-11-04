//! `settle` (#348) makes a store durable as it stands: it writes no byte,
//! the store opens to the same records after it, and a directory that does
//! not exist holds nothing to settle.

use super::*;
use crate::journal::settle;

/// Every file of `dir` with its bytes, by name.
async fn image(provider: &SimStorageProvider, dir: &str) -> Vec<(String, Vec<u8>)> {
    let mut names = provider.list_dir(dir).await.expect("list");
    names.sort();
    let mut files = Vec::new();
    for name in names {
        let file = provider
            .open(&format!("{dir}/{name}"), OpenOptions::read_only())
            .await
            .expect("open");
        let size = usize::try_from(file.size().await.expect("size")).expect("small");
        let mut bytes = vec![0; size];
        file.read_at(0, &mut bytes).await.expect("read");
        files.push((name, bytes));
    }
    files
}

#[test]
fn settle_writes_no_byte_and_the_store_opens_the_same() {
    runtime().block_on(async {
        let mut sim = sim(21);
        run(&mut sim, |provider| async move {
            let mut node = open_node(provider.clone(), "data/n1", compact())
                .await
                .expect("open");
            node.format(&config()).await.expect("format");
            node.persist_ballot(ballot(4)).await.expect("promise");
            node.append_accepted(Slot(0), ballot(4), user(0, 0xE1))
                .await
                .expect("accept");
            node.sync(MustSync::Sync).await.expect("sync");
            drop(node);
            let before = image(&provider, "data/n1").await;
            assert!(!before.is_empty(), "the store has files");
            settle(&provider, "data/n1").await.expect("settle");
            assert_eq!(image(&provider, "data/n1").await, before, "no byte written");
            let node = open_node(provider.clone(), "data/n1", compact())
                .await
                .expect("reopens");
            assert!(node.is_formatted());
            assert_eq!(node.accepted(Slot(0)), Some((ballot(4), user(0, 0xE1))));
            settle(&provider, "data/none")
                .await
                .expect("a missing directory holds nothing");
        })
        .await;
    });
}
