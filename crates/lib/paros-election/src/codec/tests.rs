use paros_journal::{Header, Record, Seq};

use super::{FORMAT, HEADER_KEY, decode, encode};
use crate::entry::{Check, Entry, Op, SessionId, Term, Txn};
use crate::path::Path;
use crate::tree::Tree;

fn path(text: &str) -> Path {
    Path::new(text).unwrap()
}

fn txn() -> Txn {
    Txn {
        checks: vec![
            Check::Version {
                path: path("/a"),
                version: 3,
            },
            Check::Holds {
                parent: path("/e"),
                term: Term(Seq(7)),
            },
        ],
        ops: vec![
            Op::Create {
                path: path("/e/n-"),
                data: b"x".to_vec(),
                ephemeral: true,
                sequential: true,
            },
            Op::Ensure { path: path("/b") },
            Op::SetData {
                path: path("/a"),
                data: vec![],
                version: Some(3),
            },
            Op::Delete {
                path: path("/c"),
                version: None,
            },
        ],
    }
}

#[test]
fn every_entry_round_trips() {
    let mut tree = Tree::new();
    let open = Entry::Open {
        session: SessionId(5),
        txn: Txn::ops(vec![Op::Ensure { path: path("/e") }]),
    };
    let _ = tree.apply(Seq(0), Some(&open));
    let entries = [
        open,
        Entry::Txn {
            session: SessionId(5),
            op_seq: 1,
            txn: txn(),
        },
        Entry::Expire {
            session: SessionId(5),
            last: Seq(0),
        },
        Entry::Checkpoint {
            at: Seq(1),
            tree: Box::new(tree),
        },
    ];
    for entry in entries {
        assert_eq!(decode(&encode(&entry)), Some(entry));
    }
}

#[test]
fn a_record_that_is_not_an_entry_decodes_to_nothing() {
    let entry = Entry::Expire {
        session: SessionId(1),
        last: Seq(2),
    };
    let good = encode(&entry);
    assert_eq!(
        decode(&Record::new(good.payload.clone())),
        None,
        "no header"
    );
    let mut other = good.clone();
    other.headers.client[0].value = vec![FORMAT + 1];
    assert_eq!(decode(&other), None, "another format");
    let mut twice = good.clone();
    twice.headers.client.push(Header {
        key: HEADER_KEY.to_vec(),
        value: vec![FORMAT],
    });
    assert_eq!(decode(&twice), None, "two marks");
    let mut long = good.clone();
    long.payload.push(0);
    assert_eq!(decode(&long), None, "bytes after the entry");
    for cut in 0..good.payload.len() {
        let mut short = good.clone();
        short.payload.truncate(cut);
        assert_eq!(decode(&short), None, "cut at {cut}");
    }
    let mut forged = encode(&Entry::Txn {
        session: SessionId(1),
        op_seq: 1,
        txn: Txn::ops(vec![Op::Ensure { path: path("/a") }]),
    });
    // The path `/a` becomes `a/`: not a path.
    let at = forged.payload.len() - 2;
    forged.payload[at] = b'a';
    forged.payload[at + 1] = b'/';
    assert_eq!(decode(&forged), None, "a bad path");
}
