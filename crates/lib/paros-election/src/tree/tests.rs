use paros_journal::Seq;

use super::{Change, ErrorKind, Fate, Tree, Turn, TxnError};
use crate::entry::{Check, Entry, Op, SessionId, Term, Txn};
use crate::path::Path;
use crate::recipes::{Counter, Latch};

const A: SessionId = SessionId(1);
const B: SessionId = SessionId(2);

fn path(text: &str) -> Path {
    Path::new(text).unwrap()
}

fn latch() -> Latch {
    Latch::new(path("/election"))
}

fn open(session: SessionId) -> Entry {
    Entry::Open {
        session,
        txn: Txn::ops(latch().join(vec![])),
    }
}

fn txn(session: SessionId, op_seq: u64, txn: Txn) -> Entry {
    Entry::Txn {
        session,
        op_seq,
        txn,
    }
}

/// A tree and the entries it folded, so a test can apply the next one.
struct Log {
    tree: Tree,
}

impl Log {
    fn new() -> Self {
        Self { tree: Tree::new() }
    }

    fn apply(&mut self, entry: &Entry) -> (Fate, Vec<Change>) {
        let seq = self.tree.next();
        let out = self.tree.apply(seq, Some(entry));
        self.tree.assert_invariants();
        out
    }
}

#[test]
fn the_lowest_candidate_holds_the_latch_and_its_term_is_where_it_took_it() {
    let mut log = Log::new();
    let (fate, changes) = log.apply(&open(A));
    assert_eq!(
        fate,
        Fate::Opened {
            session: A,
            result: Ok(())
        }
    );
    let turn = Turn {
        holder: A,
        term: Term(Seq(0)),
    };
    assert!(changes.contains(&Change::Turn(path("/election"), Some(turn))));
    log.apply(&open(B));
    assert_eq!(latch().holder(&log.tree), Some(turn), "B waits behind A");
    let order: Vec<_> = latch()
        .candidates(&log.tree)
        .into_iter()
        .map(|c| c.0)
        .collect();
    assert_eq!(order, [A, B]);
}

#[test]
fn a_copy_of_an_entry_does_nothing() {
    let mut log = Log::new();
    log.apply(&open(A));
    let counter = Counter::new(path("/count"));
    let create = txn(A, 1, Txn::ops(counter.create()));
    log.apply(&create);
    let set = txn(A, 2, counter.set(1, 0));
    assert!(matches!(
        log.apply(&set).0,
        Fate::Done { result: Ok(()), .. }
    ));
    assert_eq!(
        log.apply(&set).0,
        Fate::Duplicate {
            session: A,
            op_seq: 2
        }
    );
    assert_eq!(
        log.apply(&open(A)).0,
        Fate::Duplicate {
            session: A,
            op_seq: 0
        }
    );
    assert_eq!(counter.get(&log.tree), Some((1, 1)), "one set, one version");
    assert_eq!(log.tree.session(A).unwrap().last, Seq(2));
}

#[test]
fn an_expiry_holds_only_on_the_last_position_and_leaves_a_tombstone() {
    let mut log = Log::new();
    log.apply(&open(A));
    log.apply(&open(B));
    log.apply(&txn(A, 1, Txn::default()));
    let stale = Entry::Expire {
        session: A,
        last: Seq(0),
    };
    assert_eq!(log.apply(&stale).0, Fate::ExpireVoid { session: A });
    assert!(
        log.tree.session(A).is_some(),
        "a heartbeat after the read saves A"
    );
    let expire = Entry::Expire {
        session: A,
        last: Seq(2),
    };
    let (fate, changes) = log.apply(&expire);
    assert_eq!(fate, Fate::Expired { session: A });
    assert!(changes.contains(&Change::Expired(A)));
    let turn = latch().holder(&log.tree).unwrap();
    assert_eq!(
        turn,
        Turn {
            holder: B,
            term: Term(Seq(4))
        },
        "B takes over at the expiry"
    );
    assert!(log.tree.expired(A));
    assert!(
        log.tree.nodes().all(|(_, n)| n.owner != Some(A)),
        "A's nodes went with it"
    );
    // A late copy of A's open and of its transaction: void.
    assert_eq!(
        log.apply(&open(A)).0,
        Fate::Gone {
            session: A,
            op_seq: 0
        }
    );
    assert_eq!(
        log.apply(&txn(A, 2, Txn::default())).0,
        Fate::Gone {
            session: A,
            op_seq: 2
        }
    );
    assert_eq!(latch().holder(&log.tree), Some(turn));
}

#[test]
fn a_deposed_holder_s_guarded_transaction_fails() {
    let mut log = Log::new();
    log.apply(&open(A));
    log.apply(&open(B));
    let term = latch().holder(&log.tree).unwrap().term;
    // B revokes A: it deletes A's node. A's session lives.
    let revoke = Txn::ops(latch().leave(&log.tree, A));
    log.apply(&txn(B, 1, revoke));
    let turn = latch().holder(&log.tree).unwrap();
    assert_eq!(turn.holder, B);
    assert!(turn.term > term, "the term grows");
    let act = latch().guarded(
        term,
        vec![Op::Ensure {
            path: path("/acted"),
        }],
    );
    let (fate, _) = log.apply(&txn(A, 1, act));
    assert_eq!(
        fate,
        Fate::Done {
            session: A,
            op_seq: 1,
            result: Err(TxnError {
                check: true,
                index: 0,
                kind: ErrorKind::NotHolder
            })
        }
    );
    assert!(log.tree.node(&path("/acted")).is_none());
    let act = latch().guarded(
        turn.term,
        vec![Op::Ensure {
            path: path("/acted"),
        }],
    );
    assert!(matches!(
        log.apply(&txn(B, 2, act)).0,
        Fate::Done { result: Ok(()), .. }
    ));
    assert!(log.tree.node(&path("/acted")).is_some());
}

#[test]
fn a_transaction_is_all_or_none() {
    let mut log = Log::new();
    log.apply(&open(A));
    let ops = vec![
        Op::Ensure { path: path("/x") },
        Op::Delete {
            path: path("/missing"),
            version: None,
        },
    ];
    let (fate, _) = log.apply(&txn(A, 1, Txn::ops(ops)));
    assert!(matches!(
        fate,
        Fate::Done {
            result: Err(TxnError {
                check: false,
                index: 1,
                kind: ErrorKind::NoNode
            }),
            ..
        }
    ));
    assert!(
        log.tree.node(&path("/x")).is_none(),
        "the first op is undone"
    );
    assert_eq!(log.tree.session(A).unwrap().op_seq, 1, "the op_seq is used");
}

#[test]
fn the_rules_of_zookeeper_nodes() {
    let mut log = Log::new();
    log.apply(&open(A));
    let eph = Op::Create {
        path: path("/e"),
        data: vec![],
        ephemeral: true,
        sequential: false,
    };
    log.apply(&txn(A, 1, Txn::ops(vec![eph])));
    let under = Op::Ensure { path: path("/e/c") };
    let fate = log.apply(&txn(A, 2, Txn::ops(vec![under]))).0;
    assert!(matches!(
        fate,
        Fate::Done {
            result: Err(TxnError {
                kind: ErrorKind::EphemeralParent,
                ..
            }),
            ..
        }
    ));
    let not_empty = Op::Delete {
        path: path("/election"),
        version: None,
    };
    let fate = log.apply(&txn(A, 3, Txn::ops(vec![not_empty]))).0;
    assert!(matches!(
        fate,
        Fate::Done {
            result: Err(TxnError {
                kind: ErrorKind::NotEmpty,
                ..
            }),
            ..
        }
    ));
    let root = Op::Delete {
        path: Path::root(),
        version: None,
    };
    let fate = log.apply(&txn(A, 4, Txn::ops(vec![root]))).0;
    assert!(matches!(
        fate,
        Fate::Done {
            result: Err(TxnError {
                kind: ErrorKind::Root,
                ..
            }),
            ..
        }
    ));
    let bad = Check::Version {
        path: path("/e"),
        version: 9,
    };
    let fate = log
        .apply(&txn(
            A,
            5,
            Txn {
                checks: vec![bad],
                ops: vec![],
            },
        ))
        .0;
    assert!(matches!(
        fate,
        Fate::Done {
            result: Err(TxnError {
                kind: ErrorKind::BadVersion,
                ..
            }),
            ..
        }
    ));
}

#[test]
fn children_are_found_past_a_sibling_that_sorts_first() {
    let mut log = Log::new();
    log.apply(&open(A));
    let ops = vec![
        Op::Ensure { path: path("/a") },
        Op::Ensure { path: path("/a-x") },
        Op::Ensure { path: path("/a/b") },
    ];
    log.apply(&txn(A, 1, Txn::ops(ops)));
    let children: Vec<_> = log
        .tree
        .children(&path("/a"))
        .map(|(p, _)| p.clone())
        .collect();
    assert_eq!(children, [path("/a/b")]);
    let delete = Op::Delete {
        path: path("/a"),
        version: None,
    };
    let fate = log.apply(&txn(A, 2, Txn::ops(vec![delete]))).0;
    assert!(matches!(
        fate,
        Fate::Done {
            result: Err(TxnError {
                kind: ErrorKind::NotEmpty,
                ..
            }),
            ..
        }
    ));
}

#[test]
fn a_checkpoint_counts_only_at_its_read_position_and_with_its_state() {
    let mut log = Log::new();
    log.apply(&open(A));
    let at = log.tree.next();
    let fresh = Entry::Checkpoint {
        at,
        tree: Box::new(log.tree.clone()),
    };
    // Another entry lands between the read and the checkpoint.
    log.apply(&open(B));
    assert_eq!(log.apply(&fresh).0, Fate::Checkpoint { taken: false });
    let at = log.tree.next();
    let mut forged = log.tree.clone();
    forged.tombs.insert(SessionId(99));
    let forged = Entry::Checkpoint {
        at,
        tree: Box::new(forged),
    };
    assert_eq!(
        log.apply(&forged).0,
        Fate::Checkpoint { taken: false },
        "a state that is not the fold's"
    );
    let at = log.tree.next();
    let good = Entry::Checkpoint {
        at,
        tree: Box::new(log.tree.clone()),
    };
    assert_eq!(log.apply(&good).0, Fate::Checkpoint { taken: true });
    assert!(log.tree.checkpoints().contains(&at));
    // A new member starts at the checkpoint and folds to the same tree.
    let mut late = Tree::from_checkpoint(at, Some(&good)).expect("a checkpoint the fold takes");
    let _ = late.apply(at, Some(&good));
    assert_eq!(late, log.tree);
    assert_eq!(Tree::from_checkpoint(at, Some(&open(B))), None);
    assert_eq!(Tree::from_checkpoint(Seq(at.0 + 1), Some(&good)), None);
}

#[test]
fn a_forged_checkpoint_state_does_not_start_a_fold() {
    let mut tree = Tree::new();
    let (_, _) = tree.apply(Seq(0), Some(&open(A)));
    let at = tree.next();
    let start = |tree: &Tree| {
        let entry = Entry::Checkpoint {
            at,
            tree: Box::new(tree.clone()),
        };
        Tree::from_checkpoint(at, Some(&entry)).is_some()
    };
    assert!(start(&tree), "the fold's own state starts a fold");
    let mut late_root = tree.clone();
    late_root.nodes.get_mut(&Path::root()).unwrap().created = Seq(at.0 + 5);
    assert!(!start(&late_root), "the root is created at or below next");
    let mut full = tree.clone();
    full.nodes.get_mut(&Path::root()).unwrap().version = u64::MAX;
    assert!(!start(&full), "a version that cannot grow");
}
