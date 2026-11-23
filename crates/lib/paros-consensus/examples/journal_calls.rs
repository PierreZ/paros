//! **The journal calls: `SetLeader`, `Write`, `Read`, `Truncate`.**
//!
//! Run it: `cargo run -p paros-consensus --example journal_calls`
//!
//! `quickstart` shows the driver loop. This example shows what the log
//! *means*: the journal state machine that each node runs over the chosen
//! slots (`paros_consensus::journal::state`). Read it when a proposal of
//! yours is chosen and the answer is not the one you expected.
//!
//! # Two leaders, two layers
//!
//! - The **Paxos leader** is a node. It holds a ballot and runs Phase 2. A
//!   client must send its proposals to it, and nothing else about it reaches
//!   a client.
//! - The **writer** of the journal is a client, named by a [`LeaderUuid`].
//!   It is the one fence that a `Write` and a `Truncate` are judged
//!   against.
//!
//! The two never meet. A new Paxos leader does not change the writer, and a
//! new writer does not change the Paxos leader.
//!
//! # Judged at apply, never at propose
//!
//! `propose` and `propose_control` answer only "this node is the leader, and
//! your command has a slot". Whether the journal takes the command is
//! decided later, when the slot is applied, by each node alone and in slot
//! order. The verdict is an [`Outcome`], and the driver reads it from
//! `Ready::committed`. This is why a retry is safe: a retry takes a new
//! slot, and the journal recognizes it there.
//!
//! # What the trace shows
//!
//! 1. Writer A wins the journal with `SetLeader(A, None)` and appends.
//! 2. A retry of a write that the journal holds is a `Duplicate`: the same
//!    answer, and nothing moves.
//! 3. A write at a position that is not the next one is refused.
//! 4. Writer B takes the journal with `SetLeader(B, Some(A))`. A late write
//!    of A is refused: A is fenced. A second `SetLeader(.., Some(A))` loses
//!    the compare-and-set.
//! 5. A reader pages through the journal from a position.
//! 6. B truncates. A read below the new first position is told so, and each
//!    node drops the slots that it no longer needs.

mod common;

use paros_consensus::journal::{Control, JournalIdentifier, LeaderUuid, Outcome, Seq};
use paros_consensus::replica::LogRead;
use paros_consensus::{Config, NodeId, Slot, Storage};

use common::cluster::Cluster;

const N1: NodeId = NodeId(1);
const N2: NodeId = NodeId(2);
const N3: NodeId = NodeId(3);

const WRITER_A: LeaderUuid = LeaderUuid(0xA);
const WRITER_B: LeaderUuid = LeaderUuid(0xB);

fn main() {
    let mut cluster = Cluster::new(
        [N1, N2, N3].map(|id| Config::new(id, JournalIdentifier::UNSET).with_peers([N1, N2, N3])),
    );
    cluster.elect(N1);

    writer_a(&mut cluster);
    writer_b(&mut cluster);
    read(&mut cluster);
    truncate(&mut cluster);
}

/// The verdict of the journal on `slot`, as node 1 learned it.
fn outcome(cluster: &Cluster, slot: Slot) -> Outcome {
    let outcome = cluster.outcome(N1, slot).expect("the slot is applied");
    // Each node judged the slot alone and reached the same verdict.
    for id in [N2, N3] {
        assert_eq!(cluster.outcome(id, slot), Some(outcome));
    }
    outcome.clone()
}

/// Parts 1 to 3: one writer, its retries and its mistakes.
fn writer_a(cluster: &mut Cluster) {
    println!("== 1. writer A takes a new journal and appends");
    let slot = cluster.control(
        N1,
        Control::SetLeader {
            new: WRITER_A,
            old: None,
        },
    );
    assert!(matches!(outcome(cluster, slot), Outcome::Leader(_)));
    println!("   SetLeader(A, old = none)      -> A is the writer");
    for (seq, text) in [(0, "a0"), (1, "a1")] {
        let slot = cluster.write(N1, WRITER_A, seq, text);
        let accepted = Outcome::Accepted {
            seq: Seq(seq),
            count: 1,
        };
        assert_eq!(outcome(cluster, slot), accepted);
        println!("   Write(A, position {seq}, {text:?})   -> accepted");
    }

    println!("\n== 2. a retry is recognized");
    // The client did not hear the answer for position 1 and sends the same
    // write again. It takes a new slot. The journal finds the same records
    // at that position and acknowledges them one more time.
    let slot = cluster.write(N1, WRITER_A, 1, "a1");
    let duplicate = Outcome::Duplicate {
        seq: Seq(1),
        count: 1,
    };
    assert_eq!(outcome(cluster, slot), duplicate);
    println!("   Write(A, position 1, \"a1\") again -> duplicate, nothing moved");
    assert_eq!(cluster.journal(N1), ["a0", "a1"]);

    println!("\n== 3. a write must ask for the next position");
    let slot = cluster.write(N1, WRITER_A, 5, "a5");
    let Outcome::Refused(view) = outcome(cluster, slot) else {
        panic!("a write past the next position is refused");
    };
    assert_eq!(view.next_seq, Seq(2));
    println!(
        "   Write(A, position 5, \"a5\")   -> refused, the next position is {}",
        view.next_seq.0
    );
}

/// Part 4: the fence.
fn writer_b(cluster: &mut Cluster) {
    println!("\n== 4. writer B takes over, and A is fenced");
    let slot = cluster.control(
        N1,
        Control::SetLeader {
            new: WRITER_B,
            old: Some(WRITER_A),
        },
    );
    let Outcome::Leader(view) = outcome(cluster, slot) else {
        panic!("the compare-and-set wins: A is the writer");
    };
    assert_eq!(view.leader, Some(WRITER_B));
    assert_eq!(view.next_seq, Seq(2), "B continues where A stopped");
    println!("   SetLeader(B, old = A)         -> B is the writer, from position 2");

    // A did not hear about B. Its write was in flight; Paxos chooses it, and
    // the journal refuses it. No record of a fenced writer enters the
    // journal after the `SetLeader` slot.
    let slot = cluster.write(N1, WRITER_A, 2, "a2");
    let Outcome::Refused(view) = outcome(cluster, slot) else {
        panic!("a fenced writer is refused");
    };
    assert_eq!(view.leader, Some(WRITER_B));
    println!("   Write(A, position 2, \"a2\")   -> refused, the writer is B");

    // A third client that still believes A is the writer loses.
    let slot = cluster.control(
        N1,
        Control::SetLeader {
            new: LeaderUuid(0xC),
            old: Some(WRITER_A),
        },
    );
    assert!(matches!(outcome(cluster, slot), Outcome::LeaderRefused(_)));
    println!("   SetLeader(C, old = A)         -> refused, A is not the writer");

    let slot = cluster.write(N1, WRITER_B, 2, "b2");
    assert!(matches!(outcome(cluster, slot), Outcome::Accepted { .. }));
    let slot = cluster.write(N1, WRITER_B, 3, "b3");
    assert!(matches!(outcome(cluster, slot), Outcome::Accepted { .. }));
    println!("   Write(B, positions 2 and 3)   -> accepted");
    assert_eq!(cluster.journal(N1), ["a0", "a1", "b2", "b3"]);
}

/// Part 5: a read is a pure call on any node that learned the slots.
fn read(cluster: &mut Cluster) {
    println!("\n== 5. a reader pages through the journal on node 3");
    let mut from = Seq(0);
    loop {
        // At most two records in one page.
        let LogRead::Page(page) = cluster.node(N3).read_log(from, 2, usize::MAX) else {
            panic!("node 3 holds the journal from position 0");
        };
        if page.records.is_empty() {
            // The page of an empty read still names the writer and the end
            // of the journal: a reader that tails learns both in band.
            assert_eq!(page.state.leader, Some(WRITER_B));
            assert_eq!(page.state.next_seq, Seq(4));
            println!("   read from {} -> the end, the writer is B", from.0);
            break;
        }
        let texts: Vec<String> = page
            .records
            .iter()
            .map(|record| String::from_utf8_lossy(record).into_owned())
            .collect();
        println!("   read from {} -> {texts:?}", from.0);
        from = page.next();
    }
}

/// Part 6: truncation is a command of the writer, decided by consensus.
fn truncate(cluster: &mut Cluster) {
    println!("\n== 6. writer B truncates below position 2");
    let slots_before = cluster.node(N2).acceptor().records().len();
    let slot = cluster.control(
        N1,
        Control::Truncate {
            leader: WRITER_B,
            up_to: Seq(2),
        },
    );
    let Outcome::Trimmed(view) = outcome(cluster, slot) else {
        panic!("the writer may truncate");
    };
    assert_eq!(view.first_seq, Seq(2));
    println!("   Truncate(B, up to 2)          -> the journal starts at position 2");

    // A reader below the first position is told where the journal starts.
    let read = cluster.node(N2).read_log(Seq(0), 10, usize::MAX);
    assert!(matches!(read, LogRead::Truncated(view) if view.first_seq == Seq(2)));
    println!("   read from 0                   -> truncated, start at 2");
    assert_eq!(cluster.journal(N2), ["b2", "b3"]);

    // The truncation is in the log, so each node reached it in its own
    // walk, and each node dropped there the slots whose records are all
    // below position 2. The floor is durable: a restart reads it back.
    for id in [N1, N2, N3] {
        let floor = cluster.node(id).acceptor().first_slot();
        assert!(floor > Slot(0), "node {id} dropped a prefix of its log");
        assert_eq!(cluster.disk(id).first_slot(), floor);
    }
    let slots_after = cluster.node(N2).acceptor().records().len();
    assert!(slots_after < slots_before);
    println!(
        "   each node compacted           -> node 2 keeps {slots_after} slots of {slots_before}"
    );

    // A late `Truncate` of the fenced writer changes nothing.
    let slot = cluster.control(
        N1,
        Control::Truncate {
            leader: WRITER_A,
            up_to: Seq(4),
        },
    );
    assert!(matches!(
        outcome(cluster, slot),
        Outcome::TruncateRefused(_)
    ));
    println!("   Truncate(A, up to 4)          -> refused, the writer is B");
    assert_eq!(cluster.journal(N3), ["b2", "b3"]);
}
