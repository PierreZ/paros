use super::*;
use crate::membership::QuorumSystem;
use crate::types::NodeId;

fn grid() -> AcceptorConfig {
    AcceptorConfig::new((1..=6).map(NodeId).collect(), QuorumSystem::grid(2, 3))
}

fn basis(config: AcceptorConfig, since: Ballot) -> ReadBasis<NodeId> {
    ReadBasis {
        config,
        since,
        fence: None,
    }
}

fn ballot(round: u64) -> Ballot {
    Ballot {
        round,
        node: NodeId(0),
    }
}

/// The tally: a read addressed to row 1 (`ctx % 2`) completes on that
/// row alone — a row that is not a column — at the maximum watermark,
/// and is served only once the replica covers it.
#[test]
fn a_read_completes_on_its_row_at_the_maximum_watermark() {
    let mut reads: QuorumReads<NodeId> = QuorumReads::new();
    // Node 5 is the reader, in row 1 = {4, 5, 6}: it seeds its own
    // watermark and asks the other two.
    let addressees = reads.open(
        1,
        Some(1),
        basis(grid(), ballot(0)),
        0,
        Some((NodeId(5), Some(Slot(2)))),
    );
    assert_eq!(addressees, vec![NodeId(4), NodeId(6)]);
    assert_eq!(reads.pending()[0].row(), Some(1));
    assert!(reads.serve(|_| true).is_empty(), "one answer is no row");
    assert_eq!(
        reads.fold(1, NodeId(4), Some(Slot(3)), None),
        PreReadFold::Counted
    );
    assert_eq!(
        reads.fold(1, NodeId(4), Some(Slot(9)), None),
        PreReadFold::Ignored,
        "an acceptor answers once"
    );
    assert_eq!(
        reads.fold(2, NodeId(6), Some(Slot(3)), None),
        PreReadFold::Ignored,
        "no read at that token"
    );
    assert!(reads.serve(|_| true).is_empty(), "two of three is no row");
    assert_eq!(
        reads.fold(1, NodeId(6), None, None),
        PreReadFold::Counted,
        "a never-voted acceptor still answers"
    );
    // The row is whole: confirmed at max(2, 3, nothing) = 3, but the
    // replica has not applied slot 3 yet.
    assert!(reads.serve(|index| index <= Some(Slot(2))).is_empty());
    assert_eq!(reads.pending()[0].confirmed_index(), Some(Some(Slot(3))));
    assert_eq!(
        reads.fold(1, NodeId(5), Some(Slot(7)), None),
        PreReadFold::Ignored,
        "a confirmed read folds nothing more"
    );
    assert_eq!(
        reads.serve(|index| index <= Some(Slot(3))),
        vec![(1, Some(Slot(3)))]
    );
    assert!(reads.is_empty());
}

/// The row guard is the tally's: an answer from outside the read's row
/// is never counted, and a newer configuration it names abandons
/// nothing — so a caller that skips its own guard cannot feed `serve` a
/// watermark from outside the row.
#[test]
fn an_answer_from_outside_the_row_is_ignored() {
    let mut reads: QuorumReads<NodeId> = QuorumReads::new();
    // Row 1 = {4, 5, 6}; node 1 is in row 0.
    let _ = reads.open(1, Some(1), basis(grid(), ballot(1)), 0, None);
    assert_eq!(
        reads.fold(1, NodeId(1), Some(Slot(9)), None),
        PreReadFold::Ignored,
        "a row-0 acceptor is not an addressee of row 1"
    );
    assert_eq!(
        reads.fold(1, NodeId(1), Some(Slot(9)), Some(ballot(2))),
        PreReadFold::Ignored,
        "an answer from outside the row abandons nothing"
    );
    assert_eq!(reads.pending().len(), 1);
    assert!(reads.pending()[0].watermarks().is_empty());
    for id in [4, 5, 6] {
        assert_eq!(
            reads.fold(1, NodeId(id), Some(Slot(2)), None),
            PreReadFold::Counted
        );
    }
    assert_eq!(reads.serve(|_| true), vec![(1, Some(Slot(2)))]);
}

/// A row of acceptors that never voted confirms at the empty index,
/// which any replica covers — the read of an empty log.
#[test]
fn an_unvoted_row_confirms_at_the_empty_index() {
    let mut reads: QuorumReads<NodeId> = QuorumReads::new();
    let majority = AcceptorConfig::new((0..3).map(NodeId).collect(), QuorumSystem::Majority);
    let addressees = reads.open(
        4,
        None,
        basis(majority, ballot(0)),
        0,
        Some((NodeId(0), None)),
    );
    assert_eq!(addressees, vec![NodeId(1), NodeId(2)]);
    assert_eq!(reads.fold(4, NodeId(1), None, None), PreReadFold::Counted);
    assert_eq!(reads.serve(|index| index.is_none()), vec![(4, None)]);
}

/// A configuration that moves — at the reader, or reported by a row
/// member — abandons the read: the row asked need not intersect the
/// successor's columns.
#[test]
fn a_newer_configuration_abandons_the_read() {
    let mut reads: QuorumReads<NodeId> = QuorumReads::new();
    let _ = reads.open(0, Some(0), basis(grid(), ballot(1)), 0, None);
    assert_eq!(
        reads.fold(0, NodeId(1), Some(Slot(1)), Some(ballot(1))),
        PreReadFold::Counted,
        "the read's own configuration ballot is not newer"
    );
    assert_eq!(
        reads.fold(0, NodeId(2), Some(Slot(1)), Some(ballot(2))),
        PreReadFold::Superseded
    );
    assert!(reads.is_empty());
    let _ = reads.open(2, Some(0), basis(grid(), ballot(1)), 0, None);
    let _ = reads.open(3, Some(1), basis(grid(), ballot(3)), 0, None);
    reads.abandon_superseded(ballot(3));
    assert_eq!(reads.pending().len(), 1);
    assert_eq!(reads.pending()[0].ctx(), 3);
}

/// The TTL: a read that never completes is dropped, and a read that
/// completed but whose index the replica never reached is dropped too.
#[test]
fn a_read_expires_by_ttl() {
    let mut reads: QuorumReads<NodeId> = QuorumReads::new();
    let _ = reads.open(0, Some(0), basis(grid(), ballot(0)), 5, None);
    let _ = reads.open(1, Some(1), basis(grid(), ballot(0)), 9, None);
    reads.expire(15, 8);
    assert_eq!(reads.pending().len(), 1);
    assert_eq!(reads.pending()[0].ctx(), 1);
    reads.expire(30, 8);
    assert!(reads.is_empty());
}

#[test]
#[should_panic(expected = "a quorum read token is opened at most once")]
fn a_token_is_opened_at_most_once() {
    let mut reads: QuorumReads<NodeId> = QuorumReads::new();
    let _ = reads.open(0, Some(0), basis(grid(), ballot(0)), 0, None);
    let _ = reads.open(0, Some(0), basis(grid(), ballot(0)), 0, None);
}

/// A named row: any row of the grid is honoured, a row past the last
/// or one under a majority falls back to `ctx % rows` / the whole
/// membership (`read_row`), and the tally refuses a row its
/// configuration does not have.
#[test]
fn a_named_read_row_is_honoured_or_falls_back() {
    let grid = grid();
    assert_eq!(
        grid.read_row(4, Some(1)),
        Some(1),
        "a named row is honoured"
    );
    assert_eq!(
        grid.read_row(4, None),
        Some(0),
        "the default row is ctx % rows"
    );
    assert_eq!(
        grid.read_row(5, Some(2)),
        Some(1),
        "a row past the last falls back"
    );
    let majority = AcceptorConfig::new((1..=3).map(NodeId).collect(), QuorumSystem::Majority);
    assert_eq!(
        majority.read_row(4, Some(0)),
        None,
        "a majority names no row"
    );
    let mut reads: QuorumReads<NodeId> = QuorumReads::new();
    // Row 1 of the 2x3 grid is {4, 5, 6}, although ctx 4 defaults to row 0.
    let addressees = reads.open(4, Some(1), basis(grid, ballot(0)), 0, None);
    assert_eq!(addressees, vec![NodeId(4), NodeId(5), NodeId(6)]);
}

#[test]
#[should_panic(expected = "a quorum read is opened against a row of its configuration")]
fn a_row_the_configuration_lacks_is_refused() {
    let mut reads: QuorumReads<NodeId> = QuorumReads::new();
    let _ = reads.open(0, Some(2), basis(grid(), ballot(0)), 0, None);
}

/// The basis's fence (#260): a row whose watermarks all sit below the
/// fence still waits for the replica to cover the fence — what an
/// earlier configuration chose is not the row's to report.
#[test]
fn a_read_waits_on_its_basis_fence() {
    let mut reads: QuorumReads<NodeId> = QuorumReads::new();
    let majority = AcceptorConfig::new((0..3).map(NodeId).collect(), QuorumSystem::Majority);
    let fenced = ReadBasis {
        config: majority,
        since: ballot(7),
        fence: Some(Slot(3)),
    };
    let addressees = reads.open(0, None, fenced, 0, Some((NodeId(0), Some(Slot(2)))));
    assert_eq!(addressees, vec![NodeId(1), NodeId(2)]);
    assert_eq!(
        reads.fold(0, NodeId(1), Some(Slot(1)), Some(ballot(7))),
        PreReadFold::Counted
    );
    assert!(
        reads.serve(|index| index <= Some(Slot(2))).is_empty(),
        "a prefix at the row's maximum is below the fence"
    );
    assert_eq!(
        reads.serve(|index| index <= Some(Slot(3))),
        vec![(0, Some(Slot(3)))],
        "served at the fence once the replica covers it"
    );
}
