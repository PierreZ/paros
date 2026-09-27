//! Where a boot's fold starts: the checkpoint rules both stores share.
//!
//! A checkpoint is a bracket — a `Begin`, the whole image as ordinary
//! records, an `End` — appended as one batch, after which the journal may
//! drop the whole segments before it. Three facts decide the start:
//!
//! - **A bracket without its end is the tail of an append a crash cut.** A
//!   bracket is the last thing written until it completes (a failed append
//!   poisons the journal and the driver crashes), so an open bracket only
//!   ever ends the log. It was never relied on — a checkpoint's history is
//!   still before it, a matchmaker install was never acknowledged — and is
//!   cut before anything else is appended. (A matchmaker's `Install ..
//!   End` is a bracket for this rule only: it replaces the state, so it is
//!   folded, never skipped.)
//! - **An intact bracket is the whole state.** The fold starts at the newest
//!   one whose every entry checked out and ignores everything before it.
//! - **A damaged bracket is a copy, while its history is still on disk.**
//!   Its content repeats records that precede it, so as long as the log
//!   still begins at [`GENESIS`](super::GENESIS) — or at an older intact
//!   bracket — the fold skips it and reads the originals. That is also what
//!   keeps a crash that tore a checkpoint *before* its sync from looking
//!   like corruption. Only when every bracket is damaged and the prefix is
//!   gone does the fold have to trust one: the **oldest**, which a later
//!   bracket's prefix drop proves was synced — so what is damaged in it is
//!   rot, and is judged record by record, strictly.

use std::ops::Range;

use super::frame::{Kind, Scanned};

/// Where to start and what to skip.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Plan {
    /// Cut the log here first: the open bracket a crash left at the tail.
    pub cut_at: Option<usize>,
    /// Fold from this position.
    pub start: usize,
    /// The bracket the fold must trust although it is damaged: damage in
    /// its header or sealed ledger is a crash verdict.
    pub strict: Option<Range<usize>>,
    /// Brackets after `start` the fold skips: copies of what it already
    /// read.
    pub skip: Vec<Range<usize>>,
    /// The fold starts nowhere: the prefix is gone and no bracket survives.
    pub lost: bool,
}

/// Plan a fold over `scanned` (the live log in order). `at_genesis` says
/// the log still begins at the first index it ever had.
pub(crate) fn plan<R>(scanned: &[Scanned<R>], at_genesis: bool) -> Plan {
    let mut brackets: Vec<Range<usize>> = Vec::new();
    let mut open: Option<(usize, Kind)> = None;
    for (at, entry) in scanned.iter().enumerate() {
        match entry.kind {
            Some(kind @ (Kind::Begin | Kind::Install)) => open = Some((at, kind)),
            Some(Kind::End) => {
                if let Some((begin, Kind::Begin)) = open.take() {
                    brackets.push(begin..at + 1);
                }
            }
            _ => {}
        }
    }
    let open = open.map(|(at, _)| at);
    let cut_at = open;
    let intact = |range: &Range<usize>| scanned[range.clone()].iter().all(|e| e.record.is_some());
    // `folded`: the bracket the fold starts at, which is read, not skipped.
    let (start, strict, lost, folded) =
        if let Some(newest) = brackets.iter().rev().find(|b| intact(b)) {
            (newest.start, None, false, Some(newest.start))
        } else if at_genesis {
            (0, None, false, None)
        } else if let Some(oldest) = brackets.first() {
            (
                oldest.start,
                Some(oldest.clone()),
                false,
                Some(oldest.start),
            )
        } else {
            (0, None, true, None)
        };
    let skip = brackets
        .iter()
        .filter(|bracket| folded.is_none_or(|begin| bracket.start > begin))
        .cloned()
        .collect();
    Plan {
        cut_at,
        start,
        strict,
        skip,
        lost,
    }
}

#[cfg(test)]
mod tests {
    use moonpool_journal::EntryId;

    use super::*;
    use crate::journal::frame::epoch;

    /// A scanned entry of `kind`, intact or damaged.
    fn entry(index: u64, kind: Kind, intact: bool) -> Scanned<()> {
        Scanned {
            id: EntryId {
                index,
                epoch: epoch(kind),
                tag: [0; moonpool_journal::TAG_SIZE],
            },
            kind: Some(kind),
            record: intact.then_some(()),
        }
    }

    fn log(shape: &[(Kind, bool)]) -> Vec<Scanned<()>> {
        shape
            .iter()
            .enumerate()
            .map(|(at, (kind, intact))| entry(at as u64 + 1, *kind, *intact))
            .collect()
    }

    use Kind::{Accepted as A, Begin as B, End as E};

    #[test]
    fn the_newest_intact_checkpoint_is_the_start() {
        let p = plan(
            &log(&[
                (A, true),
                (B, true),
                (A, true),
                (E, true),
                (A, true),
                (B, true),
                (A, false),
                (E, true),
                (A, true),
            ]),
            true,
        );
        assert_eq!((p.start, p.strict, p.lost), (1, None, false));
        assert_eq!(p.skip, vec![5..8], "the damaged newer copy is skipped");
        assert_eq!(p.cut_at, None);
    }

    #[test]
    fn a_damaged_checkpoint_is_skipped_while_the_history_is_on_disk() {
        let p = plan(
            &log(&[(A, true), (B, false), (A, true), (E, true), (A, true)]),
            true,
        );
        assert_eq!((p.start, p.strict, p.lost), (0, None, false));
        assert_eq!(p.skip, vec![1..4]);
    }

    #[test]
    fn without_the_history_the_oldest_checkpoint_is_trusted_strictly() {
        let p = plan(
            &log(&[
                (B, false),
                (A, true),
                (E, true),
                (B, true),
                (A, false),
                (E, true),
            ]),
            false,
        );
        assert_eq!((p.start, p.strict), (0, Some(0..3)));
        assert_eq!(p.skip, vec![3..6]);
        let none = plan(&log(&[(A, true)]), false);
        assert!(none.lost, "no history and no checkpoint is nothing to fold");
    }

    #[test]
    fn an_install_is_folded_and_cut_only_when_open() {
        use Kind::Install as I;
        let p = plan(&log(&[(A, true), (I, false), (A, true), (E, true)]), true);
        assert_eq!((p.start, p.cut_at, p.skip.len()), (0, None, 0));
        let open = plan(&log(&[(A, true), (I, true), (A, true)]), true);
        assert_eq!(open.cut_at, Some(1));
    }

    #[test]
    fn an_open_checkpoint_at_the_tail_is_cut() {
        let p = plan(&log(&[(A, true), (B, true), (A, true)]), true);
        assert_eq!(p.cut_at, Some(1));
        assert_eq!((p.start, p.skip.len()), (0, 0));
    }
}
