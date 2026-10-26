//! The cell coordinator's failure detector against the machines' real
//! lifecycle (#654 (live machine marked down)). The board notes when each
//! machine's process last started `run_machine` and when it stopped, and
//! when the slow-machine scenario slowed a machine. From these, a machine
//! is **live since** an instant when its process runs since then, it holds
//! the id it minted, and it was never renamed (a renamed machine is
//! reachable only once the registry holds its new name).
//!
//! The oracle: in the recovery tail, the coordinator writes no
//! `MachineDown` for a machine live since before it last asked it. A
//! `MachineDown` follows an `Identify` that got no answer: when the whole
//! span of that ask lies in the quiet tail, the machine listened throughout
//! and no machine was slow, nothing excuses the silence. The final fleet
//! check judges the end state too (`chain_workload::fleet::live`).
//!
//! A `MachineDown` is judged [`NOTED`] after it was sent, never as it is
//! sent: the board notes a kill when the killed task drops, a scheduler
//! step after the kill closed the machine's connections, so a write sent
//! at the kill's own instant reads the machine live. Witness replay-recipe
//! 8586656184448954778 719326:5636923068530185909 on f729de1 (#712 (renew
//! between pages)): an operator's resumed `init` killed its target founder
//! 89 s into the tail (`FleetOps::killer`, its own BUGGIFY location, live
//! in the tail), the coordinator's `Identify` in flight was refused at
//! once, 2.8 s after the machine last answered, past the seed's
//! `machine_down_after`, and the board noted the end 15 µs after the
//! `MachineDown` was judged.

use std::time::Duration;

use moonpool_sim::{StateHandle, assert_always};
use paros::model::address::Address;
use paros::model::registry::SystemCommand;

use super::{lock, machine_board};

/// The slack past the coordinator's longest `Identify` timeout: a machine
/// that started its process listens within a late boot (2.5 s) and its
/// first record read.
const SLACK: Duration = Duration::from_secs(10);

/// Machine `addr`'s process started `run_machine` at `at`.
pub(crate) fn note_started(state: &StateHandle, addr: &Address, at: Duration) {
    lock(&machine_board(state)).started.insert(addr.clone(), at);
}

/// Machine `addr`'s process stopped at `at`: killed, or its loop ended.
pub(crate) fn note_ended(state: &StateHandle, addr: &Address, at: Duration) {
    lock(&machine_board(state)).ended.insert(addr.clone(), at);
}

/// The slow-machine scenario slows a machine until at most `until`.
pub(crate) fn note_slowed(state: &StateHandle, until: Duration) {
    let board = machine_board(state);
    let mut board = lock(&board);
    board.slowed = Some(board.slowed.map_or(until, |at| at.max(until)));
}

/// Since when machine `node` (a minted id) runs, when its process runs now,
/// it holds that id and it was never renamed.
pub(crate) fn live_since(state: &StateHandle, node: u64) -> Option<Duration> {
    if super::renamed_ids(state).contains(&node) {
        return None;
    }
    let board = machine_board(state);
    let board = lock(&board);
    let addr = board.nodes.iter().find(|(_, id)| **id == node)?.0;
    let started = *board.started.get(addr)?;
    let ended = board.ended.get(addr).is_some_and(|at| *at >= started);
    (!ended).then_some(started)
}

/// Whether a slow machine may have slowed anything after `from`.
pub(crate) fn slowed_after(state: &StateHandle, from: Duration) -> bool {
    lock(&machine_board(state))
        .slowed
        .is_some_and(|until| until >= from)
}

/// How long after a `MachineDown` was sent it is judged: far above the
/// scheduler step between a kill and the board's note of it, far below
/// the tail.
const NOTED: Duration = Duration::from_secs(1);

/// A `MachineDown` sent in the tail, judged once the board's lifecycle
/// facts are past it ([`judge_pending`]).
#[derive(Clone, Copy, Debug)]
pub(crate) struct SentDown {
    /// The machine marked down, by minted id.
    node: u64,
    /// When the `Identify` that went unanswered began, at the latest.
    asked: Duration,
    /// When the write was sent.
    sent: Duration,
}

/// A client sent `records` to `journal` at `now`: every `MachineDown`
/// among them is noted for judgement, and every one noted [`NOTED`] ago
/// is judged (see the module doc).
pub(crate) fn judge_sent(
    state: &StateHandle,
    journal: paros::core::JournalIdentifier,
    records: &[Vec<u8>],
    now: Duration,
) {
    // Only a cell control journal holds the registry's commands.
    if !lock(&machine_board(state))
        .governs
        .values()
        .any(|cell| *cell == journal)
    {
        return;
    }
    for record in records {
        let Ok(SystemCommand::MachineDown { id, .. }) = SystemCommand::decode(record) else {
            continue;
        };
        let Some(end) = moonpool_sim::chaos_ended_at() else {
            continue;
        };
        // The span of the `Identify` that went unanswered, and the boot
        // before it.
        let asked = now.saturating_sub(crate::shape::longest_internal_timeout(state) + SLACK);
        if asked < end || slowed_after(state, asked) {
            continue;
        }
        lock(&machine_board(state)).downs.push(SentDown {
            node: id.0,
            asked,
            sent: now,
        });
    }
    judge_pending(state, now);
}

/// Judge every `MachineDown` sent [`NOTED`] before `now` (see the module
/// doc): the machine it names is not live since before the ask. A machine
/// whose process ended since is not live; one that started again is live
/// since that start.
pub(crate) fn judge_pending(state: &StateHandle, now: Duration) {
    let due: Vec<SentDown> = {
        let board = machine_board(state);
        let mut board = lock(&board);
        let (due, pending): (Vec<SentDown>, Vec<SentDown>) = board
            .downs
            .drain(..)
            .partition(|down| now.saturating_sub(down.sent) >= NOTED);
        board.downs = pending;
        due
    };
    for down in due {
        let Some(since) = live_since(state, down.node) else {
            continue;
        };
        assert_always!(
            since > down.asked,
            "registry: the cell coordinator never marks a live machine down in the tail",
            { "node" => down.node }
        );
    }
}
