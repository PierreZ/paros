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

use std::time::Duration;

use moonpool_sim::{StateHandle, assert_always};
use paros::Address;
use paros::system::SystemCommand;

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

/// A client sent `records` to `journal` at `now`: every
/// `MachineDown` among them is judged (see the module doc).
pub(crate) fn judge_sent(
    state: &StateHandle,
    journal: paros::JournalIdentifier,
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
        let Some(since) = live_since(state, id.0) else {
            continue;
        };
        assert_always!(
            since > asked,
            "registry: the cell coordinator never marks a live machine down in the tail",
            { "node" => id.0 }
        );
    }
}
