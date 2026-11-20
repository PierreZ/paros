//! The five pages' runs. Each run is a short script on a level's own setup;
//! the messages, the notes and the lane states are the engine's.

use crate::paxos::WorldKind;
use crate::paxos::trace::Trace;
use crate::paxos::view::{MessageView, PartyView, WorldView};

mod matchmaker;
mod multi_paxos;
mod quorums;
mod single_decree;

use matchmaker::matchmaker;
use multi_paxos::multi_paxos;
use quorums::quorums;
use single_decree::single_decree;

/// The script that records one page's trace.
type Record = fn() -> Trace;

/// The pages, in site order, and the script that records each.
const PAGES: &[(&str, Record)] = &[
    ("single-decree", single_decree),
    ("multi-paxos", multi_paxos),
    ("matchmaker", matchmaker),
    ("compartmentalized", compartmentalized),
    ("quorums", quorums),
];

/// Every page's trace, in site order.
#[must_use]
pub fn traces() -> Vec<Trace> {
    PAGES.iter().map(|(_, record)| record()).collect()
}

/// The trace of one page, recorded now; `None` for an unknown page.
#[must_use]
pub fn trace_of(page: &str) -> Option<Trace> {
    PAGES
        .iter()
        .find(|(name, _)| *name == page)
        .map(|(_, record)| record())
}

// ---- shorthands --------------------------------------------------------------

pub(super) fn any(_: &MessageView) -> bool {
    true
}

pub(super) fn kind(name: &'static str) -> impl Fn(&MessageView) -> bool {
    move |message| message.kind == name
}

/// Every message to or from node `node` (not a matchmaker: the two id spaces
/// are different).
pub(super) fn touches(node: u64) -> impl Fn(&MessageView) -> bool {
    move |message| {
        (message.to == node && message.to_party == PartyView::Node)
            || (message.from == node && message.from_party == PartyView::Node)
    }
}

/// Node `id` holds a vote for `value`: an acceptor's vote in the
/// single-decree world, an accepted record at any slot in the log world.
pub(super) fn voted(id: u64, value: &'static str) -> impl Fn(&WorldView) -> bool {
    move |world| {
        world
            .nodes
            .iter()
            .find(|node| node.id == id)
            .is_some_and(|node| node.accepted.iter().any(|vote| vote.value == value))
    }
}

/// The value the single-decree world chose, or that it chose none.
pub(super) fn decree_outcome(world: &WorldKind) -> String {
    if let Some(broken) = world.decree().and_then(|decree| decree.violation()) {
        return broken.to_string();
    }
    match world.view().chosen {
        Some(chosen) => format!("{} is chosen at ballot {}.", chosen.value, chosen.ballot),
        None => "No value is chosen. The run waits for a majority.".to_string(),
    }
}

/// What the live nodes of a log world know is chosen, grouped by node, and
/// where each group's log starts after a truncation.
pub(super) fn log_outcome(world: &WorldKind) -> String {
    let view = world.view();
    let mut groups: Vec<(Vec<u64>, String)> = Vec::new();
    for node in view.nodes.iter().filter(|node| node.alive) {
        let chosen: Vec<String> = node
            .accepted
            .iter()
            .filter(|record| record.chosen)
            .map(|record| format!("slot {} = {}", record.slot, record.value))
            .collect();
        let mut known = chosen.join(", ");
        if let Some(floor) = node.floor.filter(|floor| *floor > 0) {
            known = if known.is_empty() {
                format!("dropped every slot below slot {floor}")
            } else {
                format!("dropped every slot below slot {floor}, then {known}")
            };
        } else if known.is_empty() {
            known = "no chosen slot".to_string();
        }
        match groups.iter_mut().find(|(_, held)| *held == known) {
            Some((ids, _)) => ids.push(node.id),
            None => groups.push((vec![node.id], known)),
        }
    }
    let lines: Vec<String> = groups
        .into_iter()
        .map(|(ids, known)| {
            let ids: Vec<String> = ids.iter().map(u64::to_string).collect();
            let who = if ids.len() == 1 {
                format!("Node {}", ids[0])
            } else {
                format!("Nodes {}", ids.join(", "))
            };
            format!("{who}: {known}.")
        })
        .collect();
    lines.join(" ")
}

// ---- Compartmentalized Paxos -------------------------------------------------

fn compartmentalized() -> Trace {
    super::compart::trace()
}
