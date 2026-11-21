//! The compartmentalized deployment, driven by hand: the world of the
//! exercises of lesson 6 (`crate::paxos::lab`).
//!
//! The same [`Run`] as the page's stories (three [`ColocatedNode`]s, two
//! [`ProxyLeader`]s, two [`ReplicaNode`]s), but the reader moves each
//! message: deliver it or lose it. A few more moves (a client writes, a
//! beat, a proxy leader crashes) are [`Verb`]s. The desk holds no protocol
//! rule: every answer comes from the roles of `paros-consensus`.
//!
//! [`ColocatedNode`]: paros_consensus::ColocatedNode
//! [`ProxyLeader`]: paros_consensus::ProxyLeader
//! [`ReplicaNode`]: paros_consensus::ReplicaNode

use paros_consensus::membership::QuorumSystem;

use super::{Layout, REPLICAS, Run, TAKE_BACK_AFTER, Who, lanes, parties, says, waiting};
use crate::paxos::lab::WireMessage;
use crate::paxos::trace::{Lane, message_label};
use crate::paxos::view::message_view;

/// A move of an exercise other than deliver and drop.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Verb {
    /// A client writes this value to node 0.
    Write(&'static str),
    /// Node 0 beats: it sends its open slots again, and takes back a slot
    /// whose proxy leader did not answer.
    LeaderBeats,
    /// Proxy leader `id` beats: it sends the Accepts of its open slots again.
    ProxyBeats(u64),
    /// Proxy leader `id` crashes. It holds nothing on disk.
    CrashProxy(u64),
}

/// The deployment of one exercise.
pub struct Desk {
    run: Run,
    /// Whether each replica held a chosen slot above its chosen prefix at
    /// some point: it learned a slot out of order and waited.
    waited: Vec<bool>,
    /// What the last move did, one sentence per step it took.
    said: Vec<String>,
}

impl Default for Desk {
    fn default() -> Self {
        Self::new()
    }
}

impl Desk {
    /// Node 0 leads the three acceptors; both proxy leaders and both
    /// replicas are up, and nothing is written.
    #[must_use]
    pub fn new() -> Self {
        Self {
            run: Run::new(Layout::Compartmentalized),
            waited: vec![false; REPLICAS],
            said: Vec::new(),
        }
    }

    /// The parties the exercises show: every one but the batcher, which no
    /// exercise uses.
    fn shown() -> impl Iterator<Item = Who> {
        parties().into_iter().filter(|who| *who != Who::Batcher)
    }

    /// The lanes, in order.
    #[must_use]
    pub fn lanes() -> Vec<Lane> {
        lanes()
            .into_iter()
            .filter(|lane| lane.id != Who::Batcher.lane())
            .collect()
    }

    /// A few lines of state per lane.
    #[must_use]
    pub fn state(&self) -> Vec<Vec<String>> {
        parties()
            .into_iter()
            .zip(self.run.state())
            .filter(|(who, _)| *who != Who::Batcher)
            .map(|(_, lines)| lines)
            .collect()
    }

    /// Whether each lane is up.
    #[must_use]
    pub fn alive(&self) -> Vec<bool> {
        Self::shown().map(|who| self.run.up(who)).collect()
    }

    /// What the acceptors hold chosen: `slot 0 = alpha`.
    #[must_use]
    pub fn chosen(&self) -> Vec<String> {
        self.run.chosen()
    }

    /// What the last move did.
    #[must_use]
    pub fn said(&self) -> &[String] {
        &self.said
    }

    /// The messages on the wire, oldest first.
    #[must_use]
    pub fn wire(&self) -> Vec<WireMessage> {
        self.run
            .wire
            .iter()
            .map(|wired| {
                let view = message_view(
                    wired.id,
                    wired.from.number(),
                    wired.to.number(),
                    0,
                    &wired.message,
                    QuorumSystem::Majority,
                );
                WireMessage {
                    id: wired.id,
                    kind: view.kind.clone(),
                    from: wired.from.lane(),
                    to: wired.to.lane(),
                    phase: view.phase.clone(),
                    label: message_label(&view, true),
                    summary: says(wired.from, wired.to, &wired.message),
                    slot: view.slot,
                }
            })
            .collect()
    }

    /// The highest slot of replica `rank`'s chosen prefix.
    #[must_use]
    pub fn learned(&self, rank: usize) -> Option<u64> {
        self.run
            .replicas
            .get(rank)
            .and_then(|replica| replica.replica().chosen_index())
            .map(|slot| slot.0)
    }

    /// Whether replica `rank` learned a slot out of order and waited.
    #[must_use]
    pub fn waited(&self, rank: usize) -> bool {
        self.waited.get(rank).copied().unwrap_or(false)
    }

    /// Whether node 0 still leads.
    #[must_use]
    pub fn leads(&self) -> bool {
        self.run.nodes[0].is_leader()
    }

    /// Deliver the message `id`. A message to a party that is down is lost.
    ///
    /// # Errors
    ///
    /// When no message on the wire has that id.
    pub fn deliver(&mut self, id: u64) -> Result<(), String> {
        self.carry(id, false)
    }

    /// Lose the message `id`.
    ///
    /// # Errors
    ///
    /// When no message on the wire has that id.
    pub fn lose(&mut self, id: u64) -> Result<(), String> {
        self.carry(id, true)
    }

    fn carry(&mut self, id: u64, lose: bool) -> Result<(), String> {
        let position = self
            .run
            .wire
            .iter()
            .position(|wired| wired.id == id)
            .ok_or_else(|| format!("No message {id} is on the wire."))?;
        let wired = self
            .run
            .wire
            .remove(position)
            .expect("the position is on the wire");
        let before = self.run.steps.len();
        self.run.carry(wired, lose);
        self.after(before);
        Ok(())
    }

    /// Play one verb.
    ///
    /// # Errors
    ///
    /// When the verb is not possible now: why, in one sentence.
    pub fn act(&mut self, verb: &Verb) -> Result<(), String> {
        let before = self.run.steps.len();
        match verb {
            Verb::Write(value) => {
                if !self.leads() {
                    return Err("Node 0 does not lead now, so it takes no write.".to_string());
                }
                self.run
                    .try_client_writes(value)
                    .map_err(|result| format!("Node 0 does not take the write: {result:?}."))?;
            }
            Verb::LeaderBeats => {
                if !self.leads() {
                    return Err("Node 0 does not lead now, so it does not beat.".to_string());
                }
                let open: Vec<String> = self.run.nodes[0]
                    .delegated_rounds()
                    .iter()
                    .map(|(slot, _)| slot.0.to_string())
                    .collect();
                let taken = self.run.leader_beats();
                let says = if let Some((slot, proxy)) = taken.first() {
                    format!(
                        "Node 0 beats. Proxy leader {} did not answer {TAKE_BACK_AFTER} times, \
                         so node 0 takes slot {} back and sends the Accepts itself.",
                        proxy.0, slot.0
                    )
                } else if open.is_empty() {
                    "Node 0 beats. It holds no open slot.".to_string()
                } else {
                    format!(
                        "Node 0 beats. Slot {} is not chosen yet, so node 0 sends it to its \
                         proxy leader again.",
                        open.join(" and ")
                    )
                };
                self.run.event("node 0 beats", says);
            }
            Verb::ProxyBeats(id) => {
                let index = usize::try_from(*id).expect("a small id");
                if self.run.proxies.get(index).is_none_or(Option::is_none) {
                    return Err(format!("Proxy leader {id} is down."));
                }
                self.run.proxy_beats(*id);
                self.run.event(
                    &format!("proxy leader {id} beats"),
                    format!(
                        "Proxy leader {id} beats. It sends the Accepts of each slot that it \
                         holds and that is not chosen yet."
                    ),
                );
            }
            Verb::CrashProxy(id) => {
                let index = usize::try_from(*id).expect("a small id");
                let Some(slot) = self.run.proxies.get_mut(index) else {
                    return Err(format!("There is no proxy leader {id}."));
                };
                if slot.take().is_none() {
                    return Err(format!("Proxy leader {id} is already down."));
                }
                self.run.event(
                    &format!("proxy leader {id} crashes"),
                    format!(
                        "Proxy leader {id} crashes. It holds nothing on disk, so nothing is \
                         lost with it."
                    ),
                );
            }
        }
        self.after(before);
        Ok(())
    }

    /// Keep what the steps since `before` said, and note each replica that
    /// waits.
    fn after(&mut self, before: usize) {
        self.said = self.run.steps[before..]
            .iter()
            .map(|step| step.says.clone())
            .collect();
        for (rank, replica) in self.run.replicas.iter().enumerate() {
            if !waiting(replica).is_empty() {
                self.waited[rank] = true;
            }
        }
    }
}
