//! The **view** of a journal controller: the last feedback of each
//! deployment of one journal, and its age.

use std::collections::BTreeMap;

use crate::contract::feedback::{MatchmakerFeedback, NodeFeedback, ProxyFeedback};
use crate::membership::{MatchmakerId, ProxyId};
use crate::types::NodeId;

/// A feedback and its age, in ticks of the holder of the view.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Aged<F> {
    /// The last feedback.
    pub feedback: F,
    /// The ticks of the holder since it arrived.
    pub age: u64,
}

/// The last feedback of each node, proxy leader and matchmaker of one
/// journal, each with its age. A new feedback replaces the last one of its
/// sender at age 0; [`View::tick`] ages every feedback by one. The view
/// holds what it was given and judges nothing: [`super::evaluate`] does.
///
/// The view does not order the feedback of one sender: a feedback that
/// arrives late replaces a newer one. So the caller delivers the feedback
/// of each sender in the order the sender gave it (one stream for each
/// sender). The counters of a feedback cannot order it: a node that
/// restarts counts its ticks from zero again. A view that is wrong this
/// way costs a wasted order at most, never safety: each order is a call
/// that the node judges again (`may_retire`, and `reconfigure` at the
/// leader's own ballot).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct View {
    nodes: BTreeMap<NodeId, Aged<NodeFeedback>>,
    proxies: BTreeMap<ProxyId, Aged<ProxyFeedback>>,
    matchmakers: BTreeMap<MatchmakerId, Aged<MatchmakerFeedback>>,
}

impl View {
    /// An empty view: a controller that starts or restarts knows nothing.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn new() -> Self {
        let view = Self::default();
        assert!(view.nodes.is_empty(), "a new view knows no node");
        assert!(
            view.matchmakers.is_empty(),
            "a new view knows no matchmaker"
        );
        view
    }

    /// Keep `feedback` as the last of its node, at age 0.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    pub fn observe_node(&mut self, feedback: NodeFeedback) {
        let id = feedback.id;
        let before = self.nodes.len();
        self.nodes.insert(id, Aged { feedback, age: 0 });
        assert!(
            self.nodes.len() <= before + 1,
            "a feedback replaces the last of its sender"
        );
        assert!(
            self.nodes.get(&id).is_some_and(|aged| aged.age == 0),
            "a new feedback is fresh"
        );
    }

    /// Keep `feedback` as the last of its proxy leader, at age 0.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    pub fn observe_proxy(&mut self, feedback: ProxyFeedback) {
        let id = feedback.id;
        let before = self.proxies.len();
        self.proxies.insert(id, Aged { feedback, age: 0 });
        assert!(
            self.proxies.len() <= before + 1,
            "a feedback replaces the last of its sender"
        );
        assert!(
            self.proxies.get(&id).is_some_and(|aged| aged.age == 0),
            "a new feedback is fresh"
        );
    }

    /// Keep `feedback` as the last of its matchmaker, at age 0.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    pub fn observe_matchmaker(&mut self, feedback: MatchmakerFeedback) {
        let id = feedback.id;
        let before = self.matchmakers.len();
        self.matchmakers.insert(id, Aged { feedback, age: 0 });
        assert!(
            self.matchmakers.len() <= before + 1,
            "a feedback replaces the last of its sender"
        );
        assert!(
            self.matchmakers.get(&id).is_some_and(|aged| aged.age == 0),
            "a new feedback is fresh"
        );
    }

    /// One tick of the holder: every feedback is one tick older.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    pub fn tick(&mut self) {
        let known = (self.nodes.len(), self.proxies.len(), self.matchmakers.len());
        for aged in self.nodes.values_mut() {
            aged.age = aged.age.saturating_add(1);
        }
        for aged in self.proxies.values_mut() {
            aged.age = aged.age.saturating_add(1);
        }
        for aged in self.matchmakers.values_mut() {
            aged.age = aged.age.saturating_add(1);
        }
        assert!(
            known == (self.nodes.len(), self.proxies.len(), self.matchmakers.len()),
            "a tick forgets no feedback"
        );
        assert!(
            self.nodes.values().all(|aged| aged.age > 0),
            "a ticked feedback is no longer new"
        );
    }

    /// The last feedback of each node.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn nodes(&self) -> &BTreeMap<NodeId, Aged<NodeFeedback>> {
        assert!(
            self.nodes.iter().all(|(id, aged)| *id == aged.feedback.id),
            "each feedback is kept under its own sender"
        );
        &self.nodes
    }

    /// The last feedback of each proxy leader.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn proxies(&self) -> &BTreeMap<ProxyId, Aged<ProxyFeedback>> {
        assert!(
            self.proxies
                .iter()
                .all(|(id, aged)| *id == aged.feedback.id),
            "each feedback is kept under its own sender"
        );
        &self.proxies
    }

    /// The last feedback of each matchmaker.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn matchmakers(&self) -> &BTreeMap<MatchmakerId, Aged<MatchmakerFeedback>> {
        assert!(
            self.matchmakers
                .iter()
                .all(|(id, aged)| *id == aged.feedback.id),
            "each feedback is kept under its own sender"
        );
        &self.matchmakers
    }
}
