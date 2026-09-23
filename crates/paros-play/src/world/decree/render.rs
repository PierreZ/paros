//! The single-decree world's view.
//!
//! Two node shapes share one [`NodeView`]: an acceptor renders its promise and
//! its one record, a proposer renders its ballot and the value its Phase 2 is
//! carrying. Everything the log world's `NodeView` has and this world does not
//! — a chosen index, a floor, an election clock — is simply `None`, so one
//! renderer serves both acts.

use super::{Attempt, DECREE, DecreeAcceptor, DecreeProposer, DecreeWorld};
use crate::view::{
    AttemptView, ChosenView, ClientView, GridCellView, MatchmakerView, NodeFlavour, NodeView,
    ReachView, SlotView, WorldFlavour, WorldView, control_kind, quorum_view, show_ballot,
    value_text,
};
use crate::world::quorum_name;
use paros_core::Ballot;

impl DecreeWorld {
    /// Render the whole world.
    #[must_use]
    pub fn view(&self) -> WorldView {
        let mut nodes: Vec<NodeView> = self
            .acceptors
            .iter()
            .map(|acceptor| self.acceptor_view(acceptor))
            .collect();
        nodes.extend(self.proposers.iter().map(|p| self.proposer_view(p)));
        WorldView {
            flavour: WorldFlavour::Decree,
            clock: 0,
            nodes,
            wire: self
                .wire
                .iter()
                .map(|entry| entry.view(self.config.quorum_system()))
                .collect(),
            clients: Vec::<ClientView>::new(),
            matchmakers: Vec::<MatchmakerView>::new(),
            chosen: self.chosen.as_ref().map(|(ballot, command)| ChosenView {
                value: value_text(command),
                control: control_kind(command),
                ballot: show_ballot(*ballot),
            }),
            reach: Some(ReachView {
                one: self.phase1_reach.iter().map(|id| id.0).collect(),
                two: self.phase2_reach.iter().map(|id| id.0).collect(),
            }),
        }
    }

    fn acceptor_view(&self, acceptor: &DecreeAcceptor) -> NodeView {
        let chosen_here = self
            .chosen
            .as_ref()
            .is_some_and(|(at, _)| acceptor.role.record(DECREE).is_some_and(|(b, _)| *b >= *at));
        NodeView {
            promised: Some(show_ballot(acceptor.role.promised())),
            accepted: acceptor
                .role
                .record(DECREE)
                .map(|(ballot, command)| {
                    vec![SlotView::accepted(
                        DECREE,
                        *ballot,
                        command,
                        chosen_here,
                        false,
                    )]
                })
                .unwrap_or_default(),
            floor: Some(acceptor.role.first_slot().0),
            ..self.bare_node(acceptor.id.0, NodeFlavour::Acceptor)
        }
    }

    fn proposer_view(&self, proposer: &DecreeProposer) -> NodeView {
        let attempt = match proposer.attempt {
            Attempt::Idle => AttemptView::Idle,
            Attempt::Phase1 => AttemptView::Phase1,
            Attempt::Phase2 => AttemptView::Phase2,
            Attempt::Preempted => AttemptView::Preempted,
            Attempt::Won => AttemptView::Won,
        };
        let proposing = proposer
            .proposing
            .as_ref()
            .or(proposer.value.as_ref())
            .map(|command| {
                SlotView::accepted(
                    DECREE,
                    proposer.ballot.unwrap_or_else(Ballot::zero),
                    command,
                    proposer.attempt == Attempt::Won,
                    false,
                )
            });
        NodeView {
            attempt: Some(attempt),
            ballot: proposer.ballot.map(show_ballot),
            accepted: proposing.into_iter().collect(),
            open_rounds: proposer.role.rounds().keys().map(|s| s.0).collect(),
            pending_accepts: !proposer.role.rounds().is_empty(),
            ..self.bare_node(proposer.id.0, NodeFlavour::Proposer)
        }
    }

    /// A node of this world with nothing on it: everything the log world's
    /// `NodeView` has and this world does not is `None`.
    fn bare_node(&self, id: u64, flavour: NodeFlavour) -> NodeView {
        NodeView {
            id,
            flavour,
            alive: true,
            // An Act I node holds no log role: `flavour` already says what
            // it is, and it is never anything else.
            role: None,
            attempt: None::<AttemptView>,
            ballot: None,
            leader: None,
            promised: None,
            accepted: Vec::new(),
            chosen_index: None,
            first_unchosen: None,
            next_slot: None,
            chosen_gap: None,
            floor: None,
            election: None,
            open_rounds: Vec::new(),
            pending_accepts: false,
            read_rounds: Vec::new(),
            recovery_remaining: 0,
            acceptors: self.config.members().iter().map(|n| n.0).collect(),
            quorum_system: quorum_name(self.config.quorum_system()),
            quorum: quorum_view(self.config.quorum_system()),
            // The single-decree world lays out no grid: its levels teach the
            // counting systems, and a grid is a log-world deployment.
            grid_cell: None::<GridCellView>,
            applied: Vec::new(),
            armed_seam: None,
            // The single-decree world runs bare roles: no configuration
            // ballot, no matchmakers, no floor, no retirement.
            acceptors_since: None,
            matchmakers: None,
            matchmaking: None,
            gc: None,
            handover: None,
            retired: false,
            wiped: false,
        }
    }
}
