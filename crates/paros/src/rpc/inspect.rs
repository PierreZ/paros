//! What an `Inspect` asks for (#243), spelled once for every server that
//! answers one (the node loop, the replica tier) and every client that
//! sends one.
//!
//! No identifier has a default (`docs/architecture.md` §3.8): an `Inspect` names
//! the journal it asks about, or asks for the node alone — its id, its cell
//! and the control journals' identifiers, which is how a client handed only an
//! address learns the identifiers before it can name any journal. An unset identifier
//! that does not ask for the node alone is refused, never read as "the
//! node's first journal".

use paros_core::{JournalId, JournalIdentifier, TenantId};

use super::{InspectReply, InspectRequest};

/// What a well-formed `Inspect` asks for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InspectTarget {
    /// The node alone: its id, its cell and the control journals' identifiers.
    Node,
    /// The node and one journal it serves.
    Journal(JournalIdentifier),
}

/// Why an `Inspect` was not answered for a journal. The node's own facts
/// are answered either way.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InspectRefusal {
    /// The request named no journal (a half of its identifier is `0`) and did not
    /// ask for the node alone.
    Unset,
    /// The request asked for the node alone and named a journal too.
    Malformed,
    /// The journal is not live on this node.
    UnknownJournal,
}

impl InspectRefusal {
    /// The refusal's wire label (`InspectReply.refusal`).
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::Unset => "unset",
            Self::Malformed => "malformed",
            Self::UnknownJournal => "unknown_journal",
        }
    }
}

impl InspectRequest {
    /// A node-only `Inspect`: the node's facts and no journal's.
    #[must_use]
    pub fn node_only() -> Self {
        Self {
            journal: 0,
            tenant: 0,
            node_only: true,
        }
    }

    /// What this request asks for, or why it is refused. Wire input: a bad
    /// request is a refusal, never a panic.
    ///
    /// # Errors
    ///
    /// [`InspectRefusal::Malformed`] for a node-only request that names a
    /// identifier half, [`InspectRefusal::Unset`] for a journal request whose
    /// identifier has an unset half.
    pub fn target(&self) -> Result<InspectTarget, InspectRefusal> {
        let journal = JournalIdentifier::new(TenantId(self.tenant), JournalId(self.journal));
        match (self.node_only, self.tenant != 0 || self.journal != 0) {
            (true, false) => Ok(InspectTarget::Node),
            (true, true) => Err(InspectRefusal::Malformed),
            (false, _) if journal.is_set() => Ok(InspectTarget::Journal(journal)),
            (false, _) => Err(InspectRefusal::Unset),
        }
    }
}

impl InspectReply {
    /// `self` with every journal field cleared and `refusal` set, keeping
    /// the node's own facts: the answer to a refused `Inspect`.
    #[must_use]
    pub fn refused(self, refusal: InspectRefusal) -> Self {
        Self {
            refusal: refusal.label().to_string(),
            ..self.node_facts()
        }
    }

    /// The node's own facts of this reply — its id, its cell and the control
    /// journals' identifiers — and nothing about any journal: the answer to a
    /// node-only `Inspect`.
    #[must_use]
    pub fn node_facts(&self) -> Self {
        Self {
            node: self.node,
            cell_id: self.cell_id,
            control_tenant: self.control_tenant,
            control_journal: self.control_journal,
            fleet_tenant: self.fleet_tenant,
            fleet_journal: self.fleet_journal,
            ..Self::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(tenant: u64, journal: u64, node_only: bool) -> InspectRequest {
        InspectRequest {
            journal,
            tenant,
            node_only,
        }
    }

    #[test]
    fn an_inspect_names_its_journal_or_asks_for_the_node_alone() {
        assert_eq!(request(0, 0, true).target(), Ok(InspectTarget::Node));
        assert_eq!(
            request(3, 4, false).target(),
            Ok(InspectTarget::Journal(JournalIdentifier::new(
                TenantId(3),
                JournalId(4)
            )))
        );
        // No identifier has a default: unset is refused, never "the first journal".
        assert_eq!(request(0, 0, false).target(), Err(InspectRefusal::Unset));
        assert_eq!(request(3, 0, false).target(), Err(InspectRefusal::Unset));
        assert_eq!(request(0, 4, false).target(), Err(InspectRefusal::Unset));
        assert_eq!(request(3, 4, true).target(), Err(InspectRefusal::Malformed));
        assert_eq!(request(0, 4, true).target(), Err(InspectRefusal::Malformed));
    }

    #[test]
    fn a_refusal_keeps_the_node_and_drops_the_journal() {
        let full = InspectReply {
            node: 9,
            cell_id: 7,
            control_tenant: 1,
            control_journal: 2,
            fleet_tenant: 3,
            fleet_journal: 4,
            leader: true,
            first_slot: 5,
            members: vec![9],
            ..InspectReply::default()
        };
        let refused = full.clone().refused(InspectRefusal::UnknownJournal);
        assert_eq!(refused.refusal, "unknown_journal");
        assert_eq!(
            (refused.node, refused.cell_id, refused.fleet_journal),
            (9, 7, 4)
        );
        assert!(!refused.leader);
        assert!(refused.members.is_empty());
        assert_eq!(refused.first_slot, 0);
    }
}
