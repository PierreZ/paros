use super::*;
use crate::membership::QuorumSystem;
use crate::types::NodeId;

/// The watermark is the oracle that a raised floor was reported by a
/// matchmaker: a raised watermark with no page folded is a state that no
/// fold reaches, and the check must name it.
#[test]
#[should_panic(expected = "a raised watermark was reported")]
fn a_raised_watermark_that_no_matchmaker_reported_breaks_the_check() {
    let config = AcceptorConfig::new(
        vec![NodeId(0), NodeId(1), NodeId(2)],
        QuorumSystem::Majority,
    );
    let ballot = Ballot {
        round: 5,
        node: NodeId(0),
    };
    let mut m = Matchmaking::new(ballot, config, RegistrationKind::Belief);
    m.watermark = Ballot {
        round: 1,
        node: NodeId(1),
    };
    let _ = m.watermark();
}
