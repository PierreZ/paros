//! The scalar codecs a caller reaches through the crate's exports: every
//! quorum system, writer mode, journal view and matchmaker phase crosses the
//! wire unchanged, and a value the wire cannot name is refused.

use paros_core::journal::{JournalView, LeaderUuid, Seq, WriterMode};
use paros_core::matchmaker::{MatchmakerPhase, ReconfigureReply};
use paros_core::membership::{MatchmakerGeneration, MatchmakerId, MatchmakerSet, QuorumSystem};
use prost::Message as ProstMessage;

use crate::{
    WireQuorumSystem, WireReconfigureReply, common, journal_view_from_proto, journal_view_to_proto,
    quorum_system_from_proto, quorum_system_to_proto, reconfigure_reply_from_wire,
    wire_reconfigure_reply, writer_mode_from_proto, writer_mode_to_proto,
};

/// A discriminant no `QuorumSystem` or `WriterMode` names.
const UNKNOWN: i32 = 99;

/// Each quorum system decodes to itself, with the wire fields its doc names.
#[test]
fn every_quorum_system_round_trips_with_its_wire_fields() {
    let cases = [
        (QuorumSystem::Majority, (0, 0, 0, 0)),
        (QuorumSystem::Flexible { q1: 4, q2: 2 }, (4, 2, 0, 0)),
        // A full-column grid leaves the ack count at zero.
        (
            QuorumSystem::Grid {
                rows: 3,
                cols: 2,
                acks: 3,
            },
            (0, 0, 3, 2),
        ),
        // A partitioned grid sends its ack count in `phase2_quorum`.
        (
            QuorumSystem::Grid {
                rows: 3,
                cols: 2,
                acks: 2,
            },
            (0, 2, 3, 2),
        ),
    ];
    for (system, (q1, q2, rows, cols)) in cases {
        let wire = quorum_system_to_proto(system);
        assert_eq!(
            (wire.phase1_quorum, wire.phase2_quorum, wire.rows, wire.cols),
            (q1, q2, rows, cols),
            "{system:?}"
        );
        assert_eq!(quorum_system_from_proto(&wire), Ok(system));
    }
}

/// The three discriminants are distinct on the wire.
#[test]
fn each_quorum_system_has_its_own_discriminant() {
    let kind = |system| quorum_system_to_proto(system).quorum_system;
    let grid = QuorumSystem::Grid {
        rows: 1,
        cols: 1,
        acks: 1,
    };
    assert_eq!(
        kind(QuorumSystem::Majority),
        i32::from(common::QuorumSystem::Majority)
    );
    assert_eq!(
        kind(QuorumSystem::Flexible { q1: 1, q2: 1 }),
        i32::from(common::QuorumSystem::Flexible)
    );
    assert_eq!(kind(grid), i32::from(common::QuorumSystem::Grid));
}

/// A discriminant the wire does not name is refused, never read as a
/// majority.
#[test]
fn an_unknown_quorum_system_is_refused() {
    let wire = WireQuorumSystem {
        quorum_system: UNKNOWN,
        ..WireQuorumSystem::default()
    };
    assert_eq!(
        quorum_system_from_proto(&wire),
        Err("unknown quorum system")
    );
}

/// Both writer modes round-trip, and an unknown value is refused, never read
/// as single-writer.
#[test]
fn writer_modes_round_trip_and_an_unknown_one_is_refused() {
    for mode in [WriterMode::Single, WriterMode::Multi] {
        let wire = writer_mode_to_proto(mode);
        assert_eq!(writer_mode_from_proto(wire.into()), Ok(mode));
    }
    assert_ne!(
        writer_mode_to_proto(WriterMode::Single),
        writer_mode_to_proto(WriterMode::Multi)
    );
    assert_eq!(
        writer_mode_from_proto(UNKNOWN),
        Err("an unknown writer mode")
    );
}

/// A journal view round-trips; an absent one is the default view; a view
/// whose first position is past its next one is refused.
#[test]
fn a_journal_view_round_trips_and_an_ill_formed_one_is_refused() {
    let view = JournalView {
        leader: Some(LeaderUuid(7)),
        next_seq: Seq(9),
        first_seq: Seq(4),
    };
    assert_eq!(
        journal_view_from_proto(Some(journal_view_to_proto(view))),
        Ok(view)
    );
    assert_eq!(journal_view_from_proto(None), Ok(JournalView::default()));
    let empty = JournalView {
        leader: None,
        next_seq: Seq(4),
        first_seq: Seq(4),
    };
    assert_eq!(
        journal_view_from_proto(Some(journal_view_to_proto(empty))),
        Ok(empty)
    );
    let ill = common::JournalView {
        leader: None,
        next_seq: 3,
        first_seq: 4,
    };
    assert_eq!(
        journal_view_from_proto(Some(ill)),
        Err("ill-formed journal view")
    );
}

/// A refused reconfiguration names the matchmaker's phase: each phase
/// crosses the wire as itself, and an unknown one is refused.
#[test]
fn a_refused_reply_carries_each_phase_and_refuses_an_unknown_one() {
    let refused = |phase| ReconfigureReply::Refused {
        matchmaker: MatchmakerId(1),
        current: MatchmakerSet::new(MatchmakerGeneration(2), [1, 5].map(MatchmakerId).into()),
        phase,
        successor: None,
    };
    let phases = [
        MatchmakerPhase::Fresh,
        MatchmakerPhase::Inactive,
        MatchmakerPhase::Active,
        MatchmakerPhase::Stopped,
    ];
    for phase in phases {
        let reply = refused(phase);
        let wire = wire_reconfigure_reply(&reply);
        let decoded =
            WireReconfigureReply::decode(wire.encode_to_vec().as_slice()).expect("decode");
        assert_eq!(reconfigure_reply_from_wire(decoded), Ok(reply));
    }
    let mut wire = wire_reconfigure_reply(&refused(MatchmakerPhase::Active));
    let Some(crate::matchmaker::reconfigure_reply::Kind::Refused(body)) = wire.kind.as_mut() else {
        panic!("a refused reply encodes as refused");
    };
    body.phase = UNKNOWN;
    assert_eq!(
        reconfigure_reply_from_wire(wire),
        Err("unknown matchmaker phase")
    );
}
