//! The node reports' oracles on hand-built report sequences: a node's
//! honest reports stay clean, and each broken report goes red at the
//! oracle that owns it.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use paros::core::journal::{Command, Control, Outcome};
use paros::core::membership::{AcceptorConfig, QuorumSystem};
use paros::core::{Ballot, Message, NodeId, Slot};
use paros::roles::node::command_hash;

use crate::audit::state::Floor;
use crate::audit::testing::{FrozenClock, red};
use crate::audit::{AuditWorld, NodeAudit};

fn ballot(round: u64, node: u64) -> Ballot {
    Ballot {
        round,
        node: NodeId(node),
    }
}

fn majority(members: &[u64]) -> AcceptorConfig {
    AcceptorConfig::new(
        members.iter().map(|m| NodeId(*m)).collect(),
        QuorumSystem::Majority,
    )
}

/// A node audit over a journal of three acceptors on plain Multi-Paxos.
fn audit() -> (Arc<AuditWorld>, NodeAudit<FrozenClock>) {
    let world = Arc::new(AuditWorld::default());
    world.lock().bootstrap = Some(majority(&[0, 1, 2]));
    let audit = NodeAudit::new(FrozenClock, world.clone());
    (world, audit)
}

const NOOP: Command = Command::Control(Control::Noop);

/// The `Promise`s of `senders` for `b` left the wire: a Phase 1 the
/// candidate of `b` may count (#458 (core oracles)).
fn promised_by(world: &AuditWorld, b: Ballot, senders: &[u64]) {
    let mut st = world.lock();
    for sender in senders {
        st.observe_promise_send(*sender, b);
    }
}

/// A `Promise` page from `from` at `b` that carries `accepted`.
fn promise(from: u64, b: Ballot, accepted: &[(u64, Ballot, Command)]) -> Message {
    Message::Promise {
        from: NodeId(from),
        ballot: b,
        from_slot: Slot(0),
        accepted: accepted
            .iter()
            .map(|(slot, eb, command)| (Slot(*slot), (*eb, command.clone())))
            .collect(),
        faulty: BTreeMap::new(),
        next_from_slot: None,
    }
}

#[test]
fn an_accept_never_exceeds_the_promise_it_was_made_under() {
    let (_, audit) = audit();
    let b = ballot(2, 0);
    assert!(
        red(|| audit.accepted(NodeId(1), Slot(0), b, b, 9)),
        "no promise reported"
    );
    audit.promised(NodeId(1), b);
    assert!(!red(|| audit.accepted(NodeId(1), Slot(0), b, b, 9)));
    let above = ballot(3, 0);
    assert!(
        red(|| audit.accepted(NodeId(1), Slot(1), above, b, 9)),
        "above the driver's word"
    );
    assert!(
        red(|| audit.accepted(NodeId(1), Slot(1), above, above, 9)),
        "above the folded promise"
    );
}

#[test]
fn an_accept_below_the_compaction_floor_is_red() {
    let (world, audit) = audit();
    let b = ballot(2, 0);
    audit.promised(NodeId(1), b);
    world.lock().floor.insert(
        1,
        // Held since before this instant (the frozen clock's 0).
        Floor {
            now: 4,
            before_last_raise: 4,
            raised_ms: 0,
        },
    );
    assert!(red(|| audit.accepted(NodeId(1), Slot(3), b, b, 9)));
    assert!(!red(|| audit.accepted(NodeId(1), Slot(4), b, b, 9)));
}

#[test]
fn a_durable_accept_settles_what_its_commit_had_in_flight() {
    let (world, audit) = audit();
    let b = ballot(2, 0);
    audit.promised(NodeId(1), b);
    world.note_in_flight(1, &[(5, 9), (6, 7)]);
    world.note_dropped_in_flight(1, &[(5, b, 9)]);
    audit.accepted(NodeId(1), Slot(5), b, b, 9);
    let st = world.lock();
    assert!(!st.in_flight.contains_key(&(1, 5)));
    assert!(st.in_flight.contains_key(&(1, 6)));
    assert!(!st.dropped_in_flight.contains_key(&(1, 5)));
    assert_eq!(st.persisted[&(1, 5)], 9);
}

#[test]
fn a_compaction_floor_never_regresses_or_passes_the_decided_frontier() {
    let (world, audit) = audit();
    world.lock().decided_prefix.insert(1, 6);
    assert!(!red(|| audit.truncated(NodeId(1), Slot(4))));
    assert!(red(|| audit.truncated(NodeId(1), Slot(3))), "a regression");
    assert!(
        red(|| audit.truncated(NodeId(1), Slot(7))),
        "past the decided prefix"
    );
    let st = world.lock();
    assert_eq!(st.truncate_watermark[&1], 7);
    assert_eq!(st.floor[&1].now, 7);
    assert!(st.compacted);
}

#[test]
fn a_trim_jump_lands_inside_the_cells_applied_frontier() {
    let (_, audit) = audit();
    assert!(
        red(|| audit.trimmed_to(NodeId(1), Slot(4))),
        "nothing applied yet"
    );
    let (world, audit) = self::audit();
    world.lock().cluster_applied_max = Some(5);
    assert!(!red(|| audit.trimmed_to(NodeId(1), Slot(4))));
    let st = world.lock();
    assert!(st.trim_jumped);
    assert_eq!(st.decided_prefix[&1], 4);
    assert_eq!(st.chosen_watermark[&1], 3);
    assert!(st.landings[&1].contains(&3));
    assert_eq!(st.frontier[&1], 4);
}

#[test]
fn an_applied_slot_was_decided_and_carries_the_decided_value() {
    let vhash = command_hash(&NOOP);
    let (_, audit) = audit();
    assert!(
        red(|| audit.applied(NodeId(1), Slot(0), vhash, &NOOP, Some(&Outcome::Noop))),
        "never decided"
    );
    let (world, audit) = self::audit();
    {
        let mut st = world.lock();
        let b = ballot(2, 0);
        st.observe_durable_accept(0, 0, b, vhash);
        st.observe_durable_accept(1, 0, b, vhash);
    }
    assert!(!red(|| audit.applied(
        NodeId(1),
        Slot(0),
        vhash,
        &NOOP,
        None
    )));
    assert!(
        red(|| audit.applied(NodeId(2), Slot(0), vhash + 1, &NOOP, None)),
        "another value"
    );
    assert!(world.lock().any_chosen);
}

#[test]
fn a_sent_promise_carries_a_durably_promised_ballot() {
    let (_, audit) = audit();
    let b = ballot(2, 0);
    let promise = Message::Promise {
        from: NodeId(1),
        ballot: b,
        from_slot: Slot(0),
        accepted: BTreeMap::new(),
        faulty: BTreeMap::new(),
        next_from_slot: None,
    };
    assert!(red(|| audit.sent(NodeId(1), NodeId(0), &promise)));
    audit.promised(NodeId(1), b);
    assert!(!red(|| audit.sent(NodeId(1), NodeId(0), &promise)));
}

#[test]
fn an_accepted_on_the_wire_needs_its_durable_accept_first() {
    let (_, audit) = audit();
    let b = ballot(2, 0);
    let vote = Message::Accepted {
        from: NodeId(1),
        ballot: b,
        slot: Slot(0),
        vhash: 9,
    };
    assert!(red(|| audit.sent(NodeId(1), NodeId(0), &vote)));
    audit.promised(NodeId(1), b);
    audit.accepted(NodeId(1), Slot(0), b, b, 9);
    assert!(!red(|| audit.sent(NodeId(1), NodeId(0), &vote)));
}

#[test]
fn an_accept_on_the_wire_is_judged_for_its_authority_addressee_and_command() {
    let (_, audit) = audit();
    let b = ballot(2, 0);
    let accept = |to_command: Command| Message::Accept {
        reply_to: paros::core::contract::Party::Node(NodeId(0)),
        leader: NodeId(0),
        ballot: b,
        slot: Slot(3),
        command: to_command,
        config: None,
    };
    let write = Command::Write(paros::core::journal::Entry {
        leader: paros::core::journal::LeaderUuid(1),
        seq: paros::core::journal::Seq(0),
        records: vec![paros::core::Value(b"x".to_vec())],
    });
    audit.promised(NodeId(0), b);
    // #578: the `Accept` may leave before its batch's flush, so the
    // missing record is judged once the next batch starts.
    assert!(!red(|| audit.sent(NodeId(0), NodeId(1), &accept(NOOP))));
    audit.commit_started(NodeId(0), &[]);
    assert!(
        red(|| audit.commit_started(NodeId(0), &[])),
        "the leader never recorded its round"
    );
    audit.accepted(NodeId(0), Slot(3), b, b, command_hash(&NOOP));
    assert!(!red(|| audit.sent(NodeId(0), NodeId(1), &accept(NOOP))));
    assert!(
        red(|| audit.sent(NodeId(0), NodeId(7), &accept(NOOP))),
        "outside the configuration"
    );
    assert!(
        red(|| audit.sent(NodeId(0), NodeId(1), &accept(write))),
        "another command"
    );
    assert!(
        red(|| audit.sent(NodeId(1), NodeId(2), &accept(NOOP))),
        "a second holder"
    );
}

#[test]
fn a_plain_leader_keeps_the_bootstrap_and_its_ballots_strictly_increase() {
    let (world, audit) = audit();
    let three = majority(&[0, 1, 2]);
    promised_by(&world, ballot(2, 0), &[1]);
    promised_by(&world, ballot(3, 0), &[1, 2]);
    assert!(!red(|| audit.elected(
        NodeId(0),
        ballot(2, 0),
        ballot(2, 0),
        0,
        &three
    )));
    assert!(world.lock().any_leader);
    assert!(
        red(|| audit.elected(NodeId(0), ballot(2, 0), ballot(2, 0), 0, &three)),
        "the same ballot again"
    );
    assert!(
        red(|| audit.elected(NodeId(0), ballot(3, 0), ballot(4, 1), 0, &three)),
        "a promise above the win"
    );
    assert!(
        red(|| audit.elected(
            NodeId(1),
            ballot(5, 1),
            ballot(5, 1),
            0,
            &majority(&[0, 1, 3])
        )),
        "another configuration"
    );
}

#[test]
fn a_second_leader_round_stamps_the_leader_change() {
    let (world, audit) = audit();
    let three = majority(&[0, 1, 2]);
    promised_by(&world, ballot(2, 0), &[1]);
    promised_by(&world, ballot(3, 1), &[2]);
    audit.elected(NodeId(0), ballot(2, 0), ballot(2, 0), 0, &three);
    assert_eq!(world.lock().first_leader_round, Some(2));
    assert_eq!(world.lock().leader_change_ms, None);
    audit.elected(NodeId(1), ballot(3, 1), ballot(3, 1), 0, &three);
    assert_eq!(world.lock().leader_change_ms, Some(0));
    assert_eq!(world.lock().leader_rounds.len(), 2);
}

#[test]
fn a_grid_leader_never_runs_a_configuration_naming_a_retired_identity() {
    let grid = AcceptorConfig::new(
        (0..4).map(NodeId).collect(),
        QuorumSystem::Grid {
            rows: 2,
            cols: 2,
            acks: 2,
        },
    );
    let world = Arc::new(AuditWorld::default());
    world.lock().bootstrap = Some(grid.clone());
    let audit = NodeAudit::new(FrozenClock, world.clone());
    promised_by(&world, ballot(2, 0), &[1, 2, 3]);
    promised_by(&world, ballot(3, 0), &[1, 2, 3]);
    assert!(!red(|| audit.elected(
        NodeId(0),
        ballot(2, 0),
        ballot(2, 0),
        0,
        &grid
    )));
    assert!(world.lock().elected_grid);
    world.lock().retired.insert(3);
    assert!(red(|| audit.elected(
        NodeId(0),
        ballot(3, 0),
        ballot(3, 0),
        0,
        &grid
    )));
}

/// A replica's floor claim dies with its incarnation, as a node's does
/// (#448): its boot walks from the floor up, so the walk that moved the
/// floor is never reported, and the claim is never judged.
#[test]
fn a_replica_boot_drops_the_floor_claim_of_its_last_incarnation() {
    let (world, audit) = audit();
    let replica = NodeId(1001);
    assert!(!red(|| audit.replica_booted(replica, None, Slot(0))));
    world.lock().decided_prefix.insert(replica.0, 6);
    assert!(!red(|| audit.truncated(replica, Slot(5))));
    assert!(world.lock().floor_claims.contains_key(&replica.0));
    assert!(!red(|| audit.replica_booted(
        replica,
        Some(Slot(4)),
        Slot(5)
    )));
    assert!(!world.lock().floor_claims.contains_key(&replica.0));
}

/// One shared deployment of three acceptors on plain Multi-Paxos.
fn deployment() -> paros::observe::audit::Deployment {
    paros::observe::audit::Deployment {
        bootstrap: majority(&[0, 1, 2]),
        pool: vec![NodeId(0), NodeId(1), NodeId(2)],
        matchmakers: Vec::new(),
        matchmaker_pool: Vec::new(),
        replica_count: 0,
    }
}

#[test]
fn a_plain_leadership_without_a_promise_quorum_is_red() {
    let (world, audit) = audit();
    let three = majority(&[0, 1, 2]);
    assert!(
        red(|| audit.elected(NodeId(0), ballot(2, 0), ballot(2, 0), 0, &three)),
        "no Promise left the wire"
    );
    audit.promised(NodeId(1), ballot(3, 0));
    audit.sent(NodeId(1), NodeId(0), &promise(1, ballot(3, 0), &[]));
    assert!(!red(|| audit.elected(
        NodeId(0),
        ballot(3, 0),
        ballot(3, 0),
        0,
        &three
    )));
    assert!(world.lock().promise_senders.contains_key(&(3, 0)));
}

#[test]
fn a_sent_promise_is_judged_against_the_records_its_sender_holds() {
    let (_, audit) = audit();
    let b = ballot(2, 0);
    let p = ballot(4, 2);
    let noop = command_hash(&NOOP);
    audit.promised(NodeId(1), b);
    audit.accepted(NodeId(1), Slot(3), b, b, noop);
    audit.promised(NodeId(1), p);
    assert!(
        red(|| audit.sent(NodeId(1), NodeId(2), &promise(1, p, &[]))),
        "the held record left out"
    );
    assert!(!red(|| audit.sent(
        NodeId(1),
        NodeId(2),
        &promise(1, p, &[(3, b, NOOP)])
    )));
    // A floor past the record: the node holds it no more.
    audit.chosen_index(NodeId(1), Slot(3));
    assert!(!red(|| audit.truncated(NodeId(1), Slot(4))));
    assert!(!red(|| audit.sent(
        NodeId(1),
        NodeId(2),
        &promise(1, p, &[])
    )));
}

#[test]
fn a_boot_rebuilds_the_records_a_promise_owes() {
    let (world, audit) = audit();
    let b = ballot(2, 0);
    let p = ballot(4, 2);
    let noop = command_hash(&NOOP);
    audit.promised(NodeId(1), b);
    audit.accepted(NodeId(1), Slot(3), b, b, noop);
    audit.accepted(NodeId(1), Slot(5), b, b, noop);
    assert_eq!(
        world.lock().batch_accepts.get(&1).map(BTreeSet::len),
        Some(2)
    );
    // The boot read back slot 5 only: slot 3 was called faulty.
    world.lock().faulty_staged.entry(1).or_default().insert(3);
    let deployment = deployment();
    audit.recovered(
        NodeId(1),
        p,
        None,
        Slot(0),
        &deployment,
        &[(Slot(5), b, noop)],
    );
    assert!(!world.lock().batch_accepts.contains_key(&1));
    assert!(
        red(|| audit.sent(NodeId(1), NodeId(2), &promise(1, p, &[]))),
        "slot 5 is held"
    );
    assert!(!red(|| audit.sent(
        NodeId(1),
        NodeId(2),
        &promise(1, p, &[(5, b, NOOP)])
    )));
}

#[test]
fn a_commit_starts_a_new_batch() {
    let (world, audit) = audit();
    let b = ballot(2, 0);
    audit.promised(NodeId(1), b);
    audit.accepted(NodeId(1), Slot(3), b, b, 9);
    assert!(world.lock().batch_accepts.contains_key(&1));
    audit.commit_started(NodeId(1), &[]);
    assert!(!world.lock().batch_accepts.contains_key(&1));
}

#[test]
fn a_delegated_accept_needs_its_leaders_record_first() {
    let (_, audit) = audit();
    let b = ballot(2, 0);
    let accept = Message::Accept {
        reply_to: paros::core::contract::Party::Node(NodeId(0)),
        leader: NodeId(0),
        ballot: b,
        slot: Slot(3),
        command: NOOP,
        config: None,
    };
    let proxy = paros::core::membership::ProxyId(0);
    audit.promised(NodeId(0), b);
    audit.sent_to_proxy(NodeId(0), proxy, &accept);
    audit.commit_started(NodeId(0), &[]);
    assert!(red(|| audit.commit_started(NodeId(0), &[])));
    // Sent before the flush, recorded by the batch that follows.
    audit.sent_to_proxy(NodeId(0), proxy, &accept);
    audit.commit_started(NodeId(0), &[]);
    audit.accepted(NodeId(0), Slot(3), b, b, command_hash(&NOOP));
    assert!(!red(|| audit.commit_started(NodeId(0), &[])));
    assert!(!red(|| audit.sent_to_proxy(NodeId(0), proxy, &accept)));
}

#[test]
fn a_corruption_crash_explains_only_the_next_boots_missing_records() {
    use paros::observe::audit::StorageFaultDecision;
    use paros::storage::{MetadataFault, StorageError};
    let (world, audit) = audit();
    let deployment = deployment();
    let b = ballot(2, 0);
    audit.promised(NodeId(1), b);
    for slot in 0..3 {
        audit.accepted(NodeId(1), Slot(slot), b, b, 9);
    }
    let held = |slots: &[u64]| -> Vec<(Slot, Ballot, u64)> {
        slots.iter().map(|s| (Slot(*s), b, 9)).collect()
    };
    let boot = |slots: &[u64]| {
        red(|| audit.recovered(NodeId(1), b, None, Slot(0), &deployment, &held(slots)))
    };
    assert!(!boot(&[0, 1, 2]), "a whole boot");
    assert!(boot(&[0, 1]), "a record lost with no crash");
    audit.storage_fault(
        NodeId(1),
        &StorageError::Metadata {
            fault: MetadataFault::Missing,
        },
        StorageFaultDecision::Crash,
    );
    assert!(!boot(&[0]), "the crash explains this boot");
    assert!(world.lock().corruption_excused_loss);
    assert!(!world.lock().persisted.contains_key(&(1, 1)));
    // The crash explained one boot: the next owes what it still holds
    // (#912 (excuses too wide)).
    assert!(boot(&[]), "a later boot drops a record");
}
