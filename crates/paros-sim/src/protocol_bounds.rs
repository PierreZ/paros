//! Deterministic public-core choreography for issue #96's bounded protocol work.

use std::collections::BTreeMap;

use async_trait::async_trait;
use moonpool_sim::{
    Process, SimContext, SimulationError, SimulationResult, Workload, assert_always,
    assert_reachable, assert_sometimes,
};
use paros::{
    Ballot, ClientId, ClientSeq, Command, Config, Entry, HardState, LEADER_RECOVERY_BATCH, Message,
    NodeId, NodeRole, PROMISE_BATCH, ProposeResult, QuorumSystem, RawNode, Slot, Storage, Value,
    command_fingerprint,
};

const SUFFIX_LEN: u64 = 2 * PROMISE_BATCH as u64 + 2;

#[derive(Clone)]
struct CoreStorage {
    id: NodeId,
    accepted: BTreeMap<Slot, (Ballot, Command)>,
}

impl CoreStorage {
    fn empty(id: u64) -> Self {
        Self {
            id: NodeId(id),
            accepted: BTreeMap::new(),
        }
    }
}

impl Storage for CoreStorage {
    fn initial_state(&self) -> (HardState, Config) {
        (
            HardState::default(),
            Config {
                id: self.id,
                peers: vec![NodeId(0), NodeId(1), NodeId(2)],
                quorum_system: QuorumSystem::Majority,
            },
        )
    }

    fn accepted(&self, slot: Slot) -> Option<(Ballot, Command)> {
        self.accepted.get(&slot).cloned()
    }

    fn first_slot(&self) -> Slot {
        Slot(0)
    }

    fn last_slot(&self) -> Slot {
        self.accepted.keys().next_back().copied().unwrap_or(Slot(0))
    }
}

fn command(slot: u64) -> Command {
    Command::User(Entry {
        client: ClientId(7),
        seq: ClientSeq(slot),
        value: Value(slot.to_le_bytes().to_vec()),
    })
}

type ReadyOutput = (Vec<(NodeId, Message)>, Option<(usize, usize, usize)>);

fn take_ready(node: &mut RawNode) -> ReadyOutput {
    let ready = node.ready();
    let messages = ready.messages().to_vec();
    let recovery = ready.recovery_batch();
    ready.advance();
    (messages, recovery)
}

/// One factory-created process that drives only public `RawNode` APIs. The
/// scenario is deterministic by design: every seed proves the same resource
/// boundary, while the main Chain campaign remains responsible for emergent
/// cluster interleavings.
pub(crate) struct ProtocolBoundsWorkload;

#[async_trait]
impl Workload for ProtocolBoundsWorkload {
    fn name(&self) -> &'static str {
        "paros-protocol-bounds"
    }

    #[allow(clippy::too_many_lines)]
    async fn run(&mut self, _ctx: &SimContext) -> SimulationResult<()> {
        let mut candidate = RawNode::new(&CoreStorage::empty(1));
        candidate.set_election_timeout(1);
        candidate.tick();
        let ballot = candidate.ballot();
        let _ = take_ready(&mut candidate);

        let mut storage = CoreStorage::empty(0);
        for slot in 0..SUFFIX_LEN {
            storage
                .accepted
                .insert(Slot(slot), (Ballot::zero(), command(slot)));
        }
        let mut acceptor = RawNode::new(&storage);
        let mut cursor = Slot(0);
        let mut pages = 0_usize;
        let mut entries_seen = 0_usize;
        let first_recovery;

        loop {
            acceptor.step(Message::Prepare {
                from: NodeId(1),
                ballot,
                from_slot: cursor,
            });
            let (messages, _) = take_ready(&mut acceptor);
            let Some(Message::Promise {
                from,
                ballot: promised,
                from_slot,
                accepted,
                next_from_slot,
            }) = messages
                .into_iter()
                .map(|(_, message)| message)
                .find(|message| matches!(message, Message::Promise { .. }))
            else {
                assert_always!(
                    false,
                    "the bounded suffix choreography receives a Promise page"
                );
                return Err(SimulationError::InvalidState("missing Promise page".into()));
            };
            assert_always!(
                accepted.len() <= PROMISE_BATCH,
                "a Promise carries at most one bounded suffix chunk",
                { "entries" => accepted.len() }
            );
            pages += 1;
            entries_seen += accepted.len();

            candidate.step(Message::Promise {
                from,
                ballot: promised,
                from_slot,
                accepted,
                next_from_slot,
            });
            let (candidate_messages, recovery) = take_ready(&mut candidate);
            if let Some(next) = next_from_slot {
                if pages == 1 {
                    assert_reachable!("a Promise suffix continues across bounded pages");
                }
                let Some(Message::Prepare {
                    from,
                    ballot: continuation_ballot,
                    from_slot,
                }) = candidate_messages
                    .into_iter()
                    .map(|(_, message)| message)
                    .find(|message| matches!(message, Message::Prepare { .. }))
                else {
                    assert_always!(
                        false,
                        "a partial Promise page requests its exact continuation"
                    );
                    return Err(SimulationError::InvalidState(
                        "missing Promise continuation".into(),
                    ));
                };
                assert_always!(
                    from == NodeId(1) && continuation_ballot == ballot && from_slot == next,
                    "a partial Promise page requests its exact continuation"
                );
                cursor = next;
            } else {
                first_recovery = recovery;
                break;
            }
        }

        assert_sometimes!(
            pages > 1
                && entries_seen == usize::try_from(SUFFIX_LEN).expect("suffix length fits usize"),
            "a Promise suffix continues across bounded pages"
        );
        assert_always!(
            candidate.role() == NodeRole::Leader,
            "only a terminal Promise page completes the election quorum"
        );

        let mut recovery = first_recovery;
        let mut recovery_pages = 0_usize;
        loop {
            let Some((started, gap_fills, remaining)) = recovery else {
                assert_always!(false, "leader recovery exposes its bounded Ready chunk");
                return Err(SimulationError::InvalidState(
                    "missing leader recovery chunk".into(),
                ));
            };
            assert_always!(
                started <= LEADER_RECOVERY_BATCH,
                "a leader starts at most one bounded recovery chunk per Ready",
                { "started" => started, "gap_fills" => gap_fills, "remaining" => remaining }
            );
            recovery_pages += 1;
            if remaining == 0 {
                break;
            }
            if recovery_pages == 1 {
                assert_reachable!(
                    "a leader recovery suffix continues across bounded Ready batches"
                );
            }
            candidate.advance_recovery();
            recovery = take_ready(&mut candidate).1;
        }
        assert_sometimes!(
            recovery_pages > 1,
            "a leader recovery suffix continues across bounded Ready batches"
        );

        // A gap fill can unlock a long suffix the node already learned chosen
        // out of order. That contiguous release is recovery work too: it must
        // not turn one Accepted ack into an unbounded Ready write batch.
        let mut gapped = RawNode::new(&CoreStorage::empty(0));
        let prior = Ballot {
            round: 1,
            node: NodeId(1),
        };
        for slot in 1..SUFFIX_LEN {
            gapped.step(Message::Commit {
                from: NodeId(1),
                ballot: prior,
                slot: Slot(slot),
                command: command(slot),
            });
            let _ = take_ready(&mut gapped);
        }
        gapped.set_election_timeout(1);
        gapped.tick();
        let gap_ballot = gapped.ballot();
        let _ = take_ready(&mut gapped);
        gapped.step(Message::Promise {
            from: NodeId(1),
            ballot: gap_ballot,
            from_slot: Slot(0),
            accepted: BTreeMap::new(),
            next_from_slot: None,
        });
        let _ = take_ready(&mut gapped);
        let noop = Command::Control(paros::Control::Noop);
        gapped.step(Message::Accepted {
            from: NodeId(1),
            ballot: gap_ballot,
            slot: Slot(0),
            vhash: command_fingerprint(&noop),
        });
        let mut prefix_pages = 0_usize;
        loop {
            let ready = gapped.ready();
            assert_always!(
                ready.writes().len() <= LEADER_RECOVERY_BATCH + 1,
                "a gap fill releases at most one bounded chosen-prefix write chunk",
                { "writes" => ready.writes().len() }
            );
            prefix_pages += 1;
            ready.advance();
            if gapped.hard_state().chosen_index == Some(Slot(SUFFIX_LEN - 1)) {
                break;
            }
            gapped.advance_recovery();
        }
        assert_sometimes!(
            prefix_pages > 1,
            "a chosen-prefix continuation drains across bounded Ready batches"
        );

        let mut nacked = RawNode::new(&CoreStorage::empty(2));
        nacked.set_election_timeout(1);
        nacked.tick();
        let rejected = nacked.ballot();
        let _ = take_ready(&mut nacked);
        nacked.step(Message::Nack {
            from: NodeId(0),
            ballot: rejected,
            promised: Ballot {
                round: u64::MAX,
                node: NodeId(0),
            },
            slot: Slot(0),
        });
        let _ = take_ready(&mut nacked);
        nacked.set_election_timeout(1);
        nacked.tick();
        assert_always!(
            nacked.ballot().round == rejected.round + 1,
            "a Nack wire hint does not select a future campaign round"
        );

        let mut stale = RawNode::new(&CoreStorage::empty(0));
        stale.set_election_timeout(1);
        stale.tick();
        let stale_ballot = stale.ballot();
        let _ = take_ready(&mut stale);
        let learned = Ballot {
            round: stale_ballot.round + 1,
            node: NodeId(1),
        };
        stale.step(Message::Commit {
            from: NodeId(1),
            ballot: learned,
            slot: Slot(0),
            command: command(0),
        });
        let _ = take_ready(&mut stale);
        stale.step(Message::Prepare {
            from: NodeId(1),
            ballot: learned,
            from_slot: Slot(1),
        });
        assert_always!(
            stale.role() == NodeRole::Follower,
            "a same-ballot continuation closes a different stale campaign"
        );

        let mut proposer = RawNode::new(&CoreStorage::empty(0));
        proposer.set_election_timeout(1);
        proposer.tick();
        let proposal_ballot = proposer.ballot();
        let _ = take_ready(&mut proposer);
        proposer.step(Message::Promise {
            from: NodeId(1),
            ballot: proposal_ballot,
            from_slot: Slot(0),
            accepted: BTreeMap::new(),
            next_from_slot: None,
        });
        let _ = take_ready(&mut proposer);
        let ProposeResult::Accepted(slot) =
            proposer.propose(ClientId(9), ClientSeq(1), Value(vec![1, 2, 3]))
        else {
            return Err(SimulationError::InvalidState(
                "leader rejected protocol-bounds proposal".into(),
            ));
        };
        let expected = command_fingerprint(&command(0));
        // Compute the actual admitted identity, not merely the payload: the
        // command helper intentionally uses a different client/sequence pair.
        let admitted = Command::User(Entry {
            client: ClientId(9),
            seq: ClientSeq(1),
            value: Value(vec![1, 2, 3]),
        });
        let admitted_hash = command_fingerprint(&admitted);
        assert_always!(
            expected != admitted_hash,
            "command fingerprints include identity"
        );
        proposer.step(Message::Accepted {
            from: NodeId(1),
            ballot: proposal_ballot,
            slot,
            vhash: admitted_hash ^ 1,
        });
        assert_always!(
            proposer.hard_state().chosen_index.is_none(),
            "a mismatched Accepted fingerprint is not credited"
        );
        proposer.step(Message::Accepted {
            from: NodeId(1),
            ballot: proposal_ballot,
            slot,
            vhash: admitted_hash,
        });
        assert_always!(
            proposer.hard_state().chosen_index == Some(slot),
            "a matching Accepted fingerprint can complete its quorum"
        );

        Ok(())
    }
}

/// One inert topology member keeps the simulator lifecycle open while the
/// workload drives the sans-I/O core directly.
pub(crate) struct ProtocolBoundsIdleProcess;

#[async_trait]
impl Process for ProtocolBoundsIdleProcess {
    fn name(&self) -> &'static str {
        "paros-protocol-bounds-idle"
    }

    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        ctx.shutdown().cancelled().await;
        Ok(())
    }
}
