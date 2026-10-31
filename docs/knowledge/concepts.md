# Concepts: the index

Every concept paros uses, its card, its code and its backing sources. The map and the card
template are in [`CLAUDE.md`](CLAUDE.md).

Status: **verified** (the card exists and a second reviewer checked it), **planned** (no card
yet; the sources are known), **waits for a source** (no source outside paros yet, so no card).

| Group | Concept | Card | Status | paros code | Backing sources |
|---|---|---|---|---|---|
| Kernel | Single-decree Paxos | [single-decree-paxos](concepts/single-decree-paxos.md) | verified | `decree.rs`, `acceptor.rs` | paxos-made-simple §2.1–§2.5, generalised-distributed-consensus §3–§4.1, paxos-made-moderately-complex §4.1, frankenpaxos `02-single-decree-paxos.md` |
| Kernel | Multi-Paxos log and stable leader | [multi-paxos-log](concepts/multi-paxos-log.md) | verified | `proposer.rs`, `replica.rs`, `node/phase2.rs`, `node/learn.rs` | paxos-made-simple §3, paxos-made-moderately-complex §2/§4.1/§4.3, paxos-made-live §4.2, frankenpaxos `03-multipaxos-core.md` |
| Kernel | Persist before send (`Ready`, `HardState`, `WriteOp`) | [persist-before-send](concepts/persist-before-send.md) | verified | `node/ready.rs`, `contract/hard_state.rs`, `contract/write.rs`, `paros-roles` `node/ready.rs` | paxos-made-simple §2.5, paxos-made-live §4.2, etcd-raft durable before send (`../references/etcd-raft/replication.md` §1), `../analysis/go-raft/etcd-raft-sans-io-patterns.md` §2 |
| Kernel | Pipelining and resend (`resend_pending`) | [pipelining-and-resend](concepts/pipelining-and-resend.md) | verified | `node/policy.rs`, `proposer/rounds.rs` | paxos-made-simple §3, paxos-made-moderately-complex §2, etcd-raft progress and inflight window (`../references/etcd-raft/replication.md` §2–§3) |
| Kernel | Catch-up of a lagging follower | [catch-up](concepts/catch-up.md) | verified | `node/catch_up.rs`, `node/replication.rs` | paxos-made-live §4.2/§5.5, paxos-made-moderately-complex §4.2, etcd-raft progress states and snapshot fallback (`../references/etcd-raft/replication.md` §2, §4) |
| Leader | Election, P2c merge and gap fill (`Noop`, `record_own_round`) | [election](concepts/election.md) | verified | `proposer/election.rs`, `proposer/recovery.rs`, `node/election.rs`, `node/phase2.rs` | paxos-made-moderately-complex §2–§4.1, paxos-made-live §4.1–§4.2, paxos-vs-raft §3.3–§5, frankenpaxos `03-multipaxos-core.md` §3 |
| Leader | Paging a campaign (`keep_alive_from`) | | waits for a source | `proposer/election.rs`, `node/election.rs` | to find: etcd-raft message size limits, or frankenpaxos recovery |
| Leader | Leadership fence and CheckQuorum | [check-quorum](concepts/check-quorum.md) | verified | `proposer/authority.rs`, `node/authority.rs` | etcd-raft CheckQuorum (`../references/etcd-raft/check-quorum.md`), flexible-paxos §3, paxos-made-live §5.2, foundationdb §6 |
| Leader | Leader handoff (`relinquish_to`) | [leader-handoff](concepts/leader-handoff.md) | verified | `node/handoff.rs` | dpaxos §4.4, paxos-made-moderately-complex §2, etcd-raft leader transfer (`../references/etcd-raft/check-quorum.md` §6) |
| Quorums | Flexible quorums | [flexible-quorums](concepts/flexible-quorums.md) | verified | `membership/quorum_system.rs`, `membership/acceptor_config.rs` | flexible-paxos §3–§5/§7, generalised-distributed-consensus §4.1 |
| Quorums | Grid quorums (acceptor partitions) | [grid-quorums](concepts/grid-quorums.md) | verified | `membership/quorum_system.rs`, `node/policy.rs` (`move_round`), `paros-roles` `node/grid.rs` | scaling-rsm-compartmentalization §3.2, flexible-paxos §4, wpaxos §3.1, frankenpaxos `04-compartmentalization.md` |
| Quorums | Zone rule | [zone-rule](concepts/zone-rule.md) | verified | `membership/zone_rule.rs`, `membership/failure_domain.rs`, `paros-model` `placement.rs` | wpaxos §3.1/§5.3, flexible-paxos §3, aws-cell-based-architecture |
| Compartmentalization | Proxy leaders | [proxy-leaders](concepts/proxy-leaders.md) | verified | `proxy_leader.rs`, `proposer/rounds.rs`, `node/policy.rs`, `models/proxy.rs`, `paros-roles` `proxy_leader.rs` | scaling-rsm-compartmentalization §3.1/§8.1, frankenpaxos `04-compartmentalization.md` §3 |
| Compartmentalization | Batchers and unbatchers | [batchers](concepts/batchers.md) | verified | `batcher.rs`, `paros-roles` `batcher.rs` | scaling-rsm-compartmentalization §4.1–§4.2/§8.4, frankenpaxos `04-compartmentalization.md` §2 |
| Compartmentalization | Replica tier | [replica-tier](concepts/replica-tier.md) | verified | `replica_node.rs`, `replica.rs`, `paros-roles` `replica.rs` | scaling-rsm-compartmentalization §3.3–§3.5/§8.5, frankenpaxos `04-compartmentalization.md` §4 |
| Reads | Leaderless quorum reads | [leaderless-reads](concepts/leaderless-reads.md) | verified | `quorum_read.rs`, `node/quorum_reads.rs`, `acceptor.rs` (`vote_watermark`), `replica_node.rs` | scaling-rsm-compartmentalization §3.4–§3.6, frankenpaxos `04-compartmentalization.md` §5; Paxos Quorum Reads (transcript to add) |
| Reconfiguration | Matchmaking and Phase 1 over every configuration | [matchmaking](concepts/matchmaking.md) | verified | `matchmaking.rs`, `node/matchmaking.rs`, `matchmaker.rs` | matchmaker-paxos §1/§2.3/§3.1–§3.3/§3.6/§4.3–§4.5/§6/§7.2, frankenpaxos `05-matchmaker-reconfiguration.md` §2–§3 |
| Reconfiguration | Reconfiguration as a round change, beliefs and the membership probe | [reconfiguration](concepts/reconfiguration.md) | verified | `node/reconfigure.rs`, `matchmaking/probe.rs`, `paros-coordinator` `tenant/moves.rs` | matchmaker-paxos §3.3/§4.2–§4.4/§6/§7.1, frankenpaxos `05-matchmaker-reconfiguration.md` §1/§6 |
| Reconfiguration | Configuration GC and retirement | [configuration-gc](concepts/configuration-gc.md) | verified | `collector.rs`, `node/gc.rs`, `paros-roles` `node/operator.rs` | matchmaker-paxos §3.4–§3.5/§4.5/§7.1/App. A/App. D, frankenpaxos `05-matchmaker-reconfiguration.md` §4 |
| Reconfiguration | Matchmaker generations and handover | [matchmaker-generations](concepts/matchmaker-generations.md) | verified | `matchmaker/generation.rs`, `matchmaker/reconfigurer.rs`, `decree.rs`, `models/handover.rs` | matchmaker-paxos §5/§7.3/App. B, frankenpaxos `05-matchmaker-reconfiguration.md` §5 |
| Storage | Corrupted storage recovery (CTRL faulty set, repair probe) | | planned | `acceptor.rs`, `proposer/probe.rs` | protocol-aware-recovery |
| Storage | Truncation, compaction floor and trim-point jump | | planned | `acceptor/retention.rs`, `node/catch_up.rs` | paxos-made-live; ceph mon |
| Storage | Boot safety (format marker, a wiped node never rejoins) | | planned | `paros-storage`, `paros-roles` `node/boot.rs` | paxos-made-live, protocol-aware-recovery |
| On the log | Writer fence (leader uuid compare-and-set, judged at apply) | | planned | `journal/state.rs` | amazon-memorydb §4.1 |

Paths in the "paros code" column are relative to `crates/lib/paros-core/src/` unless they name
another crate.

## Sources added for the knowledge base

- `../references/etcd-raft/check-quorum.md`: etcd-raft CheckQuorum, the vote lease and leader
  transfer, at commit `c51f010` (Leader group).
- `../references/etcd-raft/replication.md`: etcd-raft durable before send, the progress states,
  the inflight window and the snapshot fallback, at commit `c51f010` (Kernel group).
- To add: the Paxos Quorum Reads transcript (Charapko, Ailijiang, Demirbas, HotStorage 2019),
  and an etcd-raft analysis of the message size limits.
