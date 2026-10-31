# Concepts: the index

Every concept paros uses, its card, its code and its backing sources. The map and the card
template are in [`CLAUDE.md`](CLAUDE.md).

Status: **verified** (the card exists and a second reviewer checked it), **planned** (no card
yet; the sources are known), **waits for a source** (no source outside paros yet, so no card).

| Group | Concept | Card | Status | paros code | Backing sources |
|---|---|---|---|---|---|
| Kernel | Single-decree Paxos | | planned | `decree.rs`, `acceptor.rs` | paxos-made-simple, generalised-distributed-consensus |
| Kernel | Multi-Paxos log and stable leader | | planned | `proposer.rs`, `replica.rs` | paxos-made-moderately-complex, paxos-made-live |
| Kernel | Persist before send (`Ready`, `HardState`, `WriteOp`) | | planned | `node/ready.rs`, `contract/hard_state.rs`, `contract/write.rs` | paxos-made-live; etcd-raft `Ready` (`../analysis/go-raft/etcd-raft-sans-io-patterns.md`) |
| Kernel | Pipelining and resend (`resend_pending`) | | planned | `node/replication.rs`, `node/policy.rs` | paxos-made-moderately-complex; etcd-raft inflight window (analysis to write) |
| Kernel | Catch-up of a lagging follower | | planned | `node/catch_up.rs`, `node/replication.rs` | etcd-raft progress probe and replicate states (analysis to write) |
| Leader | Election, P2c merge and gap fill (`Noop`, `record_own_round`) | [election](concepts/election.md) | verified | `proposer/election.rs`, `proposer/recovery.rs`, `node/election.rs`, `node/phase2.rs` | paxos-made-moderately-complex §2–§4.1, paxos-made-live §4.1–§4.2, paxos-vs-raft §3.3–§5, frankenpaxos `03-multipaxos-core.md` §3 |
| Leader | Paging a campaign (`keep_alive_from`) | | waits for a source | `proposer/election.rs`, `node/election.rs` | to find: etcd-raft message size limits, or frankenpaxos recovery |
| Leader | Leadership fence and CheckQuorum | [check-quorum](concepts/check-quorum.md) | verified | `proposer/authority.rs`, `node/authority.rs` | etcd-raft CheckQuorum (`../references/etcd-raft/check-quorum.md`), flexible-paxos §3, paxos-made-live §5.2, foundationdb §6 |
| Leader | Leader handoff (`relinquish_to`) | [leader-handoff](concepts/leader-handoff.md) | verified | `node/handoff.rs` | dpaxos §4.4, paxos-made-moderately-complex §2, etcd-raft leader transfer (`../references/etcd-raft/check-quorum.md` §6) |
| Quorums | Flexible quorums | | planned | `membership/quorum_system.rs` | flexible-paxos |
| Quorums | Grid quorums (acceptor partitions) | | planned | `membership/quorum_system.rs` | flexible-paxos, scaling-rsm-compartmentalization |
| Quorums | Zone rule | | planned | `membership/zone_rule.rs` | wpaxos |
| Compartmentalization | Proxy leaders | | planned | `proxy_leader.rs`, `proposer/rounds.rs` | scaling-rsm-compartmentalization; frankenpaxos |
| Compartmentalization | Batchers and unbatchers | | planned | `batcher.rs` | scaling-rsm-compartmentalization; frankenpaxos |
| Compartmentalization | Replica tier | | planned | `replica_node.rs` | scaling-rsm-compartmentalization; frankenpaxos |
| Reads | Leaderless quorum reads | | planned | `quorum_read.rs`, `node/quorum_reads.rs` | scaling-rsm-compartmentalization §3.4; Paxos Quorum Reads (transcript to add) |
| Reconfiguration | Matchmaking and Phase 1 over every configuration | | planned | `matchmaking.rs`, `node/matchmaking.rs` | matchmaker-paxos; frankenpaxos |
| Reconfiguration | Reconfiguration as a round change, beliefs and the membership probe | | planned | `node/reconfigure.rs`, `matchmaking/probe.rs` | matchmaker-paxos |
| Reconfiguration | Configuration GC and retirement | | planned | `collector.rs`, `node/gc.rs` | matchmaker-paxos |
| Reconfiguration | Matchmaker generations and handover | | planned | `matchmaker/generation.rs`, `matchmaker/reconfigurer.rs` | matchmaker-paxos |
| Storage | Corrupted storage recovery (CTRL faulty set, repair probe) | | planned | `acceptor.rs`, `proposer/probe.rs` | protocol-aware-recovery |
| Storage | Truncation, compaction floor and trim-point jump | | planned | `acceptor/retention.rs`, `node/catch_up.rs` | paxos-made-live; ceph mon |
| Storage | Boot safety (format marker, a wiped node never rejoins) | | planned | `paros-storage`, `paros-roles` `node/boot.rs` | paxos-made-live, protocol-aware-recovery |
| On the log | Writer fence (leader uuid compare-and-set, judged at apply) | | planned | `journal/state.rs` | amazon-memorydb §4.1 |

Paths in the "paros code" column are relative to `crates/lib/paros-core/src/` unless they name
another crate.

## Sources added for the knowledge base

- `../references/etcd-raft/check-quorum.md`: etcd-raft CheckQuorum, the vote lease and leader
  transfer, at commit `c51f010` (Leader group).
- To add: the Paxos Quorum Reads transcript (Charapko, Ailijiang, Demirbas, HotStorage 2019),
  and etcd-raft analyses of the inflight window, the progress states and the message size
  limits.
