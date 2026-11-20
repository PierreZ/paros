[← 08-build-steps.md](08-build-steps.md) · [Index](README.md)

## J10. Sources

The papers and the prior art of the journal. Each one is in the repository with its transcript
or digest; `docs/references/CLAUDE.md` indexes them. The sources of the whole service (the AWS
Journal, MemoryDB, Aurora DSQL, FoundationDB's metacluster and more) are architecture §10.

**The two papers that the journal builds.**

- Compartmentalized Paxos, "Scaling Replicated State Machines with Compartmentalization"
  (Whittaker et al., PVLDB 2021), `docs/references/papers/scaling-rsm-compartmentalization/`:
  the roles (batcher, leader, proxy leader, acceptor grid, replica, unbatcher) and the leaderless
  read (§3.4).
- Matchmaker Paxos (Whittaker et al.), `docs/references/papers/matchmaker-paxos/`: matchmaking,
  reconfiguration as a round change, GC of old configurations, and the Phase-1 bypass (§3.6,
  after v1).

**Shared logs and segments** (the prior art of design A, #1213 (segments in the protocol or in
storage); digests added in PR #1215):

- Delos (Balakrishnan et al., Facebook, OSDI 2020), `docs/references/papers/delos-virtual-consensus/`:
  a VirtualLog chains Loglets (segments) through a MetaStore register; reconfiguration is seal,
  `checkTail` and a conditional write of the new chain.
- Delos log-structured protocols (Balakrishnan et al., Facebook, SOSP 2021),
  `docs/references/papers/delos-log-structured-protocols/`: trim as the minimum of each engine's
  floor; the backup of log segments.
- LinkedIn Northguard (2025), `docs/references/articles/linkedin-northguard.md`: a segment is the
  unit of replication; segment metadata in Raft vnodes.
- LazyLog (Luo et al., UIUC, SOSP 2024), `docs/references/papers/lazylog/`: the seal of a view
  must flush every acknowledged record that is not ordered yet.
- SpecLog (Bhat et al., UIUC, OSDI 2025), `docs/references/papers/speclog/`: delivery at a
  predicted position before the global cut.

**FoundationDB.** The matchmaker register is the register of FoundationDB's
`LocalGenerationReg` (`specs/tla/cas-register/`, §J3.4). A FoundationDB commit waits for every
TLog; the journal does not, by the rule of quorum everywhere (§J5.1).

**Other papers in the tree** that the journal uses through the core: DPaxos
(`docs/references/papers/dpaxos/`, the cooperative leader handoff), Flexible Paxos
(`docs/references/papers/flexible-paxos/`, the quorum intersection behind the grid), and
protocol-aware recovery (`docs/references/papers/protocol-aware-recovery/`, CTRL, §J5.3).

**The design record.** The journal design document (a Claude Doc of the project, "paros journal
design"), the consolidated v1 plan
(`/mnt/project-files/review/journal-plan-2026-11-20/journal-v1-plan.md`) and its two reviews
(`fable-review-1.md`, `fable-review-2.md` in the same folder). These files replace them where
they disagree.
