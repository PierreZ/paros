# references — index & map

External source material for `paros` (a learning **sans-IO Multi-Paxos** library in Rust): papers
we study, and analyses of external consensus codebases. This file is the map; read it first.

## Conventions

- `papers/<name>/` — a `paper.pdf` plus a markdown `transcript.md`. **The transcript is the
  searchable, citable digest** (header metadata → full abstract → section-by-section condensation →
  a "Why this matters for paros" section). Read the transcript first; open the PDF for figures/proofs.
- `<repo>/` — an analysis of an external *code* implementation (not a paper). Each follows the
  house style **(a) verbatim code + `file:line` → (b) principle → (c) maps-to-sans-IO**.
- Our own design notes (not tied to a specific external source) live one level up in
  [`../analysis/`](../analysis/), e.g. `../analysis/go-raft/`.
- When adding a paper, keep the `paper.pdf` + `transcript.md` pair and cross-link related entries.

## Papers

### Foundations (Lamport)
- [`papers/paxos-made-simple/`](papers/paxos-made-simple/) — Lamport. Single-decree Synod + the
  state-machine approach. The algorithmic kernel everything else builds on.
- [`papers/paxos-made-moderately-complex/`](papers/paxos-made-moderately-complex/) — van Renesse &
  Altinbuken. MultiPaxos as you'd actually engineer it (replicas, leaders, slots, reconfiguration).
- [`papers/paxos-made-live/`](papers/paxos-made-live/) — Chandra, Griesemer, Redstone (Google). The
  engineering reality: disk faults, master leases, snapshots, testing — what the textbook omits.

### Modern theory (Howard)
- [`papers/flexible-paxos/`](papers/flexible-paxos/) — Howard, Malkhi, Spiegelman. Quorum
  intersection is required only *across* phases, not within → Q1/Q2 flexible (and grid) quorums.
- [`papers/generalised-distributed-consensus/`](papers/generalised-distributed-consensus/) — Howard,
  Mortier. Consensus over write-once registers + four correctness rules; Paxos/Fast Paxos as
  instances. The cleanest mental model for a per-slot acceptor log.
- [`papers/paxos-vs-raft/`](papers/paxos-vs-raft/) — Howard, Mortier. The two differ *only* in
  leader election. Pairs with the etcd-raft analysis; includes a persistent-vs-volatile state checklist.

### Variants & scaling (Whittaker et al.)
- [`papers/matchmaker-paxos/`](papers/matchmaker-paxos/) — Whittaker et al. Safe reconfiguration of
  the acceptor set via per-round configuration registries (matchmakers).
- [`papers/scaling-rsm-compartmentalization/`](papers/scaling-rsm-compartmentalization/) — Whittaker
  et al. Decouple the leader into independent roles (batchers, proxy leaders, …) for throughput.

### Storage faults & recovery
- [`papers/protocol-aware-recovery/`](papers/protocol-aware-recovery/) — Alagappan, Ganesan et al.
  (CTRL/PAR). Recovering correctly from *corrupted* storage, not just crashes — the durability edge cases.

## Code implementation analyses

- [`frankenpaxos/`](frankenpaxos/) — Whittaker's Scala research codebase. Per-slot MultiPaxos, the
  compartmentalization roles, matchmaker reconfiguration, and (key for us) a **deterministic
  simulator**. Start at [`frankenpaxos/README.md`](frankenpaxos/README.md). Actor/transport-based,
  *not* sans-IO — read for protocol content, not architecture.
- [`ceph/`](ceph/) — [`ceph/mon-paxos-patterns.md`](ceph/mon-paxos-patterns.md). Ceph's production
  monitor Paxos: the **durability/persistence boundary**, recovery of uncommitted values, and
  **lease-based reads**. Sequential single-decree + I/O-coupled — a deliberate contrast to paros.
- [`foundationdb/`](foundationdb/): [`foundationdb/cluster-controller-election.md`](foundationdb/cluster-controller-election.md).
  FoundationDB's **Cluster Controller leader election**: a register-based quorum election (*not*
  Paxos) with randomized ids plus exponential backoff (anti-dueling), a heartbeat **lease** with
  step-down on lost quorum, and majority-based discovery. The production reference for Stage 3's
  tick-driven randomized election; election is **separated** from the data plane (contrast paros's
  unified ballot core).

See also the sibling sans-IO core model:
[`../analysis/go-raft/etcd-raft-sans-io-patterns.md`](../analysis/go-raft/etcd-raft-sans-io-patterns.md).

## Suggested reading order (for the sans-IO Multi-Paxos goal)

1. **Kernel** — `paxos-made-simple`, then `paxos-made-moderately-complex` (single-decree → slot log).
2. **Sans-IO shape** — `../analysis/go-raft/` (the architecture paros wants), then
   `frankenpaxos/01-framework-and-simulation.md` (deterministic simulation as the default test mode).
3. **The core** — `frankenpaxos/02`–`03` (single-decree + slot-log), with
   `generalised-distributed-consensus` for the write-once-register intuition.
4. **Quorums** — `flexible-paxos` (Q1/Q2 split; the theory behind frankenpaxos's grid quorums).
5. **Durability** — `ceph/mon-paxos-patterns.md` + `paxos-made-live` (what to persist; the
   apply-with-commit pattern) and `protocol-aware-recovery` for corruption edge cases.
6. **Later / horizons**: `paxos-vs-raft` (election trade-offs) + `foundationdb/` (production
   leader-election dynamics), `matchmaker-paxos` +
   `frankenpaxos/05` (reconfiguration), `scaling-rsm-compartmentalization` + `frankenpaxos/04`
   (throughput). Deferred past v1.
