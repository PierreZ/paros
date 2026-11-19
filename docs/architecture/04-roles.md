[← 03-8-identifiers-checkpoints-recovery-security.md](03-8-identifiers-checkpoints-recovery-security.md) · [Index](README.md) · [05-failure-model-and-zones.md →](05-failure-model-and-zones.md)

## 4. Roles

All six roles of Compartmentalized Paxos, plus the matchmakers of Matchmaker Paxos, each
placed by the cell from the counts that a person typed for a journal (rule 7, section 3.4):

| Role | Class | In paros |
|---|---|---|
| Leader (proposer) | storage | `Proposer` inside `ColocatedNode` |
| Proxy leader | stateless | `ProxyLeader`; booked per journal by `proxies` (1 by default, decision 22) and routed from the live registry |
| Acceptor, grid quorums | storage | `Acceptor`, `QuorumSystem::Majority` or `QuorumSystem::Grid` (decision 27) |
| Replica | storage | `ReplicaNode`; booked per journal by `replicas` (1 by default); serves `Read` |
| Batcher | stateless | `paros_consensus::Batcher`, `paros::roles::batcher`; booked per journal by `batchers` (1 by default); multi-writer journals only (section 2.4) |
| Unbatcher | stateless | `paros_consensus::Unbatcher`, in the batcher's process (section 2.4) |
| Matchmaker | storage | `Matchmaker`, `run_matchmaker`; one logical set per tenant, processes shared; holds the register of rule 1 for each journal of its tenant |
| Frontend (the entry role) | frontend | `paros::roles::frontend`; served by the machines that the operator gives the `frontend` role (decision 15, question 6) and named by `Resolve`; authorizes, resolves names and forwards each call to the role that serves it (section 3.5) |
| Resolver | frontend | universe level, shared by every tenant; redirects a client to its namespace's cell, never carries data (section 3.5); served beside the frontend |
| Coordinator (universe, cell, tenant) | none | not a role of a machine: any machine of the cell that proved it serves can win the term with `SetLeader`, and a stateless machine tries first (decisions 1, 7 and 15, question 5; section 3.3) |

### 4.1 How the pieces fit

Two papers, one per concern, both in `docs/references/papers/`. **Compartmentalized Paxos**
(Whittaker et al.) splits the leader's work into roles that scale on their own; **Matchmaker
Paxos** (Whittaker et al.) lets the acceptor set change without stopping. paros runs both per
journal, and the journal's leader-uuid rules (section 2) sit on top, judged at apply.

**The write path.** The leader only *sequences*: it gives each command a slot and hands it off.
A proxy leader does the Phase-2 work for that slot, so the leader's CPU and network stop being the
bottleneck. Acceptors may form acceptor partitions, so a Phase-2 quorum is a majority of one
acceptor partition and not a majority of all (#532 (acceptor partitions)).
Replicas learn chosen slots, walk the contiguous prefix and apply them; that apply is where the
journal state machine judges each `Write`, `Truncate` and `SetLeader`. Batchers and unbatchers
(multi-writer journals only, section 2.4) group many clients' writes into one slot and fan the
answers back out.

```
 clients ──► batchers ──► LEADER ──────► proxy leaders ──► acceptors (grid)
   ▲         (opt-in)    (Proposer:      (Phase 2a for      ┌────┬────┬────┐
   │                      slot = next)    one slot)         │ a1 │ a2 │ a3 │  Phase 2 quorum
   │                                         ▲              ├────┼────┼────┤  = one column
   │                                         │ 2b acks      │ a4 │ a5 │ a6 │  Phase 1 quorum
   │                                         └──────────────└────┴────┴────┘  = one row
   │                                         │ chosen(slot)
   │                                         ▼
   └──── unbatchers ◄──── replicas: walk the chosen prefix, apply in slot order
          (opt-in)        (journal state machine: leader uuid, expected_seq, first_seq)
```

**The read path.** A read never touches the leader (Compartmentalized Paxos §3.4, paros's
`QuorumRead`). The server holding the journal's replica state asks one Phase-1 quorum (a row) for
the highest slot each acceptor has voted in. Any chosen slot was voted by some acceptor in every
row, so the maximum covers everything chosen. The server waits until its applied prefix reaches
that maximum, then answers. The drawings show a full grid, where a row is a Phase-1 quorum. In
the partitioned grid of the service, Phase 1 takes F+1 of every column (section 3.4). The row
read on a full grid is safe across a change of members only with a Phase 1 that reaches each
row of the old grid (decision 31, section 2.5).

```
 client ──Read──► replica ──"highest vote?"──► one row of acceptors
                     │    ◄── max watermark w ──┘
                     │  wait until applied prefix ≥ w
 client ◄──records───┘
```

**Rule 1: seal the latest configuration** (decision 2; #1042 (retire fence rework); steps 1.6,
2.2 to 2.5). **Not proved** until the TLA+ gate of phase 2 is green: the model `config-register`
(Agreement with three ballots and a candidate that stops after any step, also with a partitioned
layout, a full grid and a successor of another size; with at most F acceptors gone, a campaign
wins in the end), whose controls (a switch before the recovered slots are held, a stored stale
belief, an abandoned successor, a store released before the switch) fail. It changes the
Matchmaker Paxos paper as paros built it, so its model comes first (`AGENTS.md`: a change to a
paper needs a TLA+ check).

**For each journal, the matchmakers hold a register: the configuration in force, and at most one
open successor.** Each configuration has an epoch, the high part of its ballot.

- **A campaign registers nothing.** A candidate reads the register. Phase 1 asks a quorum of the
  configuration in force, and of the successor when there is one; Phase 2 addresses one
  configuration.
- **A move has two events.** The **seal** is the Phase 1 of the next epoch that stops the old
  set. The **switch** is the register write that puts the successor in force. A move announces
  the successor at the matchmakers, seals the old set, copies what the old set reports, then
  switches the register.
- **The switch is the last step of a move, never a step before Phase 2.** A successor becomes the
  configuration in force only when its leader holds every recovered slot at a Phase-2 quorum of
  it (FoundationDB's order). The other order is unsafe: the review of 2026-11-16 ran TLC on
  `specs/tla/cas-register`, where the new configuration is written to the matchmakers before
  Phase 2, and `Agreement` fails in 25 steps (ballot 1 chooses a value in A, ballot 2 writes B
  to the register and stops before Phase 2, ballot 3 reads B, sees no vote and chooses a second
  value). That check is `cas_long`, and its trace is the first control of `config-register`
  (`switch_before`, step 1.6).
- **A new member withholds its ack until it holds the proof that the old set is sealed**
  (TigerBeetle's proof).
- **An open successor is always finished forward, never abandoned.** While a successor is open,
  Phase 2 writes to the successor only, and no candidate writes a third configuration. A candidate
  that finishes a successor it read announces it again at a matchmaker majority first, because
  a read can return a successor that only one matchmaker holds (`config-register`, control
  `direct`).
- **A stale belief cannot be stored**, so no election waits for a node outside these two
  configurations. A node learns its configuration from the register only (step 2.5).
- **A removed node may stop as soon as the switch is durable.** Durable means that a matchmaker
  majority holds the same switch write; a majority of registers from different writes is not enough
  (`config-register`, control `mixed`). A machine that left a configuration in force comes back
  only under a new id, with a new store (rule 5; control `rejoin`). A machine releases its store
  for a journal when the register shows a configuration in force that does not name it (rule 3,
  section 3.3). There is no retire check and no retire mark.

This deletes the retire stack (step 2.4): the registration fence scalar, the sixth leg of
`may_retire`, `retire_check.rs`, `MatchRefusal::Retired`, `RetireMark` and its incarnations,
`MatchPurpose::Rejoin`, the owner testimony and the skip of an abandoned registration (section
11 keeps them). The models `retire-fence` and `abandoned-ballot` leave CI with their rules; their
traces stay as controls of `config-register`, and the 2x2 grid of `abandoned-ballot` moves to
`config-register` first (step 1.13).

```
 candidate (ballot b, epoch e)
   │ 1. read the register ──────────► matchmakers (majority of the tenant's set)
   │ ◄──────────── C_in_force, and C_successor when one is open
   │ 2. Phase 1a(b) ────────────────► a quorum of C_in_force  (AND of C_successor, if open)
   │ ◄──────────── promises + votes    (adopt the highest-ballot vote per slot)
   │ 3. Phase 2a(b) ────────────────► one configuration, slot by slot (through proxy leaders)
   ▼
 a move A → B: announce B as successor, seal A (Phase 1 at epoch e+1), copy,
 hold every recovered slot at a Phase-2 quorum of B, then switch the register: B in force.
```

**Rule 2 at the matchmakers: sets never shrink** (#1045 (finish proposes the target); steps 1.8
and 2.1). **Not proved** until the TLA+ gate of phase 2 is green: the model `handover` (extends
`handover-gc`: only a resize record changes the size; the first write needs every new member;
the floor and the register cross the handover, also while a journal has an open successor; a
handover with at most F members down ends), whose control, the answerer rule, fails. The model
passes and its controls fail (`specs/tla/handover/`, #1097 (rework 1.8)); the code comes with
step 2.1. The model shows that the first-write rule is the size rule, not a safety rule: with a
majority, the floor and the register still cross, but the set serves with a member that holds
nothing.

- A set has a durable size, `2F+1` for each acceptor partition, and a failure never changes it.
  The size is the recorded number of members; a member that is gone does not change it.
- **The caller names the target set.** A generation handover's `Stop` carries the target set,
  and a finish proposes it again. An answer to a freeze proves only that the old set stopped; it
  never chooses the members. Every handover record asserts the size.
- A change that cannot fill the size waits and reports a typed reason.
- The first write to a new set needs every new member.
- Only a resize record changes the size (section 3.3). Rule 1 does not need the two sets to have
  the same size.

The answerer rule (`odd_answerers`, a finish that proposes the members that answered, pending
bootstraps that grow) is deleted (step 2.1).

**Matchmaker sets are generations** (#563): every matchmaking message is fenced by generation,
and so is each order of the coordinator to the matchmakers (rule 3); matchmaker quorums are
**majorities only**; the handover's decree is the shared `Proposer` + `Acceptor` over a one-slot
log (no second Paxos kernel). A handover is paged (#439): a set keeps one register per journal of
its tenant, so a `Stop` and its answers carry one page at a time.

**GC.** A configuration is forgotten once it is sealed and its successor is in force: no future
leader can need its Phase-1 quorum. The compaction floor and the GC watermark never wait on each
other.

Matchmakers are mandatory in the service (section 3.1): every journal has its tenant's set,
because reconfiguration is how a dead disk is replaced, a machine drained, a journal moved and a
quorum system changed.
