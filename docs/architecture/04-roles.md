[← 03-8-identifiers-checkpoints-recovery-security.md](03-8-identifiers-checkpoints-recovery-security.md) · [Index](README.md) · [05-failure-model-and-zones.md →](05-failure-model-and-zones.md)

## 4. Roles

All six roles of Compartmentalized Paxos, plus the matchmakers of Matchmaker Paxos, each
scalable independently by the tenant coordinator (proxy leaders, batchers and replicas per
journal, decided on 2026-10-28, #753 (per-journal proxies)):

| Role | Class | In paros |
|---|---|---|
| Leader (proposer) | storage | `Proposer` inside `ColocatedNode` |
| Proxy leader | stateless | `ProxyLeader`, `run_proxy`; booked per journal by `proxies=` (0 means in the leader's process; decided on 2026-10-28, #753 (per-journal proxies)) |
| Acceptor, grid quorums | storage | `Acceptor`, `QuorumSystem::Grid` |
| Replica | storage | `ReplicaNode`, `run_replica`; serves `Read` |
| Batcher | stateless | `paros_core::Batcher`, `paros::roles::batcher` (every machine serves it, `run_batcher` stand-alone); booked per journal by `batchers=` (0 means in the leader's process; per tenant until 2026-10-28, #145, #604, then per journal, #753 (per-journal proxies)); multi-writer journals only (section 2.4) |
| Unbatcher | stateless | `paros_core::Unbatcher`, in the batcher's process (section 2.4) |
| Matchmaker | storage | `Matchmaker`, `run_matchmaker`; one logical set per tenant, processes shared |
| Frontend (the entry role) | stateless | `paros::roles::frontend` (every `parosd` serves it, `run_frontend` stand-alone for an embedder; #723 (frontend as a parosd role), decided on 2026-10-27); booked per tenant by `frontends=` (#604) and named by `Resolve`; authorizes, resolves names and forwards each call to the role that serves it (section 3.5). To build: one pool per `(tenant, region)` in a multi-region cell (section 3.4) |
| Resolver | stateless | to build in M12: universe level, one pool per region shared by every tenant; redirects a client to its tenant's cell, never carries data (section 3.5) |
| Coordinator (universe, cell, tenant) | stateless, or a founding member at bootstrap | to build: the election library over a multi-writer journal (#240) |

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
that maximum, then answers.

```
 client ──Read──► replica ──"highest vote?"──► one row of acceptors
                     │    ◄── max watermark w ──┘
                     │  wait until applied prefix ≥ w
 client ◄──records───┘
```

**Matchmaking, then Phase 1.** A new leader (or a reconfiguration, which is just a new ballot)
first registers its ballot `b` and the configuration `C_b` it will use at a quorum of the
tenant's matchmakers. They answer with the earlier configurations still in force, `H_b`. The
leader then runs Phase 1 against a quorum of **every** configuration in `H_b`, because any of them
may hold a chosen value, and Phase 2 against `C_b` alone. Once no future leader can need an old
configuration, the matchmakers garbage-collect it, and its acceptors can retire (`may_retire`).

```
 candidate (ballot b, config C_b)
   │ 1. MatchA(b, C_b) ─────────────► matchmakers (majority of the tenant's set)
   │ ◄──────────── H_b = { C_x, C_y }  configurations of lower ballots, not yet GC'd
   │ 2. Phase 1a(b) ────────────────► a quorum of C_x  AND  a quorum of C_y
   │ ◄──────────── promises + votes    (adopt the highest-ballot vote per slot)
   │ 3. Phase 2a(b) ────────────────► C_b only, slot by slot (through proxy leaders)
   ▼
 leading in C_b.  Reconfigure = steps 1–3 again with C_new at a higher ballot.
 GC: when every slot below a floor is chosen in C_b, older configs are forgotten.
```

Matchmakers are mandatory in the service (section 3.1): every journal has its tenant's set,
because reconfiguration is how a dead disk is replaced, a machine drained, a journal moved and a
quorum system changed.
