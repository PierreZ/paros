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
| Resolver | stateless | universe level, shared by every tenant; redirects a client to its tenant's cell, never carries data (section 3.5). Since #233 every `parosd` serves it beside its frontend (`ResolveEntry`); a pool per region is the machines a client lists |
| Coordinator (universe, cell, tenant) | stateless, or an initial member at bootstrap | to build: the election library over a multi-writer journal (#240) |

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

**A node retires only when no registration above the floor names it** (decided on 2026-11-11,
#1002 (retire fence gaps), option (b)). A registration stays in the matchmakers' histories
whether or not its campaign went on, and every later Phase 1 needs a quorum of its
configuration. So a node keeps a **registration fence**: the highest registration it knows
names it. It is durable (a node scalar beside the promise, written before the registration is
sent and read back at boot), so a restart does not forget the node's own registration. Before a
node retires, it asks a matchmaker quorum of the generation in force which registrations name
it (a membership probe, which registers nothing); each answer raises the fence. A node that a
registration above the floor names stays up until a later leader raises the floor above it, and
an operator's `Retire` is refused `unchecked` until the check is done. The plain Multi-Paxos
path does not change: no scalar is written, no message is sent and no round trip is added. The
cost is one durable scalar and one matchmaker round trip for each retirement. Option (a),
removing a dead belief from the histories, was not chosen.

**Every matchmaker records the retirement** (decided on 2026-11-12, #1002 (retire fence gaps),
the TLA+ model of PR #1021 and its variants in PR #1024 (quorum retire rule with pruning)). A
quorum check alone is not enough: a slow campaign's registration can reach a matchmaker outside
the quorum after the check, and every later Phase 1 then needs the retired node. So the check
asks **every** matchmaker of the set in force. A matchmaker that holds no registration at or
above the floor naming the node records the node as **retired** for the journal, durably,
before it answers, and from then on refuses every registration whose configuration names it
(`MatchRefusal::Retired`). The node retires only once every matchmaker of one generation
agreed; agreements of an earlier generation do not count. The retired nodes cross a matchmaker
handover as a union, so a successor set refuses the same registrations. The costs: a
retirement waits while a matchmaker of the set is down (a lost matchmaker is replaced by a
handover first), and a node that asked is not named again in that journal until it rejoins.
**A retired node rejoins** (decided on 2026-11-12, #1037 (rejoin), the `rejoin_*` checks of
`specs/tla/retire-fence`). Placement can book a node again on a journal it retired from, and
the journal then restarts from the kept store. The node keeps a durable retire mark per
journal: its incarnation, and whether every matchmaker let it retire there. A boot that reads
a checked mark moves to the next incarnation, writes it durably, and asks every matchmaker to
drop its retirement record (`MatchPurpose::Rejoin`). A journal that placement books again
while it still runs on the node (a released slot that lingers, or a tenant that moved out and
came back) does the same with no reboot (`ColocatedNode::rejoin`). The node does not know if
some matchmaker took a check that did not finish, so it rejoins in both cases. Without this, a
reconfiguration onto the node is refused `Retired` for good, and the journal has no leader
(replay-recipe `2684408044022300164 1194217:12375250690498203620`). A quorum that took the rejoin is enough
(Pierre, 2026-11-12): a candidate skips the `Retired` refusal of a matchmaker the rejoin has not
reached yet, the node asks that one again in the background, and a later retirement check at the
new incarnation takes the rejoin's place. A retirement check carries the
incarnation, and a matchmaker that holds a later one refuses it (`MatchRefusal::Rejoined`).
Without the incarnation, a late check of the old incarnation can record the node retired
again after the rejoin, and a later Phase 1 then needs a node no matchmaker lets register.
The records cross a handover node by node: the later incarnation wins. A quorum
variant (a quorum records the retirement, and a candidate drops a registration that a quorum
reports retired) is safe too, but it moves the wait from the retirement to the election: with
one matchmaker down, a candidate that hears the late registration may not be able to drop it.

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
