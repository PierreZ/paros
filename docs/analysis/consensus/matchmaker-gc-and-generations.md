# Matchmaker garbage collection and matchmaker-set generations, restated for paros

Design note for the three closing steps of the Matchmaker milestone (#22): configuration
garbage collection (#123, M4.5), reconfiguration under the full fault matrix (#124, M4.6)
and matchmaker-set reconfiguration (#125, M4.7), against "Matchmaker Paxos: A
Reconfigurable Consensus Protocol" (Whittaker et al., 2021), §3.4–§3.5, §4.5, §5 and
Appendices A, B and D. It sits beside `dpaxos-leader-handoff.md` and follows its shape:
what the paper says, what paros actually has, and the rule that survives the translation.

Code: `crates/paros-core/src/node/gc.rs` (the leader's GC decision),
`crates/paros-core/src/matchmaker.rs` (generations, fencing, the durable decree
acceptor), `crates/paros-core/src/matchmaker/reconfigurer.rs` (the handover state
machine), `crates/paros-core/src/single_decree.rs` (the kernel it decides with), and the
harness's wipe / retire model in `crates/paros-sim/src/world/mod.rs`.

## 1. Garbage collection: when may a configuration be forgotten?

### The paper's rule and the wrong rule

A matchmaker's registry only ever grows: every ballot `b` that ever campaigned left
`(b, C_b)` behind, and every later Phase 1 fans out to the union of all of them. The paper
lets a leader tell the matchmakers to forget configurations below a watermark `w` once one
of three scenarios holds for every log region (§3.5):

| region of the log at election time | scenario | what discharges the obligation |
| --- | --- | --- |
| slots the leader's Phase 1 found empty | 2 | nothing was chosen there below `b` |
| slots the leader re-proposes at `b` | 1 | once chosen at `b`, `C_b`'s quorum re-learns it |
| the already-chosen prefix | 3 | `f + 1` *replicas* persisted it and a Phase-2 quorum of `C_b` knows |

Appendix D names the rule that looks equivalent and is not — DPaxos's "the new
configuration is installed, therefore the old one can be deleted". The failure is simple to
state: a leader installs `C_b`, garbage-collects at once and dies before `C_b`'s members
learned the prefix `C_old` chose; the next candidate from `C_b` runs a Phase 1 that reaches
nobody who knows slot `s`, fills it with a `Noop`, and two values are chosen for one slot.

### What paros has instead of a replica tier

paros has no separate replicas: every node is proposer, acceptor and replica at once.
Scenario 3 as written therefore has nothing to attach to. What paros has is stronger for this
purpose, and it is already load-bearing elsewhere:

- A node that **learns** a slot chosen records it as its authoritative accepted record
  (`mark_chosen` → `record_accepted`), fsynced before its chosen index advances. A member of
  `C_b` whose chosen index covers `s` therefore *answers a Phase 1 for `s`* with that record,
  and the P2c chain makes it the chosen value.
- A node that **truncated** past `s` refuses the `Prepare` below its floor. That is the
  paper's acceptor-side persisted watermark and its "already chosen, recover it out of band":
  the candidate learns `s` from a snapshot, never treats it as free.

So the condition paros can honestly satisfy for the chosen prefix is:

> **a Phase-2 quorum of `C_b` reports a chosen index at or past the election fence
> `F = next_slot − 1`.**

Any future Phase-1 quorum of `C_b` intersects that set. Together with Scenarios 1 and 2 —
which paros gets for free from the existing election (`Election::recovered` re-proposes
Region 2 at `b`; the Phase-1 completion predicate over *every* configuration in `H_b`
covers Region 3) — this is the whole forgettability condition, and it is deliberately not
Scenario 3: the chosen prefix's durability is the existing chosen-index / truncation /
snapshot machinery's, and nothing new is added to make GC possible. The rule lives in
`ColocatedNode::gc_covered`; the derivation is the module doc of `node/gc.rs`.

Two more preconditions keep the "Region 2 is decided" half honest: GC waits while any
Phase-1-shaped work is open (`leader_recovery`, the CTRL `repair_probe`, `app_repair`), the
same gates that refuse a handoff.

### The protocol

```
election over H_b            leader opens GcState{fence = read fence, prior = members(H_b)}
HeartbeatAck{chosen}         every configured peer reports its chosen index each beat
covered                      quorum(C_b) at or past the fence, recovery closed
GcRequest{b}  -> M_g         re-sent each beat until acked (a lost ack only stalls it)
GcAck × quorum(M_g)          the floor is EFFECTIVE; retirable = members(H_b) \ C_b
```

- The watermark a leader raises is **its own ballot**: it registered above every
  configuration in `H_b`, and its own registration (a reconfiguration or the effective
  configuration's restatement) is what every later campaign still finds. GC therefore never
  retires the highest reconfiguration registration, which the M4.4 stale-configuration rule
  depends on.
- A matchmaker raises its watermark **durably before acking** and refuses any campaign below
  it (`MatchRefusal::BelowWatermark`), which also re-floors the refused candidate's next
  round.
- The floor is **effective only once a matchmaker quorum acked it**: every later matchmaking
  quorum intersects that set, and the *maximum* reported watermark filters `H` (#120's
  invariant 3), so every later `H` excludes what was collected. Only then does the leader
  name the retirable acceptors (`GcStep::Effective`, surfaced through `Inspect.retirable`).
  A leader deposed in between never reports; nothing runs ahead of the acks.
- The `HeartbeatAck.chosen` field is populated only on a matchmaker deployment: the plain
  Multi-Paxos wire stays byte-identical, per the *first-class and permanent* rule.

### The two floors

The **compaction floor** (`first_slot`, `Control::Truncate`) is per node and says "these
slots are chosen and their records are gone here; recover them from a snapshot". The **GC
watermark** is per matchmaker and says "these configurations will never be returned again".
The first is what makes the second safe for Region 1 — a `C_b` member that compacted past
`F` still refuses to let a candidate treat those slots as free — and neither floor ever
needs the other to move.

### Retirement is an operator act

"Removed is not shut down" stays true: a removed acceptor keeps answering Phase 1 for the
ballots it took part in until GC says nobody will ever ask again. What GC adds is the
*permission*. In simulation the workload turns it into a fact (`RETIRE`): it reads the
leader's retirable list, parks the identity in the storage world for good, and sends the
node a `Retire` RPC, which the driver honors only while the node is neither a member of the
configuration it believes in force nor the leader. A retired identity never boots again.
The audit's convergence claim excuses a retired node only if an effective floor actually
named it.

## 2. The wiped node: amnesia is retirement, not recovery

The `prob_wipe` deferral of the disk-fault stage was the last "out of scope" note in the
architecture. It closes here, but not by letting a wiped node rejoin: a node that lost its
durable promise and voted history can regress a promise it once made, and no snapshot
restores a promise. paros's answer is the same as for a retired node — **a wiped identity
never rejoins**; the acceptor set heals around it by reconfiguration.

- moonpool's `prob_wipe` stays `0`: it wipes moonpool's storage layer, which paros does
  not use (AGENTS.md, *Storage direction*). The storage world draws its own wipe coin at a
  chaotic restart on a matchmaker seed, within the same dead-node budget as a corruption
  park (a wipe is one more way to lose every copy a node holds). A wiped identity exits at
  boot, forever.
- The client's reconfiguration composer draws the successor from the *live* pool only, and a
  dead member is the first one a `replace` or `shrink` moves out — that is how the cluster
  replaces a wiped acceptor, and the storage world's copy budget (sized by the bootstrap
  floor) is what guarantees a live quorum of every configuration a run may put in force.
- A moonpool issue asks for the reboot kind to be exposed to a restarted process so a
  harness-owned disk can honor `CrashAndWipe` directly; until then the harness coin is the
  paros-side defense in depth.

## 3. Generations: the matchmaker set is itself a chosen value

The matchmakers are the source of truth for configurations, so their membership cannot be a
static fact forever — a matchmaker that dies or loses its disk would be unreplaceable. §5 of
the paper replaces the set by a stop-the-world handover, which is acceptable because
matchmakers are idle whenever a leader is stable. paros restates it as follows.

### Vocabulary

| term | paros | paper |
| --- | --- | --- |
| generation `g` | `MatchmakerGeneration(u64)` on every matchmaker message | epoch |
| set `M_g` | `MatchmakerSet { generation, members }` | matchmaker configuration |
| phase | `Fresh`, `Inactive` (a spare), `Active`, `Stopped` (frozen) | — |
| successor link | `MatchmakerHardState::successor` | — |
| pending bootstrap | `PendingBootstrap { set, gc_watermark, history }` | bootstrapped state |

### Fencing

Every matchmaking message carries a generation: `MatchRequest`, `MatchReply`, `GcRequest`,
`GcAck` and every `ReconfigureRequest`. A matchmaker answers only its **active** generation.
Anything else is refused with what it knows — `Stopped { successor }` (it was frozen; here is
who came next, if chosen), `Generation { current }` (a stale or future generation), or
`Inactive` (a spare that holds nothing) — and never served. The node side folds only replies
from its current set and generation, and learns a newer set from any refusal that names one
(`MatchStep::Superseded`), re-campaigning under it. A frozen matchmaker stays alive: it keeps
answering `Stop`, votes in the decree, and points late proposers at its successor —
"stopped" is a protocol freeze, not a process death.

### The handover, as an explicit state machine

`MatchmakerReconfigurer` holds no durable state and is driven by the provider-generic node
driver (`paros::run_node`), so the same code runs in production and in simulation:

```
Idle
  | start(current = M_g, target)
  v
Stopping        Stop -> M_g; wait for quorum(M_g) StopAck (a StopAck naming a successor
  |             means someone already won: adopt it)
  v
Bootstrapping   reconstruct: w = max watermark, H = union of histories >= w;
  |             Bootstrap{g+1, target, w, H} -> every member of the target;
  |             wait for ALL of them (a set is chosen only once fully initialized)
  v
Deciding        single-decree Paxos over M_g as acceptors: Phase 1 at a fresh ballot,
  |             P2c adopts a competing proposal already voted, Phase 2 with the
  |             selected value; a Nack reopens higher
  v
Publishing      Chosen{g, successor} -> M_g ∪ successor; done once a quorum of each
  |             learned it (stragglers are told again by any node that meets them)
  v
Idle
```

Reconstruction is Appendix B: every completed registration of generation `g` reached a
quorum of `M_g`, which intersects the frozen quorum, so the union above the maximum
watermark is complete. The audit asserts exactly that (`reconfigurer_step`: every
registration a matchmaker quorum holds for `g` at or above the reconstructed watermark
appears in the bootstrap history). A bootstrapped set becomes authoritative only at
`Chosen`: `M_g` members record the successor link (the discovery chain for late proposers),
`M_{g+1}` members activate their pending bootstrap as their registry
(`MatchmakerWriteOp::InstallRegistry`).

### Why the single-decree kernel is still beside `ColocatedNode`, and what replaces it

The successor is decided by classic single-decree Paxos, and the question was whether to
carve that out of `ColocatedNode`. When the handover landed, `ColocatedNode` *was* Multi-Paxos in one
piece — one `Prepare` per ballot over a log suffix, paged `Promise`s, the CTRL tri-state,
the cross-configuration completion predicate, the no-op gap fill, the read fence, the
bounded leader recovery — and extracting a single decree from that would have re-derived
the proven core around a kernel it never had. So the decree is a **separate, tiny kernel**
(`single_decree.rs`: `DecreeAcceptor<V>` and `DecreeProposer<A, V>`) whose whole content is
the value-selection rule (adopt the highest-ballot vote, else propose your own), unit-tested
against the dueling-proposer case; the matchmaker embeds the acceptor half in its durable
scalars and the reconfigurer drives the proposer half. Leadership, retries and transport
stay with the caller.

That reason no longer holds as stated. The core has since been decomposed into roles
(`acceptor.rs`, `proposer.rs`, `replica.rs`, with `membership.rs` as the quorum boundary;
AGENTS.md, *The core is composable*), and `ColocatedNode` is the wiring that colocates them.
A single decree is then the same `Proposer` + `Acceptor` over a one-slot log — the
reconfigurer would drive the shared proposer against the matchmakers' shared acceptor, with
no second value-selection rule to keep in agreement. Moving the kernel onto those roles is
the next phase of that plan; until it lands, `single_decree.rs` stays the decree the
handover runs, and the majority-only rule below binds both.

### Quorum model: majorities only

Matchmaker Paxos generalizes matchmaker quorums to arbitrary quorum systems (§4). paros does
not: `MatchmakerSet::quorum_size` is a majority, every matchmaker-side quorum (registration,
GC ack, freeze, decree, publication) uses it, and `DecreeProposer` takes the acceptor *set*
and derives the same majority itself — it cannot be constructed with a quorum that fails to
intersect or exceeds its acceptors, and a reply from an identity outside the set never counts.
The handover's safety argument below is made under that model only; a flexible matchmaker
quorum system would have to replace the set's rule and the kernel together.

### The decree ballot must be unique across incarnations

The reconfigurer holds no durable state, and that includes its decree round: a node that
reboots comes back with a fresh reconfigurer counting from round 1 again. Its earlier
incarnation may have taken a Phase-1 quorum at `(1, N)` and had `V1` accepted at a minority;
the new one, proposing `V2` for the same generation, reaches a different promise quorum at
`(1, N)` and sends `Accept(1, N, V2)` to a matchmaker that already voted `V1` at that very
ballot. The core asserts there (one ballot, one value — crash beats corruption); without the
assert a later proposer's Phase 1 would see two values at one ballot and could choose the wrong
one. The handover model checker found this on seed 103.

The rule: every `Stopped` reply carries the matchmaker's current decree promise, and the
reconfigurer opens its decree at `max(own round, max promise over the stop quorum) + 1`. That
is enough because a value can only be accepted at a ballot that first held a *promise quorum*,
and every promise quorum of `M_g` intersects the stop quorum — so the floor sits at or above
every ballot the node's earlier incarnations could have had a value accepted at. Retries within
one incarnation keep the counter; a Nack still reopens above the refusing promise.

### Invariant 1, and what proved it load-bearing

> **At most one matchmaker set is authoritative per generation.**

Two reconfigurers may start concurrently with incompatible targets (two clients, two
nodes). Without the decree, each would bootstrap and publish its own successor for
`g + 1`, and two matchmaker sets would both claim generation `g + 1` — two registries,
two histories, and a campaign that completes on either. With it, the loser's Phase 1 sees
the winner's vote and proposes the winner's value; `Chosen` is only ever sent from
`Publishing`, which is entered only on a Phase-2 quorum. The audit binds each generation to
the first set it sees chosen and asserts every later report agrees.

### Liveness: a frozen generation must always be finishable

The reconfigurer holds no durable state, so the node driving a handover can die after the
freeze and leave generation `g` stopped with no successor — a cluster that can elect no
leader, because every campaign is *matchmaking first*. Three rules, all found by the sweep
(the raw hunt's livelock seeds are cited in the landing commit), keep that state finishable:

- **Any node that meets `Stopped { successor: None }` finishes the handover**
  (`MatchmakerReconfigurer::finish`). It proposes **the members that answered the freeze** —
  the only liveness it can vouch for — so a member that died since (a lost registry, a machine
  gone) never blocks the bootstrap, which waits for *every* proposed member. The decree still
  adopts whatever an earlier reconfigurer got voted, and an operator can grow the set back.
- **A phase that makes no progress is abandoned by the driver** after
  `DriverTunables::reconfigure_timeout_elections` election timeouts. The core keeps the stall
  clock (`MatchmakerReconfigurer::tick` / `stalled_for`) and exposes `abandon`; how long is
  long enough is driver policy, paced in the driver's own units and carried as a tunable the
  simulation buggifies per seed rather than a constant. Abandoning is
  always safe — the freeze and the bootstrap are idempotent, the votes are durable — and it is
  what keeps a dead proposed member from holding the `busy` refusal for the rest of a run.
- **A preempted decree backs off before reopening.** Every node that met the same frozen
  generation runs a finisher, and six finishers reopening their preempted decree on the same
  re-send cadence preempt each other forever (the sweep drove one seed's decree past round 800
  with no successor). The reconfigurer's contract leaves the pacing to the driver, which waits a
  jittered draw of ticks after a `Preempted` step — the same symmetry break the election
  timeout's jitter provides.

### Failure-driven replacement

There is deliberately no matchmaker-specific in-place disk repair. A matchmaker whose
durable state is unusable is fenced out of the next generation and a fresh one bootstrapped
in its place from the surviving quorum's frozen registries. In simulation the world loses one
matchmaker's registry for good at a restart (once per run, only where the bootstrap set
holds three or more members, so a quorum survives without it); the workload's
`RECONFIGURE_MATCHMAKERS` composer draws successors from the live matchmaker pool, replacing
or dropping the lost one first, exactly as it does for a wiped acceptor.

## 4. How the harness proves it

All of this is one dimension of the registered campaign, never a scenario of its own:

- **Ops.** `RECONFIGURE_MATCHMAKERS = 12` and `RETIRE = 13` join the chain alphabet with
  their own weight knobs; the acceptor `RECONFIGURE` composer now heals around dead
  identities.
- **Faults.** The world's wipe coin and matchmaker-loss coin; the reply-drop hooks for
  `GcAck`, `MatchmakerReconfigure`, `ReconfigureMatchmakers` and `Retire`; the resend-skip
  hooks `skip_gc_resend` and `skip_reconfigurer_resend`.
- **Oracles.** The GC request is licensed by a quorum holding the fence (the audit
  re-derives coverage from the persisted records); a campaign's watermark is at or above the
  floor effective when it opened; a matchmaker's ack reports its durable floor; the
  retirable list is `members(H_b) \ C_b`; a node that stays down retired only after an
  effective floor named it; one set per generation; reconstruction completeness; a frozen
  matchmaker never registers again; a client-visible matchmaker reconfiguration is refused
  on a plain deployment.
- **Gates.** `gc: a leader's floor becomes effective at a matchmaker quorum` and
  `generation: a matchmaker-set handover completes` are `sometimes`; every cause (a knob
  extreme, a coin, an operation drawn) is a `reachable`.

The red→green results for the two bug classes — the wrong GC rule and the decree-less
handover — are recorded in the commit messages that landed them, with the witness seeds
cited there and nowhere else (AGENTS.md, *Pinned seeds are not a regression mechanism*).
