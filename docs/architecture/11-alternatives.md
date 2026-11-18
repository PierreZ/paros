[← 10-sources.md](10-sources.md) · [Index](README.md)

## 11. Alternatives considered

- **Recovering a control quorum by surgery.** Kafka KRaft is adding an override-voters flag to
  re-bootstrap a controller quorum that lost its majority, with data loss; ScyllaDB has a recovery
  procedure where an operator picks a recovery leader by host id. paros needs neither, because its
  control journals are rebuilt from below (section 3.10).
- **One cell playing both roles.** FDB's metacluster made a cluster exactly one of standalone,
  management or data, never two. paros co-locates the universe tenant with tenants in one cell because the universe tenant is a
  tenant with its own coordinator and quorums, which FDB could not do (its management data lived
  in the system keyspace). Moving the universe tenant to a dedicated cell is the escape hatch.
- **Multiple cells from day one.** The AWS guidance recommends multiple cells and migration from
  day one. paros runs the universe machinery from day one with one cell and defers the second cell
  to M12; the fields of section 3.7 keep that deferral free of protocol changes.
- **Checkpoints as local snapshot files per replica** (Kafka KIP-630, Redpanda): rejected,
  coordinators are stateless. KIP-630 rejected snapshots in the log for bandwidth, since the
  leader replicates them to every replica; that argument is weak for paros, whose control state is
  small. Key-based log compaction is rejected for KIP-630's reasons (compaction drops deletion
  markers on a timer, so replicas can diverge; metadata is mostly events) and because `paros-core`
  would have to understand keys. Corfu-style per-entry trim plus copying live entries forward is
  kept as the future answer if write amplification is ever measured as the bottleneck, since it
  changes `paros-core`'s read semantics; the decision is #227.
- **A `pinned` flag per tenant** (2026-10-04, dropped the same day). A creator's pin would block
  rolling a cell out by evacuation; the only tenant that must stay is a cell tenant, and its
  `paros/movable:false` tag, which only paros writes, says so (section 3.7).
- **A provisioning step that names the seeds to each other**, a cluster file, gossip discovery and
  the frontend as the rendezvous: #216.
- **Seeds, a join list and a rendezvous name** (2026-10-02 to 2026-10-09, #216). Every machine
  carried the seed list and `init` had to target a seed; being a seed meant nothing after
  formation, and "seed" collided with the simulation's seed. Replaced by the machines that
  `cell init` lists, admin-admitted machines and cells, and entry names (section 3.1).
- **The `(generation, owner)` pair** (M7, #204; replaced on 2026-10-04). Two fields where one
  fence suffices, and an owner id the caller chose, so two processes could share it and both pass
  the owner check once they read the public generation. A per-term random leader uuid is
  Brooker's final MemoryDB API and cannot be shared by accident; the term counter stays, hidden
  in the core.
- **Refusing a reinstated leader uuid** (2026-10-09, #241). Remembering every uuid that ever led
  (a `term → uuid` map carried with the trim point) would make `A → B → A` impossible even for a
  misbehaving client. Rejected: `SetLeader` names both uuids, so the client that reinstates one
  does it on purpose, the state each trim point carries would grow with every term, and the
  simulation shows the journal's guarantees hold through it (section 2.3).
- **A lease in the journal** (MemoryDB's lease-fenced writes). Rejected: a lease fence depends
  on clocks and pauses; the leader uuid fences without either. A coordinator's term needs no
  lease: its own writes renew it, and only an idle cell or universe coordinator writes a small
  renewal record (decision 19, section 3.3).
- **JWT** for frontend tokens (chosen on 2026-10-04 morning, replaced the same day, #245).
  Roles become ad-hoc claims checked by hand, a token returned at tenant creation needs a signing
  key at the frontend or a call to an external issuer, and a holder cannot narrow its own token.
  Biscuit gives roles as Datalog facts, attenuation without the root key (tenant creation, users
  narrowing their tokens offline) and offline minting. JWT stays the way to plug an external
  identity provider in later, as a second `Authz` implementation or a token exchange.
- **Well-known system ids** (universe tenant `1`, cell tenant `2`, every control journal `1`,
  `0..=255` reserved; decided on 2026-10-02, reversed on 2026-10-04). They let a component find a
  control journal without asking, but every component must then agree on the convention forever,
  the cell tenant's id repeats in every cell (unique only within its cell), a rebuilt control
  journal reuses its predecessor's id, and the reserved range is a second id space every check has
  to special-case. Random ids recorded where they are created, and learned from any machine of
  the cell, cost one `Inspect` at bootstrap.
- **A single entry role** (rejected on 2026-10-07, #233): a per-tenant role cannot find the cell
  of a tenant it does not serve, so the first hop needs a universe-level answerer, the resolver.
- **A resolver folding every cell's registry** (rejected on 2026-10-07, #233): one hop instead of
  two, but the resolver is no longer thin, no cell stays statically stable without it, and it makes
  every cell depend on a universe-level role.
- **Chain replication inside a region** (rejected on 2026-10-07, #215), the variant DSQL's journal
  runs across AZs: paros keeps quorum writes everywhere, because a slow or dead acceptor costs
  nothing until reconfiguration, where a chain stalls until its membership is changed.
- **Cell-scoped integer tags** (rejected on 2026-10-07, #215): not comparable across cells during
  a move (section 5).
- **Vote-only witnesses** (rejected on 2026-10-07, #253): a vote without bytes is a guaranteed `faulty`
  from Phase 1's view and contradicts "a server answers only from records it holds" (section 2.5).
- **Two regional cells replicating to each other** (rejected on 2026-10-07, #253): a dependency
  across cells, and a tenant lives in exactly one cell (section 3.7).
- **Lease reads** (rejected on 2026-10-07, #253): paros enforces no lease (section 2.3).
- **DPaxos's expanding quorums and leader zones** (rejected on 2026-10-07): a second Paxos kernel,
  and leader-zone quorums contradict the zone rule of section 5; Matchmaker Paxos already expands
  quorums with the intent durable before Phase 1.
- **WPaxos's joint reconfiguration** (rejected on 2026-10-07): matchmakers reconfigure
  (section 4.1).
- **Per-tenant pools of proxy leaders and batchers** (2026-10-04 to 2026-10-28, #193; per
  `(tenant, region)` from 2026-10-07, #253; sized by the tenant counts `proxy_leaders=` and
  `batchers=` of #604 (role counts)). Replaced by per-journal counts (#753 (per-journal
  proxies), section 3.4): the core already holds `proxy_count` per journal, a journal's proxy
  demand comes from its own fan-out, and a weightless stateless booking keeps idle journals
  from filling the cell, the reason the pools existed.
- **`failures-per-partition` and `partitions` inside `Durability`** (2026-10-14 to 2026-10-28,
  #523 (durability names)). Replaced by `--survive machines=,zones=,regions=` (#748 (survive))
  and a separate scale count `partitions` (#755 (partitions leave durability)). The user typed a
  protocol number, not the failure to survive, and zone survival was dropped without a word over
  one or two zones. The three words of section 3.4 then replaced the one `--survive` word
  (decision 21).
- **A throughput target** (for example `--writes 50k/s`), rejected on 2026-10-28: it needs a
  cost model paros does not have, and the simulation cannot prove it.
- **A per-journal broadcast mode** (`--broadcast leader|pool`), rejected on 2026-10-28: the
  typed counts of section 3.4 already say where each role runs.
- **Hierarchical `Zoned` quorums** (deferred on 2026-10-07, #253): a later `QuorumSystem` variant once the
  simulation runs nine acceptors (section 5).

### 11.1 Choices that lost in the rework (2026-11-15 to 2026-11-17)

The rework plan of 2026-11-15 (decisions 1 to 35, section 1.1) chose between these options.
Each entry names the decision that rejected it.

**Authority and control planes**

- **Keep the election journal** (decision 1). Today's two layers stay: a coordinator wins an
  election on its own journal, then claims the control journal. A term then needs two healthy
  journals, and the election journal is on the path of each term. Replaced by a term won with
  `SetLeader(new, old)` on the control journal, with no read (rule 3).
- **Bundle the coordinator with the Paxos leader of the control journal** (proposed and dropped
  on 2026-11-15, decision 1). The coordinator moves with each leader change of the journal, and
  the control plane gets knowledge of the core. The coordinator is a client of the journal that
  holds the leader uuid, decoupled from the Paxos leader.
- **A separate election library** (decision 13). Two mechanisms for one problem. The term of the
  control-plane kit is the public election API, and the library is deleted.
- **Four levels of coordinators, or one cell actor for all** (decision 6). The goal had four
  levels and the code had one actor with a booked slot that ran nothing. Three small control
  planes of one kit (tenant, cell, universe), hosted in one process, replace both.
- **A list of coordinator candidates, or a coordinator role** (decisions 7 and 15, question 5).
  Any machine that proved it serves can be a candidate, and a stateless machine tries first.
- **Idle renewals at every level** (decision 19). An idle tenant would cost one write for each
  term. The tenant levels run in the cell's process, so the cell's renewal proves them alive.
- **Admin commands that claim a control journal.** The claim deposes the coordinator. An admin
  command is a request to the coordinator (step 3.1).

**Configurations and matchmakers**

- **Registrations in the matchmakers, the retire fence stack, owner testimony and the pruning of
  dead beliefs** (decision 2). Six mechanisms added in eight days, each for a red of the one
  before. Replaced by the register with the configuration in force and at most one successor,
  sealed by a Phase 1 of the next epoch and switched last (rule 1).
- **A register write before Phase 2 of the successor** (the review of 2026-11-16, finding 1). The
  `cas-register` trace of 25 steps shows a register that takes force before the successor holds
  the recovered values. It is the first control of `config-register` (step 1.6).
- **TigerBeetle's recover step with the same id** (decision 4). paros has reconfiguration and
  TigerBeetle does not. A machine that lost its disk takes a new id and is admitted by
  credential.
- **TigerBeetle's round-robin primary** (rule 6). paros changes membership, so `term mod n` has
  no fixed `n`. Pre-vote is the paros form of TigerBeetle's exit quorum.
- **The members that answered the freeze become the set** (rule 2). A failure shrank a set. A
  set has a durable size, the caller names its members, and only a resize record changes the
  size.

**Machines and addresses**

- **A new address written through the journal the machine serves** (decision 8). The machine
  that moved cannot reach the quorum that would record its address. An address is a hint,
  announced at boot and learned from proven senders.
- **A gossip protocol** (decision 8). The boot announcement to entry names and saved peers is
  enough, and the doctrine has no gossip.
- **No operator command for a gone machine** (decision 24). With no spare machine, nobody could
  say that a machine will not return. **A `FORCE` word that skips the safety check**
  (FoundationDB has one; decision 24): the command keeps its check, and paros adds no such word.

**Journal configuration**

- **The cell lowers a survive number by itself** (decision 17). A cell of three machines that
  loses one for good would cut its sets to one member. The sets keep their size, report "cannot
  place" and heal when a machine joins. FoundationDB never changes its redundancy mode by itself.
- **Journals follow the cell, up only** (decision 18; Pierre tapped it and took it back). A
  journal would change with no command. Only a `configure` changes a journal.
- **A create that takes the survive number of the cell** (decision 15, replaced by decision 18).
  paros never guesses a number: the survive word is required.
- **Three parts in one survive word** (`--survive machines=,zones=,regions=`, decision 21).
  Replaced by three words, each a plain count.
- **The stateless roles inside the acceptors by default, or every word required** (decision 22).
  A create gets 1 partition, and 1 proxy, 1 batcher and 1 replica apart from the acceptors.
- **Only the tenant admin, or only the cell operator, may send a `configure`** (decision 23).
  Both may.
- **`--layout` presets and a tenant default** (decision 15, question 3). 34 settings gave 1,152
  kinds of journal for each cell, of which the simulation could place 360 at most. The typed
  counts replace them.
- **Delete `init --survive` and `cell configure --control-plane`** (option B of the configuration
  study, not asked). They stay for the cell's own control journals.
- **A frontend count for each tenant** (decision 15, question 6). The frontend is a role that the
  operator gives to a machine.

**Library shapes in the service**

- **Flexible quorums as a library of `paros-core`** (decision 15, question 2 (b), replaced by
  decision 27). Flexible quorums leave the core too; the majority and the grid stay.
- **Plain Multi-Paxos in the service** (decision 15, question 2 (c)). Each journal of the
  service reconfigures and has the split of roles. Plain Multi-Paxos stays a library shape.
- **The batched store** (decision 15, question 4). Each store uses the ordered protocol, with two
  syncs for each commit. The batched store can come back as a measured change if the benchmark
  shows the cost of the second sync.

**Reads, trust and memory**

- **Check the possible stale read later** (decision 29). The unit test was red, so the test and
  the fix landed first (PR #1074 (fence a leaderless read on the promise)).
- **A refusal with a counter in place of each assertion on a peer message, or no trust rule
  now** (decision 30). The transport proves the sender and the bytes, and a role trusts a correct
  peer (section 3.11).
- **Refuse a change of members on a full grid now, or delete the row read and the full grid**
  (decision 31). The grid is proved in TLA+ first (step 1.13), and the choice follows the trace.
- **The bound on memory of the retained log inside the rework, or no memory work** (decision 32).
  The shared bytes and the one map are in step 1.12; the bound is one issue after the rework.

**Simulation and process**

- **A small number of named excuses for real Paxos limits (target under 10)** (decision 16). The
  target is zero: the simulator refuses first each fault the cell does not promise to survive.
- **Count the known reds of main, make main green first, Pierre's word for each red PR, or no
  random sweep on a PR** (decision 33). The simulation is off in CI until the cell heals itself.
- **A list of open liveness causes in the tree that lets a red pass in CI, with a heal
  scorecard** (decision 33 of 2026-11-17). Pierre replaced it on 2026-11-18: the simulation
  comes back in CI only when the mechanisms that heal the cell are built, with no list, so
  every cause blocks. The causes are a record for the gates, #1183 (liveness causes by phase).
- **Pierre picks each nightly red in the gate week of phase 7, or no one-week gate** (decision
  26). Each liveness red of that week starts a fix thread.
- **Change the docs phase by phase, or keep the plan document as the only desired state**
  (decision 28). One docs PR writes the desired state first, so Pierre reviews it before the work
  runs without him.
- **Tenant `READ_ONLY` locks for a move between cells.** The new move of a namespace (step 5.6)
  takes no lock: the old set is sealed, the new set boots from the latest set, and an abort
  ends the move (section 3.7).
