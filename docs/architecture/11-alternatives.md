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
  formation, and "seed" collided with the simulation's seed. Replaced by founding members named
  in `cell init` and admin-admitted machines and cells (section 3.1).
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
  on clocks and pauses; the leader uuid fences without either, and the election library keeps a
  lease only as a liveness hint (section 3.3).
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
- **Hierarchical `Zoned` quorums** (deferred on 2026-10-07, #253): a later `QuorumSystem` variant once the
  simulation runs nine acceptors (section 5).
