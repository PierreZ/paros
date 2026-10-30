[← 06-verification.md](06-verification.md) · [Index](README.md) · [10-sources.md →](10-sources.md)

## 7. What changes against today

Only what is still to change; landed changes (the four-call cut-over, the fenced `Truncate`, random
ids and the `JournalIdentifier`, start-and-wait plus `init`, the uniform `parosd`, the retired
read-index path and the unset `JournalIdentifier`s that meant "the first journal", #243; the
universe tenant, `JournalIdentifier` and proxy renames in the code, #244) are in the history and
AGENTS.md.

- The `(generation, owner)` pair of M7 becomes a single 128-bit leader uuid, compare-and-set by
  `SetLeader(new, old)`, with a hidden term counter in the core; `Write` takes an explicit
  `expected_seq`; journals gain a writer mode, single or multi (section 2, #241). No compatibility
  layer: `parosctl --owner` becomes `--leader`, and the chain workload's alphabet, the
  linearizability model and the audit follow. The single-writer half (PR #281), the writer mode
  (PR #296), the batch limits and the read limits landed (#241 is done); `parosctl journal
  create --mode` landed with #210 and became `--writers` with #523.
- Landed (#210): the directory is gone. Every hosted tenant has its own control journal: its
  identifier is drawn with the tenant, recorded in the universe directory and in the cell's
  `HostTenant` (with the tenant's name and `survives`), and it is born on the founding members,
  plain Multi-Paxos, majority. It holds the tenant's description (no rendezvous name) and every
  journal create and delete, each a request with an idempotency id judged at apply
  (`paros::model::tenant`). Until #476 and #225 the elected cell coordinator is the tenant coordinator
  of every hosted tenant: it fences each tenant control journal with its term uuid, answers
  `JournalRequest` at its published interface, and places journals on the founding members.
  Every machine follows the cell control journal and every hosted tenant's control journal
  (`ControlPlan`), and provisions a created journal at its first open (the machine record's
  `created` line is the commit point). `init` creates no hidden journal. Still to change: the
  acceptors' harness registry in the simulation (a `ControlPlan` over the genesis pool) becomes
  the machines' when journals move onto admitted machines (#474). Capacity is the cell
  coordinator's alone: it books a created journal's acceptors (#212 (placement)).
- `parosd` no longer runs the plain deployment (#190, landed): every journal is born with its
  tenant's matchmaker set on the founding members, and the matchmaker wire names its journal, so
  every journal of a process may name matchmakers. Proxy leaders and replicas are shared the
  same way (#193, landed): a proxy keeps one `ProxyLeader` and a replica one `ReplicaNode` and
  one store per journal, demuxed by `JournalIdentifier`. A node whose `Config::proxy_count`
  differs from its peers' (a rolling change of the count) changes which proxy runs a round, or
  makes the leader take the round back, never what is chosen; the simulation draws such nodes
  per seed. The counts stay boot data until the cell places those roles; #214 (tenant modes)
  applies the durability only. Decided on 2026-10-28: the counts leave the format marker for the
  configuration bound to the ballot (#754 (counts off the format marker)), and proxy leaders and
  batchers become per-journal counts (#753 (per-journal proxies)) instead of the per-tenant
  pools of #193. Landed with R1 on 2026-10-29: the counts are part of every `AcceptorConfig`,
  the tenant coordinator books each journal's proxy leaders, batchers and replicas and puts the
  booked counts in force by a round change (section 3.4). Landed on 2026-10-30 (#765): a
  `parosd` machine runs the booked proxy leaders of its journals, inside its node loop, and every
  machine sends to a proxy rank through the booked machine's peer lane. Still to do: a `parosd`
  machine that runs the booked replicas of its journals.
- `--survive` replaces `--failures-per-partition`, `partitions` leaves `Durability`, and
  `configure --plan`, `cell capabilities` and the status line of wants against has come with
  them (decided on 2026-10-28: #748 (survive), #755 (partitions leave durability), #749
  (configure plan), #750 (plan oracle), #751 (cell capabilities), #752 (wants against has)).
- The coordinators replace the operator's client (#212): admin calls become requests to the
  elected coordinator. Since #240, `parosctl init` no longer claims the cell control journal: the
  elected cell coordinator installs its uuid there. Since #513 and #225, no fleet operation
  claims it: the coordinator writes the cell's `JoinFleet`, and each tenant's footprint,
  `HostTenant` and `DropTenant`.

## 8. Milestones

Milestones are labels (`milestone:M7` and up); `milestone:M6` stays the epic's umbrella. The
toy is the end of M9. The epic is #184, the backlog pointer #69, the verification track #24.

| Milestone | Name | Content |
|---|---|---|
| M7 | Journal API (#204, #205) | the four calls, the journal state machine in core, the wire and the driver, the chain workload's alphabet, the linearizability checker, the race knobs and BUGGIFY sites, the cut-over |
| M8 | parosd deployable (#206 to #209, #221, #220, #196, #201) | Tokio providers linked, the stores on a real filesystem for the first time, the `JournalStores` opener, `Config` durable at `format`, `parosd provision` (replaced by `init` in M9), the uniform binary with class and capacity, Compose, `paros::client` (#221) and the `parosctl` CLI (#220), a tracing subscriber, exit codes |
| M9 | The universe with one cell (#225, #226, #227 first; landed: #216 (PR #408), #192 (PR #412), #210 (PR #347), #211 (PR #352, PR #388), #239 (PR #464), #191, #390, #404, #228, #235, #229, #230, #176, #261, #263, #264 (PR #266), #267, #202, #213, #246, #247, #241, #243, #244, #240, #190, #212 (placement function and bookings), #474 (PR #499), #501, #257, #245 (PR #535), #476, #475, #477; still to do: #248, #252) | the control hierarchy and its decisions, the fenced `Truncate` on the wire, random ids and the `(TenantId, JournalId)` `JournalIdentifier`, the leader-uuid API and its two writer modes, `init` creating the universe with its matchmaker sets, the cell tenant and its machine registry with role slots and liveness, the universe tenant with its directory and tenant creation state machine, the election library and the coordinators it runs, requests to a leader, placement inside capacity granted by the cell, the checkpoint-and-truncate library, names at the frontend, the frontend with Biscuit `Authz` routing through the universe tenant, per-tenant matchmaker sets, `parosctl status` |
| M10 | Roles per tenant (#193, #214, #194, #145, #195) | journal-tagged proxy leaders and replicas, batchers and unbatchers for multi-writer journals, tenant modes (durability, role counts) applied by the tenant coordinator, quotas, the benchmark, then scale work |
| M11 | Zones (#215) | `(region, az)` `FailureDomain`s in `AcceptorConfig` with its `cell_id` (one format bump), the two-predicate zone rule, zone round-robin placement, the `machines=0` exemption, the leader following its writer's zone, zone-kill attrition and a zone-aware budget in the simulation, zone-spread matchmaker sets |
| M12 | Multiple cells (#232, #233, then #253) | adding and removing cells with tombstones, placement across cells by `kind` and `survives` (both carried since M9 with the cell's entry endpoint), tenant locks, moving tenants and the universe tenant between cells, splitting the universe tenant by range, the resolver beside the frontend (section 3.5); last, multi-region cells: the `MultiRegion` cell kind with its witness region, the two-level zone rule, per-journal proxy leaders and batchers in the leader's region (#753 (per-journal proxies)), the universe tenant hosted in a multi-region cell, the partition through the witness in the simulation, moving tenants between cell kinds |
| M13 | Upgrades and network compatibility (#363) | decided on 2026-10-10: a very late milestone. A wire version in the RPC handshake, a cell-wide active version in the cell control journal, features that turn on only when every machine supports them, rolling upgrades one machine at a time, a downgrade by one version, mixed-version cells in the simulation. This M13 is new: the earlier M13 (multi-region cells) went into M12 on 2026-10-09 |

Verification is not a milestone: every milestone carries its own share of section 6. M9 opens
with a **simulation-first phase** (decided on 2026-10-04): storage chaos on the shipped stores
(#176, #202), three founding members (#213), the `parosd` machine lifecycle simulated as shipped (#246), the
control-plane oracle debt (#247) and TigerStyle assertions (#248) rank ahead of M9's features. Recovery (#231, section 3.10) is
deferred and carries no milestone yet. The interactive game and the lessons (`track:play`, #162,
#163) run outside the milestones, behind the service.

## 9. The toy, done means

The Compose toy is the user's demo, for running paros by hand, and is no part of the test suite
(decided on 2026-10-04): CI only checks that the image builds, and behaviour is proved by the
simulation. Its machines are plain nodes (`node1`..`node3` over three failure domains, `storage4`,
`stateless1`); `parosctl init --members` founds the cell on the first three, and an admin
admits the others with `cell add-machine`. Its journals run
`machines=1`: four `storage` machines cannot hold the five acceptors of
`machines=2` on distinct machines, so the toy cannot run it (decided on 2026-10-07,
#215).

From a fresh clone: `docker compose up`, then `parosctl init` against `node1`, which creates the
universe, its one cell and the universe tenant. Generate a root key and mint an `admin` token offline with `parosctl`, then create a tenant and
mint its `tenant` token offline. Create a
journal. `write`, `read` and `tail` from `parosctl`, addressing `acme/orders`. `set-leader` to a second
client and see the first one refused, for a `write` and for a `truncate`. Create a multi-writer
journal and append to it from two clients at once. Kill one `storage` and one `stateless`
container and keep writing. Wipe one volume, see the amnesia refusal, and see the journal healed
by reconfiguration onto another machine. Stop the universe tenant's quorum and keep writing to an
existing tenant. Kill the cell coordinator's machine and see another one elected. `parosctl
status` shows desired, available and current, and the role slots, per machine, per tenant, per
cell and for the universe. (Recovery of a lost cell control quorum is deferred, section 3.10.) The simulation is green in every shape and the
coverage-guided sweep saturates.
