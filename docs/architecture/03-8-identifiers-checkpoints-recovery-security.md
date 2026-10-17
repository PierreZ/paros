[← 03-7-universe.md](03-7-universe.md) · [Index](README.md) · [04-roles.md →](04-roles.md)

### 3.8 Identifiers

Every identifier is random or minted by the one writer that can check it, never derived from a
cell's log position, so nothing is renumbered when cells are added, removed or restored (decided
on 2026-10-02, #226). **No identifier is fixed** (decided on 2026-10-04): there is no well-known
tenant, no well-known journal and no reserved range. `0` means unset in every id space, and that
is the only value with a meaning. **No id has a default** either: an id is drawn or read, never
assumed, and unset is a state to refuse, not a value to fall back on. Names are labels beside
these ids (decided on 2026-10-07, #252): the universe's, a cell's, a tenant's and a journal's name
(sections 3.5, 3.7) are chosen and unique within their scope, never derived from or reused as an
id.

**Ids on the wire, labels for people** (decided on 2026-10-09). Every RPC and every stored record
names the universe, a cell, a tenant and a journal by its `u64` id only; a name never crosses the
protocol below the entry roles. Each of them also has a **string label**, chosen when it is
created (`universe init --name`, `cell init --name`, tenant and journal create) and shown by
`parosctl`. A label resolves to its id only at the edge (the resolver and
the frontend, section 3.5, and `parosctl` for administration), and an id never becomes a label.

**Hex ids are for local debugging only** (decided on 2026-10-10, #399). Every input and output
for a person uses names: `parosctl` prints the name of each machine, cell, universe, tenant and
journal, and takes a name wherever it takes an argument. A short hex id appears only when no
name is known. A machine has a name too: `parosd --name` (`PAROS_NAME`), by default the host of
its advertised address. The machine record and the registry keep it, and a new name registers
the machine again. `parosctl init --universe-name --cell-name` names the universe and the first
cell, and the universe directory keeps both names; a second cell with a name already in use is
refused.

- `node_id`, `cell_id`, `universe_id`: random, minted at format, `cell init` and `universe init`
  respectively, and
  stored in the machine record (`node_id`), the durable cell plan (`cell_id`) and the cell control
  journal (`universe_id`, `JoinFleet`, amended on 2026-10-10, #216). They
  are written once, unset → set, and a later mismatch is refused at boot like any `Config`
  mismatch.
- The **leader uuid** of a single-writer journal (section 2.3): 128-bit random, drawn by the
  leader for one term. A writer never chooses an identity that another process could share. The
  journal keeps no history of past uuids and does not refuse one that led before: it trusts its
  clients to draw fresh uuids (decision C, section 2.3, #241).
- **Failure domains** are not identifiers: `AcceptorConfig` carries the registry's
  `paros/zone` names unchanged (section 5).
- **Tombstones** (removed tenant ids, dropped tenants, deleted journal ids) are kept forever, a
  `u64` each. They are the one part of control state bounded by history rather than by live
  entities (section 3.9), accepted as such (decided on 2026-10-04). Names are not tombstoned
  (section 3.5).
- `TenantId(u64)`: random, drawn by the creator and recorded by the universe tenant in the `REGISTERING` step;
  the universe tenant refuses a duplicate at apply and the creator redraws. It is universe-unique, so moving a
  tenant between cells never needs a new id. The system tenants are no exception: each cell's
  cell tenant gets a random id at the cell's `init` (so two cells' cell tenants differ), and the
  universe tenant gets one when `init` creates the universe, kept when the universe tenant moves. The universe tenant records both
  (its own in its first entry, each cell tenant in that cell's entry), so its duplicate check
  covers them too. The universe tenant records each tenant's tags beside its id
  (section 3.7). FDB gave each metacluster an id prefix for the same goal; a random draw
  checked by the universe tenant needs no prefix.
- `JournalId(u64)`: random, unique within its tenant, recorded and checked at apply by the tenant
  coordinator, the single writer of the tenant's control journal; a duplicate is refused and the
  creator redraws. A tenant's **control journal** has a random id too, drawn with the tenant and
  recorded where the tenant is recorded: in the universe directory's tenant entry, and for the two system tenants
  in the cell plan (the cell tenant's in its own cell's plan, the universe tenant's in the plan of
  every admitted cell). A journal's id never changes when its tenant moves; a control journal that
  recovery rebuilds (section 3.10) gets a new one, so the old and the new can never be mistaken
  for each other.
- **Discovery replaces convention.** The only fixed starting points are a machine's addresses:
  the `JournalIdentifier`s of the cell's and the universe tenant's control journals are learned from any machine of the cell
  (section 3.2), with the cell that hosts the universe tenant now, and everything below them through their folds.
- **Every peer and client message carries its `JournalIdentifier` `(TenantId, JournalId)`**, riding
  the `Deliver` envelope (#235), so uniqueness is only ever needed where it can
  be checked. A tenant's matchmaker set is named by its `TenantId`.

### 3.9 Checkpoints

The control plane compacts its control journals with the same two calls any tenant has (decided
on 2026-10-02, #227): `paros-core` gains nothing. The pattern, shipped in `paros::client` (#230)
for the control plane first and offered to users as a recipe, never a guarantee paros enforces:

1. The coordinator pauses its own control writes (it is the single writer, and control writes are
   rare).
2. It writes a checkpoint of the folded state as a run of ordinary fenced `Write` batches.
3. It calls the fenced `Truncate` up to the run's first record.
4. Readers start at `first_seq`, which is always the first record of a complete run, and fold
   forward.

A checkpoint replaces the state entirely, so a crash between the write and the truncate is
harmless: the next fold meets a checkpoint in the middle of the log and resets on it. `Truncate`
only ever targets the first record of a complete run. A reader that gets `Truncated` restarts
from `first_seq`.

- **Control state is bounded by live entities, never by history**: the latest entry per
  `node_id`, current assignments only. That is what keeps checkpoints small. The one exception is
  the id tombstones (section 3.8).
- **Trigger**: checkpoint when the log since the last checkpoint reaches `k ×` the current state
  size, plus a time bound, which caps the extra writes at `1/k` (shipped defaults: `k = 4`, one
  minute; both are knobs in simulation). Kafka (KIP-630) snapshots only
  after a minimum number of bytes and a minimum share of changed records, and KIP-876 added a
  time trigger; Redpanda snapshots its controller after each command or at most every 60 seconds.
- **Local copies of a fold are caches only**, never the only copy of anything: a coordinator's
  memory, a machine's cached registry. That is why the checkpoint lives in the journal and not in
  a local snapshot file per replica, as Kafka and Redpanda do it: coordinators are `stateless`,
  so a newly elected coordinator has no local file to start from.
- **No checkpoint is one large record** (decided on 2026-10-10, #353). Pierre asked for the
  simplest design that scales. A checkpoint is a run of records in the journal it checkpoints,
  each record the 8-byte magic and a `paros.checkpoint.v1.CheckpointRecord`
  (`proto/checkpoint.proto`):
  - `Begin { covers_up_to }`, at position `covers_up_to`: the state covers every position below.
  - `Chunk { bytes }` records: the state cut into pieces of at most
    `ClientTunables::checkpoint_chunk_bytes` (8 KiB by default).
  - `End { chunks, checksum }`, the **commit point**: the chunk count and the CRC-32C of the
    chunks concatenated.

  The owner writes the run in batches under `checkpoint_batch_records` and
  `checkpoint_batch_bytes`; a node's `TooLarge` lowers both for the rest of the run. A small state
  fits one batch, so one slot. A large one takes as many small batches as it needs. Batches go
  one at a time; pipelining them waits until a pause is too long. No entry is ever inside a
  run, because the owner holds the writer. A reader collects a run from `Begin` and restores (or
  verifies) at a valid `End`. An entry, a new `Begin` or a gap inside a run drops it. A run with
  no `End`, or with a wrong count or checksum, is never restored. A crash inside a run is
  harmless for the same reason. The retired `Inline` and `Ref` forms (#230) decode to no form
  and are refused: no cell wrote `Ref`, and no cell outlives the change. A second checkpoint
  journal (`Ref`, Pulsar PIP-14) and a rolling sharded checkpoint were considered and rejected as
  more complex (#353).

### 3.10 Recovery (deferred)

**Deferred out of M9** (decided on 2026-10-04, #231): nothing of it is built. The simulation runs
one to three founding members (#213 (three founding members)); a cell that loses a majority of them is
`cell_lost`, excused by the oracles and never recovered. The design below stays the
direction and is taken up as its own later issue. Two questions it must answer then: with the universe
tenant's and the cell's control journals on the same founding members, recovery reads the machines' own stores
(`journals/<tenant>/<journal>/`, their `JournalIdentifier`s and assignments) to rebuild both, tombstones
included; and a new control `JournalId` must reach machines whose cell plan is written once.

Losing a control quorum is recoverable without unsafe Paxos surgery, at both levels, because
control journals are rebuilt from below (decided on 2026-10-02, #225, #231). User data is never
touched: the tenants' journals have their own quorums.

- **Cell.** `parosctl init --recover` starts a fresh cell control journal, under a new random
  `JournalId` recorded in the cell plan, and a new recovery generation; live machines re-register; tenant coordinators re-report from their own control
  journals; any node still holding the old configuration is refused.
- **Universe.** The universe directory is rebuilt from the cells' tenant lists, the way FDB's metacluster
  could rebuild a lost management cluster from its data clusters.
- Every recovery has a **dry-run** mode that reports what it would change without changing it,
  and takes a **recovery id** recorded in the journal so two recoveries cannot run at once. Both
  come from FDB's metacluster restore.

### 3.11 Transport security

**Every session can be TLS 1.3, and mutual TLS by default** (decided on 2026-10-11, PR #509
(mutual TLS), over moonpool #330 (mutual TLS)). A process makes one choice at its start, a
`paros::Transport`: plaintext, or TLS from three PEM files. A listening process serves and
dials with the same choice, so every process of a cell must make the same one.

- **One authority per universe.** Every leaf names its holder
  `spiffe://<universe>/<role>/<name>`, with the roles `machine`, `frontend`, `admin` and
  `client`. A machine's or a frontend's leaf also proves the DNS name `paros`, the one name
  every client expects: a client dials an address, and an address changes (#257), so the
  client checks the authority and the shared name, never the address.
- **Mutual TLS decides who may open a session, not what it may do.** The servers ask every
  caller for a certificate (`PAROS_TLS_CLIENT_AUTH=required`, the default; `optional` and
  `off` are the other modes). Every endpoint stays public. Authorization from the certificate
  identity is a later step.
- **Classic TLS stays.** `off` is server-authenticated TLS: any caller that trusts the
  authority opens a session.
- **Operators never write a certificate by hand.** `parosctl tls init` writes the authority
  and a certificate per machine, frontend and admin, and prints the variables each process
  reads; `tls issue`, `tls show` and `tls verify` handle one file at a time.
- **The simulation runs TLS on about half the seeds** (decided on 2026-10-11): a dice
  rolled at the start of a seed turns TLS on for every runtime, with one mode per seed. Every
  process issues its leaf at its boot with the shipped issuer, from seeded keys. The issuer's
  BUGGIFY site can flaw a leaf; the harness reissues it. Flawed handshakes are moonpool's own
  campaign (`sim-rpc-tls`).
- **Deferred:** byte-identical handshakes under simulation (moonpool's phase 4), certificate
  rotation without a restart, and identity-based authorization.
