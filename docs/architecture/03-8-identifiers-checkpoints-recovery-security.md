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
  mismatch. A machine that lost its disk mints a new `node_id` and is admitted again by
  credential (decision 4, section 3.1); the old id is never admitted again.
- The **term number** of a control journal: the count of terms in its fold (rule 3, section
  3.3). Each order of a coordinator carries it, and a receiver keeps the highest one it saw on
  disk. The **epoch** of a configuration is the high part of its ballot (decision 2, section
  4.1); an order for a journal names it.
- The **leader uuid** of a single-writer journal (section 2.3): 128-bit random, drawn by the
  leader for one term. A writer never chooses an identity that another process could share. The
  journal keeps no history of past uuids and does not refuse one that led before: it trusts its
  clients to draw fresh uuids (decision C, section 2.3, #241).
- **Failure domains** are not identifiers: `AcceptorConfig` carries the registry's
  `paros/zone` names unchanged (section 5).

**Names for people, ids for machines, and an id is never an authority** (Pierre, 2026-11-22;
goal 5 of section 1.1). This decision changes three rules of this section. The code and the
bullets below still tell the rules of the tag where they differ; #1402 (remove the tenant from
the core) tracks the change.

- A person types and reads ASCII names. Machines and protocols carry `u64` ids.
- An id is an address, never an authority. A client sends a token and names to the frontend
  (section 3.5), and it has no field to put an id in. So a caller that crafts the right `u64`
  gains nothing.
- A client does not draw an id. A create call carries a name and a request id. The service draws
  the id, and the fold checks it at apply and refuses an id that is in use: the fold of the
  universe for a `TenantId`, the fold of a tenant for a `JournalId`, which is unique inside its
  tenant (Pierre, 2026-11-22). A counter in the fold was refused: the tenant of a cell exists
  before the cell joins a universe, so no counter of the universe can give its id.
- **The rule "one id names at most one journal, forever"** (#1419 (the tenancy wrapper and
  paros-names); Pierre, 2026-11-22, who took the recommendations of the issue). The model is
  `specs/tla/journal-ids/`. The rule has four parts. A tenant fold gives a `JournalId` to at most
  one journal over the whole history, deletes included: a deleted id stays a tombstone. The
  universe fold gives a `TenantId` to at most one tenant. On a host, one key holds the records of
  at most one journal, because the host key is the pair `(TenantId, JournalId)`. A host holds
  records under a key only after the fold gave that key to the journal: a slot is booked only
  under an id that is in the fold.
- **Who draws.** The coordinator of the level draws the id: it is the single writer of the
  level's fold. The universe coordinator draws a `TenantId`, and the tenant coordinator draws a
  `JournalId`. The frontend passes only the name and the request id. When the fold refuses an
  id, the coordinator draws again. A create resumes by its request id: the fold keeps the id it
  gave, so a coordinator that restarts reads the id back and draws nothing.
- **Genesis.** At `cell init`, the cell tenant draws its `TenantId` and writes it in the cell
  plan. `universe add-cell` records that id in the universe fold: its directory half, `AddCell`
  (section 3.7), refuses an id in use, and the cell's own half, `JoinFleet`, follows only an
  accepted `AddCell`. A refused cell stays out of the universe, because its id is already on its
  disks and it cannot draw again. Safety does not depend on a fresh draw; only the join does (the
  chance of a collision is about `2^-64`). A counter in the cell is refused: every cell would get
  the same first id. The universe tenant's own id, drawn at `universe init`, and the id of each
  tenant's control journal, drawn with the tenant and recorded beside it (below), are drawn the
  same way; `journal-ids/` does not model them, and a collision there has the same `2^-64` chance.
- **Token.** A tenant token is pinned to a tenant id: it carries the tenant's `u64` as a fact
  beside its name, and the frontend checks it against the id that the name resolves to. So a
  token for a deleted tenant fails after its name is used again. This amends "Names, not ids" of
  section 3.5. **Open** (#1431 (the frontend role, the resolver, the Biscuit adapter)): how the
  offline mint of `parosctl` learns the `u64` of the tenant.
- **Birth nonce.** No birth nonce in `Assign` in the first build: the tombstones and the order
  (the id is in the fold before a booking) keep the rule without it (`journal-ids/`). **Open**
  (§3.10, recovery): a control journal that recovery rebuilds forgets its tombstones; a nonce in
  `Assign`, or the tombstones rebuilt from the hosts, is decided with recovery.
- `paros-consensus` and `paros-journal` know no tenant (section 4.4). So a peer message of the
  core carries one journal id, and the pair `(TenantId, JournalId)` is made above the core: the
  host is generic over its key, and the tenancy crate keys it by the pair.

- **Tombstones** (removed tenant ids, dropped tenants, deleted journal ids) are kept forever, a
  `u64` each. They are the one part of control state bounded by history rather than by live
  entities (section 3.9), accepted as such (decided on 2026-10-04). Names are not tombstoned
  (section 3.5).
- `TenantId(u64)`: random, drawn by the universe coordinator and recorded by the universe tenant in the `REGISTERING` step;
  the universe tenant refuses a duplicate at apply and the coordinator draws again. It is universe-unique, so moving a
  tenant between cells never needs a new id. The system tenants are no exception: each cell's
  cell tenant gets a random id at the cell's `init` (`AddCell` refuses a cell whose cell tenant's id is in use), and the
  universe tenant gets one when `init` creates the universe, kept when the universe tenant moves. The universe tenant records both
  (its own in its first entry, each cell tenant in that cell's entry), so its duplicate check
  covers them too. The universe tenant records each tenant's tags beside its id
  (section 3.7). FDB gave each metacluster an id prefix for the same goal; a random draw
  checked by the universe tenant needs no prefix.
- `JournalId(u64)`: random, unique within its tenant, recorded and checked at apply by the tenant
  coordinator, the single writer of the tenant's control journal, which also draws it; a
  duplicate is refused and the coordinator draws again. A tenant's **control journal** has a random id too, drawn with the tenant and
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
on 2026-10-02, #227): `paros-consensus` gains nothing. The pattern, shipped in `paros::client` (#230)
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

**Deferred** (decided on 2026-10-04, #231): nothing of it is built. The simulation never loses
a cell: the simulator refuses first each fault that the cell does not promise to survive (rule
8, step 1.9), so a cell that loses a control quorum is not a shape the oracle excuses. The
verdict "cell lost" that step 1.2 keeps from ground truth is deleted in step 7.2. When recovery
is taken up, a lost control quorum is read from the register and the term (rules 1 and 3), not
from a list of founding members, and a new control `JournalId` must reach machines whose cell
plan is written once.

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
- **The transport proves the sender and the bytes; a role trusts a correct peer** (decision 30,
  FoundationDB's rule). The transport must prove the sender and the bytes of each message
  (FoundationDB checks integrity at the transport, `fdbrpc/FlowTransport.actor.cpp:1309`,
  release-7.3). A role then trusts a peer of its pool: this peer is correct, but it can be late,
  it can send a message two times, and it can restart. A message that breaks a Paxos rule (two
  commands at one slot and ballot) is a detected safety violation: the assertion stays, and the
  process stops (FoundationDB asserts on the content of a peer request,
  `fdbserver/TLogServer.actor.cpp:2845`). Each handler puts a limit on each size and ignores
  what a late or repeated message of a correct peer can carry. So the code never asserts on what
  a correct peer or client can send. Each assertion on the content of a peer message names the
  Paxos rule and says why a correct peer cannot send that content (step 1.11). `Commit`,
  `CatchUpResponse` and `TrimmedTo` check their sender, a catch-up page has a bound at the
  receiver, and a `TrimmedTo` is taken only after a request. Today's transport proves the bytes: each
  frame has an XXH3-64 checksum that is verified before any decode, with TLS and without it
  (`moonpool-rpc/src/protocol/frame.rs`). Mutual TLS, the default, proves that the peer is a
  machine of the cell. The transport does not prove which machine sends, because paros does not
  read the name in the certificate (`PeerContext::identity`). The incarnation echo of step 4.2
  proves which machine answers (decision 35).
- **Deferred:** byte-identical handshakes under simulation (moonpool's phase 4), certificate
  rotation without a restart, and identity-based authorization.
