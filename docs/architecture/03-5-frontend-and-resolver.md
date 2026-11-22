[← 03-4-tenant-modes.md](03-4-tenant-modes.md) · [Index](README.md) · [03-6-status.md →](03-6-status.md)

### 3.5 The frontend and the resolver

Two entry roles (decided on 2026-10-07, #233): a **resolver** per region at the universe level, which
redirects a client to its tenant's cell, and a **frontend** inside that cell, a role that the
operator gives to machines, which forwards the client's calls.

**The frontend.** A role in front of the machines (Spanner's per-region API frontends
authenticate and route the same way). **The frontend is a machine role** (decision 15, question
6): the operator gives it to machines with `PAROS_ROLES` (section 3.2), and a tenant types no
frontend count. A client finds the frontends of its tenant's cell through the second hop of
**`Resolve`** (below), which any machine of that cell answers from the registry it already folds:
the machines that have the frontend role, and the client spreads across them, in its own zone
first. It authorizes the caller through an `Authz` trait whose implementation verifies a Biscuit
token (below), and it routes each call to the machine serving the journal, so a client never
knows placement. It forwards calls and their answers rather than redirecting the client (decided
on 2026-10-04): clients only ever reach frontends for data, which is what keeps the network the
trust boundary. Past the frontend nothing knows a tenant name, only `(TenantId, JournalId)`.

**The resolver** (decided on 2026-10-07, #233). One pool per region, at the universe level, stateless
and shared by every tenant. It answers the first hop of `Resolve`, "which references
serve tenant T", from its cached fold of the universe directory: the tenant's `TenantId`, its cell id
and that cell's entry references. It **redirects**: it never carries data and never forwards
a call. It keeps resolving from its cached fold while the universe tenant is unavailable, which is the
AWS guidance's thinnest possible router and static stability one level up (section 3.3). Before
it answers, it verifies the token's Biscuit signature and that its scope covers the tenant asked
for (an `admin` token may resolve any tenant), using only the root public keys
the universe entry carries: it holds no private key and no state of its own, and Biscuit stays out of
`paros` and `paros-consensus` as below. A resolver folds the universe directory and nothing else, never a
cell's registry, so it stays thin and every cell answers for itself.

**Resolution is two hops: one call, two answerers.** A client asks a resolver of its region for
tenant T and gets T's cell id and that cell's entry references; it then asks any machine of
that cell the same call and gets T's frontends, each tagged with its zone, from that machine's
registry fold, and it prefers a frontend in its own region and zone. Both answers are cached and
refreshed on `StaleIncarnation` or a redirect. The resolver stops at the cell: inside it, the cell's
machines answer and the frontend forwards, so forward-not-redirect stands and nodes still do no
authorization.

**Where `Resolve` stands** (#216, landed on 2026-10-10). Every machine of a cell answers both hops in one answer (`paros::machine::resolve`), with no token. Since #233
(resolver) a client with a token asks a resolver first (below). It reads two folds through the cell's machines, as a client does, and
keeps them between requests:

- The universe directory, when the cell serves it. A `READY` `users` tenant resolves to its
  `TenantId`, its control `JournalIdentifier` and its cell, with the `universe_id`. Refusals:
  `unknown_tenant`, `not_ready`, `internal` (no user name), `other_cell` (the answer names the
  tenant's cell, for M12), `no_universe` (the cell serves no universe directory yet) and
  `unavailable` (a fold could not be read to its tail, so ask another machine).
- The registry. The answer lists the cell's machines at the addresses the registry holds
  (`cell_book`). A client calls them until the frontend exists. Then the same answer carries the
  tenant's frontends, and with #404 each reference is an `InterfaceRef`.
- The tenant's batchers (#145 (batchers), #604 (role counts)): each machine that the registry
  books a `batcher` slot of the tenant on, at the address the book holds for it
  (`ResolveAck.batchers`). A client of a multi-writer journal sends its writes there
  (`Resolved::batcher_servers`, section 2.4). A tenant with no batcher gets an empty list.
- Each machine's zone (#673 (zoned resolve), landed on 2026-10-29): every `Member` of the answer
  carries the `paros/zone` tag the registry holds for it (`Member.zone`, empty when it has none).
  The client takes its own zone from its configuration, never from a draw, and ranks the
  machines, batchers and frontends of its zone first (`Resolved::prefer_zone`).
  `parosctl resolve <tenant> --zone <zone>` lists them in that order. In the simulation each
  client stands in a zone of the cell's; its oracles: an answered zone is the one the machine's
  layout placed it in, and a client asks a machine of its own zone first when the answer names one.

`paros::client::resolve` asks the entry endpoint's addresses in turn and passes over a machine
that does not answer or answers `unavailable`. `parosctl resolve <tenant>` prints the answer. An
answer is a hint from a fold: a stale one costs the client a refused call, and it resolves again.
The simulation resolves every created tenant at the entry endpoint (every machine, from a drawn
one on) beside the library's own resolution. Its oracles: a removed, internal or never-created
name never resolves, and a resolved tenant is served by the operator's cell. Machines do no
authorization: the Biscuit check is the resolver's and the frontend's (#245).

**Where the resolver stands** (#233 (resolver), landed on 2026-10-30). The resolver is a role of
`parosd`, as the frontend is (#723 (frontend as a parosd role)): every machine given the
frontend role also answers `ResolveEntry` on its own edge, with the same `Authz` and the same
wall clock (`paros::machine::resolve`'s `entry` module). There is no separate binary and no
booking: a region's pool is the machines a client lists as its entry endpoint, any machine of
any cell of the universe. For each request the resolver:

1. reads the universe directory to its tail, through the same fold the machine's `Resolve`
   keeps, within `resolve_answer_within`. When the read fails or runs out of time, it answers
   from the copy it last read to its tail (static stability, section 3.3). A machine that never
   read the directory to its tail answers `unavailable`, and the client asks the next one;
2. checks the token (`tenant.resolve`) against the root public keys of that copy, else against
   the machine's pin. A `tenant` token resolves its own tenant only; a token derived to one
   journal resolves with that journal's name in the request. A refusal is `invalid_token`,
   `expired` or `forbidden`; a malformed name is `malformed`;
3. answers a redirect: the tenant's id, its cell, the cell's name and the cell's entry endpoint
   that the directory records; for the cell that hosts the universe, the machine's universe
   pointer when the entry records none (`no_endpoint` when neither names a machine). Never the
   control journal, the machines, the batchers or the frontends: those are the second hop's.

`paros::client::resolve::resolve_through` asks the resolvers in turn, then the cell's endpoint
`Resolve`, which follows at most one `other_cell` hop when the resolver's copy was stale: a
redirect never loops. `parosctl resolve <tenant>` takes this path when it is given a token. The
simulation resolves each created tenant through the machines' resolvers with a drawn token
(the tenant's own, an `admin` one, another tenant's, an expired one, one an unknown key signed,
bytes that are no token), and the second cell's tenant through a resolver of the run's cell. Its
oracles: a token without the right is never redirected, a token with the right is never denied,
a never-created name is never redirected, a tenant is sent to the cell it was created in, and
the redirected cell resolves its own tenant. A per-incarnation BUGGIFY decision answers every
request after the first read from the copy alone, as when the universe tenant is out of reach.
Not yet: the second hop still reads the universe directory, so it answers `unavailable` while
the universe tenant cannot be read; the Compose demo has a second cell and every role.

```
 client ──Resolve(T)──► resolver (its region: universe directory fold, Biscuit sig + scope)
        ◄── cell X, X's entry refs ──┘
 client ──Resolve(T)──► any machine of cell X (registry fold)
        ◄── T's frontends, zone-tagged ──┘
 client ──Write/Read/...──► frontend T (Authz, names) ──forwards──► leader / replica / batcher
```

**Names** (decided on 2026-10-04, #239; three parts since decision 12). A user addresses
`paros://<tenant>/<namespace>/<journal>`, for example `paros://acme/billing/invoices`. The
**namespace** is the placement key with the tenant (section 3.7) and the unit of a move between
cells. The word is "namespace" because "group" already names the tenant groups in the code. The
smallest way to build it makes a namespace what the protocol calls a tenant today (step 5.8);
that is a reading of the split study, and nothing was run. The URI names data, not a location, and the client's entry endpoint (the resolvers' addresses, a
frontend's address until the resolver exists) is client configuration. **Only the entry roles resolve names**:
clients send names and never read the universe tenant, so no tenant sees another tenant's names
(amended on 2026-10-07, #233: the resolver resolves the tenant name, only for a tenant the token's
scope covers, and the frontend the journal name). Without `--frontends`, `parosctl` resolves
with operator rights; with it, `parosctl` sends names to a frontend (#192 (the frontend)). A name is free again once its delete completes; a recreated
tenant or journal draws a fresh id, so an old id never aliases a new name.

**Display** (decided on 2026-10-07, #239; amended on 2026-10-10 by Pierre's names rule).
Everything a person sees or types names an entity by its string name: the universe, a cell, a
machine, a tenant and a journal, in CLI input and output, in operator logs and in the docs. Hex
ids are for local debugging. Human output prints an id only when no name is known, as short hex,
git-style (e.g. `cell=2c94f1`), widened when a prefix is ambiguous within the listing; `--json`
keeps the full id beside the name, and a command that takes an id accepts a unique prefix.
`Resolve` gives the universe's and the cell's names with their ids (#239), and the machine
lifecycle logs name the machine (`machine=<name>`). An internal tenant (section 3.7)
shows as `<kind>:<name>` from its `paros/role` tag (`universe:acme`, `cell:paris`), display only,
never a resolvable name or an id: section 3.8's no-well-known-id rule stands.

**Where names stand** (#239, landed on 2026-10-10; the namespace part comes with step 5.8). The
library parses and prints `paros://<tenant>/<journal>` today (`paros::model::name`) and resolves
it in two hops
(`paros::client::names`): the tenant name through the universe directory, where only a `READY`
`users` tenant resolves; then the journal name through the tenant's control journal, the
`TenantControl` fold (#210 (tenant control journal)), where only a live journal resolves. A client caches a resolution and drops it
when a call is refused as naming an unknown journal; the next resolution reads the control journal
again. `parosctl` resolves the names its journal commands take. An operator can name a journal by
its ids, `id:<tenant>/<journal>` in hex, each half a unique prefix of an id that `parosctl` can
list, or all 16 digits. Since #210 (tenant control journal), the machines serve every tenant's
control journal, so a name resolves both hops on `parosd`, and `init` creates no user journal.

**Where the frontend stands** (#192 (the frontend), landed on 2026-10-11 in PR #412).
`paros::roles::frontend` runs the frontend. The contract is the `Frontend` service in
`proto/paros.proto`: each of the four calls with an `Entry` (the
token, the tenant name and the journal name). The frontend does these steps for each call:

1. It authorizes the call in names, before any I/O. A denied call reaches no machine. The
   answer carries a `FrontendVerdict` (`INVALID_TOKEN`, `EXPIRED`, `FORBIDDEN` or `MALFORMED`),
   which the client reads as `Denied`.
2. It resolves the names with the library's hops and caches each result. A machine that
   refuses the journal as unknown makes the cached result stale. While the universe directory
   cannot be read, the frontend uses the directory it read last (static stability). It also
   learns its cell again, so it follows a universe pointer that a repoint changed (#349). A namespace
   that lives in another cell, or moved there, gets the verdict `MOVED` and nothing is forwarded
   (#785 (moved record), decided on 2026-11-04; the unit is the namespace since decision 12). A
   client built to follow its tenant then resolves the name again and retries the call once, at
   the frontends of the cell that serves it. The retry is safe, because a `MOVED` call reached
   no machine.
3. It forwards the call to the machines. It follows the leader hints itself and removes them
   from the answer. It never sends a write a second time: when an attempt that may have run gets
   no answer, the answer is `UNANSWERED`, which the client reads as `Ambiguous`. A read moves on
   to the next server after any failure.

The machines are every `storage` machine of the cell that the registry holds, at the address it
registered last, and the frontend learns new addresses as any peer does (section 3.2). A journal
placed on any machine, or led from one, is reached there. When a hint names a machine the
frontend does not know, it learns the cell again at once, and the call goes on once over the cell
it learned. A second leader out of reach ends the call.

An entry with an empty tenant name names an internal journal by the call's ids, and only an
`admin` token reaches it.

**Every `parosd` can serve the frontend; the operator chooses which do** (decision 15, question
6, step 6.8; it replaces #723 (frontend as a parosd role), where every `parosd` served it). A
machine with the `frontend` role serves the `Frontend` service on its own RPC edge, with the
`BiscuitAuthz` it is given (the keys of `PAROS_ROOT_PUBLIC_KEY`), and forwards each call to the
cell it belongs to. `Resolve` names the machines that have the role, and the status lists them.
`--frontend-count`, the frontend bookings of a tenant and the rule that every `parosd` serves the
frontend are deleted. `parosctl --frontends` and `PAROS_TOKEN` still name frontends by hand, and
`run_frontend` stays in `paros::roles::frontend` for an embedder. The name is `frontend`, because
"proxy" already means the proxy leader (confirmed by Pierre on 2026-11-16).

The simulation's machines serve the frontend, the same code as `parosd`, with root keys drawn
per seed. The workload calls the tenants' journals through the frontends by name, with the
tenant's own token, with tokens derived from it (read-only, one namespace, one journal), and with
tokens that must be denied: another tenant's token, an expired token, a key that no list holds,
bytes that are not a token, a tenant token on an internal journal, a read-only token on a write
and a token derived to another journal.

**Trust** (decided on 2026-10-04). The boundary is the network: only frontends and peers reach
a node's journals (a separate network in the Compose toy; a client reaches a machine only for the
well-known `Resolve` call, section 3.2), and nodes do no authorization.

**Tokens are Biscuits** (decided on 2026-10-04, #245; JWT is in section 11). paros is its own
issuer: a root key pair, Ed25519, whose public half (with its root key id) is recorded in the
universe entry; rotation is adding a key then removing the old one. Tokens are short-lived and there
is no revocation. `parosctl` works **offline**, with no running universe: it generates root key
pairs and mints tokens for any role.

- **Roles** are facts in the authority block, and there are two (decided on 2026-10-10, #400):
  `admin` administers the universe (`init`, cells, machines, tenants, everything below), and a
  `tenant` token is scoped to one tenant for its data plane, its journals and its own view of its
  spread over its cell (#399). There is no `tenant-manager` role: it would have needed the data
  rights of every `users` tenant. Only `admin` creates, deletes and lists tenants, tenant creation
  returns no token, and a key holder mints each `tenant` token offline. No frontend or resolver
  holds a key.
- **Names, not ids** (decided on 2026-10-10, #400): a token and the verifier's facts name a
  tenant and a journal by their full string names, never by hex ids. The entry roles already
  resolve names. An `internal` tenant has no resolvable name, so only `admin` reaches it. A name
  is free again after its delete. A tenant token also carries the tenant's `u64` id (amended by
  Pierre on 2026-11-22, #1419 (the tenancy wrapper and paros-names), section 3.8): the verifier
  checks it against the id that the name resolves to, so a token for `acme` fails on a later
  tenant named `acme`. **Open** (#1431): how the offline mint learns that id.
- **Token content** (#400, `paros-authz-biscuit`). The authority block holds `role(..)`,
  `tenant(<name>)` and `tenant_id(<u64>)` for the tenant role, a `subject(..)` label and an expiry check. For each
  request the verifier adds `time`, `operation`, `op_class`, `access` (`read` or `write`),
  `target_tenant`, `target_kind` (`users` or `internal`) and `target_journal`, then runs one
  Datalog policy in the server: the meaning of a role changes without new tokens. A view of #399
  asks `view.detail` (admin only) for full detail, else its own operation for the tenant detail.
  A refusal is `InvalidToken`, `Expired` or `Forbidden`.
- **Keys** (#400). `parosctl key generate --label <name>` writes `<name>.private` (mode 0600)
  and `<name>.public`: JSON with the label, a random `u32` key id (the Biscuit `root_key_id`,
  inside the token format only) and the key in Biscuit's own text form. A token without a key id
  the universe entry holds is refused.
- **Names for people, hex ids for local debugging only** (decided on 2026-10-10): everything a
  human reads or types names a key by its label and a tenant or journal by its name. A minted
  authority block carries `root_key(<label>)`, and `token inspect` names the key that verified it. An
  idle machine pins the key with `parosd --root-public-key` to check `FormCell` and `Admit`
  (#245).
- **Derive, macaroon style**: any holder narrows a token offline with `parosctl token derive`
  (read-only, one tenant, one namespace, one journal, an earlier expiry; a token and a quota can
  have the scope of a namespace, decision 12), with no key: the key that signs the
  next block is inside the token. `--seal` removes it, so nothing more can be derived.
- **The universe entry's keys** (landed on 2026-10-15, #245 (Biscuit tokens)). The universe
  directory holds the root public keys in force (`AddRootKey`, `RemoveRootKey`, metadata
  version 3). Each key has its key id and its label, and neither is ever used again after a
  removal. The last key in force is never removed. `parosctl init --root-public-key <file>`
  writes the first key, right after the universe is formed and only while the entry holds none.
  `parosctl universe key list|add|remove` changes them, each change one idempotent write
  through `paros::client::fleet`. A rotation is `key add` of the next key, then `key remove` of
  the old one once its tokens expired.
- **Which keys a check uses** (decided on 2026-10-15, #245). The keys in the universe entry
  rule once a frontend read any. Until then, or while the entry holds none, a frontend checks
  against the keys it is configured with (`PAROS_ROOT_PUBLIC_KEY`, optional).
  A frontend reads the keys with the universe directory and keeps the keys it read last while
  the directory cannot be read. A token refused as `invalid_token` makes it read the directory
  once more and check again, so a key added is trusted at its first use. A key removed stops
  being trusted at the next read of the directory. There is no read on a timer: that would be a
  new tunable for no new guarantee. An idle machine has no universe entry, so it checks only its
  pin (section 3.1).

**Deterministic first, fewer features** (decided on 2026-10-04). Biscuit runs only in a way the
simulation can replay, and features that cannot are left out:

- Every key and every appended block's next key comes from an RNG the caller passes in
  (`new_with_rng`, `build_with_rng`, `append_with_key`): a `ChaCha20Rng` seeded with 32 bytes
  drawn from the provider's random source, the OS-seeded thread RNG in production and the seeded
  RNG in the simulation (#400). The thread-RNG defaults are banned (clippy
  `disallowed-methods`). `biscuit-auth` is pinned by git rev until 6.1: 6.0.0 still uses
  `prost` 0.10 and `rand_core` 0.6.
- The authorizer's wall-clock budget (`RunLimits::max_time`, default 1 ms, checked against
  `Instant::now()`) is set out of reach; evaluation is bounded by `max_iterations` and
  `max_facts`, which count deterministically. The returned execution time is never read.
- The current time is a `time(...)` fact the frontend or the resolver adds from its provider's
  clock, never `AuthorizerBuilder::time()`, which reads `SystemTime::now()`.
- Decisions are allow or deny; no query result is consumed, because the Datalog engine's
  `HashMap` iteration order is per process. A refusal is judged by its kind, never by which
  error came first.
- Out of scope: third-party blocks, revocation ids, P-256, snapshots, extern functions and
  Datalog queries.
- Biscuit stays out of `paros-consensus` and `paros` (its wasm32 clock needs JavaScript's
  `performance`): `paros` defines the `Authz` trait and carries tokens as opaque bytes; the
  Biscuit implementation is its own crate, `paros-authz-biscuit` (#400), used by `parosctl`,
  `paros-sim` (#192 (the frontend)), and `parosd` for its frontend role (#723 (frontend as a
  parosd role)) and its bootstrap pin (#245).

**Routing goes through the universe tenant from M9.** The tenant and namespace → cell step is
the resolver's; the frontend also resolves it from its fold of the universe directory, and with
one cell it always answers "this cell", and it runs anyway. The frontend then resolves the
journal name → `JournalId` and its placement from its fold of the tenant's control journal.
Resolvers and the cell's machines answer the same `Resolve` call, "which references serve tenant
T", so a name never changes when a second cell appears or a namespace moves.

Tenants are created and administered through the same frontends, with an `admin` token,
through the universe tenant (section 3.7): one API, one `Authz` trait, exercised in the simulation like
every other call.

**The frontend routes to the data-plane roles** (decided on 2026-10-04).
A single-writer `Write`, a `Truncate` and a `SetLeader` go to the journal's Paxos leader; a
multi-writer `Write` goes through the journal's batchers when it has any (section 2.4), and the
unbatchers' answers come back through it; a `Read` goes to whatever holds the journal's replica
state. The frontend is not the **proxy leader** of Compartmentalized Paxos, which does Phase 2 for
the Paxos leader (section 4.1); "proxy leader" is never shortened to "proxy", and the entry role
is never called a proxy (decided on 2026-10-07, #233).
