# parosd and parosctl

`parosd` is the paros daemon: every role of a paros deployment over moonpool's
Tokio providers, with `paros::journal`'s stores on a real filesystem. The
drivers and the stores are the library's — the same code the deterministic
simulation runs — and this crate adds only what a process needs: arguments, a
data directory, a tracing subscriber, signals and exit codes.

`parosctl` is the client, over `paros::client` — the library client the
simulation's workload drives too. It holds no client policy of its own: which
server to ask, following a redirect, re-sending the identical write, settling
an ambiguous one, claiming a journal and resuming a reader after a truncation
are all the library's. The split follows etcd's (`etcd`, `etcdctl`,
`clientv3`).

## The toy: a cell with Docker Compose

The copy-paste version, with Docker or without, is [`DEMO.md`](../../DEMO.md).

From a fresh clone, Docker alone (the image is a plain multi-stage Rust build,
the one build outside Nix):

```sh
docker compose up -d --build        # five machines; each formats and waits
docker compose run --rm init        # forms the cell
docker compose run --rm parosctl tenant create acme
docker compose run --rm parosctl journal create acme orders
docker compose run --rm parosctl write acme/orders hello world --leader 7
docker compose run --rm parosctl read acme/orders
```

`docker-compose.yml` runs one cell of five `parosd` machines over three failure
domains, every one the same image configured by `PAROS_*` variables alone:

| machine | class | zone | role today |
|---|---|---|---|
| `node1`, `node2`, `node3` | `storage` | `zone-a`, `zone-b`, `zone-c` | the founding members: one network alias, `members`, resolves to the three |
| `storage4` | `storage` | `zone-a` | waits for placement (M9, #211, #212) |
| `stateless1` | `stateless` | `zone-b` | waits for placement (M9) |

**Start and wait.** A machine starts with its listen address (every
interface), its advertised address (its service name), its data
directory (a named volume), its class, capacity and zone. It is
configured with **no peer**: no seed, no join list. On its first start it
**mints its `node_id`** at random and records it in its data directory (there
is no `PAROS_ID`), then waits, idle. A machine never forms a cell on its own.

**Init.** `parosctl --servers members:4500 init --members
node1:4500,node2:4500,node3:4500` runs `cell init` over the **founding
members** (`--members`, by default the servers), each named by its advertised
address (#257). It sends `CellInit` to the first member still
idle, and that machine drives a single-decree Paxos on the cell plan, every
founding member an acceptor and every one needed:

1. **Prepare.** It sends `PrepareCell` with a fresh random ballot. Each member
   says who it is, promises durably to accept no plan under a lower ballot, and
   reports any plan it already accepted.
2. **Adopt or draw.** A reported plan is finished instead of a new one. A plan
   over another member list is refused (`other_cell_init`). A listed address
   that now hosts another machine than the plan names is a wiped member: the
   plan stays, with the old machine as a dead member. When a majority of the
   plan's members are wiped, no plan can be chosen (`cell_lost`). Otherwise it
   draws the plan.
3. **Form.** It sends `FormCell(plan, ballot)` to the other members, then forms
   itself. A member accepts unless it promised a higher ballot
   (`promised_higher`); accepting is forming. Every member must answer step 1,
   and a majority of accepts chooses the plan, so one wiped member does not
   block it.

Two concurrent `init`s form at most one cell, and an interrupted one leaves no
lock: the next `init`, sent to any member, hears the accepted plan and finishes
it. A formed member keeps answering the decree from its record. Every member
then serves the **cell control journal**, **the fleet tenant's control journal**
(the fleet directory: the fleet's one cell hosts it), plain Multi-Paxos over
the founding members. No user journal comes with the cell: a tenant creates
its own (below). **No
identifier is fixed**: `init` draws every one, records them in the cell plan, and
prints them (`control=`, `election=`, `fleet_control=`); afterwards any machine's
node-only `Inspect` names the cell's control journals, which is how
`parosctl tenant` finds them. Then the founding members elect the first cell coordinator
over the cell's **election journal** (multi-writer, #240), and the winner installs
its uuid on the cell control journal with `SetLeader(uuid, unset)`. `init` waits for it
and prints it (`coordinator=`). Last come the **fleet steps** (#229): the
cell records the fleet's id (minted by `init`) on its side, and the fleet tenant records
the fleet and adds the cell, `READY`. Every step is idempotent: re-running
`init` resumes an interrupted one, and on an initialized fleet it is refused
(`already_initialized`).

**Tenants.** `parosctl tenant create acme` registers a `{users}` tenant in
the fleet directory (`REGISTERING`, under a random id and a random control journal), waits
until the cell coordinator books its footprint and hosts it, then marks it `READY`; the CLI never creates an `internal` tenant
(the fleet tenant, `{internal, fleet}`, and the cell tenant, `{internal, cell}`, which
`init` registers); `parosctl tenant delete acme` marks it `REMOVING`, waits until the cell
coordinator drops it, then removes it (#225: only the coordinator writes the cell control
journal); `parosctl tenant list` prints the fleet directory. An interrupted
delete is resumed by running it again. A tenant is created once: a second
`create` of a name the fleet tenant holds is refused (`name_taken`), and an interrupted
creation stays `REGISTERING` until it is deleted (the coordinator of #225 will
finish it). `--survives az|region` sets what the tenant's journals survive
(#252). Every tenant has its own **control journal** (#210): the cell's
founding members serve it, and it holds the tenant's description (its name,
what it survives, its cell) and every journal it created. The cell
coordinator books a tenant's footprint before it hosts it (#476); a cell with
too few free slots refuses the tenant (`no_footprint`).

**Journals.** `parosctl journal create acme orders` sends a request to the
**tenant coordinator** — until #212 and #225, the elected cell coordinator, at
the interface its election renewals publish. The coordinator draws the
journal's id, picks its members from its durability
(`--failures-per-partition F`, 0 to 3, default 1, and `--partitions P`, at
least 1, default 1: `2F+1` acceptors in each acceptor partition, #523), and
writes the request to the tenant's control journal.
Every machine folds that journal, and each member starts the journal there.
The request carries an idempotency id: `parosctl` sends the same id again
until a coordinator decides it, so a request acts once, across a coordinator
change too. A live name is refused (`name_taken`); `--writers multi` creates a
multi-writer journal (#241). `parosctl journal delete acme orders` tombstones
the journal: its id is never used again, and its name is free. `parosctl
journal list acme` folds the tenant's control journal.

**Names and ids.** A journal argument is a name, `TENANT/JOURNAL` or
`paros://TENANT/JOURNAL`, which `parosctl` resolves through the fleet directory and the
tenant's control journal (#239 (names at the edge)), or its ids, `id:TENANT/JOURNAL` in hex: a
unique prefix of an id `parosctl` can list, or all 16 digits. Human output prints ids as short
hex, widened when two ids of one listing share a prefix; `--json` prints ids whole. A node id given on the command line
(`retire --node`, `reconfigure --members`, `ID=HOST:PORT`) is hex too.

**Write and read.** `parosctl` is handed addresses only: `--servers members:4500`
stands for every founding member, and each server's node id is learned from its own
`Inspect`. The writer claims the journal on its way (`SetLeader` against the
generation it read), then writes at the tail.

**Kill and restart.** `docker compose kill node2` (a storage machine) and
`docker compose kill stateless1` (a stateless one): the journal keeps a majority and
keeps taking writes. `docker compose start node2` brings the machine back as an
existing member: same `node_id`, same stores. There is no restart policy on
purpose: exit 78 means an operator must act.

**Supersede a writer.** `parosctl set-leader acme/orders --new 8` takes the journal;
the first leader's writes and truncations are refused from then on
(`superseded`, exit 3), and the new leader's `truncate --up-to N --leader 8`
applies.

**Lose a disk.** Two ways, both refused:

- *the stores, not the identity* (`docker compose run --rm --entrypoint sh
  node2 -c 'rm -rf /var/lib/paros/journals'` while `node2` is stopped): the next
  start finds stores without their format marker and stops with **amnesia**
  (exit 78). Losing one journal's store alone parks that journal on the machine
  and keeps serving the others.
- *the whole volume* (`docker compose rm -sf node2 && docker volume rm
  paros_node2 && docker compose up -d node2`): the machine comes back as a **new
  machine** with a new `node_id`, and waits. It never rejoins as the old one: an
  `init` sent to it (`docker compose run --rm --entrypoint parosctl parosctl
  --servers node2:4500 init --members members:4500`) finishes the plan the other
  members hold, which names the old machine at that address: the cell keeps
  serving on the two members that kept their disks, and the new machine stays
  idle. Healing the cell around it is reconfiguration onto another machine,
  driven by the tenant coordinator in M9.

**What is not proven in simulation yet.** The journals' protocol, the driver
and the stores are the code the deterministic simulation runs. The machine
phase — formatting an identity, waiting, `cell init` and its decree — is the
library's `run_machine`, the code the simulation's machines run too (#246,
#277). This toy is a demo to run
by hand: no test runs it, and CI only builds its image.

## Without Docker

The same three machines on one host, configured by the environment (each
variable also has its `--flag`, see `parosd --help`), each with no peer:

```sh
for i in 1 2 3; do
  PAROS_LISTEN=127.0.0.1:450$i PAROS_DATA_DIR=node$i parosd &
done
export PAROSCTL_SERVERS=127.0.0.1:4501,127.0.0.1:4502,127.0.0.1:4503
parosctl init --members "$PAROSCTL_SERVERS"
parosctl tenant create acme
parosctl journal create acme orders
parosctl write acme/orders hello world --leader 7
parosctl read acme/orders
```

## Configuration

Environment variables, validated at startup; an unknown `PAROS_*` variable is an
error (exit 2), so a typo never silently keeps a default.

| variable | meaning |
|---|---|
| `PAROS_LISTEN` | `HOST:PORT` the machine binds; may be a wildcard (`0.0.0.0:4500`) |
| `PAROS_ADVERTISE` | `HOST:PORT` its peers and clients dial; may be a name (default: `PAROS_LISTEN`, which must then not be a wildcard) |
| `PAROS_DATA_DIR` | its identity (`machine`) and its stores |
| `PAROS_CLASS` | `storage` (default) or `stateless`; fixed at format |
| `PAROS_CAPACITY` | its capacity, in role slots (default 32): one slot holds one acceptor of one journal |
| `PAROS_ZONE` | its zone, the `paros/zone` tag placement spreads over (#575 (key:value tags)) |
| `PAROS_RACK` | its rack, the `paros/rack` tag (optional) |
| `PAROS_<FIELD>[_MS]` | one override per driver tunable (below) |
| `PAROS_TLS_CERT`, `PAROS_TLS_KEY`, `PAROS_TLS_CA` | its certificate chain, its key and the universe's authority, PEM (`parosctl tls init`): all three turn TLS on for every session it serves and dials; only some is an error |
| `PAROS_TLS_CLIENT_AUTH` | `required` (mutual TLS, the default), `optional` or `off` (server-authenticated TLS): what it asks of a caller |
| `PAROS_HTTP_LISTEN` | `HOST:PORT` of the HTTP endpoint (#366 (metrics)): `/metrics` (Prometheus text), `/healthz` and `/readyz`; none when unset |
| `PAROS_OTLP_ENDPOINT` | an OpenTelemetry collector's OTLP/gRPC URL: spans and metrics are pushed there; none when unset |
| `PAROS_OTLP_TRACES_ENDPOINT`, `PAROS_OTLP_METRICS_ENDPOINT` | one signal's collector, in place of `PAROS_OTLP_ENDPOINT` |
| `PAROS_OTLP_INTERVAL_MS` | how often the metrics are pushed (default `10000`, at least `100`) |
| `RUST_LOG` | the log filter (default `warn,parosd=info`) |

Every address is `HOST:PORT` with an explicit port; the host is an IP or a
name — a Compose service name, say. A machine binds `PAROS_LISTEN` and
advertises `PAROS_ADVERTISE` (#257). It resolves its listen address **once,
at startup**, asking again for up to 30 seconds while the name does not
resolve yet, and exits 2 if it never does. An advertised name stays a name:
the cell plan keeps it, and each peer resolves it when it dials, and again
after a failed delivery. So a machine that comes back at a new IP behind the
same name is reached again with no write. A wildcard `PAROS_LISTEN` with no
`PAROS_ADVERTISE` is refused (exit 2): nobody can dial a wildcard.

`parosctl --servers` and `init --members` take names too. List the founding
members by their advertised addresses: a machine takes part in `cell init`
only when the list names its own advertised address. A name that resolves to
several machines (a Compose alias) stands for them all, as their IPs.

## Driver tunables

`parosd` runs `DriverTunables::production()`: a 100 ms tick, a one-second
election base (a follower's timeout is drawn from 1–2 s), and the network
timeouts sized for a link across zones. The values are **reasoned, not
measured** — the benchmark that will tune them is M10's — and each lies inside
the range the simulation draws for its knob; the simulation also runs the
whole profile at once.

Each field has an environment override named after it, `_MS` for a duration:

| variable | production | floor |
|---|---|---|
| `PAROS_TICK_INTERVAL_MS` | 100 | 1 |
| `PAROS_ELECTION_TIMEOUT_BASE` (ticks) | 10 | 2 |
| `PAROS_ELECTION_BACKOFF_DOUBLINGS` | 3 | 2 |
| `PAROS_KEEP_ALIVE_INTERVAL_MS` | 5000 | 1 |
| `PAROS_KEEP_ALIVE_TIMEOUT_MS` | 3000 | 1 |
| `PAROS_CONNECTION_TIMEOUT_MS` | 3000 | 1 |
| `PAROS_DELIVERY_TIMEOUT_MS` | 2000 | 1 |
| `PAROS_READ_RETRY_TICKS` | 20 | 1 |
| `PAROS_MAX_WAIT_MS` (the longest tail wait of a `Read`) | 1000 | 0 |
| `PAROS_MIN_WAIT_MS` (the shortest non-zero tail wait) | 0 | 0 |
| `PAROS_MAX_READ_RECORDS` (records per `Read` page) | 256 | 1 |
| `PAROS_MAX_READ_BYTES` (record bytes per `Read` page) | 65536 | 1 |
| `PAROS_QUARANTINE_TICKS` | 80 | 1 |
| `PAROS_CLIENT_INBOX_CAPACITY` | 256 | 1 |
| `PAROS_PEER_INBOX_CAPACITY` | 1024 | 1 |
| `PAROS_PEER_QUEUE_CAPACITY` | 4096 | 1 |
| `PAROS_DELIVERY_BATCH` | 64 | 24 |
| `PAROS_MATCH_RESEND_TICKS` | 10 | 1 |
| `PAROS_GC_RESEND_TICKS` | 10 | 1 |
| `PAROS_RECONFIGURER_RESEND_TICKS` | 10 | 1 |
| `PAROS_RECONFIGURE_TIMEOUT_ELECTIONS` | 4 | 1 |
| `PAROS_RECONFIGURE_BACKOFF_MAX_TICKS` | 20 | 1 |
| `PAROS_PROXY_TAKE_BACK_RESENDS` | 20 | 1 |
| `PAROS_PROXY_ROUND_RESENDS` | 40 | 1 |
| `PAROS_MAX_BATCH_RECORDS` | 1024 | 1 |
| `PAROS_MAX_BATCH_BYTES` | 1048576 | 1 |
| `PAROS_MACHINE_DOWN_AFTER_MS` (the cell coordinator marks a silent machine down, #211) | 10000 | `PAROS_ELECTION_RENEW_MS` + 1 |

An override below its floor, or one that does not parse, stops the process
before it binds (exit 2). The floors are the ones no network makes valid; the
wall-clock ones are the operator's to keep: the election base, in time
(`TICK_INTERVAL_MS × ELECTION_TIMEOUT_BASE`), must outlast a Phase-1 round
trip — a promise is an `fsync` on every acceptor — and the read retry must
outlast a heartbeat round trip. Every field's contract is documented on
`paros::DriverTunables`.

## Exit codes

| code | meaning | what to do |
|---|---|---|
| 0 | stopped on `SIGTERM` / `SIGINT` | nothing |
| 75 | a storage fault crashed the process | restart it: the next boot recovers from the disk |
| 78 | the boot was refused | do **not** restart; the message says why |
| 1 | infrastructure (bind, a name that never resolved) | fix the environment |
| 2 | an invalid configuration: an unknown variable, a tunable below its floor, a name that never resolved | fix the variables |

A refusal is one of:

- **amnesia** — the stores carry no format marker: the disk was lost. A lost
  store never rejoins (its promises went with the disk); wipe the machine to
  start it as a new one, and heal the journal by reconfiguration.
- **lost identity** — stores without a machine record. Wipe the directory.
- **a class change** — the class is fixed at format.
- **another configuration** (#207) — a store was formatted under another
  membership or quorum system. Membership changes go through reconfiguration,
  never through the configuration.

Before you act on a refusal, look at the disk with the machine stopped:
`parosctl store inspect <data-dir>` (#365) prints the machine record and, for
every store, its format marker, promise, chosen index, floor and faulty slots
(`--records` adds every accepted slot). It never writes to the disk.

## parosctl

The servers come from `--servers HOST:PORT,…` (or `PAROSCTL_SERVERS`): a name
that resolves to several machines stands for them all, and each server's node
id — the one a leader hint names it by — is learned from its own `Inspect`
(`ID=HOST:PORT` names it outright, in hex). A journal is a name, `TENANT/JOURNAL` or
`paros://TENANT/JOURNAL`, or its ids, `id:TENANT/JOURNAL` in hex: both ids random and both
required, with no default tenant and no fixed id (#235, #239, `docs/architecture.md` §3.5,
§3.8).

| command | what it does |
|---|---|
| `parosctl init [--members a,b,c] [--patience-ms N]` | runs `cell init` over the founding members (default: the servers) at the first one still idle, retrying `member_unreachable` and `contended` within its patience; waits for the elected cell coordinator (#240), then registers the cell in the fleet directory (#229, #277); resumes an interrupted init, refused on an initialized fleet. Other refusals: `other_cell_init`, `cell_lost`, `not_a_member`, `stateless_member`, `malformed`, `storage` |
| `parosctl tenant create <name> [--survives az\|region]`, `parosctl tenant delete <name>`, `parosctl tenant list` | creates (once; a held name is refused) or removes (resuming an interrupted run) a tenant through the fleet directory and the cell; lists the fleet directory's fleet, cells and tenants (#229) |
| `parosctl journal create <tenant> <name> [--writers single\|multi] [--failures-per-partition F] [--partitions P]`, `parosctl journal delete <tenant> <name>`, `parosctl journal list <tenant>` | creates or deletes a tenant's journal through the tenant coordinator, one idempotent request re-sent until decided (`created`, `deleted`, `name_taken`, `unknown_journal`, `unplaceable`); lists the tenant's control journal (#210) |
| `parosctl write <journal> <record>…` | claims the journal under the leader uuid `--leader` (hex, or `PAROSCTL_LEADER`; drawn at random when absent) unless it leads already (a read finding it the leader is adopted, never re-claimed), then writes at the tail; `--no-claim` writes under `--leader` without claiming, `--seq` at a given position |
| `parosctl read <journal> [--from N] [--limit N] [--wait-ms N]` | reads records to the tail; a truncated range is reported and skipped |
| `parosctl tail <journal> [--from N]` | follows the journal until interrupted |
| `parosctl truncate <journal> --up-to N [--leader U] [--no-claim]` | drops every record below `N`, as the journal's leader (claimed like `write`); a superseded leader is refused |
| `parosctl set-leader <journal> [--new U] [--old U\|none]` | compare-and-sets the leader uuid to `--new` (drawn at random when absent), against `--old` or the leader a read finds; the journal does not refuse a uuid that led before, so never reinstate one |
| `parosctl inspect [--journal J]` | every server's view of journal `J`: leader, ballot, members and quorum system, chosen index, floor, fold, GC watermark, retirable nodes, matchmakers; without `--journal`, every server's own facts (node id, cell, control journals): no journal is inspected by default (#243) |
| `parosctl reconfigure --journal J --members a,b,c [--quorum majority\|flexible:Q1:Q2\|partitions:F:P]` | asks the leader of journal `J` for a new acceptor set; no journal is the default (#439), and a machine that does not serve `J` refuses it (`unknown_journal`) |
| `parosctl retire --journal J --node N [--gc-watermark ROUND.NODE]` | retires a machine from journal `J`, carrying the GC watermark (read from the `inspect` of `J`'s leader when absent); the machine keeps serving its other journals |
| `parosctl bench [--local N [--local-dir DIR]] [--tenant T] [--journals N] [--topology RxC,…] [--clients N] [--duration D] [--ramp D] [--mix CALL=W,…] [--rate R] [--truncate-every D] [--histograms FILE]` | the load generator (#517 (`parosctl bench`), #194 (throughput benchmark)): clients `write`, `read-tail`, `read-start` and `set-leader` the journals `bench-RxC-0`… of tenant `T` (created when missing), one set per `--topology` (`3x1` by default; several run in turn and print side by side, each on a new local cell) by the `--mix` weights, closed loop or open loop at `--rate` calls per second, against the servers or a local cell of N machines; the clients of one journal keep their writes in flight on it at once, through `paros::client::Pipeline` (#597 (several writes in flight per journal)); they print throughput and latency per call each second, then the totals; `--json` prints one report with the commit and the host; `--histograms` writes the raw HDR histograms as JSON lines; the site's bench page |
| `parosctl store inspect <data-dir> [--records] [--scratch DIR]` | offline (#365): reads a stopped machine's disk without `parosd`: the machine record, every journal store (format marker and its `Config`, promise, chosen index, floor, sealed journal state, faulty slots; `--records`: every accepted slot and its records) and every matchmaker set (marker, generation, phase, registrations). Read-only: each store is copied to a scratch directory (`--scratch`, by default a fresh one under the system's temporary directory, removed after) and the copy is opened. Names come from the cell's journals on the disk; exit 3 when the record or a store does not read |
| `parosctl tls init --out DIR --machines a,b,c [--frontends f] [--admins admin] [--universe U] [--days N]` | offline: writes the authority (`ca.crt`, `ca.key`) and a certificate per machine, frontend and admin (`<name>.crt`, `<name>.key`, mode 0600), and prints the variables each process reads; never overwrites a file |
| `parosctl tls ca`, `parosctl tls issue <name> --role machine\|frontend\|admin\|client --ca-dir DIR [--host h] [--ip a]` | one authority, or one more certificate from it (a new machine, a renewal) |
| `parosctl tls show <file>…`, `parosctl tls verify <file> --ca ca.crt` | what a file holds (identity, names, use, validity, days left); checks a certificate as a handshake would |

With TLS, every command takes `--tls-ca ca.crt` (`PAROSCTL_TLS_CA`) and, against servers
that require a client certificate, `--tls-cert admin.crt --tls-key admin.key`
(`PAROSCTL_TLS_CERT`, `PAROSCTL_TLS_KEY`).

Output is text — one line per record or answer, `key=value` details — or, with
`--json`, one JSON document per answer. Diagnostics (a claim made on the way, a
truncation gap) go to stderr.

| exit | meaning |
|---|---|
| 0 | success |
| 3 | answered, and not what was asked: refused, lost, superseded, not served |
| 4 | ambiguous: a write (or another mutation) may or may not have happened |
| 5 | no server decided anything: no leader yet, nothing served |
| 2 | bad arguments |

## Data directory

```text
<data-dir>/machine                     the machine's identity and its cell's plan
<data-dir>/journals/<tenant>/<journal>/  one moonpool-journal per journal it serves (#235)
```

The machine record is written at format (`node_id`, class, capacity, failure
domain) and holds the machine's acceptor state in `cell init`'s decree: a
`PrepareCell` records its promise (`promised <round>/<node>`) before the answer
leaves, and accepting a `FormCell` records its vote (`plan <cell_id>
<round>/<node>`, with the plan's members and journals) only after every journal
store of the plan is formatted — the commit point. Every start after
that is an existing member's: a start never formats a journal store.
