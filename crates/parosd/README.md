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

From a fresh clone, Docker alone (the image is a plain multi-stage Rust build,
the one build outside Nix):

```sh
docker compose up -d --build        # five machines; each formats and waits
docker compose run --rm init        # forms the cell; prints journals=T/J
docker compose run --rm parosctl write T/J hello world --owner 7
docker compose run --rm parosctl read T/J
```

`docker-compose.yml` runs one cell of five `parosd` machines over three failure
domains, every one the same image configured by `PAROS_*` variables alone:

| machine | class | failure domain | role today |
|---|---|---|---|
| `node1`, `node2`, `node3` | `storage` | `zone-a`, `zone-b`, `zone-c` | the seeds: one network alias, `seeds`, resolves to the three |
| `storage4` | `storage` | `zone-a` | waits for placement (M9, #211, #212) |
| `front1` | `stateless` | `zone-b` | waits for placement (M9) |

**Start and wait.** A machine starts with its listen address, its data
directory (a named volume), its class, capacity and failure domain, and its
rendezvous — here `seeds:4500`, which resolves to the three seeds. On its first
start it **mints its `node_id`** at random and records it in its data directory
(there is no `PAROS_ID`), then waits. A machine never forms a cell on its own.

**Init.** `parosctl init` goes to one seed (`node1`, which every seed's join
list names). That seed identifies every seed, mints the cell's id, records the
plan, forms every other seed and then itself; every seed then serves the **cell
control journal**, **meta's control journal** (the fleet's directory: the
fleet's one cell hosts it) and the toy's journal (the static assignment that
stands in for placement until M9), plain Multi-Paxos over the seeds. **No
frame is fixed**: `init` draws every one, records them in the cell plan, and
prints them (`control=`, `meta=`, `journals=`); afterwards any machine's
`Inspect` names the cell's control journal and meta's, which is how
`parosctl tenant` finds them. Then the first cell coordinator — the lowest seed
id, until the coordinator election of #225 — claims the cell control journal
with `SetLeader(expected_gen = 0)`. Last come the **fleet steps** (#229): the
cell records the fleet's id (minted by `init`) on its side, and meta records
the fleet and adds the cell, `READY`. Every step is idempotent: re-running
`init` resumes an interrupted one, and on an initialized fleet it is refused
(`already_initialized`).

**Tenants.** `parosctl tenant create acme [--pinned]` registers a `users`
tenant in meta (`REGISTERING`, under a random id and a random control journal,
movable unless `--pinned`), has the cell host it, then marks it `READY`; the
CLI never creates an `internal` tenant (meta and the cell tenant, which `init`
registers); `parosctl tenant delete acme` marks it `REMOVING`, has the cell drop
it, then removes it; `parosctl tenant list` prints meta. An interrupted
delete is resumed by running it again. A tenant is created once: a second
`create` of a name meta holds is refused (`name_taken`), and an interrupted
creation stays `REGISTERING` until it is deleted (the coordinator of #225 will
finish it). A tenant's footprint and its
own control journal are not created yet (#210, #225).

**Write and read.** `parosctl` is handed addresses only: `--servers seeds:4500`
stands for every seed, and each server's node id is learned from its own
`Inspect`. The writer claims the journal on its way (`SetLeader` against the
generation it read), then writes at the tail.

**Kill and restart.** `docker compose kill node2` (a storage machine) and
`docker compose kill front1` (a stateless one): the journal keeps a majority and
keeps taking writes. `docker compose start node2` brings the machine back as an
existing member: same `node_id`, same stores. There is no restart policy on
purpose: exit 78 means an operator must act.

**Supersede a writer.** `parosctl set-leader T/J --owner 8` takes the journal;
the first owner's writes and truncations are refused from then on
(`superseded`, exit 3), and the new owner's `truncate --up-to N --owner 8`
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
  --servers node2:4500 init`) is refused (`cell_exists`), since the other seeds
  serve the cell. Healing the cell around it is reconfiguration onto another machine,
  driven by the tenant coordinator in M9.

**What is not proven in simulation yet.** The journals' protocol, the driver
and the stores are the code the deterministic simulation runs. The machine
phase — formatting an identity, waiting, `init` and `FormCell` — and the
uniform start are not in the simulation yet (#216). This toy is a demo to run
by hand: no test runs it, and CI only builds its image.

## Without Docker

The same three seeds on one host, configured by the environment (each variable
also has its `--flag`, see `parosd --help`):

```sh
export PAROS_RENDEZVOUS=127.0.0.1:4501,127.0.0.1:4502,127.0.0.1:4503
export PAROS_STORE_LAYOUT=small
for i in 1 2 3; do
  PAROS_LISTEN=127.0.0.1:450$i PAROS_DATA_DIR=seed$i parosd &
done
export PAROSCTL_SERVERS=$PAROS_RENDEZVOUS
parosctl --servers 127.0.0.1:4501 init      # prints journals=T/J
parosctl write T/J hello world --owner 7
parosctl read T/J
```

## Configuration

Environment variables, validated at startup; an unknown `PAROS_*` variable is an
error (exit 2), so a typo never silently keeps a default.

| variable | meaning |
|---|---|
| `PAROS_LISTEN` | `HOST:PORT` the machine serves at, which its peers dial |
| `PAROS_DATA_DIR` | its identity (`machine`) and its stores |
| `PAROS_CLASS` | `storage` (default) or `stateless`; fixed at format |
| `PAROS_CAPACITY` | its capacity, in placement units (default 1) |
| `PAROS_FAILURE_DOMAIN` | its failure domain label |
| `PAROS_RENDEZVOUS` | the cell's seeds: one name that resolves to them, or a comma-separated join list; recorded at format, re-read on every boot |
| `PAROS_STORE_LAYOUT` | `default` (64 MiB segments) or `small` (laptops, tests) |
| `PAROS_<FIELD>[_MS]` | one override per driver tunable (below) |
| `RUST_LOG` | the log filter (default `warn,parosd=info`) |

Every address is `HOST:PORT` with an explicit port; the host is an IP or a
name — a Compose service name, say. A machine resolves its names **once, at
startup**, asking again for up to 30 seconds while a name does not resolve
yet, and exits 2 if one never does. A rendezvous name resolves to every
address it stands for. `parosctl --servers` takes names too.

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
| `PAROS_READ_POLL_TICKS` | 10 | 0 |
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

## parosctl

The servers come from `--servers HOST:PORT,…` (or `PAROSCTL_SERVERS`): a name
that resolves to several machines stands for them all, and each server's node
id — the one a leader hint names it by — is learned from its own `Inspect`
(`ID=HOST:PORT` names it outright). A journal is `TENANT/JOURNAL`, both random
and both required: there is no default tenant and no fixed id (#235,
`docs/architecture.md` §3.8).

| command | what it does |
|---|---|
| `parosctl init [--patience-ms N]` | forms the cell at the first server, a waiting seed, claims the cell control journal, then registers the cell in meta (#229); resumes an interrupted init, refused on an initialized fleet |
| `parosctl tenant create\|delete <name>`, `parosctl tenant list` | creates (once; a held name is refused) or removes (resuming an interrupted run) a tenant through meta's directory and the cell; lists meta's fleet, cells and tenants (#229) |
| `parosctl write <journal> <record>…` | claims the journal if this owner does not hold it (a read finding it the owner already is adopted, never re-claimed), then writes at the tail; `--owner` (or `PAROSCTL_OWNER`, default 1), `--generation` and `--seq` override |
| `parosctl read <journal> [--from N] [--limit N] [--wait-ms N]` | reads records to the tail; a truncated range is reported and skipped |
| `parosctl tail <journal> [--from N]` | follows the journal until interrupted |
| `parosctl truncate <journal> --up-to N [--owner N] [--generation G]` | drops every record below `N`, as the journal's owner (claimed like `write`); a superseded owner is refused |
| `parosctl set-leader <journal> --owner X [--expected G]` | compare-and-swaps the writer (against the generation read when `--expected` is absent) |
| `parosctl inspect [--journal J]` | every server's view: leader, ballot, members and quorum system, chosen index, floor, fold, GC watermark, retirable nodes, matchmakers |
| `parosctl reconfigure --members 0,1,2 [--quorum majority\|flexible:Q1:Q2\|grid:RxC]` | asks the leader for a new acceptor set |
| `parosctl retire --node N [--gc-watermark ROUND.NODE]` | retires a node, carrying the GC watermark (read from the leader's `inspect` when absent) |

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
<data-dir>/provisioned                 the journal stores it formatted
<data-dir>/journals/<tenant>/<journal>/  one moonpool-journal per journal it serves (#235)
```

The machine record is written at format (`node_id`, class, capacity, failure
domain, rendezvous) and again when the machine forms its cell: the seed running
`init` records the plan as *pending* first (a re-run resumes it), and every seed
records it as *formed* only after every journal store of the plan is formatted
and the provisioning record names them — the commit point. Every start after
that is an existing member's: a start never formats a journal store.
