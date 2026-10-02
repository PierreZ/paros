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

## A deployment on a laptop

Every process is handed the same deployment: each role's `ID=HOST:PORT`. The
acceptor pool is the bootstrap membership under a majority.

```sh
D="--node 0=127.0.0.1:4500 --matchmaker 0=127.0.0.1:4600 --replica 1000=127.0.0.1:4700"

# Provision every identity once: format its stores, record it, exit.
parosd provision matchmaker --id 0    --data-dir mm $D
parosd provision node       --id 0    --data-dir n0 $D
parosd provision replica    --id 1000 --data-dir r0 $D

# Start them: every start, the first included, is an existing member's.
parosd matchmaker --id 0    --data-dir mm $D &
parosd node       --id 0    --data-dir n0 $D &
parosd replica    --id 1000 --data-dir r0 $D &

# Write two records to journal 128 — claimed on the way — and read them back
# (from the replica too).
export PAROSCTL_SERVERS=0=127.0.0.1:4500,1000=127.0.0.1:4700
parosctl write 128 hello world --owner 7
parosctl read 128
```

A start never formats: the stores must carry their format marker, and the
configuration they were formatted under must be the one handed in. `parosd proxy --id 0 $D` runs a proxy leader (stateless) when the
deployment names `--proxy 0=…`. `--journal` (repeatable, default `128`) lists
the journals the pool serves; the first is the one the matchmakers, proxies
and replicas serve, every other one is plain Multi-Paxos over the pool.

## Exit codes

| code | meaning | what to do |
|---|---|---|
| 0 | stopped on `SIGTERM` / `SIGINT` | nothing |
| 75 | a storage fault crashed the process | restart it: the next boot recovers from the disk |
| 78 | the boot was refused | do **not** restart; the message says why |
| 1 | infrastructure (bind, address) | fix the environment |
| 2 | an inconsistent deployment on the command line | fix the arguments |

A refusal is one of three:

- **amnesia** — the store carries no format marker: the disk was lost, or the
  identity was never provisioned. A lost identity never rejoins (its promises
  went with the disk); replace it by reconfiguration.
- **already formatted** — `parosd provision` on a data directory that carries a
  provisioning record.
- **another configuration** (#207) — the store was formatted under another
  deployment (another bootstrap membership, quorum system, matchmaker set or
  count). Restore the deployment it was provisioned with; membership changes
  go through reconfiguration, never through the configuration.

## parosctl

The servers come from `--servers ID=HOST:PORT,…` (or `PAROSCTL_SERVERS`); the
id is the node id a leader hint names the server by (a bare `HOST:PORT` takes
its position in the list).

| command | what it does |
|---|---|
| `parosctl write <journal> <record>…` | claims the journal if this owner does not hold it (a read finding it the owner already is adopted, never re-claimed), then writes at the tail; `--owner` (or `PAROSCTL_OWNER`, default 1), `--generation` and `--seq` override |
| `parosctl read <journal> [--from N] [--limit N] [--wait-ms N]` | reads records to the tail; a truncated range is reported and skipped |
| `parosctl tail <journal> [--from N]` | follows the journal until interrupted |
| `parosctl truncate <journal> --up-to N` | drops every record below `N` |
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
<data-dir>/journals/<id>/   one moonpool-journal per journal a node serves
<data-dir>/matchmaker/      a matchmaker's registry
<data-dir>/replica/         a replica's chosen log
<data-dir>/provisioned      the provisioning record
```

## Provisioning

`parosd provision <role>` takes the arguments the role's start takes. It
formats every store the identity keeps — a node's one per journal, a
matchmaker's registry, a replica's log — syncs each, and only then writes the
**provisioning record**, `<data-dir>/provisioned` (the role, the id and the
journals provisioned), atomically. It is never part of a start:

- **provision twice** — refused as *already formatted* (exit 78): the record is
  there.
- **an interrupted provision** (killed before the record) — run it again: a
  store already formatted under the same deployment is left as it is, the rest
  are formatted, and the record lands. A store formatted under another
  deployment is refused.
- **a wiped volume** — the record and the stores went together, and the next
  start is refused as *amnesia*. Provisioning it again would bring a new,
  empty identity under the old id: replace it by reconfiguration instead.
- **another identity's directory** — a start whose role or id is not the
  record's exits 2.

A journal the directory creates on a running node is formatted by the node
itself on its first open and added to the record once its store has booted; a
node killed in between finds the formatted store on its next start and records
it then.
