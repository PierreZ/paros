# parosd

The paros daemon: every role of a paros deployment over moonpool's Tokio
providers, with `paros::journal`'s stores on a real filesystem. The drivers and
the stores are the library's — the same code the deterministic simulation runs
— and this crate adds only what a process needs: arguments, a data directory,
a tracing subscriber, signals and exit codes.

## A deployment on a laptop

Every process is handed the same deployment: each role's `ID=HOST:PORT`. The
acceptor pool is the bootstrap membership under a majority.

```sh
D="--node 0=127.0.0.1:4500 --matchmaker 0=127.0.0.1:4600 --replica 1000=127.0.0.1:4700"

# First boot: --first-boot formats every store, exactly once per identity.
parosd matchmaker --id 0    --data-dir mm --first-boot $D &
parosd node       --id 0    --data-dir n0 --first-boot $D &
parosd replica    --id 1000 --data-dir r0 --first-boot $D &

# Claim journal 128, write two records, read them back (from the replica too).
parosd set-leader --server 127.0.0.1:4500 --expected 0 --owner 7
parosd write --server 127.0.0.1:4500 --owner 7 --generation 1 --seq 0 hello world
parosd read  --server 127.0.0.1:4700 --from 0
```

Every later start drops `--first-boot`: the stores must carry their format
marker, and the configuration they were formatted under must be the one handed
in. `parosd proxy --id 0 $D` runs a proxy leader (stateless) when the
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

- **amnesia** — the store carries no format marker: the disk was lost. A lost
  identity never rejoins (its promises went with the disk); replace it by
  reconfiguration.
- **already formatted** — `--first-boot` on a formatted store.
- **another configuration** (#207) — the store was formatted under another
  deployment (another bootstrap membership, quorum system, matchmaker set or
  count). Restore the deployment it was provisioned with; membership changes
  go through reconfiguration, never through the configuration.

The client commands print one `key=value` line per answer (and a `record
seq=… data=…` line per record read) and exit 0 on success, 3 on an answered
but unsuccessful call (refused, lost, not served) and 1 when the server did
not answer.

## Data directory

```text
<data-dir>/journals/<id>/   one moonpool-journal per journal a node serves
<data-dir>/matchmaker/      a matchmaker's registry
<data-dir>/replica/         a replica's chosen log
```

`parosd provision` and a provisioning record outside the stores are #208;
until then the operator's `--first-boot` is the claim.
