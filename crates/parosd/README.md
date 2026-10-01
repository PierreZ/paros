# parosd

The paros server over Tokio: every role's driver from the `paros` library —
the same code the deterministic simulation tests — on a real network and a
real filesystem. One process runs one role:

| Role | Runs | Durable |
|---|---|---|
| `parosd node` | the acceptors of every journal (`run_journals`) | one store per journal under `--data-dir` |
| `parosd matchmaker` | the configuration registry (`run_matchmaker`) | `--data-dir/registry` |
| `parosd proxy` | a proxy leader's Phase-2 fan-out (`run_proxy`) | nothing |
| `parosd replica` | a learner clients read from (`run_replica`) | `--data-dir/journal-<id>` |

`paros` is a minimal client: `set-leader`, `write`, `read`, `inspect`.

**Status.** A learning project, not for production. What is proven in
simulation is the protocol and the stores' crash behaviour on a simulated
disk; on a real filesystem the stores pass the same contract suites and a
process-crash loop (`crates/paros/src/journal/tokio_fs.rs`). Still to come:
provisioning (#208 — `--first-boot` stands in), the configuration recorded at
format (#207 — today an edited topology is not refused), hostnames (#209 —
addresses are `IP:PORT`), the uniform binary, Compose and the full CLI
(#196).

## On a laptop: a node, a matchmaker and a replica

Every process of a deployment is given the same topology, as flags or as
`PAROS_*` variables (comma-separated lists):

```sh
cargo build -p parosd
export PAROS_NODES=0=127.0.0.1:4500
export PAROS_MATCHMAKERS=0=127.0.0.1:4600
export PAROS_REPLICAS=1000=127.0.0.1:4700
B=target/debug

# First boot: the operator's claim that these identities are new; the
# driver formats each store before anything reads it.
$B/parosd matchmaker --id 0    --data-dir data/mm0 --first-boot &
$B/parosd node       --id 0    --data-dir data/n0  --first-boot &
$B/parosd replica    --id 1000 --data-dir data/r0  --first-boot &
```

Claim the journal (a compare-and-swap on its generation), write, read:

```sh
$B/paros --server 127.0.0.1:4500 set-leader --expected 0 --owner 7
# set-leader won: owner=7 generation=1 first_seq=0 next_seq=0
$B/paros --server 127.0.0.1:4500 write --generation 1 --owner 7 --seq 0 hello world
# write accepted: [0, 2)
$B/paros --server 127.0.0.1:4500 write --generation 1 --owner 7 --seq 0 hello world
# write duplicate: [0, 2)        (a retry is answered by the log)
$B/paros --server 127.0.0.1:4700 read --from 0
# 0	hello
# 1	world
```

Stop everything with `SIGTERM` (each exits 0) and start it again **without**
`--first-boot`: every process is an existing member, the log is intact, the
writer keeps its generation, and a stale writer is refused:

```sh
$B/paros --server 127.0.0.1:4500 write --generation 0 --owner 9 --seq 2 stale
# write refused: owner=7 generation=1 first_seq=0 next_seq=2
```

Start an existing member on an empty directory and it is refused as
amnesiac — it may have promised ballots it no longer remembers — and exits
78. Starting `--first-boot` on a formatted store is refused the same way.

## More shapes

- **Three plain nodes** (no matchmakers): `PAROS_NODES=0=…:4500,1=…:4501,2=…:4502`.
  Pass every node to the client (`--server a,b,c`): a call to a node that is
  not the leader gets no verdict and the client asks the next.
- **Proxy leaders**: `PAROS_PROXIES=0=…:4800` and one `parosd proxy --id 0`.
- **More journals**: `PAROS_JOURNALS=128,129`. The first is the one the
  matchmakers, proxies and replicas serve; every other is plain Multi-Paxos
  over the whole pool. Client calls take `--journal`.
- **A subset bootstrap** (`--bootstrap 0,1,2` of a larger pool) needs
  matchmakers: plain Multi-Paxos never reconfigures.

## Exit codes

| Code | Meaning | Supervisor |
|---|---|---|
| 0 | stopped by `SIGTERM` / `SIGINT` | done |
| 64 | invalid command line or topology | fix the configuration |
| 70 | infrastructure failure (bind, listen) | fix and restart |
| 75 | storage fault: the fail-stop crash | restart |
| 78 | boot refused: the `--first-boot` claim and the store disagree | an operator decides |

Logging follows `RUST_LOG` (default `warn,parosd=info`; `RUST_LOG=info` adds
the driver's events).
