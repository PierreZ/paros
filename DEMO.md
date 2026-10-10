# Running the paros demo

A cell of `parosd` machines you start by hand, write to and read from. This is a demo, not a
test: nothing in CI runs it (CI only builds the image). The walkthrough that explains what each
step does is [`crates/parosd/README.md`](crates/parosd/README.md).

No id is fixed: `init` draws the cell's control journals at random, and the tenant coordinator
draws a journal's id when it creates the journal. Every command below takes the journal from
`journal create`'s output (`journal=TENANT/JOURNAL`).

## With Docker Compose

Needs Docker with the Compose plugin; nothing else (the image is a plain Rust build).

```sh
# Five machines: node1..node3 (storage, zones a/b/c), storage4, stateless1 (stateless).
docker compose up -d --build

# Form the cell over node1..node3, register the fleet and the fleet tenant. Prints one line:
#   initialized fleet=… cell=… coordinator=… members=3 control=T/J election=T/J fleet_control=T/J steps=…
docker compose run --rm init

# A tenant, then a journal in it (#210). The tenant coordinator draws the journal's id and
# picks its members from the desired mode (`--desired double` by default). Prints:
#   created journal=T/J members=…
docker compose run --rm parosctl tenant create acme
docker compose run --rm parosctl journal create acme orders | tee create.out
J=$(sed -n 's/.*journal=\([^ ]*\).*/\1/p' create.out)

# Write and read the journal. `--leader 7` claims the journal under the leader
# uuid 7 (a uuid is drawn when absent), then writes at the tail.
docker compose run --rm parosctl write "$J" hello world --leader 7
docker compose run --rm parosctl read "$J"
# A page of at most one record; at the tail, wait up to 2 s for a new one (each node caps
# the wait at its PAROS_MAX_WAIT_MS, 1 s by default).
docker compose run --rm parosctl read "$J" --limit 1
docker compose run --rm parosctl read "$J" --from 2 --wait-ms 2000

# Supersede the writer: uuid 8 takes the journal, and uuid 7 is refused from now on (exit 3).
docker compose run --rm parosctl set-leader "$J" --new 8
docker compose run --rm parosctl write "$J" stale --leader 7 --no-claim
docker compose run --rm parosctl write "$J" fresh --leader 8

# Tenants: created once (a second create of a name is refused).
docker compose run --rm parosctl tenant create globex --survives region
docker compose run --rm parosctl tenant list

# A tenant's journals: a second create of a live name is refused (exit 3), a delete is a
# tombstone, and the list shows both.
docker compose run --rm parosctl journal create acme events --mode multi
docker compose run --rm parosctl journal create acme events
docker compose run --rm parosctl journal delete acme events
docker compose run --rm parosctl journal list acme

# Admit the two idle machines into the cell (#216): each is registered in the cell
# control journal, then records its cell. A re-run prints `unchanged`.
docker compose run --rm parosctl cell add-machine storage4:4500
docker compose run --rm parosctl cell add-machine stateless1:4500

# What each machine is (node id, cell, control journals), then its view of the journal.
docker compose run --rm parosctl inspect
docker compose run --rm parosctl inspect --journal "$J"

# Kill a machine and keep writing (a majority is left), then bring it back.
docker compose kill node2
docker compose run --rm parosctl write "$J" still here --leader 8
docker compose start node2

# Logs, and tear everything down (volumes included).
docker compose logs node1
docker compose down -v
```

Add `--json` after `parosctl` for machine-readable output, e.g.
`docker compose run --rm parosctl --json tenant list`.

The journal `orders` is single-writer: a writer must hold the current leader uuid. The
four calls, the two writer modes and the limits are on the site's journal API page
(`web/site/content/parosd/journal-api.md`).

## Locally, without Docker

Three `parosd` processes on `127.0.0.1`, from the repository root. Build through Nix
(`nix develop`, or on the web the `nix shell` line in `AGENTS.md`):

```sh
nix develop --command cargo build --release -p parosd
export PATH=$PWD/target/release:$PATH

# The three machines, each with its own data directory, configured with no peer.
export PAROS_STORE_LAYOUT=small
mkdir -p /tmp/paros-demo
for i in 1 2 3; do
  PAROS_LISTEN=127.0.0.1:450$i PAROS_DATA_DIR=/tmp/paros-demo/node$i \
    parosd > /tmp/paros-demo/node$i.log 2>&1 &
done

# Every parosctl command talks to the three machines.
export PAROSCTL_SERVERS=127.0.0.1:4501,127.0.0.1:4502,127.0.0.1:4503

# `cell init` over the three, every one of them needed (the servers, by default).
parosctl init --members "$PAROSCTL_SERVERS"
parosctl tenant create acme
parosctl journal create acme orders | tee /tmp/paros-demo/create.out
J=$(sed -n 's/.*journal=\([^ ]*\).*/\1/p' /tmp/paros-demo/create.out)

parosctl write "$J" hello world --leader 7
parosctl read "$J"
parosctl tenant list
parosctl journal list acme

# Stop: SIGTERM shuts each machine down cleanly; remove the data to start over.
pkill parosd
rm -rf /tmp/paros-demo
```

## If something goes wrong

- `init` says a member is not up yet: run it again; it resumes, and on a formed fleet it is refused
  (`already_initialized`).
- `parosctl` exit codes: `0` success, `3` refused, `4` ambiguous (run it again), `5` no server
  answered.
- A machine that exits with `78` refuses to start on purpose (amnesia, a changed class, a lost
  identity): do not restart it blindly; see `crates/parosd/README.md`.
- `Cannot assign requested address (os error 99)` on a restarted machine: the demo's IPs are
  pinned in `docker-compose.yml` for this reason; if you still see it, `docker compose down -v`
  and `up -d --build` again to pick up the pinned network.
