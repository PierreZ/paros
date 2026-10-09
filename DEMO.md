# Running the paros demo

A cell of `parosd` machines you start by hand, write to and read from. This is a demo, not a
test: nothing in CI runs it (CI only builds the image). The walkthrough that explains what each
step does is [`crates/parosd/README.md`](crates/parosd/README.md).

No id is fixed: `init` draws the cell's journals at random and prints them, so every command
below takes the journal from `init`'s output (`journals=TENANT/JOURNAL`).

## With Docker Compose

Needs Docker with the Compose plugin; nothing else (the image is a plain Rust build).

```sh
# Five machines: node1..node3 (storage, zones a/b/c), storage4, stateless1 (stateless).
docker compose up -d --build

# Form the cell over node1..node3, register the fleet and the fleet tenant. Prints one line:
#   initialized fleet=… cell=… coordinator=… members=3 control=T/J fleet_control=T/J journals=T/J steps=…
docker compose run --rm init | tee init.out
J=$(sed -n 's/.* journals=\([^ ,]*\).*/\1/p' init.out)

# Write and read the cell's user journal.
docker compose run --rm parosctl write "$J" hello world --leader 7
docker compose run --rm parosctl read "$J"

# Tenants: created once (a second create of a name is refused).
docker compose run --rm parosctl tenant create acme
docker compose run --rm parosctl tenant create globex
docker compose run --rm parosctl tenant list
docker compose run --rm parosctl tenant delete acme

# What each machine is (node id, cell, control journals), then its view of the journal.
docker compose run --rm parosctl inspect
docker compose run --rm parosctl inspect --journal "$J"

# Kill a machine and keep writing (a majority is left), then bring it back.
docker compose kill node2
docker compose run --rm parosctl write "$J" still here --leader 7
docker compose start node2

# Logs, and tear everything down (volumes included).
docker compose logs node1
docker compose down -v
```

Add `--json` after `parosctl` for machine-readable output, e.g.
`docker compose run --rm parosctl --json tenant list`.

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
parosctl init --members "$PAROSCTL_SERVERS" | tee /tmp/paros-demo/init.out
J=$(sed -n 's/.* journals=\([^ ,]*\).*/\1/p' /tmp/paros-demo/init.out)

parosctl write "$J" hello world --leader 7
parosctl read "$J"
parosctl tenant create acme
parosctl tenant list

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
