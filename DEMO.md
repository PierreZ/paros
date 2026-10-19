# Running the paros demo

A cell of `parosd` machines you start by hand, write to and read from. This is a demo, not a
test: nothing in CI runs it (CI only builds the image). The walkthrough that explains what each
step does is [`crates/parosd/README.md`](crates/parosd/README.md).

No id is fixed: `init` draws the cell's control journals at random, and the tenant coordinator
draws a journal's id when it creates the journal. A journal is named by its name,
`TENANT/JOURNAL` or `paros://TENANT/JOURNAL`, or by its ids, `id:TENANT/JOURNAL` in hex.
Human output prints ids as short hex, like git; `--json` prints them whole.

## With Docker Compose

Needs Docker with the Compose plugin; nothing else (the image is a plain Rust build).

Three people run the demo, and each is a Compose tool service, so the first word of a command
says who acts:

| service | who | holds | reaches |
|---|---|---|---|
| `issuer` | the key holder | the root private key (`/secrets`, its own volume) | nothing: it works offline, with no network |
| `admin` | the operator | an `admin` token | the machines (network `internal`) |
| `acme` | the tenant `acme` | its `tenant` token | the frontend only (network `public`) |

The machines pin the root public key at their start (#245 (Biscuit tokens)), so the key comes
first. The frontend holds no key: it reads the keys `init` writes in the universe. Set the
aliases once per shell.

Bash or zsh:

```sh
alias issuer='docker compose run --rm issuer'
alias admin='docker compose run --rm admin'
alias acme='docker compose run --rm acme'
```

Fish:

```fish
alias issuer 'docker compose run --rm issuer'
alias admin 'docker compose run --rm admin'
alias acme 'docker compose run --rm acme'
```

The commands below work in both shells.

```sh
# 1. The key holder, offline, before any machine exists: the root key pair (the public half
#    goes to the volume the machines read), then the operator's admin token.
issuer key generate --label root-1 --out /secrets --public-out /public
issuer token mint --key /secrets/root-1.private --role admin --subject operator --ttl 12h \
  --out /tokens/admin.token
issuer token inspect --token-file /tokens/admin.token

# 2. Five machines, each pinned to root-1: node1..node3 (storage, zones a/b/c), storage4,
#    stateless1 (stateless), and the frontend.
docker compose up -d --build

# 3. The operator forms the cell over node1..node3 with its token. The universe records root-1,
#    which the frontend will check every token against. Prints one line:
#   initialized universe=… cell=… coordinator=… members=[…] steps=FormFleet,AddRootKey,AddCell,CellReady
admin init --members node1:4500,node2:4500,node3:4500 --root-public-key /public/root-1.public
admin universe key list

# 4. A tenant, then a journal in it (#210). The tenant coordinator draws the journal's id and
#    picks its members from its durability (`--failures-per-partition 1 --partitions 1` by
#    default: three acceptors in one acceptor partition). Prints:
#   created journal=acme/orders members=…
admin tenant create acme
admin journal create acme orders

# 5. A tenant creation returns no token: the key holder mints the tenant's, offline.
issuer token mint --key /secrets/root-1.private --role tenant --tenant acme \
  --subject orders-service --ttl 12h --out /tokens/acme.token

# 6. The tenant writes and reads through the frontend, by name. `--leader 7` claims the
#    journal under the leader uuid 7 (a uuid is drawn when absent), then writes at the tail.
acme write acme/orders hello world --leader 7
acme read acme/orders
# A page of at most one record; at the tail, wait up to 2 s for a new one (each node caps
# the wait at its PAROS_MAX_WAIT_MS, 1 s by default).
acme read acme/orders --limit 1
acme read acme/orders --from 2 --wait-ms 2000

# 7. What a token does not allow (each exit 3). acme's token on another tenant: forbidden.
admin tenant create globex
admin journal create globex ledger
acme read globex/ledger
# The tenant narrows its own token, with no key: read-only, journal `orders` only.
acme token derive --token-file /tokens/acme.token --read-only --journal orders \
  --out /tokens/acme-ro.token
acme token inspect --token-file /tokens/acme-ro.token
acme --token-file /tokens/acme-ro.token read acme/orders
acme --token-file /tokens/acme-ro.token write acme/orders nope --leader 7
# A tenant token cannot put a machine in the cell: the pinned machine refuses it.
admin --token-file /tokens/acme.token cell add-machine storage4:4500
# And the tenant cannot reach a machine at all: `node1` does not resolve on its network.
acme --servers node1:4500 inspect

# Supersede the writer: uuid 8 takes the journal, and uuid 7 is refused from now on (exit 3).
acme set-leader acme/orders --new 8
acme write acme/orders stale --leader 7 --no-claim
acme write acme/orders fresh --leader 8

# Ask any machine which references serve a tenant (#216): the client's first call. Prints:
#   tenant=acme universe=… cell=… machines=node1:4500,node2:4500,node3:4500
admin resolve acme

# Tenants: created once (a second create of a name is refused).
admin tenant create initech --survives region
admin tenant list

# A tenant's journals: a second create of a live name is refused (exit 3), a delete is a
# tombstone, and the list shows both.
admin journal create acme events --writers multi
admin journal create acme events
# A journal whose stores sync once per commit instead of twice (#599): fixed at create.
admin journal create acme clicks --store-durability batched
admin journal delete acme events
admin journal list acme

# Change a journal's durability after its create (#214 (tenant modes)): FDB's `configure`.
# The tenant coordinator books the new slots, reconfigures the journal and releases the old
# ones; `admin tenant show acme` shows the new acceptors once it is done. Without
# `--journal`, it changes the tenant's default, which a create with no durability takes.
admin configure --tenant acme --journal orders --partitions 3 --failures-per-partition 0
admin configure --tenant acme --failures-per-partition 1

# Admit the two idle machines into the cell (#216), with the admin token: each is registered
# in the cell control journal, then records its cell. A re-run prints `unchanged`.
admin cell add-machine storage4:4500
admin cell add-machine stateless1:4500

# The cell, as tables (#399): its machines, its tenants, who holds which role.
admin machine list
admin cell show
admin tenant show acme
admin roles --tenant acme

# The same tenant in its own scope: only its spread over the cell. Any other view is refused.
admin --as-tenant acme tenant show acme
admin --as-tenant acme cell show

# What each machine is (node id, cell, control journals), then its view of the journal.
admin inspect
admin inspect --journal acme/orders

# Kill a machine and keep writing (a majority is left), then bring it back.
docker compose kill node2
acme write acme/orders still here --leader 8
docker compose start node2

# Maintenance, drain and removal (#359). A window keeps the roles of a machine that goes
# down while it is open (the coordinator re-places them past `machine_replace_after`
# otherwise, #475). `--for 0` ends it. Then drain the admitted machine: once
# `machine list` shows it `drained`, remove it for good.
admin cell maintenance storage4 --for 30m
admin cell maintenance storage4 --for 0
admin cell drain-machine storage4
admin machine list
admin cell remove-machine storage4

# Rotate the root key: add the next key, sign new tokens with it, then remove the old one once
# its tokens expired. A token file is written once: remove the old one first.
issuer key generate --label root-2 --out /secrets --public-out /public
admin universe key add /public/root-2.public
docker compose run --rm --entrypoint rm issuer /tokens/acme.token
issuer token mint --key /secrets/root-2.private --role tenant --tenant acme \
  --subject orders-service --ttl 12h --out /tokens/acme.token
acme write acme/orders rotated --leader 8
admin universe key remove root-1

# Bench (#517, #194): closed-loop clients write the journals bench-3x1-0..bench-3x1-3 of tenant
# `bench` (both created when missing) and print throughput and latency per call each
# second, then the totals. It runs as the operator, straight to the machines. Ctrl-C stops
# it and still prints the totals. Add `--mix write=80,read-tail=20` for reads that check
# every byte, `--rate 200` for an open loop, `--histograms /tmp/h.json` for the raw
# histograms. Numbers compare runs on the same machine. `parosctl bench --local 3` runs
# without Compose, on a cell in its own process.
admin bench --tenant bench --journals 4 --clients 8 --duration 60s --ramp 10s

# Logs, and tear everything down (volumes included: the keys and the tokens too).
docker compose logs node1
docker compose down -v
```

Add `--json` after the service name for machine-readable output, e.g. `admin --json tenant list`.

The journal `orders` is single-writer: a writer must hold the current leader uuid. The
four calls, the two writer modes and the limits are on the site's journal API page
(`website/zola/content/parosd/journal-api.md`).

A rebuilt image does not read volumes an older build wrote (no upgrades until M13, #363
(upgrades)). If every machine exits 78 with `machine record: ...`, run `docker compose down -v`
first, then start again from step 1. A machine that exits 2 at its start with
`PAROS_ROOT_PUBLIC_KEY /public/root-1.public: No such file` started before step 1: run step 1,
then `docker compose up -d` again.

## Locally, without Docker

Three `parosd` processes on `127.0.0.1`, from the repository root. Build through Nix
(`nix develop`, or on the web the `nix shell` line in `AGENTS.md`):

```sh
nix develop --command cargo build --release -p parosd
export PATH=$PWD/target/release:$PATH

# The three machines, each with its own data directory, configured with no peer.
mkdir -p /tmp/paros-demo
for i in 1 2 3; do
  PAROS_LISTEN=127.0.0.1:450$i PAROS_DATA_DIR=/tmp/paros-demo/node$i \
    parosd > /tmp/paros-demo/node$i.log 2>&1 &
done

# Every parosctl command talks to the three machines.
export PAROSCTL_SERVERS=127.0.0.1:4501,127.0.0.1:4502,127.0.0.1:4503

# `cell init` over the three, every one of them needed (the servers, by default). The first
# 2F+1 listed machines found the cell (`--failures-per-partition`, default 1): `init` admits
# any others, and the control journals keep three acceptors.
parosctl init --members "$PAROSCTL_SERVERS"
parosctl tenant create acme
parosctl journal create acme orders | tee /tmp/paros-demo/create.out
J=$(sed -n 's/.*journal=\([^ ]*\).*/\1/p' /tmp/paros-demo/create.out)

parosctl write "$J" hello world --leader 7
parosctl read "$J"
parosctl tenant list
parosctl journal list acme

# A second cell (#232 (multiple cells)): one more machine, formed as an idle cell (no
# universe), then added to the universe the first cell hosts. A tenant goes into it with
# `--cell`, and any machine resolves it there.
PAROS_LISTEN=127.0.0.1:4504 PAROS_DATA_DIR=/tmp/paros-demo/node4 \
  parosd > /tmp/paros-demo/node4.log 2>&1 &
parosctl --servers 127.0.0.1:4504 init --idle --members 127.0.0.1:4504
parosctl universe add-cell --members 127.0.0.1:4504 --name second
parosctl tenant create globex --cell second
parosctl resolve globex
# A cell leaves the universe only once it hosts no tenant; its id never comes back.
parosctl tenant delete globex
parosctl universe remove-cell second

# Stop: SIGTERM (or Ctrl-C) stops each machine cleanly: it hands its leaderships on first.
# Remove the data to start over.
pkill parosd
rm -rf /tmp/paros-demo
```

## With mutual TLS

The same three machines, with every session encrypted and every caller proving its
certificate. One command makes every file:

```sh
parosctl tls init --out /tmp/paros-demo/tls --machines 127.0.0.1 --universe demo
# Prints the files and the variables to set. Then each machine:
export PAROS_TLS_CERT=/tmp/paros-demo/tls/127.0.0.1.crt
export PAROS_TLS_KEY=/tmp/paros-demo/tls/127.0.0.1.key
export PAROS_TLS_CA=/tmp/paros-demo/tls/ca.crt
# ...start the three parosd as above, then talk to them as the admin:
export PAROSCTL_TLS_CA=/tmp/paros-demo/tls/ca.crt
export PAROSCTL_TLS_CERT=/tmp/paros-demo/tls/admin.crt
export PAROSCTL_TLS_KEY=/tmp/paros-demo/tls/admin.key
parosctl init --members "$PAROSCTL_SERVERS"
parosctl tls show /tmp/paros-demo/tls/admin.crt
```

The three local machines share one certificate here because they share one host. On real
hosts, name each machine (`--machines parosd-1,parosd-2,parosd-3`) and give each its own
files. A `parosctl` without a certificate is refused at the handshake; set
`PAROS_TLS_CLIENT_AUTH=optional` on the machines to admit it.

## With root keys and tokens

The same three machines, pinned to a root key (#245 (Biscuit tokens)). Each idle machine then
forms a cell or joins one only for an `admin` token, and the universe keeps the key for every
frontend. Make the key and the token offline:

```sh
parosctl key generate --out /tmp/paros-demo --label demo-1
parosctl token mint --key /tmp/paros-demo/demo-1.private --role admin --ttl 1h \
  --subject operator --out /tmp/paros-demo/admin.token
# Then each machine pins the public half:
export PAROS_ROOT_PUBLIC_KEY=/tmp/paros-demo/demo-1.public
# ...start the three parosd as above, then initialize with the token and the key:
parosctl --token-file /tmp/paros-demo/admin.token init --members "$PAROSCTL_SERVERS" \
  --root-public-key /tmp/paros-demo/demo-1.public
parosctl universe key list
```

A rotation is two commands. Add the next key, and remove the old one when its tokens expired:

```sh
parosctl key generate --out /tmp/paros-demo --label demo-2
parosctl universe key add /tmp/paros-demo/demo-2.public
parosctl universe key remove demo-1
```

`init` without a token is refused `forbidden` or `invalid_token` (exit 3). A frontend reads the
keys in the universe, so `PAROS_FRONTEND_ROOT_PUBLIC_KEY` is optional.

## If something goes wrong

- `init` says a member is not up yet: run it again; it resumes, and on a formed fleet it is refused
  (`already_initialized`).
- `parosctl` exit codes: `0` success, `3` refused, `4` ambiguous (run it again), `5` no server
  answered.
- A machine that exits with `78` refuses to start on purpose (amnesia, a changed class, a lost
  identity): do not restart it blindly; see `crates/parosd/README.md`.
- A restarted container can get a new IP. Its peers find it again at its name: each machine
  advertises its service name (`PAROS_ADVERTISE`, #257), and a peer resolves the name when it
  dials.
