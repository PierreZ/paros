# Running the paros demo

A cell of `parosd` machines you start by hand, write to and read from. This is a demo, not a
test: nothing in CI runs it (CI only builds the image). The walkthrough that explains what each
step does is [`crates/bin/parosd/README.md`](crates/bin/parosd/README.md).

No id is fixed: `init` draws the cell's control journals at random, and the tenant coordinator
draws a journal's id when it creates the journal. A journal is named by its name,
`TENANT/JOURNAL` or `paros://TENANT/JOURNAL`, or by its ids, `id:TENANT/JOURNAL` in hex.
Human output prints ids as short hex, like git; `--json` prints them whole.

## With Docker Compose

Needs Docker with the Compose plugin; nothing else (the image is a plain Rust build).

The Compose file runs with mutual TLS and a pinned root key from the start. It passes a
pre-generated demo key set (`demo/keys/`) as the text itself in variables: the machine
certificate (`PAROS_TLS_*_PEM`), the root public key (`PAROS_ROOT_PUBLIC_KEY_JSON`), and the
admin certificate and token for `parosctl` (`PAROSCTL_TLS_*_PEM`, `PAROS_TOKEN`). No file is
mounted. The demo keys are public: never use them for anything else.

The image can also be built with Nix: `nix build .#image && docker load < result`. Then run
`docker compose up -d` without `--build`; it uses the loaded `paros:local` and pulls nothing.

```sh
# Five machines: node1..node3 (storage, zones a/b/c), storage4, stateless1 (stateless).
# After an upgrade, start over with `docker compose down -v`: a volume an older version wrote
# can make a machine refuse to start (exit 78).
docker compose up -d --build

# Form the cell over node1..node3, register the fleet and the fleet tenant. Prints one line:
#   initialized universe=demo cell=cell-1 coordinator=node1 members=[node1,node2,node3] admitted=[] steps=FormFleet,AddRootKey,AddCell,CellReady
docker compose run --rm init

# A tenant, then a journal in it (#210). The tenant coordinator draws the journal's id and
# picks its members from its durability (`--survive machines=1 --partitions 1` by
# default: three acceptors in one acceptor partition). Prints:
#   created journal=id:T/J members=…
docker compose run --rm parosctl tenant create acme
docker compose run --rm parosctl journal create acme orders
J=acme/orders

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

# Ask which references serve a tenant (#216): the client's first call. With a token (the
# admin token of the demo set here), a resolver checks it and names the tenant's cell first
# (#233 (resolver)); a machine of that cell then answers. Prints:
#   tenant=acme universe=… cell=… machines=node1:4500,node2:4500,node3:4500
docker compose run --rm parosctl resolve acme

# Tenants: created once (a second create of a name is refused).
docker compose run --rm parosctl tenant create globex --survives region
docker compose run --rm parosctl tenant list

# A tenant's journals: a second create of a live name is refused (exit 3), a delete is a
# tombstone, and the list shows both.
docker compose run --rm parosctl journal create acme events --writers multi
docker compose run --rm parosctl journal create acme events
# A journal whose stores sync once per commit instead of twice (#599): fixed at create.
docker compose run --rm parosctl journal create acme clicks --store-durability batched
docker compose run --rm parosctl journal delete acme events
docker compose run --rm parosctl journal list acme

# Change a journal's durability after its create (#214 (tenant modes)): FDB's `configure`.
# The tenant coordinator books the new slots, reconfigures the journal and releases the old
# ones; `parosctl tenant show acme` shows the new acceptors once it is done. Without
# `--journal`, it changes the tenant's default, which a create with no durability takes.
docker compose run --rm parosctl configure --tenant acme --journal orders --partitions 3 --survive machines=0
docker compose run --rm parosctl configure --tenant acme --survive machines=1

# Admit the two idle machines into the cell (#216): each is registered in the cell
# control journal, then records its cell. A re-run prints `unchanged`.
docker compose run --rm parosctl cell add-machine storage4:4500
docker compose run --rm parosctl cell add-machine stateless1:4500

# Every role of a journal (#604 (role counts), #753 (per-journal proxies)): a multi-writer
# journal with two replicas on the storage machines, and a proxy leader and a batcher on the
# stateless machine; and the tenant's frontend there too. `cell show` and `roles` then list
# acceptors, matchmakers, replicas, proxy leaders, batchers and frontends.
docker compose run --rm parosctl journal create acme feed --writers multi
docker compose run --rm parosctl configure --tenant acme --journal feed --replicas 2 --proxies 1 --batchers 1
docker compose run --rm parosctl configure --tenant acme --frontends 1

# The cell, as tables (#399): its machines, its tenants, who holds which role.
docker compose run --rm parosctl machine list
docker compose run --rm parosctl cell show
docker compose run --rm parosctl tenant show acme
docker compose run --rm parosctl roles --tenant acme

# The same tenant in its own scope: only its spread over the cell. Any other view is refused.
docker compose run --rm parosctl --as-tenant acme tenant show acme
docker compose run --rm parosctl --as-tenant acme cell show

# What each machine is (node id, cell, control journals), then its view of the journal.
docker compose run --rm parosctl inspect
docker compose run --rm parosctl inspect --journal "$J"

# Kill a machine and keep writing (a majority is left), then bring it back.
docker compose kill node2
docker compose run --rm parosctl write "$J" still here --leader 8
docker compose start node2

# Maintenance, drain and removal (#359). A window keeps the roles of a machine that goes
# down while it is open (the coordinator re-places them past `machine_replace_after`
# otherwise, #475). `--for 0` ends it. Then drain the admitted machine: once
# `machine list` shows it `drained`, remove it for good.
docker compose run --rm parosctl cell maintenance storage4 --for 30m
docker compose run --rm parosctl cell maintenance storage4 --for 0
docker compose run --rm parosctl cell drain-machine storage4
docker compose run --rm parosctl machine list
docker compose run --rm parosctl cell remove-machine storage4

# Bench (#517, #194): closed-loop clients write the journals bench-0..bench-3 of tenant
# `bench` (both created when missing) and print throughput and latency per call each
# second, then the totals. Ctrl-C stops it and still prints the totals. Add
# `--mix write=80,read-tail=20` for reads that check every byte, `--rate 200` for an open
# loop, `--histograms /tmp/h.json` for the raw histograms. Numbers compare runs on the same
# machine. `paros-bench --local 3` runs without Compose, on a cell in its own process.
docker compose run --rm paros-bench --tenant bench --journals 4 --clients 8 --duration 60s --ramp 10s

# A second cell (#232 (multiple cells)): cell2-a..cell2-c form an idle cell, which then joins
# the universe as cell-2. A tenant goes into it with `--cell`.
docker compose run --rm init-cell2
docker compose run --rm add-cell2
docker compose run --rm parosctl tenant create initech --cell cell-2
docker compose run --rm parosctl journal create initech orders

# The resolver (#233 (resolver)): every machine of both cells serves it. Ask a machine of
# cell-1 with the tenant's own token (`demo/keys/initech.token`): the resolver checks the
# token, sends the client to cell-2, and a machine of cell-2 names the rest. Prints:
#   a resolver of cell … sends tenant "initech" to cell cell-2
#   tenant=initech universe=demo cell=cell-2 machines=cell2-a:4500,cell2-b:4500,cell2-c:4500 …
INITECH=$(cat demo/keys/initech.token)
docker compose run --rm parosctl --token "$INITECH" resolve initech
# The same token for another tenant is refused (exit 3): its scope is initech only.
docker compose run --rm parosctl --token "$INITECH" resolve acme
# A resolver of cell-2 answers the same: any machine of the universe is an entry point.
docker compose run --rm parosctl --servers cell2:4500 --token "$INITECH" resolve initech
docker compose run --rm parosctl tenant list

# Logs, and tear everything down (volumes included).
docker compose logs node1
docker compose down -v
```

Add `--json` after `parosctl` for machine-readable output, e.g.
`docker compose run --rm parosctl --json tenant list`.

The journal `orders` is single-writer: a writer must hold the current leader uuid. The
four calls, the two writer modes and the limits are on the site's journal API page
(`website/zola/content/parosd/journal-api.md`).

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
# 2F+1 listed machines found the cell (`--survive machines=F`, default 1): `init` admits
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

`init` without a token is refused `forbidden` or `invalid_token` (exit 3).

## Through a frontend

Every machine serves the frontend once it is in a cell (#723 (frontend as a parosd role)). A
frontend checks each call's token against the keys in the universe, and resolves the journal by
its name. The tenant coordinator books a tenant's frontends on the `stateless` machines, and
`parosctl resolve` names them:

```sh
parosctl configure --tenant acme --frontends 1
parosctl resolve acme      # frontends=…
parosctl token mint --key /tmp/paros-demo/demo-1.private --role tenant --tenant acme --ttl 1h \
  --subject acme --out /tmp/paros-demo/acme.token
parosctl --frontends "$PAROSCTL_SERVERS" --token-file /tmp/paros-demo/acme.token \
  write paros://acme/orders hello --leader 7
```

## If something goes wrong

- `init` says a member is not up yet: run it again; it resumes, and on a formed fleet it is refused
  (`already_initialized`).
- `parosctl` exit codes: `0` success, `3` refused, `4` ambiguous (run it again), `5` no server
  answered.
- A machine that exits with `78` refuses to start on purpose (amnesia, a changed class, a lost
  identity): do not restart it blindly; see `crates/bin/parosd/README.md`.
- A restarted container can get a new IP. Its peers find it again at its name: each machine
  advertises its service name (`PAROS_ADVERTISE`, #257), and a peer resolves the name when it
  dials.
