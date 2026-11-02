# parosctl

The paros CLI (`publish = false`, #220), over `paros::client`. `main.rs` is the command tree,
`cmd/` holds one module per noun, `support/` what the commands share (#722 (three products)).
`paros-bench` uses the same support. It depends on `paros` and `paros-authz-biscuit`.

## Map

- `src/lib.rs` → `cmd` and `support`, shared with `paros-bench`.
- `src/support/connect.rs` → `Global`, `Runtime`, `Ending` → the options every command takes, the
  client-only RPC runtime, the library client of the servers or the frontends, and how a command
  ends.
- `src/main.rs` → `parosctl` → global options (`--frontends`/`PAROSCTL_FRONTENDS`: `write`, `read`, `tail`, `truncate` and `set-leader` go through the frontends with the token in `PAROS_TOKEN`, #192 (the frontend)), the offline `key`, `token`, `tls` and `store`
  commands (no server, no client runtime), the admin `--token`/`--token-file` (`PAROS_TOKEN`, `PAROS_TOKEN_FILE`) that `init` and `cell` present to a pinned machine (#245 (Biscuit tokens)), server ids discovered from `Inspect`, `init` vs the cell
  commands, exit codes.
- `src/cmd/universe.rs` → `parosctl universe key list|add|remove` (#245 (Biscuit tokens)): the universe entry's root public keys, by label, each change one idempotent write through `paros::client::fleet`; `parosctl universe add-cell --members a,b,c [--name]` and `remove-cell <name>` (#232 (multiple cells)): an idle cell learned with `majority_cell`, added through `FleetSession::add_cell`, and an empty cell removed for good; `parosctl universe status` (each cell's bucket ranges and tenants by bucket and by override), `universe buckets show <name>` and `universe buckets assign <range> <cell>` (#713 (bucket placement)); `read_key` reads a `.public` or `.private` file, shared with `init`.
- `src/cmd/init.rs` → `parosctl init [--members a,b,c] [--patience-ms N] [--root-public-key FILE | --root-public-key-json TEXT] [--idle]` (the universe's first root key, #245; `--idle`: `cell init` alone, a cell with no universe, #232 (multiple cells)): resolves the
  founding members (default: the servers), runs `paros::client::initialize` over them and prints
  what it came to (`cell init`, the elected coordinator, the fleet steps; #229, #240, #246, #277).
- `src/cmd/tenant.rs` → `parosctl tenant create [--cell NAME | --cell-selector SEL] [--reason TEXT]|delete|tag` (`--cell`: a `READY` cell by name, #232 (multiple cells); `--cell-selector`: by tags, #672; both write an override entry, else the tenant goes to its bucket's cell, #713; the result names the cell and the placement) over
  `paros::client::fleet` (`create --tag k:v`, `tag NAME [k:v…]`, #575 (key:value tags)); the session, refusal labels and endings `init` shares.
- `src/cmd/journal.rs` → `parosctl journal create|delete|tag|list` over `paros::client::journals` (#210): the tenant named, found in the fleet directory; one request id per run, re-sent by the library until decided. `create --store-durability ordered|batched` (#599 (durability mode per journal)), `create --tag`, `tag TENANT NAME [k:v…]` (through the frontends with the tenant's token when `--frontends` is given, `journals::tag_through`), `list -l SELECTOR` (`tenant.<key>` reads the tenant's tags) (#575). `parosctl configure --tenant T [--batchers N] [--frontend-count N] [--proxies N]` records the tenant's role counts (#604 (role counts)): only without `--journal`; a count or a mode the command does not name keeps the tenant's (read from its control journal).
- `src/cmd/cell.rs` → `parosctl cell add-machine <addr>` over `paros::client::cell` (#216): registers an idle machine in the cell control journal, then admits it. `parosctl cell drain-machine <machine>` (#474 (journals on admitted machines)): marks a registered machine draining (by name or full id), and the cell coordinator moves every journal off it; a founding member is never drained. `parosctl cell tag-machine <machine> [k:v…]` and `cell tag <cell> [k:v…]` set user tags (#575 (key:value tags)). `parosctl cell remove-machine <machine>` (#359): retires a drained machine that holds no slot, for good. `parosctl cell maintenance <machine> --for <duration>` (or `--zone`; `--for 0` ends it) (#359): opens a maintenance window, and the coordinator re-places no role of a down machine it covers. `parosctl cell configure --control-plane auto|manual [--survive machines=M]` (#685 (special tenants are tenants)): sets the control plane's mode in the cell control journal.
- `src/cmd/cell/balance.rs` → `parosctl cell balance [--pause|--resume]` (#687 (balance a cell)): one line when the cell is balanced; else the move in flight, the repair or the widest gap, and each machine's load; `--pause`/`--resume` set the balancer in the registry. `parosctl status` prints one `balance:` line.
- `src/cmd/resolve.rs` → `parosctl resolve <tenant>` over `paros::client::resolve` (#216): the servers are the entry endpoint; prints the tenant's universe and cell by name (short hex only when the answer names none) and the cell's machines.
- `src/cmd/tls.rs` → `parosctl tls init|ca|issue|show|verify`, offline (PR #509 (mutual TLS)): `init` writes the authority and a leaf per machine, frontend, admin and client in one directory and prints each process's variables; never overwrites, keys 0600; `show` reads with `x509-parser`; `verify` runs rustls's `WebPkiServerVerifier` (name `paros`) and `WebPkiClientVerifier`. The global `--tls-ca`, `--tls-cert`, `--tls-key` (`PAROSCTL_TLS_*`, or the PEM text in `--tls-*-pem`, `PAROSCTL_TLS_*_PEM`) build the client runtime's transport (`paros::net::tls::Files::dialing`). A machine's frontend serves with the machine's own `PAROS_TLS_*` (#723 (frontend as a parosd role)).
- `src/cmd/store.rs` → `parosctl store inspect <data-dir> [--records] [--scratch]` (#365 (offline store inspection)): offline, its own current-thread runtime; `paros::inspect::inspect` over `TokioStorageProvider` with a scratch directory under the system's temporary directory (removed after; refused inside the data directory), names from `paros::inspect::names` through `Labels::of`; exit 3 when the record or a store does not read.
- `src/cmd/key.rs` → `parosctl key generate|show` (#400): root key pairs, offline, entropy from the provider's random source; `<label>.private` (mode 0600) and `<label>.public`, written once; outputs name a key by its label, never its id.
- `src/cmd/token.rs` → `parosctl token mint|derive|inspect` (#400): Biscuit tokens over `paros-authz-biscuit`, offline; a token from the argument, `--token-file` or `PAROS_TOKEN`.
- `src/support/view.rs` (with `cmd/machine.rs` and `cmd/roles.rs`) → `Asker`, `parosctl machine list|show`, `cell list|show`, `tenant list|show`, `roles [--cell|--tenant|--machine]` (#399): `-l SELECTOR` on the lists and the tags in every table and `--json` (`ROLES` and `ZONE` read from `paros/roles` and `paros/zone`, #575 (key:value tags)); one `View` to the servers' cell (a tenant another cell hosts is asked again at the entry endpoint its `other_cell` refusal names, #232 (multiple cells); a tenant's `roles` read off its cell's whole view, so its stateless slots show), rows in name order, printed as a table (`output::table`) or JSON; the global `--as-tenant NAME` asks in one tenant's scope. `machine list|show` also ask every machine for `Load` (#424): the `CPU`, `MACHINE` and `DISK` columns, a `load` block, `no metrics` for a machine that does not answer.
- `src/cmd/status.rs` → `parosctl status [--tenant t] [--cell c]` (#191 (status)): the universe line when the servers' cell hosts its directory, the cell (coordinator, machines available, slots per class) or one tenant (its machines and footprint), and every journal's desired, current and available acceptors with the word the cell shows (`healthy (now degraded)` while hysteresis holds a word back); `--json` adds a `status` block. A tenant's status adds a usage table per journal, summed from every machine's `Load` (`N of M machines metered`; `usage` in `--json`, #366 (metrics)).
- `src/support/labels.rs` → `Labels` → the names every command prints (#399): one admin cell view per command, and the tenant's cell's too when a command follows a tenant there (`fetch_from`, `absorb`); short hex only when no view names an entity.
- `src/support/cells.rs` → `universe`, `tenant_cell`, `universe_label` → commands that cross cells (#232 (multiple cells)): the universe directory read from the servers' cell or through the universe pointer its registry records, then a client over the tenant's cell through the entry endpoint the directory records. `journal`, `configure` and every journal argument by name follow a tenant to its cell; `inspect` names the universe on a cell that does not host it.
- `src/support/help.rs` → `for_people` → the help as people read it: every about and argument help without its issue and section references (`#399`, `§3.5`).
- `src/cmd/data.rs` → one fn per cell command: `write`, `read`, `tail`, `truncate`,
  `set-leader`, `inspect`, `reconfigure`, `retire`.
- `src/support/names.rs` → `JournalRef`, `NodeRef` (a machine name first, then a hex prefix, #399) → a journal argument as a name (`TENANT/JOURNAL`, `paros://…`, resolved through `paros::client::names` with operator rights, or sent as it is to a frontend, #192 (the frontend)) or as ids (`id:TENANT/JOURNAL`, hex prefixes matched among the journals `parosctl` can list); a node id as a hex prefix of a server's id (#239).
- `src/support/output.rs` → `Printer`, `short` → text or one JSON document per answer (`--json`);
  diagnostics to stderr; `table` for the views; text names every entity (#399), JSON carries names and whole ids; short hex only when no name is known.

## Entry points

- `parosctl [--servers HOST:PORT|ID=HOST:PORT,…] [--json] [--timeout-ms N] <command>`; servers
  also from `PAROSCTL_SERVERS`, the writer's leader uuid from `PAROSCTL_LEADER` / `--leader` (hex;
  drawn at random when absent, so a run that names none claims a term of its own, #241).
  `parosctl init` sends `CellInit` to the first founding member still idle.
- `RUST_LOG` filters it (default `error`).

## Exit codes

- `src/main.rs`, `Ending`: `0` success · `3` refused/not served · `4` ambiguous · `5` no server
  answered · `2` bad arguments · `1` no servers configured.

## Local rules

- `parosctl` holds **no client policy**: redirects, retries, claims, ambiguity, reader resume
  and `init`'s patience are `paros::client`'s. A command only parses, calls the library, prints
  and maps an exit code.
- One module per noun in `cmd/`. Code two commands share goes into `support/`.

## Tests & gates

- `cargo nextest run -p parosctl`: the unit tests of the commands and the support.
