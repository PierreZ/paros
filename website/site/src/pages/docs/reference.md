---
layout: ../../layouts/Doc.astro
kicker: Docs
section: docs/
title: Reference
description: The four calls, the writer modes, durability, the roles, tokens, tags and the parosctl commands on one page.
---

## The four calls

A journal is an ordered log of records. Each record has a position, `seq`. Paxos decides
every call that changes the journal.

| Call | Single-writer journal | Multi-writer journal |
|---|---|---|
| `Write` | `Write(leader_uuid, seq, batch)`: fenced, at an exact position, a retry is safe | `Write(batch)`: the journal gives the position |
| `Read` | `Read(from_seq, limit, wait_ms)`: records, `first_seq`, `next_seq`, leader | the same |
| `Truncate` | `Truncate(leader_uuid, up_to)`: fenced | `Truncate(up_to)`: anyone |
| `SetLeader` | `SetLeader(new, old)`: compare-and-set of the leader uuid | refused |

**Single-writer.** One writer owns the journal through its leader uuid. A superseded writer
gets `Refused` with the current leader and `next_seq`. A retry of an accepted write gets
`Duplicate`. **Multi-writer.** Anyone appends; delivery is at least once. A batcher can
group many writers in one slot. The mode is set at create and never changes.

## Durability

`--survive machines=F --partitions P`, at `journal create` or later with `configure`.

| Option | Meaning |
|---|---|
| `F` (0 to 3) | each acceptor partition has `2F+1` acceptors and survives `F` lost machines |
| `P` (1 or more) | writes spread over `P` partitions; a write is done at a majority of one partition |
| `--store-durability batched` | one sync per commit in place of two, fixed at create |

## Layout

`--layout colocated|compartmentalized`, at `journal create` or later with `configure`.
`colocated` (the default) runs every task in the leader and acceptor processes.
`compartmentalized` books 2 proxy leaders, 2 replicas and, for a multi-writer journal,
2 batchers. A count below changes it, for example `--layout compartmentalized --proxies 3`.

## Roles

| Role | Kind | Count |
|---|---|---|
| acceptor | storage | `2F+1` per partition |
| matchmaker | storage | one set per tenant |
| replica | storage | `--replicas N` |
| proxy leader | stateless | `--proxies N` (0: the leader runs Phase 2) |
| batcher | stateless | `--batchers N`, multi-writer only |
| frontend | stateless | `--frontend-count N` per tenant |
| coordinator | stateless | one per tenant, one per cell |

`PAROS_ROLES` sets the roles a machine takes: `all` (the default), `storage` or `stateless`.
It is set when the machine formats its disk. A restart can widen it to `all`, but not narrow it.
A machine has no capacity limit: the cell places a new role on the least busy machine of a
zone, then on the one with the fewest roles. `parosctl machine list` shows each machine's
roles, the count of roles booked on it, and its CPU and disk busyness. `PAROS_ZONE` sets its
zone; a journal spreads over zones.

The cell also balances itself. When one machine of a zone hosts more roles than another by
more than `balance_spread` (default 1), for `balance_rounds` watch rounds in a row, the cell
coordinator moves one acceptor off it. So a machine that `cell add-machine` admits takes some of
the existing journals, not only the new ones. When the roles are even, a machine that is
`balance_busy_spread` busyness steps of 10 % (default 5) busier than another of its zone gives it
an acceptor, and a machine that leads more than `balance_leader_spread` (default 1) journals more
than a member hands one leadership on, with no election. Every move first lets its new member
catch up (`move_warm_lag`, `move_warm_rounds`), then moves the leader off the machine it leaves.
A move never starts while a machine is down, slow or draining: the cell repairs first.
`parosctl cell balance` shows how the balance stands, in one line when it is balanced;
`--pause` stops new moves and `--resume` starts them again.

## Tokens

paros checks each caller with a [Biscuit](https://www.biscuitsec.org/) token, signed by an
Ed25519 root key of the universe. paros is its own issuer: there is no token server, and keys
and tokens are made offline, with no running universe.

| Role | Can do |
|---|---|
| `admin` | the universe: `init`, cells, machines, tenants, and every journal |
| `tenant` | the data plane, the journals and the view of one tenant, named by its string name |

```sh
# A root key pair: keys/prod-1.private (mode 0600) and keys/prod-1.public.
parosctl key generate --out keys --label prod-1
# A tenant token for acme, valid one hour.
parosctl token mint --key keys/prod-1.private --role tenant --tenant acme --ttl 1h --subject orders-service --out acme.token
# A narrower token, with no key: read only, one journal, an earlier expiry. --seal stops more derives.
parosctl token derive --token-file acme.token --read-only --journal orders --ttl 10m --seal
# Print the blocks, and check the signature.
parosctl token inspect --token-file acme.token --key keys/prod-1.public
```

- **Who checks.** The resolver checks the signature and the scope before it names a tenant's
  cell. The frontend checks each call: its operation, `read` or `write`, the tenant and the
  journal. No frontend or resolver holds a private key. Machines do no check: only frontends
  and peers reach them.
- **Which keys.** The universe holds the root public keys in force (`parosctl init
  --root-public-key`, then `universe key list|add|remove`). A rotation is `key add` of the next
  key, then `key remove` of the old one once its tokens expired. An idle machine pins a key with
  `parosd --root-public-key` and forms or joins a cell only for an `admin` token.
- **Short lives, no revocation.** A token carries an expiry. A refusal is `invalid_token`,
  `expired` or `forbidden` (exit 3).

## Tags

Tenants, journals, machines and cells carry `key:value` tags. A key has at most 63 bytes, a
value at most 255 bytes, and an entity at most 32 tags.

**User tags** are yours: paros stores and shows them, and selects with them, but no code reads
one to decide. A user tag never changes an outcome. **Reserved tags** start with `paros/`. Only
the control plane writes them, through the command that owns each, and paros reads them as
typed policy. A tag command refuses a `paros/` key.

| Reserved tag | On | Set by | Means |
|---|---|---|---|
| `paros/kind` | tenant | `tenant create`, `init` | `users` or `internal` |
| `paros/movable` | tenant | the same | `true` or `false`; a cell's own tenant is `false` |
| `paros/roles` | machine | `PAROS_ROLES` | `all`, `storage` or `stateless` |
| `paros/zone`, `paros/rack` | machine | `PAROS_ZONE`, `PAROS_RACK` | where it runs; a journal spreads over zones |
| `paros/kind` | cell | `universe add-cell` | `regional` or `multi-region` |
| `paros/region` | cell | the same | the region of the cell |

```sh
# Birth tags at create, then the whole set again: the tags given replace the old ones.
parosctl tenant create acme --tag team:pay --tag env:prod
parosctl journal create acme orders --tag kind:events
parosctl journal tag acme orders kind:events owner:checkout
parosctl cell tag-machine node3 disk:nvme
parosctl cell tag cell-2 env:prod
# Select: key, or key:value, comma separated, all must match.
parosctl machine list -l disk:nvme
parosctl journal list acme -l tenant.team:pay,kind:events
# Place a new tenant on a cell by its tags.
parosctl tenant create stark --cell-selector env:prod --reason dedicated
```

- A tag command with no tag clears the user tags.
- A journal stores no copy of its tenant's tags: `tenant.<key>` in a journal selector reads them.
- A tenant writes the tags of its own journals with its token, through the frontends
  (`parosctl --frontends ... journal tag`, Biscuit operation `journal.tag`). An admin can write
  them too.
- `--cell-selector` picks, of the `READY` cells that match, the one with the fewest tenants, then
  the first by name. The universe records only the cell, so the tag does not decide later.

## parosctl

| Command | Does |
|---|---|
| `init --members A,B,C` | form the cell and the universe |
| `cell show`, `cell add-machine`, `cell drain-machine`, `cell remove-machine`, `cell maintenance` | the cell and its machines |
| `cell configure --control-plane auto\|manual [--survive machines=F]` | how the cell's own journals grow (auto by default) |
| `cell balance [--pause\|--resume]` | how the cell's balance stands, or stop and start its moves |
| `tenant create [--tag K:V] [--cell C\|--cell-selector K:V]`, `tenant list [-l SEL]`, `tenant show` | tenants |
| `journal create [--tag K:V]`, `journal delete`, `journal list [-l SEL]` | journals of a tenant |
| `tenant tag`, `journal tag`, `cell tag`, `cell tag-machine` | set the user tags of an entity, whole |
| `configure --tenant T [--journal J]` | durability and role counts |
| `write`, `read`, `tail`, `truncate`, `set-leader` | the data plane |
| `machine list`, `roles`, `status`, `inspect` | what runs where |
| `resolve T` | which machines serve a tenant |
| `key`, `token`, `tls` | keys, tokens and certificates, offline |

Every command takes `--json`. Human output names entities by their string names.
