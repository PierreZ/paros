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

## Roles

| Role | Class | Count |
|---|---|---|
| acceptor | storage | `2F+1` per partition |
| matchmaker | storage | one set per tenant |
| replica | storage | `--replicas N` |
| proxy leader | stateless | `--proxies N` (0: the leader runs Phase 2) |
| batcher | stateless | `--batchers N`, multi-writer only |
| frontend | stateless | `--frontend-count N` per tenant |
| coordinator | stateless | one per tenant, one per cell |

A machine gets its class (`storage` or `stateless`) when it formats its disk.
`PAROS_CAPACITY` (default 32) is its number of role slots. `PAROS_ZONE` sets its zone; a
journal spreads over zones.

## Tokens

paros checks each caller with a Biscuit token, signed by a root key of the universe. An
`admin` token runs the universe; a `tenant` token reads and writes the journals of one
tenant. Keys and tokens are made offline:

```sh
parosctl key generate --out keys --label prod-1
parosctl token mint --key keys/prod-1.private --role tenant --tenant acme --ttl 1h --subject orders-service --out acme.token
parosctl token derive --token-file acme.token --read-only --journal orders
```

## Tags

Tenants, journals, machines and cells carry `key:value` tags: a key of at most 63 bytes, a
value of at most 255 bytes, at most 32 tags on one entity.

- A key that starts with `paros/` is **reserved**. Only the control plane writes it, and paros
  reads it as a typed policy. The tag commands refuse a `paros/` key.
- Every other key is a **user tag**. paros does not read it to decide; it only selects.

| Reserved tag | On | Means |
|---|---|---|
| `paros/kind` | tenant | `internal` or `users` |
| `paros/movable` | tenant | `true` or `false` |
| `paros/class` | machine | `storage` or `stateless`, from the disk format |
| `paros/zone`, `paros/rack` | machine | where it runs; placement spreads over zones |
| `paros/kind` | cell | `regional` or `multi-region` |

```sh
parosctl tenant create acme --tag team:payments
parosctl journal tag acme orders env:prod tier:gold
parosctl journal list acme -l env:prod,tenant.team:payments
parosctl tenant create billing --cell-selector env:prod
```

A tenant sets the user tags of its own journals with its token (Biscuit operation
`journal.tag`). A tag selector names keys or `key:value` pairs, comma separated, and matches
when all of them match; `tenant.<key>` reads the tenant's tag from a journal list.

## parosctl

| Command | Does |
|---|---|
| `init --members A,B,C` | form the cell and the universe |
| `cell show`, `cell add-machine`, `cell drain-machine`, `cell remove-machine`, `cell maintenance`, `cell tag-machine` | the cell and its machines |
| `cell configure --control-plane auto\|manual [--survive machines=F]` | how the cell's own journals grow (auto by default) |
| `tenant create`, `tenant list`, `tenant show`, `tenant tag` | tenants |
| `journal create`, `journal delete`, `journal list`, `journal tag` | journals of a tenant |
| `configure --tenant T [--journal J]` | durability and role counts |
| `write`, `read`, `tail`, `truncate`, `set-leader` | the data plane |
| `machine list`, `roles`, `status`, `inspect` | what runs where |
| `resolve T` | which machines serve a tenant |
| `key`, `token`, `tls` | keys, tokens and certificates, offline |

Every command takes `--json`. Human output names entities by their string names.
