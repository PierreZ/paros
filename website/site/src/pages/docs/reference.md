---
layout: ../../layouts/Doc.astro
kicker: Docs
section: docs/
title: Reference
description: The four calls, the writer modes, durability, the roles, tokens and the parosctl commands on one page.
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

## parosctl

| Command | Does |
|---|---|
| `init --members A,B,C` | form the cell and the universe |
| `cell show`, `cell add-machine`, `cell drain-machine`, `cell remove-machine`, `cell maintenance` | the cell and its machines |
| `tenant create`, `tenant list`, `tenant show` | tenants |
| `journal create`, `journal delete`, `journal list` | journals of a tenant |
| `configure --tenant T [--journal J]` | durability and role counts |
| `write`, `read`, `tail`, `truncate`, `set-leader` | the data plane |
| `machine list`, `roles`, `status`, `inspect` | what runs where |
| `resolve T` | which machines serve a tenant |
| `key`, `token`, `tls` | keys, tokens and certificates, offline |

Every command takes `--json`. Human output names entities by their string names.
