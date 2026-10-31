+++
title = "Admin views"
description = "See the machines, cells, tenants and roles of a cell with parosctl"
weight = 2
+++

`parosctl` shows what a cell holds: its machines, its tenants, its journals and which machine
holds which role. Each view is one request to **one cell**. A founding member of that cell
reads the cell's own journals and answers. A view changes nothing.

## Commands

| Command | What it shows | Scope |
|---|---|---|
| `parosctl machine list [-l <selector>]` | every machine of the cell: name, address, class, zone, capacity, bookings, standing, up, down or slow (#361), tags | admin |
| `parosctl machine show <name>` | one machine and the roles it holds | admin |
| `parosctl cell list [-l <selector>]` | the cells of the universe and their tags | admin |
| `parosctl cell show [<name>]` | the cell, its coordinator, its machines and its tenants | admin |
| `parosctl tenant list [-l <selector>]` | the tenants of the universe and their tags | admin |
| `parosctl tenant show <name>` | a tenant, its journals and the machines they use | admin or that tenant |
| `parosctl universe status` | the cells of the universe, the buckets each holds, and its tenants by bucket and by override | admin |
| `parosctl universe buckets show <name>` | the bucket of a tenant name, the cell that bucket names, and where the tenant lives | admin |
| `parosctl roles [--cell \| --tenant <name> \| --machine <name>]` | who holds each role: the cell coordinator, the acceptors and matchmakers of each journal, the capacity bookings | admin |

| `parosctl journal list <tenant> [-l <selector>]` | the journals of a tenant and their tags | admin |

The output is a table. Add `--json` after `parosctl` to get one JSON document for a script.

## Names

Each command shows names, not ids. A machine has a name (`parosd --name`, or `PAROS_NAME`).
The default name is the host of the advertised address. `parosctl init --universe-name` and
`--cell-name` name the universe and the first cell. A command shows a short hex id only when no
name is known. Hex ids are for local debugging only.

## Placement across cells

A universe places each new tenant in one cell (#713 (bucket placement)):

1. `parosctl tenant create acme --cell cell-2` names the cell. This writes an **override**
   for the tenant. `--reason dedicated` records why, for people.
2. `parosctl tenant create acme --cell-selector env:prod` picks the cell by its tags: of the
   `READY` cells that match, the one with the fewest tenants (#672 (cell tag selector)). This
   also writes an override.
3. Without a cell, the tenant goes to its **bucket**: `CRC16(name) mod 16384`, as Redis
   Cluster does. Each of the 16384 buckets names one cell.

The first cell holds every bucket. A new cell holds no bucket, so it fills only when you give it
some: `parosctl universe buckets assign 8192-16383 cell-2`. Tenants do not move between cells
yet, so a range moves only while no tenant placed by its buckets lives in it. A cell that holds
a bucket is not removed. A dedicated cell is a cell with no bucket: only overrides put tenants
there.

## Tags

Machines, cells, tenants and journals carry `key:value` tags (#575 (key:value tags)). A key
that starts with `paros/` is reserved: paros sets it, for example `paros/class:storage` or
`paros/zone:eu-1a`. All other keys are user tags. These commands set the user tags of one
entity. The tags that you give replace all its user tags. No tags clears them.

```
parosctl cell tag-machine <machine> team:storage
parosctl cell tag <cell> env:prod
parosctl tenant tag <tenant> team:payments
parosctl journal tag <tenant> <journal> app:ledger
```

`tenant create` and `journal create` also take `--tag key:value`, one per tag. An entity has
at most 32 tags. A key has at most 63 bytes and a value at most 255 bytes.

A selector (`-l`) keeps only the entities whose tags match. It is a comma-separated list of
`key` or `key:value` terms, and every term must match. In `journal list`, a
`tenant.<key>` term reads the tags of the tenant, for example `-l tenant.team:payments`.

User tags never change what paros does. A machine's `paros/zone` and `paros/rack` tags come
from its own settings (`PAROS_ZONE`, `PAROS_RACK`), and placement spreads a journal across
zones.

## Scope

The cell filters each answer by the scope of the caller. The caller does not filter.

- **admin** sees all the details.
- **tenant** sees only its own tenant. Of each machine that the tenant uses, it sees the name,
  its `paros/zone` and `paros/rack` tags and whether the machine is up. It does not see the address, the
  capacity or the bookings. The cell refuses all other views with `forbidden`.

Add `--as-tenant <name>` after `parosctl` to ask in the scope of one tenant:

```
parosctl --as-tenant acme tenant show acme
```

Today the caller states its scope. When the frontend checks Biscuit tokens, the token will
give the scope: the `admin` role (with `view.detail`) or the `tenant` role.
