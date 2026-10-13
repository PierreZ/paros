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
| `parosctl machine list` | every machine of the cell: name, address, class, capacity, bookings, standing, up or down | admin |
| `parosctl machine show <name>` | one machine and the roles it holds | admin |
| `parosctl cell list` | the cells of the universe | admin |
| `parosctl cell show [<name>]` | the cell, its coordinator, its machines and its tenants | admin |
| `parosctl tenant list` | the tenants of the universe | admin |
| `parosctl tenant show <name>` | a tenant, its journals and the machines they use | admin or that tenant |
| `parosctl roles [--cell \| --tenant <name> \| --machine <name>]` | who holds each role: the cell coordinator, the acceptors and matchmakers of each journal, the capacity bookings | admin |

The output is a table. Add `--json` after `parosctl` to get one JSON document for a script.

## Names

Each command shows names, not ids. A machine has a name (`parosd --name`, or `PAROS_NAME`).
The default name is the host of the advertised address. `parosctl init --universe-name` and
`--cell-name` name the universe and the first cell. A command shows a short hex id only when no
name is known. Hex ids are for local debugging only.

## Scope

The cell filters each answer by the scope of the caller. The caller does not filter.

- **admin** sees all the details.
- **tenant** sees only its own tenant. Of each machine that the tenant uses, it sees the name,
  the failure domain and whether the machine is up. It does not see the address, the
  capacity or the bookings. The cell refuses all other views with `forbidden`.

Add `--as-tenant <name>` after `parosctl` to ask in the scope of one tenant:

```
parosctl --as-tenant acme tenant show acme
```

Today the caller states its scope. When the frontend checks Biscuit tokens, the token will
give the scope: the `admin` role (with `view.detail`) or the `tenant` role.
