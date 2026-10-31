+++
title = "Cells and the universe"
description = "Run more than one cell, place tenants in cells and find a tenant's cell"
weight = 2
+++

A **cell** is a group of `parosd` machines that run their own Paxos control plane. A
**universe** is the group of cells above them. One cell hosts the **universe directory**: the
journal that records each cell of the universe, each tenant and the cell that hosts the
tenant. Each tenant lives in exactly one cell. All its journals live in that cell too.

## The first cell

`parosctl init` forms the first cell and the universe at the same time. The first cell hosts
the universe directory, and it holds every bucket (see [Placement](#placement)).

```
parosctl init --universe-name demo --cell-name cell-1 --members node1:4500,node2:4500,node3:4500
```

## Add a cell

A second cell joins the universe in two steps:

1. Form the machines into an **idle cell**: a cell with no universe. Send `init --idle` to
   the machines of the new cell.
2. Add the idle cell to the universe. Send `universe add-cell` to a machine of a cell that is
   already in the universe. Give the founding members of the new cell and a name.

```
parosctl --servers cell2:4500 init --idle --members cell2-a:4500,cell2-b:4500,cell2-c:4500
parosctl universe add-cell --members cell2-a:4500,cell2-b:4500,cell2-c:4500 --name cell-2
```

The operator writes the new cell into the universe directory. Then the coordinator of the new
cell records a **universe pointer** in its own cell control journal: the directory's journal
and the machines of the cell that hosts it. Each step resumes when you run the command again.

`parosctl universe status` shows the cells, their state, the buckets each cell holds, and how
many tenants each cell hosts.

## Remove a cell

`parosctl universe remove-cell <name>` removes a cell from the universe. The id of a removed
cell never comes back. The universe refuses to remove a cell when:

- the cell hosts a tenant (`CellNotEmpty`): delete the tenant first;
- the cell holds buckets (`CellHoldsBuckets`): assign them to another cell first;
- the cell hosts the universe directory (`HostsUniverse`).

## Placement

The universe places each new tenant in one cell (#713 (bucket placement), #672 (cell tag
selector)). It uses the first rule that applies:

1. `parosctl tenant create acme --cell cell-2` names the cell. This writes an **override**
   for the tenant. `--reason dedicated` records why, for people.
2. `parosctl tenant create acme --cell-selector env:prod` selects the cell by its tags. Of the
   `READY` cells that match, the universe uses the cell with the fewest tenants. This also
   writes an override.
3. Without a cell, the tenant goes to its **bucket**: `CRC16(name) mod 16384`, as in Redis
   Cluster. Each of the 16384 buckets names one cell.

`parosctl universe buckets show <name>` shows the bucket of a tenant name, the cell that the
bucket names, and the cell of the tenant when it exists.

The first cell holds every bucket. A new cell holds no bucket. It gets tenants only from
overrides until you give it buckets:

```
parosctl universe buckets assign 8192-16383 cell-2
```

Tenants do not move between cells yet. Thus the universe refuses to move a range of buckets
while a tenant that one of these buckets placed lives in another cell (`BucketNotEmpty`).
A **dedicated cell** is a cell with no bucket: only overrides put tenants there.

## Find the cell of a tenant

A client knows only an **entry endpoint**: some machine addresses, or a DNS name in front of
them. It finds the cell of its tenant with `Resolve`, in two hops (#233 (resolver)):

1. The **resolver** checks the token, then reads the universe directory. It answers with a
   redirect: the cell of the tenant and the entry endpoint of that cell.
2. A machine of that cell answers `Resolve`. It names the machines of the cell and the
   frontends of the tenant.

Every `parosd` machine in a cell serves the resolver and the frontend. Thus any machine of
any cell is an entry point to the universe. A `tenant` token resolves only its own tenant,
and an `admin` token resolves any tenant (see [Tokens and root keys](@/parosd/tokens.md)).

```
parosctl --token "$INITECH" resolve initech
#   parosctl: tenant "initech" lives in cell cell-2: resolved through that cell's machines
#   tenant=initech universe=demo cell=cell-2 machines=cell2-a:4500,cell2-b:4500,cell2-c:4500 …
```

The resolver keeps a copy of the universe directory. When it cannot read the directory, it
answers from the copy that it read last. An answer is a hint, not a fact: when a cell refuses
a call, the client resolves again. The redirect never loops. A client follows at most one
more hop when the copy of the resolver was old.

`--zone` gives the zone of the client. The answer then lists the machines and the frontends of
that zone first (#673 (zoned resolve)).

## Commands across cells

`--servers` names the machines of one cell. A `parosctl` command that names a tenant reads the
universe directory and follows the tenant to its cell. These commands work from the machines
of any cell: `journal create`, `configure`, `write`, `read`, `tenant show`, `status --tenant`
and `roles --tenant`. The commands about one cell (`cell show`, `cell add-machine`,
`machine list`, `status`) ask the cell that `--servers` names:

```
parosctl --servers cell2:4500 cell show
```
