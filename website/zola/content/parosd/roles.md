+++
title = "Machines and roles"
description = "The two machine classes, the roles each class runs and who places them"
weight = 3
+++

Each `parosd` machine has a **class** and a **capacity**. The cell coordinator and the tenant
coordinator book **slots** on the machines for the roles of each tenant and each journal. The
registry of the cell (its control journal) records each booking. `parosctl roles` and
`parosctl cell show` show them (see [Admin views](@/parosd/admin-views.md)).

## Classes and capacity

A machine gets its class when it formats its data directory, and the class never changes:

- `storage` (the default) runs the roles that keep a durable store.
- `stateless` runs the roles that keep no state on disk.

`PAROS_CAPACITY` (default 32) is the number of role slots of the machine. One slot holds one
role of one journal, one matchmaker set or one tenant. `PAROS_ZONE` and `PAROS_RACK` set the
`paros/zone` and `paros/rack` tags. Placement spreads the roles of a journal across zones.

## The roles

| role | class | what it does | how many |
|---|---|---|---|
| acceptor | storage | votes in Paxos and keeps the accepted records of a journal | `2F+1` per acceptor partition: `--survive machines=F --partitions P` |
| matchmaker | storage | keeps the configurations of the journals of a tenant | one set per tenant, `2F+1` from the largest `F` of the tenant |
| replica | storage | learns the chosen records of a journal on a machine that holds no acceptor of it | `--replicas N` |
| proxy leader | stateless | runs Phase 2 for the leader of a journal | `--proxies N`; 0 runs Phase 2 on the leader |
| batcher | stateless | groups the writes of a multi-writer journal into one batch, then splits the answer per write | `--batchers N`, a multi-writer journal only |
| frontend | stateless | checks the token, finds the journal by its name and forwards the call | `--frontend-count N` per tenant |
| coordinator | stateless | the tenant coordinator: writes the control journal of the tenant and places its roles | one per tenant |

The resolver is not booked. Every machine in a cell serves the resolver and the frontend
endpoints (#723 (frontend as a parosd role), #233 (resolver)). A booked frontend slot only
tells `Resolve` which machines to name for the tenant. See
[Cells and the universe](@/parosd/cells.md#find-the-cell-of-a-tenant).

## Who places what

The **cell coordinator** is elected by the founding members of the cell. It is the one writer
of the cell control journal. It admits machines, watches them, and books the **footprint** of
each new tenant before the cell hosts it: one coordinator slot, the matchmaker set, one
acceptor quorum for data and one quorum for the control journal. A cell without enough free
`storage` slots refuses the tenant (`no_footprint`). When no `stateless` machine has a free
slot, the founding members host the tenant coordinator until one does.

The **tenant coordinator** places the journals of its tenant. At `journal create` it picks
the acceptors from the registered `storage` machines: zone by zone, least-loaded first. Then it
books them before it writes the create. A create the cell has no slot for is refused
(`unplaceable`). In each watch round it also books the counts that `configure` records:

```
parosctl configure --tenant acme --journal feed --replicas 2 --proxies 1 --batchers 1
parosctl configure --tenant acme --frontend-count 1
```

It books one slot per machine, with zones in round robin. A count is capped at the machines
that can take a slot. Thus a small cell places fewer roles, and a cell that grows places the
rest later. The leader of the journal then uses the new proxy and replica counts in its next
round.

## Machines that leave

The coordinators move roles off a machine that leaves:

- `parosctl cell maintenance <machine> --for 30m` opens a window. The roles of the machine
  stay while it is down. `--for 0` ends the window.
- Without a window, the cell coordinator places the roles of a down machine again after
  `machine_replace_after`.
- `parosctl cell drain-machine <machine>` moves every role off the machine. When
  `machine list` shows it `drained`, `parosctl cell remove-machine <machine>` removes it.
- A machine held slow (#361 (slow machines)) gives up the slots that the role counts booked on it.

A draining or removed machine takes no new journal.
