[Index](README.md) · [02-data-plane.md →](02-data-plane.md)

## 1. Goal

`parosd`: one binary on N machines, started with `docker compose`, serving journals to tenants
over a four-call data plane, healing itself through reconfiguration when a disk or a machine is
lost. One region first, several failure domains; multi-region cells are the last item of M12
(decided on 2026-10-07, #253; folded into M12 on 2026-10-09).

**First-class citizen: paros eats its own food** (decided on 2026-10-09). Journals, leader election and
the Paxos flavors are first-class citizens of paros itself, not only products it offers: every
hard problem paros has is solved with them, never with a side mechanism: state lives in **journals**,
who acts is decided by **leader election** over a journal, and agreement comes from one of the
**Paxos flavors** already in `paros-core`: Multi-Paxos for logs, single-decree Paxos for one-shot
decisions (the matchmaker handover's decree, the cell plan at `cell init`), Matchmaker Paxos for
reconfiguration, Compartmentalized Paxos for scale. A new control-plane need is first asked
"which journal, which election, which flavor?" before anything new is built; a cluster file, a
lock service, gossip or a hand-rolled agreement protocol is the wrong answer.

The control plane is itself stored in journals and coordinated through the same election
primitive the tenants use. It has four levels, all built the same way:
one actor elected over an election journal and installed as its control journal's leader with
`SetLeader`, holding no state that journal does not hold (section 3.3).

| Level | Elected actor | Its control journal holds |
|---|---|---|
| Universe | the universe coordinator (the universe tenant's coordinator) | tenant → cell, cell entries |
| Cell | the cell coordinator (the cell tenant's coordinator) | machine registry, capacity bookings |
| Tenant | the tenant coordinator | its name and desired state, its `survives`, the name and kind of its cell, journal names, placement inside capacity granted by the cell |
| Journal | the client leader (single-writer) or any writer (multi-writer) | the data |

A paros deployment is always a universe, and the universe runs from M9. For now it has exactly one
cell, and that cell plays both roles: it hosts the universe tenant (the universe level) and it is an
ordinary cell holding tenants. Every universe behaviour exists and is exercised from M9: `init`
creates the universe, tenants are created through the universe tenant, routing resolves tenant → cell through
the universe directory, registrations are verified on both sides. The answer is always "this cell"
today, but the code path is real and runs in every simulation. A second cell, moves between
cells and a separate resolver are M12 (section 3.7). The test for every design until then: adding
a second cell adds an entry to the universe directory and a routing choice, never a protocol or
data-model change. That is why M9 already carries each cell entry's `kind` and entry endpoint
and each tenant's `survives` (section 3.7): a universe that mixes regional and multi-region cells
adds entries, never fields.

**Every tenant can be transferred** (decided on 2026-10-04): the design must be able to move any
tenant to another cell, the universe tenant and its universe coordinator included, with no protocol or
data-model change and without stopping the tenants that do not move. Nothing may assume a tenant
stays in the cell it was born in. The reason is **rolling out a cell by evacuation**: stand up a
new cell, move every movable tenant onto it, then retire the old cell. The one tenant that never
leaves its cell is a cell's own cell tenant, because it *is* that cell (its registry and its
bookings): served from another cell it would make the cell depend on a foreign control plane,
break static stability and cross the blast-radius boundary, and after a rollout it would describe
machines that no longer exist. It is still reconfigured within its own cell (a dead founding member replaced,
a machine drained), and it retires with its cell; the new cell has its own from its `cell init`. The
moves themselves are M12; M9 carries what they need (section 3.7).

Compaction and snapshots are the user's business: paros owns the log, never the state.
`paros-core` never compacts, snapshots or verifies anything. The control plane is a tenant and
obeys the same rule: it compacts its own control journals the way any user would, using only
`Write` and `Truncate` (section 3.9). That is the dogfooding rule: the control plane gets no call
a tenant does not have.

The model is the AWS Journal, the replicated log behind Aurora DSQL, MemoryDB and Lambda, as
described in the public sources listed in section 10: a durable, ordered, fenced log that stores
decided outcomes, that a single leader appends to under its leader uuid (or that many writers
append to, unfenced), and that every consumer tails without a second protocol.

The first deliverable is a toy: a local Docker Compose toy cell an operator can initialize, create
a tenant on, write to, read from, break and watch heal. The homelab is not in scope, and one
region comes first: multi-region cells are the last item of M12 (decided on 2026-10-07, #253;
folded into M12 on 2026-10-09).
