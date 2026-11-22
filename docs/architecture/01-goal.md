[Index](README.md) · [02-data-plane.md →](02-data-plane.md)

## 1. Goal

`parosd`: one binary on N machines, started with `docker compose`, serving journals to tenants
over a four-call data plane, healing itself through reconfiguration when a disk or a machine is
lost. One region first, several failure domains; multi-region cells are the last item of M12
(decided on 2026-10-07, #253; folded into M12 on 2026-10-09).

**The cell heals with nobody watching** (the goal of the rework, 2026-11-15). Faults stop, the
machines that died stay dead, and within one bound every journal, every control journal and
every set has each member live again, or reports a typed "cannot place" reason that is true. No
operator call is part of a heal. The simulation judges this with one oracle (rule 8). Nothing
of today is kept for compatibility: store formats, the wire and working features can break
(Pierre, 2026-11-15).

**First-class citizen: paros eats its own food** (decided on 2026-10-09). Journals, leader election and
the Paxos flavors are first-class citizens of paros itself, not only products it offers: every
hard problem paros has is solved with them, never with a side mechanism: state lives in **journals**,
who acts is decided by **leader election** on a journal, and agreement comes from one of the
**Paxos flavors** already in `paros-consensus`: Multi-Paxos for logs, single-decree Paxos for one-shot
decisions (the matchmaker handover's decree, the cell plan at `cell init`), Matchmaker Paxos for
reconfiguration, Compartmentalized Paxos for scale. A new control-plane need is first asked
"which journal, which election, which flavor?" before anything new is built; a cluster file, a
lock service, a gossip protocol or a hand-rolled agreement protocol is the wrong answer.

**The election is the compare-and-set on the journal** (decisions 1 and 13). An election is not
a second journal: the actor that wins `SetLeader(new, old)` on a control journal holds the term
of that journal (rule 3, section 3.3). The same mechanism is the public election API, so a
customer elects a leader the way the coordinators do. The election journal and its library are
deleted.

The control plane is itself stored in journals and coordinated through the same primitive the
tenants use. It has three control planes of one shape, plus the journals they govern (decision
6):

| Level | Who writes its control journal | Its control journal holds |
|---|---|---|
| Universe | the universe coordinator: a term won with `SetLeader` on the universe control journal | the directory: tenant and namespace → cell, the cell entries |
| Cell | the cell coordinator: a term won with `SetLeader` on the cell control journal | the machine registry, admission, liveness, bookings, cell policy |
| Tenant | the tenant coordinator: a term won with `SetLeader` on the tenant control journal | its name and its namespaces, its journals and the configuration typed for each, the scrub |
| Journal | the client leader (single-writer) or any writer (multi-writer) | the data |

The three levels are built from one kit (section 3.3). Each level has its own control journal,
its own fold, its own term uuid and its own duties, and the levels talk only through records with
idempotency ids. The tenant, cell and universe levels run in one process, under the term of the
cell coordinator; a level gets its own actor later with a small step. The data plane needs none
of them to serve.

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

**Every namespace can be transferred** (decided on 2026-10-04 for tenants; the unit became the
namespace with decision 12 on 2026-11-15). The design must be able to move a tenant's namespace
to another cell, the universe tenant included, with no protocol or data-model change and
without stopping what does not move. Nothing may assume a namespace stays in the cell it was
born in. The reason is **rolling out a cell by evacuation**: stand up a new cell, move every
movable namespace onto it, then retire the old cell. The one tenant that never leaves its cell is a
cell's own cell tenant, because it *is* that cell (its registry and its bookings): served from
another cell it would make the cell depend on a foreign control plane, break static stability
and cross the blast-radius boundary. It is still reconfigured within its own cell (a dead machine
replaced, a machine drained), and it retires with its cell. The move is rebuilt in phase 5 on
the routine of rule 4 (decision 3, section 3.7).

Compaction and snapshots are the user's business: paros owns the log, never the state.
`paros-consensus` never compacts, snapshots or verifies anything. The control plane is a tenant and
obeys the same rule: it compacts its own control journals the way any user would, using only
`Write` and `Truncate` (section 3.9). That is the dogfooding rule: the control plane gets no call
a tenant does not have.

The model is the AWS Journal, the replicated log behind Aurora DSQL, MemoryDB and Lambda, as
described in the public sources listed in section 10: a durable, ordered, fenced log that stores
decided outcomes, that a single leader appends to under its leader uuid (or that many writers
append to, unfenced), and that every consumer tails without a second protocol.

The first deliverable is a toy: a local Docker Compose toy cell an operator can initialize, create
a tenant on, write to, read from, break and watch heal (section 9). The homelab is not in scope.

### 1.1 The goals and the root rules

**The eleven goals** (Pierre, 2026-11-22). The goals say what paros is for. Goals 1 to 6 are the
decisions of section 1 above. Goals 7 to 10 say how paros is built (section 4.4, AGENTS.md).
Goal 11 says what an operator gets: Pierre, 2026-11-22, "parosd should be easy to operate,
because of self healing and autoscaling". The scaling is for after the journal v1, whose
controller takes no action from load (journal §J6).

| # | Goal | In short |
|---|---|---|
| 1 | **A journal service for tenants** | Four calls: `Write`, `Read`, `Truncate`, `SetLeader`. A durable, ordered, fenced log. paros owns the log, never the state. |
| 2 | **The cell heals with nobody watching** | After the faults stop, each journal and each set is whole again within one bound, or reports a true reason. |
| 3 | **paros eats its own food** | State is in journals, who acts is an election on a journal, agreement is a Paxos flavor of the core. The control plane gets no call that a tenant does not have. |
| 4 | **One shape at every level** | Universe, cell and tenant are each a control journal, a pure fold and one coordinator. Levels talk only through records. |
| 5 | **The person says what, the system says where** | A person types names and what a journal must survive. The system chooses machines and ids, and refuses what it cannot place. |
| 6 | **Everything can move** | A namespace moves to another cell with no change of protocol, and a cell is rolled out by evacuation. |
| 7 | **Layers that stand alone** | Each layer is a crate that a program that is not paros can use alone. A layer depends only on the layers below it. The core has no tenant. |
| 8 | **Every role is a pure actor** | A deterministic step, persist before send, a refusal is data. I/O is only in adapters. So each layer has its model, its walk and its trace check. |
| 9 | **Proved before built** | No code before its TLA+ model, and no excuse in simulation. |
| 10 | **Everything simulates, and it runs in the browser** | Every layer, and the whole service, runs in the deterministic simulation. The same code compiles to wasm and runs in a browser. |
| 11 | **Easy to operate** | The service operates itself: it heals and it scales with no person in the loop. What a person would do by hand, a component does. Each component has one duty, and offers it as a call. |

**How goal 11 reads in a design** (Pierre, 2026-11-22). When a design leaves a task to a
person, ask which component can do it. A component that does such a task observes the system
and acts on it; it has one duty; and it offers that duty as a call, so that another component
or a tool can use it. The grid controller of a journal is one example (journal §J6): it reads
the measures of its roles and reshapes the journal. The examples are not the goal.

**The eight root rules** are the consequences of these goals in the service. Each one replaced a
stack of fixes of the code at the tag `v0.2.0-alpha.1`: rule 3 serves goals 3 and 4; rules 2, 4,
5, 6 and 8 serve goal 2; rule 7 serves goal 5; rule 1 serves goal 6.

Every change obeys these rules (the rework plan of 2026-11-15, decisions 1 to 35). A fix that
needs an exception to one of them is a sign that the rule is wrong, and the rule goes back to its
TLA+ model. Each rule is told in full in the chapter named beside it; this list is the index.

| # | Root rule | In full | What it replaces |
|---|---|---|---|
| 1 | **Seal the latest configuration.** At most two configurations matter for a journal, and a campaign registers nothing. | Section 4.1 | The retire fence stack: six mechanisms added in eight days |
| 2 | **Sets never shrink.** A set has a durable size, and the caller names its members. A failure never changes the size; only a recorded resize does. | Sections 3.3, 4.1 | "The members that answered the freeze become the set" |
| 3 | **A small authority, one writer.** A term needs one quorum, and only the coordinator writes its control journal. | Section 3.3 | The election journal on the path to a term; admin commands that fence the coordinator |
| 4 | **Recovery rebuilds.** Each term rebuilds its view from fresh proof and drives one routine to the target. | Section 3.3 | Five step machines, repairs in place, stale state taken as fact |
| 5 | **A machine is an id.** An address is a hint, and "gone" is a recorded state. | Section 3.2 | A new address written through the journal the machine serves; dead ids that stay for ever |
| 6 | **One timing model.** One backoff type, and timeouts from measured round trips. | Section 3.2 | A local backoff rule for each red; fixed guesses |
| 7 | **The person types, the cell places.** A person types what a journal must survive and its counts. The cell chooses the machines and refuses what it cannot place. | Section 3.4 | Durability frozen at create, a count that is cut in silence, `--layout`, a tenant default that journals follow, policy from each process's environment |
| 8 | **One quiescence oracle.** Faults stop, dead machines stay dead, and the cell must be healthy within one bound. | Section 6 | A window for each feature, 32 excuses, a harness that plays the operator |

**Which rules a model must still prove.** Rules 1 to 6 change a liveness or a reconfiguration
rule, so each one is **Not proved** until the TLA+ gate of its phase is green (section 7):

| Rule | Model | Gate |
|---|---|---|
| 1 Seal the latest configuration | `config-register` (new, from `cas-register`) | phases 1 and 2 |
| 2 Sets never shrink | `handover` (extends `handover-gc`) | phases 1 and 2 |
| 3 A small authority, one writer | `cell-control` (new) | phase 3 |
| 4 Recovery rebuilds | `cell-control`: every set returns to its target or reports that it cannot place | phase 4 |
| 5 A machine is an id | `cell-control` with a machine wiped and admitted again | phase 5 |
| 6 One timing model (pre-vote) | `election` (new, small) | phase 6 |

Rules 7 and 8 change no protocol rule: rule 7 is proved by its refusals in the simulation, and
rule 8 is the oracle itself. The read on a full grid (section 3.4) and the new move between
cells (section 3.7) are also not proved, each by its own model.

**The proof rule.** A liveness or reconfiguration rule gets a TLA+ model before it gets code. The
model has a `LiveSpec` with fairness, a property of the form "it ends" or "it comes back", and a
control that must fail with the rule off. TLC finds a cycle with no clock; the simulation then
proves the code. A trace from TLC becomes a red unit test and a scenario before the fix, as
`AGENTS.md` already says.

**FoundationDB is the first reference** (Pierre, 2026-11-16). Before a choice on how paros
handles a failure, a missing machine, an operator command or a configuration, its author reads
how FoundationDB does it, in its source and its documentation, and the choice cites that answer.
FoundationDB is not a law: when paros differs, the text says why. `AGENTS.md` holds this working
rule and the rule "never guess" for every session.
