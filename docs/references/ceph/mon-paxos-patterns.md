# Ceph `mon/Paxos` — durability, recovery & lease patterns

An analysis of [Ceph](https://github.com/ceph/ceph)'s monitor Paxos (`src/mon/Paxos.cc` /
`Paxos.h`), read as a reference for `paros` — a learning **sans-IO Multi-Paxos** library in Rust.

This is the third implementation in our reference library, and it is deliberately a *different
shape* from the other two:

- [`../../analysis/go-raft/etcd-raft-sans-io-patterns.md`](../../analysis/go-raft/etcd-raft-sans-io-patterns.md)
  — the **sans-IO core** model (the architecture paros wants).
- [`../frankenpaxos/`](../frankenpaxos/) — **per-slot Multi-Paxos** + a deterministic simulator.
- **This doc** — a battle-tested, production Paxos that is **single-decree and heavily
  I/O-coupled**. Read it for *content*, not architecture: the **durability/persistence boundary**,
  **recovery of uncommitted values**, and **lease-based reads**. Where it diverges from paros
  (sequential single-decree, inline blocking I/O), that divergence is itself instructive.

Ceph states its own deviations from textbook Paxos right in the header:

```cpp
// src/mon/Paxos.h:166
/**
 * This library is based on the Paxos algorithm, but varies in a few key ways:
 *  1- Only a single new value is generated at a time, simplifying the recovery logic.
 *  2- Nodes track "committed" values, and share them generously (and trustingly)
 *  3- A 'leasing' mechanism is built-in, allowing nodes to determine when it is
 *     safe to "read" their copy of the last committed value.
 */
```

That single value-at-a-time choice is the headline: Ceph decides version *N*, then *N+1*, then
*N+2* — **one proposal in flight at a time**. paros instead targets a *pipelined log of slots*,
each reaching consensus independently. Keep that contrast in mind throughout; §7 makes it explicit.

> Line numbers are from the `main` commit this was written against and will drift; symbol names are
> stable. Paths are relative to the Ceph repo root (`src/mon/Paxos.{cc,h}`).

Each section has three parts:

- **(a) What Ceph does** — verbatim C++ with a `file:line` reference.
- **(b) The principle** — the language-neutral pattern.
- **(c) Maps to sans-IO Multi-Paxos** — what to copy, what to invert, in a Rust core that does zero
  I/O and is driven by the caller (one `step(event)` entry, side effects drained via a `Ready`-style
  struct; a log of slots each holding a chosen value).

Leadership and quorum membership are **external** here — handled by the Monitor's Elector — so this
`Paxos` class assumes a known leader and quorum, much like paros separates election from consensus.

## Table of contents

1. [What's different about Ceph's Paxos](#1-whats-different-about-cephs-paxos)
2. [Persistence boundary + atomic apply-with-commit](#2-persistence-boundary--atomic-apply-with-commit)
3. [Recovery / Phase 1 (collect–last)](#3-recovery--phase-1-collectlast)
4. [Active / Phase 2 (begin–accept–commit)](#4-active--phase-2-beginacceptcommit)
5. [Lease-based reads](#5-lease-based-reads)
6. [State sharing & trim](#6-state-sharing--trim)
7. [Contrast & what to invert](#7-contrast--what-to-invert)

---

## 1. What's different about Ceph's Paxos

### (a) What Ceph does

Beyond the three deviations quoted above, the lifecycle is a small explicit state machine shared by
leader and peons:

```cpp
// src/mon/Paxos.h:207
  enum {
    STATE_RECOVERING,        // Leader/Peon is in Paxos' Recovery state
    STATE_ACTIVE,            // idle; the Peon may or may not have a valid lease
    STATE_UPDATING,          // updating to a new value
    STATE_UPDATING_PREVIOUS, // leader proposing an old value
    STATE_WRITING,           // writing a new commit. readable, but not writeable
    STATE_WRITING_PREVIOUS,  // writing a new commit from a previous round
    STATE_REFRESH,           // leader: refresh following a commit
    STATE_SHUTDOWN
  };
```

`STATE_UPDATING_PREVIOUS` / `STATE_WRITING_PREVIOUS` exist *only* because recovery may force the
leader to re-drive a value left over from a previous leader (see §3).

### (b) The principle

A consensus role is a small state machine with an explicit, enumerated status, and the same code
runs on leader and follower — the role differs by which transitions fire, not by class. Encoding
"am I re-driving an inherited value?" as a distinct state (`*_PREVIOUS`) keeps the recovery path
legible instead of hiding it behind flags.

### (c) Maps to sans-IO Multi-Paxos

paros keeps an explicit per-role status enum, but does **not** copy the single-value-at-a-time
constraint: its core advances *many* slots concurrently. The useful import is the modelling
discipline — one status enum, recovery as a first-class state — not the throughput ceiling. Treat
Ceph as the simplest, lowest-throughput corner of the design space: correct and small, but it tops
out at one decree per round-trip.

---

## 2. Persistence boundary + atomic apply-with-commit

This is the single most valuable thing to take from Ceph.

### (a) What Ceph does

The durable Paxos state is a tiny set of keys plus a contiguous run of versioned value blobs. Ceph's
header draws the on-disk picture:

```cpp
// src/mon/Paxos.h:43
 *  paxos:
 *    first_committed -> 1
 *     last_committed -> 4
 *		    1 -> value_1
 *		    ...
 *		    4 -> value_4
```

The **durable** recovery variables are few (everything else is in-memory soft state):

```cpp
// src/mon/Paxos.h:330
  version_t first_committed;   // lowest version still in the log (trim point)
  version_t last_pn;           // highest proposal number ever generated here
  version_t last_committed;    // version of the last value we know was chosen
  ...
  version_t accepted_pn;       // highest pn we have promised to accept
```

The keystone is that a value is **decoded and applied in the very same store transaction that bumps
`last_committed`** — the value blob *is* an encoded `MonitorDBStore::Transaction`:

```cpp
// src/mon/Paxos.cc:861
  auto t(std::make_shared<MonitorDBStore::Transaction>());
  // commit locally
  t->put(get_name(), "last_committed", last_committed + 1);
  // decode the value and apply its transaction to the store.
  // this value can now be read from last_committed.
  decode_append_transaction(t, new_value);
  ...
  get_store()->queue_transaction(t, new C_Committed(this));
```

Because the k/v store is atomic, `last_committed == 4` *strictly* means versions 1..4 are present and
their effects applied — no torn state, ever.

### (b) The principle

Separate the small **consensus metadata that must be durable before you act** (`accepted_pn`,
`last_committed`, `first_committed`, and the per-version value log) from the large **volatile
bookkeeping** (vote tallies, timers, callback queues). Make the application of a chosen value and the
advance of the commit watermark a *single atomic write*, so the store can never reflect a value
without reflecting its effects (or vice versa). The promise (`accepted_pn`) and any accepted-but-
uncommitted value must hit disk *before* the corresponding reply is sent.

### (c) Maps to sans-IO Multi-Paxos

This *is* the sans-IO persistence contract. paros's core never writes; instead `step()` returns a
`Ready` that says "these entries / this `accepted_pn` must be durable **before** you send the
buffered messages." The caller owns the store and decides when to fsync — and, crucially, can batch.
The atomic apply-with-commit becomes a caller-side invariant the `Ready` documents:

```rust
struct Ready {
    persist: Persist,          // hard state: max_ballot, newly-chosen (slot,value) entries, trim_point
    messages: Vec<Message>,    // MUST NOT be sent until `persist` is durable
    // ...
}
// Per-slot, so many slots persist/commit in one Ready — unlike Ceph's one-at-a-time.
```

The one thing to drop: Ceph folds the *application state machine's* mutations into the same
transaction. paros keeps the SM separate (the caller applies chosen values), so the "atomic" unit is
just consensus metadata + the value blob; SM application is the caller's downstream concern.

---

## 3. Recovery / Phase 1 (collect–last)

### (a) What Ceph does

A freshly-elected leader runs `collect()` (Phase 1 / prepare). Before contacting anyone it checks its
**own disk** for a value it accepted but never saw committed:

```cpp
// src/mon/Paxos.cc:175
  // look for uncommitted value
  if (get_store()->exists(get_name(), last_committed+1)) {
    version_t v = get_store()->get(get_name(), "pending_v");
    version_t pn = get_store()->get(get_name(), "pending_pn");
    if (v && pn && v == last_committed + 1) {
      uncommitted_pn = pn;
    } ...
    uncommitted_v = last_committed+1;
    get_store()->get(get_name(), last_committed+1, uncommitted_value);
```

It then picks a fresh, globally-unique proposal number and broadcasts `OP_COLLECT`. PN uniqueness is
achieved by scaling and tagging with the monitor's rank, and the new PN is **persisted before use**:

```cpp
// src/mon/Paxos.cc:1278
  last_pn /= 100;
  last_pn++;
  last_pn *= 100;
  last_pn += (version_t)mon.rank;
  ...
  t->put(get_name(), "last_pn", last_pn);
  get_store()->apply_transaction(t);
```

A peon, in `handle_collect()`, accepts the PN only if it is higher than its promise, and **writes the
promise to disk before replying**:

```cpp
// src/mon/Paxos.cc:266
  if (collect->pn > accepted_pn) {
    accepted_pn = collect->pn;
    ...
    t->put(get_name(), "accepted_pn", accepted_pn);
    ...
    get_store()->apply_transaction(t);   // promise is durable before we answer
  }
```

It also ships back any accepted-but-uncommitted value it has at `last_committed+1`. The leader
aggregates replies in `handle_last()`, adopting the highest-PN uncommitted value the quorum reports:

```cpp
// src/mon/Paxos.cc:554
    if (last->uncommitted_pn) {
      if (last->uncommitted_pn >= uncommitted_pn &&
	  last->last_committed >= last_committed &&
	  last->last_committed + 1 >= uncommitted_v) {
	uncommitted_v = last->last_committed+1;
	uncommitted_pn = last->uncommitted_pn;
	uncommitted_value = last->values[uncommitted_v];
      } ...
```

Once the whole quorum has answered, it either re-proposes that inherited value or, if there's nothing
pending, simply takes the lease and goes active:

```cpp
// src/mon/Paxos.cc:585
      if (uncommitted_v == last_committed+1 && uncommitted_value.length()) {
	state = STATE_UPDATING_PREVIOUS;
	begin(uncommitted_value);          // re-drive the inherited value
      } else {
	extend_lease();                    // clean — become readable
        ...
      }
```

### (b) The principle

This is textbook Paxos Phase 1: a new proposer must *learn* before it may *lead*. Adopting the
highest-numbered accepted value across a quorum guarantees that any value which might already have
been chosen is carried forward, so a new leader can never contradict a prior decision. Two details
make it robust: the promise (`accepted_pn`) is durable before the reply, and proposal numbers are
globally unique and monotonic (scaled counter + node rank), so no two leaders ever collide.

### (c) Maps to sans-IO Multi-Paxos

The algorithm transfers verbatim; the I/O inverts. In paros the caller persists `accepted_pn` and any
`pending` value, and the core *surfaces* the recovered value rather than reading the disk itself:
`handle_promise()` accumulates the quorum's reports and returns "adopt value V at ballot B" in
`Ready`. Ceph runs Phase 1 *once* per leadership term (single decree); paros runs the equivalent per
slot (or batched across the un-chosen tail), since each slot is independent. The PN scheme maps to a
`(round_counter, node_id)` ballot — same uniqueness trick, no disk read in the hot path.

---

## 4. Active / Phase 2 (begin–accept–commit)

### (a) What Ceph does

`begin()` (Phase 2a / accept) stores the proposed value plus its `pending_v`/`pending_pn` breadcrumbs
locally, then sends `OP_BEGIN` to peons:

```cpp
// src/mon/Paxos.cc:656
  auto t(std::make_shared<MonitorDBStore::Transaction>());
  t->put(get_name(), last_committed+1, new_value);
  // note which pn this pending value is for.
  t->put(get_name(), "pending_v", last_committed + 1);
  t->put(get_name(), "pending_pn", accepted_pn);
  ...
  get_store()->apply_transaction(t);
```

A peon's `handle_begin()` accepts iff the PN matches its promise, persists the value, and replies
`OP_ACCEPT`. The leader tallies accepts and commits **only when the entire quorum has accepted**:

```cpp
// src/mon/Paxos.cc:809
  // only commit (and expose committed state) when we get *all* quorum
  // members to accept.  otherwise, they may still be sharing the now
  // stale state.
  if (accepted == mon.get_quorum()) {
    commit_start();
  }
```

Commit is two-phase for durability: `commit_start()` queues the atomic transaction (§2); when it is
durable, the `C_Committed` callback fires `commit_finish()`, which bumps the in-memory watermark and
tells peons to commit:

```cpp
// src/mon/Paxos.cc:911
  last_committed++;
  ...
  MMonPaxos *commit = new MMonPaxos(mon.get_epoch(), MMonPaxos::OP_COMMIT, ...);
  commit->values[last_committed] = new_value;
  commit->last_committed = last_committed;
  mon.send_mon_message(commit, *p);
```

Crucially, `begin()` asserts `new_value.length() == 0` on entry — there is **one proposal in flight**;
the next `begin()` cannot start until this one commits or times out.

### (b) The principle

Phase 2 is propose → collect acks → learn. Two Ceph-specific choices stand out: (1) it waits for the
*whole* quorum to accept, not just a majority — trading availability for the guarantee that every live
member already holds the latest state (which makes leases cheap, §5); and (2) "chosen" is gated on
local *durability*, not just on receiving acks, so a crash right after deciding cannot lose the value.

### (c) Maps to sans-IO Multi-Paxos

Copy the BEGIN→ACCEPT→COMMIT shape and the "don't expose as chosen until durable" discipline. Do
**not** copy the one-in-flight constraint: paros assigns each command a slot and runs Phase 2 for many
slots concurrently, so its `handle_accepted()` tracks a per-`(slot, ballot)` ack set rather than one
global `accepted` set. The all-of-quorum rule is optional — paros can use a majority for availability
and pay a little more for reads (§5). The two-phase commit-then-callback becomes: `step()` returns the
chosen entry in `Ready.persist`; the caller fsyncs, then re-enters the core to release the
`commit`/learn messages — the ordering Ceph enforces with `C_Committed`.

---

## 5. Lease-based reads

### (a) What Ceph does

After a clean round the leader grants a time-bounded **read lease** to peons, so they can serve reads
locally without running consensus:

```cpp
// src/mon/Paxos.cc:987
  lease_expire = ceph::real_clock::now();
  lease_expire += ceph::make_timespan(g_conf()->mon_lease);
  ...
  lease->last_committed = last_committed;
  lease->lease_timestamp = utime_t{lease_expire};
  mon.send_mon_message(lease, *p);
```

A peon adopts the lease only if its `last_committed` matches the leader's, then becomes readable:

```cpp
// src/mon/Paxos.cc:1126
  if (auto new_expire = lease->lease_timestamp.to_real_time();
      lease_expire < new_expire) {
    lease_expire = new_expire;
    ...
  }
  state = STATE_ACTIVE;
```

A read is permitted only while the lease is unexpired (or the node is alone):

```cpp
// src/mon/Paxos.cc:1489
bool Paxos::is_readable(version_t v)
{
  ...
    ret = (mon.is_peon() || mon.is_leader()) &&
      (is_active() || is_updating() || is_writing()) &&
      last_committed > 0 && is_lease_valid();
  ...
}

// src/mon/Paxos.cc:1522
bool Paxos::is_lease_valid()
{
  return ((mon.get_quorum().size() == 1)
	  || (ceph::real_clock::now() < lease_expire));
}
```

### (b) The principle

A lease is a promise from the leader — "no new value will be committed before time *T*" — that lets
any lease-holder serve linearizable reads of `last_committed` with zero messages. It works *because*
Phase 2 waits for the whole quorum (everyone already has the latest value), and it rests on a
**bounded-clock-drift assumption**: comparing `now()` to `lease_expire` is only safe if clocks across
nodes don't diverge more than the lease slack. Cheap reads, bought with a synchrony assumption.

### (c) Maps to sans-IO Multi-Paxos

This is a production alternative to frankenpaxos's quorum reads, and it fits sans-IO beautifully
*because* the core never reads a clock. The core tracks "lease valid through logical deadline T" as
state and exposes `is_readable(at_time) -> bool`; the **caller supplies `now`**. That makes the clock
assumption explicit and, decisively, **testable**: a simulator can drive skewed or jumping clocks and
assert no stale read ever escapes — exactly the deterministic-sim story from the frankenpaxos doc.
Lease grant/renew/expire become `Ready` outputs and timer ticks the caller feeds back in, never inline
`add_event_after` calls.

---

## 6. State sharing & trim

### (a) What Ceph does

A node that is ahead ships the missing committed versions to one that is behind — just a copy of the
value blobs for the gap range:

```cpp
// src/mon/Paxos.cc:353
  version_t v = peer_last_committed + 1;
  // include incrementals
  for ( ; v <= last_committed; v++) {
    if (get_store()->exists(get_name(), v)) {
      get_store()->get(get_name(), v, m->values[v]);
      ...
    }
  }
  m->last_committed = last_committed;
```

The receiver's `store_state()` applies the contiguous run atomically (each blob written *and* its
transaction applied), and discards any now-obsolete uncommitted value:

```cpp
// src/mon/Paxos.cc:420
    t->put(get_name(), "last_committed", last_committed);
    for (auto it = start; it != end; ++it) {
      t->put(get_name(), it->first, it->second);
      decode_append_transaction(t, it->second);
    }
    // discard obsolete uncommitted value?
    if (uncommitted_v && uncommitted_v <= last_committed) {
      uncommitted_v = 0;
      uncommitted_pn = 0;
      uncommitted_value.clear();
    }
```

The log is bounded by trimming the oldest versions and advancing `first_committed`, queued alongside
the next transaction rather than done eagerly:

```cpp
// src/mon/Paxos.cc:1255
  for (version_t v = first_committed; v < end; ++v) {
    t->erase(get_name(), v);
  }
  t->put(get_name(), "first_committed", end);
```

### (b) The principle

Catch-up is just "replay the committed suffix you're missing," and because each value carries its own
state-machine transaction, applying the suffix re-derives state with no separate snapshot mechanism.
The log is kept finite by trimming a prefix and recording the new floor (`first_committed`); doing it
lazily (piggybacked on the next write) amortizes the I/O.

### (c) Maps to sans-IO Multi-Paxos

Catch-up maps to the core emitting "here are versions [a..b] the peer needs" and the receiver getting
"persist+apply these" in `Ready` — no inline disk access. Trimming becomes advisory: the core reports a
*suggested* trim point (`first_committed`); the caller decides when to actually erase, so it can batch
with other writes or skip under load. (Ceph collapses snapshotting into log-replay because values are
SM transactions; paros, keeping the SM at the caller, will likely want an explicit snapshot path once
logs get long — a place it should consult etcd/raft rather than Ceph.)

---

## 7. Contrast & what to invert

Ceph's `mon/Paxos` is correct, small, and proven — but it is the *opposite* of paros's target on two
axes: concurrency and I/O. Keep the algorithm content from §§2–6; invert the rest.

| Dimension | Ceph `mon/Paxos` | paros (sans-IO Multi-Paxos) |
|-----------|------------------|------------------------------|
| Concurrency | **One** proposal in flight; decide *N* then *N+1* (`begin()` asserts no value pending) | Pipelined **log of slots**, each decided independently |
| Log | Single `last_committed` counter + versioned blobs | `BTreeMap<Slot, Value>` + watermark |
| Commit quorum | Waits for **all** of quorum (cheap leases) | Majority by default (availability) |
| Reads | Lease-based, leader-clock | Lease *or* quorum read; **caller supplies the clock** |
| Persistence | Inline blocking `apply_transaction()` everywhere | Core returns `Ready.persist`; **caller** fsyncs & batches |
| Timers | `mon.timer.add_event_after(...)` inline | Caller-driven `tick`; timers are `Ready` outputs |
| Messaging | `mon.send_mon_message(...)` as a side effect | Messages buffered in `Ready`, sent by the caller |
| Control flow | `C_Committed` / `finish_contexts` callbacks | `step(event) -> Ready`, no callbacks |
| SM coupling | Value *is* a store txn; applied with the commit | SM is the caller's; core handles only consensus + opaque values |
| Leadership | External (Monitor Elector) | External (same separation — a point of agreement) |

Concretely, every `get_store()->apply_transaction(...)` (e.g. `Paxos.cc:287`, `:680`, `:764`,
`:1296`, `:453`) and every `mon.timer` / `mon.send_mon_message` / `C_Committed` call is a place where
Ceph *does* I/O that paros instead *describes* and hands back to the caller. The mental translation is
mechanical: **wherever Ceph acts, paros returns a description of the action.**

For the sans-IO architecture itself, the better model remains
[`../../analysis/go-raft/etcd-raft-sans-io-patterns.md`](../../analysis/go-raft/etcd-raft-sans-io-patterns.md);
for the per-slot Multi-Paxos structure, see [`../frankenpaxos/03-multipaxos-core.md`](../frankenpaxos/03-multipaxos-core.md).
Read Ceph for what those under-emphasize: the **durability boundary** (§2), **uncommitted-value
recovery** (§3), and **lease reads** (§5).
