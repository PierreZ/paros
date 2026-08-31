# Cooperative leader handoff, restated for paros

Design note for the DPaxos *Leader Handoff* technique (§Relinquishment of "DPaxos:
Managing Data Closer to Users for Low-Latency and Mobile Applications", SIGMOD 2018),
as implemented in `paros-core/src/node/handoff.rs`.

## The idea

There are two reasons leadership changes hands, and only one of them needs an election.

```
failure-driven                         cooperative / planned
──────────────                         ─────────────────────
old leader unavailable                 old leader alive
        ↓                                      ↓
election (ballot bump)                 hands its logical authority on
        ↓                                      ↓
Phase 1 over the log suffix            successor continues Phase 2
        ↓                                      ↓
new leader                             new leader — no Phase 1
```

The sitting leader already *owns* Phase-2 authority, established by a Phase 1 that
already ran. Destroying it and making another process rediscover the same facts is
wasted work when the leader is alive and cooperating. DPaxos's move is to relinquish
the authority — send the successor enough state to keep exercising it — with one rule:

> the old leader relinquishes each authority **at most once**, and once it has, it
> **never exercises that authority again**.

That rule is the whole of the safety argument, because Paxos's Phase-2 safety rests on
*one proposer per ballot*: two nodes proposing different commands under one
`(slot, ballot)` can assemble two different accept quorums, and two values are then
chosen for one slot.

## What a paros leader's authority actually is

Read off `RawNode`, a leadership consists of:

| state | transferred? | why |
| --- | --- | --- |
| `ballot` | **yes** | the authority itself; what acceptors gate on |
| `next_slot` (allocator frontier) | **yes** | keeps "one proposer per ballot" true across a change of node |
| `proposer` (open Phase-2 rounds) | **yes**, as commands | the P2c business the predecessor has not finished |
| chosen-but-unapplied tail | **yes**, as decided facts | the same claim a `Commit` makes |
| `max_promised_ballot` | no — *raised* at the successor | durable; the install's only write |
| `read_floor` | derived (`next_slot - 1`) | identical rule to a fresh leader's fence |
| `heartbeat_seq`, `read_rounds`, `CheckQuorum` window | reset | fresh-leadership state, exactly as `try_become_leader` resets it |
| `chosen` / `accepted` / `chosen_index` / `first_slot` | no | replicated state, not leadership; catch-up carries it |
| `election`, `leader_recovery`, `repair_probe`, `faulty`, `app_repair` | no — **handoff refused while any is open** | Phase-1-shaped work, meaningless without the quorum report behind it |

So the transferred object is small and its safety meaning is stateable:

```rust
Message::Relinquish {
    config_id, from, to,          // addressing (see "uniqueness" below)
    ballot,                       // the authority
    from_slot, next_slot,         // the range it covers and the frontier
    decided: BTreeMap<Slot, (Ballot, Command)>,   // already chosen
    pending: BTreeMap<Slot, Command>,             // open rounds at `ballot`
}
```

`decided` and `pending` **exactly tile** `[from_slot, next_slot)`. That is validated on
receipt, and it is what lets the successor's inherited recovery run with **gap filling
off**: an election may `Noop`-fill a slot its promise quorum reported nothing for
(quorum intersection proves the slot free), but a handoff has no quorum report, so a
slot nobody described is skipped rather than invented. `LeaderRecovery::gap_fill` carries
that distinction.

### Why the open rounds must travel

Without them the successor's allocator starts *above* an undecided slot that nothing
would ever propose again: `propose` only allocates `next_slot`, and a restart recomputes
`next_slot` the same way. The contiguous chosen prefix would freeze one below it
cluster-wide — the #54 hole, with no election coming to fill it. Re-proposing an
identical command at an identical `(slot, ballot)` is a no-op for P2b, so carrying them
is free.

## Durable relinquishment: what actually fences the old leader

The hazard the technique invites is *authority resurrection*: A hands `B` to C, C starts
proposing under `B`, A crashes, A restarts believing it still owns `B`.

**In paros that state is unreachable, and not by accident:**

1. `RawNode::new` boots every node as a `Follower` with an empty `proposer`, whatever
   the disk says. There is no durable "I am leader at `B`" record to resurrect.
2. `role = Leader` is set in exactly one place (`try_become_leader`), only from
   `Candidate`, and `on_check_leader` campaigns at
   `max(promise.round, ballot.round) + 1` — *strictly higher*. A restarted A can only
   ever lead again **above** `B`.
3. Every Phase-2 entry point is leader-gated (`start_accept_round` asserts it;
   `resend_pending` returns early), so a Follower emits no `Accept` at all.

A crash is therefore itself an abdication, and the durable-fence question collapses to a
smaller one: *A must stop exercising `B` before the `Relinquish` can be observed.*
`RawNode::relinquish_to` answers it structurally — the same call that queues the message
demotes the node:

```
relinquish_to(target)
  ├─ queue Relinquish{ballot, next_slot, tail} → target
  └─ become_follower(Some(target))            ← the authority is gone here
...later... Ready → persist → send
```

Every continuation preserves *at most one holder of `B`*:

| what happens next | outcome |
| --- | --- |
| message sent and installed | A is already a Follower; only C proposes at `B` |
| message dropped / delayed / duplicated / misrouted | A still stepped down; the payload names one successor |
| A crashes before the send | nothing changed anywhere; ordinary election |
| A crashes after the send | A reboots a Follower whose next campaign is above `B` |

Emitting the message *without* abdicating — the classic broken variant — is not
expressible through the API, which is why the two are one call rather than two.

### The one place a durable fence *would* be needed

The deterministic simulation found this, on the first version of this feature, which
allowed an authority to be handed on repeatedly (A → C → D):

> A duplicate — or a delayed replay — of *A's original* `Relinquish` reaches C again. C
> is no longer leading, but its durable promise is still `B` (it is an ordinary acceptor
> at that ballot) and the payload is addressed to it, so every wire guard passes and C
> installs `B` a second time at the frontier A sent — while D is exercising `B` from the
> same frontier. Two nodes then allocate the same slots under one ballot.

The sweep reported it as `a relinquished authority is never exercised again`,
`one physical node at a time exercises a logical Paxos authority`, and
`a node never re-installs an authority it relinquished`.

Refusing that re-install requires C to remember, **across restarts**, that it once gave
`B` up — a durable relinquishment fence. paros has no durable leadership state to hang
one on, so the fence would be a new `HardState` scalar with its own write op, storage
record, checksum and boot read-back: a large, fragile surface bought for one extra
cooperative hop.

**The rule chosen instead is one hop only** (`RawNode::can_relinquish` requires
`LeadershipOrigin::Elected`): the node that mints a ballot by winning Phase 1 at it is
the only node that may hand it on. Uniqueness is then structural again — only the minter
ever relinquishes a ballot, its payload names one successor, and no node can ever be
handed an authority it previously gave up, because the only party who could hand it back
is a successor that is not allowed to. Handing leadership on again is still possible; it
just costs the ordinary election that mints a fresh ballot for the new holder to own.

## Authority uniqueness, structurally

Four independent mechanisms, none of which relies on eventual quorum intersection to
*discover* a violation:

1. **Abdication is synchronous** with the decision (above).
2. **The intended successor travels inside the payload** (`to`). Uniqueness must not
   depend on the transport delivering to exactly one address, so a duplicate, a
   misroute, or a replay toward a second node is refused on arrival.
3. **The successor validates against its own durable promise.** `ballot < promise` is a
   dead authority (some node ran Phase 1 past it) and is refused; a re-install of an
   authority already held here, or one that would rewind the allocator, is refused too.
4. **The allocator frontier only moves forward**, so two nodes can never allocate the
   same slot under one ballot even transiently.

## Client proposals across the boundary

The boundary is the `relinquish_to` call itself — there is no "preparing" state, because
there is no window to name: the node is a `Follower` the instant it decides.

| case | what happens |
| --- | --- |
| proposal admitted just before | travels in `pending`, re-proposed verbatim by the successor, commits under the same ballot |
| Phase 2 already in progress | same — the successor re-sends the identical `(slot, ballot, command)` |
| accepted-but-not-chosen | same; this is exactly what `pending` is for |
| proposal arriving after | `ProposeResult::NotLeader(Some(successor))` — an immediate redirect |
| a client waiting on a parked reply | the driver drops held replies on demotion; the retry reaches the successor, whose inherited recovery re-registered the `(client, seq) → slot` mapping, so it is answered `Duplicate(slot)` and acked when the slot commits |

`on_accept` was changed to take its leader hint from the message's **sender** rather than
`ballot.node`: they are the same node for an elected leader and deliberately different
after a handoff. Keying on `ballot.node` sent clients to a node that had already stepped
down and — when the receiver *was* `ballot.node` — made a Follower name itself leader and
redirect clients to itself.

## Interaction with the rest of the protocol

- **CTRL / PAR recovery.** A handoff is refused while a repair probe is open, while any
  local record is `faulty`, and while an application repair is open. Blocked commitment
  determination and in-place value repair are resolved *from a promise quorum's reports*;
  there is no honest way to hand that mid-flight state to a node that gathered no quorum.
  A leader that resigns on the CTRL §4.2 recovery timeout is unaffected — it is not
  handoff-eligible in the first place.
- **Snapshots and compaction.** A pending `Snap` marker or a `Truncate` proposal is an
  ordinary in-flight round and travels in `pending`. Snapshot *custody* and chunk repair
  are driver-terminal and never part of consensus state, so they are untouched. A
  successor below the compaction floor for part of the tail simply skips those slots
  (they are already decided and truncated there).
- **Ordinary elections.** They remain authoritative. A concurrent higher-ballot Phase 1
  wins: the successor refuses a transfer its promise already dominates, and a successor
  that installs and *then* sees a higher ballot steps down by the ordinary rules
  (`on_nack`, `on_prepare`, `on_heartbeat`). A delayed relinquishment cannot resurrect
  stale authority.
- **The inherited fence.** A handoff leader never *recovered* the range below its
  inherited frontier — it only learned what the predecessor described plus what ordinary
  replication brings. If a decision only the departed leader held becomes unreachable,
  the chosen prefix stops below `read_floor` and no read confirms. After
  `HANDOFF_FENCE_ELECTIONS` election timeouts the successor resigns, and an ordinary
  Phase 1 — which *does* cover exactly that range — recovers it. Phase 1 is always the
  fallback.

## Simulation surface

Everything below lives in `paros-sim`; `paros-core` is never buggified (the perturbation
is a caller that calls differently).

**BUGGIFY locations** (`BuggifyHooks`):

- `initiate_handoff`, split into three independent probabilities by the *shape* of the
  transfer — a leader still healing a hole (0.30), one with a non-empty tail (0.20), a
  fully settled one (0.02). Biased toward the interesting states, with the clean case
  kept armed so it never disappears.
- `handoff_target` — occasionally pins the successor instead of taking the driver's
  uniform draw, so a seed can concentrate handoffs on one node.
- `drop_outgoing(Relinquish)` at 0.25 — the whole handoff lost in one message, which
  must cost availability only.
- `duplicate_outgoing(Relinquish)` at 0.25 — the replay path the `to` field and the
  stale/rewind guards exist for.

Crash coverage comes for free: the existing `BeforeSync` / `AfterSyncBeforeSend` /
`AfterApplyBeforeSync` seams and moonpool's attrition already cut the run at every point
around the relinquish, and the successor's promise raise rides the ordinary
persist-before-send edge.

**The oracle** (`paros_sim::audit`) reconstructs authority from *semantic events only* —
never from `node.role`:

- `Accept` on the wire ⇒ "this node is exercising this ballot" (`observe_authority_use`);
- `Relinquish` on the wire ⇒ "this node has released it" (`observe_authority_release`),
  folded at the **transmit** instant so it orders correctly against the abdicating
  batch's already-queued messages, and idempotently so a duplicated send is harmless;
- the core-call callback carries the shape and coverage claims only.

Invariants asserted: `a relinquished authority is never exercised again`,
`one physical node at a time exercises a logical Paxos authority`,
`only the node exercising an authority relinquishes it`,
`only a ballot's own minter relinquishes it`,
`an authority is relinquished at most once by a node`,
`a transferred allocator frontier never rewinds`,
`at most one node installs a relinquished authority`,
`a relinquished tail exactly tiles the transferred range`. The pre-existing
`one ballot proposes at most one command for a slot` is the consequence these exist
upstream of.
