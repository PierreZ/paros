# Protocol-aware recovery (CTRL)

Detect ⇒ crash keeps corruption out of protocol logic, but it wastes the one
thing a *replicated* system has that a single disk does not: other copies. A
committed entry rotted on this node is, with overwhelming probability, intact
on a quorum of peers. CTRL (protocol-aware corruption-tolerant recovery, the
Alagappan et al. work this stage implements for Multi-Paxos) is the flip from
"crash on every mismatch" to **recover-or-wait**: repair what a peer can
prove, wait when nothing can, and never, ever guess.

<!-- toc -->

## Phase 1 was the recovery query all along

The mechanism is almost disappointingly small, which is its beauty. An
acceptor's Promise already reports, per slot, what it has accepted. CTRL makes
that report **tri-state**: `accepted (ballot, value)` / `none` / **`faulty
(slot, ballot)`** — "I durably accepted something here and can no longer read
it". A leader's ordinary Phase 1 then doubles as the recovery query, and its
quorum of answers decides, per slot:

- **Some peer still has it** → re-propose that value (the ordinary P2c rule);
  the repaired record flows back to the faulty node through the ordinary
  Accept/catch-up paths. *Recovered.*
- **A full quorum answers `none`** → the entry was provably never chosen — a
  `Noop` fill is safe by quorum intersection. (This is the gap-fill rule from
  the election chapter, now sharing one unified threshold with recovery.)
- **Neither** — the tally is `faulty` plus too few `none`s to prove anything →
  **wait**. Re-query stragglers, hold the slot, resign leadership after a
  timeout. The system serves what it can and blocks what it cannot prove.

The fatal shortcut is counting `faulty` as `none`. A quorum that does so can
see a "unanimous nothing" for a slot whose chosen value survives only on the
crashed minority, no-op-fill it, and fabricate history — two values chosen for
one slot. The simulation keeps a pinned **red demo** of exactly this mutation
(`FAULTY_NONE_DEMO_SEED`): misreport `faulty` as `none` and the
one-value-per-slot oracle goes red.

## The record that must never be recovered by deletion

A promise is a *negative* guarantee — "I will reject lower ballots" — so no
peer can restore it: peers only know what was accepted, not what was refused.
Deleting a rotted promise record and rejoining fresh is the `MarkNonVoting`
bug CTRL's paper demolishes: the amnesiac node accepts from an old leader
while the new leader still counts its old promise, and a chosen value is
overwritten. paros stores the promise in **two copies**: one rotted copy heals
from its clean twin; both rotted means the node stays down (the wipe/amnesia
case, deliberately out of scope for rejoin). Recovery never deletes ballot
state — the adversarial promise-corruption runs in the sweep prove no node
ever reneges.

## Watch it live

One seeded run under the full corruption battery, now with recovery armed. A
red triangle is still an injected rot — but watch what follows: a green check
is the record **healed** (a peer's correct copy re-shipped through the
ordinary protocol paths), pause bars are a node **waiting** rather than
fabricating, and the violet box is a rotted application snapshot legally reset
and re-derived. Gold-tagged records are promise copies — the digest confirms
none of them ever reneged.

<iframe
  src="wasm-demo/index.html?embed=1&mode=ctrl&seed=4"
  title="paros: recover-or-wait under CTRL (seed 4)"
  style="width:100%;height:700px;border:1px solid #30363d;border-radius:12px"
  loading="lazy">
</iframe>

The headline is CTRL's whole guarantee, re-derived live from the run's data:
**committed data never lost** — every rotted committed record either healed
from a peer or the node waited, and no promise went backwards. When the sweep
lifts the per-record budget so *every* copy of an item can rot, the oracles
demand the wait be genuine: a slot is excused only by the world's ground truth
that no readable copy exists anywhere.

Same seed, same run, byte for byte, here and in CI: the demo replays
`run_seed(seed)` in your browser and draws the recorded `repairs` stream of
the `RunResult` (heals, waits, resets, unrecoverables) beside the corruption
ground truth; `?dump` shows the raw JSON.

## Why "wait" is a feature

Blocking feels like failure until you name the alternatives: fabricate
(unsafe — the red demo), truncate (unsafe — last chapter's tombstone), or
crash the whole node forever (the availability cliff CTRL exists to fix).
Waiting is the honest floor: serve every slot you can prove, hold the ones
you cannot, and let repair — a peer coming back, a snapshot arriving — lift
the hold. The oracles enforce that the hold *does* lift whenever a readable
copy exists anywhere in the cluster: recover-or-wait, with "wait" the
provable exception rather than the easy way out.
