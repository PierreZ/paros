# Crash and recovery: what survives

[Crash and restart safety](restart-safety.md) argued the mechanism: a node's
promises and votes are only safe if they **survive** a crash exactly as they were,
and paros enforces that with the persist-before-send `Ready` handshake and a
recovery oracle. This page lets you **watch it happen** — one seed, replayable,
computed in your browser.

The demo is a **still timeline**, not a running animation: three node lanes on one
time axis, showing every crash, the durable state that outlives it, and the restart
that rejoins. Read it at your own pace; type a different seed for a different run.

## What survives, and what doesn't

Each node has two kinds of state, and the crash is where they part ways:

- **Volatile** — the live process: whether it is the leader, its in-flight
  messages, the `RawNode` rebuilt from storage. A crash **drops all of it**.
- **Durable** — what reached stable storage before the node replied: the promised
  ballot and the accepted log, split as `HardState` plus per-slot writes. A crash
  **keeps all of it** (the sim's `StorageWorld` outlives the process, like a disk).

On the timeline, that split is the whole story: the **green line is durable state**
— it runs unbroken straight through every crash. The **grey baseline is the live
process**, and the **yellow band is leadership** — both stop dead at a ⚡, because
they are volatile. The node reboots (↻), rebuilds its volatile state from the
durable green line, and rejoins.

## Watch it live

<iframe
  src="wasm-demo/index.html?embed=1&mode=crash&seed=99"
  title="paros: crash and recovery timeline (seed 99)"
  style="width:100%;height:640px;border:1px solid #30363d;border-radius:12px"
  loading="lazy">
</iframe>

Reading the lanes:

- **⚡ crash at the seam.** A `buggify`-injected crash at a durability seam *inside*
  one `Ready` batch — the seam a plain process kill can't reach. The tag says which
  one: `fsync✓ · send✗` is `AfterSyncBeforeSend` (the writes are durable, but the
  batch's messages never left — peers must re-drive it); `pre-fsync` is
  `BeforeSync` (the whole un-synced batch is lost, and since nothing was sent, it is
  a clean "never happened"). The dashed red line is that exact persist/send seam.
- **↻ restart.** The node comes back and rebuilds from durable storage. Its
  leadership does not come back with it — that was volatile.
- **━ durable, green.** The promised ballot and accepted log, unbroken across the
  gap. The little up-ticks are slots this node chose (each one is durable).
- **✓ recovery oracle.** The badge re-checks, live from the run, that **no node's
  promised ballot ever dropped across a restart** — the promise a recovered node
  must never break. It is green because the simulation would have panicked
  otherwise: this is the `RecoveryOracle` from the previous page, made visible.

A second seed — more leadership churn, a different crash pattern — same guarantee:

<iframe
  src="wasm-demo/index.html?embed=1&mode=crash&seed=7"
  title="paros: crash and recovery timeline (seed 7)"
  style="width:100%;height:640px;border:1px solid #30363d;border-radius:12px"
  loading="lazy">
</iframe>

## Reproducibility

Same seed, same run, byte for byte, here and in CI. The whole `RunResult` —
including the new crash / restart / fsync / recovered streams the demo reads — is
what `run_seed(seed)` returns; `?dump` shows the raw JSON. Because the timeline is
**derived from that data**, not a hand-written narrative, the digest chips (seam
crashes split by fsync, restarts, durable read-backs, batches fsync'd, promises
intact) stay correct as the protocol evolves. Type any seed and read what actually
happened — the promises hold either way.
