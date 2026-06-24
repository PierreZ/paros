# How Paxos chooses one value

Single-decree Paxos answers one question: how can a cluster of acceptors agree on
a single value, and never disagree, even when the network drops, delays, and
reorders messages, and even when several proposers compete at once?

Press **Run** below and watch a value get chosen by a majority of three acceptors.

<iframe
  src="wasm-demo/index.html?embed=1&seed=0"
  title="paros: a value is chosen"
  style="width:100%;height:560px;border:1px solid #30363d;border-radius:12px"
  loading="lazy">
</iframe>

## Ballots

A proposer never just announces a value. It first claims a **ballot**: a number
that gives it the right to propose. Ballots are totally ordered as `(round,
node)`, so a higher round always wins and ties are broken by the proposer's node
id (two proposers can therefore never hold the same ballot). The demo shows the
current ballot top-left and on the proposer's badge.

## Two phases

A proposer drives two round trips, each needing a **majority** (2 of 3) to make
progress:

1. **Phase 1, Prepare then Promise.** The proposer asks the acceptors to promise
   not to accept anything older than its ballot. An acceptor that promises also
   reports any value it has already accepted. Once a majority promise, the
   proposer owns the ballot.
2. **Phase 2, Accept then Accepted.** The proposer asks the acceptors to accept a
   value at its ballot. Once a majority accept, the value is **chosen**: the
   acceptors glow green and every node learns it (Commit).

Why a majority? Any two majorities of three share at least one acceptor. That one
overlapping acceptor is what makes it impossible for two different values to both
be chosen.

## The value-selection rule

A proposer does not always get to propose its own value. If any acceptor's Promise
reports an already-accepted value, the proposer must **adopt the highest-ballot
value it saw** instead of its own. This is the rule that protects a value that may
already be chosen: a later proposer, forced to re-propose the same value, can
never change the choice.

Try seed **19** to watch it happen. Node 0 starts proposing at ballot `(1,0)`, but
node 1 cuts in with a higher ballot `(2,1)`. Node 0's late Accept is **nacked**
(rejected, shown in red), node 1's ballot wins, and a value is still chosen by all
three acceptors. The duel resolves; safety never bends.

<iframe
  src="wasm-demo/index.html?embed=1&seed=19"
  title="paros: contention resolves to one value"
  style="width:100%;height:560px;border:1px solid #30363d;border-radius:12px"
  loading="lazy">
</iframe>

## The one thing it will never do

The simulation will never show two acceptors choosing different values. That is
the single safety property the `SafetyOracle` asserts on every step of every seed,
in this browser tab and in CI alike (the [browser demo](wasm.md) chapter explains
how the same code runs in both). Seeds like **42** show proposers dueling without
ever converging, a livelock: every node has promised a different high ballot, so
no single ballot wins a promise quorum and nothing is chosen. Annoying, but never
*unsafe*. Randomized election timeouts cure the livelock in Stage 3; they were
never needed for safety.

## Reading the demo

- The **client** (left) hands a value to a node, which becomes the proposer.
- Messages are coloured by kind: Prepare, Promise, Accept, Accepted, Nack, Commit.
- Each acceptor shows its **promised ballot** and a swatch for the value it
  accepted.
- The top-left readout tracks the current ballot and the promise / accept quorums.

Same seed, same run: append `?dump` to any demo URL for the raw JSON, or
`?still=<k>` for a single frozen frame.
