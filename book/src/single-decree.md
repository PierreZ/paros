# Single-decree Paxos, live

The rest of this book explains with diagrams. This page is the real thing. The
whole paros simulation, the node driver, the client workload, the oracles,
moonpool's transport stack, and the simulated network, compiles to WebAssembly and
runs **entirely in your browser tab**. Nothing talks to a server: logical time is
driven by the simulation's event queue, so a seed plays out here exactly as it does
in CI.

The workloads and invariants animated below are defined **once** in `paros-sim` and
reused unchanged by the native runner (`cargo run -p paros-sim-runner`) and this
demo (`paros-wasm-demo`, built with `default-features = false`).

## A value is chosen

Press **Run** and watch a value get chosen by a majority of three acceptors. This
is the happy path from [How Paxos chooses one value](choose-one-value.md): one
proposer, Prepare then Promise then Accept then Accepted, and the acceptors glow
green.

<iframe
  src="wasm-demo/index.html?embed=1&seed=0"
  title="paros: a value is chosen"
  style="width:100%;height:720px;border:1px solid #30363d;border-radius:12px"
  loading="lazy">
</iframe>

## Contention resolves to one value

Seed 19 is the duel behind the [value-selection rule](choose-one-value.md#the-value-selection-rule):
node 0 proposes at ballot `(1,0)`, node 1 interrupts at `(2,1)`, node 0's late Accept
is nacked (red), and a single value is still chosen by all three acceptors. The
duel resolves; safety never bends.

<iframe
  src="wasm-demo/index.html?embed=1&seed=19"
  title="paros: contention resolves to one value"
  style="width:100%;height:720px;border:1px solid #30363d;border-radius:12px"
  loading="lazy">
</iframe>

## Reading the demo

- The **client** (left) hands a value to a node, which becomes the proposer.
- Messages are coloured by kind: Prepare, Promise, Accept, Accepted, Nack, Commit.
- Each acceptor shows its **promised ballot** and a swatch for the value it
  accepted.
- The top-left readout tracks the current ballot and the promise / accept quorums.
- The **scenario** chips below the canvas describe the run you just computed (the
  ballot, the promise/accept quorums reached, who chose the value, whether the
  proposers dueled, how many messages the network dropped), and the status line
  narrates each step as it plays. Every seed describes itself, so try a few.

## Reproducibility

The same seed always produces the same run. Append `?dump` to the demo URL to see
the raw JSON `runSeed(seed)` returns: it matches, byte for byte, what the native
runner prints for the same seed. `?seed=<n>` picks a seed and `?still=<k>` renders
a single frozen frame (useful for screenshots).

> **What the demo shows.** Three acceptors run the single-decree kernel under
> network chaos: dropped, delayed, and reordered messages, plus dueling proposers,
> are all in play. The safety oracle asserts on every seed that no two acceptors
> ever choose different values. The full Multi-Paxos protocol, the
> [replicated log](replicated-log.md) and the [stable leader](stable-leader.md),
> runs live on the next page: [Multi-Paxos: leader and log](multi-paxos.md).
