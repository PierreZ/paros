# The browser demo

The whole paros simulation — the node driver, the client workload, the oracles,
moonpool's transport stack, and the simulated network — compiles to WebAssembly
and runs **entirely in your browser tab**. Nothing talks to a server: logical
time is driven by the simulation's event queue, so a seed plays out the same way
here as it does in CI.

The workloads and invariants animated below are defined **once** in `paros-sim`
and reused unchanged by the native runner (`cargo run -p paros-sim-runner`) and
this demo (`paros-wasm-demo`, built with `default-features = false`).

<iframe
  src="wasm-demo/index.html?embed=1&seed=0"
  title="paros browser demo"
  style="width:100%;height:560px;border:1px solid #30363d;border-radius:12px"
  loading="lazy">
</iframe>

## Reproducibility

The same seed always produces the same run. Append `?dump` to the demo URL to see
the raw JSON `runSeed(seed)` returns — it matches, byte for byte, what the native
runner prints for the same seed. `?seed=<n>` picks a seed and `?still=<k>` renders
a single frozen frame (useful for screenshots).

> **Stage 2.** Three acceptors now run single-decree Paxos under network chaos:
> dropped, delayed, and reordered messages, plus dueling proposers, are all in
> play. The safety oracle asserts on every seed that no two acceptors ever choose
> different values. For a guided tour, read
> [How Paxos chooses one value](choose-one-value.md).
