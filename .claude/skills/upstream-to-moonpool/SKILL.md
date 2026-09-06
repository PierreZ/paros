---
name: upstream-to-moonpool
description: Handle a moonpool limitation paros exposes - decide whether it is reusable simulator infrastructure (open a focused issue in PierreZ/moonpool with downstream evidence, the smallest API asked for, determinism constraints and acceptance criteria, keep paros-side defense in depth) or a paros bug (fix here), and advance the moonpool git pin (one rev in three places across crates/paros/Cargo.toml and crates/paros-sim/Cargo.toml) once the fix lands and the compatibility gates pass. Also use for any moonpool API question: consult the LLM docs first.
---

# Upstreaming to moonpool

paros depends on moonpool through a **git pin** (`rev = ...` on
`moonpool-core`, `moonpool-hyper` in `crates/paros/Cargo.toml` and on
`moonpool-sim`, `moonpool-hyper` in `crates/paros-sim/Cargo.toml`; one rev,
four lines, two manifests). When paros work hits a wall in the simulator, the
question is whose wall it is.

## First, a moonpool question

For any question about moonpool's APIs or behaviour, read
<https://pierrez.github.io/moonpool/llms.html> before digging through source.
If the site is unreachable, the pinned checkout is under
`~/.cargo/git/checkouts/moonpool-*/<rev>/` and its `book/src/llms.md` is the
same guide; the `moonpool-consultant` agent knows both.

## Is it moonpool's?

Upstream when the limitation is properly reusable simulator infrastructure:
a fault the world cannot inject, a lifecycle fact a process cannot observe
(the reboot kind after a wipe), a per-group knob the builder lacks, a
determinism hole in the runtime. Keep it in paros when it is a protocol or
harness bug, or a policy only paros needs.

## Filing the issue (`PierreZ/moonpool`)

One focused issue per limitation, through the GitHub MCP tools, containing:

1. **Downstream evidence**: the paros scenario, the file and the workaround
   or gap, a seed or gate name if there is one.
2. **The smallest requested API or behaviour**, in moonpool's own terms
   (builder method, `FaultContext` primitive, `Process` hook), not paros's.
3. **Deterministic replay constraints**: which draws it may add, that it must
   be a pure function of the seed, that a recipe must still replay.
4. **Testable acceptance criteria**: the moonpool-side test that would prove
   it (`crates/moonpool-sim/tests/...`).

Link the issue from the relevant paros doc comment and PR. Meanwhile keep a
safe paros-side defense in depth (a harness stand-in, a guarded assumption),
never a silent local reimplementation of simulator infrastructure.

## Advancing the pin

1. Set the new rev on all four dependency lines (search `rev =` in both
   manifests) and let cargo refetch (`Cargo.lock` is not committed; this is a
   library workspace).
2. Read moonpool's changes between the two revs for anything that moves the
   draw schedule or the fault model (recovery mode, the single stream, group
   attrition); expect every seed to name a different run afterwards, which is
   fine, and expect the same seed to still replay identically, which is not
   negotiable.
3. Run the compatibility gates: `cargo nextest run` (the smoke, the canary
   pair, the corpus), then `cargo xtask sim run paros-chain` to saturation,
   then a few hundred `sim-paros-hunt canary` seeds (`/sim-sweep`).
4. Delete the paros-side stand-in the upstream fix replaces, in the same PR,
   and note the rev in the commit message (the root `AGENTS.md` cites pins by
   short rev when a doctrine depends on them).
