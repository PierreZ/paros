---
name: upstream-to-moonpool
description: Handle a moonpool limitation paros exposes - decide whether it is reusable simulator infrastructure (open a focused issue in PierreZ/moonpool with downstream evidence, the smallest API asked for, determinism constraints and acceptance criteria, keep paros-side defense in depth) or a paros bug (fix here), and advance the moonpool git pin (paros-consensus's moonpool-assertions line) once the fix lands and the gates pass. Also use for any moonpool API question - consult the LLM docs first.
---

# Upstreaming to moonpool

paros depends on moonpool through a **git pin**: one `rev` on `paros-consensus`'s
optional `moonpool-assertions` line in `crates/lib/paros-consensus/Cargo.toml`. The
coming `consensus-sim` crate will add moonpool-tla and moonpool-walk. When
paros work hits a wall in moonpool, the question is whose wall it is.

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
determinism hole in the runtime. Keep it in paros when it is a protocol bug, or a policy only paros needs.

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

1. Set the new rev on the one moonpool line, `moonpool-assertions` in
   `crates/lib/paros-consensus/Cargo.toml`, then run `cargo update -p
   moonpool-assertions` and commit the updated `Cargo.lock`.
2. Read moonpool's changes between the two revs for anything that changes
   the assertion macros the core's `probe!` uses.
3. Run the gates of the root `AGENTS.md` (*Build, test, gates*).
4. Delete the paros-side stand-in the upstream fix replaces, in the same PR,
   and note the rev in the commit message.
