---
name: verify
description: Run the change end to end on the shipped binaries before a pull request - scripts/verify-cell.sh starts three parosd processes on loopback and drives them with parosctl as a user does (init, tenant and journal create, write, read, supersede the writer, SIGKILL one machine and keep writing, restart it, SIGTERM the cell), one line per step, and a failed step prints its command, its output and the machines' logs. Use before you open or update a pull request that changes code parosd, parosctl or the library runs, when asked "does it still work", and to see a change working in the real cell. It does not replace the validate skill (the CI gate) or the simulation (the proof of the protocol).
---

# Verify

`validate` answers "will CI pass". This skill answers a different question: **does the cell a
user runs still do the user's flow?** A unit test can pass while the feature is broken, because
the test and the change share one view of the task. The end-to-end run is hard to fool: the
three machines form a cell and serve, or they do not.

It is a local check, decided on 2026-11-17. It is not a CI job, and it is not a test of the
protocol: the simulation proves the protocol (`AGENTS.md`, *Simulation-driven development*).
While the simulation is off in CI (decision 33), this run is the only check that starts whole
machines on each change, so do not skip it.

## Run it

```bash
nix develop --command scripts/verify-cell.sh
```

It builds `parosd` and `parosctl` (debug), picks three free loopback ports, and uses a fresh
directory under `$TMPDIR`. It needs no Docker, no TLS and no token. A green run takes less than
one minute after the build and ends with `verify-cell: every step passed`.

| Option | Effect |
|---|---|
| `--keep` | keep the directory of the run (the stores and `node1.log` … `node3.log`) after a green run |
| `--no-build` | use the binaries already in `target/debug` |
| `--port-base N` | use ports N, N+1 and N+2 |
| `VERIFY_PATIENCE_S` | how long a step waits for the cell after a start, a kill or a restart (default 60) |
| `VERIFY_RUST_LOG` | the log filter of the machines (default `warn,parosd=info`) |

## What it checks

| Step | The user's flow | What must hold |
|---|---|---|
| 1 | build | `parosd` and `parosctl` build |
| 2 | start three machines with no peer | each process stays up |
| 3 | `parosctl init` | the cell forms and a coordinator is elected |
| 4 | a second `init` | refused, exit 3, `already_initialized` |
| 5 | `tenant create`, `journal create` | the journal is created |
| 6 | `write`, `read` | the records come back |
| 7 | `set-leader`, then a write of the old leader | the old leader is refused (exit 3) and its record is not in the journal |
| 8 | SIGKILL one machine, `write`, `read` | two machines serve; no record is lost |
| 9 | start the machine again, `write`, `read` | it boots as an existing member and answers; the five records are read |
| 10 | `machine list`, `status`, `journal list` | the views answer and name the three machines |
| 11 | SIGTERM each machine | each one ends with exit code 0 |

## Read a failure

A failed step prints `FAIL step N`, the `parosctl` command, its exit code (`3` refused, `4`
ambiguous, `5` no server answered), its output, and the last lines of each machine's log. The
directory of the run is kept. Then:

- **Fix the code, not the script.** Do not raise `VERIFY_PATIENCE_S`, add a retry or delete a
  step to get a green run. A step that needs more than 60 s on an idle cell of three machines is
  a finding.
- **A red that does not come from your change is still a red.** Run the script on `main`. If
  `main` is red too, tell the coordinator; do not continue on top of it.
- **A red that comes and goes is a bug or a bad check.** Find the cause. Never run it again
  until it is green (`AGENTS.md`, *Workflow rules*, "A flaky check is a finding").
- Look at a store with the machine stopped: `target/debug/parosctl store inspect <dir>/node2`.
- More logs from the machines: `VERIFY_RUST_LOG=debug` in front of the command (the script
  does not use the `RUST_LOG` of the dev shell).

## Add your change to it

A change that adds or changes something a user can do with `parosd` or `parosctl` adds its step
to `scripts/verify-cell.sh` in the same pull request, with the property that must hold. Keep
the rules of the script:

- A step is a `parosctl` command and a check of its exit code and its output. Use `expect CODE`
  when the answer must come at once, and `eventually` only after a start, a kill or a restart.
- The script uses the same commands as `DEMO.md`, "Locally, without Docker". When a command
  changes, change both.
- Keep the run short: one cell, three machines. A scenario with many machines, many faults or a
  timing condition belongs in the simulation, not here.

## Say what you ran

In the pull request description, write the result: "verify-cell: 11 steps passed", or the step
that failed and why the pull request is still correct. Do not write "verified" for a run you did
not make. A pull request that changes only docs, the site or `specs/` does not need the run.
