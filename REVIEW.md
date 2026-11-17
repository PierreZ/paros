# Review rules

What a reviewer checks on a paros pull request, by the kind of diff. The doctrine is in
`AGENTS.md`; this file only says where to look first. A reviewer is an agent or a person who did
not write the change. Report a finding with its file and line, and say which rule it breaks.

## On every pull request

- **The description matches the diff.** Each change is named, the small ones too. A file that
  the description does not explain is a finding.
- **The evidence is real.** The description says what was run and what it printed: the gate
  (`validate`), the end-to-end run (`verify`, "verify-cell: N steps passed") when the change
  touches code that `parosd`, `parosctl` or the library runs, and the TLA+ check when a spec
  changed. "Verified" with no run named is a finding.
- **The judge did not move.** Compare the judges on the branch with `main`. A pull request that
  changes behaviour and also loosens a judge of that behaviour is a finding, whatever its
  reason (`AGENTS.md`, "A pull request does not change its own judge").
- **A new mechanism has a test that fails without it.** For a fix, the description names the
  test that was red first.
- **A comment states the invariant in the present tense.** A comment that the diff adds or
  changes says what is true now. A seed witness, a commit hash, a "decided on" date or the
  story of a fix in such a comment is a finding: it goes in the commit message. One issue
  number that names a design decision is allowed. Old comments in lines the diff does not
  touch are not a finding (the rule is applied file by file).

## When the diff touches a judge

These paths are judges. Read each change to them line by line.

| Path | Loosened means |
|---|---|
| `crates/sim/paros-sim/src/audit/`, `crates/sim/paros-sim/src/workload/` (the oracles and `check()`) | an oracle deleted or made to skip; an excuse; a bound that grew; an `assert_always!` that became `assert_always_or_unreachable!` |
| `crates/sim/paros-sim-runner/` (gates) | a gate deleted; `gates::RARE` or its ceiling grew |
| an `assert!` in `crates/lib/paros-core/` | an assertion deleted or made weaker with no Paxos rule named in its place |
| `specs/tla/` | an expected result changed in `checks.txt`; a control deleted; a constant made smaller so that a check passes |
| `scripts/verify-cell.sh` | a step deleted; `expect` changed to `eventually`; a longer patience |
| `.github/workflows/`, `scripts/check-*.sh` | a job or a step removed; a new `paths-ignore`; `continue-on-error`; a retry |
| `clippy.toml`, `[workspace.lints]`, a new `#[allow]` | a lint turned off |
| a test | `#[ignore]`, a deleted test, a retry loop, a longer timeout, a weaker assertion |
| the list of open causes (from step 1.4) | a line added; a cause moved to a later phase |

A stricter judge or a new one is not a finding.

## When the diff touches a format or the wire

- `crates/lib/paros-wire/proto/`: a field number or a method id is never used again with another
  meaning. The one renumbering is step 0.1 of the rework; after it, a removed field is
  `reserved`. Each new message has a round-trip test, and a malformed input is refused, not a
  panic.
- A store format, the format marker or `HardState`: the description says how an old store
  opens (the rework changes formats with no migration only at step 2.6).
- An assertion message of `paros-sim` or a probe of `paros-core`: the text did not change (a
  slot is the hash of its message).

## When the diff touches a fence, a ballot or a quorum

- A comparison of ballots, a promise, a leader uuid fence, a generation fence, a retire leg or a
  quorum predicate: the description names the Paxos rule, and a unit test shows the bug with the
  rule off. A rule that the paper does not have needs its model in `specs/tla/` first.
- Every quorum question goes through `membership/`. A count compared with a threshold in
  another place is a finding.
- The plain Multi-Paxos path sends and persists the same things as before.

## When the diff touches the client or the CLI

- A new outcome is typed. A retry is the identical write. `parosctl` holds no policy.
- What a user can do changed: `scripts/verify-cell.sh` has the step, and `DEMO.md`, the site
  and `crates/bin/parosd/README.md` say the same as the code.

## When the diff touches timing

- A new sleep, timeout or poll: it comes from a tunable with a documented floor, never a
  literal. A timeout that was raised to make a check pass is a finding.
