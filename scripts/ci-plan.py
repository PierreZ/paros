#!/usr/bin/env python3
"""Plan the CI jobs that a change needs, from the files that it changes.

GitHub runs at most 40 jobs at a time for the account (GitHub Pro). A job that
can find nothing must not take a runner, and a job that can find something must
always run. This script is the one place that decides it, for both workflows:

    scripts/ci-plan.py rust --base <sha>   # rust=true|false
    scripts/ci-plan.py tla --base <sha>    # groups, traces, proofs
    scripts/ci-plan.py tla --all           # every folder, traces and proofs

With `--base`, the changed files are `git diff --name-only <base> HEAD`. An
unknown or empty base (the first push of a branch) plans everything. The
result is written as `key=value` lines to `$GITHUB_OUTPUT`, or to stdout.

**Rust.** `rust` is false when every changed file is browser side, a document
or a TLA+ input (`NOT_RUST`). The jobs that build and test the Rust code then
have nothing to find: those files are not an input of any Rust build.

**TLA.** A folder of specs/tla/ is checked from its own files only: TLC and
SANY read the modules of the folder, and a folder shares a module with another
one only through a symlink (`grid/Grid.tla`). So the inputs of a folder are its
files, the targets of its symlinks, and the tools (`TLA_TOOLS`). A folder runs
when one of its inputs changed, or when a file under it changed (a file added
or deleted). A change of a tool runs every folder.

The folders that run are packed into groups, each at most `GROUP_MINUTES` of
checks (`scripts/tla-minutes.txt`), largest first, so that a job of the TLA
workflow stays inside its 30 minutes and the number of jobs is small. A folder
above `GROUP_MINUTES` (at most `FOLDER_MINUTES`) runs alone.
Every plan also judges the two tables of the folders: a folder with no line
in `scripts/tla-minutes.txt` or in `scripts/mutate-tla-shards.txt` fails it.

The trace checks (`traces`) run the walks of consensus-sim and journal-sim
against the `*Trace.tla` specs. They run in the TLA workflow when a folder of a
trace spec runs and the Rust jobs do not: when the Rust jobs run, the `test`
and `trace-edits` jobs of the Rust workflow run the same trace tests on the same
specs. The proofs (`proofs`, one job for each `*Proof.tla` module, so that each fits
the 30 minutes of a job) run for the folders that run, or all of them when the
proof script changes.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SPECS = ROOT / "specs" / "tla"
MINUTES = ROOT / "scripts" / "tla-minutes.txt"
SHARDS = ROOT / "scripts" / "mutate-tla-shards.txt"

# A job of the TLA workflow has 30 minutes, about 27 of them for its checks
# once Nix is set up. A line of `scripts/tla-minutes.txt` is the largest time
# the folder took on GitHub's runners, and the time of one folder still moves
# by a third from one runner to the next. A group holds at most
# `GROUP_MINUTES` of measured checks, so a runner 1.8 times as slow as the
# slowest one measured still finishes. A folder above it runs alone in its
# job, with less margin: a folder at `FOLDER_MINUTES` fits a runner 1.2 times
# as slow. No folder may be above `FOLDER_MINUTES`: such a folder splits its
# checks.
GROUP_MINUTES = 15.0
FOLDER_MINUTES = 22.0

# The files that no Rust build reads. Keep in step with `paths-ignore` of
# `.github/workflows/rust.yml`, which is a subset of this list.
NOT_RUST = re.compile(
    r"^(website/|docs/|specs/|scripts/build-wasm\.sh$|scripts/check-tla\.sh$"
    r"|scripts/check-proofs\.sh$|scripts/mutate-tla\.py$|scripts/tla-minutes\.txt$"
    r"|scripts/mutate-tla-shards\.txt$|\.github/workflows/tla-mutants(-slices)?\.yml$"
    r"|\.github/workflows/tla\.yml$)|\.md$"
)

# The files that every folder's check reads: a change runs every folder.
# `scripts/tla-minutes.txt` is not one: no check reads it, it only packs the
# folders into jobs, and every plan judges it (a line for each folder), so a
# change that adds the line of a new folder runs that folder alone.
TLA_TOOLS = {
    "scripts/check-tla.sh",
    "scripts/mutate-tla.py",
    "scripts/ci-plan.py",
    "flake.nix",
    "flake.lock",
    ".github/actions/nix/action.yml",
    ".github/workflows/tla.yml",
}

# The files that the proofs read besides their folder.
PROOF_TOOLS = {"scripts/check-proofs.sh", "flake.nix", "flake.lock", ".github/workflows/tla.yml",
               ".github/actions/nix/action.yml",
               "scripts/ci-plan.py"}


def changed_files(base: str | None) -> list[str] | None:
    """The files that the change touches, or None when everything must run."""
    if not base or set(base) == {"0"}:
        return None
    try:
        out = subprocess.run(["git", "diff", "--name-only", base, "HEAD"], cwd=ROOT,
                             check=True, capture_output=True, text=True).stdout
    except subprocess.CalledProcessError as err:
        print(f"no diff from {base} ({err.stderr.strip()}): plan everything", file=sys.stderr)
        return None
    return [line for line in out.splitlines() if line]


def folders() -> list[str]:
    """Every folder of specs/tla/ with a `checks.txt`, named by its path under specs/tla/."""
    found = [m.parent for m in SPECS.glob("*/checks.txt")]
    found += [m.parent for m in SPECS.glob("experiments/*/*/checks.txt")]
    return sorted(str(f.relative_to(SPECS)) for f in found)


def inputs(folder: str) -> set[str]:
    """The files that the checks of `folder` read: its files and their symlink targets."""
    result = set()
    for path in (SPECS / folder).rglob("*"):
        if path.is_symlink():
            target = path.parent / os.readlink(path)
            # One link to one file: then the link and its target are all the
            # inputs. A link to a link or to a folder would hide an input.
            if target.is_symlink() or target.is_dir():
                sys.exit(f"{path}: a link must name a file, not a link or a folder")
        if path.is_dir():
            continue
        result.add(str(path.relative_to(ROOT)))
        result.add(str(path.resolve().relative_to(ROOT)))
    return result


def pairs(path: Path) -> list[tuple[str, str]]:
    """The `<folder> <number>` lines of a table, without comments and blank lines."""
    result = []
    for raw in path.read_text().splitlines():
        line = raw.split("#", 1)[0].strip()
        if not line:
            continue
        parts = line.split()
        if len(parts) != 2:
            sys.exit(f"{path.relative_to(ROOT)}: a line must be <folder> <number>, not {raw!r}")
        result.append((parts[0], parts[1]))
    return result


def minutes(all_folders: list[str]) -> dict[str, float]:
    table = {}
    for name, value in pairs(MINUTES):
        if name in table:
            sys.exit(f"scripts/tla-minutes.txt: two lines for {name}")
        table[name] = float(value)
    missing = sorted(set(all_folders) - set(table))
    extra = sorted(set(table) - set(all_folders))
    if missing or extra:
        sys.exit(f"scripts/tla-minutes.txt: no line for {missing}, no folder for {extra}")
    too_long = sorted(f for f, m in table.items() if m > FOLDER_MINUTES)
    if too_long:
        sys.exit(f"{too_long} take more than {FOLDER_MINUTES} minutes: split the folder's checks")
    return table


def shards(all_folders: list[str]) -> None:
    """Every folder has one line in `scripts/mutate-tla-shards.txt`: the
    `TLA mutants` workflow refuses a run on a folder with no line, so a new
    folder gets its line in the pull request that adds it."""
    names = []
    for name, value in pairs(SHARDS):
        if not re.fullmatch(r"[0-9]+", value):
            sys.exit(f"scripts/mutate-tla-shards.txt: {name} has no whole number of slices")
        names.append(name)
    twice = sorted({n for n in names if names.count(n) > 1})
    if twice:
        sys.exit(f"scripts/mutate-tla-shards.txt: two lines for {twice}")
    missing = sorted(set(all_folders) - set(names))
    extra = sorted(set(names) - set(all_folders))
    if missing or extra:
        sys.exit(f"scripts/mutate-tla-shards.txt: no line for {missing}, no folder for {extra}")


def pack(selected: list[str], table: dict[str, float]) -> list[list[str]]:
    """First-fit decreasing: the largest folder first, into the first group that holds it."""
    groups: list[tuple[float, list[str]]] = []
    for folder in sorted(selected, key=lambda f: (-table[f], f)):
        for i, (load, members) in enumerate(groups):
            if load + table[folder] <= GROUP_MINUTES:
                groups[i] = (load + table[folder], members + [folder])
                break
        else:
            groups.append((table[folder], [folder]))
    return [sorted(members) for _, members in groups]


def needs_rust(changed: list[str] | None) -> bool:
    return changed is None or any(not NOT_RUST.search(f) for f in changed)


def plan_tla(changed: list[str] | None) -> dict[str, str]:
    all_folders = folders()
    table = minutes(all_folders)
    shards(all_folders)
    tools = changed is None or any(f in TLA_TOOLS for f in changed)
    selected = []
    for folder in all_folders:
        if tools:
            selected.append(folder)
            continue
        prefix = f"specs/tla/{folder}/"
        reads = inputs(folder)
        if any(f.startswith(prefix) or f in reads for f in changed):
            selected.append(folder)
    trace_folders = {f for f in all_folders if any((SPECS / f).glob("*Trace.tla"))}
    proof_folders = {f for f in all_folders if any((SPECS / f).glob("*Proof.tla"))}
    # With no diff (a full plan), the Rust workflow may not run (a manual run,
    # or a push to main that touched specs only), so the traces run here.
    traces = bool(trace_folders & set(selected)) and (changed is None or not needs_rust(changed))
    proof_tools = changed is None or any(f in PROOF_TOOLS for f in changed)
    proofs = sorted(str(m.relative_to(SPECS)) for f in all_folders
                    if proof_tools or f in selected for m in (SPECS / f).glob("*Proof.tla"))
    groups = [" ".join(g) for g in pack(selected, table)]
    return {
        "groups": json.dumps(groups),
        "any": str(bool(groups)).lower(),
        "traces": str(traces).lower(),
        "proofs": json.dumps(proofs),
        "any_proofs": str(bool(proofs)).lower(),
    }


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("workflow", choices=["rust", "tla"])
    ap.add_argument("--base", help="the commit that the change is compared with")
    ap.add_argument("--all", action="store_true", help="plan every job")
    args = ap.parse_args()
    changed = None if args.all else changed_files(args.base)
    if args.workflow == "rust":
        result = {"rust": str(needs_rust(changed)).lower()}
    else:
        result = plan_tla(changed)
    text = "".join(f"{k}={v}\n" for k, v in result.items())
    sys.stdout.write(text)
    if out := os.environ.get("GITHUB_OUTPUT"):
        with open(out, "a", encoding="utf-8") as fh:
            fh.write(text)


if __name__ == "__main__":
    main()
