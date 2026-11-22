#!/usr/bin/env python3
"""Guard mutation of the TLA+ models of specs/tla/ (#1311 (mutate spec guards)).

A guard is a conjunct of an action that changes no variable: a `/\\` item of a
definition that primes a variable, where the item has no prime, no `UNCHANGED`
and calls no action. For each guard, the script writes a copy of the tree in
which that one item is `TRUE`, and runs the checks of the folder
(`checks.txt`, the `ci` lines, as `scripts/check-tla.sh` runs them) on the
copy, the `pass` lines first: a guard set to TRUE mostly adds behaviours, which
only a `pass` line can see. A guard under a negation removes behaviours, and a
control (a `violation` line) can see that. The mutant is caught when at least one check does not give the result
that `checks.txt` names. A check of every folder that links the module counts.

A mutant that every check accepts is a survivor: the guard is not necessary,
or a property is missing. The fix is to remove the guard or to add the
property. There is no allow list (the absolute rule of AGENTS.md).

    nix develop .#tla --command scripts/mutate-tla.py decree        # one folder
    nix develop .#tla --command scripts/mutate-tla.py --list decree # the guards only
    nix develop .#tla --command scripts/mutate-tla.py --shard 2/3 truncation

`--shard i/n` takes the i-th of n slices of the mutants of the folders named
(1-based). `--grep REGEX` judges only the guards whose
`module:line action: text` (as `--list` prints it) matches REGEX, before the
slices are taken: use it to judge again the survivors of a folder after a fix.
Exit status: 0 when every mutant is caught, 1 when one survives,
2 on an error of the tool (a mutant that does not parse, a check that does not
give its expected result on the tree as it is).

A check of a mutant that runs longer than `TIMEOUT_FACTOR` times its baseline
(the same check on the tree as it is, timed once in each run)
(at least `TIMEOUT_MIN` seconds) is stopped and counts as caught: the guard
bounds the state space of the check, as for the `Mutants` workflow of the core
(a timeout counts as caught). The report names such mutants apart.

`--budget` bounds a run in seconds, for a CI job: a mutant whose checks the
budget cuts is `unfinished`, never caught, and the run fails. The fix is more
slices. `--baseline-out` times every check once (the folders of a job of the
`TLA` workflow), and `--baseline-from` gives those times to the slices, so that a slice
spends its budget on mutants. `--workers` is the number of mutants judged at
once; each TLC gets an equal share of the CPUs, so a baseline and the slices
that use it must run with the same `--workers`.
"""

import argparse
import concurrent.futures
import os
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SPECS = ROOT / "specs" / "tla"
TIMEOUT_FACTOR = 3
TIMEOUT_MIN = 60


# ---------------------------------------------------------------------------
# Lexing: blank out comments and strings, keep every column.


def blank(text: str, strings: bool = True) -> str:
    """The text with comments (and string literals when `strings`) replaced by spaces."""
    out = list(text)
    i, n, depth = 0, len(text), 0
    while i < n:
        if depth > 0:
            if text.startswith("(*", i):
                depth += 1
                out[i] = out[i + 1] = " "
                i += 2
            elif text.startswith("*)", i):
                depth -= 1
                out[i] = out[i + 1] = " "
                i += 2
            else:
                if out[i] != "\n":
                    out[i] = " "
                i += 1
        elif text.startswith("(*", i):
            depth = 1
            out[i] = out[i + 1] = " "
            i += 2
        elif text.startswith("\\*", i):
            while i < n and text[i] != "\n":
                out[i] = " "
                i += 1
        elif text[i] == '"':
            j = i + 1
            while j < n and text[j] != '"':
                j += 1 if text[j] != "\\" else 2
            if strings:
                for k in range(i + 1, min(j, n)):
                    out[k] = " "
            i = j + 1
        else:
            i += 1
    return "".join(out)


DEF_RE = re.compile(r"^([A-Za-z_][A-Za-z0-9_]*)\s*(\([^)]*\))?\s*==", re.M)
IDENT_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
# A `/\` is a bullet when only these come before it on its line.
LEAD_RE = re.compile(r"^(\s*(==|:|\(|\[|THEN|ELSE|IN|LET|\\/|/\\|->))*\s*$")


@dataclass
class Definition:
    name: str
    start: int  # offset of the first character of the body
    end: int  # offset past the body


@dataclass
class Guard:
    module: Path  # the file in its home folder (no symlink)
    definition: str
    start: int
    end: int
    line: int
    text: str


def definitions(clean: str) -> list[Definition]:
    """The top-level definitions: a name at column 0, up to the next line at column 0."""
    lines_at0 = [m.start() for m in re.finditer(r"^\S", clean, re.M)]
    defs = []
    for m in DEF_RE.finditer(clean):
        later = [p for p in lines_at0 if p > m.start()]
        end = later[0] if later else len(clean)
        defs.append(Definition(m.group(1), m.end(), end))
    return defs


def col(clean: str, pos: int) -> int:
    return pos - (clean.rfind("\n", 0, pos) + 1)


def item_end(clean: str, start: int, limit: int, column: int) -> int:
    """The end of the `/\\` item whose text starts at `start`, bullet at `column`."""
    depth, ifs, lets, i = 0, 0, 0, start
    while i < limit:
        ch = clean[i]
        if ch == "\n":
            # The next line with text ends the item when it starts at or left of the bullet.
            j = i + 1
            while j < limit:
                k = j
                while k < limit and clean[k] in " \t":
                    k += 1
                if k < limit and clean[k] == "\n":
                    j = k + 1
                    continue
                break
            if j >= limit:
                return i
            k = j
            while k < limit and clean[k] in " \t":
                k += 1
            if k - j <= column and depth == 0:
                return i
            i = j
            continue
        if ch in "([{":
            depth += 1
        elif ch in ")]}":
            if depth == 0:
                return i
            depth -= 1
        elif ch.isalpha() and (i == 0 or not (clean[i - 1].isalnum() or clean[i - 1] == "_")):
            m = IDENT_RE.match(clean, i)
            word = m.group(0)
            if depth == 0:
                if word == "IF":
                    ifs += 1
                elif word == "LET":
                    lets += 1
                elif word in ("THEN", "ELSE"):
                    if ifs == 0:
                        return i
                    if word == "ELSE":
                        ifs -= 1
                elif word == "IN":
                    if lets == 0:
                        return i
                    lets -= 1
            i += len(word)
            continue
        i += 1
    return limit


def is_action(body: str) -> bool:
    return "'" in body


def scan(path: Path, actions: set[str]) -> list[Guard]:
    """The guards of the actions of one module."""
    text = path.read_text()
    clean = blank(text)
    shown = blank(text, strings=False)
    guards = []
    for d in definitions(clean):
        body = clean[d.start : d.end]
        if not is_action(body) and not (set(IDENT_RE.findall(body)) & actions):
            continue
        for m in re.finditer(r"/\\", body):
            pos = d.start + m.start()
            line_start = clean.rfind("\n", 0, pos) + 1
            lead = clean[line_start:pos]
            if line_start < d.start:
                lead = clean[d.start : pos]
            if not LEAD_RE.match(lead):
                continue
            begin = pos + 2
            end = item_end(clean, begin, d.end, col(clean, pos))
            item = clean[begin:end]
            if not item.strip():
                continue
            if "'" in item or "UNCHANGED" in item or "ENABLED" in item:
                continue
            if set(IDENT_RE.findall(item)) & actions:
                continue
            guards.append(
                Guard(
                    path,
                    d.name,
                    begin,
                    end,
                    clean.count("\n", 0, pos) + 1,
                    " ".join(shown[begin:end].split()),
                )
            )
    return guards


def action_names(paths: list[Path]) -> set[str]:
    """The definitions that change a variable, directly or through another."""
    bodies = {}
    for p in paths:
        clean = blank(p.read_text())
        for d in definitions(clean):
            bodies[d.name] = clean[d.start : d.end]
    acts = {n for n, b in bodies.items() if is_action(b)}
    while True:
        more = {n for n, b in bodies.items() if n not in acts and set(IDENT_RE.findall(b)) & acts}
        # A definition that only names actions (Next, Spec, Fairness) is no guard host either.
        if not more:
            return acts
        acts |= more


def mutate(guard: Guard) -> str:
    text = guard.module.read_text()
    span = text[guard.start : guard.end]
    # Keep every column: TRUE, then spaces, newlines kept.
    filler = "".join("\n" if c == "\n" else " " for c in span)
    first = span.find("\n")
    first = len(span) if first < 0 else first
    replaced = " TRUE" + filler[min(5, first):]
    return text[: guard.start] + replaced + text[guard.end :]


# ---------------------------------------------------------------------------
# Folders, modules and checks.


@dataclass
class Check:
    folder: str
    name: str
    expected: str
    seed: list[str] = field(default_factory=list)
    baseline: float | None = None
    base_result: str = ""
    lock: threading.Lock = field(default_factory=threading.Lock)


@dataclass
class Folder:
    name: str
    checks: list[Check] = field(default_factory=list)


def folders_all() -> list[str]:
    out = []
    for m in sorted(SPECS.glob("*/checks.txt")) + sorted(SPECS.glob("experiments/*/*/checks.txt")):
        out.append(str(m.parent.relative_to(SPECS)))
    return out


def read_checks(folder: str) -> list[Check]:
    checks = []
    for line in (SPECS / folder / "checks.txt").read_text().splitlines():
        parts = line.split()
        if not parts or parts[0].startswith("#"):
            continue
        if len(parts) >= 3 and parts[2] == "ci":
            seed = [w[len("seed="):] for w in parts[3:] if w.startswith("seed=")]
            checks.append(Check(folder, parts[0], parts[1], ["-seed", seed[-1]] if seed else []))
    return checks


def own_modules(folder: str) -> list[Path]:
    """The modules of a folder that are not links, trace specs or proofs."""
    out = []
    for p in sorted((SPECS / folder).glob("*.tla")):
        if p.is_symlink() or p.name.endswith(("Trace.tla", "Proof.tla")):
            continue
        out.append(p)
    return out


def users(module: Path) -> list[str]:
    """Every folder that has the module, its home first."""
    home = str(module.parent.relative_to(SPECS))
    out = [home]
    for f in folders_all():
        q = SPECS / f / module.name
        if f != home and q.is_symlink() and q.resolve() == module.resolve():
            out.append(f)
    return out


# ---------------------------------------------------------------------------
# Running TLC.


def java_env(tmp: str) -> dict[str, str]:
    """The environment of one TLC or SANY process, with a temporary folder of its own.

    TLC and SANY unpack the standard modules (`TLC.tla`, `Naturals.tla`) into
    the temporary folder of Java. Two processes that share it race, and one of
    them fails to parse `TLC`.
    """
    env = dict(os.environ)
    env["JAVA_TOOL_OPTIONS"] = (env.get("JAVA_TOOL_OPTIONS", "") + f" -Djava.io.tmpdir={tmp}").strip()
    return env


def run_tlc(tree: Path, check: Check, timeout: float | None) -> tuple[str, float, str]:
    """The result of one check on a tree: pass, violation, error, parse or timeout."""
    meta = tempfile.mkdtemp(dir=tree.parent)
    t0 = time.monotonic()
    try:
        p = subprocess.run(
            ["tlc", "-workers", str(TLC_WORKERS), "-deadlock", "-metadir", meta + "/states",
             *check.seed, "-config", check.name + ".cfg", "MC.tla"],
            cwd=tree / check.folder, capture_output=True, text=True, timeout=timeout,
            env=java_env(meta),
        )
        log = p.stdout + p.stderr
    except subprocess.TimeoutExpired:
        return "timeout", time.monotonic() - t0, ""
    finally:
        shutil.rmtree(meta, ignore_errors=True)
    took = time.monotonic() - t0
    if "Model checking completed. No error has been found." in log:
        return "pass", took, log
    if re.search(r"Error: ((Invariant|Action property) .* is violated(\.| by the initial state)"
                 r"|Temporal properties were violated\.)", log):
        return "violation", took, log
    if "Parsing or semantic analysis failed" in log or "*** Errors:" in log or "Could not parse" in log:
        return "parse", took, log
    return "error", took, log


def copy_tree(dest: Path) -> Path:
    tree = dest / "tla"
    shutil.copytree(SPECS, tree, symlinks=True,
                    ignore=shutil.ignore_patterns("states", "*.out", "*.st"))
    return tree


def ensure_baseline(c: Check) -> str:
    """Time the check on the tree as it is, once, when a mutant first needs it.

    The check must give its expected result there; the answer is the error
    when it does not. A slice times only the checks its mutants reach: a
    caught mutant stops at its first failing check, so most checks of a big
    folder never run.
    """
    with c.lock:
        if c.baseline is None:
            if DEADLINE is not None and DEADLINE <= time.monotonic():
                return ""
            got, took, _ = run_tlc(BASE_TREE, c, None)
            c.baseline = took
            c.base_result = got
            print(f"baseline {c.folder}/{c.name}: {got} in {took:.0f}s", flush=True)
        if c.base_result != c.expected:
            return f"baseline {c.folder}/{c.name}: expected {c.expected}, got {c.base_result}"
        return ""


BASE_TREE: Path
TIMEOUT_MIN_S = TIMEOUT_MIN
TIMEOUT_FACTOR_X = TIMEOUT_FACTOR
TLC_WORKERS = 1
DEADLINE: float | None = None


def judge(guard: Guard, checks: list[Check], scratch: Path) -> tuple[str, str]:
    """Run the checks on the mutant; stop at the first check that fails."""
    tree = copy_tree(Path(tempfile.mkdtemp(dir=scratch)))
    try:
        (tree / guard.module.relative_to(SPECS)).write_text(mutate(guard))
        for c in checks:
            bad = ensure_baseline(c)
            if bad:
                return "tool-error", bad
            if c.baseline is None:
                return "unfinished", "the budget of the run ended"
            limit = max(TIMEOUT_MIN_S, TIMEOUT_FACTOR_X * c.baseline)
            left = None if DEADLINE is None else DEADLINE - time.monotonic()
            if left is not None and left <= 0:
                return "unfinished", "the budget of the run ended"
            got, _, log = run_tlc(tree, c, limit if left is None else min(limit, left))
            if got == "timeout" and left is not None and left < limit:
                return "unfinished", f"the budget of the run ended in {c.folder}/{c.name}"
            if got == "parse":
                tail = "\n".join(log.splitlines()[-15:])
                return "tool-error", f"{c.folder}/{c.name}: the mutant does not parse\n{tail}"
            if got == "timeout":
                return "caught-timeout", f"{c.folder}/{c.name} ran past {limit:.0f}s"
            if got != c.expected:
                kind = next((l.strip() for l in log.splitlines() if l.startswith("Error:")), "")
                return "caught", f"{c.folder}/{c.name}: {got}, expected {c.expected}. {kind}"
        return "survived", "every check gives its expected result"
    finally:
        shutil.rmtree(tree.parent, ignore_errors=True)


def parses(guard: Guard, scratch: Path) -> str:
    """The SANY errors of the mutant, empty when it parses."""
    tree = copy_tree(Path(tempfile.mkdtemp(dir=scratch)))
    try:
        (tree / guard.module.relative_to(SPECS)).write_text(mutate(guard))
        for u in users(guard.module):
            p = subprocess.run(["tlasany", "MC.tla"], cwd=tree / u, capture_output=True, text=True,
                               env=java_env(str(tree.parent)))
            log = p.stdout + p.stderr
            if p.returncode != 0 or "*** Errors:" in log or "Could not parse" in log:
                return f"{u}: " + "\n".join(log.splitlines()[-15:])
        return ""
    finally:
        shutil.rmtree(tree.parent, ignore_errors=True)


def main() -> None:
    global BASE_TREE, TIMEOUT_MIN_S, TIMEOUT_FACTOR_X, TLC_WORKERS, DEADLINE
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("folders", nargs="*", help="folders of specs/tla/ (default: every folder)")
    ap.add_argument("--list", action="store_true", help="print the guards, run nothing")
    ap.add_argument("--parse", action="store_true",
                    help="check that each mutant parses (SANY), run no check")
    ap.add_argument("--shard", default="1/1", help="i/n: the i-th of n slices of the mutants")
    ap.add_argument("--workers", type=int, default=os.cpu_count() or 1,
                    help="mutants judged at once; each TLC gets an equal share of the CPUs")
    ap.add_argument("--budget", type=float,
                    help="seconds: start no check after this; a mutant cut is `unfinished`")
    ap.add_argument("--baseline-out", type=Path,
                    help="time every check of the folders and their users, write them here, "
                         "judge no mutant")
    ap.add_argument("--baseline-from", type=Path, action="append", default=[],
                    help="a file of --baseline-out: the times of the checks, timed again "
                         "only when missing")
    ap.add_argument("--timeout-min", type=float, default=TIMEOUT_MIN, help="seconds")
    ap.add_argument("--timeout-factor", type=float, default=TIMEOUT_FACTOR)
    ap.add_argument("--out", type=Path, help="append one line per mutant to this file")
    ap.add_argument("--grep", help="judge only the guards whose `module:line action: text` "
                                   "matches this regular expression")
    ap.add_argument("--skip", type=Path,
                    help="a file of an earlier --out: leave out the mutants it judged")
    args = ap.parse_args()
    TIMEOUT_MIN_S, TIMEOUT_FACTOR_X = args.timeout_min, args.timeout_factor
    TLC_WORKERS = max(1, (os.cpu_count() or 1) // args.workers)

    folders = args.folders or folders_all()
    idx, count = (int(x) for x in args.shard.split("/"))
    assert 1 <= idx <= count

    guards: list[Guard] = []
    for f in folders:
        mods = own_modules(f)
        acts = action_names([p for p in (SPECS / f).glob("*.tla")
                             if not p.name.endswith(("Trace.tla", "Proof.tla"))])
        for m in mods:
            guards.extend(scan(m, acts))
    if args.skip:
        done = set()
        for line in args.skip.read_text().splitlines():
            parts = line.split("\t")
            if len(parts) >= 3:
                done.add((parts[1], parts[2]))
        guards = [g for g in guards
                  if (f"{g.module.relative_to(SPECS)}:{g.line} {g.definition}", g.text) not in done]
    if args.grep:
        pat = re.compile(args.grep)
        guards = [g for g in guards
                  if pat.search(f"{g.module.relative_to(SPECS)}:{g.line} {g.definition}: {g.text}")]
    guards = guards[idx - 1 :: count]

    if args.list:
        for g in guards:
            print(f"{g.module.relative_to(SPECS)}:{g.line} {g.definition}: {g.text}")
        print(f"{len(guards)} guards")
        return

    scratch = Path(tempfile.mkdtemp(prefix="mutate-tla-"))
    if args.parse:
        with concurrent.futures.ThreadPoolExecutor(args.workers) as ex:
            errors = [(g, e) for g, e in zip(guards, ex.map(lambda g: parses(g, scratch), guards)) if e]
        shutil.rmtree(scratch, ignore_errors=True)
        for g, e in errors:
            print(f"FAIL  {g.module.relative_to(SPECS)}:{g.line} {g.definition}: /\\ {g.text}\n{e}")
        print(f"{len(guards)} mutants, {len(errors)} do not parse")
        sys.exit(2 if errors else 0)
    BASE_TREE = copy_tree(scratch / "base")
    t0 = time.monotonic()
    if args.budget is not None:
        DEADLINE = t0 + args.budget
    if args.baseline_out:
        # The checks of each folder named; a folder that links one of its modules
        # times its own checks in its own run.
        need = folders
        bad = []
        with args.baseline_out.open("w") as fh:
            for f in need:
                for c in read_checks(f):
                    got, took, _ = run_tlc(BASE_TREE, c, None)
                    print(f"baseline {f}/{c.name}: {got} in {took:.0f}s", flush=True)
                    fh.write(f"{f}\t{c.name}\t{got}\t{took:.1f}\n")
                    if got != c.expected:
                        bad.append(f"{f}/{c.name}: expected {c.expected}, got {got}")
        shutil.rmtree(scratch, ignore_errors=True)
        for b in bad:
            print("FAIL  baseline " + b, file=sys.stderr)
        sys.exit(2 if bad else 0)
    need = sorted({u for g in guards for u in users(g.module)})
    checks = {f: read_checks(f) for f in need}
    for path in args.baseline_from:
        for line in path.read_text().splitlines():
            f, name, got, took = line.split("\t")
            for c in checks.get(f, []):
                if c.name == name:
                    c.baseline, c.base_result = float(took), got

    def plan(g: Guard) -> list[Check]:
        out = []
        for u in users(g.module):
            # Fast checks first: a pass check is the one a weaker guard can break.
            out.extend(sorted(checks[u], key=lambda c: c.expected != "pass"))
        return out

    results = {"caught": 0, "caught-timeout": 0, "survived": 0, "unfinished": 0, "tool-error": 0}
    lines = []
    with concurrent.futures.ThreadPoolExecutor(args.workers) as ex:
        futs = {ex.submit(judge, g, plan(g), scratch): g for g in guards}
        for fut in concurrent.futures.as_completed(futs):
            g = futs[fut]
            verdict, why = fut.result()
            results[verdict] += 1
            where = f"{g.module.relative_to(SPECS)}:{g.line} {g.definition}"
            line = f"{verdict:14} {where}: /\\ {g.text}\n               {why}"
            print(line, flush=True)
            if args.out:
                with args.out.open("a") as fh:
                    fh.write(f"{verdict}\t{where}\t{g.text}\t{why.splitlines()[0] if why else ''}\n")
            lines.append((verdict, where, g.text, why))
    shutil.rmtree(scratch, ignore_errors=True)

    print(f"\n{len(guards)} mutants in {time.monotonic() - t0:.0f}s: "
          + ", ".join(f"{v} {k}" for k, v in results.items()))
    survivors = sorted(l for l in lines if l[0] in ("survived", "tool-error"))
    if survivors:
        print("\nSurvivors and errors:")
        for verdict, where, text, why in survivors:
            print(f"  {verdict:10} {where}: /\\ {text}")
    if results["tool-error"]:
        sys.exit(2)
    if results["survived"] or results["unfinished"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
