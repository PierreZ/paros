#!/usr/bin/env python3
"""Count the assertions of each function of paros-consensus (#248 (TigerStyle assertions)).

TigerStyle asks for at least two assertions per function on average (AGENTS.md, "Assertions,
probes, spans"). This script measures it: for each module of `crates/lib/paros-consensus/src`
it counts the functions, the assertions inside them, and the density (assertions per function).
Test code (`tests.rs`, `tests/`, `models/`, `test_support.rs`) is left out.

An assertion is a call of `assert!`, `assert_eq!` or `assert_ne!`. A function is an `fn` item
with a body; a function nested in another counts on its own, and its assertions count for it
only. A `const _: () = assert!(..)` at module level is counted apart, as a compile-time
assertion.

Usage: scripts/count-assertions.py [--src DIR] [--min DENSITY] [--functions]
  --min DENSITY   fail when the density of the whole crate is below DENSITY
  --functions     list each function with its count, lowest first
"""

import argparse
import math
import pathlib
import re
import sys

ASSERT = re.compile(r"\b(?:assert|assert_eq|assert_ne)!")
FN = re.compile(r"\bfn\s+([A-Za-z_][A-Za-z0-9_]*)")
CONST_ASSERT = re.compile(r"const\s+_\s*:\s*\(\)\s*=\s*assert!")
EXEMPT = re.compile(r"(^|/)(tests\.rs|tests/|models/|models\.rs$|test_support\.rs$)")


def strip(source: str) -> str:
    """Blank out comments, strings and char literals, and keep the offsets."""
    out = []
    i, n = 0, len(source)
    while i < n:
        c = source[i]
        if source.startswith("//", i):
            j = source.find("\n", i)
            j = n if j < 0 else j
            out.append(" " * (j - i))
            i = j
        elif source.startswith("/*", i):
            j = source.find("*/", i + 2)
            j = n if j < 0 else j + 2
            out.append(re.sub(r"[^\n]", " ", source[i:j]))
            i = j
        elif c == "r" and re.match(r'r#*"', source[i:]):
            hashes = re.match(r"r(#*)\"", source[i:]).group(1)
            end = '"' + hashes
            j = source.find(end, i + 2 + len(hashes))
            j = n if j < 0 else j + len(end)
            out.append(re.sub(r"[^\n]", " ", source[i:j]))
            i = j
        elif c == '"':
            j = i + 1
            while j < n and source[j] != '"':
                j += 2 if source[j] == "\\" else 1
            j = min(j + 1, n)
            out.append(re.sub(r"[^\n]", " ", source[i:j]))
            i = j
        elif c == "'" and re.match(r"'(\\.|[^\\'])'", source[i:]):
            m = re.match(r"'(\\.|[^\\'])'", source[i:])
            out.append(" " * len(m.group(0)))
            i += len(m.group(0))
        else:
            out.append(c)
            i += 1
    return "".join(out)


def cfg_test_spans(code: str):
    """The spans of `#[cfg(test)] mod name { .. }` blocks inside a file."""
    spans = []
    for m in re.finditer(r"#\[cfg\(test\)\]\s*mod\s+\w+\s*\{", code):
        start = m.end() - 1
        end = match_brace(code, start)
        spans.append((m.start(), end))
    return spans


def match_brace(code: str, open_at: int) -> int:
    depth = 0
    for k in range(open_at, len(code)):
        if code[k] == "{":
            depth += 1
        elif code[k] == "}":
            depth -= 1
            if depth == 0:
                return k
    return len(code) - 1


def functions(code: str):
    """Each function with a body: (name, body start, body end)."""
    found = []
    for m in FN.finditer(code):
        k = m.end()
        depth = 0
        # Find the body: the first `{` at depth 0 of parentheses and angle-free
        # signature, unless a `;` ends the item first (a trait method with no body).
        while k < len(code):
            ch = code[k]
            if ch in "([":
                depth += 1
            elif ch in ")]":
                depth -= 1
            elif depth == 0 and ch == ";":
                k = -1
                break
            elif depth == 0 and ch == "{":
                break
            k += 1
        if k < 0 or k >= len(code):
            continue
        found.append((m.group(1), k, match_brace(code, k)))
    return found


def count_file(path: pathlib.Path):
    code = strip(path.read_text())
    skip = cfg_test_spans(code)
    fns = [f for f in functions(code) if not any(a <= f[1] <= b for a, b in skip)]
    asserts = [m.start() for m in ASSERT.finditer(code) if not any(a <= m.start() <= b for a, b in skip)]
    consts = len(CONST_ASSERT.findall(code))
    per_fn = []
    for name, start, end in fns:
        inner = [f for f in fns if start < f[1] and f[2] <= end]
        own = [
            p
            for p in asserts
            if start < p < end and not any(s < p < e for _, s, e in inner)
        ]
        per_fn.append((name, len(own), code.count("\n", 0, start) + 1))
    return per_fn, consts


def module_of(rel: str) -> str:
    head = rel.split("/")[0]
    return head[:-3] if head.endswith(".rs") else head


def main() -> int:
    here = pathlib.Path(__file__).resolve().parent
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--src", default=str(here.parent / "crates/lib/paros-consensus/src"))
    parser.add_argument("--min", type=float, default=None)
    parser.add_argument("--functions", action="store_true")
    args = parser.parse_args()
    src = pathlib.Path(args.src)
    modules = {}
    listing = []
    for path in sorted(src.rglob("*.rs")):
        rel = path.relative_to(src).as_posix()
        if EXEMPT.search(rel):
            continue
        per_fn, consts = count_file(path)
        mod = modules.setdefault(module_of(rel), [0, 0, 0])
        mod[0] += len(per_fn)
        mod[1] += sum(c for _, c, _ in per_fn)
        mod[2] += consts
        listing += [(c, f"{rel}:{line}", name) for name, c, line in per_fn]
    total = [sum(m[i] for m in modules.values()) for i in range(3)]
    print(f"{'module':<16} {'fns':>5} {'asserts':>8} {'per fn':>7} {'const':>6}")
    for name in sorted(modules):
        fns, asserts, consts = modules[name]
        density = asserts / fns if fns else 0.0
        print(f"{name:<16} {fns:>5} {asserts:>8} {density:>7.2f} {consts:>6}")
    density = total[1] / total[0] if total[0] else 0.0
    # The total is printed rounded down, so the printed number is always a
    # floor that `--min` accepts: a rounded-up total would be a floor above
    # the density.
    floor = math.floor(density * 10_000) / 10_000
    print(f"{'total':<16} {total[0]:>5} {total[1]:>8} {floor:>7.4f} {total[2]:>6}")
    if args.functions:
        print()
        for count, where, name in sorted(listing):
            print(f"{count:>3} {where} {name}")
    if args.min is not None and density < args.min:
        print(f"count-assertions: density {density:.6f} is below {args.min}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
