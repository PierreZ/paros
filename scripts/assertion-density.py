#!/usr/bin/env python3
"""Assertion density per function, per module (#248).

TigerStyle asks for an average of at least two assertions per function. This
script measures it over Rust sources without compiling them: it strips
comments and literals, finds every `fn` with a body by brace matching, and
counts `assert!`, `assert_eq!` and `assert_ne!` inside each body (a nested
`fn` is counted on its own, never twice). Module-level compile-time
assertions (`const _: () = assert!(..)`) belong to their module and are
reported in their own column; they count toward the module's average.

Test code is excluded: `#[cfg(test)]` items (inline modules and `mod x;`
declarations, whose files are skipped), `#[test]` functions, and any file
under a `tests/` directory or named `tests.rs`.

Run through Nix:

    nix shell nixpkgs#python3 -c python3 scripts/assertion-density.py
    nix shell nixpkgs#python3 -c python3 scripts/assertion-density.py --min 2 --per-module

With no paths it measures the issue's scope: `crates/paros-core/src` and
`crates/paros/src/driver`. Exit status is 1 when `--min` is given and a scope
(or, with `--per-module`, any module) averages below it.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from dataclasses import dataclass, field

DEFAULT_SCOPES = ["crates/paros-core/src", "crates/paros/src/driver"]

ASSERT_RE = re.compile(r"(?<![A-Za-z0-9_])(assert|assert_eq|assert_ne)\s*!")
CONST_ASSERT_RE = re.compile(r"const\s+_\s*:\s*\(\s*\)\s*=\s*\{?\s*assert\s*!")
FN_RE = re.compile(r"(?<![A-Za-z0-9_])fn\s+([A-Za-z_][A-Za-z0-9_]*)")
MOD_DECL_RE = re.compile(r"(?<![A-Za-z0-9_])mod\s+([A-Za-z_][A-Za-z0-9_]*)\s*;")


def strip(src: str) -> str:
    """Blank comments, strings and char literals, keeping offsets and newlines."""
    out = list(src)
    i, n = 0, len(src)

    def blank(a: int, b: int) -> None:
        for k in range(a, min(b, n)):
            if out[k] != "\n":
                out[k] = " "

    while i < n:
        c = src[i]
        if src.startswith("//", i):
            j = src.find("\n", i)
            j = n if j < 0 else j
            blank(i, j)
            i = j
        elif src.startswith("/*", i):
            depth, j = 1, i + 2
            while j < n and depth:
                if src.startswith("/*", j):
                    depth, j = depth + 1, j + 2
                elif src.startswith("*/", j):
                    depth, j = depth - 1, j + 2
                else:
                    j += 1
            blank(i, j)
            i = j
        elif c == "r" and re.match(r'r#*"', src[i:i + 10]) and not _ident_before(src, i):
            m = re.match(r'r(#*)"', src[i:])
            hashes = m.group(1)
            end = src.find('"' + hashes, i + len(m.group(0)))
            end = n if end < 0 else end + 1 + len(hashes)
            blank(i, end)
            i = end
        elif c == "b" and src.startswith('b"', i) and not _ident_before(src, i):
            i += 1
        elif c == '"':
            j = i + 1
            while j < n and src[j] != '"':
                j += 2 if src[j] == "\\" else 1
            blank(i, j + 1)
            i = j + 1
        elif c == "'":
            # A char literal ('x', '\n', '\u{..}') versus a lifetime ('a).
            m = re.match(r"'(\\u\{[0-9a-fA-F]+\}|\\.|[^\\'])'", src[i:])
            if m:
                blank(i, i + len(m.group(0)))
                i += len(m.group(0))
            else:
                i += 1
        else:
            i += 1
    return "".join(out)


def _ident_before(src: str, i: int) -> bool:
    return i > 0 and (src[i - 1].isalnum() or src[i - 1] == "_")


def match_brace(s: str, open_at: int) -> int:
    depth = 0
    for k in range(open_at, len(s)):
        if s[k] == "{":
            depth += 1
        elif s[k] == "}":
            depth -= 1
            if depth == 0:
                return k
    return len(s) - 1


def attributes_before(s: str, pos: int) -> str:
    """The attribute text (`#[..]` lines) directly preceding the item at `pos`."""
    k = pos
    # Walk back over visibility / qualifiers on the item's own line.
    line_start = s.rfind("\n", 0, k) + 1
    attrs = []
    k = line_start
    while True:
        prev_end = k - 1
        if prev_end <= 0:
            break
        prev_start = s.rfind("\n", 0, prev_end) + 1
        line = s[prev_start:prev_end].strip()
        if line.startswith("#[") or line.startswith("#!["):
            attrs.append(line)
            k = prev_start
        elif line == "" or line.endswith(")]") or line.endswith("]"):
            # Multi-line attribute tail, or a blank line inside doc comments.
            if line == "":
                k = prev_start
                continue
            attrs.append(line)
            k = prev_start
        else:
            break
    return " ".join(attrs)


def is_test_attr(attrs: str) -> bool:
    return bool(re.search(r"#\[\s*cfg\s*\(\s*test\s*\)\s*\]", attrs)) or bool(
        re.search(r"#\[\s*test\s*\]", attrs)
    )


@dataclass
class Fn:
    name: str
    line: int
    asserts: int


@dataclass
class Module:
    path: str
    fns: list[Fn] = field(default_factory=list)
    const_asserts: int = 0

    @property
    def fn_asserts(self) -> int:
        return sum(f.asserts for f in self.fns)

    @property
    def total(self) -> int:
        return self.fn_asserts + self.const_asserts

    @property
    def avg(self) -> float:
        return self.total / len(self.fns) if self.fns else float("inf")


def test_regions(s: str) -> list[tuple[int, int]]:
    """Spans of `#[cfg(test)]` / `#[test]` items with a body."""
    regions = []
    for m in re.finditer(r"#\[\s*(cfg\s*\(\s*test\s*\)|test)\s*\]", s):
        # The item follows: find its first `{` or `;`.
        k = m.end()
        while k < len(s) and s[k] not in "{;":
            k += 1
        if k < len(s) and s[k] == "{":
            regions.append((m.start(), match_brace(s, k)))
    return regions


def analyse(path: str) -> tuple[Module, list[str]]:
    with open(path, encoding="utf-8") as f:
        src = f.read()
    s = strip(src)
    module = Module(path)
    skipped_children = []
    for m in MOD_DECL_RE.finditer(s):
        if is_test_attr(attributes_before(s, m.start())):
            skipped_children.append(m.group(1))
    excluded = test_regions(s)

    def in_test(pos: int) -> bool:
        return any(a <= pos <= b for a, b in excluded)

    bodies = []  # (start, end, name, line)
    for m in FN_RE.finditer(s):
        if in_test(m.start()):
            continue
        k = m.end()
        paren = 0
        angle_ok = True
        while k < len(s):
            ch = s[k]
            if ch in "([":
                paren += 1
            elif ch in ")]":
                paren -= 1
            elif paren == 0 and ch in "{;":
                break
            k += 1
        if not angle_ok or k >= len(s) or s[k] == ";":
            continue  # a declaration without a body
        end = match_brace(s, k)
        bodies.append((k, end, m.group(1), s.count("\n", 0, m.start()) + 1))

    for start, end, name, line in bodies:
        nested = [(a, b) for a, b, _, _ in bodies if start < a and b < end]
        count = 0
        for a in ASSERT_RE.finditer(s, start, end):
            if any(x <= a.start() <= y for x, y in nested):
                continue
            count += 1
        module.fns.append(Fn(name, line, count))

    # Compile-time assertions outside any function body.
    for m in CONST_ASSERT_RE.finditer(s):
        if in_test(m.start()):
            continue
        if any(a <= m.start() <= b for a, b, _, _ in bodies):
            continue
        module.const_asserts += 1
    return module, skipped_children


def collect(root: str) -> list[Module]:
    files = []
    if os.path.isfile(root):
        files = [root]
    else:
        for dirpath, dirnames, filenames in os.walk(root):
            dirnames[:] = sorted(d for d in dirnames if d != "tests")
            files.extend(
                os.path.join(dirpath, f)
                for f in sorted(filenames)
                if f.endswith(".rs") and f != "tests.rs"
            )
    modules, skip = [], set()
    for path in files:
        module, children = analyse(path)
        base = os.path.dirname(path)
        stem = os.path.splitext(os.path.basename(path))[0]
        for child in children:
            # `mod x;` in `a/b.rs` lives at `a/b/x.rs` (or `a/x.rs` from lib/mod).
            here = base if stem in ("lib", "mod", "main") else os.path.join(base, stem)
            skip.add(os.path.normpath(os.path.join(here, child + ".rs")))
            skip.add(os.path.normpath(os.path.join(here, child, "mod.rs")))
        modules.append(module)
    return [m for m in modules if os.path.normpath(m.path) not in skip]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("paths", nargs="*", default=DEFAULT_SCOPES)
    parser.add_argument("--min", type=float, default=None, help="fail below this average")
    parser.add_argument(
        "--per-module", action="store_true", help="apply --min to every module, not only scopes"
    )
    parser.add_argument("--functions", action="store_true", help="list every function")
    args = parser.parse_args()

    failed = []
    grand_fns = grand_asserts = 0
    for root in args.paths:
        modules = collect(root)
        fns = sum(len(m.fns) for m in modules)
        asserts = sum(m.total for m in modules)
        grand_fns += fns
        grand_asserts += asserts
        print(f"== {root}")
        print(f"   {'module':<52} {'fns':>5} {'asserts':>8} {'const':>6} {'avg':>6}")
        for m in sorted(modules, key=lambda m: m.path):
            if not m.fns:
                continue
            rel = os.path.relpath(m.path, root if os.path.isdir(root) else os.path.dirname(root))
            flag = ""
            if args.min is not None and args.per_module and m.avg < args.min:
                flag = "  < min"
                failed.append(m.path)
            print(
                f"   {rel:<52} {len(m.fns):>5} {m.fn_asserts:>8} {m.const_asserts:>6}"
                f" {m.avg:>6.2f}{flag}"
            )
            if args.functions:
                for f in m.fns:
                    print(f"       {f.line:>5} {f.name:<44} {f.asserts:>3}")
        avg = asserts / fns if fns else float("inf")
        flag = ""
        if args.min is not None and avg < args.min:
            flag = "  < min"
            failed.append(root)
        print(f"   {'TOTAL':<52} {fns:>5} {asserts:>8} {'':>6} {avg:>6.2f}{flag}\n")
    if len(args.paths) > 1 and grand_fns:
        print(f"ALL: {grand_fns} functions, {grand_asserts} assertions, "
              f"average {grand_asserts / grand_fns:.2f}")
    if failed:
        print(f"\nbelow --min {args.min}: {len(failed)}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
