//! The declared-gate scan (#446 (a gate that never fires cannot fail the
//! sweep)): every coverage gate the source declares, read from the literal
//! messages of its macros.
//!
//! moonpool creates an assertion slot only when the assertion runs, so the
//! sweep's saturation check reads only the gates that ran at least once. A
//! gate that never runs has no slot and fails nothing. This scan gives the
//! sweep the other half: the list of gates that must have a slot. The build
//! script runs it over the crates the campaign runs and compiles the result
//! into `sim-paros-chain` ([`crate::gates`]).
//!
//! The scan is a small lexer, not a parser. It skips comments and every item
//! under `#[cfg(test)]` (a unit test's gate never runs in the campaign), and
//! for each gate macro it takes the first top-level argument that is one
//! plain string literal: `assert_reachable!("m")`, `reach_once!(flag, "m")`,
//! `assert_sometimes!(cond, "m")`, `probe!(sometimes, cond, "m")`. A gate whose
//! message is not a literal is not declared (the root *Assertion budget* rule
//! keeps every message a literal).

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

/// The macros that declare a coverage gate. `assert_sometimes_all!` and the
/// numeric `sometimes` are left out: each is evaluated on every run.
const GATE_MACROS: &[&str] = &[
    "assert_reachable",
    "reachable",
    "reach_once",
    "assert_sometimes",
    "sometimes",
    "probe",
];

/// One gate the source declares.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Gate {
    /// The gate's message: its slot's identity.
    pub msg: String,
    /// Where it is declared, `path:line`, relative to the scanned root.
    pub site: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Tok {
    Ident(String),
    Str(String),
    Punct(char),
    /// A literal that is not a plain string (number, char, byte string).
    Lit,
}

/// Lex `src` into tokens with their line numbers, comments dropped.
fn lex(src: &str) -> Vec<(Tok, usize)> {
    let chars: Vec<char> = src.chars().collect();
    let mut toks = Vec::new();
    let mut i = 0;
    let mut line = 1;
    while i < chars.len() {
        let c = chars[i];
        let start_line = line;
        if c == '\n' {
            line += 1;
            i += 1;
        } else if c.is_whitespace() {
            i += 1;
        } else if c == '/' && chars.get(i + 1) == Some(&'/') {
            while i < chars.len() && chars[i] != '\n' {
                i += 1;
            }
        } else if c == '/' && chars.get(i + 1) == Some(&'*') {
            let mut depth = 0;
            while i < chars.len() {
                if chars[i] == '/' && chars.get(i + 1) == Some(&'*') {
                    depth += 1;
                    i += 2;
                } else if chars[i] == '*' && chars.get(i + 1) == Some(&'/') {
                    depth -= 1;
                    i += 2;
                    if depth == 0 {
                        break;
                    }
                } else {
                    if chars[i] == '\n' {
                        line += 1;
                    }
                    i += 1;
                }
            }
        } else if c == '"' {
            let (s, next, lines) = lex_string(&chars, i + 1);
            toks.push((Tok::Str(s), start_line));
            line += lines;
            i = next;
        } else if (c == 'r' || c == 'b')
            && let Some((next, lines, plain)) = lex_prefixed(&chars, i)
        {
            // A raw string is still a string literal; a byte string is not.
            if let Some(s) = plain {
                toks.push((Tok::Str(s), start_line));
            } else {
                toks.push((Tok::Lit, start_line));
            }
            line += lines;
            i = next;
        } else if c == '\'' {
            // A char literal ('a', '\n', '\'') or a lifetime ('a).
            if chars.get(i + 1) == Some(&'\\') {
                let mut j = i + 2;
                while j < chars.len() && chars[j] != '\'' {
                    j += 1;
                }
                toks.push((Tok::Lit, start_line));
                i = j + 1;
            } else if chars.get(i + 2) == Some(&'\'') {
                toks.push((Tok::Lit, start_line));
                i += 3;
            } else {
                toks.push((Tok::Punct('\''), start_line));
                i += 1;
            }
        } else if c.is_alphabetic() || c == '_' {
            let mut j = i;
            while j < chars.len() && (chars[j].is_alphanumeric() || chars[j] == '_') {
                j += 1;
            }
            toks.push((Tok::Ident(chars[i..j].iter().collect()), start_line));
            i = j;
        } else if c.is_ascii_digit() {
            let mut j = i;
            while j < chars.len() && (chars[j].is_alphanumeric() || chars[j] == '_') {
                j += 1;
            }
            toks.push((Tok::Lit, start_line));
            i = j;
        } else {
            toks.push((Tok::Punct(c), start_line));
            i += 1;
        }
    }
    toks
}

/// A plain string's contents from `start` (after the quote): the value, the
/// index after the closing quote and the newlines it spans.
fn lex_string(chars: &[char], start: usize) -> (String, usize, usize) {
    let mut s = String::new();
    let mut lines = 0;
    let mut i = start;
    while i < chars.len() && chars[i] != '"' {
        if chars[i] == '\n' {
            lines += 1;
        }
        if chars[i] == '\\' && i + 1 < chars.len() {
            i += 1;
            match chars[i] {
                'n' => s.push('\n'),
                't' => s.push('\t'),
                '\\' => s.push('\\'),
                '"' => s.push('"'),
                '\'' => s.push('\''),
                '0' => s.push('\0'),
                '\n' => {
                    // A line continuation drops the newline and the
                    // indentation that follows it.
                    lines += 1;
                    while i + 1 < chars.len() && chars[i + 1].is_whitespace() {
                        if chars[i + 1] == '\n' {
                            lines += 1;
                        }
                        i += 1;
                    }
                }
                other => {
                    s.push('\\');
                    s.push(other);
                }
            }
        } else {
            s.push(chars[i]);
        }
        i += 1;
    }
    (s, i + 1, lines)
}

/// A `r"..."`, `r#"..."#`, `b"..."` or `br"..."` literal at `i`: the index
/// after it, the newlines it spans, and its value when it is a plain (raw)
/// string. `None` when `i` starts an identifier instead.
fn lex_prefixed(chars: &[char], i: usize) -> Option<(usize, usize, Option<String>)> {
    let byte = chars[i] == 'b';
    let mut j = i + 1;
    if byte && chars.get(j) == Some(&'"') {
        let (_, next, lines) = lex_string(chars, j + 1);
        return Some((next, lines, None));
    }
    if byte {
        if chars.get(j) != Some(&'r') {
            return None;
        }
        j += 1;
    }
    let mut hashes = 0;
    while chars.get(j) == Some(&'#') {
        hashes += 1;
        j += 1;
    }
    if chars.get(j) != Some(&'"') {
        return None;
    }
    j += 1;
    let body_start = j;
    loop {
        if j >= chars.len() {
            return None;
        }
        if chars[j] == '"' && (1..=hashes).all(|k| chars.get(j + k) == Some(&'#')) {
            let body: String = chars[body_start..j].iter().collect();
            let lines = body.matches('\n').count();
            return Some((j + 1 + hashes, lines, (!byte).then_some(body)));
        }
        j += 1;
    }
}

/// The index of the token that closes the group opened at `open`.
fn matching(toks: &[(Tok, usize)], open: usize) -> usize {
    let mut depth = 0usize;
    for (k, (tok, _)) in toks.iter().enumerate().skip(open) {
        match tok {
            Tok::Punct('(' | '[' | '{') => depth += 1,
            Tok::Punct(')' | ']' | '}') => {
                depth -= 1;
                if depth == 0 {
                    return k;
                }
            }
            _ => {}
        }
    }
    toks.len()
}

/// Whether the tokens at `i` are `#[cfg(test)]`; the index after it.
fn cfg_test_at(toks: &[(Tok, usize)], i: usize) -> Option<usize> {
    let want = [
        Tok::Punct('#'),
        Tok::Punct('['),
        Tok::Ident("cfg".into()),
        Tok::Punct('('),
        Tok::Ident("test".into()),
        Tok::Punct(')'),
        Tok::Punct(']'),
    ];
    let matched = want
        .iter()
        .enumerate()
        .all(|(k, tok)| toks.get(i + k).map(|(t, _)| t) == Some(tok));
    matched.then_some(i + want.len())
}

/// What one file declares: its gates, and the out-of-line modules under
/// `#[cfg(test)]` (`mod tests;`) whose files the scan skips.
struct FileScan {
    gates: Vec<(String, usize)>,
    test_modules: Vec<String>,
}

fn scan_tokens(toks: &[(Tok, usize)]) -> FileScan {
    let mut scan = FileScan {
        gates: Vec::new(),
        test_modules: Vec::new(),
    };
    let mut i = 0;
    while i < toks.len() {
        if let Some(mut j) = cfg_test_at(toks, i) {
            // Further attributes on the same item.
            while toks.get(j).map(|(t, _)| t) == Some(&Tok::Punct('#')) {
                j = matching(toks, j + 1) + 1;
            }
            // Skip the item: to its body's end, or to its `;`.
            let mut k = j;
            while k < toks.len() {
                match &toks[k].0 {
                    Tok::Punct(';') => break,
                    Tok::Punct('{') => {
                        k = matching(toks, k);
                        break;
                    }
                    Tok::Punct('(' | '[') => k = matching(toks, k) + 1,
                    _ => k += 1,
                }
            }
            if let (
                Some((Tok::Ident(m), _)),
                Some((Tok::Ident(name), _)),
                Some((Tok::Punct(';'), _)),
            ) = (
                toks.get(k.saturating_sub(2)),
                toks.get(k.saturating_sub(1)),
                toks.get(k),
            ) && m == "mod"
            {
                scan.test_modules.push(name.clone());
            }
            i = k + 1;
            continue;
        }
        if let (Tok::Ident(name), line) = &toks[i]
            && GATE_MACROS.contains(&name.as_str())
            && toks.get(i + 1).map(|(t, _)| t) == Some(&Tok::Punct('!'))
            && toks.get(i + 2).map(|(t, _)| t) == Some(&Tok::Punct('('))
            // `x.reachable!` is not a path; `moonpool_assertions::reachable!` is.
            && (i == 0 || toks[i - 1].0 != Tok::Punct('.'))
        {
            let close = matching(toks, i + 2);
            if let Some(msg) = first_literal_argument(&toks[i + 3..close]) {
                scan.gates.push((msg, *line));
            }
            // Gates nested in the arguments are scanned too.
            i += 3;
            continue;
        }
        i += 1;
    }
    scan
}

/// The first top-level argument that is exactly one plain string literal.
fn first_literal_argument(args: &[(Tok, usize)]) -> Option<String> {
    let mut depth = 0usize;
    let mut current: Vec<&Tok> = Vec::new();
    let mut arguments: Vec<Vec<&Tok>> = Vec::new();
    for (tok, _) in args {
        match tok {
            Tok::Punct('(' | '[' | '{') => depth += 1,
            Tok::Punct(')' | ']' | '}') => depth -= 1,
            Tok::Punct(',') if depth == 0 => {
                arguments.push(std::mem::take(&mut current));
                continue;
            }
            _ => {}
        }
        current.push(tok);
    }
    arguments.push(current);
    arguments.into_iter().find_map(|arg| match arg.as_slice() {
        [Tok::Str(s)] => Some(s.clone()),
        _ => None,
    })
}

/// Every gate declared under `root` (a crate's `src`), in path order. Files
/// of a `#[cfg(test)]` module are skipped.
///
/// # Panics
///
/// Panics if a directory or file under `root` cannot be read.
#[must_use]
pub fn scan_root(root: &Path, label: &str) -> Vec<Gate> {
    let mut files = Vec::new();
    collect(root, &mut files);
    files.sort();
    let mut skipped: BTreeSet<PathBuf> = BTreeSet::new();
    let mut gates = Vec::new();
    for file in files {
        if skipped.iter().any(|dir| file.starts_with(dir)) {
            continue;
        }
        let src = std::fs::read_to_string(&file)
            .unwrap_or_else(|e| panic!("read {}: {e}", file.display()));
        let scan = scan_tokens(&lex(&src));
        let base = module_dir(&file);
        for name in scan.test_modules {
            skipped.insert(base.join(format!("{name}.rs")));
            skipped.insert(base.join(name));
        }
        let rel = file.strip_prefix(root).unwrap_or(&file);
        for (msg, line) in scan.gates {
            gates.push(Gate {
                msg,
                site: format!("{label}/{}:{line}", rel.display()),
            });
        }
    }
    gates
}

/// The directory a file's out-of-line child modules live in.
fn module_dir(file: &Path) -> PathBuf {
    let dir = file.parent().unwrap_or(Path::new("")).to_path_buf();
    match file.file_stem().and_then(|s| s.to_str()) {
        Some(stem) if !matches!(stem, "lib" | "main" | "mod") => dir.join(stem),
        _ => dir,
    }
}

fn collect(dir: &Path, files: &mut Vec<PathBuf>) {
    let entries = std::fs::read_dir(dir).unwrap_or_else(|e| panic!("read {}: {e}", dir.display()));
    for entry in entries {
        let path = entry.expect("a directory entry").path();
        if path.is_dir() {
            collect(&path, files);
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            files.push(path);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{lex, scan_tokens};

    fn gates(src: &str) -> Vec<String> {
        scan_tokens(&lex(src))
            .gates
            .into_iter()
            .map(|(msg, _)| msg)
            .collect()
    }

    #[test]
    fn each_macro_form_yields_its_message() {
        let src = r#"
            fn f() {
                assert_reachable!("a");
                moonpool_assertions::reachable!("b");
                reach_once!(self.flag, "c");
                assert_sometimes!(x == "not me", "d");
                probe!(reachable, "e");
                probe!(sometimes, f(1, "no"), "f");
                assert_sometimes!(cond, r"g");
                unreachable!("not a gate");
                assert_unreachable!("not a gate either");
            }
        "#;
        assert_eq!(gates(src), ["a", "b", "c", "d", "e", "f", "g"]);
    }

    #[test]
    fn comments_test_items_and_non_literals_declare_nothing() {
        let src = r#"
            // assert_reachable!("line comment");
            /* assert_reachable!("block /* nested */ comment"); */
            /// assert_reachable!("doc");
            #[cfg(test)]
            mod tests { fn t() { assert_reachable!("unit test"); } }
            #[cfg(test)]
            #[allow(dead_code)]
            fn helper() { assert_reachable!("test helper"); }
            #[cfg(test)]
            mod out_of_line;
            fn g(m: &str) { reach_once!(flag, m); assert_reachable!("kept"); }
        "#;
        let scan = scan_tokens(&lex(src));
        let msgs: Vec<_> = scan.gates.iter().map(|(m, _)| m.as_str()).collect();
        assert_eq!(msgs, ["kept"]);
        assert_eq!(scan.test_modules, ["out_of_line"]);
    }

    #[test]
    fn a_message_split_by_a_continuation_reads_whole() {
        let src = "assert_reachable!(\"one \\\n        two\");\nreachable!('x', \"three\");";
        let scan = scan_tokens(&lex(src));
        assert_eq!(
            scan.gates,
            [("one two".to_owned(), 1), ("three".to_owned(), 3)]
        );
    }
}
