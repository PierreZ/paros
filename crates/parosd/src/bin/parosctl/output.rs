//! What `parosctl` prints: text for people (one line per record or answer,
//! `key=value` details), or one JSON document per answer with `--json`.
//! Diagnostics — a claim made on the way, a truncation gap a reader
//! jumped — go to stderr in both modes.

use paros::JournalView;
use serde_json::{Value, json};

/// The output mode.
pub struct Printer {
    json: bool,
}

impl Printer {
    pub fn new(json: bool) -> Self {
        Self { json }
    }

    /// Print one answer: `text()` for people, `doc()` with `--json`.
    pub fn emit(&self, text: impl FnOnce() -> String, doc: impl FnOnce() -> Value) {
        if self.json {
            println!("{}", doc());
        } else {
            println!("{}", text());
        }
    }
}

/// A diagnostic, on stderr (in both output modes).
pub fn note(message: &str) {
    eprintln!("parosctl: {message}");
}

/// A journal view as `key=value` pairs.
pub fn state_text(state: &JournalView) -> String {
    format!(
        "leader={} next_seq={} first_seq={}",
        state
            .leader
            .map_or_else(|| "none".to_string(), |leader| leader.to_string()),
        state.next_seq.0,
        state.first_seq.0
    )
}

/// A journal view as JSON.
pub fn state_json(state: &JournalView) -> Value {
    json!({
        "leader": state.leader.map(|leader| leader.to_string()),
        "next_seq": state.next_seq.0,
        "first_seq": state.first_seq.0,
    })
}

/// A record's bytes as text (UTF-8, lossily).
pub fn record_text(record: &[u8]) -> String {
    String::from_utf8_lossy(record).into_owned()
}

/// One id alone, abbreviated (#239): a line that names a single id has no
/// listing to widen against.
pub fn short(id: u64) -> String {
    paros::name::Abbreviations::new([id]).id(id)
}

/// Rows as an aligned table under `headers`, two spaces between columns
/// (the last column is not padded).
pub fn table<const N: usize>(headers: [&str; N], rows: &[[String; N]]) -> String {
    let mut widths = headers.map(str::len);
    for row in rows {
        for (width, cell) in widths.iter_mut().zip(row) {
            *width = (*width).max(cell.chars().count());
        }
    }
    let line = |cells: [&str; N]| {
        let mut text = String::new();
        for (i, (cell, width)) in cells.iter().zip(widths).enumerate() {
            if i + 1 == N {
                text.push_str(cell);
            } else {
                text.push_str(cell);
                text.push_str(&" ".repeat(width - cell.chars().count() + 2));
            }
        }
        text.trim_end().to_string()
    };
    let mut lines = vec![line(headers)];
    lines.extend(
        rows.iter()
            .map(|row| line(row.each_ref().map(String::as_str))),
    );
    lines.join("\n")
}

#[cfg(test)]
mod tests {
    use super::table;

    #[test]
    fn a_table_aligns_its_columns() {
        let rows = [
            ["parosd-1".to_string(), "up".to_string()],
            ["b".to_string(), "down".to_string()],
        ];
        assert_eq!(
            table(["NAME", "STATE"], &rows),
            "NAME      STATE\nparosd-1  up\nb         down"
        );
    }
}
