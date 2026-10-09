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
