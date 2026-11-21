//! The helpers that the examples of `paros-journal` share: the records and
//! the writes they build, and the line that prints an answer.
//!
//! This is Cargo's place for code shared by examples: a file
//! `examples/common.rs` would be built as an example of its own.

#![allow(dead_code)]

use paros_journal::{Answer, Fence, LeaderUuid, Record, Seq, Write};

/// A record whose payload is `text`.
pub fn record(text: &str) -> Record {
    Record::new(text.as_bytes().to_vec())
}

/// A single-writer write of `texts` by the writer `uuid`, which asks for
/// the position `expected_seq`.
pub fn fenced(uuid: LeaderUuid, expected_seq: u64, texts: &[&str]) -> Write {
    Write {
        fence: Fence::Leader {
            uuid,
            expected_seq: Seq(expected_seq),
        },
        records: texts.iter().map(|text| record(text)).collect(),
    }
}

/// A multi-writer write of `texts`: no fence, the journal gives the
/// position.
pub fn open(texts: &[&str]) -> Write {
    Write {
        fence: Fence::Open,
        records: texts.iter().map(|text| record(text)).collect(),
    }
}

/// The answer of a write accepted at `seq` with `count` records.
pub fn written(seq: u64, count: u64) -> Answer {
    Answer::Written {
        seq: Seq(seq),
        count,
    }
}

/// Print one step of an example: what was asked, and what came back.
pub fn show(what: &str, answer: &impl std::fmt::Debug) {
    println!("  {what:<46} -> {answer:?}");
}
