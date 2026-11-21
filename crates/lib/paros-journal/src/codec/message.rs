//! The bytes of the **messages between the journal roles**
//! ([`roles::Message`](crate::roles::Message), §J4.1): what a transport
//! between two processes sends.
//!
//! A message has the header of every encoding of the journal (the release,
//! then the kind 5), then a tag and the fields of its variant, in the order
//! of their declaration:
//!
//! - a ballot: `round: u64`, then `node: u64`. A slot, a segment id and a
//!   position are a `u64`.
//! - `0` `Prepare` (ballot, from), `1` `Promise` (ballot, from, page), `2`
//!   `Nack` (ballot, promised), `3` `Accept` (ballot, slot, command), `4`
//!   `Accepted` (ballot, slot), `5` `Commit` (segment, slot, command), `6`
//!   `Applied` (segment, slot, command, answer), `7` `Pull` (segment, from).
//! - a promise page: the list of votes (slot, ballot, command), the list of
//!   faulty entries (slot, ballot), then the optional next slot. Each list is
//!   in strictly increasing slot order: the decoder refuses a slot that is
//!   not above the slot before it, so one page has one encoding.
//! - an answer: a tag, `0` `Written` and `1` `Duplicate` (seq, count), `2`
//!   `Taken`, `3` `Fenced`, `4` `Early`, `5` `Truncated`, `6` `Below`, `7`
//!   `WrongMode`, `8` `Empty`, `9` `LeaderSet`, `10` `LeaderRefused`, `11`
//!   `Trimmed`, `12` `Reserved` (each with a view), `13` `Noop`.
//! - a view: the leader value, `next_seq`, `first_seq`. A view whose
//!   `first_seq` is above its `next_seq` is refused, as a boundary state is.
//!
//! The decoder trusts no peer: a message that a correct peer cannot send
//! is a [`DecodeError`], never a panic. The role that receives a decoded
//! message still judges it against its own state.

use std::collections::BTreeMap;

use paros_consensus::journal::Seq;
use paros_consensus::{Ballot, NodeId, Slot};

use super::{DecodeError, In, MESSAGE, Out};
use crate::fold::{Answer, JournalView};
use crate::roles::{Message, PromisePage};
use crate::segment::SegmentId;

/// The bytes of `message`.
#[must_use]
pub fn encode_message(message: &Message) -> Vec<u8> {
    let mut out = Out::new(MESSAGE);
    out.message(message);
    out.finish()
}

/// The message in `bytes`.
///
/// # Errors
///
/// When the bytes are not one role message of a known release.
pub fn decode_message(bytes: &[u8]) -> Result<Message, DecodeError> {
    let mut input = In::new(bytes, MESSAGE)?;
    let message = input.message()?;
    input.finish()?;
    Ok(message)
}

impl Out {
    fn message(&mut self, message: &Message) {
        match message {
            Message::Prepare { ballot, from } => {
                self.u8(0);
                self.ballot(*ballot);
                self.u64(from.0);
            }
            Message::Promise { ballot, from, page } => {
                self.u8(1);
                self.ballot(*ballot);
                self.u64(from.0);
                self.page(page);
            }
            Message::Nack { ballot, promised } => {
                self.u8(2);
                self.ballot(*ballot);
                self.ballot(*promised);
            }
            Message::Accept {
                ballot,
                slot,
                command,
            } => {
                self.u8(3);
                self.ballot(*ballot);
                self.u64(slot.0);
                self.command(command);
            }
            Message::Accepted { ballot, slot } => {
                self.u8(4);
                self.ballot(*ballot);
                self.u64(slot.0);
            }
            Message::Commit {
                segment,
                slot,
                command,
            } => {
                self.u8(5);
                self.u64(segment.0);
                self.u64(slot.0);
                self.command(command);
            }
            Message::Applied {
                segment,
                slot,
                command,
                answer,
            } => {
                self.u8(6);
                self.u64(segment.0);
                self.u64(slot.0);
                self.command(command);
                self.answer(answer);
            }
            Message::Pull { segment, from } => {
                self.u8(7);
                self.u64(segment.0);
                self.u64(from.0);
            }
        }
    }

    fn ballot(&mut self, ballot: Ballot) {
        self.u64(ballot.round);
        self.u64(ballot.node.0);
    }

    fn page(&mut self, page: &PromisePage) {
        self.u32_len(page.votes.len());
        for (slot, (ballot, command)) in &page.votes {
            self.u64(slot.0);
            self.ballot(*ballot);
            self.command(command);
        }
        self.u32_len(page.faulty.len());
        for (slot, ballot) in &page.faulty {
            self.u64(slot.0);
            self.ballot(*ballot);
        }
        self.option(page.next, |out, slot| out.u64(slot.0));
    }

    fn answer(&mut self, answer: &Answer) {
        let (tag, view) = match answer {
            Answer::Written { seq, count } => {
                self.u8(0);
                self.u64(seq.0);
                self.u64(*count);
                return;
            }
            Answer::Duplicate { seq, count } => {
                self.u8(1);
                self.u64(seq.0);
                self.u64(*count);
                return;
            }
            Answer::Noop => {
                self.u8(13);
                return;
            }
            Answer::Taken(view) => (2, view),
            Answer::Fenced(view) => (3, view),
            Answer::Early(view) => (4, view),
            Answer::Truncated(view) => (5, view),
            Answer::Below(view) => (6, view),
            Answer::WrongMode(view) => (7, view),
            Answer::Empty(view) => (8, view),
            Answer::LeaderSet(view) => (9, view),
            Answer::LeaderRefused(view) => (10, view),
            Answer::Trimmed(view) => (11, view),
            Answer::Reserved(view) => (12, view),
        };
        self.u8(tag);
        self.view(view);
    }

    fn view(&mut self, view: &JournalView) {
        assert!(
            view.first_seq <= view.next_seq,
            "an encoded view has its floor at or below its tail"
        );
        self.leader(view.leader);
        self.u64(view.next_seq.0);
        self.u64(view.first_seq.0);
    }
}

impl In<'_> {
    fn message(&mut self) -> Result<Message, DecodeError> {
        match self.u8()? {
            0 => Ok(Message::Prepare {
                ballot: self.ballot()?,
                from: self.slot()?,
            }),
            1 => Ok(Message::Promise {
                ballot: self.ballot()?,
                from: self.slot()?,
                page: self.page()?,
            }),
            2 => Ok(Message::Nack {
                ballot: self.ballot()?,
                promised: self.ballot()?,
            }),
            3 => Ok(Message::Accept {
                ballot: self.ballot()?,
                slot: self.slot()?,
                command: self.command()?,
            }),
            4 => Ok(Message::Accepted {
                ballot: self.ballot()?,
                slot: self.slot()?,
            }),
            5 => Ok(Message::Commit {
                segment: SegmentId(self.u64()?),
                slot: self.slot()?,
                command: self.command()?,
            }),
            6 => Ok(Message::Applied {
                segment: SegmentId(self.u64()?),
                slot: self.slot()?,
                command: self.command()?,
                answer: self.answer()?,
            }),
            7 => Ok(Message::Pull {
                segment: SegmentId(self.u64()?),
                from: self.slot()?,
            }),
            tag => Err(DecodeError::Tag {
                what: "message",
                tag,
            }),
        }
    }

    fn slot(&mut self) -> Result<Slot, DecodeError> {
        self.u64().map(Slot)
    }

    fn ballot(&mut self) -> Result<Ballot, DecodeError> {
        Ok(Ballot {
            round: self.u64()?,
            node: NodeId(self.u64()?),
        })
    }

    /// A slot strictly above `last`: the lists of a page are in increasing
    /// slot order, with no slot twice.
    fn next_slot(&mut self, last: Option<Slot>) -> Result<Slot, DecodeError> {
        let slot = self.slot()?;
        if last.is_some_and(|last| slot <= last) {
            return Err(DecodeError::Invalid(
                "the slots of a promise page are not increasing",
            ));
        }
        Ok(slot)
    }

    fn page(&mut self) -> Result<PromisePage, DecodeError> {
        let mut votes = BTreeMap::new();
        let mut last = None;
        for _ in 0..self.count()? {
            let slot = self.next_slot(last)?;
            last = Some(slot);
            votes.insert(slot, (self.ballot()?, self.command()?));
        }
        let mut faulty = BTreeMap::new();
        let mut last = None;
        for _ in 0..self.count()? {
            let slot = self.next_slot(last)?;
            last = Some(slot);
            faulty.insert(slot, self.ballot()?);
        }
        let next = self.option("next page", Self::slot)?;
        Ok(PromisePage {
            votes,
            faulty,
            next,
        })
    }

    fn answer(&mut self) -> Result<Answer, DecodeError> {
        Ok(match self.u8()? {
            0 => Answer::Written {
                seq: Seq(self.u64()?),
                count: self.u64()?,
            },
            1 => Answer::Duplicate {
                seq: Seq(self.u64()?),
                count: self.u64()?,
            },
            2 => Answer::Taken(self.view()?),
            3 => Answer::Fenced(self.view()?),
            4 => Answer::Early(self.view()?),
            5 => Answer::Truncated(self.view()?),
            6 => Answer::Below(self.view()?),
            7 => Answer::WrongMode(self.view()?),
            8 => Answer::Empty(self.view()?),
            9 => Answer::LeaderSet(self.view()?),
            10 => Answer::LeaderRefused(self.view()?),
            11 => Answer::Trimmed(self.view()?),
            12 => Answer::Reserved(self.view()?),
            13 => Answer::Noop,
            tag => {
                return Err(DecodeError::Tag {
                    what: "answer",
                    tag,
                });
            }
        })
    }

    fn view(&mut self) -> Result<JournalView, DecodeError> {
        let view = JournalView {
            leader: self.leader()?,
            next_seq: Seq(self.u64()?),
            first_seq: Seq(self.u64()?),
        };
        if view.first_seq <= view.next_seq {
            Ok(view)
        } else {
            Err(DecodeError::Invalid("a view with first_seq above next_seq"))
        }
    }
}

#[cfg(test)]
mod tests;
