//! **paros-election**: a coordination store over one paros journal, on the
//! ideas of the `ZooKeeper` client and of Apache Curator
//! (`docs/architecture/04-roles.md` §4.4, "The coordination store"; the
//! model is `specs/tla/coord/`). The crate name is **Open**.
//!
//! **The log is the server.** The store owns one journal in multi-writer
//! mode: every member appends, the leader and the candidates too. The
//! journal arbitrates nothing. Every member runs the same fold
//! ([`tree::Tree`]) over the journal: a pure function from the entries, in
//! order, to a tree of nodes, a table of sessions and the turn of each
//! parent. A member learns the result of its entry when its fold reaches
//! it (Delos: `propose`, then play the log to that entry; the prior art is
//! Zelos, the `ZooKeeper` clone that Facebook built on the Delos shared
//! log).
//!
//! The `ZooKeeper` concepts: the position of an entry is the zxid; a
//! [`entry::SessionId`] is drawn fresh by the program; a node has data, a
//! version and the positions of its create and its last change; an
//! ephemeral node goes with its session; a sequential node takes the
//! position of its create as its name; a transaction ([`entry::Txn`]) is
//! a `multi`, all or none; a watch is a filter on the
//! [`session::Event::Changed`] of a member, which tails the journal.
//!
//! - [`path`]: the path of a node.
//! - [`entry`]: what a member appends: `Open`, `Txn`, `Expire`,
//!   `Checkpoint`.
//! - [`tree`]: the fold.
//! - [`codec`]: the bytes of an entry in a journal record.
//! - [`session`]: a member, over one journal client.
//! - [`recipes`]: Curator's `LeaderLatch`, `GroupMember` and `SharedCount`
//!   as functions of the fold.
//!
//! The crate is pure: it depends on `paros-journal` only, reads no clock
//! and draws no random number, and builds for `wasm32-unknown-unknown`.

pub mod codec;
pub mod entry;
pub mod path;
pub mod recipes;
pub mod session;
pub mod tree;

pub use entry::{Check, Entry, Op, SessionId, Term, Txn};
pub use path::Path;
pub use session::{Config, Event, Member, Settled, State, TxnId};
pub use tree::{Change, ErrorKind, Fate, Node, Row, Tree, Turn, TxnError};
