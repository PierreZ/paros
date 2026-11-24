# paros-election

A coordination store over one paros journal, on the ideas of the ZooKeeper client and of
Apache Curator. The crate name is **Open**.

The store owns one journal in multi-writer mode. Every member appends to it, the leader and the
candidates too, and every member runs the same fold over it: a tree of nodes, a table of
sessions, and the turn of each latch. The journal arbitrates nothing. A member learns the
result of its entry when its fold reaches it. The prior art is Zelos, the ZooKeeper clone that
Facebook built on the Delos shared log.

- Sessions, renewed by heartbeats and expired by a compare-and-set on their last position.
- Nodes with data and a version; ephemeral nodes that go with their session; sequential nodes
  named by the position of their create; transactions with checks, all or none.
- Recipes as functions of the fold: `Latch` (Curator's `LeaderLatch`), `Group`
  (`GroupMember`), `Counter` (`SharedCount`).
- The term of a latch: the position at which its holder changed. A holder guards its writes in
  the store with the term, and each receiver of an order outside the store checks the term in
  the same write as the effect.
- Checkpoints and a trim that goes only to a checkpoint the fold took.

The crate is pure: it depends on `paros-journal` only, reads no clock, draws no random number
and builds for `wasm32-unknown-unknown`. A `Member` owns one journal client and has its shape:
calls in, requests out, answers in. The program gives it the time and each draw.

The rules are in `docs/architecture/04-roles.md` §4.4, "The coordination store", and their TLA+
model is `specs/tla/coord/`.

## Example

`cargo run -p paros-election --example latch`: three members elect a leader, the leader stops,
another takes over at a higher term, the fences hold, and a new member starts from a checkpoint.
