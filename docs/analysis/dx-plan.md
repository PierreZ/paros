# `paros-core` developer-experience plan

*Phase 0 of the DX work: what the Phase 1 library API will look like, how
`ColocatedNode` will call each new operation internally, and how the rewritten
`single_decree.rs` will call the same operation. Nothing here is implemented
yet; this note is the contract the next phases are built and reviewed
against.*

## What the read-through found

The core's two tiers really do speak different languages today, and every
seam the lessons complain about traces to one of five facts:

1. **The acceptor answers in outcomes, not in replies.** `Acceptor::prepare`
   returns a `PrepareOutcome`, the page is a second call
   (`promise_page`), and the page does not know which ballot or cursor it
   answers — the caller carries `ballot` and `from_slot` beside it into
   `fold_promise`'s six arguments. The lessons therefore invent
   `Result<Option<(Ballot, Command)>, Ballot>` and rebuild one-slot
   `BTreeMap`s. The `Accept` side is three calls (`admit`, `set_promise`,
   `record_accepted`) whose order is a safety rule the caller has to know.
2. **The wire is flat.** `Message::Promise` carries the page's four fields
   inline, `Message::Accepted` and `Message::Nack` carry theirs; nothing on
   the wire is a type a role can produce or consume, so the top tier's
   `step` router unpacks fields and calls the same six-argument folds.
3. **`Phase1Outcome` is two maps and a set.** P2c at a caller is
   `outcome.recovered.get(&slot)` with a default; `blocked` is a separate
   set that a caller can forget to consult (the decree does not consult it,
   correctly, because a decree can never block — but nothing says so).
4. **Boot has one path, the acceptor role has none.** `ColocatedNode::new`
   reads the durable log back through the read-only `Storage` port
   (`node/boot.rs::read_back_log`), asserting the write-side ordering as it
   goes. `Acceptor::new(promised, records, first_slot, faulty)` is the
   role's only constructor, so a lesson that reboots an acceptor cannot go
   through the port the node uses.
5. **The examples cannot share code without a crate.** Every example
   compiles `examples/common/mod.rs` as its own module, so a helper one
   lesson does not call is a `dead_code` warning in that lesson, and
   `cargo clippy --all-targets -- -D warnings` fails. The repository fixes
   lints rather than allowing them (the only `#[allow]`s are three narrow
   pedantic ones in tests and one `too_many_arguments`), so the shared
   harness has to be a `publish = false` workspace crate that `paros-core`
   takes as a dev-dependency — a dev-dependency cycle Cargo permits (it is
   how `serde` tests against `serde_derive`).

Three things the plan must not disturb, because callers outside the examples
depend on them: the matchmaker decree drives `Proposer<MatchmakerId,
Vec<MatchmakerId>>` and `Acceptor<Vec<MatchmakerId>>` over slot zero
(`matchmaker/decree.rs`, `matchmaker/generation.rs`), so every payload type
is generic over `V` and never names `Command`; the proxy leader folds
`Accepted`/`Nack` through the standalone `Rounds`; and `paros-play` computes
prompts on *clones* of the roles by re-folding a `Promise` and closing Phase 1
(`world/prompts.rs`, `world/drain.rs`), so the new operations must be
callable on a clone exactly like the old ones.

## Phase 1 — the protocol-shaped role API

Everything below is additive on the role surface. The one deliberate break is
the shape of three `Message` variants (last section).

### Typed payloads (`acceptor.rs`, generic over `V`)

`PromisePage<V>` is extended, not paralleled: it gains the two fields that
make it self-describing, so a page *is* the `Promise` reply.

```rust
/// The whole `Promise` reply: for `ballot`, covering `from_slot..`, one
/// bounded page of the tri-state, and the continuation cursor.
pub struct PromisePage<V> {
    pub ballot: Ballot,          // new: the ballot promised (== the acceptor's promise)
    pub from_slot: Slot,         // new: the cursor this page answers
    pub accepted: BTreeMap<Slot, (Ballot, V)>,
    pub faulty: BTreeMap<Slot, Ballot>,
    pub next_from_slot: Option<Slot>,
}

/// The `Accepted` reply: a durable vote for the value with this fingerprint
/// at `(ballot, slot)`.
pub struct Accepted { pub ballot: Ballot, pub slot: Slot, pub vhash: u64 }

impl Accepted {
    /// The vote an acceptor casts for `value` at `(ballot, slot)`.
    pub fn of<V: Fingerprint>(ballot: Ballot, slot: Slot, value: &V) -> Self;
}

/// The `Nack` reply: the refused ballot and the contested slot, echoed. The
/// promise that won deliberately does not travel (an untrusted wire value
/// must never pick a future campaign round) — today's wire, typed.
pub struct Nack { pub ballot: Ballot, pub slot: Slot }

/// What an acceptor answers an `Accept` with.
pub enum AcceptReply {
    /// Durably accepted; the writes carry the promise raise (if any) ahead
    /// of the record. Reply `Accepted`.
    Accepted(Accepted),
    /// The promise held dominates the ballot. Reply `Nack`.
    Nack(Nack),
    /// Below the compaction floor: the slot is chosen and truncated here.
    /// **No reply** — a `Nack` would depose a leader over a slot it cannot
    /// re-decide; a `Commit` or a snapshot heals the sender.
    BelowFloor,
}
```

All three derive `Clone, Debug, PartialEq, Eq` and the `serde` derives under
the feature (`paros-play` enables it). `Acceptor::promise_page(from_slot)`
fills `ballot` from the promise held.

### `Acceptor` operations

```rust
impl<V: Clone + PartialEq> Acceptor<V> {
    /// A fresh acceptor: nothing promised (`Ballot::zero()`), nothing
    /// accepted, no faulty entry, floor at `first_slot`.
    pub fn empty(first_slot: Slot) -> Self;

    /// Phase 1, whole: promise (or refuse) and page the suffix.
    /// `Ok(page)` on `PrepareOutcome::Promised`; `Err(Nack)` on `Refused`
    /// and on `BelowFloor` — both are answered with a `Nack` on the wire.
    /// `prepare()` / `promise_page()` stay public; this is them in one call.
    pub fn handle_prepare<W: From<AcceptorWrite<V>>>(
        &mut self, ballot: Ballot, from_slot: Slot, writes: &mut Vec<W>,
    ) -> Result<PromisePage<V>, Nack>;

    /// Phase 2, whole: admit, raise the promise when the ballot is above
    /// it, record the value — emitting `SetPromise` (if raised) **before**
    /// `AppendAccepted`. `Admitted` now means "accepted and recorded";
    /// `Refused` and `BelowFloor` touch nothing. `admit()`, `set_promise()`
    /// and `record_accepted()` stay public (learners, repairs and the
    /// handoff still need them apart).
    pub fn accept<W: From<AcceptorWrite<V>>>(
        &mut self, ballot: Ballot, slot: Slot, value: V, writes: &mut Vec<W>,
    ) -> AcceptOutcome;

    /// `accept()` with the reply built: `Accepted::of(ballot, slot, &value)`
    /// on `Admitted`, `Nack { ballot, slot }` on `Refused`, silence on
    /// `BelowFloor`.
    pub fn handle_accept<W: From<AcceptorWrite<V>>>(
        &mut self, ballot: Ballot, slot: Slot, value: V, writes: &mut Vec<W>,
    ) -> AcceptReply
    where V: Fingerprint;
}

impl Acceptor<Command> {
    /// The acceptor a boot scan reads back through the node's own `Storage`
    /// port — today's `node/boot.rs::read_back_log`, moved here with its
    /// asserts ("the durable promise dominates every accepted record", the
    /// tri-state partition). `ColocatedNode::new` calls this and derives
    /// the replica and the allocator from `records()`/`faulty()`; a lesson's
    /// `Disk` implements `Storage` and reboots through the same call.
    pub fn from_storage<S: Storage>(storage: &S) -> Self;
}
```

`from_storage` is on `Acceptor<Command>` only because `Storage` is the node's
`Command`-typed port; the decree's `Acceptor<Vec<MatchmakerId>>` keeps
reconstructing from its `DecreeRecord` through `Acceptor::new`, which stays
the generic constructor. No second recovery port is invented.

**How `ColocatedNode` calls them.**

- `node/acceptor.rs::on_prepare`: `match self.acceptor.handle_prepare(ballot,
  from_slot, &mut self.pending_writes)` — `Ok(page)` builds
  `Message::Promise { from: me, page }` after the existing role transition
  and the "promise reply ships with its durable raise" assert (which reads
  `page.ballot`); `Err(nack)` asserts "a nacked prepare never moves the
  promise" (`promised() == promise_at_entry`, the accept path's negative
  space, now shared by both refusals) and "queues no durable write", then
  queues `Message::Nack { from: me, nack }`. The role keeps distinguishing
  `Refused` from `BelowFloor` (`prepare()` is unchanged and its tests stay);
  the node never treated them differently.
- `node/acceptor.rs::on_accept`: `match self.acceptor.handle_accept(ballot,
  slot, command.clone(), &mut self.pending_writes)` — `BelowFloor` ignored;
  `Nack(nack)` keeps both negative-space asserts and queues the nack;
  `Accepted(accepted)` does the role transition, `learn_config`, and then
  the in-prefix repair case: today that branch calls `mark_chosen` *instead
  of* `record_accepted`; since `accept()` has already recorded, the tail of
  `mark_chosen` (replica learn, probe resolution, the two chosen/accepted
  coupling asserts, the prefix walk) is split into a private
  `learn_recorded(slot, &command)` that both `mark_chosen` and this branch
  call. No duplicate `AppendAccepted` is emitted and the write order in the
  batch is unchanged. The reply is `Message::Accepted { from: me, accepted }`.
- `node/boot.rs::new`: `let acceptor = Acceptor::from_storage(storage);`
  replaces `read_back_log` + `Acceptor::new`; the completeness scan and the
  `next_slot` derivation read `acceptor.records()` / `acceptor.faulty()`.
- `matchmaker/generation.rs` (the decree's acceptor side) switches
  `admit → set_promise → record_accepted` to `accept()`; the reply shapes
  there are the matchmaker's own and stay.

**How `single_decree.rs` calls them.**

```rust
struct AcceptorNode { id: NodeId, role: Acceptor<Command>, disk: Disk }

impl AcceptorNode {
    fn new(id: NodeId) -> Self { .. role: Acceptor::empty(DECREE) .. }
    /// Handle a `Prepare`: the reply is computed and its writes synced here;
    /// delivering it is a separate step the scenario may drop.
    fn on_prepare(&mut self, ballot: Ballot) -> Result<PromisePage<Command>, Nack> {
        let reply = self.role.handle_prepare(ballot, DECREE, &mut self.disk.writes);
        self.disk.sync();
        reply
    }
    fn on_accept(&mut self, ballot: Ballot, value: Command) -> AcceptReply {
        let reply = self.role.handle_accept(ballot, DECREE, value, &mut self.disk.writes);
        self.disk.sync();
        reply
    }
}
```

### `Proposer` operations

```rust
impl<Id: Copy + Ord> Campaign<Id> {
    /// A campaign over one fixed membership: `prior` is `[config]`, the one
    /// configuration an earlier ballot could have chosen under. A
    /// convenience for the fixed-membership deployment — on a matchmaker
    /// deployment `prior` is `H_b`, the matchmakers' answer, and is never
    /// derived from `config`.
    pub fn fixed(ballot: Ballot, config: AcceptorConfig<Id>, me: Option<Id>, from_slot: Slot) -> Self;
}

impl<Id: Copy + Ord, V: Clone + PartialEq> Proposer<Id, V> {
    /// Open Phase 1 for a proposer that holds **no acceptor log of its own**
    /// — the decree's reconfigurer, a lesson's stand-alone proposer: seeds
    /// the tally with nothing. A colocated candidate hands its records to
    /// `open_phase1`. Panics if `campaign.me` is `Some`: a candidate with
    /// an acceptor identity has a log to report.
    pub fn open_phase1_detached(&mut self, campaign: Campaign<Id>) -> Vec<Id>;

    /// Fold one `Promise` reply (`fold_promise` over the page's own fields).
    pub fn receive_promise(&mut self, from: Id, page: PromisePage<V>) -> PromiseFold;
    /// The probe's twin (`fold_probe_promise`).
    pub fn receive_probe_promise(&mut self, from: Id, page: &PromisePage<V>) -> PromiseFold;
}

impl<Id: Copy + Ord, V: Clone + Fingerprint> Proposer<Id, V> {
    /// Fold one `Accepted` reply (`fold_accepted`). Whether it counted.
    pub fn receive_accepted(&mut self, from: Id, accepted: &Accepted) -> bool;
    /// The same behind the column-addressee guard (`fold_accepted_in`).
    pub fn receive_accepted_in(&mut self, config: &AcceptorConfig<Id>, from: Id, accepted: &Accepted) -> bool;
    /// Whether a `Nack` supersedes work in flight (`supersedes`): the open
    /// campaign at its ballot, or the open round at its slot and ballot.
    pub fn receive_nack(&self, nack: &Nack) -> bool;
}

// `Rounds` gets `receive_accepted` / `receive_accepted_in` too (the proxy
// leader folds through `Rounds`, not `Proposer`).
```

The six-argument `fold_promise` / `fold_probe_promise` and the four-argument
`fold_accepted` / `fold_accepted_in` stay public and become the one-line
bodies the new methods delegate to, so following `receive_promise` into the
core lands on the fold the node runs.

**How `ColocatedNode` calls them.**

- `node.rs::step`: `Message::Promise { from, page } => self.on_promise(from,
  page)`; `Message::Accepted { from, accepted } => self.on_accepted(from,
  &accepted)`; `Message::Nack { from, nack } => self.on_nack(from, &nack)`.
- `node/election.rs::on_promise(from, page)`: leader → `on_probe_promise`
  → `self.proposer.receive_probe_promise(from, &page)`; candidate →
  `self.proposer.receive_promise(from, page)`; the `Continue(next)` arm's
  re-request is unchanged.
- `node/phase2.rs::on_accepted(from, accepted)`:
  `self.proposer.receive_accepted_in(&self.acceptors, from, accepted)`.
- `node/election.rs::on_nack(from, nack)`: `if
  self.proposer.receive_nack(nack) { self.become_follower(None) }`.
- `proxy_leader.rs`: `on_accepted` folds through
  `self.rounds.receive_accepted_in(..)`; the relayed nack is
  `Message::Nack { from, nack }`.
- `matchmaker/decree.rs::new`: `proposer.open_phase1_detached(Campaign::fixed(ballot,
  acceptors.clone(), None, DECREE_SLOT))` — the exact shape the lesson uses;
  `on_promise` builds a one-slot `PromisePage` from the matchmaker's `vote`
  and calls `receive_promise`.

**How `single_decree.rs` calls them.**

```rust
let config = AcceptorConfig::new(vec![A, B, C], QuorumSystem::Majority);
let mut proposer: Proposer<NodeId, Command> = Proposer::new();
let targets = proposer.open_phase1_detached(Campaign::fixed(ballot, config.clone(), None, DECREE));
// deliver Prepare to each target; for each reply that comes back:
match reply {
    Ok(page) => { proposer.receive_promise(from, page); }
    Err(nack) => { if proposer.receive_nack(&nack) { /* give up */ } }
}
if proposer.phase1_won(ballot) { let outcome = proposer.close_phase1(|_| false); .. }
// Phase 2:
proposer.open_round(DECREE, ballot, candidate.clone(), None, None);
match acceptor.on_accept(ballot, candidate.clone()) {
    AcceptReply::Accepted(accepted) => { proposer.receive_accepted(id, &accepted); }
    AcceptReply::Nack(nack) => { .. }
    AcceptReply::BelowFloor => unreachable!("a one-slot log has no floor above slot 0"),
}
if let Some((at, decided)) = proposer.decided(DECREE, &config) { .. }
```

`open_round(slot, ballot, value, own_vote, column)` is left as it is: the
lesson passes `None, None` once, with one sentence ("this proposer is not an
acceptor and a majority names no column"); a `Round::fixed`-style wrapper
would hide the two facts the multi-Paxos and grid lessons then change.

### `ProposalConstraint`

```rust
/// What P2c lets this ballot propose at one slot, read off a closed Phase 1.
pub enum ProposalConstraint<V> {
    /// Some promise reported `value` accepted at `accepted_at` — the highest
    /// ballot any promise reported for the slot. P2c binds this ballot to
    /// re-propose exactly that value.
    MustRepropose { accepted_at: Ballot, value: V },
    /// No promise reported the slot: quorum intersection proves nothing was
    /// chosen there below this ballot, so any value may be proposed.
    Free,
    /// A faulty report the tally could not clear (CTRL Case 3): neither.
    /// The repair probe decides it later; proposing here is forbidden.
    Blocked,
}

impl<Id, V: Clone> Phase1Outcome<Id, V> {
    /// The constraint at `slot` (`recovered` and `blocked`, asked together).
    pub fn constraint(&self, slot: Slot) -> ProposalConstraint<V>;
}
```

**How `ColocatedNode` calls it.** The node's per-slot walk is the recovery
pump (`RecoveryStep::{Recovered, Fill}` are `MustRepropose`/`Free` handed out
by cursor, with `Blocked` slots skipped by `recovery_blocked`) and stays as it
is. The per-slot *question* is asked in two places, and both switch:
`matchmaker/decree.rs::on_promise` selects the value with `match
outcome.constraint(DECREE_SLOT) { MustRepropose { value, .. } => value, Free
=> self.proposal.clone(), Blocked => unreachable!("a decree reports no faulty
entry") }` (today: `recovered.get(..).map_or_else(..)`, silently "free" on a
missing key); `node/election.rs::try_become_leader`'s prefix heal filters
with `matches!(outcome.constraint(slot), ProposalConstraint::MustRepropose {
.. })` instead of `!outcome.blocked.contains(slot)`. `paros-play`'s
recovery-plan clone (`world/drain.rs`) reads the same way.

**How `single_decree.rs` calls it.**

```rust
let candidate = match outcome.constraint(DECREE) {
    ProposalConstraint::MustRepropose { accepted_at, value } => { /* print P2c */ value }
    ProposalConstraint::Free => my_value.clone(),
    ProposalConstraint::Blocked => unreachable!("no acceptor here has a damaged record"),
};
```

### Small additions

- `impl Display for Ballot` → `"{round}.{node}"` (`7.8`: round 7, node 8),
  the notation every lesson already prints. `impl Display for Command` →
  a `User` entry's bytes as a quoted lossy-UTF-8 string (`"alpha"`), a
  control command as `Noop`, `Truncate(up to 3)`, `Snap(at 4)`; documented
  as a human trace format, never a wire one.
- `Message::kind(&self) -> &'static str`: the variant's name
  (`"Prepare"`, `"Promise"`, …), replacing the two identical `kind` helpers
  in `quorum_read.rs` and `proxy_leader.rs`.
- `ColocatedNode::has_ready(&self) -> bool`: true when any bucket a
  `Ready` exposes is non-empty (writes, messages, committed, snapshot
  offers, read states, recovery batch, match requests, gc requests). The
  minimal drivers of Phase 4 loop on it.

### Tests (Phase 1)

In-module `#[cfg(test)]` beside each operation, in the repository's style:
`accept` emits `SetPromise` before `AppendAccepted` when it raises, only the
record when the promise already covers the ballot, and nothing on `Refused`
or `BelowFloor` (`AcceptOutcome` pinned per case); `handle_prepare` maps
`Promised` to a page carrying the ballot and cursor and both refusals to a
`Nack` with no write; `handle_accept` maps the three arms; `from_storage`
reads back exactly what `read_back_log` did (its two asserts each get a
`#[should_panic]`); `Campaign::fixed` derives `prior == [config]`;
`open_phase1_detached` refuses `me: Some`; `receive_*` agree with `fold_*`;
`constraint` yields all three arms from one outcome with a reported, an
unreported and a blocked slot; `Display` and `kind` are spot-checked;
`has_ready` flips for each bucket. The existing suites (`cargo test
--workspace`, the node tests, the handover and proxy model checkers) run
unchanged except for the `Message` shape edits below.

## The one deliberate break: the shape of three `Message` variants

```rust
Message::Promise  { from: NodeId, page: PromisePage<Command> }   // was: from, ballot, from_slot, accepted, faulty, next_from_slot
Message::Accepted { from: NodeId, accepted: Accepted }           // was: from, ballot, slot, vhash
Message::Nack     { from: NodeId, nack: Nack }                   // was: from, ballot, slot
```

`from` stays outside the payload because an `Acceptor` has no identity (it
cannot name itself in a reply); the payload is what the role produces, the
sender is what the wiring adds. `Prepare`, `Accept` and `Commit` keep their
fields: they carry deployment data (`reply_to`, `leader`, `config`) that
belongs to the wiring, and the lessons send them through one-line
constructors in the shared harness rather than spelling `config: None` out.

Sites this touches, all in-workspace: `paros-core` (`node.rs::step`, the
three handlers, `proxy_leader.rs`, ~25 pattern matches in `node/tests/*`
and `proxy_leader.rs`'s tests), `paros`'s gRPC codec (`grpc.rs` encodes and
decodes these three variants field by field; the `.proto` is unchanged),
and `paros-play` (`narration.rs`, `view.rs`, `world/drain.rs`,
`world/prompts.rs`, `world/decree/mod.rs`, `world/verbs.rs` — eight
matches, and the two P2c-on-a-clone sites become `receive_promise` calls).
The sim harness and the driver never destructure these variants.

## Decisions carried into Phases 2–4

- **Phase 2, `single_decree.rs`.** The scenario harness models the disk as
  `Disk { writes: Vec<AcceptorWrite<Command>>, synced: usize }` with the
  `matchmaker.rs` assert ("no reply leaves ahead of an unsynced write")
  checked at the moment a reply is put on the wire. Reachability becomes a
  `Delivery` struct with four named fields — `prepare_to`, `promise_from`,
  `accept_to`, `accepted_from` — so a request and its reply are dropped
  independently. `Attempt.chosen` becomes `learned`; the harness's oracle
  `chosen_on_disk(&acceptors, &config)` groups the durable records by
  `(ballot, value)` and asks `config.has_phase2_quorum(&voters)` (never a
  count) — it is the audit's question, asked through the membership
  boundary, and the proposer has no handle on it. Scenario 4 is the
  lost-replies case (`accept_to: [A, B]`, `accepted_from: []`). The
  API-contract assertions (`PromiseFold::Answered`, `counted`, `targets ==
  [A, B, C]`) move to `crates/paros-core/tests/roles.rs`.
- **Phase 3, the shared harness** is a `publish = false` crate,
  `crates/paros-lessons`, taken by `paros-core` as a dev-dependency (see
  finding 5). Its `Wire` carries `paros_core::Message` for both tiers —
  that is what the typed payloads buy — with `drop`, `partition` and
  `deliver_until_quiet`; its `Node` (bottom tier) handles a `Prepare` /
  `Accept` by delegating to `handle_prepare` / `handle_accept` and a
  `Commit` by the learner path the roles expose, and never selects a value
  or asks a quorum question. `Disk` implements `Storage`, so a lesson's
  reboot is `Acceptor::from_storage(&disk)` — the node's own boot path.
  `learn_chosen`'s logic (adopt the ballot, record, learn, walk) is today
  `ColocatedNode::mark_chosen`, private; Phase 3 exposes the role half of it
  (a `Replica`-plus-`Acceptor` learn is two role calls the harness may make
  in order, and the plan is to keep it that way rather than add a third
  role that couples them — to be confirmed when the harness is written).
- **Phase 4, traces.** The expected trace is appended to each module doc as
  a fenced `text` block. For the snapshot test the recommendation is a
  single `tests/lesson_traces.rs` that runs each example and diffs its
  stdout against that block — the doc stays the single source of truth and
  no dev-dependency is needed. If a `trycmd`/`insta` snapshot file is
  preferred, the trace would live twice (doc and snapshot) with a second
  test keeping them equal; say which you want before Phase 4.

## Open questions for review

1. `Proposer::open_phase1_detached` — the name for "Phase 1 for a proposer
   that is not an acceptor". Alternatives considered: `open_phase1_standalone`,
   `Proposer::campaign`.
2. `AcceptReply::BelowFloor` as an explicit third arm (versus
   `Option<Result<Accepted, Nack>>`): chosen because silence is a protocol
   decision worth a name, and the lesson's `unreachable!` states the one
   fact that makes it unreachable there.
3. The `Message` variant reshaping above, given "add without breaking
   existing callers": every caller is in this workspace and the change is
   what makes the two tiers share one vocabulary; the alternative (keep the
   flat fields, add `Message::promise(from, page)` constructors and
   `as_promise()` accessors) leaves the top tier speaking the old one.
4. The trace-snapshot mechanism (Phase 4 note above).
