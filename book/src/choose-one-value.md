# How Paxos chooses one value

Single-decree Paxos agrees on one value, and never disagrees, over a network that
drops, delays and reorders, with several proposers competing. It does it with two
round trips over a majority: a proposer claims a **ballot** and learns what the
acceptors already accepted (Phase 1), then asks them to accept a value at that
ballot (Phase 2). A value accepted by a majority is **chosen**, and a chosen value
never changes.

> **Play it.** Act I runs this by hand — three acceptors, one slot, no clock, no disk.
>
> - [`act1/choose-a-value`](play/#act1/choose-a-value) — deliver the Prepare,
>   Promise, Accept and Accepted one message at a time, with one acceptor kept
>   silent, until a value is chosen by two of three.
> - [`act1/be-the-acceptor`](play/#act1/be-the-acceptor) — you answer every Prepare
>   and every Accept; the acceptor rule below is the whole level.
> - [`act1/the-duel`](play/#act1/the-duel) — two proposers preempting each other:
>   get a value chosen anyway, and count the rounds it took.

<!-- toc -->

## Ballots

A proposer never just announces a value. It first claims a **ballot**: a number that
gives it the right to propose. Ballots are totally ordered as `(round, node)`, so a
higher round always wins and ties break by node id — two proposers can therefore
never hold the same ballot.

## Two phases

Each phase is one round trip needing a **majority** (2 of 3):

1. **Phase 1, Prepare then Promise.** The proposer asks the acceptors to promise not
   to accept anything older than its ballot. An acceptor that promises also reports
   any value it has already accepted. Once a majority promise, the proposer owns the
   ballot.
2. **Phase 2, Accept then Accepted.** The proposer asks the acceptors to accept a
   value at its ballot. Once a majority accept, the value is **chosen**, and every
   node then learns it (Commit).

Two of three means the proposer makes progress while one acceptor is slow, crashed
or unreachable. Why a *majority* specifically: any two majorities share an acceptor,
and that overlap is what makes two different values impossible.
[Why one value is safe](safety.md) turns that into the argument.

## The value-selection rule

A proposer does not always get to propose its own value. If any Promise reports an
already-accepted value, the proposer must **adopt the highest-ballot value it saw**
— a later proposer, forced to re-propose the same value, can never change the
choice. Precisely stated this is Lamport's `P2c`, the whole reason Phase 1 exists;
[Why one value is safe](safety.md) derives it and
[`act1/adopt-the-value`](play/#act1/adopt-the-value) makes you obey it.

The mechanism has a name from the literature: **piggybacking** — attaching extra
information to a message already being sent, so it travels at no extra cost. A
`Promise` is never a bare "yes": it carries every value the acceptor accepted with
the ballot it was accepted at. In paros those ride in `Message::Promise`
(`crates/paros-core/src/message.rs`), filled by `Acceptor::promise_page`
(`acceptor.rs`). It is also how a node that *missed* a decision catches up: it
proposes, the chosen value comes back piggybacked, and it is forced to adopt it —
learning the consensus in the act of trying to overwrite it. That is narrower than
it looks; see [Recovery, not catch-up](safety.md#recovery-not-catch-up).

## What each acceptor remembers

An acceptor is tiny. It keeps two facts in durable storage — the highest ballot it
**promised** and the `(ballot, value)` records it **accepted** — and **one rule**
governs every reply it sends: *refuse anything below the promise you hold*, and
answer anything at or above it, persisting the write before the reply leaves.
Anything below gets a `Nack`. An equal ballot needs no special case: a ballot is
minted by exactly one proposer, so "equal" always means that same proposer asking
again — which is precisely what a proposer that won Phase 1 at `b` does when it
sends its `Accept` at `b`.

What differs between the two questions is not the comparison but what the answer
is *for*. A **Promise reports and fences**: it hands the proposer everything this
acceptor has accepted at or above the slot in question — the report P2c is built
on — and closes the door on every lower ballot for good. A **vote records**: it
writes down a `(ballot, value)` that some later ballot's Phase 1 will find and be
obliged to re-propose. That split is what
[`act1/be-the-acceptor`](play/#act1/be-the-acceptor) is built around; the "before
the reply leaves" is where [persist before send](restart-safety.md) bites.

In paros the role is `Acceptor` (`crates/paros-core/src/acceptor.rs`):
`Acceptor::promised` is the promise, durable as `HardState.max_promised_ballot`
(`state.rs`); `Acceptor::records` is the log. `Acceptor::prepare` answers the report-
and-fence question and `Acceptor::admit` the record-a-vote one — both refusing exactly
`ballot < promised` — each emitting the `AcceptorWrite` the driver must flush before the
reply leaves. Everything else in the protocol exists only to feed those two answers a
safe value.

## The one thing it will never do

The simulation will never show two acceptors choosing different values: the audit
asserts **"at most one value is ever chosen for a slot"** on every transition of
every seed, in CI (`crates/paros-sim/src/audit/`). What it *will* show is proposers
dueling without converging — every node has promised a different high ballot, so no
ballot wins a promise quorum and nothing is chosen. That is a livelock: annoying,
never unsafe, and it has a level of its own. Randomized election timeouts cure it
once we elect a [stable leader](stable-leader.md); they were never needed for safety.
