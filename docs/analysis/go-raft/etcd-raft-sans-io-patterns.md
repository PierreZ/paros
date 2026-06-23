# Sans-IO architecture patterns from `go.etcd.io/raft/v3`

A reference for building a **sans-IO Multi-Paxos** library, extracted from etcd's Raft
implementation. etcd/raft is the canonical example of a consensus *state machine* that
does **zero I/O** of its own: it never touches a disk, a socket, or a wall clock. The
application drives it and performs all side effects. That separation — not Raft's
election or log-matching rules — is what transfers to Paxos.

This document is **algorithm-agnostic**. Every section has three parts:

- **(a) What raft does** — with verbatim Go and a `file:line` reference (line numbers are
  from the checkout this was written against; symbol names are stable, line numbers drift).
- **(b) The principle** — the language-neutral pattern worth copying.
- **(c) Maps to Multi-Paxos** — how the pattern lands when the replicated object is a log
  of slots, each holding a chosen value.

Scope note: we target a **synchronous v1**. The async-storage machinery
(`MsgStorageAppend`/`MsgStorageApply`) and the protobuf layer are explicitly called out in
§6 as things *not* to copy yet.

---

## Table of contents

1. [The input/output boundary — one entry point, one output struct](#1-the-inputoutput-boundary)
2. [The Ready/Advance handshake — the ordering contract](#2-the-readyadvance-handshake)
3. [The Tick() time abstraction — logical time as caller-driven counts](#3-the-tick-time-abstraction)
4. [The RawNode vs Node split — pure core vs optional driver](#4-the-rawnode-vs-node-split)
5. [The Storage interface — read-only state recovery](#5-the-storage-interface)
6. [What to consciously NOT copy for v1](#6-what-to-consciously-not-copy-for-v1)
7. [File map / suggested reading order](#7-file-map--suggested-reading-order)

---

## 1. The input/output boundary

### (a) What raft does

**Every input is a message; one function consumes them all.** `raft.Step(m)` is the sole
entry point to the state machine. It is `O(state, event) → mutate(state)` and nothing more
— it never performs I/O.

```go
// raft.go:1085
// Step advances the state machine using the given message.
//
// Callers must treat m as immutable after passing it to Step. Mutating it
// concurrently can lead to unexpected behavior.
func (r *raft) Step(m *pb.Message) error {
	if m == nil {
		return errors.New("nil message")
	}
	// ... handle term, route by m.Type to stepFollower/stepCandidate/stepLeader ...
}
```

All the "verb" methods are thin wrappers that synthesize a message and call `Step`. From
`rawnode.go`:

```go
// rawnode.go:83
func (rn *RawNode) Campaign() error {
	return rn.raft.Step(&pb.Message{Type: pb.MsgHup.Enum()})
}

// rawnode.go:90
func (rn *RawNode) Propose(data []byte) error {
	return rn.raft.Step(&pb.Message{
		Type:    pb.MsgProp.Enum(),
		From:    new(rn.raft.id),
		Entries: []*pb.Entry{{Data: data}},
	})
}
```

Even **timeouts** become messages: a tick that crosses a threshold calls `Step` with an
internal message such as `MsgHup` or `MsgBeat` (see §3). So proposals, peer RPCs, config
changes, and timers *all* funnel through one router.

**The core never does I/O.** Outbound messages are not sent — they are *appended to a
slice*. `send()` is the only egress, and it just buffers:

```go
// raft.go:594
} else {
	if m.GetTo() == r.id {
		r.logger.Panicf("message should not be self-addressed when sending %s", m.GetType())
	}
	r.msgs = append(r.msgs, m)
	traceSendMessage(r, m)
}
```

(The `MsgAppResp`/`MsgVoteResp` branch buffers to a *second* slice, `r.msgsAfterAppend` —
that distinction is the heart of §2.)

**Everything the caller must do comes back through one struct: `Ready`.** Not callbacks,
not a writer handle — one value the caller pulls and drains.

```go
// node.go:49
// Ready encapsulates the entries and messages that are ready to read,
// be saved to stable storage, committed or sent to other peers.
// All fields in Ready are read-only.
type Ready struct {
	// The current volatile state of a Node. SoftState will be nil if there is
	// no update. It is not required to consume or store SoftState.
	*SoftState

	// The current state of a Node to be saved to stable storage BEFORE
	// Messages are sent. HardState will be nil if there is no update.
	*pb.HardState

	// ReadStates can be used for node to serve linearizable read requests
	// locally when its applied index is greater than the index in ReadState.
	ReadStates []ReadState

	// Entries specifies entries to be saved to stable storage BEFORE
	// Messages are sent.
	Entries []*pb.Entry

	// Snapshot specifies the snapshot to be saved to stable storage.
	Snapshot *pb.Snapshot

	// CommittedEntries specifies entries to be committed to a
	// store/state-machine. These have previously been appended to stable
	// storage.
	CommittedEntries []*pb.Entry

	// Messages specifies outbound messages.
	//
	// If async storage writes are not enabled, these messages must be sent
	// AFTER Entries are appended to stable storage.
	//
	// If it contains a MsgSnap message, the application MUST report back to raft
	// when the snapshot has been received or has failed by calling ReportSnapshot.
	Messages []*pb.Message

	// MustSync indicates whether the HardState and Entries must be durably
	// written to disk or if a non-durable write is permissible.
	MustSync bool
}
```

A `Ready` is built read-only and cheaply. The build only allocates fields that *changed*
since the last batch:

```go
// rawnode.go:139 — readyWithoutAccept (read-only: no obligation to handle the result)
func (rn *RawNode) readyWithoutAccept() Ready {
	r := rn.raft
	rd := Ready{
		Entries:          r.raftLog.nextUnstableEnts(),
		CommittedEntries: r.raftLog.nextCommittedEnts(rn.applyUnstableEntries()),
		Messages:         r.msgs,
	}
	if softSt := r.softState(); !softSt.equal(rn.prevSoftSt) { /* set rd.SoftState */ }
	if hardSt := r.hardState(); !isHardStateEqual(hardSt, rn.prevHardSt) {
		rd.HardState = hardSt
	}
	if r.raftLog.hasNextUnstableSnapshot() { rd.Snapshot = r.raftLog.nextUnstableSnapshot() }
	if len(r.readStates) != 0 { rd.ReadStates = r.readStates }
	rd.MustSync = MustSync(r.hardState(), rn.prevHardSt, len(rd.Entries))
	// ... (sync path) append msgsAfterAppend destined for peers to rd.Messages ...
	return rd
}
```

A cheap predicate lets a driver poll without allocating a `Ready`:

```go
// rawnode.go:448
func (rn *RawNode) HasReady() bool {
	// returns true iff softstate/hardstate changed, or there is an unstable
	// snapshot, queued messages, unstable/committed entries, or read states.
}
```

### (b) The principle

A consensus core should expose exactly **one input function** (`step(event)`) and **one
output value** (`Ready`). Model *all* stimuli — client proposals, peer messages, and timer
expiries — as events fed to the same function. Never let the core open a file, send a
packet, or call a callback. Side effects are *described* in the output struct and
*performed* by the caller. This is what makes the core trivially unit-testable and
trivially embeddable in a deterministic simulator.

A useful corollary: the output struct should carry **only deltas** (state that changed,
entries not yet durable, messages not yet sent), and building it must be a pure read so a
driver can ask "is there work?" without committing to doing it.

### (c) Maps to Multi-Paxos

- `step(msg)` consumes `Prepare`/`Promise`/`Accept`/`Accepted`/`Commit` and client proposals
  alike. One router, dispatched by message type and role (proposer/acceptor/learner).
- Your `Ready` carries: durable state to persist (highest promised ballot; per-slot accepted
  `(ballot, value)`; commit index), newly chosen entries to apply, and outbound messages.
- `send()`'s "append to a slice" is your model too: a proposer deciding to broadcast `Accept`
  pushes messages onto a buffer that surfaces in the next `Ready`.

---

## 2. The Ready/Advance handshake

This is the subtle, bug-prone, **most valuable** part. It is a contract about *what must be
durable before what becomes observable to other nodes*, plus a one-in-flight gate.

### (a) What raft does

**The contract, verbatim** (this is the spec your library must also honor):

```text
// doc.go:69
Now that you are holding onto a Node you have a few responsibilities:

First, you must read from the Node.Ready() channel and process the updates
it contains. These steps may be performed in parallel, except as noted in step 2.

1. Write HardState, Entries, and Snapshot to persistent storage if they are
not empty. Note that when writing an Entry with Index i, any
previously-persisted entries with Index >= i must be discarded.

2. Send all Messages to the nodes named in the To field. It is important that
no messages be sent until the latest HardState has been persisted to disk,
and all Entries written by any previous Ready batch (Messages may be sent while
entries from the same batch are being persisted). ... If any Message has type
MsgSnap, call Node.ReportSnapshot() after it has been sent (these messages may be large).

3. Apply Snapshot (if any) and CommittedEntries to the state machine.
If any committed Entry has Type EntryConfChange, call Node.ApplyConfChange()
to apply it to the node. ...

4. Call Node.Advance() to signal readiness for the next batch of updates.
This may be done at any time after step 1, although all updates must be processed
in the order they were returned by Ready.
```

The canonical drive loop (`doc.go:121`) is just: on tick → `Tick()`; on a `Ready` →
`saveToStorage(state, entries, snapshot)`, `send(messages)`, apply `committedEntries`, then
`Advance()`.

**Safety rule 1 — durable-before-observable.** A vote or an append-ack must not leave the
node until the state it is predicated on is on stable storage. Raft enforces this
*structurally* by splitting egress into two slices: `r.msgs` (send now) and
`r.msgsAfterAppend` (send only after this batch's `HardState`/`Entries` are durable). The
reasoning, verbatim:

```go
// raft.go:546
if m.GetType() == pb.MsgAppResp || m.GetType() == pb.MsgVoteResp || m.GetType() == pb.MsgPreVoteResp {
	// ... response messages that relate to "voting" on either leader election
	// or log appends require durability before they can be sent. It would be
	// incorrect to publish a vote in an election before that vote has been
	// synced to stable storage locally. Similarly, it would be incorrect to
	// acknowledge a log append to the leader before that entry has been
	// synced to stable storage locally.
	//
	// Per the Raft thesis, section 3.8 Persisted state and server restarts:
	// > ... each server persists its current term and vote; this is necessary
	// > to prevent the server from voting twice in the same term ... Each server
	// > also persists new log entries before they are counted towards the
	// > entries' commitment ...
	r.msgsAfterAppend = append(r.msgsAfterAppend, m)
}
```

So `Ready.Messages` is constructed to already respect ordering: in the sync path,
`msgsAfterAppend` (for peers) is only appended into the `Ready` once, and the contract's
step 2 says don't send *any* of them before `HardState` + prior `Entries` are durable.

**Safety rule 2 — snapshot xor entries.** A `Ready` that carries a snapshot carries *no*
committed entries; mixing them is ill-defined ("what should be applied first?"):

```go
// log.go:248 — hasNextCommittedEnts
if l.hasNextOrInProgressSnapshot() {
	// If we have a snapshot to apply, don't also return any committed
	// entries. Doing so raises questions about what should be applied first.
	return false
}
```

**Safety rule 3 — `MustSync` distinguishes fsync from a lazy write.** Only term/vote
changes and new entries force a durable sync:

```go
// rawnode.go:191
func MustSync(st, prevst *pb.HardState, entsnum int) bool {
	// Persistent state on all servers (updated on stable storage before
	// responding to RPCs): currentTerm, votedFor, log entries[].
	return entsnum != 0 || st.GetVote() != prevst.GetVote() || st.GetTerm() != prevst.GetTerm()
}
```

**The gate: one `Ready` in flight, released by `Advance()`.** For the pure core, `Ready()`
both builds *and* accepts the batch:

```go
// rawnode.go:131
func (rn *RawNode) Ready() Ready {
	rd := rn.readyWithoutAccept()
	rn.acceptReady(rd)
	return rd
}
```

`acceptReady` records "what changed" so it is not re-emitted, clears the egress slices, and
— crucially — stashes the **self-directed follow-ups** that must run only *after* the caller
reports the batch durable/applied. It panics if a second batch is accepted without an
intervening `Advance`:

```go
// rawnode.go:400
func (rn *RawNode) acceptReady(rd Ready) {
	if rd.SoftState != nil { rn.prevSoftSt = rd.SoftState }
	if !IsEmptyHardState(rd.HardState) { rn.prevHardSt = rd.HardState }
	if len(rd.ReadStates) != 0 { rn.raft.readStates = nil }
	if !rn.asyncStorageWrites {
		if len(rn.stepsOnAdvance) != 0 {
			rn.raft.logger.Panicf("two accepted Ready structs without call to Advance")
		}
		// self-addressed msgsAfterAppend + storage append/apply acks are queued
		// to be fed back through Step on Advance.
		for _, m := range rn.raft.msgsAfterAppend {
			if m.GetTo() == rn.raft.id { rn.stepsOnAdvance = append(rn.stepsOnAdvance, m) }
		}
		if needStorageAppendRespMsg(rn.raft, rd) { /* append append-resp */ }
		if needStorageApplyRespMsg(rd)          { /* append apply-resp  */ }
	}
	rn.raft.msgs = nil
	rn.raft.msgsAfterAppend = nil
	rn.raft.raftLog.acceptUnstable()                 // mark unstable entries "in progress"
	if len(rd.CommittedEntries) > 0 {
		index := rd.CommittedEntries[len(rd.CommittedEntries)-1].GetIndex()
		rn.raft.raftLog.acceptApplying(index, entsSize(rd.CommittedEntries), rn.applyUnstableEntries())
	}
}
```

`Advance()` is what *closes the loop*: it replays those queued self-messages back through
`Step`, which advances the durable/applied indices. Notice the actual ordering work is
encoded at `acceptReady` time, not recomputed from the `Ready`:

```go
// rawnode.go:477
func (rn *RawNode) Advance(_ Ready) {
	for i, m := range rn.stepsOnAdvance {
		_ = rn.raft.Step(m)
		rn.stepsOnAdvance[i] = nil
	}
	rn.stepsOnAdvance = rn.stepsOnAdvance[:0]
}
```

**The bookkeeping invariant** that makes "in flight" precise — three monotonic indices:

```go
// log.go:33
// committed is the highest log position known to be in stable storage on a quorum.
committed uint64
// applying is the highest log position the application has been instructed to apply.
// Incremented when accepting a Ready. Invariant: applied <= applying && applying <= committed
applying uint64
// applied is the highest log position the application has successfully applied.
// Incremented on Advance after committed entries in a Ready have been applied.
// Invariant: applied <= committed
applied uint64
```

So the cycle is: `HasReady()` → `Ready()` (builds + `acceptReady`, bumping `applying`) →
caller persists/sends/applies → `Advance()` (replays follow-ups, bumping `applied`) → next
`Ready()`. You **cannot** get the next batch until you `Advance` the current one.

### (b) The principle

Define your output contract as an explicit, ordered list of obligations, and document
which steps may run in parallel and which impose a happens-before. The two non-negotiable
edges:

1. **Durable-before-send:** any message that *promises* something to a peer (a vote, an
   acceptance/ack) must not be sent until the state it promises is on stable storage.
   Encode this by routing such messages through a separate "after durable" channel rather
   than relying on the caller to remember.
2. **Persist-before-apply / snapshot-xor-entries:** entries are applied to the state machine
   only after they are durable; a snapshot batch and an entries batch never mix.

Gate the protocol to **one batch in flight**: building the output marks work "in progress";
an explicit `advance(batch)` acknowledges completion and unlocks the next batch. Make the
"two batches without advance" case a hard error — it catches the most common integration
bug. Keep two cursors (`applying`, `applied`) so the gate is a precise index comparison, not
a boolean.

### (c) Maps to Multi-Paxos

- **Durable-before-send is the Paxos safety core.** An acceptor must fsync its newly promised
  ballot before replying `Promise`, and fsync `(ballot, value)` before replying `Accepted`.
  Model these replies as your `msgsAfterAppend` equivalent: buffered at decision time, sent
  by the caller only after the persist step of the same batch completes.
- `HardState` ↔ `{max_promised_ballot, per-slot accepted (ballot, value), commit_index}`.
  `MustSync` ↔ "did the promised ballot or any accepted entry change this batch?".
- `committed`/`applying`/`applied` ↔ chosen-up-to / handed-to-app / applied-by-app slot
  indices. A learner applies a slot only once it is chosen *and* durable.
- The snapshot-xor-entries rule is identical for log-compaction snapshots of the Paxos log.

---

## 3. The Tick() time abstraction

### (a) What raft does

The core has **no clock**. Time is an abstract "tick" the caller delivers:

```text
// doc.go:116
Finally, you need to call Node.Tick() at regular intervals (probably via a
time.Ticker). Raft has two important timeouts: heartbeat and the election
timeout. However, internally to the raft package time is represented by an
abstract "tick".
```

`Tick()` just nudges the state machine's logical clock:

```go
// rawnode.go:63
func (rn *RawNode) Tick() { rn.raft.tick() }
```

Timeouts are **integer counts of ticks**, configured by the caller, who alone decides what
a tick is worth in real time:

```go
// raft.go:130
// ElectionTick is the number of Node.Tick invocations that must pass between
// elections. ... We suggest ElectionTick = 10 * HeartbeatTick ...
ElectionTick int
// HeartbeatTick is the number of Node.Tick invocations that must pass between heartbeats.
HeartbeatTick int
```

Counters live on the state machine and are incremented per tick; crossing a threshold fires
an **internal message back through `Step`** (closing the loop with §1):

```go
// raft.go:403
// number of ticks since it reached last electionTimeout ... (follower/candidate/leader)
electionElapsed int
// number of ticks since it reached last heartbeatTimeout. only leader keeps this.
heartbeatElapsed int

// raft.go:849
func (r *raft) tickElection() {
	r.electionElapsed++
	if r.promotable() && r.pastElectionTimeout() {
		r.electionElapsed = 0
		_ = r.Step(&pb.Message{From: new(r.id), Type: pb.MsgHup.Enum()})
	}
}

// raft.go:861
func (r *raft) tickHeartbeat() {
	r.heartbeatElapsed++
	r.electionElapsed++
	// ... on electionElapsed >= electionTimeout: optional CheckQuorum, abort stalled transfer ...
	if r.state != StateLeader { return }
	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.heartbeatElapsed = 0
		_ = r.Step(&pb.Message{From: new(r.id), Type: pb.MsgBeat.Enum()})
	}
}
```

Which `tick*` runs is just a function pointer set on each state transition
(`r.tick = r.tickElection` / `r.tickHeartbeat`). Randomization (to break election ties) is
also expressed in ticks, not durations:

```go
// raft.go:2046
// pastElectionTimeout returns true if r.electionElapsed >= the randomized
// election timeout in [electiontimeout, 2 * electiontimeout - 1].
func (r *raft) pastElectionTimeout() bool { return r.electionElapsed >= r.randomizedElectionTimeout }
func (r *raft) resetRandomizedElectionTimeout() {
	r.randomizedElectionTimeout = r.electionTimeout + globalRand.Intn(r.electionTimeout)
}
```

### (b) The principle

The core must never read a wall clock. Expose a single `tick()` that advances a logical
clock by one unit; let the **caller** decide a tick's wall-time meaning and call `tick()`
on its own timer. Express every timeout as an integer tick count, not a `Duration`. When a
counter crosses its threshold, feed an internal event through the *same* `step()` entry
point — timers are just another event source. Keep the elapsed counters as plain integers
on the state struct, reset on the relevant transition.

This is exactly what makes a deterministic simulator possible: the simulator *is* the
clock. Pure logical time + caller-driven ticks means a test can advance time
instantaneously and reproducibly, with no sleeps and no flakiness.

### (c) Maps to Multi-Paxos

- A leader/proposer lease or "I haven't heard from the leader" timeout becomes an
  `electionElapsed`-style counter; crossing it fires an internal "start a new round / bump
  ballot" event through `step()`.
- Retransmission of un-acked `Accept`s, prepare-timeout backoff, and any failure-detector
  interval are all tick-count thresholds.
- Randomized backoff (to avoid dueling proposers — the Paxos analogue of dueling
  candidates) uses the same "random number of ticks in a range" trick.
- Your deterministic sim drives `tick()`; nothing else changes between production and test.

---

## 4. The RawNode vs Node split

### (a) What raft does

**`RawNode` is the pure, synchronous, single-threaded state machine.** No goroutines, no
channels, no I/O. Its fields are just the core plus a little bookkeeping for the
Ready/Advance handshake:

```go
// rawnode.go:31
// RawNode is a thread-unsafe Node.
type RawNode struct {
	raft               *raft
	asyncStorageWrites bool
	// Mutable fields.
	prevSoftSt     *SoftState
	prevHardSt     *pb.HardState
	stepsOnAdvance []*pb.Message
}
```

Its public surface is the entire protocol, callable synchronously: `Tick()`, `Campaign()`,
`Propose(data)`, `ProposeConfChange(cc)`, `Step(m)`, `HasReady()`, `Ready()`, `Advance(rd)`,
`ApplyConfChange(cc)`, `Status()`, `ReadIndex(rctx)`, `ReportUnreachable(id)`,
`ReportSnapshot(id, status)`, `TransferLeader(...)`, `ForgetLeader()`. **This is the
sans-IO object.** If you build only one thing, build this.

**`Node` is an *optional* concurrency driver layered on top.** It is one goroutine plus a
set of channels, owning a `RawNode`:

```go
// node.go:296
type node struct {
	propc      chan msgWithResult
	recvc      chan *pb.Message
	confc      chan *pb.ConfChangeV2
	confstatec chan *pb.ConfState
	readyc     chan Ready
	advancec   chan struct{}
	tickc      chan struct{}
	done       chan struct{}
	stop       chan struct{}
	status     chan chan Status
	rn *RawNode
}
```

The driver is just a `select` loop translating channel traffic into `RawNode` calls. The
key part is how it offers a `Ready` and then **blocks on `advancec`** before offering the
next one — the channel encoding of the §2 gate:

```go
// node.go:343
func (n *node) run() {
	var readyc chan Ready
	var advancec chan struct{}
	var rd Ready
	for {
		if advancec == nil && n.rn.HasReady() {
			// Build a Ready and arm readyc. Not guaranteed to actually be sent —
			// we may service another channel, loop, and rebuild it. Emitting
			// larger, less frequent Readys is fine and simplifies testing.
			rd = n.rn.readyWithoutAccept()
			readyc = n.readyc
		}
		select {
		case pm := <-propc:        m := pm.m; m.From = new(r.id); err := r.Step(m); /* reply */
		case m := <-n.recvc:       r.Step(m)                       // inbound peer message
		case cc := <-n.confc:      /* applyConfChange, maybe gate propc */
		case <-n.tickc:            n.rn.Tick()                     // a tick arrived
		case readyc <- rd:                                         // consumer took the Ready
			n.rn.acceptReady(rd)
			if !n.rn.asyncStorageWrites { advancec = n.advancec }  // ARM the gate
			readyc = nil
		case <-advancec:                                           // consumer called Advance
			n.rn.Advance(rd)
			rd = Ready{}
			advancec = nil                                         // RELEASE the gate
		case c := <-n.status:      c <- getStatus(r)
		case <-n.stop:             close(n.done); return
		}
	}
}
```

Note `advancec == nil` is the "no batch in flight" guard: a new `Ready` is only built and
armed while no previous one awaits `Advance`. (The `tickc` channel is buffered so ticks
aren't dropped while the loop is busy — `node.go:320`.)

### (b) The principle

Split the library in two layers:

- A **pure core** (`RawNode`-equivalent): synchronous, single-threaded, no I/O, no
  concurrency. Its methods *are* the protocol. This is what you test exhaustively and run in
  the simulator.
- An **optional driver**: a thin event loop (threads/async tasks/channels) that owns the
  core and shuttles real I/O in and out. It performs *no* protocol logic — it only moves
  bytes and calls core methods.

The driver is **swappable**: the channel loop is one implementation; a deterministic
simulator that calls the core's methods directly is another; a single-threaded async
runtime is a third. Because the core is pure, all three share identical, deterministic
behavior. Ship the core first; the driver is a later, separable concern.

### (c) Maps to Multi-Paxos

- Your `RawNode` equivalent (call it `PaxosNode` / `Replica`) exposes `tick()`, `step(msg)`,
  `propose(value)`, `has_ready()`, `ready()`, `advance(ready)` — synchronous, no I/O.
- In production, an async driver reads sockets → `step()`, drains `ready()` → persists +
  sends, and forwards `advance()`.
- In your deterministic simulation framework, the simulator *is* the driver: it injects
  messages via `step()`, controls time via `tick()`, and inspects/persists `ready()` — no
  second implementation, identical state machine. This is the whole reason to go sans-IO.

---

## 5. The Storage interface

### (a) What raft does

The core is a **read-only consumer** of durable state. The application owns all writes; the
library only asks storage to *read back* what was previously persisted:

```go
// storage.go:42
// Storage is an interface that may be implemented by the application to
// retrieve log entries from storage.
//
// If any Storage method returns an error, the raft instance will become
// inoperable and refuse to participate in elections; the application is
// responsible for cleanup and recovery in this case.
type Storage interface {
	// InitialState returns the saved HardState and ConfState information.
	InitialState() (*pb.HardState, *pb.ConfState, error)
	// Entries returns consecutive log entries in [lo, hi), capped by maxSize.
	// Returns ErrCompacted if lo was compacted, ErrUnavailable on a gap.
	Entries(lo, hi, maxSize uint64) ([]*pb.Entry, error)
	// Term returns the term of entry i, in [FirstIndex()-1, LastIndex()].
	Term(i uint64) (uint64, error)
	// LastIndex returns the index of the last entry in the log.
	LastIndex() (uint64, error)
	// FirstIndex returns the index of the first entry possibly available via
	// Entries (older entries folded into the latest Snapshot).
	FirstIndex() (uint64, error)
	// Snapshot returns the most recent snapshot. May return
	// ErrSnapshotTemporarilyUnavailable so raft retries later.
	Snapshot() (*pb.Snapshot, error)
}
```

Note: **every method is a read.** The error sentinels are part of the contract and tell the
core how to react — e.g. a requested index was compacted vs. genuinely missing
(`storage.go:26`):

```go
var ErrCompacted = errors.New("requested index is unavailable due to compaction")
var ErrSnapOutOfDate = errors.New("requested index is older than the existing snapshot")
var ErrUnavailable = errors.New("requested entry at index is unavailable")
var ErrSnapshotTemporarilyUnavailable = errors.New("snapshot is temporarily unavailable")
```

The reference `MemoryStorage` makes the read/write split explicit: the **reads** implement
the interface; the **writes** are *separate methods the application calls* (`SetHardState`,
`Append`, `ApplySnapshot`, `Compact`, `CreateSnapshot`) — they are not on the `Storage`
interface the core sees:

```go
// storage.go:104
type MemoryStorage struct {
	sync.Mutex                  // Append() runs on an app goroutine; reads on the raft goroutine.
	hardState *pb.HardState
	snapshot  *pb.Snapshot
	ents []*pb.Entry            // ents[i] has log position i + snapshot.Metadata.Index
}
// SetHardState (storage.go:136), Entries (read, :144), and the write helpers
// Append/ApplySnapshot/Compact/CreateSnapshot live on the concrete type, not the interface.
```

**Bootstrap/restart is "read it all back in."** Construction reads `InitialState()` for the
durable `HardState`+`ConfState`, and reads `FirstIndex()`/`LastIndex()` to position the
in-memory log at the last compaction point:

```go
// log.go:75
func newLogWithSize(storage Storage, logger Logger, maxApplyingEntsSize entryEncodingSize) *raftLog {
	firstIndex, _ := storage.FirstIndex()
	lastIndex, _ := storage.LastIndex()
	return &raftLog{
		storage:  storage,
		unstable: unstable{offset: lastIndex + 1, offsetInProgress: lastIndex + 1, ...},
		// committed/applying/applied start at the last compaction point.
		committed: firstIndex - 1, applying: firstIndex - 1, applied: firstIndex - 1,
		...
	}
}
```

`Config.Storage` itself documents the contract (`raft.go:142`): *"raft generates entries and
states to be stored ... raft reads the persisted entries and states out of Storage when it
needs ... when restarting."* The library generates; the app stores; the library reads back.

The durable types: **`HardState{Term, Vote, Commit}`** is the small must-fsync state;
**`Entry{Term, Index, Type, Data}`** is the log; **`Snapshot{Data, Metadata{Index, Term,
ConfState}}`** compacts a prefix. The split between "tiny state that must be synced before
responding to RPCs" (term/vote, per `MustSync` in §2) and "the log" is fundamental.

### (b) The principle

Define a **read-only storage trait** that the core depends on — the methods the core needs
to *recover and serve* prior state, nothing more. The application implements it and
**owns all writes**; the core never writes through it. Put write helpers on the concrete
implementation, called by the application as part of draining a `Ready` (§2 step 1).

Make storage errors part of the contract: distinguish "compacted away" from "not yet
available" from "temporarily unavailable, retry later", because the core branches on them.
Bootstrap and restart are the *same* code path: read the durable state back, position the
in-memory structures, continue. A from-scratch start is just an empty/sentinel storage.

Separate the small **must-sync state** (the few values that gate correctness) from the bulk
**log**, because they have different durability and write-frequency characteristics.

### (c) Maps to Multi-Paxos

- Your read-only trait: `initial_state()` → `(promised_ballot, commit_index, membership)`;
  `entry(slot)` / `entries(lo, hi)`; `first_index()`/`last_index()`; `snapshot()`. The core
  reads; your KV/log engine writes.
- Must-sync state ↔ `{max_promised_ballot, per-slot accepted (ballot, value), commit_index}`.
  The log ↔ chosen values per slot. Snapshot ↔ compacted prefix + membership at that slot.
- Restart = read promised ballot + accepted entries + commit index back in, reposition, and
  resume — the acceptor must never "forget" a promise or an acceptance across a crash, which
  is exactly what reading `InitialState` + the log on boot guarantees.
- The compaction error sentinels (`ErrCompacted` etc.) map directly to a Paxos log that
  truncates a chosen prefix behind a snapshot.

---

## 6. What to consciously NOT copy for v1

These are real and clever, but they will distract from a clean first cut. Skip them
deliberately.

- **Async storage writes (`MsgStorageAppend` / `MsgStorageApply`).** etcd raft can, instead
  of putting `HardState`/`Entries`/`CommittedEntries` *in* the `Ready`, emit them as special
  messages routed to dedicated local append/apply threads, with their acks taking the place
  of `Advance()` (see `rawnode.go:163` async branch, the `needStorage*Msg` helpers, and
  `doc.go`'s async section). It enables pipelining persistence with progress but roughly
  doubles the conceptual surface. **For v1, use the synchronous `Ready` + `Advance()` path
  only** — `Advance` exists precisely so you don't need this yet.

- **The protobuf `raftpb` layer.** Raft funnels *every* RPC through one giant
  protobuf-generated `Message` union (`Type` discriminator + a wide field set: `To`, `From`,
  `Term`, `LogTerm`, `Index`, `Entries`, `Commit`, `Snapshot`, `Reject`, `RejectHint`,
  `Context`, ...). That buys wire serialization and a single message type for free, at the
  cost of protobuf coupling and `Get*()` accessors everywhere. **Don't adopt protobuf for
  v1.** Use a plain tagged union (a Rust `enum`) for messages and let the *caller* own
  serialization — the core should manipulate in-memory message values and never marshal.
  The architecture is wire-agnostic precisely because messages only ever cross the boundary
  through `step(msg)` (deserialized by the caller) and `Ready.messages` (serialized by the
  caller).

- **Raft's election & log-matching internals.** `stepFollower`/`stepCandidate`/`stepLeader`,
  the `MsgApp`/`MsgVote` term rules, PreVote, CheckQuorum, the `tracker`/`quorum` progress
  machinery — these are Raft's *algorithm*, not its architecture. Your Paxos round/ballot
  logic replaces them wholesale. Study them for ideas, but don't port them.

- **Adjacent extras:** linearizable `ReadIndex`/`ReadState`, joint-consensus config changes
  (`ConfChangeV2`, the `confchange` package), and `TickQuiesced` are all deferred-feature
  territory for a v1 reference.

---

## 7. File map / suggested reading order

Read source alongside this doc in roughly this order. (Symbol names are stable; line numbers
are approximate.)

| # | Theme | File(s) | Key symbols / regions |
|---|-------|---------|------------------------|
| 1 | Big-picture usage & the contract | `doc.go` | usage loop and the 4-step Ready contract (`doc.go:69`, `:121`) |
| 2 | Output struct | `node.go` | `Ready` (`node.go:49`), `SoftState`, `IsEmptyHardState`/`IsEmptySnap` |
| 3 | Single entry point | `raft.go` | `raft.Step` (`raft.go:1085`), `send` two-queue split (`raft.go:546`) |
| 4 | Pure core + handshake | `rawnode.go` | `RawNode` (`:31`), `Ready`/`readyWithoutAccept` (`:131`,`:139`), `acceptReady` (`:400`), `Advance` (`:477`), `HasReady` (`:448`), `MustSync` (`:191`) |
| 5 | Index bookkeeping | `log.go` | `committed`/`applying`/`applied` invariants (`log.go:33`), snapshot-xor-entries (`:248`), `newLogWithSize` recovery (`:75`) |
| 6 | Time | `raft.go` | counters (`:403`), `tickElection`/`tickHeartbeat` (`:849`,`:861`), `pastElectionTimeout` (`:2046`), `Config.ElectionTick`/`HeartbeatTick` (`:130`) |
| 7 | Optional driver | `node.go` | `node` struct (`:296`), `run()` select loop + `readyc`/`advancec` gate (`:343`) |
| 8 | Storage | `storage.go` | `Storage` interface (`:42`), error sentinels (`:26`), `MemoryStorage` read/write split (`:104`) |
| 9 | (skim, then skip) | `rawnode.go`, `raftpb/` | async `MsgStorage*` branch (`rawnode.go:163`); protobuf `Message` union |

**One-paragraph takeaway.** Build a synchronous, I/O-free `Replica` whose only input is
`step(event)` (proposals, peer messages, and tick-expiries all become events) and whose only
output is a `Ready` describing what to persist, send, and apply — in that safety order, with
"durable before you tell a peer anything" as the inviolable edge. Gate it to one `Ready` in
flight, released by `advance()`. Make time a caller-driven `tick()` counted in integers.
Depend on a read-only storage trait the app writes behind. Wrap all of it in a thin,
swappable driver — your deterministic simulator being the first and most important one.
