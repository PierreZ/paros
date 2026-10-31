# etcd-raft: durable before send, the progress states, and the inflight window

An analysis of how an etcd-raft leader replicates its log: which messages wait for the disk,
how the leader tracks each follower (`Progress` and its three states), how it bounds what is
in flight (`Inflights`), and how it brings a lagging follower back (probe, then snapshot).

Source: [etcd-io/raft](https://github.com/etcd-io/raft) at commit
`c51f010c851edd0dada5af41bafbf68e54617767` (2026-10-23). Paths are relative to the repository
root. House style: **(a) verbatim code + `file:line`, then (b) principle, then (c) maps to
sans-IO and to paros**.

The whole `Ready`/`Advance` handshake is in
[`../../analysis/go-raft/etcd-raft-sans-io-patterns.md`](../../analysis/go-raft/etcd-raft-sans-io-patterns.md)
§2 (its line numbers are from an older commit). This file pins the parts the knowledge base
cites, at `c51f010`.

---

## 1. Durable before send: votes and acks wait for the disk

```go
// raft.go:546
if m.GetType() == pb.MsgAppResp || m.GetType() == pb.MsgVoteResp || m.GetType() == pb.MsgPreVoteResp {
	...
	// For most message types, this is not an issue. However, response
	// messages that relate to "voting" on either leader election or log
	// appends require durability before they can be sent. It would be
	// incorrect to publish a vote in an election before that vote has been
	// synced to stable storage locally. Similarly, it would be incorrect to
	// acknowledge a log append to the leader before that entry has been
	// synced to stable storage locally.
	...
	r.msgsAfterAppend = append(r.msgsAfterAppend, m)
```

```text
// doc.go:79
2. Send all Messages to the nodes named in the To field. It is important that
no messages be sent until the latest HardState has been persisted to disk,
and all Entries written by any previous Ready batch (Messages may be sent while
entries from the same batch are being persisted). To reduce the I/O latency, an
optimization can be applied to make leader write to disk in parallel with its
followers (as explained at section 10.2.1 in Raft thesis).
```

**(b) principle.** A message that **promises** something (a vote, an append ack) waits until
the state it promises is durable. etcd-raft makes this structural: such a message goes into a
second queue, `msgsAfterAppend`, that is released only after the write. A leader's `MsgApp`
does not promise anything, so it may leave while the leader's own copy is still being written.

**(c) maps to paros.** `Ready` (`paros-core/src/node/ready.rs`) orders the buckets: persist
`writes`, then send `messages`. `MustSync::for_batch` (`paros-core/src/contract/write.rs`) says
when the batch needs an fsync. The leader exception is #578 (leader sends Accept before its own
fsync): the driver sends a batch's `Accept`s before its flush, unless the batch raises the
promise (`split_early`, `paros-roles/src/node/ready.rs`).

## 2. Progress: one record per follower, three states

```go
// tracker/state.go:21
// StateProbe indicates a follower whose last index isn't known. Such a
// follower is "probed" (i.e. an append sent periodically) to narrow down
// its last index. In the ideal (and common) case, only one round of probing
// is necessary as the follower will react with a hint. Followers that are
// probed over extended periods of time are often offline.
StateProbe StateType = iota
// StateReplicate is the state steady in which a follower eagerly receives
// log entries to append to its log.
StateReplicate
// StateSnapshot indicates a follower that needs log entries not available
// from the leader's Raft log. Such a follower needs a full snapshot to
// return to StateReplicate.
StateSnapshot
```

```go
// tracker/progress.go:130
func (pr *Progress) BecomeProbe() {
	...
		pr.ResetState(StateProbe)
		pr.Next = pr.Match + 1
	...
}

// tracker/progress.go:146
func (pr *Progress) BecomeReplicate() {
	pr.ResetState(StateReplicate)
	pr.Next = pr.Match + 1
}
```

```go
// tracker/progress.go:205
func (pr *Progress) MaybeUpdate(n uint64) bool {
	if n <= pr.Match {
		return false
	}
	pr.Match = n
	pr.Next = max(pr.Next, n+1) // invariant: Match < Next
	pr.MsgAppFlowPaused = false
	return true
}
```

**(b) principle.** The leader keeps `Match` (known to match) and `Next` (the next index to
send) per follower. A follower whose position is unknown is **probed**: at most one append per
heartbeat until it answers. A follower that answered is **replicated** optimistically: `Next`
moves on as soon as an append leaves. A follower that needs entries the leader no longer has
gets a **snapshot**. A rejection (`MaybeDecrTo`, `tracker/progress.go:226`) moves `Next` back.

**(c) maps to paros.** paros keeps no per-follower replication progress. (A matchmaker
deployment's leader notes each member's chosen index from beat acks, for GC only.) A Paxos log is per slot, so the
leader tracks each **slot** (`Rounds`, `paros-core/src/proposer/rounds.rs`), not each
follower. A follower learns a decided slot it missed by **pulling** it: a beat carries the
leader's chosen index, and a follower that is behind sends a `CatchUpRequest`
(`on_heartbeat`, `pull_gap`, `paros-core/src/node/replication.rs`). The leader's role is
passive: it serves what is asked (`serve_catchup`, `paros-core/src/node/catch_up.rs`). The
snapshot state maps onto the trim-point jump (`Message::TrimmedTo`): paros sends no bytes and
no state machine, only the floor and the journal state.

## 3. The inflight window

```go
// tracker/inflights.go:23
// Inflights limits the number of MsgApp (represented by the largest index
// contained within) sent to followers but not yet acknowledged by them. Callers
// use Full() to check whether more messages can be sent, call Add() whenever
// they are sending a new append, and release "quota" via FreeLE() whenever an
// ack is received.
```

```go
// tracker/inflights.go:131
func (in *Inflights) Full() bool {
	return in.count == in.size || (in.maxBytes != 0 && in.bytes >= in.maxBytes)
}
```

```go
// raft.go:207
// MaxInflightMsgs limits the max number of in-flight append messages during
// optimistic replication phase. The application transportation layer usually
// has its own sending buffer over TCP/UDP. Setting MaxInflightMsgs to avoid
// overflowing that sending buffer. ...
MaxInflightMsgs int
```

**(b) principle.** Pipelining is bounded per follower, by message count and by bytes. When the
window is full, the leader sends only empty appends (`raft.go:639`), so a lost window still
gets acked or rejected and frees itself.

**(c) maps to paros.** paros bounds pages, not a per-follower window. A re-send page carries at
most `Limits::resend_page` rounds (`RESEND_BATCH` = 64), a recovery page at most
`Limits::recovery_page` (both in `paros-core/src/contract/limits.rs`), and a catch-up answer at
most `CATCHUP_BATCH` = 64 slots (`paros-core/src/node/catch_up.rs`). How many rounds are open at once is the driver's matter
(the client's pipeline and the batcher), not the core's.

## 4. Sending an append, and the fallback to a snapshot

```go
// raft.go:618
func (r *raft) maybeSendAppend(to uint64, sendIfEmpty bool) bool {
	pr := r.trk.Progress[to]
	if pr.IsPaused() {
		return false
	}

	prevIndex := pr.Next - 1
	prevTerm, err := r.raftLog.term(prevIndex)
	if err != nil {
		// The log probably got truncated at >= pr.Next, so we can't catch up the
		// follower log anymore. Send a snapshot instead.
		return r.maybeSendSnapshot(to, pr)
	}
	...
	if pr.State != tracker.StateReplicate || !pr.Inflights.Full() {
		ents, err = r.raftLog.entries(pr.Next, r.maxMsgSize)
	}
	...
```

**(b) principle.** A follower behind the leader's truncation point cannot be caught up with
entries; it gets a snapshot. Each append is cut at `MaxSizePerMsg` bytes (`raft.go:189`).

**(c) maps to paros.** `serve_catchup` answers a request below its own floor with
`Message::TrimmedTo` (the trim point and the journal state, no ballot), and the requester jumps
(`on_trimmed_to`). Each `CatchUpResponse` is bounded by `CATCHUP_BATCH`.

## 5. The follower's rejection hint

```go
// raft.go:1783
func (r *raft) handleAppendEntries(m *pb.Message) {
	...
	if mlastIndex, ok := r.raftLog.maybeAppend(a, m.GetCommit()); ok {
		r.send(&pb.Message{To: m.From, Type: pb.MsgAppResp.Enum(), Index: new(mlastIndex)})
		return
	}
	...
	// Our log does not match the leader's at index m.Index. Return a hint to the
	// leader - a guess on the maximal (index, term) at which the logs match.
```

**(b) principle.** Raft needs a log-matching check because an append carries the previous
entry's term. The rejection hint lets the leader skip back fast.

**(c) maps to paros.** Paxos has no log-matching rule: each slot is its own instance, and an
acceptor accepts any ballot at or above its promise, at any slot (`Acceptor::admit`). There is no rejection
for a gap, so there is no hint. A gap is filled by catch-up or by the next leader's gap fill.

---

## Why this matters for paros

1. Durable before send is the same rule in both systems, and so is the leader exception
   (Raft thesis §10.2.1 in etcd-raft; #578 in paros).
2. etcd-raft tracks followers; paros tracks slots. That is the structural difference between a
   Raft log and a Multi-Paxos log, and it is why paros's catch-up is a pull, not a probe.
3. etcd-raft bounds a per-follower window; paros bounds each page. Both keep one message from
   being unbounded.
