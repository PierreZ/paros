# etcd-raft: message size limits and paged log transfer

An analysis of how etcd-raft keeps every message and every in-memory read of its log bounded.
A leader never sends a follower's whole missing suffix in one message: it sends a **page**
(`MaxSizePerMsg` bytes), and the follower's answer is the cursor for the next page. A scan of
the log goes page by page too (`scan`). An empty append keeps a stalled exchange alive.

Source: [etcd-io/raft](https://github.com/etcd-io/raft) at commit
`c51f010c851edd0dada5af41bafbf68e54617767` (2026-10-23), the commit of
[`check-quorum.md`](check-quorum.md) and [`replication.md`](replication.md). Paths are relative
to the repository root. House style: **(a) verbatim code + `file:line`, then (b) principle, then
(c) maps to sans-IO and to paros**.

The inflight window and the progress states are in [`replication.md`](replication.md) §2–§3.
This file reads only the size limits and the page loop. It backs the knowledge base card
[`../../knowledge/concepts/paging-a-campaign.md`](../../knowledge/concepts/paging-a-campaign.md)
(#745 (Paxos knowledge base)).

---

## 1. The four limits

```go
// raft.go:189
// MaxSizePerMsg limits the max byte size of each append message. Smaller
// value lowers the raft recovery cost(initial probing and message lost
// during normal operation). On the other side, it might affect the
// throughput during normal replication. Note: math.MaxUint64 for unlimited,
// 0 for at most one entry per message.
MaxSizePerMsg uint64
// MaxCommittedSizePerReady limits the size of the committed entries which
// can be applying at the same time.
//
// Despite its name (preserved for compatibility), this quota applies across
// Ready structs to encompass all outstanding entries in unacknowledged
// MsgStorageApply messages when AsyncStorageWrites is enabled.
MaxCommittedSizePerReady uint64
// MaxUncommittedEntriesSize limits the aggregate byte size of the
// uncommitted entries that may be appended to a leader's log. Once this
// limit is exceeded, proposals will begin to return ErrProposalDropped
// errors. Note: 0 for no limit.
MaxUncommittedEntriesSize uint64
```

```go
// raft.go:213
// MaxInflightBytes limits the number of in-flight bytes in append messages.
// Complements MaxInflightMsgs. Ignored if zero.
//
// This effectively bounds the bandwidth-delay product. [...]
MaxInflightBytes uint64
```

```go
// raft.go:317
// default MaxCommittedSizePerReady to MaxSizePerMsg because they were
// previously the same parameter.
if c.MaxCommittedSizePerReady == 0 {
	c.MaxCommittedSizePerReady = c.MaxSizePerMsg
}
// raft.go:326
if c.MaxInflightBytes == 0 {
	c.MaxInflightBytes = noLimit
} else if c.MaxInflightBytes < c.MaxSizePerMsg {
	return errors.New("max inflight bytes must be >= max message size")
}
```

**(b) principle.** Each unbounded quantity gets its own bound: one message (`MaxSizePerMsg`),
one apply batch (`MaxCommittedSizePerReady`), the leader's uncommitted tail
(`MaxUncommittedEntriesSize`) and the bytes in flight to one follower (`MaxInflightBytes`). The
comment of `MaxSizePerMsg` names the trade-off: a small page makes recovery and a lost message
cheap, and costs throughput. The limits are per process and checked at `Config` validation, with
one relationship between two of them (a window must hold at least one message).

**(c) maps to paros.** `paros_consensus::contract::Limits` is the same idea, counted in entries,
not bytes: `promise_page`, `recovery_page`, `resend_page`, `apply_page` and `registry_page`, each
under a compile-time ceiling (`PROMISE_BATCH`, `RECOVERY_BATCH`,
`RESEND_BATCH`, `APPLY_BATCH`, `REGISTRY_PAGE` in `contract/limits.rs`) and checked by
`Limits::assert_valid` at construction (the read window has a floor, `READ_TTL_TICKS`, and no ceiling). Like etcd-raft, a limit is per process and
never on the wire: "a receiver checks a page against the ceiling, never against the sender's
size" (`contract/limits.rs` module doc).

## 2. A page always carries at least one entry

```go
// util.go:282
// limitSize returns the longest prefix of the given entries slice, such that
// its total byte size does not exceed maxSize. Always returns a non-empty slice
// if the input is non-empty, so, as an exception, if the size of the first
// entry exceeds maxSize, a non-empty slice with just this entry is returned.
func limitSize(ents []*pb.Entry, maxSize entryEncodingSize) []*pb.Entry {
	if len(ents) == 0 {
		return ents
	}
	size := entryEncodingSize(proto.Size(ents[0]))
	for limit := 1; limit < len(ents); limit++ {
		size += entryEncodingSize(proto.Size(ents[limit]))
		if size > maxSize {
			return ents[:limit]
		}
	}
	return ents
}
```

**(b) principle.** A page that can be empty while entries remain makes no progress, and the
transfer stops for good. So the first entry always goes, even when it alone is over the limit.

**(c) maps to paros.** The same rule is a compile-time assertion in paros:
`const _: () = assert!(PROMISE_BATCH > 0);` with the comment "A page that cannot carry one entry
could never make progress through a faulty or accepted suffix: the continuation cursor would
never advance" (`contract/limits.rs`). `Limits::promise_page` lies in `1..=PROMISE_BATCH`.

## 3. The leader sends one page; the answer is the cursor of the next

```go
// raft.go:618
func (r *raft) maybeSendAppend(to uint64, sendIfEmpty bool) bool {
	[...]
	var ents []*pb.Entry
	// In a throttled StateReplicate only send empty MsgApp, to ensure progress.
	// Otherwise, if we had a full Inflights and all inflight messages were in
	// fact dropped, replication to that follower would stall. Instead, an empty
	// MsgApp will eventually reach the follower (heartbeats responses prompt the
	// leader to send an append), allowing it to be acked or rejected, both of
	// which will clear out Inflights.
	if pr.State != tracker.StateReplicate || !pr.Inflights.Full() {
		ents, err = r.raftLog.entries(pr.Next, r.maxMsgSize)
	}
	if len(ents) == 0 && !sendIfEmpty {
		return false
	}
	[...]
	r.send(&pb.Message{
		To:      new(to),
		Type:    pb.MsgApp.Enum(),
		Index:   new(prevIndex),
		LogTerm: new(prevTerm),
		Entries: ents,
		Commit:  new(r.raftLog.committed),
	})
	pr.SentEntries(len(ents), uint64(payloadsSize(ents)))
```

```go
// raft.go:1554 (in stepLeader, case pb.MsgAppResp, after a successful ack)
// We've updated flow control information above, which may
// allow us to send multiple (size-limited) in-flight messages
// at once (such as when transitioning from probe to
// replicate, or when freeTo() covers multiple messages). If
// we have more entries to send, send as many messages as we
// can (without sending empty messages for the commit index)
if r.id != m.GetFrom() {
	for r.maybeSendAppend(m.GetFrom(), false /* sendIfEmpty */) {
	}
}
```

**(b) principle.** The leader reads at most `maxMsgSize` bytes from `pr.Next` (`raftLog.entries`,
`log.go:415`). The follower's ack moves `pr.Next`, so each answer names where the next page
starts. The transfer of a long suffix is a loop of bounded messages driven by the answers, never
one message that holds the whole suffix.

**(c) maps to paros.** A paros `Promise` is paged the same way, in the other direction: the
acceptor sends at most `Limits::promise_page` records from the asked slot
(`Acceptor::promise_page`), and the page carries the cursor itself (`Promise::next_from_slot`,
`None` on the last page). The candidate asks for the next page from that cursor
(`request_promise_page` in `node/election.rs`). Only a complete answer counts toward the Phase-1
quorum. etcd-raft never needs this: a Raft vote carries no log (the candidate's log is already
up to date, `raftLog.isUpToDate`, `log.go:442`), while a Paxos promise carries the acceptor's
whole accepted suffix.

## 4. An empty message keeps a stalled exchange alive

```go
// raft.go:1571
case pb.MsgHeartbeatResp:
	pr.RecentActive = true
	pr.MsgAppFlowPaused = false

	// NB: if the follower is paused (full Inflights), this will still send an
	// empty append, allowing it to recover from situations in which all the
	// messages that filled up Inflights in the first place were dropped. Note
	// also that the outgoing heartbeat already communicated the commit index.
	[...]
	if pr.Match < r.raftLog.lastIndex() || pr.State == tracker.StateProbe {
		r.sendAppend(m.GetFrom())
	}
```

**(b) principle.** A paged exchange can stall when its messages are lost. On each heartbeat
answer from a follower that lags (or that is in probe state), the leader sends an append. When
the follower's inflight window is full, that append is **empty** (`maybeSendAppend`,
`raft.go:618`): the follower answers it, and the answer clears the window and moves the exchange
on. The empty append is cheap because it carries no entries.

**(c) maps to paros.** A paging candidate re-sends its `Prepare` to the peers that already
answered on every tick, so they keep their election clocks quiet while it pages another peer
(`ColocatedNode::keep_paging_promisers`, `node/election.rs`). It asks from one past every slot it
merged (`Election::keep_alive_from`, `proposer/election.rs`), so an answered peer sends an
**empty** page, the same cheap keep-alive as etcd-raft's empty append to a full window. Before that rule, each
keep-alive asked from the campaign's first slot and carried the whole suffix again (#428 (paging
livelock)).

## 5. A scan of the log is paged too

```go
// log.go:472
// scan visits all log entries in the [lo, hi) range, returning them via the
// given callback. The callback can be invoked multiple times, with consecutive
// sub-ranges of the requested range. Returns up to pageSize bytes worth of
// entries at a time. May return more if a single entry size exceeds the limit.
[...]
func (l *raftLog) scan(lo, hi uint64, pageSize entryEncodingSize, v func([]*pb.Entry) error) error {
	for lo < hi {
		ents, err := l.slice(lo, hi, pageSize)
		if err != nil {
			return err
		} else if len(ents) == 0 {
			return fmt.Errorf("got 0 entries in [%d, %d)", lo, hi)
		}
		if err := v(ents); err != nil {
			return err
		}
		lo += uint64(len(ents))
	}
	return nil
}
```

```go
// raft.go:1000 (in hasUnappliedConfChanges, run when a candidate campaigns)
// Scan all unapplied committed entries to find a config change. Paginate the
// scan, to avoid a potentially unlimited memory spike.
lo, hi := r.raftLog.applied+1, r.raftLog.committed+1
// Reuse the maxApplyingEntsSize limit because it is used for similar purposes
// (limiting the read of unapplied committed entries) when raft sends entries
// via the Ready struct for application.
[...]
pageSize := r.raftLog.maxApplyingEntsSize
```

**(b) principle.** A local read of an unbounded range is bounded as well: the scan walks pages
of the log, and a page with no entry while the range is not done is an error, not a silent end.
The check a candidate runs before it campaigns is one such scan.

**(c) maps to paros.** The other paged walks of the core follow the same shape: the leader's
recovery after a win (`Limits::recovery_page`, `RECOVERY_BATCH`), the fair re-send of open rounds
(`Proposer::resend_page`, `RESEND_BATCH`), the apply walk (`Replica::advance`, `APPLY_BATCH`) and a
matchmaker's registry answer (`REGISTRY_PAGE`). Each has a cursor and a ceiling that is greater
than zero.

## Summary for paros

| etcd-raft | paros |
|---|---|
| `MaxSizePerMsg`, bytes per `MsgApp` | `Limits::promise_page`, records per `Promise` page (and the other pages of `Limits`) |
| `limitSize` keeps at least one entry | `PROMISE_BATCH > 0`, `promise_page` in `1..=PROMISE_BATCH` |
| The ack moves `pr.Next`, the next page starts there | `Promise::next_from_slot` is the cursor, `request_promise_page` asks from it |
| An append on each heartbeat answer from a lagging follower, empty when its window is full | A keep-alive `Prepare` from `Election::keep_alive_from`, answered with an empty page |
| `scan` pages a local read | the recovery, re-send, apply and registry pages |
| A vote carries no log | A promise carries the accepted suffix, so Phase 1 is paged |
