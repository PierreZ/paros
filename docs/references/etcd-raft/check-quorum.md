# etcd-raft: CheckQuorum, the vote lease, and leader transfer

An analysis of how etcd-raft keeps a leader honest about its quorum. Three mechanisms work
together: **CheckQuorum** (a leader that cannot hear a quorum for one election timeout steps
down), the **vote lease** (a follower that heard a leader recently refuses a vote), and
**leader transfer** (a leader hands leadership to a named follower through `MsgTimeoutNow`).

Source: [etcd-io/raft](https://github.com/etcd-io/raft) at commit
`c51f010c851edd0dada5af41bafbf68e54617767` (2026-10-23). Paths are relative to the
repository root. House style: **(a) verbatim code + `file:line`, then (b) principle, then
(c) maps to sans-IO and to paros**.

The sans-IO shape of etcd-raft as a whole (`RawNode`, `Ready`, `Step`, `Tick`) is in
[`../../analysis/go-raft/etcd-raft-sans-io-patterns.md`](../../analysis/go-raft/etcd-raft-sans-io-patterns.md).
This file reads only the leadership-liveness code.

---

## 1. The option and its one hard dependency

```go
// raft.go:224
// CheckQuorum specifies if the leader should check quorum activity. Leader
// steps down when quorum is not active for an electionTimeout.
CheckQuorum bool
```

```go
// raft.go:336
if c.ReadOnlyOption == ReadOnlyLeaseBased && !c.CheckQuorum {
	return errors.New("CheckQuorum must be enabled when ReadOnlyOption is ReadOnlyLeaseBased")
}
```

**(b) principle.** CheckQuorum is optional. A lease-based read needs it, because a lease read
trusts that no other leader exists, and only CheckQuorum makes a cut-off leader stop.

**(c) maps to paros.** paros has no option: `ColocatedNode::tick_check_quorum` always runs on a
leader whose election timeout is set. paros has no lease-based read either; every read is a
leaderless quorum read (`paros-core/src/quorum_read.rs`), so CheckQuorum in paros guards writes,
not reads.

## 2. The leader's clock fires the check once per election timeout

```go
// raft.go:861
// tickHeartbeat is run by leaders to send a MsgBeat after r.heartbeatTimeout.
func (r *raft) tickHeartbeat() {
	r.heartbeatElapsed++
	r.electionElapsed++

	if r.electionElapsed >= r.electionTimeout {
		r.electionElapsed = 0
		if r.checkQuorum {
			if err := r.Step(&pb.Message{From: new(r.id), Type: pb.MsgCheckQuorum.Enum()}); err != nil {
				r.logger.Debugf("error occurred during checking sending heartbeat: %v", err)
			}
		}
		// If current leader cannot transfer leadership in electionTimeout, it becomes leader again.
		if r.state == StateLeader && r.leadTransferee != None {
			r.abortLeaderTransfer()
		}
	}
	...
```

**(b) principle.** A leader keeps an election clock too. The clock does not start an election
on a leader; it closes an **activity window**. The window has the same length as a follower's
election timeout. The same clock also bounds a leader transfer (§5).

**(c) maps to sans-IO.** The check is a local message (`MsgCheckQuorum`) that the leader steps
into itself. No I/O, no wall clock: the caller's `Tick` drives it.

## 3. What counts as "active"

```go
// raft.go:949
// The leader always has RecentActive == true; MsgCheckQuorum makes sure to
// preserve this.
pr.RecentActive = true
```

```go
// raft.go:1380 (stepLeader, case pb.MsgAppResp)
pr.RecentActive = true
```

```go
// raft.go:1572 (stepLeader, case pb.MsgHeartbeatResp)
pr.RecentActive = true
```

```go
// tracker/tracker.go:216
func (p *ProgressTracker) QuorumActive() bool {
	votes := map[uint64]bool{}
	p.Visit(func(id uint64, pr *Progress) {
		if pr.IsLearner {
			return
		}
		votes[id] = pr.RecentActive
	})

	return p.Voters.VoteResult(votes) == quorum.VoteWon
}
```

**(b) principle.** A follower is active when the leader received an append response or a
heartbeat response from it in the window. The leader is always active. Learners do not count.
The quorum is the voters' quorum (`VoteResult`): the same quorum an election needs.

## 4. The check itself, and the reset

```go
// raft.go:1281 (stepLeader)
case pb.MsgCheckQuorum:
	if !r.trk.QuorumActive() {
		r.logger.Warningf("%x stepped down to follower since quorum is not active", r.id)
		r.becomeFollower(r.Term, None)
	}
	// Mark everyone (but ourselves) as inactive in preparation for the next
	// CheckQuorum.
	r.trk.Visit(func(id uint64, pr *tracker.Progress) {
		if id != r.id {
			pr.RecentActive = false
		}
	})
	return nil
```

**(b) principle.** At the end of each window, a leader without an active quorum becomes a
follower **at the same term**. It does not increment the term and it does not start an
election. Then every peer is marked inactive, so the next window must earn its quorum again.

**(c) maps to paros.** `ColocatedNode::tick_check_quorum` (`paros-core/src/node/authority.rs`)
has the same shape: the window age is `Authority::tick`, the credit is `Authority::credit`, the
reset is `Authority::renew`, and a failed check is `become_follower(None)`. Two differences:

- paros asks for a **Phase-2** quorum of the leader's configuration
  (`Authority::holds` calls `AcceptorConfig::has_phase2_quorum`). etcd-raft has only majority
  quorums, where the two are equal. Under flexible or grid quorums they differ.
- paros credits an ack only when it echoes the leader's own ballot
  (`on_heartbeat_ack`, `on_accepted`). etcd-raft ignores a lower-term response (`raft.go:1181`) before
  `stepLeader` runs, so the effect is the same.

## 5. The vote lease: the follower half

```go
// raft.go:1101 (Step, case m.GetTerm() > r.Term)
if m.GetType() == pb.MsgVote || m.GetType() == pb.MsgPreVote {
	force := bytes.Equal(m.GetContext(), []byte(campaignTransfer))
	inLease := r.checkQuorum && r.lead != None && r.electionElapsed < r.electionTimeout
	if !force && inLease {
		// If a server receives a RequestVote request within the minimum election timeout
		// of hearing from a current leader, it does not update its term or grant its vote
		...
		return nil
	}
}
```

**(b) principle.** With CheckQuorum on, a follower that heard its leader less than one
election timeout ago ignores a higher-term vote request. It does not raise its term. A
partitioned node that comes back with a high term therefore cannot depose a healthy leader.
A leader transfer sets `force` and passes the lease.

**(c) maps to paros.** paros has **no** vote lease. An acceptor promises any `Prepare` above its
promise (`on_prepare` in `paros-core/src/node/acceptor.rs`), whatever its election clock says.
A disruptive candidate costs one election, never safety. This is a deliberate gap, not an
oversight: see the CheckQuorum card in `docs/knowledge/concepts/check-quorum.md`.

## 6. Leader transfer: `MsgTransferLeader` and `MsgTimeoutNow`

```go
// raft.go:1628 (stepLeader, per-follower messages)
case pb.MsgTransferLeader:
	...
	// Transfer leadership should be finished in one electionTimeout, so reset r.electionElapsed.
	r.electionElapsed = 0
	r.leadTransferee = leadTransferee
	if pr.Match == r.raftLog.lastIndex() {
		r.sendTimeoutNow(leadTransferee)
		...
	} else {
		r.sendAppend(leadTransferee)
	}
```

```go
// raft.go:1304 (stepLeader, case pb.MsgProp)
if r.leadTransferee != None {
	r.logger.Debugf("%x [term %d] transfer leadership to %x is in progress; dropping proposal", r.id, r.Term, r.leadTransferee)
	return ErrProposalDropped
}
```

```go
// raft.go:1750 (stepFollower)
case pb.MsgTimeoutNow:
	r.logger.Infof("%x [term %d] received MsgTimeoutNow from %x and starts an election to get leadership.", r.id, r.Term, m.GetFrom())
	// Leadership transfers never use pre-vote even if r.preVote is true; we
	// know we are not recovering from a partition so there is no need for the
	// extra round trip.
	r.hup(campaignTransfer)
```

```go
// raft.go:2053
func (r *raft) abortLeaderTransfer() {
	r.leadTransferee = None
}
```

**(b) principle.** etcd-raft's transfer is **not** a transfer of authority. The leader stops
taking proposals, brings the target's log up to date, and tells it to campaign at once. The
target then runs an ordinary election at a **higher term**, with `force` set so the vote lease
does not block it. If the target does not win within one election timeout, the old leader
aborts the transfer and keeps leading (§2).

**(c) maps to paros.** paros's handoff (`ColocatedNode::relinquish_to`,
`paros-core/src/node/handoff.rs`) is the opposite design: the successor continues **the same
ballot** with no Phase 1, and the old leader abdicates in the same call, so there is no abort
path. See `docs/knowledge/concepts/leader-handoff.md` for the comparison.

---

## Why this matters for paros

1. CheckQuorum is the reference for `tick_check_quorum` (#95). The window length equals the
   election timeout in both systems, so a demoted leader's peers can already campaign when it
   steps down.
2. paros keeps the leader half (CheckQuorum) and drops the follower half (the vote lease).
   etcd-raft needs the lease because a Raft vote is the only thing that raises a term; paros's
   ballots are per-node and a higher `Prepare` is always safe to promise.
3. Leader transfer is a forced election; paros's handoff is a transfer of the ballot itself. The
   two are relatives, not equals.
