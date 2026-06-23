# Paxos vs Raft: Have we reached consensus on distributed consensus?

**Authors:** Heidi Howard, Richard Mortier (University of Cambridge)
**Date:** 27 Apr 2020 (arXiv:2004.05074v2). Published at PaPoC 2020 (EuroSys workshop).
**Source:** https://arxiv.org/abs/2004.05074

**Abstract (in full):** *"Distributed consensus is a fundamental primitive for constructing fault-tolerant, strongly-consistent distributed systems. Though many distributed consensus algorithms have been proposed, just two dominate production systems: Paxos, the traditional, famously subtle, algorithm; and Raft, a more recent algorithm positioned as a more understandable alternative to Paxos. In this paper, we consider the question of which algorithm, Paxos or Raft, is the better solution to distributed consensus? We analyse both to determine exactly how they differ by describing a simplified Paxos algorithm using Raft's terminology and pragmatic abstractions. We find that both Paxos and Raft take a very similar approach to distributed consensus, differing only in their approach to leader election. Most notably, Raft only allows servers with up-to-date logs to become leaders, whereas Paxos allows any server to be leader provided it then updates its log to ensure it is up-to-date. Raft's approach is surprisingly efficient given its simplicity as, unlike Paxos, it does not require log entries to be exchanged during leader election. We surmise that much of the understandability of Raft comes from the paper's clear presentation rather than being fundamental to the underlying algorithm being presented."*

---

## The thesis

Strip away presentation and the two algorithms are nearly the same leader-based, log-replicating
state-machine-replication protocol. **They differ only in leader election.** Raft's reputation for
understandability comes mostly from its excellent presentation and pragmatic abstractions, not from a
fundamentally simpler algorithm. The method: re-describe MultiPaxos in Raft's own terminology
(servers, terms, logs, `AppendEntries`/`RequestVote` RPCs) so the two can be compared line-by-line
(Appendices A & B are the two algorithms side by side, with red text marking what's unique to each).

## 2. Model

State-machine replication over `n` non-Byzantine servers, asynchronous (no clock sync for *safety*,
only for *liveness*), reliable in-order channels (TCP). Each server has a unique id `s ∈ {0..n−1}`;
operations are made unique with a (sequence number, server id) pair.

## 3. The shared structure

Three server states — **Follower** (passive, answers RPCs), **Candidate** (running an election via
`RequestVote`), **Leader** (replicating via `AppendEntries`). A monotonic **term** stamps every RPC;
the universal rule: a larger incoming term makes you update and step down to follower; a smaller term
gets a negative reply that makes the sender step down.

**Normal operation (identical in both):** the leader appends a client op to its log with the current
term (the (op, term) pair is a *log entry*), sends `AppendEntries`; a follower appends only if its log
matches the leader's up to that point (so the log stays gap-free and in-order); once a majority
acknowledge, the leader advances its **commit index**, applies the op, and piggybacks the new commit
index on later `AppendEntries`.

## 3.3 The one real difference — leader election

**Paxos (this paper's MultiPaxos formulation):**
- A follower that times out becomes a candidate and picks the next term `t` with **`t mod n = s`** —
  terms are partitioned round-robin among servers, so there is **at most one candidate (hence one
  leader) per term** by construction.
- `RequestVote` carries the candidate's term and **commit index**; a server votes yes if the term
  exceeds its own and **returns all its log entries after the candidate's commit index**.
- Once a majority vote, the candidate makes its log up-to-date: for each index past the commit index
  it adopts the received entry (the one with the **greatest term** if several), **re-stamped with the
  new term**, then becomes leader.

**Raft:**
- A follower that times out **increments** its term (any server may stand in any term) and votes for
  itself; each server grants **one vote per term**, so split votes are possible → ties are broken by
  randomized election timeouts.
- A vote is granted only if the candidate's log is **at least as up-to-date** as the voter's
  (compare last log term, then last index). So **no log entries are exchanged during election** — the
  up-to-date restriction guarantees the winner already has every committed entry.
- For safety, a new Raft leader **may not advance its commit index until it has committed an entry
  from its own current term** (the "no committing previous-term entries by count alone" rule).

## 3.4 Safety (both)

Both guarantee **State Machine Safety** (applied entry at an index is the same everywhere) via **Leader
Completeness** (an op committed at index `i` in term `t` is present at `i` in every leader of term
`> t`). Proof is by induction over terms, using majority-quorum intersection + term-ordered messages:
some server holding the committed entry must have voted for the next leader, and could not have
overwritten it. Same skeleton for both; Raft's details differ because of its election rule.

## 4. Discussion — understandability vs efficiency

**Understandability (Raft slightly ahead, not significantly):**
- In **Raft** an entry keeps a fixed (index, term) for life — clean. In **Paxos** a future leader may
  re-stamp an entry with a higher term (e.g. entries B/C in Fig. 2b), and an entry before the commit
  index may be **overwritten** (safely — only by the same operation), which is less intuitive.
- Trade-off flip side: **Paxos can commit an entry as soon as it's on a majority**; Raft cannot commit
  a *previous-term* entry by majority alone — it must first commit a current-term entry. So a Paxos
  leader only ever replicates current-term-or-already-committed entries; a Raft leader may be
  replicating uncommitted previous-term entries.

**Efficiency (Raft's election surprisingly lightweight):**
- Paxos's `t mod n = s` means concurrent candidates have *different* terms → the higher term wins, **no
  split votes**. Raft's same-term candidates can split votes → randomized backoff → slower, higher
  variance leader election.
- BUT Raft sends **no log entries during election** (the up-to-date rule does the work), whereas every
  positive Paxos `RequestVote` reply ships the follower's entries past the commit index — and Paxos may
  re-send an entry to a server that already has it (because the term changed). Net: Raft's election is
  more network-efficient despite being simpler.

## 5. Relation to classical Paxos

This "Paxos" is **MultiPaxos / multi-decree Paxos**, surveyed from the literature, closer to how Paxos
is used today than to *The Part-Time Parliament*. Mappings to classic terminology: **term** = view /
ballot number / proposal number / round; **leader** = master / primary / coordinator / distinguished
proposer; **candidate period** = phase 1, **leader period** = phase 2; `RequestVote` =
phase1a/1b = prepare/promise; `AppendEntries` = phase2a/2b = accept request/response. Terms need only
be totally ordered, partitioned per server (safety), and unboundedly growable (liveness) — round-robin
integers here, lexicographic (int, server-id) pairs elsewhere. In-order log replication is a
simplification (avoids gap-filling); classic Paxos often allows out-of-order decisions + a gap-filling
protocol, and bounds concurrent decisions (needed for reconfiguration).

## 6. Summary — the three precise differences

(i) Paxos **divides terms** between servers (round-robin), so one candidate per term; Raft lets any
follower stand but grants **one vote per term**.
(ii) Paxos followers vote for **any** higher-term candidate; Raft followers vote only for a candidate
whose log is **at least as up-to-date**.
(iii) For uncommitted previous-term entries, Paxos **re-replicates them in the current term**; Raft
**replicates them in their original term** (and can't commit-by-count until a current-term entry
commits).

Conclusion: not significantly different in understandability; Raft's leader election is surprisingly
lightweight. Both presentations are deliberately naïve and optimisable (at the cost of complexity).

---

## Why this matters for `paros`

- **Election is the seam, and it's separable.** Both protocols are "leader + in-order log replication"
  with the *only* algorithmic difference in how a leader is chosen. This validates paros keeping
  **leader election as a pluggable module** distinct from the consensus core (the same separation seen
  in frankenpaxos's `election`/`heartbeat` and Ceph's external Elector). A sans-IO core can expose
  "who is leader for this term/ballot" as soft state and let the driver decide the election rule.
- **Two designs for leader recovery, pick deliberately.** Paxos-style ("any server leads, then catches
  its log up by pulling entries during election") vs. Raft-style ("only an up-to-date server may lead,
  so no entries move during election"). The paper makes the trade-off explicit: Paxos commits on a bare
  majority but ships logs during election and re-stamps/overwrites entries; Raft's election is cheaper
  but adds the commit-an-entry-from-your-own-term rule. paros's v1 (single leader, fixed membership) can
  start with whichever is simpler to reason about and revisit later.
- **Pairs directly with the etcd-raft analysis.** The repo already has
  [`../../analysis/go-raft/etcd-raft-sans-io-patterns.md`](../../analysis/go-raft/etcd-raft-sans-io-patterns.md);
  this paper is the precise dictionary between that Raft mental model and the Paxos lineage paros
  targets — e.g. `AppendEntries`↔phase-2, `RequestVote`↔phase-1, term↔ballot.
- **Persistent vs volatile state, spelled out.** Appendix A's state list (persistent: `currentTerm`,
  `log[]`, updated on stable storage *before* responding to RPCs; volatile: `commitIndex`,
  `lastApplied`, leader's `nextIndex[]`/`matchIndex[]`) is a ready-made checklist for the sans-IO
  persistence boundary — what the caller must fsync vs. what the core may keep in memory.
- **In-order vs out-of-order is a conscious choice.** Raft (and this Paxos) decide in-order to avoid
  gap-filling; classic MultiPaxos pipelines slots out-of-order and fills gaps with no-ops (see the
  frankenpaxos multipaxos-core doc). paros targets the pipelined per-slot model, so it takes on
  gap-filling that this paper deliberately sidesteps — worth knowing which simplification you're *not*
  buying.
