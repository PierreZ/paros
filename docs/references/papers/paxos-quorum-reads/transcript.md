# Linearizable Quorum Reads in Paxos

**Authors:** Aleksey Charapko (University at Buffalo, SUNY; Microsoft), Ailidani Ailijiang
(Microsoft), Murat Demirbas (University at Buffalo, SUNY; Microsoft)
**Date:** July 2019. Published at HotStorage 2019 (USENIX Workshop on Hot Topics in Storage and
File Systems).
**Source:** <https://www.usenix.org/conference/hotstorage19/presentation/charapko> (open access;
the PDF beside this file is the USENIX copy,
<https://www.usenix.org/system/files/hotstorage19-paper-charapko.pdf>).

**Abstract (in full):** *"Many distributed systems/databases rely on Paxos for providing
linearizable reads. Linearizable reads in Paxos are achieved either through running a full read
round with followers, or via reading from a stable leader which holds leases on followers. We
introduce a third method for performing linearizable reads by eschewing the leader and only
reading from a quorum of followers. For guaranteeing linearizability, a bare quorum read is
insufficient and it needs to be amended with a rinse phase to account for pending update
operations. We present our Paxos Quorum Read (PQR) protocol that implements this. Our
evaluations show that PQR significantly improves throughput compared to the other methods. The
evaluations also show that PQR achieves comparable latency to the read from stable Paxos leader
optimization."*

---

## The one-sentence result

A client can read linearizably from Multi-Paxos without the leader and without leases: it asks
a majority of followers for the highest slot each one **accepted**, takes the maximum, and then
waits until that slot is **executed** at any one node (the *rinse*). The read returns the state
after that slot.

## 1. Introduction

- Linearizability: "once an operation is complete, every read must see that state or some later
  state."
- Two known ways to read linearizably in Paxos:
  1. Run the read through the log, a Paxos round at a majority. This is costly: a read changes
     no state, and read-to-write ratios above 100:1 are common.
  2. Read from a stable leader that holds **leases** on the followers, so no other leader can be
     elected during the lease.
- Both put the read load on the leader, which already serves every write. The followers do no
  client work.
- **The naive quorum read is not linearizable.** A client that reads the committed value from a
  majority of followers can miss its own acknowledged write: the followers know the "dirty"
  writes they *accepted*, not the *commit* that the leader decides when it sees a quorum of
  accepts.
- The fix: after the quorum read, the client **rinses** the dirty read by waiting for the highest
  accepted slot to become committed. §4.1 shows that the rinse is often not needed.
- Prototype result: up to 12% more throughput and up to 30% less latency in write-heavy
  workloads; similar to read-from-leader in read-heavy workloads.

## 2. Related work

- ZooKeeper-style reads from any replica can be stale.
- Client-driven quorum systems (Cassandra) have no global commit knowledge at the storage node, so
  linearizability needs a read-repair write to a majority.
- Systems with logical sequence numbers (chain replication, CRAQ, distributed versioning, Aurora)
  need one stable LSN source, so they need leases and a separate leader election. In Aurora the
  stable leader knows the commit status; a new leader must recover it from the storage replicas.
- PaxosStore: a follower that gets a read queries a majority and answers if no write is pending.
- Paxos Quorum Leases: per-object leases at followers for local reads; lease set, renewal and
  revocation make it complex, and a lost lease holder blocks reads and writes for a short time.
- A CockroachDB quorum-read proposal relies on HLC timestamps and gives only serializability.
- **PQR's position:** Paxos slots are the LSNs; the "committed" messages carry the global
  replication state; gaps in slot numbers are tolerated (they are normal in Multi-Paxos); no
  leases; a dirty write is handled by waiting for it to complete.

## 3. Paxos background

Three phases (Figure 1): phase 1 (propose/promise) establishes a leader with a ballot; phase 2
(accept) gets a majority to accept a command (a new one, or an earlier uncommitted one that
phase 1 reported); phase 3 (commit) tells the followers to commit and execute it. Multi-Paxos
keeps the leader across slots while its ballot is the highest, and piggybacks phase 3 on the
next phase 2. "In this paper we refer to the Multi-Paxos optimization when we mention Paxos."

## 4. Paxos Quorum Reads

Two phases (Figure 2):

- **Quorum-read phase.** The client reads the **latest accepted slot** from a majority of
  followers, *excluding the leader*. Each follower returns its highest accepted slot "regardless
  of the gaps in the slot numbers" (a follower with slots 1–4 and 6 returns 6). The client picks
  the **highest** slot of the majority. That slot's result is the outcome of the read, but it is
  not committed yet.
- **Rinse phase.** The client waits until that slot is **executed**. It asks *any single node*:
  one node is enough, "since the executed state carries information about global replication in
  the system: the command executes only when all commands on prior slots have been committed in
  the cluster." If the slot is executed, the read returns the result of that slot's command; if
  not, the client retries the rinse.

### 4.1 Rinse-free reads

- With no new commands, each follower's latest accepted slot equals its latest executed slot.
  When the quorum-read phase sees this, the read completes in one round trip.
- Per-object tracking (micro-shards, keys): if the accepted and executed slots **of one object**
  match on a majority, the value can be returned at once, "since no replication operation for
  that specific object was able to reach the majority nodes at the time a PQR read was started."
  If any contacted replica has different accepted and executed slots for the object, the client
  must rinse.

### 4.2 Flexible quorum optimization

By Flexible Paxos (only Q1 and Q2 must intersect), a large Q2 allows a read quorum smaller than a
majority: with N = 5 and Q2 = 4, a read asks 2 nodes; with N = 7 and Q2 = 5, it asks 3. The read
quorum only has to intersect every write quorum Q2. This also lets a wide-area client read from
nearby nodes.

## 5. Communication complexity

Table 1, messages handled per operation, with `n` nodes and read probability `r`, best case:

| Component | Paxos | Paxos, read from leader | PQR |
|---|---|---|---|
| Leader | `2n` | `2n(1 − r) + 2r` | `2n(1 − r)` |
| Follower | `2` | `2(1 − r)` | `2` |
| Client | `2` | `2` | `2(1 − r) + 2r(⌊n/2⌋ + 1)` |

PQR does not change the write path. It moves the read coordination to the client: the leader
handles fewer messages, and the client handles more.

## 6. Evaluation

- Go prototype in the Paxi framework; 5 replicas on AWS EC2 m5a.large; a key-value workload with
  1000 objects and 8-byte values.
- At 75% writes, PQR has better latency and about 12% higher maximum throughput than
  read-from-leader (Figure 3). The advantage falls at lower write ratios.
- High contention (2, 5 and 10 keys, 4000 ops/s): with 2 objects and 75% writes, about 155 rinse
  phases per second; **no read took more than 2 round trips** (Figure 5).
- Paxi's linearizability checker found no violation in over a hundred 30-second runs.
- **Without the rinse** (Figure 6), a quorum read has fewer non-linearizable reads than a read
  from any single node, but still many: "the quorum read guarantees reading at least the latest
  anchored write value. However, it may also return newer unanchored writes, which may then be
  missed by the next quorum read operation resulting in a linearizability violation."

## 7. Concluding remarks

Future work named by the authors: hash digests from all but one follower for large objects, and
running PQR **inside the cluster**, with followers acting as proxies for the clients (which also
allows batching of reads).

---

## Why this matters for paros

- **It is the source of paros's leaderless read.** Compartmentalized Paxos §3.4
  (`../scaling-rsm-compartmentalization/transcript.md`) builds its leaderless read on PQR. paros
  takes that read (`paros-consensus/src/quorum_read.rs`; card
  `docs/knowledge/concepts/leaderless-reads.md`).
- **The quorum-read phase maps to `PreRead`/`PreReadAck`.** PQR's "latest accepted slot,
  regardless of gaps" is paros's vote watermark (`Acceptor::vote_watermark`), and "pick the
  highest of a majority" is the maximum over a Phase-1 quorum.
- **The rinse maps to the cover check.** PQR waits until the slot is executed at one node; a paros
  server waits until its own applied prefix covers the index (`Replica::covers`). Both rest on
  the same fact: a slot is executed only after every lower slot is committed.
- **Differences to keep in mind:**
  - PQR excludes the leader from the quorum-read phase; paros asks any Phase-1 quorum, the
    leader included, and serves from any node or replica.
  - PQR is client-driven; paros runs it on the server for the client (PQR's own future work,
    §7).
  - PQR's quorum-read phase is sized against Q2 by Flexible Paxos (§4.2); paros asks a Phase-1
    quorum of the grid (one row on a full-column grid), which meets every Phase-2 quorum of the
    same configuration.
  - PQR assumes one configuration and one leader history. paros adds a read basis and a fence
    on the acceptor's promise, so a read across a campaign or a reconfiguration ends in place of
    being stale.
