# Vertical Paxos and Primary-Backup Replication

**Authors:** Leslie Lamport, Dahlia Malkhi, Lidong Zhou (Microsoft Research)

**Date:** 9 February 2009, corrected 26 August 2009. Published at PODC 2009. 16 pages.
**Source:** <https://www.microsoft.com/en-us/research/wp-content/uploads/2009/08/Vertical-Paxos-and-Primary-Backup-Replication-.pdf>
(the corrected version, the `paper.pdf` beside this file).

This is a digest read from the paper: the abstract and the key definitions are quoted, the rest is
summarised section by section. Page numbers are the paper's own.

---

## Abstract (in full)

"We introduce a class of Paxos algorithms called Vertical Paxos, in which reconfiguration can occur
in the middle of reaching agreement on an individual state-machine command. Vertical Paxos
algorithms use an auxiliary configuration master that facilitates agreement on reconfiguration. A
special case of these algorithms leads to traditional primary-backup protocols. We show how
primary-backup systems in current use can be viewed, and shown to be correct, as instances of
Vertical Paxos algorithms."

## §1 Introduction (p. 1)

- Practical systems use Paxos for their configuration, "not for the actual data replication". A
  real system is "a large number of overlapping replica groups, each responsible for maintaining a
  subset of the system's data", and failed replicas are replaced by reconfiguration.
- The paper views Paxos "not as a particular algorithm, but as a family of algorithms".
- State transfer to new replicas is costly. Vertical Paxos I lets a group "operate with the
  restored resilience, while enabling state transfer concurrently".
- "In Vertical Paxos, reconfiguration relies on an external master, itself implemented as a
  replicated state machine." The master "manages the global system state, participates in the
  reconfiguration of any replica group in the system".

## §2 Overview (pp. 2–4)

Two differences from ordinary Paxos:

- **Read and write quorums.** Every write b-quorum intersects every read b-quorum and every write
  b-quorum (the Flexible Paxos split, cited to Lampson's "ABCDs of Paxos").
- **An auxiliary configuration master.** "The master determines the set of acceptors and the
  leader for every configuration." The master is called only for reconfiguration, so it can be a
  very reliable replicated state machine on processors that "spend most of their time doing other
  things". With a master, a state machine tolerates `k` failures with `k + 1` processors.

**The name.** Draw the ballots as a 2D array, one column per consensus instance. "In standard
'horizontal' Paxos algorithms, configurations can change only as we move horizontally; they are
unchanged when we move vertically (within a single instance). In Vertical Paxos, configurations
change when we move vertically, but remain the same as we move horizontally from a ballot in one
instance to the ballot with the same number in any other instance." A configuration belongs to a
**ballot**, for every instance.

A new leader must read acceptors of lower ballots, which have other configurations. The two
variants:

- **Vertical Paxos I.** A new configuration is active at once. The previous one stays active "only
  for storing old information". Once state is transferred, the leader tells the master, and "the
  master will then tell all future leaders that they need not access that old configuration". If
  leaders fail before transfer, a leader may have to read many old configurations (the "b + 42"
  example).
- **Vertical Paxos II.** A new configuration is **initially inactive**. The leader tells the master
  when transfer from the previous configuration is complete, and the master then activates it. "The
  leader communicates only with acceptors from the new configuration and the previous active
  configuration." Ballots whose leaders failed stay inactive for ever.

## §3 The primary-backup case (pp. 4–5)

- A leader needs a write quorum of its own ballot and read quorums of lower ballots. Old ballots
  are more likely to have failed members, so keep read quorums small.
- Extreme case: any single acceptor is a read quorum, so the only write quorum is all acceptors
  (`k + 1` acceptors for `k` faults). In VP II with the new leader chosen among the current
  acceptors, the new leader alone is a read quorum of the previous ballot and does the state
  transfer with no messages. Call it the primary: this is primary-backup.
- Leases give local reads at the primary; "a new leader cannot be chosen until the previous
  leader's lease has expired" (Boxwood).
- "Most primary-backup protocols maintain a single active configuration, as captured by Vertical
  Paxos II." VP I "decouples state transfer from reconfiguration".

## §4.1 Paxos preliminaries (pp. 5–8)

- In VP I, `Ballots` is `Nat`. In VP II, "the numbers of ballots that are started but never
  activated are not considered to be in Ballots". `Prev(b)` is the next lower element of `Ballots`.
- The master computes `RQuorums(b)` and `WQuorums(b)`; "once made its choice is fixed".
- `vote[a][b]` (initially `None`, set once) and `maxBallot[a]` (never decreases).
- "A proposable value v is chosen in ballot b iff all acceptors in a write b-quorum have voted for
  v in ballot b."
- "v [is] safe at ballot number b iff no proposable value other than v is choosable in any ballot
  numbered less than b." Invariant: an acceptor votes in ballot b only for a value safe at b.
- **FindSafe** (Figure 1) walks `c = Prev(b), Prev(Prev(b)), ...`: take any vote cast at `c`, or
  wait for a read c-quorum whose members all have `maxBallot > c` and no vote at `c`, then step
  down. It needs one vote at most per ballot ("different acceptors cannot vote for different values
  in the same ballot").
- **Complete ballot:** "a value has been chosen in b or all values are safe at b". **VFindSafe**
  stops at a complete ballot instead of at 0, so a leader never reads below it.

## §4.2 Vertical Paxos I (pp. 8–10, Figures 2 and 3)

- The master starts each ballot with `newBallot(bal, completeBal)`, where `completeBal` is "the
  largest ballot number that the master knows to be complete". The master is "a network of
  processors running a reliable state-machine implementation, such as (horizontal) Paxos".
- Phase 1 runs VFindSafe down to `completeBal`. If it finds a value, Phase 2 gets it chosen. Then
  the leader sends `complete` to the master, which raises `completeBallot`. If all values were
  safe, the leader takes a client value.
- Message meanings: 1a asks `maxBallot[a] := bal` and the vote at `prevBal`; 2a asks
  `vote[a][bal] := val`, ignored when `bal < maxBallot[a]`.

## §4.3 Vertical Paxos II (pp. 10–14, Figures 4 and 5)

- "In Vertical Paxos II, a leader has to query only acceptors from a single previous ballot."
- The master **activates** ballot `b` (adds it to `Ballots`) when that makes `b` complete; ballots
  are activated in increasing order.
- `newBallot` carries `prevBal` = the largest activated ballot. The leader does Phase 1 against
  `prevBal` only, Phase 2 if it found a value, then sends `complete(bal, prevBal)`.
- Master (Figure 5): on `complete` with `msg.prevBal = curBallot`, send `activated(bal)` and set
  `curBallot := bal`. So only one ballot activates over each active ballot.
- The leader takes client values only after `activated`. "If ballot b is not activated, then the
  actions performed have no effect because b is not a ballot number."

## §5 Other primary-backup protocols (pp. 14–15)

- **Niobe** follows VP II closely; a global-state manager's configuration numbers are VP II ballot
  numbers. With a single write quorum, the master can remove an acceptor from the write quorum
  with no new ballot and no state transfer.
- **Chain Replication** is VP II with one active configuration and an external master; a 2a and the
  2bs travel down the chain, so the tail serves queries.
- **GFS**: "A file consists of a sequence of chunks, each replicated on a (possibly different) set
  of servers. A master tracks the composition of each file and the locations of its chunks." GFS
  is not consistent; it "can be made consistent ... by making each chunk a separate state machine
  implemented with Vertical Paxos".

## Relevance to paros

- The journal's register at the matchmakers plays the master: it holds each segment's grid and the
  ballot bound to it (`docs/journal/03-segments-and-records.md` §J3.4). The paper's master is "a
  replicated state machine"; ours is a compare-and-set register.
- The journal's seal and retire are close to VP II: the new grid is not in force until the old one
  is sealed, and a leader then reads only grids that are not retired. VP I's "complete ballot"
  matches a retire: no leader reads below it again.
- The GFS remark (a chunk as its own state machine with a master tracking chunk locations) is the
  shape of segments that spread one log across machines (Pierre, 2026-11-24).
- Matchmaker Paxos (`../matchmaker-paxos/`) cites Vertical Paxos as the master-based ancestor and
  replaces the master with matchmakers.
