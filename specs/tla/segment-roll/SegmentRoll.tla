----------------------------- MODULE SegmentRoll -----------------------------
(***************************************************************************)
(* The cut of a segment by a decided command, the `Roll` (step 1 of        *)
(* docs/archive/journal-design-a/prompt.md, rules R2 to R5).               *)
(*                                                                         *)
(* One Multi-Paxos log on one fixed grid of three acceptors with           *)
(* majorities. A value is 0 (a `Noop`), 1 (a client command) or k >= 2 (a  *)
(* `Roll` to segment k). A `Roll` to k is valid at a slot when the segment *)
(* of that slot is k - 1 and no earlier `Roll` to k is valid. A valid      *)
(* `Roll` at slot s ends segment k - 1 at s + Alpha, and segment k starts  *)
(* there. With Alpha = 1 the `Roll` is the last slot of its segment. A     *)
(* `Roll` that is not valid changes nothing. The segment of a slot is the  *)
(* fold of the valid `Roll`s at least Alpha slots below it (`SegAt`).      *)
(*                                                                         *)
(* Each ballot has one owner. Phase 1 takes a quorum of promises and a     *)
(* first slot `lo` whose prefix is chosen. From `lo` on, each slot gets    *)
(* the vote of the highest ballot that the quorum reported (the forced     *)
(* value), a slot below the frontier with no report gets the hole fill,    *)
(* and a slot at or above the frontier gets a fresh value: a command, or a *)
(* `Roll` to the tail segment that the owner believes plus one. Each       *)
(* proposal carries the segment of its slot as the owner believes it       *)
(* (`k`): the fold of the chosen prefix below `lo` and of its own          *)
(* proposals from `lo`.                                                    *)
(*                                                                         *)
(* Routing is the segment whose replicas get the `Commit` of a slot        *)
(* (`Sent`): the `k` of the proposal. The belief is wrong when a later     *)
(* ballot chooses, below the slot, the `Roll` of an older ballot that the  *)
(* owner's Phase 1 did not see. Two rules keep a wrong belief away from    *)
(* the replicas, each with its switch:                                     *)
(*   Route = "commit" (the rule): the `Commit` of slot s leaves only when  *)
(*     the owner knows that every slot below s + 1 - Alpha is chosen (R3   *)
(*     for the leader; with Alpha = 1, every slot below s). Its belief is  *)
(*     then a fold of a chosen prefix, so it is the true segment by        *)
(*     construction. The checks of this rule use Alpha = 1 only.           *)
(*   Window, with Alpha (the window of Paxos Made Simple): the owner       *)
(*     proposes only inside the window.                                    *)
(*                                                                         *)
(* The replicas of a segment are a disjoint set for each segment. A        *)
(* replica of segment 1 starts at slot 1. A replica of segment k > 1 takes *)
(* its start from the chain. A replica applies the value at its next slot; *)
(* a `Roll` to its segment plus one sets its end, Alpha slots later, and   *)
(* the replica applies up to its end. A replica with an end publishes the  *)
(* end of its segment and the start of the next one in the                 *)
(* chain, when the chain has none (the add-only map of R5).                *)
(*                                                                         *)
(* The switches (each one is a rule of the prompt; the other value is a    *)
(* control):                                                               *)
(*   Barrier  TRUE: a ballot proposes nothing above a `Roll` of its own    *)
(*            ballot until that `Roll` is chosen at that ballot (R4a).     *)
(*   HoleFill "noop": a hole gets a `Noop` (R4b). "any": any value.        *)
(*   ActOn    "chosen": a replica applies a chosen value only, in slot     *)
(*            order (R3). "vote": any value that an acceptor voted.        *)
(*   Base     "prefix": the owner takes its tail segment from its chosen   *)
(*            prefix (R4c). "chain": from the chain alone.                 *)
(*   Route    "commit": the `Commit` of slot s leaves when the owner knows *)
(*            that every slot below s + 1 - Alpha is chosen. "propose": it *)
(*            leaves when s is chosen at the ballot.                       *)
(*   Window   TRUE: a ballot proposes at slot s only when it knows that    *)
(*            every slot below s + 1 - Alpha is chosen (the window of      *)
(*            Paxos Made Simple, section 3: a command at slot s has its    *)
(*            effect at slot s + Alpha). FALSE: it proposes at any slot.   *)
(*            With Alpha = 1 the window is one slot: no pipeline.          *)
(*                                                                         *)
(* Limits: one fixed grid (a grid change is a round change, and it cuts    *)
(* no segment, R9); no restart and no lost disk; the chain is one map, not *)
(* the matchmakers that hold it (step 4 models the map at a quorum); no    *)
(* `Held`, no floor and no heal (steps 6 and 7).                           *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS Acceptors,  \* three acceptors, majorities
          NS,         \* slots 1..NS
          NB,         \* ballots 1..NB
          NK,         \* segments 1..NK; the `Roll` values are 2..NK
          Reps,       \* Reps[k]: the number of replicas of segment k
          Alpha,      \* a valid `Roll` at slot s ends its segment at s + Alpha
          Barrier, HoleFill, ActOn, Base, Route, Window
ASSUME Alpha \in Nat \ {0}

Slot   == 1..NS
Ballot == 1..NB
Seg    == 1..NK
Val    == 0..NK
Noop   == 0
Cmd    == 1
IsRoll(v) == v >= 2
None   == NK + 1   \* no value: outside Val

Quorum == {Q \in SUBSET Acceptors : 2 * Cardinality(Q) > Cardinality(Acceptors)}

SegReplica == UNION {{<<k, i>> : i \in 1..Reps[k]} : k \in Seg}
SegOf(r) == r[1]

VARIABLES prom,     \* prom[a]: the highest ballot that acceptor a promised
          voted,    \* every vote ever cast: [a, b, s, v]
          prop,     \* every proposal: [b, s, v, k], k the segment it routes to
          forced,   \* forced[b][s]: the value that Phase 1 forces at s, or None
          front,    \* front[b]: one past the highest slot that Phase 1 reported
          low,      \* low[b]: the first slot that ballot b recovers (`lo`), or 0
          eff,      \* eff[b]: the slot where tail[b] starts, while b is below it; or 0
          next,     \* next[b]: the next slot that ballot b proposes at
          tail,     \* tail[b]: the tail segment that ballot b believes (the last one opened)
          pend,     \* pend[b]: the slot of the pending `Roll` of b, or 0
          rstart, rnext, rend,  \* each replica: its start, next slot, end (0: none)
          chain     \* chain[k]: [start, end] (0: none)

vars == <<prom, voted, prop, forced, front, low, eff, next, tail, pend,
          rstart, rnext, rend, chain>>

-----------------------------------------------------------------------------
(* What the log decided. *)

ChosenAt(s, b, v) == \E Q \in Quorum : \A a \in Q :
                        [a |-> a, b |-> b, s |-> s, v |-> v] \in voted
Chosen(s, v)      == \E b \in Ballot : ChosenAt(s, b, v)
IsChosen(s)       == \E v \in Val : Chosen(s, v)
ChosenVal(s)      == CHOOSE v \in Val : Chosen(s, v)
PrefixChosen(s)   == \A t \in Slot : t < s => IsChosen(t)

\* The value of slot s as the fold reads it; a slot that is not chosen is
\* read as a `Noop`, so that the fold is total. Each property that reads
\* SegAt asks Known first.
ValAt(s) == IF IsChosen(s) THEN ChosenVal(s) ELSE Noop

\* No start: above every slot that a start can be.
NoStart == NS + Alpha + 1

\* The first slot of segment k. A `Roll` to k is valid at the first slot at
\* or above the start of k - 1 that holds one: it is in segment k - 1, and
\* segment k starts Alpha slots later. Any other `Roll` changes nothing.
RECURSIVE StartOf(_)
StartOf(k) ==
  IF k = 1 THEN 1
  ELSE LET from == StartOf(k - 1)
           at   == {t \in Slot : t >= from /\ ValAt(t) = k}
       IN  IF at = {} THEN NoStart
           ELSE (CHOOSE t \in at : \A u \in at : t <= u) + Alpha

\* The segment of slot s (s may be above NS).
SegAt(s) == CHOOSE k \in Seg : StartOf(k) <= s /\ \A j \in Seg : j > k => StartOf(j) > s

\* The last segment that a valid `Roll` below slot s opened.
TailAt(s) == CHOOSE k \in Seg : /\ StartOf(k) < s + Alpha
                                 /\ \A j \in Seg : j > k => StartOf(j) >= s + Alpha

\* The segment of slot s is decided: every slot that can hold a `Roll` with
\* an effect at or below s is chosen. With Alpha = 1 it is PrefixChosen(s).
Known(s) == PrefixChosen(IF s > Alpha THEN s - Alpha + 1 ELSE 1)

\* The tail of the chain: the highest segment whose start is published.
ChainTail == CHOOSE k \in Seg : chain[k].start # 0 /\ \A j \in Seg : j > k => chain[j].start = 0

-----------------------------------------------------------------------------
Init ==
  /\ prom = [a \in Acceptors |-> 0]
  /\ voted = {}
  /\ prop = {}
  /\ forced = [b \in Ballot |-> [s \in Slot |-> None]]
  /\ front = [b \in Ballot |-> 0]
  /\ low = [b \in Ballot |-> 0]
  /\ eff = [b \in Ballot |-> 0]
  /\ next = [b \in Ballot |-> 0]
  /\ tail = [b \in Ballot |-> 0]
  /\ pend = [b \in Ballot |-> 0]
  /\ rstart = [r \in SegReplica |-> IF SegOf(r) = 1 THEN 1 ELSE 0]
  /\ rnext = [r \in SegReplica |-> IF SegOf(r) = 1 THEN 1 ELSE 0]
  /\ rend = [r \in SegReplica |-> 0]
  /\ chain = [k \in Seg |-> [start |-> IF k = 1 THEN 1 ELSE 0, end |-> 0]]

\* The vote of the highest ballot that Q reported at slot s, or None.
Report(Q, s) ==
  LET vs == {x \in voted : x.a \in Q /\ x.s = s}
  IN  IF vs = {} THEN None
      ELSE (CHOOSE x \in vs : \A y \in vs : y.b <= x.b).v

\* Ballot b wins its Phase 1 at quorum Q, from slot lo. It wins at most once:
\* each quorum meets Q, whose promises are at b or above for ever.
Phase1(b, Q, lo) ==
  /\ \A a \in Q : prom[a] < b
  /\ PrefixChosen(lo)
  /\ LET f  == [s \in Slot |-> IF s >= lo THEN Report(Q, s) ELSE None]
         hi == {s \in Slot : f[s] # None}
         fr == IF hi = {} THEN lo
               ELSE (CHOOSE s \in hi : \A t \in hi : t <= s) + 1
     IN  /\ forced' = [forced EXCEPT ![b] = f]
         /\ front' = [front EXCEPT ![b] = fr]
  /\ prom' = [a \in Acceptors |-> IF a \in Q THEN b ELSE prom[a]]
  /\ next' = [next EXCEPT ![b] = lo]
  /\ low' = [low EXCEPT ![b] = lo]
  /\ LET k  == IF Base = "prefix" THEN TailAt(lo) ELSE ChainTail
         st == IF Base = "prefix" THEN StartOf(k) ELSE chain[k].start
     IN  /\ tail' = [tail EXCEPT ![b] = k]
         /\ eff' = [eff EXCEPT ![b] = IF st > lo THEN st ELSE 0]
  /\ UNCHANGED <<voted, prop, pend, rstart, rnext, rend, chain>>

\* Ballot b is at a slot below the start of its tail segment: a `Roll` is
\* in its window, and slot s is still in the segment before.
Before(b, s) == eff[b] # 0 /\ s < eff[b]

\* The values that ballot b may propose at slot s. It offers a `Roll` only
\* when its tail segment has started.
Offer(b, s) ==
  IF forced[b][s] # None THEN {forced[b][s]}
  ELSE IF s < front[b] THEN (IF HoleFill = "noop" THEN {Noop} ELSE Val)
  ELSE {Cmd} \cup (IF tail[b] < NK /\ ~Before(b, s) THEN {tail[b] + 1} ELSE {})

\* R4a: nothing above a pending Roll of b until it is chosen at b.
Free(b) == ~Barrier \/ pend[b] = 0 \/ ChosenAt(pend[b], b, tail[b])

\* Ballot b knows that every slot from low[b] to s - 1 is chosen: it has the
\* votes of a quorum at its ballot for each one.
OwnPrefix(b, s) ==
  \A t \in Slot : low[b] <= t /\ t < s =>
     \E p \in prop : p.b = b /\ p.s = t /\ ChosenAt(t, b, p.v)

\* The window: ballot b knows that every slot more than Alpha - 1 below s is
\* chosen, so no `Roll` that is not chosen can change the segment of s.
InWindow(b, s) == OwnPrefix(b, s + 1 - Alpha)

\* A ballot that did not win its Phase 1 has next[b] = 0, outside Slot.
Propose(b) ==
  /\ next[b] \in Slot
  /\ Free(b)
  /\ ~Window \/ InWindow(b, next[b])
  /\ \E v \in Offer(b, next[b]) :
       LET s      == next[b]
           before == Before(b, s)
           valid  == v = tail[b] + 1 /\ ~before
       IN  /\ prop' = prop \cup {[b |-> b, s |-> s, v |-> v,
                                   k |-> IF before THEN tail[b] - 1 ELSE tail[b]]}
           /\ next' = [next EXCEPT ![b] = s + 1]
           /\ tail' = [tail EXCEPT ![b] = IF valid THEN v ELSE tail[b]]
           /\ eff' = [eff EXCEPT ![b] = IF valid /\ Alpha > 1 THEN s + Alpha
                                        ELSE IF Before(b, s + 1) THEN eff[b] ELSE 0]
           /\ pend' = [pend EXCEPT ![b] = IF valid THEN s ELSE pend[b]]
  /\ UNCHANGED <<prom, voted, forced, front, low, rstart, rnext, rend, chain>>

\* A vote cast again changes nothing: it is a stutter.
Accept(a, p) ==
  /\ prom[a] <= p.b
  /\ voted' = voted \cup {[a |-> a, b |-> p.b, s |-> p.s, v |-> p.v]}
  /\ prom' = [prom EXCEPT ![a] = p.b]
  /\ UNCHANGED <<prop, forced, front, low, eff, next, tail, pend, rstart, rnext, rend, chain>>

-----------------------------------------------------------------------------
(* Routing: the `Commit`s that go to the replicas of a segment. *)

\* Every `Commit` that a ballot may have sent: [b, s, v, k], the slot s with
\* the value v goes to the replicas of segment k, the segment that the
\* ballot believed at the proposal. Under Route = "commit" the `Commit`
\* leaves only when the ballot knows that the slots below the window of s
\* are chosen. It is a function of the votes and the proposals, and it only
\* grows, so it is a definition and not a variable.
Sent ==
  {q \in prop : /\ ChosenAt(q.s, q.b, q.v)
                /\ Route = "commit" => InWindow(q.b, q.s)}

\* The values that replica r may apply at slot s.
Learnable(s) ==
  IF ActOn = "chosen" THEN {v \in Val : Chosen(s, v)}
  ELSE {x.v : x \in {y \in voted : y.s = s}}

\* A replica with no start has rnext[r] = 0, and a replica past the last
\* slot has rnext[r] = NS + 1: no value is learnable at either.
Apply(r) ==
  /\ rend[r] = 0 \/ rnext[r] < rend[r]
  /\ \E v \in Learnable(rnext[r]) :
       /\ rnext' = [rnext EXCEPT ![r] = rnext[r] + 1]
       /\ rend' = [rend EXCEPT ![r] = IF rend[r] = 0 /\ v = SegOf(r) + 1
                                     THEN rnext[r] + Alpha ELSE rend[r]]
  /\ UNCHANGED <<prom, voted, prop, forced, front, low, eff, next, tail, pend, rstart, chain>>

\* The write of the chain is a merge into the add-only map (R5): a field
\* that holds a value keeps it. A replica with no end writes nothing.
Publish(r) ==
  /\ chain' = [k \in Seg |->
                 IF k = SegOf(r) /\ chain[k].end = 0
                 THEN [chain[k] EXCEPT !.end = rend[r]]
                 ELSE IF k = SegOf(r) + 1 /\ chain[k].start = 0
                      THEN [chain[k] EXCEPT !.start = rend[r]]
                 ELSE chain[k]]
  /\ UNCHANGED <<prom, voted, prop, forced, front, low, eff, next, tail, pend, rstart, rnext, rend>>

\* A replica starts once; a chain with no start gives it none (a stutter).
Start(r) ==
  /\ rstart[r] = 0
  /\ rstart' = [rstart EXCEPT ![r] = chain[SegOf(r)].start]
  /\ rnext' = [rnext EXCEPT ![r] = chain[SegOf(r)].start]
  /\ UNCHANGED <<prom, voted, prop, forced, front, low, eff, next, tail, pend, rend, chain>>

Next ==
  \/ \E b \in Ballot, Q \in Quorum, lo \in 1..(NS + 1) : Phase1(b, Q, lo)
  \/ \E b \in Ballot : Propose(b)
  \/ \E a \in Acceptors, p \in prop : Accept(a, p)
  \/ \E r \in SegReplica : Apply(r) \/ Publish(r) \/ Start(r)

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Safety. *)

TypeOK ==
  /\ prom \in [Acceptors -> 0..NB]
  /\ next \in [Ballot -> 0..(NS + 1)]
  /\ low \in [Ballot -> 0..(NS + 1)]
  /\ eff \in [Ballot -> 0..(NS + Alpha)]
  /\ tail \in [Ballot -> 0..NK]
  /\ rstart \in [SegReplica -> 0..(NS + Alpha)]
  /\ rnext \in [SegReplica -> 0..(NS + Alpha)]
  /\ rend \in [SegReplica -> 0..(NS + Alpha)]

Agreement == \A s \in Slot : \A v, w \in Val :
                Chosen(s, v) /\ Chosen(s, w) => v = w

\* Two replicas of one segment never see two ends.
OneEnd == \A r1, r2 \in SegReplica :
             SegOf(r1) = SegOf(r2) /\ rend[r1] # 0 /\ rend[r2] # 0
                => rend[r1] = rend[r2]

\* "Only the tail takes appends": a replica applies only slots of its segment.
ReplicaTrue == \A r \in SegReplica :
   /\ rstart[r] # 0 =>
        /\ Known(rstart[r])
        /\ \A t \in Slot : rstart[r] <= t /\ t < rnext[r] => IsChosen(t)
        /\ SegAt(rstart[r]) = SegOf(r)
        /\ (rstart[r] > 1 => SegAt(rstart[r] - 1) = SegOf(r) - 1)
        /\ (rnext[r] > rstart[r] => SegAt(rnext[r] - 1) = SegOf(r))
   /\ rend[r] # 0 => SegAt(rend[r]) = SegOf(r) + 1

\* The chain says what the log decided.
ChainTrue == \A k \in Seg :
   /\ chain[k].start # 0 =>
        /\ Known(chain[k].start)
        /\ SegAt(chain[k].start) = k
        /\ (k > 1 => SegAt(chain[k].start - 1) = k - 1)
   /\ chain[k].end # 0 =>
        /\ Known(chain[k].end)
        /\ SegAt(chain[k].end) = k + 1
        /\ SegAt(chain[k].end - 1) = k

\* A replica keeps its start and never goes back.
ReplicaForward == [][\A r \in SegReplica :
                       rstart[r] # 0 => /\ rstart'[r] = rstart[r]
                                        /\ rnext'[r] >= rnext[r]]_vars

\* The chain is add-only: a start or an end, once written, never changes.
ChainAddOnly == [][\A k \in Seg :
                     /\ chain[k].start # 0 => chain'[k].start = chain[k].start
                     /\ chain[k].end # 0 => chain'[k].end = chain[k].end]_vars

\* Routing: a slot chosen at ballot b went to the replicas of its true
\* segment. The true segment of a slot exists once the prefix below it is
\* chosen.
NoStray == \A m \in Sent : Known(m.s) => m.k = SegAt(m.s)

\* Under Route = "commit", the true segment exists when the `Commit` leaves.
RouteKnown == Route = "commit" => \A m \in Sent : Known(m.s)

-----------------------------------------------------------------------------
(* Reach: each must be violated, so that the safe checks see the case.     *)

NoTwoRolls == ~(chain[1].end # 0 /\ NK >= 3 /\ chain[2].end # 0)

NoRollAgain == ~\E s \in Slot, b1, b2 \in Ballot, v \in Val :
                  IsRoll(v) /\ b1 < b2 /\ ChosenAt(s, b1, v) /\ ChosenAt(s, b2, v)

NoRollLost == ~\E s \in Slot : \E x \in voted : IsRoll(x.v) /\ x.s = s /\ Chosen(s, Noop)

\* A `Commit` goes to the replicas of segment 2.
NoSeg2Commit == ~\E m \in Sent : m.k = 2

\* The belief at the proposal is the true segment of each slot that is chosen
\* at the ballot of the proposal. It is NoStray under Route = "propose". Under
\* Route = "commit" it is a reach: the stale belief exists, and no `Commit`
\* carries it.
NoStaleBelief == \A p \in prop :
                    ChosenAt(p.s, p.b, p.v) /\ Known(p.s) => p.k = SegAt(p.s)

NoSeg2Apply == ~\E r \in SegReplica : SegOf(r) = 2 /\ rstart[r] # 0 /\ rnext[r] > rstart[r]
=============================================================================
