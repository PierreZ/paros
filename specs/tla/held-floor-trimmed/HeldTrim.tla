------------------------------ MODULE HeldTrim --------------------------------
(***************************************************************************)
(* A replica of a segment that a `Truncate` released, and that lags below  *)
(* the floor of the acceptors (step 6 of the segments in the core, R6 and  *)
(* R7 of `docs/architecture/04-7-segments.md`). The acceptors compact past *)
(* the `Roll` of a released segment, and drop its records at its `Hold`,   *)
(* with no report of its replicas (`held-floor-released/`). A replica of   *)
(* that segment that did not copy all of it then gets only trim points of  *)
(* a later segment (`TrimmedTo`), and keeps one segment: it can never copy *)
(* its segment again from the acceptors.                                   *)
(*                                                                         *)
(* This module extends `HeldFloor.tla` with one step, `TrimReplica`: the   *)
(* replica reads what a `TrimmedTo` carries (the journal state sealed at   *)
(* the floor of the sender, and the `first_seq` at the head of its fold)   *)
(* and marks its segment `Trimmed`: it keeps nothing more of it, pulls     *)
(* nothing and serves no read of it. The model drops what it holds of it,  *)
(* the stronger case: the code keeps those records unread.                 *)
(*                                                                         *)
(* Constant (with a control):                                              *)
(*   TrimBy "released": the jump proves that every write of the segment is *)
(*          below `first_seq`: the state at the floor `f` of the sender is *)
(*          in a later segment, and either the head `first_seq` reaches    *)
(*          `next_seq` at `f` (every write below `f` is released), or the  *)
(*          state holds a `Hold` of the segment or of a later one whose    *)
(*          `next_seq` is at or below the head `first_seq`.                *)
(*          "segment": a control, a state in a later segment is enough, so *)
(*          a replica of a segment that its peers hold (and that no        *)
(*          `Truncate` released) drops what it holds of it.                *)
(***************************************************************************)
EXTENDS HeldFloor

CONSTANT TrimBy
ASSUME TrimBy \in {"released", "segment"}

VARIABLE rtrim \* the pairs <<replica, segment>> marked `Trimmed`

TrimTypeOK == HeldTypeOK /\ rtrim \subseteq (Replicas \X Segs)

TrimInit == HeldInit /\ rtrim = {}

\* The `next_seq` of the journal state at the end of segment h, which its
\* `Hold` carries (`Hold::next_seq`).
HoldSeq(h) == Pos(LogEnd(h))

\* A `TrimmedTo` of acceptor p carries the state that p seals at its floor
\* f (with every slot below f chosen, `Pos(f)` is its `next_seq` and
\* `SegAt(f)` its segment) and the `first_seq` at the head h of the fold of
\* p (`FirstSeq(h - 1)`): the state at f can predate the `Truncate` that
\* raised f. Together they prove that segment k is released.
TrimProof(p, k) ==
  LET f == floor[p]
      h == FirstUnchosen(p)
      first == FirstSeq(h - 1)
  IN /\ f > 0
     /\ PrefixChosen(f)
     /\ SegAt(f) > k
     /\ TrimBy = "released" =>
          \/ first >= Pos(f)
          \/ \E u \in 0..(f - 1) :
               /\ ChosenVal(u) \in HoldCmds
               /\ HoldOf[ChosenVal(u)] >= k
               /\ LogEnd(HoldOf[ChosenVal(u)]) > 0
               /\ first >= HoldSeq(HoldOf[ChosenVal(u)])

\* Replica r of segment k, which does not hold all of it, takes the state of
\* a `TrimmedTo` of acceptor p as the proof that its segment is released,
\* marks it `Trimmed` and drops what it holds of it.
TrimReplica(r, k, p) ==
  /\ r \in RepOf[k]
  /\ <<r, k>> \notin rtrim
  /\ ~(LogEnd(k) > 0 /\ SegSlots(k, LogEnd(k)) \subseteq rhold[r])
  /\ TrimProof(p, k)
  /\ rtrim' = rtrim \cup {<<r, k>>}
  /\ rhold' = [rhold EXCEPT ![r] = {s \in @ : SegAt(s) # k}]
  /\ UNCHANGED <<vars, chain, held, rlost, answers, past, reported>>

TrimNext ==
  \/ HeldStep /\ UNCHANGED rtrim
  \/ \E r \in Replicas, k \in Segs, p \in Acceptors : TrimReplica(r, k, p)

TrimSpec == TrimInit /\ [][TrimNext]_<<hvars, past, reported, rtrim>>

\* A `Trimmed` segment is released: no write of it is above `first_seq`, so
\* a replica that keeps nothing of it loses no record and answers each read
\* of it as a `Truncate` would.
TrimmedReleased == \A x \in rtrim : SegReleased(x[2])

\* Reachability control: a replica marks its segment `Trimmed`.
NoTrimmed == rtrim = {}
=============================================================================
