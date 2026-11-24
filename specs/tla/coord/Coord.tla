------------------------------- MODULE Coord --------------------------------
(***************************************************************************)
(* The coordination store over a journal: the election library of #1420   *)
(* (paros-election), on the ideas of the ZooKeeper client and of Apache    *)
(* Curator (docs/architecture/04-roles.md §4.4, "The coordination store"). *)
(*                                                                         *)
(* "The log is the server." The library owns one journal in multi-writer   *)
(* mode (docs/journal/02-data-plane.md §J2.4). The journal arbitrates      *)
(* nothing: every member appends, and a copy of a record can land two      *)
(* times (at-least-once). Every member runs the same fold over the log: a  *)
(* table of sessions, the nodes of one latch, and the holder of the latch. *)
(* The fold judges each record at its position, so every member computes   *)
(* the same result at the same position.                                   *)
(*                                                                         *)
(* The records:                                                            *)
(*   open    a session opens (the member draws the session id: fresh),  *)
(*           and in the same transaction (a ZooKeeper multi) it creates    *)
(*           its ephemeral-sequential node under the latch. The node's     *)
(*           name is the position of the record.                           *)
(*   hb      a heartbeat of a session.                                     *)
(*   leave   the session deletes its node (Curator's LeaderSelector gives  *)
(*           up its turn); the session stays live.                         *)
(*   join    the session creates its node again, at the new position.      *)
(*   act     a transaction guarded by "my session holds the latch at term  *)
(*           t". It stands for every write that a holder makes into the    *)
(*           store.                                                        *)
(*   revoke  any member deletes the node of session s at position `at`    *)
(*           (ZooKeeper lets any session delete a node; Curator's          *)
(*           revocation). The session stays live: the holder is deposed    *)
(*           while it lives.                                               *)
(*   expire  any member that saw no record of session s since position    *)
(*           `last`, for a timeout on its own clock, sends it. It is a     *)
(*           compare-and-set on the position of the session's last record. *)
(*   ckpt    a copy of the whole state of the fold, taken by a member at   *)
(*           its read position `at`. A truncate goes only to a checkpoint  *)
(*           that the fold took.                                           *)
(* Each record of a session carries <<session, op_seq>>; a member sends    *)
(* one at a time, and it settles a record only from its own fold          *)
(* (SettledTrue).                                                          *)
(*                                                                         *)
(* The holder of the latch is the live session whose node has the lowest   *)
(* position. The term is the position of the record at which the holder   *)
(* changed: unique, and it grows. A holder sends orders out of the journal *)
(* with its term; the receiver keeps the highest term it saw on disk and   *)
(* refuses a lower one (the takeover rules of docs/architecture/           *)
(* 04-roles.md §4.4, unchanged).                                           *)
(*                                                                         *)
(* The faults: a copy of a record lands two times (a resend after a lost   *)
(* answer, or the network), a member stops and comes back with no memory  *)
(* (its session is lost; its late copies still land), a member reads a    *)
(* prefix that lags, an early expiry deposes a live holder (Eager: a       *)
(* member may expire any session it sees at any time; that costs           *)
(* availability, never safety).                                            *)
(*                                                                         *)
(* The rules are constants; TRUE is the rule. Each control sets one to     *)
(* FALSE and must break a property:                                        *)
(*   Dedup = FALSE       the fold applies every copy: OnceEach.            *)
(*   VoidDead = FALSE    the fold forgets an expired session and applies   *)
(*                       its late copies: EphemeralsEnd.                   *)
(*   Guard = FALSE       the fold applies an act whatever its guard:       *)
(*                       ActsByHolder.                                     *)
(*   Fence = FALSE       the receiver takes an order of any term:          *)
(*                       NoRegress.                                        *)
(*   ExpireCas = FALSE   an expire ignores `last`: ExpireOnSilence.        *)
(*   CkptFresh = FALSE   the fold takes a checkpoint even when records     *)
(*                       landed between its read position and its own      *)
(*                       position: ViewTrue.                               *)
(*   TrimToCkpt = FALSE  a truncate may go to a checkpoint that the fold   *)
(*                       did not take: ViewTrue.                           *)
(*   Tomb = FALSE        the fold keeps no tombstone of an expired          *)
(*                       session, and a late copy of its `open` brings it  *)
(*                       back: OnceEach.                                   *)
(* "Truncate to a checkpoint" alone loses the records that land between   *)
(* the read position of the writer of a checkpoint and the checkpoint      *)
(* itself: hence CkptFresh.                                                *)
(*                                                                         *)
(* What each property is. OnceEach, EphemeralsEnd, ViewTrue, SettledTrue  *)
(* and TermStep follow from the rules. ActsByHolder and NoRegress state    *)
(* the guard and the fence: their controls show that the fold and the      *)
(* receiver carry them out. ExpireCas is an availability rule (a session   *)
(* that keeps writing is not killed by a stale expiry): with Eager, any    *)
(* expiry is safe, and ExpireOnSilence only shows that the fold carries    *)
(* the rule out.                                                           *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS Members,
          MaxSess,      \* the session ids that members may draw: 1..MaxSess
          MaxOps,       \* the op_seq of a session goes up to MaxOps
          MaxDup, MaxCrash, MaxExpire, MaxRevoke, MaxOrders, MaxCkpt, MaxTrim,
          Kinds,        \* the kinds of record a session may send: a subset of OpKinds
          Lag,          \* TRUE: a member may read a prefix that lags
          Dedup, VoidDead, Tomb, Guard, Fence, ExpireCas, CkptFresh, TrimToCkpt,
          Ev            \* for the reach checks: the event that must not happen

Sess == 1..MaxSess
OpKinds == {"hb", "leave", "join", "act"}
NoSess == 0

\* The state of the fold. The fields after `term` are for the checks only
\* (the library keeps `ck` too: the checkpoints a truncate may go to).
Init0 == [live |-> {}, dead |-> {},
          last |-> [s \in Sess |-> 0], opn |-> [s \in Sess |-> 0],
          nodes |-> {}, holder |-> NoSess, term |-> 0,
          ck |-> {}, done |-> {},
          badAct |-> FALSE, badOnce |-> FALSE, badExp |-> FALSE,
          badTerm |-> FALSE]

\* Every record has one shape, so that TLC can compare any two.
Rec(k, s, n, t, last, at, st) ==
  [k |-> k, s |-> s, n |-> n, t |-> t, last |-> last, at |-> at, st |-> st]

VARIABLES full,      \* the log: every record that landed, at its position
          first,     \* the floor: the position of the first kept record
          pend,      \* the copies of records in flight to the journal
          mst,       \* "none" (no session), "wait" (a record in flight), "idle"
          msess,     \* the member's session
          mseq,      \* the op_seq of its next record
          mop,       \* the record in flight
          mread,     \* the position up to which it folded
          mfrom,     \* where its fold starts: 0 is the start of the log
          onet,      \* the orders in flight to the receiver
          seen,      \* the receiver: the highest term it saw, on disk
          nsess,     \* the session ids drawn so far
          dups, crashes, expires, revokes, orders, ckpts, trims,
          senders,   \* the <<term, session>> pairs of every order sent
          regress,   \* the receiver applied an order below one it applied
          reached    \* the event Ev happened

member == <<mst, msess, mseq, mop, mread, mfrom>>
budget == <<nsess, dups, crashes, expires, revokes, orders, ckpts,
            trims>>
vars == <<full, first, pend, member, onet, seen, budget, senders, regress,
          reached>>

-----------------------------------------------------------------------------
\* The holder: the live session whose node has the lowest position.
HolderOf(nodes, live) ==
  LET mine == {nd \in nodes : nd[1] \in live}
  IN IF mine = {} THEN NoSess
     ELSE (CHOOSE nd \in mine : \A o \in mine : nd[2] <= o[2])[1]

\* The holder after a step; the term is the position where it changed.
Rehold(f, p) ==
  LET h == HolderOf(f.nodes, f.live)
  IN IF h = f.holder THEN f
     ELSE [f EXCEPT !.holder = h, !.term = p, !.badTerm = @ \/ p <= f.term]

\* An expiry: the session is gone, and so are its nodes and its row of the
\* dedup table.
Kill(f, s, r) ==
  [f EXCEPT !.live = @ \ {s}, !.dead = @ \cup {s},
            !.nodes = {nd \in @ : nd[1] # s},
            !.opn[s] = 0,
            !.badExp = @ \/ f.last[s] # r.last]

\* The effect of an op of a live session that is not a duplicate.
Effect(f, r, p) ==
  LET g == [f EXCEPT !.last[r.s] = p, !.opn[r.s] = r.n,
                     !.done = @ \cup {<<r.s, r.n>>},
                     !.badOnce = @ \/ <<r.s, r.n>> \in f.done]
  IN CASE r.k = "hb"   -> g
       [] r.k = "leave" -> [g EXCEPT !.nodes = {nd \in @ : nd[1] # r.s}]
       [] r.k = "join" ->
            IF \E nd \in f.nodes : nd[1] = r.s THEN g
            ELSE [g EXCEPT !.nodes = @ \cup {<<r.s, p>>}]
       [] r.k = "act"  ->
            IF Guard /\ ~(f.holder = r.s /\ f.term = r.t)
            THEN g          \* void: it counts as done, it does nothing
            ELSE [g EXCEPT !.badAct = @ \/ f.holder # r.s \/ f.term # r.t]

\* The fold judges record r at position p.
Apply(f, r, p) ==
  Rehold(
    CASE r.k = "ckpt" ->
           IF ~CkptFresh \/ r.at = p - 1 THEN [f EXCEPT !.ck = @ \cup {p}]
           ELSE f
      [] r.k = "revoke" -> [f EXCEPT !.nodes = @ \ {<<r.s, r.at>>}]
      [] r.k = "expire" ->
           IF r.s \in f.live /\ (~ExpireCas \/ f.last[r.s] = r.last)
           THEN Kill(f, r.s, r) ELSE f
      [] r.k = "open" ->
           IF r.s \notin f.live /\ (~Tomb \/ r.s \notin f.dead)
           THEN [f EXCEPT !.live = @ \cup {r.s}, !.last[r.s] = p,
                          !.opn[r.s] = 0, !.nodes = @ \cup {<<r.s, p>>}]
           ELSE f
      [] OTHER ->
           IF r.s \notin f.live /\ (VoidDead \/ r.s \notin f.dead) THEN f
           ELSE IF Dedup /\ r.n <= f.opn[r.s] THEN f
           ELSE Effect(f, r, p),
    p)

\* The fold of the records from..to of the log, from the state s0.
FoldFrom(s0, from, to) ==
  LET F[i \in (from - 1)..to] ==
        IF i = from - 1 THEN s0 ELSE Apply(F[i - 1], full[i], i)
  IN F[to]

\* The fold of the whole prefix 1..to: the truth.
TrueFold(to) == FoldFrom(Init0, 1, to)
Cur == TrueFold(Len(full))

\* What a member computes: from the start, or from the floor it jumped to.
\* A floor that is a checkpoint gives the state before it; any other floor
\* gives nothing.
View(m) ==
  IF mfrom[m] = 0 THEN TrueFold(mread[m])
  ELSE LET s0 == IF full[mfrom[m]].k = "ckpt" THEN full[mfrom[m]].st
                 ELSE Init0
       IN FoldFrom(s0, mfrom[m], mread[m])

Sem(f) == <<f.live, f.dead, f.last, f.opn, f.nodes, f.holder, f.term>>

Hit(e) == reached' = (reached \/ e = Ev)

-----------------------------------------------------------------------------
Init ==
  /\ full = <<>> /\ first = 1 /\ pend = {}
  /\ mst = [m \in Members |-> "none"] /\ msess = [m \in Members |-> NoSess]
  /\ mseq = [m \in Members |-> 0]
  /\ mop = [m \in Members |-> Rec("none", NoSess, 0, 0, 0, 0, Init0)]
  /\ mread = [m \in Members |-> 0] /\ mfrom = [m \in Members |-> 0]
  /\ onet = {} /\ seen = 0
  /\ nsess = 0 /\ dups = 0 /\ crashes = 0 /\ expires = 0
  /\ revokes = 0
  /\ orders = 0 /\ ckpts = 0 /\ trims = 0
  /\ senders = {} /\ regress = FALSE /\ reached = FALSE

\* A member with no session opens one with a fresh id.
Open(m) ==
  /\ mst[m] = "none" /\ nsess < MaxSess
  /\ LET s == nsess + 1
         r == Rec("open", s, 0, 0, 0, 0, Init0)
     IN /\ nsess' = s /\ msess' = [msess EXCEPT ![m] = s]
        /\ mseq' = [mseq EXCEPT ![m] = 1]
        /\ mop' = [mop EXCEPT ![m] = r] /\ pend' = pend \cup {r}
  /\ mst' = [mst EXCEPT ![m] = "wait"]
  /\ UNCHANGED <<full, first, mread, mfrom, onet, seen, dups, crashes,
                 expires, revokes, orders, ckpts, trims, senders, regress, reached>>

\* A member with a session and nothing in flight sends its next record: a
\* heartbeat, a leave or a join of the latch, or an act guarded by the term
\* of its own view.
Send(m) ==
  /\ mst[m] = "idle" /\ mseq[m] <= MaxOps
  /\ \E r \in {Rec(k, msess[m], mseq[m], 0, 0, 0, Init0) :
                 k \in Kinds \ {"act"}}
              \cup (IF "act" \in Kinds /\ View(m).holder = msess[m]
                    THEN {Rec("act", msess[m], mseq[m], View(m).term, 0, 0,
                              Init0)}
                    ELSE {}) :
       /\ pend' = pend \cup {r} /\ mop' = [mop EXCEPT ![m] = r]
  /\ mseq' = [mseq EXCEPT ![m] = @ + 1]
  /\ mst' = [mst EXCEPT ![m] = "wait"]
  /\ UNCHANGED <<full, first, msess, mread, mfrom, onet, seen, budget,
                 senders, regress, reached>>

\* What a member learns from its fold after a read: the fate of its record
\* in flight, or that its session expired (ZooKeeper's SessionExpired).
Learn(m, v) ==
  IF msess[m] # NoSess /\ msess[m] \in v.dead
  THEN /\ mst' = [mst EXCEPT ![m] = "none"]
       /\ msess' = [msess EXCEPT ![m] = NoSess]
  ELSE /\ mst' = [mst EXCEPT ![m] =
                    IF @ = "wait" /\ msess[m] \in v.live
                       /\ (mop[m].k = "open" \/ v.opn[msess[m]] >= mop[m].n)
                    THEN "idle" ELSE @]
       /\ UNCHANGED msess

\* For the reach checks: the event of a record r that lands at position p
\* on the fold f.
Event(f, r, p) ==
  LET g == Apply(f, r, p)
  IN CASE r.k \in OpKinds /\ r.s \in f.dead -> "dead_void"
       [] r.k \in OpKinds /\ r.s \in f.live /\ r.n <= f.opn[r.s] -> "dup_void"
       [] r.k = "act" /\ r.s \in f.live /\ r.n > f.opn[r.s]
          /\ ~(f.holder = r.s /\ f.term = r.t) -> "guard_void"
       [] r.k = "expire" /\ r.s \in f.live /\ f.last[r.s] # r.last
          -> "expire_void"
       [] r.k = "open" /\ r.s \in f.dead -> "tomb_void"
       [] r.k = "ckpt" /\ r.at # p - 1 -> "ckpt_void"
       [] f.holder # NoSess /\ g.holder # NoSess /\ g.holder # f.holder
          -> "new_holder"
       [] OTHER -> "none"

\* A copy lands at the next position. It may land again later: a copy
\* whose answer was lost and that the member sent again, or a copy that the
\* network gave two times.
Land(r) ==
  /\ \/ pend' = pend \ {r} /\ UNCHANGED dups
     \/ dups < MaxDup /\ dups' = dups + 1 /\ UNCHANGED pend
  /\ full' = Append(full, r)
  /\ Hit(Event(Cur, r, Len(full) + 1))
  /\ UNCHANGED <<first, member, onet, seen, nsess, crashes, expires,
                 revokes, orders, ckpts, trims, senders, regress>>

\* A member reads up to a later position and learns from its fold. A
\* member whose next record is below the floor starts again from the floor.
Read(m) ==
  /\ \E c \in (IF Lag THEN (mread[m] + 1)..Len(full)
                ELSE {Len(full)} \ {mread[m]}) :
       IF mread[m] + 1 < first
       THEN LET fr == first
                s0 == IF full[fr].k = "ckpt" THEN full[fr].st ELSE Init0
                to == IF c < fr THEN fr ELSE c
            IN /\ mread' = [mread EXCEPT ![m] = to]
               /\ mfrom' = [mfrom EXCEPT ![m] = fr]
               /\ Learn(m, FoldFrom(s0, fr, to))
               /\ Hit("jump")
       ELSE /\ mread' = [mread EXCEPT ![m] = c]
            /\ UNCHANGED <<mfrom, reached>>
            /\ Learn(m, IF mfrom[m] = 0 THEN TrueFold(c)
                        ELSE FoldFrom(IF full[mfrom[m]].k = "ckpt"
                                      THEN full[mfrom[m]].st ELSE Init0,
                                      mfrom[m], c))
  /\ UNCHANGED <<full, first, pend, mseq, mop, onet, seen, budget, senders,
                 regress>>

\* Any member may expire a session that its fold shows live (Eager).
Expire(m) ==
  /\ expires < MaxExpire
  /\ \E s \in View(m).live :
       pend' = pend \cup {Rec("expire", s, 0, 0, View(m).last[s], 0, Init0)}
  /\ expires' = expires + 1
  /\ UNCHANGED <<full, first, member, onet, seen, nsess, dups, crashes,
                 revokes, orders, ckpts, trims, senders, regress, reached>>

\* Any member may revoke a node that its fold shows.
Revoke(m) ==
  /\ revokes < MaxRevoke
  /\ \E nd \in View(m).nodes :
       pend' = pend \cup {Rec("revoke", nd[1], 0, 0, 0, nd[2], Init0)}
  /\ revokes' = revokes + 1
  /\ UNCHANGED <<full, first, member, onet, seen, nsess, dups, crashes,
                 expires, orders, ckpts, trims, senders, regress, reached>>

\* A member whose fold shows that it holds the latch sends an order with
\* the term of its fold.
Order(m) ==
  /\ orders < MaxOrders /\ msess[m] # NoSess
  /\ View(m).holder = msess[m]
  /\ onet' = onet \cup {[t |-> View(m).term, s |-> msess[m]]}
  /\ senders' = senders \cup {<<View(m).term, msess[m]>>}
  /\ orders' = orders + 1
  /\ UNCHANGED <<full, first, pend, member, seen, nsess, dups, crashes,
                 expires, revokes, ckpts, trims, regress, reached>>

\* The receiver checks the term in the same write as the effect.
Receive(o) ==
  /\ o \in onet /\ onet' = onet \ {o}
  /\ IF Fence /\ o.t < seen
     THEN /\ Hit("refused_order") /\ UNCHANGED <<seen, regress>>
     ELSE /\ regress' = (regress \/ o.t < seen)
          /\ seen' = IF o.t > seen THEN o.t ELSE seen
          /\ UNCHANGED reached
  /\ UNCHANGED <<full, first, pend, member, budget, senders>>

\* A member sends a checkpoint of its fold at its read position.
Ckpt(m) ==
  /\ ckpts < MaxCkpt
  /\ pend' = pend \cup {Rec("ckpt", NoSess, 0, 0, 0, mread[m], View(m))}
  /\ ckpts' = ckpts + 1
  /\ UNCHANGED <<full, first, member, onet, seen, nsess, dups, crashes,
                 expires, revokes, orders, trims, senders, regress, reached>>

\* The library truncates the journal to a checkpoint.
Trim ==
  /\ trims < MaxTrim
  /\ \E q \in (first + 1)..Len(full) :
       /\ IF TrimToCkpt THEN q \in Cur.ck ELSE full[q].k = "ckpt"
       /\ first' = q
  /\ trims' = trims + 1
  /\ UNCHANGED <<full, pend, member, onet, seen, nsess, dups, crashes,
                 expires, revokes, orders, ckpts, senders, regress, reached>>

\* A member stops and comes back with no memory: its session is lost, and
\* its copies in flight still land.
Crash(m) ==
  /\ crashes < MaxCrash /\ mst[m] # "none"
  /\ mst' = [mst EXCEPT ![m] = "none"]
  /\ msess' = [msess EXCEPT ![m] = NoSess]
  /\ mread' = [mread EXCEPT ![m] = 0] /\ mfrom' = [mfrom EXCEPT ![m] = 0]
  /\ crashes' = crashes + 1
  /\ UNCHANGED <<full, first, pend, mseq, mop, onet, seen, nsess,
                 dups, expires, revokes, orders, ckpts, trims, senders, regress,
                 reached>>

Next ==
  \/ \E m \in Members :
       Open(m) \/ Send(m) \/ Read(m)
       \/ Expire(m) \/ Revoke(m) \/ Order(m) \/ Ckpt(m) \/ Crash(m)
  \/ \E r \in pend : Land(r)
  \/ \E o \in onet : Receive(o)
  \/ Trim

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
TypeOK ==
  /\ first \in 1..(Len(full) + 1)
  /\ \A m \in Members :
       /\ mst[m] \in {"none", "wait", "idle"}
       /\ mread[m] \in 0..Len(full)
       /\ mfrom[m] \in 0..Len(full)
  /\ seen \in Nat

\* Each record of a session takes effect at most one time.
OnceEach == ~Cur.badOnce

\* A node of a session exists only while the session is live.
EphemeralsEnd == \A nd \in Cur.nodes : nd[1] \in Cur.live

\* Every act that took effect came from the holder, at its term.
ActsByHolder == ~Cur.badAct

\* An expiry takes effect only while the session wrote nothing since the
\* position that its sender saw.
ExpireOnSilence == ~Cur.badExp

\* The receiver never applies an order below one it applied.
NoRegress == ~regress

\* One session sends the orders of one term.
OneSender == \A x, y \in senders : x[1] = y[1] => x[2] = y[2]

\* Each member's fold gives the true state at its read position, from the
\* start or from the floor it jumped to.
ViewTrue == \A m \in Members : Sem(View(m)) = Sem(TrueFold(mread[m]))

\* A member that settled its record learned the truth: the record took
\* effect (a void record counts too: it is done, with its answer).
SettledTrue ==
  \A m \in Members :
    mst[m] = "idle" /\ mop[m].k \in OpKinds =>
      TrueFold(mread[m]).opn[msess[m]] >= mop[m].n

\* The term changes with each change of holder, and it never goes down.
TermStep == ~Cur.badTerm

\* For the reach checks: the event Ev never happens.
NotReached == ~reached
=============================================================================
