--------------------------- MODULE SegmentRecords ---------------------------
(* The segment records of a journal across a handover of its matchmaker    *)
(* set, for #1275 (segment records across a matchmaker handover).          *)
(*                                                                         *)
(* In design A (`docs/architecture/02-data-plane.md` section 2.10) the     *)
(* matchmakers of a journal hold its segment records and its matchmaker    *)
(* record (the controller term, the leader uuid). Each change is a         *)
(* compare-and-set on that state. `handover/` checks the floor and the     *)
(* configuration register across a handover; it has no segment records.   *)
(* This model runs the record writes and the handover at the same time.    *)
(*                                                                         *)
(* The record. One segment record and the matchmaker record, as one value  *)
(* <<st, end, term, cfg>>:                                                 *)
(*   st    0 open, 1 sealed, 2 retired, 3 offloaded;                       *)
(*   end   0 before the seal; else the value of the seal compare-and-set:  *)
(*         it stands for the pair that one compare-and-set writes (the end *)
(*         and `next_seq` into the segment record, the leader uuid into   *)
(*         the matchmaker record, PR #1277);                               *)
(*   term  the controller term (#1259);                                    *)
(*   cfg   the count of reshapes that the record took.                     *)
(*                                                                         *)
(* The register. The matchmakers hold the value as a compare-and-set       *)
(* register (`cas-register/`, FoundationDB's `LocalGenerationReg`). The    *)
(* operation with index i uses the generation i. It reads at a majority of *)
(* one matchmaker generation (each member raises its read generation to i *)
(* and answers its value; the caller takes the value of the highest write) *)
(* and checks its condition on that value. If the condition holds, it      *)
(* writes the new value at generation i, one member at a time; a member    *)
(* takes the write only if its read generation is at most i and its write  *)
(* generation is below i. The operation is done when a majority of the     *)
(* generation took it. Every message is fenced by the matchmaker           *)
(* generation, so an operation writes to the generation that it read.      *)
(*                                                                         *)
(* The operations (constant Ops, one kind for each index):                 *)
(*   seal1, seal2  two first leaders of the next configuration race:       *)
(*                 if st = open and end = 0, write st = sealed and end;    *)
(*   retire        if st = sealed, write retired; when the write is done,  *)
(*                 the old acceptors retire;                               *)
(*   offload       if st = retired, write offloaded (the offloader);       *)
(*   drop          a replica that wants to drop its copy: if st =          *)
(*                 offloaded, write the same value again; when the write   *)
(*                 is done, the replica drops its copy;                    *)
(*   elect         a controller candidate: write term + 1;                 *)
(*   reshape1      the controller of term 1: if term = 1, write cfg + 1.   *)
(* A replica also drops its copy when the offload write is done (the       *)
(* offloader tells it).                                                    *)
(*                                                                         *)
(* The handover (`matchmaker/reconfigurer.rs`, `handover/`). The set M0    *)
(* (generation 0) hands over to the set T (generation 1):                  *)
(*   Stop       a member of M0 freezes: it answers nothing at generation 0 *)
(*              after that;                                                *)
(*   Close      a reconfigurer reads a frozen majority of M0 and builds    *)
(*              the state of the successor: the register of the highest    *)
(*              write generation and the highest read generation;          *)
(*   Bootstrap  a member of T stores that state;                           *)
(*   Decide     the decree over M0 chooses one state that every member of  *)
(*              T stored (the rule of `handover/`);                        *)
(*   Activate   a member of T serves generation 1; it keeps its own        *)
(*              register and read generation if they are higher.           *)
(*                                                                         *)
(* Constants select the rule. The first value is the rule.                 *)
(* CloseFrom    : "max" = the successor takes the highest write of the     *)
(*                 stop quorum (and the highest read generation).          *)
(*                "one" = a control: it copies one member of the quorum.   *)
(* FrozenServes : FALSE = a frozen member takes nothing (the rule).        *)
(*                TRUE  = a control: a frozen member still takes reads and *)
(*                 writes at generation 0, so a compare-and-set lands on   *)
(*                 the old set after the stop.                             *)
(* DropOn       : "write" = a replica drops its copy only after a write of *)
(*                 the offload mark is done at a majority (the offload     *)
(*                 itself, or its own write of the same value).            *)
(*                "read" = a control: it drops after a read that shows the *)
(*                 mark.                                                   *)
(* Again        : "ok" = a change asked again over a value that already    *)
(*                 holds what it writes is ok, and writes the value again  *)
(*                 (a seal of its own end, a retire of a retired record).  *)
(*                 So a caller whose answer was lost, and whose write a    *)
(*                 minority took, asks again and makes its write done.     *)
(*                "refuse" = a control: the change is refused, so a first  *)
(*                 leader stops on a refusal of its own end (check         *)
(*                 `again_refuse`).                                        *)
(* Faults       : the most machines that are down at one time, for good.   *)
(* Start        : the record value before the first operation, so that a   *)
(*                 check can start at a later point of the segment's life. *)
EXTENDS Naturals, FiniteSets, Sequences

CONSTANTS M0, T, Ops, Start, CloseFrom, FrozenServes, DropOn, Again, Faults

Pool  == M0 \cup T
Gens  == {0, 1}
Idx   == 1..Len(Ops)
None  == <<>>

\* The fields of a record value.
St(r) == r[1]
End(r) == r[2]
Term(r) == r[3]
Cfg(r) == r[4]

\* The end that the seal of kind k writes.
SealEnd(k) == IF k = "seal1" THEN 1 ELSE 2

\* The value r already holds what the kind k writes: the change is asked
\* again over its own write, or over a later value that kept it.
Holds(k, r) ==
    CASE k \in {"seal1", "seal2"} -> End(r) = SealEnd(k)
      [] k = "retire"            -> St(r) = 2
      [] OTHER                   -> FALSE

\* The condition of the compare-and-set of the kind k on the value r.
Pre(k, r) ==
    IF Again = "ok" /\ Holds(k, r) THEN TRUE ELSE
    CASE k = "seal1"    -> St(r) = 0 /\ End(r) = 0
      [] k = "seal2"    -> St(r) = 0 /\ End(r) = 0
      [] k = "retire"   -> St(r) = 1
      [] k = "offload"  -> St(r) = 2
      [] k = "drop"     -> St(r) = 3
      [] k = "elect"    -> TRUE
      [] k = "reshape1" -> Term(r) = 1

\* The value that the kind k writes over the value r.
Post(k, r) ==
    IF Holds(k, r) THEN r ELSE
    CASE k = "seal1"    -> <<1, 1, Term(r), Cfg(r)>>
      [] k = "seal2"    -> <<1, 2, Term(r), Cfg(r)>>
      [] k = "retire"   -> <<2, End(r), Term(r), Cfg(r)>>
      [] k = "offload"  -> <<3, End(r), Term(r), Cfg(r)>>
      [] k = "drop"     -> r
      [] k = "elect"    -> <<St(r), End(r), Term(r) + 1, Cfg(r)>>
      [] k = "reshape1" -> <<St(r), End(r), Term(r), Cfg(r) + 1>>

\* r is at or after s: the state, the term and the reshapes never go back,
\* and a seal end, once written, stays the same.
AtOrAfter(r, s) ==
    /\ St(r) >= St(s)
    /\ End(s) # 0 => End(r) = End(s)
    /\ Term(r) >= Term(s)
    /\ Cfg(r) >= Cfg(s)

VARIABLES gen,    \* machine -> its matchmaker generation (0 or 1)
          phase,  \* machine -> "active" | "frozen" | "inactive"
          up,     \* machine -> FALSE once it is down for good
          rg,     \* machine -> read generation of the register
          reg,    \* machine -> <<write generation, value>>
          ost,    \* operation -> "idle" | "read" | "done" | "refused"
          ogen,   \* operation -> the matchmaker generation it read
          oseen,  \* operation -> the value its read returned
          acked,  \* operation -> the members that took its write
          props,  \* set of successor states <<register, read generation>>
          boot,   \* set of <<machine, successor state>>: bootstraps stored
          chosen, \* None, or the successor state that the decree chose
          retired,\* TRUE once the old acceptors retired
          dropped \* TRUE once a replica dropped its copy

vars == <<gen, phase, up, rg, reg, ost, ogen, oseen, acked, props, boot,
          chosen, retired, dropped>>

Max(S) == CHOOSE x \in S : \A y \in S : y <= x
Up(a, b) == IF a >= b THEN a ELSE b
Majorities(S) == {Q \in SUBSET S : 2 * Cardinality(Q) > Cardinality(S)}

\* The members of generation g.
Set(g) == IF g = 0 THEN M0 ELSE IF chosen = None THEN {} ELSE T

Serves(m, g) ==
    /\ m \in Set(g) /\ gen[m] = g /\ up[m]
    /\ phase[m] = "active" \/ (FrozenServes /\ phase[m] = "frozen")

\* A majority of generation g where every member serves.
Serving(g) == {Q \in Majorities(Set(g)) : \A m \in Q : Serves(m, g)}

\* The register of the highest write in Q.
TopReg(Q) == LET w == Max({reg[m][1] : m \in Q})
             IN  reg[CHOOSE m \in Q : reg[m][1] = w]

\* The value that a read at Q returns.
TopVal(Q) == TopReg(Q)[2]

\* The value that the done operation i wrote.
Wrote(i) == Post(Ops[i], oseen[i])

Down == {m \in Pool : ~ up[m]}

Init == /\ gen = [m \in Pool |-> 0]
        /\ phase = [m \in Pool |-> IF m \in M0 THEN "active" ELSE "inactive"]
        /\ up = [m \in Pool |-> TRUE]
        /\ rg = [m \in Pool |-> 0]
        /\ reg = [m \in Pool |-> <<0, Start>>]
        /\ ost = [i \in Idx |-> "idle"]
        /\ ogen = [i \in Idx |-> 0]
        /\ oseen = [i \in Idx |-> Start]
        /\ acked = [i \in Idx |-> {}]
        /\ props = {}
        /\ boot = {}
        /\ chosen = None
        /\ retired = FALSE
        /\ dropped = FALSE

-----------------------------------------------------------------------------
(* The record writes. *)

\* The operation i reads at the serving majority Q of generation g. A
\* refused condition ends it.
Read(i, g, Q) ==
    /\ ost[i] = "idle"
    /\ Q \in Serving(g)
    /\ rg' = [m \in Pool |-> IF m \in Q THEN Up(rg[m], i) ELSE rg[m]]
    /\ ogen' = [ogen EXCEPT ![i] = g]
    /\ oseen' = [oseen EXCEPT ![i] = TopVal(Q)]
    /\ ost' = [ost EXCEPT ![i] = IF Pre(Ops[i], TopVal(Q)) THEN "read"
                                                          ELSE "refused"]
    /\ UNCHANGED <<gen, phase, up, reg, acked, props, boot, chosen, retired,
                   dropped>>

\* The member m takes the write of the operation i. The write is done when
\* a majority of the generation took it; members can still take it later.
Write(i, m) ==
    /\ ost[i] \in {"read", "done"}
    /\ m \notin acked[i]
    /\ Serves(m, ogen[i])
    /\ rg[m] <= i /\ reg[m][1] < i
    /\ reg' = [reg EXCEPT ![m] = <<i, Wrote(i)>>]
    /\ acked' = [acked EXCEPT ![i] = @ \cup {m}]
    /\ ost' = [ost EXCEPT ![i] =
                 IF acked'[i] \in Majorities(Set(ogen[i])) THEN "done" ELSE @]
    /\ UNCHANGED <<gen, phase, up, rg, ogen, oseen, props, boot, chosen,
                   retired, dropped>>

\* The old acceptors retire once a retire write is done.
Retire ==
    /\ ~ retired
    /\ \E i \in Idx : Ops[i] = "retire" /\ ost[i] = "done"
    /\ retired' = TRUE
    /\ UNCHANGED <<gen, phase, up, rg, reg, ost, ogen, oseen, acked, props,
                   boot, chosen, dropped>>

\* A replica drops its copy of the segment.
Drop ==
    /\ \E i \in Idx :
         /\ Ops[i] \in {"offload", "drop"}
         /\ IF DropOn = "write" THEN ost[i] = "done"
            ELSE \/ ost[i] \in {"read", "done"} /\ St(oseen[i]) = 3
                 \/ ost[i] = "done"
    /\ dropped' = TRUE
    /\ UNCHANGED <<gen, phase, up, rg, reg, ost, ogen, oseen, acked, props,
                   boot, chosen, retired>>

-----------------------------------------------------------------------------
(* The handover. *)

Stop(m) ==
    /\ m \in M0 /\ gen[m] = 0 /\ up[m] /\ phase[m] = "active"
    /\ phase' = [phase EXCEPT ![m] = "frozen"]
    /\ UNCHANGED <<gen, up, rg, reg, ost, ogen, oseen, acked, props, boot,
                   chosen, retired, dropped>>

\* The successor state that the frozen majority Q gives.
Close(Q) ==
    /\ chosen = None
    /\ Q \in Majorities(M0)
    /\ \A m \in Q : phase[m] = "frozen" /\ up[m]
    /\ \E s \in IF CloseFrom = "max"
                THEN {<<TopReg(Q), Max({rg[m] : m \in Q})>>}
                ELSE {<<reg[m], rg[m]>> : m \in Q} :
         props' = props \cup {s}
    /\ UNCHANGED <<gen, phase, up, rg, reg, ost, ogen, oseen, acked, boot,
                   chosen, retired, dropped>>

\* A member m of T (Next takes m in T and s in props) records that it holds
\* s. The model lets a down member and an active one record it too: no
\* check here is a liveness check, a record only lets Decide run, and after
\* the decree no step reads boot.
Bootstrap(m, s) ==
    /\ <<m, s>> \notin boot
    /\ boot' = boot \cup {<<m, s>>}
    /\ UNCHANGED <<gen, phase, up, rg, reg, ost, ogen, oseen, acked, props,
                   chosen, retired, dropped>>

\* The decree over M0 chooses s; it needs a live majority of M0.
Decide(s) ==
    /\ chosen = None
    /\ \A m \in T : <<m, s>> \in boot
    /\ \E Q \in Majorities(M0) : \A m \in Q : up[m]
    /\ chosen' = s
    /\ UNCHANGED <<gen, phase, up, rg, reg, ost, ogen, oseen, acked, props,
                   boot, retired, dropped>>

Activate(m) ==
    /\ chosen # None
    /\ m \in T /\ up[m] /\ gen[m] = 0
    /\ gen' = [gen EXCEPT ![m] = 1]
    /\ phase' = [phase EXCEPT ![m] = "active"]
    /\ reg' = [reg EXCEPT ![m] = IF @[1] > chosen[1][1] THEN @ ELSE chosen[1]]
    /\ rg' = [rg EXCEPT ![m] = Up(@, chosen[2])]
    /\ UNCHANGED <<up, ost, ogen, oseen, acked, props, boot, chosen, retired,
                   dropped>>

Crash(m) ==
    /\ up[m]
    /\ Cardinality(Down) < Faults
    /\ up' = [up EXCEPT ![m] = FALSE]
    /\ UNCHANGED <<gen, phase, rg, reg, ost, ogen, oseen, acked, props, boot,
                   chosen, retired, dropped>>

-----------------------------------------------------------------------------

Next ==
    \/ \E i \in Idx, g \in Gens, Q \in SUBSET Pool : Read(i, g, Q)
    \/ \E i \in Idx, m \in Pool : Write(i, m)
    \/ Retire \/ Drop
    \/ \E m \in M0 : Stop(m)
    \/ \E Q \in SUBSET M0 : Close(Q)
    \/ \E m \in T, s \in props : Bootstrap(m, s)
    \/ \E s \in props : Decide(s)
    \/ \E m \in T : Activate(m)
    \/ \E m \in Pool : Crash(m)

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* The properties. *)

Done == {i \in Idx : ost[i] = "done"}

TypeOK ==
    /\ gen \in [Pool -> Gens]
    /\ phase \in [Pool -> {"active", "frozen", "inactive"}]
    /\ ost \in [Idx -> {"idle", "read", "done", "refused"}]
    /\ \A m \in Pool : reg[m][1] \in {0} \cup Idx

\* No write that a majority took is lost: each serving majority of its
\* generation and of each later one returns it, or a later value.
Carried ==
    \A i \in Done : \A g \in Gens : g >= ogen[i] =>
        \A Q \in Serving(g) :
            /\ TopReg(Q)[1] >= i
            /\ AtOrAfter(TopVal(Q), Wrote(i))

\* No record moves back: the done writes, in the order of their
\* generations, are each at or after the one before.
NoBackward ==
    \A i, j \in Done : i < j => AtOrAfter(Wrote(j), Wrote(i))

\* The seal end is written once: every done seal wrote the same end, and
\* every serving majority returns that end or none.
SealOnce ==
    \A i \in Done : End(Wrote(i)) # 0 =>
        \A g \in Gens : \A Q \in Serving(g) :
            End(TopVal(Q)) \in {0, End(Wrote(i))}

\* A first leader stops on a refused seal (`JournalLeader::refused`). It
\* stops only on a read that holds another end, never on its own: a seal
\* asked again after a lost answer finds its own write at a minority.
SealRefusedOnOther ==
    \A i \in Idx :
        Ops[i] \in {"seal1", "seal2"} /\ ost[i] = "refused" =>
            End(oseen[i]) # SealEnd(Ops[i])

\* A leader asks its retire again after a lost answer (`JournalLeader`'s
\* tick): a refused retire read a record that is not retired.
RetireRefusedOnOther ==
    \A i \in Idx :
        Ops[i] = "retire" /\ ost[i] = "refused" => St(oseen[i]) # 2

\* A deposed controller changes nothing: a reshape of term 1 is done only
\* if no election that comes before it raised the term above 1.
ReshapeFenced ==
    \A i, j \in Done :
        Ops[i] = "reshape1" /\ Ops[j] = "elect" /\ j < i => Term(Wrote(j)) <= 1

\* The old acceptors retire only on a record that stays retired, or later.
RetireKept ==
    retired => \A g \in Gens : \A Q \in Serving(g) : St(TopVal(Q)) >= 2

\* A replica drops its copy only when every serving majority, of the old
\* set and of the successor, holds the offload mark.
DropKept ==
    dropped => \A g \in Gens : \A Q \in Serving(g) : St(TopVal(Q)) = 3

\* Not a real invariant. TLC must find a trace: a seal asked again reads its
\* own end and is done.
NoAgain ==
    ~ \E i \in Idx :
        /\ Ops[i] \in {"seal1", "seal2"} /\ ost[i] = "done"
        /\ End(oseen[i]) = SealEnd(Ops[i])

\* Not a real invariant. TLC must find a trace: a write is done at each
\* generation, and then the old acceptors retire or a replica drops its copy.
NoCrossRun ==
    ~ (/\ \E i \in Done : ogen[i] = 0
       /\ \E i \in Done : ogen[i] = 1
       /\ retired \/ dropped)
=============================================================================
