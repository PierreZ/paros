----------------------------- MODULE HandoverGc -----------------------------
(* A small model of the GC floor and the registrations across one handover   *)
(* of a matchmaker set, for #1014 (model of the handover and the GC floor)   *)
(* and #903 (matchmaker safety and liveness), item 4.                        *)
(*                                                                           *)
(* The set M0 is replaced by the set M1. A matchmaker of M0 freezes and      *)
(* reports its floor and its registrations (`Stopped`). The successor state  *)
(* is built from a majority of reports (`close_stop`): the maximum floor and *)
(* the union of the registrations above it. A member of M1 activates with    *)
(* that state and keeps its own floor if it is higher.                       *)
(*                                                                           *)
(* The leader of a ballot b raises the floor of the matchmakers to b         *)
(* (`GcRequest`). Its collector counts the acks. When it counts a majority   *)
(* of the generation that it addressed, the floor b is effective, and the    *)
(* acceptors below it are retirable.                                         *)
(*                                                                           *)
(* Collector : "watermark" = count an ack whose watermark is at or above b,  *)
(*                 applied or refused (`Collector::fold_ack` today).         *)
(*             "applied"   = count only an ack that applied the request.     *)
(* PoolLag   : TRUE  = a leader cannot learn the later set from a refusal    *)
(*                 (a member of M1 is not in its pool yet), thus the refusal *)
(*                 goes to the collector.                                    *)
(*             FALSE = the leader learns the later set and ignores the ack.  *)
(* CloseFloor : "max" = the successor floor is the maximum of the reports.   *)
(*              "one" = it is the floor of one report.                       *)
(* FrozenRegisters : TRUE = a frozen matchmaker still registers a ballot.    *)
(*                                                                           *)
(* Not in the model: the decree that chooses the successor (one choice, in   *)
(* one step), lost matchmaker state, the effective configuration scalar,     *)
(* the size of the successor set, more than one journal.                     *)
EXTENDS Naturals, FiniteSets

CONSTANTS M0, M1, NB, Collector, PoolLag, CloseFloor, FrozenRegisters

Pool    == M0 \cup M1
Ballots == 1..NB
Gens    == {0, 1}
None    == <<>>

VARIABLES gen,       \* matchmaker -> its generation (0 or 1)
          phase,     \* matchmaker -> "active" | "frozen" | "inactive"
          wm,        \* matchmaker -> GC floor
          regs,      \* matchmaker -> set of registered ballots that it keeps
          reports,   \* set of <<matchmaker, floor, registrations>>: the Stopped reports
          chosen,    \* None, or <<floor, registrations>>: the state of M1
          matched,   \* set of <<ballot, generation>>: registered at a majority
          acks,      \* <<ballot, generation>> -> matchmakers that the collector counted
          effective  \* set of <<ballot, generation>>: floors reported as effective

vars == <<gen, phase, wm, regs, reports, chosen, matched, acks, effective>>

Max(S) == CHOOSE x \in S : \A y \in S : y <= x
Up(a, b) == IF a >= b THEN a ELSE b
Set(g) == IF g = 0 THEN M0 ELSE M1
Majorities(S) == {Q \in SUBSET S : 2 * Cardinality(Q) > Cardinality(S)}

\* m serves generation g now.
Serves(m, g) == m \in Set(g) /\ gen[m] = g /\ phase[m] = "active"

Init == /\ gen = [m \in Pool |-> 0]
        /\ phase = [m \in Pool |-> IF m \in M0 THEN "active" ELSE "inactive"]
        /\ wm = [m \in Pool |-> 0]
        /\ regs = [m \in Pool |-> {}]
        /\ reports = {}
        /\ chosen = None
        /\ matched = {}
        /\ acks = [k \in Ballots \X Gens |-> {}]
        /\ effective = {}

\* A candidate registers b at m, which serves g.
Register(b, g, m) ==
    /\ m \in Set(g) /\ gen[m] = g
    /\ phase[m] = "active" \/ (FrozenRegisters /\ phase[m] = "frozen")
    /\ b >= wm[m]
    /\ \A r \in regs[m] : r < b
    /\ regs' = [regs EXCEPT ![m] = @ \cup {b}]
    /\ UNCHANGED <<gen, phase, wm, reports, chosen, matched, acks, effective>>

\* The matchmaking phase of b closes at a majority of generation g. A ballot
\* addresses one generation: a leader that moves to the later set learned it,
\* and that case is `PoolLag = FALSE`.
Match(b, g) ==
    /\ \A h \in Gens : <<b, h>> \notin matched
    /\ \E Q \in Majorities(Set(g)) : \A m \in Q : gen[m] = g /\ b \in regs[m]
    /\ matched' = matched \cup {<<b, g>>}
    /\ UNCHANGED <<gen, phase, wm, regs, reports, chosen, acks, effective>>

\* The leader of b, which addresses generation g, sends its GC request to m.
\* m applies it if it serves g. If not, it refuses and reports its current
\* floor. The collector of the leader folds the ack.
Gc(b, g, m) ==
    /\ <<b, g>> \in matched
    /\ m \in Set(g)
    /\ m \notin acks[<<b, g>>]
    /\ IF Serves(m, g)
       THEN /\ wm' = [wm EXCEPT ![m] = Up(@, b)]
            /\ regs' = [regs EXCEPT ![m] = {r \in @ : r >= Up(wm[m], b)}]
            /\ acks' = [acks EXCEPT ![<<b, g>>] = @ \cup {m}]
       ELSE \* A refusal. `newer`: m is at a later generation. `stopped`: m is
            \* frozen at g. The node drops a `stopped` refusal, and a `newer`
            \* one when it can learn the later set.
            /\ ~ (gen[m] = g /\ phase[m] = "frozen")
            /\ ~ (gen[m] > g /\ ~ PoolLag)
            /\ Collector = "watermark"
            /\ wm[m] >= b
            /\ acks' = [acks EXCEPT ![<<b, g>>] = @ \cup {m}]
            /\ UNCHANGED <<wm, regs>>
    /\ UNCHANGED <<gen, phase, reports, chosen, matched, effective>>

\* The collector reports the floor b as effective at generation g.
Effective(b, g) ==
    /\ <<b, g>> \notin effective
    /\ acks[<<b, g>>] \in Majorities(Set(g))
    /\ effective' = effective \cup {<<b, g>>}
    /\ UNCHANGED <<gen, phase, wm, regs, reports, chosen, matched, acks>>

\* The handover: m freezes at generation 0 and reports what it holds.
Stop(m) ==
    /\ m \in M0 /\ gen[m] = 0 /\ phase[m] = "active"
    /\ phase' = [phase EXCEPT ![m] = "frozen"]
    /\ reports' = reports \cup {<<m, wm[m], regs[m]>>}
    /\ UNCHANGED <<gen, wm, regs, chosen, matched, acks, effective>>

\* The stop quorum closes and the successor state is chosen, once.
Close(Q) ==
    /\ chosen = None
    /\ Q \in Majorities(M0)
    /\ \A m \in Q : \E r \in reports : r[1] = m
    /\ LET R == {r \in reports : r[1] \in Q}
           f == IF CloseFloor = "max" THEN Max({r[2] : r \in R})
                ELSE (CHOOSE r \in R : TRUE)[2]
       IN  chosen' = <<f, {b \in UNION {r[3] : r \in R} : b >= f}>>
    /\ UNCHANGED <<gen, phase, wm, regs, reports, matched, acks, effective>>

\* A member of M1 activates generation 1 with the chosen state. It keeps its
\* own floor if that is higher.
Activate(m) ==
    /\ chosen # None
    /\ m \in M1 /\ gen[m] = 0
    /\ gen' = [gen EXCEPT ![m] = 1]
    /\ phase' = [phase EXCEPT ![m] = "active"]
    /\ wm' = [wm EXCEPT ![m] = Up(@, chosen[1])]
    /\ regs' = [regs EXCEPT ![m] = {b \in chosen[2] : b >= Up(wm[m], chosen[1])}]
    /\ UNCHANGED <<reports, chosen, matched, acks, effective>>

Next ==
    \/ \E b \in Ballots, g \in Gens, m \in Pool : Register(b, g, m) \/ Gc(b, g, m)
    \/ \E b \in Ballots, g \in Gens : Match(b, g) \/ Effective(b, g)
    \/ \E m \in Pool : Stop(m) \/ Activate(m)
    \/ \E Q \in SUBSET M0 : Close(Q)

Spec == Init /\ [][Next]_vars

\* An effective floor stays held: each majority of the generation that
\* reported it, and of each later generation, that can answer a candidate now
\* reports a watermark at or above it.
FloorCarried ==
    \A e \in effective : \A g \in Gens : g >= e[2] =>
        \A Q \in Majorities(Set(g)) :
            (\A m \in Q : Serves(m, g)) => Max({wm[m] : m \in Q}) >= e[1]

\* A registration that completed at generation 0 is in the registry of each
\* member of generation 1, unless the floor of that member passed it.
RegCarried ==
    \A k \in matched : k[2] = 0 =>
        \A m \in M1 : gen[m] = 1 => (k[1] \in regs[m] \/ k[1] < wm[m])

\* Not real invariants. TLC must find a trace for each: the model reaches an
\* effective floor at generation 0 with a full generation 1.
NoHandoverAfterFloor ==
    ~ (/\ \E e \in effective : e[2] = 0
       /\ \A m \in M1 : gen[m] = 1)
=============================================================================
