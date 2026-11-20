---------------------------- MODULE HandoverChain ----------------------------
(* A chain of matchmaker set handovers, with clients that reach one member   *)
(* at a time, for #1230 (rules that no check tests). `handover/` checks one  *)
(* handover with the target rules (a named target, the size, a machine      *)
(* down). It has two generations, and each client step reaches a whole      *)
(* quorum at once, so every stop quorum builds the same successor state.    *)
(* This model checks what that leaves out:                                  *)
(*                                                                           *)
(*   - Generation 0 to 1 to 2: the clients act at each generation, and a    *)
(*     member can skip a generation when it activates (it takes the         *)
(*     bootstrap of the later one, `matchmaker/generation.rs`).             *)
(*   - A client step reaches one member at a time and keeps the set of the  *)
(*     members that acked. A read, a write or a floor can stop at a         *)
(*     minority, so two stop quorums can build two successor states.        *)
(*   - A member keeps the first bootstrap of a generation that reaches it,  *)
(*     and the decree chooses the set only (`reconfigurer.rs`: the decree   *)
(*     value is the member list). So the members of one chosen set can     *)
(*     activate with different states.                                      *)
(*   - The two fences of the state transfer: a frozen member serves         *)
(*     nothing, and a client tally counts the acks of one generation        *)
(*     (`collector.rs`: the generation check before a reply is counted).    *)
(*                                                                           *)
(* The targets are fixed: generation g + 1 has the set Sets[g + 1]. The     *)
(* target rules, the size and the crashes are `handover/`; they are left    *)
(* out here to keep the chain small. The decree is one step (`decree/`).    *)
(*                                                                           *)
(* The clients: a candidate reads the register at a write number w (each    *)
(* member that acks raises its read generation to w), then writes w with a  *)
(* compare-and-set (a member refuses when it read or holds w or later), and *)
(* the leader raises the GC floor to 1. A client stops asking once its tally*)
(* is complete.                                                             *)
(*                                                                           *)
(* Writes       : the write numbers, each with its read before it.          *)
(* Floor        : TRUE = the leader raises the GC floor too. The floor      *)
(*                  moves as the read generation does (each member keeps    *)
(*                  the maximum), so the long chain leaves it out.          *)
(*                                                                           *)
(* Fence        : TRUE  = a client tally of generation g counts only the    *)
(*                  acks that members gave while they served g (the rule).  *)
(*                FALSE = a control: an ack of a member counts for every    *)
(*                  generation whose set names the member.                  *)
(* FrozenServes : FALSE = a frozen member serves nothing (the rule).        *)
(*                TRUE  = a control: a frozen member still acks.            *)
EXTENDS Integers, FiniteSets, Sequences

CONSTANTS Sets, Writes, Floor, Fence, FrozenServes

NG   == Len(Sets)
Gens == 0..(NG - 1)
\* Sets is a sequence; generation g has the set Sets[g + 1].
Set(g) == Sets[g + 1]
Pool == UNION {Set(g) : g \in Gens}
Ops  == {<<"read", w>> : w \in Writes} \cup {<<"write", w>> : w \in Writes}
        \cup (IF Floor THEN {<<"gc", 1>>} ELSE {})
None == <<>>

ASSUME NG >= 2
ASSUME Writes \subseteq {1, 2}
ASSUME Floor \in BOOLEAN

VARIABLES gen,     \* machine -> the generation it belongs to (-1: none yet)
          phase,   \* machine -> "active" | "frozen"
          wm,      \* machine -> GC floor
          rg,      \* machine -> read generation of the register
          reg,     \* machine -> write number of the register (0: none)
          boot,    \* machine -> generation -> the bootstrap it keeps, or None
          chosen,  \* generation -> TRUE once the decree chose its set
          acks,    \* set of <<op, machine, generation it served>>
          done,    \* set of <<op, generation of the tally>>
          skipped  \* TRUE once a member activated past a generation

vars == <<gen, phase, wm, rg, reg, boot, chosen, acks, done, skipped>>

Max(S) == CHOOSE x \in S : \A y \in S : y <= x
Up(a, b) == IF a >= b THEN a ELSE b
Majorities(S) == {Q \in SUBSET S : 2 * Cardinality(Q) > Cardinality(S)}

\* m serves generation g now.
Serves(m, g) ==
    /\ gen[m] = g /\ m \in Set(g) /\ chosen[g]
    /\ phase[m] = "active" \/ (FrozenServes /\ phase[m] = "frozen")

\* A quorum of generation g where every member serves.
Serving(g) == {Q \in Majorities(Set(g)) : \A m \in Q : Serves(m, g)}

\* The members that a tally of generation g counts for op.
Counted(op, g) ==
    IF Fence THEN {m \in Set(g) : <<op, m, g>> \in acks}
    ELSE {m \in Set(g) : \E h \in Gens : <<op, m, h>> \in acks}

OpDone(op) == \E g \in Gens : <<op, g>> \in done

Init == /\ gen = [m \in Pool |-> IF m \in Set(0) THEN 0 ELSE -1]
        /\ phase = [m \in Pool |-> "active"]
        /\ wm = [m \in Pool |-> 0]
        /\ rg = [m \in Pool |-> 0]
        /\ reg = [m \in Pool |-> 0]
        /\ boot = [m \in Pool |-> [g \in Gens |-> None]]
        /\ chosen = [g \in Gens |-> g = 0]
        /\ acks = {}
        /\ done = {}
        /\ skipped = FALSE

-----------------------------------------------------------------------------
(* The clients. Each step reaches one member. *)

\* The member m acks the op. A member that does not serve answers nothing.
Ack(op, m) ==
    LET g == gen[m] IN
    /\ g >= 0 /\ Serves(m, g)
    /\ ~ OpDone(op)
    /\ <<op, m, g>> \notin acks
    /\ CASE op[1] = "read"  -> /\ rg' = [rg EXCEPT ![m] = Up(@, op[2])]
                               /\ UNCHANGED <<wm, reg>>
         [] op[1] = "write" -> /\ OpDone(<<"read", op[2]>>)
                               /\ rg[m] <= op[2] /\ reg[m] < op[2]
                               /\ reg' = [reg EXCEPT ![m] = op[2]]
                               /\ UNCHANGED <<wm, rg>>
         [] op[1] = "gc"    -> /\ wm' = [wm EXCEPT ![m] = 1]
                               /\ UNCHANGED <<rg, reg>>
    /\ acks' = acks \cup {<<op, m, g>>}
    /\ UNCHANGED <<gen, phase, boot, chosen, done, skipped>>

\* The client's tally of generation g is complete. The client asks no more,
\* so its acks are forgotten: they change nothing after this step.
Complete(op, g) ==
    /\ ~ OpDone(op)
    /\ chosen[g]
    /\ Counted(op, g) \in Majorities(Set(g))
    /\ done' = done \cup {<<op, g>>}
    /\ acks' = {x \in acks : x[1] # op}
    /\ UNCHANGED <<gen, phase, wm, rg, reg, boot, chosen, skipped>>

-----------------------------------------------------------------------------
(* The handovers. The handover of generation g to g + 1 starts once g is    *)
(* chosen, and only one runs at a time.                                     *)

\* m freezes at generation g.
Stop(m) ==
    LET g == gen[m] IN
    /\ g >= 0 /\ g + 1 \in Gens
    /\ chosen[g] /\ ~ chosen[g + 1]
    /\ m \in Set(g) /\ phase[m] = "active"
    /\ phase' = [phase EXCEPT ![m] = "frozen"]
    /\ UNCHANGED <<gen, wm, rg, reg, boot, chosen, acks, done, skipped>>

\* The successor state that a frozen quorum Q gives.
Reconstruct(Q) == <<Max({wm[m] : m \in Q}), Max({rg[m] : m \in Q}),
                    Max({reg[m] : m \in Q})>>

\* A reconfigurer closes the stop of generation g on the frozen quorum Q,
\* and sends the state to the member m of the target. The member keeps the
\* first bootstrap of the generation that reaches it.
Bootstrap(g, Q, m) ==
    /\ g + 1 \in Gens
    /\ chosen[g] /\ ~ chosen[g + 1]
    /\ Q \in Majorities(Set(g))
    /\ \A n \in Q : gen[n] = g /\ phase[n] = "frozen"
    /\ m \in Set(g + 1)
    /\ boot[m][g + 1] = None
    /\ boot' = [boot EXCEPT ![m][g + 1] = Reconstruct(Q)]
    /\ UNCHANGED <<gen, phase, wm, rg, reg, chosen, acks, done, skipped>>

\* The decree over generation g chooses the set of g + 1, once every member
\* of it holds a bootstrap (the first write needs every new member).
Decide(g) ==
    /\ g + 1 \in Gens
    /\ chosen[g] /\ ~ chosen[g + 1]
    /\ \A m \in Set(g + 1) : boot[m][g + 1] # None
    /\ chosen' = [chosen EXCEPT ![g + 1] = TRUE]
    /\ UNCHANGED <<gen, phase, wm, rg, reg, boot, acks, done, skipped>>

\* m activates the chosen generation h with its own bootstrap of h. It can
\* skip a generation. It keeps its own floor, read generation and register
\* if they are higher.
Activate(m, h) ==
    /\ chosen[h] /\ gen[m] < h
    /\ m \in Set(h)
    /\ boot[m][h] # None
    /\ LET s == boot[m][h] IN
       /\ gen' = [gen EXCEPT ![m] = h]
       /\ phase' = [phase EXCEPT ![m] = "active"]
       /\ wm' = [wm EXCEPT ![m] = Up(@, s[1])]
       /\ rg' = [rg EXCEPT ![m] = Up(@, s[2])]
       /\ reg' = [reg EXCEPT ![m] = Up(@, s[3])]
    /\ skipped' = (skipped \/ (gen[m] >= 0 /\ gen[m] < h - 1))
    /\ UNCHANGED <<boot, chosen, acks, done>>

-----------------------------------------------------------------------------

Next ==
    \/ \E op \in Ops, m \in Pool : Ack(op, m)
    \/ \E op \in Ops, g \in Gens : Complete(op, g)
    \/ \E m \in Pool : Stop(m)
    \/ \E g \in Gens, Q \in SUBSET Pool, m \in Pool : Bootstrap(g, Q, m)
    \/ \E g \in Gens : Decide(g)
    \/ \E m \in Pool, h \in Gens : Activate(m, h)

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* The properties. *)

TypeOK ==
    /\ gen \in [Pool -> {-1} \cup Gens]
    /\ phase \in [Pool -> {"active", "frozen"}]
    /\ wm \in [Pool -> {0, 1}]
    /\ rg \in [Pool -> {0} \cup Writes]
    /\ reg \in [Pool -> {0} \cup Writes]
    /\ chosen \in [Gens -> BOOLEAN]
    /\ acks \subseteq Ops \X Pool \X Gens
    /\ done \subseteq Ops \X Gens

\* An effective floor stays held by each serving quorum of the generation
\* that completed it, and of each later generation.
FloorCarried ==
    \A d \in done : d[1][1] = "gc" =>
        \A g \in Gens : g >= d[2] =>
            \A Q \in Serving(g) : Max({wm[m] : m \in Q}) >= 1

\* A completed read stays a fence: no serving quorum of that generation or
\* a later one takes a compare-and-set below it.
ReadCarried ==
    \A d \in done : d[1][1] = "read" =>
        \A g \in Gens : g >= d[2] =>
            \A Q \in Serving(g) : Max({rg[m] : m \in Q}) >= d[1][2]

\* A completed write stays the register: each serving quorum of that
\* generation or a later one reports it or a higher write.
RegisterCarried ==
    \A d \in done : d[1][1] = "write" =>
        \A g \in Gens : g >= d[2] =>
            \A Q \in Serving(g) : Max({reg[m] : m \in Q}) >= d[1][2]

\* Not a real invariant. TLC must find a trace: the members of one chosen
\* set keep two different bootstraps.
OneState ==
    \A g \in Gens : chosen[g] =>
        \A m, n \in Set(g) : boot[m][g] # None /\ boot[n][g] # None =>
            boot[m][g] = boot[n][g]

\* Not a real invariant. TLC must find a trace: the last generation is
\* chosen and serves after each op completed, the write at a generation
\* above 0, and a member skipped a generation.
NoChain ==
    ~ (/\ chosen[NG - 1]
       /\ Serving(NG - 1) # {}
       /\ \A op \in Ops : OpDone(op)
       /\ \E w \in Writes, g \in Gens : g > 0 /\ <<<<"write", w>>, g>> \in done
       /\ skipped)
=============================================================================
