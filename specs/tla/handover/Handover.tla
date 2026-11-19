------------------------------ MODULE Handover ------------------------------
(* The handover of a matchmaker set onto a target that the caller names,     *)
(* for #1097 (rework 1.8). It extends `handover-gc/` (#1014): the floor      *)
(* crosses the handover as there, and the register of `config-register/`     *)
(* (#1095) crosses it too, also while a journal has an open successor. It    *)
(* adds rule 2 of the rework (`docs/architecture/04-roles.md`): sets never   *)
(* shrink.                                                                   *)
(*                                                                           *)
(* The set M0 (generation 0) is replaced by a set that the coordinator       *)
(* names (generation 1). The steps are those of                              *)
(* `matchmaker/reconfigurer.rs`:                                             *)
(*                                                                           *)
(*   Issue      the coordinator names a target. The target has the size     *)
(*              that the control journal records, and only live members.    *)
(*   Stop       a member of M0 freezes and keeps the target durably.        *)
(*   Close      a reconfigurer reads a majority of frozen members and       *)
(*              builds the state of the successor: the maximum floor, the   *)
(*              maximum read generation and the register of the highest     *)
(*              write. It proposes a set.                                   *)
(*   Bootstrap  a member of the proposed set stores that state.             *)
(*   Decide     the decree over M0 chooses one proposal (one step here; the *)
(*              decree itself is `decree/`).                                *)
(*   Activate   a member of the chosen set serves generation 1.             *)
(*                                                                           *)
(* Beside the handover, the clients of the matchmakers: a candidate reads    *)
(* the register (it raises the read generation), writes it with a           *)
(* compare-and-set, and a leader raises the GC floor. A write `1` announces *)
(* a successor (`AB`: A in force, B open), and a write `2` switches (`B`).  *)
(*                                                                           *)
(* Constants select the rule. The first value is the rule of the rework.    *)
(*                                                                           *)
(* Finish     : "target"    = a reconfigurer proposes the target that the   *)
(*                 frozen members keep (the highest one in its quorum).     *)
(*              "answerers" = a finish proposes the members that answered   *)
(*                 the freeze, thinned to an odd count (`odd_answerers`).   *)
(* FirstWrite : "every"    = the decree chooses a proposal only when every  *)
(*                 member of it holds the bootstrap.                        *)
(*              "majority" = a majority of the proposed set is enough.      *)
(* CloseState : "max"     = the floor and the read generation are the       *)
(*                 maximum of the quorum, the register is the highest write.*)
(*              "inforce" = the register keeps only the configuration in    *)
(*                 force and drops the open successor.                      *)
(*              "noread"  = the read generation does not cross.             *)
(* Retarget   : TRUE = the coordinator names a new target when a member of  *)
(*                 the current one is down.                                 *)
(* Resize     : TRUE = the operator records a new size before the handover. *)
(* JournalFirst : TRUE = a journal records a higher survive number before  *)
(*                 its tenant's set has the size for it (decision 35).      *)
(* Clients    : TRUE = the candidates and the leader act (safety checks).   *)
(* Faults     : the most machines that are down at one time, for good.      *)
EXTENDS Naturals, FiniteSets, Sequences

CONSTANTS M0, Targets, Finish, FirstWrite, CloseState, Retarget, Resize,
          JournalFirst, Clients, Faults

Pool   == M0 \cup UNION {Targets[i] : i \in 1..Len(Targets)}
TIdx   == 1..Len(Targets)
Writes == {1, 2}
Gens   == {0, 1}
None   == <<>>

\* The value of each write: write 1 announces B, write 2 switches to B.
Val(w) == IF w = 1 THEN "AB" ELSE "B"
\* The register without its open successor.
InForce(v) == IF v = "AB" THEN "A" ELSE v

VARIABLES gen,      \* machine -> its generation (0 or 1)
          phase,    \* machine -> "active" | "frozen" | "inactive"
          up,       \* machine -> FALSE once it is down for good
          wm,       \* machine -> GC floor
          rg,       \* machine -> read generation of the register
          reg,      \* machine -> <<write generation, value>> of the register
          tgt,      \* machine -> index of the target it keeps (0: none)
          issued,   \* index of the target that the coordinator named last
          size,     \* the size of the set that the control journal records
          survive,  \* the survive number that the journal records
          props,    \* set of <<proposed set, successor state>>
          boot,     \* set of <<machine, proposal>>: bootstraps stored
          chosen,   \* None, or the proposal that the decree chose
          readDone, \* set of <<w, g>>: a read at w completed at generation g
          done,     \* set of <<w, g>>: the write w completed at generation g
          floors    \* set of <<b, g>>: the floor b is effective at generation g

vars == <<gen, phase, up, wm, rg, reg, tgt, issued, size, survive, props,
          boot, chosen, readDone, done, floors>>

Max(S) == CHOOSE x \in S : \A y \in S : y <= x
Up(a, b) == IF a >= b THEN a ELSE b
Majorities(S) == {Q \in SUBSET S : 2 * Cardinality(Q) > Cardinality(S)}
Survives(S) == (Cardinality(S) - 1) \div 2

\* The members of generation g.
Set(g) == IF g = 0 THEN M0 ELSE IF chosen = None THEN {} ELSE chosen[1]

\* The set in force for the journal of the tenant.
InForceSet == IF chosen = None THEN M0 ELSE chosen[1]

\* m serves generation g now.
Serves(m, g) == /\ m \in Set(g) /\ gen[m] = g /\ phase[m] = "active" /\ up[m]

\* A quorum of generation g where every member serves.
Serving(g) == {Q \in Majorities(Set(g)) : \A m \in Q : Serves(m, g)}

Down == {m \in Pool : ~ up[m]}

Init == /\ gen = [m \in Pool |-> 0]
        /\ phase = [m \in Pool |-> IF m \in M0 THEN "active" ELSE "inactive"]
        /\ up = [m \in Pool |-> TRUE]
        /\ wm = [m \in Pool |-> 0]
        /\ rg = [m \in Pool |-> 0]
        /\ reg = [m \in Pool |-> <<0, "A">>]
        /\ tgt = [m \in Pool |-> 0]
        /\ issued = 0
        /\ size = Cardinality(M0)
        /\ survive = Survives(M0)
        /\ props = {}
        /\ boot = {}
        /\ chosen = None
        /\ readDone = {}
        /\ done = {}
        /\ floors = {}

-----------------------------------------------------------------------------
(* The clients of the matchmakers. Each client step reaches a whole quorum   *)
(* of generation 0 at once: a step that reaches a minority adds states and   *)
(* no bug class here (`config-register/` checks it). The clients of         *)
(* generation 1 follow the same rules and are left out: the properties only *)
(* ask what generation 1 holds when it starts.                              *)

\* A candidate reads the register at the quorum Q: each member raises its
\* read generation to w.
Read(w, Q) ==
    /\ <<w, 0>> \notin readDone
    /\ Q \in Serving(0)
    /\ rg' = [m \in Pool |-> IF m \in Q THEN Up(rg[m], w) ELSE rg[m]]
    /\ readDone' = readDone \cup {<<w, 0>>}
    /\ UNCHANGED <<gen, phase, up, wm, reg, tgt, issued, size, survive, props,
                   boot, chosen, done, floors>>

\* After its read at w, the candidate writes w at the quorum Q with a
\* compare-and-set: a member refuses when it read or holds a later
\* generation, and a refusal stops the write.
Write(w, Q) ==
    /\ <<w, 0>> \in readDone
    /\ <<w, 0>> \notin done
    /\ Q \in Serving(0)
    /\ \A m \in Q : rg[m] <= w /\ reg[m][1] < w
    /\ reg' = [m \in Pool |-> IF m \in Q THEN <<w, Val(w)>> ELSE reg[m]]
    /\ done' = done \cup {<<w, 0>>}
    /\ UNCHANGED <<gen, phase, up, wm, rg, tgt, issued, size, survive, props,
                   boot, chosen, readDone, floors>>

\* The leader raises the GC floor to 1 at the quorum Q: the floor is
\* effective (the `applied` collector of `handover-gc/`).
Gc(Q) ==
    /\ <<1, 0>> \notin floors
    /\ Q \in Serving(0)
    /\ wm' = [m \in Pool |-> IF m \in Q THEN 1 ELSE wm[m]]
    /\ floors' = floors \cup {<<1, 0>>}
    /\ UNCHANGED <<gen, phase, up, rg, reg, tgt, issued, size, survive, props,
                   boot, chosen, readDone, done>>

-----------------------------------------------------------------------------
(* The control plane: the size, the survive number and the target. *)

\* The operator records a new size, before the handover starts. Only this
\* step changes the size.
RecordSize ==
    /\ Resize
    /\ issued = 0
    /\ size = Cardinality(M0)
    /\ size' = Cardinality(M0) + 2
    /\ UNCHANGED <<gen, phase, up, wm, rg, reg, tgt, issued, survive, props,
                   boot, chosen, readDone, done, floors>>

\* The journal records a survive number one higher. With the rule, the
\* tenant's set in force has the size for it first (decision 35).
RaiseSurvive ==
    /\ size > Cardinality(M0)
    /\ survive = Survives(M0)
    /\ JournalFirst \/ Survives(InForceSet) > survive
    /\ survive' = survive + 1
    /\ UNCHANGED <<gen, phase, up, wm, rg, reg, tgt, issued, size, props,
                   boot, chosen, readDone, done, floors>>

\* The coordinator names a target of the recorded size, with live members
\* only. It names a new one only when a member of the last one is down.
Issue(i) ==
    /\ chosen = None
    /\ Cardinality(Targets[i]) = size
    /\ \A m \in Targets[i] : up[m]
    /\ IF issued = 0 THEN TRUE
       ELSE Retarget /\ \E m \in Targets[issued] : ~ up[m]
    /\ issued' = i
    /\ UNCHANGED <<gen, phase, up, wm, rg, reg, tgt, size, survive, props,
                   boot, chosen, readDone, done, floors>>

Crash(m) ==
    /\ up[m]
    /\ Cardinality(Down) < Faults
    /\ up' = [up EXCEPT ![m] = FALSE]
    /\ UNCHANGED <<gen, phase, wm, rg, reg, tgt, issued, size, survive, props,
                   boot, chosen, readDone, done, floors>>

-----------------------------------------------------------------------------
(* The handover. *)

\* m freezes at generation 0, or a frozen m takes the last target. A frozen
\* member keeps its state as it is.
Stop(m) ==
    /\ issued # 0
    /\ m \in M0 /\ gen[m] = 0 /\ up[m]
    /\ phase[m] = "active" \/ (phase[m] = "frozen" /\ tgt[m] # issued)
    /\ phase' = [phase EXCEPT ![m] = "frozen"]
    /\ tgt' = [tgt EXCEPT ![m] = issued]
    /\ UNCHANGED <<gen, up, wm, rg, reg, issued, size, survive, props, boot,
                   chosen, readDone, done, floors>>

\* The members that answered, thinned to an odd count.
OddAnswerers(Q) ==
    IF Cardinality(Q) % 2 = 1 THEN Q ELSE Q \ {CHOOSE m \in Q : TRUE}

\* The register of the highest write in Q.
TopReg(Q) == LET w == Max({reg[m][1] : m \in Q})
             IN  <<w, (CHOOSE m \in Q : reg[m][1] = w)>>

\* The successor state that a frozen quorum Q gives.
Reconstruct(Q) ==
    LET top == TopReg(Q)
        v   == reg[top[2]][2]
    IN  <<Max({wm[m] : m \in Q}),
          IF CloseState = "noread" THEN 0 ELSE Max({rg[m] : m \in Q}),
          <<top[1], IF CloseState = "inforce" THEN InForce(v) ELSE v>>>>

\* A reconfigurer closes the stop on the frozen quorum Q. The coordinator's
\* own reconfigurer proposes the target that it named; a finish proposes
\* what the frozen members keep, or, with the answerer rule, the members
\* that answered.
Close(Q) ==
    /\ chosen = None
    /\ Q \in Majorities(M0)
    /\ \A m \in Q : phase[m] = "frozen" /\ up[m]
    /\ \E P \in {Targets[issued],
                 IF Finish = "target" THEN Targets[Max({tgt[m] : m \in Q})]
                 ELSE OddAnswerers(Q)} :
         props' = props \cup {<<P, Reconstruct(Q)>>}
    /\ UNCHANGED <<gen, phase, up, wm, rg, reg, tgt, issued, size, survive,
                   boot, chosen, readDone, done, floors>>

Bootstrap(m, p) ==
    /\ p \in props
    /\ m \in p[1] /\ up[m] /\ gen[m] = 0
    /\ <<m, p>> \notin boot
    /\ boot' = boot \cup {<<m, p>>}
    /\ UNCHANGED <<gen, phase, up, wm, rg, reg, tgt, issued, size, survive,
                   props, chosen, readDone, done, floors>>

Booted(p) == {m \in p[1] : <<m, p>> \in boot}

\* The decree over M0 chooses p. Its acceptors are the members of M0, so it
\* needs a live majority of them.
Decide(p) ==
    /\ chosen = None
    /\ p \in props
    /\ IF FirstWrite = "every" THEN Booted(p) = p[1]
       ELSE Booted(p) \in Majorities(p[1])
    /\ \E Q \in Majorities(M0) : \A m \in Q : up[m]
    /\ chosen' = p
    /\ UNCHANGED <<gen, phase, up, wm, rg, reg, tgt, issued, size, survive,
                   props, boot, readDone, done, floors>>

\* A member of the chosen set activates with its bootstrap. It keeps its own
\* floor, read generation and register if they are higher.
Activate(m) ==
    /\ chosen # None
    /\ m \in chosen[1] /\ up[m] /\ gen[m] = 0
    /\ <<m, chosen>> \in boot
    /\ LET s == chosen[2] IN
       /\ gen' = [gen EXCEPT ![m] = 1]
       /\ phase' = [phase EXCEPT ![m] = "active"]
       /\ wm' = [wm EXCEPT ![m] = Up(@, s[1])]
       /\ rg' = [rg EXCEPT ![m] = Up(@, s[2])]
       /\ reg' = [reg EXCEPT ![m] = IF @[1] > s[3][1] THEN @ ELSE s[3]]
    /\ UNCHANGED <<up, tgt, issued, size, survive, props, boot, chosen,
                   readDone, done, floors>>

-----------------------------------------------------------------------------

ClientNext ==
    /\ Clients
    /\ \E Q \in SUBSET M0 :
         \/ \E w \in Writes : Read(w, Q) \/ Write(w, Q)
         \/ Gc(Q)

HandoverNext ==
    \/ \E i \in TIdx : Issue(i)
    \/ \E m \in M0 : Stop(m)
    \/ \E Q \in SUBSET M0 : Close(Q)
    \/ \E m \in Pool, p \in props : Bootstrap(m, p)
    \/ \E p \in props : Decide(p)
    \/ \E m \in Pool : Activate(m)

Next ==
    \/ ClientNext
    \/ HandoverNext
    \/ RecordSize \/ RaiseSurvive
    \/ \E m \in Pool : Crash(m)

Spec == Init /\ [][Next]_vars

\* The handover steps are fair; the clients and the crashes are not.
LiveSpec ==
    /\ Spec
    /\ WF_vars(\E i \in TIdx : Issue(i))
    /\ \A m \in M0 : WF_vars(Stop(m))
    /\ WF_vars(\E Q \in SUBSET M0 : Close(Q))
    /\ \A m \in Pool : WF_vars(\E p \in props : Bootstrap(m, p))
    /\ WF_vars(\E p \in props : Decide(p))
    /\ \A m \in Pool : WF_vars(Activate(m))

-----------------------------------------------------------------------------
(* The properties. *)

TypeOK ==
    /\ gen \in [Pool -> Gens]
    /\ phase \in [Pool -> {"active", "frozen", "inactive"}]
    /\ up \in [Pool -> BOOLEAN]
    /\ wm \in [Pool -> {0, 1}]
    /\ rg \in [Pool -> {0} \cup Writes]
    /\ reg \in [Pool -> ({0} \cup Writes) \X {"A", "AB", "B"}]
    /\ tgt \in [Pool -> {0} \cup TIdx]
    /\ issued \in {0} \cup TIdx

\* Rule 2: only a resize record changes the size. The chosen set has the
\* size that the control journal records.
SizeKept == chosen # None => Cardinality(chosen[1]) = size

\* The first write to a new set needs every new member: the decree chooses
\* only a set where every member holds the state of the successor.
FirstWriteFull == chosen # None => Booted(chosen) = chosen[1]

\* An effective floor stays held: each serving quorum of the generation that
\* reported it, and of each later generation, holds it.
FloorCarried ==
    \A f \in floors : \A g \in Gens : g >= f[2] =>
        \A Q \in Serving(g) : Max({wm[m] : m \in Q}) >= f[1]

\* A completed read stays a fence: no serving quorum of a later generation
\* takes a compare-and-set below it.
ReadCarried ==
    \A r \in readDone : \A g \in Gens : g >= r[2] =>
        \A Q \in Serving(g) : Max({rg[m] : m \in Q}) >= r[1]

\* A completed write stays the register: each serving quorum of a later
\* generation reports it, or a higher write, with its value. The value of
\* write 1 is an open successor, so this also holds while one is open.
RegisterCarried ==
    \A d \in done : \A g \in Gens : g >= d[2] =>
        \A Q \in Serving(g) :
            LET top == Max({reg[m][1] : m \in Q})
            IN  /\ top >= d[1]
                /\ \A m \in Q : reg[m][1] = top => reg[m][2] = Val(top)

\* Decision 35: the matchmakers survive as many losses as the journal.
MatchmakersOutlive == survive <= Survives(InForceSet)

\* The handover ends: the decree chose a set, and each live member of it
\* serves generation 1.
Ends == <>(chosen # None /\ \A m \in chosen[1] : up[m] => gen[m] = 1)

\* Not a real invariant. TLC must find a trace: the handover ends after a
\* write with an open successor and an effective floor, and with a member of
\* M0 down.
NoHandoverWithState ==
    ~ (/\ chosen # None
       /\ \A m \in chosen[1] : gen[m] = 1
       /\ <<1, 0>> \in done
       /\ <<1, 0>> \in floors
       /\ Down # {})
=============================================================================
