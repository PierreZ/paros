--------------------------- MODULE HandoverScalars ---------------------------
(***************************************************************************)
(* The scalars of a journal across one handover of its matchmaker set, for *)
(* #1230 (rules no check tests), items "retirement records across a        *)
(* matchmaker handover" and "the effective configuration".                 *)
(*                                                                         *)
(* `handover-gc/` carries the GC floor and the registrations across a      *)
(* handover. A journal's scalars (`JournalScalars`,                        *)
(* `matchmaker/state.rs`) also hold two things that a registration does    *)
(* not:                                                                    *)
(*   - The retirement record of each node (#1002 (retire fence gaps),      *)
(*     #1037 (rejoin)): an incarnation and a flag. A retirement check      *)
(*     (`MatchPurpose::Retire`) of incarnation i records [i, retired]; a   *)
(*     rejoin (`MatchPurpose::Rejoin`) of incarnation j clears it to       *)
(*     [j, not retired]. A check of an incarnation below the record is     *)
(*     refused (`Rejoined`). Each completes only when EVERY matchmaker of  *)
(*     one generation agreed (`RetireCheck`, `RejoinCheck`, `all_agreed`). *)
(*   - The effective configuration: the highest ballot of a reconfiguration *)
(*     registration that the matchmaker took. Unlike the registration, the *)
(*     GC floor never drops it.                                            *)
(*                                                                         *)
(* The set M0 is replaced by M1 (`handover-gc/`). A member of M0 freezes   *)
(* and reports its scalars (`Stopped`); a frozen member answers no check   *)
(* and takes no registration of its generation (`generation_refusal`).    *)
(* The successor state is built from a majority of reports (`close_stop`): *)
(* the records merged node by node (`Retirement::merge`: the later         *)
(* incarnation wins, equal ones OR their flags), the maximum effective     *)
(* configuration and the maximum floor. A member of M1 activates with the  *)
(* state merged into its own (`Retirement::merge_all` in the activation).  *)
(*                                                                         *)
(* Constants (the first value is the code; each other is a control):      *)
(*   Ask        "all": a check completes when every matchmaker of its      *)
(*              generation agreed. "quorum": a majority is enough.         *)
(*   Frozen     FALSE: a frozen member answers no check. TRUE: it answers  *)
(*              and records, after its report left.                        *)
(*   Close      "merge": the records of every report of the stop quorum,   *)
(*              merged. "one": the records of one report.                  *)
(*   Eff        "scalar": the successor's effective configuration is the   *)
(*              maximum of the reports' scalars. "history": it is the      *)
(*              highest reconfiguration among the registrations carried    *)
(*              above the floor (what a matchmaker without the scalar      *)
(*              could rebuild).                                            *)
(*   Nodes, NI  the nodes that retire and rejoin, and their incarnations.  *)
(*   NB, Reconf the ballots of registrations, and those that reconfigure.  *)
(*                                                                         *)
(* Not in the model: the registrations that block a retirement (a check    *)
(* agrees only when no registration at or above its floor names the node; *)
(* `retire-fence/` checks that rule), Phase 1, the decree that chooses the *)
(* successor (one choice, in one step), lost matchmaker state, more than   *)
(* one handover (`generations/` chains them for the floor and the          *)
(* registrations), more than one journal.                                  *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS M0, M1, Nodes, NI, NB, Reconf, Ask, Frozen, Close, Eff

ASSUME Ask \in {"all", "quorum"}
ASSUME Frozen \in BOOLEAN
ASSUME Close \in {"merge", "one"}
ASSUME Eff \in {"scalar", "history"}
ASSUME Reconf \subseteq 1..NB

Pool    == M0 \cup M1
Gens    == {0, 1}
Incs    == 1..NI
Ballots == 1..NB
None    == <<>>
NoRec   == [inc |-> 0, ret |-> FALSE]

Max(S) == IF S = {} THEN 0 ELSE CHOOSE x \in S : \A y \in S : y <= x
Up(a, b) == IF a >= b THEN a ELSE b
Set(g) == IF g = 0 THEN M0 ELSE M1
Majorities(S) == {Q \in SUBSET S : 2 * Cardinality(Q) > Cardinality(S)}

\* `Retirement::merge`: the later incarnation wins; at one incarnation the
\* node is retired if either record says so.
Merge(r1, r2) ==
  IF r1.inc > r2.inc THEN r1
  ELSE IF r2.inc > r1.inc THEN r2
  ELSE [inc |-> r1.inc, ret |-> r1.ret \/ r2.ret]

RECURSIVE MergeSet(_)
MergeSet(S) ==
  IF S = {} THEN NoRec
  ELSE LET r == CHOOSE x \in S : TRUE IN Merge(r, MergeSet(S \ {r}))

VARIABLES gen,      \* matchmaker -> its generation (0 or 1)
          phase,    \* matchmaker -> "active" | "frozen" | "inactive"
          rec,      \* matchmaker -> node -> [inc, ret]: the retirement record
          wm,       \* matchmaker -> GC floor
          regs,     \* matchmaker -> registered ballots at or above the floor
          eff,      \* matchmaker -> the effective configuration (a ballot, 0: none)
          inc,      \* node -> its incarnation now
          agreed,   \* <<kind, node, incarnation, generation>> -> matchmakers that agreed
          done,     \* set of <<kind, node, incarnation, generation>>: completed checks
          matched,  \* set of <<ballot, generation>>: registered at a majority
          reports,  \* set of <<matchmaker, rec, floor, regs, eff>>
          chosen    \* None, or the successor state [rec, wm, regs, eff]

vars == <<gen, phase, rec, wm, regs, eff, inc, agreed, done, matched, reports, chosen>>

Kinds == {"retire", "rejoin"}
Keys  == Kinds \X Nodes \X Incs \X Gens

Init ==
  /\ gen = [m \in Pool |-> 0]
  /\ phase = [m \in Pool |-> IF m \in M0 THEN "active" ELSE "inactive"]
  /\ rec = [m \in Pool |-> [n \in Nodes |-> NoRec]]
  /\ wm = [m \in Pool |-> 0]
  /\ regs = [m \in Pool |-> {}]
  /\ eff = [m \in Pool |-> 0]
  /\ inc = [n \in Nodes |-> 1]
  /\ agreed = [k \in Keys |-> {}]
  /\ done = {}
  /\ matched = {}
  /\ reports = {}
  /\ chosen = None

\* m serves generation g now.
Serves(m, g) == m \in Set(g) /\ gen[m] = g /\ phase[m] = "active"

\* m answers a check of generation g: it serves g, or (control) it is a
\* frozen member of g whose report already left.
Answers(m, g) ==
  \/ Serves(m, g)
  \/ Frozen /\ m \in Set(g) /\ gen[m] = g /\ phase[m] = "frozen"

-----------------------------------------------------------------------------
(* The node *)

\* The node comes back as a new incarnation (a restart with its store).
Reboot(n) ==
  /\ inc[n] < NI
  /\ inc' = [inc EXCEPT ![n] = @ + 1]
  /\ UNCHANGED <<gen, phase, rec, wm, regs, eff, agreed, done, matched, reports, chosen>>

\* A retirement check of incarnation i of the node reaches m (`answer_probe`,
\* `retire`): m records the node as retired and agrees. The check of an
\* earlier incarnation can arrive late. A record of a later incarnation
\* refuses it (`Rejoined`).
RetireAt(n, i, g, m) ==
  /\ Answers(m, g)
  /\ i >= rec[m][n].inc
  /\ rec' = [rec EXCEPT ![m][n] = [inc |-> i, ret |-> TRUE]]
  /\ agreed' = [agreed EXCEPT ![<<"retire", n, i, g>>] = @ \cup {m}]
  /\ UNCHANGED <<gen, phase, wm, regs, eff, inc, done, matched, reports, chosen>>

\* A rejoin of the node's incarnation reaches m (`rejoin`): m clears every
\* retirement of an earlier incarnation. A record of this incarnation or a
\* later one stays. A rejoin follows a retirement of an earlier incarnation.
RejoinAt(n, g, m) ==
  /\ Answers(m, g)
  /\ inc[n] > 1
  /\ rec' = [rec EXCEPT ![m][n] = IF inc[n] > @.inc THEN [inc |-> inc[n], ret |-> FALSE] ELSE @]
  /\ agreed' = [agreed EXCEPT ![<<"rejoin", n, inc[n], g>>] = @ \cup {m}]
  /\ UNCHANGED <<gen, phase, wm, regs, eff, inc, done, matched, reports, chosen>>

\* A check completes: every matchmaker of its generation agreed (the code),
\* or a majority (the control).
Complete(k) ==
  /\ IF Ask = "all" THEN Set(k[4]) \subseteq agreed[k]
     ELSE agreed[k] \in Majorities(Set(k[4])) \/ Set(k[4]) \subseteq agreed[k]
  /\ done' = done \cup {k}
  /\ UNCHANGED <<gen, phase, rec, wm, regs, eff, inc, agreed, matched, reports, chosen>>

-----------------------------------------------------------------------------
(* Registrations and the GC floor *)

\* m serving g takes the registration of b; a reconfiguration raises its
\* effective configuration (`raise_effective`).
Register(b, g, m) ==
  /\ Serves(m, g)
  /\ b >= wm[m]
  /\ regs' = [regs EXCEPT ![m] = @ \cup {b}]
  /\ eff' = IF b \in Reconf THEN [eff EXCEPT ![m] = Up(@, b)] ELSE eff
  /\ UNCHANGED <<gen, phase, rec, wm, inc, agreed, done, matched, reports, chosen>>

Match(b, g) ==
  /\ \E Q \in Majorities(Set(g)) : \A m \in Q : gen[m] = g /\ b \in regs[m]
  /\ matched' = matched \cup {<<b, g>>}
  /\ UNCHANGED <<gen, phase, rec, wm, regs, eff, inc, agreed, done, reports, chosen>>

\* The leader of a matched ballot b raises the floor of m to b: m drops its
\* registrations below it. The effective configuration stays.
Gc(b, g, m) ==
  /\ <<b, g>> \in matched
  /\ Serves(m, g)
  /\ wm' = [wm EXCEPT ![m] = Up(@, b)]
  /\ regs' = [regs EXCEPT ![m] = {r \in @ : r >= Up(wm[m], b)}]
  /\ UNCHANGED <<gen, phase, rec, eff, inc, agreed, done, matched, reports, chosen>>

-----------------------------------------------------------------------------
(* The handover *)

Stop(m) ==
  /\ m \in M0 /\ gen[m] = 0 /\ phase[m] = "active"
  /\ phase' = [phase EXCEPT ![m] = "frozen"]
  /\ reports' = reports \cup {<<m, rec[m], wm[m], regs[m], eff[m]>>}
  /\ UNCHANGED <<gen, rec, wm, regs, eff, inc, agreed, done, matched, chosen>>

CloseStop(Q) ==
  /\ chosen = None
  /\ Q \in Majorities(M0)
  /\ \A m \in Q : \E r \in reports : r[1] = m
  /\ LET R  == {r \in reports : r[1] \in Q}
         one == CHOOSE r \in R : TRUE
         f  == Max({r[2 + 1] : r \in R})
         carried == {b \in UNION {r[4] : r \in R} : b >= f}
         recs == IF Close = "merge"
                 THEN [n \in Nodes |-> MergeSet({r[2][n] : r \in R})]
                 ELSE one[2]
         e  == IF Eff = "scalar" THEN Max({r[5] : r \in R})
               ELSE Max(carried \cap Reconf)
     IN  chosen' = [rec |-> recs, wm |-> f, regs |-> carried, eff |-> e]
  /\ UNCHANGED <<gen, phase, rec, wm, regs, eff, inc, agreed, done, matched, reports>>

\* A member of M1 activates generation 1 with the successor state, merged
\* into what it holds.
Activate(m) ==
  /\ chosen # None
  /\ m \in M1 /\ gen[m] = 0
  /\ gen' = [gen EXCEPT ![m] = 1]
  /\ phase' = [phase EXCEPT ![m] = "active"]
  /\ rec' = [rec EXCEPT ![m] = [n \in Nodes |-> Merge(@[n], chosen.rec[n])]]
  /\ wm' = [wm EXCEPT ![m] = Up(@, chosen.wm)]
  /\ regs' = [regs EXCEPT ![m] = {b \in chosen.regs : b >= Up(wm[m], chosen.wm)}]
  /\ eff' = [eff EXCEPT ![m] = Up(@, chosen.eff)]
  /\ UNCHANGED <<inc, agreed, done, matched, reports, chosen>>

Next ==
  \/ \E n \in Nodes : Reboot(n)
  \/ \E n \in Nodes, g \in Gens, m \in Pool :
       \/ \E i \in 1..inc[n] : RetireAt(n, i, g, m)
       \/ RejoinAt(n, g, m)
  \/ \E k \in Keys : Complete(k)
  \/ \E b \in Ballots, g \in Gens, m \in Pool : Register(b, g, m) \/ Gc(b, g, m)
  \/ \E b \in Ballots, g \in Gens : Match(b, g)
  \/ \E m \in Pool : Stop(m) \/ Activate(m)
  \/ \E Q \in SUBSET M0 : CloseStop(Q)

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Properties *)

\* A completed retirement of incarnation i is held by every member that
\* serves its generation or a later one: the member refuses a registration
\* that names the node (`MatchRefusal::Retired`), unless a later incarnation
\* came back (`inc > i`).
RetireHeld ==
  \A k \in done : k[1] = "retire" =>
    \A g \in Gens, m \in Pool : g >= k[4] /\ Serves(m, g) =>
      rec[m][k[2]].inc > k[3] \/ (rec[m][k[2]].inc = k[3] /\ rec[m][k[2]].ret)

\* The same, for the later generation only: what the handover carries. With
\* `Ask = "quorum"` the generation of the check itself already breaks
\* `RetireHeld` (`retire-fence/ask_quorum`); this one isolates the handover.
RetireCarried ==
  \A k \in done : k[1] = "retire" =>
    \A m \in Pool : k[4] < gen[m] /\ Serves(m, gen[m]) =>
      rec[m][k[2]].inc > k[3] \/ (rec[m][k[2]].inc = k[3] /\ rec[m][k[2]].ret)

\* A completed rejoin of incarnation j is held: no member that serves its
\* generation or a later one holds an earlier incarnation, so none refuses
\* the node as retired from before it came back.
RejoinHeld ==
  \A k \in done : k[1] = "rejoin" =>
    \A g \in Gens, m \in Pool : g >= k[4] /\ Serves(m, g) =>
      rec[m][k[2]].inc >= k[3]

\* A reconfiguration registered at a majority of g is the effective
\* configuration (or a later one) at each member that serves a later
\* generation, and at a member of each majority of g that serves it.
EffCarried ==
  \A k \in matched : k[1] \in Reconf =>
    /\ \A m \in Pool : k[2] < gen[m] /\ Serves(m, gen[m]) => eff[m] >= k[1]
    /\ \A Q \in Majorities(Set(k[2])) :
         (\A m \in Q : Serves(m, k[2])) => \E m \in Q : eff[m] >= k[1]

\* A matchmaker holds no registration below its floor: it refuses one
\* (`b >= wm`), and the GC and the activation drop them.
RegsAboveFloor == \A m \in Pool : \A r \in regs[m] : r >= wm[m]

\* An active matchmaker serves a generation whose set holds it.
ActiveInSet == \A m \in Pool : phase[m] = "active" => m \in Set(gen[m])

\* The successor state is chosen once.
ChosenOnce == [][chosen # None => chosen' = chosen]_vars

\* Reachability controls: TLC must find a state for each.
\* A retirement completes at generation 0 and every member of M1 is active.
NoRetireCarried ==
  ~ (/\ \E k \in done : k[1] = "retire" /\ k[4] = 0
     /\ \A m \in M1 : gen[m] = 1)
\* A rejoin completes after a retirement completed, and both cross.
NoRejoinCarried ==
  ~ (/\ \E k \in done : k[1] = "retire" /\ k[4] = 0
     /\ \E k \in done : k[1] = "rejoin" /\ k[4] = 0
     /\ \A m \in M1 : gen[m] = 1)
\* The floor passes a matched reconfiguration before the handover.
NoEffBelowFloor ==
  ~ (/\ \E b \in Reconf : <<b, 0>> \in matched
     /\ \E r \in reports : \E b \in Reconf : r[3] > b /\ r[5] = b
     /\ chosen # None)
=============================================================================
