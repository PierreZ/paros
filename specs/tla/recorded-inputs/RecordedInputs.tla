-------------------------- MODULE RecordedInputs --------------------------
(***************************************************************************)
(* Rule 2 of the journal's sans-IO contract: every input is recorded       *)
(* (section J4.2 of docs/journal/04-roles.md at the tag journal-design-a; #1413 (the time, the timers and *)
(* the draws in each role)).                                               *)
(*                                                                         *)
(* A host serves several roles. A role reads no clock and draws no random  *)
(* number: the driver gives the time with every input, keeps one deadline  *)
(* for each role, gives a timer only to the role that set it, and draws    *)
(* each random value and passes it in the input. Then the effects of each  *)
(* step are a function of the role's state and the recorded input alone,   *)
(* so a replay of the recorded inputs, at any later time and with any      *)
(* other random source, gives the same effects.                            *)
(*                                                                         *)
(* A role asks a timer (Arm) at the time of its input, plus a fixed        *)
(* Delay, plus a jitter that is a random draw (the jitter of a challenge   *)
(* of the election). The timer fires (Fire) at or after its deadline.      *)
(*                                                                         *)
(* Rule selects how the host and the roles do it:                          *)
(*   "recorded"  the rule: the time and the draw are in the input, and     *)
(*               the host keeps one deadline for each role;                *)
(*   "clock"     control: a role reads the clock of the process in place   *)
(*               of the time in its input;                                 *)
(*   "draw"      control: a role draws its jitter itself, and the input    *)
(*               records no draw;                                          *)
(*   "shared"    control: the host keeps one timer for every role, and a   *)
(*               fire goes to every role, as the journal Host before #1413.*)
(***************************************************************************)
EXTENDS Naturals, Sequences

CONSTANTS
  Roles,     \* the roles of the host
  MaxTime,   \* the last time of the clock
  Delay,     \* the fixed part of a timer
  Jitters,   \* the values of a draw
  MaxSteps,  \* the most steps that a behaviour records
  Rule       \* "recorded", "clock", "draw" or "shared"

ASSUME Rule \in {"recorded", "clock", "draw", "shared"}
ASSUME Jitters \subseteq Nat /\ Jitters # {}
ASSUME Delay \in Nat \ {0}

None == MaxTime + Delay + 100   \* no deadline: later than every deadline
Times == 0..MaxTime

\* An input as the driver records it.
Inputs == [kind : {"arm", "fire"}, role : Roles, now : Times, draw : Jitters \cup {0}]
\* An effect: a timer that a role sets, or a fire that a role handles.
Effects == [kind : {"set", "fired"}, role : Roles, at : Nat]

RoleStates == [armed : Nat, fires : Nat]
InitRoles == [r \in Roles |-> [armed |-> None, fires |-> 0]]

VARIABLES
  clock,     \* the clock of the process
  deadline,  \* deadline[r]: the timer of role r that the host holds
  roles,     \* roles[r]: the state of role r
  log,       \* the recorded steps, <<input, effects>>
  stray      \* a fire reached a role before its own deadline, or with none

vars == <<clock, deadline, roles, log, stray>>

(***************************************************************************)
(* The step of a role: a pure function of its state and its input. `t` is  *)
(* the clock of the process when the step runs, and `j` a draw that the    *)
(* role would make itself; under the rule the step reads neither.          *)
(***************************************************************************)
TimeOf(in, t) == IF Rule = "clock" THEN t ELSE in.now
JitterOf(in, j) == IF Rule = "draw" THEN j ELSE in.draw

Step(st, in, t, j) ==
  LET r == in.role IN
  IF in.kind = "arm"
  THEN LET at == TimeOf(in, t) + Delay + JitterOf(in, j) IN
       <<[st EXCEPT ![r].armed = at], <<[kind |-> "set", role |-> r, at |-> at]>>>>
  ELSE <<[st EXCEPT ![r].armed = None, ![r].fires = @ + 1],
         <<[kind |-> "fired", role |-> r, at |-> TimeOf(in, t)]>>>>

\* The deadline of the shared timer: the earliest timer of any role.
Earliest == CHOOSE d \in {deadline[r] : r \in Roles} : \A r \in Roles : d <= deadline[r]

TypeOK ==
  /\ clock \in Times
  /\ deadline \in [Roles -> Nat]
  /\ roles \in [Roles -> RoleStates]
  /\ log \in Seq(Inputs \X Seq(Effects))
  /\ stray \in BOOLEAN

Init ==
  /\ clock = 0
  /\ deadline = [r \in Roles |-> None]
  /\ roles = InitRoles
  /\ log = <<>>
  /\ stray = FALSE

\* Record one step of role r on input `in`, with the role's own draw `j`.
Record(in, j) ==
  LET res == Step(roles, in, clock, j)
      r == in.role
  IN /\ roles' = res[1]
     /\ log' = Append(log, <<in, res[2]>>)
     /\ stray' = (stray \/ (in.kind = "fire" /\ (roles[r].armed = None \/ roles[r].armed > clock)))

\* A role asks a timer; one that has a timer sets it again, and the host
\* keeps the new deadline. The driver draws d and records it.
Arm(r) ==
  /\ \E d, j \in Jitters :
       LET in == [kind |-> "arm", role |-> r, now |-> clock,
                  draw |-> IF Rule = "draw" THEN 0 ELSE d]
       IN /\ Record(in, j)
          /\ deadline' = [deadline EXCEPT ![r] = Step(roles, in, clock, j)[2][1].at]
  /\ UNCHANGED clock

FireInput(r) == [kind |-> "fire", role |-> r, now |-> clock, draw |-> 0]

\* The deadline that lets the host fire a timer to role r: r's own, or,
\* under "shared", the earliest of every role.
Due(r) == IF Rule = "shared" THEN Earliest ELSE deadline[r]

\* The host gives the fire to r alone, and forgets r's deadline.
FireOne(r) ==
  /\ Record(FireInput(r), 0)
  /\ deadline' = [deadline EXCEPT ![r] = None]

\* Under "shared", the fire goes to every role, one step each, r first.
\* Two roles are enough for the control.
FireTwo(r) ==
  \E r2 \in Roles \ {r} :
    LET in1 == FireInput(r)
        in2 == FireInput(r2)
        s1 == Step(roles, in1, clock, 0)
        s2 == Step(s1[1], in2, clock, 0)
    IN /\ roles' = s2[1]
       /\ log' = log \o <<<<in1, s1[2]>>, <<in2, s2[2]>>>>
       /\ stray' = (stray
                    \/ roles[r].armed = None \/ roles[r].armed > clock
                    \/ roles[r2].armed = None \/ roles[r2].armed > clock)
       /\ deadline' = [x \in Roles |-> None]

\* A timer is due: the host fires it.
Fire(r) ==
  /\ Due(r) <= clock
  /\ IF Rule = "shared" THEN FireTwo(r) ELSE FireOne(r)
  /\ UNCHANGED clock

Tick ==
  /\ clock < MaxTime
  /\ clock' = clock + 1
  /\ UNCHANGED <<deadline, roles, log, stray>>

Next ==
  \/ \E r \in Roles : Arm(r) \/ Fire(r)
  \/ Tick

\* The bound of a check: the recorded steps, as a state constraint.
Bounded == Len(log) <= MaxSteps

Spec == Init /\ [][Next]_vars

(***************************************************************************)
(* The replay: fold the recorded inputs from the first state, with a clock *)
(* `t` and a random source `j` that may differ from the recording.         *)
(***************************************************************************)
RECURSIVE Replays(_, _, _, _)
\* Whether the steps from i on give their recorded effects from state st.
Replays(st, i, t, j) ==
  IF i > Len(log) THEN TRUE
  ELSE LET res == Step(st, log[i][1], t, j)
       IN res[2] = log[i][2] /\ Replays(res[1], i + 1, t, j)

\* Every recorded run replays to the same effects, at any time and with any
\* random source.
ReplayExact == \A t \in Times, j \in Jitters : Replays(InitRoles, 1, t, j)

\* A role gets a fire only at or after the deadline that it set itself.
OwnTimer == ~stray

(***************************************************************************)
(* Reach checks: each of these is false in some behaviour, so the checks   *)
(* above judge runs that fire timers and hold two timers at once.          *)
(***************************************************************************)
NoFire == \A r \in Roles : roles[r].fires = 0
NoTwoTimers == \A r1, r2 \in Roles : r1 # r2 => (deadline[r1] = None \/ deadline[r2] = None)
=============================================================================
